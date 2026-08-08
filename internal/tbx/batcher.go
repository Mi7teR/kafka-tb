package tbx

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

var (
	// ErrCommandTooLarge — команда не помещается в батч целиком.
	// Разрезать нельзя: атомарность linked-цепочки важнее.
	ErrCommandTooLarge = errors.New("command exceeds max batch size")
	ErrClosed          = errors.New("batcher closed")
)

const linkedBit uint16 = 1

type job struct {
	cmd  *model.Command
	done chan SubmitResult
}

// SubmitResult — исход одной команды: либо исходы всех её событий, либо ошибка.
// Ровно один такой результат приходит на каждую поставленную в очередь команду.
type SubmitResult struct {
	Outcomes []Outcome
	Err      error
}

// Batcher — единственная дверь в TigerBeetle.
// Держит один воркер с одной очередью и не больше одного батча в полёте, чем
// гарантирует, что порядок применения совпадает с порядком Submit.
type Batcher struct {
	client  Client
	cfg     config.Batcher
	retry   config.Retry
	log     *slog.Logger
	metrics *obs.Metrics

	// queue — одна на обе операции. Отдельная очередь на тип операции — это
	// отдельный воркер, а два воркера и есть та самая одновременность, которой
	// быть не должно: create_accounts и create_transfers одной партиции
	// разъехались бы, и трансфер со счёта, который ещё не создан, вернул бы
	// debit_account_not_found — бизнес-отказ и DLQ для законной записи.
	queue chan *job

	// stop — единственный сигнал остановки, общий для всех участников.
	// Закрывается ровно один раз: либо из Close(), либо при отмене контекста
	// Start. Оба пути обязаны сходиться сюда — иначе отмена контекста гасит
	// цикл, а отправители продолжают считать батчер живым и виснут навсегда.
	// Протокол намеренно бесlock'овый: закрытый канал одинаково виден всем,
	// поэтому у отправителя всегда есть выход из блокирующего select.
	stopOnce sync.Once
	stop     chan struct{}

	// finished — «цикл вышел», а не «остановку запросили». Разница
	// принципиальна: пока цикл жив, он ещё может ответить уже поставленной
	// в очередь команде, и отправитель обязан этого ответа дождаться.
	// Отправитель, ждущий исход, выходит только по finished — тогда
	// «никто уже не ответит» — гарантия, а не догадка.
	finishedOnce sync.Once
	finished     chan struct{}

	// unwatch снимает подписку на отмену контекста Start.
	// Пишется в Start, читается в Close; контракт жизненного цикла
	// (ровно один Start, Close строго после него) описан в их доккоментариях.
	unwatch func() bool
	wg      sync.WaitGroup
}

func NewBatcher(c Client, cfg config.Batcher, retry config.Retry, log *slog.Logger, metrics *obs.Metrics) *Batcher {
	return &Batcher{
		client:   c,
		cfg:      cfg,
		retry:    retry,
		log:      log,
		metrics:  metrics,
		queue:    make(chan *job, cfg.MaxQueue),
		stop:     make(chan struct{}),
		finished: make(chan struct{}),
	}
}

// Start запускает цикл отправки. Вызывается ровно один раз и строго до Close;
// повторный или конкурентный с Close вызов не поддерживается.
func (b *Batcher) Start(ctx context.Context) {
	// Отмена контекста — это тот же shutdown, что и Close().
	b.unwatch = context.AfterFunc(ctx, b.signalStop)
	b.wg.Add(1)
	go func() { defer b.wg.Done(); b.loop() }()
	// Наблюдатель живёт здесь, а не в Close: путь остановки по отмене контекста
	// не проходит через Close, но отправителей отпускать обязан так же.
	go func() { b.wg.Wait(); b.signalFinished() }()
}

// SubmitAsync ставит команду в очередь и сразу возвращает канал, в который
// придёт ровно один исход. Ошибка возвращается только на самой постановке:
// пустая или слишком большая команда, остановленный батчер, отменённый контекст.
// Всё, что случилось после постановки, приходит в канал, а не в эту ошибку.
//
// Блокировка при полной очереди сохранена — это backpressure для консьюмера:
// без него синк поставит в очередь весь опрос и съест память.
//
// Канал буферизован на единицу, и писатель у него ровно один, поэтому
// вызывающий, бросивший канал не прочитав, никого не блокирует.
func (b *Batcher) SubmitAsync(ctx context.Context, cmd *model.Command) (<-chan SubmitResult, error) {
	if cmd.Len() == 0 {
		return nil, errors.New("empty command")
	}
	if cmd.Len() > b.cfg.MaxBatchSize {
		return nil, ErrCommandTooLarge
	}
	j := &job{cmd: cmd, done: make(chan SubmitResult, 1)}

	// Быстрый отказ, когда батчер уже остановлен: иначе select ниже выбирал бы
	// между stop и свободным местом в очереди случайно.
	select {
	case <-b.stop:
		return nil, ErrClosed
	default:
	}

	// stop обязателен в этом select: без него отправитель, упёршийся в полную
	// очередь после остановки циклов, блокируется навсегда.
	select {
	case <-b.stop:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case b.queue <- j:
	}

	// Ожидание исхода живёт здесь, а не у вызывающего: держатель канала знает
	// только его и не может сам выяснить, что отвечать уже некому. Отдать ему
	// голый j.done значило бы молчание вместо ErrClosed для команды, которую
	// не увидел ни один цикл, — для синка это потерянное сообщение.
	out := make(chan SubmitResult, 1)
	go func() {
		select {
		case res := <-j.done:
			out <- res
		case <-b.finished:
			// Здесь ждём именно finished, а не stop. stop означает лишь «начали
			// останавливаться»: батч этой команды может быть уже в TigerBeetle и
			// вот-вот вернуть исход. Ответить в этот момент ErrClosed — соврать
			// про применённую работу; вызывающий не обязан повторять запрос с
			// тем же id и восстановить правду ему неоткуда.
			// finished же закрывается после выхода цикла, то есть когда
			// ответить этой команде уже некому.
			//
			// Гонка «исход доставлен ровно в момент выхода цикла» реальна:
			// оба канала готовы, и select выбрал бы случайно. Приоритет исхода
			// восстанавливаем явной непустой проверкой.
			select {
			case res := <-j.done:
				out <- res
			default:
				// Команда попала в очередь так поздно, что цикл её уже не
				// увидел. Отвечаем ошибкой, а не молчим; дальше расчёт на
				// идемпотентность по id — повтор даёт TransferExists/
				// AccountExists, а MapTransferResults/MapAccountResults
				// трактуют их как StatusOK.
				out <- SubmitResult{Err: ErrClosed}
			}
		}
	}()
	return out, nil
}

// Submit ставит команду в очередь и ждёт исход.
// Блокировка при полной очереди — это backpressure для консьюмера.
func (b *Batcher) Submit(ctx context.Context, cmd *model.Command) ([]Outcome, error) {
	done, err := b.SubmitAsync(ctx, cmd)
	if err != nil {
		return nil, err
	}
	select {
	case res := <-done:
		return res.Outcomes, res.Err
	case <-ctx.Done():
		// Команда уже в очереди и может дойти до TigerBeetle после этого
		// возврата: отменить её отсюда нельзя. Вызывающий (Kafka-синк) увидит
		// ошибку и, скорее всего, повторит команду. Корректность здесь
		// целиком держится на идемпотентности по id: повтор даёт
		// TransferExists/AccountExists, а MapTransferResults/MapAccountResults
		// трактуют их как StatusOK. Это работает только потому, что id
		// приходят от вызывающего и стабильны между попытками.
		return nil, ctx.Err()
	}
}

// Close закрывает приём и дожидается, пока цикл разгребёт очередь.
// Вызывается строго после Start и не конкурентно с ним.
//
// Close не ограничен по времени: он ждёт завершения текущего запроса к
// TigerBeetle. Зависший вызов клиента задержит и Close, и отправителей,
// ожидающих свой исход. Это сознательный размен — синхронному вызывающему
// не сообщают об ошибке по операции, которая на самом деле применилась.
// Ограничивать это надо таймаутом на стороне клиента, не здесь.
func (b *Batcher) Close() {
	b.signalStop()
	if b.unwatch != nil {
		b.unwatch()
	}
	b.wg.Wait()
	// Подстраховка на случай, когда Start не вызывали: наблюдателя нет,
	// а отправитель без finished ждал бы исход вечно.
	b.signalFinished()
}

// signalStop закрывает stop ровно один раз, откуда бы ни пришёл сигнал.
func (b *Batcher) signalStop() {
	b.stopOnce.Do(func() { close(b.stop) })
}

// signalFinished закрывает finished ровно один раз.
func (b *Batcher) signalFinished() {
	b.finishedOnce.Do(func() { close(b.finished) })
}

// loop копит подряд идущие команды одной операции и отправляет накопленное,
// когда операция сменилась, набрался max_batch_size или истёк linger — что
// наступит раньше. Смена операции — такой же повод отправить, как и остальные
// два: один запрос к TigerBeetle несёт события ровно одного типа, смешать
// счета с трансферами нельзя. Отправка идёт строго последовательно, поэтому
// порядок применения совпадает с порядком постановки и на стыке операций тоже.
func (b *Batcher) loop() {
	var (
		batch []*job
		size  int
		op    model.Op
		timer *time.Timer
		tick  <-chan time.Time
	)
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer, tick = nil, nil
		}
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		stopTimer()
		if err := b.send(op, batch); err != nil {
			b.failAll(batch, err)
		}
		batch, size = nil, 0
	}

	for {
		select {
		case <-b.stop:
			// Накопленное всё равно отправляем, остаток очереди отвечаем ошибкой:
			// каждая команда, попавшая в очередь, получает ровно один исход.
			flush()
			b.drain(b.queue, ErrClosed)
			return
		case j := <-b.queue:
			// Пробег одной операции кончился — отправляем его целиком, следующий
			// начинается с этой команды.
			if len(batch) > 0 && j.cmd.Op != op {
				flush()
			}
			// Команда не влезает в остаток — отправляем накопленное и начинаем новый батч.
			if size+j.cmd.Len() > b.cfg.MaxBatchSize {
				flush()
			}
			op = j.cmd.Op
			batch = append(batch, j)
			size += j.cmd.Len()
			if size >= b.cfg.MaxBatchSize {
				flush()
				continue
			}
			if timer == nil {
				timer = time.NewTimer(b.cfg.Linger)
				tick = timer.C
			}
		case <-tick:
			flush()
		}
	}
}

func (b *Batcher) drain(queue chan *job, err error) {
	for {
		select {
		case j := <-queue:
			j.done <- SubmitResult{Err: err}
		default:
			return
		}
	}
}

func (b *Batcher) failAll(jobs []*job, err error) {
	for _, j := range jobs {
		j.done <- SubmitResult{Err: err}
	}
}

// send выбирает запрос по операции накопленного пробега. Пробег однороден по
// построению: loop отправляет накопленное, как только операция сменилась.
func (b *Batcher) send(op model.Op, jobs []*job) error {
	if op == model.OpCreateAccounts {
		return b.sendAccounts(jobs)
	}
	return b.sendTransfers(jobs)
}

func (b *Batcher) sendTransfers(jobs []*job) error {
	events := make([]types.Transfer, 0, b.cfg.MaxBatchSize)
	offsets := make([]int, len(jobs))
	for i, j := range jobs {
		offsets[i] = len(events)
		events = append(events, j.cmd.Transfers...)
		// Цепочка не должна оставаться открытой на стыке команд.
		events[len(events)-1].Flags &^= linkedBit
	}

	b.metrics.ObserveBatchSize(len(events))
	start := time.Now()
	results, err := b.call(func() (any, error) { return b.client.CreateTransfers(events) })
	b.metrics.ObserveTBLatency(string(model.OpCreateTransfers), time.Since(start))
	if err != nil {
		return err
	}
	typed, _ := results.([]types.CreateTransferResult)
	for i, j := range jobs {
		outcomes, mapErr := MapTransferResults(j.cmd, typed, offsets[i], len(events))
		j.done <- SubmitResult{Outcomes: outcomes, Err: mapErr}
	}
	return nil
}

func (b *Batcher) sendAccounts(jobs []*job) error {
	events := make([]types.Account, 0, b.cfg.MaxBatchSize)
	offsets := make([]int, len(jobs))
	for i, j := range jobs {
		offsets[i] = len(events)
		events = append(events, j.cmd.Accounts...)
		events[len(events)-1].Flags &^= linkedBit
	}

	b.metrics.ObserveBatchSize(len(events))
	start := time.Now()
	results, err := b.call(func() (any, error) { return b.client.CreateAccounts(events) })
	b.metrics.ObserveTBLatency(string(model.OpCreateAccounts), time.Since(start))
	if err != nil {
		return err
	}
	typed, _ := results.([]types.CreateAccountResult)
	for i, j := range jobs {
		outcomes, mapErr := MapAccountResults(j.cmd, typed, offsets[i], len(events))
		j.done <- SubmitResult{Outcomes: outcomes, Err: mapErr}
	}
	return nil
}

// call повторяет вызов, пока TigerBeetle не ответит или батчер не остановят.
// Ошибка вызова — всегда инфраструктурная: отказ по бизнесу приходит в результатах.
func (b *Batcher) call(fn func() (any, error)) (any, error) {
	delay := b.retry.Initial
	for attempt := 1; ; attempt++ {
		res, err := fn()
		if err == nil {
			return res, nil
		}
		b.log.Warn("tigerbeetle call failed, retrying",
			slog.Int("attempt", attempt), slog.String("error", err.Error()), slog.Duration("in", delay))

		// stop, а не только Close: при отмене контекста и лежащем TigerBeetle
		// ретраи иначе крутятся вечно, батч не завершается, горутина течёт.
		select {
		case <-b.stop:
			return nil, ErrClosed
		case <-time.After(b.jitter(delay)):
		}
		if delay < b.retry.Max {
			delay *= 2
			if delay > b.retry.Max {
				delay = b.retry.Max
			}
		}
	}
}

func (b *Batcher) jitter(d time.Duration) time.Duration {
	if !b.retry.Jitter {
		return d
	}
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}
