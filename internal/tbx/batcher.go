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
	done chan submitResult
}

type submitResult struct {
	outcomes []Outcome
	err      error
}

// Batcher — единственная дверь в TigerBeetle.
// Держит по одному in-flight батчу на каждый тип операции, чем гарантирует,
// что порядок применения совпадает с порядком Submit.
type Batcher struct {
	client  Client
	cfg     config.Batcher
	retry   config.Retry
	log     *slog.Logger
	metrics *obs.Metrics

	transfers chan *job
	accounts  chan *job

	// stop — единственный сигнал остановки, общий для всех участников.
	// Закрывается ровно один раз: либо из Close(), либо при отмене контекста
	// Start. Оба пути обязаны сходиться сюда — иначе отмена контекста гасит
	// циклы, а отправители продолжают считать батчер живым и виснут навсегда.
	// Протокол намеренно бесlock'овый: закрытый канал одинаково виден всем,
	// поэтому у отправителя всегда есть выход из блокирующего select.
	stopOnce sync.Once
	stop     chan struct{}

	// finished — «циклы вышли», а не «остановку запросили». Разница
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
		client:    c,
		cfg:       cfg,
		retry:     retry,
		log:       log,
		metrics:   metrics,
		transfers: make(chan *job, cfg.MaxQueue),
		accounts:  make(chan *job, cfg.MaxQueue),
		stop:      make(chan struct{}),
		finished:  make(chan struct{}),
	}
}

// Start запускает циклы отправки. Вызывается ровно один раз и строго до Close;
// повторный или конкурентный с Close вызов не поддерживается.
func (b *Batcher) Start(ctx context.Context) {
	// Отмена контекста — это тот же shutdown, что и Close().
	b.unwatch = context.AfterFunc(ctx, b.signalStop)
	b.wg.Add(2)
	go func() { defer b.wg.Done(); b.loop(b.transfers, b.sendTransfers) }()
	go func() { defer b.wg.Done(); b.loop(b.accounts, b.sendAccounts) }()
	// Наблюдатель живёт здесь, а не в Close: путь остановки по отмене контекста
	// не проходит через Close, но отправителей отпускать обязан так же.
	go func() { b.wg.Wait(); b.signalFinished() }()
}

// Submit ставит команду в очередь и ждёт исход.
// Блокировка при полной очереди — это backpressure для консьюмера.
func (b *Batcher) Submit(ctx context.Context, cmd *model.Command) ([]Outcome, error) {
	if cmd.Len() == 0 {
		return nil, errors.New("empty command")
	}
	if cmd.Len() > b.cfg.MaxBatchSize {
		return nil, ErrCommandTooLarge
	}
	j := &job{cmd: cmd, done: make(chan submitResult, 1)}
	queue := b.transfers
	if cmd.Op == model.OpCreateAccounts {
		queue = b.accounts
	}

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
	case queue <- j:
	}

	select {
	case res := <-j.done:
		return res.outcomes, res.err
	case <-ctx.Done():
		// Команда уже в очереди и может дойти до TigerBeetle после этого
		// возврата: отменить её отсюда нельзя. Вызывающий (Kafka-синк) увидит
		// ошибку и, скорее всего, повторит команду. Корректность здесь
		// целиком держится на идемпотентности по id: повтор даёт
		// TransferExists/AccountExists, а MapTransferResults/MapAccountResults
		// трактуют их как StatusOK. Это работает только потому, что id
		// приходят от вызывающего и стабильны между попытками.
		return nil, ctx.Err()
	case <-b.finished:
		// Здесь ждём именно finished, а не stop. stop означает лишь «начали
		// останавливаться»: батч этой команды может быть уже в TigerBeetle и
		// вот-вот вернуть исход. Ответить в этот момент ErrClosed — соврать
		// про применённую работу; для синхронного API это неисправимо, его
		// клиент не обязан повторять запрос с тем же id.
		// finished же закрывается после выхода обоих циклов, то есть когда
		// ответить этой команде уже некому.
		//
		// Гонка «исход доставлен ровно в момент выхода циклов» реальна:
		// оба канала готовы, и select выбрал бы случайно. Приоритет исхода
		// восстанавливаем явной непустой проверкой.
		select {
		case res := <-j.done:
			return res.outcomes, res.err
		default:
		}
		// Команда попала в очередь так поздно, что ни один цикл её не увидел.
		// Отвечаем ошибкой, а не виснем; дальше — тот же расчёт на
		// идемпотентность по id, что и в ветке ctx.Done() выше.
		return nil, ErrClosed
	}
}

// Close закрывает приём и дожидается, пока циклы разгребут очереди.
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

// loop собирает батч по правилу «max_batch_size или linger, что раньше».
func (b *Batcher) loop(queue chan *job, send func([]*job) error) {
	var (
		batch []*job
		size  int
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
		if err := send(batch); err != nil {
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
			b.drain(queue, ErrClosed)
			return
		case j := <-queue:
			// Команда не влезает в остаток — отправляем накопленное и начинаем новый батч.
			if size+j.cmd.Len() > b.cfg.MaxBatchSize {
				flush()
			}
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
			j.done <- submitResult{err: err}
		default:
			return
		}
	}
}

func (b *Batcher) failAll(jobs []*job, err error) {
	for _, j := range jobs {
		j.done <- submitResult{err: err}
	}
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
		j.done <- submitResult{outcomes: outcomes, err: mapErr}
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
		j.done <- submitResult{outcomes: outcomes, err: mapErr}
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
