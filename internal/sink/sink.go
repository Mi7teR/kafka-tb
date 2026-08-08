package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type (
	emitReason   = emit.Reason
	emitterIface = emit.Emitter
)

const (
	defaultCommitPeriod    = time.Second
	defaultRetryPeriod     = time.Second
	defaultShutdownTimeout = 10 * time.Second
	// defaultBatchBudget ограничивает время, на которое обработка одной
	// пачки держит блокировку ребаланса. Дефолтный rebalance timeout
	// franz-go — 60s; запас нужен, потому что бюджет проверяется только
	// между попытками, а сама попытка тоже занимает время.
	defaultBatchBudget = 30 * time.Second
)

// errHandleContract stands in for finish returning done=false with no error.
// Sending a record on without either an outcome or a failure would pin its
// partition's watermark forever, so it is treated as an infrastructure fault
// and retried like one.
var errHandleContract = errors.New("record finished with neither an outcome nor an error")

// Submitter — то, что умеет применять команду. В проде это *tbx.Batcher.
// Постановка не ждёт исход: ровно один SubmitResult приходит в возвращённый
// канал, и именно это позволяет держать в батчере больше одной команды сразу.
type Submitter interface {
	SubmitAsync(ctx context.Context, cmd *model.Command) (<-chan tbx.SubmitResult, error)
}

// offsetClient — та часть клиента Kafka, которой синк двигает офсеты.
// Выделена интерфейсом ради тестируемости processBatch/commit/OnRevoked:
// без неё эти пути невозможно прогнать без живого брокера.
// *kgo.Client реализует её как есть.
type offsetClient interface {
	CommitOffsetsSync(
		ctx context.Context,
		uncommitted map[string]map[int32]kgo.EpochOffset,
		onDone func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
	)
	SetOffsets(setOffsets map[string]map[int32]kgo.EpochOffset)
}

var _ offsetClient = (*kgo.Client)(nil)

type Sink struct {
	cl       *kgo.Client
	oc       offsetClient
	decoders codec.Registry
	sub      Submitter
	em       emitterIface
	offsets  *Offsets
	log      *slog.Logger
	metrics  *obs.Metrics

	pollSize        int
	maxInFlight     int
	commitPeriod    time.Duration
	retryPeriod     time.Duration
	batchBudget     time.Duration
	shutdownTimeout time.Duration

	// commitMu сериализует коммит. Run и OnRevoked — разные горутины, а
	// Commitable/MarkCommitted обязаны идти парой: между ними нельзя вклинить
	// второй Commitable, иначе тот же офсет уедет в брокер дважды, а
	// MarkCommitted второго вызова затрёт ватермарк первого.
	commitMu sync.Mutex
}

func New(
	cfg *config.Config,
	cl *kgo.Client,
	decoders codec.Registry,
	sub Submitter,
	em emitterIface,
	log *slog.Logger,
	metrics *obs.Metrics,
) *Sink {
	// A config built in code rather than loaded from YAML (integration
	// harnesses) never passes through config.Load's defaulting, and a zero
	// bound would submit nothing at all.
	maxInFlight := cfg.Sink.MaxInFlightPerPartition
	if maxInFlight <= 0 {
		maxInFlight = config.DefaultMaxInFlightPerPartition
	}
	return &Sink{
		cl:              cl,
		oc:              cl,
		decoders:        decoders,
		sub:             sub,
		em:              em,
		offsets:         NewOffsets(),
		log:             log,
		metrics:         metrics,
		pollSize:        cfg.Batcher.MaxBatchSize,
		maxInFlight:     maxInFlight,
		commitPeriod:    defaultCommitPeriod,
		retryPeriod:     defaultRetryPeriod,
		batchBudget:     defaultBatchBudget,
		shutdownTimeout: cfg.ShutdownTimeout,
	}
}

// newForTest собирает синк без polling-клиента: цикл Run проверяется
// интеграционно, а всё, что двигает офсеты, ходит через offsetClient.
func newForTest(
	decoders codec.Registry, sub Submitter, em emitterIface, oc offsetClient, log *slog.Logger,
) (*Sink, error) {
	return &Sink{
		oc:              oc,
		decoders:        decoders,
		sub:             sub,
		em:              em,
		offsets:         NewOffsets(),
		log:             log,
		maxInFlight:     config.DefaultMaxInFlightPerPartition,
		commitPeriod:    defaultCommitPeriod,
		retryPeriod:     defaultRetryPeriod,
		batchBudget:     defaultBatchBudget,
		shutdownTimeout: defaultShutdownTimeout,
	}, nil
}

// Run крутит цикл до отмены контекста.
func (s *Sink) Run(ctx context.Context) {
	commitTicker := time.NewTicker(s.commitPeriod)
	defer commitTicker.Stop()

	for {
		// Опрос ограничен по времени: с бесконечным Poll на тихом топике
		// периодический коммит не наступал бы до следующей пачки, и
		// последняя обработанная запись висела бы незакоммиченной.
		// Коммитить из отдельной горутины нельзя: SetOffsets в abandon
		// нельзя звать одновременно с коммитом (см. go doc SetOffsets).
		pollCtx, cancel := context.WithTimeout(ctx, s.commitPeriod)
		fetches := s.cl.PollRecords(pollCtx, s.pollSize)
		cancel()
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			// Отмена — это наш собственный дедлайн опроса или штатное
			// завершение, а не сбой fetch'а.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			s.log.Error("fetch error", slog.String("topic", t),
				slog.Int("partition", int(p)), slog.String("error", err.Error()))
		})

		// AllowRebalance снимается всегда: клиент собран с
		// BlockRebalanceOnPoll, и пропущенный вызов — хоть на успешном
		// пути, хоть на панике — навсегда подвешивает группу.
		func() {
			defer s.cl.AllowRebalance()
			s.processBatch(ctx, fetches.Records())
		}()

		if ctx.Err() != nil {
			// Финальный коммит уже отменённым контекстом не пройдёт, но и
			// висеть на недоступном брокере до бесконечности не должен.
			shutCtx, cancelShut := context.WithTimeout(
				context.WithoutCancel(ctx), s.shutdownTimeout)
			s.commit(shutCtx, slog.LevelError)
			cancelShut()
			return
		}
		select {
		case <-commitTicker.C:
			s.commit(ctx, slog.LevelError)
		default:
		}
	}
}

// processBatch обрабатывает пачку записей опроса. Записи группируются по
// (topic, partition), и каждая группа едет своей горутиной: порядок осмыслен
// только внутри партиции, между партициями его никогда не гарантировали.
//
// Все горутины джойнятся до возврата. Иначе нельзя: вызывающий сразу после
// возврата снимает блокировку ребаланса (AllowRebalance), а abandonBatch ниже
// двигает офсеты через SetOffsets — и то и другое безопасно только пока
// блокировка ещё держится и только с этой горутины.
func (s *Sink) processBatch(ctx context.Context, records []*kgo.Record) {
	for _, rec := range records {
		s.offsets.Track(rec)
	}
	deadline := time.Now().Add(s.batchBudget)

	var (
		wg        sync.WaitGroup
		abandoned atomic.Bool
	)
	for _, group := range groupByPartition(records) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Паника отдельной горутины проходит мимо любого defer вызывающего
			// — в том числе мимо AllowRebalance, который обещан «хоть на
			// панике», — и убивает процесс целиком. Ловится она поэтому здесь.
			// prepare и finish ловят свои паники сами; сюда доходит то, что
			// мимо них: await, offsets.Done и повторная паника в отложенной
			// публикации самого finish. Партиция считается брошенной: её
			// записи остаются непомеченными и потому незакоммиченными, а
			// вызывающий перемотает её назад.
			defer func() {
				r := recover()
				if r == nil {
					return
				}
				s.log.Error("panic processing partition", slog.Any("panic", r),
					slog.String("topic", group[0].Topic),
					slog.Int("partition", int(group[0].Partition)))
				abandoned.Store(true)
			}()
			if !s.runPartition(ctx, group, deadline) {
				abandoned.Store(true)
			}
		}()
	}
	wg.Wait()

	// При отмене контекста перематывать нечего: процесс уходит, а
	// незавершённые офсеты и так упираются в ватермарк и не коммитятся.
	if abandoned.Load() && ctx.Err() == nil {
		// Бюджет пачки исчерпан или запись стабильно падает: дальше держать
		// ребаланс нельзя. Перематываются ровно те партиции, у которых
		// остались непроверенные записи, — разобранная до конца партиция в
		// Pending не попадает.
		s.abandonBatch()
	}
}

// groupByPartition режет опрос на пробеги по партициям, сохраняя порядок
// поступления записей: внутри партиции это порядок офсетов, а именно на нём
// держится порядок постановки в батчер.
func groupByPartition(records []*kgo.Record) [][]*kgo.Record {
	var groups [][]*kgo.Record
	index := make(map[partitionKey]int)
	for _, rec := range records {
		k := partitionKey{rec.Topic, rec.Partition}
		i, ok := index[k]
		if !ok {
			i = len(groups)
			index[k] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], rec)
	}
	return groups
}

// runPartition доводит записи одной партиции до конца, повторяя пробег с той
// записи, на которой он сорвался. Возвращает false, если партицию пришлось
// бросить — бюджет исчерпан или контекст отменён; её непроверенные записи
// остаются непомеченными, и вызывающий перематывает партицию.
//
// Ретрай начинается именно с упавшей записи, а не с середины: следующие за ней
// уже поставленные команды тоже перепоставляются, поэтому порядок применения
// внутри партиции остаётся порядком офсетов и на повторном пробеге. Повторное
// применение уже применённой команды безвредно — id стабильны между попытками,
// а TransferExists/AccountExists трактуются как StatusOK.
func (s *Sink) runPartition(ctx context.Context, recs []*kgo.Record, deadline time.Time) bool {
	for len(recs) > 0 {
		if ctx.Err() != nil {
			return s.abandonOnShutdown(recs[0])
		}
		if !time.Now().Before(deadline) {
			return false
		}
		applied, err := s.pass(ctx, recs, deadline)
		recs = recs[applied:]
		if err == nil {
			if applied > 0 {
				continue
			}
			// Ни ошибки, ни прогресса: pass оборвали бюджет или отмена.
			// Отмену объяснит проверка в начале следующего витка.
			if ctx.Err() == nil {
				return false
			}
			continue
		}
		if ctx.Err() != nil {
			return s.abandonOnShutdown(recs[0])
		}
		// Инфраструктура: та же запись повторяется, следующие ждут её.
		s.log.Error("record failed, retrying", slog.String("topic", recs[0].Topic),
			slog.Int("partition", int(recs[0].Partition)),
			slog.Int64("offset", recs[0].Offset), slog.String("error", err.Error()))
		if !time.Now().Add(s.retryPeriod).Before(deadline) {
			return false
		}
		if !s.backoff(ctx) {
			return false
		}
	}
	return true
}

// abandonOnShutdown объясняет, почему запись остаётся незакоммиченной, и всегда
// возвращает false. Отмена контекста — штатное завершение, а не сбой: ретрая
// всё равно не будет, и ERROR здесь поднял бы дежурного на ровном месте.
func (s *Sink) abandonOnShutdown(rec *kgo.Record) bool {
	s.log.Info("shutting down, leaving record uncommitted for reprocessing",
		slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
		slog.Int64("offset", rec.Offset))
	return false
}

// pass ставит в батчер префикс recs, ни на один исход не дожидаясь, и только
// потом собирает исходы в том же порядке, публикуя и помечая каждый по мере
// прихода. Порядок постановки — это порядок применения в TigerBeetle, порядок
// сбора — порядок публикации в results и DLQ; оба совпадают с порядком офсетов
// партиции. Poison сюда тоже попадает: в батчер он не идёт, но его DLQ
// откладывается до фазы сбора, иначе публикация партиции перестала бы быть
// последовательной.
//
// Возвращает число ведущих записей, доведённых до окончательного исхода, и
// инфраструктурную ошибку, которая это остановила: ошибка принадлежит
// recs[applied]. Короткий возврат без ошибки означает, что кончился бюджет
// пачки или процесс уходит — остальные записи остаются непомеченными.
func (s *Sink) pass(ctx context.Context, recs []*kgo.Record, deadline time.Time) (int, error) {
	if len(recs) > s.maxInFlight {
		recs = recs[:s.maxInFlight]
	}
	// Постановка ограничена бюджетом пачки, а не только отменой: SubmitAsync
	// паркуется на очереди батчера, а очередь эта общая и заведомо переполняется
	// — партиций много, у каждой свой maxInFlight. Пока TigerBeetle отвечает,
	// очередь разбирается; как только он замолчал, батчер ретраит вечно, очередь
	// не двигается, и постановка без дедлайна держала бы блокировку ребаланса
	// сколь угодно долго. Исход уже поставленной команды от этого контекста не
	// зависит — его ждёт await по общему бюджету.
	enqueueCtx, cancelEnqueue := context.WithDeadline(ctx, deadline)
	defer cancelEnqueue()
	prep := make([]prepared, 0, len(recs))
	for _, rec := range recs {
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			break
		}
		p := s.prepare(enqueueCtx, rec)
		if p.err != nil && enqueueCtx.Err() != nil && ctx.Err() == nil {
			// Постановку оборвал наш собственный дедлайн: это исчерпанный
			// бюджет пачки, а не сбой записи. Ошибку не поднимаем — ретраить
			// её незачем, вызывающий бросит партицию и перемотает её.
			break
		}
		prep = append(prep, p)
		if p.err != nil {
			// Постановка сорвалась. Следующие записи партиции ставить нельзя:
			// они применились бы в TigerBeetle раньше упавшей, а упавшая — на
			// повторе, то есть после них. Порядок применения внутри партиции
			// обязан быть порядком офсетов, поэтому пробег останавливается
			// здесь, а уже поставленный префикс собирается как обычно.
			break
		}
	}
	for i, p := range prep {
		var res tbx.SubmitResult
		if p.ch != nil {
			var ok bool
			if res, ok = s.await(ctx, p.ch, deadline); !ok {
				return i, nil
			}
		}
		done, err := s.finish(ctx, recs[i], p, res)
		if err == nil && !done {
			// Контракт finish: (false, nil) значить не должно ничего — каждая
			// ветка отдаёт либо (true, nil), либо (_, err). Сегодня
			// недостижимо, но молча продвинуться дальше здесь значило бы
			// навсегда пришпилить ватермарк этой партиции: офсет остался бы в
			// pending, а Commitable никогда не увидит его завершённым.
			s.log.Error("handle contract violation: done=false with no error, retrying",
				slog.String("topic", recs[i].Topic), slog.Int("partition", int(recs[i].Partition)),
				slog.Int64("offset", recs[i].Offset))
			err = errHandleContract
		}
		if err != nil {
			return i, err
		}
		s.offsets.Done(recs[i])
	}
	return len(prep), nil
}

// await ждёт исход одной команды. Возвращает false, если ждать перестали —
// бюджет пачки кончился или процесс уходит; запись остаётся непроверенной и
// потому незакоммиченной. Уже пришедший исход имеет приоритет над обоими
// поводами уйти: бросить его значило бы соврать про применённую работу там,
// где правда уже на руках.
func (s *Sink) await(ctx context.Context, ch <-chan tbx.SubmitResult, deadline time.Time) (tbx.SubmitResult, bool) {
	select {
	case res := <-ch:
		return res, true
	default:
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case res := <-ch:
		return res, true
	case <-ctx.Done():
		return tbx.SubmitResult{}, false
	case <-timer.C:
		return tbx.SubmitResult{}, false
	}
}

// abandonBatch бросает недоработанную пачку и перематывает партиции назад.
// Уже вычитанные записи сами по себе больше не придут, поэтому без
// перемотки брошенный офсет остался бы в pending навсегда: партиция не
// коммитилась бы до конца жизни процесса, а память под done росла бы.
// Forget ставит тумбстон, следующий Track ту же партицию оживит.
func (s *Sink) abandonBatch() {
	pending := s.offsets.Pending()
	if len(pending) == 0 {
		return
	}
	// SetOffsets безопасен именно здесь: ребаланс ещё заблокирован Poll'ом,
	// а коммит идёт из этой же горутины и сейчас не выполняется.
	s.oc.SetOffsets(pending)
	for topic, parts := range pending {
		for partition, eo := range parts {
			s.offsets.Forget(topic, partition)
			s.log.Warn("batch budget exceeded, partition rewound",
				slog.String("topic", topic), slog.Int("partition", int(partition)),
				slog.Int64("offset", eo.Offset),
				slog.Duration("budget", s.batchBudget))
		}
	}
}

// backoff возвращает false, если ждать больше незачем — контекст отменён.
func (s *Sink) backoff(ctx context.Context) bool {
	t := time.NewTimer(s.retryPeriod)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// prepared — состояние одной поставленной записи: либо канал исхода, либо уже
// принятое решение, до батчера. Публикация не делается здесь ни в одном
// случае: её порядок обязан совпадать с порядком офсетов, а фаза постановки
// намеренно не ждёт исходы и потому не может ничего публиковать по порядку.
type prepared struct {
	// ch — канал ровно одного исхода; nil, если запись в батчер не пошла.
	ch <-chan tbx.SubmitResult
	// poison называет ошибку для DLQ записи, которая до батчера не дошла и
	// никогда не дойдёт. Пусто, если запись поставлена.
	poison string
	detail string
	// err — инфраструктурный сбой самой постановки.
	err error
}

// prepare декодирует запись и ставит её команду в батчер, не дожидаясь исхода.
// Паника — дефект в обработке этого сообщения, а не всего потока.
func (s *Sink) prepare(ctx context.Context, rec *kgo.Record) (p prepared) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		s.log.Error("panic handling record", slog.Any("panic", r),
			slog.String("topic", rec.Topic), slog.Int64("offset", rec.Offset))
		p = prepared{poison: "panic", detail: fmt.Sprint(r)}
	}()

	dec, derr := s.decoders.For(rec.Topic)
	if derr != nil {
		return prepared{poison: "unknown_topic", detail: derr.Error()}
	}

	cmd, derr := dec.Decode(rec.Value)
	if derr != nil {
		// Контракт codec.Decoder: любая ошибка декодинга — poison. Считать
		// её инфраструктурной значило бы повторять запись вечно.
		return prepared{poison: "decode", detail: derr.Error()}
	}

	ch, serr := s.sub.SubmitAsync(ctx, cmd)
	if serr != nil {
		if errors.Is(serr, tbx.ErrCommandTooLarge) {
			return prepared{poison: "command_too_large", detail: serr.Error()}
		}
		return prepared{err: serr}
	}
	return prepared{ch: ch}
}

// finish публикует исход записи и возвращает (true, nil), если её офсет можно
// коммитить. Ошибка означает инфраструктурную проблему: офсет остаётся на
// месте, запись будет обработана снова.
func (s *Sink) finish(
	ctx context.Context, rec *kgo.Record, p prepared, res tbx.SubmitResult,
) (done bool, err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// Паника — дефект в обработке этого сообщения, а не всего потока.
		s.log.Error("panic handling record", slog.Any("panic", r),
			slog.String("topic", rec.Topic), slog.Int64("offset", rec.Offset))
		done, err = s.emitPoison(ctx, rec, "panic", fmt.Sprint(r))
	}()

	if p.poison != "" {
		return s.emitPoison(ctx, rec, p.poison, p.detail)
	}
	if p.err != nil {
		s.metrics.IncRecords("blocked")
		return false, p.err
	}
	if res.Err != nil {
		// Настоящий батчер отказывает слишком большой команде прямо на
		// постановке, но Submitter вправе сообщить это и исходом; трактуем
		// одинаково, откуда бы ни пришло.
		if errors.Is(res.Err, tbx.ErrCommandTooLarge) {
			return s.emitPoison(ctx, rec, "command_too_large", res.Err.Error())
		}
		s.metrics.IncRecords("blocked")
		return false, res.Err
	}

	// RecordsTotal is counted here, once the record's handling is actually
	// final, not right after the outcome arrives: Results and the reject-DLQ
	// loop below can still fail and send this same record back through
	// runPartition for a retry. Counting ok/rejected before they succeed would
	// double-count on that retry and never show the intervening failure as
	// blocked.
	if e := s.em.Results(ctx, rec, res.Outcomes); e != nil {
		s.metrics.IncRecords("blocked")
		return false, e
	}
	for _, o := range res.Outcomes {
		if o.Status != tbx.StatusRejected {
			continue
		}
		detail := fmt.Sprintf("event %d (id %s): %s", o.Index, o.ID, o.Error)
		if e := s.em.DLQ(ctx, rec, emit.ReasonReject, o.Error, detail); e != nil {
			s.metrics.IncRecords("blocked")
			return false, e
		}
		s.metrics.IncDLQ(string(emit.ReasonReject), o.Error)
	}
	for _, o := range res.Outcomes {
		s.metrics.IncRecords(string(o.Status))
	}
	return true, nil
}

// emitPoison публикует запись, которая никогда не будет применена, и говорит,
// можно ли двигать её офсет: (true, nil) — только после подтверждённой записи
// в DLQ, иначе (false, err).
func (s *Sink) emitPoison(ctx context.Context, rec *kgo.Record, errName, detail string) (bool, error) {
	if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, errName, detail); e != nil {
		s.metrics.IncRecords("blocked")
		return false, e
	}
	s.metrics.IncRecords("poison")
	s.metrics.IncDLQ(string(emit.ReasonPoison), errName)
	return true, nil
}

// commit отдаёт брокеру непрерывный префикс обработанных офсетов.
// Flush до коммита обязателен: закоммитить офсет записи, чей DLQ или results
// ещё лежат в буфере продюсера, значит потерять её при падении процесса.
//
// level задаёт уровень лога провала коммита и передаётся вызывающей
// стороной, а не выводится внутри: у OnRevoked провал коммита штатный —
// это гонка revoke с закрытием клиента (контекст уже отменён), тот же офсет
// уедет при следующей отдаче партиции. У периодического и shutdown-коммита
// в Run провал означает, что брокер стабильно отклоняет коммиты, и это
// обязано быть видно алертингу.
func (s *Sink) commit(ctx context.Context, level slog.Level) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	// Reported regardless of what follows (even "nothing commitable" or a
	// failed commit below): an operator alerting on this gauge needs it to
	// reflect the current gap, not go silent whenever there is nothing new
	// to commit.
	defer s.reportCommitLag()

	offsets := s.offsets.Commitable()
	if len(offsets) == 0 {
		return
	}
	if err := s.em.Flush(ctx); err != nil {
		s.log.Error("flush before commit failed", slog.String("error", err.Error()))
		return
	}
	var failed bool
	s.oc.CommitOffsetsSync(ctx, offsets,
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				failed = true
				s.log.Log(ctx, level, "commit failed", slog.String("error", err.Error()))
				return
			}
			// Ошибка уровня партиции не поднимается в err: без этой проверки
			// незакоммиченный офсет был бы помечен как закоммиченный.
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					perr := kerr.ErrorForCode(p.ErrorCode)
					if perr == nil {
						continue
					}
					failed = true
					s.log.Log(ctx, level, "commit failed", slog.String("topic", t.Topic),
						slog.Int("partition", int(p.Partition)),
						slog.String("error", perr.Error()))
				}
			}
		})
	if failed {
		// Ватермарк не двигаем целиком: повторный коммит того же офсета
		// безвреден, а вот преждевременный MarkCommitted — нет.
		return
	}
	s.offsets.MarkCommitted(offsets)
}

// reportCommitLag publishes kafkatb_offset_commit_lag from the offsets
// tracker's current state, per topic/partition.
func (s *Sink) reportCommitLag() {
	for topic, parts := range s.offsets.CommitLag() {
		for partition, lag := range parts {
			s.metrics.SetCommitLag(topic, partition, lag)
		}
	}
}

// OnRevoked коммитит перед отдачей партиций: после Forget состояние партиции —
// тумбстон, и коммитить будет уже нечего.
func (s *Sink) OnRevoked(ctx context.Context, revoked map[string][]int32) {
	s.commit(ctx, slog.LevelWarn)
	for topic, parts := range revoked {
		for _, p := range parts {
			s.offsets.Forget(topic, p)
		}
	}
}

// NewKafkaClient собирает консьюмера с ручным коммитом и блокировкой
// ребаланса на время обработки: иначе можно закоммитить чужие партиции.
func NewKafkaClient(cfg *config.Config, onRevoked func(context.Context, map[string][]int32)) (*kgo.Client, error) {
	topics := make([]string, 0, len(cfg.Kafka.Topics))
	for _, t := range cfg.Kafka.Topics {
		topics = append(topics, t.Name)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ConsumerGroup(cfg.Kafka.Group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			onRevoked(ctx, revoked)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return cl, nil
}
