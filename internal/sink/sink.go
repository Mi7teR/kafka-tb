package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
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

// Submitter — то, что умеет применять команду. В проде это *tbx.Batcher.
type Submitter interface {
	Submit(ctx context.Context, cmd *model.Command) ([]tbx.Outcome, error)
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

	pollSize        int
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
) *Sink {
	return &Sink{
		cl:              cl,
		oc:              cl,
		decoders:        decoders,
		sub:             sub,
		em:              em,
		offsets:         NewOffsets(),
		log:             log,
		pollSize:        cfg.Batcher.MaxBatchSize,
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

// processBatch обрабатывает пачку записей по порядку, повторяя каждую до
// успеха. Записи одной партиции обязаны применяться строго по порядку:
// применить N+1, пропустив упавшую N, значит опубликовать в results и DLQ
// исход, которого при реплее уже не будет.
func (s *Sink) processBatch(ctx context.Context, records []*kgo.Record) {
	for _, rec := range records {
		s.offsets.Track(rec)
	}
	deadline := time.Now().Add(s.batchBudget)
	for _, rec := range records {
		if !time.Now().Before(deadline) {
			if ctx.Err() == nil {
				// Бюджет пачки исчерпан ещё до этой записи: длинная серия
				// медленных, но успешных записей (без единой
				// инфраструктурной ошибки) держала бы AllowRebalance так
				// же долго, как и вечный ретрай, если бы бюджет
				// проверялся только внутри applyRecord.
				s.abandonBatch()
			}
			// При отмене контекста перематывать нечего: процесс уходит, а
			// незавершённые офсеты и так упираются в ватермарк и не
			// коммитятся.
			return
		}
		if s.applyRecord(ctx, rec, deadline) {
			continue
		}
		if ctx.Err() == nil {
			// Бюджет пачки исчерпан: дальше держать ребаланс нельзя.
			s.abandonBatch()
		}
		// При отмене контекста перематывать нечего: процесс уходит, а
		// незавершённые офсеты и так упираются в ватермарк и не коммитятся.
		return
	}
}

// applyRecord доводит одну запись до конца, повторяя её при инфраструктурной
// ошибке. Возвращает false, если запись пришлось бросить: контекст отменён
// или бюджет пачки не даёт ждать следующей попытки.
func (s *Sink) applyRecord(ctx context.Context, rec *kgo.Record, deadline time.Time) bool {
	for {
		done, err := s.handle(ctx, rec)
		if err == nil {
			if done {
				s.offsets.Done(rec)
				return true
			}
			// handle'а контракт: (false, nil) значить не должно ничего —
			// каждая ветка отдаёт либо (true, nil), либо (_, err).
			// Сегодня недостижимо, но молча продвинуться дальше здесь
			// значило бы навсегда пришпилить ватермарк этой партиции:
			// офсет остался бы в pending, а Commitable никогда не увидит
			// его завершённым. Считаем это инфраструктурным сбоем — запись
			// повторяется, а по истечении бюджета партиция перематывается
			// как при любой другой зависшей записи.
			s.log.Error("handle contract violation: done=false with no error, retrying",
				slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
				slog.Int64("offset", rec.Offset))
		} else if ctx.Err() != nil {
			// Контекст уже отменён: это штатное завершение, а не сбой —
			// backoff ниже вернёт false немедленно, ретрая не будет. Запись
			// остаётся некоммиченной и будет обработана заново после рестарта.
			s.log.Info("shutting down, leaving record uncommitted for reprocessing",
				slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
				slog.Int64("offset", rec.Offset), slog.String("error", err.Error()))
		} else {
			// Инфраструктура: та же запись повторяется, следующие ждут её.
			s.log.Error("record failed, retrying", slog.String("topic", rec.Topic),
				slog.Int("partition", int(rec.Partition)),
				slog.Int64("offset", rec.Offset), slog.String("error", err.Error()))
		}
		if !time.Now().Add(s.retryPeriod).Before(deadline) {
			return false
		}
		if !s.backoff(ctx) {
			return false
		}
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

// handle возвращает (true, nil), если запись обработана окончательно и её
// офсет можно коммитить. Ошибка означает инфраструктурную проблему:
// офсет остаётся на месте, запись будет обработана снова.
func (s *Sink) handle(ctx context.Context, rec *kgo.Record) (done bool, err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// Паника — дефект в обработке этого сообщения, а не всего потока.
		s.log.Error("panic handling record", slog.Any("panic", r),
			slog.String("topic", rec.Topic), slog.Int64("offset", rec.Offset))
		if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, "panic", fmt.Sprint(r)); e != nil {
			done, err = false, e
			return
		}
		done, err = true, nil
	}()

	dec, derr := s.decoders.For(rec.Topic)
	if derr != nil {
		if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, "unknown_topic", derr.Error()); e != nil {
			return false, e
		}
		return true, nil
	}

	cmd, derr := dec.Decode(rec.Value)
	if derr != nil {
		// Контракт codec.Decoder: любая ошибка декодинга — poison. Считать
		// её инфраструктурной значило бы повторять запись вечно.
		if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, "decode", derr.Error()); e != nil {
			return false, e
		}
		return true, nil
	}

	outcomes, serr := s.sub.Submit(ctx, cmd)
	if serr != nil {
		if errors.Is(serr, tbx.ErrCommandTooLarge) {
			if e := s.em.DLQ(ctx, rec, emit.ReasonPoison, "command_too_large", serr.Error()); e != nil {
				return false, e
			}
			return true, nil
		}
		return false, serr
	}

	if e := s.em.Results(ctx, rec, outcomes); e != nil {
		return false, e
	}
	for _, o := range outcomes {
		if o.Status != tbx.StatusRejected {
			continue
		}
		detail := fmt.Sprintf("event %d (id %s): %s", o.Index, o.ID, o.Error)
		if e := s.em.DLQ(ctx, rec, emit.ReasonReject, o.Error, detail); e != nil {
			return false, e
		}
	}
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
