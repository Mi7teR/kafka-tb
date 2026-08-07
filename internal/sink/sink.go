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
	defaultCommitPeriod = time.Second
	defaultRetryPeriod  = time.Second
)

// Submitter — то, что умеет применять команду. В проде это *tbx.Batcher.
type Submitter interface {
	Submit(ctx context.Context, cmd *model.Command) ([]tbx.Outcome, error)
}

type Sink struct {
	cl       *kgo.Client
	decoders codec.Registry
	sub      Submitter
	em       emitterIface
	offsets  *Offsets
	log      *slog.Logger

	pollSize     int
	commitPeriod time.Duration
	retryPeriod  time.Duration

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
		cl:           cl,
		decoders:     decoders,
		sub:          sub,
		em:           em,
		offsets:      NewOffsets(),
		log:          log,
		pollSize:     cfg.Batcher.MaxBatchSize,
		commitPeriod: defaultCommitPeriod,
		retryPeriod:  defaultRetryPeriod,
	}
}

// newForTest собирает синк без клиента Kafka: тесты покрывают классификацию
// записи, а не цикл polling'а (он проверяется интеграционно).
func newForTest(decoders codec.Registry, sub Submitter, em emitterIface, log *slog.Logger) (*Sink, error) {
	return &Sink{
		decoders:     decoders,
		sub:          sub,
		em:           em,
		offsets:      NewOffsets(),
		log:          log,
		commitPeriod: defaultCommitPeriod,
		retryPeriod:  defaultRetryPeriod,
	}, nil
}

// Run крутит цикл до отмены контекста.
func (s *Sink) Run(ctx context.Context) error {
	commitTicker := time.NewTicker(s.commitPeriod)
	defer commitTicker.Stop()

	for {
		// PollRecords сам возвращается при отмене контекста, поэтому
		// отдельного select с ctx.Done() перед ним не нужно: он бы всё равно
		// не прервал блокировку внутри Poll.
		fetches := s.cl.PollRecords(ctx, s.pollSize)
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			s.log.Error("fetch error", slog.String("topic", t),
				slog.Int("partition", int(p)), slog.String("error", err.Error()))
		})

		s.processBatch(ctx, fetches)

		if ctx.Err() != nil {
			// Финальный коммит уже отменённым контекстом не пройдёт.
			s.commit(context.WithoutCancel(ctx))
			return nil
		}
		select {
		case <-commitTicker.C:
			s.commit(ctx)
		default:
		}
	}
}

// processBatch обрабатывает одну пачку записей и всегда снимает блокировку
// ребаланса: клиент собран с BlockRebalanceOnPoll, и пропущенный
// AllowRebalance — хоть на успешном пути, хоть на ошибочном — навсегда
// подвешивает группу.
func (s *Sink) processBatch(ctx context.Context, fetches kgo.Fetches) {
	defer s.cl.AllowRebalance()

	records := fetches.Records()
	for _, rec := range records {
		s.offsets.Track(rec)
	}
	// Записи одной партиции обрабатываются строго по порядку,
	// поэтому идём последовательно.
	for _, rec := range records {
		done, err := s.handle(ctx, rec)
		switch {
		case err != nil:
			// Инфраструктура: запись остаётся в pending, ватермарк партиции
			// упирается в неё, и ни она, ни всё, что за ней, не коммитится.
			// Она будет перечитана после ребаланса или рестарта.
			s.log.Error("record blocked", slog.String("topic", rec.Topic),
				slog.Int("partition", int(rec.Partition)),
				slog.Int64("offset", rec.Offset), slog.String("error", err.Error()))
			if !s.backoff(ctx) {
				return
			}
		case done:
			s.offsets.Done(rec)
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
		if !codec.IsPoison(derr) {
			return false, derr
		}
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
func (s *Sink) commit(ctx context.Context) {
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
	s.cl.CommitOffsetsSync(ctx, offsets,
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				failed = true
				s.log.Error("commit failed", slog.String("error", err.Error()))
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
					s.log.Error("commit failed", slog.String("topic", t.Topic),
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
	s.commit(ctx)
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
