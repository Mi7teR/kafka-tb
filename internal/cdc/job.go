package cdc

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	types "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
)

// Source is the whole of TigerBeetle this job uses. The Go client marks
// GetChangeEvents experimental and undocumented, so it is reached through
// this one interface: the day its signature changes, this declaration and the
// adapter in cmd are what has to move, not the job.
type Source interface {
	GetChangeEvents(filter types.ChangeEventsFilter) ([]types.ChangeEvent, error)
}

// Publisher hands records to Kafka and returns only once the broker has
// answered for every one of them. *kgo.Client implements it as-is; the
// narrow shape is what lets the job's tests drive publication failures.
type Publisher interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// Job streams TigerBeetle change events to Kafka.
//
// Delivery is at-least-once: a window is published and acknowledged before
// the cursor moves past it, so a crash replays the window rather than
// skipping it. Consumers deduplicate on the event timestamp, which
// TigerBeetle guarantees unique and monotonic.
type Job struct {
	cfg   config.CDC
	retry config.Retry
	src   Source
	pub   Publisher
	enc   *Encoder
	log   *slog.Logger
}

func New(
	cfg config.CDC, retry config.Retry, src Source, pub Publisher, reg *model.Registry, log *slog.Logger,
) *Job {
	// A config assembled in code rather than loaded from YAML (test
	// harnesses) never passes through config.Load's defaulting, and a zero
	// window would ask TigerBeetle for nothing at all, forever.
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = config.DefaultCDCBatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = config.DefaultCDCPollInterval
	}
	if retry.Initial <= 0 {
		retry.Initial = 100 * time.Millisecond
	}
	if retry.Max < retry.Initial {
		retry.Max = retry.Initial
	}
	return &Job{cfg: cfg, retry: retry, src: src, pub: pub, enc: NewEncoder(cfg, reg, log), log: log}
}

// Run publishes events after checkpoint until ctx is cancelled, at which
// point it returns nil: a cancelled context is a shutdown, not a failure.
//
// One turn of the loop is: ask for a window of at most cdc.batch_size events
// after the cursor, publish it, wait for every acknowledgement, then move the
// cursor. Nothing else moves the cursor — a failed query or a failed
// publication is retried from the same place with a backoff, and an empty
// answer waits cdc.poll_interval. That is what makes losing an event
// impossible and duplicating one merely possible.
func (j *Job) Run(ctx context.Context, checkpoint uint64) error {
	j.log.Info("cdc: starting", slog.String("topic", j.cfg.Topic),
		slog.Uint64("checkpoint", checkpoint), slog.Int("batch_size", j.cfg.BatchSize),
		slog.String("partition_key", j.cfg.PartitionKey))

	delay := j.retry.Initial
	for {
		if ctx.Err() != nil {
			return nil
		}
		events, err := j.src.GetChangeEvents(types.ChangeEventsFilter{
			TimestampMin: checkpoint + 1,
			Limit:        uint32(j.cfg.BatchSize),
		})
		if err != nil {
			j.log.Warn("cdc: change events query failed, retrying",
				slog.Uint64("checkpoint", checkpoint),
				slog.String("error", err.Error()), slog.Duration("in", delay))
			if !sleep(ctx, j.jitter(delay)) {
				return nil
			}
			delay = j.next(delay)
			continue
		}
		if len(events) == 0 {
			if !sleep(ctx, j.cfg.PollInterval) {
				return nil
			}
			delay = j.retry.Initial
			continue
		}
		published, err := j.publish(ctx, events, checkpoint)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			j.log.Warn("cdc: publication failed, republishing the whole window",
				slog.Uint64("checkpoint", checkpoint), slog.Int("events", len(events)),
				slog.String("error", err.Error()), slog.Duration("in", delay))
			if !sleep(ctx, j.jitter(delay)) {
				return nil
			}
			delay = j.next(delay)
			continue
		}
		checkpoint = published
		delay = j.retry.Initial
	}
}

// publish writes one window and returns the checkpoint it establishes: the
// highest timestamp in the window, now provably present in the topic.
//
// The window goes out in two steps. First every record but the one carrying
// the highest timestamp, all claiming the checkpoint the window started from.
// Only once those are acknowledged does the closing record go out, and it
// claims its own timestamp — "everything up to here is in this topic".
//
// The second step is what buys gap-freedom on a topic with many partitions.
// A crash mid-window leaves each partition holding some prefix of what was
// destined for it, and those prefixes end at different timestamps; the
// highest event timestamp then found in the topic is not a point the stream
// is complete up to. The closing record cannot land under that condition —
// it is produced only after everything else in its window is acknowledged —
// so a claim of completeness is never written unless it is true.
func (j *Job) publish(ctx context.Context, events []types.ChangeEvent, checkpoint uint64) (uint64, error) {
	closing := highest(events)
	rest := make([]*kgo.Record, 0, len(events)-1)
	for i, ev := range events {
		if i == closing {
			continue
		}
		rec, err := j.enc.Record(ev, checkpoint)
		if err != nil {
			return 0, err
		}
		rest = append(rest, rec)
	}
	if len(rest) > 0 {
		if err := j.pub.ProduceSync(ctx, rest...).FirstErr(); err != nil {
			return 0, fmt.Errorf("publish window: %w", err)
		}
	}
	last := events[closing]
	rec, err := j.enc.Record(last, last.Timestamp)
	if err != nil {
		return 0, err
	}
	if err := j.pub.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return 0, fmt.Errorf("publish closing record: %w", err)
	}
	return last.Timestamp, nil
}

// highest indexes the event with the largest timestamp. TigerBeetle answers
// in timestamp order, so this is the last element — but the cursor and the
// closing record both hinge on it, and the API it comes from is experimental,
// so it is established rather than assumed.
func highest(events []types.ChangeEvent) int {
	best := 0
	for i, ev := range events {
		if ev.Timestamp > events[best].Timestamp {
			best = i
		}
	}
	return best
}

// next is the backoff schedule: double up to retry.max. Retries never give
// up — a CDC job that exits because TigerBeetle or Kafka blinked would stop
// the stream until someone notices.
func (j *Job) next(d time.Duration) time.Duration {
	d *= 2
	if d > j.retry.Max {
		return j.retry.Max
	}
	return d
}

func (j *Job) jitter(d time.Duration) time.Duration {
	if !j.retry.Jitter {
		return d
	}
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

// sleep waits d, or returns false as soon as ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
