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
//
// The job requires the answer to be a strictly ascending, timestamp-ordered
// prefix of what follows TimestampMin, truncated by Limit. That requirement
// is checked on every window rather than trusted — see ascending.
type Source interface {
	GetChangeEvents(filter types.ChangeEventsFilter) ([]types.ChangeEvent, error)
}

// Publisher hands records to Kafka and returns only once the broker has
// answered for every one of them. *kgo.Client implements it as-is; the
// narrow shape is what lets the job's tests drive publication failures.
type Publisher interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// escalateAfter is how many consecutive failures turn a retry log line from
// WARN into ERROR. It governs both retryable paths — the change-events query
// and the publication of a window — because they fail the same way: below the
// threshold the cause is almost always a broker, a leader election or a
// cluster blinking; above it the job is stuck on something a retry cannot fix
// — a record over max.message.bytes, a topic that cannot be auto-created, a
// cdc.batch_size above what TigerBeetle will serve — and a stuck stream looks
// exactly like an idle one unless somebody says so.
const escalateAfter = 5

// Job streams TigerBeetle change events to Kafka.
//
// Delivery is at-least-once: a window is published and acknowledged before
// the cursor moves past it, so a crash replays the window rather than
// skipping it.
//
// # Deduplicating on the consumer side
//
// The event timestamp is a complete deduplication key on its own —
// TigerBeetle guarantees it unique — so no compound key is needed. But it
// must be applied *idempotently*, per event: keep a seen-set of timestamps,
// or upsert into a store keyed by timestamp. Discarding anything at or below
// a running maximum is wrong and loses events, for two independent reasons:
//
//   - The topic is keyed and multi-partition, so there is no global timestamp
//     order across it. A consumer that reads 105 from partition 0 and then
//     103 from partition 1 would discard 103 as "already handled".
//   - Replay after a partial window delivers events behind ones already seen.
//     If 104 landed and 103 did not, the republished window delivers 103
//     after 104 has been handled.
//
// A per-partition high-water mark fixes only the first and still loses 103 to
// the second.
//
// # Exactly one instance
//
// Nothing fences this job: it has no consumer group, no lock and no leader
// election. Exactly one instance may run against a given output topic. See
// Run.
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
//
// Run must never be called twice against the same output topic, in this
// process or in another. Nothing enforces it: the job has no consumer group,
// no lock and no leader election, so two instances — an overlapping rolling
// deploy, a stale pod nobody noticed — each publish the whole stream forever.
// That is permanent duplication rather than a transient one the contract
// allows, and two writers interleaving on the same key destroy the per-key
// ordering cdc.partition_key exists to provide.
//
// Retries are unbounded, so the only errors Run returns are the ones a retry
// could not fix: today, a window that is not in ascending timestamp order.
func (j *Job) Run(ctx context.Context, checkpoint uint64) error {
	j.log.Info("cdc: starting", slog.String("topic", j.cfg.Topic),
		slog.Uint64("checkpoint", checkpoint), slog.Int("batch_size", j.cfg.BatchSize),
		slog.String("partition_key", j.cfg.PartitionKey))

	delay := j.retry.Initial
	// stuck counts how many times the window at the current cursor has failed
	// to publish. Only a successful publication clears it — and only a
	// successful publication moves the cursor — so it counts failures of one
	// window, which is what tells a stuck stream from a flaky broker.
	stuck := 0
	// queryStuck is the same idea for the other retryable path: how many times
	// in a row the change-events query itself has failed. A query that can
	// never succeed is retried just as forever as a window that can never be
	// published, so it escalates on the same threshold — otherwise a
	// permanently misconfigured job logs WARN and nothing else, indefinitely.
	queryStuck := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		events, err := j.src.GetChangeEvents(types.ChangeEventsFilter{
			TimestampMin: checkpoint + 1,
			Limit:        uint32(j.cfg.BatchSize),
		})
		if err != nil {
			queryStuck++
			level, msg := slog.LevelWarn, "cdc: change events query failed, retrying"
			if queryStuck >= escalateAfter {
				level = slog.LevelError
				msg = "cdc: the stream is stuck: the change events query keeps failing"
			}
			j.log.Log(ctx, level, msg,
				slog.Uint64("checkpoint", checkpoint),
				slog.Int("batch_size", j.cfg.BatchSize),
				slog.Int("consecutive_failures", queryStuck),
				slog.String("error", err.Error()), slog.Duration("in", delay))
			if !sleep(ctx, j.jitter(delay)) {
				return nil
			}
			delay = j.next(delay)
			continue
		}
		queryStuck = 0
		if len(events) == 0 {
			if !sleep(ctx, j.cfg.PollInterval) {
				return nil
			}
			delay = j.retry.Initial
			continue
		}
		if err := ascending(events); err != nil {
			j.log.Error("cdc: refusing to publish an out-of-order window",
				slog.Uint64("checkpoint", checkpoint), slog.Int("events", len(events)),
				slog.String("error", err.Error()))
			return err
		}
		published, err := j.publish(ctx, events, checkpoint)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			stuck++
			level, msg := slog.LevelWarn, "cdc: publication failed, republishing the whole window"
			if stuck >= escalateAfter {
				level = slog.LevelError
				msg = "cdc: the stream is stuck: the same window has failed to publish repeatedly"
			}
			j.log.Log(ctx, level, msg,
				slog.Uint64("checkpoint", checkpoint),
				slog.Uint64("window_min", checkpoint+1),
				slog.Uint64("window_max", events[len(events)-1].Timestamp),
				slog.Int("events", len(events)), slog.Int("consecutive_failures", stuck),
				slog.String("error", err.Error()), slog.Duration("in", delay))
			if !sleep(ctx, j.jitter(delay)) {
				return nil
			}
			delay = j.next(delay)
			continue
		}
		checkpoint = published
		stuck = 0
		delay = j.retry.Initial
	}
}

// publish writes one window and returns the checkpoint it establishes: the
// last event's timestamp, now provably present in the topic. The caller has
// already established that events is in ascending timestamp order (see
// ascending), which is what makes the last element the highest one and what
// makes the checkpoint it claims true.
//
// The window goes out in two steps. First every record but the last, all
// claiming the checkpoint the window started from. Only once those are
// acknowledged does the closing record go out, and it claims its own
// timestamp — "everything up to here is in this topic".
//
// The second step is what buys gap-freedom on a topic with many partitions.
// A crash mid-window leaves each partition holding some prefix of what was
// destined for it, and those prefixes end at different timestamps; the
// highest event timestamp then found in the topic is not a point the stream
// is complete up to. The closing record cannot land under that condition —
// it is produced only after everything else in its window is acknowledged —
// so a claim of completeness is never written unless it is true.
func (j *Job) publish(ctx context.Context, events []types.ChangeEvent, checkpoint uint64) (uint64, error) {
	closing := len(events) - 1
	rest := make([]*kgo.Record, 0, closing)
	for _, ev := range events[:closing] {
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

// ascending checks the one thing the whole checkpoint scheme rests on:
// GetChangeEvents answers with a strictly ascending, timestamp-ordered prefix
// of what follows the cursor.
//
// The job depends on that — the closing record claims the window's last
// timestamp as "every event up to here is in this topic", and the cursor then
// jumps there. If this experimental API ever answered with an unordered set
// truncated by Limit, that claim would sit above events the window omitted,
// and the next window would start past them: a permanent gap, the one thing
// the two-step publish exists to prevent.
//
// So the dependency is checked rather than trusted or, worse, denied. A
// violation stops the job: no retry can reorder the answer, and continuing
// would corrupt the stream irreversibly.
func ascending(events []types.ChangeEvent) error {
	for i := 1; i < len(events); i++ {
		if events[i].Timestamp <= events[i-1].Timestamp {
			return fmt.Errorf(
				"cdc: change events are not in ascending timestamp order, which the checkpoint "+
					"scheme depends on: event %d has timestamp %d, event %d has %d",
				i-1, events[i-1].Timestamp, i, events[i].Timestamp)
		}
	}
	return nil
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
