package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/mailru/easyjson"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// scanTimeout bounds the startup scan. It exists so that an unreachable
// broker fails the start with a clear error instead of hanging before the
// job has published anything.
const scanTimeout = 30 * time.Second

// Resume derives the job's cursor from the output topic itself — this job
// keeps no state anywhere else, exactly like TigerBeetle's official AMQP job.
//
// It reads the last record of every partition and returns the highest
// checkpoint claimed among them; the job then continues from that timestamp
// plus one. An empty or missing topic starts from zero, i.e. replays
// everything.
//
// Every partition has to be read, not just one: the job's records are keyed
// by whatever cdc.partition_key names, so the newest progress may sit on any
// partition. Partitions with unequal tails are the normal state of affairs
// after a crash, and the checkpoint — rather than the event timestamp — is
// what makes them safe to reduce with a maximum: a record only claims a
// checkpoint that was already acknowledged in full (see Job.publish), so the
// highest claim in the topic is a point the stream is complete up to,
// whatever the partition tails look like around it.
//
// A tail record that cannot be read is skipped with a warning rather than
// failing the start: skipping one lowers the cursor and costs duplicates,
// which the contract allows, while trusting it could cost an event.
func Resume(ctx context.Context, brokers []string, topic string, log *slog.Logger) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()

	admCl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return 0, fmt.Errorf("cdc: kafka client: %w", err)
	}
	defer admCl.Close()

	tails, err := tailOffsets(ctx, kadm.NewClient(admCl), topic)
	if err != nil {
		return 0, err
	}
	if len(tails) == 0 {
		log.Info("cdc: output topic is empty, starting from the beginning",
			slog.String("topic", topic))
		return 0, nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: tails}))
	if err != nil {
		return 0, fmt.Errorf("cdc: kafka client: %w", err)
	}
	defer cl.Close()

	var checkpoint uint64
	pending := len(tails)
	for pending > 0 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return 0, fmt.Errorf("cdc: reading the tail of %s: %w", topic, ctx.Err())
		}
		var fetchErr error
		fetches.EachError(func(_ string, p int32, err error) {
			fetchErr = fmt.Errorf("cdc: reading the tail of %s partition %d: %w", topic, p, err)
		})
		if fetchErr != nil {
			return 0, fetchErr
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if _, want := tails[rec.Partition]; !want {
				return // a later record of a partition already accounted for
			}
			delete(tails, rec.Partition)
			pending--
			ts, ok := recordCheckpoint(rec, log)
			if ok && ts > checkpoint {
				checkpoint = ts
			}
		})
	}
	log.Info("cdc: resuming from the output topic",
		slog.String("topic", topic), slog.Uint64("checkpoint", checkpoint))
	return checkpoint, nil
}

// tailOffsets returns, per partition, the offset of that partition's last
// record. Partitions with nothing in them are left out, and so is a topic
// that does not exist yet: both mean there is no progress to recover.
func tailOffsets(ctx context.Context, adm *kadm.Client, topic string) (map[int32]kgo.Offset, error) {
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("cdc: list start offsets of %s: %w", topic, err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("cdc: list end offsets of %s: %w", topic, err)
	}

	tails := make(map[int32]kgo.Offset)
	var listErr error
	ends.Each(func(end kadm.ListedOffset) {
		if end.Err != nil {
			if errors.Is(end.Err, kerr.UnknownTopicOrPartition) {
				return // nothing has ever been written here
			}
			listErr = fmt.Errorf("cdc: list end offsets of %s partition %d: %w",
				topic, end.Partition, end.Err)
			return
		}
		start, ok := starts.Lookup(topic, end.Partition)
		if !ok || start.Err != nil {
			// Without a start offset there is no way to tell an empty
			// partition from one whose head was deleted by retention; the
			// end offset alone still gives a safe read position.
			start = kadm.ListedOffset{Offset: 0}
		}
		if end.Offset <= start.Offset {
			return
		}
		tails[end.Partition] = kgo.NewOffset().At(end.Offset - 1)
	})
	if listErr != nil {
		return nil, listErr
	}
	return tails, nil
}

// recordCheckpoint reads the checkpoint a record claims. A record this job
// did not write — or one a future version wrote differently — reports false
// and is skipped by the caller.
func recordCheckpoint(rec *kgo.Record, log *slog.Logger) (uint64, bool) {
	var msg Message
	if err := easyjson.Unmarshal(rec.Value, &msg); err != nil {
		log.Warn("cdc: unreadable record at the tail of the output topic, ignoring it for recovery",
			slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
			slog.Int64("offset", rec.Offset), slog.String("error", err.Error()))
		return 0, false
	}
	ts, err := strconv.ParseUint(msg.Checkpoint, 10, 64)
	if err != nil {
		log.Warn("cdc: record at the tail of the output topic has no usable checkpoint",
			slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
			slog.Int64("offset", rec.Offset), slog.String("checkpoint", msg.Checkpoint))
		return 0, false
	}
	return ts, true
}
