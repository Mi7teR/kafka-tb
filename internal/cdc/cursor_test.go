package cdc

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// fakeBroker starts an in-process Kafka with topic "events" split into
// partitions, and returns its addresses.
func fakeBroker(t *testing.T, partitions int) []string {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(int32(partitions), "events"))
	require.NoError(t, err)
	t.Cleanup(fake.Close)
	return fake.ListenAddrs()
}

// produceTo writes records to an exact partition, so a test can build a topic
// whose partitions have deliberately unequal tails.
func produceTo(t *testing.T, brokers []string, partition int32, recs ...*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, r := range recs {
		r.Partition = partition
	}
	require.NoError(t, cl.ProduceSync(ctx, recs...).FirstErr())
}

// eventRecord renders one event at ts, claiming checkpoint as the topic's
// resume point.
func eventRecord(t *testing.T, ts, checkpoint uint64) *kgo.Record {
	t.Helper()
	enc, _ := testEncoder(t, "")
	ev := sampleEvent(0)
	ev.Timestamp = ts
	rec, err := enc.Record(ev, checkpoint)
	require.NoError(t, err)
	return rec
}

func TestResumeEmptyTopicStartsAtZero(t *testing.T) {
	brokers := fakeBroker(t, 3)
	got, err := Resume(context.Background(), brokers, "events", testLog())
	require.NoError(t, err)
	require.Zero(t, got, "an empty topic replays everything from the start")
}

func TestResumeMissingTopicStartsAtZero(t *testing.T) {
	brokers := fakeBroker(t, 1)
	got, err := Resume(context.Background(), brokers, "never-created", testLog())
	require.NoError(t, err)
	require.Zero(t, got)
}

// The tail has to be read on every partition: the highest checkpoint may sit
// on any of them, and reading only one would silently replay everything the
// others already hold.
func TestResumeTakesHighestCheckpointAcrossPartitions(t *testing.T) {
	brokers := fakeBroker(t, 3)
	produceTo(t, brokers, 0, eventRecord(t, 100, 100))
	produceTo(t, brokers, 2, eventRecord(t, 300, 300))
	produceTo(t, brokers, 1, eventRecord(t, 200, 200))

	got, err := Resume(context.Background(), brokers, "events", testLog())
	require.NoError(t, err)
	require.Equal(t, uint64(300), got)
}

// Unequal tails after a crash mid-batch: partition 0 holds a record from the
// unfinished batch (checkpoint still 100, its own timestamp already 250),
// partition 1 stops at the last record of the completed batch. Resuming from
// the highest *event* timestamp would skip whatever of that batch never
// reached the broker; the checkpoint is what may be trusted.
func TestResumeIgnoresEventTimestampsOfAnUnfinishedBatch(t *testing.T) {
	brokers := fakeBroker(t, 2)
	produceTo(t, brokers, 1, eventRecord(t, 100, 100))
	produceTo(t, brokers, 0, eventRecord(t, 250, 100))

	got, err := Resume(context.Background(), brokers, "events", testLog())
	require.NoError(t, err)
	require.Equal(t, uint64(100), got, "the unfinished batch must be republished whole")
}

// A record this job did not write (or one it can no longer parse) must not be
// trusted as progress: skipping it costs duplicates, believing it could cost
// an event.
//
// The warning is asserted rather than only the cursor. An unreadable record
// yields checkpoint zero either way, so the returned number alone cannot tell
// "skipped it" apart from "read it as zero"; the log line is what makes the
// skip observable, and it is what an operator has to see to know the tail of
// their topic holds something this build does not understand.
func TestResumeSkipsUnparsableTailRecord(t *testing.T) {
	var logged bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))

	brokers := fakeBroker(t, 2)
	produceTo(t, brokers, 0, eventRecord(t, 100, 100))
	produceTo(t, brokers, 1, &kgo.Record{Topic: "events", Value: []byte("not json")})

	got, err := Resume(context.Background(), brokers, "events", log)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got)
	require.Contains(t, logged.String(), "unreadable record")
}

// A record whose checkpoint is not a number must be skipped rather than
// trusted. This is a different failure from an unparsable record: the message
// is well-formed and this job may even have written it, but its checkpoint
// says nothing.
//
// Every case below carries a high event timestamp as well, so that the test
// fails if the checkpoint is ever quietly backfilled from the timestamp --
// resuming from an event timestamp is precisely the bug the checkpoint
// exists to prevent. The warning is asserted too: skipping a tail record is
// the one thing an operator has to be able to see in the log, and it is the
// only externally visible difference between "unusable" and "zero".
func TestResumeSkipsRecordWithAnUnusableCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{"not a number", []byte(`{"timestamp":"9999","checkpoint":"tomorrow"}`)},
		{"empty", []byte(`{"timestamp":"9999","checkpoint":""}`)},
		{"absent", []byte(`{"timestamp":"9999"}`)},
		{"negative", []byte(`{"timestamp":"9999","checkpoint":"-1"}`)},
		{"overflows uint64", []byte(`{"timestamp":"9999","checkpoint":"18446744073709551616"}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logged bytes.Buffer
			log := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))

			brokers := fakeBroker(t, 2)
			produceTo(t, brokers, 0, eventRecord(t, 100, 100))
			produceTo(t, brokers, 1, &kgo.Record{Topic: "events", Value: tc.value})

			got, err := Resume(context.Background(), brokers, "events", log)
			require.NoError(t, err)
			require.Equal(t, uint64(100), got,
				"a checkpoint that cannot be read must not move the cursor, "+
					"and must never fall back to the event timestamp")
			require.Contains(t, logged.String(), "no usable checkpoint",
				"skipping a tail record has to be visible to an operator")
		})
	}
}

// The scan must fail the start rather than silently resume from zero when the
// topic cannot be read at all: returning 0 here would replay the entire change
// stream, which is exactly the outcome the checkpoint exists to avoid.
func TestResumeFailsWhenTheTopicCannotBeListed(t *testing.T) {
	brokers := fakeBroker(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := Resume(ctx, brokers, "events", testLog())
	require.Error(t, err)
	require.Zero(t, got)
	require.ErrorContains(t, err, "list start offsets of events",
		"the error must say which topic and which step failed")
	require.ErrorIs(t, err, context.Canceled)
}

// A partition with nothing in it is not progress and must not hold up the
// scan: Resume reads only the partitions that actually have a tail, so a
// topic where one partition was never written to still returns the other's
// checkpoint instead of blocking on a record that will never arrive.
func TestResumeIgnoresPartitionsThatWereNeverWritten(t *testing.T) {
	brokers := fakeBroker(t, 4)
	produceTo(t, brokers, 2, eventRecord(t, 700, 700))

	got, err := Resume(context.Background(), brokers, "events", testLog())
	require.NoError(t, err)
	require.Equal(t, uint64(700), got)
}
