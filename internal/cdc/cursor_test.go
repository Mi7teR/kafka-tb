package cdc

import (
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
func TestResumeSkipsUnparsableTailRecord(t *testing.T) {
	brokers := fakeBroker(t, 2)
	produceTo(t, brokers, 0, eventRecord(t, 100, 100))
	produceTo(t, brokers, 1, &kgo.Record{Topic: "events", Value: []byte("not json")})

	got, err := Resume(context.Background(), brokers, "events", testLog())
	require.NoError(t, err)
	require.Equal(t, uint64(100), got)
}
