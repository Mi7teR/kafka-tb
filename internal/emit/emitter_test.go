package emit

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newTestEmitter spins up an in-process fake broker and returns an Emitter
// wired to it, along with the broker addresses (captured directly from
// fake.ListenAddrs(), not recovered from the client) and the config used.
func newTestEmitter(t *testing.T) (Emitter, []string, config.Kafka) {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1),
		kfake.SeedTopics(1, "src", "src.dlq", "results"))
	require.NoError(t, err)
	t.Cleanup(fake.Close)

	brokers := fake.ListenAddrs()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	cfg := config.Kafka{DLQTopic: "src.dlq", ResultsTopic: "results"}
	return New(cl, cfg), brokers, cfg
}

func consumeOne(t *testing.T, brokers []string, topic string) *kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fetches := cl.PollRecords(ctx, 1)
	require.NoError(t, fetches.Err())
	recs := fetches.Records()
	require.Len(t, recs, 1)
	return recs[0]
}

func TestDLQPreservesPayloadAndAddsHeaders(t *testing.T) {
	em, brokers, _ := newTestEmitter(t)
	src := &kgo.Record{
		Topic: "src", Partition: 3, Offset: 42,
		Key: []byte("k"), Value: []byte(`{"broken":`),
		Timestamp: time.Unix(1700000000, 0),
	}
	require.NoError(t, em.DLQ(context.Background(), src, ReasonPoison, "json", "unexpected end of input"))
	require.NoError(t, em.Flush(context.Background()))

	got := consumeOne(t, brokers, "src.dlq")
	require.Equal(t, src.Value, got.Value, "payload must be byte-identical for replay")
	require.Equal(t, src.Key, got.Key)

	h := headerMap(got)
	require.Equal(t, "poison", h[HeaderReason])
	require.Equal(t, "json", h[HeaderError])
	require.Equal(t, "unexpected end of input", h[HeaderDetail])
	require.Equal(t, "src", h[HeaderSrcTopic])
	require.Equal(t, "3", h[HeaderSrcPartition])
	require.Equal(t, "42", h[HeaderSrcOffset])
}

func TestResultsCarryOutcomes(t *testing.T) {
	em, brokers, _ := newTestEmitter(t)
	src := &kgo.Record{Topic: "src", Partition: 0, Offset: 7, Key: []byte("k")}
	outcomes := []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusOK},
		{Index: 1, ID: "id-1", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}
	require.NoError(t, em.Results(context.Background(), src, outcomes))
	require.NoError(t, em.Flush(context.Background()))

	got := consumeOne(t, brokers, "results")
	var payload ResultsMessage
	require.NoError(t, json.Unmarshal(got.Value, &payload))
	require.Equal(t, "src", payload.Source.Topic)
	require.Equal(t, int64(7), payload.Source.Offset)
	require.Len(t, payload.Results, 2)
	require.Equal(t, "rejected", payload.Results[1].Status)
	require.Equal(t, "exceeds_credits", payload.Results[1].Error)
}

func TestResultsDisabledWhenTopicEmpty(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src.dlq"))
	require.NoError(t, err)
	t.Cleanup(fake.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	em := New(cl, config.Kafka{DLQTopic: "src.dlq"})
	err = em.Results(context.Background(), &kgo.Record{Topic: "src"}, nil)
	require.NoError(t, err, "disabled results topic must be a no-op, not an error")
}

func headerMap(r *kgo.Record) map[string]string {
	m := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
