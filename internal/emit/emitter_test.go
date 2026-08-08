package emit

import (
	"context"
	"encoding/json"
	"errors"
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
	require.NoError(t, em.DLQ(context.Background(), src, ReasonPoison, "json", "unexpected end of input").
		Wait(context.Background()))
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

	// Gap 1: Assert timestamp headers are correctly formatted
	expectedSrcTS := src.Timestamp.UTC().Format(time.RFC3339Nano)
	require.Equal(t, expectedSrcTS, h[HeaderSrcTimestamp], "src timestamp must be in RFC3339Nano format with UTC")
	require.NotEmpty(t, h[HeaderAttemptTS], "attempt timestamp must be set")
	// Verify HeaderAttemptTS parses and is not zero time
	attemptTS, err := time.Parse(time.RFC3339Nano, h[HeaderAttemptTS])
	require.NoError(t, err, "attempt timestamp must parse as RFC3339Nano")
	require.NotEqual(t, time.Time{}, attemptTS, "attempt timestamp must not be zero time")
}

func TestResultsCarryOutcomes(t *testing.T) {
	em, brokers, _ := newTestEmitter(t)
	src := &kgo.Record{Topic: "src", Partition: 0, Offset: 7, Key: []byte("k")}
	outcomes := []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusOK},
		{Index: 1, ID: "id-1", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}
	require.NoError(t, em.Results(context.Background(), src, outcomes).Wait(context.Background()))
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
	err = em.Results(context.Background(), &kgo.Record{Topic: "src"}, nil).Wait(context.Background())
	require.NoError(t, err, "disabled results topic must be a no-op, not an error")
	require.NoError(t, em.Flush(context.Background()),
		"a no-op publication must not be counted as unacknowledged")
}

func TestDLQHeaderWireNames(t *testing.T) {
	// Gap 2: Assert wire-level header names are exactly the expected literal strings.
	// This pins the contract against accidental renaming of constant values.
	em, brokers, _ := newTestEmitter(t)
	src := &kgo.Record{
		Topic: "src", Partition: 0, Offset: 0,
		Key: []byte("k"), Value: []byte("v"),
		Timestamp: time.Unix(1700000000, 0),
	}
	require.NoError(t, em.DLQ(context.Background(), src, ReasonReject, "test", "test detail").
		Wait(context.Background()))
	require.NoError(t, em.Flush(context.Background()))

	got := consumeOne(t, brokers, "src.dlq")

	// Assert exact wire-level header names (not via constants)
	expectedHeaders := map[string]bool{
		"x-kafkatb-reason":        false,
		"x-kafkatb-error":         false,
		"x-kafkatb-detail":        false,
		"x-kafkatb-src-topic":     false,
		"x-kafkatb-src-partition": false,
		"x-kafkatb-src-offset":    false,
		"x-kafkatb-src-timestamp": false,
		"x-kafkatb-attempt-ts":    false,
	}
	for _, h := range got.Headers {
		if _, exists := expectedHeaders[h.Key]; exists {
			expectedHeaders[h.Key] = true
		}
	}
	for name, found := range expectedHeaders {
		require.True(t, found, "header %q must be present in wire format", name)
	}
}

func TestDLQZeroValueTimestamp(t *testing.T) {
	// Gap 3: Document behavior when rec.Timestamp is zero (unset).
	// This records the current behavior so future changes are deliberate.
	em, brokers, _ := newTestEmitter(t)
	src := &kgo.Record{
		Topic: "src", Partition: 0, Offset: 0,
		Key: []byte("k"), Value: []byte("v"),
		Timestamp: time.Time{}, // zero value
	}
	require.NoError(t, em.DLQ(context.Background(), src, ReasonPoison, "test", "test detail").
		Wait(context.Background()))
	require.NoError(t, em.Flush(context.Background()))

	got := consumeOne(t, brokers, "src.dlq")
	h := headerMap(got)

	// Zero-value time.Time silently formats to "0001-01-01T00:00:00Z"
	zeroFormatted := time.Time{}.UTC().Format(time.RFC3339Nano)
	require.Equal(t, zeroFormatted, h[HeaderSrcTimestamp],
		"zero-value timestamp formats to %q", zeroFormatted)
}

// newFailingEmitter returns an Emitter whose DLQ topic does not exist on the
// broker and never will: with UnknownTopicRetries(1) franz-go gives up on the
// record quickly and fails its promise. The record goes through the producer
// buffer on the way, which is what makes it a fair stand-in for a real produce
// failure — Flush sees it.
func newFailingEmitter(t *testing.T) Emitter {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"))
	require.NoError(t, err)
	t.Cleanup(fake.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...), kgo.UnknownTopicRetries(1))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return New(cl, config.Kafka{DLQTopic: "no-such-topic"})
}

// Публикации выдаются все сразу и подтверждаются одним проходом ожидания:
// именно на этом держится смысл задачи — ожидание стоит одного round-trip'а на
// пачку, а не round-trip'а на запись. Заодно это пиннит, что порядок Wait не
// обязан совпадать с порядком, в котором брокер отвечает.
func TestPublicationsIssuedBeforeWaitingAllSucceed(t *testing.T) {
	const n = 200
	em, brokers, _ := newTestEmitter(t)
	ctx := context.Background()

	pubs := make([]*Publication, n)
	for i := range pubs {
		pubs[i] = em.DLQ(ctx, &kgo.Record{
			Topic: "src", Partition: 0, Offset: int64(i), Value: []byte("v"),
		}, ReasonPoison, "decode", "bad json")
	}
	for i, p := range pubs {
		require.NoError(t, p.Wait(ctx), "publication %d must be acknowledged", i)
	}
	require.NoError(t, em.Flush(ctx))

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("src.dlq"))
	require.NoError(t, err)
	defer cl.Close()
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var got int
	for got < n {
		fetches := cl.PollRecords(pollCtx, n)
		require.NoError(t, fetches.Err())
		got += len(fetches.Records())
	}
	require.Equal(t, n, got, "every issued publication must reach the topic")
}

// Провал публикации обязан доезжать до вызывающего через Wait, а не растворяться
// в буфере продюсера.
func TestWaitSurfacesPublicationFailure(t *testing.T) {
	em := newFailingEmitter(t)
	ctx := context.Background()
	err := em.DLQ(ctx, &kgo.Record{Topic: "src", Value: []byte("v")}, ReasonPoison, "decode", "x").Wait(ctx)
	require.Error(t, err, "a publication that never reached a broker must not report success")
}

// Flush обязан учитывать ошибки, а не только опустошение буфера: иначе коммит
// после него закоммитил бы офсет записи, которой в DLQ нет.
func TestFlushSurfacesUntakenPublicationFailure(t *testing.T) {
	em := newFailingEmitter(t)
	ctx := context.Background()
	em.DLQ(ctx, &kgo.Record{Topic: "src", Value: []byte("v")}, ReasonPoison, "decode", "x")

	require.Error(t, em.Flush(ctx),
		"Flush must report a failed publication nobody waited on, not just drain the buffer")
}

// Ошибку, которую вызывающий уже забрал через Wait, Flush повторять не должен:
// на неё уже отреагировали откатом партиции, а вечно валящийся Flush остановил
// бы коммит всех остальных партиций.
func TestFlushIgnoresFailureAlreadyTakenByWait(t *testing.T) {
	em := newFailingEmitter(t)
	ctx := context.Background()
	p := em.DLQ(ctx, &kgo.Record{Topic: "src", Value: []byte("v")}, ReasonPoison, "decode", "x")
	require.Error(t, p.Wait(ctx))

	require.NoError(t, em.Flush(ctx),
		"a failure the caller already handled must not block every later commit")
}

// Доложив об ошибке один раз, Flush обязан сбросить учёт: иначе один провал
// навсегда остановил бы коммит.
func TestFlushReportsUntakenFailureOnlyOnce(t *testing.T) {
	em := newFailingEmitter(t)
	ctx := context.Background()
	em.DLQ(ctx, &kgo.Record{Topic: "src", Value: []byte("v")}, ReasonPoison, "decode", "x")

	require.Error(t, em.Flush(ctx))
	require.NoError(t, em.Flush(ctx), "the same failure must not block every later commit")
}

// Провал одной публикации не имеет права утаскивать за собой остальные: у
// каждой свой исход.
func TestOnePublicationFailureLeavesOthersAcknowledged(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src", "src.dlq"))
	require.NoError(t, err)
	t.Cleanup(fake.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx := context.Background()
	good := New(cl, config.Kafka{DLQTopic: "src.dlq"})
	bad := New(cl, config.Kafka{DLQTopic: ""})
	rec := &kgo.Record{Topic: "src", Value: []byte("v")}

	first := good.DLQ(ctx, rec, ReasonPoison, "decode", "x")
	broken := bad.DLQ(ctx, rec, ReasonPoison, "decode", "x")
	second := good.DLQ(ctx, rec, ReasonPoison, "decode", "x")

	require.Error(t, broken.Wait(ctx))
	require.NoError(t, first.Wait(ctx), "a neighbour's failure must not fail this publication")
	require.NoError(t, second.Wait(ctx), "a publication issued after a failure must still be confirmed")
}

// Resolved — обещание, которого никогда не было у брокера: у него нет эмиттера,
// и учёт Flush оно трогать не имеет права.
func TestResolvedCarriesItsOwnOutcome(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, Resolved(nil).Wait(ctx))
	require.EqualError(t, Resolved(errTest).Wait(ctx), "test failure")
}

var errTest = errors.New("test failure")

// Уже пришедшее подтверждение важнее отменённого контекста: бросить готовый
// ответ значило бы соврать про уже сделанную работу.
func TestWaitPrefersDeliveredOutcomeOverCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, Resolved(nil).Wait(ctx))
}

// Не дождавшись ответа, Wait обязан вернуть ошибку, а не подтверждение.
func TestWaitOnUnfinishedPublicationRespectsContext(t *testing.T) {
	p := &Publication{done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.Wait(ctx), context.DeadlineExceeded)
}

func headerMap(r *kgo.Record) map[string]string {
	m := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
