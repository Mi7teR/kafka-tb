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

// Publications are all handed off at once and acknowledged in a single waiting pass:
// this is exactly what the point of the task rests on — waiting costs one round-trip per
// batch, not a round-trip per record. This also pins down that Wait order need
// not match the order in which the broker responds.
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

// A publication failure must reach the caller through Wait, not dissolve
// into the producer's buffer.
func TestWaitSurfacesPublicationFailure(t *testing.T) {
	em := newFailingEmitter(t)
	ctx := context.Background()
	err := em.DLQ(ctx, &kgo.Record{Topic: "src", Value: []byte("v")}, ReasonPoison, "decode", "x").Wait(ctx)
	require.Error(t, err, "a publication that never reached a broker must not report success")
}

// Flush must account for errors, not just draining the buffer: otherwise a commit
// after it would commit the offset of a record that is not in the DLQ.
func TestFlushSurfacesUntakenPublicationFailure(t *testing.T) {
	em := newFailingEmitter(t)
	ctx := context.Background()
	em.DLQ(ctx, &kgo.Record{Topic: "src", Value: []byte("v")}, ReasonPoison, "decode", "x")

	require.Error(t, em.Flush(ctx),
		"Flush must report a failed publication nobody waited on, not just drain the buffer")
}

// Flush must not repeat an error the caller has already claimed via Wait:
// it has already been reacted to by rolling back the partition, and a Flush that keeps failing forever would
// stop the commit of every other partition.
func TestFlushIgnoresFailureAlreadyTakenByWait(t *testing.T) {
	em := newFailingEmitter(t)
	ctx := context.Background()
	p := em.DLQ(ctx, &kgo.Record{Topic: "src", Value: []byte("v")}, ReasonPoison, "decode", "x")
	require.Error(t, p.Wait(ctx))

	require.NoError(t, em.Flush(ctx),
		"a failure the caller already handled must not block every later commit")
}

// Having reported an error once, Flush must reset its tracking: otherwise one failure
// would stop the commit forever.
func TestFlushReportsUntakenFailureOnlyOnce(t *testing.T) {
	em := newFailingEmitter(t)
	ctx := context.Background()
	em.DLQ(ctx, &kgo.Record{Topic: "src", Value: []byte("v")}, ReasonPoison, "decode", "x")

	require.Error(t, em.Flush(ctx))
	require.NoError(t, em.Flush(ctx), "the same failure must not block every later commit")
}

// One publication's failure has no right to drag the others down with it: each
// has its own outcome.
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

// Resolved is a promise that never touched the broker: it has no emitter,
// and it has no right to touch Flush's tracking.
func TestResolvedCarriesItsOwnOutcome(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, Resolved(nil).Wait(ctx))
	require.EqualError(t, Resolved(errTest).Wait(ctx), "test failure")
}

var errTest = errors.New("test failure")

// An acknowledgment that has already arrived matters more than a cancelled context: discarding a ready
// answer would mean lying about work already done.
func TestWaitPrefersDeliveredOutcomeOverCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, Resolved(nil).Wait(ctx))
}

// Without a response arriving in time, Wait must return an error, not an acknowledgment.
func TestWaitOnUnfinishedPublicationRespectsContext(t *testing.T) {
	p := &Publication{done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.Wait(ctx), context.DeadlineExceeded)
}

// One error's late Wait has no right to remove another from tracking, one added
// after a previous Flush already reported and reset the first: tracking must
// be kept by publication identity, not by a shared counter. It used to be
// the opposite — A reported and reset, C added, a late Wait(A)
// decremented the shared counter and erased C's error, and the next Flush
// lied that everything was acknowledged.
func TestFlushTracksFailuresByPublicationIdentity(t *testing.T) {
	em, _, _ := newTestEmitter(t)
	e := em.(*emitter)
	ctx := context.Background()

	errA := errors.New("failure A")
	pubA := e.failed(errA)

	firstFlushErr := em.Flush(ctx)
	require.Error(t, firstFlushErr, "Flush must report failure A")
	require.Contains(t, firstFlushErr.Error(), errA.Error())

	errC := errors.New("failure C")
	pubC := e.failed(errC)

	// A has already been reported and reset; a late Wait on it has no right to touch C.
	require.EqualError(t, pubA.Wait(ctx), errA.Error())

	secondFlushErr := em.Flush(ctx)
	require.Error(t, secondFlushErr, "C is still untaken and unacknowledged; the second Flush must report it")
	require.Contains(t, secondFlushErr.Error(), errC.Error(),
		"the reported error must belong to the failure actually still outstanding")

	require.EqualError(t, pubC.Wait(ctx), errC.Error())
	require.NoError(t, em.Flush(ctx), "both failures have now been taken or reported")
}

func headerMap(r *kgo.Record) map[string]string {
	m := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
