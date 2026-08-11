package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Mi7teR/kafka-tb/internal/config"
)

// fakeSource answers windows out of a fixed, ascending event log, exactly the
// way TigerBeetle's change-events query does.
type fakeSource struct {
	mu      sync.Mutex
	events  []types.ChangeEvent
	filters []types.ChangeEventsFilter
	err     error
}

func (f *fakeSource) GetChangeEvents(filter types.ChangeEventsFilter) ([]types.ChangeEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filters = append(f.filters, filter)
	if f.err != nil {
		return nil, f.err
	}
	var out []types.ChangeEvent
	for _, ev := range f.events {
		if ev.Timestamp < filter.TimestampMin {
			continue
		}
		if len(out) == int(filter.Limit) {
			break
		}
		out = append(out, ev)
	}
	return out, nil
}

func (f *fakeSource) seen() []types.ChangeEventsFilter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.ChangeEventsFilter(nil), f.filters...)
}

// fakePublisher records every ProduceSync call as its own group, so a test
// can see how the job split a window and in what order.
type fakePublisher struct {
	mu    sync.Mutex
	calls [][]*kgo.Record
	// failCalls indexes calls (from 1) that must fail instead of being acked.
	failCalls map[int]bool
	// failAll stands in for a permanently unpublishable window: a record over
	// max.message.bytes, a topic that cannot be created.
	failAll bool
}

func newFakePublisher(fail ...int) *fakePublisher {
	p := &fakePublisher{failCalls: map[int]bool{}}
	for _, n := range fail {
		p.failCalls[n] = true
	}
	return p
}

func (p *fakePublisher) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	p.mu.Lock()
	p.calls = append(p.calls, rs)
	n := len(p.calls)
	fail := p.failAll || p.failCalls[n]
	p.mu.Unlock()

	results := make(kgo.ProduceResults, 0, len(rs))
	for _, r := range rs {
		var err error
		if fail {
			err = errors.New("broker unavailable")
		}
		results = append(results, kgo.ProduceResult{Record: r, Err: err})
	}
	return results
}

func (p *fakePublisher) groups() [][]*kgo.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]*kgo.Record(nil), p.calls...)
}

func testEvents(n int) []types.ChangeEvent {
	out := make([]types.ChangeEvent, n)
	for i := range out {
		ev := sampleEvent(types.ChangeEventSinglePhase)
		ev.Timestamp = uint64(100 + i)
		ev.TransferID = idOf(int64(1000 + i))
		ev.DebitAccountID = idOf(int64(2000 + i))
		out[i] = ev
	}
	return out
}

func testJobConfig() config.CDC {
	return config.CDC{
		Topic:        "events",
		BatchSize:    3,
		PollInterval: 10 * time.Millisecond,
		PartitionKey: config.PartitionKeyDebitAccountID,
	}
}

func testRetry() config.Retry {
	return config.Retry{Initial: 5 * time.Millisecond, Max: 20 * time.Millisecond}
}

func runJob(t *testing.T, j *Job, checkpoint uint64) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- j.Run(ctx, checkpoint) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("cdc job did not stop within 10s of cancellation")
		}
	})
	return cancel, done
}

func checkpointOf(t *testing.T, rec *kgo.Record) string {
	t.Helper()
	var m struct {
		Timestamp  string `json:"timestamp"`
		Checkpoint string `json:"checkpoint"`
	}
	require.NoError(t, json.Unmarshal(rec.Value, &m))
	return m.Checkpoint
}

func timestampOf(t *testing.T, rec *kgo.Record) string {
	t.Helper()
	var m struct {
		Timestamp string `json:"timestamp"`
	}
	require.NoError(t, json.Unmarshal(rec.Value, &m))
	return m.Timestamp
}

// The window is cdc.batch_size wide and the next one starts one nanosecond
// past the last event published, so no event is asked for twice and none is
// skipped.
func TestJobWindowsAdvanceByBatchSize(t *testing.T) {
	src := &fakeSource{events: testEvents(7)}
	pub := newFakePublisher()
	j := New(testJobConfig(), testRetry(), src, pub, testRegistry(), testLog())
	runJob(t, j, 0)

	require.Eventually(t, func() bool { return len(publishedRecords(pub)) == 7 }, 5*time.Second, 5*time.Millisecond)

	filters := src.seen()
	require.GreaterOrEqual(t, len(filters), 3)
	require.Equal(t, uint64(1), filters[0].TimestampMin, "an empty topic starts from the beginning of time")
	require.Equal(t, uint32(3), filters[0].Limit)
	require.Equal(t, uint64(103), filters[1].TimestampMin)
	require.Equal(t, uint64(106), filters[2].TimestampMin)

	var got []string
	for _, rec := range publishedRecords(pub) {
		got = append(got, timestampOf(t, rec))
	}
	require.Equal(t, []string{"100", "101", "102", "103", "104", "105", "106"}, got)
}

// Within a window every record but the last claims the previous window's
// checkpoint; the last one claims its own timestamp and is published alone,
// after the rest are acknowledged. That ordering is what makes the claim
// true: a crash before it lands leaves no record claiming the batch is
// complete.
func TestJobPublishesTheClosingRecordAloneAndLast(t *testing.T) {
	src := &fakeSource{events: testEvents(3)}
	pub := newFakePublisher()
	j := New(testJobConfig(), testRetry(), src, pub, testRegistry(), testLog())
	runJob(t, j, 0)

	require.Eventually(t, func() bool { return len(pub.groups()) >= 2 }, 5*time.Second, 5*time.Millisecond)
	groups := pub.groups()

	require.Len(t, groups[0], 2, "everything but the closing record goes first")
	for _, rec := range groups[0] {
		require.Equal(t, "0", checkpointOf(t, rec))
	}
	require.Len(t, groups[1], 1, "the closing record is published alone")
	require.Equal(t, "102", timestampOf(t, groups[1][0]))
	require.Equal(t, "102", checkpointOf(t, groups[1][0]))
}

// The cursor moves only behind an acknowledgement: a failed publication is
// retried from the same place, never stepped over.
func TestJobRepublishesWindowUntilAcknowledged(t *testing.T) {
	src := &fakeSource{events: testEvents(3)}
	pub := newFakePublisher(1) // the first publication fails
	j := New(testJobConfig(), testRetry(), src, pub, testRegistry(), testLog())
	runJob(t, j, 0)

	require.Eventually(t, func() bool { return len(src.seen()) >= 2 }, 5*time.Second, 5*time.Millisecond)
	filters := src.seen()
	require.Equal(t, filters[0].TimestampMin, filters[1].TimestampMin,
		"an unacknowledged window is requested again, not skipped")

	require.Eventually(t, func() bool { return len(publishedRecords(pub)) >= 5 }, 5*time.Second, 5*time.Millisecond)
}

// An empty answer is not an error: the job waits cdc.poll_interval and asks
// again from the same place.
func TestJobPollsAfterAnEmptyWindow(t *testing.T) {
	src := &fakeSource{}
	pub := newFakePublisher()
	j := New(testJobConfig(), testRetry(), src, pub, testRegistry(), testLog())
	runJob(t, j, 41)

	require.Eventually(t, func() bool { return len(src.seen()) >= 3 }, 5*time.Second, 5*time.Millisecond)
	for _, f := range src.seen() {
		require.Equal(t, uint64(42), f.TimestampMin, "the cursor stays put while there is nothing to publish")
	}
	require.Empty(t, pub.groups())
}

// A source that is down must not kill the job or advance it.
func TestJobRetriesSourceFailures(t *testing.T) {
	src := &fakeSource{err: errors.New("tigerbeetle unreachable")}
	pub := newFakePublisher()
	j := New(testJobConfig(), testRetry(), src, pub, testRegistry(), testLog())
	_, done := runJob(t, j, 7)

	require.Eventually(t, func() bool { return len(src.seen()) >= 3 }, 5*time.Second, 5*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("job exited on a source failure: %v", err)
	default:
	}
}

// The closing record claims the window's last timestamp as a point the stream
// is complete up to, and the cursor jumps there. That is only true if the
// window is an ascending prefix, so the job checks it rather than trusting an
// experimental API — and stops, because no retry can reorder an answer and
// publishing one would leave a permanent gap.
func TestJobStopsOnAnOutOfOrderWindow(t *testing.T) {
	events := testEvents(3)
	events[1], events[2] = events[2], events[1] // 100, 102, 101
	src := &fakeSource{events: events}
	pub := newFakePublisher()
	j := New(testJobConfig(), testRetry(), src, pub, testRegistry(), testLog())

	err := j.Run(context.Background(), 0)
	require.Error(t, err, "an out-of-order window must not be published")
	require.Contains(t, err.Error(), "ascending timestamp order")
	require.Empty(t, pub.groups(), "nothing of the window may reach the topic")
}

func TestJobRejectsRepeatedTimestamps(t *testing.T) {
	events := testEvents(2)
	events[1].Timestamp = events[0].Timestamp
	src := &fakeSource{events: events}
	j := New(testJobConfig(), testRetry(), src, newFakePublisher(), testRegistry(), testLog())

	require.ErrorContains(t, j.Run(context.Background(), 0), "ascending timestamp order")
}

// A window that can never be published — a record over max.message.bytes, a
// topic that cannot be created — is retried forever by design. The operator
// has to be able to tell that from an idle stream, so the line escalates to
// ERROR and carries the window and the failure count.
func TestJobEscalatesAWindowThatKeepsFailing(t *testing.T) {
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	src := &fakeSource{events: testEvents(3)}
	pub := newFakePublisher()
	pub.failAll = true
	j := New(testJobConfig(), testRetry(), src, pub, testRegistry(), log)
	runJob(t, j, 0)

	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "level=ERROR")
	}, 5*time.Second, 5*time.Millisecond, "a window failing forever must escalate past WARN")

	out := logs.String()
	require.Contains(t, out, "the stream is stuck")
	require.Contains(t, out, "window_min=1")
	require.Contains(t, out, "window_max=102", "the failing window's range names the offending records")
	require.Contains(t, out, "consecutive_failures=5")

	// Everything before the escalation threshold stays at WARN: a broker
	// blinking is not an incident.
	require.Equal(t, escalateAfter-1,
		strings.Count(out, "level=WARN msg=\"cdc: publication failed"))
}

func publishedRecords(p *fakePublisher) []*kgo.Record {
	var out []*kgo.Record
	for _, g := range p.groups() {
		out = append(out, g...)
	}
	return out
}

// A crash between publishing and moving the cursor: the closing record of the
// window never lands, so on restart the topic's checkpoint still names the
// last completed window. The job republishes the whole unfinished window —
// the consumer sees duplicates and no gap.
func TestRestartAfterAPartialWindowDuplicatesButLosesNothing(t *testing.T) {
	brokers := fakeBroker(t, 3)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	events := testEvents(6)
	cfg := testJobConfig()
	cfg.BatchSize = 3

	// First run: the second window's opening records land (call 3), its
	// closing record never does (call 4). That is the crash the recovery has
	// to survive: part of a window is in the topic, with nothing claiming the
	// window completed.
	crashed := make(chan struct{})
	var once sync.Once
	pub := &partialPublisher{
		inner: cl,
		failFrom: func(n int) bool {
			if n < 4 {
				return false
			}
			once.Do(func() { close(crashed) })
			return true
		},
	}
	first := New(cfg, testRetry(), &fakeSource{events: events}, pub, testRegistry(), testLog())
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(ctx, 0) }()
	select {
	case <-crashed:
	case <-time.After(10 * time.Second):
		t.Fatal("the job never reached the window that was supposed to fail")
	}
	cancel()
	require.NoError(t, <-firstDone)

	// Restart: the cursor comes from the topic and nothing else.
	resumed, err := Resume(context.Background(), brokers, "events", testLog())
	require.NoError(t, err)
	require.Equal(t, uint64(102), resumed, "only the completed window may be trusted")

	second := New(cfg, testRetry(), &fakeSource{events: events}, cl, testRegistry(), testLog())
	runJob(t, second, resumed)

	var seen map[string]int
	require.Eventually(t, func() bool {
		seen = consumeTimestamps(t, brokers, "events")
		return len(seen) == 6
	}, 15*time.Second, 50*time.Millisecond)

	for _, ev := range events {
		require.Contains(t, seen, strconv.FormatUint(ev.Timestamp, 10), "no event may be missing")
	}
	require.Greater(t, seen["103"], 1, "the unfinished window is republished, duplicates and all")
}

// partialPublisher publishes through a real client until failFrom says a call
// must fail, which stands in for the process dying mid-window.
type partialPublisher struct {
	inner    *kgo.Client
	mu       sync.Mutex
	n        int
	failFrom func(int) bool
}

func (p *partialPublisher) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	p.mu.Lock()
	p.n++
	n := p.n
	p.mu.Unlock()
	if p.failFrom(n) {
		results := make(kgo.ProduceResults, 0, len(rs))
		for _, r := range rs {
			results = append(results, kgo.ProduceResult{Record: r, Err: errors.New("crashed")})
		}
		return results
	}
	return p.inner.ProduceSync(ctx, rs...)
}

// consumeTimestamps reads the whole topic and counts how often each event
// timestamp appears.
func consumeTimestamps(t *testing.T, brokers []string, topic string) map[string]int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()

	out := map[string]int{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		recs := fetches.Records()
		if len(recs) == 0 {
			return out
		}
		for _, rec := range recs {
			out[timestampOf(t, rec)]++
		}
	}
}
