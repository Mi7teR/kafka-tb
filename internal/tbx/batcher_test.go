package tbx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func transferCmd(n int, tag string) *model.Command {
	c := &model.Command{Op: model.OpCreateTransfers, Transfers: make([]types.Transfer, n), IDs: make([]string, n)}
	for i := 0; i < n; i++ {
		c.Transfers[i] = types.Transfer{ID: types.ToUint128(uint64(i + 1)), Flags: 1} // linked on all of them
		c.IDs[i] = tag + "-" + strconv.Itoa(i)
	}
	return c
}

func accountCmd(n int, tag string) *model.Command {
	c := &model.Command{Op: model.OpCreateAccounts, Accounts: make([]types.Account, n), IDs: make([]string, n)}
	for i := 0; i < n; i++ {
		c.Accounts[i] = types.Account{ID: types.ToUint128(uint64(i + 1)), Flags: 1}
		c.IDs[i] = tag + "-" + strconv.Itoa(i)
	}
	return c
}

func startBatcher(t *testing.T, fc *fakeClient, maxBatch int, linger time.Duration) (*Batcher, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: maxBatch, Linger: linger, MaxQueue: 128},
		config.Retry{Initial: time.Millisecond, Max: 10 * time.Millisecond}, testLogger(), nil)
	b.Start(ctx)
	t.Cleanup(func() { cancel(); b.Close() })
	return b, cancel
}

func TestBatcherNeverSplitsCommand(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 10, 20*time.Millisecond)

	var wg sync.WaitGroup
	for _, n := range []int{6, 6, 6} {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := b.Submit(context.Background(), transferCmd(n, "c"))
			// assert, not require: FailNow from a non-test goroutine is UB.
			assert.NoError(t, err)
		}(n)
	}
	wg.Wait()

	for _, batch := range fc.batches() {
		require.LessOrEqual(t, len(batch), 10)
		require.Zero(t, len(batch)%6, "batch %d is not a whole number of commands", len(batch))
	}
}

func TestBatcherClearsTrailingLinkedPerCommand(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 100, 5*time.Millisecond)
	_, err := b.Submit(context.Background(), transferCmd(3, "c"))
	require.NoError(t, err)

	batches := fc.batches()
	require.Len(t, batches, 1)
	last := batches[0][len(batches[0])-1]
	require.Zero(t, last.Flags&1, "trailing linked must be cleared")
	require.NotZero(t, batches[0][0].Flags&1, "inner linked must survive")
}

func TestBatcherRespectsMaxBatchSize(t *testing.T) {
	// Linger is deliberately longer than the test: the only path to a flush is the
	// max_batch_size threshold. Six commands of 2 events each with max=6 must yield exactly
	// two batches, each hitting the boundary precisely.
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 6, time.Hour)

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := b.Submit(context.Background(), transferCmd(2, "c"))
			// assert, not require: FailNow from a non-test goroutine is UB.
			assert.NoError(t, err)
			assert.Len(t, out, 2)
		}()
	}
	wg.Wait()

	batches := fc.batches()
	require.Len(t, batches, 2, "size threshold must have flushed exactly two batches")
	for i, batch := range batches {
		require.Len(t, batch, 6, "batch %d must fill max_batch_size exactly", i)
	}
}

func TestBatcherRoutesResultsToOwner(t *testing.T) {
	// Reject every event whose Amount == 7: this checks
	// that the outcome landed on exactly the command that event belonged to.
	fc := &fakeClient{resultsFor: func(batch []types.Transfer) []types.CreateTransferResult {
		out := make([]types.CreateTransferResult, len(batch))
		for i, tr := range batch {
			out[i].Status = types.TransferCreated
			if tr.Amount == types.ToUint128(7) {
				out[i].Status = types.TransferExceedsCredits
			}
		}
		return out
	}}
	b, _ := startBatcher(t, fc, 100, 10*time.Millisecond)

	mark := transferCmd(2, "marked")
	mark.Transfers[1].Amount = types.ToUint128(7)
	plain := transferCmd(2, "plain")

	var wg sync.WaitGroup
	var markOut, plainOut []Outcome
	var markErr, plainErr error
	wg.Add(2)
	go func() { defer wg.Done(); markOut, markErr = b.Submit(context.Background(), mark) }()
	go func() { defer wg.Done(); plainOut, plainErr = b.Submit(context.Background(), plain) }()
	wg.Wait()
	require.NoError(t, markErr)
	require.NoError(t, plainErr)

	require.Equal(t, StatusOK, markOut[0].Status)
	require.Equal(t, StatusRejected, markOut[1].Status)
	require.Equal(t, "exceeds_credits", markOut[1].Error)
	for _, o := range plainOut {
		require.Equal(t, StatusOK, o.Status)
	}
}

func TestBatcherRetriesInfraError(t *testing.T) {
	fc := &fakeClient{failTimes: 3, err: errors.New("connection refused")}
	b, _ := startBatcher(t, fc, 100, time.Millisecond)
	out, err := b.Submit(context.Background(), transferCmd(1, "c"))
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, StatusOK, out[0].Status)
	require.Len(t, fc.batches(), 1, "successful batch must be sent exactly once")
}

func TestBatcherRetriesInfraErrorOnAccounts(t *testing.T) {
	fc := &fakeClient{failTimes: 3, err: errors.New("connection refused")}
	b, _ := startBatcher(t, fc, 100, time.Millisecond)
	out, err := b.Submit(context.Background(), accountCmd(1, "a"))
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, StatusOK, out[0].Status)

	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.Len(t, fc.accountBatches, 1, "successful batch must be sent exactly once")
}

func TestBatcherRejectsOversizedCommand(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 4, time.Millisecond)
	_, err := b.Submit(context.Background(), transferCmd(5, "c"))
	require.ErrorIs(t, err, ErrCommandTooLarge)
}

func TestBatcherSubmitAfterCloseFails(t *testing.T) {
	fc := &fakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Millisecond, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)
	cancel()
	b.Close()
	_, err := b.Submit(context.Background(), transferCmd(1, "c"))
	require.ErrorIs(t, err, ErrClosed)
}

// C1: cancelling Start's context stops the loop. Submitters stuck
// on a full queue must be given a way out, otherwise Close() hangs
// and along with it the whole process shutdown.
func TestBatcherCloseReturnsWithBlockedSubmitters(t *testing.T) {
	fc := &fakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Hour, MaxQueue: 2},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)

	cancel()
	time.Sleep(50 * time.Millisecond) // let the loops exit on cancellation

	const submitters = 5
	errs := make(chan error, submitters)
	for i := 0; i < submitters; i++ {
		go func() {
			_, err := b.Submit(context.Background(), transferCmd(1, "c"))
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond) // let the submitters fill the buffer

	closed := make(chan struct{})
	go func() { b.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return after Start context cancel with blocked submitters")
	}

	for i := 0; i < submitters; i++ {
		select {
		case err := <-errs:
			require.ErrorIs(t, err, ErrClosed)
		case <-time.After(2 * time.Second):
			t.Fatal("Submit did not return after shutdown")
		}
	}
}

// C2: after Start's context is cancelled, no Submit must hang
// waiting for an outcome — otherwise the Kafka message is lost without a response.
func TestBatcherSubmitAfterContextCancelFails(t *testing.T) {
	fc := &fakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Hour, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)
	t.Cleanup(b.Close)

	cancel()
	time.Sleep(50 * time.Millisecond) // let the loops exit on cancellation

	errs := make(chan error, 1)
	go func() {
		_, err := b.Submit(context.Background(), transferCmd(1, "c"))
		errs <- err
	}()
	select {
	case err := <-errs:
		require.ErrorIs(t, err, ErrClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("Submit hung after Start context was cancelled")
	}
}

// F1: Close() during a batch that is already in flight has no right to lie to the submitter.
// TigerBeetle applied the events — so Submit must return the real outcomes,
// not ErrClosed. For a synchronous API this is the only source of truth:
// its HTTP client does not store an offset and is not obligated to repeat the request with the same id.
func TestBatcherCloseDeliversOutcomeForInFlightBatch(t *testing.T) {
	fc := &fakeClient{
		enterTransfers:   make(chan struct{}),
		releaseTransfers: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: 5 * time.Millisecond, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)

	type result struct {
		out []Outcome
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := b.Submit(context.Background(), transferCmd(2, "c"))
		done <- result{out, err}
	}()

	// The batch reached the client and got stuck inside the call.
	select {
	case <-fc.enterTransfers:
	case <-time.After(2 * time.Second):
		t.Fatal("client call never started")
	}

	closed := make(chan struct{})
	go func() { b.Close(); close(closed) }()
	time.Sleep(50 * time.Millisecond) // let Close close stop and move on to wg.Wait

	fc.releaseTransfers <- struct{}{} // TigerBeetle responded: the events were applied

	select {
	case r := <-done:
		require.NoError(t, r.err, "Submit must report the real outcome of applied work, not ErrClosed")
		require.Len(t, r.out, 2)
		for i, o := range r.out {
			require.Equal(t, StatusOK, o.Status, "outcome %d", i)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Submit hung across Close()")
	}

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return")
	}
}

// SubmitAsync exists for the sake of batching: a synchronous Submit keeps the queue
// empty, and the batch always assembles from a single command. Here max_batch_size is 10,
// linger is an hour: the only path to a flush is ten commands enqueued
// before the first one gets an outcome. A synchronous caller cannot assemble such
// a batch in principle, so the test also incidentally establishes the very fact of
// non-blocking submission.
func TestBatcherSubmitAsyncBatchesInSubmitOrder(t *testing.T) {
	const n = 10
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, n, time.Hour)

	chans := make([]<-chan SubmitResult, n)
	for i := 0; i < n; i++ {
		ch, err := b.SubmitAsync(context.Background(), markedTransferCmd(uint64(i+1), 1))
		require.NoError(t, err)
		chans[i] = ch
	}

	for i, ch := range chans {
		select {
		case res := <-ch:
			require.NoError(t, res.Err, "command %d", i)
			require.Len(t, res.Outcomes, 1)
			require.Equal(t, "c"+strconv.Itoa(i+1)+"-0", res.Outcomes[0].ID,
				"outcome %d belongs to another command", i)
			require.Equal(t, StatusOK, res.Outcomes[0].Status)
		case <-time.After(2 * time.Second):
			t.Fatalf("no outcome for command %d", i)
		}
	}

	batches := fc.batches()
	require.Len(t, batches, 1, "ten async commands must be assembled into a single batch")
	require.Len(t, batches[0], n)
	for i, ev := range batches[0] {
		require.Equal(t, types.ToUint128(uint64(i+1)), ev.UserData128,
			"event %d is out of submit order", i)
	}
}

// Backpressure: a full queue must block submission, otherwise the sink
// would enqueue an entire poll and eat up memory.
func TestBatcherSubmitAsyncBlocksWhenQueueIsFull(t *testing.T) {
	fc := &fakeClient{
		enterTransfers:   make(chan struct{}),
		releaseTransfers: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	// max_batch_size=1: each command flushes immediately, so the loop gets stuck
	// inside the client call and the queue stops draining.
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 1, Linger: time.Hour, MaxQueue: 2},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)

	pumpDone := make(chan struct{})
	defer func() {
		cancel()
		b.Close()
		close(pumpDone)
	}()

	_, err := b.SubmitAsync(context.Background(), transferCmd(1, "c0"))
	require.NoError(t, err)
	select {
	case <-fc.enterTransfers: // the loop is inside the client call and not reading the queue
	case <-time.After(2 * time.Second):
		t.Fatal("client call never started")
	}

	// Exactly MaxQueue commands fit into the buffer and return immediately.
	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() {
			_, aerr := b.SubmitAsync(context.Background(), transferCmd(1, "c"))
			done <- aerr
		}()
		select {
		case aerr := <-done:
			require.NoError(t, aerr)
		case <-time.After(2 * time.Second):
			t.Fatalf("SubmitAsync %d blocked although the queue had room", i)
		}
	}

	blocked := make(chan error, 1)
	go func() {
		_, aerr := b.SubmitAsync(context.Background(), transferCmd(1, "full"))
		blocked <- aerr
	}()
	select {
	case <-blocked:
		t.Fatal("SubmitAsync returned although the queue was full: backpressure is gone")
	case <-time.After(200 * time.Millisecond):
	}

	// Release the client: the loop picks up the next command, freeing a slot.
	go func() {
		for {
			select {
			case <-fc.enterTransfers:
				select {
				case fc.releaseTransfers <- struct{}{}:
				case <-pumpDone:
					return
				}
			case <-pumpDone:
				return
			}
		}
	}()
	fc.releaseTransfers <- struct{}{}

	select {
	case aerr := <-blocked:
		require.NoError(t, aerr)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitAsync stayed blocked after the queue drained")
	}
}

// A caller is entitled to abandon the channel without reading it: there is exactly one writer, and the channel
// is buffered, so the loop has no right to stall.
func TestBatcherSubmitAsyncAbandonedChannelDoesNotWedgeLoop(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 1, time.Hour)

	for i := 0; i < 20; i++ {
		_, err := b.SubmitAsync(context.Background(), transferCmd(1, "abandoned"))
		require.NoError(t, err)
	}

	done := make(chan SubmitResult, 1)
	go func() {
		out, err := b.Submit(context.Background(), transferCmd(1, "last"))
		done <- SubmitResult{Outcomes: out, Err: err}
	}()
	select {
	case res := <-done:
		require.NoError(t, res.Err)
		require.Len(t, res.Outcomes, 1)
	case <-time.After(2 * time.Second):
		t.Fatal("loop wedged after callers abandoned their result channels")
	}
	require.Len(t, fc.batches(), 21, "every abandoned command must still have been sent")
}

func TestBatcherSubmitAsyncRejectsBadCommands(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 4, time.Millisecond)

	ch, err := b.SubmitAsync(context.Background(), &model.Command{Op: model.OpCreateTransfers})
	require.EqualError(t, err, "empty command")
	require.Nil(t, ch)

	ch, err = b.SubmitAsync(context.Background(), transferCmd(5, "c"))
	require.ErrorIs(t, err, ErrCommandTooLarge)
	require.Nil(t, ch)
}

func TestBatcherSubmitAsyncAfterCloseFails(t *testing.T) {
	fc := &fakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Millisecond, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)
	cancel()
	b.Close()

	ch, err := b.SubmitAsync(context.Background(), transferCmd(1, "c"))
	require.ErrorIs(t, err, ErrClosed)
	require.Nil(t, ch)
}

// A cancelled context must release a submitter stuck against a full
// queue. No loops are running — nothing drains the queue — so the outcome
// of the race here is unambiguous.
func TestBatcherSubmitAsyncCancelledContextOnFullQueue(t *testing.T) {
	fc := &fakeClient{}
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Hour, MaxQueue: 1},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	defer b.Close()

	_, err := b.SubmitAsync(context.Background(), transferCmd(1, "queued"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch, err := b.SubmitAsync(ctx, transferCmd(1, "cancelled"))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ch)
}

// A command submitted so late that no loop will ever see it must
// get ErrClosed. Silence on the channel is a lost Kafka message.
// There are no loops here at all: Start was never called, there is no one to respond by construction.
func TestBatcherSubmitAsyncUnseenCommandGetsErrClosed(t *testing.T) {
	fc := &fakeClient{}
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Hour, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)

	ch, err := b.SubmitAsync(context.Background(), transferCmd(1, "unseen"))
	require.NoError(t, err)
	b.Close()

	select {
	case res := <-ch:
		require.ErrorIs(t, res.Err, ErrClosed)
		require.Nil(t, res.Outcomes)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitAsync went silent for a command no loop could see")
	}
}

// Async variant of F1: Close during an in-flight batch has no right to
// substitute ErrClosed for the real outcome. The race "the outcome is delivered exactly at
// the moment the loops exit" is caught right here.
func TestBatcherSubmitAsyncCloseDeliversOutcomeForInFlightBatch(t *testing.T) {
	fc := &fakeClient{
		enterTransfers:   make(chan struct{}),
		releaseTransfers: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: 5 * time.Millisecond, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)

	ch, err := b.SubmitAsync(context.Background(), transferCmd(2, "c"))
	require.NoError(t, err)

	select {
	case <-fc.enterTransfers:
	case <-time.After(2 * time.Second):
		t.Fatal("client call never started")
	}

	closed := make(chan struct{})
	go func() { b.Close(); close(closed) }()
	time.Sleep(50 * time.Millisecond) // let Close close stop and move on to wg.Wait

	fc.releaseTransfers <- struct{}{} // TigerBeetle responded: the events were applied

	select {
	case res := <-ch:
		require.NoError(t, res.Err, "applied work must not be reported as ErrClosed")
		require.Len(t, res.Outcomes, 2)
		for i, o := range res.Outcomes {
			require.Equal(t, StatusOK, o.Status, "outcome %d", i)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no outcome delivered across Close()")
	}

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return")
	}
}

// SubmitAsync hands the caller j.done itself, so "every command receives exactly one
// result" rests on the claim that a command which slips past stop is still seen by a
// drain. This is that race, run wide: hundreds of submissions crossing a shutdown.
// Silence is a lost Kafka message and two answers would mean two writers on one
// channel; ErrClosed and a real outcome are both correct answers here, and which one
// a given submitter gets is genuinely undecided.
func TestBatcherEverySubmissionAnsweredAcrossShutdown(t *testing.T) {
	const submitters = 200
	fc := &fakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Millisecond, MaxQueue: 16},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)

	const (
		refused    = -1
		unanswered = 0
		once       = 1
		twice      = 2
	)
	answers := make(chan int, submitters)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ch, err := b.SubmitAsync(context.Background(), transferCmd(1, "race"))
			if err != nil {
				// Refusal at submission is an answer too: nothing was enqueued.
				answers <- refused
				return
			}
			select {
			case <-ch:
			case <-time.After(10 * time.Second):
				answers <- unanswered
				return
			}
			select {
			case <-ch:
				answers <- twice
			case <-time.After(100 * time.Millisecond):
				answers <- once
			}
		}()
	}
	close(start)
	// Long enough for some submissions to be applied, short enough that the rest are
	// still in flight when stop closes: the seam is the point of the test.
	time.Sleep(2 * time.Millisecond)
	cancel()
	b.Close()

	wg.Wait()
	close(answers)
	for a := range answers {
		require.NotEqual(t, unanswered, a, "a command on the queue never got a result")
		require.NotEqual(t, twice, a, "a command got two results")
	}
}

func TestBatcherAccountsGoToSeparateBatches(t *testing.T) {
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 100, 5*time.Millisecond)
	acc := &model.Command{Op: model.OpCreateAccounts, Accounts: make([]types.Account, 2), IDs: []string{"a", "b"}}
	_, err := b.Submit(context.Background(), acc)
	require.NoError(t, err)
	_, err = b.Submit(context.Background(), transferCmd(1, "c"))
	require.NoError(t, err)

	fc.mu.Lock()
	defer fc.mu.Unlock()
	require.Len(t, fc.accountBatches, 1)
	require.Len(t, fc.transferBatches, 1)
}

// TestBatcherRecordsMetrics verifies sendTransfers/sendAccounts actually
// observe BatchSize and TBLatency on a real registry, not just that the
// constructor accepts a *obs.Metrics argument.
func TestBatcherRecordsMetrics(t *testing.T) {
	fc := &fakeClient{}
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg)
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: 5 * time.Millisecond, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), m)
	b.Start(ctx)
	t.Cleanup(func() { cancel(); b.Close() })

	_, err := b.Submit(context.Background(), transferCmd(2, "t"))
	require.NoError(t, err)
	_, err = b.Submit(context.Background(), accountCmd(1, "a"))
	require.NoError(t, err)

	var batchSize dto.Metric
	require.NoError(t, m.BatchSize.Write(&batchSize))
	require.Equal(t, uint64(2), batchSize.GetHistogram().GetSampleCount(), "expected two batches observed")

	var transferLatency, accountLatency dto.Metric
	require.NoError(t, m.TBLatency.WithLabelValues(string(model.OpCreateTransfers)).(prometheus.Metric).Write(&transferLatency))
	require.NoError(t, m.TBLatency.WithLabelValues(string(model.OpCreateAccounts)).(prometheus.Metric).Write(&accountLatency))
	require.Equal(t, uint64(1), transferLatency.GetHistogram().GetSampleCount())
	require.Equal(t, uint64(1), accountLatency.GetHistogram().GetSampleCount())
}
