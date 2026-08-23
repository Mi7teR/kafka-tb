package sink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type stubDecoder struct {
	cmd *model.Command
	err error
}

func (s stubDecoder) Decode([]byte) (*model.Command, error) { return s.cmd, s.err }

// result returns a channel that already holds res: SubmitAsync's contract is exactly
// one SubmitResult per accepted command, and all the stubs below honor it.
func result(res tbx.SubmitResult) <-chan tbx.SubmitResult {
	ch := make(chan tbx.SubmitResult, 1)
	ch <- res
	return ch
}

type stubSubmitter struct {
	outcomes []tbx.Outcome
	err      error
	calls    int
}

func (s *stubSubmitter) SubmitAsync(context.Context, *model.Command) (<-chan tbx.SubmitResult, error) {
	s.calls++
	return result(tbx.SubmitResult{Outcomes: s.outcomes, Err: s.err}), nil
}

// scriptedSubmitter returns a preset result on each call;
// the last element of errs repeats. It is needed to distinguish "failed and
// recovered" from "always fails". A schedule by call number only makes sense
// for a single partition — for several there is idSubmitter.
type scriptedSubmitter struct {
	outcomes []tbx.Outcome
	errs     []error
	calls    int
}

func (s *scriptedSubmitter) SubmitAsync(context.Context, *model.Command) (<-chan tbx.SubmitResult, error) {
	err := s.errs[min(s.calls, len(s.errs)-1)]
	s.calls++
	if err != nil {
		return result(tbx.SubmitResult{Err: err}), nil
	}
	return result(tbx.SubmitResult{Outcomes: s.outcomes}), nil
}

// slowSubmitter is always successful, but hands out the outcome only after delay — this is what
// a round-trip to TigerBeetle looks like. It is needed to model a batch with no
// single infrastructure error that is nevertheless collectively slow:
// the budget must catch this case too, not only an endless retry.
type slowSubmitter struct {
	delay    time.Duration
	outcomes []tbx.Outcome
	calls    int
}

func (s *slowSubmitter) SubmitAsync(context.Context, *model.Command) (<-chan tbx.SubmitResult, error) {
	s.calls++
	ch := make(chan tbx.SubmitResult, 1)
	outcomes := s.outcomes
	delay := s.delay
	go func() {
		time.Sleep(delay)
		ch <- tbx.SubmitResult{Outcomes: outcomes}
	}()
	return ch, nil
}

// idDecoder makes a command out of the record's bytes whose sole id is those bytes.
// It is needed as soon as partitions run in parallel: a scenario scripted by
// call number stops being deterministic once two partitions submit
// commands at the same time — the schedule has to be keyed by the record itself.
type idDecoder struct{}

func (idDecoder) Decode(v []byte) (*model.Command, error) {
	return &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: []types.Transfer{{ID: types.ToUint128(1)}},
		IDs:       []string{string(v)},
	}, nil
}

// mixedDecoder is the same as idDecoder, but declares the listed ids poison.
type mixedDecoder struct{ poison map[string]bool }

func (d mixedDecoder) Decode(v []byte) (*model.Command, error) {
	if d.poison[string(v)] {
		return nil, codec.Poison("bad json")
	}
	return idDecoder{}.Decode(v)
}

// idSubmitter responds with success only for the ids listed in ok, and with a permanent
// infrastructure error for everything else, and it also records the submission
// order — that order is precisely what dictates the application order in TigerBeetle.
type idSubmitter struct {
	mu    sync.Mutex
	ok    map[string]bool
	order []string
}

func (s *idSubmitter) SubmitAsync(_ context.Context, cmd *model.Command) (<-chan tbx.SubmitResult, error) {
	id := cmd.IDs[0]
	s.mu.Lock()
	s.order = append(s.order, id)
	ok := s.ok[id]
	s.mu.Unlock()
	if !ok {
		return result(tbx.SubmitResult{Err: errors.New("tigerbeetle unavailable")}), nil
	}
	return result(tbx.SubmitResult{
		Outcomes: []tbx.Outcome{{Index: 0, ID: id, Status: tbx.StatusOK}},
	}), nil
}

func (s *idSubmitter) submitted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

// barrierSubmitter responds only after n calls have entered it
// simultaneously. With sequential partition processing that will never happen.
type barrierSubmitter struct {
	mu    sync.Mutex
	seen  int
	n     int
	ready chan struct{}
}

func newBarrierSubmitter(n int) *barrierSubmitter {
	return &barrierSubmitter{n: n, ready: make(chan struct{})}
}

func (b *barrierSubmitter) SubmitAsync(
	ctx context.Context, cmd *model.Command,
) (<-chan tbx.SubmitResult, error) {
	b.mu.Lock()
	b.seen++
	if b.seen == b.n {
		close(b.ready)
	}
	b.mu.Unlock()
	select {
	case <-b.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return result(tbx.SubmitResult{
		Outcomes: []tbx.Outcome{{Index: 0, ID: cmd.IDs[0], Status: tbx.StatusOK}},
	}), nil
}

// blockingSubmitter accepts the command and never responds: this is what
// TigerBeetle looks like once it has stopped responding after submission.
type blockingSubmitter struct {
	mu    sync.Mutex
	calls int
}

func (b *blockingSubmitter) SubmitAsync(context.Context, *model.Command) (<-chan tbx.SubmitResult, error) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	return make(chan tbx.SubmitResult), nil
}

func (b *blockingSubmitter) Calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// enqueueBlockingSubmitter parks on submission itself and releases only on
// context cancellation: this is what a batcher looks like whose queue is jammed because
// TigerBeetle has stopped responding and retries never drain it.
type enqueueBlockingSubmitter struct {
	mu    sync.Mutex
	calls int
}

func (b *enqueueBlockingSubmitter) SubmitAsync(
	ctx context.Context, _ *model.Command,
) (<-chan tbx.SubmitResult, error) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *enqueueBlockingSubmitter) Calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// refusingSubmitter refuses submission for a command with a listed id and
// records everything that reached it up to then: only this log reveals whether
// the partition's next record went to TigerBeetle before the failed one.
type refusingSubmitter struct {
	mu     sync.Mutex
	refuse map[string]bool
	order  []string
}

func (s *refusingSubmitter) SubmitAsync(_ context.Context, cmd *model.Command) (<-chan tbx.SubmitResult, error) {
	id := cmd.IDs[0]
	s.mu.Lock()
	s.order = append(s.order, id)
	refuse := s.refuse[id]
	s.mu.Unlock()
	if refuse {
		return nil, errors.New("enqueue refused")
	}
	return result(tbx.SubmitResult{
		Outcomes: []tbx.Outcome{{Index: 0, ID: id, Status: tbx.StatusOK}},
	}), nil
}

func (s *refusingSubmitter) submitted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

// cancellingSubmitter responds to the first after records immediately, and starting
// with the next one cancels the context and goes silent — this is what SIGTERM
// looks like in the middle of a poll.
type cancellingSubmitter struct {
	mu     sync.Mutex
	seen   int
	after  int
	cancel context.CancelFunc
}

func (g *cancellingSubmitter) SubmitAsync(_ context.Context, cmd *model.Command) (<-chan tbx.SubmitResult, error) {
	g.mu.Lock()
	g.seen++
	n := g.seen
	g.mu.Unlock()
	if n > g.after {
		g.cancel()
		return make(chan tbx.SubmitResult), nil
	}
	return result(tbx.SubmitResult{
		Outcomes: []tbx.Outcome{{Index: 0, ID: cmd.IDs[0], Status: tbx.StatusOK}},
	}), nil
}

// recordingEmitter can fail selectively: a DLQ failure and a Results failure are
// different branches of finish, and they cannot be verified with one shared switch.
// The mutex is mandatory: partitions publish in parallel.
type recordingEmitter struct {
	mu      sync.Mutex
	dlq     []dlqCall
	results int
	// published is a cross-cutting log of publications, both results and DLQ, with the
	// source record's offset. Publication order within a partition must be
	// offset order, and this is the only log that can verify that.
	published   []publication
	failDLQ     error
	failResults error
	// failResultsAt fails a results publication for exactly the listed
	// offsets: one record's failure must not be allowed to drag the batch down with it,
	// and a single shared switch cannot verify that.
	failResultsAt map[int64]error
	// flushErr is what Flush responds with; a commit must react to this.
	flushErr error
}

type dlqCall struct {
	reason  string
	errName string
}

type publication struct {
	kind      string
	partition int32
	offset    int64
}

func (r *recordingEmitter) DLQ(
	_ context.Context, rec *kgo.Record, reason emitReason, errName, _ string,
) *emit.Publication {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failDLQ != nil {
		return emit.Resolved(r.failDLQ)
	}
	r.dlq = append(r.dlq, dlqCall{reason: string(reason), errName: errName})
	r.published = append(r.published,
		publication{kind: "dlq", partition: rec.Partition, offset: rec.Offset})
	return emit.Resolved(nil)
}

func (r *recordingEmitter) Results(_ context.Context, rec *kgo.Record, _ []tbx.Outcome) *emit.Publication {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failResults != nil {
		return emit.Resolved(r.failResults)
	}
	if err, ok := r.failResultsAt[rec.Offset]; ok {
		return emit.Resolved(err)
	}
	r.results++
	r.published = append(r.published,
		publication{kind: "results", partition: rec.Partition, offset: rec.Offset})
	return emit.Resolved(nil)
}
func (r *recordingEmitter) Flush(context.Context) error { return r.flushErr }
func (r *recordingEmitter) Close()                      {}

func (r *recordingEmitter) resultsCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.results
}

// publishedFor returns one partition's publication log: between partitions
// order is not guaranteed and never was.
func (r *recordingEmitter) publishedFor(partition int32) []publication {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []publication
	for _, p := range r.published {
		if p.partition == partition {
			out = append(out, p)
		}
	}
	return out
}

// panicOnDLQEmitter panics on every DLQ publication — including the one
// finish issues in response to a panic. This lets the panic escape prepare's and
// finish's recovers and reach the partition goroutine itself.
type panicOnDLQEmitter struct{}

func (panicOnDLQEmitter) DLQ(context.Context, *kgo.Record, emitReason, string, string) *emit.Publication {
	panic("dlq exploded")
}
func (panicOnDLQEmitter) Results(context.Context, *kgo.Record, []tbx.Outcome) *emit.Publication {
	return emit.Resolved(nil)
}
func (panicOnDLQEmitter) Flush(context.Context) error { return nil }
func (panicOnDLQEmitter) Close()                      {}

var _ emitterIface = panicOnDLQEmitter{}

// resultsThenPanicOnceEmitter issues a real, controllable Results publication
// and panics on its first DLQ call — modelling a panic in finish after
// Results has already been issued to the broker but before the reject DLQ
// publication is. Every DLQ call after the first (the panic-recovery's own
// poison publication) behaves like deferredEmitter. Used to prove the
// already-issued Results publication is not orphaned by the recovery.
type resultsThenPanicOnceEmitter struct {
	deferredEmitter
	dlqCalls int
}

func (e *resultsThenPanicOnceEmitter) DLQ(
	ctx context.Context, rec *kgo.Record, reason emitReason, errName, detail string,
) *emit.Publication {
	e.dlqCalls++
	if e.dlqCalls == 1 {
		panic("dlq exploded")
	}
	return e.deferredEmitter.DLQ(ctx, rec, reason, errName, detail)
}

var _ emitterIface = (*resultsThenPanicOnceEmitter)(nil)

// deferredEmitter publishes without responding: promises accumulate, and
// release completes them — in any order. This is what real asynchronous publication looks
// like, where the broker responds later and not necessarily in hand-off order. Without such
// a stub there is nothing to verify "Done only after acknowledgment" with: for an already-resolved
// promise, acknowledgment and hand-off are indistinguishable.
type deferredEmitter struct {
	mu        sync.Mutex
	published []publication
	resolve   []func(error)
	// rejectAt holds offsets whose publication the broker will reject.
	rejectAt map[int64]error
}

func (d *deferredEmitter) pend(kind string, rec *kgo.Record) *emit.Publication {
	p, resolve := emit.NewTestPublication()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.published = append(d.published,
		publication{kind: kind, partition: rec.Partition, offset: rec.Offset})
	err := d.rejectAt[rec.Offset]
	d.resolve = append(d.resolve, func(error) { resolve(err) })
	return p
}

func (d *deferredEmitter) DLQ(
	_ context.Context, rec *kgo.Record, _ emitReason, _, _ string,
) *emit.Publication {
	return d.pend("dlq", rec)
}

func (d *deferredEmitter) Results(_ context.Context, rec *kgo.Record, _ []tbx.Outcome) *emit.Publication {
	return d.pend("results", rec)
}
func (d *deferredEmitter) Flush(context.Context) error { return nil }
func (d *deferredEmitter) Close()                      {}

// releaseAll completes all accumulated promises in reverse hand-off order:
// the broker's response order need not match publication order, and the sink
// must remain correct under exactly that kind of mismatch.
func (d *deferredEmitter) releaseAll() int {
	d.mu.Lock()
	pending := d.resolve
	d.resolve = nil
	d.mu.Unlock()
	for i := len(pending) - 1; i >= 0; i-- {
		pending[i](nil)
	}
	return len(pending)
}

// releaseKind resolves only the accumulated promises whose publication kind
// matches, leaving the rest pending. Needed to prove that acknowledging one
// of a record's publications alone is not enough to complete it while
// another is still outstanding — releaseAll can't distinguish that from
// "everything was awaited". Relies on d.resolve and d.published growing in
// lockstep in pend(), which holds as long as this is the only kind of
// partial release used against a given deferredEmitter.
func (d *deferredEmitter) releaseKind(kind string) int {
	d.mu.Lock()
	var toResolve []func(error)
	remaining := d.resolve[:0:0]
	for i, resolve := range d.resolve {
		if d.published[i].kind == kind {
			toResolve = append(toResolve, resolve)
			continue
		}
		remaining = append(remaining, resolve)
	}
	d.resolve = remaining
	d.mu.Unlock()
	for _, resolve := range toResolve {
		resolve(nil)
	}
	return len(toResolve)
}

func (d *deferredEmitter) publishedFor(partition int32) []publication {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []publication
	for _, p := range d.published {
		if p.partition == partition {
			out = append(out, p)
		}
	}
	return out
}

var _ emitterIface = (*deferredEmitter)(nil)

// stubClient substitutes for *kgo.Client in the two calls the sink uses to move
// offsets. Without it, processBatch/commit/OnRevoked cannot be run.
type stubClient struct {
	committed  []map[string]map[int32]kgo.EpochOffset
	setOffsets []map[string]map[int32]kgo.EpochOffset
	commitErr  error
	partErr    int16
}

func (c *stubClient) CommitOffsetsSync(
	_ context.Context,
	offsets map[string]map[int32]kgo.EpochOffset,
	onDone func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
) {
	c.committed = append(c.committed, offsets)
	if c.commitErr != nil {
		onDone(nil, nil, nil, c.commitErr)
		return
	}
	resp := &kmsg.OffsetCommitResponse{}
	for topic, parts := range offsets {
		rt := kmsg.OffsetCommitResponseTopic{Topic: topic}
		for p := range parts {
			rt.Partitions = append(rt.Partitions,
				kmsg.OffsetCommitResponseTopicPartition{Partition: p, ErrorCode: c.partErr})
		}
		resp.Topics = append(resp.Topics, rt)
	}
	onDone(nil, nil, resp, nil)
}

func (c *stubClient) SetOffsets(m map[string]map[int32]kgo.EpochOffset) {
	c.setOffsets = append(c.setOffsets, m)
}

var _ offsetClient = (*stubClient)(nil)

// The compiler must confirm that the stub implements the real emit.Emitter:
// otherwise tests would be green against a signature that does not exist in production.
var _ emitterIface = (*recordingEmitter)(nil)

func oneTransferCmd() *model.Command {
	return &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: []types.Transfer{{ID: types.ToUint128(1)}},
		IDs:       []string{"id-0"},
	}
}

func newSink(t *testing.T, d codec.Decoder, sub Submitter, em emitterIface) *Sink {
	t.Helper()
	s, err := newForTest(codec.Registry{"src": d}, sub, em, &stubClient{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	return s
}

func clientOf(t *testing.T, s *Sink) *stubClient {
	t.Helper()
	c, ok := s.oc.(*stubClient)
	require.True(t, ok)
	return c
}

// recID names a record so the stub batcher can tell one apart from another.
func recID(partition int32, offset int64) string {
	return fmt.Sprintf("%d/%d", partition, offset)
}

func srcRec(partition int32, offset int64) *kgo.Record {
	return &kgo.Record{
		Topic: "src", Partition: partition, Offset: offset, LeaderEpoch: 3,
		Value: []byte(recID(partition, offset)),
	}
}

// handleOne drives one record through the sink's real path — submission,
// waiting for the outcome, publication — and returns the same thing single-record
// handling used to return before pipelining: whether it was driven to completion, and with what error.
func handleOne(t *testing.T, s *Sink, rec *kgo.Record) (bool, error) {
	t.Helper()
	applied, err := s.pass(context.Background(), []*kgo.Record{rec}, time.Now().Add(time.Minute))
	return applied == 1, err
}

func TestHandlePoisonGoesToDLQ(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{}
	s := newSink(t, stubDecoder{err: codec.Poison("bad json")}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src", Value: []byte("x")})
	require.NoError(t, err)
	require.True(t, done, "poison record must be marked done")
	require.Len(t, em.dlq, 1)
	require.Equal(t, "poison", em.dlq[0].reason)
	require.Zero(t, sub.calls, "poison must never reach TigerBeetle")
}

func TestHandleRejectGoesToDLQAndResults(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
	require.Equal(t, "reject", em.dlq[0].reason)
	require.Equal(t, "exceeds_credits", em.dlq[0].errName)
	require.Equal(t, 1, em.results)
}

func TestHandleSuccessEmitsResultsOnly(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Empty(t, em.dlq)
	require.Equal(t, 1, em.results)
}

// An infrastructure error does not allow the offset to move.
func TestHandleInfraErrorBlocks(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{err: errors.New("tigerbeetle unavailable")}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done)
	require.Empty(t, em.dlq)
}

// A command bigger than the batch — a retry will not fix it: this is poison.
func TestHandleCommandTooLargeGoesToDLQ(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{err: tbx.ErrCommandTooLarge}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
	require.Equal(t, "poison", em.dlq[0].reason)
	require.Equal(t, "command_too_large", em.dlq[0].errName)
	require.Zero(t, em.results)
}

// If the DLQ write fails — this is also infrastructure, the offset stays put.
func TestHandleDLQFailureBlocks(t *testing.T) {
	em := &recordingEmitter{failDLQ: errors.New("broker down")}
	s := newSink(t, stubDecoder{err: codec.Poison("bad json")}, &stubSubmitter{}, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done, "must not commit offset if DLQ write failed")
}

// A panic during processing does not crash the process.
func TestHandleRecoversPanic(t *testing.T) {
	em := &recordingEmitter{}
	s := newSink(t, panicDecoder{}, &stubSubmitter{}, em)
	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
	require.Equal(t, "poison", em.dlq[0].reason)
	require.Equal(t, "panic", em.dlq[0].errName)
}

// A panic combined with a live infrastructure problem in the DLQ must not commit the offset.
func TestHandlePanicWithDLQFailureBlocks(t *testing.T) {
	em := &recordingEmitter{failDLQ: errors.New("broker down")}
	s := newSink(t, panicDecoder{}, &stubSubmitter{}, em)
	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done)
}

// capturedLog is one record captured by captureHandler.
type capturedLog struct {
	level slog.Level
	msg   string
}

// captureHandler is a minimal slog.Handler that records level+message pairs
// so a test can assert on what was logged, and at what level.
type captureHandler struct {
	mu   sync.Mutex
	logs *[]capturedLog
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.logs = append(*h.logs, capturedLog{level: r.Level, msg: r.Message})
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// P1: a cancelled context is a graceful shutdown, not a failure. backoff
// returns false immediately on a cancelled context, so there is no retry —
// logging "record failed, retrying" at ERROR would page someone for a clean
// shutdown. This pins the fix: no ERROR-level record-failure log when the
// context is already cancelled.
func TestRunPartitionCancelledContextDoesNotLogError(t *testing.T) {
	var logs []capturedLog
	em := &recordingEmitter{}
	sub := &stubSubmitter{err: errors.New("tigerbeetle unavailable")}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.log = slog.New(&captureHandler{logs: &logs})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := srcRec(0, 0)
	ok := s.runPartition(ctx, []*kgo.Record{rec}, time.Now().Add(time.Minute))

	require.False(t, ok, "a cancelled context must abandon the record, not commit it")
	require.NotEmpty(t, logs, "expected a shutdown log explaining the uncommitted record")
	for _, l := range logs {
		require.NotEqual(t, slog.LevelError, l.level,
			"cancelled context must not log at ERROR: %q", l.msg)
	}
}

type panicDecoder struct{}

func (panicDecoder) Decode([]byte) (*model.Command, error) { panic("boom") }

// An unknown topic is a configuration error, but the process must not be killed over
// a single message: write it to the DLQ.
func TestHandleUnknownTopic(t *testing.T) {
	em := &recordingEmitter{}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, em)
	done, err := handleOne(t, s, &kgo.Record{Topic: "other"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
	require.Equal(t, "unknown_topic", em.dlq[0].errName)
}

// codec.Decoder's contract: any decoding error is poison. An error that is not a
// PoisonError must not go into an infinite retry.
func TestHandleAnyDecodeErrorIsPoison(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{}
	s := newSink(t, stubDecoder{err: errors.New("plain decode failure")}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
	require.Equal(t, "poison", em.dlq[0].reason)
	require.Equal(t, "decode", em.dlq[0].errName)
	require.Zero(t, sub.calls)
}

// A record's results-publication failure is infrastructure: the offset stays put, the DLQ is not written.
func TestHandleResultsFailureBlocks(t *testing.T) {
	em := &recordingEmitter{failResults: errors.New("results topic down")}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done, "offset must not move if results were not published")
	require.Empty(t, em.dlq)
}

// A reject is published in results but does not make it to the DLQ — the offset still stays put.
func TestHandleDLQFailureInsideRejectLoopBlocks(t *testing.T) {
	em := &recordingEmitter{failDLQ: errors.New("dlq down")}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done)
	require.Equal(t, 1, em.results, "results already published — this branch is for the reject-cycle")
}

// TestHandleMetricsByOutcome verifies handle actually increments RecordsTotal
// and DLQTotal on a real registry for each outcome class, not just that
// *obs.Metrics can be plumbed through. s.metrics is set directly (this test
// is in-package) rather than through New/newForTest, so every other test in
// this file — which never touches s.metrics — keeps running against a nil
// *obs.Metrics exactly as before.
func TestHandleMetricsByOutcome(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg)

	em := &recordingEmitter{}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusOK},
	}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.metrics = m
	_, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("ok")))

	sub = &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s = newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, &recordingEmitter{})
	s.metrics = m
	_, err = handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("rejected")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.DLQTotal.WithLabelValues("reject", "exceeds_credits")))

	s = newSink(t, stubDecoder{err: codec.Poison("bad json")}, &stubSubmitter{}, &recordingEmitter{})
	s.metrics = m
	_, err = handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("poison")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.DLQTotal.WithLabelValues("poison", "decode")))

	s = newSink(t, stubDecoder{cmd: oneTransferCmd()},
		&stubSubmitter{err: errors.New("tigerbeetle unavailable")}, &recordingEmitter{})
	s.metrics = m
	_, err = handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("blocked")))
}

// I1: a Results-write failure is an infrastructure error like any other
// early return from handle — it must count as blocked, not as ok, and the
// record must not be counted twice when it is retried later.
func TestHandleResultsFailureIncrementsBlockedOnceNotOK(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg)

	em := &recordingEmitter{failResults: errors.New("results topic down")}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.metrics = m

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done)
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("blocked")))
	require.Zero(t, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("ok")))
}

// I1: a record that fails once (infrastructure error) and succeeds on the
// retry must count "ok" exactly once in total, not once per attempt —
// counting has to happen at the point handling is actually final, which for
// a retried record is only the last, successful call to handle.
func TestProcessBatchRetriedRecordCountsOKOnce(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg)

	em := &recordingEmitter{}
	sub := &scriptedSubmitter{
		outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}},
		errs:     []error{errors.New("tigerbeetle unavailable"), nil},
	}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.metrics = m
	s.retryPeriod = time.Millisecond

	s.processBatch(context.Background(), []*kgo.Record{srcRec(0, 0)})

	require.Equal(t, 2, sub.calls, "record must have been retried")
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("ok")),
		"ok must be counted exactly once total, not once per attempt")
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("blocked")))
}

func TestProcessBatchMarksEveryRecordDone(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	s.processBatch(context.Background(), []*kgo.Record{srcRec(0, 0), srcRec(0, 1)})

	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(2), s.offsets.Commitable()["src"][0].Offset)
	require.Empty(t, clientOf(t, s).setOffsets, "successful batch must not be rewound")
}

// C1: the failed record itself is retried, it is not skipped in favor of the next one.
func TestProcessBatchRetriesSameRecord(t *testing.T) {
	em := &recordingEmitter{}
	sub := &scriptedSubmitter{
		outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}},
		errs:     []error{errors.New("tigerbeetle unavailable"), nil},
	}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.retryPeriod = time.Millisecond

	rec := srcRec(0, 7)
	s.processBatch(context.Background(), []*kgo.Record{rec})

	require.Equal(t, 2, sub.calls, "same record must be retried")
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(8), s.offsets.Commitable()["src"][0].Offset)
	require.Empty(t, em.dlq, "infrastructure failure is not a reason for DLQ")
	require.Equal(t, 1, em.results, "result must be published exactly once")
}

// C2: a permanent infrastructure error does not hold the rebalance longer than the budget —
// the batch is abandoned, and its partitions are rewound back and forgotten.
func TestProcessBatchAbandonsAndRewindsOnBudget(t *testing.T) {
	em := &recordingEmitter{}
	// Scheduled by record, not by call number: partitions now run
	// in parallel, and "the first call succeeds" is no longer deterministic.
	sub := &idSubmitter{ok: map[string]bool{recID(0, 10): true}}
	s := newSink(t, idDecoder{}, sub, em)
	s.retryPeriod = time.Millisecond
	s.batchBudget = 20 * time.Millisecond

	records := []*kgo.Record{srcRec(0, 10), srcRec(0, 11), srcRec(1, 4)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(context.Background(), records)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processBatch did not fit in budget — retry is not limited")
	}

	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "failed batch must be rewound")
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {
			0: {Epoch: 3, Offset: 11}, // 10 was processed and marked Done with epoch 3
			1: {Epoch: -1, Offset: 4}, // never reached at all — offset 3 is not Done, sentinel
		},
	}, cl.setOffsets[0])
	require.Zero(t, s.offsets.InFlight(), "failed partitions are forgotten")
	require.Empty(t, s.offsets.Commitable(), "forgotten partition must not commit anything")
}

// C4: a batch with no single infrastructure error — every record succeeds, but
// slowly — must be bounded by the budget just as much as a batch stuck on
// retries: the budget is checked at the start of every runPartition iteration, not only
// on a particular record's outcome.
func TestProcessBatchAbandonsSlowSuccessfulBatch(t *testing.T) {
	em := &recordingEmitter{}
	sub := &slowSubmitter{
		delay:    20 * time.Millisecond,
		outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}},
	}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.batchBudget = 50 * time.Millisecond
	// One record in flight: without this, pipelining turns this batch into a
	// fast one (all five round-trips run in parallel) and there is nothing for the budget to
	// catch. It is specifically the budget being verified here, not pipelining — the fact that
	// the budget also applies to records in flight is the responsibility of
	// TestProcessBatchAbandonsWhileWaitingForOutcomes.
	s.maxInFlight = 1

	records := []*kgo.Record{srcRec(0, 0), srcRec(0, 1), srcRec(0, 2), srcRec(0, 3), srcRec(0, 4)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(context.Background(), records)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("processBatch did not fit in budget — slow successful batch is not limited")
	}

	require.Less(t, sub.calls, len(records),
		"batch must be abandoned before reaching the end")
	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "failed batch must be rewound")
	require.Equal(t, int64(2), cl.setOffsets[0]["src"][0].Offset,
		"rewind must start from the first unprocessed record")
	require.Zero(t, s.offsets.InFlight(), "failed partition is forgotten")
}

// Context cancellation is not a reason to rewind: the process is shutting down, and unfinished
// offsets already sit against the watermark anyway.
func TestProcessBatchDoesNotRewindOnCancel(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{err: errors.New("tigerbeetle unavailable")}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.processBatch(ctx, []*kgo.Record{srcRec(0, 0), srcRec(0, 1)})

	require.Empty(t, clientOf(t, s).setOffsets)
	require.Equal(t, 2, s.offsets.InFlight())
}

// The application order within a partition is the batcher submission order, and
// the batcher applies commands in queue order. Submission proceeds without waiting for
// outcomes, so it is specifically that order that needs to be checked, not the response order.
func TestProcessBatchPreservesSubmitOrderWithinPartition(t *testing.T) {
	const n = 500
	sub := &idSubmitter{ok: make(map[string]bool, n)}
	records := make([]*kgo.Record, n)
	want := make([]string, n)
	wantPub := make([]publication, n)
	for i := range records {
		records[i] = srcRec(0, int64(i))
		want[i] = recID(0, int64(i))
		wantPub[i] = publication{kind: "results", offset: int64(i)}
		sub.ok[want[i]] = true
	}
	em := &recordingEmitter{}
	s := newSink(t, idDecoder{}, sub, em)

	s.processBatch(context.Background(), records)

	require.Equal(t, want, sub.submitted(), "submission to batcher must follow offset order")
	require.Equal(t, wantPub, em.publishedFor(0), "outcome publication must follow offset order")
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(n), s.offsets.Commitable()["src"][0].Offset)
}

// Partitions must run in parallel. The barrier resolves only if all four
// goroutines have reached the batcher at the same time; with sequential processing
// the first one would hang on it forever, wait for the cancellation, and return an error — then
// not a single record would reach publication.
func TestProcessBatchRunsPartitionsConcurrently(t *testing.T) {
	const parts = 4
	sub := newBarrierSubmitter(parts)
	records := make([]*kgo.Record, parts)
	for i := range records {
		records[i] = srcRec(int32(i), 0)
	}
	em := &recordingEmitter{}
	s := newSink(t, idDecoder{}, sub, em)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(ctx, records)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("processBatch did not return")
	}

	require.Equal(t, parts, em.resultsCount(),
		"barrier did not converge: partitions are processed sequentially")
	require.Zero(t, s.offsets.InFlight())
}

// Poison does not go to the batcher, but it must be published in its rightful place: a
// partition's outcome order is offset order, poison or not.
func TestProcessBatchPublishesPoisonInOffsetOrder(t *testing.T) {
	const n = 8
	poison := map[string]bool{}
	sub := &idSubmitter{ok: map[string]bool{}}
	records := make([]*kgo.Record, n)
	want := make([]publication, n)
	for i := range records {
		id := recID(0, int64(i))
		records[i] = srcRec(0, int64(i))
		if i%2 == 1 {
			poison[id] = true
			want[i] = publication{kind: "dlq", offset: int64(i)}
			continue
		}
		sub.ok[id] = true
		want[i] = publication{kind: "results", offset: int64(i)}
	}
	em := &recordingEmitter{}
	s := newSink(t, mixedDecoder{poison: poison}, sub, em)

	s.processBatch(context.Background(), records)

	require.Equal(t, want, em.publishedFor(0))
	require.Len(t, sub.submitted(), n/2, "poison must not enter the batcher at all")
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(n), s.offsets.Commitable()["src"][0].Offset)
}

// Context cancellation mid-poll: exactly the records whose outcome
// arrived and was published are marked Done. Records submitted but not acknowledged remain
// in pending — only the contiguous prefix up to the first of them is committed, so
// after a restart the partition will be re-read starting exactly there. There is nothing to rewind via
// SetOffsets here: the process is shutting down.
func TestProcessBatchCancelledMidPollLeavesUnverifiedRecords(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := &cancellingSubmitter{after: 2, cancel: cancel}
	em := &recordingEmitter{}
	s := newSink(t, idDecoder{}, sub, em)

	records := []*kgo.Record{srcRec(0, 0), srcRec(0, 1), srcRec(0, 2), srcRec(0, 3)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(ctx, records)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("processBatch did not return after context cancellation")
	}

	require.Equal(t, []publication{
		{kind: "results", offset: 0},
		{kind: "results", offset: 1},
	}, em.publishedFor(0), "only records with outcomes are published")
	require.Equal(t, 2, s.offsets.InFlight(), "record without outcome cannot be marked Done")
	require.Equal(t, int64(2), s.offsets.Commitable()["src"][0].Offset,
		"only confirmed record prefix is committed")
	require.Empty(t, clientOf(t, s).setOffsets)
}

// The budget must also apply to records already in flight: submission
// went through instantly, but TigerBeetle went silent. All records are submitted before the first
// wait — that is exactly what pipelining is — but they must not be waited on
// longer than the budget, otherwise the rebalance is held indefinitely.
func TestProcessBatchAbandonsWhileWaitingForOutcomes(t *testing.T) {
	sub := &blockingSubmitter{}
	em := &recordingEmitter{}
	s := newSink(t, idDecoder{}, sub, em)
	s.retryPeriod = time.Millisecond
	s.batchBudget = 50 * time.Millisecond

	records := []*kgo.Record{srcRec(0, 5), srcRec(0, 6), srcRec(1, 2)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(context.Background(), records)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("processBatch did not fit in budget — waiting for outcomes is not limited")
	}

	require.Equal(t, len(records), sub.Calls(),
		"all records must be submitted without waiting for outcomes")
	require.Zero(t, em.resultsCount(), "there were no outcomes — nothing to publish")
	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "failed batch must be rewound")
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {
			0: {Epoch: -1, Offset: 5},
			1: {Epoch: -1, Offset: 2},
		},
	}, cl.setOffsets[0], "both partitions are rewound, each from its own first record")
	require.Zero(t, s.offsets.InFlight(), "failed partitions are forgotten")
}

// F1: the budget must also bound submission itself. SubmitAsync parks on the
// batcher's queue, and the sink deliberately overflows that queue: there are many partitions, each with
// its own maxInFlight. As long as TigerBeetle responds, the queue drains; the
// moment it goes silent, a stuck submission would hold the rebalance block
// indefinitely — while the group's limit for it is 60s.
func TestProcessBatchAbandonsWhenEnqueueBlocks(t *testing.T) {
	sub := &enqueueBlockingSubmitter{}
	em := &recordingEmitter{}
	s := newSink(t, idDecoder{}, sub, em)
	s.retryPeriod = time.Millisecond
	s.batchBudget = 50 * time.Millisecond

	records := []*kgo.Record{srcRec(0, 5), srcRec(0, 6)}
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		s.processBatch(context.Background(), records)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processBatch did not return — submission is not limited by batch budget")
	}

	require.Less(t, time.Since(start), time.Second,
		"submission must yield to budget constraints, not hang")
	require.Zero(t, em.resultsCount(), "no records were submitted — nothing to publish")
	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "failed batch must be rewound")
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {0: {Epoch: -1, Offset: 5}},
	}, cl.setOffsets[0])
	require.Zero(t, s.offsets.InFlight(), "failed partition is forgotten")
}

// F3: a submission failure must stop the partition's run. Otherwise offsets
// after the failed one would go to TigerBeetle while it would not — exactly the inversion of application
// order the run is supposed to rule out.
func TestProcessBatchStopsSubmittingAfterEnqueueFailure(t *testing.T) {
	sub := &refusingSubmitter{refuse: map[string]bool{recID(0, 0): true}}
	em := &recordingEmitter{}
	s := newSink(t, idDecoder{}, sub, em)
	s.retryPeriod = time.Millisecond
	s.batchBudget = 50 * time.Millisecond

	records := []*kgo.Record{srcRec(0, 0), srcRec(0, 1), srcRec(0, 2)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(context.Background(), records)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("processBatch did not return")
	}

	submitted := sub.submitted()
	require.NotEmpty(t, submitted, "failed record must be attempted at least once")
	for _, id := range submitted {
		require.Equal(t, recID(0, 0), id,
			"after submission failure, no subsequent record from the partition can enter the batcher")
	}
	require.Zero(t, em.resultsCount(), "no records were applied")
	require.Equal(t, int64(0), clientOf(t, s).setOffsets[0]["src"][0].Offset,
		"rewind must start from the failed record")
}

// F4: a panic in a partition goroutine has no right to crash the process. The caller's deferred
// AllowRebalance is promised to fire "even on panic", but a panic in a
// separate goroutine bypasses any defer of the caller — it has to be caught
// in the goroutine itself. Here the DLQ panics, including the very publication
// finish issues in response to a panic: prepare's and finish's recovers do not catch that.
func TestProcessBatchSurvivesPanicInPartitionGoroutine(t *testing.T) {
	sub := &idSubmitter{ok: map[string]bool{}}
	s := newSink(t, mixedDecoder{poison: map[string]bool{recID(0, 4): true}},
		sub, panicOnDLQEmitter{})
	s.retryPeriod = time.Millisecond
	s.batchBudget = 50 * time.Millisecond

	require.NotPanics(t, func() {
		s.processBatch(context.Background(), []*kgo.Record{srcRec(0, 4)})
	})

	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "partition with panic must be rewound")
	require.Equal(t, int64(4), cl.setOffsets[0]["src"][0].Offset)
	require.Zero(t, s.offsets.InFlight(), "failed partition is forgotten")
	require.Empty(t, s.offsets.Commitable(), "offset of record with panic must not be committed")
}

// F2 (review): a panic between issuing Results and issuing the reject DLQ has no
// right to orphan the Results publication already issued — finish's recover must
// wait for it too, together with the poison DLQ, rather than replace the set of publications
// wholesale. Without the fix, the Results publication would be lost: offsets.Done would
// happen based solely on the poison DLQ's ack, even though both were sent to the broker.
func TestFinishPanicAfterResultsAwaitsAlreadyIssuedPublication(t *testing.T) {
	em := &resultsThenPanicOnceEmitter{}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	rec := srcRec(0, 0)
	s.offsets.Track(rec)

	type passResult struct {
		applied int
		err     error
	}
	done := make(chan passResult, 1)
	go func() {
		applied, err := s.pass(context.Background(), []*kgo.Record{rec}, time.Now().Add(time.Minute))
		done <- passResult{applied, err}
	}()

	// Both publications must be issued — Results before the panic, the poison DLQ
	// after the recover — and neither is acknowledged yet: pass must hang.
	require.Eventually(t, func() bool { return len(em.publishedFor(0)) == 2 }, time.Second, time.Millisecond,
		"Results and poison DLQ must be published even if panic occurred between them")
	select {
	case r := <-done:
		t.Fatalf("pass completed before both publications were confirmed: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 1, s.offsets.InFlight(), "record without confirmation of both publications cannot be marked Done")
	require.Empty(t, s.offsets.Commitable())

	// Only the poison DLQ is acknowledged: if recover replaced the set of
	// publications wholesale (the bug), this alone would be enough for Done — Results would
	// have been lost long ago. With the fix, the record must remain in flight.
	require.Equal(t, 1, em.releaseKind("dlq"))
	select {
	case r := <-done:
		t.Fatalf("pass completed with unconfirmed Results: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 1, s.offsets.InFlight(),
		"orphaned Results publication must still keep record in flight")

	require.Equal(t, 1, em.releaseKind("results"))

	r := <-done
	require.NoError(t, r.err)
	require.Equal(t, 1, r.applied)
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset,
		"record must become commitable only after ack of both publications")
}

// T16: publication is now asynchronous, and the whole "offset only after ack" contract
// rests on Done happening strictly after acknowledgment. While the broker
// stays silent, the record must remain unfinished — otherwise "acknowledged"
// gets substituted for "handed to the buffer", and no one would notice a lost DLQ for a payment.
func TestRecordIsNotDoneUntilPublicationIsAcknowledged(t *testing.T) {
	em := &deferredEmitter{}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	rec := srcRec(0, 0)
	s.offsets.Track(rec)

	type passResult struct {
		applied int
		err     error
	}
	done := make(chan passResult, 1)
	go func() {
		applied, err := s.pass(context.Background(), []*kgo.Record{rec}, time.Now().Add(time.Minute))
		done <- passResult{applied, err}
	}()

	// The publication is issued but not acknowledged: pass must hang, and the record must
	// stay in flight and not enter the commit.
	require.Eventually(t, func() bool { return len(em.publishedFor(0)) == 1 }, time.Second, time.Millisecond,
		"publication must be issued without waiting for broker")
	select {
	case r := <-done:
		t.Fatalf("pass completed before publication was confirmed: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 1, s.offsets.InFlight(), "unconfirmed record cannot be marked Done")
	require.Empty(t, s.offsets.Commitable(), "unconfirmed offset must not be committed")

	require.Equal(t, 1, em.releaseAll())

	r := <-done
	require.NoError(t, r.err)
	require.Equal(t, 1, r.applied)
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset,
		"confirmed record must become commitable")
}

// T16: a results-publication failure is an infrastructure class. The record is not
// marked Done, and its partition goes through the existing rollback path: it must not
// be silently lost.
func TestProcessBatchResultsPublicationFailureRewindsPartition(t *testing.T) {
	em := &recordingEmitter{failResultsAt: map[int64]error{0: errors.New("results topic down")}}
	sub := &idSubmitter{ok: map[string]bool{recID(0, 0): true, recID(0, 1): true}}
	s := newSink(t, idDecoder{}, sub, em)
	s.retryPeriod = time.Millisecond
	s.batchBudget = 50 * time.Millisecond

	records := []*kgo.Record{srcRec(0, 0), srcRec(0, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(context.Background(), records)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("processBatch did not return")
	}

	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "partition with failed publication must be rewound")
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {0: {Epoch: -1, Offset: 0}},
	}, cl.setOffsets[0], "rewind must start from the record whose publication was not confirmed")
	require.Empty(t, s.offsets.Commitable(), "offset of unpublished record must not be committed")
	require.Zero(t, s.offsets.InFlight())
}

// T16: one record's publication failure has no right to drag its neighbors'
// acknowledgments down with it. Records before the failed one are acknowledged and marked Done;
// the rollback starts exactly at the failed one.
func TestProcessBatchOneFailedPublicationLeavesEarlierOnesConfirmed(t *testing.T) {
	em := &recordingEmitter{failResultsAt: map[int64]error{2: errors.New("results topic down")}}
	ok := map[string]bool{}
	records := make([]*kgo.Record, 4)
	for i := range records {
		records[i] = srcRec(0, int64(i))
		ok[recID(0, int64(i))] = true
	}
	sub := &idSubmitter{ok: ok}
	s := newSink(t, idDecoder{}, sub, em)
	s.retryPeriod = time.Millisecond
	s.batchBudget = 50 * time.Millisecond

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(context.Background(), records)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("processBatch did not return")
	}

	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1)
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {0: {Epoch: 3, Offset: 2}},
	}, cl.setOffsets[0],
		"offsets 0 and 1 are confirmed and marked Done despite failure of offset 2")
}

// T16: acknowledgments arrive in an order different from the one publications were issued in —
// and that changes nothing. Publication order within a partition must remain
// offset order, and Done must occur in that same order.
func TestProcessBatchPublicationOrderFollowsOffsetsWithOutOfOrderAcks(t *testing.T) {
	const (
		parts = 3
		perP  = 40
	)
	em := &deferredEmitter{}
	sub := &idSubmitter{ok: make(map[string]bool, parts*perP)}
	var records []*kgo.Record
	for p := range int32(parts) {
		for o := range int64(perP) {
			records = append(records, srcRec(p, o))
			sub.ok[recID(p, o)] = true
		}
	}
	s := newSink(t, idDecoder{}, sub, em)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.processBatch(context.Background(), records)
	}()

	// Acknowledgments are released in batches and in reverse order — that is how the broker
	// actually responds: response order does not match hand-off order.
	released := 0
	deadline := time.After(30 * time.Second)
	for released < parts*perP {
		select {
		case <-done:
			released = parts * perP
		case <-deadline:
			t.Fatal("processBatch did not return — confirmations are not handled")
		case <-time.After(time.Millisecond):
			released += em.releaseAll()
		}
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("processBatch did not return")
	}

	for p := range int32(parts) {
		want := make([]publication, perP)
		for o := range int64(perP) {
			want[o] = publication{kind: "results", partition: p, offset: o}
		}
		require.Equal(t, want, em.publishedFor(p),
			"partition %d publication must follow offset order", p)
		require.Equal(t, int64(perP), s.offsets.Commitable()["src"][p].Offset,
			"partition %d must reach completion", p)
	}
	require.Zero(t, s.offsets.InFlight())
}

// T16: Flush before a commit must account for errors, not just draining the
// buffer. A Flush failure means some publication is not acknowledged —
// nothing may be committed after it.
func TestCommitSkipsCommitWhenFlushFails(t *testing.T) {
	em := &recordingEmitter{flushErr: errors.New("producer flush failed")}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, em)

	rec := srcRec(0, 0)
	s.offsets.Track(rec)
	s.offsets.Done(rec)
	s.commit(context.Background(), slog.LevelError)

	cl := clientOf(t, s)
	require.Empty(t, cl.committed, "failed Flush must stop commit")
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset,
		"uncommitted offset must remain commitable")
}

func TestCommitSkipsMarkCommittedOnTransportError(t *testing.T) {
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, &recordingEmitter{})
	cl := clientOf(t, s)
	cl.commitErr = errors.New("broker unreachable")

	rec := srcRec(0, 0)
	s.offsets.Track(rec)
	s.offsets.Done(rec)
	s.commit(context.Background(), slog.LevelError)

	require.Len(t, cl.committed, 1)
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset,
		"failed commit must not be marked as completed")
}

// A partition-level error is not surfaced in the callback's err — without checking it,
// the offset would be marked committed without actually being so.
func TestCommitSkipsMarkCommittedOnPartitionError(t *testing.T) {
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, &recordingEmitter{})
	cl := clientOf(t, s)
	cl.partErr = kerr.RebalanceInProgress.Code

	rec := srcRec(0, 0)
	s.offsets.Track(rec)
	s.offsets.Done(rec)
	s.commit(context.Background(), slog.LevelError)

	require.Len(t, cl.committed, 1)
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset)
}

func TestCommitMarksCommittedOnSuccess(t *testing.T) {
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, &recordingEmitter{})
	rec := srcRec(0, 0)
	s.offsets.Track(rec)
	s.offsets.Done(rec)
	s.commit(context.Background(), slog.LevelError)

	require.Empty(t, s.offsets.Commitable(), "successful commit moves watermark")
}

// F3: commit must publish kafkatb_offset_commit_lag from the offsets
// tracker's current state — the gauge was previously registered but never
// written, so it read a permanently misleading 0 no matter how far behind
// the sink actually was.
func TestCommitPublishesCommitLagMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg)
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, &recordingEmitter{})
	s.metrics = m

	r0 := srcRec(0, 0)
	r1 := srcRec(0, 1)
	s.offsets.Track(r0)
	s.offsets.Track(r1)
	s.offsets.Done(r0)
	// offset 1 is tracked but not yet done: the commit below only advances
	// the watermark through offset 0, leaving a real backlog behind it.
	s.commit(context.Background(), slog.LevelError)

	require.Equal(t, 1.0, testutil.ToFloat64(m.CommitLag.WithLabelValues("src", "0")),
		"one record (offset 1) tracked beyond the committed watermark")

	s.offsets.Done(r1)
	s.commit(context.Background(), slog.LevelError)

	require.Equal(t, 0.0, testutil.ToFloat64(m.CommitLag.WithLabelValues("src", "0")),
		"fully caught up: lag must return to 0, not go stale at the previous value")
}

// Forget zeroes out the partition's state, so the commit must go first.
func TestOnRevokedCommitsBeforeForget(t *testing.T) {
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, &recordingEmitter{})
	rec := srcRec(0, 0)
	s.offsets.Track(rec)
	s.offsets.Done(rec)

	s.OnRevoked(context.Background(), map[string][]int32{"src": {0}})

	cl := clientOf(t, s)
	require.Len(t, cl.committed, 1, "commit must occur before Forget")
	require.Equal(t, int64(1), cl.committed[0]["src"][0].Offset)
	require.Empty(t, s.offsets.Commitable(), "partition is forgotten")
}

// Kafka records per poll and TigerBeetle events per request are unrelated
// dimensions: a record can carry many events, so a small TigerBeetle batch must
// not cap how many records a poll may return.
func TestNewTakesPollSizeFromSinkConfig(t *testing.T) {
	cfg := &config.Config{
		Batcher:         config.Batcher{MaxBatchSize: 8, MaxQueue: 100},
		Sink:            config.Sink{MaxInFlightPerPartition: 10, PollSize: 500},
		ShutdownTimeout: time.Second,
	}
	s := New(cfg, nil, nil, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	require.Equal(t, 500, s.pollSize)
}

// New already defends the hand-built-config path for maxInFlight; a zero
// shutdown timeout is the same class of mistake and has a worse outcome — an
// already-expired context makes the final flush and commit fail every time, so
// the whole last poll replays on every restart.
func TestNewClampsZeroShutdownTimeout(t *testing.T) {
	cfg := &config.Config{
		Batcher: config.Batcher{MaxBatchSize: 8, MaxQueue: 100},
		Sink:    config.Sink{MaxInFlightPerPartition: 10, PollSize: 500},
	}
	s := New(cfg, nil, nil, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	require.Equal(t, defaultShutdownTimeout, s.shutdownTimeout)
}

// records_total counts records and events_total counts events. Mixing the two
// under one metric made every ratio across its labels wrong by the average
// events-per-message factor: a 3-transfer message that succeeded added 3 to
// {result="ok"}, while the same message failing on infrastructure added 1 to
// {result="blocked"}.
func TestMetricsSeparateRecordAndEventUnits(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg)

	cmd := &model.Command{
		Op: model.OpCreateTransfers,
		Transfers: []types.Transfer{
			{ID: types.ToUint128(1)}, {ID: types.ToUint128(2)}, {ID: types.ToUint128(3)},
		},
		IDs: []string{"id-0", "id-1", "id-2"},
	}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusOK},
		{Index: 1, ID: "id-1", Status: tbx.StatusOK},
		{Index: 2, ID: "id-2", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newSink(t, stubDecoder{cmd: cmd}, sub, &recordingEmitter{})
	s.metrics = m
	_, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)

	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("rejected")),
		"one record in, one record counted — a record with any rejected event is rejected")
	require.Zero(t, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("ok")),
		"the record is not both ok and rejected")
	require.Equal(t, 2.0, testutil.ToFloat64(m.EventsTotal.WithLabelValues("ok")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.EventsTotal.WithLabelValues("rejected")))
}

// submitRefusingSubmitter refuses at submission, the way the real batcher
// refuses a command it can never apply.
type submitRefusingSubmitter struct{ err error }

func (s *submitRefusingSubmitter) SubmitAsync(
	context.Context, *model.Command,
) (<-chan tbx.SubmitResult, error) {
	return nil, s.err
}

// A command the batcher can never apply is a data error, and the only way out
// is the DLQ: treating it as infrastructure wedges the partition forever —
// retry, budget, rewind, re-read, same error. Reachable through any decoder
// that can produce a zero-event command, which the JSON one cannot but a
// future Protobuf or Avro one may.
func TestHandleEmptyCommandIsPoisonNotInfrastructure(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg)

	em := &recordingEmitter{}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()},
		&submitRefusingSubmitter{err: tbx.ErrEmptyCommand}, em)
	s.metrics = m

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err, "a data error must not be reported as an infrastructure failure")
	require.True(t, done, "the record is finished — it goes to the DLQ and its offset advances")
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("poison")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.DLQTotal.WithLabelValues("poison", "empty_command")))
}

// The same two data errors may arrive as an outcome rather than at submission
// — the interface allows it — and must be classified the same way there.
func TestHandleEmptyCommandOutcomeIsPoison(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg)

	em := &recordingEmitter{}
	sub := &stubSubmitter{err: tbx.ErrEmptyCommand}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.metrics = m

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, 1.0, testutil.ToFloat64(m.DLQTotal.WithLabelValues("poison", "empty_command")))
}
