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

// result отдаёт канал, в котором уже лежит res: контракт SubmitAsync — ровно
// один SubmitResult на принятую команду, и все стабы ниже отвечают им.
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

// scriptedSubmitter отдаёт заранее заданный результат на каждый вызов;
// последний элемент errs повторяется. Нужен, чтобы отличить «упало и
// починилось» от «падает всегда». Расписание по номеру вызова осмысленно
// только для одной партиции — для нескольких есть idSubmitter.
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

// slowSubmitter всегда успешен, но исход отдаёт только через delay — так
// выглядит round-trip до TigerBeetle. Нужен, чтобы смоделировать пачку без
// единой инфраструктурной ошибки, которая тем не менее коллективно медленная:
// бюджет должен ловить и такой случай, а не только вечный ретрай.
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

// idDecoder делает из байтов записи команду, чей единственный id — эти байты.
// Нужен, как только партиции идут параллельно: сценарий, расписанный по
// номеру вызова, перестаёт быть детерминированным, когда две партиции ставят
// команды одновременно, — расписывать приходится по самой записи.
type idDecoder struct{}

func (idDecoder) Decode(v []byte) (*model.Command, error) {
	return &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: []types.Transfer{{ID: types.ToUint128(1)}},
		IDs:       []string{string(v)},
	}, nil
}

// mixedDecoder — тот же idDecoder, но перечисленные id объявляет poison.
type mixedDecoder struct{ poison map[string]bool }

func (d mixedDecoder) Decode(v []byte) (*model.Command, error) {
	if d.poison[string(v)] {
		return nil, codec.Poison("bad json")
	}
	return idDecoder{}.Decode(v)
}

// idSubmitter отвечает успехом только на перечисленные в ok id и вечной
// инфраструктурной ошибкой на всё остальное, а также записывает порядок
// постановки — именно он задаёт порядок применения в TigerBeetle.
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

// barrierSubmitter отвечает только после того, как в него одновременно зашли n
// вызовов. При последовательной обработке партиций такого не случится никогда.
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

// blockingSubmitter принимает команду и не отвечает никогда: так выглядит
// TigerBeetle, который перестал отвечать уже после постановки.
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

// enqueueBlockingSubmitter паркуется на самой постановке и отпускает только по
// отмене контекста: так выглядит батчер, чья очередь забита, потому что
// TigerBeetle перестал отвечать и ретраи разбирают её вечно.
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

// refusingSubmitter отказывает в постановке команде с перечисленным id и
// записывает всё, что до него дошло: только по этому журналу видно, не уехала
// ли следующая запись партиции в TigerBeetle раньше упавшей.
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

// cancellingSubmitter отвечает первым after записям сразу, а начиная со
// следующей отменяет контекст и уходит в молчание — так выглядит SIGTERM
// посреди опроса.
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

// recordingEmitter умеет отказывать выборочно: провал DLQ и провал Results —
// разные ветки finish, и их нельзя проверить одним общим переключателем.
// Мьютекс обязателен: партиции публикуют параллельно.
type recordingEmitter struct {
	mu      sync.Mutex
	dlq     []dlqCall
	results int
	// published — сквозной журнал публикаций, и results, и DLQ, с офсетом
	// исходной записи. Порядок публикации внутри партиции обязан быть
	// порядком офсетов, и проверить это можно только по такому журналу.
	published   []publication
	failDLQ     error
	failResults error
	// failResultsAt проваливает публикацию results ровно у перечисленных
	// офсетов: провал одной записи не имеет права утаскивать за собой пачку,
	// и проверить это общим переключателем невозможно.
	failResultsAt map[int64]error
	// flushErr — то, чем отвечает Flush; коммит обязан на это реагировать.
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

// publishedFor отдаёт журнал публикаций одной партиции: между партициями
// порядок не гарантируется и никогда не гарантировался.
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

// panicOnDLQEmitter паникует на каждой публикации в DLQ — в том числе на той,
// которой finish отвечает на панику. Так паника выходит за пределы recover'ов
// prepare и finish и добирается до самой горутины партиции.
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

// deferredEmitter публикует, не отвечая: обещания копятся, а завершает их
// release — в любом порядке. Так выглядит настоящая асинхронная публикация, у
// которой брокер отвечает позже и не обязательно в порядке выдачи. Без такого
// стаба «Done только после подтверждения» проверить нечем: у уже готового
// обещания подтверждение и выдача неразличимы.
type deferredEmitter struct {
	mu        sync.Mutex
	published []publication
	resolve   []func(error)
	// rejectAt — офсеты, чью публикацию брокер отвергнет.
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

// releaseAll завершает все накопленные обещания в обратном порядке выдачи:
// порядок ответов брокера не обязан совпадать с порядком публикации, и синк
// обязан оставаться корректным именно при таком расхождении.
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

// stubClient подменяет *kgo.Client в тех двух вызовах, которыми синк двигает
// офсеты. Без него processBatch/commit/OnRevoked невозможно прогнать.
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

// Компилятор обязан подтвердить, что стаб реализует настоящий emit.Emitter:
// иначе тесты зелены на сигнатуре, которой в проде нет.
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

// recID именует запись так, чтобы стаб-батчер мог отличить одну от другой.
func recID(partition int32, offset int64) string {
	return fmt.Sprintf("%d/%d", partition, offset)
}

func srcRec(partition int32, offset int64) *kgo.Record {
	return &kgo.Record{
		Topic: "src", Partition: partition, Offset: offset, LeaderEpoch: 3,
		Value: []byte(recID(partition, offset)),
	}
}

// handleOne прогоняет одну запись через боевой путь синка — постановка,
// ожидание исхода, публикация — и отвечает тем же, чем отвечала обработка
// одной записи до пайплайнинга: доведена ли она до конца и с какой ошибкой.
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

// Инфраструктурная ошибка не даёт двигать офсет.
func TestHandleInfraErrorBlocks(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{err: errors.New("tigerbeetle unavailable")}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done)
	require.Empty(t, em.dlq)
}

// Команда больше батча — ретрай её не починит: это poison.
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

// Если DLQ не пишется — это тоже инфраструктура, офсет стоит.
func TestHandleDLQFailureBlocks(t *testing.T) {
	em := &recordingEmitter{failDLQ: errors.New("broker down")}
	s := newSink(t, stubDecoder{err: codec.Poison("bad json")}, &stubSubmitter{}, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done, "must not commit offset if DLQ write failed")
}

// Паника в обработке не роняет процесс.
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

// Паника при живой инфраструктурной проблеме в DLQ не должна коммитить офсет.
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

// Неизвестный топик — конфигурационная ошибка, но убивать процесс из-за
// одного сообщения нельзя: пишем в DLQ.
func TestHandleUnknownTopic(t *testing.T) {
	em := &recordingEmitter{}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, em)
	done, err := handleOne(t, s, &kgo.Record{Topic: "other"})
	require.NoError(t, err)
	require.True(t, done)
	require.Len(t, em.dlq, 1)
	require.Equal(t, "unknown_topic", em.dlq[0].errName)
}

// Контракт codec.Decoder: любая ошибка декодинга — poison. Ошибка не типа
// PoisonError не должна уходить в бесконечный ретрай.
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

// Провал записи в results — инфраструктура: офсет стоит, DLQ не пишется.
func TestHandleResultsFailureBlocks(t *testing.T) {
	em := &recordingEmitter{failResults: errors.New("results topic down")}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done, "offset must not move if results were not published")
	require.Empty(t, em.dlq)
}

// Reject опубликован в results, но не доехал до DLQ — офсет всё равно стоит.
func TestHandleDLQFailureInsideRejectLoopBlocks(t *testing.T) {
	em := &recordingEmitter{failDLQ: errors.New("dlq down")}
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := handleOne(t, s, &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done)
	require.Equal(t, 1, em.results, "results уже опубликованы — ветка именно про reject-цикл")
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
	require.Empty(t, clientOf(t, s).setOffsets, "успешная пачка не перематывается")
}

// C1: упавшая запись повторяется сама, а не пропускается ради следующей.
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

	require.Equal(t, 2, sub.calls, "та же запись должна повториться")
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(8), s.offsets.Commitable()["src"][0].Offset)
	require.Empty(t, em.dlq, "инфраструктурный сбой не повод для DLQ")
	require.Equal(t, 1, em.results, "результат публикуется ровно один раз")
}

// C2: вечная инфраструктурная ошибка не держит ребаланс дольше бюджета —
// пачка бросается, а её партиции перематываются назад и забываются.
func TestProcessBatchAbandonsAndRewindsOnBudget(t *testing.T) {
	em := &recordingEmitter{}
	// Расписание по записи, а не по номеру вызова: партиции теперь идут
	// параллельно, и «первый вызов успешен» перестало быть детерминированным.
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
		t.Fatal("processBatch не уложилась в бюджет — ретрай не ограничен")
	}

	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "брошенная пачка обязана перематываться")
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {
			0: {Epoch: 3, Offset: 11}, // 10 обработана и Done с epoch 3
			1: {Epoch: -1, Offset: 4}, // до неё не дошли вовсе — offset 3 не Done, сентинел
		},
	}, cl.setOffsets[0])
	require.Zero(t, s.offsets.InFlight(), "брошенные партиции забыты")
	require.Empty(t, s.offsets.Commitable(), "забытая партиция ничего не коммитит")
}

// C4: пачка без единой инфраструктурной ошибки — каждая запись успешна, но
// медленная — обязана уложиться в бюджет так же, как и пачка, упирающаяся в
// ретраи: бюджет проверяется в начале каждого витка runPartition, а не только
// на исходе конкретной записи.
func TestProcessBatchAbandonsSlowSuccessfulBatch(t *testing.T) {
	em := &recordingEmitter{}
	sub := &slowSubmitter{
		delay:    20 * time.Millisecond,
		outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}},
	}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.batchBudget = 50 * time.Millisecond
	// Одна запись в полёте: без этого пайплайнинг превращает эту пачку в
	// быструю (все пять round-trip'ов идут параллельно) и бюджету нечего
	// ловить. Проверяется здесь именно бюджет, а не пайплайнинг — за то, что
	// бюджет действует и на записи в полёте, отвечает
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
		t.Fatal("processBatch не уложилась в бюджет — медленная успешная пачка не ограничена")
	}

	require.Less(t, sub.calls, len(records),
		"пачка обязана быть брошена, не дойдя до конца")
	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "брошенная пачка обязана перематываться")
	require.Equal(t, int64(2), cl.setOffsets[0]["src"][0].Offset,
		"перематывать нужно с первой недошедшей записи")
	require.Zero(t, s.offsets.InFlight(), "брошенная партиция забыта")
}

// Отмена контекста — не повод перематывать: процесс уходит, а незавершённые
// офсеты и так упираются в ватермарк.
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

// Порядок применения внутри партиции — это порядок постановки в батчер, а
// батчер применяет команды в порядке очереди. Постановка идёт не дожидаясь
// исходов, поэтому проверять нужно именно её порядок, а не порядок ответов.
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

	require.Equal(t, want, sub.submitted(), "постановка в батчер обязана идти по офсетам")
	require.Equal(t, wantPub, em.publishedFor(0), "публикация исходов обязана идти по офсетам")
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(n), s.offsets.Commitable()["src"][0].Offset)
}

// Партиции обязаны идти параллельно. Барьер сходится только если все четыре
// горутины оказались в батчере одновременно; при последовательной обработке
// первая упрётся в него навсегда, дождётся отмены и вернёт ошибку — тогда до
// публикации не дойдёт ни одна запись.
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
		t.Fatal("processBatch не вернулась")
	}

	require.Equal(t, parts, em.resultsCount(),
		"барьер не сошёлся: партиции обрабатываются последовательно")
	require.Zero(t, s.offsets.InFlight())
}

// Poison в батчер не идёт, но публиковаться обязан на своём месте: порядок
// исходов партиции — это порядок офсетов, poison там или не poison.
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
	require.Len(t, sub.submitted(), n/2, "poison не должен попадать в батчер вообще")
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(n), s.offsets.Commitable()["src"][0].Offset)
}

// Отмена контекста посреди опроса: помечены Done ровно те записи, чей исход
// пришёл и опубликован. Записи, поставленные, но не подтверждённые, остаются
// в pending — коммитится только непрерывный префикс до первой из них, поэтому
// после рестарта партиция перечитается ровно с неё. Перематывать через
// SetOffsets здесь нечего: процесс уходит.
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
		t.Fatal("processBatch не вернулась после отмены контекста")
	}

	require.Equal(t, []publication{
		{kind: "results", offset: 0},
		{kind: "results", offset: 1},
	}, em.publishedFor(0), "публикуются ровно записи с исходом")
	require.Equal(t, 2, s.offsets.InFlight(), "запись без исхода не может быть Done")
	require.Equal(t, int64(2), s.offsets.Commitable()["src"][0].Offset,
		"коммитится только префикс подтверждённых записей")
	require.Empty(t, clientOf(t, s).setOffsets)
}

// Бюджет обязан действовать и на записи, которые уже в полёте: постановка
// прошла мгновенно, а TigerBeetle замолчал. Все записи поставлены до первого
// ожидания — именно это и есть пайплайнинг — но ждать их дольше бюджета
// нельзя, иначе ребаланс держится сколь угодно долго.
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
		t.Fatal("processBatch не уложилась в бюджет — ожидание исходов не ограничено")
	}

	require.Equal(t, len(records), sub.Calls(),
		"все записи обязаны быть поставлены не дожидаясь исходов")
	require.Zero(t, em.resultsCount(), "исходов не было — публиковать нечего")
	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "брошенная пачка обязана перематываться")
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {
			0: {Epoch: -1, Offset: 5},
			1: {Epoch: -1, Offset: 2},
		},
	}, cl.setOffsets[0], "перематываются обе партиции, каждая со своей первой записи")
	require.Zero(t, s.offsets.InFlight(), "брошенные партиции забыты")
}

// F1: бюджет обязан ограничивать и саму постановку. SubmitAsync паркуется на
// очереди батчера, и очередь эту синк переполняет намеренно: партиций много, у
// каждой свой maxInFlight. Пока TigerBeetle отвечает, очередь разбирается; как
// только он замолчал, застрявшая постановка держала бы блокировку ребаланса
// сколь угодно долго — а её лимит у группы 60s.
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
		t.Fatal("processBatch не вернулась — постановка не ограничена бюджетом пачки")
	}

	require.Less(t, time.Since(start), time.Second,
		"постановка обязана сдаваться на бюджете, а не висеть")
	require.Zero(t, em.resultsCount(), "ни одна запись не поставлена — публиковать нечего")
	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "брошенная пачка обязана перематываться")
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {0: {Epoch: -1, Offset: 5}},
	}, cl.setOffsets[0])
	require.Zero(t, s.offsets.InFlight(), "брошенная партиция забыта")
}

// F3: провал постановки обязан останавливать пробег партиции. Иначе офсеты
// после упавшего уедут в TigerBeetle, а он — нет: ровно та инверсия порядка
// применения, которую пробег обязан исключать.
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
		t.Fatal("processBatch не вернулась")
	}

	submitted := sub.submitted()
	require.NotEmpty(t, submitted, "упавшая запись обязана быть хотя бы попытана")
	for _, id := range submitted {
		require.Equal(t, recID(0, 0), id,
			"после провала постановки ни одна следующая запись партиции не может попасть в батчер")
	}
	require.Zero(t, em.resultsCount(), "применённых записей не было")
	require.Equal(t, int64(0), clientOf(t, s).setOffsets[0]["src"][0].Offset,
		"перематывать нужно с упавшей записи")
}

// F4: паника в горутине партиции не имеет права уронить процесс. defer
// AllowRebalance у вызывающего обещает сняться «хоть на панике», но паника в
// отдельной горутине проходит мимо любого defer вызывающего — ловить её
// приходится в самой горутине. Здесь паникует DLQ, в том числе та публикация,
// которой finish отвечает на панику: recover'ы prepare и finish такое не ловят.
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
	require.Len(t, cl.setOffsets, 1, "партиция с паникой обязана перематываться")
	require.Equal(t, int64(4), cl.setOffsets[0]["src"][0].Offset)
	require.Zero(t, s.offsets.InFlight(), "брошенная партиция забыта")
	require.Empty(t, s.offsets.Commitable(), "офсет записи с паникой не коммитится")
}

// F2 (review): паника между выдачей Results и выдачей reject DLQ не имеет
// права осиротить уже выданную публикацию Results — recover в finish обязан
// дождаться и её вместе с поисоновым DLQ, а не заменить набор публикаций
// целиком. Без фикса Results-публикация терялась бы: offsets.Done наступал
// бы по одному только ack поисонового DLQ, хотя брокеру ушли обе.
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

	// Обе публикации обязаны быть выданы — Results до паники, DLQ поисона
	// после recover'а, — и ни одна ещё не подтверждена: pass обязана висеть.
	require.Eventually(t, func() bool { return len(em.publishedFor(0)) == 2 }, time.Second, time.Millisecond,
		"Results и поисоновый DLQ обязаны быть выданы, даже если между ними была паника")
	select {
	case r := <-done:
		t.Fatalf("pass завершилась до подтверждения обеих публикаций: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 1, s.offsets.InFlight(), "запись без подтверждения обеих публикаций не может быть Done")
	require.Empty(t, s.offsets.Commitable())

	// Подтверждается только поисоновый DLQ: если бы recover заменял набор
	// публикаций целиком (баг), этого было бы достаточно для Done — Results
	// давно потерян. С фиксом record обязана остаться в полёте.
	require.Equal(t, 1, em.releaseKind("dlq"))
	select {
	case r := <-done:
		t.Fatalf("pass завершилась при неподтверждённом Results: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 1, s.offsets.InFlight(),
		"осиротевшая публикация Results обязана по-прежнему держать запись в полёте")

	require.Equal(t, 1, em.releaseKind("results"))

	r := <-done
	require.NoError(t, r.err)
	require.Equal(t, 1, r.applied)
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset,
		"запись обязана стать коммитабельной только после ack обеих публикаций")
}

// T16: публикация теперь асинхронна, и весь контракт «офсет только после ack»
// держится на том, что Done наступает строго после подтверждения. Пока брокер
// молчит, запись обязана оставаться незавершённой — иначе «подтверждено»
// подменяется на «отдано в буфер», а потерянный DLQ платежа не заметит никто.
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

	// Публикация выдана, но не подтверждена: pass обязана висеть, а запись —
	// оставаться в полёте и не попадать в коммит.
	require.Eventually(t, func() bool { return len(em.publishedFor(0)) == 1 }, time.Second, time.Millisecond,
		"публикация обязана выдаваться, не дожидаясь брокера")
	select {
	case r := <-done:
		t.Fatalf("pass завершилась до подтверждения публикации: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 1, s.offsets.InFlight(), "неподтверждённая запись не может быть Done")
	require.Empty(t, s.offsets.Commitable(), "неподтверждённый офсет не имеет права коммититься")

	require.Equal(t, 1, em.releaseAll())

	r := <-done
	require.NoError(t, r.err)
	require.Equal(t, 1, r.applied)
	require.Zero(t, s.offsets.InFlight())
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset,
		"подтверждённая запись обязана стать коммитабельной")
}

// T16: провал публикации в results — инфраструктурный класс. Запись не
// помечается Done, а её партиция уходит в откат существующим путём: молча
// потерять её нельзя.
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
		t.Fatal("processBatch не вернулась")
	}

	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1, "партиция с непрошедшей публикацией обязана перематываться")
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {0: {Epoch: -1, Offset: 0}},
	}, cl.setOffsets[0], "перематывать нужно с записи, чья публикация не подтверждена")
	require.Empty(t, s.offsets.Commitable(), "офсет неопубликованной записи не коммитится")
	require.Zero(t, s.offsets.InFlight())
}

// T16: провал публикации одной записи не имеет права утаскивать за собой
// подтверждения соседей. Записи до упавшей подтверждены и помечены Done;
// откат начинается ровно с упавшей.
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
		t.Fatal("processBatch не вернулась")
	}

	cl := clientOf(t, s)
	require.Len(t, cl.setOffsets, 1)
	require.Equal(t, map[string]map[int32]kgo.EpochOffset{
		"src": {0: {Epoch: 3, Offset: 2}},
	}, cl.setOffsets[0],
		"офсеты 0 и 1 подтверждены и помечены Done, несмотря на провал офсета 2")
}

// T16: подтверждения приходят не в том порядке, в каком публикации выданы, —
// и это ничего не меняет. Порядок публикации внутри партиции обязан оставаться
// порядком офсетов, а Done — наступать по тому же порядку.
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

	// Подтверждения выдаются пачками и в обратном порядке — так брокер и
	// отвечает: порядок ответов не совпадает с порядком выдачи.
	released := 0
	deadline := time.After(30 * time.Second)
	for released < parts*perP {
		select {
		case <-done:
			released = parts * perP
		case <-deadline:
			t.Fatal("processBatch не вернулась — подтверждения не разбираются")
		case <-time.After(time.Millisecond):
			released += em.releaseAll()
		}
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("processBatch не вернулась")
	}

	for p := range int32(parts) {
		want := make([]publication, perP)
		for o := range int64(perP) {
			want[o] = publication{kind: "results", partition: p, offset: o}
		}
		require.Equal(t, want, em.publishedFor(p),
			"публикация партиции %d обязана идти по офсетам", p)
		require.Equal(t, int64(perP), s.offsets.Commitable()["src"][p].Offset,
			"партиция %d обязана дойти до конца", p)
	}
	require.Zero(t, s.offsets.InFlight())
}

// T16: Flush перед коммитом обязан учитывать ошибки, а не только опустошение
// буфера. Провал Flush означает, что какая-то публикация не подтверждена —
// коммитить после него нельзя ничего.
func TestCommitSkipsCommitWhenFlushFails(t *testing.T) {
	em := &recordingEmitter{flushErr: errors.New("producer flush failed")}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, em)

	rec := srcRec(0, 0)
	s.offsets.Track(rec)
	s.offsets.Done(rec)
	s.commit(context.Background(), slog.LevelError)

	cl := clientOf(t, s)
	require.Empty(t, cl.committed, "провалившийся Flush обязан остановить коммит")
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset,
		"незакоммиченный офсет обязан остаться коммитабельным")
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
		"неудавшийся коммит не должен помечаться выполненным")
}

// Ошибка уровня партиции не поднимается в err коллбэка — без её проверки
// офсет был бы помечен закоммиченным, не будучи им.
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

	require.Empty(t, s.offsets.Commitable(), "успешный коммит двигает ватермарк")
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

// Forget обнуляет состояние партиции, поэтому коммит обязан идти первым.
func TestOnRevokedCommitsBeforeForget(t *testing.T) {
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, &recordingEmitter{})
	rec := srcRec(0, 0)
	s.offsets.Track(rec)
	s.offsets.Done(rec)

	s.OnRevoked(context.Background(), map[string][]int32{"src": {0}})

	cl := clientOf(t, s)
	require.Len(t, cl.committed, 1, "коммит обязан произойти до Forget")
	require.Equal(t, int64(1), cl.committed[0]["src"][0].Offset)
	require.Empty(t, s.offsets.Commitable(), "партиция забыта")
}
