package sink

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/codec"
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

type stubSubmitter struct {
	outcomes []tbx.Outcome
	err      error
	calls    int
}

func (s *stubSubmitter) Submit(context.Context, *model.Command) ([]tbx.Outcome, error) {
	s.calls++
	return s.outcomes, s.err
}

// scriptedSubmitter отдаёт заранее заданный результат на каждый вызов;
// последний элемент errs повторяется. Нужен, чтобы отличить «упало и
// починилось» от «падает всегда».
type scriptedSubmitter struct {
	outcomes []tbx.Outcome
	errs     []error
	calls    int
}

func (s *scriptedSubmitter) Submit(context.Context, *model.Command) ([]tbx.Outcome, error) {
	err := s.errs[min(s.calls, len(s.errs)-1)]
	s.calls++
	if err != nil {
		return nil, err
	}
	return s.outcomes, nil
}

// slowSubmitter всегда успешен, но перед возвратом ждёт delay. Нужен, чтобы
// смоделировать пачку без единой инфраструктурной ошибки, которая тем не
// менее коллективно медленная — бюджет должен ловить и такой случай, а не
// только вечный ретрай.
type slowSubmitter struct {
	delay    time.Duration
	outcomes []tbx.Outcome
	calls    int
}

func (s *slowSubmitter) Submit(context.Context, *model.Command) ([]tbx.Outcome, error) {
	time.Sleep(s.delay)
	s.calls++
	return s.outcomes, nil
}

// recordingEmitter умеет отказывать выборочно: провал DLQ и провал Results —
// разные ветки handle, и их нельзя проверить одним общим переключателем.
type recordingEmitter struct {
	dlq         []dlqCall
	results     int
	failDLQ     error
	failResults error
}

type dlqCall struct {
	reason  string
	errName string
}

func (r *recordingEmitter) DLQ(_ context.Context, _ *kgo.Record, reason emitReason, errName, _ string) error {
	if r.failDLQ != nil {
		return r.failDLQ
	}
	r.dlq = append(r.dlq, dlqCall{reason: string(reason), errName: errName})
	return nil
}

func (r *recordingEmitter) Results(context.Context, *kgo.Record, []tbx.Outcome) error {
	if r.failResults != nil {
		return r.failResults
	}
	r.results++
	return nil
}
func (r *recordingEmitter) Flush(context.Context) error { return nil }
func (r *recordingEmitter) Close()                      {}

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

func srcRec(partition int32, offset int64) *kgo.Record {
	return &kgo.Record{Topic: "src", Partition: partition, Offset: offset, LeaderEpoch: 3}
}

func TestHandlePoisonGoesToDLQ(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{}
	s := newSink(t, stubDecoder{err: codec.Poison("bad json")}, sub, em)

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src", Value: []byte("x")})
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

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done)
	require.Empty(t, em.dlq)
}

// Команда больше батча — ретрай её не починит: это poison.
func TestHandleCommandTooLargeGoesToDLQ(t *testing.T) {
	em := &recordingEmitter{}
	sub := &stubSubmitter{err: tbx.ErrCommandTooLarge}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.Error(t, err)
	require.False(t, done, "must not commit offset if DLQ write failed")
}

// Паника в обработке не роняет процесс.
func TestHandleRecoversPanic(t *testing.T) {
	em := &recordingEmitter{}
	s := newSink(t, panicDecoder{}, &stubSubmitter{}, em)
	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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
	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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
func TestApplyRecordCancelledContextDoesNotLogError(t *testing.T) {
	var logs []capturedLog
	em := &recordingEmitter{}
	sub := &stubSubmitter{err: errors.New("tigerbeetle unavailable")}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.log = slog.New(&captureHandler{logs: &logs})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := srcRec(0, 0)
	ok := s.applyRecord(ctx, rec, time.Now().Add(time.Minute))

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
	done, err := s.handle(context.Background(), &kgo.Record{Topic: "other"})
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

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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
	_, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("ok")))

	sub = &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "id-0", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s = newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, &recordingEmitter{})
	s.metrics = m
	_, err = s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("rejected")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.DLQTotal.WithLabelValues("reject", "exceeds_credits")))

	s = newSink(t, stubDecoder{err: codec.Poison("bad json")}, &stubSubmitter{}, &recordingEmitter{})
	s.metrics = m
	_, err = s.handle(context.Background(), &kgo.Record{Topic: "src"})
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(m.RecordsTotal.WithLabelValues("poison")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.DLQTotal.WithLabelValues("poison", "decode")))

	s = newSink(t, stubDecoder{cmd: oneTransferCmd()},
		&stubSubmitter{err: errors.New("tigerbeetle unavailable")}, &recordingEmitter{})
	s.metrics = m
	_, err = s.handle(context.Background(), &kgo.Record{Topic: "src"})
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

	done, err := s.handle(context.Background(), &kgo.Record{Topic: "src"})
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
	sub := &scriptedSubmitter{
		outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}},
		errs:     []error{nil, errors.New("tigerbeetle unavailable")},
	}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
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
// ретраи: бюджет проверяется в начале каждой итерации, а не только внутри
// applyRecord.
func TestProcessBatchAbandonsSlowSuccessfulBatch(t *testing.T) {
	em := &recordingEmitter{}
	sub := &slowSubmitter{
		delay:    10 * time.Millisecond,
		outcomes: []tbx.Outcome{{Index: 0, ID: "id-0", Status: tbx.StatusOK}},
	}
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, sub, em)
	s.batchBudget = 20 * time.Millisecond

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
