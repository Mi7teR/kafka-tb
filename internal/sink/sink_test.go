package sink

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
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
			0: {Epoch: 3, Offset: 11}, // 10 обработана, перечитывать с 11
			1: {Epoch: 3, Offset: 4},  // до неё не дошли вовсе
		},
	}, cl.setOffsets[0])
	require.Zero(t, s.offsets.InFlight(), "брошенные партиции забыты")
	require.Empty(t, s.offsets.Commitable(), "забытая партиция ничего не коммитит")
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
	s.commit(context.Background())

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
	s.commit(context.Background())

	require.Len(t, cl.committed, 1)
	require.Equal(t, int64(1), s.offsets.Commitable()["src"][0].Offset)
}

func TestCommitMarksCommittedOnSuccess(t *testing.T) {
	s := newSink(t, stubDecoder{cmd: oneTransferCmd()}, &stubSubmitter{}, &recordingEmitter{})
	rec := srcRec(0, 0)
	s.offsets.Track(rec)
	s.offsets.Done(rec)
	s.commit(context.Background())

	require.Empty(t, s.offsets.Commitable(), "успешный коммит двигает ватермарк")
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
