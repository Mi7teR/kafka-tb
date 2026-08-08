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
		c.Transfers[i] = types.Transfer{ID: types.ToUint128(uint64(i + 1)), Flags: 1} // linked у всех
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
			// assert, а не require: FailNow из не-тестовой горутины — UB.
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
	// Linger заведомо длиннее теста: единственный путь к флашу — порог
	// max_batch_size. Шесть команд по 2 события при max=6 обязаны дать ровно
	// два батча, каждый упирающийся в границу точно.
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 6, time.Hour)

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := b.Submit(context.Background(), transferCmd(2, "c"))
			// assert, а не require: FailNow из не-тестовой горутины — UB.
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
	// Отклоняем каждое событие, у которого Amount == 7: так проверяем,
	// что исход попал именно в ту команду, где это событие лежало.
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

// C1: отмена контекста Start останавливает цикл. Отправители, застрявшие
// на переполненной очереди, обязаны получить выход, иначе Close() виснет
// и вместе с ним весь shutdown процесса.
func TestBatcherCloseReturnsWithBlockedSubmitters(t *testing.T) {
	fc := &fakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Hour, MaxQueue: 2},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)

	cancel()
	time.Sleep(50 * time.Millisecond) // дать циклам выйти по отмене

	const submitters = 5
	errs := make(chan error, submitters)
	for i := 0; i < submitters; i++ {
		go func() {
			_, err := b.Submit(context.Background(), transferCmd(1, "c"))
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond) // дать отправителям заполнить буфер

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

// C2: после отмены контекста Start ни один Submit не должен зависнуть
// на ожидании исхода — иначе сообщение Kafka теряется без ответа.
func TestBatcherSubmitAfterContextCancelFails(t *testing.T) {
	fc := &fakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 10, Linger: time.Hour, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	b.Start(ctx)
	t.Cleanup(b.Close)

	cancel()
	time.Sleep(50 * time.Millisecond) // дать циклам выйти по отмене

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

// F1: Close() во время уже летящего батча не имеет права соврать отправителю.
// TigerBeetle применил события — значит Submit обязан вернуть настоящие исходы,
// а не ErrClosed. Для синхронного API это единственный источник правды:
// его HTTP-клиент не хранит offset и не обязан повторять запрос с тем же id.
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

	// Батч дошёл до клиента и застрял внутри вызова.
	select {
	case <-fc.enterTransfers:
	case <-time.After(2 * time.Second):
		t.Fatal("client call never started")
	}

	closed := make(chan struct{})
	go func() { b.Close(); close(closed) }()
	time.Sleep(50 * time.Millisecond) // дать Close закрыть stop и уйти в wg.Wait

	fc.releaseTransfers <- struct{}{} // TigerBeetle ответил: события применены

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

// SubmitAsync существует ради батчинга: синхронный Submit держит очередь
// пустой, и батч всегда собирается из одной команды. Здесь max_batch_size — 10,
// linger — час: единственный путь к флашу — десять команд, поставленных в
// очередь до того, как первая получила исход. Синхронный вызывающий такой
// батч собрать не может в принципе, поэтому тест заодно фиксирует сам факт
// неблокирующей постановки.
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

// Backpressure: полная очередь обязана блокировать постановку, иначе синк
// поставит в очередь весь опрос и съест память.
func TestBatcherSubmitAsyncBlocksWhenQueueIsFull(t *testing.T) {
	fc := &fakeClient{
		enterTransfers:   make(chan struct{}),
		releaseTransfers: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	// max_batch_size=1: каждая команда флашится сразу, поэтому цикл застревает
	// внутри вызова клиента и очередь перестаёт разгребаться.
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
	case <-fc.enterTransfers: // цикл внутри вызова клиента и очередь не читает
	case <-time.After(2 * time.Second):
		t.Fatal("client call never started")
	}

	// Ровно MaxQueue команд помещаются в буфер и возвращаются немедленно.
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

	// Отпускаем клиент: цикл забирает следующую команду, место освобождается.
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

// Вызывающий вправе бросить канал не прочитав: писатель ровно один и канал
// буферизован, поэтому цикл не имеет права встать.
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

// Отменённый контекст обязан отпустить отправителя, упёршегося в полную
// очередь. Циклы не запущены — очередь никто не разгребает, поэтому исход
// гонки здесь однозначен.
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

// Команда, поставленная так поздно, что её не увидит ни один цикл, обязана
// получить ErrClosed. Молчание в канал — потерянное сообщение Kafka.
// Циклов здесь нет вовсе: Start не вызывали, отвечать некому по построению.
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

// Асинхронный вариант F1: Close во время летящего батча не имеет права
// подменить настоящий исход на ErrClosed. Гонка «исход доставлен ровно в
// момент выхода циклов» ловится именно здесь.
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
	time.Sleep(50 * time.Millisecond) // дать Close закрыть stop и уйти в wg.Wait

	fc.releaseTransfers <- struct{}{} // TigerBeetle ответил: события применены

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
