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

func startBatcher(t *testing.T, fc *fakeClient, maxBatch int, linger time.Duration) (*Batcher, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: maxBatch, Linger: linger, MaxQueue: 128},
		config.Retry{Initial: time.Millisecond, Max: 10 * time.Millisecond}, testLogger())
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
			require.NoError(t, err)
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
	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, 5, 20*time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = b.Submit(context.Background(), transferCmd(2, "c")) }()
	}
	wg.Wait()
	for _, batch := range fc.batches() {
		require.LessOrEqual(t, len(batch), 5)
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
	wg.Add(2)
	go func() { defer wg.Done(); markOut, _ = b.Submit(context.Background(), mark) }()
	go func() { defer wg.Done(); plainOut, _ = b.Submit(context.Background(), plain) }()
	wg.Wait()

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
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger())
	b.Start(ctx)
	cancel()
	b.Close()
	_, err := b.Submit(context.Background(), transferCmd(1, "c"))
	require.ErrorIs(t, err, ErrClosed)
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
