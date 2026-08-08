package tbx

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

// orderedAccountCmd/orderedTransferCmd are single-event commands tagged with
// a shared sequence number: both operations fold into one application
// order, and the tag reveals not only overlap but also inversion.
func orderedAccountCmd(seq uint64) *model.Command {
	return &model.Command{
		Op:       model.OpCreateAccounts,
		Accounts: []types.Account{{ID: types.ToUint128(seq), UserData64: seq}},
		IDs:      []string{"c" + strconv.FormatUint(seq, 10)},
	}
}

func orderedTransferCmd(seq uint64) *model.Command {
	return &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: []types.Transfer{{ID: types.ToUint128(seq), UserData64: seq}},
		IDs:       []string{"c" + strconv.FormatUint(seq, 10)},
	}
}

// mixedBarrierClient is a barrier shared between both operations: it brings
// together calls to CreateAccounts and CreateTransfers. The batcher must never
// bring them together, no matter how many operation types the queue carries. Records the
// application order tagged by operation ("a1", "t2").
type mixedBarrierClient struct {
	n       int
	timeout time.Duration
	gate    chan struct{}

	mu       sync.Mutex
	arrived  int
	applied  []string
	timedout bool
}

func newMixedBarrierClient(n int, timeout time.Duration) *mixedBarrierClient {
	return &mixedBarrierClient{n: n, timeout: timeout, gate: make(chan struct{})}
}

// enter releases the caller only once n calls have arrived at the client
// simultaneously. If that has not happened within timeout, the call is released
// anyway — otherwise the batcher would hang — but the fact that "there was no
// simultaneity" is preserved in timedout.
func (c *mixedBarrierClient) enter() {
	c.mu.Lock()
	c.arrived++
	last := c.arrived == c.n
	c.mu.Unlock()
	if last {
		close(c.gate)
		return
	}
	select {
	case <-c.gate:
	case <-time.After(c.timeout):
		c.mu.Lock()
		c.timedout = true
		c.mu.Unlock()
	}
}

func (c *mixedBarrierClient) record(prefix string, marks []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range marks {
		c.applied = append(c.applied, prefix+strconv.FormatUint(m, 10))
	}
}

func (c *mixedBarrierClient) CreateTransfers(ts []types.Transfer) ([]types.CreateTransferResult, error) {
	c.enter()
	marks := make([]uint64, len(ts))
	for i, t := range ts {
		marks[i] = t.UserData64
	}
	c.record("t", marks)
	return defaultTransferResults(len(ts)), nil
}

func (c *mixedBarrierClient) CreateAccounts(as []types.Account) ([]types.CreateAccountResult, error) {
	c.enter()
	marks := make([]uint64, len(as))
	for i, a := range as {
		marks[i] = a.UserData64
	}
	c.record("a", marks)
	return defaultAccountResults(len(as)), nil
}

func (c *mixedBarrierClient) order() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.applied...)
}

func (c *mixedBarrierClient) overlapped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.timedout
}

func (c *mixedBarrierClient) LookupAccounts([]types.Uint128) ([]types.Account, error) {
	return nil, nil
}

func (c *mixedBarrierClient) LookupTransfers([]types.Uint128) ([]types.Transfer, error) {
	return nil, nil
}

func (c *mixedBarrierClient) GetAccountTransfers(types.AccountFilter) ([]types.Transfer, error) {
	return nil, nil
}

func (c *mixedBarrierClient) GetAccountBalances(types.AccountFilter) ([]types.AccountBalance, error) {
	return nil, nil
}

func (c *mixedBarrierClient) QueryAccounts(types.QueryFilter) ([]types.Account, error) {
	return nil, nil
}

func (c *mixedBarrierClient) QueryTransfers(types.QueryFilter) ([]types.Transfer, error) {
	return nil, nil
}
func (c *mixedBarrierClient) Nop() error { return nil }
func (c *mixedBarrierClient) Close()     {}

// The batcher owns ordering as a whole, not per operation type: create_accounts and
// create_transfers from one partition must apply in sequence and in the order they
// were submitted. This is exactly the sequence an ordinary producer writes —
// open an account and immediately debit it; if it comes apart, that yields
// debit_account_not_found, i.e. a business rejection and a DLQ for a legitimate transfer.
//
// The sink submits a partition's records as a pipeline, without waiting for each one's
// outcome, so the test submits both commands back-to-back via SubmitAsync — this is
// the real scenario, not an artificial race.
func TestBatcherMixedOpsNeverInFlightTogether(t *testing.T) {
	fc := newMixedBarrierClient(2, 300*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	// max_batch_size=1: each command goes out immediately, no need to wait for linger.
	b := NewBatcher(fc, config.Batcher{MaxBatchSize: 1, Linger: time.Hour, MaxQueue: 8},
		config.Retry{Initial: time.Millisecond, Max: 10 * time.Millisecond}, testLogger(), nil)
	b.Start(ctx)
	t.Cleanup(func() { cancel(); b.Close() })

	chans := make([]<-chan SubmitResult, 0, 2)
	for _, cmd := range []*model.Command{orderedAccountCmd(1), orderedTransferCmd(2)} {
		ch, err := b.SubmitAsync(context.Background(), cmd)
		require.NoError(t, err)
		chans = append(chans, ch)
	}
	for i, ch := range chans {
		select {
		case res := <-ch:
			require.NoError(t, res.Err, "command %d", i+1)
		case <-time.After(10 * time.Second):
			t.Fatalf("no outcome for command %d", i+1)
		}
	}

	require.False(t, fc.overlapped(),
		"команды разных операций оказались в полёте одновременно")
	require.Equal(t, []string{"a1", "t2"}, fc.order(),
		"порядок применения разошёлся с порядком постановки")
}
