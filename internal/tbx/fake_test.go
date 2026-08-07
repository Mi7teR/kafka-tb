package tbx

import (
	"sync"

	types "github.com/tigerbeetle/tigerbeetle-go"
)

// fakeClient записывает батчи как они пришли и умеет отдавать
// заданные результаты и инфраструктурные ошибки.
type fakeClient struct {
	mu sync.Mutex

	transferBatches [][]types.Transfer
	accountBatches  [][]types.Account

	// failNext[i] > 0 — вернуть ошибку столько раз подряд, потом успех.
	failTimes int
	err       error

	// resultsFor вызывается на каждый батч трансферов.
	resultsFor func(batch []types.Transfer) []types.CreateTransferResult
}

func (f *fakeClient) CreateTransfers(ts []types.Transfer) ([]types.CreateTransferResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failTimes > 0 {
		f.failTimes--
		return nil, f.err
	}
	cp := make([]types.Transfer, len(ts))
	copy(cp, ts)
	f.transferBatches = append(f.transferBatches, cp)
	if f.resultsFor != nil {
		return f.resultsFor(cp), nil
	}
	return defaultTransferResults(len(cp)), nil
}

func (f *fakeClient) CreateAccounts(as []types.Account) ([]types.CreateAccountResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]types.Account, len(as))
	copy(cp, as)
	f.accountBatches = append(f.accountBatches, cp)
	return defaultAccountResults(len(cp)), nil
}

// defaultTransferResults/defaultAccountResults give MapTransferResults/MapAccountResults
// a dense, batch-sized "everything created" response: the TigerBeetle "created" status
// is 0xFFFFFFFF, not the zero value, and Task 4's mapping rejects any response whose
// length does not match the submitted batch size.
func defaultTransferResults(n int) []types.CreateTransferResult {
	out := make([]types.CreateTransferResult, n)
	for i := range out {
		out[i].Status = types.TransferCreated
	}
	return out
}

func defaultAccountResults(n int) []types.CreateAccountResult {
	out := make([]types.CreateAccountResult, n)
	for i := range out {
		out[i].Status = types.AccountCreated
	}
	return out
}

func (f *fakeClient) batches() [][]types.Transfer {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]types.Transfer, len(f.transferBatches))
	copy(out, f.transferBatches)
	return out
}

func (f *fakeClient) LookupAccounts([]types.Uint128) ([]types.Account, error)   { return nil, nil }
func (f *fakeClient) LookupTransfers([]types.Uint128) ([]types.Transfer, error) { return nil, nil }
func (f *fakeClient) GetAccountTransfers(types.AccountFilter) ([]types.Transfer, error) {
	return nil, nil
}
func (f *fakeClient) GetAccountBalances(types.AccountFilter) ([]types.AccountBalance, error) {
	return nil, nil
}
func (f *fakeClient) QueryAccounts(types.QueryFilter) ([]types.Account, error)   { return nil, nil }
func (f *fakeClient) QueryTransfers(types.QueryFilter) ([]types.Transfer, error) { return nil, nil }
func (f *fakeClient) Nop() error                                                 { return nil }
func (f *fakeClient) Close()                                                     {}
