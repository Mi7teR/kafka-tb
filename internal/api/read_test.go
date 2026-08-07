package api

import (
	"context"
	"errors"
	"testing"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubClient — минимальная реализация tbx.Client для тестов read-хендлеров.
// Каждый метод, которым тест не пользуется, паникует при вызове — так
// нечаянный лишний вызов виден сразу, а не тонет в нулевом значении.
type stubClient struct {
	lookupAccountsFn      func([]types.Uint128) ([]types.Account, error)
	lookupTransfersFn     func([]types.Uint128) ([]types.Transfer, error)
	getAccountTransfersFn func(types.AccountFilter) ([]types.Transfer, error)
	getAccountBalancesFn  func(types.AccountFilter) ([]types.AccountBalance, error)
	queryAccountsFn       func(types.QueryFilter) ([]types.Account, error)
	queryTransfersFn      func(types.QueryFilter) ([]types.Transfer, error)
}

func (c *stubClient) CreateAccounts([]types.Account) ([]types.CreateAccountResult, error) {
	panic("not used by read handlers")
}

func (c *stubClient) CreateTransfers([]types.Transfer) ([]types.CreateTransferResult, error) {
	panic("not used by read handlers")
}

func (c *stubClient) LookupAccounts(ids []types.Uint128) ([]types.Account, error) {
	if c.lookupAccountsFn == nil {
		panic("LookupAccounts unexpectedly called")
	}
	return c.lookupAccountsFn(ids)
}

func (c *stubClient) LookupTransfers(ids []types.Uint128) ([]types.Transfer, error) {
	if c.lookupTransfersFn == nil {
		panic("LookupTransfers unexpectedly called")
	}
	return c.lookupTransfersFn(ids)
}

func (c *stubClient) GetAccountTransfers(f types.AccountFilter) ([]types.Transfer, error) {
	if c.getAccountTransfersFn == nil {
		panic("GetAccountTransfers unexpectedly called")
	}
	return c.getAccountTransfersFn(f)
}

func (c *stubClient) GetAccountBalances(f types.AccountFilter) ([]types.AccountBalance, error) {
	if c.getAccountBalancesFn == nil {
		panic("GetAccountBalances unexpectedly called")
	}
	return c.getAccountBalancesFn(f)
}

func (c *stubClient) QueryAccounts(f types.QueryFilter) ([]types.Account, error) {
	if c.queryAccountsFn == nil {
		panic("QueryAccounts unexpectedly called")
	}
	return c.queryAccountsFn(f)
}

func (c *stubClient) QueryTransfers(f types.QueryFilter) ([]types.Transfer, error) {
	if c.queryTransfersFn == nil {
		panic("QueryTransfers unexpectedly called")
	}
	return c.queryTransfersFn(f)
}

func (c *stubClient) Nop() error { return nil }
func (c *stubClient) Close()     {}

var _ tbx.Client = (*stubClient)(nil)

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	st, ok := status.FromError(err)
	require.True(t, ok, "expected a gRPC status error, got %v", err)
	require.Equal(t, want, st.Code())
}

func TestListAccountTransfersAppliesCursorAndDefaultsLimit(t *testing.T) {
	accountID := types.ToUint128(7)
	var gotFilter types.AccountFilter
	stub := &stubClient{
		getAccountTransfersFn: func(f types.AccountFilter) ([]types.Transfer, error) {
			gotFilter = f
			return []types.Transfer{
				{ID: types.ToUint128(1), Ledger: 1, Code: 1, Amount: types.ToUint128(100), Timestamp: 10},
				{ID: types.ToUint128(2), Ledger: 1, Code: 1, Amount: types.ToUint128(200), Timestamp: 20},
			}, nil
		},
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 50})

	resp, err := srv.ListAccountTransfers(context.Background(), &kafkatbv1.ListAccountTransfersRequest{
		AccountId: model.FormatID(accountID),
		Cursor:    5,
	})

	require.NoError(t, err)
	require.Equal(t, accountID, gotFilter.AccountID)
	require.Equal(t, uint64(5), gotFilter.TimestampMin)
	require.Equal(t, uint32(50), gotFilter.Limit, "limit 0 must default to MaxPageSize")
	require.Len(t, resp.Transfers, 2)
	require.Equal(t, uint64(21), resp.NextCursor, "next_cursor is the last element's timestamp + 1")
}

func TestListAccountTransfersLimitAboveMaxIsInvalidArgument(t *testing.T) {
	srv := NewServer(&stubClient{}, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.ListAccountTransfers(context.Background(), &kafkatbv1.ListAccountTransfersRequest{
		AccountId: model.FormatID(types.ToUint128(1)),
		Limit:     11,
	})

	requireCode(t, err, codes.InvalidArgument)
}

func TestListAccountTransfersInvalidAccountIDIsInvalidArgument(t *testing.T) {
	srv := NewServer(&stubClient{}, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.ListAccountTransfers(context.Background(), &kafkatbv1.ListAccountTransfersRequest{
		AccountId: "not-a-uuid",
	})

	requireCode(t, err, codes.InvalidArgument)
}

func TestListAccountTransfersEmptyPageHasZeroCursor(t *testing.T) {
	stub := &stubClient{
		getAccountTransfersFn: func(types.AccountFilter) ([]types.Transfer, error) { return nil, nil },
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 10})

	resp, err := srv.ListAccountTransfers(context.Background(), &kafkatbv1.ListAccountTransfersRequest{
		AccountId: model.FormatID(types.ToUint128(1)),
	})

	require.NoError(t, err)
	require.Empty(t, resp.Transfers)
	require.Equal(t, uint64(0), resp.NextCursor)
}

func TestListAccountBalancesUsesAccountLedgerScale(t *testing.T) {
	accountID := types.ToUint128(9)
	stub := &stubClient{
		getAccountBalancesFn: func(types.AccountFilter) ([]types.AccountBalance, error) {
			return []types.AccountBalance{
				{DebitsPosted: types.ToUint128(500), CreditsPosted: types.ToUint128(100), Timestamp: 30},
			}, nil
		},
		lookupAccountsFn: func(ids []types.Uint128) ([]types.Account, error) {
			require.Equal(t, []types.Uint128{accountID}, ids)
			return []types.Account{{ID: accountID, Ledger: 1, Code: 1}}, nil
		},
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 10})

	resp, err := srv.ListAccountBalances(context.Background(), &kafkatbv1.ListAccountBalancesRequest{
		AccountId: model.FormatID(accountID),
	})

	require.NoError(t, err)
	require.Len(t, resp.Balances, 1)
	require.Equal(t, "5.00", resp.Balances[0].DebitsPosted)
	require.Equal(t, "1.00", resp.Balances[0].CreditsPosted)
	require.Equal(t, uint64(31), resp.NextCursor)
}

func TestListAccountBalancesEmptyPageSkipsAccountLookup(t *testing.T) {
	lookupCalled := false
	stub := &stubClient{
		getAccountBalancesFn: func(types.AccountFilter) ([]types.AccountBalance, error) { return nil, nil },
		lookupAccountsFn: func([]types.Uint128) ([]types.Account, error) {
			lookupCalled = true
			return nil, nil
		},
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 10})

	resp, err := srv.ListAccountBalances(context.Background(), &kafkatbv1.ListAccountBalancesRequest{
		AccountId: model.FormatID(types.ToUint128(1)),
	})

	require.NoError(t, err)
	require.Empty(t, resp.Balances)
	require.False(t, lookupCalled, "an empty page must not need the account's ledger scale")
}

func TestGetAccountsClientErrorIsUnavailable(t *testing.T) {
	stub := &stubClient{
		lookupAccountsFn: func([]types.Uint128) ([]types.Account, error) {
			return nil, errors.New("connection refused")
		},
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.GetAccounts(context.Background(), &kafkatbv1.GetAccountsRequest{
		Id: []string{model.FormatID(types.ToUint128(1))},
	})

	requireCode(t, err, codes.Unavailable)
}

func TestGetAccountsInvalidIDIsInvalidArgument(t *testing.T) {
	srv := NewServer(&stubClient{}, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.GetAccounts(context.Background(), &kafkatbv1.GetAccountsRequest{Id: []string{"nope"}})

	requireCode(t, err, codes.InvalidArgument)
}

func TestGetTransfersReturnsConvertedTransfers(t *testing.T) {
	id := types.ToUint128(1)
	debit := types.ToUint128(2)
	credit := types.ToUint128(3)
	stub := &stubClient{
		lookupTransfersFn: func(ids []types.Uint128) ([]types.Transfer, error) {
			require.Equal(t, []types.Uint128{id}, ids)
			return []types.Transfer{{
				ID: id, DebitAccountID: debit, CreditAccountID: credit,
				Amount: types.ToUint128(1234), Ledger: 1, Code: 1, Timestamp: 99,
			}}, nil
		},
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 10})

	resp, err := srv.GetTransfers(context.Background(), &kafkatbv1.GetTransfersRequest{
		Id: []string{model.FormatID(id)},
	})

	require.NoError(t, err)
	require.Len(t, resp.Transfers, 1)
	require.Equal(t, "12.34", resp.Transfers[0].Amount)
	require.Equal(t, model.FormatID(debit), resp.Transfers[0].DebitAccountId)
	require.Equal(t, model.FormatID(credit), resp.Transfers[0].CreditAccountId)
}

func TestQueryTransfersResolvesOptionalLedgerAndCode(t *testing.T) {
	var gotFilter types.QueryFilter
	stub := &stubClient{
		queryTransfersFn: func(f types.QueryFilter) ([]types.Transfer, error) {
			gotFilter = f
			return nil, nil
		},
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.QueryTransfers(context.Background(), &kafkatbv1.QueryTransfersRequest{
		Ledger: "USD",
		Code:   "customer",
	})

	require.NoError(t, err)
	require.Equal(t, uint32(1), gotFilter.Ledger)
	require.Equal(t, uint16(1), gotFilter.Code)
}

func TestQueryTransfersEmptyLedgerAndCodeAreUnfiltered(t *testing.T) {
	var gotFilter types.QueryFilter
	stub := &stubClient{
		queryTransfersFn: func(f types.QueryFilter) ([]types.Transfer, error) {
			gotFilter = f
			return nil, nil
		},
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.QueryTransfers(context.Background(), &kafkatbv1.QueryTransfersRequest{})

	require.NoError(t, err)
	require.Equal(t, uint32(0), gotFilter.Ledger)
	require.Equal(t, uint16(0), gotFilter.Code)
}

func TestQueryTransfersUnknownLedgerIsInvalidArgument(t *testing.T) {
	srv := NewServer(&stubClient{}, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.QueryTransfers(context.Background(), &kafkatbv1.QueryTransfersRequest{Ledger: "GBP"})

	requireCode(t, err, codes.InvalidArgument)
}

func TestQueryAccountsUnknownCodeIsInvalidArgument(t *testing.T) {
	srv := NewServer(&stubClient{}, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.QueryAccounts(context.Background(), &kafkatbv1.QueryAccountsRequest{Code: "merchant"})

	requireCode(t, err, codes.InvalidArgument)
}

func TestQueryAccountsClientErrorIsUnavailable(t *testing.T) {
	stub := &stubClient{
		queryAccountsFn: func(types.QueryFilter) ([]types.Account, error) {
			return nil, errors.New("timeout")
		},
	}
	srv := NewServer(stub, testRegistry(), config.API{MaxPageSize: 10})

	_, err := srv.QueryAccounts(context.Background(), &kafkatbv1.QueryAccountsRequest{})

	requireCode(t, err, codes.Unavailable)
}
