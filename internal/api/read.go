package api

import (
	"context"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// parseIDs преобразует UUID-строки запроса в Uint128. Первая непарсящаяся
// строка — InvalidArgument, а не паника или молчаливый пропуск.
func parseIDs(ss []string) ([]types.Uint128, error) {
	ids := make([]types.Uint128, len(ss))
	for i, s := range ss {
		id, err := model.ParseID(s)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		ids[i] = id
	}
	return ids, nil
}

// resolveLimit применяет потолок cfg.MaxPageSize и подставляет его же как
// значение по умолчанию, если клиент лимит не указал.
func (s *Server) resolveLimit(limit uint32) (uint32, error) {
	if limit > s.cfg.MaxPageSize {
		return 0, status.Errorf(codes.InvalidArgument,
			"limit %d exceeds max_page_size %d", limit, s.cfg.MaxPageSize)
	}
	if limit == 0 {
		return s.cfg.MaxPageSize, nil
	}
	return limit, nil
}

// nextTransferCursor выставляет next_cursor как timestamp последнего
// возвращённого элемента + 1; пустая страница даёт курсор 0.
func nextTransferCursor(transfers []types.Transfer) uint64 {
	if len(transfers) == 0 {
		return 0
	}
	return transfers[len(transfers)-1].Timestamp + 1
}

func nextAccountCursor(accounts []types.Account) uint64 {
	if len(accounts) == 0 {
		return 0
	}
	return accounts[len(accounts)-1].Timestamp + 1
}

func nextBalanceCursor(balances []types.AccountBalance) uint64 {
	if len(balances) == 0 {
		return 0
	}
	return balances[len(balances)-1].Timestamp + 1
}

func unavailable(err error) error {
	return status.Errorf(codes.Unavailable, "tigerbeetle: %v", err)
}

func invalidArgument(err error) error {
	return status.Error(codes.InvalidArgument, err.Error())
}

func (s *Server) GetAccounts(_ context.Context, req *kafkatbv1.GetAccountsRequest) (*kafkatbv1.GetAccountsResponse, error) {
	ids, err := parseIDs(req.GetId())
	if err != nil {
		return nil, err
	}
	accounts, err := s.c.LookupAccounts(ids)
	if err != nil {
		return nil, unavailable(err)
	}
	out := make([]*kafkatbv1.Account, 0, len(accounts))
	for _, a := range accounts {
		p, err := accountToProto(a, s.reg)
		if err != nil {
			return nil, invalidArgument(err)
		}
		out = append(out, p)
	}
	return &kafkatbv1.GetAccountsResponse{Accounts: out}, nil
}

func (s *Server) GetTransfers(_ context.Context, req *kafkatbv1.GetTransfersRequest) (*kafkatbv1.GetTransfersResponse, error) {
	ids, err := parseIDs(req.GetId())
	if err != nil {
		return nil, err
	}
	transfers, err := s.c.LookupTransfers(ids)
	if err != nil {
		return nil, unavailable(err)
	}
	out := make([]*kafkatbv1.Transfer, 0, len(transfers))
	for _, t := range transfers {
		p, err := transferToProto(t, s.reg)
		if err != nil {
			return nil, invalidArgument(err)
		}
		out = append(out, p)
	}
	return &kafkatbv1.GetTransfersResponse{Transfers: out}, nil
}

func (s *Server) ListAccountTransfers(_ context.Context, req *kafkatbv1.ListAccountTransfersRequest) (*kafkatbv1.ListAccountTransfersResponse, error) {
	accountID, err := model.ParseID(req.GetAccountId())
	if err != nil {
		return nil, invalidArgument(err)
	}
	limit, err := s.resolveLimit(req.GetLimit())
	if err != nil {
		return nil, err
	}
	filter := types.AccountFilter{
		AccountID:    accountID,
		TimestampMin: req.GetCursor(),
		Limit:        limit,
		Flags: types.AccountFilterFlags{
			Debits: true, Credits: true, Reversed: req.GetReversed(),
		}.ToUint32(),
	}
	transfers, err := s.c.GetAccountTransfers(filter)
	if err != nil {
		return nil, unavailable(err)
	}
	out := make([]*kafkatbv1.Transfer, 0, len(transfers))
	for _, t := range transfers {
		p, err := transferToProto(t, s.reg)
		if err != nil {
			return nil, invalidArgument(err)
		}
		out = append(out, p)
	}
	return &kafkatbv1.ListAccountTransfersResponse{
		Transfers:  out,
		NextCursor: nextTransferCursor(transfers),
	}, nil
}

// ListAccountBalances возвращает историю баланса счёта. AccountBalance не
// несёт ledger, поэтому масштаб для форматирования сумм берётся из самого
// счёта — но только когда есть что форматировать: пустая страница не требует
// LookupAccounts, а на непустой странице сам факт наличия history-записей
// означает, что счёт существует.
func (s *Server) ListAccountBalances(_ context.Context, req *kafkatbv1.ListAccountBalancesRequest) (*kafkatbv1.ListAccountBalancesResponse, error) {
	accountID, err := model.ParseID(req.GetAccountId())
	if err != nil {
		return nil, invalidArgument(err)
	}
	limit, err := s.resolveLimit(req.GetLimit())
	if err != nil {
		return nil, err
	}
	filter := types.AccountFilter{
		AccountID:    accountID,
		TimestampMin: req.GetCursor(),
		Limit:        limit,
		Flags: types.AccountFilterFlags{
			Debits: true, Credits: true, Reversed: req.GetReversed(),
		}.ToUint32(),
	}
	balances, err := s.c.GetAccountBalances(filter)
	if err != nil {
		return nil, unavailable(err)
	}
	if len(balances) == 0 {
		return &kafkatbv1.ListAccountBalancesResponse{}, nil
	}
	accounts, err := s.c.LookupAccounts([]types.Uint128{accountID})
	if err != nil {
		return nil, unavailable(err)
	}
	if len(accounts) == 0 {
		return nil, status.Error(codes.Internal, "account balances exist but account was not found")
	}
	scale, err := s.reg.ScaleByLedgerID(accounts[0].Ledger)
	if err != nil {
		return nil, invalidArgument(err)
	}
	out := make([]*kafkatbv1.Balance, len(balances))
	for i, b := range balances {
		out[i] = balanceToProto(b, scale)
	}
	return &kafkatbv1.ListAccountBalancesResponse{
		Balances:   out,
		NextCursor: nextBalanceCursor(balances),
	}, nil
}

func (s *Server) QueryTransfers(_ context.Context, req *kafkatbv1.QueryTransfersRequest) (*kafkatbv1.QueryTransfersResponse, error) {
	filter, err := s.buildQueryFilter(req.GetUserData_128(), req.GetUserData_64(), req.GetUserData_32(),
		req.GetLedger(), req.GetCode(), req.GetLimit(), req.GetCursor())
	if err != nil {
		return nil, err
	}
	transfers, err := s.c.QueryTransfers(filter)
	if err != nil {
		return nil, unavailable(err)
	}
	out := make([]*kafkatbv1.Transfer, 0, len(transfers))
	for _, t := range transfers {
		p, err := transferToProto(t, s.reg)
		if err != nil {
			return nil, invalidArgument(err)
		}
		out = append(out, p)
	}
	return &kafkatbv1.QueryTransfersResponse{
		Transfers:  out,
		NextCursor: nextTransferCursor(transfers),
	}, nil
}

func (s *Server) QueryAccounts(_ context.Context, req *kafkatbv1.QueryAccountsRequest) (*kafkatbv1.QueryAccountsResponse, error) {
	filter, err := s.buildQueryFilter(req.GetUserData_128(), req.GetUserData_64(), req.GetUserData_32(),
		req.GetLedger(), req.GetCode(), req.GetLimit(), req.GetCursor())
	if err != nil {
		return nil, err
	}
	accounts, err := s.c.QueryAccounts(filter)
	if err != nil {
		return nil, unavailable(err)
	}
	out := make([]*kafkatbv1.Account, 0, len(accounts))
	for _, a := range accounts {
		p, err := accountToProto(a, s.reg)
		if err != nil {
			return nil, invalidArgument(err)
		}
		out = append(out, p)
	}
	return &kafkatbv1.QueryAccountsResponse{
		Accounts:   out,
		NextCursor: nextAccountCursor(accounts),
	}, nil
}

// buildQueryFilter — общая часть QueryTransfers/QueryAccounts. ledger и code
// необязательны: пустая строка оставляет соответствующее поле фильтра
// нулевым, что TigerBeetle трактует как "любой".
func (s *Server) buildQueryFilter(userData128 string, userData64 uint64, userData32 uint32,
	ledger, code string, limit uint32, cursor uint64,
) (types.QueryFilter, error) {
	var userData types.Uint128
	if userData128 != "" {
		id, err := model.ParseID(userData128)
		if err != nil {
			return types.QueryFilter{}, invalidArgument(err)
		}
		userData = id
	}
	var ledgerID uint32
	if ledger != "" {
		l, err := s.reg.Ledger(ledger)
		if err != nil {
			return types.QueryFilter{}, invalidArgument(err)
		}
		ledgerID = l.ID
	}
	var codeID uint16
	if code != "" {
		c, err := s.reg.Code(code)
		if err != nil {
			return types.QueryFilter{}, invalidArgument(err)
		}
		codeID = c
	}
	limit, err := s.resolveLimit(limit)
	if err != nil {
		return types.QueryFilter{}, err
	}
	return types.QueryFilter{
		UserData128:  userData,
		UserData64:   userData64,
		UserData32:   userData32,
		Ledger:       ledgerID,
		Code:         codeID,
		TimestampMin: cursor,
		Limit:        limit,
	}, nil
}
