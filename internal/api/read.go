package api

import (
	"context"
	"math"

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

// cursorBounds направляет курсор в тот конец диапазона [TimestampMin,
// TimestampMax], который в данном направлении сужается: вперёд — нижняя
// граница, назад — верхняя. Курсор 0 в обеих полях означает "без границы"
// для TigerBeetle, что естественно совпадает с первой страницей, когда
// клиент курсор ещё не передавал.
func cursorBounds(cursor uint64, reversed bool) (min, max uint64) {
	if reversed {
		return 0, cursor
	}
	return cursor, 0
}

// nextCursor выставляет курсор следующей страницы по timestamp последнего
// возвращённого элемента: вперёд +1, назад −1. Курсор 0 на выходе значит
// "страниц больше нет" — это и естественный случай пустой страницы, и явно
// обработанная граница uint64 (вперёд upon MaxUint64, назад upon 0), где
// шаг увёл бы курсор за пределы типа.
func nextCursor(lastTimestamp uint64, reversed bool) uint64 {
	if reversed {
		if lastTimestamp == 0 {
			return 0
		}
		return lastTimestamp - 1
	}
	if lastTimestamp == math.MaxUint64 {
		return 0
	}
	return lastTimestamp + 1
}

func nextTransferCursor(transfers []types.Transfer, reversed bool) uint64 {
	if len(transfers) == 0 {
		return 0
	}
	return nextCursor(transfers[len(transfers)-1].Timestamp, reversed)
}

func nextAccountCursor(accounts []types.Account, reversed bool) uint64 {
	if len(accounts) == 0 {
		return 0
	}
	return nextCursor(accounts[len(accounts)-1].Timestamp, reversed)
}

func nextBalanceCursor(balances []types.AccountBalance, reversed bool) uint64 {
	if len(balances) == 0 {
		return 0
	}
	return nextCursor(balances[len(balances)-1].Timestamp, reversed)
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
	timestampMin, timestampMax := cursorBounds(req.GetCursor(), req.GetReversed())
	filter := types.AccountFilter{
		AccountID:    accountID,
		TimestampMin: timestampMin,
		TimestampMax: timestampMax,
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
		NextCursor: nextTransferCursor(transfers, req.GetReversed()),
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
	timestampMin, timestampMax := cursorBounds(req.GetCursor(), req.GetReversed())
	filter := types.AccountFilter{
		AccountID:    accountID,
		TimestampMin: timestampMin,
		TimestampMax: timestampMax,
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
		NextCursor: nextBalanceCursor(balances, req.GetReversed()),
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
		Transfers: out,
		// QueryTransfersRequest has no reversed field: this filter is always
		// forward, so the cursor always advances TimestampMin.
		NextCursor: nextTransferCursor(transfers, false),
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
		Accounts: out,
		// QueryAccountsRequest has no reversed field: this filter is always
		// forward, so the cursor always advances TimestampMin.
		NextCursor: nextAccountCursor(accounts, false),
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
