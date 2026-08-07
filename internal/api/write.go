package api

import (
	"context"
	"errors"
	"fmt"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// commandFromTransfers переводит запрос в model.Command теми же хелперами,
// что и JSON-декодер (internal/codec/jsonc), — иначе контракт записи
// разъедется между Kafka- и API-путём. Как и декодер, снимает linked с
// последнего элемента: TigerBeetle отклоняет батч, заканчивающийся открытой
// цепочкой.
func (s *Server) commandFromTransfers(in []*kafkatbv1.Transfer) (*model.Command, error) {
	cmd := &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: make([]types.Transfer, len(in)),
		IDs:       make([]string, len(in)),
	}
	for i, pt := range in {
		t, err := s.transferFromProto(pt)
		if err != nil {
			return nil, err
		}
		cmd.Transfers[i] = t
		cmd.IDs[i] = pt.GetId()
	}
	clearLinked(&cmd.Transfers[len(cmd.Transfers)-1].Flags)
	return cmd, nil
}

// transferFromProto mirrors internal/codec/jsonc.Decoder.transfer field for
// field, including the post/void pending-transfer branch: for those flags
// pending_id is required and debit/credit account ids are optional (forwarded
// when present, left zero when omitted — TigerBeetle asserts they match the
// pending transfer's accounts if given). Diverging from that branching would
// let the gRPC/REST write path reject requests the Kafka path accepts, or
// vice versa.
func (s *Server) transferFromProto(pt *kafkatbv1.Transfer) (types.Transfer, error) {
	var t types.Transfer
	var err error
	if t.ID, err = model.ParseID(pt.GetId()); err != nil {
		return t, err
	}
	flags, err := s.reg.TransferFlags(pt.GetFlags())
	if err != nil {
		return t, err
	}
	t.Flags = flags.ToUint16()

	postOrVoid := flags.PostPendingTransfer || flags.VoidPendingTransfer
	if postOrVoid {
		if pt.GetPendingId() == "" {
			return t, fmt.Errorf("pending_id required for post/void")
		}
		if t.PendingID, err = model.ParseID(pt.GetPendingId()); err != nil {
			return t, err
		}
		if pt.GetDebitAccountId() != "" {
			if t.DebitAccountID, err = model.ParseID(pt.GetDebitAccountId()); err != nil {
				return t, err
			}
		}
		if pt.GetCreditAccountId() != "" {
			if t.CreditAccountID, err = model.ParseID(pt.GetCreditAccountId()); err != nil {
				return t, err
			}
		}
	} else {
		if pt.GetPendingId() != "" {
			return t, fmt.Errorf("pending_id only valid with post/void flags")
		}
		if t.DebitAccountID, err = model.ParseID(pt.GetDebitAccountId()); err != nil {
			return t, err
		}
		if t.CreditAccountID, err = model.ParseID(pt.GetCreditAccountId()); err != nil {
			return t, err
		}
	}

	ledger, err := s.reg.Ledger(pt.GetLedger())
	if err != nil {
		return t, err
	}
	t.Ledger = ledger.ID
	if t.Code, err = s.reg.Code(pt.GetCode()); err != nil {
		return t, err
	}
	if t.Amount, err = model.ParseAmount(pt.GetAmount(), ledger.Scale); err != nil {
		return t, err
	}
	if pt.GetUserData_128() != "" {
		if t.UserData128, err = model.ParseID(pt.GetUserData_128()); err != nil {
			return t, err
		}
	}
	t.UserData64 = pt.GetUserData_64()
	t.UserData32 = pt.GetUserData_32()
	t.Timeout = pt.GetTimeout()
	return t, nil
}

// commandFromAccounts — аналог commandFromTransfers для счетов.
func (s *Server) commandFromAccounts(in []*kafkatbv1.Account) (*model.Command, error) {
	cmd := &model.Command{
		Op:       model.OpCreateAccounts,
		Accounts: make([]types.Account, len(in)),
		IDs:      make([]string, len(in)),
	}
	for i, pa := range in {
		a, err := s.accountFromProto(pa)
		if err != nil {
			return nil, err
		}
		cmd.Accounts[i] = a
		cmd.IDs[i] = pa.GetId()
	}
	clearLinked(&cmd.Accounts[len(cmd.Accounts)-1].Flags)
	return cmd, nil
}

func (s *Server) accountFromProto(pa *kafkatbv1.Account) (types.Account, error) {
	var a types.Account
	var err error
	if a.ID, err = model.ParseID(pa.GetId()); err != nil {
		return a, err
	}
	ledger, err := s.reg.Ledger(pa.GetLedger())
	if err != nil {
		return a, err
	}
	a.Ledger = ledger.ID
	if a.Code, err = s.reg.Code(pa.GetCode()); err != nil {
		return a, err
	}
	flags, err := s.reg.AccountFlags(pa.GetFlags())
	if err != nil {
		return a, err
	}
	a.Flags = flags.ToUint16()
	if pa.GetUserData_128() != "" {
		if a.UserData128, err = model.ParseID(pa.GetUserData_128()); err != nil {
			return a, err
		}
	}
	a.UserData64 = pa.GetUserData_64()
	a.UserData32 = pa.GetUserData_32()
	return a, nil
}

// linked — младший бит и у TransferFlags, и у AccountFlags (см.
// internal/codec/jsonc/decoder.go).
const linkedBit uint16 = 1

func clearLinked(flags *uint16) { *flags &^= linkedBit }

// writeResults переводит tbx.Outcome в проводной WriteResult. Порядок
// исходов совпадает с порядком запроса — Batcher/MapTransferResults
// гарантируют это по построению (newOutcomes индексирует по cmd.IDs).
func writeResults(outcomes []tbx.Outcome) []*kafkatbv1.WriteResult {
	out := make([]*kafkatbv1.WriteResult, len(outcomes))
	for i, o := range outcomes {
		out[i] = &kafkatbv1.WriteResult{
			Id:     o.ID,
			Status: string(o.Status),
			Error:  o.Error,
		}
	}
	return out
}

// submitErr классифицирует ошибку Submit: слишком большой батч — ошибка
// вызывающего (InvalidArgument), любая другая — недоступность TigerBeetle.
// Бизнес-отказ отдельного элемента сюда не попадает — он приходит в
// outcomes с err == nil.
func submitErr(err error) error {
	if errors.Is(err, tbx.ErrCommandTooLarge) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}

// CreateTransfers is not a plain pass-through to TigerBeetle: a business
// rejection (insufficient funds, unknown account) is not a transport error.
// It returns 200/OK with a per-element outcome in the response body. Only a
// malformed request is InvalidArgument, and only TigerBeetle being
// unreachable is Unavailable.
func (s *Server) CreateTransfers(ctx context.Context, in *kafkatbv1.CreateTransfersRequest) (*kafkatbv1.CreateTransfersResponse, error) {
	if len(in.GetTransfers()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "transfers: empty")
	}
	cmd, err := s.commandFromTransfers(in.GetTransfers())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	outcomes, err := s.sub.Submit(ctx, cmd)
	if err != nil {
		return nil, submitErr(err)
	}
	return &kafkatbv1.CreateTransfersResponse{Results: writeResults(outcomes)}, nil
}

// CreateAccounts — тот же контракт, что CreateTransfers, для счетов.
func (s *Server) CreateAccounts(ctx context.Context, in *kafkatbv1.CreateAccountsRequest) (*kafkatbv1.CreateAccountsResponse, error) {
	if len(in.GetAccounts()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "accounts: empty")
	}
	cmd, err := s.commandFromAccounts(in.GetAccounts())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	outcomes, err := s.sub.Submit(ctx, cmd)
	if err != nil {
		return nil, submitErr(err)
	}
	return &kafkatbv1.CreateAccountsResponse{Results: writeResults(outcomes)}, nil
}
