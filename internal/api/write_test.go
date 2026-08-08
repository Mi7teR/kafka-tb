package api

import (
	"context"
	"testing"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stubSubmitter struct {
	outcomes []tbx.Outcome
	err      error
	got      *model.Command
}

func (s *stubSubmitter) Submit(_ context.Context, cmd *model.Command) ([]tbx.Outcome, error) {
	s.got = cmd
	return s.outcomes, s.err
}

func newTestServer(sub Submitter) *Server {
	return NewServer(nil, sub, testRegistry(), config.API{MaxPageSize: 1000}, testLimits())
}

func req() *kafkatbv1.CreateTransfersRequest {
	return &kafkatbv1.CreateTransfersRequest{Transfers: []*kafkatbv1.Transfer{{
		Id:              "0193f8a1-7c2e-7000-8000-000000000001",
		DebitAccountId:  "0193f8a1-0000-7000-8000-000000000010",
		CreditAccountId: "0193f8a1-0000-7000-8000-000000000020",
		Amount:          "12.34", Ledger: "USD", Code: "customer",
	}}}
}

// Бизнес-отказ — не ошибка транспорта: 200 с исходом по элементу.
func TestCreateTransfersReturnsRejectionInBody(t *testing.T) {
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "0193f8a1-7c2e-7000-8000-000000000001", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newTestServer(sub)
	resp, err := s.CreateTransfers(context.Background(), req())
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Equal(t, "rejected", resp.Results[0].Status)
	require.Equal(t, "exceeds_credits", resp.Results[0].Error)
}

func TestCreateTransfersInvalidInputIsInvalidArgument(t *testing.T) {
	s := newTestServer(&stubSubmitter{})
	r := req()
	r.Transfers[0].Amount = "12.345"
	_, err := s.CreateTransfers(context.Background(), r)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCreateTransfersInfraErrorIsUnavailable(t *testing.T) {
	s := newTestServer(&stubSubmitter{err: context.DeadlineExceeded})
	_, err := s.CreateTransfers(context.Background(), req())
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestCreateTransfersEmptyIsInvalidArgument(t *testing.T) {
	s := newTestServer(&stubSubmitter{})
	_, err := s.CreateTransfers(context.Background(), &kafkatbv1.CreateTransfersRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// Смешанный запрос: один из нескольких переводов отклонён TigerBeetle.
// Исход должен приземлиться на правильный id, а не быть общим отказом на
// весь запрос.
func TestCreateTransfersMixedOutcomesLandOnRightID(t *testing.T) {
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "0193f8a1-7c2e-7000-8000-000000000001", Status: tbx.StatusOK},
		{Index: 1, ID: "0193f8a1-7c2e-7000-8000-000000000002", Status: tbx.StatusRejected, Error: "exceeds_credits"},
		{Index: 2, ID: "0193f8a1-7c2e-7000-8000-000000000003", Status: tbx.StatusOK},
	}}
	s := newTestServer(sub)
	r := &kafkatbv1.CreateTransfersRequest{Transfers: []*kafkatbv1.Transfer{
		{
			Id: "0193f8a1-7c2e-7000-8000-000000000001", DebitAccountId: "0193f8a1-0000-7000-8000-000000000010",
			CreditAccountId: "0193f8a1-0000-7000-8000-000000000020", Amount: "12.34", Ledger: "USD", Code: "customer",
		},
		{
			Id: "0193f8a1-7c2e-7000-8000-000000000002", DebitAccountId: "0193f8a1-0000-7000-8000-000000000010",
			CreditAccountId: "0193f8a1-0000-7000-8000-000000000020", Amount: "1.00", Ledger: "USD", Code: "customer",
		},
		{
			Id: "0193f8a1-7c2e-7000-8000-000000000003", DebitAccountId: "0193f8a1-0000-7000-8000-000000000010",
			CreditAccountId: "0193f8a1-0000-7000-8000-000000000020", Amount: "5.00", Ledger: "USD", Code: "customer",
		},
	}}

	resp, err := s.CreateTransfers(context.Background(), r)

	require.NoError(t, err)
	require.Len(t, resp.Results, 3)
	require.Equal(t, "0193f8a1-7c2e-7000-8000-000000000001", resp.Results[0].Id)
	require.Equal(t, "ok", resp.Results[0].Status)
	require.Equal(t, "0193f8a1-7c2e-7000-8000-000000000002", resp.Results[1].Id)
	require.Equal(t, "rejected", resp.Results[1].Status)
	require.Equal(t, "exceeds_credits", resp.Results[1].Error)
	require.Equal(t, "0193f8a1-7c2e-7000-8000-000000000003", resp.Results[2].Id)
	require.Equal(t, "ok", resp.Results[2].Status)
	// The last element of the batch must have linked cleared so TigerBeetle
	// never sees a chain left open at the end of a command.
	require.False(t, sub.got.Transfers[len(sub.got.Transfers)-1].TransferFlags().Linked)
}

func accountReq() *kafkatbv1.CreateAccountsRequest {
	return &kafkatbv1.CreateAccountsRequest{Accounts: []*kafkatbv1.Account{{
		Id: "0193f8a1-7c2e-7000-8000-000000000001", Ledger: "USD", Code: "customer",
	}}}
}

func TestCreateAccountsReturnsRejectionInBody(t *testing.T) {
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "0193f8a1-7c2e-7000-8000-000000000001", Status: tbx.StatusRejected, Error: "exists"},
	}}
	s := newTestServer(sub)
	resp, err := s.CreateAccounts(context.Background(), accountReq())
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	require.Equal(t, "rejected", resp.Results[0].Status)
	require.Equal(t, "exists", resp.Results[0].Error)
}

func TestCreateAccountsInvalidInputIsInvalidArgument(t *testing.T) {
	s := newTestServer(&stubSubmitter{})
	r := accountReq()
	r.Accounts[0].Ledger = "unknown-ledger"
	_, err := s.CreateAccounts(context.Background(), r)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCreateAccountsInfraErrorIsUnavailable(t *testing.T) {
	s := newTestServer(&stubSubmitter{err: context.DeadlineExceeded})
	_, err := s.CreateAccounts(context.Background(), accountReq())
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestCreateAccountsEmptyIsInvalidArgument(t *testing.T) {
	s := newTestServer(&stubSubmitter{})
	_, err := s.CreateAccounts(context.Background(), &kafkatbv1.CreateAccountsRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCreateTransfersTooLargeIsInvalidArgument(t *testing.T) {
	s := newTestServer(&stubSubmitter{err: tbx.ErrCommandTooLarge})
	_, err := s.CreateTransfers(context.Background(), req())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// F2: limits.max_events_per_message must gate the API's command-building
// path the same way it gates internal/codec/jsonc.Decoder on the Kafka
// door — otherwise Batcher.Submit's MaxBatchSize is the API's only ceiling,
// and a tuned-down limits config could leave the API accepting an event
// count Kafka would reject as poison.
func TestCreateTransfersOverEventLimitIsInvalidArgument(t *testing.T) {
	s := NewServer(nil, &stubSubmitter{}, testRegistry(), config.API{MaxPageSize: 10},
		config.Limits{MaxEventsPerMessage: 2, MaxMessageBytes: 1 << 20})
	r := &kafkatbv1.CreateTransfersRequest{Transfers: []*kafkatbv1.Transfer{
		{
			Id: "0193f8a1-7c2e-7000-8000-000000000001", DebitAccountId: "0193f8a1-0000-7000-8000-000000000010",
			CreditAccountId: "0193f8a1-0000-7000-8000-000000000020", Amount: "1.00", Ledger: "USD", Code: "customer",
		},
		{
			Id: "0193f8a1-7c2e-7000-8000-000000000002", DebitAccountId: "0193f8a1-0000-7000-8000-000000000010",
			CreditAccountId: "0193f8a1-0000-7000-8000-000000000020", Amount: "1.00", Ledger: "USD", Code: "customer",
		},
		{
			Id: "0193f8a1-7c2e-7000-8000-000000000003", DebitAccountId: "0193f8a1-0000-7000-8000-000000000010",
			CreditAccountId: "0193f8a1-0000-7000-8000-000000000020", Amount: "1.00", Ledger: "USD", Code: "customer",
		},
	}}

	_, err := s.CreateTransfers(context.Background(), r)

	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// F2 mirror for CreateAccounts.
func TestCreateAccountsOverEventLimitIsInvalidArgument(t *testing.T) {
	s := NewServer(nil, &stubSubmitter{}, testRegistry(), config.API{MaxPageSize: 10},
		config.Limits{MaxEventsPerMessage: 2, MaxMessageBytes: 1 << 20})
	r := &kafkatbv1.CreateAccountsRequest{Accounts: []*kafkatbv1.Account{
		{Id: "0193f8a1-7c2e-7000-8000-000000000001", Ledger: "USD", Code: "customer"},
		{Id: "0193f8a1-7c2e-7000-8000-000000000002", Ledger: "USD", Code: "customer"},
		{Id: "0193f8a1-7c2e-7000-8000-000000000003", Ledger: "USD", Code: "customer"},
	}}

	_, err := s.CreateAccounts(context.Background(), r)

	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// F4: a rejected outcome must carry a human-readable WriteResult.detail —
// the field must not ship permanently empty. A successful outcome has
// nothing to explain, so detail stays empty there.
func TestCreateTransfersRejectionPopulatesDetail(t *testing.T) {
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "0193f8a1-7c2e-7000-8000-000000000001", Status: tbx.StatusOK},
		{Index: 1, ID: "0193f8a1-7c2e-7000-8000-000000000002", Status: tbx.StatusRejected, Error: "exceeds_credits"},
	}}
	s := newTestServer(sub)
	r := &kafkatbv1.CreateTransfersRequest{Transfers: []*kafkatbv1.Transfer{
		{
			Id: "0193f8a1-7c2e-7000-8000-000000000001", DebitAccountId: "0193f8a1-0000-7000-8000-000000000010",
			CreditAccountId: "0193f8a1-0000-7000-8000-000000000020", Amount: "1.00", Ledger: "USD", Code: "customer",
		},
		{
			Id: "0193f8a1-7c2e-7000-8000-000000000002", DebitAccountId: "0193f8a1-0000-7000-8000-000000000010",
			CreditAccountId: "0193f8a1-0000-7000-8000-000000000020", Amount: "1.00", Ledger: "USD", Code: "customer",
		},
	}}

	resp, err := s.CreateTransfers(context.Background(), r)

	require.NoError(t, err)
	require.Empty(t, resp.Results[0].Detail, "a successful outcome has nothing to explain")
	require.Contains(t, resp.Results[1].Detail, "0193f8a1-7c2e-7000-8000-000000000002")
	require.Contains(t, resp.Results[1].Detail, "exceeds credits")
}

// TestCreateTransfersPostPendingTransferAllowsOmittedAccountIDs pins parity
// with internal/codec/jsonc.Decoder: for post_pending_transfer/
// void_pending_transfer, debit/credit account ids are optional (TigerBeetle
// takes them from the referenced pending transfer), only pending_id is
// required. Diverging here would make the API write path reject a request
// the Kafka path accepts.
func TestCreateTransfersPostPendingTransferAllowsOmittedAccountIDs(t *testing.T) {
	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "0193f8a1-7c2e-7000-8000-000000000001", Status: tbx.StatusOK},
	}}
	s := newTestServer(sub)
	r := &kafkatbv1.CreateTransfersRequest{Transfers: []*kafkatbv1.Transfer{{
		Id:        "0193f8a1-7c2e-7000-8000-000000000001",
		PendingId: "0193f8a1-7c2e-7000-8000-000000000099",
		Amount:    "12.34", Ledger: "USD", Code: "customer",
		Flags: []string{"post_pending_transfer"},
	}}}

	_, err := s.CreateTransfers(context.Background(), r)

	require.NoError(t, err)
}

// TestCreateTransfersPendingIDRequiredForPostPendingTransfer is the mirror
// case: post_pending_transfer without pending_id must be InvalidArgument,
// same as the decoder.
func TestCreateTransfersPendingIDRequiredForPostPendingTransfer(t *testing.T) {
	s := newTestServer(&stubSubmitter{})
	r := &kafkatbv1.CreateTransfersRequest{Transfers: []*kafkatbv1.Transfer{{
		Id:     "0193f8a1-7c2e-7000-8000-000000000001",
		Amount: "12.34", Ledger: "USD", Code: "customer",
		Flags: []string{"post_pending_transfer"},
	}}}

	_, err := s.CreateTransfers(context.Background(), r)

	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// Два независимых запроса к API не упорядочены между собой и никогда не были.
// Пустой ключ порядка — это утверждение именно об этом: батчер раздаёт такие
// команды разным воркерам по кругу, вместо того чтобы выстраивать весь API в
// одну очередь за чьей-то партицией.
func TestWritePathLeavesOrderKeyEmpty(t *testing.T) {
	sub := &stubSubmitter{}
	s := newTestServer(sub)

	_, err := s.CreateTransfers(context.Background(), req())
	require.NoError(t, err)
	require.Empty(t, sub.got.Key, "запрос API не упорядочен ни с чем")

	_, err = s.CreateAccounts(context.Background(), accountReq())
	require.NoError(t, err)
	require.Empty(t, sub.got.Key, "запрос API не упорядочен ни с чем")
}
