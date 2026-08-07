package tbx

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func cmd3() *model.Command {
	return &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: make([]types.Transfer, 3),
		IDs:       []string{"id-0", "id-1", "id-2"},
	}
}

// Ответ плотный: results[i] относится к событию i батча.
func TestMapTransferResultsPositional(t *testing.T) {
	got, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferCreated, Timestamp: 100},
		{Status: types.TransferExceedsCredits},
		{Status: types.TransferCreated, Timestamp: 102},
	}, 0)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, uint64(100), got[0].Timestamp)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, "exceeds_credits", got[1].Error)
	require.Equal(t, "id-1", got[1].ID)
	require.Equal(t, StatusOK, got[2].Status)
}

// exists — идемпотентный повтор, а не отказ.
func TestMapTransferResultsExistsIsSuccess(t *testing.T) {
	got, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferExists},
		{Status: types.TransferCreated},
		{Status: types.TransferCreated},
	}, 0)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Empty(t, got[0].Error)
}

// Команда занимает окно [offset, offset+Len) внутри общего батча.
func TestMapTransferResultsHonoursOffset(t *testing.T) {
	results := make([]types.CreateTransferResult, 13)
	for i := range results {
		results[i].Status = types.TransferCreated
	}
	results[11].Status = types.TransferExceedsCredits

	got, err := MapTransferResults(cmd3(), results, 10)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, StatusOK, got[2].Status)
}

// Пустой ответ означает, что успешны все события.
func TestMapTransferResultsEmptyMeansAllOK(t *testing.T) {
	got, err := MapTransferResults(cmd3(), nil, 0)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for _, o := range got {
		require.Equal(t, StatusOK, o.Status)
	}
}

// Ответ не той длины — нарушение контракта: молча разъезжаться нельзя.
func TestMapTransferResultsCountMismatch(t *testing.T) {
	_, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferCreated},
	}, 0)
	require.ErrorIs(t, err, ErrResultCountMismatch)
}

func TestMapAccountResults(t *testing.T) {
	c := &model.Command{
		Op:       model.OpCreateAccounts,
		Accounts: make([]types.Account, 2),
		IDs:      []string{"a-0", "a-1"},
	}
	got, err := MapAccountResults(c, []types.CreateAccountResult{
		{Status: types.AccountExists},
		{Status: types.AccountLinkedEventFailed},
	}, 0)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, "linked_event_failed", got[1].Error)
}
