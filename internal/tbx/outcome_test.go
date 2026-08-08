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

// The response is dense: results[i] belongs to event i of the batch.
func TestMapTransferResultsPositional(t *testing.T) {
	got, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferCreated, Timestamp: 100},
		{Status: types.TransferExceedsCredits},
		{Status: types.TransferCreated, Timestamp: 102},
	}, 0, 3)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, uint64(100), got[0].Timestamp)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, "exceeds_credits", got[1].Error)
	require.Equal(t, "id-1", got[1].ID)
	require.Equal(t, StatusOK, got[2].Status)
}

// exists is an idempotent repeat, not a rejection.
func TestMapTransferResultsExistsIsSuccess(t *testing.T) {
	got, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferExists},
		{Status: types.TransferCreated},
		{Status: types.TransferCreated},
	}, 0, 3)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Empty(t, got[0].Error)
}

// The command occupies the window [offset, offset+Len) within the overall batch. The window
// ends exactly on the batchSize boundary (10+3=13) — this is also the boundary
// case for the off-by-one: `>` and not `>=` in the window check.
func TestMapTransferResultsHonoursOffset(t *testing.T) {
	results := make([]types.CreateTransferResult, 13)
	for i := range results {
		results[i].Status = types.TransferCreated
	}
	results[11].Status = types.TransferExceedsCredits

	got, err := MapTransferResults(cmd3(), results, 10, 13)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, StatusOK, got[2].Status)
}

// Regression test for Finding 1 (Critical): an empty response for a non-empty
// command is not "everything succeeded" but a contract violation. TigerBeetle returns
// a dense array of exactly the request's length, and an empty array only for an empty
// request, so an empty response when 3 events were expected means this
// response is not about this batch at all.
func TestMapTransferResultsEmptyResultsAgainstNonEmptyBatchIsMismatch(t *testing.T) {
	_, err := MapTransferResults(cmd3(), nil, 0, 3)
	require.ErrorIs(t, err, ErrResultCountMismatch)
}

// offset far outside the batch — the command's window does not fit, even if
// the results length happened to match something else.
func TestMapTransferResultsOffsetFarOutsideBatchIsMismatch(t *testing.T) {
	results := make([]types.CreateTransferResult, 13)
	for i := range results {
		results[i].Status = types.TransferCreated
	}
	_, err := MapTransferResults(cmd3(), results, 999999, 13)
	require.ErrorIs(t, err, ErrResultCountMismatch)
}

// A response longer than the declared batchSize is also a contract violation: someone
// else's or a stale array could coincidentally have ended up longer.
func TestMapTransferResultsResultsLongerThanBatchSizeIsMismatch(t *testing.T) {
	results := make([]types.CreateTransferResult, 13)
	_, err := MapTransferResults(cmd3(), results, 0, 10)
	require.ErrorIs(t, err, ErrResultCountMismatch)
}

// A response of the wrong length is a contract violation: it must not silently diverge.
func TestMapTransferResultsCountMismatch(t *testing.T) {
	_, err := MapTransferResults(cmd3(), []types.CreateTransferResult{
		{Status: types.TransferCreated},
	}, 0, 3)
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
	}, 0, 2)
	require.NoError(t, err)
	require.Equal(t, StatusOK, got[0].Status)
	require.Equal(t, StatusRejected, got[1].Status)
	require.Equal(t, "linked_event_failed", got[1].Error)
}

// Finding 3: consecutive capital letters are one abbreviation, not
// a series of one-letter words.
func TestErrorNameConsecutiveCapitalsAsOneWord(t *testing.T) {
	got := errorName(types.AccountIDMustNotBeZero.String(), "Account")
	require.Equal(t, "id_must_not_be_zero", got)
}

// Finding 3: the default branch of Status.String() (e.g. "CreateTransferStatus(1)")
// does not carry the expected prefix — TrimPrefix silently becomes a no-op. This is not
// a real TigerBeetle status name, so the result must be explicitly
// marked as unknown rather than mimic ordinary snake_case.
func TestErrorNameUnknownStatusIsMarkedExplicitly(t *testing.T) {
	raw := types.CreateTransferStatus(9999).String()
	got := errorName(raw, "Transfer")
	require.Equal(t, "unknown_status_"+raw, got)
}
