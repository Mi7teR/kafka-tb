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

// exists — идемпотентный повтор, а не отказ.
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

// Команда занимает окно [offset, offset+Len) внутри общего батча. Окно
// заканчивается ровно на границе batchSize (10+3=13) — это же граничный
// случай на off-by-one: `>` а не `>=` в проверке окна.
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

// Регрессионный тест на Finding 1 (Critical): пустой ответ для непустой
// команды — не "всё успешно", а нарушение контракта. TigerBeetle возвращает
// плотный массив ровно длины запроса и пустой массив только для пустого
// запроса, так что пустой ответ при ожидаемых 3 событиях означает, что этот
// ответ вообще не про данный батч.
func TestMapTransferResultsEmptyResultsAgainstNonEmptyBatchIsMismatch(t *testing.T) {
	_, err := MapTransferResults(cmd3(), nil, 0, 3)
	require.ErrorIs(t, err, ErrResultCountMismatch)
}

// offset далеко за пределами батча — окно команды не влезает, даже если бы
// длина результатов сошлась с чем-то другим.
func TestMapTransferResultsOffsetFarOutsideBatchIsMismatch(t *testing.T) {
	results := make([]types.CreateTransferResult, 13)
	for i := range results {
		results[i].Status = types.TransferCreated
	}
	_, err := MapTransferResults(cmd3(), results, 999999, 13)
	require.ErrorIs(t, err, ErrResultCountMismatch)
}

// Ответ длиннее заявленного batchSize — тоже нарушение контракта: чужой или
// устаревший массив мог случайно оказаться длиннее.
func TestMapTransferResultsResultsLongerThanBatchSizeIsMismatch(t *testing.T) {
	results := make([]types.CreateTransferResult, 13)
	_, err := MapTransferResults(cmd3(), results, 0, 10)
	require.ErrorIs(t, err, ErrResultCountMismatch)
}

// Ответ не той длины — нарушение контракта: молча разъезжаться нельзя.
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

// Finding 3: последовательные заглавные буквы — это одна аббревиатура, не
// набор однобуквенных слов.
func TestErrorNameConsecutiveCapitalsAsOneWord(t *testing.T) {
	got := errorName(types.AccountIDMustNotBeZero.String(), "Account")
	require.Equal(t, "id_must_not_be_zero", got)
}

// Finding 3: default-ветка Status.String() (например, "CreateTransferStatus(1)")
// не несёт ожидаемого префикса — TrimPrefix молча становится no-op. Это не
// настоящее имя статуса TigerBeetle, поэтому результат должен быть явно
// помечен как неизвестный, а не мимикрировать под обычный snake_case.
func TestErrorNameUnknownStatusIsMarkedExplicitly(t *testing.T) {
	raw := types.CreateTransferStatus(9999).String()
	got := errorName(raw, "Transfer")
	require.Equal(t, "unknown_status_"+raw, got)
}
