package api

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func testRegistry() *model.Registry {
	return model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"customer": 1},
	})
}

// testLimits is a generous limits.* config for tests that don't specifically
// exercise F2's ceilings — large enough that no test payload trips it by
// accident.
func testLimits() config.Limits {
	return config.Limits{MaxEventsPerMessage: 1000, MaxMessageBytes: 1 << 20}
}

// Кредитовый счёт: баланс = credits - debits.
func TestAccountBalanceCreditNormal(t *testing.T) {
	acc := types.Account{
		ID: types.ToUint128(1), Ledger: 1, Code: 1,
		DebitsPosted: types.ToUint128(125000), CreditsPosted: types.ToUint128(140000),
	}
	got, err := accountToProto(acc, testRegistry())
	require.NoError(t, err)
	require.Equal(t, "150.00", got.Balance)
	require.Equal(t, "1250.00", got.DebitsPosted)
	require.Equal(t, "USD", got.Ledger)
	require.Equal(t, "customer", got.Code)
}

// Дебетовый счёт (credits_must_not_exceed_debits): баланс = debits - credits.
func TestAccountBalanceDebitNormal(t *testing.T) {
	acc := types.Account{
		ID: types.ToUint128(1), Ledger: 1, Code: 1,
		Flags:        types.AccountFlags{CreditsMustNotExceedDebits: true}.ToUint16(),
		DebitsPosted: types.ToUint128(140000), CreditsPosted: types.ToUint128(125000),
	}
	got, err := accountToProto(acc, testRegistry())
	require.NoError(t, err)
	require.Equal(t, "150.00", got.Balance)
	require.Contains(t, got.Flags, "credits_must_not_exceed_debits")
}

// I2 regression: an account that was imported must surface "imported" in
// its flags, not be misreported as though it were not imported.
func TestAccountImportedFlagIsSurfaced(t *testing.T) {
	acc := types.Account{
		ID: types.ToUint128(1), Ledger: 1, Code: 1,
		Flags: types.AccountFlags{Imported: true}.ToUint16(),
	}
	got, err := accountToProto(acc, testRegistry())
	require.NoError(t, err)
	require.Contains(t, got.Flags, "imported")
}

func TestAccountUnknownLedgerIsError(t *testing.T) {
	_, err := accountToProto(types.Account{Ledger: 99, Code: 1}, testRegistry())
	require.ErrorContains(t, err, "unknown ledger")
}

// Кредитовый счёт с отрицательным итогом (долг): результат ведущий минус.
func TestAccountBalanceCreditNormalNegative(t *testing.T) {
	acc := types.Account{
		ID: types.ToUint128(1), Ledger: 1, Code: 1,
		DebitsPosted: types.ToUint128(140000), CreditsPosted: types.ToUint128(125000),
	}
	got, err := accountToProto(acc, testRegistry())
	require.NoError(t, err)
	require.Equal(t, "-150.00", got.Balance)
}

// Дебетовый счёт (credits_must_not_exceed_debits) с отрицательным итогом.
func TestAccountBalanceDebitNormalNegative(t *testing.T) {
	acc := types.Account{
		ID: types.ToUint128(1), Ledger: 1, Code: 1,
		Flags:        types.AccountFlags{CreditsMustNotExceedDebits: true}.ToUint16(),
		DebitsPosted: types.ToUint128(125000), CreditsPosted: types.ToUint128(140000),
	}
	got, err := accountToProto(acc, testRegistry())
	require.NoError(t, err)
	require.Equal(t, "-150.00", got.Balance)
}

func TestTransferToProtoRoundTrip(t *testing.T) {
	id, _ := model.ParseID("00000000-0000-0000-0000-000000000001")
	debit, _ := model.ParseID("00000000-0000-0000-0000-000000000002")
	credit, _ := model.ParseID("00000000-0000-0000-0000-000000000003")
	tr := types.Transfer{
		ID: id, DebitAccountID: debit, CreditAccountID: credit,
		Amount: types.ToUint128(12345), Ledger: 1, Code: 1,
		Flags:     types.TransferFlags{Pending: true}.ToUint16(),
		Timestamp: 42,
	}
	got, err := transferToProto(tr, testRegistry())
	require.NoError(t, err)
	require.Equal(t, "123.45", got.Amount)
	require.Equal(t, "USD", got.Ledger)
	require.Equal(t, "customer", got.Code)
	require.Contains(t, got.Flags, "pending")
	require.Equal(t, uint64(42), got.Timestamp)
}

// I2 regression: a transfer that was imported must surface "imported" in
// its flags, not be misreported as though it were not imported.
func TestTransferImportedFlagIsSurfaced(t *testing.T) {
	tr := types.Transfer{
		Ledger: 1, Code: 1,
		Flags: types.TransferFlags{Imported: true}.ToUint16(),
	}
	got, err := transferToProto(tr, testRegistry())
	require.NoError(t, err)
	require.Contains(t, got.Flags, "imported")
}

func TestTransferUnknownCodeIsError(t *testing.T) {
	_, err := transferToProto(types.Transfer{Ledger: 1, Code: 99}, testRegistry())
	require.ErrorContains(t, err, "unknown code")
}

func TestBalanceToProto(t *testing.T) {
	b := types.AccountBalance{
		DebitsPosted: types.ToUint128(1000), CreditsPosted: types.ToUint128(500),
		Timestamp: 7,
	}
	got := balanceToProto(b, 2)
	require.Equal(t, "10.00", got.DebitsPosted)
	require.Equal(t, "5.00", got.CreditsPosted)
	require.Equal(t, uint64(7), got.Timestamp)
}
