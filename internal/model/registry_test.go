package model

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func testRegistry() *Registry {
	return NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"customer": 1},
	})
}

// I2 regression: "imported" must be rejected on the write path with an
// explicit, actionable error — importing requires caller-supplied event
// timestamps, which this connector does not support.
func TestAccountFlagsRejectsImported(t *testing.T) {
	_, err := testRegistry().AccountFlags([]string{"linked", "imported"})
	require.ErrorContains(t, err, "imported")
	require.ErrorContains(t, err, "read-only in this connector")
}

func TestTransferFlagsRejectsImported(t *testing.T) {
	_, err := testRegistry().TransferFlags([]string{"pending", "imported"})
	require.ErrorContains(t, err, "imported")
	require.ErrorContains(t, err, "read-only in this connector")
}

// The CDC job needs the reverse of TransferFlags/AccountFlags: an event
// carries flags as a bit set and the message must name them.
func TestTransferFlagNamesRoundTrip(t *testing.T) {
	r := NewRegistry(&config.Config{})
	names := []string{
		"linked", "pending", "post_pending_transfer", "void_pending_transfer",
		"balancing_debit", "balancing_credit", "closing_debit", "closing_credit",
	}
	for _, n := range names {
		f, err := r.TransferFlags([]string{n})
		require.NoError(t, err)
		require.Equal(t, []string{n}, r.TransferFlagNames(f.ToUint16()))
	}
	all, err := r.TransferFlags(names)
	require.NoError(t, err)
	require.Equal(t, names, r.TransferFlagNames(all.ToUint16()))
	require.Empty(t, r.TransferFlagNames(0))
}

func TestAccountFlagNamesRoundTrip(t *testing.T) {
	r := NewRegistry(&config.Config{})
	names := []string{
		"linked", "debits_must_not_exceed_credits",
		"credits_must_not_exceed_debits", "history", "closed",
	}
	for _, n := range names {
		f, err := r.AccountFlags([]string{n})
		require.NoError(t, err)
		require.Equal(t, []string{n}, r.AccountFlagNames(f.ToUint16()))
	}
	all, err := r.AccountFlags(names)
	require.NoError(t, err)
	require.Equal(t, names, r.AccountFlagNames(all.ToUint16()))
	require.Empty(t, r.AccountFlagNames(0))
}

// imported is rejected on the way in — this connector never sets it — but an
// event that carries it must still say so: dropping a flag from a change
// event would misreport what TigerBeetle actually holds.
func TestFlagNamesReportImported(t *testing.T) {
	r := NewRegistry(&config.Config{})
	require.Equal(t, []string{"imported"}, r.TransferFlagNames(types.TransferFlags{Imported: true}.ToUint16()))
	require.Equal(t, []string{"imported"}, r.AccountFlagNames(types.AccountFlags{Imported: true}.ToUint16()))
}

// An unknown bit must not be swallowed: it names itself numerically, the same
// way an unknown ledger or code does.
func TestFlagNamesReportUnknownBits(t *testing.T) {
	r := NewRegistry(&config.Config{})
	require.Equal(t, []string{"linked", "bit_15"}, r.TransferFlagNames(1|1<<15))
	require.Equal(t, []string{"linked", "bit_15"}, r.AccountFlagNames(1|1<<15))
}

// multiRegistry has more than one ledger and code so that the reverse lookups
// have something to get wrong: a table built with a single entry passes even
// if the lookup ignores its argument.
func multiRegistry() *Registry {
	return NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{
			"USD": {ID: 1, Scale: 2},
			"JPY": {ID: 2, Scale: 0},
			"BTC": {ID: 3, Scale: 8},
		},
		Codes: map[string]uint16{"customer": 1, "payment": 2, "fee": 7},
	})
}

// Ledger is the forward half of the value contract: a message names a ledger
// and the connector has to turn that into the id and scale TigerBeetle wants.
func TestLedgerLooksUpIDAndScale(t *testing.T) {
	r := multiRegistry()
	for name, want := range map[string]config.Ledger{
		"USD": {ID: 1, Scale: 2},
		"JPY": {ID: 2, Scale: 0},
		"BTC": {ID: 3, Scale: 8},
	} {
		got, err := r.Ledger(name)
		require.NoError(t, err)
		require.Equal(t, want, got, "ledger %s", name)
	}
}

func TestLedgerUnknownNameIsAnError(t *testing.T) {
	_, err := multiRegistry().Ledger("EUR")
	require.ErrorContains(t, err, "unknown ledger")
	require.ErrorContains(t, err, `"EUR"`, "the error must name the ledger the message asked for")
}

// LedgerName is the reverse half: a change event carries a numeric ledger id
// and the published message has to name it.
func TestLedgerNameReversesLedger(t *testing.T) {
	r := multiRegistry()
	for _, name := range []string{"USD", "JPY", "BTC"} {
		l, err := r.Ledger(name)
		require.NoError(t, err)
		got, err := r.LedgerName(l.ID)
		require.NoError(t, err)
		require.Equal(t, name, got)
	}
}

func TestLedgerNameUnknownIDIsAnError(t *testing.T) {
	_, err := multiRegistry().LedgerName(99)
	require.ErrorContains(t, err, "unknown ledger id 99")
}

// ScaleByLedgerID is what the CDC job renders amounts with: the wrong scale
// turns 1.00 into 100.00 without anything failing, so it is worth pinning
// per ledger rather than once.
func TestScaleByLedgerID(t *testing.T) {
	r := multiRegistry()
	for id, want := range map[uint32]int32{1: 2, 2: 0, 3: 8} {
		got, err := r.ScaleByLedgerID(id)
		require.NoError(t, err)
		require.Equal(t, want, got, "ledger id %d", id)
	}
}

func TestScaleByLedgerIDUnknownIDIsAnError(t *testing.T) {
	_, err := multiRegistry().ScaleByLedgerID(99)
	require.ErrorContains(t, err, "unknown ledger id 99")
}

func TestCodeLooksUpValue(t *testing.T) {
	r := multiRegistry()
	for name, want := range map[string]uint16{"customer": 1, "payment": 2, "fee": 7} {
		got, err := r.Code(name)
		require.NoError(t, err)
		require.Equal(t, want, got, "code %s", name)
	}
}

func TestCodeUnknownNameIsAnError(t *testing.T) {
	_, err := multiRegistry().Code("refund")
	require.ErrorContains(t, err, "unknown code")
	require.ErrorContains(t, err, `"refund"`)
}

func TestCodeNameReversesCode(t *testing.T) {
	r := multiRegistry()
	for _, name := range []string{"customer", "payment", "fee"} {
		v, err := r.Code(name)
		require.NoError(t, err)
		got, err := r.CodeName(v)
		require.NoError(t, err)
		require.Equal(t, name, got)
	}
}

func TestCodeNameUnknownValueIsAnError(t *testing.T) {
	_, err := multiRegistry().CodeName(99)
	require.ErrorContains(t, err, "unknown code 99")
}

// An empty config must not resolve anything. NewRegistry builds its reverse
// tables by iterating the forward ones, and a zero value is the id and code a
// message that omitted them would carry.
func TestEmptyRegistryResolvesNothing(t *testing.T) {
	r := NewRegistry(&config.Config{})
	_, err := r.Ledger("USD")
	require.Error(t, err)
	_, err = r.LedgerName(0)
	require.Error(t, err)
	_, err = r.ScaleByLedgerID(0)
	require.Error(t, err)
	_, err = r.Code("customer")
	require.Error(t, err)
	_, err = r.CodeName(0)
	require.Error(t, err)
}

// A flag name this build does not know must be refused rather than silently
// dropped: accepting it would apply a transfer the caller did not ask for.
func TestTransferFlagsRejectsUnknownName(t *testing.T) {
	_, err := testRegistry().TransferFlags([]string{"linked", "teleport"})
	require.ErrorContains(t, err, "unknown transfer flag")
	require.ErrorContains(t, err, `"teleport"`)
}

func TestAccountFlagsRejectsUnknownName(t *testing.T) {
	_, err := testRegistry().AccountFlags([]string{"linked", "teleport"})
	require.ErrorContains(t, err, "unknown account flag")
	require.ErrorContains(t, err, `"teleport"`)
}

// The flag tables are per entity and must not be interchangeable: an account
// flag on a transfer (and the reverse) is a caller mistake, not a synonym.
func TestFlagTablesAreNotInterchangeable(t *testing.T) {
	r := testRegistry()
	_, err := r.TransferFlags([]string{"history"})
	require.ErrorContains(t, err, "unknown transfer flag")
	_, err = r.AccountFlags([]string{"pending"})
	require.ErrorContains(t, err, "unknown account flag")
}

// No flags at all is not an error: most transfers set none.
func TestNoFlagNamesYieldsZeroFlags(t *testing.T) {
	r := testRegistry()
	tf, err := r.TransferFlags(nil)
	require.NoError(t, err)
	require.Zero(t, tf.ToUint16())
	af, err := r.AccountFlags(nil)
	require.NoError(t, err)
	require.Zero(t, af.ToUint16())
}
