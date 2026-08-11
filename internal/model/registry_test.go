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
