package model

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/stretchr/testify/require"
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
