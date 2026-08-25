package tbx

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/stretchr/testify/require"
)

// A misconfigured address is the failure an operator actually meets here, and
// the wrapping is the whole point of NewClient existing: the TigerBeetle
// client's own message says nothing about where the address came from, so
// without the prefix the log line is unattributable.
//
// Only the failure is unit-tested. Succeeding requires a live replica, so the
// success path belongs to the integration suite, which builds real clients
// against a real TigerBeetle -- faking it here would test the fake.
func TestNewClientWrapsAConnectFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.TigerBeetle
	}{
		{"unparsable address", config.TigerBeetle{Addresses: []string{"nonsense"}}},
		{"no addresses", config.TigerBeetle{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(tc.cfg)
			require.Nil(t, c, "a client that failed to connect must not be returned")
			require.ErrorContains(t, err, "tigerbeetle connect:",
				"the error has to name the subsystem that refused")
		})
	}
}
