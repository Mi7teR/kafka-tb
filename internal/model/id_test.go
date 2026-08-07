package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIDRoundTrip(t *testing.T) {
	const s = "0193f8a1-7c2e-7000-8000-000000000001"
	u, err := ParseID(s)
	require.NoError(t, err)
	require.Equal(t, s, FormatID(u))
}

func TestParseIDRejects(t *testing.T) {
	for _, s := range []string{"", "not-a-uuid", "0193f8a1-7c2e-7000-8000", "00000000-0000-0000-0000-000000000000"} {
		_, err := ParseID(s)
		require.Error(t, err, s)
	}
}
