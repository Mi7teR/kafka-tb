package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAmount(t *testing.T) {
	cases := []struct {
		in    string
		scale int32
		want  string // decimal representation of minor units
	}{
		{"0", 2, "0"},
		{"12.34", 2, "1234"},
		{"12.3", 2, "1230"},
		{"12", 2, "1200"},
		{"0.01", 2, "1"},
		{"7", 0, "7"},
		{"340282366920938463463374607431768211455", 0, "340282366920938463463374607431768211455"}, // max u128
	}
	for _, c := range cases {
		got, err := ParseAmount(c.in, c.scale)
		require.NoError(t, err, c.in)
		bi := got.BigInt()
		require.Equal(t, c.want, bi.String(), c.in)
	}
}

func TestParseAmountRejects(t *testing.T) {
	bad := []struct {
		in    string
		scale int32
	}{
		{"", 2}, {"abc", 2}, {"-1", 2}, {"1.234", 2}, {"1.2.3", 2},
		{"1e5", 2}, {" 1", 2}, {"1 ", 2}, {"+1", 2}, {".", 2}, {"1.", 2}, {".5", 2},
		{"340282366920938463463374607431768211456", 0},  // u128 overflow
		{"3402823669209384634633746074317682114.56", 2}, // overflow after scaling
	}
	for _, c := range bad {
		_, err := ParseAmount(c.in, c.scale)
		require.Error(t, err, c.in)
	}
}

func TestAmountRoundTrip(t *testing.T) {
	for _, s := range []string{"0.00", "1.00", "12.34", "999999.99"} {
		u, err := ParseAmount(s, 2)
		require.NoError(t, err)
		require.Equal(t, s, FormatAmount(u, 2))
	}
}

func TestFormatAmountScaleZero(t *testing.T) {
	u, err := ParseAmount("42", 0)
	require.NoError(t, err)
	require.Equal(t, "42", FormatAmount(u, 0))
}
