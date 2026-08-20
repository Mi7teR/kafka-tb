package model

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

// u128 builds a Uint128 from its two halves. TigerBeetle stores the value
// little-endian, which is also what Uint128.Uint64 reads back, so this is the
// exact inverse of the split FormatAmount's fast-path guard makes.
func u128(hi, lo uint64) types.Uint128 {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], lo)
	binary.LittleEndian.PutUint64(b[8:16], hi)
	return types.BytesToUint128(b)
}

func TestU128HelperRoundTrips(t *testing.T) {
	lo, hi := u128(7, 9).Uint64()
	require.Equal(t, uint64(9), lo)
	require.Equal(t, uint64(7), hi)
}

// The fast path exists only if it is indistinguishable from the big.Int path.
// This walks every scale the config allows against the values where the two
// implementations could plausibly disagree: the ends of the uint64 range,
// where the fast path stops being taken, and every place a digit count or a
// zero-pad width changes.
func TestFormatAmountFastPathMatchesBig(t *testing.T) {
	for scale := int32(0); scale <= 18; scale++ {
		div := pow10u64[scale]
		values := []types.Uint128{
			u128(0, 0),                           // zero
			u128(0, 1),                           // one minor unit
			u128(0, div-1),                       // largest value under one major unit
			u128(0, div),                         // exactly one major unit
			u128(0, div+1),                       // one major unit and one minor
			u128(0, div*2-1),                     //
			u128(0, math.MaxUint64),              // last value the fast path takes
			u128(1, 0),                           // 2^64: first value it must not
			u128(1, 1),                           //
			u128(math.MaxUint64, math.MaxUint64), // max uint128
		}
		// A fractional part shorter than the scale is where "%0*s" earns its
		// keep: every truncated width from one digit up to the full scale.
		for d := int32(1); d < scale; d++ {
			values = append(values, u128(0, div+pow10u64[d]), u128(0, div*3+pow10u64[d]-1))
		}
		for _, v := range values {
			lo, hi := v.Uint64()
			require.Equal(t, formatAmountBig(v, scale), FormatAmount(v, scale),
				"scale=%d hi=%d lo=%d", scale, hi, lo)
		}
	}
}

// The boundary the fast path is guarded on, stated on its own so a regression
// in the guard cannot hide inside the table above.
func TestFormatAmountUint64Boundary(t *testing.T) {
	require.Equal(t, "184467440737095516.15", FormatAmount(u128(0, math.MaxUint64), 2))
	require.Equal(t, "184467440737095516.16", FormatAmount(u128(1, 0), 2),
		"2^64 is one minor unit past the last value the fast path handles")
	require.Equal(t, "0.00", FormatAmount(u128(0, 0), 2))
	require.Equal(t, "0.01", FormatAmount(u128(0, 1), 2))
	require.Equal(t, "3402823669209384634633746074317682114.55",
		FormatAmount(u128(math.MaxUint64, math.MaxUint64), 2), "max uint128 at scale 2")
	require.Equal(t, "340282366920938463463374607431768211455",
		FormatAmount(u128(math.MaxUint64, math.MaxUint64), 0), "max uint128 at scale 0")
}

// Random values, both sides of the boundary, at every scale.
func TestFormatAmountFastPathMatchesBigRandom(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for scale := int32(0); scale <= 18; scale++ {
		for i := 0; i < 2000; i++ {
			var v types.Uint128
			if i%4 == 0 {
				v = u128(r.Uint64(), r.Uint64())
			} else {
				v = u128(0, r.Uint64()>>uint(r.Intn(64)))
			}
			require.Equal(t, formatAmountBig(v, scale), FormatAmount(v, scale))
		}
	}
}

// FuzzFormatAmount is the whole equivalence claim in one sentence: for any
// uint128 and any scale the config allows, the two paths must produce the
// same bytes.
func FuzzFormatAmount(f *testing.F) {
	seeds := []struct {
		hi, lo uint64
		scale  int32
	}{
		{0, 0, 0}, {0, 0, 2}, {0, 1, 2}, {0, 1234567, 2}, {0, 100, 3},
		{0, math.MaxUint64, 0}, {0, math.MaxUint64, 18}, {1, 0, 18},
		{math.MaxUint64, math.MaxUint64, 18}, {0, 1000000000000000000, 18},
	}
	for _, s := range seeds {
		f.Add(s.hi, s.lo, s.scale)
	}
	f.Fuzz(func(t *testing.T, hi, lo uint64, scale int32) {
		if scale < 0 || scale > 18 {
			return
		}
		v := u128(hi, lo)
		want := formatAmountBig(v, scale)
		if got := FormatAmount(v, scale); got != want {
			t.Fatalf("FormatAmount(hi=%d lo=%d, scale=%d) = %q, big path = %q", hi, lo, scale, got, want)
		}
	})
}
