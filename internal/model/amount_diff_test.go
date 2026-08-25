package model

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strings"
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

// parseAmountReference is a verbatim copy of ParseAmount as it stood before
// the uint64 fast path was added. It is the oracle for everything below: the
// bar is not "the two branches inside ParseAmount agree with each other" but
// "ParseAmount accepts, rejects and words its errors exactly as it did
// before", because its rejections are the sink's poison contract and its
// error strings are read by operators out of DLQ headers.
//
// It is deliberately not factored to share code with the implementation. An
// oracle that shares the code under test proves nothing.
func parseAmountReference(s string, scale int32) (types.Uint128, error) {
	if scale < 0 || int(scale) >= len(pow10) {
		return types.Uint128{}, fmt.Errorf("amount %q: unsupported scale %d", s, scale)
	}
	if s == "" {
		return types.Uint128{}, fmt.Errorf("amount: empty")
	}
	intPart, fracPart, hasDot := strings.Cut(s, ".")
	if intPart == "" || (hasDot && fracPart == "") {
		return types.Uint128{}, fmt.Errorf("amount %q: malformed", s)
	}
	if !isDigits(intPart) || (hasDot && !isDigits(fracPart)) {
		return types.Uint128{}, fmt.Errorf("amount %q: only digits and one dot allowed", s)
	}
	if int32(len(fracPart)) > scale {
		return types.Uint128{}, fmt.Errorf("amount %q: has %d decimals, scale is %d", s, len(fracPart), scale)
	}
	digits := intPart + fracPart + strings.Repeat("0", int(scale)-len(fracPart))
	bi, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return types.Uint128{}, fmt.Errorf("amount %q: not a number", s)
	}
	if bi.Cmp(maxU128) > 0 {
		return types.Uint128{}, fmt.Errorf("amount %q: exceeds uint128", s)
	}
	return types.BigIntToUint128(bi), nil
}

// requireParseAgrees is the whole equivalence claim: same acceptance, same
// value, same error text.
func requireParseAgrees(t *testing.T, s string, scale int32) {
	t.Helper()
	want, wantErr := parseAmountReference(s, scale)
	got, gotErr := ParseAmount(s, scale)
	if wantErr != nil {
		require.Error(t, gotErr, "input %q scale %d: reference rejected, fast path accepted", s, scale)
		require.Equal(t, wantErr.Error(), gotErr.Error(), "input %q scale %d: error text changed", s, scale)
		return
	}
	require.NoError(t, gotErr, "input %q scale %d: reference accepted", s, scale)
	require.Equal(t, want, got, "input %q scale %d", s, scale)
}

// malformedAmounts is every shape ParseAmount is required to keep rejecting.
// It is the table from TestParseAmountRejects plus the ones a digit-scanning
// fast path could plausibly wave through.
var malformedAmounts = []string{
	"", "abc", "-1", "1.2.3", "1e5", " 1", "1 ", "+1", ".", "1.", ".5",
	"1_000", "0x10", "1,00", "1.2e3", "١٢", "1..2", "--1", "1-", "٠",
	"\t1", "1\n", "NaN", "Inf", "1.-2", "-0", "+0.5", "1.2.", "..1",
}

// TestParseAmountMatchesBig walks the fast path against its oracle at every
// scale the config allows, over the values where the two could plausibly
// disagree: the ends of the uint64 range where the fast path stops being
// taken, the uint128 ceiling above it, fractions shorter than the scale, and
// leading zeros — which make a digit count useless as an overflow test.
func TestParseAmountMatchesBig(t *testing.T) {
	for scale := int32(0); scale <= 18; scale++ {
		div := pow10u64[scale]
		inputs := []string{
			"0",
			"1",
			decimalOf(new(big.Int).SetUint64(1), scale),                    // one minor unit
			decimalOf(new(big.Int).SetUint64(div), scale),                  // one major unit
			decimalOf(new(big.Int).SetUint64(div-1), scale),                //
			decimalOf(new(big.Int).SetUint64(div+1), scale),                //
			decimalOf(new(big.Int).SetUint64(math.MaxUint64), scale),       // last value the fast path takes
			decimalOf(new(big.Int).SetUint64(math.MaxUint64-1), scale),     //
			decimalOf(bigPow2(64), scale),                                  // 2^64: first value it must not
			decimalOf(new(big.Int).Add(bigPow2(64), big.NewInt(1)), scale), //
			decimalOf(maxU128, scale),                                      // max uint128
			decimalOf(new(big.Int).Add(maxU128, big.NewInt(1)), scale),     // one above it: must be rejected
			"00000000000000000000000000000001",                             // leading zeros: 32 digits, value 1
			strings.Repeat("0", 40),                                        // 40 digits, value 0
			strings.Repeat("9", 39),                                        // above uint128 whatever the scale
			strings.Repeat("9", 20),                                        // just above uint64, 20 digits
			strings.Repeat("9", 19),                                        // just below it
		}
		// A fraction shorter than the scale is where the trailing-zero shift
		// earns its keep: every truncated width from one digit to the full
		// scale, on both sides of the uint64 boundary.
		maxU64Dec := decimalOf(new(big.Int).SetUint64(math.MaxUint64), scale)
		for d := int32(1); d <= scale; d++ {
			inputs = append(inputs,
				"1."+strings.Repeat("0", int(d)-1)+"1",
				"1."+strings.Repeat("9", int(d)),
				"0."+strings.Repeat("0", int(d)-1)+"1",
				// 2^64-1 with its fraction cut to d digits: the same
				// truncation the wire does, right at the boundary.
				maxU64Dec[:len(maxU64Dec)-int(scale-d)],
			)
		}
		for _, in := range inputs {
			requireParseAgrees(t, in, scale)
		}
		for _, in := range malformedAmounts {
			requireParseAgrees(t, in, scale)
		}
	}
}

// The scales outside pow10's range are rejected before either path is
// reached; the oracle has to agree there too.
func TestParseAmountUnsupportedScaleMatchesBig(t *testing.T) {
	for _, scale := range []int32{-1, -2, 19, 20, math.MaxInt32, math.MinInt32} {
		requireParseAgrees(t, "1.23", scale)
		requireParseAgrees(t, "", scale)
		requireParseAgrees(t, "abc", scale)
	}
}

// The uint64 boundary stated on its own, so a regression in the fast path's
// overflow guard cannot hide inside the table above.
func TestParseAmountUint64Boundary(t *testing.T) {
	// 2^64-1 minor units at scale 2 is the last value the fast path takes.
	last, err := ParseAmount("184467440737095516.15", 2)
	require.NoError(t, err)
	require.Equal(t, "18446744073709551615", last.BigInt().String())
	// One minor unit more is 2^64 and must come out of the big path intact.
	first, err := ParseAmount("184467440737095516.16", 2)
	require.NoError(t, err)
	require.Equal(t, "18446744073709551616", first.BigInt().String())
	// The uint128 ceiling and one past it.
	max, err := ParseAmount("3402823669209384634633746074317682114.55", 2)
	require.NoError(t, err)
	require.Equal(t, "340282366920938463463374607431768211455", max.BigInt().String())
	_, err = ParseAmount("3402823669209384634633746074317682114.56", 2)
	require.EqualError(t, err, `amount "3402823669209384634633746074317682114.56": exceeds uint128`)
	// A leading-zero run long enough to make the string longer than any
	// uint64, holding a value that still fits in one.
	pad, err := ParseAmount(strings.Repeat("0", 64)+"12.34", 2)
	require.NoError(t, err)
	require.Equal(t, "1234", pad.BigInt().String())
}

// Random values, both sides of the boundary, at every scale.
func TestParseAmountMatchesBigRandom(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for scale := int32(0); scale <= 18; scale++ {
		for i := 0; i < 2000; i++ {
			var v *big.Int
			switch i % 4 {
			case 0: // spread across the whole uint128 range
				v = new(big.Int).Rand(r, new(big.Int).Add(maxU128, big.NewInt(1)))
			case 1: // straddling 2^64
				v = new(big.Int).Add(bigPow2(64), big.NewInt(r.Int63n(2001)-1000))
			default: // inside the fast path, at every magnitude
				v = new(big.Int).SetUint64(r.Uint64() >> uint(r.Intn(64)))
			}
			in := decimalOf(v, scale)
			requireParseAgrees(t, in, scale)
			// The same value with a fraction truncated to its significant
			// digits, which is the common shape on the wire.
			requireParseAgrees(t, strings.TrimRight(strings.TrimRight(in, "0"), "."), scale)
		}
	}
}

// FuzzParseAmountMatchesBig is the equivalence claim without a table: for any
// string and any scale, the fast path and the pre-split implementation must
// return the same value or the same error text.
func FuzzParseAmountMatchesBig(f *testing.F) {
	seeds := []string{
		"0", "1", "12.34", "12.3", "12", "0.01", "7", "", "abc", "-1", "1.234",
		"1.2.3", "1e5", " 1", "1.", ".5", "000000000000000000000000000001",
		"18446744073709551615", "18446744073709551616",
		"340282366920938463463374607431768211455",
		"340282366920938463463374607431768211456",
		"184467440737095516.15", "184467440737095516.16",
		"3402823669209384634633746074317682114.55",
	}
	for _, s := range seeds {
		for _, scale := range []int32{0, 2, 18} {
			f.Add(s, scale)
		}
	}
	f.Fuzz(func(t *testing.T, s string, scale int32) {
		want, wantErr := parseAmountReference(s, scale)
		got, gotErr := ParseAmount(s, scale)
		switch {
		case wantErr != nil && gotErr == nil:
			t.Fatalf("ParseAmount(%q, %d) accepted %v, reference rejected: %v", s, scale, got, wantErr)
		case wantErr == nil && gotErr != nil:
			t.Fatalf("ParseAmount(%q, %d) rejected: %v, reference accepted %v", s, scale, gotErr, want)
		case wantErr != nil:
			if wantErr.Error() != gotErr.Error() {
				t.Fatalf("ParseAmount(%q, %d) error %q, reference %q", s, scale, gotErr, wantErr)
			}
		case got != want:
			t.Fatalf("ParseAmount(%q, %d) = %v, reference = %v", s, scale, got.BigInt(), want.BigInt())
		}
	})
}

// decimalOf renders v minor units as the decimal string ParseAmount takes at
// this scale. It is the inverse of what ParseAmount computes, written with
// math/big so it cannot inherit a bug from the fast path it feeds.
func decimalOf(v *big.Int, scale int32) string {
	if scale <= 0 || int(scale) >= len(pow10) {
		return v.String()
	}
	q, r := new(big.Int).QuoRem(v, pow10[scale], new(big.Int))
	frac := r.String()
	return q.String() + "." + strings.Repeat("0", int(scale)-len(frac)) + frac
}

func bigPow2(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }
