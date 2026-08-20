package model

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"

	types "github.com/tigerbeetle/tigerbeetle-go"
)

// maxU128 = 2^128 - 1
var maxU128 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

var pow10 [19]*big.Int

// pow10u64 mirrors pow10 for FormatAmount's fast path. 10^18 is the largest
// power of ten that fits in a uint64 and scale is bounded by len(pow10)-1 =
// 18, so every entry is exact — there is no rounding here and no float
// anywhere near it.
var pow10u64 [19]uint64

func init() {
	for i := range pow10 {
		pow10[i] = new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(i)), nil)
	}
	pow10u64[0] = 1
	for i := 1; i < len(pow10u64); i++ {
		pow10u64[i] = pow10u64[i-1] * 10
	}
}

// ParseAmount converts a decimal string to minor units at the ledger's scale.
// Float is deliberately not used: rounding is unacceptable.
func ParseAmount(s string, scale int32) (types.Uint128, error) {
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

// FormatAmount renders minor units as a decimal string at the ledger's scale.
//
// Two paths, one output. Everything whose high 64 bits are zero — which is
// every amount a ledger realistically carries — is formatted with integer
// division and strconv; anything larger goes through math/big. The split
// exists because the CDC encoder calls this nine times per change event (the
// transfer amount plus a four-field balance snapshot of both accounts), which
// made it 69.5% of every object that job allocated and ~21% of its CPU.
//
// The fast path is only ever taken where it produces byte-identical output to
// the big.Int path, and formatAmountBig is kept as a named function precisely
// so the two can be compared directly: see TestFormatAmountFastPathMatchesBig
// and FuzzFormatAmount. Float is deliberately absent from both paths — an
// amount that rounds is worse than an amount that is slow.
func FormatAmount(u types.Uint128, scale int32) string {
	// A scale outside pow10's range is a programming error that the big path
	// already panics on; it is not the fast path's job to change that.
	if lo, hi := u.Uint64(); hi == 0 && scale >= 0 && int(scale) < len(pow10u64) {
		return formatUint64(lo, scale)
	}
	return formatAmountBig(u, scale)
}

// formatAmountBig is FormatAmount's reference implementation: correct for the
// whole uint128 range and the fallback above 2^64-1.
func formatAmountBig(u types.Uint128, scale int32) string {
	bi := u.BigInt()
	if scale == 0 {
		return bi.String()
	}
	q, r := new(big.Int).QuoRem(bi, pow10[scale], new(big.Int))
	return fmt.Sprintf("%s.%0*s", q.String(), scale, r.String())
}

// formatUint64 is the fast path. scale is known to be in [0, 18].
func formatUint64(v uint64, scale int32) string {
	if scale == 0 {
		return strconv.FormatUint(v, 10)
	}
	div := pow10u64[scale]
	q, r := v/div, v%div

	// 19 digits of quotient (scale >= 1, so q <= (2^64-1)/10), a dot, and at
	// most 18 of fraction.
	var buf [38]byte
	b := strconv.AppendUint(buf[:0], q, 10)
	b = append(b, '.')
	// r < div, so it has at most scale digits — the missing ones are leading
	// zeros, which is what the big path's "%0*s" width writes.
	digits := 1
	for t := r; t >= 10; t /= 10 {
		digits++
	}
	for pad := int(scale) - digits; pad > 0; pad-- {
		b = append(b, '0')
	}
	b = strconv.AppendUint(b, r, 10)
	return string(b)
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}
