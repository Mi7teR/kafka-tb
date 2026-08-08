package model

import (
	"fmt"
	"math/big"
	"strings"

	types "github.com/tigerbeetle/tigerbeetle-go"
)

// maxU128 = 2^128 - 1
var maxU128 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

var pow10 [19]*big.Int

func init() {
	for i := range pow10 {
		pow10[i] = new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(i)), nil)
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

func FormatAmount(u types.Uint128, scale int32) string {
	bi := u.BigInt()
	if scale == 0 {
		return bi.String()
	}
	q, r := new(big.Int).QuoRem(bi, pow10[scale], new(big.Int))
	return fmt.Sprintf("%s.%0*s", q.String(), scale, r.String())
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}
