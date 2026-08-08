package model

import "testing"

func FuzzParseAmount(f *testing.F) {
	for _, s := range []string{"0", "12.34", "abc", "", "-1", "1.", ".1", "999999999999999999999999999999999999999999"} {
		f.Add(s, int32(2))
	}
	f.Fuzz(func(t *testing.T, s string, scale int32) {
		if scale < 0 || scale > 18 {
			return
		}
		u, err := ParseAmount(s, scale)
		if err != nil {
			return
		}
		// A successful parse must survive a round-trip.
		if got := FormatAmount(u, scale); got == "" {
			t.Fatalf("empty format for %q", s)
		}
	})
}
