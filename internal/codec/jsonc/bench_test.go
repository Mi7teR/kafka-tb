package jsonc

import (
	"fmt"
	"strings"
	"testing"
)

func benchPayload(n int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"operation":"create_transfers","transfers":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"id":"0193f8a1-7c2e-7000-8000-%012d",`+
			`"debit_account_id":"0193f8a1-0000-7000-8000-000000000010",`+
			`"credit_account_id":"0193f8a1-0000-7000-8000-000000000020",`+
			`"amount":"12.34","ledger":"USD","code":"payment","flags":[]}`, i+1)
	}
	sb.WriteString(`]}`)
	return []byte(sb.String())
}

func BenchmarkDecodeJSON(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			d := newFuzzDecoder()
			p := benchPayload(n)
			b.SetBytes(int64(len(p)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := d.Decode(p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
