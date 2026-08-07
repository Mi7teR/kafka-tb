package model

import "testing"

func BenchmarkParseAmount(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseAmount("12345.67", 2); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFormatAmount(b *testing.B) {
	u, _ := ParseAmount("12345.67", 2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FormatAmount(u, 2)
	}
}

func BenchmarkParseID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseID("0193f8a1-7c2e-7000-8000-000000000001"); err != nil {
			b.Fatal(err)
		}
	}
}
