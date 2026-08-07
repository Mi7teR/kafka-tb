package tbx

import (
	"strconv"
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

func BenchmarkMapResults(b *testing.B) {
	const n = 8189
	c := &model.Command{Op: model.OpCreateTransfers, Transfers: make([]types.Transfer, n), IDs: make([]string, n)}
	for i := range c.IDs {
		c.IDs[i] = strconv.Itoa(i)
	}
	res := make([]types.CreateTransferResult, n)
	for i := range res {
		res[i].Status = types.TransferCreated
		if i%100 == 0 {
			res[i].Status = types.TransferExceedsCredits
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := MapTransferResults(c, res, 0, n); err != nil {
			b.Fatal(err)
		}
	}
}
