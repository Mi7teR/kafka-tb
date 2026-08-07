package tbx

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
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

func transferCmdBench(n int) *model.Command {
	c := &model.Command{Op: model.OpCreateTransfers, Transfers: make([]types.Transfer, n), IDs: make([]string, n)}
	for i := 0; i < n; i++ {
		c.Transfers[i] = types.Transfer{ID: types.ToUint128(uint64(i + 1)), Flags: 1}
		c.IDs[i] = strconv.Itoa(i)
	}
	return c
}

func BenchmarkBatcherAssemble(b *testing.B) {
	fc := &fakeClient{}
	bt := NewBatcher(fc, config.Batcher{MaxBatchSize: 8189, Linger: time.Millisecond, MaxQueue: 1024},
		config.Retry{Initial: time.Millisecond, Max: time.Millisecond}, testLogger(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); bt.Close() }()
	bt.Start(ctx)

	cmd := transferCmdBench(10)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := bt.Submit(ctx, cmd); err != nil {
				// b.Error, а не b.Fatal: FailNow из не-бенчмарочной горутины — UB.
				b.Error(err)
				return
			}
		}
	})
}
