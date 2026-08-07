package tbx

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Инвариант упаковки: каждый батч состоит из целых команд,
// не превышает лимит, и суммарно уходят все события.
func TestBatcherPackingInvariants(t *testing.T) {
	const maxBatch = 64
	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	rng := rand.New(rand.NewSource(seed))

	fc := &fakeClient{}
	b, _ := startBatcher(t, fc, maxBatch, 5*time.Millisecond)

	sizes := make([]int, 200)
	total := 0
	for i := range sizes {
		sizes[i] = 1 + rng.Intn(maxBatch)
		total += sizes[i]
	}

	var wg sync.WaitGroup
	for i, n := range sizes {
		wg.Add(1)
		go func(i, n int) {
			defer wg.Done()
			out, err := b.Submit(context.Background(), transferCmd(n, "c"+strconv.Itoa(i)))
			require.NoError(t, err)
			require.Len(t, out, n)
		}(i, n)
	}
	wg.Wait()

	sent := 0
	for _, batch := range fc.batches() {
		require.LessOrEqual(t, len(batch), maxBatch)
		require.NotEmpty(t, batch)
		require.Zero(t, batch[len(batch)-1].Flags&linkedBit, "batch must not end with an open chain")
		sent += len(batch)
	}
	require.Equal(t, total, sent, "every submitted event must reach TigerBeetle exactly once")
}
