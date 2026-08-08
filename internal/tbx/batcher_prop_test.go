package tbx

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

// markedTransferCmd stamps each event with the command's marker (UserData128)
// and its position within the command (UserData64).
// Without such a marker, command boundaries could not be reconstructed from the recorded batch,
// and a packer that cuts a linked chain in half would pass the checks.
func markedTransferCmd(marker uint64, n int) *model.Command {
	c := &model.Command{Op: model.OpCreateTransfers, Transfers: make([]types.Transfer, n), IDs: make([]string, n)}
	for i := 0; i < n; i++ {
		c.Transfers[i] = types.Transfer{
			ID:          types.ToUint128(uint64(i + 1)),
			Flags:       linkedBit, // linked on all of them, the batcher must clear it only on the command's tail
			UserData128: types.ToUint128(marker),
			UserData64:  uint64(i),
		}
		c.IDs[i] = "c" + strconv.FormatUint(marker, 10) + "-" + strconv.Itoa(i)
	}
	return c
}

// Packing invariant: every batch decomposes into whole commands, laid out
// contiguously and in the original event order; within a command, linked is cleared exactly on
// the last event; no command is lost or sent twice.
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

	// marker (UserData128) -> command index in sizes. Uint128 is comparable, so it works as a key.
	byMarker := make(map[types.Uint128]int, len(sizes))
	for i := range sizes {
		byMarker[types.ToUint128(uint64(i+1))] = i
	}

	var wg sync.WaitGroup
	for i, n := range sizes {
		wg.Add(1)
		go func(i, n int) {
			defer wg.Done()
			out, err := b.Submit(context.Background(), markedTransferCmd(uint64(i+1), n))
			// assert, not require: FailNow from a non-test goroutine is UB.
			assert.NoError(t, err)
			assert.Len(t, out, n)
		}(i, n)
	}
	wg.Wait()

	seen := make(map[int]int, len(sizes))
	sent := 0
	for bi, batch := range fc.batches() {
		require.NotEmpty(t, batch, "batch %d is empty", bi)
		require.LessOrEqual(t, len(batch), maxBatch, "batch %d exceeds max_batch_size", bi)
		sent += len(batch)

		// Parsing the batch into commands: an event opens a command only if it is
		// that command's event #0, and from there the whole command must lie contiguously.
		for pos := 0; pos < len(batch); {
			marker := batch[pos].UserData128
			cmdIdx, ok := byMarker[marker]
			require.True(t, ok, "batch %d pos %d: event carries an unknown command marker", bi, pos)
			require.Zero(t, batch[pos].UserData64,
				"batch %d pos %d: command %d does not start at its first event — command was split", bi, pos, cmdIdx)

			n := sizes[cmdIdx]
			require.LessOrEqual(t, pos+n, len(batch),
				"batch %d: command %d (%d events) does not fit — command was split at the batch edge", bi, cmdIdx, n)

			for k := 0; k < n; k++ {
				ev := batch[pos+k]
				require.Equal(t, marker, ev.UserData128,
					"batch %d pos %d: command %d is not contiguous", bi, pos+k, cmdIdx)
				require.Equal(t, uint64(k), ev.UserData64,
					"batch %d pos %d: command %d events are out of order", bi, pos+k, cmdIdx)
				if k == n-1 {
					require.Zero(t, ev.Flags&linkedBit,
						"batch %d: command %d must end with linked cleared", bi, cmdIdx)
				} else {
					require.NotZero(t, ev.Flags&linkedBit,
						"batch %d: command %d cleared linked at event %d, before its end", bi, cmdIdx, k)
				}
			}

			seen[cmdIdx]++
			require.Equal(t, 1, seen[cmdIdx], "command %d was sent more than once", cmdIdx)
			pos += n
		}
	}

	require.Equal(t, total, sent, "every submitted event must reach TigerBeetle exactly once")
	require.Len(t, seen, len(sizes), "every submitted command must reach TigerBeetle exactly once")
}
