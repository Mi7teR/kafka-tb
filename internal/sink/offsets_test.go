package sink

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func rec(offset int64) *kgo.Record {
	return &kgo.Record{Topic: "t", Partition: 0, Offset: offset, LeaderEpoch: 5}
}

func commitOffset(t *testing.T, o *Offsets) (int64, bool) {
	t.Helper()
	m := o.Commitable()
	tp, ok := m["t"]
	if !ok {
		return 0, false
	}
	eo, ok := tp[0]
	return eo.Offset, ok
}

// Коммитим только непрерывный префикс: дырка останавливает watermark.
func TestCommitableStopsAtGap(t *testing.T) {
	o := NewOffsets()
	for _, r := range []*kgo.Record{rec(0), rec(1), rec(2)} {
		o.Track(r)
	}
	o.Done(rec(0))
	o.Done(rec(2)) // 1 ещё в работе

	got, ok := commitOffset(t, o)
	require.True(t, ok)
	require.Equal(t, int64(1), got, "commit offset is last done + 1")

	o.Done(rec(1))
	got, ok = commitOffset(t, o)
	require.True(t, ok)
	require.Equal(t, int64(3), got)
}

func TestCommitableEmptyWhenNothingDone(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(10))
	_, ok := commitOffset(t, o)
	require.False(t, ok)
}

func TestCommitableIsIdempotent(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	first := o.Commitable()
	require.Equal(t, int64(1), first["t"][0].Offset)
	require.Equal(t, first, o.Commitable(), "repeated Commitable must not rewind")

	o.MarkCommitted(first)
	require.Empty(t, o.Commitable(), "nothing new to commit")
}

func TestCommitableCarriesLeaderEpoch(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	require.Equal(t, int32(5), o.Commitable()["t"][0].Epoch)
}

func TestForgetDropsPartitionState(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	o.Forget("t", 0)
	_, ok := commitOffset(t, o)
	require.False(t, ok)
	require.Zero(t, o.InFlight())
}

func TestInFlightCounts(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Track(rec(1))
	require.Equal(t, 2, o.InFlight())
	o.Done(rec(0))
	require.Equal(t, 1, o.InFlight())
}

// Партиции не влияют друг на друга.
func TestPartitionsAreIndependent(t *testing.T) {
	o := NewOffsets()
	a := &kgo.Record{Topic: "t", Partition: 0, Offset: 0}
	b := &kgo.Record{Topic: "t", Partition: 1, Offset: 0}
	o.Track(a)
	o.Track(b)
	o.Done(b)
	m := o.Commitable()
	_, hasA := m["t"][0]
	_, hasB := m["t"][1]
	require.False(t, hasA)
	require.True(t, hasB)
}

// TestConcurrentTrackDoneAcrossPartitions drives many goroutines calling
// Track/Done concurrently across several partitions and asserts the final
// commitable offsets are exactly last-done+1 per partition.
//
// Per Track's contract, a given partition's records are tracked in offset
// order by a single goroutine (mirroring Kafka's per-partition read order);
// completion (Done) fires concurrently and out of order, from many
// goroutines, across all partitions at once.
func TestConcurrentTrackDoneAcrossPartitions(t *testing.T) {
	const (
		partitions = 4
		perPart    = 200
	)
	o := NewOffsets()

	recs := make([][]*kgo.Record, partitions)
	for p := 0; p < partitions; p++ {
		for i := 0; i < perPart; i++ {
			recs[p] = append(recs[p], &kgo.Record{
				Topic:       "t",
				Partition:   int32(p),
				Offset:      int64(i),
				LeaderEpoch: 7,
			})
		}
	}

	var wg sync.WaitGroup
	for p := 0; p < partitions; p++ {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPart; i++ {
				o.Track(recs[p][i])
			}
		}()
	}
	wg.Wait()

	require.Equal(t, partitions*perPart, o.InFlight())

	wg = sync.WaitGroup{}
	for p := 0; p < partitions; p++ {
		for i := 0; i < perPart; i++ {
			r := recs[p][i]
			wg.Add(1)
			go func() {
				defer wg.Done()
				o.Done(r)
			}()
		}
	}
	wg.Wait()

	require.Zero(t, o.InFlight())

	m := o.Commitable()
	require.Len(t, m["t"], partitions)
	for p := 0; p < partitions; p++ {
		eo, ok := m["t"][int32(p)]
		require.True(t, ok, "partition %d must be commitable", p)
		require.Equal(t, int64(perPart), eo.Offset, "partition %d commit offset", p)
		require.Equal(t, int32(7), eo.Epoch, "partition %d epoch", p)
	}
}

// F1 regression: out-of-order Track must not permanently exclude an
// earlier offset from ever becoming commitable.
func TestOutOfOrderTrackDoesNotLoseEarlierOffset(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(5))
	o.Track(rec(3))
	o.Done(rec(5))
	_, ok := commitOffset(t, o)
	require.False(t, ok, "offset 3 is still pending; nothing must be commitable")

	o.Done(rec(3))
	got, ok := commitOffset(t, o)
	require.True(t, ok)
	require.Equal(t, int64(6), got)
}

// F2 regression: Forget must tombstone the partition so a late Done (or
// Track) from a worker still running at revoke time cannot resurrect it.
func TestForgetTombstonesAgainstLateActivity(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Track(rec(1))
	o.Forget("t", 0)

	o.Done(rec(0)) // late completion from a worker still running at revoke time
	_, ok := commitOffset(t, o)
	require.False(t, ok, "forgotten partition must not become commitable again")
	require.Zero(t, o.InFlight())

	o.Track(rec(2)) // a late Track must not resurrect it either
	_, ok = commitOffset(t, o)
	require.False(t, ok, "forgotten partition must stay gone even after a late Track")
}

// F3 regression: a duplicate Done for the same offset must not double-count
// against inflight state that other genuinely in-flight records rely on.
func TestDuplicateDoneDoesNotDoubleDecrementInFlight(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Track(rec(1))
	o.Done(rec(0))
	o.Done(rec(0)) // duplicate

	require.Equal(t, 1, o.InFlight())
}

// F4 regression: the committed epoch must belong to the boundary record
// actually being committed, not whichever Done happened to run last.
func TestCommittedEpochMatchesBoundaryRecord(t *testing.T) {
	o := NewOffsets()
	o.Track(&kgo.Record{Topic: "t", Partition: 0, Offset: 0, LeaderEpoch: 5})
	o.Track(&kgo.Record{Topic: "t", Partition: 0, Offset: 1, LeaderEpoch: 6})
	o.Done(&kgo.Record{Topic: "t", Partition: 0, Offset: 1, LeaderEpoch: 6})
	o.Done(&kgo.Record{Topic: "t", Partition: 0, Offset: 0, LeaderEpoch: 5})

	eo := o.Commitable()["t"][0]
	require.Equal(t, int64(2), eo.Offset)
	require.Equal(t, int32(6), eo.Epoch, "epoch must belong to offset 1, the boundary record")
}

// F5 regression: a partition reassigned back to this consumer after Forget
// must become live again — Forget's tombstone must not be permanent.
func TestTrackAfterForgetRevivesPartition(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	o.Forget("t", 0)

	o.Track(rec(1)) // group reassigned the partition back to us
	o.Done(rec(1))

	got, ok := commitOffset(t, o)
	require.True(t, ok, "reassigned partition must become commitable again")
	require.Equal(t, int64(2), got)
}

// F5 regression: without an intervening Track, Forget's tombstone must
// still gate a late Done from before the revoke — the tombstone's original
// job must survive the fix that lets Track revive the partition.
func TestForgetStillGatesLateDoneWithoutTrack(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Forget("t", 0)

	o.Done(rec(0)) // late completion from a worker still running at revoke time

	_, ok := commitOffset(t, o)
	require.False(t, ok, "forgotten partition with no intervening Track must stay absent")
	require.Zero(t, o.InFlight())
}

// F5 regression: InFlight must correctly count work tracked on a partition
// revived by Track after Forget.
func TestInFlightCountsRevivedPartition(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Done(rec(0))
	o.Forget("t", 0)

	o.Track(rec(1))
	o.Track(rec(2))
	require.Equal(t, 2, o.InFlight())

	o.Done(rec(1))
	require.Equal(t, 1, o.InFlight())
}

// TestMarkCommittedDropsDoneEntries proves done entries below the new
// committed watermark are pruned so the map cannot grow without bound.
func TestMarkCommittedDropsDoneEntries(t *testing.T) {
	o := NewOffsets()
	o.Track(rec(0))
	o.Track(rec(1))
	o.Done(rec(0))
	o.Done(rec(1))

	m := o.Commitable()
	o.MarkCommitted(m)

	st := o.p[partitionKey{"t", 0}]
	require.Empty(t, st.done, "done entries below the new committed watermark must be dropped")
}
