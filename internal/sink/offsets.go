package sink

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type partitionKey struct {
	topic     string
	partition int32
}

type partitionState struct {
	// pending holds unfinished offsets, mapped to that record's leader epoch:
	// Pending must return the epoch, so that a rewind via
	// SetOffsets does not lose it. No assumption about order:
	// any offset can appear and finish in any order
	// relative to the others.
	pending map[int64]int32
	// done holds finished offsets, mapped to that record's leader epoch.
	done map[int64]int32
	// committed is the last offset handed out by Commitable and confirmed by
	// MarkCommitted; -1 means "nothing committed yet", so it is not
	// confused with a legitimate first commit at offset 0.
	committed int64
	// lowest is the smallest offset passed to Track since this state was
	// created. It is the baseline CommitLag uses while committed is still the
	// sentinel: a partition resumed at offset 1,000,000 is one record behind on
	// its first poll, not a million, and Forget drops committed, so a revived
	// partition needs the same treatment as a freshly assigned one.
	lowest int64
	// highest is the largest offset ever passed to Track for this
	// partition. Unlike committed, it never decreases and is never
	// cleaned up: CommitLag would not be able to tell "the partition is fully
	// drained" apart from "nothing has ever been read from it" if it relied only on
	// pending/done — both are empty in the first case just as in the second.
	highest int64
	// forgotten marks a partition revoked via Forget. While the flag is
	// set, Done does nothing — this suppresses a late Done from a
	// worker that was still running at the moment of revoke. Track, conversely,
	// clears the flag and starts the partition with clean state: the next
	// Track means the group has reassigned the partition to this consumer, and
	// it is legitimately being read again.
	forgotten bool
}

// Offsets tracks record completion and hands out for commit
// only a contiguous prefix. Committing a gap would mean losing a message.
type Offsets struct {
	mu sync.Mutex
	p  map[partitionKey]*partitionState
}

func NewOffsets() *Offsets {
	return &Offsets{p: make(map[partitionKey]*partitionState)}
}

// Track registers a record as "in progress". The order of Track calls for a
// partition does not matter — offsets can arrive in any order.
//
// If the partition was marked by Forget, Track clears the tombstone and gives it
// clean state: the partition has been reassigned to this consumer again
// (cooperative-sticky rebalancing), and it must not stay dead
// forever. Fresh pending/done means a late Done with an offset from before
// revoke can only do anything if that same offset was read again
// and got its own Track after the revival.
func (o *Offsets) Track(rec *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	k := partitionKey{rec.Topic, rec.Partition}
	st, ok := o.p[k]
	if !ok || st.forgotten {
		st = &partitionState{
			pending:   make(map[int64]int32),
			done:      make(map[int64]int32),
			committed: -1,
			lowest:    -1,
		}
		o.p[k] = st
	}
	st.pending[rec.Offset] = rec.LeaderEpoch
	if rec.Offset > st.highest {
		st.highest = rec.Offset
	}
	if st.lowest < 0 || rec.Offset < st.lowest {
		st.lowest = rec.Offset
	}
}

// Done moves an offset from pending to done. If the offset is not in pending
// (a repeated Done, a Done without a Track, or the partition is forgotten/does not exist),
// the call is ignored — no counter gets corrupted.
func (o *Offsets) Done(rec *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	st, ok := o.p[partitionKey{rec.Topic, rec.Partition}]
	if !ok || st.forgotten {
		return
	}
	if _, pending := st.pending[rec.Offset]; !pending {
		return
	}
	delete(st.pending, rec.Offset)
	st.done[rec.Offset] = rec.LeaderEpoch
}

// Commitable hands out the offset of the next unprocessed record — exactly
// what Kafka expects in an OffsetCommit. The watermark is min(pending) if
// there are unfinished records (everything below it is safe, pending itself is not), and
// if pending is empty, max(done)+1. The epoch is taken from the record at the boundary
// (watermark-1) — that is the one that is actually committed.
func (o *Offsets) Commitable() map[string]map[int32]kgo.EpochOffset {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]map[int32]kgo.EpochOffset)
	for k, st := range o.p {
		var watermark int64
		switch {
		case len(st.pending) > 0:
			watermark = minOffset(st.pending)
		case len(st.done) > 0:
			watermark = maxOffset(st.done) + 1
		default:
			continue
		}
		if watermark <= st.committed {
			continue
		}
		epoch, ok := st.done[watermark-1]
		if !ok {
			// The boundary record is not finished yet — there is nothing to commit.
			continue
		}
		tp, ok := out[k.topic]
		if !ok {
			tp = make(map[int32]kgo.EpochOffset)
			out[k.topic] = tp
		}
		tp[k.partition] = kgo.EpochOffset{Epoch: epoch, Offset: watermark}
	}
	return out
}

// Pending hands out, for every partition with records registered but not
// finished, the minimum such offset — the point from which the partition
// needs to be re-read if an abandoned batch was not finished. EpochOffset.Epoch
// is franz-go's lastConsumedEpoch, the epoch of the record at offset-1, not the epoch
// of the returned (not yet read) record itself; if offset-1 is not
// finished (e.g. nothing was read before it by this consumer),
// we hand out -1 — franz-go's documented sentinel that disables
// truncation detection checking for this partition.
// Partitions with no unfinished records, and tombstones, do not appear in the result.
func (o *Offsets) Pending() map[string]map[int32]kgo.EpochOffset {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]map[int32]kgo.EpochOffset)
	for k, st := range o.p {
		if st.forgotten || len(st.pending) == 0 {
			continue
		}
		offset := minOffset(st.pending)
		epoch, ok := st.done[offset-1]
		if !ok {
			epoch = -1
		}
		tp, ok := out[k.topic]
		if !ok {
			tp = make(map[int32]kgo.EpochOffset)
			out[k.topic] = tp
		}
		tp[k.partition] = kgo.EpochOffset{Epoch: epoch, Offset: offset}
	}
	return out
}

// MarkCommitted is called after a successful commit, so as not to send
// the same offset again, and it cleans done of records below the new
// watermark, so the map does not grow without bound.
func (o *Offsets) MarkCommitted(committed map[string]map[int32]kgo.EpochOffset) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for topic, parts := range committed {
		for part, eo := range parts {
			st, ok := o.p[partitionKey{topic, part}]
			if !ok || st.forgotten {
				continue
			}
			st.committed = eo.Offset
			for offset := range st.done {
				if offset < st.committed {
					delete(st.done, offset)
				}
			}
		}
	}
}

// Forget marks a partition as revoked: its state is replaced with a tombstone,
// which suppresses a late Done from a worker that has not yet learned about the revoke.
// The tombstone is not permanent: the next Track for the same partition means the
// group has reassigned it to this consumer, and it clears the tombstone (see Track).
func (o *Offsets) Forget(topic string, partition int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.p[partitionKey{topic, partition}] = &partitionState{forgotten: true}
}

// CommitLag returns, per partition with tracked state, the number of
// tracked records at or beyond the committed watermark — i.e. the gap
// between the highest offset ever tracked and the committed watermark.
// highest is inclusive (it names an offset actually seen), while committed
// follows Kafka's OffsetCommit convention of naming the next offset to
// fetch (exclusive of everything already committed), so the two are
// reconciled as (highest + 1) - committed rather than a bare subtraction —
// otherwise a partition that has just fully caught up would report -1
// instead of 0. Before the first commit, committed is the sentinel -1
// (nothing committed yet, see partitionState.committed) and the baseline is
// the lowest offset this consumer has tracked: what it is behind on is what
// it has read, not everything that precedes it in the log.
// Forgotten (revoked) partitions are excluded — there is nothing left for
// this consumer to be behind on.
func (o *Offsets) CommitLag() map[string]map[int32]int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]map[int32]int64)
	for k, st := range o.p {
		if st.forgotten {
			continue
		}
		committed := st.committed
		if committed < 0 {
			// Nothing committed yet. The baseline is where this consumer
			// started reading the partition, not the beginning of the log.
			committed = st.lowest
		}
		tp, ok := out[k.topic]
		if !ok {
			tp = make(map[int32]int64)
			out[k.topic] = tp
		}
		tp[k.partition] = st.highest + 1 - committed
	}
	return out
}

func (o *Offsets) InFlight() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, st := range o.p {
		n += len(st.pending)
	}
	return n
}

func minOffset[V any](m map[int64]V) int64 {
	min, first := int64(0), true
	for off := range m {
		if first || off < min {
			min, first = off, false
		}
	}
	return min
}

func maxOffset(m map[int64]int32) int64 {
	max, first := int64(0), true
	for off := range m {
		if first || off > max {
			max, first = off, false
		}
	}
	return max
}
