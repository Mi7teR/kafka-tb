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
	// pending — офсеты, зарегистрированные Track, но ещё не завершённые.
	// Отсутствие предположений о порядке: любой офсет может появиться и
	// завершиться в любом порядке относительно остальных.
	pending map[int64]struct{}
	// done — завершённые офсеты, отображённые на leader epoch той записи.
	done map[int64]int32
	// committed — последний офсет, отданный Commitable и подтверждённый
	// MarkCommitted; -1 означает "ещё ничего не коммитили", чтобы не
	// спутать с легитимным первым коммитом на офсете 0.
	committed int64
	// forgotten помечает партицию, отозванную через Forget. После этого
	// Track ничего не делает (не может воскресить партицию), а Done тоже
	// ничего не делает, так как pending/done очищены, а Done никогда не
	// создаёт состояние сам.
	forgotten bool
}

// Offsets отслеживает завершённость записей и отдаёт для коммита
// только непрерывный префикс. Коммит дырки означал бы потерю сообщения.
type Offsets struct {
	mu sync.Mutex
	p  map[partitionKey]*partitionState
}

func NewOffsets() *Offsets {
	return &Offsets{p: make(map[partitionKey]*partitionState)}
}

// Track регистрирует запись как «в работе». Порядок вызовов Track для
// партиции не имеет значения — офсеты могут приходить в любом порядке.
func (o *Offsets) Track(rec *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	k := partitionKey{rec.Topic, rec.Partition}
	st, ok := o.p[k]
	if !ok {
		st = &partitionState{
			pending:   make(map[int64]struct{}),
			done:      make(map[int64]int32),
			committed: -1,
		}
		o.p[k] = st
	}
	if st.forgotten {
		return
	}
	st.pending[rec.Offset] = struct{}{}
}

// Done переносит офсет из pending в done. Если офсета нет в pending
// (повторный Done, Done без Track, либо партиция забыта/не существует),
// вызов игнорируется — никакой счётчик не портится.
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

// Commitable отдаёт офсет следующей необработанной записи — ровно то,
// что Kafka ожидает в OffsetCommit. Ватермарк — это min(pending), если
// есть незавершённые записи (всё ниже безопасно, само pending — нет), а
// если pending пуст — max(done)+1. Epoch берётся у записи на границе
// (watermark-1) — именно она реально коммитится.
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
			// Граничная запись ещё не завершена — коммитить нечего.
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

// MarkCommitted вызывается после успешного коммита, чтобы не слать
// один и тот же офсет повторно, и чистит done от записей ниже нового
// ватермарка, чтобы карта не росла бесконечно.
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

// Forget помечает партицию как отозванную: состояние заменяется тумбстоном,
// так что последующие Track/Done для неё не смогут его воскресить.
func (o *Offsets) Forget(topic string, partition int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.p[partitionKey{topic, partition}] = &partitionState{forgotten: true}
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

func minOffset(m map[int64]struct{}) int64 {
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
