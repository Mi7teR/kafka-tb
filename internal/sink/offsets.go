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
	// anchor — первый офсет, когда-либо увиденный по этой партиции
	// (устанавливается один раз). Пока next == anchor, ничего ещё не
	// завершено — next просто указывает на границу, а не на прогресс.
	anchor int64
	// next — первый ещё не завершённый офсет; всё до него готово к коммиту.
	next    int64
	hasNext bool
	// done — завершённые офсеты выше next, ждущие закрытия дырки.
	done  map[int64]struct{}
	epoch int32
	// inflight — сколько записей взято в работу и ещё не завершено.
	inflight int
	// committed — последний офсет, уже отправленный в MarkCommitted;
	// -1 означает "ещё ничего не коммитили", чтобы не спутать с
	// легитимным первым коммитом на офсете 0.
	committed int64
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

// Track регистрирует запись как «в работе». Для каждой партиции вызовы
// Track должны идти в порядке возрастания офсета (как и гарантирует Kafka
// при последовательном чтении партиции) — это устанавливает нижнюю границу,
// с которой Done() отсчитывает непрерывный завершённый префикс.
func (o *Offsets) Track(rec *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	st := o.state(rec)
	if !st.hasNext {
		st.next, st.anchor, st.hasNext = rec.Offset, rec.Offset, true
	}
	st.inflight++
}

func (o *Offsets) Done(rec *kgo.Record) {
	o.mu.Lock()
	defer o.mu.Unlock()
	st := o.state(rec)
	st.epoch = rec.LeaderEpoch
	if st.inflight > 0 {
		st.inflight--
	}
	if !st.hasNext {
		st.next, st.anchor, st.hasNext = rec.Offset, rec.Offset, true
	}
	if rec.Offset < st.next {
		return // уже учтён
	}
	st.done[rec.Offset] = struct{}{}
	for {
		if _, ok := st.done[st.next]; !ok {
			return
		}
		delete(st.done, st.next)
		st.next++
	}
}

// Commitable отдаёт офсет следующей необработанной записи — ровно то,
// что Kafka ожидает в OffsetCommit.
func (o *Offsets) Commitable() map[string]map[int32]kgo.EpochOffset {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]map[int32]kgo.EpochOffset)
	for k, st := range o.p {
		// next == anchor means nothing has completed yet for this
		// partition — next is just the low-water mark from Track,
		// not a sign of progress. Only next > anchor proves the
		// anchor offset itself (and possibly more) is done.
		if !st.hasNext || st.next <= st.anchor || st.committed == st.next {
			continue
		}
		tp, ok := out[k.topic]
		if !ok {
			tp = make(map[int32]kgo.EpochOffset)
			out[k.topic] = tp
		}
		tp[k.partition] = kgo.EpochOffset{Epoch: st.epoch, Offset: st.next}
	}
	return out
}

// MarkCommitted вызывается после успешного коммита, чтобы не слать
// один и тот же офсет повторно.
func (o *Offsets) MarkCommitted(committed map[string]map[int32]kgo.EpochOffset) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for topic, parts := range committed {
		for part, eo := range parts {
			if st, ok := o.p[partitionKey{topic, part}]; ok {
				st.committed = eo.Offset
			}
		}
	}
}

func (o *Offsets) Forget(topic string, partition int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.p, partitionKey{topic, partition})
}

func (o *Offsets) InFlight() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, st := range o.p {
		n += st.inflight
	}
	return n
}

func (o *Offsets) state(rec *kgo.Record) *partitionState {
	k := partitionKey{rec.Topic, rec.Partition}
	st, ok := o.p[k]
	if !ok {
		st = &partitionState{done: make(map[int64]struct{}), committed: -1}
		o.p[k] = st
	}
	return st
}
