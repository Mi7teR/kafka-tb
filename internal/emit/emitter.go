package emit

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/mailru/easyjson"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Emitter publishes to the DLQ and results without waiting for the broker: both publications
// return a promise, and only Wait on it means "the broker has acknowledged". A
// synchronous publication would cost a round-trip per record, and the offset cannot
// be moved before acknowledgment anyway — so waiting is factored out of the hand-off and
// done as a batch.
type Emitter interface {
	DLQ(ctx context.Context, rec *kgo.Record, reason Reason, errName, detail string) *Publication
	Results(ctx context.Context, rec *kgo.Record, outcomes []tbx.Outcome) *Publication
	Flush(ctx context.Context) error
	Close()
}

// Publication is the promise of one publication. A record handed to the broker is not yet
// written: only a Wait that returns nil counts as acknowledgment.
type Publication struct {
	// e is the emitter that tracks unclaimed errors for Flush. nil for
	// Resolved: a publication that was never handed off anywhere has nothing to track.
	e    *emitter
	done chan struct{}
	// err is written exactly once, before close(done), and is read only
	// after it: the channel is what provides happens-before here.
	err   error
	taken atomic.Bool
}

// Resolved returns an already-completed promise with outcome err. It is needed where
// a publication never reached the broker at all — a disabled results topic, stubs in
// tests — so that the caller does not need a second code path.
func Resolved(err error) *Publication {
	done := make(chan struct{})
	close(done)
	return &Publication{done: done, err: err}
}

// NewTestPublication returns a not-yet-completed promise and a function that
// completes it exactly once. Exported for the sake of the Emitter test double
// in internal/sink (deferredEmitter), which needs a real
// *Publication with timing under its control — this is the only way to
// verify that offsets.Done happens exclusively after an ack, not at
// the publication's hand-off itself. The name sets it apart from Resolved and produce —
// the only paths by which the real Emitter constructs a publication — so it does
// not look like part of the production API.
func NewTestPublication() (*Publication, func(error)) {
	p := &Publication{done: make(chan struct{})}
	var once sync.Once
	return p, func(err error) {
		once.Do(func() {
			p.err = err
			close(p.done)
		})
	}
}

// Wait returns the publication's outcome: nil means the broker has acknowledged it and
// the record's offset can be moved. An acknowledgment that has already arrived takes priority over
// ctx cancellation: discarding a ready answer would mean lying about work already
// done. A ctx error is not the publication's outcome but a refusal to wait for it: the record
// remains unacknowledged and will be processed again.
func (p *Publication) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.take()
	default:
	}
	select {
	case <-p.done:
		return p.take()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// take returns the outcome and removes the error from Flush's tracking: the caller has
// seen it and must react on its own — there is no one left to report it a second time. Without this,
// one persistently failing record would fail Flush forever, and along with it
// the commit of every other partition.
func (p *Publication) take() error {
	if p.err != nil && p.e != nil && p.taken.CompareAndSwap(false, true) {
		p.e.takeFailure(p)
	}
	return p.err
}

//go:generate easyjson -disallow_unknown_fields $GOFILE

//easyjson:json
type ResultsMessage struct {
	Source  Source        `json:"source"`
	Results []ResultEntry `json:"results"`
	EmitTS  string        `json:"emitted_at"`
}

//easyjson:json
type Source struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

//easyjson:json
type ResultEntry struct {
	Index  int    `json:"index"`
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type emitter struct {
	cl  *kgo.Client
	cfg config.Kafka

	// mu guards the tracking of failed publications whose outcome no one has claimed.
	// Exactly those — and only those — are allowed to fail Flush: a commit after such a
	// Flush would commit the offset of a record that is in neither the DLQ nor results.
	//
	// Tracking is keyed by publication identity (a map, not a shared counter):
	// otherwise a Wait arriving after Flush has already reported and reset the
	// counter would decrement it blindly and could erase the error of a different,
	// later-added publication. order holds the order errors appeared in
	// solely so Flush can name the earliest one still not claimed —
	// that is part of the error text, not part of the "fail or not" decision.
	mu      sync.Mutex
	untaken map[*Publication]error
	order   []*Publication
}

func New(cl *kgo.Client, cfg config.Kafka) Emitter {
	return &emitter{cl: cl, cfg: cfg}
}

// DLQ publishes the original bytes unchanged: replay must be possible
// without reassembling the message from something else.
func (e *emitter) DLQ(ctx context.Context, rec *kgo.Record, reason Reason, errName, detail string) *Publication {
	out := &kgo.Record{
		Topic: e.cfg.DLQTopic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: []kgo.RecordHeader{
			{Key: HeaderReason, Value: []byte(reason)},
			{Key: HeaderError, Value: []byte(errName)},
			{Key: HeaderDetail, Value: []byte(detail)},
			{Key: HeaderSrcTopic, Value: []byte(rec.Topic)},
			{Key: HeaderSrcPartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
			{Key: HeaderSrcOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
			{Key: HeaderSrcTimestamp, Value: []byte(rec.Timestamp.UTC().Format(time.RFC3339Nano))},
			{Key: HeaderAttemptTS, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		},
	}
	return e.produce(ctx, out, "produce dlq")
}

// Results publishes a command's processing outcomes. An empty ResultsTopic disables
// the results stream: the publication becomes an already-acknowledged no-op.
func (e *emitter) Results(ctx context.Context, rec *kgo.Record, outcomes []tbx.Outcome) *Publication {
	if e.cfg.ResultsTopic == "" {
		return Resolved(nil)
	}
	msg := ResultsMessage{
		Source:  Source{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset},
		Results: make([]ResultEntry, len(outcomes)),
		EmitTS:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	for i, o := range outcomes {
		msg.Results[i] = ResultEntry{Index: o.Index, ID: o.ID, Status: string(o.Status), Error: o.Error}
	}
	body, err := easyjson.Marshal(msg)
	if err != nil {
		return e.failed(fmt.Errorf("marshal results: %w", err))
	}
	return e.produce(ctx, &kgo.Record{Topic: e.cfg.ResultsTopic, Key: rec.Key, Value: body}, "produce results")
}

// produce hands the record to the broker and returns a promise without waiting for a response.
// In franz-go, record order within a partition is Produce call order,
// so publication order is set here, not by the order of Wait calls.
func (e *emitter) produce(ctx context.Context, out *kgo.Record, what string) *Publication {
	p := &Publication{e: e, done: make(chan struct{})}
	// The callback must be fast and non-blocking: franz-go invokes all
	// promises sequentially from a single worker.
	e.cl.Produce(ctx, out, func(_ *kgo.Record, err error) {
		if err != nil {
			p.err = fmt.Errorf("%s: %w", what, err)
			e.addFailure(p, p.err)
		}
		close(p.done)
	})
	return p
}

// failed returns a promise that failed before reaching the broker at all — but on the same
// tracking as a failure of the publication itself: it must not be silently lost either
// way.
func (e *emitter) failed(err error) *Publication {
	p := Resolved(err)
	p.e = e
	e.addFailure(p, err)
	return p
}

// addFailure registers p with Flush's tracking by its own identity: two distinct
// failures must not share one slot and interfere with each other's removal from tracking.
func (e *emitter) addFailure(p *Publication, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.untaken == nil {
		e.untaken = make(map[*Publication]error)
	}
	e.untaken[p] = err
	e.order = append(e.order, p)
}

// takeFailure removes p from Flush's tracking if it is still listed there. If Flush
// has already reported this exact error and reset tracking, there is nothing to remove — decrementing
// via a shared counter is not permitted here, otherwise one publication's late Wait
// would erase a different one added after that reset.
func (e *emitter) takeFailure(p *Publication) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.untaken, p)
}

// Flush drains the producer's buffer and reports publications that
// failed and whose outcome no one claimed: a commit after such a Flush
// would commit the offset of a record that is in neither the DLQ nor results. Tracking is
// reset in the process — a reported error must not fail every subsequent commit.
func (e *emitter) Flush(ctx context.Context) error {
	if err := e.cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush producer: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.untaken) == 0 {
		return nil
	}
	// order may contain entries already removed by take (meaning they are
	// no longer in untaken); firstErr is the error of the earliest one that survived
	// to this Flush unclaimed.
	var firstErr error
	for _, p := range e.order {
		if err, ok := e.untaken[p]; ok {
			firstErr = err
			break
		}
	}
	err := fmt.Errorf("%d unacknowledged publication(s): %w", len(e.untaken), firstErr)
	e.untaken, e.order = nil, nil
	return err
}

func (e *emitter) Close() { e.cl.Close() }
