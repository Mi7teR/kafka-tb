package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type (
	emitReason   = emit.Reason
	emitterIface = emit.Emitter
)

const (
	defaultCommitPeriod    = time.Second
	defaultRetryPeriod     = time.Second
	defaultShutdownTimeout = 10 * time.Second
	// defaultBatchBudget bounds the time processing one
	// batch is allowed to hold the rebalance block. franz-go's default rebalance
	// timeout is 60s; margin is needed because the budget is checked only
	// between attempts, and the attempt itself also takes time.
	defaultBatchBudget = 30 * time.Second
)

// Submitter is whatever can apply a command. In production this is *tbx.Batcher.
// Submission does not wait for the outcome: exactly one SubmitResult arrives on the returned
// channel, and this is precisely what allows keeping more than one command in the batcher at a time.
type Submitter interface {
	SubmitAsync(ctx context.Context, cmd *model.Command) (<-chan tbx.SubmitResult, error)
}

// offsetClient is the part of the Kafka client the sink uses to move offsets.
// It is factored into an interface for the testability of processBatch/commit/OnRevoked:
// without it these paths cannot be run without a live broker.
// *kgo.Client implements it as-is.
type offsetClient interface {
	CommitOffsetsSync(
		ctx context.Context,
		uncommitted map[string]map[int32]kgo.EpochOffset,
		onDone func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
	)
	SetOffsets(setOffsets map[string]map[int32]kgo.EpochOffset)
}

var _ offsetClient = (*kgo.Client)(nil)

type Sink struct {
	cl       *kgo.Client
	oc       offsetClient
	decoders codec.Registry
	sub      Submitter
	em       emitterIface
	offsets  *Offsets
	log      *slog.Logger
	metrics  *obs.Metrics

	pollSize        int
	maxInFlight     int
	commitPeriod    time.Duration
	retryPeriod     time.Duration
	batchBudget     time.Duration
	shutdownTimeout time.Duration

	// commitMu serializes committing. Run and OnRevoked are different goroutines, and
	// Commitable/MarkCommitted must go in pairs: a second Commitable must not be wedged
	// between them, otherwise the same offset would go to the broker twice, and
	// the second call's MarkCommitted would overwrite the first one's watermark.
	commitMu sync.Mutex
}

func New(
	cfg *config.Config,
	cl *kgo.Client,
	decoders codec.Registry,
	sub Submitter,
	em emitterIface,
	log *slog.Logger,
	metrics *obs.Metrics,
) *Sink {
	// A config built in code rather than loaded from YAML (integration
	// harnesses) never passes through config.Load's defaulting, and a zero
	// bound would submit nothing at all.
	maxInFlight := cfg.Sink.MaxInFlightPerPartition
	if maxInFlight <= 0 {
		maxInFlight = config.DefaultMaxInFlightPerPartition
	}
	return &Sink{
		cl:              cl,
		oc:              cl,
		decoders:        decoders,
		sub:             sub,
		em:              em,
		offsets:         NewOffsets(),
		log:             log,
		metrics:         metrics,
		pollSize:        cfg.Batcher.MaxBatchSize,
		maxInFlight:     maxInFlight,
		commitPeriod:    defaultCommitPeriod,
		retryPeriod:     defaultRetryPeriod,
		batchBudget:     defaultBatchBudget,
		shutdownTimeout: cfg.ShutdownTimeout,
	}
}

// newForTest assembles a sink without a polling client: the Run loop is verified
// via integration tests, while everything that moves offsets goes through offsetClient.
func newForTest(
	decoders codec.Registry, sub Submitter, em emitterIface, oc offsetClient, log *slog.Logger,
) (*Sink, error) {
	return &Sink{
		oc:              oc,
		decoders:        decoders,
		sub:             sub,
		em:              em,
		offsets:         NewOffsets(),
		log:             log,
		maxInFlight:     config.DefaultMaxInFlightPerPartition,
		commitPeriod:    defaultCommitPeriod,
		retryPeriod:     defaultRetryPeriod,
		batchBudget:     defaultBatchBudget,
		shutdownTimeout: defaultShutdownTimeout,
	}, nil
}

// Run spins the loop until the context is cancelled.
func (s *Sink) Run(ctx context.Context) {
	commitTicker := time.NewTicker(s.commitPeriod)
	defer commitTicker.Stop()

	for {
		// Polling is time-bounded: with an unbounded Poll on a quiet topic, the
		// periodic commit would not arrive until the next batch, and
		// the last processed record would hang uncommitted.
		// Committing from a separate goroutine is not allowed: SetOffsets in abandon
		// must not be called concurrently with a commit (see go doc SetOffsets).
		pollCtx, cancel := context.WithTimeout(ctx, s.commitPeriod)
		fetches := s.cl.PollRecords(pollCtx, s.pollSize)
		cancel()
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			// Cancellation is our own poll deadline or a routine
			// shutdown, not a fetch failure.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			s.log.Error("fetch error", slog.String("topic", t),
				slog.Int("partition", int(p)), slog.String("error", err.Error()))
		})

		// AllowRebalance is always released: the client is built with
		// BlockRebalanceOnPoll, and a skipped call — whether on the success
		// path or on panic — permanently wedges the group.
		func() {
			defer s.cl.AllowRebalance()
			s.processBatch(ctx, fetches.Records())
		}()

		if ctx.Err() != nil {
			// The final commit will not go through with an already-cancelled context, but it also
			// must not hang on an unreachable broker forever.
			shutCtx, cancelShut := context.WithTimeout(
				context.WithoutCancel(ctx), s.shutdownTimeout)
			s.commit(shutCtx, slog.LevelError)
			cancelShut()
			return
		}
		select {
		case <-commitTicker.C:
			s.commit(ctx, slog.LevelError)
		default:
		}
	}
}

// processBatch processes one poll's batch of records. Records are grouped by
// (topic, partition), and each group travels on its own goroutine: order is meaningful
// only within a partition — it was never guaranteed between partitions.
//
// All goroutines are joined before returning. It cannot be otherwise: the caller, right after
// the return, releases the rebalance block (AllowRebalance), and abandonBatch below
// moves offsets via SetOffsets — both are safe only while the
// block is still held and only from this goroutine.
func (s *Sink) processBatch(ctx context.Context, records []*kgo.Record) {
	for _, rec := range records {
		s.offsets.Track(rec)
	}
	deadline := time.Now().Add(s.batchBudget)

	var (
		wg        sync.WaitGroup
		abandoned atomic.Bool
	)
	for _, group := range groupByPartition(records) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A panic in a separate goroutine bypasses any defer of the caller —
			// including AllowRebalance, which is promised "even on
			// panic" — and kills the whole process. That is why it is caught here.
			// prepare and finish catch their own panics; what reaches here is what gets
			// past them: await, offsets.Done, and a repeated panic in finish's own
			// deferred publication. The partition is considered abandoned: its
			// records remain unmarked and therefore uncommitted, and
			// the caller will rewind it.
			defer func() {
				r := recover()
				if r == nil {
					return
				}
				s.log.Error("panic processing partition", slog.Any("panic", r),
					slog.String("topic", group[0].Topic),
					slog.Int("partition", int(group[0].Partition)))
				abandoned.Store(true)
			}()
			if !s.runPartition(ctx, group, deadline) {
				abandoned.Store(true)
			}
		}()
	}
	wg.Wait()

	// On context cancellation there is nothing to rewind: the process is shutting down, and
	// unfinished offsets already sit against the watermark and are not committed anyway.
	if abandoned.Load() && ctx.Err() == nil {
		// The batch budget ran out, or a record is failing persistently: the rebalance
		// block can no longer be held. Exactly the partitions that still have
		// unprocessed records are rewound — a partition fully drained does not
		// end up in Pending.
		s.abandonBatch()
	}
}

// groupByPartition cuts the poll into per-partition runs, preserving the record arrival
// order: within a partition this is offset order, and that is exactly what the
// submission order to the batcher rests on.
func groupByPartition(records []*kgo.Record) [][]*kgo.Record {
	var groups [][]*kgo.Record
	index := make(map[partitionKey]int)
	for _, rec := range records {
		k := partitionKey{rec.Topic, rec.Partition}
		i, ok := index[k]
		if !ok {
			i = len(groups)
			index[k] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], rec)
	}
	return groups
}

// runPartition drives one partition's records to completion, retrying the run from the
// record it broke on. Returns false if the partition had to be
// abandoned — budget exhausted or context cancelled; its unprocessed records
// remain unmarked, and the caller rewinds the partition.
//
// The retry starts specifically at the failed record, not partway through: the commands
// already submitted after it are also resubmitted, so the application order
// within the partition remains offset order on the retry run too. Reapplying
// an already-applied command is harmless — ids are stable across attempts,
// and TransferExists/AccountExists are treated as StatusOK.
func (s *Sink) runPartition(ctx context.Context, recs []*kgo.Record, deadline time.Time) bool {
	for len(recs) > 0 {
		if ctx.Err() != nil {
			return s.abandonOnShutdown(recs[0])
		}
		if !time.Now().Before(deadline) {
			return false
		}
		applied, err := s.pass(ctx, recs, deadline)
		recs = recs[applied:]
		if err == nil {
			if applied > 0 {
				continue
			}
			// Neither an error nor progress: pass was cut short by the budget or a cancellation.
			// A cancellation will be explained by the check at the top of the next iteration.
			if ctx.Err() == nil {
				return false
			}
			continue
		}
		if ctx.Err() != nil {
			return s.abandonOnShutdown(recs[0])
		}
		// Infrastructure: the same record is retried, the following ones wait for it.
		s.log.Error("record failed, retrying", slog.String("topic", recs[0].Topic),
			slog.Int("partition", int(recs[0].Partition)),
			slog.Int64("offset", recs[0].Offset), slog.String("error", err.Error()))
		if !time.Now().Add(s.retryPeriod).Before(deadline) {
			return false
		}
		if !s.backoff(ctx) {
			return false
		}
	}
	return true
}

// abandonOnShutdown explains why the record stays uncommitted, and always
// returns false. Context cancellation is a routine shutdown, not a failure: there will
// be no retry anyway, and an ERROR here would page the on-call for nothing.
func (s *Sink) abandonOnShutdown(rec *kgo.Record) bool {
	s.log.Info("shutting down, leaving record uncommitted for reprocessing",
		slog.String("topic", rec.Topic), slog.Int("partition", int(rec.Partition)),
		slog.Int64("offset", rec.Offset))
	return false
}

// pass submits recs' prefix to the batcher without waiting for a single outcome, then
// collects the outcomes in the same order, handing publications to the broker, and only
// as a third phase waits for those publications to be acknowledged and marks records
// Done. Submission order is application order in TigerBeetle; publication
// hand-off order is the order of records in results and the DLQ; both match the
// partition's offset order. Poison also passes through here: it does not go to the batcher,
// but its DLQ publication is deferred to the collection phase, otherwise the partition's
// publication would stop being sequential.
//
// Three phases rather than two, specifically for the sake of acknowledgment: waiting for the broker right
// after handing off would mean paying a round-trip per record — exactly what
// pipelining has already removed for TigerBeetle. Handed-off publications fly in parallel,
// so the combined wait phase costs the maximum, not the sum.
//
// Returns the number of leading records driven to a final outcome and
// acknowledged by the broker, and the infrastructure error that stopped this:
// the error belongs to recs[applied]. A short return with no error means
// the batch budget ran out or the process is shutting down — the remaining records stay
// unmarked.
func (s *Sink) pass(ctx context.Context, recs []*kgo.Record, deadline time.Time) (int, error) {
	if len(recs) > s.maxInFlight {
		recs = recs[:s.maxInFlight]
	}
	if len(recs) == 0 {
		// Unreachable: the caller never calls pass with an empty slice, and sink.New
		// clamps maxInFlight to at least one, so the truncation above cannot
		// leave zero. The check stands here as an explicit contract: an empty
		// run is (0, nil), with no context or slices built, and any
		// future access to recs[0] below is already guarded.
		return 0, nil
	}
	// Submission is bounded by the batch budget, not just by cancellation: SubmitAsync
	// parks on the batcher's queue, and that queue is shared and can certainly overflow
	// — there are many partitions, each with its own maxInFlight. As long as TigerBeetle responds,
	// the queue drains; the moment it goes silent, the batcher retries forever, the queue
	// stops moving, and submission without a deadline would hold the rebalance block
	// indefinitely. The outcome of an already-submitted command does not depend on this
	// context — await waits for it against the overall budget.
	enqueueCtx, cancelEnqueue := context.WithDeadline(ctx, deadline)
	defer cancelEnqueue()
	prep := make([]prepared, 0, len(recs))
	for _, rec := range recs {
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			break
		}
		p := s.prepare(enqueueCtx, rec)
		if p.err != nil && enqueueCtx.Err() != nil && ctx.Err() == nil {
			// Submission was cut short by our own deadline: this is an exhausted
			// batch budget, not a record failure. We do not raise the error — there is no point
			// retrying it, the caller will abandon the partition and rewind it.
			break
		}
		prep = append(prep, p)
		if p.err != nil {
			// Submission failed. The partition's following records must not be submitted:
			// they would apply in TigerBeetle before the failed one, while the failed one applies
			// on retry, i.e. after them. The application order within a partition
			// must be offset order, so the run stops
			// here, and the prefix already submitted is collected as usual.
			break
		}
	}
	// issued holds records' publications in offset order; issued[i] belongs to
	// recs[i]. They are collected rather than awaited in place: see the comment above.
	issued := make([]issuedPubs, 0, len(prep))
	var failErr error
	for i, p := range prep {
		var res tbx.SubmitResult
		if p.ch != nil {
			var ok bool
			if res, ok = s.await(ctx, p.ch, deadline); !ok {
				// Batch budget or cancellation: the remaining records are not published
				// at all, while the ones already handed off must still be acknowledged
				// below — otherwise their offsets cannot be moved.
				break
			}
		}
		pub, err := s.finish(ctx, recs[i], p, res)
		if err != nil {
			// The error belongs to recs[i], but it can only be declared after
			// the publications of the records before it have been acknowledged: pass must
			// return the index of the first unfinished record, and that may
			// turn out to be an earlier one whose publication did not go through.
			failErr = err
			break
		}
		issued = append(issued, pub)
	}

	applied, err := s.confirm(ctx, recs, issued, deadline)
	if err != nil || applied < len(issued) {
		return applied, err
	}
	return applied, failErr
}

// confirm waits for the acknowledgment of handed-off publications in offset order and
// marks Done exactly the leading prefix of records whose publications the broker
// has acknowledged. A record whose publication is not acknowledged never becomes
// Done under any circumstances: its offset will not be committed, the partition will be rewound, and
// the record will be processed again — it must not be lost in the DLQ.
//
// Waiting is bounded by the batch budget: franz-go retries a publication until it succeeds,
// and without a deadline a silent broker would hold the rebalance block
// indefinitely.
func (s *Sink) confirm(
	ctx context.Context, recs []*kgo.Record, issued []issuedPubs, deadline time.Time,
) (int, error) {
	if len(issued) == 0 {
		return 0, nil
	}
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for i, pub := range issued {
		for _, p := range pub.pubs {
			err := p.Wait(waitCtx)
			if err == nil {
				continue
			}
			if waitCtx.Err() != nil && errors.Is(err, waitCtx.Err()) {
				// No response was received in time: the batch budget ran out or the process
				// is shutting down. This is not a record failure — there is nothing to retry,
				// the whole partition will be rewound. Cross-checking against the error itself
				// is mandatory: a real broker failure that happened to arrive right at
				// the expired deadline must be visible as a failure, not
				// lost in the general "no response in time".
				return i, nil
			}
			s.metrics.IncRecords("blocked")
			return i, err
		}
		// Counted here, not at publication hand-off: before acknowledgment
		// a record's processing is not final — it can still be sent for a retry,
		// and then ok/rejected would be counted twice.
		s.count(pub)
		s.offsets.Done(recs[i])
	}
	return len(issued), nil
}

// count records the final outcome of an acknowledged record.
func (s *Sink) count(pub issuedPubs) {
	if pub.isPoison {
		s.metrics.IncRecords("poison")
		s.metrics.IncDLQ(string(emit.ReasonPoison), pub.poison)
		return
	}
	for _, o := range pub.outcomes {
		if o.Status == tbx.StatusRejected {
			s.metrics.IncDLQ(string(emit.ReasonReject), o.Error)
		}
	}
	for _, o := range pub.outcomes {
		s.metrics.IncRecords(string(o.Status))
	}
}

// await waits for one command's outcome. Returns false if waiting was abandoned —
// the batch budget ran out or the process is shutting down; the record stays unprocessed and
// therefore uncommitted. An outcome that has already arrived takes priority over both
// reasons to leave: discarding it would mean lying about applied work when
// the truth is already in hand.
func (s *Sink) await(ctx context.Context, ch <-chan tbx.SubmitResult, deadline time.Time) (tbx.SubmitResult, bool) {
	select {
	case res := <-ch:
		return res, true
	default:
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case res := <-ch:
		return res, true
	case <-ctx.Done():
		return tbx.SubmitResult{}, false
	case <-timer.C:
		return tbx.SubmitResult{}, false
	}
}

// abandonBatch abandons an unfinished batch and rewinds partitions back.
// Records already fetched will not arrive again on their own, so without a
// rewind an abandoned offset would stay in pending forever: the partition would never
// commit for the rest of the process's life, and the memory under done would keep growing.
// Forget places a tombstone; the next Track for that same partition will revive it.
func (s *Sink) abandonBatch() {
	pending := s.offsets.Pending()
	if len(pending) == 0 {
		return
	}
	// SetOffsets is safe specifically here: the rebalance is still blocked by Poll,
	// and a commit runs from this same goroutine and is not in progress right now.
	s.oc.SetOffsets(pending)
	for topic, parts := range pending {
		for partition, eo := range parts {
			s.offsets.Forget(topic, partition)
			s.log.Warn("batch budget exceeded, partition rewound",
				slog.String("topic", topic), slog.Int("partition", int(partition)),
				slog.Int64("offset", eo.Offset),
				slog.Duration("budget", s.batchBudget))
		}
	}
}

// backoff returns false if there is no point waiting any longer — the context is cancelled.
func (s *Sink) backoff(ctx context.Context) bool {
	t := time.NewTimer(s.retryPeriod)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// prepared is the state of one submitted record: either an outcome channel, or a
// decision already made before the batcher. No publication is done here in either
// case: its order must match offset order, and the submission phase
// deliberately does not wait for outcomes and therefore cannot publish anything in order.
type prepared struct {
	// ch is the channel for exactly one outcome; nil if the record never went to the batcher.
	ch <-chan tbx.SubmitResult
	// poison names the error for the DLQ of a record that never reached the batcher and
	// never will. Empty if the record was submitted.
	poison string
	detail string
	// err is an infrastructure failure of the submission itself.
	err error
}

// prepare decodes the record and submits its command to the batcher, without waiting for the outcome.
// A panic is a defect in handling this message, not the whole stream.
func (s *Sink) prepare(ctx context.Context, rec *kgo.Record) (p prepared) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		s.log.Error("panic handling record", slog.Any("panic", r),
			slog.String("topic", rec.Topic), slog.Int64("offset", rec.Offset))
		p = prepared{poison: "panic", detail: fmt.Sprint(r)}
	}()

	dec, derr := s.decoders.For(rec.Topic)
	if derr != nil {
		return prepared{poison: "unknown_topic", detail: derr.Error()}
	}

	cmd, derr := dec.Decode(rec.Value)
	if derr != nil {
		// codec.Decoder's contract: any decoding error is poison. Treating
		// it as infrastructural would mean retrying the record forever.
		return prepared{poison: "decode", detail: derr.Error()}
	}

	ch, serr := s.sub.SubmitAsync(ctx, cmd)
	if serr != nil {
		if errors.Is(serr, tbx.ErrCommandTooLarge) {
			return prepared{poison: "command_too_large", detail: serr.Error()}
		}
		return prepared{err: serr}
	}
	return prepared{ch: ch}
}

// issuedPubs holds one record's publications, handed off to the broker but not yet
// acknowledged, together with what needs to be counted once they are acknowledged.
// An empty pubs is impossible: every record that reaches publication has either
// results or a DLQ entry.
type issuedPubs struct {
	pubs []*emit.Publication
	// isPoison distinguishes a record that will never be applied from one that was:
	// which branch count takes for the metric depends on this flag, not on the
	// poison value. poison is only the error text for the DLQ and metric;
	// making it the sole flag would let an empty errName
	// silently fall through into the outcomes branch.
	isPoison bool
	// poison names the error for the DLQ of a record that will never be applied.
	// Meaningful only when isPoison is true.
	poison string
	// outcomes holds the applied command's outcomes; nil for poison.
	outcomes []tbx.Outcome
}

// finish hands the record's publications to the broker without waiting for acknowledgment, and
// returns them. An error means an infrastructure problem before publication:
// the offset stays put, the record will be processed again. The absence of an error
// does not yet mean the offset can be moved — confirm decides that.
func (s *Sink) finish(
	ctx context.Context, rec *kgo.Record, p prepared, res tbx.SubmitResult,
) (pub issuedPubs, err error) {
	// pubs holds the publications already handed to the broker by the time a panic
	// happens below, if it does. A panic caught by recover replaces the outcome, but has no right to
	// erase what has already been sent: that publication will still land and must
	// be waited for, otherwise Done would happen based solely on the poison DLQ's ack,
	// while this first publication hangs unaccounted for.
	var pubs []*emit.Publication
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// A panic is a defect in handling this message, not the whole stream.
		s.log.Error("panic handling record", slog.Any("panic", r),
			slog.String("topic", rec.Topic), slog.Int64("offset", rec.Offset))
		poison := s.emitPoison(ctx, rec, "panic", fmt.Sprint(r))
		pub = issuedPubs{pubs: append(pubs, poison.pubs...), isPoison: true, poison: poison.poison}
		err = nil
	}()

	if p.poison != "" {
		return s.emitPoison(ctx, rec, p.poison, p.detail), nil
	}
	if p.err != nil {
		s.metrics.IncRecords("blocked")
		return issuedPubs{}, p.err
	}
	if res.Err != nil {
		// The real batcher rejects a too-large command right at
		// submission, but a Submitter is entitled to report this as an outcome instead; we treat it
		// the same regardless of where it came from.
		if errors.Is(res.Err, tbx.ErrCommandTooLarge) {
			return s.emitPoison(ctx, rec, "command_too_large", res.Err.Error()), nil
		}
		s.metrics.IncRecords("blocked")
		return issuedPubs{}, res.Err
	}

	pubs = make([]*emit.Publication, 0, 1+len(res.Outcomes))
	pubs = append(pubs, s.em.Results(ctx, rec, res.Outcomes))
	for _, o := range res.Outcomes {
		if o.Status != tbx.StatusRejected {
			continue
		}
		detail := fmt.Sprintf("event %d (id %s): %s", o.Index, o.ID, o.Error)
		pubs = append(pubs, s.em.DLQ(ctx, rec, emit.ReasonReject, o.Error, detail))
	}
	return issuedPubs{pubs: pubs, outcomes: res.Outcomes}, nil
}

// emitPoison hands the broker a record that will never be applied.
// Its offset can be moved only after confirm has waited for the acknowledgment of
// this publication.
func (s *Sink) emitPoison(ctx context.Context, rec *kgo.Record, errName, detail string) issuedPubs {
	return issuedPubs{
		pubs:     []*emit.Publication{s.em.DLQ(ctx, rec, emit.ReasonPoison, errName, detail)},
		isPoison: true,
		poison:   errName,
	}
}

// commit hands the broker a contiguous prefix of processed offsets.
// A Flush before committing is mandatory: committing the offset of a record whose DLQ or results
// publication is still sitting in the producer's buffer would mean losing it if the process crashes.
//
// level sets the log level for a failed commit and is passed in by the
// caller rather than decided internally: for OnRevoked a failed commit is routine —
// it is a race between revoke and the client closing (the context is already cancelled), and the same offset
// will go out on the partition's next assignment. For the periodic and shutdown commit
// in Run, a failure means the broker is consistently rejecting commits, and this
// must be visible to alerting.
func (s *Sink) commit(ctx context.Context, level slog.Level) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	// Reported regardless of what follows (even "nothing commitable" or a
	// failed commit below): an operator alerting on this gauge needs it to
	// reflect the current gap, not go silent whenever there is nothing new
	// to commit.
	defer s.reportCommitLag()

	offsets := s.offsets.Commitable()
	if len(offsets) == 0 {
		return
	}
	if err := s.em.Flush(ctx); err != nil {
		s.log.Error("flush before commit failed", slog.String("error", err.Error()))
		return
	}
	var failed bool
	s.oc.CommitOffsetsSync(ctx, offsets,
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				failed = true
				s.log.Log(ctx, level, "commit failed", slog.String("error", err.Error()))
				return
			}
			// A partition-level error is not surfaced in err: without this check,
			// an uncommitted offset would be marked as committed.
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					perr := kerr.ErrorForCode(p.ErrorCode)
					if perr == nil {
						continue
					}
					failed = true
					s.log.Log(ctx, level, "commit failed", slog.String("topic", t.Topic),
						slog.Int("partition", int(p.Partition)),
						slog.String("error", perr.Error()))
				}
			}
		})
	if failed {
		// We do not move the watermark at all: recommitting the same offset
		// is harmless, but a premature MarkCommitted is not.
		return
	}
	s.offsets.MarkCommitted(offsets)
}

// reportCommitLag publishes kafkatb_offset_commit_lag from the offsets
// tracker's current state, per topic/partition.
func (s *Sink) reportCommitLag() {
	for topic, parts := range s.offsets.CommitLag() {
		for partition, lag := range parts {
			s.metrics.SetCommitLag(topic, partition, lag)
		}
	}
}

// OnRevoked commits before partitions are given up: after Forget a partition's state is
// a tombstone, and there would be nothing left to commit.
func (s *Sink) OnRevoked(ctx context.Context, revoked map[string][]int32) {
	s.commit(ctx, slog.LevelWarn)
	for topic, parts := range revoked {
		for _, p := range parts {
			s.offsets.Forget(topic, p)
		}
	}
}

// NewKafkaClient assembles a consumer with manual commit and a rebalance
// block held during processing: otherwise it is possible to commit someone else's partitions.
func NewKafkaClient(cfg *config.Config, onRevoked func(context.Context, map[string][]int32)) (*kgo.Client, error) {
	topics := make([]string, 0, len(cfg.Kafka.Topics))
	for _, t := range cfg.Kafka.Topics {
		topics = append(topics, t.Name)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ConsumerGroup(cfg.Kafka.Group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsRevoked(func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			onRevoked(ctx, revoked)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return cl, nil
}
