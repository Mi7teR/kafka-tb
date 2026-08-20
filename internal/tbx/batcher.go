package tbx

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

var (
	// ErrCommandTooLarge means the command does not fit into a batch whole.
	// It cannot be cut: the atomicity of the linked chain matters more.
	ErrCommandTooLarge = errors.New("command exceeds max batch size")
	ErrClosed          = errors.New("batcher closed")
)

const linkedBit uint16 = 1

type job struct {
	cmd  *model.Command
	done chan SubmitResult
}

// SubmitResult is the outcome of one command: either the outcomes of all its events, or an error.
// Exactly one such result arrives for every command placed on the queue.
type SubmitResult struct {
	Outcomes []Outcome
	Err      error
}

// Batcher is the single door into TigerBeetle.
// It keeps one worker with one queue and no more than one batch in flight, which
// guarantees that the application order matches the Submit order.
type Batcher struct {
	client  Client
	cfg     config.Batcher
	retry   config.Retry
	log     *slog.Logger
	metrics *obs.Metrics

	// queue is one for both operations. A separate queue per operation type would be a
	// separate worker, and two workers are exactly the kind of concurrency that
	// must not exist: create_accounts and create_transfers from one partition
	// would come apart, and a transfer from an account that has not been created yet would return
	// debit_account_not_found — a business rejection and a DLQ for a legitimate record.
	queue chan *job

	// stop is the single stop signal, shared by all participants.
	// It is closed exactly once: either from Close(), or when Start's context is
	// cancelled. Both paths must converge here — otherwise cancelling the context extinguishes
	// the loop while submitters keep believing the batcher is alive and hang forever.
	// The protocol is deliberately lock-free: a closed channel is equally visible to everyone,
	// so a submitter always has a way out of a blocking select.
	stopOnce sync.Once
	stop     chan struct{}

	// finished means "the loop has exited and the queue has been drained", not "a
	// stop was requested". The difference is essential: while the loop is alive, it
	// can still respond to a command already placed on the queue, and the submitter
	// must wait for that response. A submitter that has to wait for the batcher
	// rather than for its own outcome exits only on finished — only then is
	// "no one will respond anymore" a guarantee rather than a guess.
	// Only watchLate needs it; the ordinary submitter holds j.done, which settle
	// answers.
	finishedOnce sync.Once
	finished     chan struct{}

	// unwatch cancels the subscription to Start's context cancellation.
	// It is written in Start and read in Close; the lifecycle contract
	// (exactly one Start, Close strictly after it) is described in their doc comments.
	unwatch func() bool
	wg      sync.WaitGroup
}

func NewBatcher(c Client, cfg config.Batcher, retry config.Retry, log *slog.Logger, metrics *obs.Metrics) *Batcher {
	return &Batcher{
		client:   c,
		cfg:      cfg,
		retry:    retry,
		log:      log,
		metrics:  metrics,
		queue:    make(chan *job, cfg.MaxQueue),
		stop:     make(chan struct{}),
		finished: make(chan struct{}),
	}
}

// Start launches the send loop. It is called exactly once and strictly before Close;
// a repeated call, or one concurrent with Close, is not supported.
func (b *Batcher) Start(ctx context.Context) {
	// Context cancellation is the same shutdown as Close().
	b.unwatch = context.AfterFunc(ctx, b.signalStop)
	b.wg.Add(1)
	go func() { defer b.wg.Done(); b.loop() }()
	// The watcher lives here, not in Close: the stop path via context cancellation
	// does not go through Close, but it must release submitters just the same.
	go func() { b.wg.Wait(); b.settle() }()
}

// SubmitAsync places the command on the queue and immediately returns a channel that
// will receive exactly one outcome. An error is returned only at submission itself:
// an empty or too-large command, a stopped batcher, a cancelled context.
// Everything that happens after submission arrives on the channel, not as this error.
//
// Blocking on a full queue is preserved — this is backpressure for the consumer:
// without it the sink would enqueue an entire poll and eat up memory.
//
// The channel is buffered by one, and it has exactly one writer, so a
// caller that abandons the channel without reading it does not block anyone.
func (b *Batcher) SubmitAsync(ctx context.Context, cmd *model.Command) (<-chan SubmitResult, error) {
	if cmd.Len() == 0 {
		return nil, errors.New("empty command")
	}
	if cmd.Len() > b.cfg.MaxBatchSize {
		return nil, ErrCommandTooLarge
	}
	j := &job{cmd: cmd, done: make(chan SubmitResult, 1)}

	// Fast-fail when the batcher is already stopped: otherwise the select below would choose
	// randomly between stop and free room in the queue.
	select {
	case <-b.stop:
		return nil, ErrClosed
	default:
	}

	// stop is mandatory in this select: without it, a submitter stuck against a full
	// queue after the loops have stopped blocks forever.
	select {
	case <-b.stop:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case b.queue <- j:
	}

	// The command is on the queue and shutdown had not begun when it got there, so
	// somebody is guaranteed to answer it: j.done goes to the caller as it is.
	//
	// The guarantee is an ordering argument, not a hope. Every shutdown path closes
	// stop first and drains the queue only afterwards (loop's stop branch, settle).
	// Operations on one channel are totally ordered, so a stop this receive does not
	// observe as closed is a stop that closes after it — and therefore after the
	// send above, which precedes it in program order. A drain that starts after our
	// send finds the job already in the buffer and empties the buffer, so it cannot
	// miss it.
	//
	// This ordering is what keeps the hot path off a process-wide channel. The
	// previous shape — a goroutine per command selecting on j.done and the shared
	// finished — made every waiter lock and unlock that one channel, which was 93%
	// of all mutex delay in the sink (.superpowers/sdd/perf-pprof.md §2b).
	select {
	case <-b.stop:
		// Shutdown raced this submission: the drain may already be behind us, and
		// then nothing would ever write to j.done. Only this case needs finished.
		return b.watchLate(j), nil
	default:
		return j.done, nil
	}
}

// watchLate waits for a command that landed on the queue with shutdown already under
// way — the one case where no drain is guaranteed to see it. It is off the hot path
// by construction: it is reached only once stop is closed.
func (b *Batcher) watchLate(j *job) <-chan SubmitResult {
	out := make(chan SubmitResult, 1)
	go func() {
		select {
		case res := <-j.done:
			out <- res
		case <-b.finished:
			// We wait specifically on finished here, not on stop. stop only means "shutdown has
			// started": this command's batch may already be in TigerBeetle and
			// about to return an outcome. Responding with ErrClosed at this moment would be a lie
			// about work that was actually applied; the caller is not obligated to repeat the request with
			// the same id, and it has no way to recover the truth.
			// finished, on the other hand, is closed after the loop has exited and the
			// queue has been drained, i.e. when there is truly no one left to answer.
			//
			// The race "the outcome is delivered exactly when the loop exits" is real:
			// both channels are ready, and select would choose randomly. We restore
			// priority for the outcome with an explicit non-empty check.
			select {
			case res := <-j.done:
				out <- res
			default:
				// The command reached the queue so late that no drain
				// saw it. We respond with an error rather than staying silent; from here on it relies
				// on idempotency by id — a retry yields TransferExists/
				// AccountExists, and MapTransferResults/MapAccountResults
				// treat them as StatusOK.
				out <- SubmitResult{Err: ErrClosed}
			}
		}
	}()
	return out
}

// Submit places the command on the queue and waits for the outcome.
// Blocking on a full queue is backpressure for the consumer.
func (b *Batcher) Submit(ctx context.Context, cmd *model.Command) ([]Outcome, error) {
	done, err := b.SubmitAsync(ctx, cmd)
	if err != nil {
		return nil, err
	}
	select {
	case res := <-done:
		return res.Outcomes, res.Err
	case <-ctx.Done():
		// The command is already on the queue and may still reach TigerBeetle after this
		// return: it cannot be cancelled from here. The caller (the Kafka sink) will see
		// the error and most likely retry the command. Correctness here rests
		// entirely on idempotency by id: a retry yields
		// TransferExists/AccountExists, and MapTransferResults/MapAccountResults
		// treat them as StatusOK. This works only because the ids
		// come from the caller and are stable across attempts.
		return nil, ctx.Err()
	}
}

// Close closes intake and waits for the loop to drain the queue.
// It is called strictly after Start and not concurrently with it.
//
// Close is not bounded in time: it waits for the current request to
// TigerBeetle to finish. A hung client call delays both Close and the submitters
// waiting for their outcome. This is a deliberate tradeoff — a synchronous caller
// is not told of an error for an operation that in fact was applied.
// Bounding this belongs to a timeout on the client side, not here.
func (b *Batcher) Close() {
	b.signalStop()
	if b.unwatch != nil {
		b.unwatch()
	}
	b.wg.Wait()
	// A safeguard for the case where Start was never called: there is no watcher,
	// no loop to drain the queue, and a submitter holding j.done would wait for an
	// outcome forever.
	b.settle()
}

// settle closes the door behind the loop: it answers whatever is still on the queue
// and only then declares that nobody will answer anymore. Both halves matter.
//
// The drain is what lets SubmitAsync hand j.done to the caller: it is the last
// receiver on the queue, and it runs after stop is closed, so it sees every command
// that got in while stop was still open — including when Start was never called and
// there is no loop to drain at all. Draining before closing finished is deliberate:
// an outcome must beat ErrClosed wherever both are possible.
//
// It runs from Close and from the watcher, possibly at the same time. That is safe:
// a queued job is received by exactly one of them, and only its receiver answers it,
// so the "exactly one result per command" rule holds either way.
func (b *Batcher) settle() {
	b.drain(b.queue, ErrClosed)
	b.signalFinished()
}

// signalStop closes stop exactly once, no matter where the signal came from.
func (b *Batcher) signalStop() {
	b.stopOnce.Do(func() { close(b.stop) })
}

// signalFinished closes finished exactly once.
func (b *Batcher) signalFinished() {
	b.finishedOnce.Do(func() { close(b.finished) })
}

// loop accumulates consecutive commands of one operation and sends what has
// accumulated when the operation changes, max_batch_size is reached, or linger expires — whichever
// comes first. A change of operation is as much a reason to send as the other
// two: one request to TigerBeetle carries events of exactly one type, accounts cannot be mixed
// with transfers. Sending happens strictly sequentially, so the
// application order matches the submission order, including at the seam between operations.
func (b *Batcher) loop() {
	var (
		batch []*job
		size  int
		op    model.Op
		timer *time.Timer
		tick  <-chan time.Time
	)
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer, tick = nil, nil
		}
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		stopTimer()
		if err := b.send(op, batch); err != nil {
			b.failAll(batch, err)
		}
		batch, size = nil, 0
	}

	for {
		select {
		case <-b.stop:
			// We still send what has accumulated, and answer the rest of the queue with an error:
			// every command that made it onto the queue gets exactly one outcome.
			flush()
			b.drain(b.queue, ErrClosed)
			return
		case j := <-b.queue:
			// One operation's run has ended — send it whole, the next one
			// starts with this command.
			if len(batch) > 0 && j.cmd.Op != op {
				flush()
			}
			// The command does not fit in what remains — send what has accumulated and start a new batch.
			if size+j.cmd.Len() > b.cfg.MaxBatchSize {
				flush()
			}
			op = j.cmd.Op
			batch = append(batch, j)
			size += j.cmd.Len()
			if size >= b.cfg.MaxBatchSize {
				flush()
				continue
			}
			if timer == nil {
				timer = time.NewTimer(b.cfg.Linger)
				tick = timer.C
			}
		case <-tick:
			flush()
		}
	}
}

func (b *Batcher) drain(queue chan *job, err error) {
	for {
		select {
		case j := <-queue:
			j.done <- SubmitResult{Err: err}
		default:
			return
		}
	}
}

func (b *Batcher) failAll(jobs []*job, err error) {
	for _, j := range jobs {
		j.done <- SubmitResult{Err: err}
	}
}

// send picks the request by the accumulated run's operation. The run is homogeneous by
// construction: loop sends what has accumulated as soon as the operation changes.
func (b *Batcher) send(op model.Op, jobs []*job) error {
	if op == model.OpCreateAccounts {
		return b.sendAccounts(jobs)
	}
	return b.sendTransfers(jobs)
}

func (b *Batcher) sendTransfers(jobs []*job) error {
	events := make([]types.Transfer, 0, b.cfg.MaxBatchSize)
	offsets := make([]int, len(jobs))
	for i, j := range jobs {
		offsets[i] = len(events)
		events = append(events, j.cmd.Transfers...)
		// The chain must not stay open at the seam between commands.
		events[len(events)-1].Flags &^= linkedBit
	}

	b.metrics.ObserveBatchSize(len(events))
	start := time.Now()
	results, err := b.call(func() (any, error) { return b.client.CreateTransfers(events) })
	b.metrics.ObserveTBLatency(string(model.OpCreateTransfers), time.Since(start))
	if err != nil {
		return err
	}
	typed, _ := results.([]types.CreateTransferResult)
	for i, j := range jobs {
		outcomes, mapErr := MapTransferResults(j.cmd, typed, offsets[i], len(events))
		j.done <- SubmitResult{Outcomes: outcomes, Err: mapErr}
	}
	return nil
}

func (b *Batcher) sendAccounts(jobs []*job) error {
	events := make([]types.Account, 0, b.cfg.MaxBatchSize)
	offsets := make([]int, len(jobs))
	for i, j := range jobs {
		offsets[i] = len(events)
		events = append(events, j.cmd.Accounts...)
		events[len(events)-1].Flags &^= linkedBit
	}

	b.metrics.ObserveBatchSize(len(events))
	start := time.Now()
	results, err := b.call(func() (any, error) { return b.client.CreateAccounts(events) })
	b.metrics.ObserveTBLatency(string(model.OpCreateAccounts), time.Since(start))
	if err != nil {
		return err
	}
	typed, _ := results.([]types.CreateAccountResult)
	for i, j := range jobs {
		outcomes, mapErr := MapAccountResults(j.cmd, typed, offsets[i], len(events))
		j.done <- SubmitResult{Outcomes: outcomes, Err: mapErr}
	}
	return nil
}

// call retries the call until TigerBeetle responds or the batcher is stopped.
// A call error is always infrastructural: business rejections arrive in the results.
func (b *Batcher) call(fn func() (any, error)) (any, error) {
	delay := b.retry.Initial
	for attempt := 1; ; attempt++ {
		res, err := fn()
		if err == nil {
			return res, nil
		}
		b.log.Warn("tigerbeetle call failed, retrying",
			slog.Int("attempt", attempt), slog.String("error", err.Error()), slog.Duration("in", delay))

		// stop, not just Close: on context cancellation with TigerBeetle down,
		// retries would otherwise spin forever, the batch never completes, the goroutine leaks.
		select {
		case <-b.stop:
			return nil, ErrClosed
		case <-time.After(b.jitter(delay)):
		}
		if delay < b.retry.Max {
			delay *= 2
			if delay > b.retry.Max {
				delay = b.retry.Max
			}
		}
	}
}

func (b *Batcher) jitter(d time.Duration) time.Duration {
	if !b.retry.Jitter {
		return d
	}
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}
