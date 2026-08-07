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
	types "github.com/tigerbeetle/tigerbeetle-go"
)

var (
	// ErrCommandTooLarge — команда не помещается в батч целиком.
	// Разрезать нельзя: атомарность linked-цепочки важнее.
	ErrCommandTooLarge = errors.New("command exceeds max batch size")
	ErrClosed          = errors.New("batcher closed")
)

const linkedBit uint16 = 1

type job struct {
	cmd  *model.Command
	done chan submitResult
}

type submitResult struct {
	outcomes []Outcome
	err      error
}

// Batcher — единственная дверь в TigerBeetle.
// Держит по одному in-flight батчу на каждый тип операции, чем гарантирует,
// что порядок применения совпадает с порядком Submit.
type Batcher struct {
	client Client
	cfg    config.Batcher
	retry  config.Retry
	log    *slog.Logger

	transfers chan *job
	accounts  chan *job

	// closeMu guards the transition from "accepting" to "closed" against
	// concurrent Submit calls. Without it, Submit's enqueue select can race
	// Close(): both "<-closed" and "queue<-j" become ready at once, and if
	// the send wins, the job lands in a channel no loop is left to drain,
	// hanging Submit forever. Close takes the write lock so no enqueue can
	// be in flight when it flips closed and stops the loops.
	closeMu   sync.RWMutex
	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
}

func NewBatcher(c Client, cfg config.Batcher, retry config.Retry, log *slog.Logger) *Batcher {
	return &Batcher{
		client:    c,
		cfg:       cfg,
		retry:     retry,
		log:       log,
		transfers: make(chan *job, cfg.MaxQueue),
		accounts:  make(chan *job, cfg.MaxQueue),
		closed:    make(chan struct{}),
	}
}

func (b *Batcher) Start(ctx context.Context) {
	b.wg.Add(2)
	go func() { defer b.wg.Done(); b.loop(ctx, b.transfers, b.sendTransfers) }()
	go func() { defer b.wg.Done(); b.loop(ctx, b.accounts, b.sendAccounts) }()
}

// Submit ставит команду в очередь и ждёт исход.
// Блокировка при полной очереди — это backpressure для консьюмера.
func (b *Batcher) Submit(ctx context.Context, cmd *model.Command) ([]Outcome, error) {
	if cmd.Len() == 0 {
		return nil, errors.New("empty command")
	}
	if cmd.Len() > b.cfg.MaxBatchSize {
		return nil, ErrCommandTooLarge
	}
	j := &job{cmd: cmd, done: make(chan submitResult, 1)}
	queue := b.transfers
	if cmd.Op == model.OpCreateAccounts {
		queue = b.accounts
	}

	// Hold the read lock across the check-and-enqueue: Close() cannot flip
	// closed and shut down the loops until every in-flight enqueue holding
	// this lock has finished, so a job that gets sent here is guaranteed a
	// live loop to receive it (either processed or drained on shutdown).
	b.closeMu.RLock()
	if b.isClosed() {
		b.closeMu.RUnlock()
		return nil, ErrClosed
	}
	select {
	case <-ctx.Done():
		b.closeMu.RUnlock()
		return nil, ctx.Err()
	case queue <- j:
		b.closeMu.RUnlock()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-j.done:
		return res.outcomes, res.err
	}
}

// Close закрывает приём и дожидается, пока циклы разгребут очереди.
func (b *Batcher) Close() {
	b.closeOnce.Do(func() {
		b.closeMu.Lock()
		close(b.closed)
		b.closeMu.Unlock()
	})
	b.wg.Wait()
}

// isClosed сообщает, был ли уже вызван Close.
// Вызывается только под b.closeMu (RLock или Lock).
func (b *Batcher) isClosed() bool {
	select {
	case <-b.closed:
		return true
	default:
		return false
	}
}

// loop собирает батч по правилу «max_batch_size или linger, что раньше».
func (b *Batcher) loop(ctx context.Context, queue chan *job, send func([]*job) error) {
	var (
		batch []*job
		size  int
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
		if err := send(batch); err != nil {
			b.failAll(batch, err)
		}
		batch, size = nil, 0
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			b.drain(queue, ctx.Err())
			return
		case <-b.closed:
			flush()
			b.drain(queue, ErrClosed)
			return
		case j := <-queue:
			// Команда не влезает в остаток — отправляем накопленное и начинаем новый батч.
			if size+j.cmd.Len() > b.cfg.MaxBatchSize {
				flush()
			}
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
			j.done <- submitResult{err: err}
		default:
			return
		}
	}
}

func (b *Batcher) failAll(jobs []*job, err error) {
	for _, j := range jobs {
		j.done <- submitResult{err: err}
	}
}

func (b *Batcher) sendTransfers(jobs []*job) error {
	events := make([]types.Transfer, 0, b.cfg.MaxBatchSize)
	offsets := make([]int, len(jobs))
	for i, j := range jobs {
		offsets[i] = len(events)
		events = append(events, j.cmd.Transfers...)
		// Цепочка не должна оставаться открытой на стыке команд.
		events[len(events)-1].Flags &^= linkedBit
	}

	results, err := b.call(func() (any, error) { return b.client.CreateTransfers(events) })
	if err != nil {
		return err
	}
	typed, _ := results.([]types.CreateTransferResult)
	for i, j := range jobs {
		outcomes, mapErr := MapTransferResults(j.cmd, typed, offsets[i], len(events))
		j.done <- submitResult{outcomes: outcomes, err: mapErr}
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

	results, err := b.call(func() (any, error) { return b.client.CreateAccounts(events) })
	if err != nil {
		return err
	}
	typed, _ := results.([]types.CreateAccountResult)
	for i, j := range jobs {
		outcomes, mapErr := MapAccountResults(j.cmd, typed, offsets[i], len(events))
		j.done <- submitResult{outcomes: outcomes, err: mapErr}
	}
	return nil
}

// call повторяет вызов, пока TigerBeetle не ответит или батчер не закроют.
// Ошибка вызова — всегда инфраструктурная: отказ по бизнесу приходит в результатах.
func (b *Batcher) call(fn func() (any, error)) (any, error) {
	delay := b.retry.Initial
	for attempt := 1; ; attempt++ {
		res, err := fn()
		if err == nil {
			return res, nil
		}
		b.log.Warn("tigerbeetle call failed, retrying",
			slog.Int("attempt", attempt), slog.String("error", err.Error()), slog.Duration("in", delay))

		select {
		case <-b.closed:
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
