package emit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Emitter публикует DLQ и results, не дожидаясь брокера: обе публикации
// возвращают обещание, и только Wait по нему означает «брокер подтвердил».
// Синхронная публикация стоила бы round-trip на запись, а офсет всё равно
// нельзя двигать раньше подтверждения — поэтому ожидание вынесено из выдачи и
// делается пачкой.
type Emitter interface {
	DLQ(ctx context.Context, rec *kgo.Record, reason Reason, errName, detail string) *Publication
	Results(ctx context.Context, rec *kgo.Record, outcomes []tbx.Outcome) *Publication
	Flush(ctx context.Context) error
	Close()
}

// Publication — обещание одной публикации. Выданная брокеру запись ещё не
// записана: подтверждением считается только Wait, вернувший nil.
type Publication struct {
	// e — эмиттер, ведущий учёт незабранных ошибок для Flush. nil у
	// Resolved: у публикации, которую никуда не выдавали, учитывать нечего.
	e    *emitter
	done chan struct{}
	// err записывается ровно один раз, до close(done), и читается только
	// после него: канал и даёт здесь happens-before.
	err   error
	taken atomic.Bool
}

// Resolved отдаёт уже завершённое обещание с исходом err. Нужен там, где
// публикация не дошла до брокера вовсе, — отключённый топик results, стабы в
// тестах, — чтобы у вызывающего не было второго кода обработки.
func Resolved(err error) *Publication {
	done := make(chan struct{})
	close(done)
	return &Publication{done: done, err: err}
}

// NewPending отдаёт ещё не завершённое обещание и функцию, которой его
// завершают ровно один раз. Нужен любой реализации Emitter вне этого пакета:
// без него публикацию можно было бы отдать только уже готовой, а весь смысл
// обещания в том, что ответ приходит позже.
func NewPending() (*Publication, func(error)) {
	p := &Publication{done: make(chan struct{})}
	var once sync.Once
	return p, func(err error) {
		once.Do(func() {
			p.err = err
			close(p.done)
		})
	}
}

// Wait возвращает исход публикации: nil означает, что брокер её подтвердил и
// офсет записи можно двигать. Уже пришедшее подтверждение имеет приоритет над
// отменой ctx: бросить готовый ответ значило бы соврать про уже сделанную
// работу. Ошибка ctx — не исход публикации, а отказ её дожидаться: запись
// остаётся неподтверждённой и будет обработана снова.
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

// take отдаёт исход и снимает ошибку с учёта Flush: вызывающий её увидел и
// обязан отреагировать сам, второй раз докладывать о ней некому. Без этого
// одна стабильно падающая запись валила бы Flush вечно, а вместе с ним и
// коммит всех остальных партиций.
func (p *Publication) take() error {
	if p.err != nil && p.e != nil && p.taken.CompareAndSwap(false, true) {
		p.e.takeFailure()
	}
	return p.err
}

type ResultsMessage struct {
	Source  Source        `json:"source"`
	Results []ResultEntry `json:"results"`
	EmitTS  string        `json:"emitted_at"`
}

type Source struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type ResultEntry struct {
	Index  int    `json:"index"`
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type emitter struct {
	cl  *kgo.Client
	cfg config.Kafka

	// mu защищает учёт провалившихся публикаций, чей исход никто не забрал.
	// Именно они — и только они — обязаны валить Flush: коммит после такого
	// Flush закоммитил бы офсет записи, которой в DLQ или results нет.
	mu       sync.Mutex
	untaken  int
	firstErr error
}

func New(cl *kgo.Client, cfg config.Kafka) Emitter {
	return &emitter{cl: cl, cfg: cfg}
}

// DLQ публикует исходные байты без изменений: реплей должен быть возможен
// без обратной сборки сообщения.
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

// Results публикует исходы обработки команды. Пустой ResultsTopic отключает
// поток результатов: публикация становится уже подтверждённым no-op'ом.
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
	body, err := json.Marshal(msg)
	if err != nil {
		return e.failed(fmt.Errorf("marshal results: %w", err))
	}
	return e.produce(ctx, &kgo.Record{Topic: e.cfg.ResultsTopic, Key: rec.Key, Value: body}, "produce results")
}

// produce выдаёт запись брокеру и возвращает обещание, не дожидаясь ответа.
// Порядок записей внутри партиции у franz-go — порядок вызовов Produce,
// поэтому порядок публикации задаётся здесь, а не порядком Wait.
func (e *emitter) produce(ctx context.Context, out *kgo.Record, what string) *Publication {
	p := &Publication{e: e, done: make(chan struct{})}
	// Коллбэк обязан быть быстрым и неблокирующим: franz-go вызывает все
	// обещания последовательно одним воркером.
	e.cl.Produce(ctx, out, func(_ *kgo.Record, err error) {
		if err != nil {
			p.err = fmt.Errorf("%s: %w", what, err)
			e.addFailure(p.err)
		}
		close(p.done)
	})
	return p
}

// failed отдаёт обещание, провалившееся ещё до брокера, — но на том же учёте,
// что и провал самой публикации: потерять его молча нельзя ни в том, ни в
// другом случае.
func (e *emitter) failed(err error) *Publication {
	p := Resolved(err)
	p.e = e
	e.addFailure(err)
	return p
}

func (e *emitter) addFailure(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.untaken++
	if e.firstErr == nil {
		e.firstErr = err
	}
}

func (e *emitter) takeFailure() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.untaken == 0 {
		// Ошибку уже доложил Flush и сбросил учёт; повторно её не вычитаем.
		return
	}
	e.untaken--
	if e.untaken == 0 {
		e.firstErr = nil
	}
}

// Flush опустошает буфер продюсера и докладывает о публикациях, которые
// провалились, а их исход никто не забрал: коммит после такого Flush
// закоммитил бы офсет записи, которой в DLQ или results нет. Учёт при этом
// сбрасывается — доложенная ошибка не должна валить каждый следующий коммит.
func (e *emitter) Flush(ctx context.Context) error {
	if err := e.cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush producer: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.untaken == 0 {
		return nil
	}
	err := fmt.Errorf("%d unacknowledged publication(s): %w", e.untaken, e.firstErr)
	e.untaken, e.firstErr = 0, nil
	return err
}

func (e *emitter) Close() { e.cl.Close() }
