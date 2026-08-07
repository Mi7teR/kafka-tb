package emit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Emitter interface {
	DLQ(ctx context.Context, rec *kgo.Record, reason Reason, errName, detail string) error
	Results(ctx context.Context, rec *kgo.Record, outcomes []tbx.Outcome) error
	Flush(ctx context.Context) error
	Close()
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
}

func New(cl *kgo.Client, cfg config.Kafka) Emitter {
	return &emitter{cl: cl, cfg: cfg}
}

// DLQ публикует исходные байты без изменений: реплей должен быть возможен
// без обратной сборки сообщения.
func (e *emitter) DLQ(ctx context.Context, rec *kgo.Record, reason Reason, errName, detail string) error {
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
	if err := e.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("produce dlq: %w", err)
	}
	return nil
}

// Results публикует исходы обработки команды. Пустой ResultsTopic отключает
// поток результатов: вызов становится no-op.
func (e *emitter) Results(ctx context.Context, rec *kgo.Record, outcomes []tbx.Outcome) error {
	if e.cfg.ResultsTopic == "" {
		return nil
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
		return fmt.Errorf("marshal results: %w", err)
	}
	out := &kgo.Record{Topic: e.cfg.ResultsTopic, Key: rec.Key, Value: body}
	if err := e.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("produce results: %w", err)
	}
	return nil
}

func (e *emitter) Flush(ctx context.Context) error { return e.cl.Flush(ctx) }
func (e *emitter) Close()                          { e.cl.Close() }
