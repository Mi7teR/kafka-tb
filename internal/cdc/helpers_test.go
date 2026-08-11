package cdc

import (
	"bytes"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// syncBuffer is a log sink a test can read while the job goroutine is still
// writing to it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// headerMap flattens a record's headers for assertions.
func headerMap(rec *kgo.Record) map[string]string {
	out := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		out[h.Key] = string(h.Value)
	}
	return out
}
