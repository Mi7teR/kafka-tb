package cdc

import "github.com/twmb/franz-go/pkg/kgo"

// headerMap flattens a record's headers for assertions.
func headerMap(rec *kgo.Record) map[string]string {
	out := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		out[h.Key] = string(h.Value)
	}
	return out
}
