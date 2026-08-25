package cdc

import (
	"io"
	"log/slog"
	"testing"

	"github.com/mailru/easyjson"
	types "github.com/tigerbeetle/tigerbeetle-go"

	"github.com/Mi7teR/kafka-tb/internal/config"
)

// benchEncoder is the encoder the job builds, with the log discarded: an
// unknown ledger or code warns once and then never again, so logging is not
// on the measured path anyway.
func benchEncoder() *Encoder {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewEncoder(config.CDC{Topic: "events", PartitionKey: config.PartitionKeyDebitAccountID},
		testRegistry(), log)
}

// BenchmarkEncoderRecord measures what the CDC job does per change event.
// The event is fully populated, which is the expensive shape: every optional
// id is present, so nothing is omitted from the body.
func BenchmarkEncoderRecord(b *testing.B) {
	enc := benchEncoder()
	ev := sampleEvent(types.ChangeEventSinglePhase)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec, err := enc.Record(ev, ev.Timestamp)
		if err != nil {
			b.Fatal(err)
		}
		if len(rec.Value) == 0 {
			b.Fatal("empty body")
		}
	}
}

// BenchmarkEncoderRecordSparse is the other shape: no optional ids, no flags.
// It produces a shorter body, which is where the serialization buffer's
// growth behaviour differs.
func BenchmarkEncoderRecordSparse(b *testing.B) {
	enc := benchEncoder()
	ev := sampleEvent(types.ChangeEventSinglePhase)
	ev.TransferPendingID = types.Uint128{}
	ev.TransferUserData128 = types.Uint128{}
	ev.DebitAccountUserData128 = types.Uint128{}
	ev.CreditAccountUserData128 = types.Uint128{}
	ev.TransferTimeout = 0
	ev.TransferFlags = 0
	ev.DebitAccountFlags = 0
	ev.CreditAccountFlags = 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec, err := enc.Record(ev, ev.Timestamp)
		if err != nil {
			b.Fatal(err)
		}
		if len(rec.Value) == 0 {
			b.Fatal("empty body")
		}
	}
}

// BenchmarkEncoderMarshal isolates the serialization step Record spends most
// of its allocation on, so the writer-reuse change can be read on its own
// rather than through the record-building around it. The message is built
// once by Record and recovered by decoding its own body, so the field mapping
// is not duplicated here.
func BenchmarkEncoderMarshal(b *testing.B) {
	enc := benchEncoder()
	ev := sampleEvent(types.ChangeEventSinglePhase)
	rec, err := enc.Record(ev, ev.Timestamp)
	if err != nil {
		b.Fatal(err)
	}
	var msg Message
	if err := easyjson.Unmarshal(rec.Value, &msg); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body, err := enc.marshal(&msg)
		if err != nil {
			b.Fatal(err)
		}
		if len(body) == 0 {
			b.Fatal("empty body")
		}
	}
}

// BenchmarkEasyjsonMarshal is the same message through the call the encoder
// used to make, kept as the comparison point for the one above.
func BenchmarkEasyjsonMarshal(b *testing.B) {
	enc := benchEncoder()
	ev := sampleEvent(types.ChangeEventSinglePhase)
	rec, err := enc.Record(ev, ev.Timestamp)
	if err != nil {
		b.Fatal(err)
	}
	var msg Message
	if err := easyjson.Unmarshal(rec.Value, &msg); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body, err := easyjson.Marshal(msg)
		if err != nil {
			b.Fatal(err)
		}
		if len(body) == 0 {
			b.Fatal("empty body")
		}
	}
}
