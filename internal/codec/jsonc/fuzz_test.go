package jsonc

import (
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/codec"
)

// Invariant: the decoder either returns a command or a PoisonError.
// No panics and no other error classes.
func FuzzDecode(f *testing.F) {
	f.Add([]byte(okTransfers))
	f.Add([]byte(`{"operation":"create_accounts","accounts":[]}`))
	f.Add([]byte(`{`))
	f.Add([]byte(``))
	f.Fuzz(func(t *testing.T, payload []byte) {
		d := newFuzzDecoder()
		cmd, err := d.Decode(payload)
		if err != nil {
			if !codec.IsPoison(err) {
				t.Fatalf("non-poison error: %v", err)
			}
			return
		}
		if cmd.Len() == 0 {
			t.Fatal("decoded empty command")
		}
	})
}
