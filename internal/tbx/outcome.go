package tbx

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

type Status string

const (
	StatusOK       Status = "ok"
	StatusRejected Status = "rejected"
)

// ErrResultCountMismatch means TigerBeetle returned a response that does not match the batch.
// It cannot be parsed positionally: outcomes would land on the wrong commands.
var ErrResultCountMismatch = errors.New("tigerbeetle result count does not match batch size")

// Outcome is the outcome of a single event within a command.
type Outcome struct {
	Index     int
	ID        string
	Status    Status
	Error     string // machine-readable TigerBeetle status name, empty on success
	Timestamp uint64
}

// MapTransferResults cuts the command's window out of the batch's dense response.
// offset is the position of the command's first event within the sent batch,
// batchSize is the size of the whole batch the command was sent in.
func MapTransferResults(cmd *model.Command, results []types.CreateTransferResult, offset, batchSize int) ([]Outcome, error) {
	out := newOutcomes(cmd)
	if len(results) != batchSize {
		return nil, fmt.Errorf("%w: got %d results, expected batch size %d",
			ErrResultCountMismatch, len(results), batchSize)
	}
	if offset < 0 || offset+len(out) > batchSize {
		return nil, fmt.Errorf("%w: command window [%d,%d) does not fit batch size %d",
			ErrResultCountMismatch, offset, offset+len(out), batchSize)
	}
	for i := range out {
		r := results[offset+i]
		out[i].Timestamp = r.Timestamp
		if r.Status == types.TransferCreated || r.Status == types.TransferExists {
			continue
		}
		out[i].Status = StatusRejected
		out[i].Error = errorName(r.Status.String(), "Transfer")
	}
	return out, nil
}

// MapAccountResults cuts the command's window out of the batch's dense response.
// offset is the position of the command's first event within the sent batch,
// batchSize is the size of the whole batch the command was sent in.
func MapAccountResults(cmd *model.Command, results []types.CreateAccountResult, offset, batchSize int) ([]Outcome, error) {
	out := newOutcomes(cmd)
	if len(results) != batchSize {
		return nil, fmt.Errorf("%w: got %d results, expected batch size %d",
			ErrResultCountMismatch, len(results), batchSize)
	}
	if offset < 0 || offset+len(out) > batchSize {
		return nil, fmt.Errorf("%w: command window [%d,%d) does not fit batch size %d",
			ErrResultCountMismatch, offset, offset+len(out), batchSize)
	}
	for i := range out {
		r := results[offset+i]
		out[i].Timestamp = r.Timestamp
		if r.Status == types.AccountCreated || r.Status == types.AccountExists {
			continue
		}
		out[i].Status = StatusRejected
		out[i].Error = errorName(r.Status.String(), "Account")
	}
	return out, nil
}

func newOutcomes(cmd *model.Command) []Outcome {
	out := make([]Outcome, cmd.Len())
	for i := range out {
		out[i] = Outcome{Index: i, ID: cmd.IDs[i], Status: StatusOK}
	}
	return out
}

// errorName converts "TransferExceedsCredits" to "exceeds_credits":
// it strips the type prefix and converts CamelCase to snake_case. If the expected
// prefix is missing (e.g. String() fell into the default branch and returned something
// like "CreateTransferStatus(1)"), this is not a real TigerBeetle status name — the raw
// string is returned as-is, tagged with "unknown_status_".
func errorName(s, prefix string) string {
	trimmed, ok := strings.CutPrefix(s, prefix)
	if !ok {
		return "unknown_status_" + s
	}
	runes := []rune(trimmed)
	var sb strings.Builder
	sb.Grow(len(trimmed) + 8)
	for i, r := range runes {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				prevUpper := runes[i-1] >= 'A' && runes[i-1] <= 'Z'
				nextLower := i+1 < len(runes) && runes[i+1] >= 'a' && runes[i+1] <= 'z'
				if !prevUpper || nextLower {
					sb.WriteByte('_')
				}
			}
			sb.WriteRune(r + ('a' - 'A'))
			continue
		}
		sb.WriteRune(r)
	}
	return sb.String()
}
