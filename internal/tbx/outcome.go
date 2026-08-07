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

// ErrResultCountMismatch — TigerBeetle вернул ответ, не соответствующий батчу.
// Разбирать его позиционно нельзя: исходы уедут не тем командам.
var ErrResultCountMismatch = errors.New("tigerbeetle result count does not match batch size")

// Outcome — исход одного события внутри команды.
type Outcome struct {
	Index     int
	ID        string
	Status    Status
	Error     string // машиночитаемое имя статуса TigerBeetle, пусто при успехе
	Timestamp uint64
}

// MapTransferResults вырезает окно команды из плотного ответа батча.
// offset — позиция первого события команды внутри отправленного батча.
func MapTransferResults(cmd *model.Command, results []types.CreateTransferResult, offset int) ([]Outcome, error) {
	out := newOutcomes(cmd)
	if len(results) == 0 {
		return out, nil // пустой ответ = все события применены
	}
	if offset+len(out) > len(results) {
		return nil, fmt.Errorf("%w: got %d results, command needs [%d,%d)",
			ErrResultCountMismatch, len(results), offset, offset+len(out))
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

// MapAccountResults вырезает окно команды из плотного ответа батча.
// offset — позиция первого события команды внутри отправленного батча.
func MapAccountResults(cmd *model.Command, results []types.CreateAccountResult, offset int) ([]Outcome, error) {
	out := newOutcomes(cmd)
	if len(results) == 0 {
		return out, nil
	}
	if offset+len(out) > len(results) {
		return nil, fmt.Errorf("%w: got %d results, command needs [%d,%d)",
			ErrResultCountMismatch, len(results), offset, offset+len(out))
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

// errorName переводит "TransferExceedsCredits" в "exceeds_credits":
// снимает префикс типа и переводит CamelCase в snake_case.
func errorName(s, prefix string) string {
	s = strings.TrimPrefix(s, prefix)
	var sb strings.Builder
	sb.Grow(len(s) + 8)
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				sb.WriteByte('_')
			}
			sb.WriteRune(r + ('a' - 'A'))
			continue
		}
		sb.WriteRune(r)
	}
	return sb.String()
}
