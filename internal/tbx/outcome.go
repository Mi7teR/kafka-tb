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
// offset — позиция первого события команды внутри отправленного батча,
// batchSize — размер всего батча, которым команда была отправлена.
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

// MapAccountResults вырезает окно команды из плотного ответа батча.
// offset — позиция первого события команды внутри отправленного батча,
// batchSize — размер всего батча, которым команда была отправлена.
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

// errorName переводит "TransferExceedsCredits" в "exceeds_credits":
// снимает префикс типа и переводит CamelCase в snake_case. Если ожидаемого
// префикса нет (например, String() ушёл в default-ветку и вернул что-то вроде
// "CreateTransferStatus(1)"), это не настоящее имя статуса TigerBeetle — сырая
// строка возвращается как есть за пометкой "unknown_status_".
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
