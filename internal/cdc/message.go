// Package cdc publishes TigerBeetle change events to Kafka: the counterpart
// of the sink, and this project's answer to TigerBeetle's official
// `tigerbeetle amqp` job.
//
// The message format is the official job's skeleton with this connector's
// value conventions — ledger and code as names from the registries, amounts
// as decimal strings at the ledger's scale, ids as UUID strings, flags as
// name lists — so that one vocabulary covers what the sink accepts and what
// this job produces. It is deliberately not wire-compatible with the official
// AMQP consumer.
package cdc

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/mailru/easyjson/jwriter"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
)

// Kafka headers, named after the official job's. They let a consumer route or
// filter without parsing the body.
const (
	HeaderEventType         = "event_type"
	HeaderLedger            = "ledger"
	HeaderTransferCode      = "transfer_code"
	HeaderDebitAccountCode  = "debit_account_code"
	HeaderCreditAccountCode = "credit_account_code"
	HeaderTimestamp         = "timestamp"
)

// eventTypeNames maps TigerBeetle's change-event types to the names this
// project uses. The zero value of ChangeEventType is a real type
// (single_phase), so an unknown value is named numerically rather than
// defaulted to anything.
var eventTypeNames = map[types.ChangeEventType]string{
	types.ChangeEventSinglePhase:     "single_phase",
	types.ChangeEventTwoPhasePending: "two_phase_pending",
	types.ChangeEventTwoPhasePosted:  "two_phase_posted",
	types.ChangeEventTwoPhaseVoided:  "two_phase_voided",
	types.ChangeEventTwoPhaseExpired: "two_phase_expired",
}

//go:generate easyjson -disallow_unknown_fields $GOFILE

// Message is one change event on the CDC topic.
//
// Checkpoint is this job's entire progress state — it keeps none anywhere
// else, exactly like the official job. It reads: every event with a timestamp
// up to and including Checkpoint is present in this topic. On startup the job
// takes the highest Checkpoint across every partition's tail and resumes from
// it, so the field must never claim more than has actually been acknowledged.
// See publisher.go for what upholds that claim.
//
//easyjson:json
type Message struct {
	Type          string   `json:"type"`
	Timestamp     string   `json:"timestamp"`
	Checkpoint    string   `json:"checkpoint"`
	Ledger        string   `json:"ledger"`
	Transfer      Transfer `json:"transfer"`
	DebitAccount  Account  `json:"debit_account"`
	CreditAccount Account  `json:"credit_account"`
}

//easyjson:json
type Transfer struct {
	ID          string   `json:"id"`
	Amount      string   `json:"amount"`
	PendingID   string   `json:"pending_id,omitempty"`
	UserData128 string   `json:"user_data_128,omitempty"`
	UserData64  uint64   `json:"user_data_64,omitempty"`
	UserData32  uint32   `json:"user_data_32,omitempty"`
	Timeout     string   `json:"timeout,omitempty"`
	Code        string   `json:"code"`
	Flags       []string `json:"flags"`
	Timestamp   string   `json:"timestamp"`
}

// Account is one side's snapshot as of the event: the balances TigerBeetle
// held once the event was applied, plus the account's own attributes.
//
//easyjson:json
type Account struct {
	ID             string   `json:"id"`
	DebitsPending  string   `json:"debits_pending"`
	DebitsPosted   string   `json:"debits_posted"`
	CreditsPending string   `json:"credits_pending"`
	CreditsPosted  string   `json:"credits_posted"`
	UserData128    string   `json:"user_data_128,omitempty"`
	UserData64     uint64   `json:"user_data_64,omitempty"`
	UserData32     uint32   `json:"user_data_32,omitempty"`
	Code           string   `json:"code"`
	Flags          []string `json:"flags"`
	Timestamp      string   `json:"timestamp"`
}

// gap identifies a registry (or vocabulary) gap already reported, so that one
// missing entry is warned about once instead of once per event.
type gap struct {
	kind  string
	value uint64
}

// Encoder turns change events into Kafka records.
//
// It is used from the job's single goroutine, which is why warned is a plain
// map with no lock around it.
type Encoder struct {
	topic        string
	partitionKey string
	reg          *model.Registry
	log          *slog.Logger
	// warned remembers which unknown ledger ids, codes and event types have
	// already been reported. A gap in the registry is a static condition: it
	// affects every event on that ledger or with that code, so warning per
	// occurrence floods the log at the full event rate and tells the operator
	// nothing the first line did not.
	warned map[gap]bool
	// w and scratch are the JSON serialization buffer, kept across events
	// rather than rebuilt per event. Same single-goroutine argument as
	// warned: nothing locks them because nothing shares them. See marshal
	// for why the record body is still allocated fresh every time.
	w       jwriter.Writer
	scratch []byte
}

// scratchSize is the reused serialization buffer's capacity. A fully
// populated message — every optional id present, flags on both accounts — is
// 1,062 bytes, so this holds one whole message with room for long ledger,
// code and flag names and never chains a second chunk. A message that does
// outgrow it still encodes correctly, just through easyjson's chunk chain;
// see marshal.
const scratchSize = 2048

func NewEncoder(cfg config.CDC, reg *model.Registry, log *slog.Logger) *Encoder {
	key := cfg.PartitionKey
	if key == "" {
		key = config.PartitionKeyDebitAccountID
	}
	return &Encoder{
		topic:        cfg.Topic,
		partitionKey: key,
		reg:          reg,
		log:          log,
		warned:       map[gap]bool{},
		scratch:      make([]byte, 0, scratchSize),
	}
}

// first reports whether this kind/value pair is being seen for the first
// time, and records it either way.
func (e *Encoder) first(kind string, value uint64) bool {
	g := gap{kind: kind, value: value}
	if e.warned[g] {
		return false
	}
	e.warned[g] = true
	return true
}

// Record renders one event as a Kafka record carrying checkpoint as the
// topic's resume point.
func (e *Encoder) Record(ev types.ChangeEvent, checkpoint uint64) (*kgo.Record, error) {
	ledger, scale := e.ledger(ev)
	msg := Message{
		Type:       e.eventType(ev),
		Timestamp:  strconv.FormatUint(ev.Timestamp, 10),
		Checkpoint: strconv.FormatUint(checkpoint, 10),
		Ledger:     ledger,
		Transfer: Transfer{
			ID:          model.FormatID(ev.TransferID),
			Amount:      model.FormatAmount(ev.TransferAmount, scale),
			PendingID:   optionalID(ev.TransferPendingID),
			UserData128: optionalID(ev.TransferUserData128),
			UserData64:  ev.TransferUserData64,
			UserData32:  ev.TransferUserData32,
			Timeout:     timeout(ev.TransferTimeout),
			Code:        e.code(ev.TransferCode, "transfer", ev.Timestamp),
			Flags:       flagList(e.reg.TransferFlagNames(ev.TransferFlags)),
			Timestamp:   strconv.FormatUint(ev.TransferTimestamp, 10),
		},
		DebitAccount: Account{
			ID:             model.FormatID(ev.DebitAccountID),
			DebitsPending:  model.FormatAmount(ev.DebitAccountDebitsPending, scale),
			DebitsPosted:   model.FormatAmount(ev.DebitAccountDebitsPosted, scale),
			CreditsPending: model.FormatAmount(ev.DebitAccountCreditsPending, scale),
			CreditsPosted:  model.FormatAmount(ev.DebitAccountCreditsPosted, scale),
			UserData128:    optionalID(ev.DebitAccountUserData128),
			UserData64:     ev.DebitAccountUserData64,
			UserData32:     ev.DebitAccountUserData32,
			Code:           e.code(ev.DebitAccountCode, "debit_account", ev.Timestamp),
			Flags:          flagList(e.reg.AccountFlagNames(ev.DebitAccountFlags)),
			Timestamp:      strconv.FormatUint(ev.DebitAccountTimestamp, 10),
		},
		CreditAccount: Account{
			ID:             model.FormatID(ev.CreditAccountID),
			DebitsPending:  model.FormatAmount(ev.CreditAccountDebitsPending, scale),
			DebitsPosted:   model.FormatAmount(ev.CreditAccountDebitsPosted, scale),
			CreditsPending: model.FormatAmount(ev.CreditAccountCreditsPending, scale),
			CreditsPosted:  model.FormatAmount(ev.CreditAccountCreditsPosted, scale),
			UserData128:    optionalID(ev.CreditAccountUserData128),
			UserData64:     ev.CreditAccountUserData64,
			UserData32:     ev.CreditAccountUserData32,
			Code:           e.code(ev.CreditAccountCode, "credit_account", ev.Timestamp),
			Flags:          flagList(e.reg.AccountFlagNames(ev.CreditAccountFlags)),
			Timestamp:      strconv.FormatUint(ev.CreditAccountTimestamp, 10),
		},
	}
	body, err := e.marshal(&msg)
	if err != nil {
		return nil, fmt.Errorf("marshal change event %d: %w", ev.Timestamp, err)
	}
	return &kgo.Record{
		Topic: e.topic,
		Key:   []byte(e.key(msg)),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: HeaderEventType, Value: []byte(msg.Type)},
			{Key: HeaderLedger, Value: []byte(msg.Ledger)},
			{Key: HeaderTransferCode, Value: []byte(msg.Transfer.Code)},
			{Key: HeaderDebitAccountCode, Value: []byte(msg.DebitAccount.Code)},
			{Key: HeaderCreditAccountCode, Value: []byte(msg.CreditAccount.Code)},
			{Key: HeaderTimestamp, Value: []byte(msg.Timestamp)},
		},
	}, nil
}

// marshal renders msg as JSON, byte for byte what easyjson.Marshal produces,
// reusing the encoder's writer and scratch chunk instead of building a fresh
// pair per event.
//
// The copy at the end is the point of the whole function and must not be
// optimised away. The slice returned here becomes kgo.Record.Value, and
// franz-go retains that memory until the broker acknowledges the record — so
// it cannot be a buffer the next event overwrites. Handing out the scratch
// would corrupt records still in flight silently: no crash, no error, just
// garbage arriving at a consumer. What is reused is everything up to the
// body; what is fresh, every time, is the body.
func (e *Encoder) marshal(msg *Message) ([]byte, error) {
	e.w.Buffer.Buf = e.scratch[:0]
	msg.MarshalEasyJSON(&e.w)
	if e.w.Error != nil {
		// The writer may be holding chained chunks that only BuildBytes or
		// DumpTo would release, and neither runs on this path. Drop the
		// whole thing rather than carry half a message into the next event.
		err := e.w.Error
		e.w = jwriter.Writer{}
		e.scratch = make([]byte, 0, scratchSize)
		return nil, err
	}
	n := e.w.Size()
	if n == len(e.w.Buffer.Buf) {
		// The message fit in the scratch chunk, which is the common case and
		// the reason the scratch exists: one allocation for the body, none
		// for the serialization.
		body := make([]byte, n)
		copy(body, e.w.Buffer.Buf)
		e.scratch = e.w.Buffer.Buf[:0]
		e.w.Buffer.Buf = nil
		return body, nil
	}
	// The message outgrew the scratch, so easyjson chained its own pooled
	// chunks behind it. BuildBytes concatenates them into a fresh slice and
	// hands every chunk it walked back to easyjson's pool — the scratch
	// included, which is why the scratch cannot be kept here. Replace it with
	// one that fits, so this happens once per size increase and not per
	// event.
	body, err := e.w.BuildBytes()
	if err != nil {
		e.w = jwriter.Writer{}
		e.scratch = make([]byte, 0, scratchSize)
		return nil, err
	}
	e.scratch = make([]byte, 0, 2*n)
	return body, nil
}

// key picks the record key per cdc.partition_key. The key decides which
// ordering a consumer gets, which is why it is configuration and not a
// decision taken here.
func (e *Encoder) key(msg Message) string {
	switch e.partitionKey {
	case config.PartitionKeyCreditAccountID:
		return msg.CreditAccount.ID
	case config.PartitionKeyLedger:
		return msg.Ledger
	case config.PartitionKeyTransferID:
		return msg.Transfer.ID
	default:
		return msg.DebitAccount.ID
	}
}

// ledger names the event's ledger and returns the scale its amounts are
// written at. An id missing from the registry is reported numerically and
// its amounts stay in minor units: losing a financial event because a config
// entry is missing would be far worse than an ugly message, and inventing a
// scale would misstate the amount.
//
// The warning is emitted once per unknown ledger id, not once per event: see
// Encoder.warned.
func (e *Encoder) ledger(ev types.ChangeEvent) (string, int32) {
	name, err := e.reg.LedgerName(ev.Ledger)
	if err != nil {
		if e.first("ledger", uint64(ev.Ledger)) {
			e.log.Warn("cdc: unknown ledger, publishing the numeric value and unscaled amounts "+
				"(logged once per ledger id)",
				slog.Uint64("ledger", uint64(ev.Ledger)), slog.Uint64("timestamp", ev.Timestamp))
		}
		return strconv.FormatUint(uint64(ev.Ledger), 10), 0
	}
	scale, err := e.reg.ScaleByLedgerID(ev.Ledger)
	if err != nil {
		// Unreachable: LedgerName succeeded, so the ledger is in the registry.
		return name, 0
	}
	return name, scale
}

// code names a code, or reports it numerically with a warning. field says
// which of the three codes it is, so the operator knows what to add to the
// registry.
//
// The warning is emitted once per unknown code value, not once per event or
// per field: see Encoder.warned.
func (e *Encoder) code(v uint16, field string, ts uint64) string {
	name, err := e.reg.CodeName(v)
	if err != nil {
		if e.first("code", uint64(v)) {
			e.log.Warn("cdc: unknown code, publishing the numeric value (logged once per code)",
				slog.String("field", field), slog.Uint64("code", uint64(v)),
				slog.Uint64("timestamp", ts))
		}
		return strconv.FormatUint(uint64(v), 10)
	}
	return name
}

// eventType names the event's type, numerically if this build does not know
// it: a type added by a newer TigerBeetle must not be silently renamed to an
// existing one.
func (e *Encoder) eventType(ev types.ChangeEvent) string {
	if name, ok := eventTypeNames[ev.Type]; ok {
		return name
	}
	if e.first("event_type", uint64(ev.Type)) {
		e.log.Warn("cdc: unknown change event type, publishing the numeric value "+
			"(logged once per type)",
			slog.Uint64("type", uint64(ev.Type)), slog.Uint64("timestamp", ev.Timestamp))
	}
	return strconv.FormatUint(uint64(ev.Type), 10)
}

// flagList makes a flag list safe to marshal. model's flagNames returns nil
// for no flags, and nil marshals as null — but a flagless transfer or account
// is the common case, and the format documents flags as an array. A consumer
// validating against a schema would break on the majority of messages, so an
// empty list is written as [].
func flagList(names []string) []string {
	if names == nil {
		return []string{}
	}
	return names
}

// optionalID renders an id, or "" for the zero id so the field is omitted:
// the all-zero UUID is not an id, and the sink would reject it as one.
func optionalID(u types.Uint128) string {
	if u == (types.Uint128{}) {
		return ""
	}
	return model.FormatID(u)
}

// timeout renders a pending transfer's timeout the way the sink accepts it —
// a Go duration string — rather than as a bare number of seconds.
func timeout(seconds uint32) string {
	if seconds == 0 {
		return ""
	}
	return (time.Duration(seconds) * time.Second).String()
}
