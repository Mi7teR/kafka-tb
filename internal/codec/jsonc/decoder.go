package jsonc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

type Decoder struct {
	reg *model.Registry
	lim config.Limits
}

func New(reg *model.Registry, lim config.Limits) *Decoder {
	return &Decoder{reg: reg, lim: lim}
}

type message struct {
	Operation string         `json:"operation"`
	Transfers []jsonTransfer `json:"transfers"`
	Accounts  []jsonAccount  `json:"accounts"`
}

type jsonTransfer struct {
	ID              string   `json:"id"`
	DebitAccountID  string   `json:"debit_account_id"`
	CreditAccountID string   `json:"credit_account_id"`
	PendingID       string   `json:"pending_id"`
	Amount          string   `json:"amount"`
	Ledger          string   `json:"ledger"`
	Code            string   `json:"code"`
	Flags           []string `json:"flags"`
	UserData128     string   `json:"user_data_128"`
	UserData64      uint64   `json:"user_data_64"`
	UserData32      uint32   `json:"user_data_32"`
	Timeout         string   `json:"timeout"`
}

type jsonAccount struct {
	ID          string   `json:"id"`
	Ledger      string   `json:"ledger"`
	Code        string   `json:"code"`
	Flags       []string `json:"flags"`
	UserData128 string   `json:"user_data_128"`
	UserData64  uint64   `json:"user_data_64"`
	UserData32  uint32   `json:"user_data_32"`
}

func (d *Decoder) Decode(payload []byte) (cmd *model.Command, err error) {
	// A panic in the parser must not crash the process: turn it into poison.
	defer func() {
		if r := recover(); r != nil {
			cmd, err = nil, codec.Poison("panic while decoding: %v", r)
		}
	}()

	if len(payload) > d.lim.MaxMessageBytes {
		return nil, codec.Poison("message too large: %d bytes, limit %d", len(payload), d.lim.MaxMessageBytes)
	}
	if err := checkDepth(payload, d.lim.MaxJSONDepth); err != nil {
		return nil, err
	}

	var m message
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, codec.Poison("json: %v", err)
	}
	if dec.More() {
		return nil, codec.Poison("json: trailing data after first value")
	}

	switch model.Op(m.Operation) {
	case model.OpCreateTransfers:
		if len(m.Accounts) > 0 {
			return nil, codec.Poison("operation create_transfers must not carry accounts")
		}
		return d.decodeTransfers(m.Transfers)
	case model.OpCreateAccounts:
		if len(m.Transfers) > 0 {
			return nil, codec.Poison("operation create_accounts must not carry transfers")
		}
		return d.decodeAccounts(m.Accounts)
	default:
		return nil, codec.Poison("unknown operation %q", m.Operation)
	}
}

func (d *Decoder) decodeTransfers(in []jsonTransfer) (*model.Command, error) {
	if len(in) == 0 {
		return nil, codec.Poison("transfers: empty")
	}
	if len(in) > d.lim.MaxEventsPerMessage {
		return nil, codec.Poison("too many events: %d, limit %d", len(in), d.lim.MaxEventsPerMessage)
	}
	cmd := &model.Command{
		Op:        model.OpCreateTransfers,
		Transfers: make([]types.Transfer, len(in)),
		IDs:       make([]string, len(in)),
	}
	for i, jt := range in {
		t, err := d.transfer(jt)
		if err != nil {
			return nil, codec.Poison("transfers[%d]: %v", i, err)
		}
		cmd.Transfers[i] = t
		cmd.IDs[i] = jt.ID
	}
	// A chain cannot be left open at a batch boundary.
	clearLinked(&cmd.Transfers[len(cmd.Transfers)-1].Flags)
	return cmd, nil
}

func (d *Decoder) transfer(jt jsonTransfer) (types.Transfer, error) {
	var t types.Transfer
	var err error
	if t.ID, err = model.ParseID(jt.ID); err != nil {
		return t, err
	}
	flags, err := d.reg.TransferFlags(jt.Flags)
	if err != nil {
		return t, err
	}
	t.Flags = flags.ToUint16()

	postOrVoid := flags.PostPendingTransfer || flags.VoidPendingTransfer
	if postOrVoid {
		if jt.PendingID == "" {
			return t, fmt.Errorf("pending_id required for post/void")
		}
		if t.PendingID, err = model.ParseID(jt.PendingID); err != nil {
			return t, err
		}
		// debit/credit account IDs are optional here: if the producer
		// supplies them, TigerBeetle asserts they match the pending
		// transfer's accounts. Forward them when present, leave zero
		// when omitted.
		if jt.DebitAccountID != "" {
			if t.DebitAccountID, err = model.ParseID(jt.DebitAccountID); err != nil {
				return t, err
			}
		}
		if jt.CreditAccountID != "" {
			if t.CreditAccountID, err = model.ParseID(jt.CreditAccountID); err != nil {
				return t, err
			}
		}
	} else {
		if jt.PendingID != "" {
			return t, fmt.Errorf("pending_id only valid with post/void flags")
		}
		if t.DebitAccountID, err = model.ParseID(jt.DebitAccountID); err != nil {
			return t, err
		}
		if t.CreditAccountID, err = model.ParseID(jt.CreditAccountID); err != nil {
			return t, err
		}
	}

	ledger, err := d.reg.Ledger(jt.Ledger)
	if err != nil {
		return t, err
	}
	t.Ledger = ledger.ID
	if t.Code, err = d.reg.Code(jt.Code); err != nil {
		return t, err
	}
	if t.Amount, err = model.ParseAmount(jt.Amount, ledger.Scale); err != nil {
		return t, err
	}
	if jt.UserData128 != "" {
		if t.UserData128, err = model.ParseID(jt.UserData128); err != nil {
			return t, fmt.Errorf("user_data_128: %w", err)
		}
	}
	t.UserData64 = jt.UserData64
	t.UserData32 = jt.UserData32
	if jt.Timeout != "" {
		secs, err := parseTimeoutSeconds(jt.Timeout)
		if err != nil {
			return t, err
		}
		t.Timeout = secs
	}
	return t, nil
}

func (d *Decoder) decodeAccounts(in []jsonAccount) (*model.Command, error) {
	if len(in) == 0 {
		return nil, codec.Poison("accounts: empty")
	}
	if len(in) > d.lim.MaxEventsPerMessage {
		return nil, codec.Poison("too many events: %d, limit %d", len(in), d.lim.MaxEventsPerMessage)
	}
	cmd := &model.Command{
		Op:       model.OpCreateAccounts,
		Accounts: make([]types.Account, len(in)),
		IDs:      make([]string, len(in)),
	}
	for i, ja := range in {
		var a types.Account
		var err error
		if a.ID, err = model.ParseID(ja.ID); err != nil {
			return nil, codec.Poison("accounts[%d]: %v", i, err)
		}
		ledger, err := d.reg.Ledger(ja.Ledger)
		if err != nil {
			return nil, codec.Poison("accounts[%d]: %v", i, err)
		}
		a.Ledger = ledger.ID
		if a.Code, err = d.reg.Code(ja.Code); err != nil {
			return nil, codec.Poison("accounts[%d]: %v", i, err)
		}
		flags, err := d.reg.AccountFlags(ja.Flags)
		if err != nil {
			return nil, codec.Poison("accounts[%d]: %v", i, err)
		}
		a.Flags = flags.ToUint16()
		if ja.UserData128 != "" {
			if a.UserData128, err = model.ParseID(ja.UserData128); err != nil {
				return nil, codec.Poison("accounts[%d].user_data_128: %v", i, err)
			}
		}
		a.UserData64 = ja.UserData64
		a.UserData32 = ja.UserData32
		cmd.Accounts[i] = a
		cmd.IDs[i] = ja.ID
	}
	clearLinked(&cmd.Accounts[len(cmd.Accounts)-1].Flags)
	return cmd, nil
}

// linked is the low bit in both TransferFlags and AccountFlags.
const linkedBit uint16 = 1

func clearLinked(flags *uint16) { *flags &^= linkedBit }

func parseTimeoutSeconds(s string) (uint32, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("timeout %q: %w", s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("timeout %q: must be positive", s)
	}
	secs := int64(d / time.Second)
	if secs == 0 || secs > int64(^uint32(0)) {
		return 0, fmt.Errorf("timeout %q: out of range for uint32 seconds", s)
	}
	return uint32(secs), nil
}

// checkDepth counts nesting before full parsing, so that
// deeply nested garbage cannot exhaust the stack.
func checkDepth(payload []byte, max int) error {
	depth, maxSeen, inStr, esc := 0, 0, false, false
	for _, b := range payload {
		switch {
		case esc:
			esc = false
		case inStr && b == '\\':
			esc = true
		case b == '"':
			inStr = !inStr
		case inStr:
		case b == '{' || b == '[':
			depth++
			if depth > maxSeen {
				maxSeen = depth
			}
			if maxSeen > max {
				return codec.Poison("json nesting too deep: limit %d", max)
			}
		case b == '}' || b == ']':
			depth--
		}
	}
	return nil
}
