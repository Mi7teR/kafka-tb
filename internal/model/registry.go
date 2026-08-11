package model

import (
	"fmt"

	"github.com/Mi7teR/kafka-tb/internal/config"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

type Registry struct {
	ledgers     map[string]config.Ledger
	ledgerNames map[uint32]string
	codes       map[string]uint16
	codeNames   map[uint16]string
}

func NewRegistry(cfg *config.Config) *Registry {
	r := &Registry{
		ledgers:     cfg.Ledgers,
		ledgerNames: make(map[uint32]string, len(cfg.Ledgers)),
		codes:       cfg.Codes,
		codeNames:   make(map[uint16]string, len(cfg.Codes)),
	}
	for name, l := range cfg.Ledgers {
		r.ledgerNames[l.ID] = name
	}
	for name, v := range cfg.Codes {
		r.codeNames[v] = name
	}
	return r
}

func (r *Registry) Ledger(name string) (config.Ledger, error) {
	l, ok := r.ledgers[name]
	if !ok {
		return config.Ledger{}, fmt.Errorf("unknown ledger %q", name)
	}
	return l, nil
}

func (r *Registry) LedgerName(id uint32) (string, error) {
	n, ok := r.ledgerNames[id]
	if !ok {
		return "", fmt.Errorf("unknown ledger id %d", id)
	}
	return n, nil
}

func (r *Registry) ScaleByLedgerID(id uint32) (int32, error) {
	name, err := r.LedgerName(id)
	if err != nil {
		return 0, err
	}
	return r.ledgers[name].Scale, nil
}

func (r *Registry) Code(name string) (uint16, error) {
	v, ok := r.codes[name]
	if !ok {
		return 0, fmt.Errorf("unknown code %q", name)
	}
	return v, nil
}

func (r *Registry) CodeName(v uint16) (string, error) {
	n, ok := r.codeNames[v]
	if !ok {
		return "", fmt.Errorf("unknown code %d", v)
	}
	return n, nil
}

func (r *Registry) TransferFlags(names []string) (types.TransferFlags, error) {
	var f types.TransferFlags
	for _, n := range names {
		switch n {
		case "linked":
			f.Linked = true
		case "pending":
			f.Pending = true
		case "post_pending_transfer":
			f.PostPendingTransfer = true
		case "void_pending_transfer":
			f.VoidPendingTransfer = true
		case "balancing_debit":
			f.BalancingDebit = true
		case "balancing_credit":
			f.BalancingCredit = true
		case "closing_debit":
			f.ClosingDebit = true
		case "closing_credit":
			f.ClosingCredit = true
		case "imported":
			return f, fmt.Errorf("transfer flag %q is read-only in this connector: importing requires caller-supplied event timestamps, which this connector does not support", n)
		default:
			return f, fmt.Errorf("unknown transfer flag %q", n)
		}
	}
	return f, nil
}

// transferFlagNames and accountFlagNames are the bit -> name tables the CDC
// job needs, in bit order. They mirror TransferFlags/AccountFlags above, and
// the round-trip tests hold the two directions together: a name added on one
// side and forgotten on the other fails there.
//
// imported appears here although it is rejected on the way in: this connector
// never sets that flag, but an event that carries it must still say so.
var (
	transferFlagNames = []string{
		"linked", "pending", "post_pending_transfer", "void_pending_transfer",
		"balancing_debit", "balancing_credit", "closing_debit", "closing_credit",
		"imported",
	}
	accountFlagNames = []string{
		"linked", "debits_must_not_exceed_credits", "credits_must_not_exceed_debits",
		"history", "imported", "closed",
	}
)

// TransferFlagNames is the reverse of TransferFlags: it names the bits set in
// a transfer's flags, in bit order.
func (r *Registry) TransferFlagNames(flags uint16) []string {
	return flagNames(flags, transferFlagNames)
}

// AccountFlagNames is the reverse of AccountFlags: it names the bits set in an
// account's flags, in bit order.
func (r *Registry) AccountFlagNames(flags uint16) []string {
	return flagNames(flags, accountFlagNames)
}

// flagNames returns nil rather than an empty slice for no flags, so that a
// marshaled message carries [] only where the encoder chooses to.
// A bit with no name is reported as bit_N: a flag this build does not know
// must not vanish from the message.
func flagNames(flags uint16, table []string) []string {
	var out []string
	for bit := 0; bit < 16; bit++ {
		if flags&(1<<bit) == 0 {
			continue
		}
		if bit < len(table) {
			out = append(out, table[bit])
			continue
		}
		out = append(out, fmt.Sprintf("bit_%d", bit))
	}
	return out
}

func (r *Registry) AccountFlags(names []string) (types.AccountFlags, error) {
	var f types.AccountFlags
	for _, n := range names {
		switch n {
		case "linked":
			f.Linked = true
		case "debits_must_not_exceed_credits":
			f.DebitsMustNotExceedCredits = true
		case "credits_must_not_exceed_debits":
			f.CreditsMustNotExceedDebits = true
		case "history":
			f.History = true
		case "closed":
			f.Closed = true
		case "imported":
			return f, fmt.Errorf("account flag %q is read-only in this connector: importing requires caller-supplied event timestamps, which this connector does not support", n)
		default:
			return f, fmt.Errorf("unknown account flag %q", n)
		}
	}
	return f, nil
}
