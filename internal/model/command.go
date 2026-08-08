package model

import types "github.com/tigerbeetle/tigerbeetle-go"

type Op string

const (
	OpCreateTransfers Op = "create_transfers"
	OpCreateAccounts  Op = "create_accounts"
)

// Command is the result of decoding one message.
// Exactly one of the Transfers/Accounts fields is populated, per Op.
type Command struct {
	Op        Op
	Transfers []types.Transfer
	Accounts  []types.Account
	// IDs holds the original string ids in the same order — needed to report
	// outcomes without a reverse conversion.
	IDs []string
}

func (c *Command) Len() int {
	if c.Op == OpCreateAccounts {
		return len(c.Accounts)
	}
	return len(c.Transfers)
}
