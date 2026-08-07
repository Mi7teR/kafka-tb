package api

import (
	"math/big"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/model"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

// balance считает «человеческий» остаток по направлению счёта, чтобы
// клиенту не надо было помнить, дебетовый счёт или кредитовый: для
// credits_must_not_exceed_debits это debits - credits, иначе credits - debits.
func balance(a types.Account, scale int32) string {
	debits, credits := a.DebitsPosted.BigInt(), a.CreditsPosted.BigInt()
	var diff big.Int
	if a.AccountFlags().CreditsMustNotExceedDebits {
		diff.Sub(debits, credits)
	} else {
		diff.Sub(credits, debits)
	}
	neg := diff.Sign() < 0
	if neg {
		diff.Abs(&diff)
	}
	s := model.FormatAmount(types.BigIntToUint128(&diff), scale)
	if neg {
		return "-" + s
	}
	return s
}

// formatUint128OrEmpty renders an optional Uint128 field (user_data_128,
// pending_id) as "" when it is zero, matching how the JSON decoder treats an
// absent value: internal/codec/jsonc leaves the field zero rather than
// parsing an all-zero UUID string.
func formatUint128OrEmpty(u types.Uint128) string {
	if u == (types.Uint128{}) {
		return ""
	}
	return model.FormatID(u)
}

func accountToProto(a types.Account, reg *model.Registry) (*kafkatbv1.Account, error) {
	ledgerName, err := reg.LedgerName(a.Ledger)
	if err != nil {
		return nil, err
	}
	scale, err := reg.ScaleByLedgerID(a.Ledger)
	if err != nil {
		return nil, err
	}
	codeName, err := reg.CodeName(a.Code)
	if err != nil {
		return nil, err
	}
	return &kafkatbv1.Account{
		Id:             model.FormatID(a.ID),
		Ledger:         ledgerName,
		Code:           codeName,
		Flags:          flagNamesFromAccount(a.AccountFlags()),
		DebitsPending:  model.FormatAmount(a.DebitsPending, scale),
		DebitsPosted:   model.FormatAmount(a.DebitsPosted, scale),
		CreditsPending: model.FormatAmount(a.CreditsPending, scale),
		CreditsPosted:  model.FormatAmount(a.CreditsPosted, scale),
		Balance:        balance(a, scale),
		UserData_128:   formatUint128OrEmpty(a.UserData128),
		UserData_64:    a.UserData64,
		UserData_32:    a.UserData32,
		Timestamp:      a.Timestamp,
	}, nil
}

func transferToProto(t types.Transfer, reg *model.Registry) (*kafkatbv1.Transfer, error) {
	ledgerName, err := reg.LedgerName(t.Ledger)
	if err != nil {
		return nil, err
	}
	scale, err := reg.ScaleByLedgerID(t.Ledger)
	if err != nil {
		return nil, err
	}
	codeName, err := reg.CodeName(t.Code)
	if err != nil {
		return nil, err
	}
	return &kafkatbv1.Transfer{
		Id:              model.FormatID(t.ID),
		DebitAccountId:  model.FormatID(t.DebitAccountID),
		CreditAccountId: model.FormatID(t.CreditAccountID),
		Amount:          model.FormatAmount(t.Amount, scale),
		Ledger:          ledgerName,
		Code:            codeName,
		Flags:           flagNamesFromTransfer(t.TransferFlags()),
		PendingId:       formatUint128OrEmpty(t.PendingID),
		UserData_128:    formatUint128OrEmpty(t.UserData128),
		UserData_64:     t.UserData64,
		UserData_32:     t.UserData32,
		Timeout:         t.Timeout,
		Timestamp:       t.Timestamp,
	}, nil
}

func balanceToProto(b types.AccountBalance, scale int32) *kafkatbv1.Balance {
	return &kafkatbv1.Balance{
		DebitsPending:  model.FormatAmount(b.DebitsPending, scale),
		DebitsPosted:   model.FormatAmount(b.DebitsPosted, scale),
		CreditsPending: model.FormatAmount(b.CreditsPending, scale),
		CreditsPosted:  model.FormatAmount(b.CreditsPosted, scale),
		Timestamp:      b.Timestamp,
	}
}

// flagNamesFromAccount и flagNamesFromTransfer публикуют "imported" наравне
// с остальными флагами — это часть реального состояния счёта/перевода в
// TigerBeetle, и умолчание сделало бы ответ API недостоверным. Симметрично
// на запись: model.Registry.AccountFlags/TransferFlags отклоняют "imported"
// как ввод с явной ошибкой, потому что импорт требует передаваемых
// вызывающей стороной timestamp события, а этот коннектор их не принимает.
func flagNamesFromAccount(f types.AccountFlags) []string {
	var names []string
	if f.Linked {
		names = append(names, "linked")
	}
	if f.DebitsMustNotExceedCredits {
		names = append(names, "debits_must_not_exceed_credits")
	}
	if f.CreditsMustNotExceedDebits {
		names = append(names, "credits_must_not_exceed_debits")
	}
	if f.History {
		names = append(names, "history")
	}
	if f.Imported {
		names = append(names, "imported")
	}
	if f.Closed {
		names = append(names, "closed")
	}
	return names
}

func flagNamesFromTransfer(f types.TransferFlags) []string {
	var names []string
	if f.Linked {
		names = append(names, "linked")
	}
	if f.Pending {
		names = append(names, "pending")
	}
	if f.PostPendingTransfer {
		names = append(names, "post_pending_transfer")
	}
	if f.VoidPendingTransfer {
		names = append(names, "void_pending_transfer")
	}
	if f.BalancingDebit {
		names = append(names, "balancing_debit")
	}
	if f.BalancingCredit {
		names = append(names, "balancing_credit")
	}
	if f.ClosingDebit {
		names = append(names, "closing_debit")
	}
	if f.ClosingCredit {
		names = append(names, "closing_credit")
	}
	if f.Imported {
		names = append(names, "imported")
	}
	return names
}
