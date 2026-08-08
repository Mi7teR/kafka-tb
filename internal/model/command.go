package model

import types "github.com/tigerbeetle/tigerbeetle-go"

type Op string

const (
	OpCreateTransfers Op = "create_transfers"
	OpCreateAccounts  Op = "create_accounts"
)

// Command — результат декодинга одного сообщения или одного API-вызова.
// Заполнено ровно одно из полей Transfers/Accounts, согласно Op.
type Command struct {
	Op        Op
	Transfers []types.Transfer
	Accounts  []types.Account
	// IDs хранит исходные строковые id в том же порядке — нужны для отчёта
	// об исходах без обратной конверсии.
	IDs []string
	// Key — ключ порядка. Две команды с одним ключом применяются в TigerBeetle
	// строго в порядке постановки; между разными ключами порядок не определён
	// и не требуется. Синк ставит сюда "topic/partition" — ровно ту единицу,
	// внутри которой порядок обязателен. Пустой ключ значит «ни с чем не
	// упорядочена»: так ставит API, где два независимых запроса никогда не
	// имели между собой порядка.
	Key string
}

func (c *Command) Len() int {
	if c.Op == OpCreateAccounts {
		return len(c.Accounts)
	}
	return len(c.Transfers)
}
