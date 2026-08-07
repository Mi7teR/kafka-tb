package tbx

import (
	"fmt"

	types "github.com/tigerbeetle/tigerbeetle-go"

	"github.com/Mi7teR/kafka-tb/internal/config"
)

// Client — узкий интерфейс поверх клиента TigerBeetle.
// Существует ради подмены в тестах: настоящий клиент требует живого кластера.
type Client interface {
	CreateAccounts([]types.Account) ([]types.CreateAccountResult, error)
	CreateTransfers([]types.Transfer) ([]types.CreateTransferResult, error)
	LookupAccounts([]types.Uint128) ([]types.Account, error)
	LookupTransfers([]types.Uint128) ([]types.Transfer, error)
	GetAccountTransfers(types.AccountFilter) ([]types.Transfer, error)
	GetAccountBalances(types.AccountFilter) ([]types.AccountBalance, error)
	QueryAccounts(types.QueryFilter) ([]types.Account, error)
	QueryTransfers(types.QueryFilter) ([]types.Transfer, error)
	Nop() error
	Close()
}

func NewClient(cfg config.TigerBeetle) (Client, error) {
	c, err := types.NewClient(types.ToUint128(cfg.ClusterID), cfg.Addresses)
	if err != nil {
		return nil, fmt.Errorf("tigerbeetle connect: %w", err)
	}
	return c, nil
}
