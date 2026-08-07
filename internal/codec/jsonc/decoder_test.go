package jsonc

import (
	"strings"
	"testing"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/stretchr/testify/require"
)

// newFuzzDecoder builds a Decoder with a fixed test config. It has no
// *testing.T because FuzzDecode and the benchmarks call it too; newDecoder
// delegates to it so there is a single definition of the test fixture.
func newFuzzDecoder() *Decoder {
	cfg := &config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"payment": 1},
		Limits:  config.Limits{MaxMessageBytes: 1 << 20, MaxEventsPerMessage: 8189, MaxJSONDepth: 32},
	}
	return New(model.NewRegistry(cfg), cfg.Limits)
}

func newDecoder(t *testing.T) *Decoder {
	t.Helper()
	return newFuzzDecoder()
}

const okTransfers = `{
  "operation": "create_transfers",
  "transfers": [
    {"id":"0193f8a1-7c2e-7000-8000-000000000001",
     "debit_account_id":"0193f8a1-0000-7000-8000-000000000010",
     "credit_account_id":"0193f8a1-0000-7000-8000-000000000020",
     "amount":"12.34","ledger":"USD","code":"payment","flags":["linked"]},
    {"id":"0193f8a1-7c2e-7000-8000-000000000002",
     "debit_account_id":"0193f8a1-0000-7000-8000-000000000020",
     "credit_account_id":"0193f8a1-0000-7000-8000-000000000030",
     "amount":"12.34","ledger":"USD","code":"payment","flags":[]}
  ]}`

func TestDecodeTransfers(t *testing.T) {
	cmd, err := newDecoder(t).Decode([]byte(okTransfers))
	require.NoError(t, err)
	require.Equal(t, model.OpCreateTransfers, cmd.Op)
	require.Len(t, cmd.Transfers, 2)
	require.Equal(t, uint32(1), cmd.Transfers[0].Ledger)
	require.Equal(t, uint16(1), cmd.Transfers[0].Code)
	require.Equal(t, "1234", cmd.Transfers[0].Amount.BigInt().String())
	require.Equal(t, []string{
		"0193f8a1-7c2e-7000-8000-000000000001",
		"0193f8a1-7c2e-7000-8000-000000000002",
	}, cmd.IDs)
}

// Флаг linked на последнем элементе сообщения снимается: TigerBeetle
// запрещает открытую цепочку на границе батча.
func TestDecodeClearsTrailingLinked(t *testing.T) {
	body := strings.Replace(okTransfers, `"flags":[]`, `"flags":["linked"]`, 1)
	cmd, err := newDecoder(t).Decode([]byte(body))
	require.NoError(t, err)
	last := cmd.Transfers[len(cmd.Transfers)-1]
	require.Zero(t, last.Flags&uint16(1), "trailing linked flag must be cleared")
}

// Пост/войд-перевод может нести debit/credit account id как ассерт
// TigerBeetle-у, что они совпадают со счетами pending-перевода: декодер
// обязан их прокинуть, а не обнулить.
func TestDecodePostPendingForwardsAccountIDs(t *testing.T) {
	body := `{"operation":"create_transfers","transfers":[
	  {"id":"0193f8a1-7c2e-7000-8000-000000000001",
	   "pending_id":"0193f8a1-7c2e-7000-8000-000000000099",
	   "debit_account_id":"0193f8a1-0000-7000-8000-000000000010",
	   "credit_account_id":"0193f8a1-0000-7000-8000-000000000020",
	   "amount":"12.34","ledger":"USD","code":"payment","flags":["post_pending_transfer"]}
	]}`
	cmd, err := newDecoder(t).Decode([]byte(body))
	require.NoError(t, err)
	require.Len(t, cmd.Transfers, 1)
	require.NotZero(t, cmd.Transfers[0].DebitAccountID)
	require.NotZero(t, cmd.Transfers[0].CreditAccountID)
}

// Когда продюсер не указывает debit/credit account id на пост/войде, они
// должны остаться нулевыми, а не быть обязательными.
func TestDecodePostPendingAllowsOmittedAccountIDs(t *testing.T) {
	body := `{"operation":"create_transfers","transfers":[
	  {"id":"0193f8a1-7c2e-7000-8000-000000000001",
	   "pending_id":"0193f8a1-7c2e-7000-8000-000000000099",
	   "amount":"12.34","ledger":"USD","code":"payment","flags":["void_pending_transfer"]}
	]}`
	cmd, err := newDecoder(t).Decode([]byte(body))
	require.NoError(t, err)
	require.Len(t, cmd.Transfers, 1)
	require.Zero(t, cmd.Transfers[0].DebitAccountID)
	require.Zero(t, cmd.Transfers[0].CreditAccountID)
}

func TestDecodeAccounts(t *testing.T) {
	body := `{"operation":"create_accounts","accounts":[
	  {"id":"0193f8a1-0000-7000-8000-000000000010","ledger":"USD","code":"payment","flags":["history"]}]}`
	cmd, err := newDecoder(t).Decode([]byte(body))
	require.NoError(t, err)
	require.Equal(t, model.OpCreateAccounts, cmd.Op)
	require.Len(t, cmd.Accounts, 1)
}

func TestDecodePoison(t *testing.T) {
	cases := map[string]string{
		"not json":          `{`,
		"unknown field":     strings.Replace(okTransfers, `"amount"`, `"amont"`, 1),
		"bad amount":        strings.Replace(okTransfers, `"12.34"`, `"12.345"`, 1),
		"bad uuid":          strings.Replace(okTransfers, `"0193f8a1-7c2e-7000-8000-000000000001"`, `"nope"`, 1),
		"unknown ledger":    strings.Replace(okTransfers, `"USD"`, `"XXX"`, 1),
		"unknown code":      strings.Replace(okTransfers, `"payment"`, `"nope"`, 1),
		"unknown flag":      strings.Replace(okTransfers, `"linked"`, `"teleport"`, 1),
		"empty array":       `{"operation":"create_transfers","transfers":[]}`,
		"unknown operation": `{"operation":"drop_database","transfers":[]}`,
		"mixed operations": `{"operation":"create_transfers","transfers":[
		  {"id":"0193f8a1-7c2e-7000-8000-000000000001",
		   "debit_account_id":"0193f8a1-0000-7000-8000-000000000010",
		   "credit_account_id":"0193f8a1-0000-7000-8000-000000000020",
		   "amount":"12.34","ledger":"USD","code":"payment","flags":[]}
		],"accounts":[
		  {"id":"0193f8a1-0000-7000-8000-000000000010","ledger":"USD","code":"payment","flags":[]}
		]}`,
		"zero id": strings.Replace(okTransfers, `"0193f8a1-7c2e-7000-8000-000000000001"`, `"00000000-0000-0000-0000-000000000000"`, 1),
	}
	d := newDecoder(t)
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := d.Decode([]byte(body))
			require.Error(t, err)
			require.True(t, codec.IsPoison(err), "want poison, got %v", err)
		})
	}
}

func TestDecodeRejectsOversizedPayload(t *testing.T) {
	cfg := config.Limits{MaxMessageBytes: 16, MaxEventsPerMessage: 8189, MaxJSONDepth: 32}
	d := New(model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"payment": 1},
	}), cfg)
	_, err := d.Decode([]byte(okTransfers))
	require.True(t, codec.IsPoison(err))
	require.ErrorContains(t, err, "message too large")
}

func TestDecodeRejectsTooManyEvents(t *testing.T) {
	cfg := config.Limits{MaxMessageBytes: 1 << 20, MaxEventsPerMessage: 1, MaxJSONDepth: 32}
	d := New(model.NewRegistry(&config.Config{
		Ledgers: map[string]config.Ledger{"USD": {ID: 1, Scale: 2}},
		Codes:   map[string]uint16{"payment": 1},
	}), cfg)
	_, err := d.Decode([]byte(okTransfers))
	require.True(t, codec.IsPoison(err))
	require.ErrorContains(t, err, "too many events")
}
