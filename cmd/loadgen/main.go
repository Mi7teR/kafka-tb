// Command loadgen produces synthetic TigerBeetle transfers into a Kafka
// topic in the wire format internal/codec/jsonc decodes, for load-testing
// the connector.
//
// It first creates its own pool of accounts (a create_accounts message),
// then streams create_transfers messages that reference that pool. Every
// transfer's user_data_64 carries its publish time in nanoseconds, so
// end-to-end latency can be measured by comparing it against the time the
// connector later reports the transfer as applied.
//
// loadgen assumes the target topic has a single partition: the account pool
// must be consumed before any transfer that references it, and Kafka only
// orders records within a partition.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ledgerName and codeName match configs/example.yaml and the integration
// test harness. loadgen has no -config flag, so it targets whatever
// connector config defines these names.
const (
	ledgerName = "USD"
	codeName   = "payment"
)

func main() {
	var (
		brokers    = flag.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
		topic      = flag.String("topic", "", "target input topic (required)")
		count      = flag.Int("count", 1000, "total number of transfers to generate")
		accounts   = flag.Int("accounts", 100, "size of the account pool")
		hotAccount = flag.Bool("hot-account", false, "route every transfer's debit leg through a single account")
		chain      = flag.Int("chain", 1, "pack N transfers into one message, linked")
		rate       = flag.Float64("rate", 0, "target transfers/sec, 0 = unlimited")
		garbagePct = flag.Float64("garbage-pct", 0, "percent of messages that are intentionally malformed")
	)
	flag.Parse()

	if err := validateFlags(*topic, *count, *accounts, *chain, *hotAccount, *garbagePct); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	if err != nil {
		log.Fatalf("loadgen: kafka client: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	pool := make([]string, *accounts)
	for i := range pool {
		pool[i] = uuid.NewString()
	}
	if err := seedAccounts(ctx, cl, *topic, pool); err != nil {
		log.Fatalf("loadgen: seed accounts: %v", err)
	}

	stats, err := run(ctx, cl, runConfig{
		topic:      *topic,
		count:      *count,
		pool:       pool,
		hotAccount: *hotAccount,
		chain:      *chain,
		rate:       *rate,
		garbagePct: *garbagePct,
	})
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}

	fmt.Printf("sent %d messages (%d transfers, %d garbage) in %s -> %.1f transfers/sec\n",
		stats.messages, stats.transfers, stats.garbage, stats.elapsed, stats.transfersPerSec())
}

func validateFlags(topic string, count, accounts, chain int, hotAccount bool, garbagePct float64) error {
	if topic == "" {
		return fmt.Errorf("-topic is required")
	}
	if count <= 0 {
		return fmt.Errorf("-count must be > 0")
	}
	if accounts < 2 {
		return fmt.Errorf("-accounts must be >= 2: every transfer needs a distinct debit and credit account")
	}
	if chain <= 0 {
		return fmt.Errorf("-chain must be > 0")
	}
	if garbagePct < 0 || garbagePct > 100 {
		return fmt.Errorf("-garbage-pct must be within 0..100")
	}
	return nil
}

// --- wire format (mirrors internal/codec/jsonc's accepted shape) ---------

type wireMessage struct {
	Operation string         `json:"operation"`
	Transfers []wireTransfer `json:"transfers,omitempty"`
	Accounts  []wireAccount  `json:"accounts,omitempty"`
}

type wireTransfer struct {
	ID              string   `json:"id"`
	DebitAccountID  string   `json:"debit_account_id"`
	CreditAccountID string   `json:"credit_account_id"`
	Amount          string   `json:"amount"`
	Ledger          string   `json:"ledger"`
	Code            string   `json:"code"`
	Flags           []string `json:"flags,omitempty"`
	UserData64      uint64   `json:"user_data_64,omitempty"`
}

type wireAccount struct {
	ID     string `json:"id"`
	Ledger string `json:"ledger"`
	Code   string `json:"code"`
}

// --- account seeding -------------------------------------------------------

// accountBatchSize keeps each create_accounts message well under a sane
// message-size limit while still being generous.
const accountBatchSize = 1000

// seedAccounts publishes the account pool as create_accounts messages and
// waits for every one of them to be durably written before returning: any
// transfer referencing this pool must never be observed by the connector
// ahead of the account that creates it.
func seedAccounts(ctx context.Context, cl *kgo.Client, topic string, pool []string) error {
	for start := 0; start < len(pool); start += accountBatchSize {
		end := min(start+accountBatchSize, len(pool))
		batch := pool[start:end]
		accs := make([]wireAccount, len(batch))
		for i, id := range batch {
			accs[i] = wireAccount{ID: id, Ledger: ledgerName, Code: codeName}
		}
		body, err := marshalJSON(wireMessage{Operation: "create_accounts", Accounts: accs})
		if err != nil {
			return err
		}
		rec := &kgo.Record{Topic: topic, Key: []byte(batch[0]), Value: body}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			return fmt.Errorf("produce accounts[%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

// --- transfer generation ---------------------------------------------------

type runConfig struct {
	topic      string
	count      int
	pool       []string
	hotAccount bool
	chain      int
	rate       float64
	garbagePct float64
}

type runStats struct {
	messages  int
	transfers int
	garbage   int
	elapsed   time.Duration
}

func (s runStats) transfersPerSec() float64 {
	if s.elapsed <= 0 {
		return 0
	}
	return float64(s.transfers) / s.elapsed.Seconds()
}

// run streams count transfers to topic, packed chain-wide per message, and
// returns once every produced message has been acknowledged by the broker.
func run(ctx context.Context, cl *kgo.Client, cfg runConfig) (runStats, error) {
	start := time.Now()
	var errCount atomic.Int64
	promise := func(_ *kgo.Record, err error) {
		if err != nil {
			errCount.Add(1)
		}
	}

	var interval time.Duration
	if cfg.rate > 0 {
		interval = time.Duration(float64(time.Second) / cfg.rate)
	}
	nextAllowed := time.Now()

	stats := runStats{}
	sent := 0
	for sent < cfg.count {
		n := min(cfg.chain, cfg.count-sent)

		if interval > 0 {
			now := time.Now()
			if now.Before(nextAllowed) {
				time.Sleep(nextAllowed.Sub(now))
			}
			nextAllowed = nextAllowed.Add(interval * time.Duration(n))
		}

		garbage := rand.Float64()*100 < cfg.garbagePct
		key, body, err := buildMessage(cfg, n, garbage)
		if err != nil {
			return stats, fmt.Errorf("build message: %w", err)
		}

		cl.Produce(ctx, &kgo.Record{Topic: cfg.topic, Key: key, Value: body}, promise)

		stats.messages++
		stats.transfers += n
		if garbage {
			stats.garbage++
		}
		sent += n
	}

	if err := cl.Flush(ctx); err != nil {
		return stats, fmt.Errorf("flush: %w", err)
	}
	if n := errCount.Load(); n > 0 {
		return stats, fmt.Errorf("%d/%d messages failed to produce", n, stats.messages)
	}

	stats.elapsed = time.Since(start)
	return stats, nil
}

// buildMessage renders one wire message carrying n transfers. When garbage
// is true, it instead renders one of a few payloads the decoder is
// guaranteed to poison, so -garbage-pct exercises the DLQ path.
func buildMessage(cfg runConfig, n int, garbage bool) (key, body []byte, err error) {
	if garbage {
		return nil, garbagePayload(), nil
	}

	transfers := make([]wireTransfer, n)
	now := uint64(time.Now().UnixNano())
	for i := range transfers {
		var debit, credit string
		if cfg.hotAccount {
			debit, credit = cfg.pool[0], creditAccount(cfg.pool, cfg.pool[0])
		} else {
			debit, credit = randomPair(cfg.pool)
		}
		var flags []string
		if i < n-1 {
			flags = []string{"linked"}
		}
		transfers[i] = wireTransfer{
			ID:              uuid.NewString(),
			DebitAccountID:  debit,
			CreditAccountID: credit,
			Amount:          randomAmount(),
			Ledger:          ledgerName,
			Code:            codeName,
			Flags:           flags,
			UserData64:      now,
		}
	}
	body, err = marshalJSON(wireMessage{Operation: "create_transfers", Transfers: transfers})
	if err != nil {
		return nil, nil, err
	}
	return []byte(transfers[0].DebitAccountID), body, nil
}

// creditAccount returns a random pool member other than debit.
func creditAccount(pool []string, debit string) string {
	for {
		c := pool[rand.IntN(len(pool))]
		if c != debit {
			return c
		}
	}
}

// randomPair returns two distinct random accounts from the pool.
func randomPair(pool []string) (debit, credit string) {
	debit = pool[rand.IntN(len(pool))]
	credit = creditAccount(pool, debit)
	return debit, credit
}

// randomAmount returns a random positive amount string in USD's minor unit
// scale (cents), e.g. "12.34".
func randomAmount() string {
	minor := rand.Int64N(100_000) + 1
	return fmt.Sprintf("%d.%02d", minor/100, minor%100)
}

// garbagePayload returns one of a few payloads the decoder rejects at
// decode time (poison), never at TigerBeetle apply time.
func garbagePayload() []byte {
	forms := []string{
		`{"operation":"create_transfers","transfers":[{"id":"not-a-uuid","debit_account_id":"x","credit_account_id":"y","amount":"1.00","ledger":"USD","code":"payment"}]}`,
		`{"operation":"create_transfers","transfers":[{"id":"` + uuid.NewString() + `","debit_account_id":"` + uuid.NewString() + `","credit_account_id":"` + uuid.NewString() + `","amount":"not-a-number","ledger":"USD","code":"payment"}]}`,
		`{"operation":"bogus_operation","transfers":[]}`,
		`{"operation":"create_transfers","transfers":[{"id":"` + uuid.NewString() + `"`, // truncated JSON
	}
	return []byte(forms[rand.IntN(len(forms))])
}

// marshalJSON is a thin wrapper so callers don't repeat the import.
func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}
