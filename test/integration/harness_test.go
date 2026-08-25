//go:build integration

// Package integration drives the real sink against a real Redpanda broker and
// a real TigerBeetle replica, both running in containers.
//
// Both containers are booted once per package run (TestMain) and shared by
// every test: booting them per test costs minutes and buys nothing, because
// isolation comes from names instead — each test derives its topics, its
// consumer group and its account ids from t.Name() and from fresh UUIDs, so no
// two tests can see each other's data.
package integration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
	types "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/codec/jsonc"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/sink"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
)

const (
	redpandaImage    = "docker.redpanda.com/redpandadata/redpanda:v25.2.4"
	tigerbeetleImage = "ghcr.io/tigerbeetle/tigerbeetle:0.17.9"

	// The single ledger every test uses. Ledger id and scale are shared;
	// isolation between tests comes from account ids, which are fresh UUIDs.
	ledgerName  = "USD"
	ledgerID    = uint32(1)
	ledgerScale = int32(2)
	codeName    = "payment"
	codeValue   = uint16(1)
)

var (
	sharedBrokers []string
	sharedTBAddr  string
	// sharedBinary is the kafkatb binary built once for the subcommand/SIGTERM
	// tests, which drive the real CLI as a subprocess rather than calling
	// package code directly — that is the only way to genuinely exercise OS
	// signal delivery.
	sharedBinary string
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()

	rp, err := redpanda.Run(ctx, redpandaImage, redpanda.WithAutoCreateTopics())
	defer func() { _ = testcontainers.TerminateContainer(rp) }()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start redpanda:", err)
		return 1
	}
	seed, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "redpanda seed broker:", err)
		return 1
	}

	tb, addr, err := startTigerBeetleContainer(ctx)
	defer func() { _ = testcontainers.TerminateContainer(tb) }()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start tigerbeetle:", err)
		return 1
	}

	bin, cleanup, err := buildKafkatbBinary(ctx)
	defer cleanup()
	if err != nil {
		fmt.Fprintln(os.Stderr, "build kafkatb:", err)
		return 1
	}
	sharedBinary = bin

	sharedBrokers, sharedTBAddr = []string{seed}, addr
	return m.Run()
}

// buildKafkatbBinary compiles cmd/kafkatb once, into a temp dir the returned
// cleanup removes. On macOS the TigerBeetle client's prebuilt static library
// isn't 8-byte aligned, which the modern linker rejects; Makefile works
// around that with the classic linker, and this does the same directly since
// exec'ing make from here would just build the same thing a second time.
//
// When KAFKATB_COVERDIR is set the binary is built with coverage
// instrumentation. cmd/kafkatb has no tests of its own and is only ever
// exercised the way an operator exercises it -- as a subprocess, over a real
// signal -- so this is the only way its coverage can be counted at all.
// See kafkatbCommand for why the variable is ours rather than GOCOVERDIR.
func buildKafkatbBinary(ctx context.Context) (string, func(), error) {
	dir, err := os.MkdirTemp("", "kafkatb-bin-*")
	cleanup := func() { _ = os.RemoveAll(dir) }
	if err != nil {
		return "", func() {}, err
	}

	bin := filepath.Join(dir, "kafkatb")
	args := []string{"build", "-o", bin}
	if os.Getenv("KAFKATB_COVERDIR") != "" {
		args = append(args, "-cover", "-covermode=atomic",
			"-coverpkg=github.com/Mi7teR/kafka-tb/...")
	}
	if runtime.GOOS == "darwin" {
		args = append(args, "-ldflags=-extldflags=-Wl,-ld_classic")
	}
	args = append(args, "github.com/Mi7teR/kafka-tb/cmd/kafkatb")

	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("go build: %w: %s", err, out)
	}
	return bin, cleanup, nil
}

// kafkatbCommand builds the command for one run of the kafkatb binary.
//
// When coverage is being collected it points the child's GOCOVERDIR at the
// suite's own directory. That has to be done explicitly: "go test" sets
// GOCOVERDIR itself, to a temporary directory it throws away once the run has
// been given -test.gocoverdir, so a child that simply inherited the variable
// would write its counters where nothing ever reads them -- and cmd/kafkatb
// would keep reporting 0% while actually being exercised. The coverage
// runtime flushes on exit, including the non-zero exits these tests assert on.
func kafkatbCommand(args ...string) *exec.Cmd {
	cmd := exec.Command(sharedBinary, args...)
	if dir := os.Getenv("KAFKATB_COVERDIR"); dir != "" {
		cmd.Env = append(os.Environ(), "GOCOVERDIR="+dir)
	}
	return cmd
}

// TigerBeetle needs its data file formatted before the replica can start, so
// the container runs both steps through a shell. --privileged is required:
// without it io_uring setup fails and the replica never listens.
func startTigerBeetleContainer(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:      tigerbeetleImage,
		Entrypoint: []string{"/bin/sh", "-c"},
		Cmd: []string{
			"mkdir -p /data && " +
				"/tigerbeetle format --cluster=0 --replica=0 --replica-count=1 /data/0.tigerbeetle && " +
				"/tigerbeetle start --cache-grid=512MiB --addresses=0.0.0.0:3000 /data/0.tigerbeetle",
		},
		ExposedPorts: []string{"3000/tcp"},
		Privileged:   true,
		WaitingFor: wait.ForAll(
			wait.ForLog("listening on"),
			wait.ForListeningPort("3000/tcp"),
		).WithStartupTimeoutDefault(3 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		return c, "", err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return c, "", err
	}
	port, err := c.MappedPort(ctx, "3000/tcp")
	if err != nil {
		return c, "", err
	}
	// TigerBeetle's address parser takes an IP literal, not a hostname:
	// "localhost:PORT" is rejected as "invalid client cluster address".
	if host == "localhost" {
		host = "127.0.0.1"
	}
	return c, fmt.Sprintf("%s:%s", host, port.Port()), nil
}

// startRedpanda returns the seed brokers of the package-wide Redpanda.
func startRedpanda(t *testing.T) []string {
	t.Helper()
	require.NotEmpty(t, sharedBrokers, "redpanda not started")
	return sharedBrokers
}

// startTigerBeetle returns the address of the package-wide TigerBeetle replica.
func startTigerBeetle(t *testing.T) string {
	t.Helper()
	require.NotEmpty(t, sharedTBAddr, "tigerbeetle not started")
	return sharedTBAddr
}

// testConfig builds a valid sink config whose Kafka names are unique to the
// calling test.
func testConfig(t *testing.T, brokers []string, tbAddr string) *config.Config {
	t.Helper()
	name := kafkaName(t.Name())
	return &config.Config{
		TigerBeetle: config.TigerBeetle{ClusterID: 0, Addresses: []string{tbAddr}},
		Batcher: config.Batcher{
			MaxBatchSize: config.MaxBatchSize,
			Linger:       1 * time.Millisecond,
			MaxQueue:     1000,
		},
		CDC: config.CDC{
			Topic:        name + ".cdc",
			BatchSize:    100,
			PollInterval: 100 * time.Millisecond,
			PartitionKey: config.PartitionKeyDebitAccountID,
		},
		Kafka: config.Kafka{
			Brokers:      brokers,
			Group:        name + ".group",
			Topics:       []config.Topic{{Name: name + ".in", Codec: "json"}},
			DLQTopic:     name + ".dlq",
			ResultsTopic: name + ".results",
		},
		Limits: config.Limits{
			MaxMessageBytes:     1 << 20,
			MaxEventsPerMessage: config.MaxBatchSize,
			MaxJSONDepth:        32,
		},
		Ledgers:         map[string]config.Ledger{ledgerName: {ID: ledgerID, Scale: ledgerScale}},
		Codes:           map[string]uint16{codeName: codeValue},
		Retry:           config.Retry{Initial: 50 * time.Millisecond, Max: 2 * time.Second, Jitter: true},
		ShutdownTimeout: 10 * time.Second,
	}
}

// kafkaName keeps only characters Kafka accepts in a topic name.
func kafkaName(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, s)
}

// createTopics creates the input, DLQ, results and CDC topics with one
// partition each. One partition is deliberate: every test asserts on
// ordering, and ordering is only defined within a partition.
func createTopics(t *testing.T, cfg *config.Config) {
	t.Helper()
	createTopicsWithCDCPartitions(t, cfg, 1)
}

// createTopicsWithCDCPartitions is createTopics with the CDC topic spread over
// cdcPartitions partitions. The CDC tests need more than one: the job's
// records are keyed, so a window lands on several partitions at once and its
// recovery scan has to read every one of their tails. With a single partition
// that scan is trivially correct and proves nothing.
func createTopicsWithCDCPartitions(t *testing.T, cfg *config.Config, cdcPartitions int32) {
	t.Helper()
	names := []string{cfg.Kafka.DLQTopic, cfg.Kafka.ResultsTopic}
	for _, tp := range cfg.Kafka.Topics {
		names = append(names, tp.Name)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	require.NoError(t, err)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopics(ctx, 1, 1, nil, names...)
	require.NoError(t, err)
	for _, r := range resp {
		require.NoError(t, r.Err, "create topic %s", r.Topic)
	}
	if cfg.CDC.Topic == "" {
		return
	}
	cdcResp, err := adm.CreateTopics(ctx, cdcPartitions, 1, nil, cfg.CDC.Topic)
	require.NoError(t, err)
	for _, r := range cdcResp {
		require.NoError(t, r.Err, "create topic %s", r.Topic)
	}
}

// newTBClient dials TigerBeetle and closes the connection with the test.
func newTBClient(t *testing.T, cfg *config.Config) tbx.Client {
	t.Helper()
	c, err := tbx.NewClient(cfg.TigerBeetle)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// runSink assembles the production pipeline — batcher, decoders, emitter,
// consumer — and runs it. The returned stop cancels it and waits for a clean
// shutdown; it is idempotent and also runs as a test cleanup.
func runSink(t *testing.T, ctx context.Context, cfg *config.Config, tb tbx.Client) (stop func()) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	runCtx, cancel := context.WithCancel(ctx)

	batcher := tbx.NewBatcher(tb, cfg.Batcher, cfg.Retry, log, nil)
	batcher.Start(runCtx)

	reg := model.NewRegistry(cfg)
	decoders, err := codec.NewRegistry(cfg.Kafka.Topics, func(name string) (codec.Decoder, error) {
		if name != "json" {
			return nil, fmt.Errorf("unsupported codec %q", name)
		}
		return jsonc.New(reg, cfg.Limits), nil
	})
	require.NoError(t, err)

	producer, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	require.NoError(t, err)
	em := emit.New(producer, cfg.Kafka)

	// The revoke callback is installed before the sink exists, so it reaches
	// the sink through a holder rather than a plain captured variable: the
	// callback runs on franz-go's goroutine and a bare write/read pair would
	// be a data race.
	var holder sinkHolder
	cl, err := sink.NewKafkaClient(cfg, holder.onRevoked)
	require.NoError(t, err)
	s := sink.New(cfg, cl, decoders, batcher, em, log, nil)
	holder.set(s)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(runCtx)
		// Consumer first: closing it triggers the revoke callback, which
		// commits and flushes through the emitter, so the emitter must still
		// be alive at that point.
		cl.Close()
		em.Close()
		batcher.Close()
	}()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Minute):
				t.Error("sink did not shut down within 2m")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// runSinkAbrupt starts the sink the same way runSink does, except for the
// two things that make a stop graceful: it wires no OnRevoked commit (a
// revoke is a no-op here), and s.Run is handed a context this function never
// cancels. The only way to stop this instance is the returned kill, which
// closes the Kafka client out from under the still-running Run loop: the
// in-flight PollRecords call returns with IsClientClosed()==true, and Run
// exits through its very first return statement — before the ctx-cancellation
// final commit and before any revoke commit can run. Whatever the ordinary
// periodic commit ticker already committed is all that survives.
//
// This is the closest approximation to a hard process kill (SIGKILL) this
// in-process harness can express honestly. It is not identical: franz-go's
// Close still performs a synchronous LeaveGroup exchange with the broker
// before PollRecords unblocks, so the broker learns about the departure
// immediately instead of via session-timeout, and the two internal commit
// paths are skipped by construction rather than by the process simply
// vanishing. What it does genuinely exercise is the property under test: no
// commit synchronized with "stop" ever runs, so recovery depends entirely on
// whatever the periodic ticker committed.
func runSinkAbrupt(t *testing.T, cfg *config.Config, tb tbx.Client) (kill func()) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	batcher := tbx.NewBatcher(tb, cfg.Batcher, cfg.Retry, log, nil)
	batcher.Start(context.Background())

	reg := model.NewRegistry(cfg)
	decoders, err := codec.NewRegistry(cfg.Kafka.Topics, func(name string) (codec.Decoder, error) {
		if name != "json" {
			return nil, fmt.Errorf("unsupported codec %q", name)
		}
		return jsonc.New(reg, cfg.Limits), nil
	})
	require.NoError(t, err)

	producer, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	require.NoError(t, err)
	em := emit.New(producer, cfg.Kafka)

	// No-op onRevoked: a revoke triggered as a side effect of the kill below
	// must not be able to commit anything, or this would just be the
	// graceful path under another name.
	cl, err := sink.NewKafkaClient(cfg, func(context.Context, map[string][]int32) {})
	require.NoError(t, err)
	s := sink.New(cfg, cl, decoders, batcher, em, log, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(context.Background())
	}()

	var once sync.Once
	kill = func() {
		once.Do(func() {
			cl.Close()
			select {
			case <-done:
			case <-time.After(2 * time.Minute):
				t.Error("sink did not exit within 2m of an abrupt client close")
			}
			// Not part of the property under test: plain resource cleanup so
			// the rest of the suite does not leak connections and goroutines.
			em.Close()
			batcher.Close()
		})
	}
	t.Cleanup(kill)
	return kill
}

type sinkHolder struct {
	mu sync.Mutex
	s  *sink.Sink
}

func (h *sinkHolder) set(s *sink.Sink) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s = s
}

func (h *sinkHolder) onRevoked(ctx context.Context, revoked map[string][]int32) {
	h.mu.Lock()
	s := h.s
	h.mu.Unlock()
	if s != nil {
		s.OnRevoked(ctx, revoked)
	}
}

// --- TigerBeetle helpers -------------------------------------------------

// createAccounts creates one account per entry in flags — History is forced on
// so balances can be inspected — and returns the generated ids in order.
func createAccounts(t *testing.T, c tbx.Client, flags ...types.AccountFlags) []string {
	t.Helper()
	ids := make([]string, len(flags))
	accounts := make([]types.Account, len(flags))
	for i, f := range flags {
		f.History = true
		id := uuid.NewString()
		u, err := model.ParseID(id)
		require.NoError(t, err)
		accounts[i] = types.Account{ID: u, Ledger: ledgerID, Code: codeValue, Flags: f.ToUint16()}
		ids[i] = id
	}
	res, err := c.CreateAccounts(accounts)
	require.NoError(t, err)
	require.Len(t, res, len(accounts), "create_accounts result array is not dense")
	for i, r := range res {
		require.Equal(t, types.AccountCreated, r.Status, "account %d (%s)", i, ids[i])
	}
	return ids
}

// seedAccounts creates the plain debit/credit pair most scenarios need.
func seedAccounts(t *testing.T, c tbx.Client) (debit, credit string) {
	t.Helper()
	ids := createAccounts(t, c, types.AccountFlags{}, types.AccountFlags{})
	return ids[0], ids[1]
}

// fund moves money straight through the client, bypassing Kafka: it sets up
// preconditions, it is not part of what a test asserts on.
func fund(t *testing.T, c tbx.Client, from, credit, amount string) {
	t.Helper()
	res, err := c.CreateTransfers([]types.Transfer{newTransfer(t, uuid.NewString(), from, credit, amount, 0)})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, types.TransferCreated, res[0].Status)
}

func newTransfer(t *testing.T, id, debit, credit, amount string, flags uint16) types.Transfer {
	t.Helper()
	tid, err := model.ParseID(id)
	require.NoError(t, err)
	d, err := model.ParseID(debit)
	require.NoError(t, err)
	cr, err := model.ParseID(credit)
	require.NoError(t, err)
	amt, err := model.ParseAmount(amount, ledgerScale)
	require.NoError(t, err)
	return types.Transfer{
		ID: tid, DebitAccountID: d, CreditAccountID: cr,
		Amount: amt, Ledger: ledgerID, Code: codeValue, Flags: flags,
	}
}

// accountBalance is credits_posted - debits_posted in minor units. Signed: an
// unrestricted debit account legitimately goes negative.
//
// It returns an error instead of failing the test because polling conditions
// run it on a different goroutine, where require would call t.FailNow off the
// test goroutine.
func accountBalance(c tbx.Client, id string) (*big.Int, error) {
	u, err := model.ParseID(id)
	if err != nil {
		return nil, err
	}
	accounts, err := c.LookupAccounts([]types.Uint128{u})
	if err != nil {
		return nil, err
	}
	if len(accounts) != 1 {
		return nil, fmt.Errorf("account %s not found", id)
	}
	return new(big.Int).Sub(accounts[0].CreditsPosted.BigInt(), accounts[0].DebitsPosted.BigInt()), nil
}

func balanceMinor(t *testing.T, c tbx.Client, id string) *big.Int {
	t.Helper()
	v, err := accountBalance(c, id)
	require.NoError(t, err)
	return v
}

func balanceOf(t *testing.T, c tbx.Client, id string) string {
	t.Helper()
	return formatSigned(balanceMinor(t, c, id))
}

func formatSigned(v *big.Int) string {
	if v.Sign() < 0 {
		return "-" + model.FormatAmount(types.BigIntToUint128(new(big.Int).Neg(v)), ledgerScale)
	}
	return model.FormatAmount(types.BigIntToUint128(v), ledgerScale)
}

// lookupTransfers returns the transfers TigerBeetle actually holds for ids.
// Missing ids are simply absent from the reply, so the length is the count of
// transfers that were applied.
func lookupTransfers(t *testing.T, c tbx.Client, ids []string) []types.Transfer {
	t.Helper()
	us := make([]types.Uint128, len(ids))
	for i, id := range ids {
		u, err := model.ParseID(id)
		require.NoError(t, err)
		us[i] = u
	}
	out, err := c.LookupTransfers(us)
	require.NoError(t, err)
	return out
}

// --- Kafka helpers -------------------------------------------------------

// transferSpec describes one transfer inside a create_transfers message.
type transferSpec struct {
	ID     string
	Debit  string
	Credit string
	Amount string
	Flags  []string
	// PendingID and Timeout are what a two-phase flow needs: the timeout on
	// the pending leg, the id of that leg on the post or void that resolves
	// it. Both are omitted from the message when empty.
	PendingID string
	Timeout   string
}

// transfersJSON renders one create_transfers message. It is written by hand
// rather than marshalled from the decoder's own structs on purpose: the wire
// format is what this suite is testing.
func transfersJSON(specs ...transferSpec) string {
	var sb strings.Builder
	sb.WriteString(`{"operation":"create_transfers","transfers":[`)
	for i, s := range specs {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb,
			`{"id":%q,"debit_account_id":%q,"credit_account_id":%q,"amount":%q,"ledger":%q,"code":%q`,
			s.ID, s.Debit, s.Credit, s.Amount, ledgerName, codeName)
		if s.PendingID != "" {
			fmt.Fprintf(&sb, `,"pending_id":%q`, s.PendingID)
		}
		if s.Timeout != "" {
			fmt.Fprintf(&sb, `,"timeout":%q`, s.Timeout)
		}
		if len(s.Flags) > 0 {
			sb.WriteString(`,"flags":[`)
			for j, f := range s.Flags {
				if j > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, "%q", f)
			}
			sb.WriteByte(']')
		}
		sb.WriteByte('}')
	}
	sb.WriteString(`]}`)
	return sb.String()
}

func transferJSON(id, debit, credit, amount string) string {
	return transfersJSON(transferSpec{ID: id, Debit: debit, Credit: credit, Amount: amount})
}

func produce(t *testing.T, brokers []string, topic string, payloads []string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()

	recs := make([]*kgo.Record, len(payloads))
	for i, p := range payloads {
		recs[i] = &kgo.Record{Topic: topic, Value: []byte(p)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, cl.ProduceSync(ctx, recs...).FirstErr())
}

// settleWindow is how long a reader keeps polling after it has the records it
// was waiting for, to catch a record that should not have been produced.
const settleWindow = 3 * time.Second

// readTopic reads topic from the beginning until it has seen want records,
// then keeps reading for settleWindow to catch extras, and requires exactly
// want. Waiting for "at least n and then no more" is the only honest way to
// assert on a stream that another goroutine is still writing to.
func readTopic(t *testing.T, brokers []string, topic string, want int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer cl.Close()

	var out []*kgo.Record
	deadline := time.Now().Add(timeout)
	for {
		budget := time.Until(deadline)
		if len(out) >= want {
			budget = settleWindow
		}
		if budget <= 0 {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		fetches := cl.PollFetches(ctx)
		cancel()
		// The context deadline above is our own polling bound, not a real
		// fetch error: without filtering it out, a genuinely broken consumer
		// (bad auth, missing topic, ...) would be indistinguishable from an
		// empty topic — every poll "fails" with DeadlineExceeded either way.
		fetches.EachError(func(errTopic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			require.NoError(t, err, "fetch error on topic %s partition %d", errTopic, partition)
		})
		before := len(out)
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
		if len(out) >= want && len(out) == before {
			// Settle window elapsed with nothing new.
			break
		}
	}
	require.Len(t, out, want, "topic %s", topic)
	return out
}

// dlqRecords reads exactly n records from the DLQ topic.
func dlqRecords(t *testing.T, brokers []string, topic string, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	return readTopic(t, brokers, topic, n, timeout)
}

func header(t *testing.T, rec *kgo.Record, key string) string {
	t.Helper()
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	t.Fatalf("record has no header %q", key)
	return ""
}

// requireBalance waits until an account reaches want, then holds it there for
// settleWindow: "reaches and stays" is the assertion, because a double-apply
// would show up as an overshoot a moment later.
func requireBalance(t *testing.T, c tbx.Client, id, want string, timeout time.Duration) {
	t.Helper()
	is := func(v string) func() bool {
		return func() bool {
			b, err := accountBalance(c, id)
			return err == nil && formatSigned(b) == v
		}
	}
	require.Eventually(t, is(want), timeout, 200*time.Millisecond,
		"account %s never reached %s (last %s)", id, want, balanceOf(t, c, id))
	require.Never(t, func() bool { return !is(want)() }, settleWindow, 200*time.Millisecond,
		"account %s moved off %s (now %s)", id, want, balanceOf(t, c, id))
}

// waitBalanceAtLeast waits until an account has received at least minor units,
// used to catch the sink partway through a stream.
func waitBalanceAtLeast(t *testing.T, c tbx.Client, id string, minor int64, timeout time.Duration) {
	t.Helper()
	want := big.NewInt(minor)
	require.Eventually(t, func() bool {
		b, err := accountBalance(c, id)
		return err == nil && b.Cmp(want) >= 0
	}, timeout, 20*time.Millisecond, "account %s never reached %d minor units", id, minor)
}

// withGroup copies cfg with a different consumer group, so the same topic can
// be consumed again from the beginning.
func withGroup(cfg *config.Config, suffix string) *config.Config {
	clone := *cfg
	clone.Kafka.Group = cfg.Kafka.Group + "." + suffix
	return &clone
}

// createTopicsWithSourcePartitions is createTopics with the source topics
// spread over n partitions. A rebalance scenario needs more than one: with a
// single partition there is nothing for the group to split between two
// consumers, so no partition ever migrates and the revoke path never runs.
func createTopicsWithSourcePartitions(t *testing.T, cfg *config.Config, n int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	require.NoError(t, err)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adm := kadm.NewClient(cl)

	resp, err := adm.CreateTopics(ctx, 1, 1, nil, cfg.Kafka.DLQTopic, cfg.Kafka.ResultsTopic)
	require.NoError(t, err)
	for _, r := range resp {
		require.NoError(t, r.Err, "create topic %s", r.Topic)
	}

	srcs := make([]string, 0, len(cfg.Kafka.Topics))
	for _, tp := range cfg.Kafka.Topics {
		srcs = append(srcs, tp.Name)
	}
	srcResp, err := adm.CreateTopics(ctx, n, 1, nil, srcs...)
	require.NoError(t, err)
	for _, r := range srcResp {
		require.NoError(t, r.Err, "create topic %s", r.Topic)
	}
}

// produceKeyed is produce with a record key, so the broker's partitioner
// spreads the batch across partitions instead of parking it on one. Without a
// key franz-go picks a single partition per batch, which would leave a
// multi-partition topic effectively single-partition.
func produceKeyed(t *testing.T, brokers []string, topic string, keys, payloads []string) {
	t.Helper()
	require.Len(t, keys, len(payloads))
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()

	recs := make([]*kgo.Record, len(payloads))
	for i, p := range payloads {
		recs[i] = &kgo.Record{Topic: topic, Key: []byte(keys[i]), Value: []byte(p)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, cl.ProduceSync(ctx, recs...).FirstErr())
}
