package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

const validCfg = `
tigerbeetle:
  cluster_id: 0
  addresses: ["3000"]
batcher:
  max_batch_size: 8189
  linger: 1ms
  max_queue: 1000
kafka:
  brokers: ["localhost:9092"]
  group: kafkatb
  topics:
    - {name: ledger.transfers, codec: json}
  dlq_topic: ledger.transfers.dlq
  results_topic: ledger.results
limits:
  max_message_bytes: 1048576
  max_events_per_message: 8189
  max_json_depth: 32
ledgers:
  USD: {id: 1, scale: 2}
codes:
  payment: 1
retry: {initial: 100ms, max: 30s, jitter: true}
shutdown_timeout: 30s
metrics_addr: ":9464"
`

func TestLoadValid(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	l, ok := cfg.LedgerByName("USD")
	require.True(t, ok)
	require.Equal(t, uint32(1), l.ID)
	require.Equal(t, int32(2), l.Scale)
	c, ok := cfg.CodeByName("payment")
	require.True(t, ok)
	require.Equal(t, uint16(1), c)
}

func TestLoadRejectsOversizedBatch(t *testing.T) {
	body := replace(validCfg, "max_batch_size: 8189", "max_batch_size: 9000")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "max_batch_size")
}

func TestLoadRejectsDuplicateLedgerID(t *testing.T) {
	body := replace(validCfg, "ledgers:\n  USD: {id: 1, scale: 2}",
		"ledgers:\n  USD: {id: 1, scale: 2}\n  EUR: {id: 1, scale: 2}")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "duplicate ledger id")
}

func TestLoadRejectsEmptyDLQ(t *testing.T) {
	body := replace(validCfg, "dlq_topic: ledger.transfers.dlq", "dlq_topic: \"\"")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "dlq_topic")
}

// P2: TigerBeetle rejects hostnames only at connect time, with an opaque
// error. config.validate must catch it at load time instead.
func TestLoadAcceptsBarePortAddress(t *testing.T) {
	// validCfg already uses a bare port ("3000"); this pins that it stays valid.
	_, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
}

func TestLoadAcceptsIPPortAddress(t *testing.T) {
	body := replace(validCfg, `addresses: ["3000"]`, `addresses: ["127.0.0.1:3000"]`)
	_, err := Load(writeCfg(t, body))
	require.NoError(t, err)
}

func TestLoadRejectsHostnameAddress(t *testing.T) {
	body := replace(validCfg, `addresses: ["3000"]`, `addresses: ["localhost:3000"]`)
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "tigerbeetle.addresses")
	require.ErrorContains(t, err, "localhost:3000")
	require.ErrorContains(t, err, "hostnames are not supported")
}

// Also do this (Task 12 review): metrics_addr moved from a hardcoded
// literal in main.go into config, validated like the other listen
// addresses — must not be empty.
func TestLoadRejectsEmptyMetricsAddr(t *testing.T) {
	body := replace(validCfg, `metrics_addr: ":9464"`, `metrics_addr: ""`)
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "metrics_addr")
}

// sink.max_in_flight_per_partition is optional: an existing config that never
// heard of it must keep loading, with a bound that actually submits something.
func TestLoadDefaultsMaxInFlightPerPartition(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Equal(t, DefaultMaxInFlightPerPartition, cfg.Sink.MaxInFlightPerPartition)
}

// F2: a config written before sink.max_in_flight_per_partition existed may have
// a batcher smaller than the default. Such a config must keep loading — the
// default is clamped to the ceiling, not enforced against it. Failing here
// would break every existing deployment with a small queue on upgrade.
func TestLoadClampsDefaultMaxInFlightToCeiling(t *testing.T) {
	body := replace(validCfg, "max_batch_size: 8189", "max_batch_size: 500")
	body = replace(body, "  max_queue: 1000", "  max_queue: 100")
	body = replace(body, "max_events_per_message: 8189", "max_events_per_message: 500")
	cfg, err := Load(writeCfg(t, body))
	require.NoError(t, err)
	require.Equal(t, 600, cfg.Sink.MaxInFlightPerPartition,
		"default is clamped to max_queue + max_batch_size")
}

// It is specifically the default that gets clamped: an explicitly set value
// above the ceiling is the config author's mistake, and it must not be silently reduced.
func TestLoadRejectsExplicitMaxInFlightAboveSmallCeiling(t *testing.T) {
	body := replace(validCfg, "max_batch_size: 8189", "max_batch_size: 500")
	body = replace(body, "  max_queue: 1000",
		"  max_queue: 100\nsink:\n  max_in_flight_per_partition: 1000")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "sink.max_in_flight_per_partition")
}

// The reachable ceiling is max_queue + max_batch_size, not max_queue: a job
// dequeued into the batch under assembly frees its queue slot while its caller
// stays parked for the whole TigerBeetle call. A bound above that can never be
// reached, so it is a config error rather than a silently useless setting.
func TestLoadRejectsUnreachableMaxInFlightPerPartition(t *testing.T) {
	body := replace(validCfg, "  max_queue: 1000",
		"  max_queue: 1000\nsink:\n  max_in_flight_per_partition: 9190")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "sink.max_in_flight_per_partition")

	body = replace(validCfg, "  max_queue: 1000",
		"  max_queue: 1000\nsink:\n  max_in_flight_per_partition: 9189")
	cfg, err := Load(writeCfg(t, body))
	require.NoError(t, err, "exactly max_queue + max_batch_size is still reachable")
	require.Equal(t, 9189, cfg.Sink.MaxInFlightPerPartition)
}

func TestLoadRejectsNegativeMaxInFlightPerPartition(t *testing.T) {
	body := replace(validCfg, "  max_queue: 1000",
		"  max_queue: 1000\nsink:\n  max_in_flight_per_partition: -1")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "sink.max_in_flight_per_partition")
}

// The --mode flag/field is gone (replaced by cobra subcommands), but the
// property it used to demonstrate — KAFKATB_* overrides the file — still
// needs a live field to exercise it against.
func TestEnvOverridesFile(t *testing.T) {
	t.Setenv("KAFKATB_KAFKA_GROUP", "from-env")
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Equal(t, "from-env", cfg.Kafka.Group)
}

// Nested keys get the same env treatment as top-level ones.
func TestEnvOverridesNestedFile(t *testing.T) {
	t.Setenv("KAFKATB_BATCHER_MAX_QUEUE", "42")
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Equal(t, 42, cfg.Batcher.MaxQueue)
}

func TestLoadRejectsUnknownTopLevelKey(t *testing.T) {
	body := validCfg + "\nmax_batch_sze: 100\n"
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "unknown key")
	require.ErrorContains(t, err, "max_batch_sze")
}

func TestLoadRejectsUnknownNestedKey(t *testing.T) {
	body := replace(validCfg, "max_batch_size: 8189", "max_batch_size: 8189\n  max_batch_sze: 100")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "unknown key")
	require.ErrorContains(t, err, "batcher.max_batch_sze")
}

// Unknown keys are checked below the map/slice boundary too: a typo inside
// a ledgers entry, or inside a topics list element, must not be silently
// accepted as an extra field on that entry.
func TestLoadRejectsUnknownKeyInsideLedgerEntry(t *testing.T) {
	body := replace(validCfg, "USD: {id: 1, scale: 2}", "USD: {id: 1, scale: 2, scal: 2}")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "unknown key")
	require.ErrorContains(t, err, "ledgers.USD.scal")
}

func TestLoadRejectsUnknownKeyInsideTopicsEntry(t *testing.T) {
	body := replace(validCfg, "{name: ledger.transfers, codec: json}", "{name: ledger.transfers, codec: json, codc: json}")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "unknown key")
	require.ErrorContains(t, err, "kafka.topics[0].codc")
}

// A flag takes precedence over KAFKATB_* env, which takes precedence over
// the file — the full chain the task requires.
func TestFlagOverridesEnv(t *testing.T) {
	t.Setenv("KAFKATB_METRICS_ADDR", ":9000")

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.String("metrics-addr", "", "")
	require.NoError(t, fs.Parse([]string{"--metrics-addr=:9500"}))

	cfg, err := Load(writeCfg(t, validCfg), WithFlag("metrics_addr", fs.Lookup("metrics-addr")))
	require.NoError(t, err)
	require.Equal(t, ":9500", cfg.MetricsAddr)
}

// An unset flag must not shadow a lower-precedence source.
func TestUnsetFlagDoesNotOverrideEnv(t *testing.T) {
	t.Setenv("KAFKATB_METRICS_ADDR", ":9000")

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.String("metrics-addr", "", "")
	require.NoError(t, fs.Parse(nil))

	cfg, err := Load(writeCfg(t, validCfg), WithFlag("metrics_addr", fs.Lookup("metrics-addr")))
	require.NoError(t, err)
	require.Equal(t, ":9000", cfg.MetricsAddr)
}

// replace performs a single-occurrence string replacement.
func replace(s, old, new string) string {
	return strings.Replace(s, old, new, 1)
}

func TestLoadDefaultsCDC(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Empty(t, cfg.CDC.Topic, "an absent cdc section leaves the job disabled")
	require.Equal(t, DefaultCDCBatchSize, cfg.CDC.BatchSize)
	require.Equal(t, 2730, cfg.CDC.BatchSize,
		"the default is TigerBeetle's real change-event ceiling: a window's cost is almost"+
			" entirely per-window, so the largest window is the cheapest per event")
	require.Equal(t, DefaultCDCPollInterval, cfg.CDC.PollInterval)
	require.Equal(t, PartitionKeyDebitAccountID, cfg.CDC.PartitionKey)
}

// TestPprofDefaultsOff pins the default of a flag whose wrong value is a
// remote debugging surface rather than a misconfiguration: a config that
// never mentions pprof must not get it.
func TestPprofDefaultsOff(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.False(t, cfg.Pprof)
}

func TestPprofCanBeEnabled(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg+"pprof: true\n"))
	require.NoError(t, err)
	require.True(t, cfg.Pprof)
}

func TestLoadCDCSection(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg+`
cdc:
  topic: ledger.events
  batch_size: 500
  poll_interval: 250ms
  partition_key: ledger
`))
	require.NoError(t, err)
	require.Equal(t, "ledger.events", cfg.CDC.Topic)
	require.Equal(t, 500, cfg.CDC.BatchSize)
	require.Equal(t, 250*time.Millisecond, cfg.CDC.PollInterval)
	require.Equal(t, PartitionKeyLedger, cfg.CDC.PartitionKey)
}

func TestLoadRejectsUnknownPartitionKey(t *testing.T) {
	_, err := Load(writeCfg(t, validCfg+`
cdc: {topic: ledger.events, partition_key: account}
`))
	require.ErrorContains(t, err, "cdc.partition_key")
}

// 2730 is the last cdc.batch_size TigerBeetle serves and 2731 is the first it
// refuses. Bounding this by batcher.max_batch_size (8189) instead let a config
// load cleanly and then never publish an event, so the boundary is pinned on
// both sides and the message has to name the real limit — an operator reading
// it should not have to know why a change event is not a transfer.
func TestLoadRejectsOversizedCDCBatch(t *testing.T) {
	_, err := Load(writeCfg(t, validCfg+`
cdc: {topic: ledger.events, batch_size: 2731}
`))
	require.ErrorContains(t, err, "cdc.batch_size")
	require.ErrorContains(t, err, "2730", "the message must name the real ceiling")

	_, err = Load(writeCfg(t, validCfg+`
cdc: {topic: ledger.events, batch_size: 8189}
`))
	require.ErrorContains(t, err, "cdc.batch_size",
		"batcher.max_batch_size is a transfer count and must not be accepted here")

	cfg, err := Load(writeCfg(t, validCfg+`
cdc: {topic: ledger.events, batch_size: 2730}
`))
	require.NoError(t, err, "exactly the ceiling is still accepted")
	require.Equal(t, MaxCDCBatchSize, cfg.CDC.BatchSize)
}

func TestLoadRejectsNonPositiveCDCPollInterval(t *testing.T) {
	_, err := Load(writeCfg(t, validCfg+`
cdc: {topic: ledger.events, poll_interval: 0s}
`))
	require.ErrorContains(t, err, "cdc.poll_interval")
}

// A message the decoder accepts must be one the batcher can accept too: the
// batcher refuses a command larger than batcher.max_batch_size, and the sink
// treats that refusal as poison, so a limit above the configured batch size
// dead-letters legitimate messages.
func TestLoadRejectsMaxEventsPerMessageAboveBatchSize(t *testing.T) {
	body := replace(validCfg, "max_batch_size: 8189", "max_batch_size: 100")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "max_events_per_message")
	require.ErrorContains(t, err, "max_batch_size")
}

func TestLoadAcceptsMaxEventsPerMessageAtBatchSize(t *testing.T) {
	body := replace(validCfg, "max_batch_size: 8189", "max_batch_size: 100")
	body = replace(body, "max_events_per_message: 8189", "max_events_per_message: 100")
	body = replace(body, "max_queue: 1000", "max_queue: 1000\n  # keep the ceiling wide")
	_, err := Load(writeCfg(t, body))
	require.NoError(t, err)
}

// batcher.linger is what makes a batch a batch: at zero the flush timer is
// ready the moment it is created, so the worker sends whatever single command
// it has and mean batch size collapses to ~1. An absent key must therefore
// default, and an explicit zero must be rejected rather than silently obeyed.
func TestLoadDefaultsBatcherLinger(t *testing.T) {
	body := replace(validCfg, "  linger: 1ms\n", "")
	cfg, err := Load(writeCfg(t, body))
	require.NoError(t, err)
	require.Equal(t, DefaultBatcherLinger, cfg.Batcher.Linger)
}

func TestLoadRejectsZeroBatcherLinger(t *testing.T) {
	body := replace(validCfg, "linger: 1ms", "linger: 0s")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "batcher.linger")
}

// Kafka records per poll and TigerBeetle events per request are different
// dimensions — one record can carry many events — so poll size gets its own
// key instead of borrowing batcher.max_batch_size.
func TestLoadDefaultsPollSize(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Equal(t, DefaultPollSize, cfg.Sink.PollSize)
}

func TestLoadPollSizeIsIndependentOfMaxBatchSize(t *testing.T) {
	body := replace(validCfg, "max_batch_size: 8189", "max_batch_size: 8")
	body = replace(body, "max_events_per_message: 8189", "max_events_per_message: 8")
	body = replace(body, "  max_queue: 1000", "  max_queue: 1000\nsink:\n  poll_size: 500")
	cfg, err := Load(writeCfg(t, body))
	require.NoError(t, err)
	require.Equal(t, 500, cfg.Sink.PollSize,
		"a small TigerBeetle batch must not throttle the Kafka poll")
}

func TestLoadRejectsNonPositivePollSize(t *testing.T) {
	body := replace(validCfg, "  max_queue: 1000", "  max_queue: 1000\nsink:\n  poll_size: 0")
	_, err := Load(writeCfg(t, body))
	require.ErrorContains(t, err, "sink.poll_size")
}
