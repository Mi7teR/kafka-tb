package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

const validCfg = `
mode: all
tigerbeetle:
  cluster_id: 0
  addresses: ["3000"]
batcher:
  max_batch_size: 8189
  linger: 5ms
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
api:
  grpc_addr: ":9090"
  http_addr: ":8080"
  max_page_size: 1000
ledgers:
  USD: {id: 1, scale: 2}
codes:
  payment: 1
retry: {initial: 100ms, max: 30s, jitter: true}
shutdown_timeout: 30s
`

func TestLoadValid(t *testing.T) {
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Equal(t, ModeAll, cfg.Mode)
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

func TestEnvOverride(t *testing.T) {
	t.Setenv("KAFKATB_MODE", "sink")
	cfg, err := Load(writeCfg(t, validCfg))
	require.NoError(t, err)
	require.Equal(t, ModeSink, cfg.Mode)
}

// replace performs a single-occurrence string replacement.
func replace(s, old, new string) string {
	return strings.Replace(s, old, new, 1)
}
