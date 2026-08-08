//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// subcommandConfigYAML renders a minimal, valid kafkatb config file for the
// CLI subcommand/SIGTERM tests: real (containerized) Kafka and TigerBeetle,
// names unique to the calling test so subcommand tests never collide with
// each other or with the rest of the suite.
func subcommandConfigYAML(name, metricsAddr string) string {
	return fmt.Sprintf(`
tigerbeetle:
  cluster_id: 0
  addresses: [%q]
batcher:
  max_batch_size: 100
  linger: 1ms
  max_queue: 100
kafka:
  brokers: [%q]
  group: %s.group
  topics:
    - {name: %s.in, codec: json}
  dlq_topic: %s.dlq
  results_topic: %s.results
limits:
  max_message_bytes: 1048576
  max_events_per_message: 100
  max_json_depth: 32
ledgers:
  USD: {id: 1, scale: 2}
codes:
  payment: 1
retry: {initial: 50ms, max: 2s, jitter: true}
shutdown_timeout: 10s
metrics_addr: %q
`, sharedTBAddr, sharedBrokers[0], name, name, name, name, metricsAddr)
}

// freeAddr returns a loopback address with a currently-unused port, for
// pointing an about-to-be-started subprocess's metrics server at.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// waitReady polls the subprocess's /readyz until it answers 200 (TigerBeetle
// reachable and the consumer joined its group) or the timeout elapses — the
// proof that the process actually started, not just that it forked.
func waitReady(t *testing.T, metricsAddr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	url := "http://" + metricsAddr + "/readyz"
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx // bounded by the deadline loop
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("subprocess never became ready at %s within %s", url, timeout)
}

// runSubcommand starts the built kafkatb binary with subcommand and a config
// pointing at the shared containers, waits for it to report ready, sends
// SIGTERM, and requires it to exit 0 within timeout. It is the SIGTERM
// contract every subcommand that actually runs a pipeline must meet.
func runSubcommandAndSIGTERM(t *testing.T, subcommand string) {
	t.Helper()
	name := kafkaName(t.Name())
	createTopics(t, testConfig(t, sharedBrokers, sharedTBAddr))
	metricsAddr := freeAddr(t)
	cfgPath := filepath.Join(t.TempDir(), "cfg.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(subcommandConfigYAML(name, metricsAddr)), 0o600))

	cmd := exec.Command(sharedBinary, subcommand, "--config", cfgPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())

	waitReady(t, metricsAddr, 30*time.Second)

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		require.NoError(t, err, "kafkatb %s: %s", subcommand, stderr.String())
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("kafkatb %s did not exit within 30s of SIGTERM", subcommand)
	}
}

func TestSubcommandSinkStartsAndStopsOnSIGTERM(t *testing.T) {
	runSubcommandAndSIGTERM(t, "sink")
}

func TestSubcommandRunStartsAndStopsOnSIGTERM(t *testing.T) {
	runSubcommandAndSIGTERM(t, "run")
}

// cdc isn't implemented yet (Task 24): it must fail fast with a clear error
// instead of pretending to run, so there is nothing to SIGTERM here — the
// contract under test is "fails immediately and says why."
func TestSubcommandCDCFailsNotImplemented(t *testing.T) {
	name := kafkaName(t.Name())
	metricsAddr := freeAddr(t)
	cfgPath := filepath.Join(t.TempDir(), "cfg.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(subcommandConfigYAML(name, metricsAddr)), 0o600))

	cmd := exec.Command(sharedBinary, "cdc", "--config", cfgPath)
	out, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(string(out)), "not implemented")
}
