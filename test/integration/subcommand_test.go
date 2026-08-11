//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/Mi7teR/kafka-tb/internal/config"
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
cdc:
  topic: %s.cdc
  batch_size: 100
  poll_interval: 100ms
  partition_key: debit_account_id
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
`, sharedTBAddr, sharedBrokers[0], name, name, name, name, name, metricsAddr)
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

// subcommandHooks are the two points a scenario needs to reach inside
// runSubcommandAndSIGTERM: before the subprocess is started (the topics
// already exist, so this is where a test can put something in them and add
// CLI arguments), and once it reports ready but before it is signalled (this
// is where a test asserts on what the running process did).
type subcommandHooks struct {
	before func(cfg *config.Config) []string
	ready  func(cfg *config.Config)
}

// runSubcommand starts the built kafkatb binary with subcommand and a config
// pointing at the shared containers, waits for it to report ready, sends
// SIGTERM, and requires it to exit 0 within timeout. It is the SIGTERM
// contract every subcommand that actually runs a pipeline must meet. The
// process's own output is returned so a caller can assert on what it logged.
func runSubcommandAndSIGTERM(t *testing.T, subcommand string, hooks subcommandHooks) string {
	t.Helper()
	name := kafkaName(t.Name())
	cfg := testConfig(t, sharedBrokers, sharedTBAddr)
	createTopics(t, cfg)
	metricsAddr := freeAddr(t)
	cfgPath := filepath.Join(t.TempDir(), "cfg.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(subcommandConfigYAML(name, metricsAddr)), 0o600))

	args := []string{subcommand, "--config", cfgPath}
	if hooks.before != nil {
		args = append(args, hooks.before(cfg)...)
	}
	cmd := exec.Command(sharedBinary, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	require.NoError(t, cmd.Start())

	waitReady(t, metricsAddr, 30*time.Second)
	if hooks.ready != nil {
		hooks.ready(cfg)
	}

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		require.NoError(t, err, "kafkatb %s: %s", subcommand, out.String())
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("kafkatb %s did not exit within 30s of SIGTERM", subcommand)
	}
	return out.String()
}

func TestSubcommandSinkStartsAndStopsOnSIGTERM(t *testing.T) {
	runSubcommandAndSIGTERM(t, "sink", subcommandHooks{})
}

func TestSubcommandRunStartsAndStopsOnSIGTERM(t *testing.T) {
	runSubcommandAndSIGTERM(t, "run", subcommandHooks{})
}

// The real CLI, as its own process, against a real replica: a transfer the
// sink applied has to come back out of the CDC topic.
//
// The assertion is on the record, not on the absence of error lines in the
// log. "No query failure was logged" is satisfied just as well by a job that
// publishes nothing at all, which is the failure this test exists to catch.
func TestSubcommandCDCPublishesWhatTheSinkApplied(t *testing.T) {
	var applied uint64
	var id string

	out := runSubcommandAndSIGTERM(t, "cdc", subcommandHooks{
		before: func(cfg *config.Config) []string {
			tb := newTBClient(t, cfg)
			debit, credit := seedAccounts(t, tb)

			id = uuid.NewString()
			produce(t, sharedBrokers, cfg.Kafka.Topics[0].Name,
				[]string{transferJSON(id, debit, credit, "4.50")})

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			stop := runSink(t, ctx, cfg, tb)
			requireBalance(t, tb, credit, "4.50", applyTimeout)
			stop()

			applied = transferTimestamps(t, tb, []string{id})[0]
			// The cursor starts immediately below the one transfer this test
			// applied, so the subprocess publishes that event and nothing
			// else: the change stream is cluster-wide and this replica is
			// shared with every other test in the package.
			return []string{"--timestamp-last", strconv.FormatUint(applied-1, 10)}
		},
		ready: func(cfg *config.Config) {
			events := readCDC(t, sharedBrokers, cfg.CDC.Topic, 1, cdcTimeout)
			require.Equal(t, applied, events[0].timestamp,
				"the CDC topic holds no record for the transfer the sink applied")
			require.Equal(t, id, events[0].msg.Transfer.ID)
			require.Equal(t, "4.50", events[0].msg.Transfer.Amount)
			require.Equal(t, "single_phase", events[0].msg.Type)
			require.Equal(t, applied, events[0].checkpoint,
				"a closed window's last record claims its own timestamp")
		},
	})
	require.Contains(t, out, "cdc: starting")
	require.NotContains(t, out, "change events query failed")
	require.NotContains(t, out, "publication failed")
}

// Without an output topic there is nowhere to publish and no cursor to
// recover: the CDC job must say so and exit rather than idle silently.
func TestSubcommandCDCRefusesWithoutATopic(t *testing.T) {
	name := kafkaName(t.Name())
	metricsAddr := freeAddr(t)
	cfgPath := filepath.Join(t.TempDir(), "cfg.yaml")
	body := strings.Replace(subcommandConfigYAML(name, metricsAddr), name+".cdc", "", 1)
	require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o600))

	cmd := exec.Command(sharedBinary, "cdc", "--config", cfgPath)
	out, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(out), "cdc.topic")
}
