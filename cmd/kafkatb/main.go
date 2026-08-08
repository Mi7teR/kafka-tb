// Command kafkatb runs the Kafka -> TigerBeetle connector: it consumes
// Kafka and applies commands to TigerBeetle.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/codec/jsonc"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	"github.com/Mi7teR/kafka-tb/internal/sink"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := newRootCmd(log).Execute(); err != nil {
		log.Error("kafkatb", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

// newRootCmd builds the kafkatb command tree. The persistent --config flag
// selects the config file for every subcommand; --metrics-addr demonstrates
// (and is generally useful for) the flag > KAFKATB_* env > file > defaults
// precedence viper gives us: it is bound to the metrics_addr config key and,
// left unset, defers to the env var / file / default in that order.
func newRootCmd(log *slog.Logger) *cobra.Command {
	var cfgPath string

	root := &cobra.Command{
		Use:           "kafkatb",
		Short:         "Kafka -> TigerBeetle connector",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&cfgPath, "config", "configs/example.yaml", "path to config file")
	root.PersistentFlags().String("metrics-addr", "", "override the metrics_addr config value")

	loadConfig := func(cmd *cobra.Command) (*config.Config, error) {
		return config.Load(cfgPath, config.WithFlag("metrics_addr", cmd.Flags().Lookup("metrics-addr")))
	}

	// runUntilSignal loads the config, runs body until ctx is cancelled by
	// SIGINT/SIGTERM or body returns on its own, and logs the outcome the
	// same way for every subcommand that actually runs a pipeline.
	runUntilSignal := func(cmd *cobra.Command, body func(ctx context.Context, cfg *config.Config) error) error {
		cfg, err := loadConfig(cmd)
		if err != nil {
			return fmt.Errorf("config: %w", err)
		}

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		// batcher.Close() below is deliberately unbounded (it waits out any
		// in-flight TigerBeetle call by design), so a slow shutdown can take a
		// while. NotifyContext keeps its signal handler registered until stop is
		// called, which otherwise wouldn't happen until the command returns — a
		// second SIGTERM during that window would be swallowed and only SIGKILL
		// would work. Calling stop as soon as the first signal lands restores
		// the default disposition, so a second SIGTERM terminates the process.
		go func() {
			<-ctx.Done()
			stop()
			log.Info("shutdown signal received, a second SIGTERM will force exit")
		}()

		if err := body(ctx, cfg); err != nil {
			return fmt.Errorf("shutdown with error: %w", err)
		}
		log.Info("stopped cleanly")
		return nil
	}

	sinkCmd := &cobra.Command{
		Use:   "sink",
		Short: "run only the Kafka -> TigerBeetle consumer",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runUntilSignal(cmd, func(ctx context.Context, cfg *config.Config) error {
				return runSink(ctx, cfg, log)
			})
		},
	}

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "run everything this process supports",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Currently identical to sink: the CDC job (Task 24) doesn't exist
			// yet. Once it does, run must start both concurrently.
			return runUntilSignal(cmd, func(ctx context.Context, cfg *config.Config) error {
				return runSink(ctx, cfg, log)
			})
		},
	}

	cdcCmd := &cobra.Command{
		Use:   "cdc",
		Short: "run only the TigerBeetle -> Kafka CDC job",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if _, err := loadConfig(cmd); err != nil {
				return fmt.Errorf("config: %w", err)
			}
			return errors.New("cdc: not implemented yet (see task 24)")
		},
	}

	root.AddCommand(sinkCmd, runCmd, cdcCmd)
	return root
}

// sinkHolder lets the OnPartitionsRevoked callback reach the sink once it
// exists: the callback is registered before the sink can be constructed
// (the client it revokes partitions on doesn't exist yet either), and it
// runs on franz-go's own goroutine, so a bare captured variable would race.
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

// runSink wires the Kafka -> TigerBeetle pipeline together and tears it down
// in order once ctx is cancelled (SIGINT/SIGTERM), or as soon as it returns
// early on a construction error. Shutdown: stop consuming (cl.Close, which
// runs the revoke callback's final commit), flush and close the DLQ/results
// producer, close the batcher (waits out any in-flight TigerBeetle call by
// design — see tbx.Batcher.Close), then close the TigerBeetle client. Each
// component is closed via defer, registered right after it is successfully
// built, which both guarantees a construction error never leaks whatever
// was already started and reproduces that exact order (defers run last-in,
// first-out). Each server bounds its own graceful drain by
// cfg.ShutdownTimeout or a fixed internal timeout; runSink does not impose an
// additional one so that a synchronous caller is never told a transfer
// failed when it may have applied.
func runSink(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	tbClient, err := tbx.NewClient(cfg.TigerBeetle)
	if err != nil {
		return fmt.Errorf("tigerbeetle client: %w", err)
	}
	defer tbClient.Close()

	metrics := obs.NewMetrics(prometheus.DefaultRegisterer)

	batcher := tbx.NewBatcher(tbClient, cfg.Batcher, cfg.Retry, log, metrics)
	batcher.Start(ctx)
	defer batcher.Close()

	reg := model.NewRegistry(cfg)

	var holder sinkHolder
	decoders, err := codec.NewRegistry(cfg.Kafka.Topics, func(name string) (codec.Decoder, error) {
		if name != "json" {
			return nil, fmt.Errorf("unsupported codec %q", name)
		}
		return jsonc.New(reg, cfg.Limits), nil
	})
	if err != nil {
		return fmt.Errorf("codec registry: %w", err)
	}

	pcl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	producer := emit.New(pcl, cfg.Kafka)
	defer producer.Close()

	consumer, err := sink.NewKafkaClient(cfg, holder.onRevoked)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	s := sink.New(cfg, consumer, decoders, batcher, producer, log, metrics)
	holder.set(s)

	ready := func() error {
		if err := tbClient.Nop(); err != nil {
			return fmt.Errorf("tigerbeetle: %w", err)
		}
		if id, gen := consumer.GroupMetadata(); id == "" || gen == -1 {
			return errors.New("consumer: not joined to group")
		}
		return nil
	}
	metricsSrv := obs.NewServer(cfg.MetricsAddr, ready, log)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return metricsSrv.Serve(gctx) })
	g.Go(func() error { s.Run(gctx); return nil })
	return g.Wait()
}
