// Command kafkatb runs the Kafka <-> TigerBeetle connector: the sink
// consumes Kafka and applies commands to TigerBeetle, and the CDC job
// publishes TigerBeetle's change events back to Kafka.
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

	"github.com/Mi7teR/kafka-tb/internal/cdc"
	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/codec/jsonc"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	"github.com/Mi7teR/kafka-tb/internal/sink"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
)

// cdcSingleWriterWarning appears in the help of every subcommand that can
// start the CDC job. The sink is fenced by its Kafka consumer group; the CDC
// job has no such fence and none is planned, so the constraint has to be
// impossible to miss.
const cdcSingleWriterWarning = `EXACTLY ONE CDC INSTANCE MAY RUN AGAINST A GIVEN OUTPUT TOPIC.
Nothing enforces this: the CDC job has no consumer group, no lock and no
leader election. Two instances -- an overlapping rolling deploy, a stale pod
-- each publish the whole change stream forever. That is permanent
duplication, not the bounded kind the at-least-once contract allows, and two
writers interleaving on the same key destroy the per-key ordering
cdc.partition_key exists to provide. Deploy the CDC job as a single replica
and cut the old instance over before starting the new one.`

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
			// A signal arriving while a pipeline is still starting up — the
			// CDC job's cursor scan, say — surfaces as a cancelled context
			// wrapped in whatever failed. That is the shutdown doing its job,
			// not a failure, and must not be reported as one.
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				log.Info("stopped cleanly")
				return nil
			}
			return fmt.Errorf("shutdown with error: %w", err)
		}
		log.Info("stopped cleanly")
		return nil
	}

	// timestampLast overrides the cursor the CDC job would otherwise recover
	// from the output topic. It is registered on both commands that can run
	// the job, and only counts when explicitly set: 0 is a meaningful value
	// (replay everything), so "unset" cannot be spelled as a zero default.
	withTimestampLast := func(cmd *cobra.Command) *cobra.Command {
		cmd.Flags().Uint64("timestamp-last", 0,
			"resume the CDC job after this TigerBeetle timestamp instead of the one "+
				"recovered from the output topic (0 replays everything)")
		return cmd
	}
	cdcStart := func(cmd *cobra.Command) *uint64 {
		if !cmd.Flags().Changed("timestamp-last") {
			return nil
		}
		ts, err := cmd.Flags().GetUint64("timestamp-last")
		if err != nil {
			return nil // unreachable: the flag is declared as uint64 above
		}
		return &ts
	}

	sinkCmd := &cobra.Command{
		Use:   "sink",
		Short: "run only the Kafka -> TigerBeetle consumer",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runUntilSignal(cmd, func(ctx context.Context, cfg *config.Config) error {
				return run(ctx, cfg, log, jobs{sink: true})
			})
		},
	}

	runCmd := withTimestampLast(&cobra.Command{
		Use:   "run",
		Short: "run everything this process supports",
		Long: "Run the Kafka -> TigerBeetle sink and, when cdc.topic names one, the\n" +
			"TigerBeetle -> Kafka CDC job, in one process over one TigerBeetle client.\n\n" +
			cdcSingleWriterWarning,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runUntilSignal(cmd, func(ctx context.Context, cfg *config.Config) error {
				// The CDC job runs alongside the sink whenever cdc.topic names
				// one. Left empty it is simply off, so a config written before
				// the job existed keeps working.
				cdcOn := cfg.CDC.Topic != ""
				// --timestamp-last only means something to the CDC job. Taking
				// it and quietly doing nothing with it would let an operator
				// believe a replay had been requested.
				if !cdcOn && cmd.Flags().Changed("timestamp-last") {
					return errors.New(
						"--timestamp-last: the CDC job is off (cdc.topic is empty), so there is no cursor to set")
				}
				return run(ctx, cfg, log, jobs{
					sink:      true,
					cdc:       cdcOn,
					cdcCursor: cdcStart(cmd),
				})
			})
		},
	})

	cdcCmd := withTimestampLast(&cobra.Command{
		Use:   "cdc",
		Short: "run only the TigerBeetle -> Kafka CDC job",
		Long: "Stream TigerBeetle's change events to the Kafka topic named by cdc.topic.\n\n" +
			cdcSingleWriterWarning,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runUntilSignal(cmd, func(ctx context.Context, cfg *config.Config) error {
				if cfg.CDC.Topic == "" {
					return errors.New("cdc.topic: must not be empty to run the CDC job")
				}
				return run(ctx, cfg, log, jobs{cdc: true, cdcCursor: cdcStart(cmd)})
			})
		},
	})

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

// jobs says which of the process's two pipelines to start, and where the CDC
// one should resume from. A nil cdcCursor means "recover it from the output
// topic"; a non-nil one is the operator's --timestamp-last.
type jobs struct {
	sink      bool
	cdc       bool
	cdcCursor *uint64
}

// run starts the requested pipelines over one shared TigerBeetle client and
// returns once ctx is cancelled (SIGINT/SIGTERM) or a pipeline gives up.
//
// Shutdown order for the sink is what it always was: stop consuming
// (consumer.Close, which runs the revoke callback's final commit), flush and
// close the DLQ/results producer, close the batcher (which waits out any
// in-flight TigerBeetle call by design — see tbx.Batcher.Close), and only
// then close the TigerBeetle client. That is what startSink's stop function
// does, and the client is closed by this function's defer afterwards. Each
// server bounds its own graceful drain by cfg.ShutdownTimeout or a fixed
// internal timeout; run does not impose an additional one, so that a
// synchronous caller is never told a transfer failed when it may have
// applied.
func run(ctx context.Context, cfg *config.Config, log *slog.Logger, which jobs) error {
	tbClient, err := tbx.NewClient(cfg.TigerBeetle)
	if err != nil {
		return fmt.Errorf("tigerbeetle client: %w", err)
	}
	defer tbClient.Close()

	metrics := obs.NewMetrics(prometheus.DefaultRegisterer)
	g, gctx := errgroup.WithContext(ctx)

	// TigerBeetle answering is the one readiness condition both pipelines
	// share; each adds its own.
	checks := []func() error{func() error {
		if err := tbClient.Nop(); err != nil {
			return fmt.Errorf("tigerbeetle: %w", err)
		}
		return nil
	}}

	if which.sink {
		stop, ready, err := startSink(gctx, g, cfg, log, tbClient, metrics)
		if err != nil {
			return err
		}
		defer stop()
		checks = append(checks, ready)
	}
	if which.cdc {
		stop, err := startCDC(gctx, g, cfg, log, tbClient, which.cdcCursor)
		if err != nil {
			return err
		}
		defer stop()
	}

	metricsSrv := obs.NewServer(cfg.MetricsAddr, func() error {
		for _, check := range checks {
			if err := check(); err != nil {
				return err
			}
		}
		return nil
	}, log)
	g.Go(func() error { return metricsSrv.Serve(gctx) })
	return g.Wait()
}

// startSink wires the Kafka -> TigerBeetle pipeline on top of tbClient and
// adds it to g. The returned stop closes what was built, in the order the
// shutdown contract requires; a construction error leaves nothing running.
func startSink(
	ctx context.Context, g *errgroup.Group, cfg *config.Config,
	log *slog.Logger, tbClient tbx.Client, metrics *obs.Metrics,
) (stop func(), ready func() error, err error) {
	batcher := tbx.NewBatcher(tbClient, cfg.Batcher, cfg.Retry, log, metrics)
	batcher.Start(ctx)

	reg := model.NewRegistry(cfg)
	decoders, err := codec.NewRegistry(cfg.Kafka.Topics, func(name string) (codec.Decoder, error) {
		if name != "json" {
			return nil, fmt.Errorf("unsupported codec %q", name)
		}
		return jsonc.New(reg, cfg.Limits), nil
	})
	if err != nil {
		batcher.Close()
		return nil, nil, fmt.Errorf("codec registry: %w", err)
	}

	pcl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	if err != nil {
		batcher.Close()
		return nil, nil, fmt.Errorf("kafka producer: %w", err)
	}
	producer := emit.New(pcl, cfg.Kafka)

	var holder sinkHolder
	consumer, err := sink.NewKafkaClient(cfg, holder.onRevoked)
	if err != nil {
		producer.Close()
		batcher.Close()
		return nil, nil, fmt.Errorf("kafka consumer: %w", err)
	}

	s := sink.New(cfg, consumer, decoders, batcher, producer, log, metrics)
	holder.set(s)
	g.Go(func() error { s.Run(ctx); return nil })

	stop = func() {
		consumer.Close()
		producer.Close()
		batcher.Close()
	}
	ready = func() error {
		if id, gen := consumer.GroupMetadata(); id == "" || gen == -1 {
			return errors.New("consumer: not joined to group")
		}
		return nil
	}
	return stop, ready, nil
}

// startCDC wires the TigerBeetle -> Kafka job on top of the same client and
// adds it to g. The cursor is resolved before the job starts: either the
// operator's --timestamp-last, or the highest checkpoint the output topic
// itself holds. This job keeps no state anywhere else.
func startCDC(
	ctx context.Context, g *errgroup.Group, cfg *config.Config,
	log *slog.Logger, tbClient tbx.Client, cursor *uint64,
) (stop func(), err error) {
	// GetChangeEvents is experimental and deliberately absent from
	// tbx.Client, so it is reached through cdc.Source. Every client
	// tbx.NewClient can return implements it; the check is here so a future
	// substitute fails loudly instead of panicking.
	src, ok := tbClient.(cdc.Source)
	if !ok {
		return nil, errors.New("cdc: this TigerBeetle client does not expose change events")
	}

	checkpoint := uint64(0)
	switch {
	case cursor != nil:
		checkpoint = *cursor
		log.Info("cdc: cursor overridden by --timestamp-last", slog.Uint64("checkpoint", checkpoint))
	default:
		if checkpoint, err = cdc.Resume(ctx, cfg.Kafka.Brokers, cfg.CDC.Topic, log); err != nil {
			// A signal landing during the startup scan cancels ctx, and
			// whatever the scan was in the middle of fails with it. Saying so
			// explicitly is what lets the caller tell that shutdown from a
			// broker that could not be reached — the scan's own timeout still
			// reports as the failure it is.
			if ctx.Err() != nil {
				return nil, fmt.Errorf("cdc: cursor recovery interrupted: %w: %w", ctx.Err(), err)
			}
			return nil, err
		}
	}

	pcl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Kafka.Brokers...))
	if err != nil {
		return nil, fmt.Errorf("cdc: kafka producer: %w", err)
	}
	job := cdc.New(cfg.CDC, cfg.Retry, src, pcl, model.NewRegistry(cfg), log)
	g.Go(func() error { return job.Run(ctx, checkpoint) })
	return pcl.Close, nil
}
