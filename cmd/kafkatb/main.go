// Command kafkatb runs the Kafka -> TigerBeetle connector: depending on
// -mode it consumes Kafka and applies commands to TigerBeetle, serves the
// gRPC/REST ledger API, or both.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/Mi7teR/kafka-tb/internal/api"
	"github.com/Mi7teR/kafka-tb/internal/codec"
	"github.com/Mi7teR/kafka-tb/internal/codec/jsonc"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/emit"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/obs"
	"github.com/Mi7teR/kafka-tb/internal/sink"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
)

// metricsAddr serves /metrics, /healthz and /readyz. It is deliberately not
// derived from cfg.API.HTTPAddr: that address is already owned by the
// grpc-gateway REST mux in api/sink modes, and binding both to the same port
// would fail at startup.
const metricsAddr = ":9464"

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/example.yaml", "path to config file")
	mode := flag.String("mode", "", "override mode: sink|api|all")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Error("config", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if *mode != "" {
		cfg.Mode = config.Mode(*mode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("shutdown with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("stopped cleanly")
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

// run wires every component together and tears them down in order once ctx
// is cancelled (SIGINT/SIGTERM). Shutdown: stop consuming (cl.Close, which
// runs the revoke callback's final commit), flush and close the DLQ/results
// producer, close the batcher (waits out any in-flight TigerBeetle call by
// design — see tbx.Batcher.Close), then close the TigerBeetle client. Each
// server bounds its own graceful drain by cfg.ShutdownTimeout or a fixed
// internal timeout; run does not impose an additional one so that a
// synchronous caller is never told a transfer failed when it may have
// applied.
func run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	tbClient, err := tbx.NewClient(cfg.TigerBeetle)
	if err != nil {
		return fmt.Errorf("tigerbeetle client: %w", err)
	}

	metrics := obs.NewMetrics(prometheus.DefaultRegisterer)

	batcher := tbx.NewBatcher(tbClient, cfg.Batcher, cfg.Retry, log, metrics)
	batcher.Start(ctx)

	reg := model.NewRegistry(cfg)

	var (
		consumer *kgo.Client
		producer emit.Emitter
		s        *sink.Sink
	)
	var holder sinkHolder
	if cfg.Mode != config.ModeAPI {
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
		producer = emit.New(pcl, cfg.Kafka)

		consumer, err = sink.NewKafkaClient(cfg, holder.onRevoked)
		if err != nil {
			return fmt.Errorf("kafka consumer: %w", err)
		}
		s = sink.New(cfg, consumer, decoders, batcher, producer, log, metrics)
		holder.set(s)
	}

	var apiSrv *api.Server
	if cfg.Mode != config.ModeSink {
		apiSrv = api.NewServer(tbClient, batcher, reg, cfg.API)
	}

	ready := func() error {
		if err := tbClient.Nop(); err != nil {
			return fmt.Errorf("tigerbeetle: %w", err)
		}
		if consumer != nil {
			if id, gen := consumer.GroupMetadata(); id == "" || gen == -1 {
				return errors.New("consumer: not joined to group")
			}
		}
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return obs.Serve(gctx, metricsAddr, ready) })
	if s != nil {
		g.Go(func() error { s.Run(gctx); return nil })
	}
	if apiSrv != nil {
		g.Go(func() error { return apiSrv.Serve(gctx, cfg.API) })
	}
	runErr := g.Wait()

	if consumer != nil {
		consumer.Close()
	}
	if producer != nil {
		producer.Close()
	}
	batcher.Close()
	tbClient.Close()

	return runErr
}
