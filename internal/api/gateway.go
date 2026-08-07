package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

// shutdownTimeout bounds how long Serve waits, once ctx is cancelled or
// either server exits early, for gRPC's GracefulStop and HTTP's Shutdown to
// drain in-flight requests before Serve returns regardless of their state.
// Var (not const) so tests can shrink it instead of waiting out the full
// bound.
var shutdownTimeout = 10 * time.Second

// Serve поднимает gRPC на cfg.GRPCAddr и REST/JSON-шлюз (сгенерированный из
// того же proto) на cfg.HTTPAddr и возвращает управление, когда оба
// остановились — по отмене ctx или по ранней ошибке любого из двух серверов.
// Остановка ограничена shutdownTimeout и не ждёт зависшие RPC: gRPC получает
// GracefulStop, а если тот не укладывается в срок — Stop() без ожидания
// результата (сам зависший обработчик, например Submit внутри
// Batcher.Close, может и не вернуться — это не в силах Go остановить
// принудительно); HTTP получает Shutdown с тем же дедлайном, конкурентно с
// gRPC, а не после него. На любом раннем выходе (до старта серверов)
// закрывает уже занятые слушатели, чтобы порт не оставался занятым.
func (s *Server) Serve(ctx context.Context, cfg config.API) (err error) {
	grpcSrv := grpc.NewServer()
	kafkatbv1.RegisterLedgerServer(grpcSrv, s)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	defer func() {
		if err != nil {
			_ = lis.Close()
		}
	}()

	// grpc-gateway's default marshaler sets UnmarshalOptions.DiscardUnknown,
	// silently dropping unrecognized JSON fields instead of rejecting the
	// request. That contradicts internal/codec/jsonc.Decoder, which sets
	// DisallowUnknownFields so a misspelled field (e.g. "user_data128" for
	// user_data_128) is poison on the Kafka door — on a money path a typo
	// must fail loudly, not silently apply a zeroed field. Leaving
	// UnmarshalOptions at its zero value keeps DiscardUnknown false, so REST
	// rejects the same request Kafka would dead-letter.
	//
	// The two doors can never be fully symmetric: gRPC's binary protobuf
	// wire format has no concept of a misspelled field name (an unknown
	// field number is just skipped per the protobuf spec, and there is no
	// gRPC-level equivalent of DisallowUnknownFields to turn that off) — this
	// marshaler option only closes the gap for the REST/JSON door, which is
	// the one that has JSON field names to typo in the first place.
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
		MarshalOptions: protojson.MarshalOptions{EmitUnpopulated: true},
	}))
	if err := kafkatbv1.RegisterLedgerHandlerFromEndpoint(ctx, mux, dialAddr(lis),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}); err != nil {
		return fmt.Errorf("register gateway: %w", err)
	}
	// http.MaxBytesHandler bounds the REST request body the same way
	// limits.max_message_bytes bounds the Kafka decoder (internal/codec/
	// jsonc.Decoder checks it before parsing): without it, protojson buffers
	// an unbounded POST body whole on a public port.
	handler := http.MaxBytesHandler(mux, int64(s.lim.MaxMessageBytes))
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	httpLis, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	defer func() {
		if err != nil {
			_ = httpLis.Close()
		}
	}()

	// grpcSrv.Serve blocks past GracefulStop/Stop until every in-flight RPC
	// handler returns (see grpc-go's Server.stop), so a stuck handler can
	// keep this goroutine alive forever. It is intentionally not joined via
	// an errgroup.Wait: doing so would make Serve's own return wait on it
	// too. The buffered channel lets the goroutine exit (or leak) without
	// anyone needing to receive from it.
	grpcErrCh := make(chan error, 1)
	go func() { grpcErrCh <- grpcSrv.Serve(lis) }()

	httpErrCh := make(chan error, 1)
	go func() {
		err := httpSrv.Serve(httpLis)
		if err != nil && errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpErrCh <- err
	}()

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-grpcErrCh:
	case serveErr = <-httpErrCh:
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	var sg errgroup.Group
	sg.Go(func() error {
		stopped := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			// GracefulStop is still waiting on an in-flight RPC handler
			// (e.g. Submit blocked inside Batcher.Close's TigerBeetle
			// call). Force-stop without waiting for it: the handler may
			// still never return, but Serve must not hang on it.
			go grpcSrv.Stop()
		}
		return nil
	})
	sg.Go(func() error { return httpSrv.Shutdown(shutdownCtx) })

	if shutdownErr := sg.Wait(); shutdownErr != nil && serveErr == nil {
		serveErr = shutdownErr
	}
	return serveErr
}

// dialAddr returns the address the gateway mux dials to reach the gRPC
// server. lis.Addr() is a wildcard (e.g. "[::]:9090") for the documented
// production config ("grpc_addr: \":9090\""); dialing a wildcard back is a
// historically flaky pattern on Linux, the deployment target. Dial loopback
// on the listener's actual port instead; a host-specific listener (as used
// in tests) is dialed as-is.
func dialAddr(lis net.Listener) string {
	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok || !tcpAddr.IP.IsUnspecified() {
		return lis.Addr().String()
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(tcpAddr.Port))
}
