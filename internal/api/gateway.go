package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// httpShutdownTimeout bounds how long Serve waits for in-flight HTTP
// requests to drain once the context is cancelled.
const httpShutdownTimeout = 10 * time.Second

// Serve поднимает gRPC на cfg.GRPCAddr и REST/JSON-шлюз (сгенерированный из
// того же proto) на cfg.HTTPAddr, гасит оба по отмене ctx и возвращает
// управление только после того, как оба остановились.
func (s *Server) Serve(ctx context.Context, cfg config.API) error {
	grpcSrv := grpc.NewServer()
	kafkatbv1.RegisterLedgerServer(grpcSrv, s)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	mux := runtime.NewServeMux()
	if err := kafkatbv1.RegisterLedgerHandlerFromEndpoint(ctx, mux, lis.Addr().String(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}); err != nil {
		return fmt.Errorf("register gateway: %w", err)
	}
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	httpLis, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return grpcSrv.Serve(lis) })
	g.Go(func() error {
		if err := httpSrv.Serve(httpLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		grpcSrv.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), httpShutdownTimeout)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}
