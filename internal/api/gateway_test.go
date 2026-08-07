package api

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

// freeAddr asks the OS for an unused loopback port, then releases it
// immediately so Serve can bind it. There is a theoretical race if another
// process grabs the same port in between, but on loopback with an
// ephemeral port the OS just handed out this is not observed in practice —
// it is the standard "find a free port" idiom for in-process Go tests.
func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

// waitForDial polls addr until a TCP connection succeeds, so the test does
// not race Serve's own binding of the listener.
func waitForDial(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to accept connections", addr)
}

// TestServeServesGRPCAndRESTAndStopsOnCancel exercises the actual bootstrap:
// one write RPC over REST, one over gRPC, both against the same running
// Serve, then cancellation must make Serve return.
func TestServeServesGRPCAndRESTAndStopsOnCancel(t *testing.T) {
	grpcAddr := freeAddr(t)
	httpAddr := freeAddr(t)

	sub := &stubSubmitter{outcomes: []tbx.Outcome{
		{Index: 0, ID: "0193f8a1-7c2e-7000-8000-000000000001", Status: tbx.StatusOK},
	}}
	srv := NewServer(nil, sub, testRegistry(), config.API{MaxPageSize: 10})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- srv.Serve(ctx, config.API{GRPCAddr: grpcAddr, HTTPAddr: httpAddr, MaxPageSize: 10})
	}()

	waitForDial(t, grpcAddr)
	waitForDial(t, httpAddr)

	// REST: POST /v1/transfers.
	reqBody, err := protojson.Marshal(req())
	require.NoError(t, err)
	httpResp, err := http.Post("http://"+httpAddr+"/v1/transfers", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	defer func() { _ = httpResp.Body.Close() }()
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	var buf bytes.Buffer
	_, err = buf.ReadFrom(httpResp.Body)
	require.NoError(t, err)
	var restResp kafkatbv1.CreateTransfersResponse
	require.NoError(t, protojson.Unmarshal(buf.Bytes(), &restResp))
	require.Len(t, restResp.Results, 1)
	require.Equal(t, "ok", restResp.Results[0].Status)

	// gRPC: CreateAccounts over a real connection.
	sub.outcomes = []tbx.Outcome{
		{Index: 0, ID: "0193f8a1-7c2e-7000-8000-000000000001", Status: tbx.StatusOK},
	}
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	client := kafkatbv1.NewLedgerClient(conn)
	grpcResp, err := client.CreateAccounts(context.Background(), accountReq())
	require.NoError(t, err)
	require.Len(t, grpcResp.Results, 1)
	require.Equal(t, "ok", grpcResp.Results[0].Status)

	cancel()
	select {
	case err := <-serveDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after context cancellation")
	}
}

// TestServeReleasesGRPCListenerOnHTTPBindFailure reproduces C1: if the HTTP
// listener fails to bind after the gRPC listener already succeeded, Serve
// must close the gRPC listener before returning — otherwise the port stays
// bound and a supervisor's retry leaks a socket per attempt.
func TestServeReleasesGRPCListenerOnHTTPBindFailure(t *testing.T) {
	grpcAddr := freeAddr(t)
	httpAddr := freeAddr(t)

	// Occupy the HTTP address so Serve's own httpLis bind fails.
	blocker, err := net.Listen("tcp", httpAddr)
	require.NoError(t, err)
	defer func() { _ = blocker.Close() }()

	srv := NewServer(nil, &stubSubmitter{}, testRegistry(), config.API{MaxPageSize: 10})
	err = srv.Serve(context.Background(), config.API{GRPCAddr: grpcAddr, HTTPAddr: httpAddr, MaxPageSize: 10})
	require.Error(t, err)

	// If the gRPC listener leaked, this rebind fails with "address already
	// in use".
	lis, err := net.Listen("tcp", grpcAddr)
	require.NoError(t, err, "grpc listener should have been released after Serve failed")
	require.NoError(t, lis.Close())
}

// blockingSubmitter simulates Batcher.Close waiting on an in-flight
// TigerBeetle call: Submit never returns and ignores context cancellation,
// exactly the scenario I2 describes — a CreateTransfers stuck inside Submit
// hangs GracefulStop.
type blockingSubmitter struct {
	started chan struct{}
}

func (b *blockingSubmitter) Submit(context.Context, *model.Command) ([]tbx.Outcome, error) {
	close(b.started)
	select {} // blocks forever; nothing can force this goroutine to return.
}

// TestServeShutdownIsBoundedByStuckRPC reproduces I2: an RPC parked inside
// Submit must not stop Serve from returning once ctx is cancelled — the
// shutdown has to be bounded even when GracefulStop can't finish.
func TestServeShutdownIsBoundedByStuckRPC(t *testing.T) {
	orig := shutdownTimeout
	shutdownTimeout = 300 * time.Millisecond
	t.Cleanup(func() { shutdownTimeout = orig })

	grpcAddr := freeAddr(t)
	httpAddr := freeAddr(t)

	sub := &blockingSubmitter{started: make(chan struct{})}
	srv := NewServer(nil, sub, testRegistry(), config.API{MaxPageSize: 10})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- srv.Serve(ctx, config.API{GRPCAddr: grpcAddr, HTTPAddr: httpAddr, MaxPageSize: 10})
	}()

	waitForDial(t, grpcAddr)
	waitForDial(t, httpAddr)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	client := kafkatbv1.NewLedgerClient(conn)
	go func() { _, _ = client.CreateTransfers(context.Background(), req()) }()

	select {
	case <-sub.started:
	case <-time.After(2 * time.Second):
		t.Fatal("RPC never reached Submit")
	}

	cancel()
	select {
	case err := <-serveDone:
		require.NoError(t, err)
	case <-time.After(shutdownTimeout + 5*time.Second):
		t.Fatal("Serve did not return within the shutdown bound")
	}
}
