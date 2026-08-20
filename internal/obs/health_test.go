package obs

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// freeAddr grabs an OS-assigned free port, then closes the listener so Serve
// can bind it. Racy in theory.
func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func waitUp(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
}

func TestServeHealthzOK(t *testing.T) {
	addr := freeAddr(t)
	srv := NewServer(addr, func() error { return nil }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	resp, err := http.Get("http://" + addr + "/healthz")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	require.NoError(t, <-done)
}

// TestPprofOffByDefault is the guard on the one thing about pprof that must
// not drift: a Server built the ordinary way exposes no debugging surface.
// /debug/pprof/ is checked because net/http/pprof's init() registers its
// handlers on http.DefaultServeMux the moment the package is linked in, so a
// server that reached for the default mux — or a future refactor that did —
// would serve these without anyone asking.
func TestPprofOffByDefault(t *testing.T) {
	addr := freeAddr(t)
	srv := NewServer(addr, func() error { return nil }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/heap"} {
		resp, err := http.Get("http://" + addr + path)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusNotFound, resp.StatusCode, "%s must not be served by default", path)
	}
	// WithPprof(false) is what a caller passing a config flag through hands
	// over when the flag is unset, so it must mean exactly the same thing.
	require.False(t, NewServer(addr, func() error { return nil }, nil, WithPprof(false)).pprof)

	cancel()
	require.NoError(t, <-done)
}

// TestPprofEnabled checks the flag actually does something, so that
// TestPprofOffByDefault cannot pass by pprof being broken outright.
func TestPprofEnabled(t *testing.T) {
	addr := freeAddr(t)
	srv := NewServer(addr, func() error { return nil }, nil, WithPprof(true))
	// The profilers WithPprof turns on are process-global; put them back
	// where they were so the rest of the package's tests run unprofiled.
	t.Cleanup(func() {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/block"} {
		resp, err := http.Get("http://" + addr + path)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s", path)
	}
	require.NotZero(t, runtime.SetMutexProfileFraction(-1), "the mutex profiler was left off")

	cancel()
	require.NoError(t, <-done)
}

func TestServeReadyzReflectsReadyFunc(t *testing.T) {
	addr := freeAddr(t)
	readyErr := errors.New("not ready yet")
	srv := NewServer(addr, func() error { return readyErr }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	resp, err := http.Get("http://" + addr + "/readyz")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Contains(t, string(body), readyErr.Error())
}

func TestServeMetricsServesRegistry(t *testing.T) {
	addr := freeAddr(t)
	srv := NewServer(addr, func() error { return nil }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	resp, err := http.Get("http://" + addr + "/metrics")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestServeReturnsOnContextCancel(t *testing.T) {
	addr := freeAddr(t)
	srv := NewServer(addr, func() error { return nil }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
}

// I2: a stuck ready (in production, tbx.Client.Nop against an unreachable
// TigerBeetle cluster) must not hang /readyz forever. checkReady bounds the
// call by readyTimeout and answers 503 instead of waiting for it.
func TestServeReadyzTimesOutWhenReadyBlocksForever(t *testing.T) {
	addr := freeAddr(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	srv := NewServer(addr, func() error { <-block; return nil }, nil)
	srv.readyTimeout = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	start := time.Now()
	resp, err := http.Get("http://" + addr + "/readyz")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Less(t, time.Since(start), time.Second,
		"must answer within readyTimeout, not wait for the blocked ready()")
}

// I2: however many probes arrive while TigerBeetle is stuck, at most one
// call to ready is ever outstanding — a stuck cluster costs one leaked
// goroutine total, not one per probe.
func TestServeReadyzCollapsesConcurrentProbes(t *testing.T) {
	addr := freeAddr(t)
	var calls int32
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	srv := NewServer(addr, func() error {
		atomic.AddInt32(&calls, 1)
		<-block
		return nil
	}, nil)
	srv.readyTimeout = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get("http://" + addr + "/readyz")
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		}()
	}
	wg.Wait()

	require.EqualValues(t, 1, atomic.LoadInt32(&calls),
		"N concurrent probes against a blocked ready must invoke it exactly once")
}

// I2: a shutdown racing a still-blocked probe must not turn a clean exit
// into an error — Shutdown's deadline expiring because of it is logged, not
// fatal.
func TestServeShutdownDoesNotErrorOnBlockedProbe(t *testing.T) {
	addr := freeAddr(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	srv := NewServer(addr, func() error { <-block; return nil }, nil)
	srv.readyTimeout = time.Minute // outlasts shutdownTimeout below
	srv.shutdownTimeout = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitUp(t, addr)

	go func() {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	time.Sleep(20 * time.Millisecond) // let the request reach checkReady

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "a blocked readiness probe must not turn a clean shutdown into an error")
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
}
