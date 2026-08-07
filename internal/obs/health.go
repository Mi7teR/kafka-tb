// Package obs provides the connector's Prometheus instrumentation and its
// HTTP health/metrics endpoints.
package obs

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// defaultReadyTimeout bounds a single /readyz probe. ready (in
	// production, tbx.Client.Nop) takes no context and retries an
	// unreachable TigerBeetle cluster indefinitely; without a bound a probe
	// would hang forever and leak a goroutine per request.
	defaultReadyTimeout = 3 * time.Second
	// defaultShutdownTimeout bounds how long Serve waits for its HTTP server
	// to drain in-flight requests once ctx is cancelled.
	defaultShutdownTimeout = 5 * time.Second
)

// errReadyTimeout is returned to a /readyz caller when ready did not
// complete within readyTimeout. The underlying call keeps running
// detached — it is not cancellable — but at most one is ever outstanding
// (see checkReady).
var errReadyTimeout = errors.New("readiness check timed out")

// readyCall is one in-flight (possibly still running) call to Server.ready.
// done is closed once the call returns, at which point err holds its result.
type readyCall struct {
	done chan struct{}
	err  error
}

// Server serves /metrics (the default Prometheus registry), /healthz
// (process alive) and /readyz (calls ready, bounded by readyTimeout).
type Server struct {
	addr  string
	ready func() error
	log   *slog.Logger

	readyTimeout    time.Duration
	shutdownTimeout time.Duration

	// mu guards inFlight: readyz probes must collapse to at most one
	// outstanding call to ready, however many requests arrive concurrently
	// or however long a stuck TigerBeetle takes to answer.
	mu       sync.Mutex
	inFlight *readyCall
}

// NewServer builds a Server. log may be nil, in which case shutdown
// diagnostics are dropped instead of logged.
func NewServer(addr string, ready func() error, log *slog.Logger) *Server {
	return &Server{
		addr:            addr,
		ready:           ready,
		log:             log,
		readyTimeout:    defaultReadyTimeout,
		shutdownTimeout: defaultShutdownTimeout,
	}
}

// checkReady runs s.ready, bounded by s.readyTimeout. However many callers
// arrive while a call is outstanding, only one call to s.ready is ever in
// flight: later callers wait on that same call, reusing its result if it
// lands within their own timeout window. A stuck ready therefore costs at
// most one leaked goroutine for the duration of the outage, not one per
// probe — the goroutine is never abandoned mid-flight to start another.
func (s *Server) checkReady() error {
	s.mu.Lock()
	call := s.inFlight
	if call == nil {
		call = &readyCall{done: make(chan struct{})}
		s.inFlight = call
		s.mu.Unlock()
		go func() {
			err := s.ready()
			call.err = err
			close(call.done)
			s.mu.Lock()
			s.inFlight = nil
			s.mu.Unlock()
		}()
	} else {
		s.mu.Unlock()
	}

	select {
	case <-call.done:
		return call.err
	case <-time.After(s.readyTimeout):
		return errReadyTimeout
	}
}

// Serve starts an HTTP server on s.addr. It blocks until ctx is cancelled or
// the server fails to start, then shuts the server down and returns.
func (s *Server) Serve(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if err := s.checkReady(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	httpSrv := &http.Server{Addr: s.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.ListenAndServe() }()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		// A /readyz request can still be blocked on checkReady when the
		// shutdown deadline expires — that is a harmless race with an
		// in-flight probe, not a failure of shutdown itself, and must not
		// turn a clean process exit into a non-zero one.
		if errors.Is(err, context.DeadlineExceeded) {
			if s.log != nil {
				s.log.Warn("metrics server shutdown deadline exceeded, a readiness probe was still in flight",
					slog.String("error", err.Error()))
			}
			return nil
		}
		return err
	}
	return nil
}
