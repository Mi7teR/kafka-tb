// Package api implements kafkatbv1.LedgerServer: named ledgers/codes/flags
// and decimal amount strings over TigerBeetle's raw integer model, plus the
// gRPC/HTTP server bootstrap in gateway.go.
package api

import (
	"context"

	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
)

// Submitter is the write path into TigerBeetle: the same interface
// *tbx.Batcher satisfies. Kept narrow so write.go can be tested against a
// stub without a live batcher.
type Submitter interface {
	Submit(ctx context.Context, cmd *model.Command) ([]tbx.Outcome, error)
}

// Server implements kafkatbv1.LedgerServer.
type Server struct {
	kafkatbv1.UnimplementedLedgerServer

	c   tbx.Client
	sub Submitter
	reg *model.Registry
	cfg config.API
	lim config.Limits
}

// NewServer wires the API server. lim is the same limits.* config that
// gates the Kafka decoder (internal/codec/jsonc.Decoder) — CreateTransfers/
// CreateAccounts enforce MaxEventsPerMessage and Serve's REST handler
// enforces MaxMessageBytes, so a tuned-down config cannot make the API
// accept what Kafka rejects.
func NewServer(c tbx.Client, sub Submitter, reg *model.Registry, cfg config.API, lim config.Limits) *Server {
	return &Server{c: c, sub: sub, reg: reg, cfg: cfg, lim: lim}
}
