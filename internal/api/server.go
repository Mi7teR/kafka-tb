// Package api implements the gRPC read API described by
// proto/kafkatb/v1/kafkatb.proto: named ledgers/codes/flags and decimal
// amount strings over TigerBeetle's raw integer model.
package api

import (
	kafkatbv1 "github.com/Mi7teR/kafka-tb/gen/kafkatb/v1"
	"github.com/Mi7teR/kafka-tb/internal/config"
	"github.com/Mi7teR/kafka-tb/internal/model"
	"github.com/Mi7teR/kafka-tb/internal/tbx"
)

// Server implements the read side of kafkatbv1.LedgerServer. The write RPCs
// (CreateAccounts, CreateTransfers) and the gRPC/HTTP server bootstrap are
// added in a later task; embedding UnimplementedLedgerServer keeps Server
// satisfying the full interface until then.
type Server struct {
	kafkatbv1.UnimplementedLedgerServer

	c   tbx.Client
	reg *model.Registry
	cfg config.API
}

func NewServer(c tbx.Client, reg *model.Registry, cfg config.API) *Server {
	return &Server{c: c, reg: reg, cfg: cfg}
}
