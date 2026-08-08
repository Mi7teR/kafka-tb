package model

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	types "github.com/tigerbeetle/tigerbeetle-go"
)

var ErrZeroID = errors.New("id must not be zero")

// ParseID converts a UUID string to Uint128. The UUID bytes are placed as-is,
// so the reverse conversion gives back the same string.
func ParseID(s string) (types.Uint128, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return types.Uint128{}, fmt.Errorf("parse id %q: %w", s, err)
	}
	if u == uuid.Nil {
		return types.Uint128{}, ErrZeroID
	}
	var b [16]byte
	copy(b[:], u[:])
	return types.BytesToUint128(b), nil
}

func FormatID(u types.Uint128) string {
	b := u.Bytes()
	var id uuid.UUID
	copy(id[:], b[:])
	return id.String()
}
