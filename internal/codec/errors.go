package codec

import (
	"errors"
	"fmt"
)

// PoisonError means the data is invalid. A retry is pointless; the message goes to the DLQ.
type PoisonError struct {
	Detail string
}

func (e *PoisonError) Error() string { return e.Detail }

func Poison(format string, args ...any) error {
	return &PoisonError{Detail: fmt.Sprintf(format, args...)}
}

func IsPoison(err error) bool {
	var p *PoisonError
	return errors.As(err, &p)
}
