package codec

import (
	"errors"
	"fmt"
)

// PoisonError — данные некорректны. Ретрай бессмысленен, сообщение идёт в DLQ.
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
