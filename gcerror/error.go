package gcerror

import "errors"

var (
	ErrGetter = errors.New("nil getter")
	ErrKey    = errors.New("empty key")
)
