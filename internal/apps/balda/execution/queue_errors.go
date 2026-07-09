package execution

import (
	"errors"
)

// ErrCommandQueueFull means the durable command stream rejected new work due to pressure.
var ErrCommandQueueFull = errors.New("command queue is full")

// IsCommandQueueFull reports whether an error came from command stream pressure.
func IsCommandQueueFull(err error) bool {
	return errors.Is(err, ErrCommandQueueFull)
}
