package breaker

import (
	"time"
)

type CircuitBreakerOptions struct {
	threshold        int
	baseResetTimeout time.Duration
	maxResetTimeout  time.Duration
}

type CircuitState int

const (
	Closed CircuitState = iota
	Open
	HalfOpen
)

func orDefault[T comparable](val, fallback T) T {
	var zero T
	if val == zero {
		return fallback
	}
	return val
}

func (cs CircuitState) String() string {
	switch cs {
	case Closed:
		return "Closed"
	case Open:
		return "Open"
	case HalfOpen:
		return "HalfOpen"
	default:
		return "Unknown"
	}
}
