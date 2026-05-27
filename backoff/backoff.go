package backoff

import "time"

type BackoffDelay struct {
	delay        time.Duration
	baseDelay    time.Duration
	maxDelay     time.Duration
	stableWindow time.Duration
	timerStart   time.Time
}

func NewBackoffDelay(delay, baseDelay, maxDelay, stableWindow time.Duration) *BackoffDelay {
	return &BackoffDelay{
		delay:        delay,
		baseDelay:    baseDelay,
		maxDelay:     maxDelay,
		stableWindow: stableWindow,
	}
}

func (bd *BackoffDelay) StartTimer() {
	bd.timerStart = time.Now()
}

func (bd *BackoffDelay) escalate() {
	bd.delay *= 2
	if bd.delay > bd.maxDelay {
		bd.resetBackoff()
	}
}

func (bd *BackoffDelay) resetBackoff() {
	bd.delay = bd.baseDelay
}

func (bd *BackoffDelay) Wait() {
	if time.Since(bd.timerStart) > bd.stableWindow {
		bd.resetBackoff()
	}

	time.Sleep(bd.delay)
	bd.escalate()
}
