package backoff

import (
	"testing"
	"time"
)

func TestNewBackoffDelay(t *testing.T) {
	bo := NewBackoffDelay(1*time.Second, 1*time.Second, 16*time.Second, 30*time.Second)

	if bo.delay != 1*time.Second {
		t.Errorf("expected delay 1s, got %v", bo.delay)
	}
	if bo.baseDelay != 1*time.Second {
		t.Errorf("expected baseDelay 1s, got %v", bo.baseDelay)
	}
	if bo.maxDelay != 16*time.Second {
		t.Errorf("expected maxDelay 16s, got %v", bo.maxDelay)
	}
	if bo.stableWindow != 30*time.Second {
		t.Errorf("expected stableWindow 30s, got %v", bo.stableWindow)
	}
}

func TestEscalateDoublesDelay(t *testing.T) {
	bo := NewBackoffDelay(1*time.Second, 1*time.Second, 16*time.Second, 30*time.Second)

	bo.escalate()
	if bo.delay != 2*time.Second {
		t.Errorf("expected 2s after first escalate, got %v", bo.delay)
	}

	bo.escalate()
	if bo.delay != 4*time.Second {
		t.Errorf("expected 4s after second escalate, got %v", bo.delay)
	}

	bo.escalate()
	if bo.delay != 8*time.Second {
		t.Errorf("expected 8s after third escalate, got %v", bo.delay)
	}
}

func TestEscalateResetsAtMaxDelay(t *testing.T) {
	bo := NewBackoffDelay(8*time.Second, 1*time.Second, 16*time.Second, 30*time.Second)

	bo.escalate() // 16s
	if bo.delay != 16*time.Second {
		t.Errorf("expected 16s, got %v", bo.delay)
	}

	bo.escalate() // 32s > maxDelay → resets to 1s
	if bo.delay != 1*time.Second {
		t.Errorf("expected reset to 1s, got %v", bo.delay)
	}
}

func TestResetBackoff(t *testing.T) {
	bo := NewBackoffDelay(8*time.Second, 2*time.Second, 16*time.Second, 30*time.Second)

	bo.resetBackoff()
	if bo.delay != 2*time.Second {
		t.Errorf("expected reset to baseDelay 2s, got %v", bo.delay)
	}
}

func TestWaitEscalatesDelay(t *testing.T) {
	bo := NewBackoffDelay(10*time.Millisecond, 10*time.Millisecond, 160*time.Millisecond, 1*time.Hour)
	bo.StartTimer()

	start := time.Now()
	bo.Wait()
	elapsed := time.Since(start)

	if elapsed < 10*time.Millisecond {
		t.Errorf("expected at least 10ms sleep, got %v", elapsed)
	}
	if bo.delay != 20*time.Millisecond {
		t.Errorf("expected delay escalated to 20ms, got %v", bo.delay)
	}
}

func TestWaitResetsAfterStableWindow(t *testing.T) {
	bo := NewBackoffDelay(100*time.Millisecond, 10*time.Millisecond, 1*time.Second, 50*time.Millisecond)
	bo.StartTimer()

	// Wait longer than stable window
	time.Sleep(60 * time.Millisecond)

	bo.Wait()

	// After Wait, delay should have been reset to baseDelay then escalated
	if bo.delay != 20*time.Millisecond {
		t.Errorf("expected delay reset to base (10ms) then escalated to 20ms, got %v", bo.delay)
	}
}

func TestStartTimerSetsTime(t *testing.T) {
	bo := NewBackoffDelay(1*time.Second, 1*time.Second, 16*time.Second, 30*time.Second)

	before := time.Now()
	bo.StartTimer()
	after := time.Now()

	if bo.timerStart.Before(before) || bo.timerStart.After(after) {
		t.Errorf("timerStart not set correctly")
	}
}

func TestFullEscalationCycle(t *testing.T) {
	bo := NewBackoffDelay(1*time.Millisecond, 1*time.Millisecond, 8*time.Millisecond, 1*time.Hour)
	bo.StartTimer()

	// 1ms → 2ms → 4ms → 8ms → 16ms (exceeds max) → resets to 1ms
	expected := []time.Duration{
		2 * time.Millisecond,
		4 * time.Millisecond,
		8 * time.Millisecond,
		1 * time.Millisecond, // reset after exceeding max
	}

	for i, exp := range expected {
		bo.Wait()
		if bo.delay != exp {
			t.Errorf("step %d: expected %v, got %v", i+1, exp, bo.delay)
		}
	}
}
