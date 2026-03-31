package await

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestT_Errorf(t *testing.T) {
	at := &T{TB: t}
	at.Errorf("error: %s", "details")
	require.Equal(t, []string{"error: details"}, at.errors)
}

func TestT_Errorf_CollectsAll(t *testing.T) {
	at := &T{TB: t}
	at.Errorf("first: %d", 1)
	at.Errorf("second: %d", 2)
	require.Equal(t, []string{"first: 1", "second: 2"}, at.errors)
}

func TestT_Helper(t *testing.T) {
	at := &T{TB: t}
	// Just verify it doesn't panic
	at.Helper()
}

func TestRequire_ImmediateSuccess(t *testing.T) {
	called := 0
	Require(t, func(t *T) {
		called++
		require.True(t, true) //nolint:testifylint
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, 1, called, "condition should be called exactly once")
}

func TestRequire_EventualSuccess(t *testing.T) {
	var counter atomic.Int32
	go func() {
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo
		counter.Store(42)
	}()

	Require(t, func(t *T) {
		require.Equal(t, int32(42), counter.Load())
	}, time.Second, 10*time.Millisecond)
}

func TestRequire_MultipleAssertions(t *testing.T) {
	type state struct {
		count  atomic.Int32
		ready  atomic.Bool
		status atomic.Value
	}

	s := &state{}
	s.status.Store("initializing")

	go func() {
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo
		s.count.Store(5)
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo
		s.status.Store("ready")
		s.ready.Store(true)
	}()

	Require(t, func(t *T) {
		require.True(t, s.ready.Load(), "should be ready")
		require.Equal(t, int32(5), s.count.Load(), "count should be 5")
		require.Equal(t, "ready", s.status.Load().(string), "status should be ready")
	}, time.Second, 10*time.Millisecond)
}

func TestRequire_RetriesUntilSuccess(t *testing.T) {
	var attempts atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(100 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	Require(t, func(t *T) {
		attempts.Add(1)
		require.True(t, ready.Load())
	}, time.Second, 10*time.Millisecond)

	require.Greater(t, attempts.Load(), int32(1), "should have retried multiple times")
}

func TestRequire_FailNowStopsIteration(t *testing.T) {
	// When require.* fails, it calls FailNow which should stop the current
	// iteration but allow retry
	var attempts atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	Require(t, func(t *T) {
		attempts.Add(1)
		if !ready.Load() {
			require.Fail(t, "not ready yet")
			// This line should not execute after FailNow in require.Fail
			t.Error("should not reach here")
		}
	}, time.Second, 10*time.Millisecond)

	require.True(t, ready.Load())
	require.Greater(t, attempts.Load(), int32(1))
}

func TestRequire_CodeAfterFailureNotExecuted(t *testing.T) {
	var reachedAfterFailure atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(50 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	Require(t, func(t *T) {
		require.True(t, ready.Load(), "not ready")
		// This should only execute on the successful iteration
		reachedAfterFailure.Add(1)
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(1), reachedAfterFailure.Load(),
		"code after failing require should only run once (on success)")
}

func TestRequire_TickTimingRespected(t *testing.T) {
	// Verify that poll interval is actually respected — a slow condition
	// should not compress the interval between attempts.
	var attempts atomic.Int32
	start := time.Now()

	Require(t, func(t *T) {
		n := attempts.Add(1)
		if n < 4 {
			require.Fail(t, "not yet")
		}
	}, time.Second, 25*time.Millisecond)

	elapsed := time.Since(start)
	// 3 failures × 25ms poll = at least 75ms before 4th attempt succeeds
	require.GreaterOrEqual(t, elapsed, 60*time.Millisecond, "should respect poll interval")
	require.Equal(t, int32(4), attempts.Load())
}

// fakeTB is a minimal testing.TB implementation for testing failure scenarios.
type fakeTB struct {
	testing.TB // embed for interface satisfaction
	mu         sync.Mutex
	failed     bool
	errors     []string
	fatals     []string
}

func (f *fakeTB) Helper()             {}
func (f *fakeTB) Name() string        { return "fake" }
func (f *fakeTB) Failed() bool        { f.mu.Lock(); defer f.mu.Unlock(); return f.failed }
func (f *fakeTB) Cleanup(fn func())   {}
func (f *fakeTB) Logf(string, ...any) {}

func (f *fakeTB) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = true
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.mu.Lock()
	f.failed = true
	f.fatals = append(f.fatals, fmt.Sprintf(format, args...))
	f.mu.Unlock()
	runtime.Goexit()
}

func (f *fakeTB) FailNow() {
	f.mu.Lock()
	f.failed = true
	f.mu.Unlock()
	runtime.Goexit()
}

func (f *fakeTB) hasFatal(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.fatals {
		if contains(msg, substr) {
			return true
		}
	}
	return false
}

func (f *fakeTB) hasError(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range f.errors {
		if contains(msg, substr) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func runWithFakeTB(fn func(tb *fakeTB)) *fakeTB {
	tb := &fakeTB{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(tb)
	}()
	<-done
	return tb
}

func TestRequire_TimeoutFailsTest(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(t *T) {
			require.True(t, false, "never succeeds") //nolint:testifylint
		}, 50*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("not satisfied after"), "expected timeout fatal message")
}

func TestRequire_TimeoutReportsAllErrors(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(t *T) {
			require.Equal(t, "expected", "actual", "values must match")
			require.Equal(t, 1, 2, "numbers must match")
		}, 50*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	// The first require.Equal fails and calls FailNow, so only that error is
	// captured per attempt. But it should be reported.
	require.True(t, tb.hasError("last attempt errors"), "expected errors in output")
}

func TestRequiref_IncludesMessageOnTimeout(t *testing.T) {
	tb := runWithFakeTB(func(tb *fakeTB) {
		Requiref(tb, func(t *T) {
			require.True(t, false) //nolint:testifylint
		}, 50*time.Millisecond, 10*time.Millisecond, "workflow %s not ready", "wf-123")
	})
	require.True(t, tb.Failed(), "expected the fake TB to be marked as failed")
	require.True(t, tb.hasFatal("workflow wf-123 not ready"), "expected custom message in fatal")
}

func TestRequire_PanicPropagated(t *testing.T) {
	// A panic in the condition should propagate immediately, not be
	// silently retried until timeout.
	require.PanicsWithValue(t, "unexpected nil pointer", func() {
		Require(t, func(_ *T) {
			panic("unexpected nil pointer")
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
}

func TestRequire_DetectsMisuseOfRealT(t *testing.T) {
	// Simulate someone accidentally using require.X(realT, ...) inside the
	// callback. This calls realT.Errorf (marks failed) then realT.FailNow
	// (Goexit). Require should detect the misuse via tb.Failed().
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(_ *T) {
			tb.Errorf("wrong t used")
			tb.FailNow() // simulates require.X calling FailNow on the real TB
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.True(t, tb.hasFatal("use the *await.T"), "expected misuse detection message")
}

func TestRequire_DetectsMisuseOnSuccessPath(t *testing.T) {
	// When assert.X(s.T(), ...) is used inside the callback, Errorf is called
	// on the real test (marking it failed) but FailNow is NOT called, so the
	// condition appears to pass. Require should still detect this.
	tb := runWithFakeTB(func(tb *fakeTB) {
		Require(tb, func(_ *T) {
			// Simulates assert.Equal(t, ...) — marks failed, no FailNow.
			tb.Errorf("assert-style misuse")
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.True(t, tb.hasFatal("use the *await.T"), "expected misuse detection message")
}

func TestRequire_SkipsWhenAlreadyFailed(t *testing.T) {
	conditionCalled := false
	tb := runWithFakeTB(func(tb *fakeTB) {
		tb.Errorf("previous failure") // mark as failed before Require
		Require(tb, func(t *T) {
			conditionCalled = true
		}, time.Second, 10*time.Millisecond)
	})
	require.True(t, tb.Failed())
	require.False(t, conditionCalled, "condition should not run when test already failed")
}

func TestRequire_NoGoroutineLeak(t *testing.T) {
	// Run Require, then verify no leftover goroutines from the polling loop.
	before := runtime.NumGoroutine()

	var ready atomic.Bool
	go func() {
		time.Sleep(30 * time.Millisecond) //nolint:forbidigo
		ready.Store(true)
	}()

	Require(t, func(t *T) {
		require.True(t, ready.Load())
	}, time.Second, 10*time.Millisecond)

	// Give goroutines a moment to clean up
	time.Sleep(50 * time.Millisecond) //nolint:forbidigo
	after := runtime.NumGoroutine()

	// Allow a small delta for background runtime goroutines
	require.InDelta(t, before, after, 2, "goroutine count should not grow: before=%d after=%d", before, after)
}
