// Package await provides polling-based test assertions as a replacement
// for testify's Eventually, EventuallyWithT, and their formatted variants.
//
// Improvements over testify's eventually functions:
//
//   - Better errors: testify's Eventually(func() bool) swallows errors — on
//     timeout you get "condition never satisfied" with no context about why.
//     This package captures all assertion failure messages and reports them.
//
//   - Misuse detection: accidentally using the real *testing.T (e.g. s.T() or
//     suite assertion methods) instead of the callback's collect T is a
//     common mistake. This package detects it and fails with a clear message.
//
//   - Panic propagation: if the condition panics (e.g. nil dereference), the
//     panic is propagated immediately rather than being silently swallowed
//     or retried until timeout.
//     See https://github.com/stretchr/testify/issues/1810
//
//   - No goroutine leaks: testify's Eventually may return on timeout while
//     the condition goroutine is still running, causing "panic: Fail in
//     goroutine after Test has completed" crashes and data races. This
//     package waits for each attempt to finish before starting the next.
//     See https://github.com/stretchr/testify/issues/1611
//
//   - Condition always runs: testify's Eventually can fail without ever
//     running the condition due to a timer/ticker race with short timeouts.
//     This package runs the condition immediately on the first iteration.
//     See https://github.com/stretchr/testify/issues/1652
//
//   - Single API: replaces four testify functions (Eventually, Eventuallyf,
//     EventuallyWithT, EventuallyWithTf) with just Require and Requiref.
package await

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// T is passed to the condition callback. It intercepts assertion failures
// so the polling loop can retry. Pass it to require.* functions — it
// satisfies testing.TB.
//
// Only use T for assertions (require.*, t.Errorf, t.FailNow). Methods like
// t.Cleanup, t.Skip, and t.Setenv delegate to the real test and should not
// be called inside polling conditions.
type T struct {
	testing.TB
	errors []string
}

// Errorf records an error message for reporting on timeout.
func (t *T) Errorf(format string, args ...any) {
	t.errors = append(t.errors, fmt.Sprintf(format, args...))
}

// FailNow is called by require.* on failure. It triggers runtime.Goexit()
// which terminates the goroutine and is detected by Require to retry.
// Unlike testing.TB.FailNow(), this does NOT mark the test as failed.
func (t *T) FailNow() {
	runtime.Goexit()
}

// Require runs condition repeatedly until it completes without assertion
// failures, or until the timeout expires. The timeout is capped at the test's
// deadline if one is set.
//
// The condition receives an *await.T for assertions. Pass it to require.*
// functions. When assertions fail, Require catches the failure and retries.
//
// A goroutine is used per attempt so that runtime.Goexit (called by
// require.FailNow) terminates only the attempt, not the test.
//
// Example:
//
//	await.Require(t, func(t *await.T) {
//	    resp, err := client.GetStatus(ctx)
//	    require.NoError(t, err)
//	    require.Equal(t, "ready", resp.Status)
//	}, 5*time.Second, 200*time.Millisecond)
func Require(tb testing.TB, condition func(*T), timeout, pollInterval time.Duration) {
	tb.Helper()
	run(tb, condition, timeout, pollInterval, "")
}

// Requiref is like Require but accepts a format string that is included in the
// failure message when the condition is not satisfied before the timeout.
//
// Example:
//
//	await.Requiref(t, func(t *await.T) {
//	    require.Equal(t, "ready", status.Load())
//	}, 5*time.Second, 200*time.Millisecond, "workflow %s did not reach ready state", wfID)
func Requiref(tb testing.TB, condition func(*T), timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	run(tb, condition, timeout, pollInterval, fmt.Sprintf(msg, args...))
}

func run(tb testing.TB, condition func(*T), timeout, pollInterval time.Duration, msg string) {
	tb.Helper()

	// Skip if the test already failed — no point polling.
	if tb.Failed() {
		tb.Logf("await.Require: skipping (test already failed)")
		return
	}

	deadline := time.Now().Add(timeout)

	// Cap at the test's deadline if one is set, so we don't sleep past it.
	if d, ok := tb.(interface{ Deadline() (time.Time, bool) }); ok {
		if testDeadline, hasDeadline := d.Deadline(); hasDeadline && testDeadline.Before(deadline) {
			deadline = testDeadline
		}
	}

	polls := 0

	for {
		polls++
		t := &T{TB: tb}

		// Run condition in a goroutine so that runtime.Goexit (called by
		// require.FailNow) terminates only this goroutine, not the test.
		//
		// Channel protocol:
		//   true      → condition passed
		//   false     → assertion failed (Goexit from FailNow)
		//   panicVal  → condition panicked (propagated to caller)
		done := make(chan any, 1)
		go func() {
			defer func() {
				// Order matters: recover() returns nil during Goexit,
				// so a non-nil value means a real panic.
				if r := recover(); r != nil {
					done <- r // propagate panic
					return
				}
				// If we reach here via Goexit (from FailNow), send false.
				// If condition completed normally, true was already sent.
				select {
				case done <- false:
				default:
				}
			}()
			condition(t)
			done <- true // success - condition completed without FailNow
		}()

		result := <-done
		switch v := result.(type) {
		case bool:
			if v {
				// Detect misuse: assert.X(s.T(), ...) marks the real test as
				// failed but does not call FailNow, so the condition appears to
				// pass while the test is actually broken.
				if tb.Failed() {
					tb.Fatalf("await.Require: the test was marked failed directly — " +
						"use the *await.T passed to the callback, not s.T() or suite assertion methods")
				}
				return // condition passed
			}
		default:
			// Condition panicked — propagate immediately.
			panic(v)
		}

		// Detect misuse: require.NoError(s.T(), ...) inside the callback marks
		// the real test as failed via Errorf then calls FailNow (Goexit).
		if tb.Failed() {
			tb.Fatalf("await.Require: the test was marked failed directly — " +
				"use the *await.T passed to the callback, not s.T() or suite assertion methods")
			return
		}

		// Check timeout before sleeping.
		if time.Now().After(deadline) {
			if len(t.errors) > 0 {
				tb.Errorf("last attempt errors:\n%s", strings.Join(t.errors, "\n"))
			}
			if msg != "" {
				tb.Fatalf("await.Require: %s (not satisfied after %v, %d polls)", msg, timeout, polls)
			} else {
				tb.Fatalf("await.Require: condition not satisfied after %v (%d polls)", timeout, polls)
			}
			return
		}

		// Wait before next attempt, but respect deadline.
		remaining := time.Until(deadline)
		if remaining < pollInterval {
			time.Sleep(remaining) //nolint:forbidigo
		} else {
			time.Sleep(pollInterval) //nolint:forbidigo
		}
	}
}
