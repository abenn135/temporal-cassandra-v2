package umpire

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// Model is a generic, embeddable base for property-based test models using rapid.
// The type parameter M is the concrete model type that embeds Model.
//
// The Run method discovers actions via reflection on M using these conventions:
//   - Methods named Do* with signature func() are state machine actions.
//   - A method named CheckInvariant with signature func() is the invariant
//     checker (registered as the unnamed "" action in rapid.T.Repeat).
//
// Actions access the current *rapid.T via m.T().
type Model[M any] struct {
	Umpire *Umpire
	self   M
	t      *rapid.T
}

// NewModel creates a Model backed by the given umpire.
// self is a reference to the embedding struct (needed for action discovery).
func NewModel[M any](u *Umpire, self M) Model[M] {
	return Model[M]{Umpire: u, self: self}
}

// T returns the current rapid.T for the active action.
// Use this for require assertions and t.Skip() in Do*/CheckInvariant methods.
func (m *Model[M]) T() *rapid.T {
	return m.t
}

// CheckRules runs all registered umpire rules and fails on any violations.
func (m *Model[M]) CheckRules() {
	m.t.Helper()
	violations := m.Umpire.CheckRules(false)
	require.Empty(m.t, violations, "property violations: %v", violations)
}

// Run discovers Do*, CheckInvariant, and Cleanup methods on the concrete model
// and runs the state machine via t.Repeat. Cleanup is called via defer after
// each rapid iteration. Umpire.Reset is called automatically before Cleanup.
//
// Required methods on M:
//   - CheckInvariant()  — invariant checker (run as the unnamed "" rapid action)
//   - Cleanup()         — teardown after each iteration
//
// Optional methods on M:
//   - RegisterRules()   — called once at the start to register umpire rules
func (m *Model[M]) Run(t *rapid.T) {
	t.Helper()
	actions, lifecycle := m.discoverActions()
	if _, ok := actions[""]; !ok {
		t.Fatalf("model %T must have a CheckInvariant() method", m.self)
	}
	if lifecycle.cleanup == nil {
		t.Fatalf("model %T must have a Cleanup() method", m.self)
	}
	m.Umpire.Reset()
	m.t = t
	if lifecycle.registerRules != nil {
		lifecycle.registerRules()
	}
	defer func() {
		m.t = t
		lifecycle.cleanup()
		m.Umpire.Reset()
	}()
	t.Repeat(actions)
}

type modelLifecycle struct {
	cleanup       func()
	registerRules func()
}

// discoverActions reflects on self to find Do* actions and lifecycle methods.
func (m *Model[M]) discoverActions() (actions map[string]func(*rapid.T), lifecycle modelLifecycle) {
	val := reflect.ValueOf(m.self)
	typ := val.Type()

	actions = make(map[string]func(*rapid.T))
	for i := range typ.NumMethod() {
		method := typ.Method(i)
		// Must be func(receiver) with no params and no return values.
		if method.Type.NumIn() != 1 || method.Type.NumOut() != 0 {
			continue
		}

		fn := val.Method(i)

		switch {
		case strings.HasPrefix(method.Name, "Do"):
			actions[method.Name] = func(t *rapid.T) {
				m.t = t
				fn.Call(nil)
			}
		case method.Name == "CheckInvariant":
			actions[""] = func(t *rapid.T) {
				m.t = t
				fn.Call(nil)
			}
		case method.Name == "Cleanup":
			lifecycle.cleanup = func() { fn.Call(nil) }
		case method.Name == "RegisterRules":
			lifecycle.registerRules = func() { fn.Call(nil) }
		}
	}
	return actions, lifecycle
}

// T is the test context for property-based test actions.
type T = rapid.T

// Check runs a property-based test. The testFn receives a test context
// and should set up the model and call m.Run(t).
func Check(t *testing.T, testFn func(t *T)) {
	t.Helper()
	rapid.Check(t, testFn)
}

// Draw randomly selects an element from the slice using the rapid generator.
// Skips the test step if the slice is empty.
func Draw[T any](rt *rapid.T, label string, items []T) T {
	rt.Helper()
	if len(items) == 0 {
		rt.Skip("no items for " + label)
	}
	return rapid.SampledFrom(items).Draw(rt, label)
}
