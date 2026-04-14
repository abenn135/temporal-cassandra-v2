package umpire

import (
	"pgregory.net/rapid"
	"reflect"
	"strings"
	"testing"
)

// Model is a generic, embeddable base for property-based test models using rapid.
// The type parameter M is the concrete model type that embeds Model.
//
// The Run method discovers actions via reflection on M using these conventions:
//   - Methods named Do* with signature func(*T) are state machine actions.
//   - A method named CheckInvariant with signature func(*T) is the invariant
//     checker (registered as the unnamed "" action in rapid.T.Repeat).
//
// Actions access the current test wrapper via m.T().
type Model[M any] struct {
	Umpire *Umpire
	self   M
	t      *T
}

// NewModel creates a Model backed by the given umpire.
// self is a reference to the embedding struct (needed for action discovery).
func NewModel[M any](u *Umpire, self M) Model[M] {
	return Model[M]{Umpire: u, self: self}
}

// T returns the current test wrapper for the active action.
func (m *Model[M]) T() *T {
	return m.t
}

// CheckRules runs all registered umpire rules and fails on any violations.
func (m *Model[M]) CheckRules() {
	m.t.Helper()
	violations := m.Umpire.CheckRules(false)
	if len(violations) != 0 {
		m.t.Fatalf("property violations: %v", violations)
	}
}

// Run discovers Do*, CheckInvariant, and Cleanup methods on the concrete model
// and runs the state machine via t.Repeat. Cleanup is called via defer after
// each rapid iteration. Umpire.Reset is called automatically before Cleanup.
//
// Required methods on M:
//   - CheckInvariant(*T)  — invariant checker (run as the unnamed "" rapid action)
//   - Cleanup(*T)         — teardown after each iteration
//
// Optional methods on M:
//   - RegisterRules()   — called once at the start to register umpire rules
func (m *Model[M]) Run(t *T) {
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
		lifecycle.cleanup(t)
		m.Umpire.Reset()
	}()
	t.raw.Repeat(actions)
}

type modelLifecycle struct {
	cleanup       func(*T)
	registerRules func()
}

// discoverActions reflects on self to find Do* actions and lifecycle methods.
func (m *Model[M]) discoverActions() (actions map[string]func(*rapid.T), lifecycle modelLifecycle) {
	val := reflect.ValueOf(m.self)
	typ := val.Type()
	tType := reflect.TypeOf((*T)(nil))

	actions = make(map[string]func(*rapid.T))
	for i := range typ.NumMethod() {
		method := typ.Method(i)
		fn := val.Method(i)

		switch {
		case strings.HasPrefix(method.Name, "Do"):
			if method.Type.NumIn() != 2 || method.Type.In(1) != tType || method.Type.NumOut() != 0 {
				continue
			}
			actions[method.Name] = func(t *rapid.T) {
				mt := newT(t)
				m.t = mt
				fn.Call([]reflect.Value{reflect.ValueOf(mt)})
			}
		case method.Name == "CheckInvariant":
			if method.Type.NumIn() != 2 || method.Type.In(1) != tType || method.Type.NumOut() != 0 {
				continue
			}
			actions[""] = func(t *rapid.T) {
				mt := newT(t)
				m.t = mt
				fn.Call([]reflect.Value{reflect.ValueOf(mt)})
			}
		case method.Name == "Cleanup":
			if method.Type.NumIn() != 2 || method.Type.In(1) != tType || method.Type.NumOut() != 0 {
				continue
			}
			lifecycle.cleanup = func(t *T) { fn.Call([]reflect.Value{reflect.ValueOf(t)}) }
		case method.Name == "RegisterRules":
			if method.Type.NumIn() != 1 || method.Type.NumOut() != 0 {
				continue
			}
			lifecycle.registerRules = func() { fn.Call(nil) }
		default:
		}
	}
	return actions, lifecycle
}

// Check runs a property-based test. The testFn receives a test context
// and should set up the model and call m.Run(t).
func Check(t *testing.T, testFn func(t *T)) {
	t.Helper()
	rapid.Check(t, func(rt *rapid.T) {
		testFn(newT(rt))
	})
}

// Draw randomly selects an element from the slice using the rapid generator.
// Skips the test step if the slice is empty.
func Draw[V any](t *T, label string, items []V) V {
	t.Helper()
	if len(items) == 0 {
		t.Skip("no items for " + label)
	}
	return rapid.SampledFrom(items).Draw(t.raw, label)
}
