package umpire

import "pgregory.net/rapid"

// T is the test context for property-based test actions.
type T struct {
	raw *rapid.T
}

func newT(t *rapid.T) *T {
	return &T{raw: t}
}

func (t *T) Helper() {
	t.raw.Helper()
}

func (t *T) Skip(args ...any) {
	t.raw.Skip(args...)
}

func (t *T) Skipf(format string, args ...any) {
	t.raw.Skipf(format, args...)
}

func (t *T) Logf(format string, args ...any) {
	t.raw.Logf(format, args...)
}

func (t *T) Errorf(format string, args ...any) {
	t.raw.Errorf(format, args...)
}

func (t *T) FailNow() {
	t.raw.FailNow()
}

func (t *T) Fatalf(format string, args ...any) {
	t.raw.Fatalf(format, args...)
}
