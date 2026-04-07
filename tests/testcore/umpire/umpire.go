package umpire

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
)

// Umpire records traffic facts and supports rule-based property checking.
type Umpire struct {
	mu            sync.Mutex
	nextSeq       int64
	history       []*Record
	safetyRules   []SafetyRule
	livenessRules []LivenessRule
}

// TrafficObserver is implemented by model types that define how gRPC traffic
// is converted into umpire facts. The method receives the Umpire for recording
// facts and the standard gRPC observer arguments. It is called on a zero value
// during setup — it must not access model instance fields.
type TrafficObserver interface {
	ObserveTraffic(u *Umpire, ctx context.Context, method string, req, resp any, err error)
}

// New creates an Umpire with a server-side interceptor for the given model
// type M. M must implement TrafficObserver. Returns the Umpire and the gRPC
// interceptor to pass to testcore.WithServerInterceptor.
func New[M TrafficObserver]() (*Umpire, grpc.UnaryServerInterceptor) {
	var zero M
	u := &Umpire{}
	i := NewInterceptor(nil, func(ctx context.Context, method string, req, resp any, err error) {
		zero.ObserveTraffic(u, ctx, method, req, resp, err)
	})
	return u, i.UnaryServerInterceptor()
}

// Record adds a fact to the history.
func (u *Umpire) Record(fact Fact) *Record {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.nextSeq++
	r := &Record{Seq: u.nextSeq, Fact: fact}
	u.history = append(u.history, r)
	return r
}

// AddRule registers a rule. The rule must implement SafetyRule or LivenessRule.
func (u *Umpire) AddRule(rule any) {
	u.mu.Lock()
	defer u.mu.Unlock()
	switch r := rule.(type) {
	case SafetyRule:
		u.safetyRules = append(u.safetyRules, r)
	case LivenessRule:
		u.livenessRules = append(u.livenessRules, r)
	default:
		panic(fmt.Sprintf("umpire: rule %T must implement SafetyRule or LivenessRule", rule))
	}
}

// CheckRules runs all registered rules against the history.
// Safety rules are checked first, then liveness rules.
// When final is true, unresolved liveness conditions become violations.
func (u *Umpire) CheckRules(final bool) []Violation {
	u.mu.Lock()
	history := make([]*Record, len(u.history))
	copy(history, u.history)
	safetyRules := u.safetyRules
	livenessRules := u.livenessRules
	u.mu.Unlock()

	var violations []Violation
	for _, rule := range safetyRules {
		violations = append(violations, rule.Check(history)...)
	}
	for _, rule := range livenessRules {
		violations = append(violations, rule.Check(history, final)...)
	}
	return violations
}

// Reset clears all state: history and registered rules.
func (u *Umpire) Reset() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.history = nil
	u.nextSeq = 0
	u.safetyRules = nil
	u.livenessRules = nil
}

// History returns a snapshot of all records.
func (u *Umpire) History() []*Record {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]*Record, len(u.history))
	copy(out, u.history)
	return out
}

func (u *Umpire) String() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return fmt.Sprintf("Umpire{records=%d, safety=%d, liveness=%d}",
		len(u.history), len(u.safetyRules), len(u.livenessRules))
}
