package umpire

import (
	"fmt"
	"sort"
)

// Violation describes a rule violation found during checking.
type Violation struct {
	Rule    string
	Message string
	Tags    map[string]string
}

func (v Violation) String() string {
	s := fmt.Sprintf("[%s] %s", v.Rule, v.Message)
	if len(v.Tags) > 0 {
		keys := make([]string, 0, len(v.Tags))
		for k := range v.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		s += " {"
		for i, k := range keys {
			if i > 0 {
				s += ", "
			}
			s += fmt.Sprintf("%s=%s", k, v.Tags[k])
		}
		s += "}"
	}
	return s
}

// SafetyRule checks invariants that must hold at every observation point.
// Violations indicate immediate bugs.
type SafetyRule interface {
	Name() string
	// Check inspects the recorded history and returns any violations found.
	Check(history []*Record) []Violation
}

// LivenessRule tracks conditions that must eventually be satisfied.
// Implementations maintain their own pending/resolved state across calls.
type LivenessRule interface {
	Name() string
	// Check updates internal pending/resolved state based on the history.
	// When final is true, all unresolved pending conditions become violations.
	Check(history []*Record, final bool) []Violation
}
