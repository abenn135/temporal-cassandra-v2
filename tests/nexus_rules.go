package tests

import (
	"fmt"

	"go.temporal.io/server/tests/testcore/umpire"
)

// nexusEventCausalityRule checks that every task completion/failure
// has a prior start task event for the same operation.
type nexusEventCausalityRule struct{}

func (r *nexusEventCausalityRule) Name() string { return "nexus-event-causality" }

func (r *nexusEventCausalityRule) Check(history []*umpire.Record) []umpire.Violation {
	startSeen := make(map[string]bool)
	var violations []umpire.Violation
	for _, rec := range history {
		switch rec.Fact.(type) {
		case *nexusStartTask:
			startSeen[rec.Fact.Key()] = true
		case *nexusTaskCompleted, *nexusTaskFailed:
			if !startSeen[rec.Fact.Key()] {
				violations = append(violations, umpire.Violation{
					Rule:    r.Name(),
					Message: fmt.Sprintf("%T without prior start task", rec.Fact),
					Tags:    map[string]string{"operationID": rec.Fact.Key()},
				})
			}
		}
	}
	return violations
}

// nexusCompletedOpConsistencyRule checks that operations the model considers
// completed have at least one task completion event in the traffic history.
type nexusCompletedOpConsistencyRule struct {
	ops func() []*modelOp
}

func (r *nexusCompletedOpConsistencyRule) Name() string { return "nexus-completed-op-consistency" }

func (r *nexusCompletedOpConsistencyRule) Check(history []*umpire.Record) []umpire.Violation {
	completions := make(map[string]bool)
	for _, rec := range history {
		if _, ok := rec.Fact.(*nexusTaskCompleted); ok {
			completions[rec.Fact.Key()] = true
		}
	}
	var violations []umpire.Violation
	for _, op := range r.ops() {
		if op.status == modelStatusCompleted && !completions[op.operationID] {
			violations = append(violations, umpire.Violation{
				Rule:    r.Name(),
				Message: "operation completed in model but no task completion observed",
				Tags:    map[string]string{"operationID": op.operationID},
			})
		}
	}
	return violations
}

// nexusAtMostOneCompletionRule checks that no operation has more than one
// task-completed event. Duplicate completions indicate an idempotency bug.
type nexusAtMostOneCompletionRule struct{}

func (r *nexusAtMostOneCompletionRule) Name() string { return "nexus-at-most-one-completion" }

func (r *nexusAtMostOneCompletionRule) Check(history []*umpire.Record) []umpire.Violation {
	counts := make(map[string]int)
	var violations []umpire.Violation
	for _, rec := range history {
		if _, ok := rec.Fact.(*nexusTaskCompleted); ok {
			counts[rec.Fact.Key()]++
			if counts[rec.Fact.Key()] > 1 {
				violations = append(violations, umpire.Violation{
					Rule:    r.Name(),
					Message: fmt.Sprintf("operation has %d task completions", counts[rec.Fact.Key()]),
					Tags:    map[string]string{"operationID": rec.Fact.Key()},
				})
			}
		}
	}
	return violations
}

// nexusNoPostTerminalTasksRule checks that no start task events appear for an
// operation after its last completion/failure event.
type nexusNoPostTerminalTasksRule struct {
	ops func() []*modelOp
}

func (r *nexusNoPostTerminalTasksRule) Name() string { return "nexus-no-post-terminal-tasks" }

func (r *nexusNoPostTerminalTasksRule) Check(history []*umpire.Record) []umpire.Violation {
	// Find the last completion/failure seq for each operation.
	lastTerminalSeq := make(map[string]int64)
	for _, rec := range history {
		switch rec.Fact.(type) {
		case *nexusTaskCompleted, *nexusTaskFailed:
			if rec.Seq > lastTerminalSeq[rec.Fact.Key()] {
				lastTerminalSeq[rec.Fact.Key()] = rec.Seq
			}
		}
	}

	var violations []umpire.Violation
	for _, rec := range history {
		if _, ok := rec.Fact.(*nexusStartTask); !ok {
			continue
		}
		termSeq, hasTerminal := lastTerminalSeq[rec.Fact.Key()]
		if !hasTerminal || rec.Seq <= termSeq {
			continue
		}
		// Only flag if the model also considers the op terminal.
		for _, op := range r.ops() {
			if op.operationID == rec.Fact.Key() && isTerminalStatus(op.status) {
				violations = append(violations, umpire.Violation{
					Rule:    r.Name(),
					Message: "start task after operation reached terminal state",
					Tags: map[string]string{
						"operationID": rec.Fact.Key(),
						"eventSeq":    fmt.Sprintf("%d", rec.Seq),
						"terminalSeq": fmt.Sprintf("%d", termSeq),
					},
				})
			}
		}
	}
	return violations
}

// nexusRunningOpsGetTasksRule is a liveness rule that checks every operation
// that was started eventually received at least one start task event.
type nexusRunningOpsGetTasksRule struct {
	ops func() []*modelOp
}

func (r *nexusRunningOpsGetTasksRule) Name() string { return "nexus-running-ops-get-tasks" }

func (r *nexusRunningOpsGetTasksRule) Check(history []*umpire.Record, final bool) []umpire.Violation {
	if !final {
		return nil
	}
	startSeen := make(map[string]bool)
	for _, rec := range history {
		if _, ok := rec.Fact.(*nexusStartTask); ok {
			startSeen[rec.Fact.Key()] = true
		}
	}
	var violations []umpire.Violation
	for _, op := range r.ops() {
		if !startSeen[op.operationID] {
			violations = append(violations, umpire.Violation{
				Rule:    r.Name(),
				Message: "operation never received a start task event",
				Tags: map[string]string{
					"operationID": op.operationID,
					"status":      fmt.Sprintf("%d", op.status),
				},
			})
		}
	}
	return violations
}

func isTerminalStatus(s modelOpStatus) bool {
	return s == modelStatusCompleted || s == modelStatusFailed || s == modelStatusTerminated
}

// RegisterRules is discovered by umpire.Model and called automatically at the
// start of each rapid iteration.
func (m *nexusPropModel) RegisterRules() {
	m.Umpire.AddRule(&nexusEventCausalityRule{})
	m.Umpire.AddRule(&nexusCompletedOpConsistencyRule{ops: m.sortedOps})
	m.Umpire.AddRule(&nexusAtMostOneCompletionRule{})
	m.Umpire.AddRule(&nexusNoPostTerminalTasksRule{ops: m.sortedOps})
	m.Umpire.AddRule(&nexusRunningOpsGetTasksRule{ops: m.sortedOps})
}
