package tests

import (
	"fmt"

	"go.temporal.io/server/tests/testcore/umpire"
)

type nexusTaskCausalityRule struct{}

func (r *nexusTaskCausalityRule) Name() string { return "nexus-task-causality" }

func (r *nexusTaskCausalityRule) Check(history []*umpire.Record) []umpire.Violation {
	dispatched := make(map[string]map[nexusTaskKind]bool)
	var violations []umpire.Violation

	for _, rec := range history {
		event, ok := rec.Fact.(*nexusTaskEvent)
		if !ok {
			continue
		}
		if event.Outcome == nexusTaskOutcomeDispatched {
			if dispatched[event.OperationID] == nil {
				dispatched[event.OperationID] = make(map[nexusTaskKind]bool)
			}
			dispatched[event.OperationID][event.Kind] = true
			continue
		}
		if dispatched[event.OperationID][event.Kind] {
			continue
		}
		violations = append(violations, umpire.Violation{
			Rule:    r.Name(),
			Message: fmt.Sprintf("%s %s without prior dispatch", event.Kind, event.Outcome),
			Tags: map[string]string{
				"operationID": event.OperationID,
				"kind":        string(event.Kind),
				"outcome":     string(event.Outcome),
			},
		})
	}

	return violations
}

type nexusTerminalConsistencyRule struct{}

func (r *nexusTerminalConsistencyRule) Name() string { return "nexus-terminal-consistency" }

func (r *nexusTerminalConsistencyRule) Check(history []*umpire.Record) []umpire.Violation {
	startCompleted := make(map[string]bool)
	cancelCompleted := make(map[string]bool)
	terminalStatus := make(map[string]modelOpStatus)

	for _, rec := range history {
		switch event := rec.Fact.(type) {
		case *nexusTaskEvent:
			if event.Outcome != nexusTaskOutcomeCompleted {
				continue
			}
			switch event.Kind {
			case nexusTaskKindStart:
				startCompleted[event.OperationID] = true
			case nexusTaskKindCancel:
				cancelCompleted[event.OperationID] = true
			}
		case *umpire.Transition[modelOpStatus]:
			if isTerminalStatus(event.To) {
				terminalStatus[event.EntityID] = event.To
			}
		}
	}

	var violations []umpire.Violation
	for operationID, status := range terminalStatus {
		switch status {
		case modelStatusCompleted:
			if startCompleted[operationID] {
				continue
			}
			violations = append(violations, umpire.Violation{
				Rule:    r.Name(),
				Message: "operation completed in model but no start completion observed",
				Tags:    map[string]string{"operationID": operationID},
			})
		case modelStatusCanceled:
			if cancelCompleted[operationID] {
				continue
			}
			violations = append(violations, umpire.Violation{
				Rule:    r.Name(),
				Message: "operation canceled in model but no cancel completion observed",
				Tags:    map[string]string{"operationID": operationID},
			})
		default:
		}
	}

	return violations
}

type nexusNoPostTerminalDispatchRule struct{}

func (r *nexusNoPostTerminalDispatchRule) Name() string { return "nexus-no-post-terminal-dispatch" }

func (r *nexusNoPostTerminalDispatchRule) Check(history []*umpire.Record) []umpire.Violation {
	lastTerminalSeq := umpire.LastTransitionSeqTo(history, isTerminalStatus)

	var violations []umpire.Violation
	for _, rec := range history {
		event, ok := rec.Fact.(*nexusTaskEvent)
		if !ok || event.Outcome != nexusTaskOutcomeDispatched {
			continue
		}
		termSeq, hasTerminal := lastTerminalSeq[event.OperationID]
		if !hasTerminal || rec.Seq <= termSeq {
			continue
		}
		violations = append(violations, umpire.Violation{
			Rule:    r.Name(),
			Message: "task dispatched after operation reached terminal state",
			Tags: map[string]string{
				"operationID": event.OperationID,
				"kind":        string(event.Kind),
				"eventSeq":    fmt.Sprintf("%d", rec.Seq),
				"terminalSeq": fmt.Sprintf("%d", termSeq),
			},
		})
	}

	return violations
}

func isTerminalStatus(s modelOpStatus) bool {
	return s == modelStatusCompleted || s == modelStatusCanceled || s == modelStatusFailed || s == modelStatusTerminated
}

func (m *nexusPropModel) RegisterRules() {
	m.Umpire.AddRule(&nexusTaskCausalityRule{})
	m.Umpire.AddRule(&nexusTerminalConsistencyRule{})
	m.Umpire.AddRule(&nexusNoPostTerminalDispatchRule{})
}
