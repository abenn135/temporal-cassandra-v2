package tests

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/testcore/umpire"
)

type nexusTaskKind string

const (
	nexusTaskKindStart  nexusTaskKind = "start"
	nexusTaskKindCancel nexusTaskKind = "cancel"
)

type nexusTaskOutcome string

const (
	nexusTaskOutcomeDispatched nexusTaskOutcome = "dispatched"
	nexusTaskOutcomeCompleted  nexusTaskOutcome = "completed"
	nexusTaskOutcomeFailed     nexusTaskOutcome = "failed"
)

type nexusTaskEvent struct {
	OperationID string
	Kind        nexusTaskKind
	Outcome     nexusTaskOutcome
}

func (e *nexusTaskEvent) Key() string { return e.OperationID }

type modelOpStatus int

const (
	modelStatusRunning modelOpStatus = iota
	modelStatusCompleted
	modelStatusCanceled
	modelStatusFailed
	modelStatusTerminated
)

type modelOp struct {
	operationID     string
	runID           string
	status          modelOpStatus
	cancelRequested bool
}

type nexusPropModel struct {
	umpire.Model[*nexusPropModel]
	env          *NexusTestEnv
	endpointName string
	taskQueue    string
	prefix       string
	ops          map[string]*modelOp
	nextOpSeq    int
	tasks        chan *workflowservice.PollNexusTaskQueueResponse
	cancelPoller context.CancelFunc
}

var propIterCounter atomic.Int64

func TestNexusStandaloneProp(t *testing.T) {
	t.Parallel()

	u := &umpire.Umpire{}

	env := newNexusTestEnv(t, false, nexusStandaloneOpts...)
	taskQueue := "prop-test-tq"
	endpointName := env.createNexusEndpoint(t, testcore.RandomizedNexusEndpoint(t.Name()), taskQueue).Spec.Name

	umpire.Check(t, func(t *umpire.T) {
		ctx, cancel := context.WithCancel(env.Context())
		m := &nexusPropModel{
			env:          env,
			endpointName: endpointName,
			taskQueue:    taskQueue,
			prefix:       fmt.Sprintf("r%d", propIterCounter.Add(1)),
			ops:          make(map[string]*modelOp),
			tasks:        make(chan *workflowservice.PollNexusTaskQueueResponse, 10),
			cancelPoller: cancel,
		}
		m.Model = umpire.NewModel(u, m)
		m.startPoller(ctx)
		m.Run(t)
	})
}

func (m *nexusPropModel) DoStartOperation(t *umpire.T) {
	opID := fmt.Sprintf("%s-op-%d", m.prefix, m.nextOpSeq)
	m.nextOpSeq++

	resp, err := startNexusOperation(m.env.TestEnv, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: opID,
		RequestId:   opID,
		Endpoint:    m.endpointName,
	})
	require.NoError(t, err)
	require.True(t, resp.GetStarted(), "expected operation %s to start", opID)

	m.ops[opID] = &modelOp{
		operationID: opID,
		runID:       resp.RunId,
		status:      modelStatusRunning,
	}
}

// DoPollAndComplete polls for a nexus task and responds with success.
func (m *nexusPropModel) DoPollAndComplete(t *umpire.T) {
	resp := m.pollTask(t)

	if start := resp.GetRequest().GetStartOperation(); start != nil {
		op := m.ops[start.GetRequestId()]
		require.NotNil(t, op, "unknown operation %s", start.GetRequestId())
		_, err := m.env.FrontendClient().RespondNexusTaskCompleted(m.env.Context(), &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: m.env.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: resp.TaskToken,
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponse_Sync{
								Payload: &commonpb.Payload{},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		m.recordTask(start.GetRequestId(), nexusTaskKindStart, nexusTaskOutcomeCompleted)
		m.setOpStatus(op, modelStatusCompleted)
		return
	}

	if cancel := resp.GetRequest().GetCancelOperation(); cancel != nil {
		op := m.ops[cancel.GetOperationToken()]
		require.NotNil(t, op, "unknown operation %s", cancel.GetOperationToken())
		_, err := m.env.FrontendClient().RespondNexusTaskCompleted(m.env.Context(), &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: m.env.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: resp.TaskToken,
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_CancelOperation{
					CancelOperation: &nexuspb.CancelOperationResponse{},
				},
			},
		})
		require.NoError(t, err)
		m.recordTask(cancel.GetOperationToken(), nexusTaskKindCancel, nexusTaskOutcomeCompleted)
		m.setOpStatus(op, modelStatusCanceled)
	}
}

// DoPollAndFail polls for a nexus task and responds with a retryable handler error.
func (m *nexusPropModel) DoPollAndFail(t *umpire.T) {
	resp := m.pollTask(t)

	if start := resp.GetRequest().GetStartOperation(); start != nil {
		m.recordTask(start.GetRequestId(), nexusTaskKindStart, nexusTaskOutcomeFailed)
	} else if cancel := resp.GetRequest().GetCancelOperation(); cancel != nil {
		m.recordTask(cancel.GetOperationToken(), nexusTaskKindCancel, nexusTaskOutcomeFailed)
	}
	require.NoError(t, m.env.respondNexusTaskFailed(m.env.Context(), resp.TaskToken, &nexus.HandlerError{
		Type:          nexus.HandlerErrorTypeInternal,
		RetryBehavior: nexus.HandlerErrorRetryBehaviorRetryable,
		Message:       "prop-test retryable handler error",
	}))
}

func (m *nexusPropModel) DoTerminateOperation(t *umpire.T) {
	op := m.pickRunningOp(t)

	_, err := m.env.FrontendClient().TerminateNexusOperationExecution(
		m.env.Context(),
		&workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   m.env.Namespace().String(),
			OperationId: op.operationID,
			RunId:       op.runID,
			RequestId:   fmt.Sprintf("term-%s", op.operationID),
			Reason:      "prop-test termination",
		},
	)
	require.NoError(t, err)
	m.setOpStatus(op, modelStatusTerminated)
}

func (m *nexusPropModel) DoCancelOperation(t *umpire.T) {
	op := m.pickRunningOp(t)
	if op.cancelRequested {
		t.Skip("already cancel-requested")
	}

	_, err := m.env.FrontendClient().RequestCancelNexusOperationExecution(
		m.env.Context(),
		&workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   m.env.Namespace().String(),
			OperationId: op.operationID,
			RunId:       op.runID,
			RequestId:   fmt.Sprintf("cancel-%s", op.operationID),
		},
	)
	require.NoError(t, err)
	op.cancelRequested = true
}

func (m *nexusPropModel) DoDescribeOperation(t *umpire.T) {
	op := m.pickAnyOp(t)

	resp, err := m.env.FrontendClient().DescribeNexusOperationExecution(
		m.env.Context(),
		&workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   m.env.Namespace().String(),
			OperationId: op.operationID,
			RunId:       op.runID,
		},
	)
	if m.handleDeletedOp(err, op) {
		return
	}
	require.NoError(t, err)
	require.Equal(t, toProtoStatus(op.status), resp.GetInfo().GetStatus(),
		"operation %s: expected %v, got %v", op.operationID, toProtoStatus(op.status), resp.GetInfo().GetStatus())
	require.Equal(t, op.runID, resp.RunId, "operation %s: run ID mismatch", op.operationID)
}

func (m *nexusPropModel) CheckInvariant(t *umpire.T) {
	m.CheckRules()

	// Check server state matches model.
	for _, op := range m.sortedOps() {
		resp, err := m.env.FrontendClient().DescribeNexusOperationExecution(
			m.env.Context(),
			&workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   m.env.Namespace().String(),
				OperationId: op.operationID,
				RunId:       op.runID,
			},
		)
		if m.handleDeletedOp(err, op) {
			continue
		}
		require.NoError(t, err)
		require.Equal(t, toProtoStatus(op.status), resp.GetInfo().GetStatus(),
			"invariant: operation %s status mismatch", op.operationID)
	}
}

func (m *nexusPropModel) recordTask(operationID string, kind nexusTaskKind, outcome nexusTaskOutcome) {
	m.Umpire.Record(&nexusTaskEvent{
		OperationID: operationID,
		Kind:        kind,
		Outcome:     outcome,
	})
}

func (m *nexusPropModel) setOpStatus(op *modelOp, to modelOpStatus) {
	umpire.RecordTransition(m.Umpire, op.operationID, &op.status, to)
}

// startPoller runs a background goroutine that continuously polls for nexus
// tasks and queues them for task-processing actions.
func (m *nexusPropModel) startPoller(ctx context.Context) {
	go func() {
		for ctx.Err() == nil {
			pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			resp, err := m.env.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: m.env.Namespace().String(),
				Identity:  uuid.NewString(),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: m.taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			})
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err != nil || resp.GetTaskToken() == nil {
				continue
			}
			if start := resp.GetRequest().GetStartOperation(); start != nil {
				m.recordTask(start.GetRequestId(), nexusTaskKindStart, nexusTaskOutcomeDispatched)
			} else if cancelReq := resp.GetRequest().GetCancelOperation(); cancelReq != nil {
				m.recordTask(cancelReq.GetOperationToken(), nexusTaskKindCancel, nexusTaskOutcomeDispatched)
			}
			m.tasks <- resp
		}
	}()
}

// pollTask waits briefly for a task from the background poller.
func (m *nexusPropModel) pollTask(t *umpire.T) *workflowservice.PollNexusTaskQueueResponse {
	select {
	case resp := <-m.tasks:
		return resp
	case <-time.After(2 * time.Second):
		t.Skip("no task available")
		return nil
	}
}

func (m *nexusPropModel) Cleanup(t *umpire.T) {
	m.cancelPoller()
	for _, op := range m.sortedOps() {
		// Terminate running ops first.
		if op.status == modelStatusRunning {
			_, _ = m.env.FrontendClient().TerminateNexusOperationExecution(
				m.env.Context(),
				&workflowservice.TerminateNexusOperationExecutionRequest{
					Namespace:   m.env.Namespace().String(),
					OperationId: op.operationID,
					RunId:       op.runID,
					RequestId:   fmt.Sprintf("cleanup-term-%s", op.operationID),
					Reason:      "prop-test cleanup",
				},
			)
		}
		// Delete all ops (idempotent for already-deleted).
		require.Eventually(t, func() bool {
			_, _ = m.env.FrontendClient().DeleteNexusOperationExecution(
				m.env.Context(),
				&workflowservice.DeleteNexusOperationExecutionRequest{
					Namespace:   m.env.Namespace().String(),
					OperationId: op.operationID,
					RunId:       op.runID,
				},
			)
			_, err := m.env.FrontendClient().DescribeNexusOperationExecution(
				m.env.Context(),
				&workflowservice.DescribeNexusOperationExecutionRequest{
					Namespace:   m.env.Namespace().String(),
					OperationId: op.operationID,
					RunId:       op.runID,
				},
			)
			var notFoundErr *serviceerror.NotFound
			return errors.As(err, &notFoundErr)
		}, 10*time.Second, 200*time.Millisecond, "operation %s was not deleted during cleanup", op.operationID)
	}
}

func (m *nexusPropModel) handleDeletedOp(err error, op *modelOp) bool {
	if op.status != modelStatusTerminated &&
		op.status != modelStatusCompleted &&
		op.status != modelStatusCanceled &&
		op.status != modelStatusFailed {
		return false
	}
	var notFoundErr *serviceerror.NotFound
	if !errors.As(err, &notFoundErr) {
		return false
	}
	delete(m.ops, op.operationID)
	return true
}

func (m *nexusPropModel) pickRunningOp(t *umpire.T) *modelOp {
	var running []*modelOp
	for _, op := range m.ops {
		if op.status == modelStatusRunning {
			running = append(running, op)
		}
	}
	if len(running) == 0 {
		t.Skip("no running operations")
	}
	sort.Slice(running, func(i, j int) bool {
		return running[i].operationID < running[j].operationID
	})
	return umpire.Draw(t, "runningOp", running)
}

func (m *nexusPropModel) pickAnyOp(t *umpire.T) *modelOp {
	ops := m.sortedOps()
	if len(ops) == 0 {
		t.Skip("no operations")
	}
	return umpire.Draw(t, "anyOp", ops)
}

func (m *nexusPropModel) sortedOps() []*modelOp {
	ops := make([]*modelOp, 0, len(m.ops))
	for _, op := range m.ops {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].operationID < ops[j].operationID
	})
	return ops
}

func toProtoStatus(s modelOpStatus) enumspb.NexusOperationExecutionStatus {
	switch s {
	case modelStatusRunning:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING
	case modelStatusCompleted:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED
	case modelStatusCanceled:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED
	case modelStatusFailed:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED
	case modelStatusTerminated:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED
	default:
		panic(fmt.Sprintf("unknown model status: %d", s))
	}
}
