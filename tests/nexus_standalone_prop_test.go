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
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/testcore/umpire"
)

// Nexus-specific typed facts for the umpire.

type nexusStartTask struct{ OperationID string }

func (f *nexusStartTask) Key() string { return f.OperationID }

type nexusCancelTask struct{ OperationID string }

func (f *nexusCancelTask) Key() string { return f.OperationID }

type nexusTaskCompleted struct{ OperationID string }

func (f *nexusTaskCompleted) Key() string { return f.OperationID }

type nexusTaskFailed struct{ OperationID string }

func (f *nexusTaskFailed) Key() string { return f.OperationID }

type modelOpStatus int

const (
	modelStatusRunning modelOpStatus = iota
	modelStatusCompleted
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

	u, icpt := umpire.New[*nexusPropModel]()

	env := newNexusTestEnv(t, false, append(nexusStandaloneOpts,
		testcore.WithServerInterceptor(icpt),
	)...)
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

// ObserveTraffic records poll responses as umpire facts. Called on a zero-value
// receiver during setup — must not access model fields. Completion/failure
// facts are recorded by the Do* actions which have the operation ID directly.
func (*nexusPropModel) ObserveTraffic(u *umpire.Umpire, _ context.Context, _ string, _, resp any, err error) {
	if err != nil {
		return
	}
	pollResp, ok := resp.(*workflowservice.PollNexusTaskQueueResponse)
	if !ok || pollResp.GetTaskToken() == nil {
		return
	}
	if start := pollResp.GetRequest().GetStartOperation(); start != nil {
		u.Record(&nexusStartTask{OperationID: start.GetRequestId()})
	} else if cancel := pollResp.GetRequest().GetCancelOperation(); cancel != nil {
		u.Record(&nexusCancelTask{OperationID: cancel.GetOperationToken()})
	}
}

func (m *nexusPropModel) DoStartOperation() {
	opID := fmt.Sprintf("%s-op-%d", m.prefix, m.nextOpSeq)
	m.nextOpSeq++

	resp, err := startNexusOperation(m.env.TestEnv, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: opID,
		RequestId:   opID,
		Endpoint:    m.endpointName,
	})
	require.NoError(m.T(), err)
	require.True(m.T(), resp.GetStarted())

	m.ops[opID] = &modelOp{
		operationID: opID,
		runID:       resp.RunId,
		status:      modelStatusRunning,
	}
}

// DoPollAndComplete polls for a nexus task and responds with success.
func (m *nexusPropModel) DoPollAndComplete() {
	resp := m.pollTask()
	if resp == nil {
		m.T().Skip("no task available")
	}

	if start := resp.GetRequest().GetStartOperation(); start != nil {
		op := m.ops[start.GetRequestId()]
		require.NotNil(m.T(), op, "unknown operation %s", start.GetRequestId())
		require.NoError(m.T(), m.env.respondNexusStartOperationSyncSuccess(
			m.env.Context(), resp.TaskToken, &commonpb.Payload{}, nil))
		m.Umpire.Record(&nexusTaskCompleted{OperationID: start.GetRequestId()})
		op.status = modelStatusCompleted
		return
	}

	if cancel := resp.GetRequest().GetCancelOperation(); cancel != nil {
		require.NoError(m.T(), m.env.respondNexusCancelOperationCompleted(
			m.env.Context(), resp.TaskToken))
		m.Umpire.Record(&nexusTaskCompleted{OperationID: cancel.GetOperationToken()})
	}
}

// DoPollAndFail polls for a nexus task and responds with a retryable handler error.
func (m *nexusPropModel) DoPollAndFail() {
	resp := m.pollTask()
	if resp == nil {
		m.T().Skip("no task available")
	}

	var opID string
	if start := resp.GetRequest().GetStartOperation(); start != nil {
		opID = start.GetRequestId()
	} else if cancel := resp.GetRequest().GetCancelOperation(); cancel != nil {
		opID = cancel.GetOperationToken()
	}
	require.NoError(m.T(), m.env.respondNexusTaskFailed(m.env.Context(), resp.TaskToken, &nexus.HandlerError{
		Type:          nexus.HandlerErrorTypeInternal,
		RetryBehavior: nexus.HandlerErrorRetryBehaviorRetryable,
		Message:       "prop-test retryable handler error",
	}))
	m.Umpire.Record(&nexusTaskFailed{OperationID: opID})
}

func (m *nexusPropModel) DoTerminateOperation() {
	op := m.pickRunningOp()

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
	require.NoError(m.T(), err)
	op.status = modelStatusTerminated
}

func (m *nexusPropModel) DoCancelOperation() {
	op := m.pickRunningOp()
	if op.cancelRequested {
		m.T().Skip("already cancel-requested")
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
	require.NoError(m.T(), err)
	op.cancelRequested = true
}

func (m *nexusPropModel) DoDescribeOperation() {
	op := m.pickAnyOp()

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
	require.NoError(m.T(), err)
	require.Equal(m.T(), toProtoStatus(op.status), resp.GetInfo().GetStatus(),
		"operation %s: expected %v, got %v", op.operationID, toProtoStatus(op.status), resp.GetInfo().GetStatus())
	require.Equal(m.T(), op.runID, resp.RunId)
}

func (m *nexusPropModel) CheckInvariant() {
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
		require.NoError(m.T(), err)
		require.Equal(m.T(), toProtoStatus(op.status), resp.GetInfo().GetStatus(),
			"invariant: operation %s status mismatch", op.operationID)
	}
}

// startPoller runs a background goroutine that continuously polls for nexus
// tasks and sends them to the tasks channel.
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
			if err != nil {
				continue
			}
			if resp.GetTaskToken() != nil {
				m.tasks <- resp
			}
		}
	}()
}

// pollTask returns the next available task from the background poller,
// or nil if none is available.
func (m *nexusPropModel) pollTask() *workflowservice.PollNexusTaskQueueResponse {
	select {
	case resp := <-m.tasks:
		return resp
	default:
		return nil
	}
}

func (m *nexusPropModel) handleDeletedOp(err error, op *modelOp) bool {
	if op.status != modelStatusTerminated && op.status != modelStatusCompleted && op.status != modelStatusFailed {
		return false
	}
	var notFoundErr *serviceerror.NotFound
	if !errors.As(err, &notFoundErr) {
		return false
	}
	delete(m.ops, op.operationID)
	return true
}

func (m *nexusPropModel) Cleanup() {
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
		require.Eventually(m.T(), func() bool {
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
		}, 10*time.Second, 200*time.Millisecond)
	}
}

func (m *nexusPropModel) hasRunningOps() bool {
	for _, op := range m.ops {
		if op.status == modelStatusRunning {
			return true
		}
	}
	return false
}

func (m *nexusPropModel) pickRunningOp() *modelOp {
	var running []*modelOp
	for _, op := range m.ops {
		if op.status == modelStatusRunning {
			running = append(running, op)
		}
	}
	if len(running) == 0 {
		m.T().Skip("no running operations")
	}
	sort.Slice(running, func(i, j int) bool {
		return running[i].operationID < running[j].operationID
	})
	return umpire.Draw(m.T(), "runningOp", running)
}

func (m *nexusPropModel) pickAnyOp() *modelOp {
	ops := m.sortedOps()
	if len(ops) == 0 {
		m.T().Skip("no operations")
	}
	return umpire.Draw(m.T(), "anyOp", ops)
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
	case modelStatusFailed:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED
	case modelStatusTerminated:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED
	default:
		panic(fmt.Sprintf("unknown model status: %d", s))
	}
}
