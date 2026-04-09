package tests

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NexusStandaloneTestSuite struct {
	parallelsuite.Suite[*NexusStandaloneTestSuite]
}

func TestNexusStandaloneTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusStandaloneTestSuite{})
}

func (s *NexusStandaloneTestSuite) opts() []testcore.TestOption {
	return []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
	}
}

func (s *NexusStandaloneTestSuite) TestStartStandaloneNexusOperation() {
	s.Run("StartAndDescribe", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		testInput := payload.EncodeString("test-input")
		testHeader := map[string]string{"test-key": "test-value"}
		testUserMetadata := &sdkpb.UserMetadata{
			Summary: payload.EncodeString("test-summary"),
			Details: payload.EncodeString("test-details"),
		}
		testSearchAttributes := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("test-value"),
			},
		}
		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "test-op",
			Endpoint:         endpointName,
			Input:            testInput,
			NexusHeader:      testHeader,
			UserMetadata:     testUserMetadata,
			SearchAttributes: testSearchAttributes,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		for _, tc := range []struct {
			name  string
			runID string
		}{
			{name: "WithRunID", runID: startResp.RunId},
			{name: "WithEmptyRunID", runID: ""},
		} {
			descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: "test-op",
				RunId:       tc.runID,
			})
			s.NoError(err, tc.name)
			s.Equal(startResp.RunId, descResp.RunId, tc.name)

			info := descResp.GetInfo()
			protorequire.ProtoEqual(s.T(), &nexuspb.NexusOperationExecutionInfo{
				OperationId:            "test-op",
				RunId:                  startResp.RunId,
				Endpoint:               endpointName,
				Service:                "test-service",
				Operation:              "test-operation",
				Status:                 enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
				State:                  enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED,
				ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
				NexusHeader:            testHeader,
				UserMetadata:           testUserMetadata,
				SearchAttributes:       testSearchAttributes,
				Attempt:                1,
				StateTransitionCount:   1,
				// Dynamic fields copied from actual response for comparison.
				RequestId:         info.GetRequestId(),
				ScheduleTime:      info.GetScheduleTime(),
				ExpirationTime:    info.GetExpirationTime(),
				ExecutionDuration: info.GetExecutionDuration(),
			}, info)
			s.NotEmpty(descResp.GetLongPollToken(), tc.name)
			s.NotEmpty(info.GetRequestId(), tc.name)
			s.NotNil(info.GetScheduleTime(), tc.name)
			s.NotNil(info.GetExpirationTime(), tc.name)
			s.NotNil(info.GetExecutionDuration(), tc.name)
		}

		// Describe with IncludeInput.
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:    env.Namespace().String(),
			OperationId:  "test-op",
			RunId:        startResp.RunId,
			IncludeInput: true,
		})
		s.NoError(err)
		protorequire.ProtoEqual(s.T(), testInput, descResp.GetInput())
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})

	s.Run("IDConflictPolicyFail", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		resp1, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Second start with different request ID should fail.
		_, err = s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			RequestId:   "different-request-id",
		})
		s.Error(err)
		var alreadyStartedErr *serviceerror.AlreadyExists
		s.ErrorAs(err, &alreadyStartedErr)
		s.ErrorContains(err, "nexus operation execution already started")

		// Second start with same request ID should return existing run.
		resp2, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})

	s.Run("IDConflictPolicyUseExisting", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		resp1, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		resp2, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "test-op",
			Endpoint:         endpointName,
			RequestId:        "different-request-id",
			IdConflictPolicy: enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING,
		})
		s.NoError(err)
		s.Equal(resp1.RunId, resp2.RunId)
		s.False(resp2.GetStarted())
	})
}

func (s *NexusStandaloneTestSuite) TestDescribeStandaloneNexusOperation() {
	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "does-not-exist",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
		s.Equal("operation not found for ID: does-not-exist", notFound.Error())
	})

	s.Run("LongPollStateChange", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Obtain longpoll token.
		firstResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.NotEmpty(firstResp.GetLongPollToken())
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, firstResp.GetInfo().GetStatus())

		ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
		defer cancel()

		// Start polling.
		type describeResult struct {
			resp *workflowservice.DescribeNexusOperationExecutionResponse
			err  error
		}
		describeResultCh := make(chan describeResult, 1)

		go func() {
			resp, err := env.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
				Namespace:      env.Namespace().String(),
				OperationId:    "test-op",
				RunId:          startResp.RunId,
				IncludeOutcome: true,
				LongPollToken:  firstResp.GetLongPollToken(),
			})
			describeResultCh <- describeResult{resp: resp, err: err}
		}()

		// Wait 1s to ensure the poll is still active.
		select {
		case result := <-describeResultCh:
			s.NoError(result.err)
			s.T().Fatal("DescribeNexusOperationExecution returned before the state changed")
		case <-time.After(1 * time.Second):
		}

		// Terminate the operation.
		terminateErrCh := make(chan error, 1)
		go func() {
			_, err := env.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				Reason:      "test termination",
			})
			terminateErrCh <- err
		}()

		select {
		case err := <-terminateErrCh:
			s.NoError(err)
		case <-ctx.Done():
			s.T().Fatal("TerminateNexusOperationExecution timed out")
		}

		// Verify the longpoll result.
		var longPollResp *workflowservice.DescribeNexusOperationExecutionResponse
		select {
		case result := <-describeResultCh:
			s.NoError(result.err)
			longPollResp = result.resp
		case <-ctx.Done():
			s.T().Fatal("DescribeNexusOperationExecution timed out")
		}

		s.Equal(startResp.RunId, longPollResp.GetRunId())
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, longPollResp.GetInfo().GetStatus())
		s.Greater(longPollResp.GetInfo().GetStateTransitionCount(), firstResp.GetInfo().GetStateTransitionCount())
	})

	s.Run("LongPollTimeoutReturnsEmptyResponse", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		firstResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.NotEmpty(firstResp.GetLongPollToken())

		env.OverrideDynamicConfig(nexusoperation.LongPollBuffer, time.Second)
		env.OverrideDynamicConfig(nexusoperation.LongPollTimeout, 10*time.Millisecond)

		ctx, cancel := context.WithTimeout(env.Context(), 5*time.Second)
		defer cancel()

		longPollResp, err := env.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:     env.Namespace().String(),
			OperationId:   "test-op",
			RunId:         startResp.RunId,
			LongPollToken: firstResp.GetLongPollToken(),
		})
		s.NoError(err)
		protorequire.ProtoEqual(s.T(), &workflowservice.DescribeNexusOperationExecutionResponse{}, longPollResp)

		// Frontend still imposes its own deadline upstream, so the buffer must fit within that.
		env.OverrideDynamicConfig(nexusoperation.LongPollBuffer, 29*time.Second)
		env.OverrideDynamicConfig(nexusoperation.LongPollTimeout, 10*time.Millisecond)

		longPollResp, err = env.FrontendClient().DescribeNexusOperationExecution(context.Background(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:     env.Namespace().String(),
			OperationId:   "test-op",
			RunId:         startResp.RunId,
			LongPollToken: firstResp.GetLongPollToken(),
		})
		s.NoError(err)
		protorequire.ProtoEqual(s.T(), &workflowservice.DescribeNexusOperationExecutionResponse{}, longPollResp)
	})

	s.Run("IncludeOutcome_Failure", func(s *NexusStandaloneTestSuite) {
		// TODO: Add canceled last-attempt-failure coverage here once standalone cancellation tasks
		// can be completed through the public Nexus task APIs.

		testCases := []struct {
			name                   string
			setup                  func(*workflowservice.StartNexusOperationExecutionRequest)
			respond                func(context.Context, *NexusTestEnv, *workflowservice.PollNexusTaskQueueResponse) error
			expectedStatus         enumspb.NexusOperationExecutionStatus
			expectedFailureMessage string
		}{
			{
				name: "TimeoutLastAttemptFailure",
				setup: func(req *workflowservice.StartNexusOperationExecutionRequest) {
					req.ScheduleToCloseTimeout = durationpb.New(2 * time.Second)
				},
				respond: func(ctx context.Context, s *NexusTestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					_, err := s.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
						Namespace: s.Namespace().String(),
						Identity:  "test-worker",
						TaskToken: task.GetTaskToken(),
						Error: &nexuspb.HandlerError{
							ErrorType: string(nexus.HandlerErrorTypeInternal),
							Failure: &nexuspb.Failure{
								Message: "last attempt failure",
							},
						},
					})
					return err
				},
				expectedStatus:         enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT,
				expectedFailureMessage: "last attempt failure",
			},
			{
				name: "TerminalFailure",
				respond: func(ctx context.Context, s *NexusTestEnv, task *workflowservice.PollNexusTaskQueueResponse) error {
					_, err := s.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
						Namespace: s.Namespace().String(),
						Identity:  "test-worker",
						TaskToken: task.GetTaskToken(),
						Response: &nexuspb.Response{
							Variant: &nexuspb.Response_StartOperation{
								StartOperation: &nexuspb.StartOperationResponse{
									Variant: &nexuspb.StartOperationResponse_Failure{
										Failure: &failurepb.Failure{Message: "final failure"},
									},
								},
							},
						},
					})
					return err
				},
				expectedStatus:         enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED,
				expectedFailureMessage: "final failure",
			},
		}

		for _, tc := range testCases {
			s.Run(tc.name, func(s *NexusStandaloneTestSuite) {
				env := newNexusTestEnv(s.T(), false, s.opts()...)
				taskQueue := testcore.RandomizedNexusEndpoint(s.T().Name())
				endpointName := env.createNexusEndpoint(s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), taskQueue).GetSpec().GetName()
				startReq := &workflowservice.StartNexusOperationExecutionRequest{
					OperationId: "test-op",
					Endpoint:    endpointName,
				}
				if tc.setup != nil {
					tc.setup(startReq)
				}

				startResp, err := s.startNexusOperation(env, startReq)
				s.NoError(err)

				ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
				defer cancel()

				pollerErrCh := make(chan error, 1)
				go func() {
					task, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
						Namespace: env.Namespace().String(),
						Identity:  "test-worker",
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueue,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
					})
					if err != nil {
						pollerErrCh <- err
						return
					}
					pollerErrCh <- tc.respond(ctx, env, task)
				}()

				s.EventuallyWithT(func(t *assert.CollectT) {
					descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
						Namespace:      env.Namespace().String(),
						OperationId:    "test-op",
						RunId:          startResp.RunId,
						IncludeOutcome: true,
					})
					require.NoError(t, err)
					require.Equal(t, tc.expectedStatus, descResp.GetInfo().GetStatus())
					require.Equal(t, tc.expectedFailureMessage, descResp.GetFailure().GetMessage())

					pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
						Namespace:   env.Namespace().String(),
						OperationId: "test-op",
						RunId:       startResp.RunId,
						WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					})
					require.NoError(t, err)
					require.Equal(t, tc.expectedFailureMessage, pollResp.GetFailure().GetMessage())
				}, 10*time.Second, 100*time.Millisecond)

				s.NoError(<-pollerErrCh)
			})
		}
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace: env.Namespace().String(),
		})
		s.Error(err)
		s.ErrorContains(err, "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestStandaloneNexusOperationCancel() {
	s.Run("RequestCancel", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		// Verify state after cancel — operation is still running
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		protorequire.ProtoEqual(s.T(), &nexuspb.NexusOperationExecutionInfo{
			OperationId:            "test-op",
			RunId:                  startResp.RunId,
			Endpoint:               endpointName,
			Service:                "test-service",
			Operation:              "test-operation",
			Status:                 enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
			State:                  enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED,
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			NexusHeader:            map[string]string{},
			SearchAttributes:       &commonpb.SearchAttributes{},
			Attempt:                1,
			StateTransitionCount:   descResp.GetInfo().GetStateTransitionCount(),
			// Dynamic fields copied from actual response for comparison.
			RequestId:         descResp.GetInfo().GetRequestId(),
			ScheduleTime:      descResp.GetInfo().GetScheduleTime(),
			ExpirationTime:    descResp.GetInfo().GetExpirationTime(),
			ExecutionDuration: descResp.GetInfo().GetExecutionDuration(),
		}, descResp.GetInfo())
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, descResp.GetInfo().GetStatus())
	})

	// TODO: Enable once cancel is fully implemented.
	s.Run("AlreadyCanceled", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Cancel the operation.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "cancel-request-id",
		})
		s.NoError(err)

		// Cancel again with same request ID — should be idempotent.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "cancel-request-id",
		})
		s.NoError(err)

		// Cancel with a different request ID — should error.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "different-request-id",
		})
		s.Error(err)
		s.Contains(err.Error(), "cancellation already requested")
	})

	// TODO: Enable once cancel/terminate interaction is fully implemented for standalone Nexus operations.
	s.Run("AlreadyTerminated", func(s *NexusStandaloneTestSuite) {
		t := s.T()
		t.Skip("Cancel/terminate interaction not yet fully implemented for standalone Nexus operations")

		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Terminate the operation first.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Cancel a terminated operation — should error.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.Error(err)
		s.Contains(err.Error(), "operation already completed")
	})

	// TODO: Enable once cancel is fully implemented for standalone Nexus operations.
	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		t := s.T()
		t.Skip("Cancel not yet fully implemented for standalone Nexus operations")

		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "does-not-exist",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestTerminateStandaloneNexusOperation() {
	s.Run("Terminate", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Verify outcome after terminate.
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:      env.Namespace().String(),
			OperationId:    "test-op",
			RunId:          startResp.RunId,
			IncludeOutcome: true,
		})
		s.NoError(err)
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
		failure := descResp.GetFailure()
		s.NotNil(failure)
		s.Equal("test termination", failure.GetMessage())
		s.NotNil(failure.GetTerminatedFailureInfo())
	})

	s.Run("AlreadyTerminated", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Terminate the operation.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Terminate again with same request ID — should be idempotent.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination again",
		})
		s.NoError(err)

		// Terminate with a different request ID — should error.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "different-request-id",
			Reason:      "test termination different",
		})
		s.Error(err)
		s.Contains(err.Error(), "already terminated")
	})

	// TODO: Enable once terminate is fully implemented for standalone Nexus operations.
	s.Run("AlreadyCanceled", func(s *NexusStandaloneTestSuite) {
		t := s.T()
		t.Skip("Terminate not yet fully implemented for standalone Nexus operations")

		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Cancel the operation first.
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(env.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		// Terminate a canceled operation — should succeed.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			RequestId:   "terminate-request-id",
			Reason:      "test termination",
		})
		s.NoError(err)

		// Verify state changed to terminated (terminate overrides cancel request).
		descResp, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, descResp.GetInfo().GetStatus())
	})

	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "does-not-exist",
			Reason:      "test termination",
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound)
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "", // required field
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestListStandaloneNexusOperation() {
	s.Run("ListAndVerifyFields", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "list-test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "OperationId = 'list-test-op'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		op := listResp.GetOperations()[0]
		protorequire.ProtoEqual(s.T(), &nexuspb.NexusOperationExecutionListInfo{
			OperationId:          "list-test-op",
			RunId:                startResp.RunId,
			Endpoint:             endpointName,
			Service:              "test-service",
			Operation:            "test-operation",
			Status:               enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
			StateTransitionCount: op.GetStateTransitionCount(),
			SearchAttributes:     op.GetSearchAttributes(),
			// Dynamic fields copied from actual response for comparison.
			ScheduleTime: op.GetScheduleTime(),
		}, op)
		s.NotNil(op.GetScheduleTime())
	})

	s.Run("ListWithQueryFilter", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointA := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()
		endpointB := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "filter-op-1",
			Endpoint:    endpointA,
		})
		s.NoError(err)

		_, err = s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "filter-op-2",
			Endpoint:    endpointB,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s'", endpointA),
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		s.Equal("filter-op-1", listResp.GetOperations()[0].GetOperationId())
	})

	s.Run("ListWithCustomSearchAttributes", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		testSA := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("list-sa-value"),
			},
		}
		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:      "sa-op",
			Endpoint:         endpointName,
			SearchAttributes: testSA,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "CustomKeywordField = 'list-sa-value'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		s.Equal("sa-op", listResp.GetOperations()[0].GetOperationId())
		returnedSA := listResp.GetOperations()[0].GetSearchAttributes().GetIndexedFields()["CustomKeywordField"]
		s.NotNil(returnedSA)
		var returnedValue string
		s.NoError(payload.Decode(returnedSA, &returnedValue))
		s.Equal("list-sa-value", returnedValue)
	})

	s.Run("QueryByExecutionStatus", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "status-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "ExecutionStatus = 'Running' AND OperationId = 'status-op'",
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		s.Equal("status-op", listResp.GetOperations()[0].GetOperationId())
	})

	s.Run("QueryByMultipleFields", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "multi-op",
			Endpoint:    endpointName,
			Service:     "multi-service",
		})
		s.NoError(err)

		var listResp *workflowservice.ListNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			listResp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s' AND Service = 'multi-service'", endpointName),
			})
			require.NoError(t, err)
			require.Len(t, listResp.GetOperations(), 1)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		s.Equal("multi-op", listResp.GetOperations()[0].GetOperationId())
	})

	s.Run("PageSizeCapping", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		for i := range 2 {
			_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("paged-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		// Wait for both to be indexed.
		query := fmt.Sprintf("Endpoint = '%s'", endpointName)
		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     query,
			})
			require.NoError(t, err)
			require.Len(t, resp.GetOperations(), 2)
		}, testcore.WaitForESToSettle, 100*time.Millisecond)

		// Override max page size to 1.
		env.OverrideDynamicConfig(dynamicconfig.FrontendVisibilityMaxPageSize, 1)

		// PageSize 0 should default to max (1), returning only 1 result.
		resp, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  0,
			Query:     query,
		})
		s.NoError(err)
		s.Len(resp.GetOperations(), 1)

		// PageSize > max should also be capped. First page.
		resp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  2,
			Query:     query,
		})
		s.NoError(err)
		s.Len(resp.GetOperations(), 1)

		// Second page.
		resp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace:     env.Namespace().String(),
			PageSize:      2,
			Query:         query,
			NextPageToken: resp.GetNextPageToken(),
		})
		s.NoError(err)
		s.Len(resp.GetOperations(), 1)

		// No more results.
		resp, err = env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace:     env.Namespace().String(),
			PageSize:      2,
			Query:         query,
			NextPageToken: resp.GetNextPageToken(),
		})
		s.NoError(err)
		s.Empty(resp.GetOperations())
		s.Nil(resp.GetNextPageToken())
	})

	s.Run("InvalidQuery", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "invalid query")
	})

	s.Run("InvalidSearchAttribute", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "NonExistentField")
	})

	s.Run("NamespaceNotFound", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().ListNexusOperationExecutions(env.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: "non-existent-namespace",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
		s.ErrorContains(err, "non-existent-namespace")
	})
}

func (s *NexusStandaloneTestSuite) TestCountStandaloneNexusOperation() {
	s.Run("CountByOperationID", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "count-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "OperationId = 'count-op'",
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	s.Run("CountByEndpoint", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		for i := range 3 {
			_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("count-ep-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s'", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(3), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	s.Run("CountByExecutionStatus", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "count-status-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("ExecutionStatus = 'Running' AND Endpoint = '%s'", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	s.Run("GroupByExecutionStatus", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		for i := range 3 {
			_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("group-op-%d", i),
				Endpoint:    endpointName,
			})
			s.NoError(err)
		}

		var countResp *workflowservice.CountNexusOperationExecutionsResponse
		s.EventuallyWithT(func(t *assert.CollectT) {
			var err error
			countResp, err = env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("Endpoint = '%s' GROUP BY ExecutionStatus", endpointName),
			})
			require.NoError(t, err)
			require.Equal(t, int64(3), countResp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
		s.Len(countResp.GetGroups(), 1)
		s.Equal(int64(3), countResp.GetGroups()[0].GetCount())
		var groupValue string
		s.NoError(payload.Decode(countResp.GetGroups()[0].GetGroupValues()[0], &groupValue))
		s.Equal("Running", groupValue)
	})

	s.Run("CountByCustomSearchAttribute", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		for i := range 2 {
			_, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
				OperationId: fmt.Sprintf("count-sa-op-%d", i),
				Endpoint:    endpointName,
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"CustomKeywordField": payload.EncodeString("count-sa-value"),
					},
				},
			})
			s.NoError(err)
		}

		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
				Namespace: env.Namespace().String(),
				Query:     "CustomKeywordField = 'count-sa-value'",
			})
			require.NoError(t, err)
			require.Equal(t, int64(2), resp.GetCount())
		}, testcore.WaitForESToSettle, 100*time.Millisecond)
	})

	s.Run("GroupByUnsupportedField", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "GROUP BY Endpoint",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "'GROUP BY' clause is only supported for ExecutionStatus")
	})

	s.Run("InvalidQuery", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "invalid query syntax !!!",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "invalid query")
	})

	s.Run("InvalidSearchAttribute", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     "NonExistentField = 'value'",
		})
		s.ErrorAs(err, new(*serviceerror.InvalidArgument))
		s.ErrorContains(err, "NonExistentField")
	})

	s.Run("NamespaceNotFound", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().CountNexusOperationExecutions(env.Context(), &workflowservice.CountNexusOperationExecutionsRequest{
			Namespace: "non-existent-namespace",
		})
		s.ErrorAs(err, new(*serviceerror.NamespaceNotFound))
		s.ErrorContains(err, "non-existent-namespace")
	})
}

func (s *NexusStandaloneTestSuite) TestDeleteStandaloneNexusOperation() {
	s.Run("Scheduled", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		s.eventuallyDeleted(env, s.T(), "test-op", startResp.RunId)
	})

	s.Run("NoRunID", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
			// RunId not set
		})
		s.NoError(err)

		_, err = env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
		})
		s.NoError(err)

		s.eventuallyDeleted(env, s.T(), "test-op", startResp.RunId)
	})

	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "does-not-exist",
		})
		s.ErrorAs(err, new(*serviceerror.NotFound))
	})

	s.Run("AlreadyDeleted", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.NoError(err)

		s.eventuallyDeleted(env, s.T(), "test-op", startResp.RunId)

		_, err = env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
		})
		s.ErrorAs(err, new(*serviceerror.NotFound))
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().DeleteNexusOperationExecution(env.Context(), &workflowservice.DeleteNexusOperationExecutionRequest{
			Namespace: env.Namespace().String(),
		})
		s.Error(err)
		s.ErrorContains(err, "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) TestStandaloneNexusOperationPoll() {
	s.Run("WaitStageClosed", func(s *NexusStandaloneTestSuite) {
		for _, tc := range []struct {
			name      string
			withRunID bool
		}{
			{name: "WithEmptyRunID"},
			{name: "WithRunID", withRunID: true},
		} {
			s.Run(tc.name, func(s *NexusStandaloneTestSuite) {
				env := newNexusTestEnv(s.T(), false, s.opts()...)
				endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

				startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
					OperationId: "test-op",
					Endpoint:    endpointName,
				})
				s.NoError(err)

				ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
				defer cancel()

				// Start pollling.
				type pollResult struct {
					resp *workflowservice.PollNexusOperationExecutionResponse
					err  error
				}
				pollResultCh := make(chan pollResult, 1)
				pollStartedCh := make(chan struct{}, 1)

				go func() {
					runID := ""
					if tc.withRunID {
						runID = startResp.RunId
					}

					pollStartedCh <- struct{}{}
					resp, err := env.FrontendClient().PollNexusOperationExecution(ctx, &workflowservice.PollNexusOperationExecutionRequest{
						Namespace:   env.Namespace().String(),
						OperationId: "test-op",
						RunId:       runID,
						WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					})
					pollResultCh <- pollResult{resp: resp, err: err}
				}()

				select {
				case <-pollStartedCh:
				case <-ctx.Done():
					s.T().Fatal("PollNexusOperationExecution did not start before timeout")
				}

				// PollNexusOperationExecution should not resolve before the operation is closed.
				select {
				case result := <-pollResultCh:
					s.NoError(result.err)
					s.T().Fatal("PollNexusOperationExecution returned before the state changed")
				default:
				}

				// Terminate the operation.
				terminateErrCh := make(chan error, 1)
				go func() {
					_, err := env.FrontendClient().TerminateNexusOperationExecution(ctx, &workflowservice.TerminateNexusOperationExecutionRequest{
						Namespace:   env.Namespace().String(),
						OperationId: "test-op",
						RunId:       startResp.RunId,
						Reason:      "test termination",
					})
					terminateErrCh <- err
				}()

				select {
				case err := <-terminateErrCh:
					s.NoError(err)
				case <-ctx.Done():
					s.T().Fatal("TerminateNexusOperationExecution timed out")
				}

				// Verify the poll result.
				var result pollResult
				select {
				case result = <-pollResultCh:
				case <-ctx.Done():
					s.T().Fatal("PollNexusOperationExecution did not resolve before timeout")
				}
				s.NoError(result.err)
				pollResp := result.resp

				protorequire.ProtoEqual(s.T(), &workflowservice.PollNexusOperationExecutionResponse{
					RunId:          startResp.RunId,
					WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
					OperationToken: pollResp.GetOperationToken(),
					Outcome: &workflowservice.PollNexusOperationExecutionResponse_Failure{
						Failure: pollResp.GetFailure(),
					},
				}, pollResp)
				s.NotNil(pollResp.GetFailure().GetTerminatedFailureInfo())
			})
		}
	})

	s.Run("UnspecifiedWaitStageDefaultsToClosed", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		// Terminate the operation.
		_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			Reason:      "test termination",
		})
		s.NoError(err)

		// Poll with UNSPECIFIED WaitStage — should behave the same as CLOSED.
		pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "test-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED,
		})
		s.NoError(err)
		protorequire.ProtoEqual(s.T(), &workflowservice.PollNexusOperationExecutionResponse{
			RunId:          startResp.RunId,
			WaitStage:      enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			OperationToken: pollResp.GetOperationToken(),
			Outcome: &workflowservice.PollNexusOperationExecutionResponse_Failure{
				Failure: pollResp.GetFailure(),
			},
		}, pollResp)
		s.NotNil(pollResp.GetFailure().GetTerminatedFailureInfo())
	})

	s.Run("ReturnsLastAttemptFailure", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		taskQueue := testcore.RandomizedNexusEndpoint(s.T().Name())
		endpointName := env.createNexusEndpoint(s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), taskQueue).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:            "test-op",
			Endpoint:               endpointName,
			ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		})
		s.NoError(err)

		ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
		defer cancel()

		pollerErrCh := make(chan error, 1)
		go func() {
			task, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test-worker",
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			})
			if err != nil {
				pollerErrCh <- err
				return
			}
			_, err = env.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test-worker",
				TaskToken: task.GetTaskToken(),
				Error: &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure: &nexuspb.Failure{
						Message: "last attempt failure",
					},
				},
			})
			pollerErrCh <- err
		}()

		s.EventuallyWithT(func(t *assert.CollectT) {
			pollResp, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   env.Namespace().String(),
				OperationId: "test-op",
				RunId:       startResp.RunId,
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED, pollResp.GetWaitStage())
			require.Equal(t, "last attempt failure", pollResp.GetFailure().GetMessage())
		}, 10*time.Second, 100*time.Millisecond)

		s.NoError(<-pollerErrCh)
	})

	s.Run("NamespaceNotFound", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   "non-existent-namespace",
			OperationId: "test-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		var namespaceNotFoundErr *serviceerror.NamespaceNotFound
		s.ErrorAs(err, &namespaceNotFoundErr)
		s.Contains(namespaceNotFoundErr.Error(), "non-existent-namespace")
	})

	s.Run("NotFound", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)
		endpointName := env.createRandomNexusEndpoint(s.T()).GetSpec().GetName()

		startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
			OperationId: "test-op",
			Endpoint:    endpointName,
		})
		s.NoError(err)

		_, err = env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "non-existent-op",
			RunId:       startResp.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Equal("operation not found for ID: non-existent-op", notFoundErr.Error())
	})

	// Validates that request validation is wired up in the frontend.
	// Exhaustive validation cases are covered in unit tests.
	s.Run("Validation", func(s *NexusStandaloneTestSuite) {
		env := newNexusTestEnv(s.T(), false, s.opts()...)

		_, err := env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: "", // required field
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		s.Error(err)
		s.Contains(err.Error(), "operation_id is required")
	})
}

func (s *NexusStandaloneTestSuite) startNexusOperation(
	env *NexusTestEnv,
	req *workflowservice.StartNexusOperationExecutionRequest,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	req.Namespace = cmp.Or(req.Namespace, env.Namespace().String())
	req.Service = cmp.Or(req.Service, "test-service")
	req.Operation = cmp.Or(req.Operation, "test-operation")
	req.RequestId = cmp.Or(req.RequestId, env.Tv().RequestID())
	if req.ScheduleToCloseTimeout == nil {
		req.ScheduleToCloseTimeout = durationpb.New(10 * time.Minute)
	}

	var (
		resp *workflowservice.StartNexusOperationExecutionResponse
		err  error
	)
	s.Eventually(func() bool {
		resp, err = env.FrontendClient().StartNexusOperationExecution(env.Context(), req)
		if err == nil {
			return true
		}

		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			return true
		}

		message := notFound.Error()
		return message != "endpoint not registered" &&
			!strings.HasPrefix(message, "could not find Nexus endpoint by name:")
	}, 10*time.Second, 100*time.Millisecond, "start operation should succeed once the endpoint is visible")

	return resp, err
}

func (s *NexusStandaloneTestSuite) eventuallyDeleted(env *NexusTestEnv, t *testing.T, operationID, runID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := env.FrontendClient().DescribeNexusOperationExecution(env.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
			RunId:       runID,
		})
		var notFoundErr *serviceerror.NotFound
		return errors.As(err, &notFoundErr)
	}, 10*time.Second, 100*time.Millisecond)
}
