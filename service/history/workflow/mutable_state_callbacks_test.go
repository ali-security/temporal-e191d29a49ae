package workflow

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testCompletionCallbacks(n int) []*commonpb.Callback {
	cbs := make([]*commonpb.Callback, n)
	for i := range cbs {
		cbs[i] = &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/callback"},
			},
		}
	}
	return cbs
}

// enableChasmCallbacks turns on the CHASM callback path and caps the execution at
// maxCallbacks. It returns a fresh mutable state built against the reconfigured shard.
func (s *mutableStateSuite) enableChasmCallbacks(maxCallbacks int) *MutableStateImpl {
	s.mockConfig.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockConfig.EnableCHASMCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockConfig.EnableWorkflowUpdateCallbacks = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)

	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	s.Require().NoError(chasmRegistry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))
	s.mockShard.SetChasmRegistry(chasmRegistry)

	cfg := test.NewCallbacksValidatorConfig()
	cfg.MaxCallbacksPerExecution = func(string) int { return maxCallbacks }
	s.mockShard.SetCallbackValidator(test.NewCallbacksValidator(s.T(), cfg))

	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	return NewMutableState(
		s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)
}

func (s *mutableStateSuite) chasmWorkflowComponent(ms *MutableStateImpl) *chasmworkflow.Workflow {
	wf, _, err := ms.ChasmWorkflowComponentReadOnly(context.Background())
	s.Require().NoError(err)
	return wf
}

func (s *mutableStateSuite) TestChasmCompletionCallbacks_LimitEnforcedOnWritePath() {
	ms := s.enableChasmCallbacks(2)

	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "req-1",
				CompletionCallbacks: testCompletionCallbacks(3),
			},
		},
	)
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
	s.ErrorContains(err, "cannot attach more than 2 callbacks to an execution")

	// Nothing was written: neither the started event nor any callback.
	s.Equal(int64(common.FirstEventID), ms.GetNextEventID())
}

func (s *mutableStateSuite) TestChasmCompletionCallbacks_WithinLimitAttaches() {
	ms := s.enableChasmCallbacks(2)

	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           "req-1",
				CompletionCallbacks: testCompletionCallbacks(2),
			},
		},
	)
	s.NoError(err)

	wf := s.chasmWorkflowComponent(ms)
	s.Len(wf.Callbacks, 2)
	s.Equal(int64(2), wf.GetTotalCallbacksCount())
	s.NotZero(wf.GetTotalCallbacksSize())
}

// The rebuild path replays events another cluster already committed. Re-checking the limit
// there would stall the replication task instead of protecting anything, so an over-limit
// event must still apply cleanly.
func (s *mutableStateSuite) TestChasmCompletionCallbacks_LimitNotEnforcedOnRebuildPath() {
	ms := s.enableChasmCallbacks(2)

	cbs := testCompletionCallbacks(3)
	startEvent := &historypb.HistoryEvent{
		EventId:   common.FirstEventID,
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType:        &commonpb.WorkflowType{Name: "test-workflow-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				CompletionCallbacks: cbs,
			},
		},
	}

	err := ms.ApplyWorkflowExecutionStartedEvent(
		nil,
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		"req-1",
		startEvent,
	)
	s.NoError(err)

	wf := s.chasmWorkflowComponent(ms)
	s.Len(wf.Callbacks, 3)
	s.Equal(int64(3), wf.GetTotalCallbacksCount())
}

func (s *mutableStateSuite) TestChasmUpdateCallbacks_LimitEnforcedOnWritePathOnly() {
	ms := s.enableChasmCallbacks(2)

	_, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: tests.WorkflowID, RunId: tests.RunID},
		&historyservice.StartWorkflowExecutionRequest{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{RequestId: "req-1"},
		},
	)
	s.NoError(err)
	_, err = ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)

	updateID := "update-id"
	acceptedRequest := func(requestID string, n int) *updatepb.Request {
		return &updatepb.Request{
			Meta:                &updatepb.Meta{UpdateId: updateID},
			RequestId:           requestID,
			CompletionCallbacks: testCompletionCallbacks(n),
		}
	}

	_, err = ms.AddWorkflowExecutionUpdateAcceptedEvent(updateID, "msg-id", 1, acceptedRequest("req-2", 3))
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
	s.ErrorContains(err, "cannot attach more than 2 callbacks to an execution")

	// The same over-limit update replays without complaint.
	event := &historypb.HistoryEvent{
		EventId:   ms.GetNextEventID(),
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
			WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
				ProtocolInstanceId: updateID,
				AcceptedRequest:    acceptedRequest("req-3", 3),
			},
		},
	}
	s.NoError(ms.ApplyWorkflowExecutionUpdateAcceptedEvent(event))

	wf := s.chasmWorkflowComponent(ms)
	s.Equal(int64(3), wf.GetTotalCallbacksCount())
}
