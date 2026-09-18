package workflow

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/service/history/historybuilder"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Both components hold callbacks, so both have to be a CompletionSource for their delivery.
var (
	_ callback.CompletionSource = (*Workflow)(nil)
	_ callback.CompletionSource = (*WorkflowUpdate)(nil)
)

type Workflow struct {
	chasm.UnimplementedComponent

	// For now, the workflow's execution state is managed by mutable_state_impl, not the CHASM
	// engine. WorkflowState only carries the bookkeeping that the CHASM tree itself owns.
	*chasmworkflowpb.WorkflowState

	// MSPointer is a special in-memory field for accessing the underlying mutable state.
	chasm.MSPointer

	// Callbacks map is used to store the callbacks for the workflow.
	Callbacks chasm.Map[string, *callback.Callback]

	// Operations map is used to store the Nexus operations for the workflow, keyed by scheduled event ID.
	Operations chasm.Map[int64, *nexusoperation.Operation]

	// IncomingSignals map is used to track incoming signals, keyed by request ID,
	// to allow DescribeWorkflow to resolve RequestIDRef signal backlinks.
	IncomingSignals chasm.Map[string, *chasmworkflowpb.IncomingSignalData]

	// Updates indexed by update ID, used to store the update components.
	Updates chasm.Map[string, *WorkflowUpdate]
}

func NewWorkflow(
	_ chasm.MutableContext,
	msPointer chasm.MSPointer,
) *Workflow {
	return &Workflow{
		WorkflowState: &chasmworkflowpb.WorkflowState{},
		MSPointer:     msPointer,
	}
}

func (w *Workflow) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	// NOTE: closeTransactionHandleRootLifecycleChange() is bypassed in tree.go
	//
	// NOTE: detached mode is not implemented yet, so always return Running here.
	// Otherwise, tasks for callback component can't be executed after workflow is closed.
	return chasm.LifecycleStateRunning
}

func (w *Workflow) ContextMetadata(_ chasm.Context) map[string]string {
	// TODO: Export workflow metadata from the CHASM workflow root instead of CloseTransaction().
	return nil
}

func (w *Workflow) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewInternal("workflow root Terminate should not be called")
}

// ProcessCloseCallbacks triggers "WorkflowClosed" callbacks using the CHASM implementation.
// It schedules all workflow-level and update-level callbacks that are in STANDBY state.
func (w *Workflow) ProcessCloseCallbacks(ctx chasm.MutableContext) error {
	if err := callback.ScheduleStandbyCallbacks(ctx, w.Callbacks); err != nil {
		return err
	}
	return w.ProcessAllUpdateCloseCallbacks(ctx)
}

// ProcessAllUpdateCloseCallbacks triggers callbacks for all updates without touching
// workflow-level callbacks. This is used when the workflow is continuing to a new run
// (ContinueAsNew, retry, cron): workflow-level callbacks are inherited by the new run,
// but update callbacks must fire now because the update was aborted on the old run.
func (w *Workflow) ProcessAllUpdateCloseCallbacks(ctx chasm.MutableContext) error {
	for _, updateField := range w.Updates {
		if err := callback.ScheduleStandbyCallbacks(ctx, updateField.Get(ctx).Callbacks); err != nil {
			return err
		}
	}
	return nil
}

// ProcessUpdateCallbacks triggers callbacks for a single updateID if exists.
func (w *Workflow) ProcessUpdateCallbacks(ctx chasm.MutableContext, updateID string) error {
	update, exists := w.Updates[updateID]
	if !exists {
		return serviceerror.NewNotFoundf("update with ID %s not found", updateID)
	}
	return callback.ScheduleStandbyCallbacks(ctx, update.Get(ctx).Callbacks)
}

// RejectUpdate stores the rejection failure on the WorkflowUpdate component and
// fires any pending callbacks. This is used when a reapplied update (after reset)
// is rejected by the worker's validator - the callbacks need to deliver the
// rejection failure to the caller.
func (w *Workflow) RejectUpdate(ctx chasm.MutableContext, updateID string, rejectionFailure *failurepb.Failure) error {
	updateField, exists := w.Updates[updateID]
	if !exists {
		return nil // no callbacks registered for this update
	}

	upd := updateField.Get(ctx)
	upd.RejectionFailure = rejectionFailure

	return callback.ScheduleStandbyCallbacks(ctx, upd.Callbacks)
}

// CallbackAddition is a set of completion callbacks that a single request is about to attach
// to an execution. UpdateID is empty when the callbacks target the workflow itself rather
// than one of its updates.
type CallbackAddition struct {
	UpdateID  string
	RequestID string
	Callbacks []*commonpb.Callback
}

// completionCallbackID defines the stable key used for keeping track of attached completion
// callbacks. requestID (unique per API call) + idx (position within the request) ensures
// unique, idempotent callback IDs. Unlike HSM callbacks, CHASM replicates entire trees rather
// than replaying events, so deterministic cross-cluster IDs based on event version are not
// needed.
func completionCallbackID(requestID string, idx int) string {
	return fmt.Sprintf("%s-%d", requestID, idx)
}

// hasCallbacksForRequest reports whether requestID has already attached its callbacks to
// target. Attaching is atomic, so the presence of the first key means they are all present.
func hasCallbacksForRequest(target chasm.Map[string, *callback.Callback], requestID string) bool {
	_, ok := target[completionCallbackID(requestID, 0)]
	return ok
}

// callbacksInfo describes every callback attached to this execution: the workflow's own plus
// those on each of its updates.
//
// The totals are denormalized onto WorkflowState so that the aggregate limits stay O(1)
// instead of deserializing every update component. They are reported as recorded and are
// undercounted for executions that predate the fields, which report zero.
func (w *Workflow) callbacksInfo() callbacks.CurrentCallbacksInfo {
	return callbacks.CurrentCallbacksInfo{
		Count:     int(w.GetTotalCallbacksCount()),
		TotalSize: int(w.GetTotalCallbacksSize()),
	}
}

// ValidateCallbackAdditions checks that every addition can be attached to this execution
// without breaching the execution-wide callback limits or, for update callbacks,
// maxCallbacksPerUpdateID.
//
// Callers MUST invoke this on the write path only, never from an Apply* function.
// MutableStateRebuilder drives those during NDC replication, history import, and reset, where
// the event has already been committed on another cluster: rejecting it there stalls the
// replication task rather than protecting anything, and lowering a limit would retroactively
// wedge every execution already above it. Callbacks are validated as they are attached and
// then kept as-is.
func (w *Workflow) ValidateCallbackAdditions(
	ctx chasm.Context,
	additions []CallbackAddition,
	namespaceName string,
	validator callbacks.Validator,
	maxCallbacksPerUpdateID int,
) error {
	var pending []*commonpb.Callback
	for _, addition := range additions {
		if len(addition.Callbacks) == 0 {
			continue
		}

		target := w.Callbacks
		if addition.UpdateID != "" {
			target = nil
			if updateField, ok := w.Updates[addition.UpdateID]; ok {
				target = updateField.Get(ctx).Callbacks
			}
		}

		// A request that already attached its callbacks is a no-op at attach time, so counting
		// it again here would reject retries that are actually within the limits. This has to
		// precede the limit checks: target already holds the callbacks being re-offered.
		if hasCallbacksForRequest(target, addition.RequestID) {
			continue
		}

		if addition.UpdateID != "" && len(addition.Callbacks)+len(target) > maxCallbacksPerUpdateID {
			return serviceerror.NewFailedPreconditionf(
				"cannot attach more than %d callbacks to update %q (%d callbacks already attached)",
				maxCallbacksPerUpdateID,
				addition.UpdateID,
				len(target),
			)
		}

		pending = append(pending, addition.Callbacks...)
	}

	if len(pending) == 0 {
		return nil
	}
	return validator.ValidateAdditions(namespaceName, pending, w.callbacksInfo())
}

// addCallbacksToMap converts common callbacks to CHASM callback components and inserts them
// into the target map, and keeps the execution's denormalized callback totals in step.
//
// All callbacks are converted up front, so target is not mutated unless every callback can be
// converted successfully (atomic from the caller's POV). Re-attaching a request that is
// already present is a no-op, which keeps the totals accurate when the same callbacks arrive
// on more than one event (an update's admitted and accepted events, say).
func (w *Workflow) addCallbacksToMap(
	ctx chasm.MutableContext,
	target chasm.Map[string, *callback.Callback],
	requestID string,
	eventTime *timestamppb.Timestamp,
	completionCallbacks []*commonpb.Callback,
) error {
	if hasCallbacksForRequest(target, requestID) {
		return nil
	}

	chasmCBs := make([]*callbackspb.Callback, len(completionCallbacks))
	for i, cb := range completionCallbacks {
		chasmCB, err := callback.FromAPICallback(cb)
		if err != nil {
			return err
		}
		chasmCBs[i] = chasmCB
	}

	for idx, chasmCB := range chasmCBs {
		callbackObj := callback.NewCallback(requestID, eventTime, chasmCB)
		target[completionCallbackID(requestID, idx)] = chasm.NewComponentField(ctx, callbackObj)
		w.TotalCallbacksCount++
		w.TotalCallbacksSize += int64(completionCallbacks[idx].Size())
	}
	return nil
}

// AddCompletionCallbacks creates completion callbacks using the CHASM implementation.
//
// Limits are not checked here: see ValidateCallbackAdditions, which the write path calls
// before the event carrying these callbacks is written.
func (w *Workflow) AddCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	if len(completionCallbacks) == 0 {
		return nil
	}

	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}

	return w.addCallbacksToMap(ctx, w.Callbacks, requestID, eventTime, completionCallbacks)
}

// AddUpdateCompletionCallbacks creates update completion callbacks using the CHASM
// implementation.
//
// Limits are not checked here: see ValidateCallbackAdditions, which the write path calls
// before the event carrying these callbacks is written.
func (w *Workflow) AddUpdateCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	updateID string,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	if len(completionCallbacks) == 0 {
		return nil
	}

	if w.Updates == nil {
		w.Updates = make(chasm.Map[string, *WorkflowUpdate], 1)
	}
	if _, ok := w.Updates[updateID]; !ok {
		workflowUpdateObj := NewWorkflowUpdate(ctx, updateID, w.MSPointer)
		workflowUpdateObj.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
		w.Updates[updateID] = chasm.NewComponentField(ctx, workflowUpdateObj)
	}

	update := w.Updates[updateID].Get(ctx)
	if update.Callbacks == nil {
		update.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}
	return w.addCallbacksToMap(ctx, update.Callbacks, requestID, eventTime, completionCallbacks)
}

// addAndApplyHistoryEvent adds a history event to the workflow and applies the corresponding event definition,
// looked up by Go type. This is the preferred way to add and apply events as it provides go-to-definition navigation.
func addAndApplyHistoryEvent[D EventDefinition](
	w *Workflow,
	ctx chasm.MutableContext,
	setAttributes func(*historypb.HistoryEvent),
) (*historypb.HistoryEvent, error) {
	def, ok := eventDefinitionByGoType[D](workflowContextFromChasm(ctx).registry)
	if !ok {
		return nil, serviceerror.NewInternalf("no event definition registered for Go type %T", (*D)(nil))
	}
	event := w.AddHistoryEvent(def.Type(), setAttributes)
	return event, def.Apply(ctx, w, event)
}

// AddIncomingSignalEvent adds an entry for the signal requestID -> eventID mapping to
// track all signals that have been received by the workflow.
// Note that since signals are buffered, the eventID may the common.BufferedEventID, which
// will be updated to a concrete eventID once this signal is flushed to the DB.
// If caller tries to add an already-existing eventID, this function will ignore and silently return
// instead of overwriting -- use UpdateIncomingSignalEvent to update existing entries.
func (w *Workflow) AddIncomingSignalEvent(
	ctx chasm.MutableContext,
	requestID string,
	eventID int64,
) error {
	if w.IncomingSignals == nil {
		w.IncomingSignals = make(chasm.Map[string, *chasmworkflowpb.IncomingSignalData])
	}
	if w.HasIncomingSignalEvent(ctx, requestID) {
		return nil
	}
	w.IncomingSignals[requestID] = chasm.NewDataField(ctx, &chasmworkflowpb.IncomingSignalData{
		// This might be common.BufferedEventID, which will be updated via UpdateIncomingSignalEvent
		// once this signal is flushed to DB.
		EventId: eventID,
	})
	return nil
}

// UpdateIncomingSignalEvent updates the eventID for an existing signal requestID in the map.
// If the requestID is not in the map, this is a no-op (e.g. when called for non-signal request IDs
// during buffer flush).
func (w *Workflow) UpdateIncomingSignalEvent(
	ctx chasm.MutableContext,
	requestID string,
	eventID int64,
) error {
	if w.HasIncomingSignalEvent(ctx, requestID) {
		w.IncomingSignals[requestID].Get(ctx).EventId = eventID
	}

	return nil
}

// HasIncomingSignalEvent returns true if a signal with this requestID is already persisted
// in this CHASM tree.
func (w *Workflow) HasIncomingSignalEvent(_ chasm.Context, requestID string) bool {
	_, exists := w.IncomingSignals[requestID]
	return exists
}

// HasAnyBufferedEvent returns true if the workflow has any buffered event matching the given filter.
func (w *Workflow) HasAnyBufferedEvent(filter historybuilder.BufferedEventFilter) bool {
	return w.MSPointer.HasAnyBufferedEvent(filter)
}

func (w *Workflow) WorkflowTypeName() string {
	return w.GetWorkflowTypeName()
}
