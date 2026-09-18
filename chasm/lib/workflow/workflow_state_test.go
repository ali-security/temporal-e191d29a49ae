package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newWorkflowTestTree(
	t *testing.T,
	backend chasm.NodeBackend,
	serializedNodes map[string]*persistencespb.ChasmNode,
) *chasm.Node {
	t.Helper()

	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(NewLibrary(NewRegistry())))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))

	if len(serializedNodes) == 0 {
		return chasm.NewEmptyTree(
			registry, clock.NewRealTimeSource(), backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler,
		)
	}
	root, err := chasm.NewTreeFromDB(
		serializedNodes, registry, clock.NewRealTimeSource(), backend, chasm.DefaultPathEncoder, logger,
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	return root
}

// TestWorkflowStateReplacesEmptyState pins the on-disk compatibility of swapping the workflow
// root component's state proto from emptypb.Empty to WorkflowState. Executions written by an
// older server persisted either no data blob at all (NewWorkflow left Empty nil) or the zero
// encoding of Empty, and both must load as a zero-valued WorkflowState with their callbacks
// intact.
func TestWorkflowStateReplacesEmptyState(t *testing.T) {
	t.Parallel()

	// Produce a persisted tree the way the current code writes one.
	backend := &chasm.MockNodeBackend{
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
	root := newWorkflowTestTree(t, backend, nil)
	mutableCtx := chasm.NewMutableContext(t.Context(), root)
	require.NoError(t, root.SetRootComponent(NewWorkflow(mutableCtx, chasm.NewMSPointer(backend))))

	// Go through the tree so the root node is marked dirty, as the write path does.
	component, err := root.ComponentByPath(mutableCtx, nil)
	require.NoError(t, err)
	wf := component.(*Workflow)
	require.NoError(t, wf.AddCompletionCallbacks(
		mutableCtx,
		timestamppb.Now(),
		"req-1",
		[]*commonpb.Callback{nexusCallback("http://cb-1")},
	))
	mutation, err := root.CloseTransaction()
	require.NoError(t, err)
	require.Contains(t, mutation.UpdatedNodes, "")

	emptyEncoded, err := proto.Marshal(&emptypb.Empty{})
	require.NoError(t, err)

	for _, tc := range []struct {
		name         string
		overrideData bool
		data         *commonpb.DataBlob
		wantCBCount  int64
	}{
		{
			// NewWorkflow never set Empty, so serializeComponentNode wrote no blob at all.
			name:         "NoDataBlob",
			overrideData: true,
			data:         nil,
			wantCBCount:  0,
		},
		{
			// Belt and braces: an explicitly encoded Empty is also zero bytes.
			name:         "EncodedEmpty",
			overrideData: true,
			data:         &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: emptyEncoded},
			wantCBCount:  0,
		},
		{
			// Guards the two cases above against passing vacuously: the blob this server
			// writes must come back with its counters intact.
			name:        "CurrentEncoding",
			wantCBCount: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serializedNodes := make(map[string]*persistencespb.ChasmNode, len(mutation.UpdatedNodes))
			for path, node := range mutation.UpdatedNodes {
				serializedNodes[path] = proto.Clone(node).(*persistencespb.ChasmNode)
			}
			if tc.overrideData {
				// Rewrite the root's payload to what a pre-WorkflowState server would have stored.
				serializedNodes[""].Data = tc.data
			}

			reloaded := newWorkflowTestTree(t, backend, serializedNodes)
			component, err := reloaded.ComponentByPath(chasm.NewContext(t.Context(), reloaded), nil)
			require.NoError(t, err)

			loadedWF, ok := component.(*Workflow)
			require.True(t, ok)
			require.NotNil(t, loadedWF.WorkflowState)
			require.Equal(t, tc.wantCBCount, loadedWF.GetTotalCallbacksCount())

			// The rest of the tree is unaffected by the state swap.
			require.Len(t, loadedWF.Callbacks, 1)
			require.Contains(t, loadedWF.Callbacks, "req-1-0")
		})
	}
}
