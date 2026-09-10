/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clusterstateregistry

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	fakek8s "sigs.k8s.io/cluster-autoscaler/pkg/utils/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// RetentionSetupArgs configures the shared retained-Node reuse scenario.
type RetentionSetupArgs struct {
	OptsOverride integration.AutoscalingOptionOverride
	NodeCount    int
}

// RetentionSetup supplies the autoscaler and provider reconciliation controls.
type RetentionSetup struct {
	Autoscaler core.Autoscaler
	NodeGroup  cloudprovider.NodeGroup
	K8s        *fakek8s.Kubernetes
	// CompleteResumes finishes backend Starts without changing Kubernetes readiness.
	CompleteResumes func() error
	// Refresh reconciles and publishes provider observations.
	Refresh func() error
}

// RetentionSetupFactory creates active Nodes with stable UIDs and an opted-in group.
type RetentionSetupFactory func(*testing.T, context.Context, RetentionSetupArgs) RetentionSetup

// RunTestClusterStateRegistryRetention exercises draining, retained membership and old-Node reuse.
func RunTestClusterStateRegistryRetention(t *testing.T, factory RetentionSetupFactory) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)
		setup := factory(t, ctx, RetentionSetupArgs{
			NodeCount: 4,
			OptsOverride: func(opts *config.AutoscalingOptions) {
				opts.ScaleDownEnabled = true
				opts.CordonNodeBeforeTerminate = false
				opts.ScaleDownDelayAfterAdd = 0
				opts.ScaleDownSimulationTimeout = 24 * time.Hour
				opts.MaxScaleDownParallelism = 10
				opts.MaxNodesTotal = 4
				opts.NodeGroupDefaults.ScaleDownUtilizationThreshold = 0.5
				opts.NodeGroupDefaults.ScaleDownUnneededTime = time.Minute
				opts.NodeGroupDefaults.MaxNodeProvisionTime = 5 * time.Minute
			},
		})
		synctest.Wait()
		require.Len(t, setup.K8s.Nodes().Items, 4)
		identities := make(map[string]types.UID)
		for _, node := range setup.K8s.Nodes().Items {
			identities[node.Spec.ProviderID] = node.UID
		}
		cpu, memory := getNodeCpuAndMemory(&setup.K8s.Nodes().Items[0])
		for i, node := range setup.K8s.Nodes().Items[:2] {
			require.NotEmpty(t, node.UID)
			setup.K8s.AddPod(testutils.BuildTestPod(fmt.Sprintf("busy-%d", i), cpu*8/10, memory*8/10, testutils.WithNodeName(node.Name)))
		}
		require.NoError(t, synctestutils.RunOnceAfter(t, setup.Autoscaler, 0))
		require.NoError(t, synctestutils.RunOnceAfter(t, setup.Autoscaler, time.Minute+time.Microsecond))
		assertNodeGroupSize(t, setup.NodeGroup, setup.K8s, 2, 4)
		require.NoError(t, setup.Refresh())
		view, err := cloudprovider.GetNodeGroupAccounting(ctx, setup.NodeGroup)
		require.NoError(t, err)
		require.NotNil(t, view)
		require.Len(t, view.Instances, 4)
		require.Len(t, view.InactiveInstanceIDs, 2)
		inactive := slices.Clone(view.InactiveInstanceIDs)

		setup.K8s.AddPod(testutils.BuildTestPod("pending", cpu*8/10, memory*8/10, testutils.MarkUnschedulable()))
		require.NoError(t, synctestutils.RunOnceAfter(t, setup.Autoscaler, 10*time.Second))
		assertNodeGroupSize(t, setup.NodeGroup, setup.K8s, 3, 4)
		require.NoError(t, setup.Refresh())
		for range 2 {
			require.NoError(t, synctestutils.RunOnceAfter(t, setup.Autoscaler, 10*time.Second))
			assertNodeGroupSize(t, setup.NodeGroup, setup.K8s, 3, 4)
		}
		view, err = cloudprovider.GetNodeGroupAccounting(ctx, setup.NodeGroup)
		require.NoError(t, err)
		require.Equal(t, 1, view.UpcomingInactiveNodes)
		require.Len(t, view.InactiveInstanceIDs, 2)
		require.NoError(t, setup.CompleteResumes())
		synctest.Wait()
		require.NoError(t, synctestutils.RunOnceAfter(t, setup.Autoscaler, time.Second))
		view, err = cloudprovider.GetNodeGroupAccounting(ctx, setup.NodeGroup)
		require.NoError(t, err)
		require.Equal(t, 1, view.UpcomingInactiveNodes, "backend completion with an old heartbeat is not usable")

		for _, node := range setup.K8s.Nodes().Items {
			if !slices.Contains(inactive, node.Spec.ProviderID) {
				continue
			}
			require.True(t, taints.HasToBeDeletedTaint(&node))
			for i := range node.Status.Conditions {
				if node.Status.Conditions[i].Type == apiv1.NodeReady {
					node.Status.Conditions[i].Status = apiv1.ConditionTrue
					node.Status.Conditions[i].LastHeartbeatTime = metav1.NewTime(time.Now())
				}
			}
			_, err := setup.K8s.Client.CoreV1().Nodes().UpdateStatus(ctx, &node, metav1.UpdateOptions{})
			require.NoError(t, err)
		}
		require.NoError(t, synctestutils.RunOnceAfter(t, setup.Autoscaler, time.Second))
		view, err = cloudprovider.GetNodeGroupAccounting(ctx, setup.NodeGroup)
		require.NoError(t, err)
		require.Zero(t, view.UpcomingInactiveNodes)
		require.Len(t, view.InactiveInstanceIDs, 1)
		for _, node := range setup.K8s.Nodes().Items {
			if !slices.Contains(inactive, node.Spec.ProviderID) {
				continue
			}
			require.Equal(t, identities[node.Spec.ProviderID], node.UID)
			if slices.Contains(view.InactiveInstanceIDs, node.Spec.ProviderID) {
				require.True(t, taints.HasToBeDeletedTaint(&node))
				continue
			}
			require.False(t, taints.HasToBeDeletedTaint(&node))
			require.False(t, cloudprovider.IsNodeSuspended(&node))
		}
		assertNodeGroupSize(t, setup.NodeGroup, setup.K8s, 3, 4)
		require.NoError(t, setup.Refresh())
		require.NoError(t, synctestutils.RunOnceAfter(t, setup.Autoscaler, 10*time.Second))
		assertNodeGroupSize(t, setup.NodeGroup, setup.K8s, 3, 4)
	})
}
