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

package clusterstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate/utils"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupconfig"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type accountingGroup struct {
	cloudprovider.NodeGroup
	view     *cloudprovider.NodeGroupAccountingSnapshot
	included bool
	err      error
	decrease int
}

func (g *accountingGroup) SuspendedNodesIncludedInTargetSize() bool { return g.included }
func (g *accountingGroup) GetNodeGroupAccounting(context.Context) (*cloudprovider.NodeGroupAccountingSnapshot, error) {
	return g.view, g.err
}
func (g *accountingGroup) DecreaseTargetSize(_ context.Context, delta int) error {
	g.decrease += delta
	return nil
}

func newAccountingRegistry(t *testing.T) (*ClusterStateRegistry, *accountingGroup, []*apiv1.Node) {
	t.Helper()
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	group := &accountingGroup{
		NodeGroup: provider.BuildNodeGroup("ng", 0, 10, 2, true, false, "", nil),
		view:      &cloudprovider.NodeGroupAccountingSnapshot{TargetSize: 2},
	}
	provider.InsertNodeGroup(group)
	var nodes []*apiv1.Node
	for i := range 3 {
		name := fmt.Sprintf("n%d", i)
		node := testutils.BuildTestNode(name, 2000, 1000, testutils.IsReady(true))
		node.UID, node.Spec.ProviderID = types.UID(name), name
		node.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
		provider.AddNode("ng", node)
		nodes = append(nodes, node)
		group.view.Instances = append(group.view.Instances, cloudprovider.Instance{Id: name})
	}
	group.view.InactiveInstanceIDs = []string{"n2"}
	logRecorder, _ := utils.NewStatusMapRecorder(&fake.Clientset{}, "kube-system", record.NewFakeRecorder(20), false, "status")
	registry := NewNotifiedClusterStateRegistry(provider, logRecorder, newBackoff(),
		nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 5 * time.Minute}),
		newMockTemplateNodeInfoRegistry(map[string]*framework.NodeInfo{"ng": framework.NewTestNodeInfo(nodes[0])}),
		WithConfig(ClusterStateRegistryConfig{MaxTotalUnreadyPercentage: 45}))
	return registry, group, nodes
}

func TestSuspendedTargetAccounting(t *testing.T) {
	for _, included := range []bool{true, false} {
		t.Run(fmt.Sprint(included), func(t *testing.T) {
			csr, group, nodes := newAccountingRegistry(t)
			group.included = included
			if included {
				group.view.TargetSize = 3
			}
			require.NoError(t, csr.UpdateNodes(t.Context(), nodes, time.Now()))
			assert.True(t, csr.IsNodeGroupHealthy(t.Context(), "ng"))
			count, ok := csr.getUpcomingNodesInNodeGroup(t.Context(), "ng")
			require.True(t, ok)
			assert.Zero(t, count)
			actual, target := csr.GetAutoscaledNodesCount()
			assert.Equal(t, group.view.TargetSize, actual)
			assert.Equal(t, group.view.TargetSize, target)
			assert.Nil(t, csr.GetIncorrectNodeGroupSize("ng"))
			assert.False(t, cloudprovider.IsNodeSuspended(nodes[2]), "informer object remains unchanged")
		})
	}
}

func TestSuspendedTargetWithoutObservation(t *testing.T) {
	csr, group, nodes := newAccountingRegistry(t)
	now := time.Now()
	group.view = nil
	nodes[2].Status.Conditions = append(nodes[2].Status.Conditions, apiv1.NodeCondition{Type: "Suspended", Status: apiv1.ConditionTrue})
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now))
	assert.True(t, csr.IsNodeGroupHealthy(t.Context(), "ng"))
	require.NoError(t, group.NodeGroup.IncreaseSize(t.Context(), 1))
	csr.RegisterScaleUp(t.Context(), group, 1, now)
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now.Add(time.Minute)))
	assert.True(t, csr.HasNodeGroupStartedScaleUp("ng"))
	upcoming, _ := csr.GetUpcomingNodes(t.Context())
	assert.Equal(t, 1, upcoming["ng"])
	for i := range nodes[2].Status.Conditions {
		if nodes[2].Status.Conditions[i].Type == "Suspended" {
			nodes[2].Status.Conditions[i].Status = apiv1.ConditionFalse
		}
	}
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now.Add(2*time.Minute)))
	assert.False(t, csr.HasNodeGroupStartedScaleUp("ng"))
}

func TestProviderOwnedRegisteredResume(t *testing.T) {
	for _, staleReady := range []bool{true, false} {
		t.Run(fmt.Sprint(staleReady), func(t *testing.T) {
			csr, group, nodes := newAccountingRegistry(t)
			now := time.Now()
			testutils.SetNodeReadyState(nodes[2], staleReady, now.Add(-time.Hour))
			nodes[2].Spec.Taints = []apiv1.Taint{{Key: taints.ToBeDeletedTaint, Effect: apiv1.TaintEffectNoSchedule}}
			require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now))
			group.view.TargetSize, group.view.UpcomingInactiveNodes = 3, 1
			csr.RegisterScaleUp(t.Context(), group, 1, now)
			require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now.Add(time.Minute)))
			assert.True(t, csr.HasNodeGroupStartedScaleUp("ng"))
			upcoming, _ := csr.GetUpcomingNodes(t.Context())
			assert.Equal(t, 1, upcoming["ng"])

			fresh := nodes[2].DeepCopy()
			fresh.Spec.Taints = nil
			testutils.SetNodeReadyState(fresh, true, now.Add(time.Minute))
			group.view.NodeObservations = []*apiv1.Node{fresh}
			group.view.InactiveInstanceIDs, group.view.UpcomingInactiveNodes = nil, 0
			require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now.Add(2*time.Minute)))
			assert.False(t, csr.HasNodeGroupStartedScaleUp("ng"))
			upcoming, _ = csr.GetUpcomingNodes(t.Context())
			assert.Empty(t, upcoming)
			assert.True(t, taints.HasToBeDeletedTaint(nodes[2]))

			fresh = fresh.DeepCopy()
			fresh.Spec.Unschedulable = true
			testutils.SetNodeReadyState(fresh, false, now.Add(3*time.Minute))
			group.view.NodeObservations = []*apiv1.Node{fresh}
			require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now.Add(3*time.Minute)))
			upcoming, _ = csr.GetUpcomingNodes(t.Context())
			assert.Empty(t, upcoming, "ordinary Unready is not a new activation")
		})
	}
}

func TestProviderOwnedInactiveMembershipAndFailures(t *testing.T) {
	csr, group, nodes := newAccountingRegistry(t)
	now := time.Now()
	group.view.Instances[2].Status = &cloudprovider.InstanceStatus{
		State:     cloudprovider.InstanceCreating,
		ErrorInfo: &cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: "resume-failed"},
	}
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes[:2], now))
	assert.Empty(t, csr.GetUnregisteredNodes(), "Node-less inactive member is not failed creation")
	assert.Empty(t, csr.GetCreatedNodesWithErrors())
	group.view.Instances = append(group.view.Instances, cloudprovider.Instance{
		Id: "new-failed", Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating,
			ErrorInfo: &cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: "fresh-failed"}},
	})
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes[:2], now))
	require.Len(t, csr.GetCreatedNodesWithErrors()["ng"], 1)
	assert.Equal(t, "new-failed", csr.GetCreatedNodesWithErrors()["ng"][0].Spec.ProviderID)
}

func TestProviderOwnedBoundedAdmission(t *testing.T) {
	csr, group, nodes := newAccountingRegistry(t)
	now := time.Now()
	group.view.TargetSize, group.view.UpcomingInactiveNodes = 5, 1
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now))
	upcoming, _ := csr.GetUpcomingNodes(t.Context())
	assert.Equal(t, 1, upcoming["ng"], "partial acceptance does not admit an arbitrary fresh deficit")
	assert.False(t, csr.HasNodeGroupStartedScaleUp("ng"), "snapshot must not fabricate a request")
	csr.RegisterFailedScaleUp(t.Context(), group, 1, cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: "failed"}, now)
	upcoming, _ = csr.GetUpcomingNodes(t.Context())
	assert.Empty(t, upcoming)
	group.view.UpcomingInactiveNodes, group.view.UnavailableTargetNodes = 0, 1
	csr.RegisterScaleUp(t.Context(), group, 2, now.Add(time.Minute))
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now.Add(time.Minute)))
	assert.Equal(t, 2, csr.calculateUpcomingNodes("ng", csr.perNodeGroupReadiness["ng"], csr.acceptableRanges["ng"]))
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes, now.Add(7*time.Minute)))
	assert.Equal(t, -2, group.decrease, "only the ordinary aggregate request expires")
	assert.Equal(t, 5, group.view.TargetSize, "read-only reconciliation does not cancel accepted reservations")
}

func TestProviderOwnedUnknownObservation(t *testing.T) {
	csr, group, nodes := newAccountingRegistry(t)
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes, time.Now()))
	group.err = fmt.Errorf("unknown ownership")
	require.ErrorContains(t, csr.UpdateNodes(t.Context(), nodes, time.Now()), "unknown ownership")
}

func TestProviderOwnedExpiredFreshReservation(t *testing.T) {
	csr, group, nodes := newAccountingRegistry(t)
	now := time.Now()
	group.view.TargetSize = 3
	group.view.UnavailableTargetNodes = 1
	group.view.InactiveInstanceIDs = nil
	group.view.Instances = group.view.Instances[:2]
	csr.RegisterScaleUp(t.Context(), group, 1, now.Add(-10*time.Minute))
	csr.RegisterFailedScaleUp(t.Context(), group, 1, cloudprovider.InstanceErrorInfo{
		ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: "expired-fresh",
	}, now)
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes[:2], now))
	assert.False(t, csr.HasNodeGroupStartedScaleUp("ng"))
	assert.False(t, csr.IsNodeGroupAtTargetSize("ng"), "withheld target is not fulfilled capacity")
	assert.Zero(t, group.decrease, "provider-owned expired work is not canceled by aggregate completion")
	group.view.TargetSize = 4
	csr.RegisterScaleUp(t.Context(), group, 1, now.Add(10*time.Minute))
	require.NoError(t, csr.UpdateNodes(t.Context(), nodes[:2], now.Add(10*time.Minute)))
	assert.Equal(t, 1, csr.calculateUpcomingNodes("ng", csr.perNodeGroupReadiness["ng"], csr.acceptableRanges["ng"]))
	upper, err := csr.NodesForUpperLimits(t.Context(), nodes[:2], map[string]*framework.NodeInfo{"ng": framework.NewTestNodeInfo(nodes[0])})
	require.NoError(t, err)
	assert.Len(t, upper, 4, "old unavailable capacity still consumes upper limits")
}

func TestProviderOwnedUpperLimitsReadsCurrentTargets(t *testing.T) {
	for _, snapshot := range []bool{true, false} {
		t.Run(fmt.Sprint(snapshot), func(t *testing.T) {
			csr, group, nodes := newAccountingRegistry(t)
			if !snapshot {
				group.view = nil
			}
			require.NoError(t, csr.UpdateNodes(t.Context(), nodes, time.Now()))
			if snapshot {
				view := *group.view
				view.TargetSize, view.UpcomingInactiveNodes = 3, 1
				group.view = &view
			} else {
				group.NodeGroup.(*testprovider.TestNodeGroup).SetTargetSize(3)
			}
			templates := map[string]*framework.NodeInfo{"ng": framework.NewTestNodeInfo(nodes[0])}
			upper, err := csr.NodesForUpperLimits(t.Context(), nodes[:2], templates)
			require.NoError(t, err)
			assert.Len(t, upper, 3, "accepted target must not wait for CSR recalculation")
			group.err = fmt.Errorf("unknown accepted target")
			upper, err = csr.NodesForUpperLimits(t.Context(), nodes[:2], templates)
			require.ErrorContains(t, err, "unknown accepted target")
			assert.Nil(t, upper, "failed reads must not reuse cached headroom")
		})
	}
}
