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

package actuation

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/budgets"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/deletiontracker"
	coretest "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/observers/nodegroupchange"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
	kubeutil "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

type providerOwnedActuationGroup struct {
	cloudprovider.NodeGroup
	mu          sync.Mutex
	client      *fake.Clientset
	view        *cloudprovider.NodeGroupAccountingSnapshot
	unknown     bool
	protected   map[string]bool
	deleteCalls int
	onDelete    func([]*apiv1.Node) error
}

func (g *providerOwnedActuationGroup) SuspendedNodesIncludedInTargetSize() bool { return false }
func (g *providerOwnedActuationGroup) GetNodeGroupAccounting(context.Context) (*cloudprovider.NodeGroupAccountingSnapshot, error) {
	if g.unknown {
		return nil, fmt.Errorf("unknown provider state")
	}
	return g.view, nil
}
func (g *providerOwnedActuationGroup) MarkToBeDeleted(ctx context.Context, node *apiv1.Node, cordon bool) (*apiv1.Node, bool, error) {
	g.mu.Lock()
	blocked := cordon || g.unknown || g.protected[node.Name]
	g.mu.Unlock()
	if blocked {
		return nil, true, fmt.Errorf("node cannot be marked")
	}
	updated, err := taints.MarkToBeDeleted(ctx, node, g.client, false)
	return updated, true, err
}
func (g *providerOwnedActuationGroup) CleanToBeDeleted(ctx context.Context, node *apiv1.Node, _ bool) (*apiv1.Node, bool, error) {
	if g.unknown {
		return nil, true, fmt.Errorf("unknown ownership")
	}
	if g.protected[node.Name] {
		return node, true, nil
	}
	updated, err := taints.CleanToBeDeleted(ctx, node, g.client, false)
	return updated, true, err
}
func (g *providerOwnedActuationGroup) DeleteNodes(_ context.Context, nodes []*apiv1.Node) error {
	g.deleteCalls++
	if g.onDelete != nil {
		return g.onDelete(nodes)
	}
	return nil
}

type providerOwnedActuationProvider struct {
	cloudprovider.CloudProvider
	group *providerOwnedActuationGroup
}

func (p *providerOwnedActuationProvider) NodeGroupForNode(context.Context, *apiv1.Node) (cloudprovider.NodeGroup, error) {
	return p.group, nil
}
func (p *providerOwnedActuationProvider) NodeGroups(context.Context) []cloudprovider.NodeGroup {
	return []cloudprovider.NodeGroup{p.group}
}

func providerOwnedActuationFixture(t *testing.T, count int) (*Actuator, *providerOwnedActuationGroup, []*apiv1.Node) {
	t.Helper()
	group := &providerOwnedActuationGroup{
		NodeGroup: sizedNodeGroup("group", count, false, false),
		view:      &cloudprovider.NodeGroupAccountingSnapshot{TargetSize: count}, protected: make(map[string]bool),
	}
	var nodes []*apiv1.Node
	var objects []runtime.Object
	for i := range count {
		node := generateNode(fmt.Sprintf("node-%d", i))
		node.UID, node.Spec.ProviderID = types.UID(node.Name), "provider://"+node.Name
		node.Spec.Unschedulable = true
		node.Spec.Taints = []apiv1.Taint{{Key: "admin", Effect: apiv1.TaintEffectNoSchedule}}
		nodes, objects = append(nodes, node), append(objects, node)
		group.view.Instances = append(group.view.Instances, cloudprovider.Instance{Id: node.Spec.ProviderID})
	}
	group.client = fake.NewSimpleClientset(objects...)
	provider := &providerOwnedActuationProvider{CloudProvider: testprovider.NewTestCloudProviderBuilder().Build(), group: group}
	registry := kubeutil.NewListerRegistry(kubeutil.NewTestNodeLister(nodes), nil, kubeutil.NewTestPodLister(nil), nil, nil, nil, nil, nil, nil)
	autoscalingCtx, err := coretest.NewScaleTestAutoscalingContext(config.AutoscalingOptions{
		MaxGracefulTerminationSec: 1,
	}, group.client, registry, provider, nil, nil, nil)
	require.NoError(t, err)
	a := NewActuator(&autoscalingCtx, nodegroupchange.NewNodeGroupChangeObserversList(),
		deletiontracker.NewNodeDeletionTracker(0), options.NodeDeleteOptions{}, nil, nil)
	return a, group, nodes
}

func assertProviderOwnedTaint(t *testing.T, group *providerOwnedActuationGroup, node *apiv1.Node, expected bool) {
	t.Helper()
	fresh, err := group.client.CoreV1().Nodes().Get(t.Context(), node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, expected, taints.HasToBeDeletedTaint(fresh))
	require.True(t, fresh.Spec.Unschedulable)
	require.Equal(t, node.Spec.Taints[0], fresh.Spec.Taints[0])
}

func TestProviderOwnedTaintFailureCleanup(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			a, group, nodes := providerOwnedActuationFixture(t, 2)
			group.client.PrependReactor("update", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
				node := action.(clienttesting.UpdateAction).GetObject().(*apiv1.Node)
				if node.Name == nodes[1].Name {
					return true, nil, fmt.Errorf("mark denied")
				}
				if accepted && taints.HasToBeDeletedTaint(node) {
					group.mu.Lock()
					group.protected[node.Name] = true
					group.mu.Unlock()
				}
				return false, nil, nil
			})
			_, err := a.taintNodesSync(t.Context(), []*budgets.NodeGroupView{{Group: group, Nodes: nodes}})
			require.Error(t, err)
			assertProviderOwnedTaint(t, group, nodes[0], accepted)
			assertProviderOwnedTaint(t, group, nodes[1], false)
		})
	}
}

func TestProviderOwnedPartialFailureCleanup(t *testing.T) {
	for _, outcome := range []string{"partial", "unknown", "policy removed", "rejected"} {
		t.Run(outcome, func(t *testing.T) {
			a, group, nodes := providerOwnedActuationFixture(t, 2)
			for _, node := range nodes {
				require.NoError(t, a.taintNode(t.Context(), node))
				a.nodeDeletionTracker.StartDeletion(group.Id(), node.Name)
			}
			group.onDelete = func([]*apiv1.Node) error {
				switch outcome {
				case "partial":
					group.protected[nodes[0].Name] = true
				case "unknown":
					group.unknown = true
				case "policy removed":
					group.view = nil
					for _, node := range nodes {
						group.protected[node.Name] = true
					}
				}
				return fmt.Errorf("provider failure")
			}
			batcher := NewNodeDeletionBatcher(a.autoscalingCtx, nodegroupchange.NewNodeGroupChangeObserversList(), a.nodeDeletionTracker, 0)
			batcher.deleteNodesAndRegisterStatus(t.Context(), nodes, group.Id(), false)
			require.Equal(t, 1, group.deleteCalls)
			results, _ := a.nodeDeletionTracker.DeletionResults()
			require.Len(t, results, 2)
			for i, node := range nodes {
				assertProviderOwnedTaint(t, group, node, outcome == "unknown" || outcome == "policy removed" || outcome == "partial" && i == 0)
			}
		})
	}
}

func TestProviderOwnedDrainFailure(t *testing.T) {
	for _, outcome := range []string{"unaccepted", "accepted during drain", "unknown during drain"} {
		t.Run(outcome, func(t *testing.T) {
			a, group, nodes := providerOwnedActuationFixture(t, 1)
			node := nodes[0]
			require.NoError(t, a.taintNode(t.Context(), node))
			a.nodeDeletionTracker.StartDeletionWithDrain(group.Id(), node.Name)
			evictions := 0
			group.client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				require.Equal(t, "eviction", action.GetSubresource())
				evictions++
				group.protected[node.Name] = outcome == "accepted during drain"
				group.unknown = outcome == "unknown during drain"
				return true, nil, apierrors.NewTooManyRequests("PDB blocks eviction", 0)
			})
			batcher := &countingBatcher{}
			evictor := NewEvictor(nil, SingleRuleDrainConfig(1), false)
			evictor.EvictionRetryTime = 0
			scheduler := NewGroupDeletionScheduler(a.autoscalingCtx, a.nodeDeletionTracker, batcher, evictor)
			scheduler.ScheduleDeletion(t.Context(), framework.NewTestNodeInfo(node, removablePod("pod", node.Name)), group, 1, true)
			require.Zero(t, batcher.addedNodes)
			require.Zero(t, group.deleteCalls)
			require.Equal(t, 1, evictions)
			assertProviderOwnedTaint(t, group, node, outcome != "unaccepted")
		})
	}
}

func TestProviderOwnedActuationSnapshot(t *testing.T) {
	a, group, nodes := providerOwnedActuationFixture(t, 3)
	group.view.TargetSize = 2
	group.view.InactiveInstanceIDs = []string{nodes[1].Spec.ProviderID, nodes[2].Spec.ProviderID}
	group.view.UpcomingInactiveNodes = 1
	nodes[1].Spec.Taints = append(nodes[1].Spec.Taints, apiv1.Taint{
		Key: taints.ToBeDeletedTaint, Value: fmt.Sprint(time.Now().Add(-24 * time.Hour).Unix()), Effect: apiv1.TaintEffectNoSchedule,
	})
	snapshot, err := a.createSnapshot(t.Context(), nodes)
	require.NoError(t, err)
	_, err = snapshot.GetNodeInfo(nodes[0].Name)
	require.NoError(t, err)
	for _, node := range nodes[1:] {
		_, err = snapshot.GetNodeInfo(node.Name)
		require.Error(t, err)
	}
	group.unknown = true
	_, err = a.createSnapshot(t.Context(), nodes)
	require.Error(t, err)
}
