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

package core

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
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/actuation"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/deletiontracker"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/orchestrator"
	coretest "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/observers/loopstart"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupconfig"
	processorstest "sigs.k8s.io/cluster-autoscaler/pkg/processors/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

type retentionStaticGroup struct {
	cloudprovider.NodeGroup
	snapshot        *cloudprovider.NodeGroupAccountingSnapshot
	reconciliations int
	cleanupErr      error
}

func (g *retentionStaticGroup) SuspendedNodesIncludedInTargetSize() bool { return false }
func (g *retentionStaticGroup) GetNodeGroupAccounting(context.Context) (*cloudprovider.NodeGroupAccountingSnapshot, error) {
	return g.snapshot, nil
}
func (g *retentionStaticGroup) DecreaseTargetSize(context.Context, int) error {
	g.reconciliations++
	return nil
}
func (g *retentionStaticGroup) MarkToBeDeleted(context.Context, *apiv1.Node, bool) (*apiv1.Node, bool, error) {
	return nil, true, fmt.Errorf("test group does not accept deletion")
}
func (g *retentionStaticGroup) CleanToBeDeleted(_ context.Context, node *apiv1.Node, _ bool) (*apiv1.Node, bool, error) {
	return node, true, g.cleanupErr
}

func TestRetentionReconciliationDoesNotBlockOtherGroups(t *testing.T) {
	now := time.Now()
	grown := 0
	provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(group string, delta int) error {
		assert.Equal(t, "b", group)
		grown += delta
		return nil
	}).Build()
	a := &retentionStaticGroup{
		NodeGroup: provider.BuildNodeGroup("a", 0, 10, 3, true, false, "", &config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: time.Second}),
		snapshot: &cloudprovider.NodeGroupAccountingSnapshot{TargetSize: 3,
			Instances:           []cloudprovider.Instance{{Id: "a"}, {Id: "resume"}},
			InactiveInstanceIDs: []string{"resume"}, UpcomingInactiveNodes: 1},
	}
	provider.InsertNodeGroup(a)
	provider.AddNodeGroup("b", 0, 10, 1)
	var nodes []*apiv1.Node
	var scheduled []*apiv1.Pod
	for _, name := range []string{"a", "resume", "b"} {
		node := testutils.BuildTestNode(name, 2000, units.GiB, testutils.IsReady(true))
		node.UID, node.Spec.ProviderID = types.UID(name), name
		node.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
		group := "a"
		if name == "b" {
			group = "b"
		}
		provider.AddNode(group, node)
		nodes = append(nodes, node)
		if name != "resume" {
			scheduled = append(scheduled, testutils.BuildTestPod("busy-"+name, 1800, units.GiB*8/10, testutils.WithNodeName(name)))
		}
	}
	client := fake.NewSimpleClientset(nodes[0], nodes[1], nodes[2])
	pods := &podListerMock{}
	initialPods := pods.On("List").Return(scheduled, nil)
	dsLister, err := kube_util.NewTestDaemonSetLister(nil)
	require.NoError(t, err)
	listers := kube_util.NewListerRegistry(kube_util.NewTestNodeLister(nodes), kube_util.NewTestNodeLister(nodes),
		pods, kube_util.NewTestPodDisruptionBudgetLister(nil), dsLister, nil, nil, nil, nil)
	opts := config.AutoscalingOptions{
		EstimatorName: estimator.BinpackingEstimatorName, MaxNodeGroupBinpackingDuration: time.Second,
		MaxCoresTotal: 100, MaxMemoryTotal: 100 * units.GiB,
		NodeGroupDefaults: config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 5 * time.Minute},
	}
	callbacks := newStaticAutoscalerProcessorCallbacks()
	processors, templates := processorstest.NewTestProcessors(opts)
	autoscalingCtx, err := coretest.NewScaleTestAutoscalingContext(opts, client, listers, provider, callbacks, nil, templates)
	require.NoError(t, err)
	require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(t.Context(), nodes, scheduled, nil, nil))
	require.NoError(t, templates.Recompute(t.Context(), &autoscalingCtx, nodes, nil, taints.TaintConfig{}, now))
	csr := clusterstate.NewNotifiedClusterStateRegistry(provider, autoscalingCtx.LogRecorder, coretest.NewBackoff(),
		nodegroupconfig.NewDefaultNodeGroupConfigProcessor(opts.NodeGroupDefaults), templates)
	csr.RegisterScaleUp(t.Context(), a, 2, now.Add(-time.Minute))
	actuator := actuation.NewActuator(&autoscalingCtx, csr, deletiontracker.NewNodeDeletionTracker(0), options.NodeDeleteOptions{}, nil, processors.NodeGroupConfigProcessor)
	autoscalingCtx.ScaleDownActuator = actuator
	su := orchestrator.New()
	su.Initialize(&autoscalingCtx, processors, csr, newEstimatorBuilder(), taints.TaintConfig{},
		resourcequotas.NewTrackerFactory(resourcequotas.TrackerOptions{
			QuotaProvider: resourcequotas.NewCloudQuotasProvider(provider), CustomResourcesProcessor: processors.CustomResourcesProcessor,
		}))
	autoscalingCtx.ExpanderStrategy = coretest.NewMockReportingStrategy(t, nil, nil)
	autoscaler := &StaticAutoscaler{
		AutoscalingContext: &autoscalingCtx, clusterStateRegistry: csr, scaleDownActuator: actuator,
		scaleDownPlanner: &candidateTrackingFakePlanner{}, scaleUpOrchestrator: su, processors: processors,
		processorCallbacks: callbacks, loopStartNotifier: loopstart.NewObserversList(nil),
	}
	require.NoError(t, autoscaler.RunOnce(t.Context(), now))
	require.NotNil(t, csr.GetIncorrectNodeGroupSize("a"))
	a.snapshot.UpcomingInactiveNodes, a.snapshot.UnavailableTargetNodes = 0, 2
	csr.RegisterFailedScaleUp(t.Context(), a, 2, cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: "expired"}, now)
	initialPods.Unset()
	pending := testutils.BuildTestPod("pending", 1800, units.GiB*8/10, testutils.MarkUnschedulable())
	pods.On("List").Return(append(scheduled, pending), nil)
	require.NoError(t, autoscaler.RunOnce(t.Context(), now.Add(2*time.Second)))
	assert.Equal(t, 1, grown)
	require.NoError(t, autoscaler.RunOnce(t.Context(), now.Add(3*time.Second)))
	assert.GreaterOrEqual(t, a.reconciliations, 2)
	assert.Equal(t, 3, a.snapshot.TargetSize)
}

func TestRetentionStaticFiltering(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			now := time.Now()
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			group := &retentionStaticGroup{
				NodeGroup: provider.BuildNodeGroup("ng", 0, 10, 1, true, false, "", nil),
				snapshot: &cloudprovider.NodeGroupAccountingSnapshot{TargetSize: 1,
					Instances:           []cloudprovider.Instance{{Id: "active"}, {Id: "retained"}, {Id: "retiring"}},
					InactiveInstanceIDs: []string{"retained", "retiring"}},
			}
			provider.InsertNodeGroup(group)
			var nodes []*apiv1.Node
			for _, name := range []string{"active", "retained", "retiring"} {
				node := testutils.BuildTestNode(name, 2000, 1000, testutils.IsReady(true))
				node.Spec.ProviderID = name
				node.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
				provider.AddNode("ng", node)
				nodes = append(nodes, node)
			}
			nodes[1].Spec.Taints = []apiv1.Taint{{Key: taints.ToBeDeletedTaint, Value: "1", Effect: apiv1.TaintEffectNoSchedule}}
			nodes[1].Spec.Unschedulable = true
			if unknown {
				group.cleanupErr = fmt.Errorf("unknown ownership")
			}
			client := fake.NewSimpleClientset(nodes[0], nodes[1], nodes[2])
			dsLister, err := kube_util.NewTestDaemonSetLister(nil)
			require.NoError(t, err)
			listers := kube_util.NewListerRegistry(
				kube_util.NewTestNodeLister(nodes), kube_util.NewTestNodeLister(nodes), kube_util.NewTestPodLister(nil),
				kube_util.NewTestPodDisruptionBudgetLister(nil), dsLister, nil, nil, nil, nil)
			opts := config.AutoscalingOptions{ScaleDownEnabled: true, MaxNodesTotal: 2}
			callbacks := newStaticAutoscalerProcessorCallbacks()
			processors, templates := processorstest.NewTestProcessors(opts)
			autoscalingCtx, err := coretest.NewScaleTestAutoscalingContext(opts, client, listers, provider, callbacks, nil, templates)
			require.NoError(t, err)
			csr := clusterstate.NewClusterStateRegistry(provider, autoscalingCtx.LogRecorder, coretest.NewBackoff(),
				nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 5 * time.Minute}), templates)
			actuator := actuation.NewActuator(&autoscalingCtx, csr, deletiontracker.NewNodeDeletionTracker(0), options.NodeDeleteOptions{}, nil, processors.NodeGroupConfigProcessor)
			autoscalingCtx.ScaleDownActuator = actuator
			planner := &candidateTrackingFakePlanner{}
			autoscaler := &StaticAutoscaler{
				AutoscalingContext: &autoscalingCtx, clusterStateRegistry: csr, scaleDownActuator: actuator,
				scaleDownPlanner: planner, processors: processors, processorCallbacks: callbacks, loopStartNotifier: loopstart.NewObserversList(nil),
			}
			err = autoscaler.RunOnce(t.Context(), now)
			if unknown {
				require.ErrorContains(t, err, "unknown ownership")
			} else {
				require.NoError(t, err)
				assert.Equal(t, map[string]bool{"active": true}, planner.lastCandidateNodes)
				assertSnapshotNodeCount(t, autoscaler.ClusterSnapshot, 1)
				assert.Len(t, csr.GetClusterReadiness().Registered, 3)
				require.NoError(t, autoscaler.RunOnce(t.Context(), now.Add(time.Hour)))
				assert.Equal(t, map[string]bool{"active": true}, planner.lastCandidateNodes)
			}
			node, err := client.CoreV1().Nodes().Get(t.Context(), "retained", metav1.GetOptions{})
			require.NoError(t, err)
			assert.True(t, taints.HasToBeDeletedTaint(node))
			assert.True(t, node.Spec.Unschedulable)
		})
	}
}
