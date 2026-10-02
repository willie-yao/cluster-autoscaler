/*
Copyright The Kubernetes Authors.

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

package inmemory

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestScaleUp_SuspendedNodeLimits(t *testing.T) {
	testCases := []struct {
		name             string
		maxNodesTotal    int
		maxLimits        map[string]int64
		useReadyTemplate bool
		atomic           bool
	}{
		{name: "nodes", maxNodesTotal: 1},
		{name: "ready suspended template", maxNodesTotal: 1, useReadyTemplate: true},
		{name: "atomic node group", maxNodesTotal: 1, atomic: true},
		{name: "cores", maxLimits: map[string]int64{cloudprovider.ResourceNameCores: 1}},
		{name: "memory", maxLimits: map[string]int64{cloudprovider.ResourceNameMemory: units.GiB}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			options := integration.NewTestConfig().WithOverrides(func(o *config.AutoscalingOptions) {
				o.MaxNodesTotal = tc.maxNodesTotal
				o.ScaleDownEnabled = false
				o.NodeGroupDefaults.MaxNodeStartupTime = 15 * time.Minute
				o.NodeGroupDefaults.MaxNodeProvisionTime = 15 * time.Minute
			}).ResolveOptions()
			infra := integration.SetupInfrastructure(t)
			fakes := infra.Fakes

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer synctestutils.TearDown(cancel)

				nodes := []*apiv1.Node{
					test.BuildTestNode("ng-0", 1000, units.GiB),
					test.BuildTestNode("ng-1", 1000, units.GiB),
				}
				for i, node := range nodes {
					test.SetNodeReadyState(node, tc.useReadyTemplate && i == 1, time.Now().Add(-time.Hour))
					test.SetNodeCondition(node, kube_util.NodeSuspended, apiv1.ConditionTrue, time.Now().Add(-time.Hour))
					node.Spec.Taints = []apiv1.Taint{{
						Key:    "status-taint.cluster-autoscaler.kubernetes.io/suspended",
						Effect: apiv1.TaintEffectNoSchedule,
					}}
					fakes.K8s.AddNode(node)
				}
				resumed := 0
				var provider *testprovider.TestCloudProvider
				builder := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(id string, delta int) error {
					require.Equal(t, 1, delta)
					require.Less(t, resumed, len(nodes))
					node := nodes[resumed].DeepCopy()
					test.SetNodeCondition(node, kube_util.NodeSuspended, apiv1.ConditionFalse, time.Now())
					fakes.K8s.UpdateNode(node)
					resumed++
					// Resuming a VM does not change the total including suspended VMs.
					provider.GetNodeGroup(id).(*testprovider.TestNodeGroup).SetTargetSize(len(nodes))
					return nil
				})
				if !tc.useReadyTemplate {
					builder.WithMachineTemplates(map[string]*framework.NodeInfo{
						"ng": framework.NewTestNodeInfo(test.BuildTestNode("template", 1000, units.GiB, test.IsReady(true))),
					})
				}
				provider = builder.Build()
				groupOptions := options.NodeGroupDefaults
				groupOptions.ZeroOrMaxNodeScaling = tc.atomic
				provider.AddNodeGroupWithCustomOptions("ng", 0, len(nodes)+1, len(nodes), &groupOptions)
				for _, node := range nodes {
					provider.AddNode("ng", node)
				}
				provider.SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, tc.maxLimits))
				autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).WithCloudProvider(provider).Build(ctx)
				require.NoError(t, err)
				fakes.K8s.AddPod(test.BuildTestPod("p1", 600, 100, test.MarkUnschedulable()))
				fakes.K8s.AddPod(test.BuildTestPod("p2", 600, 100, test.MarkUnschedulable()))

				synctestutils.MustRunOnceAfter(t, autoscaler, 10*time.Second)
				assert.Equal(t, 1, resumed, "suspended nodes must not use the scale-up limit")
				for range 3 {
					synctestutils.MustRunOnceAfter(t, autoscaler, 10*time.Second)
				}
				assert.Equal(t, 1, resumed, "the resuming node must use the scale-up limit")

				for _, node := range fakes.K8s.Nodes().Items {
					if node.Name == nodes[0].Name {
						test.SetNodeReadyState(&node, true, time.Now())
						node.Spec.Taints = nil
						fakes.K8s.UpdateNode(&node)
					}
				}
				synctestutils.MustRunOnceAfter(t, autoscaler, 10*time.Second)
				assert.Equal(t, 1, resumed, "the ready node must use the scale-up limit")
			})
		})
	}
}
