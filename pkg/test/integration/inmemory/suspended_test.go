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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

const suspendedTestStep = 10 * time.Second

// suspendedNode returns a suspended Node of 1 core and 1 GiB. A Node whose
// VM was just stopped can still look Ready.
func suspendedNode(name string, ready bool) *apiv1.Node {
	node := test.BuildTestNode(name, 1000, units.GiB)
	test.SetNodeReadyState(node, ready, time.Now().Add(-time.Hour))
	test.SetNodeCondition(node, kube_util.NodeSuspended, apiv1.ConditionTrue, time.Now().Add(-time.Hour))
	node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{Key: "status-taint.cluster-autoscaler.kubernetes.io/suspended", Effect: apiv1.TaintEffectNoSchedule})
	return node
}

// providerTemplate returns a template of 1 core and 1 GiB.
func providerTemplate() *framework.NodeInfo {
	return framework.NewTestNodeInfo(test.BuildTestNode("ng-template", 1000, units.GiB, test.IsReady(true)))
}

// newSuspendedNodeGroup adds node group "ng" with maxSize and the suspended
// nodes to a new test cloud provider. The target size counts suspended Nodes,
// and IncreaseSize resumes suspended Nodes in the order of nodes instead of
// adding Nodes, like a provider that suspends VMs. Without a template, the core
// builds one from the Nodes.
func newSuspendedNodeGroup(t *testing.T, fakes *integration.FakeSet, nodes []*apiv1.Node, maxSize int, template *framework.NodeInfo) *testprovider.TestCloudProvider {
	t.Helper()
	var provider *testprovider.TestCloudProvider
	builder := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(id string, delta int) error {
		if delta <= 0 {
			return nil
		}
		group := provider.GetNodeGroup(id).(*testprovider.TestNodeGroup)
		current := map[string]apiv1.Node{}
		for _, node := range fakes.K8s.Nodes().Items {
			current[node.Name] = node
		}
		for _, suspended := range nodes {
			node := current[suspended.Name]
			if delta == 0 {
				break
			}
			if kube_util.IsNodeSuspended(&node) {
				test.SetNodeCondition(&node, kube_util.NodeSuspended, apiv1.ConditionFalse, time.Now())
				fakes.K8s.UpdateNode(&node)
				size, _ := group.TargetSize(context.Background())
				group.SetTargetSize(size - 1)
				delta--
			}
		}
		if delta != 0 {
			return fmt.Errorf("no suspended node to resume in %s", id)
		}
		return nil
	})
	if template != nil {
		builder = builder.WithMachineTemplates(map[string]*framework.NodeInfo{"ng": template})
	}
	provider = builder.Build()
	provider.AddNodeGroup("ng", 0, maxSize, len(nodes))
	for _, node := range nodes {
		fakes.K8s.AddNode(node)
		provider.AddNode("ng", node)
	}
	return provider
}

// countResumed returns how many Nodes have the condition "Suspended=False".
func countResumed(fakes *integration.FakeSet) int {
	count := 0
	for _, node := range fakes.K8s.Nodes().Items {
		for _, condition := range node.Status.Conditions {
			if condition.Type == kube_util.NodeSuspended && condition.Status == apiv1.ConditionFalse {
				count++
			}
		}
	}
	return count
}

// markResumedReady makes every resumed Node Ready.
func markResumedReady(fakes *integration.FakeSet) {
	for _, node := range fakes.K8s.Nodes().Items {
		for _, condition := range node.Status.Conditions {
			if condition.Type == kube_util.NodeSuspended && condition.Status == apiv1.ConditionFalse {
				test.SetNodeReadyState(&node, true, time.Now())
				node.Spec.Taints = nil
				fakes.K8s.UpdateNode(&node)
			}
		}
	}
}

func suspendedTestOptions(overrides ...integration.AutoscalingOptionOverride) config.AutoscalingOptions {
	return integration.NewTestConfig().WithOverrides(append([]integration.AutoscalingOptionOverride{
		func(o *config.AutoscalingOptions) {
			o.NodeGroupDefaults.MaxNodeProvisionTime = 15 * time.Minute
			o.NodeGroupDefaults.MaxNodeStartupTime = 15 * time.Minute
			o.ScaleDownEnabled = false
		},
	}, overrides...)...).ResolveOptions()
}

func TestScaleUp_MaxNodesTotalWithSuspendedNodes(t *testing.T) {
	options := suspendedTestOptions(func(o *config.AutoscalingOptions) { o.MaxNodesTotal = 1 })
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		provider := newSuspendedNodeGroup(t, fakes, []*apiv1.Node{suspendedNode("ng-0", false), suspendedNode("ng-1", false)}, 3, providerTemplate())
		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).WithCloudProvider(provider).Build(ctx)
		require.NoError(t, err)
		// Each Pod needs a whole Node.
		fakes.K8s.AddPod(test.BuildTestPod("p1", 600, 100, test.MarkUnschedulable()))
		fakes.K8s.AddPod(test.BuildTestPod("p2", 600, 100, test.MarkUnschedulable()))

		// The two suspended Nodes fill max-nodes-total, but only one Node may resume.
		synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
		assert.Equal(t, 1, countResumed(fakes))
		size, _ := provider.GetNodeGroup("ng").TargetSize(ctx)
		assert.Equal(t, 2, size)

		// The resuming Node counts, so the second Pod doesn't resume another Node.
		for range 3 {
			synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
		}
		assert.Equal(t, 1, countResumed(fakes), "a resuming Node must count against max-nodes-total")

		markResumedReady(fakes)
		synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
		assert.Equal(t, 1, countResumed(fakes), "a resumed Node must count against max-nodes-total")

		autoscaler.(*core.StaticAutoscaler).MaxNodesTotal = 2
		synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
		assert.Equal(t, 2, countResumed(fakes), "the last suspended Node must not count against max-nodes-total")
	})
}

// TestScaleUp_MaxNodesTotalWithSuspendedTemplateNode uses a suspended Node that
// still looks Ready as the node group's template. The upcoming Node that stands
// for a resuming Node must still count against max-nodes-total.
func TestScaleUp_MaxNodesTotalWithSuspendedTemplateNode(t *testing.T) {
	options := suspendedTestOptions(func(o *config.AutoscalingOptions) { o.MaxNodesTotal = 1 })
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		// The resume starts ng-1, so ng-0 stays the only template candidate.
		nodes := []*apiv1.Node{suspendedNode("ng-1", false), suspendedNode("ng-0", true)}
		provider := newSuspendedNodeGroup(t, fakes, nodes, 3, nil)
		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).WithCloudProvider(provider).Build(ctx)
		require.NoError(t, err)
		fakes.K8s.AddPod(test.BuildTestPod("p1", 600, 100, test.MarkUnschedulable()))
		fakes.K8s.AddPod(test.BuildTestPod("p2", 600, 100, test.MarkUnschedulable()))

		synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
		assert.Equal(t, 1, countResumed(fakes))
		for range 3 {
			synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
		}
		assert.Equal(t, 1, countResumed(fakes), "a resuming Node must count against max-nodes-total")
	})
}

func TestScaleUp_ResourceLimitsWithSuspendedNodes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		maxLimits map[string]int64
	}{
		{name: "cores", maxLimits: map[string]int64{cloudprovider.ResourceNameCores: 1, cloudprovider.ResourceNameMemory: 1000 * units.GiB}},
		{name: "memory", maxLimits: map[string]int64{cloudprovider.ResourceNameCores: 1000, cloudprovider.ResourceNameMemory: units.GiB}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := suspendedTestOptions()
			infra := integration.SetupInfrastructure(t)
			fakes := infra.Fakes

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer synctestutils.TearDown(cancel)

				provider := newSuspendedNodeGroup(t, fakes, []*apiv1.Node{suspendedNode("ng-0", false), suspendedNode("ng-1", false)}, 3, providerTemplate())
				minLimits := map[string]int64{cloudprovider.ResourceNameCores: 0, cloudprovider.ResourceNameMemory: 0}
				provider.SetResourceLimiter(cloudprovider.NewResourceLimiter(minLimits, tc.maxLimits))
				autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).WithCloudProvider(provider).Build(ctx)
				require.NoError(t, err)
				fakes.K8s.AddPod(test.BuildTestPod("p1", 600, 100, test.MarkUnschedulable()))
				fakes.K8s.AddPod(test.BuildTestPod("p2", 600, 100, test.MarkUnschedulable()))

				// The suspended Nodes exceed the limit, but one Node fits in it.
				synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
				assert.Equal(t, 1, countResumed(fakes))

				for range 3 {
					synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
				}
				assert.Equal(t, 1, countResumed(fakes), "a resuming Node must count against the limit")
			})
		})
	}
}

// TestScaleDown_MinCoresWithSuspendedNodes records how the minimum cores limit
// treats suspended Nodes. It doesn't define the intended behavior.
func TestScaleDown_MinCoresWithSuspendedNodes(t *testing.T) {
	options := integration.NewTestConfig().WithOverrides(integration.WithScaleDownUnneededTime(time.Minute)).ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		require.NoError(t, err)
		template := test.BuildTestNode("ng-template", 1000, units.GiB, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng", testprovider.WithNodes(template, 2))
		// Park the second Node the way a provider that suspends VMs does.
		for _, node := range fakes.K8s.Nodes().Items {
			if node.Name == "ng-node-1" {
				test.SetNodeReadyState(&node, false, time.Now())
				test.SetNodeCondition(&node, kube_util.NodeSuspended, apiv1.ConditionTrue, time.Now())
				node.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/scale-down-disabled": "true"}
				fakes.K8s.UpdateNode(&node)
			}
		}
		// The empty active Node is unneeded. With the suspended Node, the
		// cluster has 2 cores, one above the minimum.
		fakes.CloudProvider.SetResourceLimit(cloudprovider.ResourceNameCores, 1, 1000)

		synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)
		synctestutils.MustRunOnceAfter(t, autoscaler, time.Minute+time.Nanosecond)
		synctestutils.MustRunOnceAfter(t, autoscaler, suspendedTestStep)

		// The suspended Node doesn't count toward the minimum, so the active
		// Node is kept.
		size, _ := fakes.CloudProvider.GetNodeGroup("ng").TargetSize(ctx)
		assert.Equal(t, 2, size)
		assert.Len(t, fakes.K8s.Nodes().Items, 2)
	})
}
