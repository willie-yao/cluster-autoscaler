/*
Copyright 2025 The Kubernetes Authors.

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
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	fakecloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	synctestutils "sigs.k8s.io/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	unneededTime = 1 * time.Minute
)

func TestStaticAutoscaler_FullLifecycle(t *testing.T) {
	stepDuration := 10 * time.Second
	config := integration.NewTestConfig().
		WithOverrides(
			integration.WithCloudProviderName("gce"),
			integration.WithScaleDownUnneededTime(unneededTime),
		)

	options := config.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		// Configure a test NodeGroup with a single Node.
		n := test.BuildTestNode("ng1-node-0", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng1", fakecloudprovider.WithNode(n))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("p1", 600, 100, n.Name))

		// Create a pending Pod that doesn't fit on the single existing Node.
		p := test.BuildTestPod("p2", 600, 100, test.MarkUnschedulable())
		fakes.K8s.AddPod(p)

		// Run a loop, CA should scale up a single Node for the pending Pod.
		synctestutils.MustRunOnceAfter(t, autoscaler, stepDuration)
		tg1, _ := fakes.CloudProvider.GetNodeGroup("ng1").TargetSize(context.Background())
		assert.Equal(t, 2, tg1)
		assert.Equal(t, 2, len(fakes.K8s.Nodes().Items))

		// Delete the Pod that was pending, the second Node should be empty now.
		fakes.K8s.DeletePod(p.Namespace, p.Name)

		// Run CA loop once to mark the Node as unneeded.
		synctestutils.MustRunOnceAfter(t, autoscaler, stepDuration)
		// Run another CA loop after the unneeded time elapses, CA should delete the Node.
		synctestutils.MustRunOnceAfter(t, autoscaler, unneededTime+time.Nanosecond)

		finalSize, _ := fakes.CloudProvider.GetNodeGroup("ng1").TargetSize(context.Background())
		assert.Equal(t, 1, finalSize)
	})
}

func TestScaleUp_ResourceLimits(t *testing.T) {
	config := integration.NewTestConfig()

	options := config.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		n := test.BuildTestNode("ng-node-0", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng", fakecloudprovider.WithNode(n))
		// The first pod can fit on the existing node
		fakes.K8s.AddPod(test.BuildTestPod("pod1", 600, 100, test.MarkUnschedulable()))
		// The second pod should trigger the scaleup, if a resource request allows it
		fakes.K8s.AddPod(test.BuildTestPod("pod2", 600, 100, test.MarkUnschedulable()))

		// Scale-up should be blocked.
		fakes.CloudProvider.SetResourceLimit(cloudprovider.ResourceNameCores, 0, 1)

		synctestutils.MustRunOnceAfter(t, autoscaler, unneededTime)
		size, _ := fakes.CloudProvider.GetNodeGroup("ng").TargetSize(context.Background())
		assert.Equal(t, 1, size, "Should not scale up when max cores limit is reached")

		// Scale-up should succeed.
		fakes.CloudProvider.SetResourceLimit(cloudprovider.ResourceNameCores, 0, 2)

		synctestutils.MustRunOnceAfter(t, autoscaler, unneededTime)
		newSize, _ := fakes.CloudProvider.GetNodeGroup("ng").TargetSize(context.Background())
		assert.Equal(t, 2, newSize, "Should scale up after resource limit is increased")
	})
}

func TestScaleUp_EnforceMinimumResourceLimits(t *testing.T) {
	for _, tc := range []struct {
		name       string
		resource   string
		minSize    int
		pendingPod bool
	}{
		{name: "normal scale-up then minimum enforcement within cores", resource: cloudprovider.ResourceNameCores, minSize: 2, pendingPod: true},
		{name: "normal scale-up then minimum enforcement within nodes", resource: resourcequotas.ResourceNodes, minSize: 2, pendingPod: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := integration.NewTestConfig().WithOverrides(func(o *config.AutoscalingOptions) {
				o.ScaleDownEnabled = false
				o.EnforceNodeGroupMinSize = true
			}).ResolveOptions()
			infra := integration.SetupInfrastructure(t)
			fakes := infra.Fakes

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer synctestutils.TearDown(cancel)
				autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
				require.NoError(t, err)
				template := test.BuildTestNode("template", 1000, 1000, test.IsReady(true))
				for _, id := range []string{"ng1", "ng2"} {
					fakes.CloudProvider.AddNodeGroup(id, fakecloudprovider.WithNodes(template, 1), fakecloudprovider.WithNGSize(tc.minSize, 4))
					fakes.K8s.AddPod(test.BuildScheduledTestPod(id+"-pod", 600, 100, id+"-node-0"))
				}
				if tc.pendingPod {
					fakes.K8s.AddPod(test.BuildTestPod("pending", 600, 100, test.MarkUnschedulable()))
				}
				fakes.CloudProvider.SetResourceLimit(tc.resource, 0, 3)

				synctestutils.MustRunOnceAfter(t, autoscaler, 10*time.Second)
				assert.Equal(t, 3, len(fakes.K8s.Nodes().Items), "new nodes must fit the resource limit")
			})
		})
	}
}

func TestScaleUp_EnforceMinimumMaxNodesTotal(t *testing.T) {
	for _, tc := range []struct {
		name             string
		groups           []string
		unmanagedNode    bool
		pendingPod       bool
		pendingGroups    []string
		salvo            bool
		existingUpcoming bool
		startingNode     bool
		minSize          int
		maxNodesTotal    int
	}{
		{name: "one group", groups: []string{"ng1"}, unmanagedNode: true, minSize: 3, maxNodesTotal: 3},
		{name: "groups share the allowance", groups: []string{"ng1", "ng2"}, minSize: 3, maxNodesTotal: 3},
		{name: "normal scale-up then minimum enforcement", groups: []string{"ng1", "ng2"}, pendingPod: true, minSize: 2, maxNodesTotal: 3},
		{name: "normal scale-up leaves room for minimum enforcement", groups: []string{"ng1", "ng2"}, pendingPod: true, minSize: 2, maxNodesTotal: 4},
		{name: "salvo scale-ups then minimum enforcement", groups: []string{"ng1", "ng2", "ng3"}, pendingGroups: []string{"ng1", "ng2"}, salvo: true, minSize: 2, maxNodesTotal: 5},
		{name: "existing upcoming node counts once", groups: []string{"ng1", "ng2"}, existingUpcoming: true, minSize: 3, maxNodesTotal: 3},
		{name: "registered starting node without earlier request", groups: []string{"ng1"}, pendingPod: true, startingNode: true, minSize: 4, maxNodesTotal: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := integration.NewTestConfig().WithOverrides(func(o *config.AutoscalingOptions) {
				o.ScaleDownEnabled = false
				o.EnforceNodeGroupMinSize = true
				o.MaxNodesTotal = tc.maxNodesTotal
				o.NodeGroupDefaults.MaxNodeStartupTime = time.Minute
				o.SalvoScaleUp = tc.salvo
				o.SalvoScaleUpBudget = time.Minute
			}).ResolveOptions()
			infra := integration.SetupInfrastructure(t)
			fakes := infra.Fakes

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer synctestutils.TearDown(cancel)
				autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
				require.NoError(t, err)
				for _, id := range tc.groups {
					template := test.BuildTestNode("template", 1000, 1000, test.IsReady(true), test.WithNodeLabels(map[string]string{"group": id}))
					nodeCount := 1
					if tc.startingNode {
						nodeCount++
					}
					fakes.CloudProvider.AddNodeGroup(id, fakecloudprovider.WithNodes(template, nodeCount), fakecloudprovider.WithNGSize(tc.minSize, 4))
					fakes.K8s.AddPod(test.BuildScheduledTestPod(id+"-pod", 600, 100, id+"-node-0"))
					if tc.startingNode {
						for _, node := range fakes.K8s.Nodes().Items {
							if node.Name == id+"-node-1" {
								node.CreationTimestamp.Time = time.Now()
								test.SetNodeReadyState(&node, false, time.Now())
								fakes.K8s.UpdateNode(&node)
							}
						}
					}
				}
				if tc.unmanagedNode {
					fakes.K8s.AddNode(test.BuildTestNode("unmanaged", 1000, 1000, test.IsReady(true)))
				}
				if tc.pendingPod {
					fakes.K8s.AddPod(test.BuildTestPod("pending", 600, 100, test.MarkUnschedulable()))
				}
				for _, id := range tc.pendingGroups {
					pod := test.BuildTestPod(id+"-pending", 600, 100, test.MarkUnschedulable())
					pod.Spec.NodeSelector = map[string]string{"group": id}
					fakes.K8s.AddPod(pod)
				}
				synctestutils.MustRunOnceAfter(t, autoscaler, 10*time.Second)
				assert.Equal(t, tc.maxNodesTotal, len(fakes.K8s.Nodes().Items), "new nodes must fit the cluster allowance")
				expectedNodes := tc.maxNodesTotal
				if tc.existingUpcoming {
					for _, node := range fakes.K8s.Nodes().Items {
						if node.Name != "ng1-node-0" && node.Name != "ng2-node-0" {
							node.CreationTimestamp.Time = time.Now()
							test.SetNodeReadyState(&node, false, time.Now())
							fakes.K8s.UpdateNode(&node)
						}
					}
					expectedNodes++
					autoscaler.(*core.StaticAutoscaler).MaxNodesTotal = expectedNodes
				}
				synctestutils.MustRunOnceAfter(t, autoscaler, 10*time.Second)
				assert.Equal(t, expectedNodes, len(fakes.K8s.Nodes().Items), "minimum enforcement must use the remaining allowance")
			})
		})
	}
}

// TestFixNodeGroupSize_ZeroOrMaxNodeScaling verifies that fixNodeGroupSize skips
// attempting DecreaseTargetSize on ZeroOrMaxNodeScaling node groups when registered
// nodes mismatch the target size after MaxNodeProvisionTime.
func TestFixNodeGroupSize_ZeroOrMaxNodeScaling(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithCloudProviderName("gce"),
		)

	options := testConfig.ResolveOptions()
	infra := integration.SetupInfrastructure(t)
	fakes := infra.Fakes

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer synctestutils.TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, infra).Build(ctx)
		assert.NoError(t, err)

		nodeGroupOpts := &config.NodeGroupAutoscalingOptions{
			ZeroOrMaxNodeScaling: true,
			MaxNodeProvisionTime: 10 * time.Second,
		}

		templateNode := test.BuildTestNode("atomic-ng-node-0", 2000, 8*1024*1024*1024, test.IsReady(true))
		ng := fakes.CloudProvider.AddNodeGroup(
			"atomic-ng",
			fakecloudprovider.WithNodes(templateNode, 2),
			fakecloudprovider.WithNGSize(0, 10),
			fakecloudprovider.WithOptions(nodeGroupOpts),
		)

		// Add non-empty pods running on each node to prevent scale-down from deleting them.
		fakes.K8s.AddPod(test.BuildScheduledTestPod("pod-0", 1000, 1000, "atomic-ng-node-0"))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("pod-1", 1000, 1000, "atomic-ng-node-1"))

		// First cycle: Autoscaler initializes cluster state with the 2 registered nodes.
		synctestutils.MustRunOnceAfter(t, autoscaler, time.Second)

		// Simulate discrepancy: TargetSize is 4 in cloud provider, but only 2 nodes exist.
		ng.SetTargetSize(4)

		// Second cycle: Autoscaler observes discrepancy (2 registered nodes vs target 4).
		synctestutils.MustRunOnceAfter(t, autoscaler, time.Second)

		// Third cycle: Advance time past MaxNodeProvisionTime (10s).
		// fixNodeGroupSize triggers: for ZeroOrMaxNodeScaling, it skips decreasing target size.
		// RunOnce succeeds without error.
		err = synctestutils.RunOnceAfter(t, autoscaler, 15*time.Second)
		assert.NoError(t, err)

		// Verify target size was not decreased and remained 4.
		targetSize, err := ng.TargetSize(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 4, targetSize)
	})
}
