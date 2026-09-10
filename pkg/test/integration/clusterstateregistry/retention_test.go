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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	fakeprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/test/integration"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

type retainingProvider struct {
	cloudprovider.CloudProvider
	group *retainingGroup
}

func (p *retainingProvider) NodeGroups(context.Context) []cloudprovider.NodeGroup {
	return []cloudprovider.NodeGroup{p.group}
}
func (p *retainingProvider) NodeGroupForNode(ctx context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	group, err := p.CloudProvider.NodeGroupForNode(ctx, node)
	if err != nil || group == nil {
		return group, err
	}
	return p.group, nil
}
func (p *retainingProvider) Refresh(ctx context.Context) error { return p.group.refresh(ctx) }

type retainingGroup struct {
	cloudprovider.NodeGroup
	mu        sync.Mutex
	client    kubernetes.Interface
	target    int
	nodes     map[string]*apiv1.Node
	inactive  map[string]bool
	accepted  map[string]time.Time
	completed bool
	view      *cloudprovider.NodeGroupAccountingSnapshot
}

func (g *retainingGroup) SuspendedNodesIncludedInTargetSize() bool { return false }
func (g *retainingGroup) GetNodeGroupAccounting(context.Context) (*cloudprovider.NodeGroupAccountingSnapshot, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.view, nil
}
func (g *retainingGroup) TargetSize(context.Context) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.target, nil
}
func (g *retainingGroup) publish() {
	view := &cloudprovider.NodeGroupAccountingSnapshot{TargetSize: g.target, UpcomingInactiveNodes: len(g.accepted)}
	for id, node := range g.nodes {
		view.Instances = append(view.Instances, cloudprovider.Instance{Id: id})
		view.NodeObservations = append(view.NodeObservations, node.DeepCopy())
		if g.inactive[id] {
			view.InactiveInstanceIDs = append(view.InactiveInstanceIDs, id)
		}
	}
	g.view = view
}
func (g *retainingGroup) DeleteNodes(ctx context.Context, nodes []*apiv1.Node) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, node := range nodes {
		if g.inactive[node.Spec.ProviderID] {
			return fmt.Errorf("node %s is inactive", node.Name)
		}
		g.inactive[node.Spec.ProviderID] = true
		g.target--
	}
	g.publish()
	return nil
}
func (g *retainingGroup) IncreaseSize(_ context.Context, delta int) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id := range g.inactive {
		if _, accepted := g.accepted[id]; delta > 0 && !accepted {
			g.accepted[id] = time.Now()
			g.target++
			delta--
		}
	}
	g.publish()
	if delta != 0 {
		return fmt.Errorf("test retained inventory exhausted: %d", delta)
	}
	return nil
}
func (g *retainingGroup) completeResumes() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.completed = true
	return nil
}
func (g *retainingGroup) DecreaseTargetSize(context.Context, int) error { return nil }
func (g *retainingGroup) MarkToBeDeleted(ctx context.Context, node *apiv1.Node, cordon bool) (*apiv1.Node, bool, error) {
	if cordon {
		return nil, true, fmt.Errorf("test provider requires taint-only configuration")
	}
	updated, err := taints.MarkToBeDeleted(ctx, node, g.client, false)
	return updated, true, err
}
func (g *retainingGroup) CleanToBeDeleted(ctx context.Context, node *apiv1.Node, _ bool) (*apiv1.Node, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inactive[node.Spec.ProviderID] {
		return node, true, nil
	}
	updated, err := taints.CleanToBeDeleted(ctx, node, g.client, false)
	return updated, true, err
}
func (g *retainingGroup) refresh(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, previous := range g.nodes {
		node, err := g.client.CoreV1().Nodes().Get(ctx, previous.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if accepted, found := g.accepted[id]; found && g.completed {
			for _, condition := range node.Status.Conditions {
				if condition.Type == apiv1.NodeReady && condition.Status == apiv1.ConditionTrue && condition.LastHeartbeatTime.Time.After(accepted) {
					node, err = taints.CleanToBeDeleted(ctx, node, g.client, false)
					if err != nil {
						return err
					}
					delete(g.inactive, id)
					delete(g.accepted, id)
					break
				}
			}
		}
		status := apiv1.ConditionFalse
		if g.inactive[id] {
			status = apiv1.ConditionTrue
		}
		found := false
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == "Suspended" {
				node.Status.Conditions[i].Status = status
				found = true
			}
		}
		if !found {
			node.Status.Conditions = append(node.Status.Conditions, apiv1.NodeCondition{Type: "Suspended", Status: status})
		}
		node, err = g.client.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		g.nodes[id] = node
	}
	g.publish()
	return nil
}

func TestClusterStateRegistryRetention(t *testing.T) {
	RunTestClusterStateRegistryRetention(t, func(t *testing.T, ctx context.Context, args RetentionSetupArgs) RetentionSetup {
		infra := integration.SetupInfrastructure(t)
		template := testutils.BuildTestNode("template", 1000, 2*units.GiB, testutils.IsReady(true))
		base := infra.Fakes.CloudProvider.AddNodeGroup("ng", fakeprovider.WithNodes(template, args.NodeCount))
		group := &retainingGroup{NodeGroup: base, client: infra.Fakes.K8s.Client, target: args.NodeCount,
			nodes: make(map[string]*apiv1.Node), inactive: make(map[string]bool), accepted: make(map[string]time.Time)}
		for _, node := range infra.Fakes.K8s.Nodes().Items {
			node.UID = types.UID(node.Name)
			infra.Fakes.K8s.UpdateNode(&node)
			group.nodes[node.Spec.ProviderID] = node.DeepCopy()
		}
		group.publish()
		provider := &retainingProvider{CloudProvider: infra.Fakes.CloudProvider, group: group}
		opts := integration.NewTestConfig().WithOverrides(args.OptsOverride).ResolveOptions()
		autoscaler, _, err := integration.DefaultAutoscalingBuilder(opts, infra).WithCloudProvider(provider).Build(ctx)
		require.NoError(t, err)
		return RetentionSetup{Autoscaler: autoscaler, NodeGroup: group, K8s: infra.Fakes.K8s,
			CompleteResumes: group.completeResumes, Refresh: func() error { return provider.Refresh(ctx) }}
	})
}
