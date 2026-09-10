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

package cloudprovider

import (
	"context"
	"fmt"

	apiv1 "k8s.io/api/core/v1"
)

// NodeGroupTargetSizeWithSuspended optionally specifies the units of TargetSize.
// Groups without this interface include suspended instances in their target.
type NodeGroupTargetSizeWithSuspended interface {
	SuspendedNodesIncludedInTargetSize() bool
}

// NodeGroupAccounting provides a coherent, read-only accounting observation.
// GetNodeGroupAccounting must not reconcile, write objects, wait for cloud
// operations, or notify observers. Unknown state must return an error.
// A nil snapshot without an error selects ordinary independent inventory reads;
// NodeGroupTargetSizeWithSuspended still determines target units.
type NodeGroupAccounting interface {
	GetNodeGroupAccounting(context.Context) (*NodeGroupAccountingSnapshot, error)
}

// NodeGroupAccountingSnapshot describes membership and availability at one observation.
// Providers retain ownership of operations, readiness publication and restrictions.
// Returned data must not be modified after publication.
type NodeGroupAccountingSnapshot struct {
	TargetSize int
	Instances  []Instance
	// InactiveInstanceIDs contains exact Instance.Id values, including Node-less
	// members. These instances are neither scheduling nor registration candidates.
	InactiveInstanceIDs []string
	// NodeObservations contains authoritative Kubernetes observations. Core replaces
	// matching informer Nodes, or includes a missing Node, without modifying either
	// input. Name, UID and provider identity must agree when replacing a Node.
	NodeObservations []*apiv1.Node
	// UpcomingInactiveNodes counts live accepted inactive reservations in the
	// active target. It does not include ordinary fresh-instance provisioning.
	UpcomingInactiveNodes int
	// UnavailableTargetNodes counts accepted target reservations that cannot be
	// admitted as upcoming, including failed or expired fresh capacity without
	// instance identities. Providers
	// report failures through the existing scale-state notifier before publication.
	// These reservations still consume upper limits, never resource minimums.
	UnavailableTargetNodes int
}

// NodeGroupDeletionTaintHandler optionally owns deletion-taint marking and cleanup.
// The bool argument requests cordoning. The returned bool indicates handling:
// only false with no error permits legacy taint handling. Owned or uncertain
// restrictions must never fall through, including after a policy change.
type NodeGroupDeletionTaintHandler interface {
	MarkToBeDeleted(context.Context, *apiv1.Node, bool) (*apiv1.Node, bool, error)
	CleanToBeDeleted(context.Context, *apiv1.Node, bool) (*apiv1.Node, bool, error)
}

// SuspendedNodesIncludedInTargetSize returns true unless a group opts out.
func SuspendedNodesIncludedInTargetSize(group NodeGroup) bool {
	if accounting, ok := group.(NodeGroupTargetSizeWithSuspended); ok {
		return accounting.SuspendedNodesIncludedInTargetSize()
	}
	return true
}

// GetNodeGroupAccounting reads and validates the optional accounting view.
func GetNodeGroupAccounting(ctx context.Context, group NodeGroup) (*NodeGroupAccountingSnapshot, error) {
	provider, ok := group.(NodeGroupAccounting)
	if !ok {
		return nil, nil
	}
	view, err := provider.GetNodeGroupAccounting(ctx)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, nil
	}
	if view.TargetSize < 0 || view.UpcomingInactiveNodes < 0 || view.UnavailableTargetNodes < 0 {
		return nil, fmt.Errorf("invalid accounting counts for node group %s", group.Id())
	}
	instances := make(map[string]bool, len(view.Instances))
	for _, instance := range view.Instances {
		if instance.Id == "" || instances[instance.Id] {
			return nil, fmt.Errorf("invalid accounting membership for node group %s: %q", group.Id(), instance.Id)
		}
		instances[instance.Id] = true
	}
	inactive := make(map[string]bool, len(view.InactiveInstanceIDs))
	for _, id := range view.InactiveInstanceIDs {
		if !instances[id] || inactive[id] {
			return nil, fmt.Errorf("invalid inactive instance for node group %s: %q", group.Id(), id)
		}
		inactive[id] = true
	}
	if view.UpcomingInactiveNodes > len(inactive) || view.UpcomingInactiveNodes > view.TargetSize ||
		view.UnavailableTargetNodes > view.TargetSize-view.UpcomingInactiveNodes ||
		(view.UpcomingInactiveNodes > 0 && SuspendedNodesIncludedInTargetSize(group)) {
		return nil, fmt.Errorf("invalid inactive target reservations for node group %s", group.Id())
	}
	names := make(map[string]bool, len(view.NodeObservations))
	ids := make(map[string]bool, len(view.NodeObservations))
	for _, node := range view.NodeObservations {
		if node == nil || node.Name == "" || node.UID == "" || !instances[node.Spec.ProviderID] ||
			names[node.Name] || ids[node.Spec.ProviderID] {
			return nil, fmt.Errorf("invalid Node observation for node group %s", group.Id())
		}
		names[node.Name], ids[node.Spec.ProviderID] = true, true
	}
	return view, nil
}

// ReadNodeGroupAccounting reads accounting views once for a cluster observation.
func ReadNodeGroupAccounting(ctx context.Context, groups []NodeGroup) (map[string]*NodeGroupAccountingSnapshot, error) {
	views := make(map[string]*NodeGroupAccountingSnapshot)
	for _, group := range groups {
		view, err := GetNodeGroupAccounting(ctx, group)
		if err != nil {
			return nil, err
		}
		if view != nil {
			views[group.Id()] = view
		}
	}
	return views, nil
}

// IsNodeSuspended reports whether a Node has Suspended=True.
func IsNodeSuspended(node *apiv1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == "Suspended" {
			return condition.Status == apiv1.ConditionTrue
		}
	}
	return false
}

// NormalizeNodeGroupObservations combines informer Nodes with provider observations.
// Inactive membership overrides stale readiness without changing informer objects.
func NormalizeNodeGroupObservations(nodes []*apiv1.Node, views map[string]*NodeGroupAccountingSnapshot) ([]*apiv1.Node, error) {
	observations := make(map[string]*apiv1.Node)
	observedIDs := make(map[string]string)
	inactive := make(map[string]bool)
	for _, view := range views {
		for _, id := range view.InactiveInstanceIDs {
			inactive[id] = true
		}
		for _, node := range view.NodeObservations {
			if _, found := observations[node.Name]; found {
				return nil, fmt.Errorf("duplicate provider Node observation: %s", node.Name)
			}
			observations[node.Name] = node
			if name, found := observedIDs[node.Spec.ProviderID]; found && name != node.Name {
				return nil, fmt.Errorf("duplicate provider identity in Node observations: %s", node.Spec.ProviderID)
			}
			observedIDs[node.Spec.ProviderID] = node.Name
		}
	}
	result := make([]*apiv1.Node, 0, len(nodes)+len(observations))
	normalize := func(node *apiv1.Node) *apiv1.Node {
		if !inactive[node.Spec.ProviderID] || IsNodeSuspended(node) {
			return node
		}
		node = node.DeepCopy()
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == "Suspended" {
				node.Status.Conditions[i].Status = apiv1.ConditionTrue
				return node
			}
		}
		node.Status.Conditions = append(node.Status.Conditions, apiv1.NodeCondition{Type: "Suspended", Status: apiv1.ConditionTrue})
		return node
	}
	for _, node := range nodes {
		if name, found := observedIDs[node.Spec.ProviderID]; found && name != node.Name {
			return nil, fmt.Errorf("provider Node observation name changed for instance %s", node.Spec.ProviderID)
		}
		if fresh, found := observations[node.Name]; found {
			if fresh.UID != node.UID || fresh.Spec.ProviderID != node.Spec.ProviderID {
				return nil, fmt.Errorf("provider Node observation identity changed: %s", node.Name)
			}
			node = fresh
			delete(observations, node.Name)
		}
		result = append(result, normalize(node))
	}
	for _, node := range observations {
		result = append(result, normalize(node))
	}
	return result, nil
}

// FilterOutInactiveNodes excludes suspended Nodes from opted-in groups.
// Suspended-inclusive groups without an accounting view retain legacy behavior.
func FilterOutInactiveNodes(ctx context.Context, provider CloudProvider, nodes []*apiv1.Node, views map[string]*NodeGroupAccountingSnapshot) ([]*apiv1.Node, error) {
	result := make([]*apiv1.Node, 0, len(nodes))
	for _, node := range nodes {
		if !IsNodeSuspended(node) {
			result = append(result, node)
			continue
		}
		group, err := provider.NodeGroupForNode(ctx, node)
		if err != nil {
			return nil, err
		}
		if group == nil || (SuspendedNodesIncludedInTargetSize(group) && views[group.Id()] == nil) {
			result = append(result, node)
		}
	}
	return result, nil
}
