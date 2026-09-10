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

package context

import (
	"context"
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

// MarkNodeToBeDeleted delegates owned restrictions or uses legacy taint handling.
func (a *AutoscalingContext) MarkNodeToBeDeleted(ctx context.Context, node *apiv1.Node) (*apiv1.Node, error) {
	return a.handleDeletionTaint(ctx, node, true)
}

// CleanNodeToBeDeleted delegates cleanup without bypassing provider ownership.
func (a *AutoscalingContext) CleanNodeToBeDeleted(ctx context.Context, node *apiv1.Node) (*apiv1.Node, error) {
	return a.handleDeletionTaint(ctx, node, false)
}

func (a *AutoscalingContext) handleDeletionTaint(ctx context.Context, node *apiv1.Node, mark bool) (*apiv1.Node, error) {
	group, err := a.CloudProvider.NodeGroupForNode(ctx, node)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, fmt.Errorf("cannot establish deletion-taint ownership for node %s without a node group", node.Name)
	}
	if handler, ok := group.(cloudprovider.NodeGroupDeletionTaintHandler); ok {
		action := handler.CleanToBeDeleted
		if mark {
			action = handler.MarkToBeDeleted
		}
		updated, handled, err := action(ctx, node, a.CordonNodeBeforeTerminate)
		if err != nil {
			return nil, err
		}
		if handled {
			if updated == nil || updated.Name != node.Name || updated.UID != node.UID || updated.Spec.ProviderID != node.Spec.ProviderID {
				return nil, fmt.Errorf("invalid deletion-taint result for node %s", node.Name)
			}
			return updated, nil
		}
	}
	if !cloudprovider.SuspendedNodesIncludedInTargetSize(group) && cloudprovider.IsNodeSuspended(node) {
		return nil, fmt.Errorf("cannot modify deletion taint on suspended node %s without a provider handler", node.Name)
	}
	if mark {
		return taints.MarkToBeDeleted(ctx, node, a.ClientSet, a.CordonNodeBeforeTerminate)
	}
	return taints.CleanToBeDeleted(ctx, node, a.ClientSet, a.CordonNodeBeforeTerminate)
}
