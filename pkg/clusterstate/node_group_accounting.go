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
	"slices"

	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
)

func (csr *ClusterStateRegistry) calculateUpcomingNodes(id string, readiness Readiness, acceptable AcceptableRange) int {
	upcoming := calculateUpcomingNodesInNodeGroup(readiness, acceptable)
	if csr.suspendedExcludedFromTarget[id] {
		upcoming += len(readiness.Suspended)
	}
	if view := csr.nodeGroupAccounting[id]; view != nil {
		if csr.suspendedExcludedFromTarget[id] {
			// Accepted inactive work is distinct from ordinary fresh provisioning.
			fresh := max(0, upcoming-view.UpcomingInactiveNodes-view.UnavailableTargetNodes)
			return fresh + view.UpcomingInactiveNodes
		}
		upcoming -= max(0, len(view.InactiveInstanceIDs)-len(readiness.Suspended))
		upcoming -= view.UnavailableTargetNodes
	}
	return upcoming
}

func (csr *ClusterStateRegistry) activeInstances(id string, instances []cloudprovider.Instance) []cloudprovider.Instance {
	view := csr.nodeGroupAccounting[id]
	if view == nil {
		return instances
	}
	result := make([]cloudprovider.Instance, 0, len(instances))
	for _, instance := range instances {
		if !slices.Contains(view.InactiveInstanceIDs, instance.Id) {
			result = append(result, instance)
		}
	}
	return result
}

func (csr *ClusterStateRegistry) registrationInstances(instances map[string][]cloudprovider.Instance) map[string][]cloudprovider.Instance {
	result := make(map[string][]cloudprovider.Instance, len(instances))
	for id, groupInstances := range instances {
		result[id] = csr.activeInstances(id, groupInstances)
	}
	return result
}

func (csr *ClusterStateRegistry) accountingContainsInstance(id string) bool {
	for _, view := range csr.nodeGroupAccounting {
		for _, instance := range view.Instances {
			if instance.Id == id {
				return true
			}
		}
	}
	return false
}

// NodesForUpperLimits adds missing active target reservations to the supplied
// scheduling inventory. These copies must not be used for scheduling or minimums.
func (csr *ClusterStateRegistry) NodesForUpperLimits(ctx context.Context, nodes []*apiv1.Node, templates map[string]*framework.NodeInfo) ([]*apiv1.Node, error) {
	// Partial scale-ups can change target without registering a successful request.
	targets := make(map[string]int)
	for _, group := range csr.cloudProvider.NodeGroups(ctx) {
		view, err := cloudprovider.GetNodeGroupAccounting(ctx, group)
		if err != nil {
			return nil, err
		}
		if view != nil {
			targets[group.Id()] = view.TargetSize
		} else if !cloudprovider.SuspendedNodesIncludedInTargetSize(group) {
			target, err := group.TargetSize(ctx)
			if err != nil {
				return nil, err
			}
			targets[group.Id()] = target
		}
	}
	csr.Lock()
	defer csr.Unlock()
	result := append([]*apiv1.Node(nil), nodes...)
	for id, target := range targets {
		readiness := csr.perNodeGroupReadiness[id]
		present := 0
		for _, node := range nodes {
			if slices.Contains(readiness.Registered, node.Name) || node.Annotations[annotations.NodeUpcomingGroupAnnotation] == id {
				present++
			}
		}
		target = max(target, csr.acceptableRanges[id].CurrentTarget)
		if view := csr.nodeGroupAccounting[id]; view != nil {
			target = max(target, view.TargetSize)
		}
		missing := target - present
		if missing <= 0 {
			continue
		}
		template, found := templates[id]
		if !found || template == nil || template.Node() == nil {
			return nil, fmt.Errorf("missing node template for upper-limit reservations in group %s", id)
		}
		for i := range missing {
			node := template.Node().DeepCopy()
			node.Name = fmt.Sprintf("upper-limit-%s-%d", id, i)
			if node.Annotations == nil {
				node.Annotations = make(map[string]string)
			}
			node.Annotations[annotations.NodeUpcomingAnnotation] = "true"
			node.Annotations[annotations.NodeUpcomingGroupAnnotation] = id
			result = append(result, node)
		}
	}
	return result, nil
}
