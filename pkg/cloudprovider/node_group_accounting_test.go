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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type observationGroup struct {
	NodeGroup
	view *NodeGroupAccountingSnapshot
}

func (*observationGroup) Id() string                               { return "group" }
func (*observationGroup) SuspendedNodesIncludedInTargetSize() bool { return false }
func (g *observationGroup) GetNodeGroupAccounting(context.Context) (*NodeGroupAccountingSnapshot, error) {
	return g.view, nil
}

func TestProviderOwnedAccountingValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		view  NodeGroupAccountingSnapshot
		valid bool
	}{
		{"parked", NodeGroupAccountingSnapshot{TargetSize: 0, Instances: []Instance{{Id: "id"}}, InactiveInstanceIDs: []string{"id"}}, true},
		{"pending", NodeGroupAccountingSnapshot{TargetSize: 1, Instances: []Instance{{Id: "id"}}, InactiveInstanceIDs: []string{"id"}, UpcomingInactiveNodes: 1}, true},
		{"unmaterialized expired", NodeGroupAccountingSnapshot{TargetSize: 1, UnavailableTargetNodes: 1}, true},
		{"negative target", NodeGroupAccountingSnapshot{TargetSize: -1}, false},
		{"unknown member", NodeGroupAccountingSnapshot{InactiveInstanceIDs: []string{"missing"}}, false},
		{"duplicate member", NodeGroupAccountingSnapshot{Instances: []Instance{{Id: "id"}, {Id: "id"}}}, false},
		{"duplicate inactive", NodeGroupAccountingSnapshot{Instances: []Instance{{Id: "id"}}, InactiveInstanceIDs: []string{"id", "id"}}, false},
		{"missing accepted member", NodeGroupAccountingSnapshot{TargetSize: 1, UpcomingInactiveNodes: 1}, false},
		{"reservations exceed target", NodeGroupAccountingSnapshot{TargetSize: 1, UnavailableTargetNodes: 2}, false},
		{"invalid Node", NodeGroupAccountingSnapshot{Instances: []Instance{{Id: "id"}}, NodeObservations: []*apiv1.Node{{}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GetNodeGroupAccounting(t.Context(), &observationGroup{view: &tc.view})
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	require.True(t, SuspendedNodesIncludedInTargetSize(nil))
}

func TestProviderOwnedNodeObservations(t *testing.T) {
	node := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "uid"}, Spec: apiv1.NodeSpec{ProviderID: "id"}}
	fresh := node.DeepCopy()
	fresh.Status.Conditions = []apiv1.NodeCondition{{Type: apiv1.NodeReady, Status: apiv1.ConditionTrue}}
	view := &NodeGroupAccountingSnapshot{Instances: []Instance{{Id: "id"}}, NodeObservations: []*apiv1.Node{fresh}}
	for _, raw := range [][]*apiv1.Node{{node}, nil} {
		nodes, err := NormalizeNodeGroupObservations(raw, map[string]*NodeGroupAccountingSnapshot{"group": view})
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, fresh, nodes[0])
	}
	view.InactiveInstanceIDs = []string{"id"}
	nodes, err := NormalizeNodeGroupObservations([]*apiv1.Node{node}, map[string]*NodeGroupAccountingSnapshot{"group": view})
	require.NoError(t, err)
	require.True(t, IsNodeSuspended(nodes[0]))
	require.False(t, IsNodeSuspended(fresh))
	require.Empty(t, node.Status.Conditions)
	fresh.UID = "replacement"
	_, err = NormalizeNodeGroupObservations([]*apiv1.Node{node}, map[string]*NodeGroupAccountingSnapshot{"group": view})
	require.ErrorContains(t, err, "identity changed")
}
