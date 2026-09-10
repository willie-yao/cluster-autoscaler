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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type taintHandlerGroup struct {
	cloudprovider.NodeGroup
	handle func(*apiv1.Node, bool) (*apiv1.Node, bool, error)
}

func (g *taintHandlerGroup) MarkToBeDeleted(_ context.Context, n *apiv1.Node, cordon bool) (*apiv1.Node, bool, error) {
	return g.handle(n, cordon)
}
func (g *taintHandlerGroup) CleanToBeDeleted(_ context.Context, n *apiv1.Node, cordon bool) (*apiv1.Node, bool, error) {
	return g.handle(n, cordon)
}

func TestProviderOwnedDeletionTaints(t *testing.T) {
	for _, mark := range []bool{true, false} {
		for _, mode := range []string{"legacy", "owned", "unknown", "invalid-result"} {
			t.Run(fmt.Sprintf("%s/mark=%t", mode, mark), func(t *testing.T) {
				node := testutils.BuildTestNode("n", 1000, 1000)
				node.UID, node.Spec.ProviderID = "uid", "id"
				node.Spec.Unschedulable = true
				if !mark {
					node.Spec.Taints = []apiv1.Taint{{Key: taints.ToBeDeletedTaint, Effect: apiv1.TaintEffectNoSchedule}}
				}
				provider := testprovider.NewTestCloudProviderBuilder().Build()
				group := &taintHandlerGroup{NodeGroup: provider.BuildNodeGroup("ng", 0, 10, 1, true, false, "", nil)}
				group.handle = func(n *apiv1.Node, cordon bool) (*apiv1.Node, bool, error) {
					assert.False(t, cordon)
					switch mode {
					case "legacy":
						return nil, false, nil
					case "unknown":
						return nil, false, fmt.Errorf("unknown ownership")
					case "invalid-result":
						replacement := n.DeepCopy()
						replacement.UID = "replacement"
						return replacement, true, nil
					default:
						return n, true, nil
					}
				}
				provider.InsertNodeGroup(group)
				provider.AddNode("ng", node)
				client := fake.NewSimpleClientset(node)
				a := &AutoscalingContext{CloudProvider: provider, AutoscalingOptions: config.AutoscalingOptions{}}
				a.AutoscalingKubeClients = AutoscalingKubeClients{ClientSet: client}
				action := a.CleanNodeToBeDeleted
				if mark {
					action = a.MarkNodeToBeDeleted
				}
				updated, err := action(t.Context(), node)
				if mode == "unknown" || mode == "invalid-result" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.True(t, updated.Spec.Unschedulable, "administrative cordon is preserved")
				}
				if mode != "legacy" {
					assert.Empty(t, client.Actions(), "handled or uncertain ownership cannot reach legacy mutation")
				}
			})
		}
	}
}
