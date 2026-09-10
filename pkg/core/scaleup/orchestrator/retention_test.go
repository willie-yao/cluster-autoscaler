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

package orchestrator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	coretest "sigs.k8s.io/cluster-autoscaler/pkg/core/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupconfig"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	processorstest "sigs.k8s.io/cluster-autoscaler/pkg/processors/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	testutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

type retentionLimitGroup struct {
	cloudprovider.NodeGroup
	snapshot *cloudprovider.NodeGroupAccountingSnapshot
}

func (g *retentionLimitGroup) SuspendedNodesIncludedInTargetSize() bool { return false }
func (g *retentionLimitGroup) GetNodeGroupAccounting(context.Context) (*cloudprovider.NodeGroupAccountingSnapshot, error) {
	return g.snapshot, nil
}

func TestRetentionUpperLimitsWithClosedAdmission(t *testing.T) {
	testRetentionUpperLimitsWithClosedAdmission(t, false, []string{"pending", "terminal", "fresh"})
}

func TestRetentionMinSizeUpperLimitsWithClosedAdmission(t *testing.T) {
	testRetentionUpperLimitsWithClosedAdmission(t, true, []string{"pending", "terminal", "fresh"})
}

func TestSuspendedLegacyUpperLimits(t *testing.T) {
	for _, enforceMin := range []bool{false, true} {
		t.Run(fmt.Sprintf("minimum=%t", enforceMin), func(t *testing.T) {
			testRetentionUpperLimitsWithClosedAdmission(t, enforceMin, []string{"legacy"})
		})
	}
}

func testRetentionUpperLimitsWithClosedAdmission(t *testing.T, enforceMin bool, modes []string) {
	t.Helper()
	for _, limit := range []string{"nodes", "cpu", "memory"} {
		for _, mode := range modes {
			for _, headroom := range []int{0, 1} {
				t.Run(fmt.Sprintf("%s/%s/headroom=%d", limit, mode, headroom), func(t *testing.T) {
					ctx := context.Background()
					now := time.Now()
					template := testutils.BuildTestNode("template", 2000, units.GiB, testutils.IsReady(true))
					grown := 0
					provider := testprovider.NewTestCloudProviderBuilder().
						WithMachineTypes([]string{"b", "c"}).
						WithMachineTemplates(map[string]*framework.NodeInfo{
							"b": framework.NewNodeInfo(template, nil),
							"c": framework.NewNodeInfo(template, nil),
						}).
						WithOnScaleUp(func(group string, delta int) error {
							if enforceMin {
								assert.Contains(t, []string{"b", "c"}, group)
							} else {
								assert.Equal(t, "b", group)
							}
							grown += delta
							return nil
						}).Build()
					target := 4
					if mode == "legacy" {
						target = 3
					}
					a := &retentionLimitGroup{
						NodeGroup: provider.BuildNodeGroup("a", 0, target, target, true, false, "", nil),
						snapshot:  &cloudprovider.NodeGroupAccountingSnapshot{TargetSize: target},
					}
					if mode == "legacy" {
						provider.InsertNodeGroup(a.NodeGroup)
					} else {
						provider.InsertNodeGroup(a)
					}
					minSize := 0
					if enforceMin {
						minSize = 1
						provider.InsertNodeGroup(provider.BuildNodeGroup("c", minSize, 10, 0, true, false, "c", nil))
					}
					provider.InsertNodeGroup(provider.BuildNodeGroup("b", minSize, 10, 0, true, false, "b", nil))
					var raw, active []*apiv1.Node
					var scheduled []*apiv1.Pod
					for i := range target {
						name := fmt.Sprintf("a-%d", i)
						node := testutils.BuildTestNode(name, 2000, units.GiB, testutils.IsReady(true))
						node.Spec.ProviderID = name
						if i < 2 || mode == "legacy" {
							active = append(active, node)
							scheduled = append(scheduled, testutils.BuildTestPod("busy-"+name, 1800, units.GiB*8/10, testutils.WithNodeName(name)))
							if i == 2 {
								node.Status.Conditions = append(node.Status.Conditions, apiv1.NodeCondition{Type: "Suspended", Status: apiv1.ConditionTrue})
							}
						} else {
							if mode == "fresh" {
								continue
							}
							a.snapshot.InactiveInstanceIDs = append(a.snapshot.InactiveInstanceIDs, name)
							if mode == "terminal" {
								a.snapshot.UnavailableTargetNodes++
							} else {
								a.snapshot.UpcomingInactiveNodes++
							}
						}
						provider.AddNode("a", node)
						raw = append(raw, node)
						a.snapshot.Instances = append(a.snapshot.Instances, cloudprovider.Instance{Id: name})
					}
					views := map[string]*cloudprovider.NodeGroupAccountingSnapshot{}
					if mode != "legacy" {
						views["a"] = a.snapshot
					}
					normalized, err := cloudprovider.NormalizeNodeGroupObservations(raw, views)
					require.NoError(t, err)
					active, err = cloudprovider.FilterOutInactiveNodes(ctx, provider, normalized, views)
					require.NoError(t, err)
					expectedScheduling := 2
					if mode == "legacy" {
						expectedScheduling = 3
					}
					require.Len(t, active, expectedScheduling)
					opts := config.AutoscalingOptions{
						EstimatorName: estimator.BinpackingEstimatorName, MaxNodeGroupBinpackingDuration: time.Second,
						MaxCoresTotal: config.DefaultMaxClusterCores, MaxMemoryTotal: config.DefaultMaxClusterMemory * units.GiB,
						EnforceNodeGroupMinSize: enforceMin,
					}
					switch limit {
					case "nodes":
						opts.MaxNodesTotal = target + headroom
					case "cpu":
						opts.MaxCoresTotal = int64((target + headroom) * 2)
					case "memory":
						opts.MaxMemoryTotal = int64(target+headroom) * units.GiB
					}
					provider.SetResourceLimiter(cloudprovider.NewResourceLimiter(
						map[string]int64{}, map[string]int64{cloudprovider.ResourceNameCores: opts.MaxCoresTotal, cloudprovider.ResourceNameMemory: opts.MaxMemoryTotal}))
					listers := kube_util.NewListerRegistry(nil, nil, kube_util.NewTestPodLister(scheduled), nil, nil, nil, nil, nil, nil)
					processors, templates := processorstest.NewTestProcessors(opts)
					autoscalingCtx, err := coretest.NewScaleTestAutoscalingContext(opts, &fake.Clientset{}, listers, provider, nil, nil, templates)
					require.NoError(t, err)
					require.NoError(t, autoscalingCtx.ClusterSnapshot.SetClusterState(ctx, active, scheduled, nil, nil))
					require.NoError(t, templates.Recompute(ctx, &autoscalingCtx, active, nil, taints.TaintConfig{}, now))
					csr := clusterstate.NewNotifiedClusterStateRegistry(provider, autoscalingCtx.LogRecorder, coretest.NewBackoff(),
						nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 5 * time.Minute}), templates)
					require.NoError(t, csr.UpdateNodes(ctx, raw, now))
					csr.RegisterFailedScaleUp(ctx, a, 2, cloudprovider.InstanceErrorInfo{ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: "backoff"}, now)
					upcoming, _ := csr.GetUpcomingNodes(ctx)
					require.Empty(t, upcoming)
					autoscalingCtx.ExpanderStrategy = coretest.NewMockReportingStrategy(t, nil, nil)
					o := New()
					o.Initialize(&autoscalingCtx, processors, csr, newEstimatorBuilder(), taints.TaintConfig{},
						resourcequotas.NewTrackerFactory(resourcequotas.TrackerOptions{
							QuotaProvider: resourcequotas.NewCloudQuotasProvider(provider), CustomResourcesProcessor: processors.CustomResourcesProcessor,
						}))
					pod := testutils.BuildTestPod("pending", 1800, units.GiB*8/10)
					var result *status.ScaleUpStatus
					var scaleErr errors.AutoscalerError
					if enforceMin {
						result, scaleErr = o.ScaleUpToNodeGroupMinSize(ctx, active, templates.GetNodeInfos())
						require.NoError(t, scaleErr)
						assert.Len(t, result.ScaleUpInfos, headroom)
					} else {
						result, scaleErr = o.ScaleUp(ctx, []*apiv1.Pod{pod}, active, nil, templates.GetNodeInfos(), false)
					}
					if headroom > 0 {
						require.NoError(t, scaleErr)
						require.True(t, result.WasSuccessful())
					}
					assert.Equal(t, headroom, grown)
					schedulingNodes, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
					require.NoError(t, err)
					assert.Len(t, schedulingNodes, expectedScheduling, "upper reservations are not scheduling destinations")
				})
			}
		}
	}
}
