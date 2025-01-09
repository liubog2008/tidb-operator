// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pingcap/tidb-operator/apis/core/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/client"
	"github.com/pingcap/tidb-operator/pkg/pdapi/v1"
	pdv1 "github.com/pingcap/tidb-operator/pkg/timanager/apis/pd/v1"
	pdm "github.com/pingcap/tidb-operator/pkg/timanager/pd"
	"github.com/pingcap/tidb-operator/pkg/utils/fake"
	"github.com/pingcap/tidb-operator/pkg/utils/task/v3"
)

const fakeVersion = "v1.2.3"

func TestTaskPod(t *testing.T) {
	cases := []struct {
		desc          string
		state         *ReconcileContext
		objs          []client.Object
		needRefresh   bool
		needTrasferTo string
		// if true, cannot apply pod
		unexpectedErr bool

		expectUpdatedPod         bool
		expectedPodIsTerminating bool
		expectedStatus           task.Status
	}{
		{
			desc: "no pod but healthy",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
				},
				Healthy: true,
			},
			needRefresh: true,

			expectedStatus: task.SWait,
		},
		{
			desc: "no pod and unhealthy",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
				},
			},

			expectUpdatedPod: true,
			expectedStatus:   task.SComplete,
		},
		{
			desc: "no pod and unhealthy, failed to apply",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
				},
			},
			unexpectedErr: true,

			expectedStatus: task.SFail,
		},
		{
			desc: "pod spec changed",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						return obj
					}),
				},
			},

			expectUpdatedPod:         false,
			expectedPodIsTerminating: true,
			expectedStatus:           task.SWait,
		},
		{
			desc: "pod spec changed, failed to delete",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						return obj
					}),
				},
			},
			unexpectedErr: true,

			expectedStatus: task.SFail,
		},
		{
			desc: "pod spec changed, pod is healthy, pod is leader",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						obj.Status.Conditions = []metav1.Condition{
							{
								Type:   v1alpha1.CondHealth,
								Status: metav1.ConditionTrue,
							},
						}
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						return obj
					}),
					pds: []*v1alpha1.PD{
						fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
							obj.Spec.Version = fakeVersion
							obj.Status.Conditions = []metav1.Condition{
								{
									Type:               v1alpha1.CondHealth,
									Status:             metav1.ConditionTrue,
									LastTransitionTime: metav1.Now(),
								},
							}
							return obj
						}),
						fake.FakeObj("aaa-yyy", func(obj *v1alpha1.PD) *v1alpha1.PD {
							obj.Spec.Version = fakeVersion
							obj.Status.Conditions = []metav1.Condition{
								{
									Type:   v1alpha1.CondHealth,
									Status: metav1.ConditionTrue,
								},
							}
							return obj
						}),
					},
				},
				Healthy:  true,
				IsLeader: true,
			},
			needTrasferTo: "aaa-yyy",

			expectedStatus: task.SWait,
		},
		{
			desc: "pod spec changed, pod is healthy, pod is leader, no transferee",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						obj.Status.Conditions = []metav1.Condition{
							{
								Type:   v1alpha1.CondHealth,
								Status: metav1.ConditionTrue,
							},
						}
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						return obj
					}),
					pds: []*v1alpha1.PD{
						fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
							obj.Spec.Version = fakeVersion
							obj.Status.Conditions = []metav1.Condition{
								{
									Type:               v1alpha1.CondHealth,
									Status:             metav1.ConditionTrue,
									LastTransitionTime: metav1.Now(),
								},
							}
							return obj
						}),
					},
				},
				Healthy:  true,
				IsLeader: true,
			},

			expectedStatus: task.SFail,
		},
		{
			desc: "pod spec changed, pod is healthy, pod is not leader",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						obj.Status.Conditions = []metav1.Condition{
							{
								Type:   v1alpha1.CondHealth,
								Status: metav1.ConditionTrue,
							},
						}
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						return obj
					}),
					pds: []*v1alpha1.PD{
						fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
							obj.Spec.Version = fakeVersion
							obj.Status.Conditions = []metav1.Condition{
								{
									Type:               v1alpha1.CondHealth,
									Status:             metav1.ConditionTrue,
									LastTransitionTime: metav1.Now(),
								},
							}
							return obj
						}),
						fake.FakeObj("aaa-yyy", func(obj *v1alpha1.PD) *v1alpha1.PD {
							obj.Spec.Version = fakeVersion
							obj.Status.Conditions = []metav1.Condition{
								{
									Type:   v1alpha1.CondHealth,
									Status: metav1.ConditionTrue,
								},
							}
							return obj
						}),
					},
				},
				Healthy: true,
			},

			expectUpdatedPod:         false,
			expectedPodIsTerminating: true,
			expectedStatus:           task.SWait,
		},
		{
			desc: "pod spec hash not changed, config changed, hot reload policy",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						obj.Spec.UpdateStrategy.Config = v1alpha1.ConfigUpdateStrategyHotReload
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyConfigHash:  "newest",
							v1alpha1.LabelKeyPodSpecHash: "6d6499ffc7",
						}
						return obj
					}),
				},
				ConfigHash: "newest",
			},

			expectUpdatedPod: true,
			expectedStatus:   task.SComplete,
		},
		{
			desc: "pod spec hash not changed, config changed, restart policy",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						obj.Spec.UpdateStrategy.Config = v1alpha1.ConfigUpdateStrategyRestart
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyConfigHash:  "old",
							v1alpha1.LabelKeyPodSpecHash: "7cd7474797",
						}
						return obj
					}),
				},
				ConfigHash: "newest",
			},

			expectedPodIsTerminating: true,
			expectUpdatedPod:         false,
			expectedStatus:           task.SWait,
		},
		{
			desc: "pod spec hash not changed, pod labels changed, config not changed",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						obj.Spec.UpdateStrategy.Config = v1alpha1.ConfigUpdateStrategyRestart
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyConfigHash:  "newest",
							v1alpha1.LabelKeyPodSpecHash: "6d6499ffc7",
							"xxx":                        "yyy",
						}
						return obj
					}),
				},
				ConfigHash: "newest",
			},

			expectUpdatedPod: true,
			expectedStatus:   task.SComplete,
		},
		{
			desc: "pod spec hash not changed, pod labels changed, config not changed, apply failed",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						obj.Spec.UpdateStrategy.Config = v1alpha1.ConfigUpdateStrategyRestart
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyConfigHash:  "newest",
							v1alpha1.LabelKeyPodSpecHash: "6d6499ffc7",
							"xxx":                        "yyy",
						}
						return obj
					}),
				},
				ConfigHash: "newest",
			},
			unexpectedErr: true,

			expectedStatus: task.SFail,
		},
		{
			desc: "all are not changed",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Spec.Version = fakeVersion
						obj.Spec.UpdateStrategy.Config = v1alpha1.ConfigUpdateStrategyRestart
						return obj
					}),
					cluster: fake.FakeObj[v1alpha1.Cluster]("aaa"),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstance:    "aaa-xxx",
							v1alpha1.LabelKeyConfigHash:  "newest",
							v1alpha1.LabelKeyPodSpecHash: "6d6499ffc7",
						}
						return obj
					}),
				},
				ConfigHash: "newest",
			},

			expectedStatus: task.SComplete,
		},
	}

	for i := range cases {
		c := &cases[i]
		t.Run(c.desc, func(tt *testing.T) {
			tt.Parallel()

			ctx := context.Background()
			var objs []client.Object
			objs = append(objs, c.state.PD(), c.state.Cluster())
			if c.state.Pod() != nil {
				objs = append(objs, c.state.Pod())
			}
			for _, pd := range c.state.PDSlice() {
				if pd.Name == c.state.PD().Name {
					continue
				}
				objs = append(objs, pd)
			}
			fc := client.NewFakeClient(objs...)
			for _, obj := range c.objs {
				require.NoError(tt, fc.Apply(ctx, obj), c.desc)
			}

			var acts []action
			if c.needRefresh {
				acts = append(acts, refresh())
			}
			if c.needTrasferTo != "" {
				acts = append(acts, transferLeader(ctx, c.needTrasferTo, nil))
			}

			c.state.PDClient = NewFakePDClient(t, acts...)

			if c.unexpectedErr {
				// cannot update pod
				fc.WithError("patch", "*", errors.NewInternalError(fmt.Errorf("fake internal err")))
				fc.WithError("delete", "*", errors.NewInternalError(fmt.Errorf("fake internal err")))
			}

			res, done := task.RunTask(ctx, TaskPod(c.state, fc))
			assert.Equal(tt, c.expectedStatus.String(), res.Status().String(), res.Message())
			assert.False(tt, done, c.desc)

			assert.Equal(tt, c.expectedPodIsTerminating, c.state.PodIsTerminating, c.desc)

			if c.expectUpdatedPod {
				expectedPod := newPod(c.state.Cluster(), c.state.PD(), c.state.ConfigHash)
				actual := c.state.Pod().DeepCopy()
				actual.Kind = ""
				actual.APIVersion = ""
				actual.ManagedFields = nil
				assert.Equal(tt, expectedPod, actual, c.desc)
			}
		})
	}
}

func NewFakePDClient(t *testing.T, acts ...action) pdm.PDClient {
	ctrl := gomock.NewController(t)
	pdc := pdm.NewMockPDClient(ctrl)
	for _, act := range acts {
		act(ctrl, pdc)
	}

	return pdc
}

type action func(ctrl *gomock.Controller, pdc *pdm.MockPDClient)

func refresh() action {
	return func(ctrl *gomock.Controller, pdc *pdm.MockPDClient) {
		cache := pdm.NewMockMemberCache[pdv1.Member](ctrl)
		cache.EXPECT().Refresh()
		pdc.EXPECT().Members().Return(cache)
	}
}

func transferLeader(ctx context.Context, name string, err error) action {
	return func(ctrl *gomock.Controller, pdc *pdm.MockPDClient) {
		underlay := pdapi.NewMockPDClient(ctrl)
		pdc.EXPECT().Underlay().Return(underlay)
		underlay.EXPECT().TransferPDLeader(ctx, name).Return(err)
	}
}