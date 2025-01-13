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

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/pingcap/tidb-operator/apis/core/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/client"
	"github.com/pingcap/tidb-operator/pkg/controllers/common"
	"github.com/pingcap/tidb-operator/pkg/utils/fake"
	"github.com/pingcap/tidb-operator/pkg/utils/task/v3"
	"github.com/pingcap/tidb-operator/pkg/volumes"
)

func TestTaskPVC(t *testing.T) {
	cases := []struct {
		desc          string
		state         common.PDState
		pvcs          []*corev1.PersistentVolumeClaim
		unexpectedErr bool

		expectedStatus task.Status
		expectedPVCNum int
	}{
		{
			desc: "no pvc",
			state: &state{
				pd: fake.FakeObj[v1alpha1.PD]("aaa-xxx"),
			},
			expectedStatus: task.SComplete,
			expectedPVCNum: 0,
		},
		{
			desc: "create a data vol",
			state: &state{
				pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
					obj.Spec.Volumes = []v1alpha1.Volume{
						{
							Name:    "data",
							Storage: resource.MustParse("10Gi"),
						},
					}
					return obj
				}),
			},
			expectedStatus: task.SComplete,
			expectedPVCNum: 1,
		},
		{
			desc: "has a data vol",
			state: &state{
				pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
					obj.Spec.Volumes = []v1alpha1.Volume{
						{
							Name:    "data",
							Storage: resource.MustParse("10Gi"),
						},
					}
					return obj
				}),
			},
			pvcs: []*corev1.PersistentVolumeClaim{
				fake.FakeObj("data-aaa-pd-xxx", func(obj *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					obj.Status.Phase = corev1.ClaimBound
					return obj
				}),
			},
			expectedStatus: task.SComplete,
			expectedPVCNum: 1,
		},
		{
			desc: "has a data vol, but failed to apply",
			state: &state{
				pd: fake.FakeObj("aaa-xxx", func(obj *v1alpha1.PD) *v1alpha1.PD {
					obj.Spec.Volumes = []v1alpha1.Volume{
						{
							Name:    "data",
							Storage: resource.MustParse("10Gi"),
						},
					}
					return obj
				}),
			},
			pvcs: []*corev1.PersistentVolumeClaim{
				fake.FakeObj("data-aaa-pd-xxx", func(obj *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
					obj.Status.Phase = corev1.ClaimBound
					return obj
				}),
			},
			unexpectedErr: true,

			expectedStatus: task.SFail,
			expectedPVCNum: 1,
		},
	}

	for i := range cases {
		c := &cases[i]
		t.Run(c.desc, func(tt *testing.T) {
			tt.Parallel()

			ctx := context.Background()
			var objs []client.Object
			objs = append(objs, c.state.PD())
			fc := client.NewFakeClient(objs...)
			for _, obj := range c.pvcs {
				require.NoError(tt, fc.Apply(ctx, obj), c.desc)
			}

			ctrl := gomock.NewController(tt)
			vm := volumes.NewMockModifier(ctrl)
			expectedPVCs := newPVCs(c.state.PD())
			for _, expected := range expectedPVCs {
				for _, current := range c.pvcs {
					if current.Name == expected.Name {
						vm.EXPECT().GetActualVolume(ctx, expected, current).Return(&volumes.ActualVolume{
							Desired: &volumes.DesiredVolume{},
							PVC:     current,
						}, nil)
						vm.EXPECT().ShouldModify(ctx, &volumes.ActualVolume{
							Desired: &volumes.DesiredVolume{},
							PVC:     current,
						}).Return(false)
					}
				}
			}

			if c.unexpectedErr {
				// cannot update pvc
				fc.WithError("patch", "*", errors.NewInternalError(fmt.Errorf("fake internal err")))
			}

			res, done := task.RunTask(ctx, TaskPVC(c.state, logr.Discard(), fc, vm))
			assert.Equal(tt, c.expectedStatus.String(), res.Status().String(), res.Message())
			assert.False(tt, done, c.desc)

			pvcs := corev1.PersistentVolumeClaimList{}
			require.NoError(tt, fc.List(ctx, &pvcs), c.desc)
			assert.Len(tt, pvcs.Items, c.expectedPVCNum, c.desc)
		})
	}
}
