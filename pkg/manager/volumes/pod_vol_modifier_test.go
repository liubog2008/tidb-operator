package volumes

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/stretchr/testify/assert"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/manager/volumes/delegation"
)

func newTestPodForModify() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "test",
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "test",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc",
						},
					},
				},
			},
		},
	}
}

func newTestPVCForModify(sc *string, specSize, statusSize string, anno map[string]string) *corev1.PersistentVolumeClaim {
	a := resource.MustParse(specSize)
	b := resource.MustParse(statusSize)

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test",
			Annotations: anno,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: a,
				},
			},
			VolumeName:       "test-pv",
			StorageClassName: sc,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: b,
			},
		},
	}
}

func newTestPVForModify() *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
	}
}

func newTestSCForModify(name, provisioner string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Provisioner: provisioner,
	}
}

func newTidbClusterForModify(leaderCount int32) *v1alpha1.TidbCluster {
	return &v1alpha1.TidbCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "test",
		},
		Status: v1alpha1.TidbClusterStatus{
			TiKV: v1alpha1.TiKVStatus{
				Stores: map[string]v1alpha1.TiKVStore{
					"1": {
						PodName:     "test",
						LeaderCount: leaderCount,
					},
				},
			},
		},
	}
}

func TestModify(t *testing.T) {
	oldSize := "10Gi"
	newSize := "20Gi"
	oldSc := "old"
	// newSc := "new"

	provisioner := "test"

	cases := []struct {
		desc              string
		tc                *v1alpha1.TidbCluster
		pod               *corev1.Pod
		shouldEvictLeader bool

		pvc  *corev1.PersistentVolumeClaim
		pv   *corev1.PersistentVolume
		sc   *storagev1.StorageClass
		size string

		isModifyVolumeFinished bool

		expectedPVC    *corev1.PersistentVolumeClaim
		expectedWait   bool
		expectedHasErr bool
	}{
		{
			desc: "volume is not changed",
			tc:   newTidbClusterForModify(0),
			pod:  newTestPodForModify(),

			pvc:  newTestPVCForModify(&oldSc, oldSize, oldSize, nil),
			pv:   newTestPVForModify(),
			sc:   newTestSCForModify(oldSc, provisioner),
			size: oldSize,

			expectedPVC:  newTestPVCForModify(&oldSc, oldSize, oldSize, nil),
			expectedWait: false,
		},
		{
			desc: "volume size is changed, and revision has not been upgraded",
			tc:   newTidbClusterForModify(0),
			pod:  newTestPodForModify(),

			pvc:  newTestPVCForModify(&oldSc, oldSize, oldSize, nil),
			pv:   newTestPVForModify(),
			sc:   newTestSCForModify(oldSc, provisioner),
			size: newSize,

			isModifyVolumeFinished: false,

			expectedPVC: newTestPVCForModify(&oldSc, oldSize, oldSize, map[string]string{
				annoKeyPVCSpecRevision:     "1",
				annoKeyPVCSpecStorageClass: oldSc,
				annoKeyPVCSpecStorageSize:  newSize,
			}),
			expectedWait:   false,
			expectedHasErr: true,
		},
		{
			desc: "volume size is changed, and delegate modification is finished",
			tc:   newTidbClusterForModify(0),
			pod:  newTestPodForModify(),

			pvc: newTestPVCForModify(&oldSc, oldSize, oldSize, map[string]string{
				annoKeyPVCSpecRevision:     "1",
				annoKeyPVCSpecStorageClass: oldSc,
				annoKeyPVCSpecStorageSize:  newSize,
			}),
			pv:   newTestPVForModify(),
			sc:   newTestSCForModify(oldSc, provisioner),
			size: newSize,

			isModifyVolumeFinished: true,

			expectedPVC: newTestPVCForModify(&oldSc, newSize, oldSize, map[string]string{
				annoKeyPVCSpecRevision:     "1",
				annoKeyPVCSpecStorageClass: oldSc,
				annoKeyPVCSpecStorageSize:  newSize,
			}),
			expectedWait:   false,
			expectedHasErr: true,
		},
		{
			desc: "volume size is changed, and fs resize is finished",
			tc:   newTidbClusterForModify(0),
			pod:  newTestPodForModify(),

			pvc: newTestPVCForModify(&oldSc, newSize, newSize, map[string]string{
				annoKeyPVCSpecRevision:     "1",
				annoKeyPVCSpecStorageClass: oldSc,
				annoKeyPVCSpecStorageSize:  newSize,
			}),
			pv:   newTestPVForModify(),
			sc:   newTestSCForModify(oldSc, provisioner),
			size: newSize,

			isModifyVolumeFinished: true,

			expectedPVC: newTestPVCForModify(&oldSc, newSize, newSize, map[string]string{
				annoKeyPVCSpecRevision:       "1",
				annoKeyPVCSpecStorageClass:   oldSc,
				annoKeyPVCSpecStorageSize:    newSize,
				annoKeyPVCStatusRevision:     "1",
				annoKeyPVCStatusStorageClass: oldSc,
				annoKeyPVCStatusStorageSize:  newSize,
			}),
			expectedWait: false,
		},
		{
			desc:              "volume size is changed, but leader count is not 0",
			tc:                newTidbClusterForModify(10),
			pod:               newTestPodForModify(),
			shouldEvictLeader: true,

			pvc:  newTestPVCForModify(&oldSc, oldSize, oldSize, nil),
			pv:   newTestPVForModify(),
			sc:   newTestSCForModify(oldSc, provisioner),
			size: newSize,

			isModifyVolumeFinished: false,

			expectedPVC:  newTestPVCForModify(&oldSc, oldSize, oldSize, nil),
			expectedWait: true,
		},
		{
			desc:              "volume size is changed, and leader count is 0",
			tc:                newTidbClusterForModify(0),
			pod:               newTestPodForModify(),
			shouldEvictLeader: true,

			pvc:  newTestPVCForModify(&oldSc, oldSize, oldSize, nil),
			pv:   newTestPVForModify(),
			sc:   newTestSCForModify(oldSc, provisioner),
			size: newSize,

			isModifyVolumeFinished: false,

			expectedPVC: newTestPVCForModify(&oldSc, oldSize, oldSize, map[string]string{
				annoKeyPVCSpecRevision:     "1",
				annoKeyPVCSpecStorageClass: oldSc,
				annoKeyPVCSpecStorageSize:  newSize,
			}),
			expectedWait:   false,
			expectedHasErr: true,
		},
	}

	for _, c := range cases {
		kc := fake.NewSimpleClientset(
			c.pvc,
			c.pv,
			c.sc,
		)
		stopCh := make(chan struct{})

		f := informers.NewSharedInformerFactory(kc, 0)
		pvcLister := f.Core().V1().PersistentVolumeClaims().Lister()
		pvLister := f.Core().V1().PersistentVolumes().Lister()
		scLister := f.Storage().V1().StorageClasses().Lister()

		f.Start(stopCh)
		f.WaitForCacheSync(stopCh)

		m := delegation.NewMockVolumeModifier(provisioner, time.Hour)

		m.ModifyVolumeFunc = func(_ context.Context, pvc *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, sc *storagev1.StorageClass) (bool, error) {
			return !c.isModifyVolumeFinished, nil
		}

		pvm := &podVolModifier{
			deps: &controller.Dependencies{
				KubeClientset:      kc,
				PVCLister:          pvcLister,
				PVLister:           pvLister,
				StorageClassLister: scLister,
			},
			modifiers: map[string]delegation.VolumeModifier{
				m.Name(): m,
			},
		}

		desired := []DesiredVolume{
			{
				Name:         "test",
				Size:         c.size,
				StorageClass: c.sc,
			},
		}

		wait, err := pvm.Modify(c.tc, c.pod, desired, c.shouldEvictLeader)
		if err != nil {
			assert.True(t, c.expectedHasErr, c.desc+", err: %v", err)
		}
		assert.Equal(t, c.expectedWait, wait, c.desc)

		resultPVC, err := kc.CoreV1().PersistentVolumeClaims(c.pvc.Namespace).Get(context.TODO(), c.pvc.Name, metav1.GetOptions{})
		assert.NoError(t, err, c.desc)
		delete(resultPVC.Annotations, annoKeyPVCLastTransitionTimestamp)
		assert.Equal(t, c.expectedPVC, resultPVC, c.desc)
	}
}
