package volumes

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pingcap/tidb-operator/pkg/manager/volumes/delegation"
)

func newTestPVCForGetVolumePhase(size string, sc *string, annotations map[string]string) *corev1.PersistentVolumeClaim {
	q := resource.MustParse(size)

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: q,
				},
			},
			StorageClassName: sc,
		},
	}
}

func newStorageClassForGetVolumePhase(name, provisioner string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Provisioner: provisioner,
	}
}

func TestGetVolumePhase(t *testing.T) {
	oldScName := "old"
	newScName := "new"

	oldSize := "10Gi"
	newSize := "20Gi"

	lastTransTime := metav1.Now().Format(time.RFC3339)

	cases := []struct {
		desc     string
		pvc      *corev1.PersistentVolumeClaim
		sc       *storagev1.StorageClass
		size     string
		expected VolumePhase
	}{
		{
			desc: "size and sc are not modified",
			pvc:  newTestPVCForGetVolumePhase(oldSize, &oldScName, nil),
			sc:   newStorageClassForGetVolumePhase(oldScName, "ebs.csi.aws.com"),
			size: oldSize,

			expected: VolumePhaseModified,
		},
		{
			desc: "desired size is changed, but spec revision is not changed",
			pvc:  newTestPVCForGetVolumePhase(oldSize, &oldScName, nil),
			sc:   newStorageClassForGetVolumePhase(oldScName, "ebs.csi.aws.com"),
			size: newSize,

			expected: VolumePhasePreparing,
		},
		{
			desc: "desired sc is changed, but spec revision is not changed",
			pvc:  newTestPVCForGetVolumePhase(oldSize, &oldScName, nil),
			sc:   newStorageClassForGetVolumePhase(newScName, "ebs.csi.aws.com"),
			size: oldSize,

			expected: VolumePhasePreparing,
		},
		{
			desc: "desired size and sc are changed, and spec revision is changed",
			pvc: newTestPVCForGetVolumePhase(oldSize, &oldScName, map[string]string{
				annoKeyPVCSpecRevision:            "1",
				annoKeyPVCSpecStorageClass:        newScName,
				annoKeyPVCSpecStorageSize:         newSize,
				annoKeyPVCLastTransitionTimestamp: lastTransTime,
			}),
			sc:   newStorageClassForGetVolumePhase(newScName, "ebs.csi.aws.com"),
			size: newSize,

			expected: VolumePhaseModifying,
		},
		{
			desc: "desired size and sc are changed, spec revision and status revision are also changed",
			pvc: newTestPVCForGetVolumePhase(oldSize, &oldScName, map[string]string{
				annoKeyPVCSpecRevision:            "1",
				annoKeyPVCSpecStorageClass:        newScName,
				annoKeyPVCSpecStorageSize:         newSize,
				annoKeyPVCLastTransitionTimestamp: lastTransTime,
				annoKeyPVCStatusRevision:          "1",
				annoKeyPVCStatusStorageClass:      newScName,
				annoKeyPVCStatusStorageSize:       newSize,
			}),
			sc:   newStorageClassForGetVolumePhase(newScName, "ebs.csi.aws.com"),
			size: newSize,

			expected: VolumePhaseModified,
		},
		{
			desc: "after size is changed, need to wait cool down time to change sc",
			pvc: newTestPVCForGetVolumePhase(oldSize, &oldScName, map[string]string{
				annoKeyPVCSpecRevision:            "1",
				annoKeyPVCSpecStorageClass:        oldScName,
				annoKeyPVCSpecStorageSize:         newSize,
				annoKeyPVCLastTransitionTimestamp: lastTransTime,
				annoKeyPVCStatusRevision:          "1",
				annoKeyPVCStatusStorageClass:      oldScName,
				annoKeyPVCStatusStorageSize:       newSize,
			}),
			sc:   newStorageClassForGetVolumePhase(newScName, "ebs.csi.aws.com"),
			size: newSize,

			expected: VolumePhasePending,
		},
		{
			desc: "after size is changed, need to wait cool down time to change sc, but cool down time is 0",
			pvc: newTestPVCForGetVolumePhase(oldSize, &oldScName, map[string]string{
				annoKeyPVCSpecRevision:            "1",
				annoKeyPVCSpecStorageClass:        oldScName,
				annoKeyPVCSpecStorageSize:         newSize,
				annoKeyPVCLastTransitionTimestamp: lastTransTime,
				annoKeyPVCStatusRevision:          "1",
				annoKeyPVCStatusStorageClass:      oldScName,
				annoKeyPVCStatusStorageSize:       newSize,
			}),
			sc:   newStorageClassForGetVolumePhase(newScName, "no.wait.time.sc"),
			size: newSize,

			expected: VolumePhasePreparing,
		},
	}

	pvm := &podVolModifier{
		modifiers: map[string]delegation.VolumeModifier{
			"ebs.csi.aws.com": delegation.NewMockVolumeModifier("ebs.csi.aws.com", time.Hour*6),
			"no.wait.time.sc": delegation.NewMockVolumeModifier("no.wait.time.sc", 0),
		},
	}

	g := NewGomegaWithT(t)
	for _, c := range cases {
		actual := ActualVolume{
			PVC: c.pvc,
			Desired: &DesiredVolume{
				StorageClass: c.sc,
				Size:         resource.MustParse(c.size),
			},
		}
		phase := pvm.getVolumePhase(&actual)
		g.Expect(phase).Should(Equal(c.expected), c.desc)
	}
}
