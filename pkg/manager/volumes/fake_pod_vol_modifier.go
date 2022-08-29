package volumes

import (
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"

	corev1 "k8s.io/api/core/v1"
)

var _ PodVolumeModifier = &FakePodVolumeModifier{}

type FakePodVolumeModifier struct {
	ModifyFunc func(tc *v1alpha1.TidbCluster, pod *corev1.Pod, expected []DesiredVolume, shouldEvictLeader bool) (bool, error)
}

func (pvm *FakePodVolumeModifier) SetResult(completed bool, err error) {
	pvm.ModifyFunc = func(tc *v1alpha1.TidbCluster, pod *corev1.Pod, expected []DesiredVolume, shouldEvictLeader bool) (bool, error) {
		return completed, err
	}
}

func (pvm *FakePodVolumeModifier) Modify(tc *v1alpha1.TidbCluster, pod *corev1.Pod,
	expected []DesiredVolume, shouldEvictLeader bool) (bool, error) {
	if pvm.ModifyFunc == nil {
		return false, nil
	}
	return pvm.ModifyFunc(tc, pod, expected, shouldEvictLeader)
}
