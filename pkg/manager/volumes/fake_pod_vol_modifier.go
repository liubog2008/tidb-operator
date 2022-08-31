package volumes

import (
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"

	corev1 "k8s.io/api/core/v1"
)

var _ PodVolumeModifier = &FakePodVolumeModifier{}

type FakePodVolumeModifier struct {
	ModifyFunc            func(tc *v1alpha1.TidbCluster, pod *corev1.Pod, expected []DesiredVolume, shouldEvictLeader bool) (bool, error)
	GetDesiredVolumesFunc func(tc *v1alpha1.TidbCluster, mt v1alpha1.MemberType) ([]DesiredVolume, error)
	GetActualVolumesFunc  func(pod *corev1.Pod, vs []DesiredVolume) ([]ActualVolume, error)
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

func (pvm *FakePodVolumeModifier) GetDesiredVolumes(tc *v1alpha1.TidbCluster, mt v1alpha1.MemberType) ([]DesiredVolume, error) {
	if pvm.GetDesiredVolumesFunc == nil {
		return nil, nil
	}
	return pvm.GetDesiredVolumesFunc(tc, mt)
}

func (pvm *FakePodVolumeModifier) GetActualVolumes(pod *corev1.Pod, vs []DesiredVolume) ([]ActualVolume, error) {
	if pvm.GetActualVolumesFunc == nil {
		return nil, nil
	}
	return pvm.GetActualVolumesFunc(pod, vs)
}
