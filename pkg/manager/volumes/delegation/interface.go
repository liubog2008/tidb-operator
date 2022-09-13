package delegation

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

type VolumeModifier interface {
	MinWaitDuration() time.Duration
	// ModifyVolume modifies the underlay volume of pvc to match the args of storageclass
	ModifyVolume(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, sc *storagev1.StorageClass) (bool, error)

	Validate(spvc, dpvc *corev1.PersistentVolumeClaim, ssc, dsc *storagev1.StorageClass) error

	Name() string
}

var _ VolumeModifier = &MockVolumeModifier{}

type ModifyVolumeFunc func(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, sc *storagev1.StorageClass) (bool, error)

type ValidateFunc func(spvc, dpvc *corev1.PersistentVolumeClaim, ssc, dsc *storagev1.StorageClass) error

type MockVolumeModifier struct {
	name            string
	minWaitDuration time.Duration

	ValidateFunc     ValidateFunc
	ModifyVolumeFunc ModifyVolumeFunc
}

func NewMockVolumeModifier(name string, minWaitDuration time.Duration) *MockVolumeModifier {
	return &MockVolumeModifier{
		name:            name,
		minWaitDuration: minWaitDuration,
	}
}

func (m *MockVolumeModifier) Name() string {
	return m.name
}

func (m *MockVolumeModifier) MinWaitDuration() time.Duration {
	return m.minWaitDuration
}

func (m *MockVolumeModifier) ModifyVolume(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, sc *storagev1.StorageClass) (bool, error) {
	return m.ModifyVolumeFunc(ctx, pvc, pv, sc)
}

func (m *MockVolumeModifier) Validate(spvc, dpvc *corev1.PersistentVolumeClaim, ssc, dsc *storagev1.StorageClass) error {
	return m.ValidateFunc(spvc, dpvc, ssc, dsc)
}
