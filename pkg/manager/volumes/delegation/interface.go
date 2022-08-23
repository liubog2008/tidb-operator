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

	Name() string
}
