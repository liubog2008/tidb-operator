package volumes

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"
)

type VolumePhase int

const (
	VolumePhaseUnknown VolumePhase = iota
	// 1. isPVCRevisionChanged: false
	// 2. needModify: true
	// 3. waitForNextTime: true
	VolumePhasePending
	// 1. isPVCRevisionChanged: false
	// 2. needModify: true
	// 3. waitForNextTime: false
	VolumePhasePreparing
	// 1. isPVCRevisionChanged: true
	// 2. needModify: true/false
	// 3. waitForNextTime: false
	VolumePhaseWaitForLeaderEviction
	// 1. isPVCRevisionChanged: true
	// 2. needModify: true/false
	// 3. waitForNextTime: true
	VolumePhaseModifying
	// 1. isPVCRevisionChanged: false
	// 2. needModify: false
	// 3. waitForNextTime: true/false
	VolumePhaseModified
)

func (p VolumePhase) String() string {
	switch p {
	case VolumePhasePending:
		return "Pending"
	case VolumePhasePreparing:
		return "Preparing"
	case VolumePhaseWaitForLeaderEviction:
		return "WaitForLeaderEviction"
	case VolumePhaseModifying:
		return "Modifying"
	case VolumePhaseModified:
		return "Modified"
	}

	return "Unknown"
}

func getVolumePhase(vol *ActualVolume) VolumePhase {
	if isPVCRevisionChanged(vol.PVC) {
		if !waitForNextTime(vol.PVC) {
			return VolumePhaseWaitForLeaderEviction
		}
		return VolumePhaseModifying
	}

	if !needModify(vol.PVC, vol.Desired) {
		return VolumePhaseModified
	}

	if waitForNextTime(vol.PVC) {
		return VolumePhasePending
	}

	return VolumePhasePreparing
}

func isPVCRevisionChanged(pvc *corev1.PersistentVolumeClaim) bool {
	specRevision := pvc.Annotations[annoKeyPVCSpecRevision]
	statusRevision := pvc.Annotations[annoKeyPVCSpecRevision]

	return specRevision != statusRevision
}

func waitForNextTime(pvc *corev1.PersistentVolumeClaim) bool {
	str, ok := pvc.Annotations[annoKeyPVCLastTransitionTimestamp]
	if !ok {
		return false
	}
	timestamp, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return false
	}
	d := time.Since(timestamp)

	return d < defaultModifyWaitingDuration
}

func needModify(pvc *corev1.PersistentVolumeClaim, desired *DesiredVolume) bool {
	size := desired.Size
	scName := ""
	if desired.StorageClass != nil {
		scName = desired.StorageClass.Name
	}

	return isPVCStatusMatched(pvc, scName, size)
}

func isPVCStatusMatched(pvc *corev1.PersistentVolumeClaim, scName, size string) bool {
	isChanged := false
	oldSc, ok := pvc.Annotations[annoKeyPVCStatusStorageClass]
	if !ok {
		oldSc = ignoreNil(pvc.Spec.StorageClassName)
	}
	if oldSc != scName {
		isChanged = true
	}

	oldSize, ok := pvc.Annotations[annoKeyPVCStatusStorageSize]
	if !ok {
		quantity := getStorageSize(pvc.Spec.Resources.Requests)
		oldSize = quantity.String()
	}
	if oldSize != size {
		isChanged = true
	}
	klog.Infof("old sc %s vs new sc %v, old size %v vs new size %v", oldSc, scName, oldSize, size)

	return isChanged
}
