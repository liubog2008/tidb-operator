package volumes

import (
	"fmt"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"

	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	storagelister "k8s.io/client-go/listers/storage/v1"
	klog "k8s.io/klog/v2"
)

// TODO: it should be refactored
func GetDesiredVolumesForTCComponent(tc *v1alpha1.TidbCluster, mt v1alpha1.MemberType, scLister storagelister.StorageClassLister) ([]DesiredVolume, error) {
	desiredVolumes := []DesiredVolume{}

	storageVolumes := []v1alpha1.StorageVolume{}
	switch mt {
	case v1alpha1.PDMemberType:
		sc, err := getStorageClass(tc.Spec.PD.StorageClassName, scLister)
		if err != nil {
			return nil, err
		}
		name := v1alpha1.GetStorageVolumeName("", mt)
		size := getStorageSize(tc.Spec.PD.Requests)
		d := DesiredVolume{
			Name:         string(name),
			Size:         size.String(),
			StorageClass: sc,
		}
		desiredVolumes = append(desiredVolumes, d)

		storageVolumes = tc.Spec.PD.StorageVolumes

	case v1alpha1.TiDBMemberType:
		storageVolumes = tc.Spec.TiDB.StorageVolumes

	case v1alpha1.TiKVMemberType:
		sc, err := getStorageClass(tc.Spec.TiKV.StorageClassName, scLister)
		if err != nil {
			return nil, err
		}
		name := v1alpha1.GetStorageVolumeName("", mt)
		size := getStorageSize(tc.Spec.TiKV.Requests)
		d := DesiredVolume{
			Name:         string(name),
			Size:         size.String(),
			StorageClass: sc,
		}
		desiredVolumes = append(desiredVolumes, d)

		storageVolumes = tc.Spec.TiKV.StorageVolumes

	case v1alpha1.TiFlashMemberType:
		for i, claim := range tc.Spec.TiFlash.StorageClaims {
			sc, err := getStorageClass(claim.StorageClassName, scLister)
			if err != nil {
				return nil, err
			}
			name := v1alpha1.GetStorageVolumeNameForTiFlash(i)
			size := getStorageSize(claim.Resources.Requests)
			d := DesiredVolume{
				Name:         string(name),
				Size:         size.String(),
				StorageClass: sc,
			}
			desiredVolumes = append(desiredVolumes, d)
		}

	case v1alpha1.TiCDCMemberType:
		storageVolumes = tc.Spec.TiCDC.StorageVolumes

	case v1alpha1.PumpMemberType:
		sc, err := getStorageClass(tc.Spec.Pump.StorageClassName, scLister)
		if err != nil {
			return nil, err
		}
		name := v1alpha1.GetStorageVolumeName("", mt)
		size := getStorageSize(tc.Spec.Pump.Requests)
		d := DesiredVolume{
			Name:         string(name),
			Size:         size.String(),
			StorageClass: sc,
		}
		desiredVolumes = append(desiredVolumes, d)
	default:
		return nil, fmt.Errorf("unsupported member type %s", mt)
	}

	for _, sv := range storageVolumes {
		if quantity, err := resource.ParseQuantity(sv.StorageSize); err == nil {
			sc, err := getStorageClass(sv.StorageClassName, scLister)
			if err != nil {
				return nil, err
			}
			name := v1alpha1.GetStorageVolumeName(sv.Name, mt)
			d := DesiredVolume{
				Name:         string(name),
				Size:         quantity.String(),
				StorageClass: sc,
			}

			desiredVolumes = append(desiredVolumes, d)

		} else {
			klog.Warningf("StorageVolume %q in %s .spec.%s is invalid", sv.Name, getTcKey(tc), mt)
		}
	}

	return desiredVolumes, nil
}

func getStorageClass(name *string, scLister storagelister.StorageClassLister) (*storagev1.StorageClass, error) {
	if name == nil {
		return nil, nil
	}
	return scLister.Get(*name)
}
