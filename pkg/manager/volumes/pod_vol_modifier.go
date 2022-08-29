package volumes

import (
	"context"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	errutil "k8s.io/apimachinery/pkg/util/errors"
	klog "k8s.io/klog/v2"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/manager/volumes/delegation"
	"github.com/pingcap/tidb-operator/pkg/manager/volumes/delegation/aws"
)

type PodVolumeModifier interface {
	Modify(tc *v1alpha1.TidbCluster, pod *corev1.Pod, expected []DesiredVolume, shouldEvictLeader bool) (bool, error)
}

type DesiredVolume struct {
	Name         string
	Size         string
	StorageClass *storagev1.StorageClass
}

type ActualVolume struct {
	Desired *DesiredVolume
	PVC     *corev1.PersistentVolumeClaim
	PV      *corev1.PersistentVolume
	Phase   VolumePhase
}

type podVolModifier struct {
	deps *controller.Dependencies

	modifiers map[string]delegation.VolumeModifier
}

func NewPodVolumeModifier(deps *controller.Dependencies) PodVolumeModifier {
	return &podVolModifier{
		deps: deps,
		modifiers: map[string]delegation.VolumeModifier{
			"aws": aws.NewEBSModifier(deps.AWSConfig),
		},
	}
}

func (p *podVolModifier) Modify(tc *v1alpha1.TidbCluster, pod *corev1.Pod, expected []DesiredVolume, shouldEvictLeader bool) (bool, error) {
	ctx := context.TODO()

	actual, err := p.getActualVolumes(pod, expected)
	if err != nil {
		return false, err
	}

	completed := true
	isEvicted := true
	if shouldEvictLeader {
		isEvicted = !isLeaderEvictedOrTimeout(tc, pod)
	}

	errs := []error{}

	for i := range actual {
		vol := &actual[i]
		klog.Infof("try to sync volume %s/%s, phase: %s", vol.PVC.Namespace, vol.PVC.Name, vol.Phase)

		switch vol.Phase {
		case VolumePhasePreparing:
			if err := p.modifyPVCAnnoSpec(ctx, vol, shouldEvictLeader); err != nil {
				errs = append(errs, err)
				continue
			}

			fallthrough
		case VolumePhaseWaitForLeaderEviction:
			if shouldEvictLeader {
				if isEvicted {

					completed = false
					continue
				}
				if err := p.modifyPVCAnnoSpecLastTransitionTimestamp(ctx, vol); err != nil {
					errs = append(errs, err)
					continue
				}
			}

			fallthrough
		case VolumePhaseModifying:
			wait, err := p.modifyVolume(ctx, vol)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if wait {
				completed = false
				continue
			}
			// try to resize fs
			synced, err := p.syncPVCSize(ctx, vol)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !synced {
				completed = false
				continue
			}
			if err := p.modifyPVCAnnoStatus(ctx, vol); err != nil {
				errs = append(errs, err)
			}
		case VolumePhasePending, VolumePhaseModified:
		}

	}

	return completed, errutil.NewAggregate(errs)
}

func getDesiredVolumeByName(vs []DesiredVolume, name string) *DesiredVolume {
	for i := range vs {
		v := &vs[i]
		if v.Name == name {
			return v
		}
	}

	return nil
}
func (p *podVolModifier) getBoundPVFromPVC(pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolume, error) {
	name := pvc.Spec.VolumeName

	return p.deps.PVLister.Get(name)
}

func (p *podVolModifier) getPVC(ns string, vol *corev1.Volume) (*corev1.PersistentVolumeClaim, error) {
	if vol.PersistentVolumeClaim == nil {
		return nil, nil
	}

	pvc, err := p.deps.PVCLister.PersistentVolumeClaims(ns).Get(vol.PersistentVolumeClaim.ClaimName)
	if err != nil {
		return nil, err
	}

	return pvc, nil
}

func (p *podVolModifier) getActualVolumes(pod *corev1.Pod, vs []DesiredVolume) ([]ActualVolume, error) {
	vols := []ActualVolume{}

	for i := range pod.Spec.Volumes {
		vol := &pod.Spec.Volumes[i]
		actual, err := p.NewActualVolumeOfPod(vs, pod.Namespace, vol)
		if err != nil {
			return nil, err
		}
		if actual == nil {
			continue
		}

		vols = append(vols, *actual)
	}

	return vols, nil
}

func (p *podVolModifier) NewActualVolumeOfPod(vs []DesiredVolume, ns string, vol *corev1.Volume) (*ActualVolume, error) {
	pvc, err := p.getPVC(ns, vol)
	if err != nil {
		return nil, err
	}
	if pvc == nil {
		return nil, nil
	}

	// TODO: fix the case when pvc is pending
	pv, err := p.getBoundPVFromPVC(pvc)
	if err != nil {
		return nil, err
	}

	desired := getDesiredVolumeByName(vs, vol.Name)

	actual := ActualVolume{
		Desired: desired,
		PVC:     pvc,
		PV:      pv,
	}

	phase := getVolumePhase(&actual)
	actual.Phase = phase

	return &actual, nil
}

func upgradeRevision(pvc *corev1.PersistentVolumeClaim) {
	rev := 1
	str, ok := pvc.Annotations[annoKeyPVCSpecRevision]
	if ok {
		oldRev, err := strconv.Atoi(str)
		if err != nil {
			klog.Warningf("revision format err: %v, reset to 0", err)
			oldRev = 0
		}
		rev = oldRev + 1
	}

	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}

	pvc.Annotations[annoKeyPVCSpecRevision] = strconv.Itoa(rev)
}

func isPVCSpecMatched(pvc *corev1.PersistentVolumeClaim, scName, size string) bool {
	isChanged := false
	oldSc := pvc.Annotations[annoKeyPVCSpecStorageClass]
	if oldSc != scName {
		isChanged = true
	}

	oldSize, ok := pvc.Annotations[annoKeyPVCSpecStorageSize]
	if !ok {
		quantity := getStorageSize(pvc.Spec.Resources.Requests)
		oldSize = quantity.String()
	}
	if oldSize != size {
		isChanged = true
	}

	return isChanged
}

func snapshotStorageClassAndSize(pvc *corev1.PersistentVolumeClaim, scName, size string) bool {
	isChanged := isPVCSpecMatched(pvc, scName, size)

	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}

	pvc.Annotations[annoKeyPVCSpecStorageClass] = scName
	pvc.Annotations[annoKeyPVCSpecStorageSize] = size

	return isChanged
}

func setLastTransitionTimestamp(pvc *corev1.PersistentVolumeClaim) {
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}

	pvc.Annotations[annoKeyPVCLastTransitionTimestamp] = metav1.Now().Format(time.RFC3339)
}

func (p *podVolModifier) modifyPVCAnnoSpecLastTransitionTimestamp(ctx context.Context, vol *ActualVolume) error {
	pvc := vol.PVC.DeepCopy()
	setLastTransitionTimestamp(pvc)
	updated, err := p.deps.KubeClientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	vol.PVC = updated

	return nil

}

// upgrade revision and snapshot the expected storageclass and size of volume
func (p *podVolModifier) modifyPVCAnnoSpec(ctx context.Context, vol *ActualVolume, shouldEvict bool) error {
	pvc := vol.PVC.DeepCopy()

	size := vol.Desired.Size
	scName := ""
	if vol.Desired.StorageClass != nil {
		scName = vol.Desired.StorageClass.Name
	}

	isChanged := snapshotStorageClassAndSize(pvc, scName, size)
	if isChanged {
		upgradeRevision(pvc)
	}

	if !shouldEvict {
		setLastTransitionTimestamp(pvc)
	}

	updated, err := p.deps.KubeClientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	vol.PVC = updated

	return nil
}

func (p *podVolModifier) syncPVCSize(ctx context.Context, vol *ActualVolume) (bool, error) {
	capacity := vol.PVC.Status.Capacity.Storage()
	requestSize := vol.PVC.Spec.Resources.Requests.Storage()
	if requestSize.String() == vol.Desired.Size && capacity.String() == vol.Desired.Size {
		return true, nil
	}
	if requestSize.String() == vol.Desired.Size {
		return false, nil
	}

	pvc := vol.PVC.DeepCopy()
	q, err := resource.ParseQuantity(vol.Desired.Size)
	if err != nil {
		return false, err
	}
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = q

	updated, err := p.deps.KubeClientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return false, err
	}

	vol.PVC = updated

	return false, nil
}

func (p *podVolModifier) modifyPVCAnnoStatus(ctx context.Context, vol *ActualVolume) error {
	pvc := vol.PVC.DeepCopy()

	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}

	pvc.Annotations[annoKeyPVCStatusRevision] = pvc.Annotations[annoKeyPVCSpecRevision]
	pvc.Annotations[annoKeyPVCStatusStorageClass] = pvc.Annotations[annoKeyPVCSpecStorageClass]
	pvc.Annotations[annoKeyPVCStatusStorageSize] = pvc.Annotations[annoKeyPVCSpecStorageSize]

	updated, err := p.deps.KubeClientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	vol.PVC = updated

	return nil
}

func (p *podVolModifier) modifyVolume(ctx context.Context, vol *ActualVolume) (bool, error) {
	m, err := p.getVolumeModifier(vol)
	if err != nil {
		return false, err
	}

	q, err := resource.ParseQuantity(vol.Desired.Size)
	if err != nil {
		return false, err
	}
	pvc := vol.PVC.DeepCopy()
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = q

	return m.ModifyVolume(ctx, pvc, vol.PV, vol.Desired.StorageClass)
}

func (p *podVolModifier) getVolumeModifier(vol *ActualVolume) (delegation.VolumeModifier, error) {
	// TODO(liubo02)
	return p.modifiers["aws"], nil
}

func isLeaderEvictedOrTimeout(tc *v1alpha1.TidbCluster, pod *corev1.Pod) bool {
	for _, store := range tc.Status.TiKV.Stores {
		if store.PodName == pod.Name {
			if store.LeaderCount == 0 {
				klog.V(4).Infof("leader count of store %s become 0", store.ID)
				return true
			}

			if status, exist := tc.Status.TiKV.EvictLeader[pod.Name]; exist && !status.BeginTime.IsZero() {
				timeout := tc.TiKVEvictLeaderTimeout()
				if time.Since(status.BeginTime.Time) > timeout {
					klog.Infof("leader eviction begins at %q but timeout (threshold: %v)", status.BeginTime.Format(time.RFC3339), timeout)
					return true
				}
			}

			return false
		}
	}

	return true
}
