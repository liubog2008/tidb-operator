package volumes

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	errutil "k8s.io/apimachinery/pkg/util/errors"
	klog "k8s.io/klog/v2"

	"github.com/pingcap/tidb-operator/pkg/apis/label"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/manager/volumes/delegation"
)

const (
	annoKeyPVCSpecRevision     = "spec.tidb.pingcap.com/revison"
	annoKeyPVCSpecStorageClass = "spec.tidb.pingcap.com/storage-class"
	annoKeyPVCSpecStorageSize  = "spec.tidb.pingcap.com/storage-size"

	annoKeyPVCStatusRevision     = "status.tidb.pingcap.com/revison"
	annoKeyPVCStatusStorageClass = "status.tidb.pingcap.com/storage-class"
	annoKeyPVCStatusStorageSize  = "status.tidb.pingcap.com/storage-size"

	annoKeyPVCLastTransitionTimestamp = "status.tidb.pingcap.com/last-transition-timestamp"

	defaultModifyWaitingDuration = time.Hour * 6
)

type VolumePhase int

const (
	// 1. isPVCRevisionChanged: false
	// 2. needModify: true
	// 3. waitForNextTime: true
	VolumePhasePending VolumePhase = iota
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

type PVCModifierInterface interface {
	Sync(tc *v1alpha1.TidbCluster) error
}

type selectorFactory struct {
	cache map[v1alpha1.MemberType]*labels.Requirement
}

func (sf *selectorFactory) NewSelector(instance string, mt v1alpha1.MemberType) (labels.Selector, error) {
	selector, err := label.New().Instance(instance).Selector()
	if err != nil {
		return nil, err
	}
	r, ok := sf.cache[mt]
	if !ok {
		return nil, fmt.Errorf("can't get selector for %v", mt)
	}

	selector.Add(*r)

	return selector, nil
}

// pd => pd
// tidb => tidb
// tikv => tikv
// tiflash => tiflash
// ticdc => ticdc
// pump => pump
func convertMemberTypeToLabelVal(mt v1alpha1.MemberType) string {
	return string(mt)
}

func NewSelectorFactory() (*selectorFactory, error) {
	mts := []v1alpha1.MemberType{
		v1alpha1.PDMemberType,
		v1alpha1.TiDBMemberType,
		v1alpha1.TiKVMemberType,
		v1alpha1.TiFlashMemberType,
		v1alpha1.TiCDCMemberType,
		v1alpha1.PumpMemberType,
	}

	sf := &selectorFactory{
		cache: make(map[v1alpha1.MemberType]*labels.Requirement),
	}

	for _, mt := range mts {
		req, err := labels.NewRequirement(label.ComponentLabelKey, selection.Equals, []string{
			convertMemberTypeToLabelVal(mt),
		})
		if err != nil {
			return nil, err
		}
		sf.cache[mt] = req
	}
	return sf, nil
}

type pvcModifier struct {
	deps *controller.Dependencies
	sf   *selectorFactory

	modifiers map[string]delegation.VolumeModifier
}

type actualVolume struct {
	name    v1alpha1.StorageVolumeName
	desired *desiredVolume
	pvc     *corev1.PersistentVolumeClaim
	pv      *corev1.PersistentVolume
	phase   VolumePhase
}

type desiredVolume struct {
	size resource.Quantity
	sc   *storagev1.StorageClass
}

type componentVolumeContext struct {
	context.Context
	tc     *v1alpha1.TidbCluster
	status v1alpha1.ComponentStatus

	shouldEvict bool
	pod         *corev1.Pod

	desiredVolumes map[v1alpha1.StorageVolumeName]desiredVolume
	actualVolumes  []actualVolume
}

func (c *componentVolumeContext) ComponentID() string {
	return fmt.Sprintf("%s/%s:%s", c.tc.GetNamespace(), c.tc.GetName(), c.status.MemberType())
}

func (p *pvcModifier) Sync(tc *v1alpha1.TidbCluster) error {
	components := tc.AllComponentStatus()
	errs := []error{}

	for _, comp := range components {
		ctx, err := p.buildContextForTC(tc, comp)
		if err != nil {
			errs = append(errs, fmt.Errorf("build ctx used by resize for %s failed: %w", ctx.ComponentID(), err))
			continue
		}

		// TODO
		// p.updateVolumeStatus(ctx)

		err = p.modifyVolumes(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("modify volumes for %s failed: %w", ctx.ComponentID(), err))
			continue
		}
	}

	return errutil.NewAggregate(errs)
}

func getStorageSize(r corev1.ResourceList) resource.Quantity {
	return r[corev1.ResourceStorage]
}

func ignoreNil(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func getTcKey(tc *v1alpha1.TidbCluster) string {
	return fmt.Sprintf("%s/%s", tc.GetNamespace(), tc.GetName())
}

func getComponentKey(tc *v1alpha1.TidbCluster, mt v1alpha1.MemberType) string {
	return fmt.Sprintf("%s:%s", getTcKey(tc), mt)
}

func (p *pvcModifier) getStorageClass(name *string) (*storagev1.StorageClass, error) {
	if name == nil {
		return nil, nil
	}
	return p.deps.StorageClassLister.Get(*name)
}

// TODO: it should be refactored
func (p *pvcModifier) getDesiredVolumes(tc *v1alpha1.TidbCluster, mt v1alpha1.MemberType) (map[v1alpha1.StorageVolumeName]desiredVolume, error) {
	desiredVolumeMap := map[v1alpha1.StorageVolumeName]desiredVolume{}

	storageVolumes := []v1alpha1.StorageVolume{}
	switch mt {
	case v1alpha1.PDMemberType:
		sc, err := p.getStorageClass(tc.Spec.PD.StorageClassName)
		if err != nil {
			return nil, err
		}
		d := desiredVolume{
			size: getStorageSize(tc.Spec.PD.Requests),
			sc:   sc,
		}
		name := v1alpha1.GetStorageVolumeName("", mt)
		desiredVolumeMap[name] = d

		storageVolumes = tc.Spec.PD.StorageVolumes

	case v1alpha1.TiDBMemberType:
		storageVolumes = tc.Spec.TiDB.StorageVolumes

	case v1alpha1.TiKVMemberType:
		sc, err := p.getStorageClass(tc.Spec.TiKV.StorageClassName)
		if err != nil {
			return nil, err
		}
		d := desiredVolume{
			size: getStorageSize(tc.Spec.TiKV.Requests),
			sc:   sc,
		}
		name := v1alpha1.GetStorageVolumeName("", mt)
		desiredVolumeMap[name] = d

		storageVolumes = tc.Spec.TiKV.StorageVolumes

	case v1alpha1.TiFlashMemberType:
		for i, claim := range tc.Spec.TiFlash.StorageClaims {
			sc, err := p.getStorageClass(claim.StorageClassName)
			if err != nil {
				return nil, err
			}
			d := desiredVolume{
				size: getStorageSize(claim.Resources.Requests),
				sc:   sc,
			}
			name := v1alpha1.GetStorageVolumeNameForTiFlash(i)
			desiredVolumeMap[name] = d
		}

	case v1alpha1.TiCDCMemberType:
		storageVolumes = tc.Spec.TiCDC.StorageVolumes

	case v1alpha1.PumpMemberType:
		sc, err := p.getStorageClass(tc.Spec.Pump.StorageClassName)
		if err != nil {
			return nil, err
		}
		d := desiredVolume{
			size: getStorageSize(tc.Spec.Pump.Requests),
			sc:   sc,
		}
		name := v1alpha1.GetStorageVolumeName("", mt)
		desiredVolumeMap[name] = d
	default:
		return nil, fmt.Errorf("unsupported member type %s", mt)
	}

	for _, sv := range storageVolumes {
		if quantity, err := resource.ParseQuantity(sv.StorageSize); err == nil {
			sc, err := p.getStorageClass(sv.StorageClassName)
			if err != nil {
				return nil, err
			}
			d := desiredVolume{
				size: quantity,
				sc:   sc,
			}

			name := v1alpha1.GetStorageVolumeName(sv.Name, mt)

			desiredVolumeMap[name] = d

		} else {
			klog.Warningf("StorageVolume %q in %s .spec.%s is invalid", sv.Name, getTcKey(tc), mt)
		}
	}

	return desiredVolumeMap, nil
}

func (p *pvcModifier) buildContextForTC(tc *v1alpha1.TidbCluster, status v1alpha1.ComponentStatus) (*componentVolumeContext, error) {
	comp := status.MemberType()

	ctx := &componentVolumeContext{
		Context: context.TODO(),
		tc:      tc,
		status:  status,
	}

	vs, err := p.getDesiredVolumes(tc, comp)
	if err != nil {
		return nil, err
	}
	ctx.desiredVolumes = vs

	selectedPod, actualVolumes, err := p.getActualVolumes(tc, comp, vs)
	if err != nil {
		return nil, err
	}
	ctx.actualVolumes = actualVolumes
	ctx.pod = selectedPod
	ctx.shouldEvict = comp == v1alpha1.TiKVMemberType

	return ctx, nil
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

	return d > defaultModifyWaitingDuration
}

func needModify(actual *actualVolume, desired *desiredVolume) bool {
	scName, ok := actual.pvc.Annotations[annoKeyPVCStatusStorageClass]
	if !ok {
		scName = ignoreNil(actual.pvc.Spec.StorageClassName)
	}
	if !isStorageClassMatched(desired.sc, scName) {
		return true
	}

	str, ok := actual.pvc.Annotations[annoKeyPVCStatusStorageSize]
	if ok {
		sz, err := resource.ParseQuantity(str)
		if err != nil {
			return true
		}
		if !desired.size.Equal(sz) {
			return true
		}
	}

	size := getStorageSize(actual.pvc.Spec.Resources.Requests)
	return !desired.size.Equal(size)
}

func getVolumePhase(actual *actualVolume, desired *desiredVolume) VolumePhase {
	if isPVCRevisionChanged(actual.pvc) {
		if !waitForNextTime(actual.pvc) {
			return VolumePhaseWaitForLeaderEviction
		}
		return VolumePhaseModifying
	}

	if !needModify(actual, desired) {
		return VolumePhaseModified
	}

	if waitForNextTime(actual.pvc) {
		return VolumePhasePending
	}

	return VolumePhasePreparing
}

func (p *pvcModifier) getActualVolumes(tc *v1alpha1.TidbCluster, mt v1alpha1.MemberType, desiredVolumeMap map[v1alpha1.StorageVolumeName]desiredVolume) (*corev1.Pod, []actualVolume, error) {
	selector, err := p.sf.NewSelector(tc.GetInstanceName(), mt)
	if err != nil {
		return nil, nil, err
	}

	ns := tc.GetNamespace()

	pods, err := p.deps.PodLister.Pods(ns).List(selector)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list Pods: %v", err)
	}

	pvcs, err := p.deps.PVCLister.PersistentVolumeClaims(ns).List(selector)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list PVCs: %v", err)
	}

	findPVC := func(pvcName string) (*corev1.PersistentVolumeClaim, error) {
		for _, pvc := range pvcs {
			if pvc.Name == pvcName {
				return pvc, nil
			}
		}
		return nil, fmt.Errorf("failed to find PVC %s", pvcName)
	}

	sort.Slice(pods, func(i, k int) bool {
		a, b := pods[i].Name, pods[k].Name
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		return a < b
	})

	var ret []actualVolume
	var selectedPod *corev1.Pod

	for _, pod := range pods {
		vols := []actualVolume{}
		hasModifyingVolume := false
		hasPrepareVolume := false
		isEvicting := isLeaderEvicting(pod)

		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				pvc, err := findPVC(vol.PersistentVolumeClaim.ClaimName)
				if err != nil {
					klog.Warningf("Failed to find PVC %s of Pod %s/%s, maybe some labels are lost",
						vol.PersistentVolumeClaim.ClaimName, pod.Namespace, pod.Name)
					continue
				}
				pv, err := p.getBoundPVFromPVC(pvc)
				if err != nil {
					klog.Warningf("Failed to find PV of PVC %s of Pod %s/%s",
						vol.PersistentVolumeClaim.ClaimName, pod.Namespace, pod.Name)
					continue
				}
				name := v1alpha1.StorageVolumeName(vol.Name)
				desired, ok := desiredVolumeMap[name]
				if !ok {
					klog.Warningf("Failed to get desired volume %s %s", getComponentKey(tc, mt), name)
					continue
				}
				actual := actualVolume{
					desired: &desired,
					name:    name,
					pvc:     pvc,
					pv:      pv,
				}

				phase := getVolumePhase(&actual, &desired)
				actual.phase = phase
				switch phase {
				case VolumePhaseModifying:
					hasModifyingVolume = true
				case VolumePhasePreparing:
					hasPrepareVolume = true
				}

				vols = append(vols, actual)
			}
		}
		// choose volumes of the pod which is evicting leader
		if isEvicting {
			return pod, vols, nil
		}
		// choose volumes of the pod who has a volume in modifying status
		if hasModifyingVolume {
			return pod, vols, nil
		}
		if hasPrepareVolume {
			selectedPod = pod
			ret = vols
		}
	}

	return selectedPod, ret, nil
}

func (p *pvcModifier) modifyVolumes(ctx *componentVolumeContext) error {
	if err := p.tryToRecreateSTS(ctx); err != nil {
		return err
	}

	if err := p.tryToModifyPVC(ctx); err != nil {
		return err
	}

	return nil
}

func (p *pvcModifier) isStatefulSetSynced(ctx *componentVolumeContext, ns, name string) (bool, error) {
	sts, err := p.deps.StatefulSetLister.StatefulSets(ns).Get(name)
	if err != nil {
		klog.Warningf("skip to resize sts %s for component %s because %v", name, ctx.ComponentID(), err)
		return false, err
	}

	for _, volTemplate := range sts.Spec.VolumeClaimTemplates {
		volName := v1alpha1.StorageVolumeName(volTemplate.Name)
		size := getStorageSize(volTemplate.Spec.Resources.Requests)
		desired, exist := ctx.desiredVolumes[volName]
		if !exist {
			klog.Warningf("volume %s in sts for cluster %s dose not exist in desired volumes", volName, ctx.ComponentID())
			continue
		}
		if !desired.size.Equal(size) {
			return false, nil
		}
		scName := volTemplate.Spec.StorageClassName
		if !isStorageClassMatched(desired.sc, ignoreNil(scName)) {
			return false, nil
		}
	}

	return true, nil
}

func isStorageClassMatched(sc *storagev1.StorageClass, scName string) bool {
	if sc == nil && scName == "" {
		return true
	}
	if sc.Name == scName {
		return true
	}

	return false
}

func (p *pvcModifier) tryToRecreateSTS(ctx *componentVolumeContext) error {
	ns := ctx.tc.GetNamespace()
	name := controller.MemberName(ctx.tc.GetName(), ctx.status.MemberType())

	isSynced, err := p.isStatefulSetSynced(ctx, ns, name)
	if err != nil {
		return err
	}
	if isSynced {
		return nil
	}

	orphan := metav1.DeletePropagationOrphan
	if err := p.deps.KubeClientset.AppsV1().StatefulSets(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &orphan}); err != nil {
		return fmt.Errorf("delete sts %s/%s for component %s failed: %s", ns, name, ctx.ComponentID(), err)
	}

	klog.Infof("recreate statefulset %s/%s for component %s", ns, name, ctx.ComponentID())

	// component manager will create the sts in next reconciliation
	return nil
}

func (p *pvcModifier) tryToModifyPVC(ctx *componentVolumeContext) error {
	errs := []error{}
	isLeaderEvicted := true
	syncedCount := 0
	if ctx.shouldEvict {
		if !isLeaderEvictedOrTimeout(ctx.tc, ctx.pod) {
			isLeaderEvicted = false
		}
	}
	for i := range ctx.actualVolumes {
		vol := &ctx.actualVolumes[i]
		switch vol.phase {
		case VolumePhasePreparing:
			if err := p.modifyPVCAnnoSpec(ctx, vol, ctx.shouldEvict); err != nil {
				errs = append(errs, err)
				continue
			}

			fallthrough
		case VolumePhaseWaitForLeaderEviction:
			if ctx.shouldEvict {
				if !isLeaderEvicted {
					errs = append(errs, fmt.Errorf("waiting for leader eviction"))
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
			if !wait {
				if err := p.modifyPVCAnnoStatus(ctx, vol); err != nil {
					errs = append(errs, err)
					continue
				}
			}
		case VolumePhasePending, VolumePhaseModified:
		}

		syncedCount++
	}

	if !ctx.shouldEvict {
		return errutil.NewAggregate(errs)
	}

	if syncedCount == len(ctx.actualVolumes) {
		if err := p.endEvictLeader(ctx); err != nil {
			errs = append(errs, err)
		}

		return errutil.NewAggregate(errs)
	}

	if err := p.evictLeader(ctx); err != nil {
		errs = append(errs, err)
	}

	return errutil.NewAggregate(errs)
}

func isLeaderEvicting(pod *corev1.Pod) bool {
	_, exist := pod.Annotations[v1alpha1.EvictLeaderAnnKeyForResize]
	return exist
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

func (p *pvcModifier) evictLeader(ctx *componentVolumeContext) error {
	if isLeaderEvicting(ctx.pod) {
		return nil
	}
	pod := ctx.pod.DeepCopy()

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}

	pod.Annotations[v1alpha1.EvictLeaderAnnKeyForResize] = v1alpha1.EvictLeaderValueNone
	newPod, err := p.deps.KubeClientset.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("add leader eviction annotation to pod %s/%s failed: %s", pod.Namespace, pod.Name, err)
	}

	ctx.pod = newPod

	return nil
}

func (p *pvcModifier) endEvictLeader(ctx *componentVolumeContext) error {
	if !isLeaderEvicting(ctx.pod) {
		return nil
	}
	pod := ctx.pod.DeepCopy()

	delete(pod.Annotations, v1alpha1.EvictLeaderAnnKeyForResize)
	newPod, err := p.deps.KubeClientset.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("add leader eviction annotation to pod %s/%s failed: %s", pod.Namespace, pod.Name, err)
	}

	ctx.pod = newPod

	return nil
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

func snapshotStorageClassAndSize(pvc *corev1.PersistentVolumeClaim, scName, size string) bool {
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

func (p *pvcModifier) modifyPVCAnnoSpecLastTransitionTimestamp(ctx *componentVolumeContext, vol *actualVolume) error {
	pvc := vol.pvc.DeepCopy()
	setLastTransitionTimestamp(pvc)
	updated, err := p.deps.KubeClientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	vol.pvc = updated

	return nil

}

// upgrade revision and snapshot the expected storageclass and size of volume
func (p *pvcModifier) modifyPVCAnnoSpec(ctx *componentVolumeContext, vol *actualVolume, shouldEvict bool) error {
	pvc := vol.pvc.DeepCopy()

	size := vol.desired.size.String()
	scName := ""
	if vol.desired.sc != nil {
		scName = vol.desired.sc.Name
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

	vol.pvc = updated

	return nil
}

func (p *pvcModifier) modifyPVCAnnoStatus(ctx *componentVolumeContext, vol *actualVolume) error {
	pvc := vol.pvc.DeepCopy()

	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}

	pvc.Annotations[annoKeyPVCStatusRevision] = pvc.Annotations[annoKeyPVCSpecRevision]
	pvc.Annotations[annoKeyPVCStatusStorageClass] = pvc.Annotations[annoKeyPVCSpecStorageClass]
	pvc.Annotations[annoKeyPVCStatusStorageSize] = pvc.Annotations[annoKeyPVCSpecStorageSize]

	updated, err := p.deps.KubeClientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).UpdateStatus(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	vol.pvc = updated

	return nil
}

func (p *pvcModifier) modifyVolume(ctx *componentVolumeContext, vol *actualVolume) (bool, error) {
	m, err := p.getVolumeModifier(vol)
	if err != nil {
		return false, err
	}

	return m.ModifyVolume(ctx, vol.pvc, vol.pv, vol.desired.sc)
}

func (p *pvcModifier) getVolumeModifier(vol *actualVolume) (delegation.VolumeModifier, error) {
	// TODO(liubo02)
	return p.modifiers["aws"], nil
}

func (p *pvcModifier) getBoundPVFromPVC(pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolume, error) {
	name := pvc.Spec.VolumeName

	return p.deps.PVLister.Get(name)
}
