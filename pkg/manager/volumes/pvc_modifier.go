package volumes

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	errutil "k8s.io/apimachinery/pkg/util/errors"
	klog "k8s.io/klog/v2"

	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
)

const (
	annoKeyPVCSpecRevision     = "spec.tidb.pingcap.com/revision"
	annoKeyPVCSpecStorageClass = "spec.tidb.pingcap.com/storage-class"
	annoKeyPVCSpecStorageSize  = "spec.tidb.pingcap.com/storage-size"

	annoKeyPVCStatusRevision     = "status.tidb.pingcap.com/revision"
	annoKeyPVCStatusStorageClass = "status.tidb.pingcap.com/storage-class"
	annoKeyPVCStatusStorageSize  = "status.tidb.pingcap.com/storage-size"

	annoKeyPVCLastTransitionTimestamp = "status.tidb.pingcap.com/last-transition-timestamp"

	defaultModifyWaitingDuration = time.Minute * 1
)

type PVCModifierInterface interface {
	Sync(tc *v1alpha1.TidbCluster) error
}

type pvcModifier struct {
	deps *controller.Dependencies
	sf   *selectorFactory
	pm   PodVolumeModifier
}

func NewPVCModifier(deps *controller.Dependencies) PVCModifierInterface {
	return &pvcModifier{
		deps: deps,
		sf:   MustNewSelectorFactory(),
		pm:   NewPodVolumeModifier(deps),
	}
}

type componentVolumeContext struct {
	context.Context
	tc     *v1alpha1.TidbCluster
	status v1alpha1.ComponentStatus

	shouldEvict bool

	pods []*corev1.Pod

	desiredVolumes []DesiredVolume
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
			errs = append(errs, fmt.Errorf("build ctx used by modifier for %s failed: %w", ctx.ComponentID(), err))
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
func (p *pvcModifier) getDesiredVolumes(tc *v1alpha1.TidbCluster, mt v1alpha1.MemberType) ([]DesiredVolume, error) {
	desiredVolumes := []DesiredVolume{}

	storageVolumes := []v1alpha1.StorageVolume{}
	switch mt {
	case v1alpha1.PDMemberType:
		sc, err := p.getStorageClass(tc.Spec.PD.StorageClassName)
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
		sc, err := p.getStorageClass(tc.Spec.TiKV.StorageClassName)
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
			sc, err := p.getStorageClass(claim.StorageClassName)
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
		sc, err := p.getStorageClass(tc.Spec.Pump.StorageClassName)
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
			sc, err := p.getStorageClass(sv.StorageClassName)
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

	pods, err := p.getPodsOfComponent(tc, comp)
	if err != nil {
		return nil, err
	}

	ctx.pods = pods
	ctx.shouldEvict = comp == v1alpha1.TiKVMemberType

	return ctx, nil
}

func (p *pvcModifier) getPodsOfComponent(tc *v1alpha1.TidbCluster, mt v1alpha1.MemberType) ([]*corev1.Pod, error) {
	selector, err := p.sf.NewSelector(tc.GetInstanceName(), mt)
	if err != nil {
		return nil, err
	}

	ns := tc.GetNamespace()

	pods, err := p.deps.PodLister.Pods(ns).List(selector)
	if err != nil {
		return nil, fmt.Errorf("failed to list Pods: %w", err)
	}

	sort.Slice(pods, func(i, k int) bool {
		a, b := pods[i].Name, pods[k].Name
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		return a < b
	})

	return pods, nil
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
		volName := volTemplate.Name
		size := getStorageSize(volTemplate.Spec.Resources.Requests)
		desired := getDesiredVolumeByName(ctx.desiredVolumes, volName)
		if desired == nil {
			klog.Warningf("volume %s in sts for cluster %s dose not exist in desired volumes", volName, ctx.ComponentID())
			continue
		}
		if desired.Size != size.String() {
			return false, nil
		}
		scName := volTemplate.Spec.StorageClassName
		if !isStorageClassMatched(desired.StorageClass, ignoreNil(scName)) {
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
	for _, pod := range ctx.pods {
		completed, err := p.pm.Modify(ctx.tc, pod, ctx.desiredVolumes, ctx.shouldEvict)
		if err != nil {
			return err
		}

		if !completed {
			if ctx.shouldEvict {
				if err := p.evictLeader(pod); err != nil {
					return err
				}
			}

			return fmt.Errorf("wait for volume modification of %s/%s completed", pod.Namespace, pod.Name)
		}

		if err := p.endEvictLeader(pod); err != nil {
			return err
		}
	}

	return nil
}

func isLeaderEvicting(pod *corev1.Pod) bool {
	_, exist := pod.Annotations[v1alpha1.EvictLeaderAnnKeyForResize]
	return exist
}

func (p *pvcModifier) evictLeader(pod *corev1.Pod) error {
	if isLeaderEvicting(pod) {
		return nil
	}
	pod = pod.DeepCopy()

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}

	pod.Annotations[v1alpha1.EvictLeaderAnnKeyForResize] = v1alpha1.EvictLeaderValueNone
	if _, err := p.deps.KubeClientset.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("add leader eviction annotation to pod %s/%s failed: %s", pod.Namespace, pod.Name, err)
	}

	return nil
}

func (p *pvcModifier) endEvictLeader(pod *corev1.Pod) error {
	if !isLeaderEvicting(pod) {
		return nil
	}
	pod = pod.DeepCopy()

	delete(pod.Annotations, v1alpha1.EvictLeaderAnnKeyForResize)
	if _, err := p.deps.KubeClientset.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("add leader eviction annotation to pod %s/%s failed: %s", pod.Namespace, pod.Name, err)
	}

	return nil
}
