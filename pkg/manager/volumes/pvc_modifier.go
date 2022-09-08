package volumes

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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
		if comp.MemberType() == v1alpha1.TiKVMemberType && tc.Status.TiKV.Phase == v1alpha1.UpgradePhase {
			klog.Infof("skip to modify volumes for cluster %s/%s because cluster is upgrading", tc.Namespace, tc.Name)
			continue
		}

		ctx, err := p.buildContextForTC(tc, comp)
		if err != nil {
			errs = append(errs, fmt.Errorf("build ctx used by modifier for %s failed: %w", ctx.ComponentID(), err))
			continue
		}

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

func (p *pvcModifier) buildContextForTC(tc *v1alpha1.TidbCluster, status v1alpha1.ComponentStatus) (*componentVolumeContext, error) {
	comp := status.MemberType()

	ctx := &componentVolumeContext{
		Context: context.TODO(),
		tc:      tc,
		status:  status,
	}

	vs, err := p.pm.GetDesiredVolumes(tc, comp)
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
		actual, err := p.pm.GetActualVolumes(pod, ctx.desiredVolumes)
		if err != nil {
			return err
		}

		isNeed := p.pm.ShouldModify(actual)

		if ctx.shouldEvict {
			// ensure leader eviction is finished and tikv store is up
			if !isNeed {
				if err := p.endEvictLeader(ctx.tc, pod); err != nil {
					return err
				}

				if !isTiKVStoreUp(ctx.tc, pod) {
					return fmt.Errorf("wait for tikv store %s/%s up", pod.Namespace, pod.Name)
				}

				continue
			}

			// try to evict leader if need to modify
			isEvicted := isLeaderEvictedOrTimeout(ctx.tc, pod)
			if !isEvicted {
				if ensureTiKVLeaderEvictionCondition(ctx.tc, metav1.ConditionTrue) {
					// return to sync tc
					return fmt.Errorf("try to evict leader for tidbcluster %s/%s", ctx.tc.Namespace, ctx.tc.Name)
				}
				if err := p.evictLeader(ctx.tc, pod); err != nil {
					return err
				}

				return fmt.Errorf("wait for leader eviction of %s/%s completed", pod.Namespace, pod.Name)
			}
		}

		if !isNeed {
			continue
		}

		if err := p.pm.Modify(actual); err != nil {
			return err
		}
	}

	if ctx.shouldEvict {
		if ensureTiKVLeaderEvictionCondition(ctx.tc, metav1.ConditionFalse) {
			// return to sync tc
			return fmt.Errorf("try to stop evicting leader for tidbcluster %s/%s", ctx.tc.Namespace, ctx.tc.Name)
		}
	}

	return nil
}

func ensureTiKVLeaderEvictionCondition(tc *v1alpha1.TidbCluster, status metav1.ConditionStatus) bool {
	if meta.IsStatusConditionPresentAndEqual(tc.Status.TiKV.Conditions, v1alpha1.ConditionTypeLeaderEvicting, status) {
		return false
	}

	var reason, message string

	switch status {
	case metav1.ConditionTrue:
		reason = "ModifyVolume"
		message = "Evicting leader for volume modification"
	case metav1.ConditionFalse:
		reason = "NoLeaderEviction"
		message = "Leader can be scheduled to all nodes"
	case metav1.ConditionUnknown:
		reason = "Unknown"
		message = "Leader eviction status is unknown"
	}
	cond := metav1.Condition{
		Type:    v1alpha1.ConditionTypeLeaderEvicting,
		Status:  status,
		Reason:  reason,
		Message: message,
	}

	meta.SetStatusCondition(&tc.Status.TiKV.Conditions, cond)
	return true
}

func isTiKVStoreUp(tc *v1alpha1.TidbCluster, pod *corev1.Pod) bool {
	// wait store to be Up
	for _, store := range tc.Status.TiKV.Stores {
		if store.PodName == pod.Name && store.State != v1alpha1.TiKVStateUp {
			return false
		}
	}

	return true
}

func isLeaderEvictionFinished(tc *v1alpha1.TidbCluster, pod *corev1.Pod) bool {
	if _, exist := tc.Status.TiKV.EvictLeader[pod.Name]; exist {
		return false
	}

	return true

}

func isLeaderEvicting(pod *corev1.Pod) bool {
	_, exist := pod.Annotations[v1alpha1.EvictLeaderAnnKeyForResize]
	return exist
}

func (p *pvcModifier) evictLeader(tc *v1alpha1.TidbCluster, pod *corev1.Pod) error {
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

func (p *pvcModifier) endEvictLeader(tc *v1alpha1.TidbCluster, pod *corev1.Pod) error {
	if isLeaderEvicting(pod) {
		pod = pod.DeepCopy()

		delete(pod.Annotations, v1alpha1.EvictLeaderAnnKeyForResize)
		if _, err := p.deps.KubeClientset.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("add leader eviction annotation to pod %s/%s failed: %s", pod.Namespace, pod.Name, err)
		}
	}

	if !isLeaderEvictionFinished(tc, pod) {
		return fmt.Errorf("wait for leader eviction of %s/%s finished", pod.Namespace, pod.Name)
	}

	return nil
}
