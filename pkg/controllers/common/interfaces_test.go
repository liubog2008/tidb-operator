package common

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/pingcap/tidb-operator/apis/core/v1alpha1"
)

type fakeState[T any] struct {
	ns   string
	name string
	obj  *T
}

func (f *fakeState[T]) Object() *T {
	return f.obj
}

func (f *fakeState[T]) Initializer() ResourceInitializer[*T] {
	return NewResource(func(obj *T) { f.obj = obj }).
		WithNamespace(Namespace(f.ns)).
		WithName(Name(f.name)).
		Initializer()
}

type fakeSliceState[T any] struct {
	ns     string
	labels map[string]string
	objs   []*T
}

func (f *fakeSliceState[T]) Slice() []*T {
	return f.objs
}

func (f *fakeSliceState[T]) Initializer() ResourceSliceInitializer[*T] {
	return NewResourceSlice(func(objs []*T) { f.objs = objs }).
		WithNamespace(Namespace(f.ns)).
		WithLabels(Labels(f.labels)).
		Initializer()
}

type fakePDState struct {
	s *fakeState[v1alpha1.PD]
}

func (f *fakePDState) PD() *v1alpha1.PD {
	return f.s.Object()
}

func (f *fakePDState) PDInitializer() PDInitializer {
	return f.s.Initializer()
}

type fakeClusterState struct {
	s *fakeState[v1alpha1.Cluster]
}

func (f *fakeClusterState) Cluster() *v1alpha1.Cluster {
	return f.s.Object()
}

func (f *fakeClusterState) ClusterInitializer() ClusterInitializer {
	return f.s.Initializer()
}

type fakePodState struct {
	s *fakeState[corev1.Pod]
}

func (f *fakePodState) Pod() *corev1.Pod {
	return f.s.Object()
}

func (f *fakePodState) PodInitializer() PodInitializer {
	return f.s.Initializer()
}

type fakePDSliceState struct {
	s *fakeSliceState[v1alpha1.PD]
}

func (f *fakePDSliceState) PDSlice() []*v1alpha1.PD {
	return f.s.Slice()
}

func (f *fakePDSliceState) PDSliceInitializer() PDSliceInitializer {
	return f.s.Initializer()
}
