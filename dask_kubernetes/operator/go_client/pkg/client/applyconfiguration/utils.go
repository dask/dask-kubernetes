// Code generated by applyconfiguration-gen. DO NOT EDIT.

package applyconfiguration

import (
	v1 "github.com/dask/dask-kubernetes/dask_kubernetes/operator/go_client/pkg/apis/kubernetes.dask.org/v1"
	kubernetesdaskorgv1 "github.com/dask/dask-kubernetes/dask_kubernetes/operator/go_client/pkg/client/applyconfiguration/kubernetes.dask.org/v1"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
)

// ForKind returns an apply configuration type for the given GroupVersionKind, or nil if no
// apply configuration type exists for the given GroupVersionKind.
func ForKind(kind schema.GroupVersionKind) interface{} {
	switch kind {
	// Group=kubernetes.dask.org, Version=v1
	case v1.SchemeGroupVersion.WithKind("DaskAutoscaler"):
		return &kubernetesdaskorgv1.DaskAutoscalerApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskAutoscalerSpec"):
		return &kubernetesdaskorgv1.DaskAutoscalerSpecApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskCluster"):
		return &kubernetesdaskorgv1.DaskClusterApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskClusterSpec"):
		return &kubernetesdaskorgv1.DaskClusterSpecApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskClusterStatus"):
		return &kubernetesdaskorgv1.DaskClusterStatusApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskJob"):
		return &kubernetesdaskorgv1.DaskJobApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskJobSpec"):
		return &kubernetesdaskorgv1.DaskJobSpecApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskJobStatus"):
		return &kubernetesdaskorgv1.DaskJobStatusApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskWorkerGroup"):
		return &kubernetesdaskorgv1.DaskWorkerGroupApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("DaskWorkerGroupSpec"):
		return &kubernetesdaskorgv1.DaskWorkerGroupSpecApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("JobSpec"):
		return &kubernetesdaskorgv1.JobSpecApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("SchedulerSpec"):
		return &kubernetesdaskorgv1.SchedulerSpecApplyConfiguration{}
	case v1.SchemeGroupVersion.WithKind("WorkerSpec"):
		return &kubernetesdaskorgv1.WorkerSpecApplyConfiguration{}

	}
	return nil
}
