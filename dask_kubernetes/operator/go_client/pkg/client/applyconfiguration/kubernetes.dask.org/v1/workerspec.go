// Code generated by applyconfiguration-gen. DO NOT EDIT.

package v1

import (
	v1 "k8s.io/api/core/v1"
)

// WorkerSpecApplyConfiguration represents an declarative configuration of the WorkerSpec type for use
// with apply.
type WorkerSpecApplyConfiguration struct {
	Replicas *int        `json:"replicas,omitempty"`
	Spec     *v1.PodSpec `json:"spec,omitempty"`
}

// WorkerSpecApplyConfiguration constructs an declarative configuration of the WorkerSpec type for use with
// apply.
func WorkerSpec() *WorkerSpecApplyConfiguration {
	return &WorkerSpecApplyConfiguration{}
}

// WithReplicas sets the Replicas field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the Replicas field is set to the value of the last call.
func (b *WorkerSpecApplyConfiguration) WithReplicas(value int) *WorkerSpecApplyConfiguration {
	b.Replicas = &value
	return b
}

// WithSpec sets the Spec field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the Spec field is set to the value of the last call.
func (b *WorkerSpecApplyConfiguration) WithSpec(value v1.PodSpec) *WorkerSpecApplyConfiguration {
	b.Spec = &value
	return b
}
