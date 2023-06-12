// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"
	json "encoding/json"
	"fmt"

	v1 "github.com/dask/dask-kubernetes/v2023/dask_kubernetes/operator/go_client/pkg/apis/kubernetes.dask.org/v1"
	kubernetesdaskorgv1 "github.com/dask/dask-kubernetes/v2023/dask_kubernetes/operator/go_client/pkg/client/applyconfiguration/kubernetes.dask.org/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeDaskAutoscalers implements DaskAutoscalerInterface
type FakeDaskAutoscalers struct {
	Fake *FakeKubernetesV1
	ns   string
}

var daskautoscalersResource = v1.SchemeGroupVersion.WithResource("daskautoscalers")

var daskautoscalersKind = v1.SchemeGroupVersion.WithKind("DaskAutoscaler")

// Get takes name of the daskAutoscaler, and returns the corresponding daskAutoscaler object, and an error if there is any.
func (c *FakeDaskAutoscalers) Get(ctx context.Context, name string, options metav1.GetOptions) (result *v1.DaskAutoscaler, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(daskautoscalersResource, c.ns, name), &v1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.DaskAutoscaler), err
}

// List takes label and field selectors, and returns the list of DaskAutoscalers that match those selectors.
func (c *FakeDaskAutoscalers) List(ctx context.Context, opts metav1.ListOptions) (result *v1.DaskAutoscalerList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(daskautoscalersResource, daskautoscalersKind, c.ns, opts), &v1.DaskAutoscalerList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1.DaskAutoscalerList{ListMeta: obj.(*v1.DaskAutoscalerList).ListMeta}
	for _, item := range obj.(*v1.DaskAutoscalerList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested daskAutoscalers.
func (c *FakeDaskAutoscalers) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(daskautoscalersResource, c.ns, opts))

}

// Create takes the representation of a daskAutoscaler and creates it.  Returns the server's representation of the daskAutoscaler, and an error, if there is any.
func (c *FakeDaskAutoscalers) Create(ctx context.Context, daskAutoscaler *v1.DaskAutoscaler, opts metav1.CreateOptions) (result *v1.DaskAutoscaler, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(daskautoscalersResource, c.ns, daskAutoscaler), &v1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.DaskAutoscaler), err
}

// Update takes the representation of a daskAutoscaler and updates it. Returns the server's representation of the daskAutoscaler, and an error, if there is any.
func (c *FakeDaskAutoscalers) Update(ctx context.Context, daskAutoscaler *v1.DaskAutoscaler, opts metav1.UpdateOptions) (result *v1.DaskAutoscaler, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(daskautoscalersResource, c.ns, daskAutoscaler), &v1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.DaskAutoscaler), err
}

// Delete takes name of the daskAutoscaler and deletes it. Returns an error if one occurs.
func (c *FakeDaskAutoscalers) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(daskautoscalersResource, c.ns, name, opts), &v1.DaskAutoscaler{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeDaskAutoscalers) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(daskautoscalersResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &v1.DaskAutoscalerList{})
	return err
}

// Patch applies the patch and returns the patched daskAutoscaler.
func (c *FakeDaskAutoscalers) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.DaskAutoscaler, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(daskautoscalersResource, c.ns, name, pt, data, subresources...), &v1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.DaskAutoscaler), err
}

// Apply takes the given apply declarative configuration, applies it and returns the applied daskAutoscaler.
func (c *FakeDaskAutoscalers) Apply(ctx context.Context, daskAutoscaler *kubernetesdaskorgv1.DaskAutoscalerApplyConfiguration, opts metav1.ApplyOptions) (result *v1.DaskAutoscaler, err error) {
	if daskAutoscaler == nil {
		return nil, fmt.Errorf("daskAutoscaler provided to Apply must not be nil")
	}
	data, err := json.Marshal(daskAutoscaler)
	if err != nil {
		return nil, err
	}
	name := daskAutoscaler.Name
	if name == nil {
		return nil, fmt.Errorf("daskAutoscaler.Name must be provided to Apply")
	}
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(daskautoscalersResource, c.ns, *name, types.ApplyPatchType, data), &v1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.DaskAutoscaler), err
}
