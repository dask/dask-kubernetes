// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	kubernetesdaskorgv1 "github.com/dask/dask-kubernetes/dask_kubernetes/operator/go_client/pkg/apis/kubernetes.dask.org/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeDaskAutoscalers implements DaskAutoscalerInterface
type FakeDaskAutoscalers struct {
	Fake *FakeKubernetesV1
	ns   string
}

var daskautoscalersResource = schema.GroupVersionResource{Group: "kubernetes.dask.org", Version: "v1", Resource: "daskautoscalers"}

var daskautoscalersKind = schema.GroupVersionKind{Group: "kubernetes.dask.org", Version: "v1", Kind: "DaskAutoscaler"}

// Get takes name of the daskAutoscaler, and returns the corresponding daskAutoscaler object, and an error if there is any.
func (c *FakeDaskAutoscalers) Get(ctx context.Context, name string, options v1.GetOptions) (result *kubernetesdaskorgv1.DaskAutoscaler, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(daskautoscalersResource, c.ns, name), &kubernetesdaskorgv1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*kubernetesdaskorgv1.DaskAutoscaler), err
}

// List takes label and field selectors, and returns the list of DaskAutoscalers that match those selectors.
func (c *FakeDaskAutoscalers) List(ctx context.Context, opts v1.ListOptions) (result *kubernetesdaskorgv1.DaskAutoscalerList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(daskautoscalersResource, daskautoscalersKind, c.ns, opts), &kubernetesdaskorgv1.DaskAutoscalerList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &kubernetesdaskorgv1.DaskAutoscalerList{ListMeta: obj.(*kubernetesdaskorgv1.DaskAutoscalerList).ListMeta}
	for _, item := range obj.(*kubernetesdaskorgv1.DaskAutoscalerList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested daskAutoscalers.
func (c *FakeDaskAutoscalers) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(daskautoscalersResource, c.ns, opts))

}

// Create takes the representation of a daskAutoscaler and creates it.  Returns the server's representation of the daskAutoscaler, and an error, if there is any.
func (c *FakeDaskAutoscalers) Create(ctx context.Context, daskAutoscaler *kubernetesdaskorgv1.DaskAutoscaler, opts v1.CreateOptions) (result *kubernetesdaskorgv1.DaskAutoscaler, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(daskautoscalersResource, c.ns, daskAutoscaler), &kubernetesdaskorgv1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*kubernetesdaskorgv1.DaskAutoscaler), err
}

// Update takes the representation of a daskAutoscaler and updates it. Returns the server's representation of the daskAutoscaler, and an error, if there is any.
func (c *FakeDaskAutoscalers) Update(ctx context.Context, daskAutoscaler *kubernetesdaskorgv1.DaskAutoscaler, opts v1.UpdateOptions) (result *kubernetesdaskorgv1.DaskAutoscaler, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(daskautoscalersResource, c.ns, daskAutoscaler), &kubernetesdaskorgv1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*kubernetesdaskorgv1.DaskAutoscaler), err
}

// Delete takes name of the daskAutoscaler and deletes it. Returns an error if one occurs.
func (c *FakeDaskAutoscalers) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(daskautoscalersResource, c.ns, name, opts), &kubernetesdaskorgv1.DaskAutoscaler{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeDaskAutoscalers) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(daskautoscalersResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &kubernetesdaskorgv1.DaskAutoscalerList{})
	return err
}

// Patch applies the patch and returns the patched daskAutoscaler.
func (c *FakeDaskAutoscalers) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *kubernetesdaskorgv1.DaskAutoscaler, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(daskautoscalersResource, c.ns, name, pt, data, subresources...), &kubernetesdaskorgv1.DaskAutoscaler{})

	if obj == nil {
		return nil, err
	}
	return obj.(*kubernetesdaskorgv1.DaskAutoscaler), err
}