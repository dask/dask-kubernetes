// Code generated by client-gen. DO NOT EDIT.

package v1

import (
	"context"
	"time"

	v1 "github.com/dask/dask-kubernetes/v2023/dask_kubernetes/operator/go_client/pkg/apis/kubernetes.dask.org/v1"
	scheme "github.com/dask/dask-kubernetes/v2023/dask_kubernetes/operator/go_client/pkg/client/clientset/versioned/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// DaskJobsGetter has a method to return a DaskJobInterface.
// A group's client should implement this interface.
type DaskJobsGetter interface {
	DaskJobs(namespace string) DaskJobInterface
}

// DaskJobInterface has methods to work with DaskJob resources.
type DaskJobInterface interface {
	Create(ctx context.Context, daskJob *v1.DaskJob, opts metav1.CreateOptions) (*v1.DaskJob, error)
	Update(ctx context.Context, daskJob *v1.DaskJob, opts metav1.UpdateOptions) (*v1.DaskJob, error)
	UpdateStatus(ctx context.Context, daskJob *v1.DaskJob, opts metav1.UpdateOptions) (*v1.DaskJob, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.DaskJob, error)
	List(ctx context.Context, opts metav1.ListOptions) (*v1.DaskJobList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.DaskJob, err error)
	DaskJobExpansion
}

// daskJobs implements DaskJobInterface
type daskJobs struct {
	client rest.Interface
	ns     string
}

// newDaskJobs returns a DaskJobs
func newDaskJobs(c *KubernetesV1Client, namespace string) *daskJobs {
	return &daskJobs{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the daskJob, and returns the corresponding daskJob object, and an error if there is any.
func (c *daskJobs) Get(ctx context.Context, name string, options metav1.GetOptions) (result *v1.DaskJob, err error) {
	result = &v1.DaskJob{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("daskjobs").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of DaskJobs that match those selectors.
func (c *daskJobs) List(ctx context.Context, opts metav1.ListOptions) (result *v1.DaskJobList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1.DaskJobList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("daskjobs").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested daskJobs.
func (c *daskJobs) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("daskjobs").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a daskJob and creates it.  Returns the server's representation of the daskJob, and an error, if there is any.
func (c *daskJobs) Create(ctx context.Context, daskJob *v1.DaskJob, opts metav1.CreateOptions) (result *v1.DaskJob, err error) {
	result = &v1.DaskJob{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("daskjobs").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(daskJob).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a daskJob and updates it. Returns the server's representation of the daskJob, and an error, if there is any.
func (c *daskJobs) Update(ctx context.Context, daskJob *v1.DaskJob, opts metav1.UpdateOptions) (result *v1.DaskJob, err error) {
	result = &v1.DaskJob{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("daskjobs").
		Name(daskJob.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(daskJob).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *daskJobs) UpdateStatus(ctx context.Context, daskJob *v1.DaskJob, opts metav1.UpdateOptions) (result *v1.DaskJob, err error) {
	result = &v1.DaskJob{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("daskjobs").
		Name(daskJob.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(daskJob).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the daskJob and deletes it. Returns an error if one occurs.
func (c *daskJobs) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("daskjobs").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *daskJobs) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("daskjobs").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched daskJob.
func (c *daskJobs) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.DaskJob, err error) {
	result = &v1.DaskJob{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("daskjobs").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
