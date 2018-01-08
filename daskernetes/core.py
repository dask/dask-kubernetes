import getpass
import logging
import os
import socket
from urllib.parse import urlparse
import uuid
from weakref import finalize, ref
import yaml

from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client
from distributed.deploy import LocalCluster
import kubernetes
from kubernetes import client

logger = logging.getLogger(__name__)

config_fn = os.path.expanduser('~/.daskernetes.yaml')
if os.path.exists(config_fn):
    with open(config_fn) as f:
        config = yaml.load(f)
else:
    config = {}


def make_worker_spec(image='daskdev/dask:latest',
                     threads_per_worker=1,
                     env={}):
    return client.V1PodSpec(
        restart_policy='Never',
        containers=[
            client.V1Container(
                name='dask-worker',
                image=image,
                args=[
                    'dask-worker',
                    '--nthreads', str(threads_per_worker),
                    '--no-bokeh',
                ],
                env=[client.V1EnvVar(name=k, value=v)
                     for k, v in env.items()],
            )
        ]
    )


class KubeCluster(object):
    """ Launch a Dask cluster on Kubernetes

    This enables dynamically launching Dask workers on a Kubernetes cluster.
    It starts a dask scheduler in this local process and dynamically creates
    worker pods with Kubernetes.

    Parameters
    ----------
    name: str
        Name given to the pods.  Defaults to ``dask-user-random``
    namespace: str
        Namespace in which to launch the workers.  Defaults to current
        namespace if available or "default"
    labels: dict
        Additional labels to add to pod
    n_workers: int
        Number of workers on initial launch.  Use ``scale_up`` in the future
    host: str
        Listen address for local scheduler.  Defaults to 0.0.0.0
    port: int
        Port of local scheduler
    **kwargs: dict
        Additional keyword arguments to pass to LocalCluster

    Examples
    --------
    >>> from daskernetes import KubeCluster
    >>> cluster = KubeCluster()
    >>> cluster.scale_up(10)

    Alternatively have Dask allocate workers based on need
    >>> cluster.adapt()
    """
    def __init__(
            self,
            worker_spec=None,
            name=None,
            namespace=None,
            labels=None,
            n_workers=0,
            host='0.0.0.0',
            port=8786,
            env={},
            **kwargs,
    ):
        if namespace is None:
            namespace = _namespace_default()
        self.namespace = namespace

        if name is None:
            name = 'dask-%s-%s' % (getpass.getuser(), str(uuid.uuid4())[:10])

        labels = dict(labels or {})
        env = dict(env)

        # Default labels that can't be overwritten
        labels['org.pydata.dask/cluster-name'] = name
        labels['app'] = 'dask'
        labels['component'] = 'dask-worker'
        self.labels = labels

        if worker_spec is None:
            try:
                worker_spec = config['worker']['spec']
            except KeyError:
                worker_spec = make_worker_spec()

        worker_spec = deserialize_pod_spec(worker_spec)
        self.worker_spec = copy_kub(worker_spec)

        self.worker_meta = client.V1ObjectMeta(
                generate_name=name + '-',
                labels=labels
        )

        try:
            kubernetes.config.load_incluster_config()
        except kubernetes.config.ConfigException:
            kubernetes.config.load_kube_config()

        self.api = client.CoreV1Api()

        self.cluster = LocalCluster(ip=host or socket.gethostname(),
                                    scheduler_port=port,
                                    n_workers=0, **kwargs)

        # update env
        env['DASK_SCHEDULER_ADDRESS'] = self.cluster.scheduler.address
        for container in self.worker_spec.containers:
            if container.env is None:
                container.env = []
            for k, v in env.items():
                container.env.append(client.V1EnvVar(name=k, value=v))

        finalize(self, cleanup_pods, self.namespace, self.labels)

        self._cached_widget = None

        if n_workers:
            self.scale_up(n_workers)

    @property
    def _worker_pod(self):
        return client.V1Pod(metadata=self.worker_meta, spec=self.worker_spec)

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        if self._cached_widget:
            return self._cached_widget
        import ipywidgets
        layout = ipywidgets.Layout(width='150px')
        n_workers = ipywidgets.IntText(0, description='Workers', layout=layout)
        threads = ipywidgets.IntText(1, description='Cores', layout=layout)
        actual = ipywidgets.Text('0', description='Actual', layout=layout)
        button = ipywidgets.Button(description='Scale', layout=layout)
        box = ipywidgets.HBox([ipywidgets.VBox([n_workers, threads]),
                               ipywidgets.VBox([button, actual])])
        self._cached_widget = box

        def cb(b):
            self.threads_per_worker = threads.value
            n = n_workers.value
            self.scale_up(n)

        button.on_click(cb)

        worker_ref = ref(actual)
        scheduler_ref = ref(self.scheduler)

        IOLoop.current().add_callback(_update_worker_label, worker_ref,
                                      scheduler_ref)
        return box

    def _ipython_display_(self, **kwargs):
        return self._widget()._ipython_display_(**kwargs)

    @property
    def scheduler(self):
        return self.cluster.scheduler

    @property
    def scheduler_address(self):
        return self.scheduler.address

    def pods(self):
        return self.api.list_namespaced_pod(
            self.namespace,
            label_selector=format_labels(self.labels)
        ).items

    def logs(self, pod):
        return self.api.read_namespaced_pod_log(pod.metadata.name,
                                                pod.metadata.namespace)

    def scale_up(self, n, **kwargs):
        """
        Make sure we have n dask-workers available for this cluster
        """
        pods = self.pods()

        out = [self.api.create_namespaced_pod(self.namespace, self._worker_pod)
               for _ in range(n - len(pods))]

        return out
        # fixme: wait for this to be ready before returning!

    def scale_down(self, workers):
        """
        When the worker process exits, Kubernetes leaves the pods in a completed
        state. Kill them when we are asked to.
        """
        # Get the existing worker pods
        pods = self.pods()

        # Work out pods that we are going to delete
        # Each worker to delete is given in the form "tcp://<worker ip>:<port>"
        # Convert this to a set of IPs
        ips = set(urlparse(worker).hostname for worker in workers)
        to_delete = [
            p for p in pods
            # Every time we run, purge any completed pods as well as the specified ones
            if p.status.phase == 'Succeeded' or p.status.pod_ip in ips
        ]
        if not to_delete:
            return
        for pod in to_delete:
            try:
                self.api.delete_namespaced_pod(
                    pod.metadata.name,
                    self.namespace,
                    client.V1DeleteOptions()
                )
                logger.info('Deleted pod: %s', pod.metadata.name)
            except client.rest.ApiException as e:
                # If a pod has already been removed, just ignore the error
                if e.status != 404:
                    raise

    def __enter__(self):
        return self

    def close(self):
        self.scale_down(self.cluster.scheduler.workers)
        self.cluster.close()

    def __exit__(self, type, value, traceback):
        cleanup_pods(self.namespace, self.labels)
        self.cluster.__exit__(type, value, traceback)

    def __del__(self):
        self.close()  # TODO: do this more cleanly

    def adapt(self):
        """ Have cluster dynamically allocate workers based on load

        http://distributed.readthedocs.io/en/latest/adaptive.html
        """
        from distributed.deploy import Adaptive
        return Adaptive(self.scheduler, self)


def cleanup_pods(namespace, labels):
    api = client.CoreV1Api()
    pods = api.list_namespaced_pod(namespace, label_selector=format_labels(labels))
    for pod in pods.items:
        try:
            api.delete_namespaced_pod(pod.metadata.name, namespace,
                                      client.V1DeleteOptions())
            logger.info('Deleted pod: %s', pod.metadata.name)
        except client.rest.ApiException as e:
            # ignore error if pod is already removed
            if e.status != 404:
                raise


def format_labels(labels):
    return ','.join(['{}={}'.format(k, v) for k, v in labels.items()])


@gen.coroutine
def _update_worker_label(worker_ref, scheduler_ref):
    """ Periodically check the scheduler's workers and update widget

    See Also
    --------
    KubeCluster._widget
    """
    while True:
        worker = worker_ref()
        scheduler = scheduler_ref()
        if worker and scheduler:
            worker.value = str(len(scheduler.workers))
        else:
            return
        yield gen.sleep(0.5)


def _namespace_default():
    """
    Get current namespace if running in a k8s cluster

    If not in a k8s cluster with service accounts enabled, default to
    'default'

    Taken from https://github.com/jupyterhub/kubespawner/blob/master/kubespawner/spawner.py#L125
    """
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    return 'default'


class _FakeResponse(object):
    """ Kubernetes public API expects a response object """
    def __init__(self, data):
        self._data = data

    @property
    def data(self):
        import json
        return json.dumps(self._data)


def deserialize(x, cls):
    """ Deserialize object to a Kubernetes object

    Parameters
    ----------
    x: object
        Either a Pod, a dictionary definition, or a filename to a
        yaml-deserializable definition.
    cls: type
        kubernetes.client type like V1PodSpec

    Examples
    --------
    >>> deserialize({...}, client.V1PodSpec)

    Returns
    -------
    kubernetes.client.V1Pod
    """
    if isinstance(x, str):
        import yaml
        with open(x) as f:
            x = yaml.load(f)
    if isinstance(x, dict):
        x = client.ApiClient().deserialize(_FakeResponse(x), cls)
    return x


def deserialize_pod_spec(x):
    """ Special version of deserialize that handles special cases """
    spec = deserialize(x, client.V1PodSpec)
    if isinstance(x, dict):
        for c, spec_c in zip(x['containers'], spec.containers):
            if c.get('security_context'):
                spec_c.security_context = deserialize(c['security_context'], client.V1SecurityContext)
    return spec


def copy_kub(obj):
    return deserialize(obj.to_dict(), type(obj))


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('name', help='Name of the cluster')
    argparser.add_argument('namespace', help='Namespace to spawn cluster in')
    argparser.add_argument(
        '--worker-image',
        default='daskdev/dask:latest',
        help='Worker pod image. Should have same version of python as client')

    args = argparser.parse_args()

    cluster = KubeCluster(
        args.name,
        args.namespace,
        args.worker_image,
        {},
        n_workers=1,
    )
    client = Client(cluster)
    print(client.submit(lambda x: x + 1, 10).result())


if __name__ == '__main__':
    main()
