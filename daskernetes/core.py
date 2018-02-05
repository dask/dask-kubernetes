import getpass
import logging
import os
import socket
import copy
from urllib.parse import urlparse
import uuid
from weakref import finalize, ref

try:
    import yaml
except ImportError:
    yaml = False
from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client
from distributed.deploy import LocalCluster
from kubernetes import client, config

from daskernetes.objects import make_pod_from_dict

logger = logging.getLogger(__name__)



class KubeCluster(object):
    """ Launch a Dask cluster on Kubernetes

    This dynamically launches Dask workers on a Kubernetes cluster.
    It starts a Dask scheduler in this local process and creates remote
    worker pods with Kubernetes.

    Parameters
    ----------
    name: str
        Name given to the pods.  Defaults to ``dask-user-random``
    namespace: str
        Namespace in which to launch the workers.  Defaults to current
        namespace if available or "default"
    n_workers: int
        Number of workers on initial launch.  Use ``scale_up`` in the future
    env: dict
        Dictionariy of environment variables to pass to worker pod
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
            worker_pod_template,
            name=None,
            namespace=None,
            n_workers=0,
            host='0.0.0.0',
            port=0,
            env=None,
            **kwargs,
    ):
        self.cluster = LocalCluster(ip=host or socket.gethostname(),
                                    scheduler_port=port,
                                    n_workers=0, **kwargs)
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self.core_api = client.CoreV1Api()

        if namespace is None:
            namespace = _namespace_default()

        if name is None:
            name = 'dask-%s-%s' % (getpass.getuser(), str(uuid.uuid4())[:10])

        self.env = env
        self.name = name

        self.worker_pod_template = copy.deepcopy(worker_pod_template)
        # Default labels that can't be overwritten
        if self.worker_pod_template.metadata.labels is None:
            self.worker_pod_template.metadata.labels = {}
        self.worker_pod_template.metadata.labels['dask.pydata.org/cluster-name'] = name
        self.worker_pod_template.metadata.labels['app'] = 'dask'
        self.worker_pod_template.metadata.labels['component'] = 'dask-worker'
        self.worker_pod_template.metadata.namespace = namespace

        finalize(self, cleanup_pods, self.namespace, worker_pod_template.metadata.labels)

        self._cached_widget = None

        if n_workers:
            self.scale_up(n_workers)

    @classmethod
    def from_dict(cls, pod_spec, **kwargs):
        """ Create cluster with worker pod spec defined by Python dictionary

        Examples
        --------
        >>> spec = {
        ...     'metadata': {},
        ...     'spec': {
        ...         'containers': [{
        ...             'args': ['dask-worker', '$(DASK_SCHEDULER_ADDRESS)',
        ...                      '--nthreads', '1',
        ...                      '--death-timeout', '60'],
        ...             'command': None,
        ...             'image': image_name,
        ...             'name': 'dask-worker',
        ...         }],
        ...     'restartPolicy': 'Never',
        ...     }
        ... }
        >>> cluster = KubeCluster.from_dict(spec, namespace='my-ns')  # doctest: +SKIP
        """
        return cls(make_pod_from_dict(pod_spec), **kwargs)

    @classmethod
    def from_yaml(cls, yaml_path, **kwargs):
        """ Create cluster with worker pod spec defined by a YAML file

        We can start a cluster with pods defined in an accompanying YAML file
        like the following:

        .. code-block:: yaml

            kind: Pod
            metadata:
                labels:
                    foo: bar
                    baz: quux
            spec:
                containers:
                -   image: daskdev/dask
                    name: dask-worker
                    args: [dask-worker, $(DASK_SCHEDULER_ADDRESS), --nthreads, '2', --memory-limit, 8GB]
                restartPolicy: Never

        Examples
        --------
        >>> cluster = KubeCluster.from_yaml('pod.yaml', namespace='my-ns')  # doctest: +SKIP
        """
        if not yaml:
            raise ImportError("PyYaml is required to use yaml functionality, please install it!")
        with open(yaml_path) as f:
            d = yaml.safe_load(f)
            return cls.from_dict(d)

    @property
    def namespace(self):
        return self.worker_pod_template.metadata.namespace

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        if self._cached_widget:
            return self._cached_widget
        import ipywidgets
        layout = ipywidgets.Layout(width='150px')

        n_workers = ipywidgets.IntText(0, description='Requested', layout=layout)
        actual = ipywidgets.Text('0', description='Actual', layout=layout)
        button = ipywidgets.Button(description='Scale', layout=layout)
        box = ipywidgets.VBox([n_workers, actual, button])
        self._cached_widget = box

        def cb(b):
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

    def _make_pod(self):
        pod = copy.deepcopy(self.worker_pod_template)
        if pod.spec.containers[0].env is None:
            pod.spec.containers[0].env = []
        pod.spec.containers[0].env.append(
            client.V1EnvVar(name='DASK_SCHEDULER_ADDRESS', value=self.scheduler_address)
        )
        if self.env:
            pod.spec.containers[0].env.extend([
                client.V1EnvVar(name=k, value=str(v))
                for k, v in self.env.items()
            ])
        pod.metadata.generate_name = self.name
        return pod

    def pods(self):
        return self.core_api.list_namespaced_pod(
            self.namespace,
            label_selector=format_labels(self.worker_pod_template.metadata.labels)
        ).items

    def logs(self, pod):
        return self.core_api.read_namespaced_pod_log(pod.metadata.name,
                                                     pod.metadata.namespace)

    def scale_up(self, n, **kwargs):
        """
        Make sure we have n dask-workers available for this cluster
        """
        pods = self.pods()

        out = [
            self.core_api.create_namespaced_pod(self.namespace, self._make_pod())
            for _ in range(n - len(pods))
        ]

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
                self.core_api.delete_namespaced_pod(
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
        cleanup_pods(self.namespace, self.worker_pod_template.metadata.labels)
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
    if labels:
        return ','.join(['{}={}'.format(k, v) for k, v in labels.items()])
    else:
        return ''


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
