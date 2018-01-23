import getpass
import logging
import os
import socket
from urllib.parse import urlparse
import uuid
from weakref import finalize, ref

from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client
from distributed.deploy import LocalCluster
from kubernetes import client, config

logger = logging.getLogger(__name__)


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
    worker_image: str
        Docker image and tag
    worker_labels: dict
        Additional labels to add to pod
    n_workers: int
        Number of workers on initial launch.  Use ``scale_up`` in the future
    threads_per_worker: int
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
            name=None,
            namespace=None,
            worker_image='daskdev/dask:latest',
            worker_labels=None,
            n_workers=0,
            threads_per_worker=1,
            host='0.0.0.0',
            port=0,
            env={},
            **kwargs,
    ):
        self.cluster = LocalCluster(ip=host or socket.gethostname(),
                                    scheduler_port=port,
                                    n_workers=0, **kwargs)

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self.api = client.CoreV1Api()

        if namespace is None:
            namespace = _namespace_default()

        if name is None:
            name = 'dask-%s-%s' % (getpass.getuser(), str(uuid.uuid4())[:10])

        self.namespace = namespace
        self.name = name
        self.worker_image = worker_image
        self.worker_labels = (worker_labels or {}).copy()
        self.threads_per_worker = threads_per_worker
        self.env = dict(env)

        # Default labels that can't be overwritten
        self.worker_labels['dask.pydata.org/cluster-name'] = name
        self.worker_labels['app'] = 'dask'
        self.worker_labels['component'] = 'dask-worker'

        finalize(self, cleanup_pods, self.namespace, self.worker_labels)

        self._cached_widget = None

        if n_workers:
            self.scale_up(n_workers)

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

    def _make_pod(self):
        return client.V1Pod(
            metadata=client.V1ObjectMeta(
                generate_name=self.name + '-',
                labels=self.worker_labels
            ),
            spec=client.V1PodSpec(
                restart_policy='Never',
                containers=[
                    client.V1Container(
                        name='dask-worker',
                        image=self.worker_image,
                        args=[
                            'dask-worker',
                            self.scheduler_address,
                            '--nthreads', str(self.threads_per_worker),
                        ],
                        env=[client.V1EnvVar(name=k, value=v)
                             for k, v in self.env.items()],
                    )
                ]
            )
        )

    def pods(self):
        return self.api.list_namespaced_pod(
            self.namespace,
            label_selector=format_labels(self.worker_labels)
        ).items

    def logs(self, pod):
        return self.api.read_namespaced_pod_log(pod.metadata.name,
                                                pod.metadata.namespace)

    def scale_up(self, n, **kwargs):
        """
        Make sure we have n dask-workers available for this cluster
        """
        pods = self.pods()

        out = [self.api.create_namespaced_pod(self.namespace, self._make_pod())
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
        cleanup_pods(self.namespace, self.worker_labels)
        self.cluster.__exit__(type, value, traceback)

    def __del__(self):
        self.close()  # TODO: do this more cleanly

    def adapt(self):
        """ Have cluster dynamically allocate workers based on load

        http://distributed.readthedocs.io/en/latest/adaptive.html
        """
        from distributed.deploy import Adaptive
        return Adaptive(self.scheduler, self)


def cleanup_pods(namespace, worker_labels):
    api = client.CoreV1Api()
    pods = api.list_namespaced_pod(namespace, label_selector=format_labels(worker_labels))
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
