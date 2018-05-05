import getpass
import logging
import os
import socket
import string
import time
from urllib.parse import urlparse
import uuid
from weakref import finalize

try:
    import yaml
except ImportError:
    yaml = False

from tornado import gen
from distributed.deploy import LocalCluster, Cluster
from distributed.config import config
from distributed.comm.utils import offload
import kubernetes

from .objects import make_pod_from_dict, clean_pod_template

logger = logging.getLogger(__name__)


class KubeCluster(Cluster):
    """ Launch a Dask cluster on Kubernetes

    This starts a local Dask scheduler and then dynamically launches
    Dask workers on a Kubernetes cluster.

    **Environments**

    Your worker pod image should have a similar environment to your local
    environment, including versions of Python, dask, cloudpickle, and any
    libraries that you may wish to use (like NumPy, Pandas, or Scikit-Learn).
    See examples below for suggestions on how to manage and check for this.

    **Resources**

    Your Kubernetes resource limits and requests should match the
    ``--memory-limit`` and ``--nthreads`` parameters given to the
    ``dask-worker`` command.

    Parameters
    ----------
    pod_template: kubernetes.client.V1PodSpec
        A Kubernetes specification for a Pod for a dask worker.
    name: str (optional)
        Name given to the pods.  Defaults to ``dask-$USER-random``
    namespace: str (optional)
        Namespace in which to launch the workers.
        Defaults to current namespace if available or "default"
    n_workers: int
        Number of workers on initial launch.
        Use ``scale_up`` to increase this number in the future
    env: Dict[str, str]
        Dictionary of environment variables to pass to worker pod
    host: str
        Listen address for local scheduler.  Defaults to 0.0.0.0
    port: int
        Port of local scheduler
    **kwargs: dict
        Additional keyword arguments to pass to LocalCluster

    Examples
    --------
    >>> from dask_kubernetes import KubeCluster, make_pod_spec
    >>> pod_spec = make_pod_spec(image='daskdev/dask:latest',
    ...                          memory_limit='4G', memory_request='4G',
    ...                          cpu_limit=1, cpu_request=1,
    ...                          env={'EXTRA_PIP_PACKAGES': 'fastparquet git+https://github.com/dask/distributed'})
    >>> cluster = KubeCluster(pod_spec)
    >>> cluster.scale_up(10)

    You can also create clusters with worker pod specifications as dictionaries
    or stored in YAML files

    >>> cluster = KubeCluster.from_yaml('worker-template.yml')
    >>> cluster = KubeCluster.from_dict({...})

    Rather than explicitly setting a number of workers you can also ask the
    cluster to allocate workers dynamically based on current workload

    >>> cluster.adapt()

    You can pass this cluster directly to a Dask client

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    You can verify that your local environment matches your worker environments
    by calling ``client.get_versions(check=True)``.  This will raise an
    informative error if versions do not match.

    >>> client.get_versions(check=True)

    The ``daskdev/dask`` docker images support ``EXTRA_PIP_PACKAGES``,
    ``EXTRA_APT_PACKAGES`` and ``EXTRA_CONDA_PACKAGES`` environment variables
    to help with small adjustments to the worker environments.  We recommend
    the use of pip over conda in this case due to a much shorter startup time.
    These environment variables can be modified directly from the KubeCluster
    constructor methods using the ``env=`` keyword.  You may list as many
    packages as you like in a single string like the following:

    >>> pip = 'pyarrow gcsfs git+https://github.com/dask/distributed'
    >>> conda = '-c conda-forge scikit-learn'
    >>> KubeCluster.from_yaml(..., env={'EXTRA_PIP_PACKAGES': pip,
    ...                                 'ExtRA_CONDA_PACKAGES': conda})

    You can also start a KubeCluster with no arguments *if* the YAML file
    defining the worker template is referred to in the
    ``DASKERNETES_WORKER_TEMPLATE_PATH`` environment variable

        $ export DASKERNETES_WORKER_TEMPLATE_PATH=worker_template.yaml

    >>> cluster = KubeCluster()  # automatically finds 'worker_template.yaml'

    See Also
    --------
    KubeCluster.from_yaml
    KubeCluster.from_dict
    KubeCluster.adapt
    """
    def __init__(
            self,
            pod_template=None,
            name=None,
            namespace=None,
            n_workers=0,
            host='0.0.0.0',
            port=0,
            env=None,
            **kwargs
    ):
        if pod_template is None:
            if 'kubernetes-worker-template-path' in config:
                import yaml
                with open(config['kubernetes-worker-template-path']) as f:
                    d = yaml.safe_load(f)
                pod_template = make_pod_from_dict(d)
            else:
                msg = ("Worker pod specification not provided. See KubeCluster "
                       "docstring for ways to specify workers")
                raise ValueError(msg)

        self.cluster = LocalCluster(ip=host or socket.gethostname(),
                                    scheduler_port=port,
                                    n_workers=0, **kwargs)
        try:
            kubernetes.config.load_incluster_config()
        except kubernetes.config.ConfigException:
            kubernetes.config.load_kube_config()

        self.core_api = kubernetes.client.CoreV1Api()

        if namespace is None:
            namespace = _namespace_default()

        if name is None:
            worker_name = config.get('kubernetes-worker-name', 'dask-{user}-{uuid}')
            name = worker_name.format(user=escape(getpass.getuser()),
                                      uuid=str(uuid.uuid4())[:10], **os.environ)

        self.pod_template = clean_pod_template(pod_template)
        # Default labels that can't be overwritten
        self.pod_template.metadata.labels['dask.pydata.org/cluster-name'] = name
        self.pod_template.metadata.labels['app'] = 'dask'
        self.pod_template.metadata.labels['component'] = 'dask-worker'
        self.pod_template.metadata.namespace = namespace

        self.pod_template.spec.containers[0].env.append(
            kubernetes.client.V1EnvVar(name='DASK_SCHEDULER_ADDRESS',
                                       value=self.scheduler_address)
        )
        if env:
            self.pod_template.spec.containers[0].env.extend([
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in env.items()
            ])
        self.pod_template.metadata.generate_name = name

        finalize(self, _cleanup_pods, self.namespace, self.pod_template.metadata.labels)

        if n_workers:
            self.scale(n_workers)

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
        ...             'image': 'daskdev/dask:latest',
        ...             'name': 'dask-worker',
        ...         }],
        ...     'restartPolicy': 'Never',
        ...     }
        ... }
        >>> cluster = KubeCluster.from_dict(spec, namespace='my-ns')  # doctest: +SKIP

        See Also
        --------
        KubeCluster.from_yaml
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
                -   image: daskdev/dask:latest
                    name: dask-worker
                    args: [dask-worker, $(DASK_SCHEDULER_ADDRESS), --nthreads, '2', --memory-limit, 8GB]
                restartPolicy: Never

        Examples
        --------
        >>> cluster = KubeCluster.from_yaml('pod.yaml', namespace='my-ns')  # doctest: +SKIP

        See Also
        --------
        KubeCluster.from_dict
        """
        if not yaml:
            raise ImportError("PyYaml is required to use yaml functionality, please install it!")
        with open(yaml_path) as f:
            d = yaml.safe_load(f)
            return cls.from_dict(d, **kwargs)

    @property
    def namespace(self):
        return self.pod_template.metadata.namespace

    @property
    def name(self):
        return self.pod_template.metadata.generate_name

    def __repr__(self):
        return 'KubeCluster("%s", workers=%d)' % (self.scheduler.address,
                                                  len(self.pods()))

    @property
    def scheduler(self):
        return self.cluster.scheduler

    @property
    def scheduler_address(self):
        return self.scheduler.address

    def pods(self):
        """ A list of kubernetes pods corresponding to current workers

        See Also
        --------
        KubeCluster.logs
        """
        return self.core_api.list_namespaced_pod(
            self.namespace,
            label_selector=format_labels(self.pod_template.metadata.labels)
        ).items

    def logs(self, pod):
        """ Logs from a worker pod

        You can get this pod object from the ``pods`` method.

        Parameters
        ----------
        pod: kubernetes.client.V1Pod
            The pod from which we want to collect logs.

        See Also
        --------
        KubeCluster.pods
        Client.get_worker_logs
        """
        return self.core_api.read_namespaced_pod_log(pod.metadata.name,
                                                     pod.metadata.namespace)

    def scale(self, n):
        """ Scale cluster to n workers

        Parameters
        ----------
        n: int
            Target number of workers

        Example
        -------
        >>> cluster.scale(10)  # scale cluster to ten workers

        See Also
        --------
        KubeCluster.scale_up
        KubeCluster.scale_down
        """
        pods = self._cleanup_succeeded_pods(self.pods())
        if n >= len(pods):
            return self.scale_up(n, pods=pods)
        else:
            n_to_delete = len(pods) - n
            # Before trying to close running workers, check if we can cancel
            # pending pods (in case the kubernetes cluster was too full to
            # provision those pods in the first place).
            running_workers = list(self.scheduler.workers.keys())
            running_ips = set(urlparse(worker).hostname
                              for worker in running_workers)
            pending_pods = [p for p in pods
                            if p.status.pod_ip not in running_ips]
            if pending_pods:
                pending_to_delete = pending_pods[:n_to_delete]
                logger.debug("Deleting pending pods: %s", pending_to_delete)
                self._delete_pods(pending_to_delete)
                n_to_delete = n_to_delete - len(pending_to_delete)
                if n_to_delete <= 0:
                    return

            to_close = select_workers_to_close(self.scheduler, n_to_delete)
            logger.debug("Closing workers: %s", to_close)
            if len(to_close) < len(self.scheduler.workers):
                # Close workers cleanly to migrate any temporary results to
                # remaining workers.
                @gen.coroutine
                def f(to_close):
                    yield self.scheduler.retire_workers(
                        workers=to_close, remove=True, close_workers=True)
                    yield offload(self.scale_down, to_close)

                self.scheduler.loop.add_callback(f, to_close)
                return

            # Terminate all pods without waiting for clean worker shutdown
            self.scale_down(to_close)

    def _delete_pods(self, to_delete):
        for pod in to_delete:
            try:
                self.core_api.delete_namespaced_pod(
                    pod.metadata.name,
                    self.namespace,
                    kubernetes.client.V1DeleteOptions()
                )
                logger.info('Deleted pod: %s', pod.metadata.name)
            except kubernetes.client.rest.ApiException as e:
                # If a pod has already been removed, just ignore the error
                if e.status != 404:
                    raise

    def _cleanup_succeeded_pods(self, pods):
        terminated_pods = [p for p in pods if p.status.phase == 'Succeeded']
        self._delete_pods(terminated_pods)
        return [p for p in pods if p.status.phase != 'Succeeded']

    def scale_up(self, n, pods=None, **kwargs):
        """
        Make sure we have n dask-workers available for this cluster

        Examples
        --------
        >>> cluster.scale_up(20)  # ask for twenty workers
        """
        pods = pods or self._cleanup_succeeded_pods(self.pods())
        to_create = n - len(pods)
        new_pods = []
        for i in range(3):
            try:
                for _ in range(to_create):
                    new_pods.append(self.core_api.create_namespaced_pod(
                        self.namespace, self.pod_template))
                    to_create -= 1
                break
            except kubernetes.client.rest.ApiException as e:
                if e.status == 500 and 'ServerTimeout' in e.body:
                    logger.info("Server timeout, retry #%d", i + 1)
                    time.sleep(1)
                    last_exception = e
                    continue
                else:
                    raise
        else:
            raise last_exception

        return new_pods
        # fixme: wait for this to be ready before returning!

    def scale_down(self, workers, pods=None):
        """ Remove the pods for the requested list of workers

        When scale_down is called by the _adapt async loop, the workers are
        assumed to have been cleanly closed first and in-memory data has been
        migrated to the remaining workers.

        Note that when the worker process exits, Kubernetes leaves the pods in
        a 'Succeeded' state that we collect here.

        If some workers have not been closed, we just delete the pods with
        matching ip addresses.

        Parameters
        ----------
        workers: List[str] List of addresses of workers to close
        """
        # Get the existing worker pods
        pods = pods or self._cleanup_succeeded_pods(self.pods())

        # Work out the list of pods that we are going to delete
        # Each worker to delete is given in the form "tcp://<worker ip>:<port>"
        # Convert this to a set of IPs
        ips = set(urlparse(worker).hostname for worker in workers)
        to_delete = [p for p in pods if p.status.pod_ip in ips]
        if not to_delete:
            return
        self._delete_pods(to_delete)

    def __enter__(self):
        return self

    def close(self):
        """ Close this cluster """
        self.scale_down(self.cluster.scheduler.workers)
        self.cluster.close()

    def __exit__(self, type, value, traceback):
        _cleanup_pods(self.namespace, self.pod_template.metadata.labels)
        self.cluster.__exit__(type, value, traceback)


def _cleanup_pods(namespace, labels):
    """ Remove all pods with these labels in this namespace """
    api = kubernetes.client.CoreV1Api()
    pods = api.list_namespaced_pod(namespace, label_selector=format_labels(labels))
    for pod in pods.items:
        try:
            api.delete_namespaced_pod(pod.metadata.name, namespace,
                                      kubernetes.client.V1DeleteOptions())
            logger.info('Deleted pod: %s', pod.metadata.name)
        except kubernetes.client.rest.ApiException as e:
            # ignore error if pod is already removed
            if e.status != 404:
                raise


def format_labels(labels):
    """ Convert a dictionary of labels into a comma separated string """
    if labels:
        return ','.join(['{}={}'.format(k, v) for k, v in labels.items()])
    else:
        return ''


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


def select_workers_to_close(scheduler, n_to_close):
    """ Select n workers to close from scheduler """
    workers = list(scheduler.workers.values())
    assert n_to_close <= len(workers)
    key = lambda ws: ws.info['memory']
    to_close = set(sorted(scheduler.idle, key=key)[:n_to_close])

    if len(to_close) < n_to_close:
        rest = sorted(workers, key=key, reverse=True)
        while len(to_close) < n_to_close:
            to_close.add(rest.pop())

    return [ws.address for ws in to_close]


valid_characters = string.ascii_letters + string.digits + '_-.'


def escape(s):
    return ''.join(c for c in s if c in valid_characters)
