import getpass
import logging
import os
import socket
import string
import threading
from urllib.parse import urlparse
import uuid
from weakref import finalize
import asyncio

try:
    import yaml
except ImportError:
    yaml = False

import dask
from distributed.deploy import LocalCluster, Cluster
from distributed.utils import log_errors
from distributed.utils import thread_state
import kubernetes_asyncio as kubernetes

from .objects import make_pod_from_dict, clean_pod_template
from .auth import ClusterAuth

logger = logging.getLogger(__name__)

# k8s pod states
PENDING = "Pending"
RUNNING = "Running"
SUCCEEDED = "Succeeded"
FAILED = "Failed"
UNKOWN = "Unknown"
COMPLETED = "Completed"
CRAHLOOPBACKOFF = "CrashLoopBackOff"


class KubeCluster(Cluster):
    """ Launch a Dask cluster on Kubernetes

    This starts a local Dask scheduler and then dynamically launches
    Dask workers on a Kubernetes cluster. The Kubernetes cluster is taken
    to be either the current one on which this code is running, or as a
    fallback, the default one configured in a kubeconfig file.

    **Environments**

    Your worker pod image should have a similar environment to your local
    environment, including versions of Python, dask, cloudpickle, and any
    libraries that you may wish to use (like NumPy, Pandas, or Scikit-Learn).
    See examples below for suggestions on how to manage and check for this.

    **Network**

    Since the Dask scheduler is launched locally, for it to work, we need to
    be able to open network connections between this local node and all the
    workers nodes on the Kubernetes cluster. If the current process is not
    already on a Kubernetes node, some network configuration will likely be
    required to make this work.

    **Resources**

    Your Kubernetes resource limits and requests should match the
    ``--memory-limit`` and ``--nthreads`` parameters given to the
    ``dask-worker`` command.

    Parameters
    ----------
    pod_template: kubernetes_asyncio.client.V1PodSpec
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
    auth: List[ClusterAuth] (optional)
        Configuration methods to attempt in order.  Defaults to
        ``[InCluster(), KubeConfig()]``.
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
    >>> cluster.scale(10)

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
    ...                                 'EXTRA_CONDA_PACKAGES': conda})

    You can also start a KubeCluster with no arguments *if* the worker template
    is specified in the Dask config files, either as a full template in
    ``kubernetes.worker-template`` or a path to a YAML file in
    ``kubernetes.worker-template-path``.

    See https://docs.dask.org/en/latest/configuration.html for more
    information about setting configuration values.::

        $ export DASK_KUBERNETES__WORKER_TEMPLATE_PATH=worker_template.yaml

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
        n_workers=None,
        host=None,
        port=None,
        env=None,
        auth=ClusterAuth.DEFAULT,
        asynchronous=False,
        **kwargs
    ):
        name = name or dask.config.get("kubernetes.name")
        namespace = namespace or dask.config.get("kubernetes.namespace")
        self.n_workers = (
            n_workers
            if n_workers is not None
            else dask.config.get("kubernetes.count.start")
        )
        self.host = host or dask.config.get("kubernetes.host")
        self.port = port if port is not None else dask.config.get("kubernetes.port")
        self.cluster_kwargs = kwargs
        self.auth = auth
        self._asynchronous = asynchronous
        self.env = env if env is not None else dask.config.get("kubernetes.env")

        if not pod_template and dask.config.get("kubernetes.worker-template", None):
            d = dask.config.get("kubernetes.worker-template")
            d = dask.config.expand_environment_variables(d)
            pod_template = make_pod_from_dict(d)

        if not pod_template and dask.config.get(
            "kubernetes.worker-template-path", None
        ):
            import yaml

            fn = dask.config.get("kubernetes.worker-template-path")
            fn = fn.format(**os.environ)
            with open(fn) as f:
                d = yaml.safe_load(f)
            d = dask.config.expand_environment_variables(d)
            pod_template = make_pod_from_dict(d)

        if not pod_template:
            msg = (
                "Worker pod specification not provided. See KubeCluster "
                "docstring for ways to specify workers"
            )
            raise ValueError(msg)

        if namespace is None:
            namespace = _namespace_default()

        name = name.format(
            user=getpass.getuser(), uuid=str(uuid.uuid4())[:10], **os.environ
        )
        name = escape(name)

        self.pod_template = clean_pod_template(pod_template)

        # Default labels that can't be overwritten
        self.pod_template.metadata.labels["dask.org/cluster-name"] = name
        self.pod_template.metadata.labels["user"] = escape(getpass.getuser())
        self.pod_template.metadata.labels["app"] = "dask"
        self.pod_template.metadata.labels["component"] = "dask-worker"
        self.pod_template.metadata.namespace = namespace
        self.pod_template.metadata.generate_name = name

        self._manual_scale_target = n_workers or 0
        self._periodic_task = None
        self.task_queue = asyncio.Queue()
        self.task_worker = None
        self.core_api = None  # async k8s client

        self.cluster = LocalCluster(
            ip=self.host or socket.gethostname(),
            scheduler_port=self.port,
            n_workers=0,
            asynchronous=asynchronous,
            **self.cluster_kwargs,
        )

        self.pod_template.spec.containers[0].env.append(
            kubernetes.client.V1EnvVar(
                name="DASK_SCHEDULER_ADDRESS", value=self.scheduler_address
            )
        )
        if self.env:
            self.pod_template.spec.containers[0].env.extend(
                [
                    kubernetes.client.V1EnvVar(name=k, value=str(v))
                    for k, v in self.env.items()
                ]
            )

        metadata_name = kubernetes.client.V1ObjectFieldSelector(
            field_path="metadata.name"
        )
        env_var_source = kubernetes.client.V1EnvVarSource(field_ref=metadata_name)
        hostname = kubernetes.client.V1EnvVar(
            name="HOSTNAME", value_from=env_var_source
        )

        self.pod_template.spec.containers[0].env.extend([hostname])

        additional_args = ["--name", "$(HOSTNAME)"]
        if not self.pod_template.spec.containers[0].args:
            self.pod_template.spec.containers[0].args = additional_args
        else:
            self.pod_template.spec.containers[0].args.extend(additional_args)

        self.start()

    @property
    def loop(self):
        return self.scheduler.loop

    @property
    def asynchronous(self):
        if (
            hasattr(self.loop, "_thread_identity")
            and self.loop._thread_identity == threading.get_ident()
        ):
            return True
        if self._asynchronous:
            return True
        if getattr(thread_state, "asynchronous", False):
            return True
        return False

    async def _start(self):
        await ClusterAuth.load_first(self.auth)
        self.core_api = kubernetes.client.CoreV1Api()

        finalize(
            self, _cleanup_pods_sync, self.namespace, self.pod_template.metadata.labels
        )

        if self.n_workers:
            # will call KubeCluster.scale_up/scale_down
            self.scale(self.n_workers)

    async def periodic(self):
        """Periodic Callback to check cluster has the correct size
        """
        # peridically check cluster is at the correct size
        # don't assign member variables or collisions may happen
        while True:
            pods = await self._pods()
            # cleanup messy pods
            pods = await self._cleanup_terminated_pods(pods)

            logger.debug(
                f"Current Cluster Size: {len(pods)} Cluster Target: {self._manual_scale_target}"
            )

            if self.task_queue.empty():
                if len(pods) != self._manual_scale_target:
                    self.scale(self._manual_scale_target)
                # wait a tick if there is nothing left in the queue
                await asyncio.sleep(0.5)

            else:
                await self._run_queued_func()

    def start(self):
        if self.asynchronous:
            self._periodic_task = asyncio.ensure_future(self.periodic())
            self._started = self._start()
        else:
            return self.sync(self._start)

    def __await__(self):
        return self._started.__await__()

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

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
              - image: daskdev/dask:latest
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
            raise ImportError(
                "PyYaml is required to use yaml functionality, please install it!"
            )
        with open(yaml_path) as f:
            d = yaml.safe_load(f)
            d = dask.config.expand_environment_variables(d)
            return cls.from_dict(d, **kwargs)

    @property
    def namespace(self):
        return self.pod_template.metadata.namespace

    @property
    def name(self):
        return self.pod_template.metadata.generate_name

    def __repr__(self):
        if self.asynchronous:
            return 'KubeCluster("%s")' % (self.scheduler.address,)
        else:
            return 'KubeCluster("%s", workers=%d)' % (
                self.scheduler.address,
                len(self.pods()),
            )

    @property
    def scheduler(self):
        return self.cluster.scheduler

    @property
    def scheduler_address(self):
        return self.scheduler.address

    def sync(self, func, *args, **kwargs):
        return self.cluster.sync(func, *args, **kwargs)

    async def _pods(self):
        nsp = await self.core_api.list_namespaced_pod(
            self.namespace,
            label_selector=format_labels(self.pod_template.metadata.labels),
        )

        return nsp.items

    def pods(self):
        """ A list of kubernetes pods corresponding to current workers

        See Also
        --------
        KubeCluster.logs
        """
        return self.sync(self._pods)

    async def _logs(self, pod=None):
        if pod is None:
            pods = await self._pods()
            result = {
                pod.status.pod_ip: await self._logs(pod)
                for pod in pods
                if pod.status.phase != "Pending"
            }
            assert None not in result
            return result

        return await self.core_api.read_namespaced_pod_log(
            pod.metadata.name, pod.metadata.namespace
        )

    def logs(self, pod=None):
        """ Logs from a worker pod

        You can get this pod object from the ``pods`` method.

        If no pod is specified all pod logs will be returned. On large clusters
        this could end up being rather large.

        Parameters
        ----------
        pod: kubernetes.client.V1Pod
            The pod from which we want to collect logs.
f
        See Also
        --------
        KubeCluster.pods
        Client.get_worker_logs
        """
        return self.sync(self._logs, pod=pod)

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
        self._manual_scale_target = n
        with log_errors():
            if n >= len(self.scheduler.workers):
                self.task_queue.put_nowait(self._scale_up)
            else:
                self.task_queue.put_nowait((self._scale_down_with_pending_check))

            self._sync_tasks()

    def _sync_tasks(self):
        """Internal function to synchronously execute all queued task.
        Used in synchronous scale functions.
        """
        if not self.asynchronous:
            self.sync(self._run_queued_func, True)
        if not self._periodic_task:
            # probably coming in from adapt()
            # not periodic checking for the queue
            self.loop.add_callback(self._run_queued_func, True)

    async def _scale_down_with_pending_check(self):
        pods = await self._pods()
        pods = await self._cleanup_terminated_pods(pods)
        n = self._manual_scale_target
        n_to_delete = len(pods) - n

        # Before trying to close running workers, check if we can cancel
        # pending pods (in case the kubernetes cluster was too full to
        # provision those pods in the first place).
        running_workers = list(self.scheduler.workers.keys())
        running_ips = set(urlparse(worker).hostname for worker in running_workers)
        running_names = [
            v.name for w, v in self.scheduler.workers.items() if v.has_what
        ]
        running_things = [
            (w, v.name) for w, v in self.scheduler.workers.items() if v.has_what
        ]
        print(running_things)
        # Dask is Fast! A worker can register with the scheduler before k8s
        # moves the pod from `PENDING` to `RUNNING` below we remove
        # any workers in a `PENDING` state which have non-zero
        # memory or currently processing a task
        pending_pods = [
            p
            for p in pods
            if (p.status.pod_ip not in running_ips)
            and (p.metadata.name not in running_names)
        ]
        if pending_pods:
            pending_to_delete = pending_pods[:n_to_delete]

            logger.debug("Deleting pending pods: %s", pending_to_delete)
            await self._delete_pods(pending_to_delete)
            n_to_delete = n_to_delete - len(pending_to_delete)
            if n_to_delete <= 0:
                return
        to_close = select_workers_to_close(self.scheduler, n_to_delete)
        logger.debug("Closing workers: %s", to_close)

        if len(to_close) < len(self.scheduler.workers):
            await self.scheduler.retire_workers(workers=to_close)

        # Terminate all pods without waiting for clean worker shutdown
        return await self._scale_down(to_close)

    async def _delete_pods(self, to_delete):
        for pod in to_delete:
            try:
                await self.core_api.delete_namespaced_pod(
                    pod.metadata.name,
                    self.namespace,
                    kubernetes.client.V1DeleteOptions(),
                )
                pod_info = pod.metadata.name
                if pod.status.reason:
                    pod_info += " [{}]".format(pod.status.reason)
                if pod.status.message:
                    pod_info += " {}".format(pod.status.message)
                logger.info("Deleted pod: %s", pod_info)
            except kubernetes.client.rest.ApiException as e:
                # If a pod has already been removed, just ignore the error
                if e.status != 404:
                    raise

    async def _cleanup_terminated_pods(self, pods):
        terminated_phases = {SUCCEEDED, FAILED, COMPLETED}
        terminated_pods = [p for p in pods if p.status.phase in terminated_phases]
        await self._delete_pods(terminated_pods)
        pods = [p for p in pods if p.status.phase not in terminated_phases]
        return pods

    def scale_up(self, n, **kwargs):
        self._manual_scale_target = n
        self.task_queue.put_nowait(self._scale_up)
        return self._sync_tasks()

    async def _scale_up(self, pods=None, **kwargs):
        """
        Use the ``.scale`` method instead
        """
        maximum = dask.config.get("kubernetes.count.max")
        status_timeout = dask.config.get("kubernetes.status-timeout", 5)
        last_exception = Exception("Failed to Scale up")

        if not pods:
            pods = await self._pods()
            current_pod_size = len(pods)

        if maximum is not None and maximum < self._manual_scale_target:
            logger.info(
                "Tried to scale beyond maximum number of workers %d > %d",
                self._manual_scale_target,
                maximum,
            )
            logger.info("Resetting target to: %d", self._manual_scale_target)
            self._manual_scale_target = maximum

        target_size = self._manual_scale_target

        # if cluster is scaling up/down quickly we may already have
        # met requirements
        if len(pods) >= target_size:
            return pods

        async def _():
            for i in range(3):
                try:
                    new_pod = await self.core_api.create_namespaced_pod(
                        self.namespace, self.pod_template
                    )
                    logger.debug("Added 1 new pod")
                    break
                except kubernetes.client.rest.ApiException as e:
                    if e.status == 500 and "ServerTimeout" in e.body:
                        logger.info("Server timeout, retry #%d", i + 1)
                        asyncio.sleep(1)
                        last_exception = e
                        pods = await self._pods()
                        pods = await self._cleanup_terminated_pods(pods)
                        continue
                    else:
                        raise
            else:
                raise last_exception

        # create pods concurrently
        results = await asyncio.gather(
            *[_() for p in range(target_size)], return_exceptions=True
        )
        pods = await self._pods()
        assert len(pods) >= target_size
        return pods

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
        self.sync(self._scale_down, workers, pods)
        self._sync_tasks()

    async def _scale_down(self, workers, pods=None):
        # Get the existing worker pods
        if pods is None:
            pods = await self._pods()
            pods = await self._cleanup_terminated_pods(pods)

        # Work out the list of pods that we are going to delete
        # Each worker to delete is given in the form "tcp://<worker ip>:<port>"
        # Convert this to a set of IPs
        ips = set(urlparse(worker).hostname for worker in workers)
        to_delete = [p for p in pods if p.status.pod_ip in ips]
        if not to_delete:
            return
        else:
            logger.debug("Deleting %d pods", len(to_delete))
        deleted_pods = await self._delete_pods(to_delete)
        return deleted_pods

    def __enter__(self):
        return self

    async def _run_queued_func(self, all=False):
        func = await self.task_queue.get()
        # execute function
        await func()
        self.task_queue.task_done()

        if all:
            while not self.task_queue.empty():
                self._run_queued_func(all=True)

    async def _close(self, **kwargs):
        self.scale_down(self.cluster.scheduler.workers)

        if self._periodic_task:
            self._periodic_task.cancel()

        # run everything in the queue
        if not self.task_queue.empty():
            await self._run_queued_func(all=True)

        # https://github.com/tomplus/kubernetes_asyncio/issues/25
        # maybe we need to delete the client on close
        del self.core_api
        return self.cluster.close(**kwargs)

    def close(self, **kwargs):
        """ Close this cluster """
        return self.sync(self._close, **kwargs)

    def __exit__(self, type, value, traceback):
        _cleanup_pods_sync(self.namespace, self.pod_template.metadata.labels)
        self.cluster.__exit__(type, value, traceback)


def _cleanup_pods_sync(namespace, labels):
    import kubernetes

    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()
    api = kubernetes.client.CoreV1Api()
    pods = api.list_namespaced_pod(namespace, label_selector=format_labels(labels))
    for pod in pods.items:
        try:
            api.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=namespace,
                body=kubernetes.client.V1DeleteOptions(),
            )
            logger.info("Deleted pod: %s", pod.metadata.name)
        except kubernetes.client.rest.ApiException as e:
            # ignore error if pod is already removed
            if e.status != 404:
                raise
    del api


def format_labels(labels):
    """ Convert a dictionary of labels into a comma separated string """
    if labels:
        return ",".join(["{}={}".format(k, v) for k, v in labels.items()])
    else:
        return ""


def _namespace_default():
    """
    Get current namespace if running in a k8s cluster

    If not in a k8s cluster with service accounts enabled, default to
    'default'

    Taken from https://github.com/jupyterhub/kubespawner/blob/master/kubespawner/spawner.py#L125
    """
    ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    return "default"


def select_workers_to_close(scheduler, n_to_close):
    """ Select n workers to close from scheduler """
    workers = list(scheduler.workers.values())
    assert n_to_close <= len(workers)
    key = lambda ws: ws.metrics["memory"]
    to_close = set(sorted(scheduler.idle, key=key)[:n_to_close])

    if len(to_close) < n_to_close:
        rest = sorted(workers, key=key, reverse=True)
        while len(to_close) < n_to_close:
            to_close.add(rest.pop())

    return [ws.address for ws in to_close]


valid_characters = string.ascii_letters + string.digits + "_-."


def escape(s):
    return "".join(c for c in s if c in valid_characters)
