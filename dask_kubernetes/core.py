import asyncio
import copy
import getpass
import logging
import os
import socket
import string
import time
from urllib.parse import urlparse
import uuid
import weakref
from weakref import finalize

try:
    import yaml
except ImportError:
    yaml = False

import dask
import dask.distributed
import distributed.security
from distributed.deploy import SpecCluster, ProcessInterface
from distributed.comm.utils import offload
from distributed.utils import Log, Logs
import kubernetes_asyncio as kubernetes
from kubernetes_asyncio.client.rest import ApiException

from .objects import (
    make_pod_from_dict,
    make_service_from_dict,
    clean_pod_template,
    clean_service_template,
)
from .auth import ClusterAuth

logger = logging.getLogger(__name__)

SCHEDULER_PORT = 8786


class Pod(ProcessInterface):
    """ A superclass for Kubernetes Pods
    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(self, core_api, pod_template, namespace, loop=None, **kwargs):
        self._pod = None
        self.core_api = core_api
        self.pod_template = copy.deepcopy(pod_template)
        self.base_labels = self.pod_template.metadata.labels
        self.namespace = namespace
        self.name = None
        self.loop = loop
        self.kwargs = kwargs
        super().__init__()

    @property
    def cluster_name(self):
        return self.pod_template.metadata.labels["dask.org/cluster-name"]

    async def start(self, **kwargs):
        retry_count = 0  # Retry 10 times
        while True:
            try:
                self._pod = await self.core_api.create_namespaced_pod(
                    self.namespace, self.pod_template
                )
                return await super().start(**kwargs)
            except ApiException as e:
                if retry_count < 10:
                    logger.debug("Error when creating pod, retrying... - %s", str(e))
                    await asyncio.sleep(1)
                    retry_count += 1
                else:
                    raise e

    async def close(self, **kwargs):
        name, namespace = self._pod.metadata.name, self.namespace
        if self._pod:
            try:
                await self.core_api.delete_namespaced_pod(name, namespace)
            except ApiException as e:
                if e.reason == "Not Found":
                    logger.debug(
                        "Pod %s in namespace %s has been deleated already.",
                        name,
                        namespace,
                    )
                else:
                    raise

        await super().close(**kwargs)

    async def logs(self):
        try:
            log = await self.core_api.read_namespaced_pod_log(
                self._pod.metadata.name, self.namespace
            )
        except ApiException as e:
            if "waiting to start" in str(e):
                log = ""
            else:
                raise e
        return Log(log)

    async def describe_pod(self):
        self._pod = await self.core_api.read_namespaced_pod(
            self._pod.metadata.name, self.namespace
        )
        return self._pod

    def __repr__(self):
        return "<Pod %s: status=%s>" % (type(self).__name__, self.status)


class Worker(Pod):
    """ A Remote Dask Worker controled by Kubernetes
    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    name (optional):
        The name passed to the dask-worker CLI at creation time.
    """

    def __init__(self, scheduler: str, name=None, **kwargs):
        super().__init__(**kwargs)

        self.scheduler = scheduler

        self.pod_template.metadata.labels["dask.org/component"] = "worker"
        self.pod_template.spec.containers[0].env.append(
            kubernetes.client.V1EnvVar(
                name="DASK_SCHEDULER_ADDRESS", value=self.scheduler
            )
        )
        if name is not None:
            worker_name_args = ["--name", str(name)]
            self.pod_template.spec.containers[0].args += worker_name_args


class Scheduler(Pod):
    """ A Remote Dask Scheduler controled by Kubernetes
    Parameters
    ----------
    idle_timeout: str, optional
        The scheduler task will exit after this amount of time
        if there are no requests from the client. Default is to
        never timeout.
    service_wait_timeout_s: int (optional)
        Timeout, in seconds, to wait for the remote scheduler service to be ready.
        Defaults to 30 seconds.
        Set to 0 to disable the timeout (not recommended).
    """

    def __init__(self, idle_timeout: str, service_wait_timeout_s: int = None, **kwargs):
        super().__init__(**kwargs)
        self.service = None
        self._idle_timeout = idle_timeout
        self._service_wait_timeout_s = service_wait_timeout_s
        if self._idle_timeout is not None:
            self.pod_template.spec.containers[0].args += [
                "--idle-timeout",
                self._idle_timeout,
            ]

    async def start(self, **kwargs):
        await super().start(**kwargs)

        while (await self.describe_pod()).status.phase == "Pending":
            await asyncio.sleep(0.1)

        while self.address is None:
            logs = await self.logs()
            for line in logs.splitlines():
                if "Scheduler at:" in line:
                    self.address = line.split("Scheduler at:")[1].strip()
            await asyncio.sleep(0.1)

        self.service = await self._create_service()
        self.address = "tcp://{name}.{namespace}:{port}".format(
            name=self.service.metadata.name,
            namespace=self.namespace,
            port=SCHEDULER_PORT,
        )
        if self.service.spec.type == "LoadBalancer":
            # Wait for load balancer to be assigned
            start = time.time()
            while self.service.status.load_balancer.ingress is None:
                if (
                    self._service_wait_timeout_s > 0
                    and time.time() > start + self._service_wait_timeout_s
                ):
                    raise asyncio.TimeoutError(
                        "Timed out waiting for Load Balancer to be provisioned."
                    )
                self.service = await self.core_api.read_namespaced_service(
                    self.cluster_name, self.namespace
                )
                await asyncio.sleep(0.2)

            [loadbalancer_ingress] = self.service.status.load_balancer.ingress
            loadbalancer_host = loadbalancer_ingress.hostname or loadbalancer_ingress.ip
            self.external_address = "tcp://{host}:{port}".format(
                host=loadbalancer_host, port=SCHEDULER_PORT
            )
        # FIXME Set external address when using nodeport service type

        # FIXME Create an optional Ingress just in case folks want to configure one

    async def close(self, **kwargs):
        if self.service:
            await self.core_api.delete_namespaced_service(
                self.cluster_name, self.namespace
            )
        await super().close(**kwargs)

    async def _create_service(self):
        service_template_dict = dask.config.get("kubernetes.scheduler-service-template")
        self.service_template = clean_service_template(
            make_service_from_dict(service_template_dict)
        )
        self.service_template.metadata.name = self.cluster_name
        self.service_template.metadata.labels = copy.deepcopy(self.base_labels)

        self.service_template.spec.selector["dask.org/cluster-name"] = self.cluster_name
        if self.service_template.spec.type is None:
            self.service_template.spec.type = dask.config.get(
                "kubernetes.scheduler-service-type"
            )
        await self.core_api.create_namespaced_service(
            self.namespace, self.service_template
        )
        return await self.core_api.read_namespaced_service(
            self.cluster_name, self.namespace
        )


class KubeCluster(SpecCluster):
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
    pod_template: kubernetes.client.V1Pod
        A Kubernetes specification for a Pod for a dask worker.
    scheduler_pod_template: kubernetes.client.V1Pod (optional)
        A Kubernetes specification for a Pod for a dask scheduler.
        Defaults to the pod_template.
    name: str (optional)
        Name given to the pods.  Defaults to ``dask-$USER-random``
    namespace: str (optional)
        Namespace in which to launch the workers.
        Defaults to current namespace if available or "default"
    n_workers: int
        Number of workers on initial launch.
        Use ``scale`` to change this number in the future
    env: Dict[str, str]
        Dictionary of environment variables to pass to worker pod
    host: str
        Listen address for local scheduler.  Defaults to 0.0.0.0
    port: int
        Port of local scheduler
    auth: List[ClusterAuth] (optional)
        Configuration methods to attempt in order.  Defaults to
        ``[InCluster(), KubeConfig()]``.
    idle_timeout: str (optional)
        The scheduler task will exit after this amount of time
        if there are no requests from the client. Default is to
        never timeout.
    scheduler_service_wait_timeout: int (optional)
        Timeout, in seconds, to wait for the remote scheduler service to be ready.
        Defaults to 30 seconds.
        Set to 0 to disable the timeout (not recommended).
    deploy_mode: str (optional)
        Run the scheduler as "local" or "remote".
        Defaults to ``"local"``.
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
        idle_timeout=None,
        deploy_mode=None,
        interface=None,
        protocol=None,
        dashboard_address=None,
        security=None,
        scheduler_service_wait_timeout=None,
        scheduler_pod_template=None,
        **kwargs
    ):
        self.pod_template = pod_template
        self.scheduler_pod_template = scheduler_pod_template
        self._generate_name = name
        self._namespace = namespace
        self._n_workers = n_workers
        self._idle_timeout = idle_timeout
        self._deploy_mode = deploy_mode
        self._protocol = protocol
        self._interface = interface
        self._dashboard_address = dashboard_address
        self._scheduler_service_wait_timeout = scheduler_service_wait_timeout
        self.security = security
        if self.security and not isinstance(
            self.security, distributed.security.Security
        ):
            raise RuntimeError(
                "Security object is not a valid distributed.security.Security object"
            )
        self.host = host
        self.port = port
        self.env = env
        self.auth = auth
        self.kwargs = kwargs
        super().__init__(**self.kwargs)

    def _get_pod_template(self, pod_template, pod_type):
        if not pod_template and dask.config.get(
            "kubernetes.{}-template".format(pod_type), None
        ):
            d = dask.config.get("kubernetes.{}-template".format(pod_type))
            d = dask.config.expand_environment_variables(d)
            pod_template = make_pod_from_dict(d)

        if not pod_template and dask.config.get(
            "kubernetes.{}-template-path".format(pod_type), None
        ):
            import yaml

            fn = dask.config.get("kubernetes.{}-template-path".format(pod_type))
            fn = fn.format(**os.environ)
            with open(fn) as f:
                d = yaml.safe_load(f)
            d = dask.config.expand_environment_variables(d)
            pod_template = make_pod_from_dict(d)
        return pod_template

    def _fill_pod_templates(self, pod_template, pod_type):
        pod_template = copy.deepcopy(pod_template)

        # Default labels that can't be overwritten
        pod_template.metadata.labels["dask.org/cluster-name"] = self._generate_name
        pod_template.metadata.labels["dask.org/component"] = pod_type
        pod_template.metadata.labels["user"] = escape(getpass.getuser())
        pod_template.metadata.labels["app"] = "dask"
        pod_template.metadata.namespace = self._namespace

        if self.env:
            pod_template.spec.containers[0].env.extend(
                [
                    kubernetes.client.V1EnvVar(name=k, value=str(v))
                    for k, v in self.env.items()
                ]
            )
        pod_template.metadata.generate_name = self._generate_name

        return pod_template

    async def _start(self):
        self._generate_name = self._generate_name or dask.config.get("kubernetes.name")
        self._namespace = self._namespace or dask.config.get("kubernetes.namespace")
        self._idle_timeout = self._idle_timeout or dask.config.get(
            "kubernetes.idle-timeout"
        )
        self._scheduler_service_wait_timeout = (
            self._scheduler_service_wait_timeout
            or dask.config.get("kubernetes.scheduler-service-wait-timeout")
        )
        self._deploy_mode = self._deploy_mode or dask.config.get(
            "kubernetes.deploy-mode"
        )

        self._n_workers = (
            self._n_workers
            if self._n_workers is not None
            else dask.config.get("kubernetes.count.start")
        )
        self.host = self.host or dask.config.get("kubernetes.host")
        self.port = (
            self.port if self.port is not None else dask.config.get("kubernetes.port")
        )
        self._protocol = self._protocol or dask.config.get("kubernetes.protocol")
        self._interface = self._interface or dask.config.get("kubernetes.interface")
        self._dashboard_address = self._dashboard_address or dask.config.get(
            "kubernetes.dashboard_address"
        )
        self.env = (
            self.env if self.env is not None else dask.config.get("kubernetes.env")
        )

        self.pod_template = self._get_pod_template(self.pod_template, pod_type="worker")
        self.scheduler_pod_template = self._get_pod_template(
            self.scheduler_pod_template, pod_type="scheduler"
        )
        if not self.pod_template:
            msg = (
                "Worker pod specification not provided. See KubeCluster "
                "docstring for ways to specify workers"
            )
            raise ValueError(msg)

        base_pod_template = self.pod_template
        self.pod_template = clean_pod_template(self.pod_template, pod_type="worker")

        if not self.scheduler_pod_template:
            self.scheduler_pod_template = base_pod_template
            self.scheduler_pod_template.spec.containers[0].args = ["dask-scheduler"]

        self.scheduler_pod_template = clean_pod_template(
            self.scheduler_pod_template, pod_type="scheduler"
        )

        await ClusterAuth.load_first(self.auth)

        self.core_api = kubernetes.client.CoreV1Api()

        if self._namespace is None:
            self._namespace = _namespace_default()

        self._generate_name = self._generate_name.format(
            user=getpass.getuser(), uuid=str(uuid.uuid4())[:10], **os.environ
        )
        self._generate_name = escape(self._generate_name)

        self.pod_template = self._fill_pod_templates(
            self.pod_template, pod_type="worker"
        )
        self.scheduler_pod_template = self._fill_pod_templates(
            self.scheduler_pod_template, pod_type="scheduler"
        )

        finalize(
            self, _cleanup_resources, self._namespace, self.pod_template.metadata.labels
        )

        common_options = {
            "core_api": self.core_api,
            "namespace": self._namespace,
            "loop": self.loop,
        }

        if self._deploy_mode == "local":
            self.scheduler_spec = {
                "cls": dask.distributed.Scheduler,
                "options": {
                    "protocol": self._protocol,
                    "interface": self._interface,
                    "host": self.host,
                    "port": self.port,
                    "dashboard_address": self._dashboard_address,
                    "security": self.security,
                },
            }
        elif self._deploy_mode == "remote":
            self.scheduler_spec = {
                "cls": Scheduler,
                "options": {
                    "idle_timeout": self._idle_timeout,
                    "service_wait_timeout_s": self._scheduler_service_wait_timeout,
                    "pod_template": self.scheduler_pod_template,
                    **common_options,
                },
            }
        else:
            raise RuntimeError("Unknown deploy mode %s" % self._deploy_mode)

        self.new_spec = {
            "cls": Worker,
            "options": {"pod_template": self.pod_template, **common_options},
        }
        self.worker_spec = {i: self.new_spec for i in range(self._n_workers)}

        await super()._start()

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

    def scale(self, n):
        # A shim to maintain backward compatibility
        # https://github.com/dask/distributed/issues/3054
        maximum = dask.config.get("kubernetes.count.max")
        if maximum is not None and maximum < n:
            logger.info(
                "Tried to scale beyond maximum number of workers %d > %d", n, maximum
            )
            n = maximum
        return super().scale(n)

    async def _logs(self, scheduler=True, workers=True):
        """ Return logs for the scheduler and workers
        Parameters
        ----------
        scheduler : boolean
            Whether or not to collect logs for the scheduler
        workers : boolean or Iterable[str], optional
            A list of worker addresses to select.
            Defaults to all workers if `True` or no workers if `False`
        Returns
        -------
        logs: Dict[str]
            A dictionary of logs, with one item for the scheduler and one for
            each worker
        """
        logs = Logs()

        if scheduler:
            logs["Scheduler"] = await self.scheduler.logs()

        if workers:
            worker_logs = await asyncio.gather(
                *[w.logs() for w in self.workers.values()]
            )
            for key, log in zip(self.workers, worker_logs):
                logs[key] = log

        return logs


def _cleanup_resources(namespace, labels):
    """ Remove all pods with these labels in this namespace """
    import kubernetes

    core_api = kubernetes.client.CoreV1Api()

    pods = core_api.list_namespaced_pod(namespace, label_selector=format_labels(labels))
    for pod in pods.items:
        try:
            core_api.delete_namespaced_pod(pod.metadata.name, namespace)
            logger.info("Deleted pod: %s", pod.metadata.name)
        except kubernetes.client.rest.ApiException as e:
            # ignore error if pod is already removed
            if e.status != 404:
                raise

    services = core_api.list_namespaced_service(
        namespace, label_selector=format_labels(labels)
    )
    for service in services.items:
        try:
            core_api.delete_namespaced_service(service.metadata.name, namespace)
            logger.info("Deleted service: %s", service.metadata.name)
        except kubernetes.client.rest.ApiException as e:
            # ignore error if service is already removed
            if e.status != 404:
                raise


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


def escape(s):
    valid_characters = string.ascii_letters + string.digits + "-."
    return "".join(c for c in s if c in valid_characters)
