import asyncio
import copy
import getpass
import logging
import os
import time
import uuid
import warnings

import aiohttp
import yaml
import dask
import dask.distributed
import distributed.security
from distributed.deploy import SpecCluster, ProcessInterface
from distributed.utils import format_dashboard_link, Log, Logs
import kubernetes_asyncio as kubernetes
from kubernetes_asyncio.client.rest import ApiException

from ..common.objects import (
    make_pod_from_dict,
    make_service_from_dict,
    make_pdb_from_dict,
    clean_pod_template,
    clean_service_template,
    clean_pdb_template,
)
from ..common.auth import ClusterAuth
from ..common.utils import (
    get_current_namespace,
    escape,
)
from ..common.networking import (
    get_external_address_for_scheduler_service,
    get_scheduler_address,
)

logger = logging.getLogger(__name__)

SCHEDULER_PORT = 8786


class Pod(ProcessInterface):
    """A superclass for Kubernetes Pods

    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(
        self,
        cluster,
        core_api,
        policy_api,
        pod_template,
        namespace,
        loop=None,
        **kwargs
    ):
        self._pod = None
        self.cluster = cluster
        self.core_api = core_api
        self.policy_api = policy_api
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
                    await asyncio.sleep(0.1)
                    retry_count += 1
                else:
                    raise e

    async def close(self, **kwargs):
        if self._pod:
            retry_count = 0  # Retry 10 times
            while True:
                name, namespace = self._pod.metadata.name, self.namespace
                try:
                    await self.core_api.delete_namespaced_pod(name, namespace)
                    return await super().close(**kwargs)
                except ApiException as e:
                    if e.reason == "Not Found":
                        logger.debug(
                            "Pod %s in namespace %s has been deleted already.",
                            name,
                            namespace,
                        )
                        return await super().close(**kwargs)
                    else:
                        raise
                except aiohttp.client_exceptions.ClientConnectorError as e:
                    if retry_count < 10:
                        logger.debug("Connection error, retrying... - %s", str(e))
                        await asyncio.sleep(0.1)
                        retry_count += 1
                    else:
                        raise e

    async def logs(self):
        try:
            log = await self.core_api.read_namespaced_pod_log(
                self._pod.metadata.name,
                self.namespace,
                container=self.pod_template.spec.containers[0].name,
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
    """A Remote Dask Worker controled by Kubernetes
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
    """A Remote Dask Scheduler controled by Kubernetes
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

    def __init__(
        self,
        idle_timeout: str,
        service_wait_timeout_s: int = None,
        service_name_retries: int = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.cluster._log("Creating scheduler pod on cluster. This may take some time.")
        self.service = None
        self._idle_timeout = idle_timeout
        self._service_wait_timeout_s = service_wait_timeout_s
        self._service_name_retries = service_name_retries
        if self._idle_timeout is not None:
            self.pod_template.spec.containers[0].args += [
                "--idle-timeout",
                self._idle_timeout,
            ]
        self.pdb = None

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
        self.external_address = await get_external_address_for_scheduler_service(
            self.core_api,
            self.service,
            service_name_resolution_retries=self._service_name_retries,
        )

        self.pdb = await self._create_pdb()

    async def close(self, **kwargs):
        if self.service:
            await self.core_api.delete_namespaced_service(
                self.cluster_name, self.namespace
            )
        if self.pdb:
            await self.policy_api.delete_namespaced_pod_disruption_budget(
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
        service = await self.core_api.read_namespaced_service(
            self.cluster_name, self.namespace
        )
        if service.spec.type == "LoadBalancer":
            # Wait for load balancer to be assigned
            start = time.time()
            while service.status.load_balancer.ingress is None:
                if (
                    self._service_wait_timeout_s > 0
                    and time.time() > start + self._service_wait_timeout_s
                ):
                    raise asyncio.TimeoutError(
                        "Timed out waiting for Load Balancer to be provisioned."
                    )
                service = await self.core_api.read_namespaced_service(
                    self.cluster_name, self.namespace
                )
                await asyncio.sleep(0.2)
        return service

    async def _create_pdb(self):
        pdb_template_dict = dask.config.get("kubernetes.scheduler-pdb-template")
        self.pdb_template = clean_pdb_template(make_pdb_from_dict(pdb_template_dict))
        self.pdb_template.metadata.name = self.cluster_name
        self.pdb_template.metadata.labels = copy.deepcopy(self.base_labels)
        self.pdb_template.spec.selector.match_labels[
            "dask.org/cluster-name"
        ] = self.cluster_name
        await self.policy_api.create_namespaced_pod_disruption_budget(
            self.namespace, self.pdb_template
        )
        return await self.policy_api.read_namespaced_pod_disruption_budget(
            self.cluster_name, self.namespace
        )


class KubeCluster(SpecCluster):
    """Launch a Dask cluster on Kubernetes

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
    pod_template: (kubernetes.client.V1Pod, dict, str)
        A Kubernetes specification for a Pod for a dask worker. Can be either a
        ``V1Pod``, a dict representation of a pod, or a path to a yaml file
        containing a pod specification.
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
    scheduler_service_name_resolution_retries: int (optional)
        Number of retries to resolve scheduler service name when running
        from within the Kubernetes cluster.
        Defaults to 20.
        Must be set to 1 or greater.
    deploy_mode: str (optional)
        Run the scheduler as "local" or "remote".
        Defaults to ``"remote"``.
    apply_default_affinity: str (optional)
        Apply a default affinity to pods: "required", "preferred" or "none"
        Defaults to ``"preferred"``.
    **kwargs: dict
        Additional keyword arguments to pass to SpecCluster

    Examples
    --------
    >>> from dask_kubernetes.classic import KubeCluster, make_pod_spec
    >>> pod_spec = make_pod_spec(image='ghcr.io/dask/dask:latest',
    ...                          memory_limit='4G', memory_request='4G',
    ...                          cpu_limit=1, cpu_request=1,
    ...                          env={'EXTRA_PIP_PACKAGES': 'fastparquet git+https://github.com/dask/distributed'})
    >>> cluster = KubeCluster(pod_spec)
    >>> cluster.scale(10)

    You can also create clusters with worker pod specifications as dictionaries
    or stored in YAML files

    >>> cluster = KubeCluster('worker-template.yml')
    >>> cluster = KubeCluster({...})

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

    The ``ghcr.io/dask/dask`` docker images support ``EXTRA_PIP_PACKAGES``,
    ``EXTRA_APT_PACKAGES`` and ``EXTRA_CONDA_PACKAGES`` environment variables
    to help with small adjustments to the worker environments.  We recommend
    the use of pip over conda in this case due to a much shorter startup time.
    These environment variables can be modified directly from the KubeCluster
    constructor methods using the ``env=`` keyword.  You may list as many
    packages as you like in a single string like the following:

    >>> pip = 'pyarrow gcsfs git+https://github.com/dask/distributed'
    >>> conda = '-c conda-forge scikit-learn'
    >>> KubeCluster(..., env={'EXTRA_PIP_PACKAGES': pip,
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
        scheduler_service_name_resolution_retries=None,
        scheduler_pod_template=None,
        apply_default_affinity="preferred",
        **kwargs
    ):
        if isinstance(pod_template, str):
            with open(pod_template) as f:
                pod_template = dask.config.expand_environment_variables(
                    yaml.safe_load(f)
                )
        if isinstance(pod_template, dict):
            pod_template = make_pod_from_dict(pod_template)

        if isinstance(scheduler_pod_template, str):
            with open(scheduler_pod_template) as f:
                scheduler_pod_template = dask.config.expand_environment_variables(
                    yaml.safe_load(f)
                )
        if isinstance(scheduler_pod_template, dict):
            scheduler_pod_template = make_pod_from_dict(scheduler_pod_template)

        self.pod_template = copy.deepcopy(pod_template)
        self.scheduler_pod_template = copy.deepcopy(scheduler_pod_template)
        self.apply_default_affinity = apply_default_affinity
        self._generate_name = dask.config.get("kubernetes.name", override_with=name)
        self.namespace = dask.config.get(
            "kubernetes.namespace", override_with=namespace
        )
        self._n_workers = dask.config.get(
            "kubernetes.count.start", override_with=n_workers
        )
        self._idle_timeout = dask.config.get(
            "kubernetes.idle-timeout", override_with=idle_timeout
        )
        self._deploy_mode = dask.config.get(
            "kubernetes.deploy-mode", override_with=deploy_mode
        )
        self._protocol = dask.config.get("kubernetes.protocol", override_with=protocol)
        self._interface = dask.config.get(
            "kubernetes.interface", override_with=interface
        )
        self._dashboard_address = dask.config.get(
            "kubernetes.dashboard_address", override_with=dashboard_address
        )
        self._scheduler_service_wait_timeout = dask.config.get(
            "kubernetes.scheduler-service-wait-timeout",
            override_with=scheduler_service_wait_timeout,
        )
        self._scheduler_service_name_resolution_retries = dask.config.get(
            "kubernetes.scheduler-service-name-resolution-retries",
            override_with=scheduler_service_name_resolution_retries,
        )
        self.security = security
        if self.security and not isinstance(
            self.security, distributed.security.Security
        ):
            raise RuntimeError(
                "Security object is not a valid distributed.security.Security object"
            )
        self.host = dask.config.get("kubernetes.host", override_with=host)
        self.port = dask.config.get("kubernetes.port", override_with=port)
        self.env = dask.config.get("kubernetes.env", override_with=env)
        self.auth = auth
        self.kwargs = kwargs
        super().__init__(**self.kwargs)

    @property
    def dashboard_link(self):
        host = self.scheduler_address.split("://")[1].split("/")[0].split(":")[0]
        return format_dashboard_link(host, self.forwarded_dashboard_port)

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
        pod_template.metadata.namespace = self.namespace

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
        self.pod_template = clean_pod_template(
            self.pod_template,
            apply_default_affinity=self.apply_default_affinity,
            pod_type="worker",
        )

        if not self.scheduler_pod_template:
            self.scheduler_pod_template = base_pod_template
        self.scheduler_pod_template.spec.containers[0].args = ["dask-scheduler"]

        self.scheduler_pod_template = clean_pod_template(
            self.scheduler_pod_template,
            apply_default_affinity=self.apply_default_affinity,
            pod_type="scheduler",
        )

        await ClusterAuth.load_first(self.auth)

        self.core_api = kubernetes.client.CoreV1Api()
        self.policy_api = kubernetes.client.PolicyV1Api()

        if self.namespace is None:
            self.namespace = get_current_namespace()

        environ = {k: v for k, v in os.environ.items() if k not in ["user", "uuid"]}
        self._generate_name = self._generate_name.format(
            user=getpass.getuser(), uuid=str(uuid.uuid4())[:10], **environ
        )
        self._generate_name = escape(self._generate_name)

        self.pod_template = self._fill_pod_templates(
            self.pod_template, pod_type="worker"
        )
        self.scheduler_pod_template = self._fill_pod_templates(
            self.scheduler_pod_template, pod_type="scheduler"
        )

        common_options = {
            "cluster": self,
            "core_api": self.core_api,
            "policy_api": self.policy_api,
            "namespace": self.namespace,
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
                    "service_name_retries": self._scheduler_service_name_resolution_retries,
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

        self.name = self.pod_template.metadata.generate_name

        await super()._start()

        if self._deploy_mode == "local":
            self.forwarded_dashboard_port = self.scheduler.services["dashboard"].port
        else:
            dashboard_address = await get_scheduler_address(
                self.scheduler.service.metadata.name,
                self.namespace,
                port_name="http-dashboard",
            )
            self.forwarded_dashboard_port = dashboard_address.split(":")[-1]

    @classmethod
    def from_dict(cls, pod_spec, **kwargs):
        """Create cluster with worker pod spec defined by Python dictionary

        Deprecated, please use the `KubeCluster` constructor directly.

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
        ...             'image': 'ghcr.io/dask/dask:latest',
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
        warnings.warn(
            "KubeCluster.from_dict is deprecated, use the constructor directly"
        )
        return cls(pod_spec, **kwargs)

    @classmethod
    def from_yaml(cls, yaml_path, **kwargs):
        """Create cluster with worker pod spec defined by a YAML file

        Deprecated, please use the `KubeCluster` constructor directly.

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
              - image: ghcr.io/dask/dask:latest
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
        warnings.warn(
            "KubeCluster.from_yaml is deprecated, use the constructor directly"
        )
        return cls(yaml_path, **kwargs)

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
        """Return logs for the scheduler and workers
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
