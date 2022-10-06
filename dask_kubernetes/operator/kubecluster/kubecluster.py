from __future__ import annotations

import asyncio
import atexit
from contextlib import suppress
from enum import Enum
import getpass
import os
import time
from typing import ClassVar
import warnings
import weakref
import uuid

import kubernetes_asyncio as kubernetes
import yaml

import dask.config
from distributed.core import Status, rpc
from distributed.deploy import Cluster
from distributed.utils import (
    Log,
    Logs,
    TimeoutError,
    format_dashboard_link,
)

from dask_kubernetes.common.auth import ClusterAuth
from dask_kubernetes.operator.controller import (
    wait_for_service,
)

from dask_kubernetes.common.networking import (
    get_scheduler_address,
    wait_for_scheduler,
    wait_for_scheduler_comm,
)
from dask_kubernetes.common.utils import get_current_namespace


class CreateMode(Enum):
    CREATE_ONLY = "CREATE_ONLY"
    CREATE_OR_CONNECT = "CREATE_OR_CONNECT"
    CONNECT_ONLY = "CONNECT_ONLY"


class KubeCluster(Cluster):
    """Launch a Dask Cluster on Kubernetes using the Operator

    This cluster manager creates a Dask cluster by deploying
    the necessary kubernetes resources the Dask Operator needs
    to create pods. It can also connect to an existing cluster
    by providing the name of the cluster.

    Parameters
    ----------
    name: str (required)
        Name given the Dask cluster.
    namespace: str (optional)
        Namespace in which to launch the workers.
        Defaults to current namespace if available or "default"
    image: str (optional)
        Image to run in Scheduler and Worker Pods.
    n_workers: int
        Number of workers on initial launch.
        Use ``scale`` to change this number in the future
    resources: Dict[str, str]
    env: List[dict] | Dict[str, str]
        List of environment variables to pass to worker pod.
        Can be a list of dicts using the same structure as k8s envs
        or a single dictionary of key/value pairs
    worker_command: List[str] | str
        The command to use when starting the worker.
        If command consists of multiple words it should be passed as a list of strings.
        Defaults to ``"dask-worker"``.
    auth: List[ClusterAuth] (optional)
        Configuration methods to attempt in order.  Defaults to
        ``[InCluster(), KubeConfig()]``.
    port_forward_cluster_ip: bool (optional)
        If the chart uses ClusterIP type services, forward the
        ports locally. If you are running it locally it should
        be the port you are forwarding to ``<port>``.
    create_mode: CreateMode (optional)
        How to handle cluster creation if the cluster resource already exists.
        Default behaviour is to create a new clustser if one with that name
        doesn't exist, or connect to an existing one if it does.
        You can also set ``CreateMode.CREATE_ONLY`` to raise an exception if a cluster
        with that name already exists. Or ``CreateMode.CONNECT_ONLY`` to raise an exception
        if a cluster with that name doesn't exist.
    shutdown_on_close: bool (optional)
        Whether or not to delete the cluster resource when this object is closed.
        Defaults to ``True`` when creating a cluster and ``False`` when connecting to an existing one.
    resource_timeout: int (optional)
        Time in seconds to wait for the controller to take action before giving up.
        If the ``DaskCluster`` resource that gets created isn't moved into a known ``status.phase`` by the controller
        then it is likely the controller isn't running or is malfunctioning and we time out and clean up with a
        useful error.
        Defaults to ``60`` seconds.
    scheduler_service_type: str (optional)
        Kubernetes service type to use for the scheduler. Defaults to ``ClusterIP``.
    custom_cluster_spec: str | dict (optional)
        Path to a YAML manifest or a dictionary representation of a ``DaskCluster`` resource object which will be
        used to create the cluster instead of generating one from the other keyword arguments.
    **kwargs: dict
        Additional keyword arguments to pass to LocalCluster

    Examples
    --------
    >>> from dask_kubernetes.operator import KubeCluster
    >>> cluster = KubeCluster(name="foo")

    You can add another group of workers (default is 3 workers)
    >>> cluster.add_worker_group('additional', n=4)

    You can then resize the cluster with the scale method
    >>> cluster.scale(10)

    And optionally scale a specific worker group
    >>> cluster.scale(10, worker_group='additional')

    You can also resize the cluster adaptively and give
    it a range of workers
    >>> cluster.adapt(20, 50)

    You can pass this cluster directly to a Dask client
    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    You can also access cluster logs
    >>> cluster.get_logs()

    You can also connect to an existing cluster
    >>> existing_cluster = KubeCluster.from_name(name="ialreadyexist")

    See Also
    --------
    KubeCluster.from_name
    """

    _instances: ClassVar[weakref.WeakSet[KubeCluster]] = weakref.WeakSet()

    def __init__(
        self,
        *,
        name=None,
        namespace=None,
        image=None,
        n_workers=None,
        resources=None,
        env=None,
        worker_command=None,
        auth=ClusterAuth.DEFAULT,
        port_forward_cluster_ip=None,
        create_mode=None,
        shutdown_on_close=None,
        resource_timeout=None,
        scheduler_service_type=None,
        custom_cluster_spec=None,
        **kwargs,
    ):

        name = dask.config.get("kubernetes.name", override_with=name)
        self.namespace = (
            dask.config.get("kubernetes.namespace", override_with=namespace)
            or get_current_namespace()
        )
        self.image = dask.config.get("kubernetes.image", override_with=image)
        self.n_workers = dask.config.get(
            "kubernetes.count.start", override_with=n_workers
        )
        if dask.config.get("kubernetes.count.max"):
            warnings.warn(
                "Setting a maximum number of workers is no longer supported. "
                "Please use Kubernetes Resource Quotas instead."
            )
        self.resources = dask.config.get(
            "kubernetes.resources", override_with=resources
        )
        self.env = dask.config.get("kubernetes.env", override_with=env)
        self.worker_command = dask.config.get(
            "kubernetes.worker-command", override_with=worker_command
        )
        self.auth = auth
        self.port_forward_cluster_ip = dask.config.get(
            "kubernetes.port-forward-cluster-ip", override_with=port_forward_cluster_ip
        )
        self.create_mode = dask.config.get(
            "kubernetes.create-mode", override_with=create_mode
        )
        self.shutdown_on_close = dask.config.get(
            "kubernetes.shutdown-on-close", override_with=shutdown_on_close
        )
        self._resource_timeout = dask.config.get(
            "kubernetes.resource-timeout", override_with=resource_timeout
        )
        self._custom_cluster_spec = dask.config.get(
            "kubernetes.custom-cluster-spec", override_with=custom_cluster_spec
        )
        self.scheduler_service_type = dask.config.get(
            "kubernetes.scheduler-service-type", override_with=scheduler_service_type
        )

        if self._custom_cluster_spec:
            if isinstance(self._custom_cluster_spec, str):
                with open(self._custom_cluster_spec) as f:
                    self._custom_cluster_spec = yaml.safe_load(f.read())
            name = self._custom_cluster_spec["metadata"]["name"]

        if isinstance(self.worker_command, str):
            self.worker_command = self.worker_command.split(" ")

        name = name.format(
            user=getpass.getuser(), uuid=str(uuid.uuid4())[:10], **os.environ
        )

        self._instances.add(self)

        super().__init__(name=name, **kwargs)
        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    @property
    def dashboard_link(self):
        host = self.scheduler_address.split("://")[1].split("/")[0].split(":")[0]
        return format_dashboard_link(host, self.forwarded_dashboard_port)

    async def _start(self):
        await ClusterAuth.load_first(self.auth)
        cluster_exists = (await self._get_cluster()) is not None

        if cluster_exists and self.create_mode == CreateMode.CREATE_ONLY:
            raise ValueError(
                f"Cluster {self.name} already exists and create mode is '{CreateMode.CREATE_ONLY}'"
            )
        elif cluster_exists:
            await self._connect_cluster()
        elif not cluster_exists and self.create_mode == CreateMode.CONNECT_ONLY:
            raise ValueError(
                f"Cluster {self.name} doesn't and create mode is '{CreateMode.CONNECT_ONLY}'"
            )
        else:
            await self._create_cluster()

        await super()._start()

    async def _create_cluster(self):
        if self.shutdown_on_close is None:
            self.shutdown_on_close = True
        async with kubernetes.client.api_client.ApiClient() as api_client:
            core_api = kubernetes.client.CoreV1Api(api_client)
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)

            if not self._custom_cluster_spec:
                data = make_cluster_spec(
                    name=self.name,
                    env=self.env,
                    resources=self.resources,
                    worker_command=self.worker_command,
                    n_workers=self.n_workers,
                    image=self.image,
                    scheduler_service_type=self.scheduler_service_type,
                )
            else:
                data = self._custom_cluster_spec
            try:
                await custom_objects_api.create_namespaced_custom_object(
                    group="kubernetes.dask.org",
                    version="v1",
                    plural="daskclusters",
                    namespace=self.namespace,
                    body=data,
                )
            except kubernetes.client.ApiException as e:
                raise RuntimeError(
                    "Failed to create DaskCluster resource. "
                    "Are the Dask Custom Resource Definitions installed? "
                    "https://kubernetes.dask.org/en/latest/operator.html#installing-the-operator"
                ) from e

            try:
                await self._wait_for_controller()
            except TimeoutError as e:
                await self._close()
                raise e
            await wait_for_scheduler(self.name, self.namespace)
            await wait_for_service(core_api, f"{self.name}-scheduler", self.namespace)
            scheduler_address = await self._get_scheduler_address()
            await wait_for_scheduler_comm(scheduler_address)
            self.scheduler_comm = rpc(scheduler_address)
            dashboard_address = await get_scheduler_address(
                f"{self.name}-scheduler",
                self.namespace,
                port_name="http-dashboard",
            )
            self.forwarded_dashboard_port = dashboard_address.split(":")[-1]

    async def _connect_cluster(self):
        if self.shutdown_on_close is None:
            self.shutdown_on_close = False
        async with kubernetes.client.api_client.ApiClient() as api_client:
            core_api = kubernetes.client.CoreV1Api(api_client)
            cluster_spec = await self._get_cluster()
            container_spec = cluster_spec["spec"]["worker"]["spec"]["containers"][0]
            self.image = container_spec["image"]
            self.n_workers = cluster_spec["spec"]["worker"]["replicas"]
            if "resources" in container_spec:
                self.resources = container_spec["resources"]
            else:
                self.resources = None
            if "env" in container_spec:
                self.env = container_spec["env"]
            else:
                self.env = {}
            service_name = f"{cluster_spec['metadata']['name']}-scheduler"
            await wait_for_scheduler(self.name, self.namespace)
            await wait_for_service(core_api, service_name, self.namespace)
            scheduler_address = await self._get_scheduler_address()
            await wait_for_scheduler_comm(scheduler_address)
            self.scheduler_comm = rpc(scheduler_address)
            dashboard_address = await get_scheduler_address(
                service_name,
                self.namespace,
                port_name="http-dashboard",
            )
            self.forwarded_dashboard_port = dashboard_address.split(":")[-1]

    async def _get_cluster(self):
        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            try:
                return await custom_objects_api.get_namespaced_custom_object(
                    group="kubernetes.dask.org",
                    version="v1",
                    plural="daskclusters",
                    namespace=self.namespace,
                    name=self.name,
                )
            except kubernetes.client.exceptions.ApiException as e:
                return None

    async def _get_scheduler_address(self):
        address = await get_scheduler_address(f"{self.name}-scheduler", self.namespace)
        return address

    async def _wait_for_controller(self):
        """Wait for the operator to set the status.phase."""
        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            watch = kubernetes.watch.Watch()
            async for event in watch.stream(
                func=custom_objects_api.list_namespaced_custom_object,
                group="kubernetes.dask.org",
                version="v1",
                plural="daskclusters",
                namespace=self.namespace,
                field_selector=f"metadata.name={self.name}",
                timeout_seconds=self._resource_timeout,
            ):
                cluster = event["object"]
                if "status" in cluster and "phase" in cluster["status"]:
                    return
                await asyncio.sleep(0.1)
        raise TimeoutError(
            f"Dask Cluster resource not actioned after {self._resource_timeout} seconds, is the Dask Operator running?"
        )

    def get_logs(self):
        """Get logs for Dask scheduler and workers.

        Examples
        --------
        >>> cluster.get_logs()
        {'foo': ...,
        'foo-default-worker-0269dbfa0cfd4a22bcd9d92ae032f4d2': ...,
        'foo-default-worker-7c1ccb04cd0e498fb21babaedd00e5d4': ...,
        'foo-default-worker-d65bee23bdae423b8d40c5da7a1065b6': ...}
        Each log will be a string of all logs for that container. To view
        it is recommeded that you print each log.
        >>> print(cluster.get_logs()["testdask-scheduler-5c8ffb6b7b-sjgrg"])
        ...
        distributed.scheduler - INFO - -----------------------------------------------
        distributed.scheduler - INFO - Clear task state
        distributed.scheduler - INFO -   Scheduler at:   tcp://10.244.0.222:8786
        distributed.scheduler - INFO -   dashboard at:                     :8787
        ...
        """
        return self.sync(self._get_logs)

    async def _get_logs(self):
        async with kubernetes.client.api_client.ApiClient() as api_client:
            core_api = kubernetes.client.CoreV1Api(api_client)
            logs = Logs()

            pods = await core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"dask.org/cluster-name={self.name}",
            )

            for pod in pods.items:
                if "scheduler" in pod.metadata.name or "worker" in pod.metadata.name:
                    try:
                        if pod.status.phase != "Running":
                            raise ValueError(
                                f"Cannot get logs for pod with status {pod.status.phase}.",
                            )
                        log = Log(
                            await core_api.read_namespaced_pod_log(
                                pod.metadata.name, pod.metadata.namespace
                            )
                        )
                    except (ValueError, kubernetes.client.exceptions.ApiException):
                        log = Log(f"Cannot find logs. Pod is {pod.status.phase}.")
                logs[pod.metadata.name] = log

        return logs

    def add_worker_group(
        self,
        name,
        n_workers=3,
        image=None,
        resources=None,
        worker_command=None,
        env=None,
        custom_spec=None,
    ):
        """Create a dask worker group by name

        Parameters
        ----------
        name: str
            Name of the worker group
        n_workers: int
            Number of workers on initial launch.
            Use ``.scale(n_workers, worker_group=name)`` to change this number in the future.
        image: str (optional)
            Image to run in Scheduler and Worker Pods.
            If ommitted will use the cluster default.
        resources: Dict[str, str]
            Resources to be passed to the underlying pods.
            If ommitted will use the cluster default.
        env: List[dict]
            List of environment variables to pass to worker pod.
            If ommitted will use the cluster default.
        custom_spec: dict (optional)
            A dictionary representation of a worker spec which will be used to create the ``DaskWorkerGroup`` instead
            of generating one from the other keyword arguments.

        Examples
        --------
        >>> cluster.add_worker_group("high-mem-workers", n_workers=5)
        """
        return self.sync(
            self._add_worker_group,
            name=name,
            n_workers=n_workers,
            image=image,
            resources=resources,
            worker_command=worker_command,
            env=env,
            custom_spec=custom_spec,
        )

    async def _add_worker_group(
        self,
        name,
        n_workers=3,
        image=None,
        resources=None,
        worker_command=None,
        env=None,
        custom_spec=None,
    ):
        if custom_spec is not None:
            spec = custom_spec
        else:
            spec = make_worker_spec(
                cluster_name=self.name,
                env=env or self.env,
                resources=resources or self.resources,
                worker_command=worker_command or self.worker_command,
                n_workers=n_workers or self.n_workers,
                image=image or self.image,
            )
        data = {
            "apiVersion": "kubernetes.dask.org/v1",
            "kind": "DaskWorkerGroup",
            "metadata": {"name": f"{self.name}-{name}"},
            "spec": {
                "cluster": f"{self.name}",
                "worker": spec,
            },
        }

        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            await custom_objects_api.create_namespaced_custom_object(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskworkergroups",
                namespace=self.namespace,
                body=data,
            )

    def delete_worker_group(self, name):
        """Delete a dask worker group by name

        Parameters
        ----------
        name: str
            Name of the worker group

        Examples
        --------
        >>> cluster.delete_worker_group("high-mem-workers")
        """
        return self.sync(self._delete_worker_group, name)

    async def _delete_worker_group(self, name):
        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            await custom_objects_api.delete_namespaced_custom_object(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskworkergroups",
                namespace=self.namespace,
                name=f"{self.name}-{name}",
            )

    def close(self, timeout=3600):
        """Delete the dask cluster"""
        return self.sync(self._close, timeout=timeout)

    async def _close(self, timeout=None):
        await super()._close()
        if self.shutdown_on_close:
            async with kubernetes.client.api_client.ApiClient() as api_client:
                custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
                await custom_objects_api.delete_namespaced_custom_object(
                    group="kubernetes.dask.org",
                    version="v1",
                    plural="daskclusters",
                    namespace=self.namespace,
                    name=self.name,
                )
            start = time.time()
            while (await self._get_cluster()) is not None:
                if time.time() > start + timeout:
                    raise TimeoutError(
                        f"Timed out deleting cluster resource {self.name}"
                    )
                await asyncio.sleep(1)

    def scale(self, n, worker_group="default"):
        """Scale cluster to n workers

        Parameters
        ----------
        n : int
            Target number of workers
        worker_group : str
            Worker group to scale

        Examples
        --------
        >>> cluster.scale(10)  # scale cluster to ten workers
        >>> cluster.scale(7, worker_group="high-mem-workers") # scale worker group high-mem-workers to seven workers
        """

        return self.sync(self._scale, n, worker_group)

    async def _scale(self, n, worker_group="default"):
        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            custom_objects_api.api_client.set_default_header(
                "content-type", "application/merge-patch+json"
            )
            # Disable adaptivity if enabled
            with suppress(kubernetes.client.ApiException):
                await custom_objects_api.delete_namespaced_custom_object(
                    group="kubernetes.dask.org",
                    version="v1",
                    plural="daskautoscalers",
                    namespace=self.namespace,
                    name=self.name,
                )
            await custom_objects_api.patch_namespaced_custom_object_scale(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskworkergroups",
                namespace=self.namespace,
                name=f"{self.name}-{worker_group}",
                body={"spec": {"replicas": n}},
            )

    def adapt(self, minimum=None, maximum=None):
        """Turn on adaptivity

        Parameters
        ----------
        minimum : int
            Minimum number of workers
        minimum : int
            Maximum number of workers

        Examples
        --------
        >>> cluster.adapt()  # Allow scheduler to add/remove workers within k8s cluster resource limits
        >>> cluster.adapt(minimum=1, maximum=10) # Allow scheduler to add/remove workers within 1-10 range
        """
        return self.sync(self._adapt, minimum, maximum)

    async def _adapt(self, minimum=None, maximum=None):
        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            custom_objects_api.api_client.set_default_header(
                "content-type", "application/merge-patch+json"
            )
            try:
                await custom_objects_api.patch_namespaced_custom_object_scale(
                    group="kubernetes.dask.org",
                    version="v1",
                    plural="daskautoscalers",
                    namespace=self.namespace,
                    name=self.name,
                    body={"spec": {"minimum": minimum, "maximum": maximum}},
                )
            except kubernetes.client.ApiException:
                await custom_objects_api.create_namespaced_custom_object(
                    group="kubernetes.dask.org",
                    version="v1",
                    plural="daskautoscalers",
                    namespace=self.namespace,
                    body={
                        "apiVersion": "kubernetes.dask.org/v1",
                        "kind": "DaskAutoscaler",
                        "metadata": {
                            "name": self.name,
                            "dask.org/cluster-name": self.name,
                            "dask.org/component": "autoscaler",
                        },
                        "spec": {
                            "cluster": self.name,
                            "minimum": minimum,
                            "maximum": maximum,
                        },
                    },
                )

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.close()

    @classmethod
    def from_name(cls, name, **kwargs):
        """Create an instance of this class to represent an existing cluster by name.

        Will fail if a cluster with that name doesn't already exist.

        Parameters
        ----------
        name: str
            Name of the cluster to connect to

        Examples
        --------
        >>> cluster = KubeCluster.from_name(name="simple-cluster")
        """
        return cls(name=name, create_mode=CreateMode.CONNECT_ONLY, **kwargs)


def make_cluster_spec(
    name,
    image="ghcr.io/dask/dask:latest",
    n_workers=None,
    resources=None,
    env=None,
    worker_command="dask-worker",
    scheduler_service_type="ClusterIP",
):
    """Generate a ``DaskCluster`` kubernetes resource.

    Populate a template with some common options to generate a ``DaskCluster`` kubernetes resource.

    Parameters
    ----------
    name: str
        Name of the cluster
    image: str (optional)
        Container image to use for the scheduler and workers
    n_workers: int (optional)
        Number of workers in the default worker group
    resources: dict (optional)
        Resource limits to set on scheduler and workers
    env: dict (optional)
        Environment variables to set on scheduler and workers
    worker_command: str (optional)
        Worker command to use when starting the workers
    """
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {"name": name},
        "spec": {
            "worker": make_worker_spec(
                cluster_name=name,
                env=env,
                resources=resources,
                worker_command=worker_command,
                n_workers=n_workers,
                image=image,
            ),
            "scheduler": make_scheduler_spec(
                cluster_name=name,
                env=env,
                resources=resources,
                image=image,
                scheduler_service_type=scheduler_service_type,
            ),
        },
    }


def make_worker_spec(
    cluster_name,
    image="ghcr.io/dask/dask:latest",
    n_workers=3,
    resources=None,
    env=None,
    worker_command="dask-worker",
):
    if isinstance(env, dict):
        env = [{"name": key, "value": value} for key, value in env.items()]
    else:
        # If they gave us a list, assume its a list of dicts and already ready to go
        env = env

    if isinstance(worker_command, str):
        worker_command = worker_command.split(" ")

    args = worker_command + ["--name", "$(DASK_WORKER_NAME)"]

    return {
        "cluster": cluster_name,
        "replicas": n_workers,
        "spec": {
            "containers": [
                {
                    "name": "worker",
                    "image": image,
                    "args": args,
                    "env": env,
                    "resources": resources,
                }
            ]
        },
    }


def make_scheduler_spec(
    cluster_name,
    env=None,
    resources=None,
    image="ghcr.io/dask/dask:latest",
    scheduler_service_type="ClusterIP",
):
    # TODO: Take the values provided in the current class constructor
    # and build a DaskWorker compatible dict
    if isinstance(env, dict):
        env = [{"name": key, "value": value} for key, value in env.items()]
    else:
        # If they gave us a list, assume its a list of dicts and already ready to go
        env = env

    return {
        "spec": {
            "containers": [
                {
                    "name": "scheduler",
                    "image": image,
                    "args": ["dask-scheduler", "--host", "0.0.0.0"],
                    "env": env,
                    "resources": resources,
                    "ports": [
                        {
                            "name": "tcp-comm",
                            "containerPort": 8786,
                            "protocol": "TCP",
                        },
                        {
                            "name": "http-dashboard",
                            "containerPort": 8787,
                            "protocol": "TCP",
                        },
                    ],
                    "readinessProbe": {
                        "httpGet": {"port": "http-dashboard", "path": "/health"},
                        "initialDelaySeconds": 5,
                        "periodSeconds": 10,
                    },
                    "livenessProbe": {
                        "httpGet": {"port": "http-dashboard", "path": "/health"},
                        "initialDelaySeconds": 15,
                        "periodSeconds": 20,
                    },
                }
            ]
        },
        "service": {
            "type": scheduler_service_type,
            "selector": {
                "dask.org/cluster-name": cluster_name,
                "dask.org/component": "scheduler",
            },
            "ports": [
                {
                    "name": "tcp-comm",
                    "protocol": "TCP",
                    "port": 8786,
                    "targetPort": "tcp-comm",
                },
                {
                    "name": "http-dashboard",
                    "protocol": "TCP",
                    "port": 8787,
                    "targetPort": "http-dashboard",
                },
            ],
        },
    }


@atexit.register
def reap_clusters():
    async def _reap_clusters():
        for cluster in list(KubeCluster._instances):
            if cluster.shutdown_on_close and cluster.status != Status.closed:
                await ClusterAuth.load_first(cluster.auth)
                with suppress(TimeoutError):
                    if cluster.asynchronous:
                        await cluster.close(timeout=10)
                    else:
                        cluster.close(timeout=10)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(_reap_clusters())
