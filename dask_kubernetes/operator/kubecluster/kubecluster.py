from __future__ import annotations

import asyncio
import atexit
import getpass
import logging
import os
import time
import uuid
import warnings
import weakref
from contextlib import suppress
from enum import Enum
from typing import ClassVar, Dict, List, Optional

import dask.config
import httpx
import kr8s
import yaml
from distributed.core import Status, rpc
from distributed.deploy import Cluster
from distributed.utils import Log, Logs, TimeoutError, format_dashboard_link
from kr8s.asyncio.objects import Pod, Service
from rich import box
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from tornado.ioloop import IOLoop

from dask_kubernetes.exceptions import CrashLoopBackOffError, SchedulerStartupError
from dask_kubernetes.operator._objects import (
    DaskAutoscaler,
    DaskCluster,
    DaskWorkerGroup,
)
from dask_kubernetes.operator.networking import (
    get_scheduler_address,
    wait_for_scheduler,
    wait_for_scheduler_comm,
)
from dask_kubernetes.operator.validation import validate_cluster_name

logger = logging.getLogger(__name__)


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
    name: str
        Name given the Dask cluster. Required except when custom_cluster_spec is
        passed, in which case it's ignored in favor of
        custom_cluster_spec["metadata"]["name"].
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
    port_forward_cluster_ip: bool (optional)
        If the chart uses ClusterIP type services, forward the
        ports locally. If you are running it locally it should
        be the port you are forwarding to ``<port>``.
    create_mode: CreateMode (optional)
        How to handle cluster creation if the cluster resource already exists.
        Default behavior is to create a new cluster if one with that name
        doesn't exist, or connect to an existing one if it does.
        You can also set ``CreateMode.CREATE_ONLY`` to raise an exception if a cluster
        with that name already exists. Or ``CreateMode.CONNECT_ONLY`` to raise an exception
        if a cluster with that name doesn't exist.
    shutdown_on_close: bool (optional)
        Whether or not to delete the cluster resource when this object is closed.
        Defaults to ``True`` when creating a cluster and ``False`` when connecting to an existing one.
    idle_timeout: int (optional)
        If set Kubernetes will delete the cluster automatically if the scheduler is idle for longer than
        this timeout in seconds.
    resource_timeout: int (optional)
        Time in seconds to wait for Kubernetes resources to enter their expected state.
        Example: If the ``DaskCluster`` resource that gets created isn't moved into a known ``status.phase``
        by the controller then it is likely the controller isn't running or is malfunctioning and we time
        out and clean up with a useful error.
        Example 2: If the scheduler Pod enters a ``CrashBackoffLoop`` state for longer than this timeout we
        give up with a useful error.
        Defaults to ``60`` seconds.
    scheduler_service_type: str (optional)
        Kubernetes service type to use for the scheduler. Defaults to ``ClusterIP``.
    jupyter: bool (optional)
        Start Jupyter on the scheduler node.
    custom_cluster_spec: str | dict (optional)
        Path to a YAML manifest or a dictionary representation of a ``DaskCluster`` resource object which will be
        used to create the cluster instead of generating one from the other keyword arguments.
    scheduler_forward_port: int (optional)
        The port to use when forwarding the scheduler dashboard. Will utilize a random port by default
    quiet: bool
        If enabled, suppress all printed output.
        Defaults to ``False``.

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
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        image: Optional[str] = None,
        n_workers: Optional[int] = None,
        resources: Optional[Dict[str, str]] = None,
        env: Optional[List[dict] | Dict[str, str]] = None,
        worker_command: Optional[List[str]] = None,
        port_forward_cluster_ip: Optional[bool] = None,
        create_mode: Optional[CreateMode] = None,
        shutdown_on_close: Optional[bool] = None,
        idle_timeout: Optional[int] = None,
        resource_timeout: Optional[int] = None,
        scheduler_service_type: Optional[str] = None,
        custom_cluster_spec: Optional[str | dict] = None,
        scheduler_forward_port: Optional[int] = None,
        jupyter: bool = False,
        loop: Optional[IOLoop] = None,
        asynchronous: bool = False,
        quiet: bool = False,
        **kwargs,
    ):
        name = dask.config.get("kubernetes.name", override_with=name)
        self.namespace = dask.config.get(
            "kubernetes.namespace", override_with=namespace
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
        self.scheduler_forward_port = dask.config.get(
            "kubernetes.scheduler-forward-port", override_with=scheduler_forward_port
        )
        self.jupyter = dask.config.get(
            "kubernetes.scheduler-jupyter", override_with=jupyter
        )
        self.idle_timeout = dask.config.get(
            "kubernetes.idle-timeout", override_with=idle_timeout
        )

        if self._custom_cluster_spec is not None:
            if isinstance(self._custom_cluster_spec, str):
                with open(self._custom_cluster_spec) as f:
                    self._custom_cluster_spec = yaml.safe_load(f.read())
            name = self._custom_cluster_spec["metadata"]["name"]

        if isinstance(self.worker_command, str):
            self.worker_command = self.worker_command.split(" ")

        if self.resources is not None:
            try:
                # Validate `resources` param is a dictionary whose
                # keys must either be 'limits' or 'requests'
                assert isinstance(
                    self.resources, dict
                ), f"resources must be dict type, found {type(resources)}"
                for field in self.resources:
                    if field in ("limits", "requests"):
                        assert isinstance(
                            self.resources[field], dict
                        ), f"key of '{field}' must be dict type"
                    else:
                        raise ValueError(f"resources has unknown field '{field}'")
            except AssertionError as e:
                raise TypeError from e

        name = name.format(
            user=getpass.getuser(), uuid=str(uuid.uuid4())[:10], **os.environ
        )
        validate_cluster_name(name)
        self._instances.add(self)
        self._rich_spinner = Spinner("dots", speed=0.5)
        self._startup_component_status: dict = {}

        super().__init__(
            name=name, loop=loop, asynchronous=asynchronous, quiet=quiet, **kwargs
        )

        # If https://github.com/dask/distributed/pull/7941 is merged we can
        # simplify the next 8 lines to ``if not self.called_from_running_loop:``
        try:
            called_from_running_loop = (
                getattr(loop, "asyncio_loop", None) is asyncio.get_running_loop()
            )
        except RuntimeError:
            called_from_running_loop = asynchronous

        if not called_from_running_loop:
            self._loop_runner.start()
            self.sync(self._start)

    def _log(self, log):
        temp = self.quiet
        self.quiet = True
        super()._log(log)
        self.quiet = temp

    @property
    def dashboard_link(self):
        host = self.scheduler_address.split("://")[1].split("/")[0].split(":")[0]
        return format_dashboard_link(host, self.forwarded_dashboard_port)

    async def _start(self):
        if not self.namespace:
            api = await kr8s.asyncio.api()
            self.namespace = api.namespace
        try:
            watch_component_status_task = asyncio.create_task(
                self._watch_component_status()
            )
            if not self.quiet:
                show_rich_output_task = asyncio.create_task(self._show_rich_output())
            cluster = await DaskCluster(self.name, namespace=self.namespace)
            cluster_exists = await cluster.exists()

            if cluster_exists and self.create_mode == CreateMode.CREATE_ONLY:
                raise ValueError(
                    f"Cluster {self.name} already exists and create mode is '{CreateMode.CREATE_ONLY}'"
                )
            elif cluster_exists:
                self._log("Connecting to existing cluster")
                await self._connect_cluster()
            elif not cluster_exists and self.create_mode == CreateMode.CONNECT_ONLY:
                raise ValueError(
                    f"Cluster {self.name} doesn't already exist and create "
                    f"mode is '{CreateMode.CONNECT_ONLY}'"
                )
            else:
                self._log("Creating cluster")
                await self._create_cluster()

            await super()._start()
            self._log(f"Ready, dashboard available at {self.dashboard_link}")
        finally:
            watch_component_status_task.cancel()
            if not self.quiet:
                show_rich_output_task.cancel()

    def __await__(self):
        async def _():
            if self.status == Status.created:
                await self._start()
            return self

        return _().__await__()

    async def _create_cluster(self):
        if self.shutdown_on_close is None:
            self.shutdown_on_close = True

        if not self._custom_cluster_spec:
            self._log("Generating cluster spec")
            data = make_cluster_spec(
                name=self.name,
                env=self.env,
                resources=self.resources,
                worker_command=self.worker_command,
                n_workers=self.n_workers,
                image=self.image,
                scheduler_service_type=self.scheduler_service_type,
                idle_timeout=self.idle_timeout,
                jupyter=self.jupyter,
            )
        else:
            data = self._custom_cluster_spec
        try:
            self._log("Creating DaskCluster object")
            cluster = await DaskCluster(data, namespace=self.namespace)
            await cluster.create()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise RuntimeError(
                    "Failed to create DaskCluster resource."
                    "Are the Dask Custom Resource Definitions installed? "
                    "https://kubernetes.dask.org/en/latest/operator.html#installing-the-operator"
                ) from e
            else:
                raise e

        try:
            self._log("Waiting for controller to action cluster")
            await self._wait_for_controller()
        except TimeoutError as e:
            await self._close()
            raise e
        try:
            self._log("Waiting for scheduler pod")
            await wait_for_scheduler(
                self.name,
                self.namespace,
                timeout=self._resource_timeout,
            )
        except CrashLoopBackOffError as e:
            scheduler_pod = await Pod.get(
                namespace=self.namespace,
                label_selector=f"dask.org/component=scheduler,dask.org/cluster-name={self.name}",
            )
            await self._close()
            raise SchedulerStartupError(
                "Scheduler failed to start.",
                "Scheduler Pod logs:",
                "\n".join([line async for line in scheduler_pod.logs()]),
            ) from e
        self._log("Waiting for scheduler service")
        await wait_for_service(f"{self.name}-scheduler", self.namespace)
        scheduler_address = await self._get_scheduler_address()
        self._log("Connecting to scheduler")
        await wait_for_scheduler_comm(scheduler_address)
        self.scheduler_comm = rpc(scheduler_address)
        local_port = self.scheduler_forward_port
        if local_port:
            local_port = int(local_port)
        self._log("Getting dashboard URL")
        dashboard_address = await get_scheduler_address(
            f"{self.name}-scheduler",
            self.namespace,
            port_name="http-dashboard",
            port_forward_cluster_ip=self.port_forward_cluster_ip,
            local_port=local_port,
        )
        self.forwarded_dashboard_port = dashboard_address.split(":")[-1]

    async def _connect_cluster(self):
        if self.shutdown_on_close is None:
            self.shutdown_on_close = False
        cluster = await DaskCluster.get(self.name, namespace=self.namespace)
        container_spec = cluster.spec.worker.spec.containers[0]
        self.image = container_spec.image
        self.n_workers = cluster.replicas
        if "resources" in container_spec:
            self.resources = container_spec.resources
        else:
            self.resources = None
        if "env" in container_spec:
            self.env = container_spec.env
        else:
            self.env = {}
        self.jupyter = "--jupyter" in cluster.spec.scheduler.spec.containers[0].args
        service_name = f"{cluster.name}-scheduler"
        self._log("Waiting for scheduler pod")
        await wait_for_scheduler(
            self.name, self.namespace, timeout=self._resource_timeout
        )
        self._log("Waiting for scheduler service")
        await wait_for_service(service_name, self.namespace)
        scheduler_address = await self._get_scheduler_address()
        self._log("Connecting to scheduler")
        await wait_for_scheduler_comm(scheduler_address)
        self.scheduler_comm = rpc(scheduler_address)
        local_port = self.scheduler_forward_port
        if local_port:
            local_port = int(local_port)
        self._log("Getting dashboard URL")
        dashboard_address = await get_scheduler_address(
            service_name,
            self.namespace,
            port_name="http-dashboard",
            port_forward_cluster_ip=self.port_forward_cluster_ip,
            local_port=local_port,
        )
        self.forwarded_dashboard_port = dashboard_address.split(":")[-1]

    async def _get_scheduler_address(self):
        address = await get_scheduler_address(
            f"{self.name}-scheduler",
            self.namespace,
            port_forward_cluster_ip=self.port_forward_cluster_ip,
        )
        return address

    async def _wait_for_controller(self):
        """Wait for the operator to set the status.phase."""
        start = time.time()
        cluster = await DaskCluster.get(self.name, namespace=self.namespace, timeout=30)
        while start + self._resource_timeout > time.time():
            if await cluster.ready():
                return
            await asyncio.sleep(0.25)
        raise TimeoutError(
            f"Dask Cluster resource not actioned after {self._resource_timeout} seconds, is the Dask Operator running?"
        )

    async def _watch_component_status(self):
        while True:
            # Get DaskCluster status
            with suppress(kr8s.NotFoundError):
                cluster = await DaskCluster.get(self.name, namespace=self.namespace)
                if "status" in cluster.raw and "phase" in cluster.status:
                    self._startup_component_status["cluster"] = cluster.status.phase

            # Get Scheduler Pod status
            with suppress(kr8s.NotFoundError):
                scheduler_pod = await Pod.get(
                    namespace=self.namespace,
                    label_selector=f"dask.org/component=scheduler,dask.org/cluster-name={self.name}",
                )

                phase = scheduler_pod.status.phase
                if scheduler_pod.status.phase == "Running":
                    if not await scheduler_pod.ready():
                        phase = "Health Checking"
                    if "container_statuses" in scheduler_pod.status:
                        for container in scheduler_pod.status.container_statuses:
                            if "waiting" in container.state:
                                phase = container.state.waiting.reason

                self._startup_component_status["schedulerpod"] = phase

            # Get Scheduler Service status
            with suppress(kr8s.NotFoundError):
                await Service.get(self.name + "-scheduler", namespace=self.namespace)
                self._startup_component_status["schedulerservice"] = "Created"

            # Get DaskWorkerGroup status
            with suppress(kr8s.NotFoundError):
                await DaskWorkerGroup.get(
                    self.name + "-default", namespace=self.namespace
                )
                self._startup_component_status["workergroup"] = "Created"

            await asyncio.sleep(1)

    async def generate_rich_output(self):
        table = Table(show_header=False, box=box.SIMPLE, expand=True)
        table.add_column("Component")
        table.add_column("Status", justify="right")

        for label, component in [
            ("DaskCluster", "cluster"),
            ("Scheduler Pod", "schedulerpod"),
            ("Scheduler Service", "schedulerservice"),
            ("Default Worker Group", "workergroup"),
        ]:
            try:
                status = self._startup_component_status[component]
            except KeyError:
                status = "-"
            if status in ["Running", "Created"]:
                status = "[green]" + status
            if status in ["Pending", "Health Checking"]:
                status = "[yellow]" + status
            if status in ["CrashLoopBackOff", "Error"]:
                status = "[red]" + status
            table.add_row(label, status)

        if self._cluster_manager_logs:
            self._rich_spinner.update(text=self._cluster_manager_logs[-1][1])
        return Panel(
            Group(table, self._rich_spinner),
            title=f"Creating KubeCluster [magenta]'{self.name}'",
            width=80,
        )

    async def _show_rich_output(self):
        with Live(await self.generate_rich_output(), auto_refresh=False) as live:
            while True:
                await asyncio.sleep(0.25)
                live.update(await self.generate_rich_output(), refresh=True)

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
        logs = Logs()

        pods = await kr8s.asyncio.get(
            "pods",
            namespace=self.namespace,
            label_selector=f"dask.org/cluster-name={self.name}",
        )

        for pod in pods:
            if "scheduler" in pod.name or "worker" in pod.name:
                try:
                    if pod.status.phase != "Running":
                        raise ValueError(
                            f"Cannot get logs for pod with status {pod.status.phase}.",
                        )
                    log = Log("\n".join([line async for line in pod.logs()]))
                except ValueError:
                    log = Log(f"Cannot find logs. Pod is {pod.status.phase}.")
            logs[pod.name] = log

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
                env=env or self.env,
                resources=resources or self.resources,
                worker_command=worker_command or self.worker_command,
                n_workers=n_workers or self.n_workers,
                image=image or self.image,
            )
        wg = await DaskWorkerGroup(
            {
                "apiVersion": "kubernetes.dask.org/v1",
                "kind": "DaskWorkerGroup",
                "metadata": {
                    "name": f"{self.name}-{name}",
                    "namespace": self.namespace,
                },
                "spec": {
                    "cluster": f"{self.name}",
                    "worker": spec,
                },
            }
        )
        await wg.create()

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
        wg = await DaskWorkerGroup(f"{self.name}-{name}", namespace=self.namespace)
        await wg.delete()

    def close(self, timeout=3600):
        """Delete the dask cluster"""
        return self.sync(self._close, timeout=timeout)

    async def _close(self, timeout=3600):
        await super()._close()
        if self.shutdown_on_close:
            try:
                cluster = await DaskCluster.get(self.name, namespace=self.namespace)
                await cluster.delete()
            except kr8s.NotFoundError:
                logger.warning(
                    "Failed to delete DaskCluster, looks like it has already been deleted."
                )
                return
            start = time.time()
            while await cluster.exists():
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
        # Disable adaptivity if enabled
        with suppress(kr8s.NotFoundError):
            autoscaler = await DaskAutoscaler(self.name, self.namespace)
            await autoscaler.delete()

        wg = await DaskWorkerGroup(
            f"{self.name}-{worker_group}", namespace=self.namespace
        )
        await wg.scale(n)
        for instance in self._instances:
            if instance.name == self.name:
                instance.scheduler_info = self.scheduler_info

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
        autoscaler = await DaskAutoscaler(
            {
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
            self.namespace,
        )
        try:
            await autoscaler.patch({"spec": {"minimum": minimum, "maximum": maximum}})
        except kr8s.NotFoundError:
            await autoscaler.create()

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
        defaults = {"create_mode": CreateMode.CONNECT_ONLY, "shutdown_on_close": False}
        kwargs = defaults | kwargs
        return cls(
            name=name,
            **kwargs,
        )

    @property
    def jupyter_link(self):
        if self.jupyter:
            return self.dashboard_link.replace("/status", "/jupyter/lab")
        raise RuntimeError("KubeCluster not started with jupyter enabled")


def make_cluster_spec(
    name,
    image="ghcr.io/dask/dask:latest",
    n_workers=None,
    resources=None,
    env=None,
    worker_command="dask-worker",
    scheduler_service_type="ClusterIP",
    idle_timeout=0,
    jupyter=False,
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
    idle_timeout: int (optional)
        Timeout to cleanup idle cluster
    jupyter: bool (optional)
        Start Jupyter on the Dask scheduler
    """
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {"name": name},
        "spec": {
            "idleTimeout": idle_timeout,
            "worker": make_worker_spec(
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
                jupyter=jupyter,
            ),
        },
    }


def make_worker_spec(
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

    args = worker_command + [
        "--name",
        "$(DASK_WORKER_NAME)",
        "--dashboard",
        "--dashboard-address",
        "8788",
    ]

    return {
        "replicas": n_workers,
        "spec": {
            "containers": [
                {
                    "name": "worker",
                    "image": image,
                    "args": args,
                    "env": env,
                    "resources": resources,
                    "ports": [
                        {
                            "name": "http-dashboard",
                            "containerPort": 8788,
                            "protocol": "TCP",
                        },
                    ],
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
    jupyter=False,
):
    # TODO: Take the values provided in the current class constructor
    # and build a DaskWorker compatible dict
    if isinstance(env, dict):
        env = [{"name": key, "value": value} for key, value in env.items()]
    else:
        # If they gave us a list, assume its a list of dicts and already ready to go
        env = env
    args = ["dask-scheduler", "--host", "0.0.0.0"]
    if jupyter:
        args.append("--jupyter")

    return {
        "spec": {
            "containers": [
                {
                    "name": "scheduler",
                    "image": image,
                    "args": args,
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
                        "initialDelaySeconds": 0,
                        "periodSeconds": 1,
                        "timeoutSeconds": 300,
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


async def wait_for_service(service_name, namespace):
    """Block until service is available."""
    while True:
        with suppress(kr8s.NotFoundError):
            service = await Service.get(service_name, namespace)
            if await service.ready():
                return
        await asyncio.sleep(0.1)


@atexit.register
def reap_clusters():
    async def _reap_clusters():
        for cluster in list(KubeCluster._instances):
            if cluster.shutdown_on_close and cluster.status != Status.closed:
                with suppress(TimeoutError):
                    if cluster.asynchronous:
                        await cluster.close(timeout=10)
                    else:
                        cluster.close(timeout=10)

    asyncio.run(_reap_clusters())
