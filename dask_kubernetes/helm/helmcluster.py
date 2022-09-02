import asyncio
import aiohttp
import subprocess
import warnings
from contextlib import suppress
import json

from distributed.deploy import Cluster
from distributed.core import rpc, Status
from distributed.utils import Log, Logs
import kubernetes_asyncio as kubernetes

from ..common.auth import ClusterAuth
from ..common.utils import (
    get_current_namespace,
    check_dependency,
)
from ..common.networking import get_external_address_for_scheduler_service


class HelmCluster(Cluster):
    """Connect to a Dask cluster deployed via the Helm Chart.

    This cluster manager connects to an existing Dask deployment that was
    created by the Dask Helm Chart. Enabling you to perform basic cluster actions
    such as scaling and log retrieval.

    Parameters
    ----------
    release_name: str
        Name of the helm release to connect to.
    namespace: str (optional)
        Namespace in which to launch the workers.
        Defaults to current namespace if available or "default"
    port_forward_cluster_ip: bool (optional)
        If the chart uses ClusterIP type services, forward the ports locally.
        If you are using ``HelmCluster`` from the Jupyter session that was installed
        by the helm chart this should be ``False``. If you are running it locally it should
        be the port you are forwarding to ``<port>``.
    auth: List[ClusterAuth] (optional)
        Configuration methods to attempt in order.  Defaults to
        ``[InCluster(), KubeConfig()]``.
    scheduler_name: str (optional)
        Name of the Dask scheduler deployment in the current release.
        Defaults to "scheduler".
    worker_name: str (optional)
        Name of the Dask worker deployment in the current release.
        Defaults to "worker".
    node_host: str (optional)
        A node address. Can be provided in case scheduler service type is
        ``NodePort`` and you want to manually specify which node to connect to.
    node_port: int (optional)
        A node address. Can be provided in case scheduler service type is
        ``NodePort`` and you want to manually specify which port to connect to.
    **kwargs: dict
        Additional keyword arguments to pass to Cluster.

    Examples
    --------
    >>> from dask_kubernetes import HelmCluster
    >>> cluster = HelmCluster(release_name="myhelmrelease")

    You can then resize the cluster with the scale method

    >>> cluster.scale(10)

    You can pass this cluster directly to a Dask client

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    You can also access cluster logs

    >>> cluster.get_logs()

    See Also
    --------
    HelmCluster.scale
    HelmCluster.logs
    """

    def __init__(
        self,
        release_name=None,
        auth=ClusterAuth.DEFAULT,
        namespace=None,
        port_forward_cluster_ip=False,
        scheduler_name="scheduler",
        worker_name="worker",
        node_host=None,
        node_port=None,
        name=None,
        **kwargs,
    ):
        self.release_name = release_name
        self.namespace = namespace or get_current_namespace()
        if name is None:
            name = self.release_name + "." + self.namespace
        check_dependency("helm")
        check_dependency("kubectl")
        status = subprocess.run(
            ["helm", "-n", self.namespace, "status", self.release_name],
            capture_output=True,
            encoding="utf-8",
        )
        if status.returncode != 0:
            raise RuntimeError(f"No such helm release {self.release_name}.")
        self.auth = auth
        self.core_api = None
        self.scheduler_comm = None
        self.port_forward_cluster_ip = port_forward_cluster_ip
        self._supports_scaling = True
        self.scheduler_name = scheduler_name
        self.worker_name = worker_name
        self.node_host = node_host
        self.node_port = node_port

        super().__init__(name=name, **kwargs)
        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def _start(self):
        await ClusterAuth.load_first(self.auth)
        self.core_api = kubernetes.client.CoreV1Api()
        self.apps_api = kubernetes.client.AppsV1Api()
        self.scheduler_comm = rpc(await self._get_scheduler_address())
        await super()._start()

    async def _get_scheduler_address(self):
        # Get the chart name
        chart = subprocess.check_output(
            [
                "helm",
                "-n",
                self.namespace,
                "list",
                "-f",
                self.release_name,
                "--output",
                "json",
            ],
            encoding="utf-8",
        )
        chart = json.loads(chart)[0]["chart"]
        # extract name from {{.Chart.Name }}-{{ .Chart.Version }}
        chart_name = "-".join(chart.split("-")[:-1])
        # Follow the spec in the dask/dask helm chart
        self.chart_name = (
            f"{chart_name}-" if chart_name not in self.release_name else ""
        )

        service_name = f"{self.release_name}-{self.chart_name}{self.scheduler_name}"
        service = await self.core_api.read_namespaced_service(
            service_name, self.namespace
        )
        address = await get_external_address_for_scheduler_service(
            self.core_api, service, port_forward_cluster_ip=self.port_forward_cluster_ip
        )
        if address is None:
            raise RuntimeError("Unable to determine scheduler address.")
        return address

    async def _wait_for_workers(self):
        while True:
            n_workers = len(self.scheduler_info["workers"])
            deployments = await self.apps_api.list_namespaced_deployment(
                namespace=self.namespace
            )
            deployment_replicas = 0
            for deployment in deployments.items:
                if (
                    f"{self.release_name}-{self.chart_name}{self.worker_name}"
                    in deployment.metadata.name
                ):
                    deployment_replicas += deployment.spec.replicas
            if n_workers == deployment_replicas:
                return
            else:
                await asyncio.sleep(0.2)

    def get_logs(self):
        """Get logs for Dask scheduler and workers.

        Examples
        --------
        >>> cluster.get_logs()
        {'testdask-scheduler-5c8ffb6b7b-sjgrg': ...,
        'testdask-worker-64c8b78cc-992z8': ...,
        'testdask-worker-64c8b78cc-hzpdc': ...,
        'testdask-worker-64c8b78cc-wbk4f': ...}

        Each log will be a string of all logs for that container. To view
        it is recommeded that you print each log.

        >>> print(cluster.get_logs()["testdask-scheduler-5c8ffb6b7b-sjgrg"])
        ...
        distributed.scheduler - INFO - -----------------------------------------------
        distributed.scheduler - INFO - Clear task state
        distributed.scheduler - INFO -   Scheduler at:     tcp://10.1.6.131:8786
        distributed.scheduler - INFO -   dashboard at:                     :8787
        ...
        """
        return self.sync(self._get_logs)

    async def _get_logs(self):
        logs = Logs()

        pods = await self.core_api.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"release={self.release_name},app=dask",
        )

        for pod in pods.items:
            if "scheduler" in pod.metadata.name or "worker" in pod.metadata.name:
                try:
                    if pod.status.phase != "Running":
                        raise ValueError(
                            f"Cannot get logs for pod with status {pod.status.phase}.",
                        )
                    log = Log(
                        await self.core_api.read_namespaced_pod_log(
                            pod.metadata.name, pod.metadata.namespace
                        )
                    )
                except (ValueError, kubernetes.client.exceptions.ApiException):
                    log = Log(f"Cannot find logs. Pod is {pod.status.phase}.")
                logs[pod.metadata.name] = log

        return logs

    def __await__(self):
        async def _():
            if self.status == Status.created:
                await self._start()
            elif self.status == Status.running:
                await self._wait_for_workers()
            return self

        return _().__await__()

    def scale(self, n_workers, worker_group=None):
        """Scale cluster to n workers.

        This sets the Dask worker deployment size to the requested number.
        It also allows you to set the worker deployment size of another worker group.
        Workers will not be terminated gracefull so be sure to only scale down
        when all futures have been retrieved by the client and the cluster is idle.

        Examples
        --------

        >>> cluster
        HelmCluster(my-dask.default, 'tcp://localhost:51481', workers=4, threads=241, memory=2.95 TiB)
        >>> cluster.scale(4)
        >>> cluster
        HelmCluster(my-dask.default, 'tcp://localhost:51481', workers=5, threads=321, memory=3.94 TiB)
        >>> cluster.scale(5, worker_group="high-mem-workers")
        >>> cluster
        HelmCluster(my-dask.default, 'tcp://localhost:51481', workers=9, threads=325, memory=3.94 TiB)
        """
        return self.sync(self._scale, n_workers, worker_group=worker_group)

    async def _scale(self, n_workers, worker_group=None):
        deployment = f"{self.release_name}-{self.chart_name}{self.worker_name}"
        if worker_group:
            deployment += f"-{worker_group}"
        try:
            await self.apps_api.patch_namespaced_deployment(
                name=deployment,
                namespace=self.namespace,
                body={
                    "spec": {
                        "replicas": n_workers,
                    }
                },
            )
        except kubernetes.client.exceptions.ApiException as e:
            if worker_group:
                raise ValueError(f"No such worker group {worker_group}") from e
            raise e

    def adapt(self, *args, **kwargs):
        """Turn on adaptivity (Not recommended)."""
        raise NotImplementedError(
            "It is not recommended to run ``HelmCluster`` in adaptive mode. "
            "When scaling down workers the decision on which worker to remove is left to Kubernetes, which "
            "will not necessarily remove the same worker that Dask would choose. This may result in lost futures and "
            "recalculation. It is recommended to manage scaling yourself with the ``HelmCluster.scale`` method."
        )

    async def _adapt(self, *args, **kwargs):
        return super().adapt(*args, **kwargs)

    async def _close(self, *args, **kwargs):
        """Close the cluster."""
        warnings.warn(
            "It is not possible to close a HelmCluster object. \n"
            "Please delete the cluster via the helm CLI: \n\n"
            f"  $ helm delete --namespace {self.namespace} {self.release_name}"
        )

    @classmethod
    def from_name(cls, name):
        release_name, namespace = name.split(".")
        return cls(release_name=release_name, namespace=namespace)


async def discover(
    auth=ClusterAuth.DEFAULT,
    namespace=None,
):
    await ClusterAuth.load_first(auth)
    async with kubernetes.client.api_client.ApiClient() as api:
        core_api = kubernetes.client.CoreV1Api(api)
        namespace = namespace or get_current_namespace()
        try:
            pods = await core_api.list_pod_for_all_namespaces(
                label_selector="app=dask,component=scheduler",
            )
            for pod in pods.items:
                with suppress(KeyError):
                    yield (
                        pod.metadata.labels["release"] + "." + pod.metadata.namespace,
                        HelmCluster,
                    )
        except aiohttp.client_exceptions.ClientConnectorError:
            warnings.warn("Unable to connect to Kubernetes cluster")
