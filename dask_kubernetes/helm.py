import asyncio
import aiohttp
import shutil
import subprocess
import warnings
from contextlib import suppress

from distributed.deploy import Cluster
from distributed.core import rpc
from distributed.utils import Log, Logs, LoopRunner
import kubernetes_asyncio as kubernetes

from .auth import ClusterAuth
from .core import _namespace_default


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
        be ``True``.
    auth: List[ClusterAuth] (optional)
        Configuration methods to attempt in order.  Defaults to
        ``[InCluster(), KubeConfig()]``.
    scheduler_name: str (optional)
        Name of the Dask scheduler deployment in the current release.
        Defaults to "scheduler".
    worker_name: str (optional)
        Name of the Dask worker deployment in the current release.
        Defaults to "worker".
    **kwargs: dict
        Additional keyword arguments to pass to Cluster

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
        loop=None,
        asynchronous=False,
        scheduler_name="dask-scheduler",
        worker_name="dask-worker",
    ):
        self.release_name = release_name
        self.namespace = namespace or _namespace_default()
        self.name = self.release_name + "." + self.namespace
        self.check_helm_dependency()
        status = subprocess.run(
            ["helm", "-n", self.namespace, "status", self.release_name],
            capture_output=True,
            encoding="utf-8",
        )
        if status.returncode != 0:
            raise RuntimeError(f"No such helm release {self.release_name}.")
        self.auth = auth
        self.namespace
        self.core_api = None
        self.scheduler_comm = None
        self.port_forward_cluster_ip = port_forward_cluster_ip
        self._supports_scaling = True
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop
        self.scheduler_name = scheduler_name
        self.worker_name = worker_name

        super().__init__(asynchronous=asynchronous)
        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    @staticmethod
    def check_helm_dependency():
        if shutil.which("helm") is None:
            raise RuntimeError(
                "Missing dependency helm. "
                "Please install helm following the instructions for your OS. "
                "https://helm.sh/docs/intro/install/"
            )

    async def _start(self):
        await ClusterAuth.load_first(self.auth)
        self.core_api = kubernetes.client.CoreV1Api()
        self.apps_api = kubernetes.client.AppsV1Api()
        self.scheduler_comm = rpc(await self._get_scheduler_address())
        await super()._start()

    async def _get_scheduler_address(self):
        service_name = f"{self.release_name}-{self.scheduler_name}"
        service = await self.core_api.read_namespaced_service(
            service_name, self.namespace
        )
        [port] = [port.port for port in service.spec.ports if port.name == service_name]
        if service.spec.type == "LoadBalancer":
            lb = service.status.load_balancer.ingress[0]
            host = lb.hostname or lb.ip
            return f"tcp://{host}:{port}"
        elif service.spec.type == "NodePort":
            [port] = [
                port.node_port
                for port in service.spec.ports
                if port.name == service_name
            ]
            nodes = await self.core_api.list_node()
            host = nodes.items[0].status.addresses[0].address
            return f"tcp://{host}:{port}"
        elif service.spec.type == "ClusterIP":
            if self.port_forward_cluster_ip:
                warnings.warn(
                    f"""
                    Sorry we do not currently support local port forwarding.

                    Please port-forward the service locally yourself with the following command.

                    kubectl port-forward --namespace {self.namespace} svc/{service_name} {port}:{port} &
                    """
                )  # FIXME Handle this port forward here with the kubernetes library
                return f"tcp://localhost:{port}"
            return f"tcp://{service.spec.cluster_ip}:{port}"
        raise RuntimeError("Unable to determine scheduler address.")

    async def _wait_for_workers(self):
        while True:
            n_workers = len(self.scheduler_info["workers"])
            deployment = await self.apps_api.read_namespaced_deployment(
                name=f"{self.release_name}-{self.worker_name}", namespace=self.namespace
            )
            deployment_replicas = deployment.spec.replicas
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
                logs[pod.metadata.name] = Log(
                    await self.core_api.read_namespaced_pod_log(
                        pod.metadata.name, pod.metadata.namespace
                    )
                )

        return logs

    def __await__(self):
        async def _():
            if self.status == "created":
                await self._start()
            elif self.status == "running":
                await self._wait_for_workers()
            return self

        return _().__await__()

    def scale(self, n_workers):
        """Scale cluster to n workers.

        This sets the Dask worker deployment size to the requested number.
        Workers will not be terminated gracefull so be sure to only scale down
        when all futures have been retrieved by the client and the cluster is idle.

        Examples
        --------

        >>> cluster
        HelmCluster('tcp://localhost:8786', workers=3, threads=18, memory=18.72 GB)
        >>> cluster.scale(4)
        >>> cluster
        HelmCluster('tcp://localhost:8786', workers=4, threads=24, memory=24.96 GB)

        """
        return self.sync(self._scale, n_workers)

    async def _scale(self, n_workers):
        await self.apps_api.patch_namespaced_deployment(
            name=f"{self.release_name}-{self.worker_name}",
            namespace=self.namespace,
            body={
                "spec": {
                    "replicas": n_workers,
                }
            },
        )

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

    def close(self, *args, **kwargs):
        """Close the cluster."""
        raise NotImplementedError(
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
        namespace = namespace or _namespace_default()
        try:
            pods = await core_api.list_pod_for_all_namespaces(
                label_selector=f"app=dask,component=scheduler",
            )
            for pod in pods.items:
                with suppress(KeyError):
                    yield (
                        pod.metadata.labels["release"] + "." + pod.metadata.namespace,
                        HelmCluster,
                    )
        except aiohttp.client_exceptions.ClientConnectorError:
            warnings.warn("Unable to connect to Kubernetes cluster")
