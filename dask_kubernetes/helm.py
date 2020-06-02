import asyncio
import shutil
import subprocess
import warnings

from distributed.deploy import Cluster
from distributed.core import rpc
from distributed.utils import Log, Logs, LoopRunner
import kubernetes_asyncio as kubernetes

from .auth import ClusterAuth
from .core import _namespace_default

if shutil.which("helm") is None:
    raise ImportError(
        "Missing dependency helm. "
        "Please install helm following the instructions for your OS. "
        "https://helm.sh/docs/intro/install/"
    )


class HelmCluster(Cluster):
    """ Connect to a Dask cluster deployed via the Helm Chart.

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
    auth: List[ClusterAuth] (optional)
        Configuration methods to attempt in order.  Defaults to
        ``[InCluster(), KubeConfig()]``.
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
        loop=None,
        asynchronous=False,
    ):
        self.release_name = release_name
        self.namespace = namespace or _namespace_default()
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
        self._supports_scaling = False
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        super().__init__(asynchronous=asynchronous)
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
        service_name = f"{self.release_name}-scheduler"
        service = await self.core_api.read_namespaced_service(
            service_name, self.namespace
        )
        [port] = [port.port for port in service.spec.ports if port.name == service_name]
        if service.spec.type == "LoadBalancer":
            lb = service.status.load_balancer.ingress[0]
            host = lb.hostname or lb.ip
            return f"tcp://{host}:{port}"
        elif service.spec.type == "NodePort":
            nodes = await self.core_api.list_node()
            host = nodes.items[0].status.addresses[0].address
            return f"tcp://{host}:{port}"
        elif service.spec.type == "ClusterIP":
            warnings.warn(
                f"""
                Your Dask cluster has not been exposed outside of your Kubernetes cluster.
                Please port-forward the service locally if you haven't already.

                kubectl port-forward --namespace {self.namespace} svc/{service_name} {port}:{port} &
                """
            )  # FIXME Handle this port forward here with the kubernetes library
            return f"tcp://localhost:{port}"
        raise RuntimeError("Unable to determine scheduler address.")

    async def _wait_for_workers(self):
        while True:
            n_workers = len(self.scheduler_info["workers"])
            deployment = await self.apps_api.read_namespaced_deployment(
                name=f"{self.release_name}-worker", namespace=self.namespace
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
            name=f"{self.release_name}-worker",
            namespace=self.namespace,
            body={"spec": {"replicas": n_workers,}},
        )

    def adapt(self, *args, **kwargs):
        """Turn on adaptivity (Not recommended).

        While it is possible it is not recommended to run ``HelmCluster`` in adaptive mode.
        When scaling down workers the decision on which worker to remove is left to Kubernetes, which
        will not necessarily remove the same worker that Dask would choose. This may result in lost futures and
        recalculation. It is recommended to manage scaling yourself with the ``HelmCluster.scale`` method.

        """
        warnings.warn(
            "\n".join(self.adapt.__doc__.split("\n")[2:]), UserWarning,
        )  # Show a warning containing the body of the docstring
        return self.sync(self._adapt, *args, **kwargs)

    async def _adapt(self, *args, **kwargs):
        return super().adapt(*args, **kwargs)
