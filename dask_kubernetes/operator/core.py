import subprocess
import json
import tempfile
import kubernetes_asyncio as kubernetes

from distributed.core import rpc
from distributed.deploy import Cluster

from distributed.utils import Log, Logs, LoopRunner

from dask_kubernetes.auth import ClusterAuth
from .daskcluster import (
    build_cluster_spec,
    build_worker_group_spec,
    wait_for_scheduler,
)

from dask_kubernetes.utils import (
    get_external_address_for_scheduler_service,
    check_dependency,
)


class KubeCluster2(Cluster):
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
    env: Dict[str, str]
        Dictionary of environment variables to pass to worker pod
    auth: List[ClusterAuth] (optional)
        Configuration methods to attempt in order.  Defaults to
        ``[InCluster(), KubeConfig()]``.
    port_forward_cluster_ip: bool (optional)
        If the chart uses ClusterIP type services, forward the
        ports locally. If you are running it locally it should
        be the port you are forwarding to ``<port>``.
    **kwargs: dict
        Additional keyword arguments to pass to LocalCluster

    Examples
    --------
    >>> from dask_kubernetes import KubeCluster2
    >>> cluster = KubeCluster2(name="foo")
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
    See Also
    --------
    KubeCluster2.from_name
    """

    def __init__(
        self,
        name,
        namespace="default",
        image="daskdev/dask:latest",
        n_workers=3,
        resources={},
        env={},
        loop=None,
        asynchronous=False,
        auth=ClusterAuth.DEFAULT,
        port_forward_cluster_ip=None,
        **kwargs,
    ):
        self.name = name
        # TODO: Set namespace to None and get default namespace from user's context
        self.namespace = namespace
        self.core_api = None
        self.custom_api = None
        self.image = image
        self.n_workers = n_workers
        self.resources = resources
        self.env = env
        self.auth = auth
        self.port_forward_cluster_ip = port_forward_cluster_ip
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop
        self.worker_groups = ["default-worker-group"]
        check_dependency("kubectl")

        # TODO: Check if cluster already exists
        super().__init__(asynchronous=asynchronous, **kwargs)
        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def _start(self):
        await ClusterAuth.load_first(self.auth)
        await kubernetes.config.load_kube_config()
        async with kubernetes.client.api_client.ApiClient() as api_client:
            self.core_api = kubernetes.client.CoreV1Api(api_client)
            data = build_cluster_spec(
                self.name, self.image, self.n_workers, self.resources, self.env
            )
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            config_path = temp_file.name
            with open(config_path, "w") as f:
                json.dump(data, f)
            cluster = subprocess.check_output(
                [
                    "kubectl",
                    "apply",
                    "-f",
                    temp_file.name,
                    "-n",
                    self.namespace,
                ],
                encoding="utf-8",
            )
            await wait_for_scheduler(f"{self.name}-cluster", self.namespace)
            self.scheduler_comm = rpc(await self._get_scheduler_address())
            await super()._start()

    async def _get_scheduler_address(self):
        service_name = f"{self.name}-cluster"
        service = await self.core_api.read_namespaced_service(
            service_name, self.namespace
        )
        address = await get_external_address_for_scheduler_service(
            self.core_api, service, port_forward_cluster_ip=self.port_forward_cluster_ip
        )
        if address is None:
            raise RuntimeError("Unable to determine scheduler address.")
        return address

    def get_logs(self):
        """Get logs for Dask scheduler and workers.

        Examples
        --------
        >>> cluster.get_logs()
        {'foo-cluster-scheduler': ...,
        'foo-cluster-default-worker-group-worker-0269dbfa0cfd4a22bcd9d92ae032f4d2': ...,
        'foo-cluster-default-worker-group-worker-7c1ccb04cd0e498fb21babaedd00e5d4': ...,
        'foo-cluster-default-worker-group-worker-d65bee23bdae423b8d40c5da7a1065b6': ...}
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

        pods = await self.core_api.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"dask.org/cluster-name={self.name}-cluster",
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

    def add_worker_group(self, name, n=3):
        data = build_worker_group_spec(name, self.image, n, self.resources, self.env)
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        config_path = temp_file.name
        with open(config_path, "w") as f:
            json.dump(data, f)
        workers = subprocess.check_output(
            [
                "kubectl",
                "apply",
                "-f",
                temp_file.name,
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )
        self.worker_groups.append(data["metadata"]["name"])

    def delete_worker_group(self, name):
        subprocess.check_output(
            [
                "kubectl",
                "delete",
                "daskworkergroup",
                name,
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )

    def close(self):
        super().close()
        patch = {"metadata": {"finalizers": []}}
        json_patch = json.dumps(patch)
        subprocess.check_output(
            [
                "kubectl",
                "patch",
                "daskcluster",
                f"{self.name}-cluster",
                "--patch",
                str(json_patch),
                "--type=merge",
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )
        subprocess.check_output(
            [
                "kubectl",
                "delete",
                "daskcluster",
                f"{self.name}-cluster",
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )
        # TODO: Remove these lines when kopf adoptons work
        for name in self.worker_groups:
            if name != "default-worker-group":
                self.delete_worker_group(name)

    def scale(self, n, worker_group="default"):
        scaler = subprocess.check_output(
            [
                "kubectl",
                "scale",
                f"--replicas={n}",
                "daskworkergroup",
                f"{worker_group}-worker-group",
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )

    def adapt(self, minimum, maximum):
        # TODO: Implement when add adaptive kopf handler
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.close()

    @classmethod
    def from_name(cls, name, **kwargs):
        """Create an instance of this class to represent an existing cluster by name."""
        # TODO: Implement when switch to k8s python client
        raise NotImplementedError()
