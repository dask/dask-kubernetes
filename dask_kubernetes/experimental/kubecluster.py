import asyncio
import kubernetes_asyncio as kubernetes

from distributed.core import rpc
from distributed.deploy import Cluster

from distributed.utils import Log, Logs, LoopRunner

from dask_kubernetes.auth import ClusterAuth
from dask_kubernetes.operator.daskcluster import (
    build_cluster_spec,
    build_worker_group_spec,
    wait_for_service,
)

from dask_kubernetes.utils import (
    check_dependency,
    get_scheduler_address,
    wait_for_scheduler,
)


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
    >>> from dask_kubernetes import KubeCluster
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
    See Also
    --------
    KubeCluster.from_name
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
        self.image = image
        self.n_workers = n_workers
        self.resources = resources
        self.env = env
        self.auth = auth
        self.port_forward_cluster_ip = port_forward_cluster_ip
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop
        check_dependency("kubectl")

        # TODO: Check if cluster already exists
        super().__init__(asynchronous=asynchronous, **kwargs)
        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def _start(self):
        await ClusterAuth.load_first(self.auth)

        async with kubernetes.client.api_client.ApiClient() as api_client:
            core_api = kubernetes.client.CoreV1Api(api_client)
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            data = build_cluster_spec(
                self.name, self.image, self.n_workers, self.resources, self.env
            )
            await custom_objects_api.create_namespaced_custom_object(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskclusters",
                namespace=self.namespace,
                body=data,
            )
            await wait_for_scheduler(f"{self.name}-cluster", self.namespace)
            await wait_for_service(core_api, data["metadata"]["name"], self.namespace)
            self.scheduler_comm = rpc(await self._get_scheduler_address())
            await super()._start()

    async def _get_scheduler_address(self):
        service_name = f"{self.name}-cluster"
        address = await get_scheduler_address(service_name, self.namespace)
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
        async with kubernetes.client.api_client.ApiClient() as api_client:
            core_api = kubernetes.client.CoreV1Api(api_client)
            logs = Logs()

            pods = await core_api.list_namespaced_pod(
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
                            await core_api.read_namespaced_pod_log(
                                pod.metadata.name, pod.metadata.namespace
                            )
                        )
                    except (ValueError, kubernetes.client.exceptions.ApiException):
                        log = Log(f"Cannot find logs. Pod is {pod.status.phase}.")
                logs[pod.metadata.name] = log

        return logs

    def add_worker_group(self, name, n=3):
        """Create a dask worker group by name

        Parameters
        ----------
        name: str
            Name of the worker group
        n: int
            Target number of workers for worker group

        Examples
        --------
        >>> cluster.add_worker_group("high-mem-workers", n=5)
        """
        # TODO: Once adoptions work correctly we can enable this method
        raise NotImplementedError()
        # return self.sync(self._add_worker_group, name, n)

    async def _add_worker_group(self, name, n=3):
        data = build_worker_group_spec(
            f"{self.name}-cluster-{name}", self.image, n, self.resources, self.env
        )
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
        # TODO: Once adoptions work correctly we can enable this method
        raise NotImplementedError()
        # return self.sync(self._delete_worker_group, name)

    async def _delete_worker_group(self, name):
        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            await custom_objects_api.delete_namespaced_custom_object(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskworkergroups",
                namespace=self.namespace,
                name=name,
            )

    def close(self):
        """Delete the dask cluster"""
        return self.sync(self._close)

    async def _close(self):
        await super()._close()
        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            await custom_objects_api.delete_namespaced_custom_object(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskclusters",
                namespace=self.namespace,
                name=f"{self.name}-cluster",
            )
            while True:
                try:
                    await custom_objects_api.get_namespaced_custom_object(
                        group="kubernetes.dask.org",
                        version="v1",
                        plural="daskclusters",
                        namespace=self.namespace,
                        name=f"{self.name}-cluster",
                    )
                    await asyncio.sleep(1)
                except kubernetes.client.exceptions.ApiException as e:
                    if "Not Found" in str(e):
                        break
                    else:
                        raise e

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
            await custom_objects_api.patch_namespaced_custom_object_scale(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskworkergroups",
                namespace=self.namespace,
                name=f"{self.name}-cluster-{worker_group}-worker-group",
                body={"spec": {"replicas": n}},
            )

    def adapt(self, *args, **kwargs):
        """Turn on adaptivity"""
        raise NotImplementedError(
            "Adaptive mode is not supported yet for this KubeCluster."
        )

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.close()

    @classmethod
    def from_name(cls, name, **kwargs):
        """Create an instance of this class to represent an existing cluster by name.

        Parameters
        ----------
        name: str
            Name of the cluster to connect to

        Examples
        --------
        >>> cluster = KubeCluster.from_name(name="simple-cluster")
        """
        # TODO: Implement when switch to k8s python client
        raise NotImplementedError()
