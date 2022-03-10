# import asyncio

# import subprocess

from distributed.deploy import Cluster
from distributed.core import rpc

# import kopf
import kubernetes

from dask_kubernetes.utils import (
    #     namespace_default,
    get_external_address_for_scheduler_service,
    #     check_dependency,
)
from dask_kubernetes.auth import ClusterAuth
from daskcluster import (
    build_cluster_spec,
)  # , wait_for_scheduler, build_worker_group_spec


class KubeCluster2(Cluster):
    """Launch a Dask Cluster on Kubernetes using the Operator"""

    def __init__(
        self,
        name,
        namespace="default",
        image="daskdev/dask:latest",
        replicas=3,
        resources={},
        env={},
        asynchronous=False,
        auth=ClusterAuth.DEFAULT,
        **kwargs
    ):
        self.name = name
        self.namespace = namespace
        self.core_api = None
        self.custom_api = None
        self.image = image
        self.replicas = replicas
        self.resources = resources
        self.env = env
        self.auth = auth

        super().__init__(asynchronous=asynchronous, **kwargs)
        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def _start(self):
        await ClusterAuth.load_first(self.auth)
        self.core_api = kubernetes.client.CoreV1Api()
        self.custom_api = kubernetes.client.CustomObjectsApi()
        data = build_cluster_spec(
            self.name, self.image, self.replicas, self.resources, self.env
        )
        cluster = self.custom_api.create_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskclusters",
            namespace=self.namespace,
            body=data,
        )
        self.scheduler_comm = rpc(await self._get_scheduler_address())
        await super()._start()

    async def _get_scheduler_address(self):
        service_name = self.name
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
        pass

    @classmethod
    def from_name(cls, name, **kwargs):
        pass


if __name__ == "__main__":
    cluster = KubeCluster2(name="foo")
    print(cluster.status)
