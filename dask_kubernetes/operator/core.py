import subprocess

from distributed.deploy import Cluster
from distributed.core import rpc

# import kopf
import kubernetes

from dask_kubernetes.utils import (
    #     namespace_default,
    get_external_address_for_scheduler_service,
    #     check_dependency,
)


class KubeCluster2(Cluster):
    """Launch a Dask Cluster on Kubernetes using the Operator"""

    def __init__(self, name, loop=None, asynchronous=False, **kwargs):
        self.name = name
        self.core_api = None
        self.scheduler_comm = None

        status = subprocess.run(
            ["kopf", "run", ".daskcluster.py"],
            capture_output=True,
            encoding="utf-8",
        )
        super().__init__(asynchronous=asynchronous, **kwargs)
        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def _start(self):
        self.core_api = kubernetes.client.CoreV1Api()
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
