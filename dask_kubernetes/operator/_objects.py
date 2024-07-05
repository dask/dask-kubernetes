from __future__ import annotations

from typing import List

from kr8s.asyncio.objects import Deployment, Pod, Service, new_class


class DaskCluster(new_class("DaskCluster", "kubernetes.dask.org/v1")):
    scalable = True
    scalable_spec = "worker.replicas"

    async def worker_groups(self) -> List[DaskWorkerGroup]:
        return await DaskWorkerGroup.list(
            label_selector=f"dask.org/cluster-name={self.name}",
            namespace=self.namespace,
        )

    async def scheduler_pod(self) -> Pod:
        pods = []
        while not pods:
            pods = await Pod.list(
                label_selector=",".join(
                    [
                        f"dask.org/cluster-name={self.name}",
                        "dask.org/component=scheduler",
                    ]
                ),
                namespace=self.namespace,
            )
        assert len(pods) == 1
        return pods[0]

    async def scheduler_deployment(self) -> Deployment:
        deployments = []
        while not deployments:
            deployments = await Deployment.list(
                label_selector=",".join(
                    [
                        f"dask.org/cluster-name={self.name}",
                        "dask.org/component=scheduler",
                    ]
                ),
                namespace=self.namespace,
            )
        assert len(deployments) == 1
        return deployments[0]

    async def scheduler_service(self) -> Service:
        services = []
        while not services:
            services = await Service.list(
                label_selector=",".join(
                    [
                        f"dask.org/cluster-name={self.name}",
                        "dask.org/component=scheduler",
                    ]
                ),
                namespace=self.namespace,
            )
        assert len(services) == 1
        return services[0]

    async def ready(self) -> bool:
        await self.async_refresh()
        return (
            "status" in self.raw
            and "phase" in self.status
            and self.status.phase == "Running"
        )


class DaskWorkerGroup(new_class("DaskWorkerGroup", "kubernetes.dask.org/v1")):
    scalable = True
    scalable_spec = "worker.replicas"

    async def pods(self) -> List[Pod]:
        return await Pod.list(
            label_selector=",".join(
                [
                    f"dask.org/cluster-name={self.spec.cluster}",
                    "dask.org/component=worker",
                    f"dask.org/workergroup-name={self.name}",
                ]
            ),
            namespace=self.namespace,
        )

    async def deployments(self) -> List[Deployment]:
        return await Deployment.list(
            label_selector=",".join(
                [
                    f"dask.org/cluster-name={self.spec.cluster}",
                    "dask.org/component=worker",
                    f"dask.org/workergroup-name={self.name}",
                ]
            ),
            namespace=self.namespace,
        )

    async def cluster(self) -> DaskCluster:
        return await DaskCluster.get(self.spec.cluster, namespace=self.namespace)


class DaskAutoscaler(new_class("DaskAutoscaler", "kubernetes.dask.org/v1")):
    async def cluster(self) -> DaskCluster:
        return await DaskCluster.get(self.spec.cluster, namespace=self.namespace)


class DaskJob(new_class("DaskJob", "kubernetes.dask.org/v1")):
    async def cluster(self) -> DaskCluster:
        return await DaskCluster.get(self.name, namespace=self.namespace)

    async def pod(self) -> Pod:
        pods = []
        while not pods:
            pods = await Pod.list(
                label_selector=",".join(
                    [
                        f"dask.org/cluster-name={self.name}",
                        "dask.org/component=job-runner",
                    ]
                ),
                namespace=self.namespace,
            )
        assert len(pods) == 1
        return pods[0]
