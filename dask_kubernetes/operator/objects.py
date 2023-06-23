from __future__ import annotations
from typing import List

from kr8s.asyncio.objects import APIObject, Pod, Deployment, Service


class DaskCluster(APIObject):
    version = "kubernetes.dask.org/v1"
    endpoint = "daskclusters"
    kind = "DaskCluster"
    plural = "daskclusters"
    singular = "daskcluster"
    namespaced = True
    scalable = True
    scalable_spec = "worker.replicas"

    async def worker_groups(self) -> List[DaskWorkerGroup]:
        return await self.api.get(
            DaskWorkerGroup.endpoint,
            label_selector=f"dask.org/cluster-name={self.name}",
            namespace=self.namespace,
        )

    async def scheduler_pod(self) -> Pod:
        pods = []
        while not pods:
            pods = await self.api.get(
                Pod.endpoint,
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
            deployments = await self.api.get(
                Deployment.endpoint,
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
            services = await self.api.get(
                Service.endpoint,
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


class DaskWorkerGroup(APIObject):
    version = "kubernetes.dask.org/v1"
    endpoint = "daskworkergroups"
    kind = "DaskWorkerGroup"
    plural = "daskworkergroups"
    singular = "daskworkergroup"
    namespaced = True
    scalable = True

    async def pods(self) -> List[Pod]:
        return await self.api.get(
            Pod.endpoint,
            label_selector=",".join(
                [
                    f"dask.org/cluster-name={self.spec['cluster']}",
                    "dask.org/component=worker",
                    f"dask.org/workergroup-name={self.name}",
                ]
            ),
            namespace=self.namespace,
        )

    async def deployments(self) -> List[Deployment]:
        return await self.api.get(
            Deployment.endpoint,
            label_selector=",".join(
                [
                    f"dask.org/cluster-name={self.spec['cluster']}",
                    "dask.org/component=worker",
                    f"dask.org/workergroup-name={self.name}",
                ]
            ),
            namespace=self.namespace,
        )

    async def cluster(self) -> DaskCluster:
        return await DaskCluster.get(self.spec["cluster"], namespace=self.namespace)


class DaskAutoscaler(APIObject):
    version = "kubernetes.dask.org/v1"
    endpoint = "daskautoscalers"
    kind = "DaskAutoscaler"
    plural = "daskautoscalers"
    singular = "daskautoscaler"
    namespaced = True

    async def cluster(self) -> DaskCluster:
        return await DaskCluster.get(self.spec["cluster"], namespace=self.namespace)


class DaskJob(APIObject):
    version = "kubernetes.dask.org/v1"
    endpoint = "daskjobs"
    kind = "DaskJob"
    plural = "daskjobs"
    singular = "daskjob"
    namespaced = True

    async def cluster(self) -> DaskCluster:
        return await DaskCluster.get(self.name, namespace=self.namespace)

    async def pod(self) -> Pod:
        pods = []
        while not pods:
            pods = await self.api.get(
                Pod.endpoint,
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
