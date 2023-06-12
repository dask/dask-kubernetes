from kr8s.asyncio.objects import APIObject


class DaskCluster(APIObject):
    version = "kubernetes.dask.org/v1"
    endpoint = "daskclusters"
    kind = "DaskCluster"
    plural = "daskclusters"
    singular = "daskcluster"
    namespaced = True
    scalable = True
    scalable_spec = "worker.replicas"


class DaskWorkerGroup(APIObject):
    version = "kubernetes.dask.org/v1"
    endpoint = "daskworkergroups"
    kind = "DaskWorkerGroup"
    plural = "daskworkergroups"
    singular = "daskworkergroup"
    namespaced = True
    scalable = True


class DaskAutoscaler(APIObject):
    version = "kubernetes.dask.org/v1"
    endpoint = "daskautoscalers"
    kind = "DaskAutoscaler"
    plural = "daskautoscalers"
    singular = "daskautoscaler"
    namespaced = True


class DaskJob(APIObject):
    version = "kubernetes.dask.org/v1"
    endpoint = "daskjobs"
    kind = "DaskJob"
    plural = "daskjobs"
    singular = "daskjob"
    namespaced = True
