from typing import Callable, AsyncIterator, Tuple

import kubernetes_asyncio as kubernetes

from dask_kubernetes.operator.kubecluster import KubeCluster
from dask_kubernetes.common.auth import ClusterAuth
from dask_kubernetes.common.utils import get_current_namespace


async def discover() -> AsyncIterator[Tuple[str, Callable]]:

    await ClusterAuth.load_first()

    try:
        async with kubernetes.client.api_client.ApiClient() as api_client:
            custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
            clusters = await custom_objects_api.list_namespaced_custom_object(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskclusters",
                namespace=get_current_namespace(),
            )
        for cluster in clusters["items"]:
            yield (cluster["metadata"]["name"], KubeCluster)
    except Exception:
        return
