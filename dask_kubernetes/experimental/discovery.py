from typing import Callable, AsyncIterator, Tuple

import kubernetes_asyncio as kubernetes

from .kubecluster import KubeCluster
from ..common.auth import ClusterAuth
from ..common.utils import get_current_namespace


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
            yield (cluster["metadata"]["name"].replace("-cluster", ""), KubeCluster)
    except Exception:
        return
