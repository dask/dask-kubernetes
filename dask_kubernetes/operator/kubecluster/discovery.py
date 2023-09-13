from typing import AsyncIterator, Callable, Tuple

import kr8s

from dask_kubernetes.operator._objects import DaskCluster  # noqa
from dask_kubernetes.operator.kubecluster import KubeCluster


async def discover() -> AsyncIterator[Tuple[str, Callable]]:
    try:
        clusters = await kr8s.asyncio.get("daskclusters", namespace=kr8s.ALL)
        for cluster in clusters:
            yield (cluster.name, KubeCluster)
    except Exception:
        return
