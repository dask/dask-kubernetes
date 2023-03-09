import pytest

from dask.distributed import Client
from dask_kubernetes.operator import KubeCluster
from dask_kubernetes.operator import discover


@pytest.mark.asyncio
async def test_discovery(async_cluster):
    clusters = [name async for name, _ in discover()]
    assert async_cluster.name in clusters
    async with KubeCluster.from_name(async_cluster.name, asynchronous=True) as cluster2:
        assert async_cluster == cluster2
        assert "id" in cluster2.scheduler_info
        async with Client(cluster2, asynchronous=True) as client:
            assert await client.submit(lambda x: x + 1, 10).result() == 11
