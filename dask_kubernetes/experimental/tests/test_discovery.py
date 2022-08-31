import pytest

from dask.distributed import Client
from dask_kubernetes.experimental import KubeCluster
from dask_kubernetes.experimental.discovery import discover


@pytest.mark.asyncio
async def test_discovery(cluster):
    clusters = [name async for name, _ in discover()]
    assert cluster.name in clusters
    with KubeCluster.from_name(cluster.name) as cluster2:
        assert cluster == cluster2
        assert "id" in cluster2.scheduler_info
        with Client(cluster2) as client:
            assert client.submit(lambda x: x + 1, 10).result() == 11
