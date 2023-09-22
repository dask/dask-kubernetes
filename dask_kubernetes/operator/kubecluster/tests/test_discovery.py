import pytest
from dask.distributed import Client

from dask_kubernetes.operator import KubeCluster, discover


@pytest.mark.anyio
@pytest.mark.skip(reason="Flaky in CI")
async def test_discovery(kopf_runner, docker_image):
    with kopf_runner:
        async with KubeCluster(
            image=docker_image, n_workers=1, asynchronous=True
        ) as cluster:
            clusters = [name async for name, _ in discover()]
            assert cluster.name in clusters
            async with KubeCluster.from_name(
                cluster.name, asynchronous=True
            ) as cluster2:
                assert cluster == cluster2
                assert "id" in cluster2.scheduler_info
                async with Client(cluster2, asynchronous=True) as client:
                    assert await client.submit(lambda x: x + 1, 10).result() == 11
