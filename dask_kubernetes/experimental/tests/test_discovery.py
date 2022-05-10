import pytest

from dask.distributed import Client
from dask_ctl.discovery import (
    list_discovery_methods,
    discover_cluster_names,
)

from dask_kubernetes.experimental.kubecluster import KubeCluster


@pytest.mark.asyncio
async def test_from_name(cluster):
    # Check cluster listed in discovery
    discovery = "kubecluster_experimental"
    assert discovery in list_discovery_methods()
    cluster_names = [
        cluster async for cluster in discover_cluster_names(discovery=discovery)
    ]
    assert len(cluster_names) == 1
    discovered_name, discovered_class = cluster_names[0]
    assert discovered_name == cluster.name
    assert discovered_class == KubeCluster

    # Recreate cluster manager from name
    with discovered_class.from_name(discovered_name) as cluster2:
        assert "id" in cluster2.scheduler_info
        with Client(cluster2) as client:
            assert client.submit(lambda x: x + 1, 10).result() == 11
