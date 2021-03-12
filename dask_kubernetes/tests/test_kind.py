import pytest
from subprocess import check_output

import kubernetes_asyncio as kubernetes

from dask_kubernetes.auth import ClusterAuth


def test_config_detection(k8s_cluster):
    assert b"pytest-kind" in check_output(["kubectl", "config", "current-context"])


@pytest.mark.asyncio
async def test_auth(k8s_cluster):
    await ClusterAuth.load_first(ClusterAuth.DEFAULT)
    core_v1_api = kubernetes.client.CoreV1Api()
    request = await core_v1_api.list_namespace()
    assert "default" in [namespace.metadata.name for namespace in request.items]

    request = await core_v1_api.list_node()
    assert "pytest-kind-control-plane" in [node.metadata.name for node in request.items]
