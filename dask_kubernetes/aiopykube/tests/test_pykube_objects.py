import pytest

import asyncio
import uuid

from dask_kubernetes.aiopykube import HTTPClient, KubeConfig
from dask_kubernetes.aiopykube.objects import Pod


@pytest.mark.anyio
async def test_pod_create_and_delete(docker_image, k8s_cluster):
    api = HTTPClient(KubeConfig.from_env())
    name = "test-" + uuid.uuid4().hex[:10]
    pod = Pod(
        api,
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name},
            "spec": {
                "containers": [
                    # Cannot use `gcr.io/google_containers/pause` as it's not available
                    # for arm64
                    {
                        "name": "pause",
                        "image": docker_image,
                        "command": ["sleep", "1000"],
                    },
                ]
            },
        },
    )

    await pod.create()
    assert await pod.exists()
    await pod.delete()
    while await pod.exists():
        await asyncio.sleep(0.1)
    assert not await pod.exists()
