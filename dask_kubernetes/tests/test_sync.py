import asyncio
import os
import uuid

import pytest
from dask_kubernetes import KubeCluster, make_pod_spec, ClusterAuth, KubeConfig, KubeAuth
from dask.distributed import Client, wait
from distributed.utils import sync
from distributed.utils_test import loop, captured_logger  # noqa: F401
import kubernetes

TEST_DIR = os.path.abspath(os.path.join(__file__, '..'))
CONFIG_DEMO = os.path.join(TEST_DIR, 'config-demo.yaml')
FAKE_CERT = os.path.join(TEST_DIR, 'fake-cert-file')
FAKE_KEY = os.path.join(TEST_DIR, 'fake-key-file')
FAKE_CA = os.path.join(TEST_DIR, 'fake-ca-file')


@pytest.fixture
def api():
    asyncio.get_event_loop().run_until_complete(ClusterAuth.load_first())
    return kubernetes.client.CoreV1Api()


@pytest.fixture
def ns(api):
    name = 'test-dask-kubernetes' + str(uuid.uuid4())[:10]
    ns = kubernetes.client.V1Namespace(metadata=kubernetes.client.V1ObjectMeta(name=name))
    api.create_namespace(ns)
    try:
        yield name
    finally:
        api.delete_namespace(name, kubernetes.client.V1DeleteOptions())


@pytest.fixture
def pod_spec(image_name):
    yield make_pod_spec(
        image=image_name,
        extra_container_config={'imagePullPolicy': 'IfNotPresent'}
    )


@pytest.fixture
def cluster(pod_spec, ns, loop):
    with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
        yield cluster


@pytest.fixture
def client(cluster):
    with Client(cluster) as client:
        yield client


def test_fixtures(client):
    client.scheduler_info()
