import asyncio

import pytest

import kubernetes

from dask_kubernetes.auth import ClusterAuth, KubeConfig, InCluster


def pytest_addoption(parser):
    parser.addoption(
        "--worker-image",
        default="daskdev/dask:latest",
        help="Worker image to use for testing",
    )
    parser.addoption("--context", default=None, help="kubectl context to use")
    parser.addoption(
        "--in-cluster", action="store_true", default=False, help="are we in cluster?"
    )
    parser.addoption("--namespace", default="default", help="Cluster namespace to use")


@pytest.fixture
def image_name(request):
    return request.config.getoption("--worker-image")


@pytest.fixture(scope="session")
def context(request):
    return request.config.getoption("--context")


@pytest.fixture(scope="session")
def in_cluster(request):
    return request.config.getoption("--in-cluster")


@pytest.fixture(scope="session")
def auth(in_cluster, context):
    if in_cluster:
        auth = [InCluster()]
    elif context:
        auth = [KubeConfig(context=context)]
    else:
        auth = None
    return auth


@pytest.fixture(scope="module")
def ns(request):
    """Use this fixture in all integration tests that need live K8S cluster."""
    return request.config.getoption("--namespace")
