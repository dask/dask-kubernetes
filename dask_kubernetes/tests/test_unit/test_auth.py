import base64
import os

import pytest

import kubernetes_asyncio as kubernetes

from dask_kubernetes import KubeCluster
from dask_kubernetes.objects import make_pod_spec, clean_pod_template
from dask_kubernetes.auth import ClusterAuth, KubeConfig, KubeAuth, InCluster


TEST_DIR = os.path.abspath(os.path.join(__file__, ".."))
CONFIG_DEMO = os.path.join(TEST_DIR, "data", "config-demo.yaml")
FAKE_CERT = os.path.join(TEST_DIR, "data", "fake-cert-file")
FAKE_KEY = os.path.join(TEST_DIR, "data", "fake-key-file")
FAKE_CA = os.path.join(TEST_DIR, "data", "fake-ca-file")


cluster_kwargs = dict(
    pod_template=clean_pod_template(make_pod_spec("image.jpg")), asynchronous=True
)


class NoOpAwaitable(object):
    """An awaitable object that always returns None."""

    def __await__(self):
        async def f():
            return None

        return f().__await__()


@pytest.mark.parametrize("auth", [None, [], [InCluster()]])
@pytest.mark.asyncio
async def test_cluster_calls_auth(mocker, no_kubernetes, auth):
    mocker.patch.object(ClusterAuth, "load_first")
    ClusterAuth.load_first.side_effect = [NoOpAwaitable()]

    await KubeCluster(auth=auth, **cluster_kwargs)

    ClusterAuth.load_first.assert_called_once_with(auth)


@pytest.mark.asyncio
async def test_auth_missing():
    with pytest.raises(kubernetes.config.ConfigException) as info:
        await ClusterAuth.load_first(auth=[])

    assert "No authorization methods were provided" in str(info.value)


@pytest.mark.asyncio
async def test_auth_tries_all_methods():
    fails = {"count": 0}

    class FailAuth(ClusterAuth):
        def load(self):
            fails["count"] += 1
            raise kubernetes.config.ConfigException("Fail #{count}".format(**fails))

    with pytest.raises(kubernetes.config.ConfigException) as info:
        await ClusterAuth.load_first(auth=[FailAuth()] * 3)

    assert "Fail #3" in str(info.value)
    assert fails["count"] == 3


@pytest.mark.asyncio
async def test_auth_kubeconfig_with_filename():
    await KubeConfig(config_file=CONFIG_DEMO).load()

    # we've set the default configuration, so check that it is default
    config = kubernetes.client.Configuration()
    assert config.host == "https://1.2.3.4"
    assert config.cert_file == FAKE_CERT
    assert config.key_file == FAKE_KEY
    assert config.ssl_ca_cert == FAKE_CA


@pytest.mark.asyncio
async def test_auth_kubeconfig_with_context():
    await KubeConfig(config_file=CONFIG_DEMO, context="exp-scratch").load()

    # we've set the default configuration, so check that it is default
    config = kubernetes.client.Configuration()
    assert config.host == "https://5.6.7.8"
    assert config.api_key["authorization"] == "Basic {}".format(
        base64.b64encode(b"exp:some-password").decode("ascii")
    )


@pytest.mark.asyncio
async def test_auth_explicit():
    await KubeAuth(
        host="https://9.8.7.6", username="abc", password="some-password"
    ).load()

    config = kubernetes.client.Configuration()
    assert config.host == "https://9.8.7.6"
    assert config.username == "abc"
    assert config.password == "some-password"
    assert config.get_basic_auth_token() == "Basic {}".format(
        base64.b64encode(b"abc:some-password").decode("ascii")
    )
