import logging
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import uuid
from typing import Final, Iterator

import pytest
from kopf.testing import KopfRunner
from pytest_kind.cluster import KindCluster

DIR: Final[pathlib.Path] = pathlib.Path(__file__).parent.absolute()


def check_dependency(dependency):
    if shutil.which(dependency) is None:
        raise RuntimeError(
            f"Missing dependency {dependency}. "
            f"Please install {dependency} following the instructions for your OS. "
        )


check_dependency("helm")
check_dependency("kubectl")
check_dependency("docker")

DISABLE_LOGGERS: Final[list[str]] = ["httpcore", "httpx"]


def pytest_configure() -> None:
    for logger_name in DISABLE_LOGGERS:
        logger = logging.getLogger(logger_name)
        logger.disabled = True


@pytest.fixture()
def kopf_runner(k8s_cluster: KindCluster, namespace: str) -> KopfRunner:
    yield KopfRunner(
        ["run", "-m", "dask_kubernetes.operator", "--verbose", "--namespace", namespace]
    )


@pytest.fixture(scope="session")
def docker_image() -> str:
    image_name = "dask-kubernetes:dev"
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    subprocess.run(
        [
            "docker",
            "build",
            "-t",
            image_name,
            "--build-arg",
            f"PYTHON={python_version}",
            "./ci/",
        ],
        check=True,
    )
    return image_name


@pytest.fixture(scope="session")
def k8s_cluster(
    request: pytest.FixtureRequest, docker_image: str
) -> Iterator[KindCluster]:
    image = None
    if version := os.environ.get("KUBERNETES_VERSION"):
        image = f"kindest/node:v{version}"

    kind_cluster = KindCluster(
        name="pytest-kind",
        image=image,
    )
    kind_cluster.create()
    os.environ["KUBECONFIG"] = str(kind_cluster.kubeconfig_path)
    kind_cluster.load_docker_image(docker_image)
    yield kind_cluster
    del os.environ["KUBECONFIG"]
    if not request.config.getoption("keep_cluster"):
        kind_cluster.delete()


@pytest.fixture(scope="session", autouse=True)
def install_istio(k8s_cluster: KindCluster) -> None:
    if bool(os.environ.get("TEST_ISTIO", False)):
        check_dependency("istioctl")
        subprocess.run(
            ["istioctl", "install", "--set", "profile=demo", "-y"], check=True
        )
        k8s_cluster.kubectl(
            "label", "namespace", "default", "istio-injection=enabled", "--overwrite"
        )


@pytest.fixture(autouse=True)
def namespace(k8s_cluster: KindCluster) -> Iterator[str]:
    ns = "dask-k8s-pytest-" + uuid.uuid4().hex[:10]
    k8s_cluster.kubectl("create", "ns", ns)
    yield ns
    k8s_cluster.kubectl("delete", "ns", ns, "--wait=false", "--ignore-not-found=true")


@pytest.fixture(scope="session", autouse=True)
def install_gateway(k8s_cluster: KindCluster) -> Iterator[None]:
    if bool(os.environ.get("TEST_DASK_GATEWAY", False)):
        check_dependency("helm")
        # To ensure the operator can coexist with Gateway
        subprocess.run(
            [
                "helm",
                "upgrade",
                "dask-gateway",
                "dask-gateway",
                "--install",
                "--repo=https://helm.dask.org",
                "--create-namespace",
                "--namespace",
                "dask-gateway",
            ],
            check=True,
            env={**os.environ},
        )
        yield
        subprocess.run(
            [
                "helm",
                "delete",
                "--namespace",
                "dask-gateway",
                "dask-gateway",
            ],
            check=True,
            env={**os.environ},
        )
    else:
        yield


@pytest.fixture(scope="session", autouse=True)
def customresources(k8s_cluster: KindCluster) -> Iterator[None]:
    temp_dir = tempfile.TemporaryDirectory()
    crd_path = os.path.join(DIR, "operator", "customresources")

    def run_generate(crd_path: str, patch_path: str, temp_path: str) -> None:
        subprocess.run(
            ["k8s-crd-resolver", "-r", "-j", patch_path, crd_path, temp_path],
            check=True,
            env={**os.environ},
        )

    for crd in ["daskcluster", "daskworkergroup", "daskjob", "daskautoscaler"]:
        run_generate(
            os.path.join(crd_path, f"{crd}.yaml"),
            os.path.join(crd_path, f"{crd}.patch.yaml"),
            os.path.join(temp_dir.name, f"{crd}.yaml"),
        )

    k8s_cluster.kubectl("apply", "-f", temp_dir.name)
    yield
    k8s_cluster.kubectl("delete", "--wait=false", "-f", temp_dir.name)
    temp_dir.cleanup()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
