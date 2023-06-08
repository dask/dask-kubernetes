import pytest

import pathlib
import os
import subprocess
import tempfile
import uuid

from kopf.testing import KopfRunner
from pytest_kind.cluster import KindCluster

from dask_kubernetes.common.utils import check_dependency

DIR = pathlib.Path(__file__).parent.absolute()

check_dependency("helm")
check_dependency("kubectl")
check_dependency("docker")


@pytest.fixture()
def kopf_runner(k8s_cluster):
    yield KopfRunner(["run", "-m", "dask_kubernetes.operator", "--verbose"])


@pytest.fixture(scope="session")
def docker_image():
    image_name = "dask-kubernetes:dev"
    subprocess.run(["docker", "build", "-t", image_name, "./ci/"], check=True)
    return image_name


@pytest.fixture(scope="session")
def k8s_cluster(request, docker_image):
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
def install_istio(k8s_cluster):
    if bool(os.environ.get("TEST_ISTIO", False)):
        check_dependency("istioctl")
        subprocess.run(
            ["istioctl", "install", "--set", "profile=demo", "-y"], check=True
        )
        k8s_cluster.kubectl(
            "label", "namespace", "default", "istio-injection=enabled", "--overwrite"
        )


@pytest.fixture(autouse=True)
def ns(k8s_cluster):
    ns = "dask-k8s-pytest-" + uuid.uuid4().hex[:10]
    k8s_cluster.kubectl("create", "ns", ns)
    yield ns
    k8s_cluster.kubectl("delete", "ns", ns, "--wait=false", "--ignore-not-found=true")


@pytest.fixture(scope="session", autouse=True)
def install_gateway(k8s_cluster):
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
def customresources(k8s_cluster):

    temp_dir = tempfile.TemporaryDirectory()
    crd_path = os.path.join(DIR, "operator", "customresources")

    def run_generate(crd_path, patch_path, temp_path):
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
    k8s_cluster.kubectl("delete", "-f", temp_dir.name)
    temp_dir.cleanup()
