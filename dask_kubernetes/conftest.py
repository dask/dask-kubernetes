import pytest

from glob import glob
import pathlib
import os
import subprocess

from kopf.testing import KopfRunner

from dask_kubernetes.common.utils import check_dependency

DIR = pathlib.Path(__file__).parent.absolute()

check_dependency("helm")
check_dependency("kubectl")
check_dependency("docker")


@pytest.fixture()
async def kopf_runner(k8s_cluster):
    yield KopfRunner(["run", "-m", "dask_kubernetes.operator", "--verbose"])


@pytest.fixture(scope="session")
def docker_image():
    image_name = "dask-kubernetes:dev"
    subprocess.check_output(["docker", "build", "-t", image_name, "./ci/"])
    return image_name


@pytest.fixture(scope="session")
def k8s_cluster(kind_cluster, docker_image):
    os.environ["KUBECONFIG"] = str(kind_cluster.kubeconfig_path)
    kind_cluster.load_docker_image(docker_image)
    yield kind_cluster
    del os.environ["KUBECONFIG"]


@pytest.fixture(scope="session")
def ns(k8s_cluster):
    return "default"


@pytest.fixture(scope="session", autouse=True)
def customresources(k8s_cluster):
    crd_path = glob(os.path.join(DIR, "operator", "customresources"))
    k8s_cluster.kubectl("apply", "-f", *crd_path)
    yield
    k8s_cluster.kubectl("delete", "-f", *crd_path)
