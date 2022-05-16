import pytest

import pathlib
import os
import shutil
import subprocess
import tempfile

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


def run_generate(crd_path, patch_path, temp_path):
    subprocess.run(
        ["k8s-crd-resolver", "-r", "-j", patch_path, crd_path, temp_path],
        check=True,
        env={**os.environ},
    )


@pytest.fixture(scope="session", autouse=True)
def customresources(k8s_cluster):

    temp_dir = tempfile.TemporaryDirectory()
    crd_path = os.path.join(DIR, "operator", "customresources")

    run_generate(
        os.path.join(crd_path, "daskcluster.yaml"),
        os.path.join(crd_path, "daskcluster.patch.yaml"),
        os.path.join(temp_dir.name, "daskcluster.yaml"),
    )
    run_generate(
        os.path.join(crd_path, "daskworkergroup.yaml"),
        os.path.join(crd_path, "daskworkergroup.patch.yaml"),
        os.path.join(temp_dir.name, "daskworkergroup.yaml"),
    )
    shutil.copyfile(
        os.path.join(DIR, "operator", "deployment", "manifests", "kopfpeering.yaml"),
        os.path.join(temp_dir.name, "kopfpeering.yaml"),
    )

    k8s_cluster.kubectl("apply", "-f", temp_dir.name)
    yield
    k8s_cluster.kubectl("delete", "-f", temp_dir.name)
    temp_dir.cleanup()
