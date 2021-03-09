import pytest

import os
import subprocess

from dask_kubernetes.utils import check_dependency

check_dependency("helm")
check_dependency("kubectl")
check_dependency("docker")


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
