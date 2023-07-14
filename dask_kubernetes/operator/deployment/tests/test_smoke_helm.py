import subprocess
import uuid
from pathlib import Path

import pytest

from dask_kubernetes.operator import KubeCluster


@pytest.fixture(scope="module")
def operator_image(k8s_cluster):
    image_name = "dask-kubernetes-operator:dev"
    project_root = Path(__file__).parent.parent.parent.parent.parent
    dockerfile = Path(__file__).parent.parent / "Dockerfile"
    subprocess.run(
        ["docker", "build", "-t", image_name, "-f", dockerfile, str(project_root)],
        check=True,
    )
    k8s_cluster.load_docker_image(image_name)
    return image_name


@pytest.fixture
def install_cluster_role_helm_chart(k8s_cluster, operator_image, ns):
    this_dir = Path(__file__).parent
    helm_chart_dir = this_dir.parent / "helm" / "dask-kubernetes-operator"
    release_name = f"pytest-smoke-operator-{str(uuid.uuid4())[:8]}"
    docker_image_name = operator_image.split(":")[0]
    docker_image_tag = operator_image.split(":")[1]
    subprocess.run(
        [
            "helm",
            "install",
            "-n",
            ns,
            release_name,
            str(helm_chart_dir),
            f"--set=image.name={docker_image_name}",
            f"--set=image.tag={docker_image_tag}",
            "--wait",
        ],
        check=True,
    )
    yield
    subprocess.run(
        ["helm", "uninstall", "-n", ns, release_name],
        check=True,
    )


@pytest.mark.timeout(180)
def test_smoke_helm_deployment_role(install_cluster_role_helm_chart, ns):
    with KubeCluster(
        name="pytest-smoke-cluster",
        namespace=ns,
        shutdown_on_close=True,
    ) as cluster:
        cluster.scale(2)
        client = cluster.get_client()
        client.wait_for_workers(2, timeout=120)
