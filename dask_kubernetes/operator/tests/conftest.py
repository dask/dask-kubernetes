import pytest

from glob import glob
import pathlib
import os
import tempfile
import subprocess

DIR = pathlib.Path(__file__).parent.absolute()


def run_generate(crd_path, patch_path, temp_path):
    subprocess.run(["k8s-crd-resolver", "-r", "-j", patch_path, crd_path, temp_path], check=True, env={**os.environ})


@pytest.fixture(scope="session", autouse=True)
def customresources(k8s_cluster):

    temp_dir = tempfile.TemporaryDirectory()
    crd_path = os.path.join(DIR, "..", "customresources")

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

    k8s_cluster.kubectl("apply", "-f", temp_dir.name)
    yield
    k8s_cluster.kubectl("delete", "-f", temp_dir.name)
    temp_dir.cleanup()
