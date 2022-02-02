import pytest

from glob import glob
import pathlib
import os

DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture(scope="session", autouse=True)
def customresources(k8s_cluster):
    crd_path = glob(os.path.join(DIR, "..", "customresources"))
    k8s_cluster.kubectl("apply", "-f", *crd_path)
    yield
    k8s_cluster.kubectl("delete", "-f", *crd_path)
