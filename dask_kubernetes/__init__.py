from importlib import import_module
from warnings import warn

from . import config
from .common.auth import (
    AutoRefreshConfiguration,
    AutoRefreshKubeConfigLoader,
    ClusterAuth,
    InCluster,
    KubeAuth,
    KubeConfig,
)
from .common.objects import clean_pod_template, make_pod_from_dict, make_pod_spec
from .helm import HelmCluster

__all__ = ["HelmCluster", "KubeCluster"]

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


def __getattr__(name):
    if name == "KubeCluster":
        new_module = import_module("dask_kubernetes.classic")
        return getattr(new_module, name)

    raise AttributeError(f"module {__name__} has no attribute {name}")
