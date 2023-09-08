from importlib import import_module
from warnings import warn

from . import config
from .common.auth import (
    ClusterAuth,
    KubeAuth,
    KubeConfig,
    InCluster,
    AutoRefreshKubeConfigLoader,
    AutoRefreshConfiguration,
)
from .helm import HelmCluster
from .common.objects import make_pod_spec, make_pod_from_dict, clean_pod_template

__all__ = ["HelmCluster", "KubeCluster"]

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


def __getattr__(name):
    if name == "KubeCluster":
        new_module = import_module("dask_kubernetes.classic")
        return getattr(new_module, name)

    raise AttributeError(f"module {__name__} has no attribute {name}")
