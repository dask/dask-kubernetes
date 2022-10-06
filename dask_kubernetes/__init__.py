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
        warn(
            "It looks like you are using the classic implementation of KubeCluster. "
            "Please consider migrating to the new operator based implementation "
            "https://kubernetes.dask.org/en/latest/kubecluster_migrating.html. "
            "To suppress this warning import KubeCluster directly from dask_kubernetes.classic. "
            "But note this will be removed in the future. ",
            DeprecationWarning,
            stacklevel=2,
        )
        new_module = import_module("dask_kubernetes.classic")
        return getattr(new_module, name)

    raise AttributeError(f"module {__name__} has no attribute {name}")
