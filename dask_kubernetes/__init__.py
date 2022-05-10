from . import config
from .common.auth import (
    ClusterAuth,
    KubeAuth,
    KubeConfig,
    InCluster,
    AutoRefreshKubeConfigLoader,
    AutoRefreshConfiguration,
)
from .classic import KubeCluster
from .helm import HelmCluster
from .common.objects import make_pod_spec, make_pod_from_dict, clean_pod_template

__all__ = ["HelmCluster", "KubeCluster"]

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
