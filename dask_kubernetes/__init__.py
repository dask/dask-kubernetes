from . import config
from .auth import (
    ClusterAuth,
    KubeAuth,
    KubeConfig,
    InCluster,
    AutoRefreshKubeConfigLoader,
    AutoRefreshConfiguration,
)
from .core import KubeCluster
from .helm import HelmCluster
from .operator.core import KubeCluster2
from .objects import make_pod_spec, make_pod_from_dict, clean_pod_template

__all__ = ["HelmCluster", "KubeCluster", "KubeCluster2"]

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
