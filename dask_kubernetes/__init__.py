from . import config
from .auth import ClusterAuth, KubeAuth, KubeConfig, InCluster
from .core import KubeCluster
from .helm import HelmCluster
from .objects import make_pod_spec, make_pod_from_dict, clean_pod_template

__all__ = [KubeCluster, HelmCluster]

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
