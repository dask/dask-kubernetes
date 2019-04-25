from . import config
from .auth import ClusterAuth, KubeAuth, KubeConfig, InCluster
from .core import KubeCluster
from .objects import make_pod_spec, make_pod_from_dict

__all__ = [KubeCluster]

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
