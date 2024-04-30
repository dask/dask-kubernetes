from . import config

__all__ = []

try:
    from ._version import version as __version__  # noqa
    from ._version import version_tuple as __version_tuple__  # noqa
except ImportError:
    __version__ = "0.0.0"
    __version_tuple__ = (0, 0, 0)
