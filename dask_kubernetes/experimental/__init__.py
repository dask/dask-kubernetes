from importlib import import_module
from warnings import warn


def __getattr__(name):
    no_longer_experimental = [
        "KubeCluster",
        "make_cluster_spec",
        "make_scheduler_spec",
        "make_worker_spec",
        "discover",
    ]
    if name in no_longer_experimental:
        warn(
            f"Yay {name} is no longer experimental 🎉. "
            "You can import it directly from dask_kubernetes or explicitly from dask_kubernetes.operator",
            DeprecationWarning,
            stacklevel=2,
        )
        new_module = import_module("dask_kubernetes.operator")
        return getattr(new_module, name)

    raise AttributeError(f"module {__name__} has no attribute {name}")
