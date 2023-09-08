Troubleshooting
===============

This page contains common problems and resolutions.

Why am I losing data during scale down?
---------------------------------------

When scaling down a cluster the controller will attempt to coordinate with the Dask scheduler and
decide which workers to remove. If the controller cannot communicate with the scheduler it will fall
back to last-in-first-out scaling and will remove the worker with the lowest uptime, even if that worker
is actively processing data. This can result in loss of data and recalculation of a graph.

This commonly happens if the version of Dask on the scheduler is very different to the verison on the controller.

To mitigate this Dask has an optional HTTP API which is more decoupled than the RPC and allows for better
support between versions.

See `https://github.com/dask/dask-kubernetes/issues/807 <https://github.com/dask/dask-kubernetes/issues/807>`_
