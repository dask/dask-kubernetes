from typing import Any

import kopf


@kopf.on.startup()
async def noop_startup(logger: kopf.Logger, **__: Any) -> None:
    logger.info(
        "Plugin 'noop' running. This does nothing. "
        "See https://kubernetes.dask.org/en/latest/operator_extending.html "
        "for details on writing plugins for the Dask Operator controller."
    )
