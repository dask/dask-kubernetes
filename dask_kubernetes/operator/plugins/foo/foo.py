import kopf


@kopf.on.startup()
async def foo_startup(logger, **kwargs):
    logger.info("Plugin 'foo' running.")
