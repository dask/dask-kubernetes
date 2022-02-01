import kopf


@kopf.on.create("daskcluster")
def daskcluster_create(spec, name, namespace, logger, **kwargs):
    logger.info(
        f"A DaskCluster has been created called {name} in {namespace} with the following config: {spec}"
    )
