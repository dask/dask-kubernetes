import kopf


@kopf.on.create("daskjob")
async def daskjob_create(spec, name, namespace, logger, **kwargs):
    # TODO Create a Dask Cluster
    # TODO Create a pod that will use the Dask cluster
    pass


@kopf.on.field(
    "pods", field="status.phase", labels={"dask-cluster": kopf.PRESENT}, new="Succeeded"
)
async def daskjob_succeeded(old, new, **kwargs):
    # TODO Once pod has comepleted delete the Dask Cluster again
    pass
