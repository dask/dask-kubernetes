import re

KUBECLUSTER_CONTAINER_NAME = "dask-container"
KUBERNETES_MAX_RESOURCE_NAME_LENGTH = 63
SCHEDULER_NAME_TEMPLATE = "{cluster_name}-scheduler"
MAX_CLUSTER_NAME_LEN = KUBERNETES_MAX_RESOURCE_NAME_LENGTH - len(
    SCHEDULER_NAME_TEMPLATE.format(cluster_name="")
)
VALID_CLUSTER_NAME = re.compile(
    rf"^(?=.{{,{MAX_CLUSTER_NAME_LEN}}}$)[a-z0-9]([-a-z0-9]*[a-z0-9])?$"
)
