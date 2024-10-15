"""Validate resources"""

from dask_kubernetes.constants import MAX_CLUSTER_NAME_LEN, VALID_CLUSTER_NAME
from dask_kubernetes.exceptions import ValidationError


def validate_cluster_name(cluster_name: str) -> None:
    """Raise exception if cluster name is too long and/or has invalid characters"""
    if not VALID_CLUSTER_NAME.match(cluster_name):
        raise ValidationError(
            message=(
                f"The DaskCluster {cluster_name} is invalid: a lowercase RFC 1123 subdomain must "
                "consist of lower case alphanumeric characters, '-' or '.', and must start "
                "and end with an alphanumeric character. DaskCluster name must also be under "
                f"{MAX_CLUSTER_NAME_LEN} characters."
            )
        )
