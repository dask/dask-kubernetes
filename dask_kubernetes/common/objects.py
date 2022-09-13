"""
Convenience functions for creating pod templates.
"""
from collections import namedtuple
import copy
from kubernetes import client
import json

from kubernetes.client.configuration import Configuration

from dask_kubernetes.constants import KUBECLUSTER_CONTAINER_NAME

_FakeResponse = namedtuple("_FakeResponse", ["data"])


class DummyApiClient(client.ApiClient):
    """A Dummy API client that is to be used solely for serialization/deserialization.

    This is to avoid starting a threadpool at initialization and for adapting the
    deserialize method to accept a python dictionary instead of a Response-like
    interface.
    """

    def __init__(self):
        self.configuration = Configuration.get_default_copy()

    def deserialize(self, dict_, klass):
        return super().deserialize(_FakeResponse(json.dumps(dict_)), klass)


SERIALIZATION_API_CLIENT = DummyApiClient()


def _set_k8s_attribute(obj, attribute, value):
    """
    Set a specific value on a kubernetes object's attribute

    obj
        an object from Kubernetes Python API client
    attribute
        Should be a Kubernetes API style attribute (with camelCase)
    value
        Can be anything (string, list, dict, k8s objects) that can be
        accepted by the k8s python client
    """
    current_value = None
    attribute_name = None
    # All k8s python client objects have an 'attribute_map' property
    # which has as keys python style attribute names (api_client)
    # and as values the kubernetes JSON API style attribute names
    # (apiClient). We want to allow users to use the JSON API style attribute
    # names only.
    for python_attribute, json_attribute in obj.attribute_map.items():
        if json_attribute == attribute:
            attribute_name = python_attribute
            break
    else:
        raise ValueError(
            "Attribute must be one of {}".format(obj.attribute_map.values())
        )

    if hasattr(obj, attribute_name):
        current_value = getattr(obj, attribute_name)

    if current_value is not None:
        # This will ensure that current_value is something JSONable,
        # so a dict, list, or scalar
        current_value = SERIALIZATION_API_CLIENT.sanitize_for_serialization(
            current_value
        )

    if isinstance(current_value, dict):
        # Deep merge our dictionaries!
        setattr(obj, attribute_name, merge_dictionaries(current_value, value))
    elif isinstance(current_value, list):
        # Just append lists
        setattr(obj, attribute_name, current_value + value)
    else:
        # Replace everything else
        setattr(obj, attribute_name, value)


def merge_dictionaries(a, b, path=None, update=True):
    """
    Merge two dictionaries recursively.

    From https://stackoverflow.com/a/25270947
    """
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dictionaries(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            elif isinstance(a[key], list) and isinstance(b[key], list):
                for idx, _ in enumerate(b[key]):
                    a[key][idx] = merge_dictionaries(
                        a[key][idx],
                        b[key][idx],
                        path + [str(key), str(idx)],
                        update=update,
                    )
            elif update:
                a[key] = b[key]
            else:
                raise Exception("Conflict at %s" % ".".join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def make_pod_spec(
    image,
    labels={},
    threads_per_worker=1,
    env={},
    extra_container_config={},
    extra_pod_config={},
    resources=None,
    memory_limit=None,
    memory_request=None,
    cpu_limit=None,
    cpu_request=None,
    gpu_limit=None,
    annotations={},
):
    """
    Create generic pod template from input parameters

    Parameters
    ----------
    image : str
        Docker image name
    labels : dict
        Dict of labels to pass to ``V1ObjectMeta``
    threads_per_worker : int
        Number of threads per each worker
    env : dict
        Dict of environment variables to pass to ``V1Container``
    extra_container_config : dict
        Extra config attributes to set on the container object
    extra_pod_config : dict
        Extra config attributes to set on the pod object
    resources : str
        Resources for task constraints like "GPU=2 MEM=10e9". Resources are applied
        separately to each worker process (only relevant when starting multiple
        worker processes. Passed to the `--resources` option in ``dask-worker``.
    memory_limit : int, float, or str
        Bytes of memory per process that the worker can use (applied to both
        ``dask-worker --memory-limit`` and ``spec.containers[].resources.limits.memory``).
        This can be:
            - an integer (bytes), note 0 is a special case for no memory management.
            - a float (bytes). Note: fraction of total system memory is not supported by k8s.
            - a string (like 5GiB or 5000M). Note: 'GB' is not supported by k8s.
            - 'auto' for automatically computing the memory limit.  [default: auto]
    memory_request : int, float, or str
        Like ``memory_limit`` (applied only to ``spec.containers[].resources.requests.memory``
        and ignored by ``dask-worker``).
    cpu_limit : float or str
        CPU resource limits (applied to ``spec.containers[].resources.limits.cpu``).
    cpu_request : float or str
        CPU resource requests (applied to ``spec.containers[].resources.requests.cpu``).
    gpu_limit : int
        GPU resource limits (applied to ``spec.containers[].resources.limits."nvidia.com/gpu"``).
    annotations : dict
        Dict of annotations passed to ``V1ObjectMeta``

    Returns
    -------
    pod : V1PodSpec

    Examples
    --------
    >>> make_pod_spec(image='ghcr.io/dask/dask:latest', memory_limit='4G', memory_request='4G')
    """
    args = [
        "dask-worker",
        "$(DASK_SCHEDULER_ADDRESS)",
        "--nthreads",
        str(threads_per_worker),
        "--death-timeout",
        "60",
    ]
    if memory_limit:
        args.extend(["--memory-limit", str(memory_limit)])
    if resources:
        args.extend(["--resources", str(resources)])
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(labels=labels, annotations=annotations),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name=KUBECLUSTER_CONTAINER_NAME,
                    image=image,
                    args=args,
                    env=[client.V1EnvVar(name=k, value=v) for k, v in env.items()],
                )
            ],
        ),
    )

    resources = client.V1ResourceRequirements(limits={}, requests={})

    if cpu_request:
        resources.requests["cpu"] = cpu_request
    if memory_request:
        resources.requests["memory"] = memory_request

    if cpu_limit:
        resources.limits["cpu"] = cpu_limit
    if gpu_limit:
        resources.limits["nvidia.com/gpu"] = gpu_limit
    if memory_limit:
        resources.limits["memory"] = memory_limit

    pod.spec.containers[0].resources = resources

    for key, value in extra_container_config.items():
        _set_k8s_attribute(pod.spec.containers[0], key, value)

    for key, value in extra_pod_config.items():
        _set_k8s_attribute(pod.spec, key, value)
    return pod


def make_pod_from_dict(dict_):
    containers = dict_.get("spec", {}).get("containers", [])
    for i, container in enumerate(containers):
        container.setdefault("name", f"dask-{i}")
    return SERIALIZATION_API_CLIENT.deserialize(dict_, client.V1Pod)


def make_service_from_dict(dict_):
    return SERIALIZATION_API_CLIENT.deserialize(dict_, client.V1Service)


def make_pdb_from_dict(dict_):
    return SERIALIZATION_API_CLIENT.deserialize(dict_, client.V1PodDisruptionBudget)


def clean_pod_template(
    pod_template, apply_default_affinity="preferred", pod_type="worker"
):
    """Normalize pod template"""
    pod_template = copy.deepcopy(pod_template)

    # Make sure metadata / labels / env objects exist, so they can be modified
    # later without a lot of `is None` checks
    if pod_template.metadata is None:
        pod_template.metadata = client.V1ObjectMeta()
    if pod_template.metadata.labels is None:
        pod_template.metadata.labels = {}

    if pod_template.spec.containers[0].env is None:
        pod_template.spec.containers[0].env = []

    # add default tolerations
    tolerations = [
        client.V1Toleration(
            key="k8s.dask.org/dedicated",
            operator="Equal",
            value=pod_type,
            effect="NoSchedule",
        ),
        # GKE currently does not permit creating taints on a node pool
        # with a `/` in the key field
        client.V1Toleration(
            key="k8s.dask.org_dedicated",
            operator="Equal",
            value=pod_type,
            effect="NoSchedule",
        ),
    ]

    if pod_template.spec.tolerations is None:
        pod_template.spec.tolerations = tolerations
    else:
        pod_template.spec.tolerations.extend(tolerations)

    # add default node affinity to k8s.dask.org/node-purpose=worker
    if apply_default_affinity != "none":
        # for readability
        affinity = pod_template.spec.affinity

        if affinity is None:
            affinity = client.V1Affinity()
        if affinity.node_affinity is None:
            affinity.node_affinity = client.V1NodeAffinity()

        # a common object for both a preferred and a required node affinity
        node_selector_term = client.V1NodeSelectorTerm(
            match_expressions=[
                client.V1NodeSelectorRequirement(
                    key="k8s.dask.org/node-purpose", operator="In", values=[pod_type]
                )
            ]
        )

        if apply_default_affinity == "required":
            if (
                affinity.node_affinity.required_during_scheduling_ignored_during_execution
                is None
            ):
                affinity.node_affinity.required_during_scheduling_ignored_during_execution = client.V1NodeSelector(
                    node_selector_terms=[]
                )
            affinity.node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms.append(
                node_selector_term
            )
        elif apply_default_affinity == "preferred":
            if (
                affinity.node_affinity.preferred_during_scheduling_ignored_during_execution
                is None
            ):
                affinity.node_affinity.preferred_during_scheduling_ignored_during_execution = (
                    []
                )
            preferred_scheduling_terms = [
                client.V1PreferredSchedulingTerm(
                    preference=node_selector_term, weight=100
                )
            ]
            affinity.node_affinity.preferred_during_scheduling_ignored_during_execution.extend(
                preferred_scheduling_terms
            )
        else:
            raise ValueError(
                'Attribute apply_default_affinity must be one of "none", "preferred", or "required".'
            )
        pod_template.spec.affinity = affinity

    return pod_template


def clean_service_template(service_template):
    """Normalize service template and check for type errors"""

    service_template = copy.deepcopy(service_template)

    # Make sure metadata / labels objects exist, so they can be modified
    # later without a lot of `is None` checks
    if service_template.metadata is None:
        service_template.metadata = client.V1ObjectMeta()
    if service_template.metadata.labels is None:
        service_template.metadata.labels = {}

    return service_template


def clean_pdb_template(pdb_template):
    """Normalize pdb template and check for type errors"""

    pdb_template = copy.deepcopy(pdb_template)

    # Make sure metadata / labels objects exist, so they can be modified
    # later without a lot of `is None` checks
    if pdb_template.metadata is None:
        pdb_template.metadata = client.V1ObjectMeta()
    if pdb_template.metadata.labels is None:
        pdb_template.metadata.labels = {}
    if pdb_template.spec.selector is None:
        pdb_template.spec.selector = client.V1LabelSelector()

    return pdb_template
