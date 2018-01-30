from kubernetes import client

def _set_k8s_attribute(obj, attribute, value):
    """
    Set a specific value on a kubernetes object's attribute

    obj
        an object from Kubernetes Python API client
    attribute
        Either be name of a python client class attribute (api_client)
        or attribute name from JSON Kubernetes API (apiClient)
    value
        Can be anything (string, list, dict, k8s objects) that can be
        accepted by the k8s python client
    """
    # FIXME: We should be doing a recursive merge here

    current_value = None
    attribute_name = None
    # All k8s python client objects have an 'attribute_map' property
    # which has as keys python style attribute names (api_client)
    # and as values the kubernetes JSON API style attribute names
    # (apiClient). We want to allow this to use either.
    for python_attribute, json_attribute in obj.attribute_map.items():
        if json_attribute == attribute or python_attribute == attribute:
            attribute_name = python_attribute
            break
    else:
        raise ValueError('Attribute must be one of {}'.format(obj.attribute_map.values()))

    if hasattr(obj, attribute_name):
        current_value = getattr(obj, python_attribute)

    if current_value is not None:
        # This will ensure that current_value is something JSONable,
        # so a dict, list, or scalar
        current_value = client.ApiClient().sanitize_for_serialization(
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
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            elif isinstance(a[key], list) and isinstance(b[key], list):
                for idx, val in enumerate(b[key]):
                    a[key][idx] = merge(a[key][idx], b[key][idx], path + [str(key), str(idx)], update=update)
            elif update:
                a[key] = b[key]
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
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
        memory_limit=None,
        memory_request=None,
        cpu_limit=None,
        cpu_request=None,
):
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            labels=labels
        ),
        spec=client.V1PodSpec(
            restart_policy='Never',
            containers=[
                client.V1Container(
                    name='dask-worker',
                    image=image,
                    args=[
                        'dask-worker',
                        '$(DASK_SCHEDULER_ADDRESS)',
                        '--nthreads', str(threads_per_worker),
                    ],
                    env=[client.V1EnvVar(name=k, value=v)
                            for k, v in env.items()],
                )
            ]
        )
    )


    resources = client.V1ResourceRequirements(limits={}, requests={})

    if cpu_request:
        resources.requests['cpu'] = cpu_request
    if memory_request:
        resources.requests['memory'] = memory_request

    if cpu_limit:
        resources.limits['cpu'] = cpu_limit
    if memory_limit:
        resources.limits['memory'] = memory_limit

    pod.spec.containers[0].resources = resources

    for key, value in extra_container_config.items():
        _set_k8s_attribute(
            pod.spec.containers[0],
            key,
            value
        )

    for key, value in extra_pod_config.items():
        _set_k8s_attribute(
            pod.spec,
            key,
            value
        )
    return pod
