"""Utility functions."""
import os
import shutil
import string

import kubernetes_asyncio as kubernetes


def format_labels(labels):
    """Convert a dictionary of labels into a comma separated string"""
    if labels:
        return ",".join(["{}={}".format(k, v) for k, v in labels.items()])
    else:
        return ""


def escape(s):
    valid_characters = string.ascii_letters + string.digits + "-"
    return "".join(c for c in s if c in valid_characters).lower()


def get_current_namespace():
    """
    Get current namespace if running in a k8s cluster

    If not in a k8s cluster with service accounts enabled, default to
    'default'

    Taken from https://github.com/jupyterhub/kubespawner/blob/master/kubespawner/spawner.py#L125
    """
    ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    try:
        _, active_context = kubernetes.config.list_kube_config_contexts()
        return active_context["context"]["namespace"]
    except KeyError:
        return "default"


def check_dependency(dependency):
    if shutil.which(dependency) is None:
        raise RuntimeError(
            f"Missing dependency {dependency}. "
            f"Please install {dependency} following the instructions for your OS. "
        )
