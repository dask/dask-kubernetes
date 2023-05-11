import click
import yaml
import json

from dask_kubernetes.operator import make_cluster_spec


class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


@click.group(
    name="kubernetes",
    help="Utilities for working with Dask Kubernetes",
    context_settings=dict(ignore_unknown_options=True),
)
def main():
    pass


@main.group(help="Generate Kubernetes specs for Dask resources")
def gen():
    pass


@gen.command(
    help="Generate a DaskCluster spec, see ``dask_kubernetes.operator.make_cluster_spec``."
)
@click.option("--name", type=str, required=True, help="Name of the DaskCluster")
@click.option(
    "--image",
    type=str,
    default=None,
    help="Container image to use (default 'ghcr.io/dask/dask:latest')",
)
@click.option("--n-workers", type=int, default=3, help="Number of workers")
@click.option("--resources", type=str, default=None, help="Resource constraints")
@click.option(
    "--env", "-e", type=str, default=None, multiple=True, help="Environment variables"
)
@click.option(
    "--worker-command",
    type=str,
    default=None,
    help="Worker command (default 'dask-worker')",
)
@click.option(
    "--scheduler-service-type",
    type=str,
    default=None,
    help="Service type for scheduler (default 'ClusterIP')",
)
def cluster(**kwargs):
    if "resources" in kwargs and kwargs["resources"] is not None:
        kwargs["resources"] = json.loads(kwargs["resources"])

    if "env" in kwargs:
        tmp = {}
        for item in kwargs["env"]:
            key, val = item.split("=")
            tmp[key] = val
        kwargs["env"] = tmp

    filtered_kwargs = {k: v for k, v in kwargs.items() if v is not None}

    click.echo(
        yaml.dump(
            make_cluster_spec(**filtered_kwargs),
            Dumper=NoAliasDumper,
        )
    )
