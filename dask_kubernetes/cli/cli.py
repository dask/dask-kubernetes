import json
import time

import click
import yaml
from rich.console import Console

from dask_kubernetes.operator import KubeCluster, make_cluster_spec

console = Console()


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
@click.option(
    "--jupyter",
    type=bool,
    default=False,
    is_flag=True,
    help="Start Jupyter on the scheduler (default 'False')",
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


@main.command(help="Port-forward the scheduler of a DaskCluster resource")
@click.argument("cluster")
def port_forward(cluster):
    with console.status(f"Connecting to cluster {cluster}"):
        try:
            kcluster = KubeCluster.from_name(
                cluster, shutdown_on_close=False, quiet=True
            )
        except ValueError:
            raise click.ClickException(f"No such cluster {cluster}")
    try:
        console.print(f"Scheduler at: [magenta][not bold]{kcluster.scheduler_address}")
        console.print(f"Dashboard at: [cyan][not bold]{kcluster.dashboard_link}")
        if kcluster.jupyter:
            console.print(f"Jupyter at: [orange3][not bold]{kcluster.jupyter_link}")
        console.print("Press ctrl+c to exit", style="bright_black")
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        console.print("Shutting down port-forward")
        kcluster.close()
