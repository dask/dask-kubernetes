import subprocess
import json
import re
import time

from distributed.core import rpc
from distributed.deploy import Cluster

from distributed.utils import Log, Logs

from dask_kubernetes.auth import ClusterAuth
from .daskcluster import (
    build_cluster_spec,
    build_worker_group_spec,
)


class KubeCluster2(Cluster):
    """Launch a Dask Cluster on Kubernetes using the Operator"""

    def __init__(
        self,
        name,
        namespace="default",
        image="daskdev/dask:latest",
        replicas=3,
        resources={},
        env={},
        asynchronous=False,
        auth=ClusterAuth.DEFAULT,
        **kwargs,
    ):
        self.name = name
        self.namespace = namespace
        self.core_api = None
        self.custom_api = None
        self.image = image
        self.replicas = replicas
        self.resources = resources
        self.env = env
        self.auth = auth

        # TODO: Check if cluster already exists
        super().__init__(asynchronous=asynchronous, **kwargs)
        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def _start(self):
        data = build_cluster_spec(
            self.name, self.image, self.replicas, self.resources, self.env
        )
        with open("data.json", "w") as jfile:
            json.dump(data, jfile)
        cluster = subprocess.check_output(
            [
                "kubectl",
                "apply",
                "-f",
                "data.json",
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )
        time.sleep(5)
        pf = subprocess.Popen(
            [
                "kubectl",
                "port-forward",
                f"service/{self.name}-cluster",
                "8786:8786",
                "-n",
                f"{self.namespace}",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        time.sleep(2)
        self.scheduler_comm = rpc("localhost:8786")
        await super()._start()

    async def _get_scheduler_address(self):
        address = str(
            re.search(
                r"Scheduler at:   (.*)\\ndistributed.scheduler",
                self.get_logs()["simple-cluster-scheduler"],
            )
        ).split(" ")[-1][:-11]
        return {address}

    def get_logs(self):
        """Get logs for Dask scheduler and workers."""
        return self.sync(self._get_logs)

    async def _get_logs(self):
        logs = Logs()
        pods = subprocess.check_output(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )
        pod_names = map(
            lambda s: s.split(" ")[0],
            [s for s in str(pods).split("\n") if "scheduler" or "worker" in s][1:-1],
        )
        for name in pod_names:
            log = Log(
                subprocess.check_output(
                    [
                        "kubectl",
                        "logs",
                        f"{name}",
                        "-n",
                        self.namespace,
                    ]
                )
            )
            logs[name] = log
        return logs

    def add_worker_group(self, name, n=3):
        data = build_worker_group_spec(name, self.image, n, self.resources, self.env)
        with open("data.json", "w") as jfile:
            json.dump(data, jfile)
        workers = subprocess.check_output(
            [
                "kubectl",
                "apply",
                "-f",
                "data.json",
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )

    def scale(self, worker_group, n):
        scaler = subprocess.check_output(
            [
                "kubectl",
                "scale",
                f"--replicas={n}",
                "daskworkergroup",
                f"{worker_group}-worker-group",
                "-n",
                self.namespace,
            ],
            encoding="utf-8",
        )

    def adapt(self, minimum, maximum):
        # TODO: Implement when add adaptive kopf handler
        raise NotImplementedError()

    @classmethod
    def from_name(cls, name, **kwargs):
        """Create an instance of this class to represent an existing cluster by name."""
        # TODO: Implement when switch to k8s python client
        raise NotImplementedError()
