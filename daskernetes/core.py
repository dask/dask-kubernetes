import logging
import socket
import argparse
from urllib.parse import urlparse
from weakref import finalize

from distributed import Client
from distributed.deploy import LocalCluster
from kubernetes import client, config

logger = logging.getLogger(__name__)


class KubeCluster(object):
    def __init__(
            self,
            name='dask',
            namespace='dask',
            worker_image='daskdev/dask:latest',
            worker_labels=None,
            n_workers=0,
            threads_per_worker=1,
            host='0.0.0.0',
            port=8786,
            **kwargs,
    ):
        self.cluster = LocalCluster(ip=host or socket.gethostname(),
                                    scheduler_port=port,
                                    n_workers=0, **kwargs)

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self.api = client.CoreV1Api()

        self.namespace = namespace
        self.name = name  # TODO: should this be unique by default?
        self.worker_image = worker_image
        self.worker_labels = (worker_labels or {}).copy()
        self.threads_per_worker = threads_per_worker

        # Default labels that can't be overwritten
        self.worker_labels['org.pydata.dask/cluster-name'] = name
        self.worker_labels['app'] = 'dask'
        self.worker_labels['component'] = 'dask-worker'

        finalize(self, cleanup_pods, self.namespace, self.worker_labels)

        if n_workers:
            self.scale_up(n_workers)

    @property
    def scheduler(self):
        return self.cluster.scheduler

    @property
    def scheduler_address(self):
        return self.scheduler.address

    def _make_pod(self):
        return client.V1Pod(
            metadata=client.V1ObjectMeta(
                generate_name=self.name + '-',
                labels=self.worker_labels
            ),
            spec=client.V1PodSpec(
                restart_policy='Never',
                containers=[
                    client.V1Container(
                        name='dask-worker',
                        image=self.worker_image,
                        args=[
                            'dask-worker',
                            self.scheduler_address,
                            '--nthreads', str(self.threads_per_worker),
                        ]
                    )
                ]
            )
        )

    def pods(self):
        return self.api.list_namespaced_pod(
            self.namespace,
            label_selector=format_labels(self.worker_labels)
        ).items

    def logs(self, pod):
        return self.api.read_namespaced_pod_log(pod.metadata.name,
                                                pod.metadata.namespace)

    def scale_up(self, n, **kwargs):
        """
        Make sure we have n dask-workers available for this cluster
        """
        pods = self.pods()
        if(len(pods) == n):
            # We already have the number of workers we need!
            return
        for _ in range(n - len(pods)):
            self.api.create_namespaced_pod(self.namespace, self._make_pod())

        # FIXME: Wait for this to be ready before returning!

    def scale_down(self, workers):
        """
        When the worker process exits, Kubernetes leaves the pods in a completed
        state. Kill them when we are asked to.
        """
        # Get the existing worker pods
        pods = self.pods()

        # Work out pods that we are going to delete
        # Each worker to delete is given in the form "tcp://<worker ip>:<port>"
        # Convert this to a set of IPs
        ips = set(urlparse(worker).hostname for worker in workers)
        to_delete = [
            p for p in pods
            # Every time we run, purge any completed pods as well as the specified ones
            if p.status.phase == 'Succeeded' or p.status.pod_ip in ips
        ]
        if not to_delete:
            return
        for pod in to_delete:
            try:
                self.api.delete_namespaced_pod(
                    pod.metadata.name,
                    self.namespace,
                    client.V1DeleteOptions()
                )
                logger.info('Deleted pod: %s', pod.metadata.name)
            except client.rest.ApiException as e:
                # If a pod has already been removed, just ignore the error
                if e.status != 404:
                    raise

    def __enter__(self):
        return self

    def close(self):
        self.scale_down(self.cluster.scheduler.workers)
        self.cluster.close()

    def __exit__(self, type, value, traceback):
        cleanup_pods(self.namespace, self.worker_labels)
        self.cluster.__exit__(type, value, traceback)

    def __del__(self):
        self.close()  # TODO: do this more cleanly


def cleanup_pods(namespace, worker_labels):
    api = client.CoreV1Api()
    pods = api.list_namespaced_pod(namespace, label_selector=format_labels(worker_labels))
    for pod in pods.items:
        try:
            api.delete_namespaced_pod(pod.metadata.name, namespace,
                                      client.V1DeleteOptions())
            logger.info('Deleted pod: %s', pod.metadata.name)
        except client.rest.ApiException as e:
            # ignore error if pod is already removed
            if e.status != 404:
                raise


def format_labels(labels):
    return ','.join(['{}={}'.format(k, v) for k, v in labels.items()])


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('name', help='Name of the cluster')
    argparser.add_argument('namespace', help='Namespace to spawn cluster in')
    argparser.add_argument(
        '--worker-image',
        default='daskdev/dask:latest',
        help='Worker pod image. Should have same version of python as client')

    args = argparser.parse_args()

    cluster = KubeCluster(
        args.name,
        args.namespace,
        args.worker_image,
        {},
        n_workers=1,
    )
    client = Client(cluster)
    print(client.submit(lambda x: x + 1, 10).result())


if __name__ == '__main__':
    main()
