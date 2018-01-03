import logging
import tornado
import argparse
from urllib.parse import urlparse


from distributed import Client
from distributed.deploy import Adaptive
from distributed.utils import LoopRunner, sync
from distributed.scheduler import Scheduler
from kubernetes import client, config


class KubeCluster(object):
    def __init__(
            self,
            name,
            namespace,
            worker_image,
            worker_labels,
            scheduler_address
    ):
        self.log = logging.getLogger("distributed.deploy.adaptive")

        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self.api = client.CoreV1Api()

        self.namespace = namespace
        self.name = name
        self.worker_image = worker_image
        self.scheduler_address = scheduler_address
        self.worker_labels = worker_labels.copy()

        # Default labels that can't be overwritten
        self.worker_labels['org.pydata.dask/cluster-name'] = name
        self.worker_labels['app'] = 'dask'
        self.worker_labels['component'] = 'dask-worker'

    def _make_pod(self):
        return client.V1Pod(
            metadata = client.V1ObjectMeta(
                generate_name=self.name + '-',
                labels=self.worker_labels
            ),
            spec = client.V1PodSpec(
                restart_policy = 'Never',
                containers = [
                    client.V1Container(
                        name = 'dask-worker',
                        image = self.worker_image,
                        env = [
                            client.V1EnvVar(
                                name = 'POD_IP',
                                value_from = client.V1EnvVarSource(
                                    field_ref = client.V1ObjectFieldSelector(
                                        field_path = 'status.podIP'
                                    )
                                )
                            ),
                            client.V1EnvVar(
                                name = 'POD_NAME',
                                value_from = client.V1EnvVarSource(
                                    field_ref = client.V1ObjectFieldSelector(
                                        field_path = 'metadata.name'
                                    )
                                )
                            ),
                        ],
                        args = [
                            'dask-worker',
                            self.scheduler_address,
                            '--nprocs', '1',
                            '--nthreads', '1',
                            '--host', '$(POD_IP)',
                            '--name', '$(POD_NAME)',
                        ]
                    )
                ]
            )
        )


    def _format_labels(self, labels):
        return ','.join(['{}={}'.format(k, v) for k, v in self.worker_labels.items()])

    def scale_up(self, n, **kwargs):
        """
        Make sure we have n dask-workers available for this cluster
        """
        pods = self.api.list_namespaced_pod(
            self.namespace,
            label_selector=self._format_labels(self.worker_labels)
        )
        if(len(pods.items) == n):
            # We already have the number of workers we need!
            return
        for _ in range(n - len(pods.items)):
            created = self.api.create_namespaced_pod(self.namespace, self._make_pod())

        # FIXME: Wait for this to be ready before returning!

    def scale_down(self, workers):
        """
        When the worker process exits, Kubernetes leaves the pods in a completed
        state. Kill them when we are asked to.
        """
        # Get the existing worker pods
        pods = self.api.list_namespaced_pod(self.namespace, label_selector=self._format_labels(self.worker_labels))

        # Work out pods that we are going to delete
        # Each worker to delete is given in the form "tcp://<worker ip>:<port>"
        # Convert this to a set of IPs
        ips = set(urlparse(worker).hostname for worker in workers)
        to_delete = [
            p for p in pods.items
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
                self.log.info('Deleted pod: %s', pod.metadata.name)
            except client.rest.ApiException as e:
                # If a pod has already been removed, just ignore the error
                if e.status != 404:
                    raise


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('name', help='Name of the cluster')
    argparser.add_argument('namespace', help='Namespace to spawn cluster in')
    argparser.add_argument(
        '--worker-image',
        default='daskdev/dask:latest',
        help='Worker pod image. Should have same version of python as client')

    args = argparser.parse_args()

    scheduler = Scheduler()
    scheduler.start(('0.0.0.0', '8786'))
    cluster = KubeCluster(
        args.name,
        args.namespace,
        args.worker_image,
        {},
        scheduler.address
    )
    adapative_cluster = Adaptive(scheduler, cluster)
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()
