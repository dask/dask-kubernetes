"""
Defines different methods to configure a connection to a Kubernetes cluster.
"""
import logging

import kubernetes

logger = logging.getLogger(__name__)


class ClusterAuth(object):
    """
    An abstract base class for methods for configuring a connection to a
    Kubernetes API server.

    Examples
    --------
    >>> from dask_kubernetes import KubeConfig
    >>> auth = KubeConfig(context='minikube')

    >>> from dask_kubernetes import KubeAuth
    >>> auth = KubeAuth(host='https://localhost', username='superuser', password='pass')

    """

    def load(self):
        """
        Load Kubernetes configuration and set as default

        Raises
        ------

        kubernetes.client.KubeConfigException
        """
        raise NotImplementedError()

    @staticmethod
    def load_first(auth=None):
        """
        Load the first valid configuration in the list *auth*. A single
        configuration method can be passed.

        Parameters
        ----------

        auth: List[ClusterAuth] (optional)
            Configuration methods to attempt in order.  Defaults to
            ``[InCluster(), KubeConfig()]``.

        """
        if auth is None:
            auth = ClusterAuth.DEFAULT
        elif isinstance(auth, ClusterAuth):
            auth = [auth]
        elif isinstance(auth, list):
            if not auth:
                raise kubernetes.config.ConfigException(
                    "No authorization methods were provided"
                )
        else:
            msg = (
                "Invalid authorization method provided. See ClusterAuth "
                "docstring for ways to specify authentication methods"
            )
            raise ValueError(msg)

        auth_exc = None
        for auth_instance in auth:
            try:
                auth_instance.load()
            except kubernetes.config.ConfigException as exc:
                logger.debug(
                    "Failed to load configuration with %s method: %s",
                    auth_instance.__class__,
                    exc,
                )
                auth_exc = exc
            else:
                break
        else:
            raise auth_exc


class InCluster(ClusterAuth):
    """Configure the Kubernetes connection from a container's environment.

    This authentication method is intended for use when the client is running
    in a container started by Kubernetes with an authorized service account.
    This loads the mounted service account token and discovers the Kubernetes
    API via Kubernetes service discovery.
    """

    def load(self):
        kubernetes.config.load_incluster_config()


class KubeConfig(ClusterAuth):
    """Configure the Kubernetes connection from a kubeconfig file.

    Parameters
    ----------
    config_file: str (optional)
        The path of the kubeconfig file to load.  Defaults to the value of the
        ``KUBECONFIG`` environment variable, or the string ``~/.kube/config``.
    context: str (optional)
        The kubeconfig context to use.  Defaults to the value of ``current-context``
        in the configuration file.
    persist_config: bool (optional)
        Whether changes to the configuration will be saved back to disk (e.g.
        GCP token refresh).  Defaults to ``True``.

    """

    def __init__(self, config_file=None, context=None, persist_config=True):
        self.config_file = config_file
        self.context = context
        self.persist_config = persist_config

    def load(self):
        kubernetes.config.load_kube_config(
            self.config_file, self.context, None, self.persist_config
        )


class KubeAuth(ClusterAuth):
    """Configure the Kubernetes connection explicitly.

    Parameters
    ----------
    host: str
        The base URL of the Kubernetes host to connect
    username: str (optional)
        Username for HTTP basic authentication
    password: str (optional)
        Password for HTTP basic authentication
    debug: bool (optional)
        Debug switch
    verify_ssl: bool (optional)
        Set this to false to skip verifying SSL certificate when calling API
        from https server.  Defaults to ``True``.
    ssl_ca_cert: str (optional)
        Set this to customize the certificate file to verify the peer.
    cert_file: str (optional)
        Client certificate file
    key_file: str (optional)
        Client key file
    assert_hostname: bool (optional)
        Set this to True/False to enable/disable SSL hostname verification.
        Defaults to True.
    proxy: str (optional)
        URL for a proxy to connect through
    """

    def __init__(self, host, **kwargs):
        # We need to create a new configuration in this way, because if we just
        # instantiate a new Configuration object we will get the default
        # values.
        config = type.__call__(kubernetes.client.Configuration)
        config.host = host
        for key, value in kwargs.items():
            setattr(config, key, value)
        self.config = config

    def load(self):
        kubernetes.client.Configuration.set_default(self.config)


ClusterAuth.DEFAULT = [InCluster(), KubeConfig()]
