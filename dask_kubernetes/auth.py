"""
Defines different methods to configure a connection to a Kubernetes cluster.
"""
import asyncio
import base64
import contextlib
import copy
import datetime
import json
import logging
import os

import kubernetes
import kubernetes_asyncio

from kubernetes_asyncio.client import Configuration
from kubernetes_asyncio.config.kube_config import KubeConfigLoader, KubeConfigMerger
from kubernetes_asyncio.config.google_auth import google_auth_credentials
from kubernetes_asyncio.config.dateutil import parse_rfc3339

logger = logging.getLogger(__name__)

tzUTC = datetime.timezone.utc


class AutoRefreshKubeConfigLoader(KubeConfigLoader):
    """
    Extends KubeConfigLoader, automatically attempts to refresh authentication
    credentials before they expire.
    """

    def __init__(self, *args, **kwargs):
        super(AutoRefreshKubeConfigLoader, self).__init__(*args, **kwargs)

        self._retry_count = 0
        self._max_retries = float("Inf")
        self.auto_refresh = True
        self.refresh_task = None
        self.last_refreshed = None
        self.token_expire_ts = None

    def __del__(self):
        self.auto_refresh = False

    def extract_oid_expiration_from_provider(self, provider):
        """
        Extracts the expiration datestamp for the provider token
        Parameters
        ----------
        provider : authentication provider dictionary.

        Returns
        -------
        expires : expiration timestamp
        """
        parts = provider["config"]["id-token"].split(".")

        if len(parts) != 3:
            raise ValueError("oidc: JWT tokens should contain 3 period-delimited parts")

        id_token = parts[1]
        # Re-pad the unpadded JWT token
        id_token += (4 - len(id_token) % 4) * "="
        jwt_attributes = json.loads(base64.b64decode(id_token).decode("utf8"))
        expires = jwt_attributes.get("exp")

        return expires

    async def create_refresh_task_from_expiration_timestamp(self, expiration_timestamp):
        """
        Takes an expiration timestamp, and creates a refresh task to ensure that the token
        does not expire.

        Parameters
        ----------
        expiration_timestamp : time at which the current authentication token will expire

        Returns
        -------
        N/A
        """
        # Set our token expiry to be actual expiry - 20%
        expiry = parse_rfc3339(expiration_timestamp)
        expiry_delta = datetime.timedelta(
            seconds=(expiry - datetime.datetime.now(tz=tzUTC)).total_seconds()
        )
        scaled_expiry_delta = datetime.timedelta(
            seconds=0.8 * expiry_delta.total_seconds()
        )

        self.refresh_task = asyncio.create_task(
            self.refresh_after(
                when=scaled_expiry_delta.total_seconds(), reschedule_on_failure=True
            ),
            name="dask_auth_auto_refresh",
        )

        self.last_refreshed = datetime.datetime.now(tz=tzUTC)
        self.token_expire_ts = self.last_refreshed + scaled_expiry_delta

    async def refresh_after(self, when, reschedule_on_failure=False):
        """
        Refresh kuberenetes authentication
        Parameters
        ----------
        when : Seconds before we should refresh. This should be set to some delta before
            the actual token expiration time, or you will likely see authentication race
            / failure conditions.

        reschedule_on_failure : If the refresh task fails, re-try in 30 seconds, until
            _max_retries is exceeded, then raise an exception.
        """

        if not self.auto_refresh:
            return

        logger.debug(
            msg=f"Refresh_at coroutine sleeping for "
            f"{int(when // 60)} minutes {(when % 60):0.2f} seconds."
        )
        try:
            await asyncio.sleep(when)
            if self.provider == "gcp":
                await self.refresh_gcp_token()
            elif self.provider == "oidc":
                await self.refresh_oid_token()
                return
            elif "exec" in self._user:
                logger.warning(msg="Auto-refresh doesn't support generic ExecProvider")
                return

        except Exception as e:
            logger.warning(
                msg=f"Authentication refresh failed for provider '{self.provider}.'",
                exc_info=e,
            )
            if not reschedule_on_failure or self._retry_count > self._max_retries:
                raise

            logger.warning(msg=f"Retrying '{self.provider}' in 30 seconds.")
            self._retry_count += 1
            self.refresh_task = asyncio.create_task(self.refresh_after(30))

    async def refresh_oid_token(self):
        """
        Adapted from kubernetes_asyncio/config/kube_config:_load_oid_token

        Refreshes the existing oid token, if necessary, and creates a refresh task
        that will keep the token from expiring.

        Returns
        -------
        """
        provider = self._user["auth-provider"]

        logger.debug("Refreshing OID token.")

        if "config" not in provider:
            raise ValueError("oidc: missing configuration")

        if (not self.token_expire_ts) or (
            self.token_expire_ts <= datetime.datetime.now(tz=tzUTC)
        ):
            await self._refresh_oidc(provider)
            expires = self.extract_oid_expiration_from_provider(provider=provider)

            await self.create_refresh_task_from_expiration_timestamp(
                expiration_timestamp=expires
            )

            self.token = "Bearer {}".format(provider["config"]["id-token"])

    async def refresh_gcp_token(self):
        """
        Adapted from kubernetes_asyncio/config/kube_config:load_gcp_token

        Refreshes the existing gcp token, if necessary, and creates a refresh task
        that will keep the token from expiring.

        Returns
        -------
        """
        if "config" not in self._user["auth-provider"]:
            self._user["auth-provider"].value["config"] = {}

        config = self._user["auth-provider"]["config"]

        if (not self.token_expire_ts) or (
            self.token_expire_ts <= datetime.datetime.now(tz=tzUTC)
        ):

            logger.debug("Refreshing GCP token.")
            if self._get_google_credentials is not None:
                if asyncio.iscoroutinefunction(self._get_google_credentials):
                    credentials = await self._get_google_credentials()
                else:
                    credentials = self._get_google_credentials()
            else:
                # config is read-only.
                extra_args = " --force-auth-refresh"
                _config = {
                    "cmd-args": config["cmd-args"] + extra_args,
                    "cmd-path": config["cmd-path"],
                }
                credentials = await google_auth_credentials(_config)

            config.value["access-token"] = credentials.token
            config.value["expiry"] = credentials.expiry

            # Set our token expiry to be actual expiry - 20%
            await self.create_refresh_task_from_expiration_timestamp(
                expiration_timestamp=config.value["expiry"]
            )

            if self._config_persister:
                self._config_persister(self._config.value)

            self.token = "Bearer %s" % config["access-token"]

    async def _load_oid_token(self):
        """
        Overrides KubeConfigLoader implementation.
        Returns
        -------
        Auth token
        """
        await self.refresh_oid_token()

        return self.token

    async def load_gcp_token(self):
        """
        Override KubeConfigLoader implementation so that we can keep track of the expiration timestamp
        and automatically refresh auth tokens.

        Returns
        -------
        GCP access token
        """
        await self.refresh_gcp_token()

        return self.token


class AutoRefreshConfiguration(Configuration):
    """
    Extends kubernetes_async Configuration to support automatic token refresh.
    Lets us keep track of the original loader object, which can be used
    to regenerate the authentication token.
    """

    def __init__(self, loader, refresh_frequency=None, *args, **kwargs):
        super(AutoRefreshConfiguration, self).__init__(*args, **kwargs)

        # Set refresh api callback
        self.refresh_api_key_hook = self.refresh_api_key
        self.last_refreshed = datetime.datetime.now(tz=tzUTC)
        self.loader = loader

    # Adapted from kubernetes_asyncio/client/configuration.py:__deepcopy__
    def __deepcopy__(self, memo):
        """
        Modified so that we don't try to deep copy the loader off the config
        """
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k not in ("logger", "logger_file_handler", "loader"):
                setattr(result, k, copy.deepcopy(v, memo))

        # shallow copy loader object
        result.loader = self.loader
        # shallow copy of loggers
        result.logger = copy.copy(self.logger)
        # use setters to configure loggers
        result.logger_file = self.logger_file
        result.debug = self.debug

        return result

    def refresh_api_key(self, client_configuration):
        """
        Checks to see if the loader has updated the authentication token. If it
        has, the token is copied from the loader into the current configuration.

        This function is assigned to Configuration.refresh_api_key_hook, and will
        fire when entering get_api_key_with_prefix, before the api_key is retrieved.
        """
        if self.loader.last_refreshed is not None:
            if (
                self.last_refreshed is None
                or self.last_refreshed < self.loader.last_refreshed
            ):
                logger.debug("Entering refresh_api_key_hook")
                client_configuration.api_key[
                    "authorization"
                ] = client_configuration.loader.token
                self.last_refreshed = datetime.datetime.now(tz=tzUTC)


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

    async def load(self):
        """
        Load Kubernetes configuration and set as default

        Raises
        ------

        kubernetes.client.KubeConfigException
        """
        raise NotImplementedError()

    @staticmethod
    async def load_first(auth=None):
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
                raise kubernetes_asyncio.config.ConfigException(
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
                await auth_instance.load()
            except (
                kubernetes_asyncio.config.ConfigException,
                kubernetes.config.ConfigException,
            ) as exc:
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

    async def load(self):
        kubernetes.config.load_incluster_config()
        kubernetes_asyncio.config.load_incluster_config()


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

    async def load(self):
        with contextlib.suppress(KeyError):
            if self.config_file is None:
                self.config_file = os.path.abspath(
                    os.path.expanduser(os.environ.get("KUBECONFIG", "~/.kube/config"))
                )

        await self.load_kube_config()

    # Adapted from from kubernetes_asyncio/config/kube_config.py:get_kube_config_loader_for_yaml_file
    def get_kube_config_loader_for_yaml_file(self):
        kcfg = KubeConfigMerger(self.config_file)
        config_persister = None
        if self.persist_config:
            config_persister = kcfg.save_changes()

        return AutoRefreshKubeConfigLoader(
            config_dict=kcfg.config,
            config_base_path=None,
            config_persister=config_persister,
        )

    # Adapted from kubernetes_asyncio/config/kube_config.py:load_kube_config
    async def load_kube_config(self):
        # Create a config loader, this will automatically refresh our credentials before they expire
        loader = self.get_kube_config_loader_for_yaml_file()

        # Grab our async + callback aware configuration
        config = AutoRefreshConfiguration(loader)

        await loader.load_and_set(config)
        Configuration.set_default(config)


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

    async def load(self):
        kubernetes.client.Configuration.set_default(self.config)
        await kubernetes_asyncio.client.Configuration.set_default(self.config)


ClusterAuth.DEFAULT = [InCluster(), KubeConfig()]
