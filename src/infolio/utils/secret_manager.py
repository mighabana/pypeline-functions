import os
import threading
from abc import ABC, abstractmethod

import boto3

from infolio.utils.logger import get_logger

logger = get_logger(__name__)


class BaseSecretManager(ABC):
    """
    Abstract base class for all secret managers.

    Notes
    -----
    This class defines the required interface for any secret manager
    implementation. All subclasses must implement the ``get_secret`` method.
    """

    @abstractmethod
    def get_secret(self, key: str) -> str:
        """
        Retrieve a secret based on the provided key.

        Parameters
        ----------
        key : str
            The name or identifier of the secret to retrieve.

        Returns
        -------
        str
            The value of the requested secret.

        Raises
        ------
        KeyError
            If the secret is not found.
        """
        pass


class EnvSecretManager(BaseSecretManager):
    """Secret manager that retrieves secrets from environment variables."""

    def get_secret(self, key: str) -> str:
        """
        Retrieve a secret from environment variables.

        Parameters
        ----------
        key : str
            The environment variable key to fetch.

        Returns
        -------
        str
            The value of the environment variable if it exists, otherwise ``None``.

        Warns
        -----
        UserWarning
            If the key is not found in environment variables.
        """
        value = os.getenv(key)
        if value is None:
            logger.warning(f"Secret key '{key}' not found in environment variables.")
        return value


class AWSSecretManager(BaseSecretManager):
    """
    Secret manager that retrieves secrets from AWS Secrets Manager.

    Supports optional in-memory caching to reduce repeated calls
    to the AWS API.

    Parameters
    ----------
    region_name : str
        AWS region name (e.g., "us-east-1").
    aws_access_key_id : str, optional
        AWS access key ID. If not provided, boto3 will fall back to the
        default credential resolution chain.
    aws_secret_access_key : str, optional
        AWS secret access key. If not provided, boto3 will fall back to the
        default credential resolution chain.
    aws_session_token : str, optional
        AWS session token for temporary credentials.
    use_cache : bool, default=True
        If True, cache retrieved secrets in memory for reuse.

    Raises
    ------
    ImportError
        If boto3 is not installed.
    """

    def __init__(
        self,
        region_name: str,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        use_cache: bool = True,
    ) -> None:
        self.client = boto3.client(
            "secretsmanager",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
        self.use_cache = use_cache
        self._cache: dict[str, str] = {}
        self._lock = threading.Lock()

    def get_secret(self, key: str) -> str:
        """
        Retrieve a secret from AWS Secrets Manager.

        Parameters
        ----------
        key : str
            The name or identifier of the secret in AWS Secrets Manager.

        Returns
        -------
        str
            The value of the requested secret.

        Raises
        ------
        self.client.exceptions.ResourceNotFoundException
            If the secret does not exist in AWS Secrets Manager.
        Exception
            For any other unexpected boto3 errors.
        """
        if self.use_cache:
            with self._lock:
                if key in self._cache:
                    logger.debug(f"Cache hit for secret '{key}'.")
                    return self._cache[key]

        try:
            response = self.client.get_secret_value(SecretId=key)
            secret = response.get("SecretString", "")
            if self.use_cache:
                with self._lock:
                    self._cache[key] = secret
            logger.info(f"Successfully retrieved secret '{key}' from AWS Secrets Manager.")
            return secret
        except self.client.exceptions.ResourceNotFoundException:
            logger.error(f"Secret '{key}' not found in AWS Secrets Manager.")
            raise
        except Exception as e:
            logger.exception(f"Error retrieving secret '{key}': {e}")
            raise

    def refresh(self, key: str | None = None) -> None:
        """
        Refresh cached secrets by removing them from memory.

        Parameters
        ----------
        key : str, optional
            If provided, only the specified secret is removed from the cache.
            If ``None``, all cached secrets are cleared.

        Returns
        -------
        None
        """
        with self._lock:
            if key:
                if key in self._cache:
                    del self._cache[key]
                    logger.info(f"Secret '{key}' cache cleared")
            else:
                self._cache.clear()
                logger.info("All cached secrets cleared.")
