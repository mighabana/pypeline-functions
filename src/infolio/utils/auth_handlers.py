from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import requests

from infolio.utils.logger import get_logger

if TYPE_CHECKING:
    from .api_client import ApiClient  # Adjust path as needed

logger = get_logger(__name__)

# TODO: implement logging


class AuthHandler(ABC):
    """
    Abstract base class for authentication handlers.

    Notes
    -----
    Subclasses must implement the `reauthenticate` method.
    """

    @abstractmethod
    def reauthenticate(self, client: ApiClient) -> bool:
        """
        Refresh or obtain authentication credentials.

        Parameters
        ----------
        client : ApiClient
            The API client instance using this authentication handler.

        Returns
        -------
        bool
            True if authentication succeeded, False otherwise.
        """
        pass


class ApiKeyAuthHandler(AuthHandler):
    """Simple API Key handler that injects the key into headers."""

    def __init__(self, key_name: str, api_key: str) -> None:
        """
        Initialize ApiKeyAuthHandler with a header key and API key.

        Parameters
        ----------
        key_name : str
            The header key to use for the API key.
        api_key : str
            The API key value to inject into the headers.
        """
        self.key_name = key_name
        self.api_key = api_key

    def reauthenticate(self, client: ApiClient) -> bool:
        """
        Inject the API key into the client headers.

        Parameters
        ----------
        client : ApiClient
            The API client instance that will use this API key.

        Returns
        -------
        bool
            Always returns True since API key injection is deterministic.
        """
        client.default_headers[self.key_name] = self.api_key
        return True


class BearerTokenAuthHandler(AuthHandler):
    """
    Bearer Token handler for APIs that require client_credentials flow.

    Typically used when you need to call a `token_url` with `client_id`
    and `client_secret` to obtain an access token.
    """

    def __init__(self, token_url: str, client_id: str, client_secret: str) -> None:
        """
        Initialize BearerTokenAuthHandler with token endpoint and client credentials.

        Parameters
        ----------
        token_url : str
            The URL to obtain the bearer token.
        client_id : str
            The client ID for the API.
        client_secret : str
            The client secret for the API.
        """
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret

    def reauthenticate(self, client: ApiClient) -> bool:
        """
        Obtain a bearer token and update the client headers.

        Parameters
        ----------
        client : ApiClient
            The API client instance that will use the bearer token.

        Returns
        -------
        bool
            True if token retrieval succeeded, False otherwise.
        """
        try:
            response = requests.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=5,
            )
            response.raise_for_status()
            token_data = response.json()
            client.default_headers["Authorization"] = f"Bearer {token_data['access_token']}"
            return True
        except Exception:
            return False


class OAuth2AuthHandler(AuthHandler):
    """
    OAuth2 handler that uses a refresh token to obtain a new access token.

    Parameters
    ----------
    token_url : str
        The OAuth2 token endpoint.
    client_id : str
        The client ID issued by the API provider.
    client_secret : str
        The client secret issued by the API provider.
    refresh_token : str
        The long-lived refresh token to exchange for new access tokens.
    scope : str, optional
        The scope(s) to request. Some providers require explicitly passing it.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        scope: str | None = None,
    ) -> None:
        """
        Initialize OAuth2AuthHandler with token endpoint, credentials, and refresh token.

        Parameters
        ----------
        token_url : str
            The OAuth2 token endpoint.
        client_id : str
            The client ID issued by the API provider.
        client_secret : str
            The client secret issued by the API provider.
        refresh_token : str
            The refresh token used to obtain new access tokens.
        scope : str, optional
            Optional scope for the OAuth2 request.
        """
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.scope = scope

    def reauthenticate(self, client: ApiClient) -> bool:
        """
        Use the refresh token to obtain a new access token and update client headers.

        Parameters
        ----------
        client : ApiClient
            The API client instance that will use the access token.

        Returns
        -------
        bool
            True if access token retrieval succeeded, False otherwise.
        """
        try:
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            if self.scope:
                payload["scope"] = self.scope

            response = requests.post(self.token_url, data=payload, timeout=5)
            response.raise_for_status()
            token_data = response.json()

            client.default_headers["Authorization"] = f"Bearer {token_data['access_token']}"

            if "refresh_token" in token_data:
                self.refresh_token = token_data["refresh_token"]

            return True
        except Exception:
            return False


class UsernamePasswordAuthHandler(AuthHandler):
    """
    Authentication handler for APIs that require username/password login to obtain
    a Bearer token. The token is cached and refreshed when expired.

    Parameters
    ----------
    login_url : str
        URL of the API login/authentication endpoint.
    username : str
        Username for authentication.
    password : str
        Password for authentication.
    extra_headers : dict, optional
        Extra headers to include in the login request.
    token_field : str, default="access_token"
        Field in the login response JSON containing the token.
    expires_in_field : str, default="expires_in"
        Field in the login response JSON containing token lifetime (seconds).
    """  # noqa: D205

    def __init__(
        self,
        login_url: str,
        username: str,
        password: str,
        extra_headers: dict[str, str] | None = None,
        token_field: str = "access_token",
        expires_in_field: str = "expires_in",
    ) -> None:
        self.login_url = login_url
        self.username = username
        self.password = password
        self.extra_headers = extra_headers or {}
        self.token_field = token_field
        self.expires_in_field = expires_in_field

        self._access_token = None
        self._expires_at = 0

    def reauthenticate(self, client: ApiClient) -> bool:
        """
        Authenticate with username/password and update the client's headers
        with a Bearer token.

        Parameters
        ----------
        client : ApiClient
            The API client instance using this authentication handler.

        Returns
        -------
        bool
            True if authentication succeeded, False otherwise.
        """  # noqa: D205
        try:
            payload = {
                "grant_type": "client_credentials"
            }
            response = requests.post(
                self.login_url,
                data=payload,
                headers=self.extra_headers,
                auth=(self.username, self.password),
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            token = data.get(self.token_field)
            if not token:
                return False

            # set expiry (with a small buffer before actual expiration)
            self._access_token = token
            self._expires_at = time.time() + int(data.get(self.expires_in_field, "3600")) - 30

            # update client headers
            client.default_headers["Authorization"] = f"Bearer {self._access_token}"
            return True
        except Exception:
            return False


class RefreshTokenAuthHandler(AuthHandler):
    """
    Generalized handler for APIs using refresh token authentication flows.

    Supports various token refresh mechanisms, including those that:
    - Require both refresh and client credentials.
    - Return new refresh tokens in the response.
    - Use custom header or body parameters.

    Parameters
    ----------
    token_url : str
        The endpoint used to refresh access tokens.
    refresh_token : str
        The refresh token used to obtain new access tokens.
    client_id : str | None, optional
        Client ID for authentication, if required by the provider.
    client_secret : str | None, optional
        Client secret for authentication, if required.
    refresh_field : str, default="refresh_token"
        The key in the request payload representing the refresh token.
    access_token_field : str, default="access_token"
        The key in the response JSON representing the new access token.
    new_refresh_field : str, optional
        The key in the response JSON representing a rotated refresh token.
    headers : dict[str, str], optional
        Additional headers to include in the token refresh request.
    grant_type : str, default="refresh_token"
        Grant type to include in the payload (customizable for non-standard APIs).
    token_prefix : str, default="Bearer "
        Prefix to apply before the access token in the Authorization header.
    auth_header_field : str, default="Authorization"
        The key in the default headers to be used to save the access token.
    """

    def __init__(
        self,
        token_url: str,
        refresh_token: str,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_field: str = "refresh_token",
        access_token_field: str = "access_token",
        new_refresh_field: str | None = None,
        headers: dict[str, str] | None = None,
        grant_type: str = "refresh_token",
        token_prefix: str = "Bearer ",
        auth_header_field: str = "Authorization"
    ) -> None:
        self.token_url = token_url
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_field = refresh_field
        self.access_token_field = access_token_field
        self.new_refresh_field = new_refresh_field
        self.headers = headers
        self.grant_type = grant_type
        self.token_prefix = token_prefix
        self.auth_header_field = auth_header_field

    def reauthenticate(self, client: ApiClient) -> bool:
        """
        Refresh the access token using the stored refresh token and update client headers.

        Parameters
        ----------
        client : ApiClient
            The API client instance using this authentication handler.

        Returns
        -------
        bool
            True if the token refresh succeeded, False otherwise.
        """
        try:
            payload = {self.refresh_field: self.refresh_token,
                       "grant_type": self.grant_type}

            if self.client_id:
                payload["client_id"] = self.client_id
            if self.client_secret:
                payload["client_secret"] = self.client_secret

            response = requests.post(
                self.token_url, data=payload, headers=self.headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            access_token = data.get(self.access_token_field)

            if not access_token:
                return False

            client.default_headers[self.auth_header_field] = f"{self.token_prefix}{access_token}"

            if self.new_refresh_field and self.new_refresh_field in data:
                self.refresh_token = data[self.new_refresh_field]

            return True
        except Exception as e:
            logger.error(e)
            return False
