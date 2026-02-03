from __future__ import annotations

from typing import Any

import requests
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt

from infolio.utils.auth_handlers import AuthHandler
from infolio.utils.logger import get_logger

logger = get_logger(__name__)


class RateLimitError(Exception):
    """Raised when API rate limits are exceeded.

    Parameters
    ----------
    retry_after : int
        Number of seconds to wait before retrying the request.
    """

    def __init__(self, retry_after: int) -> None:
        self.retry_after = retry_after


class AuthenticationError(Exception):
    """Raised when reauthentication fails."""


class BadRequestError(Exception):
    """Raised for non-retryable 400 Bad Request errors.

    Parameters
    ----------
    response : requests.Response
        Response object containing the error details.
    """

    def __init__(self, response: requests.Response) -> None:
        self.response = response
        super().__init__(f"400 Bad Request: {response.text}")


class ApiClient:
    """
    Generic API client with retry logic, rate limit handling, and hybrid authentication.

    This class wraps `requests` with retry logic (via `tenacity`),
    built-in rate-limit handling, and optional authentication via an `AuthHandler`.

    Parameters
    ----------
    base_url : str
        Base URL of the API.
    headers : dict of str, str, optional
        Default headers to include with every request.
    timeout : int, default=5
        Timeout for requests in seconds.
    max_retries : int, default=5
        Maximum number of retry attempts for requests.
    auth_handler : AuthHandler, optional
        Optional authentication handler implementing a `reauthenticate()` method.

    Notes
    -----
    Subclasses may override `reauthenticate()` if no `auth_handler` is provided.
    """

    HTTP_BAD_REQUEST = 400
    HTTP_UNAUTHORIZED = 401
    HTTP_TOO_MANY_REQUESTS = 429

    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] | None = None,
        timeout: int = 5,
        max_retries: int = 5,
        auth_handler: AuthHandler | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.default_headers = headers or {}
        self.timeout = timeout
        self.max_retries = max_retries
        self.auth_handler = auth_handler

    def _custom_wait(self, retry_state: RetryCallState) -> float:
        """
        Wait strategy for `tenacity`, based on exception type.

        Parameters
        ----------
        retry_state : RetryCallState
            The retry state object from tenacity.

        Returns
        -------
        float
            The number of seconds to wait before the next retry.
        """
        exception = retry_state.outcome.exception()
        if isinstance(exception, RateLimitError):
            return exception.retry_after
        if hasattr(exception, "retry_after"):
            return exception.retry_after
        return 2  # fallback

    def _get_retry_decorator(self) -> retry:
        """
        Construct the retry decorator for API requests.

        Returns
        -------
        retry
            Configured tenacity retry decorator.
        """
        return retry(
            retry=retry_if_exception_type(
                (requests.exceptions.RequestException, RateLimitError)),
            stop=stop_after_attempt(self.max_retries),
            wait=self._custom_wait,
        )

    def _build_url(self, endpoint: str) -> str:
        """
        Build a full URL from a base URL and endpoint.

        Parameters
        ----------
        endpoint : str
            API endpoint path or relative URL.

        Returns
        -------
        str
            Fully qualified URL.
        """
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        xml_query: str | None = None,
    ) -> requests.Response:
        """
        Send a generic HTTP request with retry logic.

        Parameters
        ----------
        method : str
            HTTP method (e.g., "GET", "POST").
        endpoint : str
            API endpoint relative to `base_url`.
        params : dict, optional
            Query parameters for the request.
        data : dict, optional
            Request body data for POST/PUT requests.
        extra_headers : dict, optional
            Additional headers to merge with default headers.
        xml_query : str, optional
            XML query string to append to the URL.

        Returns
        -------
        requests.Response
            The HTTP response object.

        Raises
        ------
        BadRequestError
            If the server returns a 400 response.
        RateLimitError
            If the server returns a 429 rate limit response.
        requests.exceptions.HTTPError
            For other HTTP errors.
        """
        decorated = self._get_retry_decorator()(self._make_request)
        return decorated(method, endpoint, params, data, extra_headers, xml_query)

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        xml_query: str | None = None,
    ) -> requests.Response:
        """
        Perform a raw HTTP request.

        Includes handling for 400 and 429 errors.

        Returns
        -------
        requests.Response
            The HTTP response object.
        """
        headers = {**self.default_headers, **(extra_headers or {})}
        url = self._build_url(endpoint)

        if xml_query:
            url += ("&" if "?" in url else "?") + "XML=" + xml_query

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=data,
            timeout=self.timeout,
        )

        if response.status_code == self.HTTP_BAD_REQUEST:
            raise BadRequestError(response)
        if response.status_code == self.HTTP_TOO_MANY_REQUESTS:
            retry_after = int(response.headers.get("Retry-After", "1"))
            raise RateLimitError(retry_after)

        response.raise_for_status()
        return response

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None
    ) -> requests.Response:
        """
        Perform a GET request with retry and optional reauthentication.

        Parameters
        ----------
        endpoint : str
            API endpoint relative to `base_url`.
        params : dict, optional
            Query parameters.
        extra_headers : dict, optional
            Additional headers.

        Returns
        -------
        requests.Response
            Response from the GET request.
        """
        decorated_get = self._get_retry_decorator()(self._get_request_with_auth_retry)
        return decorated_get(endpoint, params, extra_headers, data)

    def _get_request_with_auth_retry(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None
    ) -> requests.Response:
        """
        Perform a GET request and retry authentication if a 401 occurs.

        Returns
        -------
        requests.Response
            HTTP response.

        Raises
        ------
        AuthenticationError
            If reauthentication fails.
        """
        try:
            return self._get_request(endpoint, params, extra_headers, data)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == self.HTTP_UNAUTHORIZED:
                success = self.auth_handler.reauthenticate(
                    self) if self.auth_handler else self.reauthenticate()
                if success:
                    return self._get_request(endpoint, params, extra_headers, data)
                raise AuthenticationError("Reauthentication failed.") from e
            raise

    def _get_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None
    ) -> requests.Response:
        """
        Perform a simple GET request with error handling.

        Returns
        -------
        requests.Response
            The HTTP response object.
        """
        headers = {**self.default_headers, **(extra_headers or {})}
        url = self._build_url(endpoint)
        content_type = headers.get(
            "Content-Type", headers.get("content-type", "")).lower()

        if content_type == "application/json":
            response = requests.get(
                url, headers=headers, params=params, json=data, timeout=self.timeout)
        else:
            response = requests.get(
                url, headers=headers, params=params, data=data, timeout=self.timeout)

        if response.status_code == self.HTTP_BAD_REQUEST:
            raise BadRequestError(response)
        if response.status_code == self.HTTP_TOO_MANY_REQUESTS:
            retry_after = int(response.headers.get("Retry-After", "1"))
            raise RateLimitError(retry_after)

        response.raise_for_status()
        return response

    def post(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """
        Perform a POST request with retry and optional reauthentication.

        Parameters
        ----------
        endpoint : str
            API endpoint.
        data : dict, optional
            Request body.
        extra_headers : dict, optional
            Additional headers.
        params : dict, optional
            URL query parameters.

        Returns
        -------
        requests.Response
            Response object.
        """
        decorated_post = self._get_retry_decorator()(self._post_request_with_auth_retry)
        return decorated_post(endpoint, data, extra_headers, params)

    def _post_request_with_auth_retry(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """
        Perform a POST request and reauthenticate if 401 occurs.

        Returns
        -------
        requests.Response
            HTTP response object.

        Raises
        ------
        AuthenticationError
            If reauthentication fails.
        """
        try:
            return self._post_request(endpoint, data, extra_headers, params)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == self.HTTP_UNAUTHORIZED:
                success = self.auth_handler.reauthenticate(
                    self) if self.auth_handler else self.reauthenticate()
                if success:
                    return self._post_request(endpoint, data, extra_headers, params)
                raise AuthenticationError("Reauthentication failed.") from e
            raise

    def _post_request(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """
        Perform the actual POST request, handling content type detection and errors.

        Returns
        -------
        requests.Response
            The HTTP response object.
        """
        url = self._build_url(endpoint)
        headers = {**self.default_headers, **(extra_headers or {})}

        content_type = headers.get(
            "Content-Type", headers.get("content-type", "")).lower()

        if "application/json" in content_type:
            response = requests.post(
                url, json=data, headers=headers, timeout=self.timeout, params=params)
        else:
            response = requests.post(
                url, data=data, headers=headers, timeout=self.timeout, params=params)

        if response.status_code == self.HTTP_BAD_REQUEST:
            raise BadRequestError(response)
        if response.status_code == self.HTTP_TOO_MANY_REQUESTS:
            retry_after = int(response.headers.get("Retry-After", "1"))
            raise RateLimitError(retry_after)

        response.raise_for_status()
        return response

    def reauthenticate(self) -> bool:
        """
        Attempt to reauthenticate the client.

        Returns
        -------
        bool
            True if reauthentication succeeded, False otherwise.

        Notes
        -----
        This method should be overridden in subclasses if no
        `auth_handler` is provided.
        """
        if self.auth_handler:
            return self.auth_handler.reauthenticate(self)
        else:
            return False
