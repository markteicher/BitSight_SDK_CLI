#!/usr/bin/env python3
"""
File: core/transport.py

Canonical transport layer for the BitSight SDK and CLI.

Responsibilities:
    - Build hardened HTTP sessions.
    - Validate transport and proxy configuration.
    - Validate BitSight API connectivity.
    - Map failures deterministically to StatusCode values.
    - Prevent credentials from being written to application logs.

BitSight API endpoint:
    https://api.bitsighttech.com/ratings/v1/current-ratings
"""

from __future__ import annotations

import logging
import socket
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import quote, urlparse, urlunparse

import requests
from requests import Response, Session

from core.status_codes import StatusCode


logger = logging.getLogger(__name__)


BITSIGHT_CURRENT_RATINGS_URL = (
    "https://api.bitsighttech.com/ratings/v1/current-ratings"
)

SUPPORTED_PROXY_SCHEMES = frozenset({"http", "https"})
DEFAULT_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class TransportConfig:
    """
    Immutable HTTP transport configuration.

    Attributes:
        api_key:
            BitSight API credential.

        timeout:
            Request timeout in seconds.

        proxy_url:
            Optional HTTP or HTTPS proxy URL.

        proxy_username:
            Optional proxy username.

        proxy_password:
            Optional proxy password.

        verify_ssl:
            Whether TLS certificates must be validated.
    """

    api_key: str
    timeout: int = DEFAULT_TIMEOUT_SECONDS

    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

    verify_ssl: bool = True


class TransportError(Exception):
    """
    Transport-layer exception with deterministic StatusCode mapping.
    """

    def __init__(
        self,
        message: str,
        status_code: StatusCode,
        http_status: Optional[int] = None,
    ) -> None:
        if not isinstance(status_code, StatusCode):
            raise TypeError("status_code must be a StatusCode")

        if http_status is not None:
            if isinstance(http_status, bool) or not isinstance(http_status, int):
                raise TypeError("http_status must be an integer or None")

            if not 100 <= http_status <= 599:
                raise ValueError("http_status must be between 100 and 599")

        super().__init__(message)

        self.status_code = status_code
        self.http_status = http_status


def _validate_transport_config(cfg: TransportConfig) -> None:
    """
    Validate all transport configuration values.

    Raises:
        TypeError:
            If cfg is not a TransportConfig.

        TransportError:
            If any transport setting is invalid.
    """
    if not isinstance(cfg, TransportConfig):
        raise TypeError("cfg must be a TransportConfig")

    if not isinstance(cfg.api_key, str) or not cfg.api_key.strip():
        raise TransportError(
            "BitSight API key is missing",
            StatusCode.AUTH_API_KEY_MISSING,
        )

    if isinstance(cfg.timeout, bool) or not isinstance(cfg.timeout, int):
        raise TransportError(
            "Transport timeout must be an integer",
            StatusCode.CONFIG_INVALID,
        )

    if cfg.timeout <= 0:
        raise TransportError(
            "Transport timeout must be greater than zero",
            StatusCode.CONFIG_INVALID,
        )

    if not isinstance(cfg.verify_ssl, bool):
        raise TransportError(
            "verify_ssl must be a boolean",
            StatusCode.CONFIG_INVALID,
        )

    _validate_proxy_config(cfg)


def _validate_proxy_config(cfg: TransportConfig) -> None:
    """
    Validate proxy URL and credential consistency.
    """
    has_username = cfg.proxy_username is not None
    has_password = cfg.proxy_password is not None

    if cfg.proxy_url is None:
        if has_username or has_password:
            raise TransportError(
                "Proxy credentials were provided without proxy_url",
                StatusCode.CONFIG_INVALID,
            )

        return

    if not isinstance(cfg.proxy_url, str) or not cfg.proxy_url.strip():
        raise TransportError(
            "proxy_url must be a non-empty string",
            StatusCode.CONFIG_INVALID,
        )

    parsed = urlparse(cfg.proxy_url.strip())

    if parsed.scheme.lower() not in SUPPORTED_PROXY_SCHEMES:
        raise TransportError(
            "proxy_url must use http:// or https://",
            StatusCode.CONFIG_INVALID,
        )

    if not parsed.hostname:
        raise TransportError(
            "proxy_url must include a hostname",
            StatusCode.CONFIG_INVALID,
        )

    if parsed.username is not None or parsed.password is not None:
        raise TransportError(
            "Proxy credentials must be provided through proxy_username and "
            "proxy_password",
            StatusCode.CONFIG_INVALID,
        )

    if parsed.query:
        raise TransportError(
            "proxy_url must not contain query parameters",
            StatusCode.CONFIG_INVALID,
        )

    if parsed.fragment:
        raise TransportError(
            "proxy_url must not contain a fragment",
            StatusCode.CONFIG_INVALID,
        )

    try:
        parsed.port
    except ValueError as exc:
        raise TransportError(
            "proxy_url contains an invalid port",
            StatusCode.CONFIG_INVALID,
        ) from exc

    if has_username != has_password:
        raise TransportError(
            "proxy_username and proxy_password must be provided together",
            StatusCode.CONFIG_INVALID,
        )

    if has_username:
        if (
            not isinstance(cfg.proxy_username, str)
            or not cfg.proxy_username
        ):
            raise TransportError(
                "proxy_username must be a non-empty string",
                StatusCode.CONFIG_INVALID,
            )

        if not isinstance(cfg.proxy_password, str):
            raise TransportError(
                "proxy_password must be a string",
                StatusCode.CONFIG_INVALID,
            )


def _build_proxies(
    cfg: TransportConfig,
) -> Optional[Dict[str, str]]:
    """
    Build a requests-compatible proxy mapping.

    Returns:
        Proxy mapping for HTTP and HTTPS requests, or None when no proxy is
        configured.
    """
    if cfg.proxy_url is None:
        return None

    parsed = urlparse(cfg.proxy_url.strip())
    hostname = parsed.hostname

    if hostname is None:
        raise TransportError(
            "proxy_url must include a hostname",
            StatusCode.CONFIG_INVALID,
        )

    formatted_hostname = (
        f"[{hostname}]"
        if ":" in hostname and not hostname.startswith("[")
        else hostname
    )

    host_and_port = formatted_hostname

    if parsed.port is not None:
        host_and_port = f"{formatted_hostname}:{parsed.port}"

    if cfg.proxy_username is not None and cfg.proxy_password is not None:
        encoded_username = quote(cfg.proxy_username, safe="")
        encoded_password = quote(cfg.proxy_password, safe="")
        netloc = (
            f"{encoded_username}:{encoded_password}@{host_and_port}"
        )
    else:
        netloc = host_and_port

    proxy_url = urlunparse(
        (
            parsed.scheme.lower(),
            netloc,
            parsed.path,
            parsed.params,
            "",
            "",
        )
    )

    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def build_session(
    cfg: TransportConfig,
) -> Tuple[Session, Optional[Dict[str, str]]]:
    """
    Build a hardened requests session and explicit proxy mapping.

    Args:
        cfg:
            Validated transport configuration.

    Returns:
        A tuple containing the configured Session and optional proxy mapping.

    Raises:
        TransportError:
            If the transport configuration is invalid.
    """
    _validate_transport_config(cfg)

    session = requests.Session()

    # Ignore environment-defined proxy and authentication settings. Transport
    # routing must be controlled only by explicit application configuration.
    session.trust_env = False

    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "bitsight-sdk-cli",
        }
    )

    return session, _build_proxies(cfg)


def _raise_for_http_status(response: Response) -> None:
    """
    Map an unsuccessful HTTP response to a deterministic TransportError.
    """
    http_status = response.status_code

    if http_status == 200:
        return

    mappings = {
        400: (
            "BitSight API rejected the request",
            StatusCode.API_BAD_REQUEST,
        ),
        401: (
            "BitSight API authentication failed",
            StatusCode.API_UNAUTHORIZED,
        ),
        403: (
            "BitSight API access is forbidden",
            StatusCode.API_FORBIDDEN,
        ),
        404: (
            "BitSight API endpoint or resource was not found",
            StatusCode.API_NOT_FOUND,
        ),
        405: (
            "BitSight API method is not allowed",
            StatusCode.API_METHOD_NOT_ALLOWED,
        ),
        409: (
            "BitSight API request conflicts with current state",
            StatusCode.API_CONFLICT,
        ),
        429: (
            "BitSight API rate limit was exceeded",
            StatusCode.API_RATE_LIMITED,
        ),
    }

    mapped_error = mappings.get(http_status)

    if mapped_error is not None:
        message, status_code = mapped_error

        raise TransportError(
            message,
            status_code,
            http_status,
        )

    if 500 <= http_status <= 599:
        raise TransportError(
            "BitSight API returned a server-side error",
            StatusCode.API_SERVER_ERROR,
            http_status,
        )

    raise TransportError(
        f"BitSight API returned unexpected HTTP status {http_status}",
        StatusCode.API_UNEXPECTED_RESPONSE,
        http_status,
    )


def validate_bitsight_api(
    session: Session,
    cfg: TransportConfig,
    proxies: Optional[Dict[str, str]],
) -> None:
    """
    Validate BitSight API connectivity and authentication.

    Success:
        Returns None.

    Failure:
        Raises TransportError with a deterministic StatusCode.
    """
    if not isinstance(session, Session):
        raise TypeError("session must be a requests.Session")

    _validate_transport_config(cfg)

    logger.info("Validating BitSight API connectivity")

    try:
        response = session.get(
            BITSIGHT_CURRENT_RATINGS_URL,
            params={
                "limit": 1,
                "offset": 0,
            },
            auth=(cfg.api_key.strip(), ""),
            timeout=cfg.timeout,
            proxies=proxies,
            verify=cfg.verify_ssl,
        )

    except requests.exceptions.ProxyError as exc:
        raise TransportError(
            "Proxy connection failed",
            StatusCode.TRANSPORT_PROXY_ERROR,
        ) from exc

    except requests.exceptions.SSLError as exc:
        raise TransportError(
            "TLS validation or negotiation failed",
            StatusCode.TRANSPORT_SSL_ERROR,
        ) from exc

    except requests.exceptions.Timeout as exc:
        raise TransportError(
            "BitSight API request timed out",
            StatusCode.TRANSPORT_TIMEOUT,
        ) from exc

    except requests.exceptions.ConnectionError as exc:
        cause = exc.__cause__

        if isinstance(cause, socket.gaierror):
            raise TransportError(
                "BitSight API hostname resolution failed",
                StatusCode.TRANSPORT_DNS_FAILURE,
            ) from exc

        message = str(exc).lower()

        if any(
            indicator in message
            for indicator in (
                "name or service not known",
                "nodename nor servname provided",
                "temporary failure in name resolution",
                "getaddrinfo failed",
            )
        ):
            raise TransportError(
                "BitSight API hostname resolution failed",
                StatusCode.TRANSPORT_DNS_FAILURE,
            ) from exc

        if "connection reset" in message:
            raise TransportError(
                "BitSight API connection was reset",
                StatusCode.TRANSPORT_CONNECTION_RESET,
            ) from exc

        if "network is unreachable" in message:
            raise TransportError(
                "BitSight API network is unreachable",
                StatusCode.TRANSPORT_UNREACHABLE,
            ) from exc

        raise TransportError(
            "BitSight API connection failed",
            StatusCode.TRANSPORT_CONNECTION_FAILED,
        ) from exc

    except requests.exceptions.RequestException as exc:
        raise TransportError(
            "BitSight API request failed",
            StatusCode.TRANSPORT_UNKNOWN,
        ) from exc

    _raise_for_http_status(response)
