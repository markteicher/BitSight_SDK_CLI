#!/usr/bin/env python3
"""
core/transport.py

Canonical transport layer for BitSight SDK + CLI.

Responsibilities:
- Build hardened HTTP sessions
- Enforce proxy correctness
- Validate BitSight API connectivity
- Map failures deterministically to StatusCode
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import logging
import requests
from requests import Session

from core.status_codes import StatusCode


# ============================================================
# Configuration
# ============================================================

@dataclass(frozen=True)
class TransportConfig:
    base_url: str
    api_key: str
    timeout: int = 60

    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

    verify_ssl: bool = True


# ============================================================
# Errors
# ============================================================

class TransportError(Exception):
    """
    Transport-layer exception with deterministic StatusCode mapping.
    """

    def __init__(
        self,
        message: str,
        status_code: StatusCode,
        http_status: Optional[int] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.http_status = http_status


# ============================================================
# Internal helpers
# ============================================================

def _normalize_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise TransportError(
            "Invalid base_url",
            StatusCode.CONFIG_INVALID,
        )
    return base_url.strip().rstrip("/")


def _validate_proxy_config(cfg: TransportConfig) -> None:
    if cfg.proxy_url is None:
        if cfg.proxy_username or cfg.proxy_password:
            raise TransportError(
                "Proxy credentials provided without proxy_url",
                StatusCode.CONFIG_INVALID,
            )
        return

    parsed = urlparse(cfg.proxy_url)

    if parsed.scheme not in ("http", "https"):
        raise TransportError(
            "proxy_url must start with http:// or https://",
            StatusCode.CONFIG_INVALID,
        )

    if not parsed.hostname:
        raise TransportError(
            "proxy_url missing hostname",
            StatusCode.CONFIG_INVALID,
        )

    if (cfg.proxy_username is None) != (cfg.proxy_password is None):
        raise TransportError(
            "proxy_username and proxy_password must be provided together",
            StatusCode.CONFIG_INVALID,
        )


def _build_proxies(cfg: TransportConfig) -> Optional[Dict[str, str]]:
    if not cfg.proxy_url:
        return None

    parsed = urlparse(cfg.proxy_url)

    if cfg.proxy_username and cfg.proxy_password:
        netloc = f"{cfg.proxy_username}:{cfg.proxy_password}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        parsed = parsed._replace(netloc=netloc)

    proxy = urlunparse(parsed)
    return {"http": proxy, "https": proxy}


# ============================================================
# Public API
# ============================================================

def build_session(cfg: TransportConfig) -> Tuple[Session, Optional[Dict[str, str]]]:
    """
    Build a hardened requests.Session and proxy mapping.
    """
    _validate_proxy_config(cfg)

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    proxies = _build_proxies(cfg)
    return session, proxies


def validate_bitsight_api(
    session: Session,
    cfg: TransportConfig,
    proxies: Optional[Dict[str, str]],
) -> None:
    """
    Validate BitSight API connectivity and authentication.

    Success:
        - Returns None

    Failure:
        - Raises TransportError with deterministic StatusCode
    """

    base_url = _normalize_base_url(cfg.base_url)

    if not cfg.api_key or not cfg.api_key.strip():
        raise TransportError(
            "API key missing",
            StatusCode.AUTH_API_KEY_MISSING,
        )

    url = f"{base_url}/ratings/v1/current-ratings"
    params = {"limit": 1, "offset": 0}

    logging.info("Validating BitSight API connectivity: %s", url)

    try:
        resp = session.get(
            url,
            params=params,
            auth=(cfg.api_key, ""),
            timeout=cfg.timeout,
            proxies=proxies,
            verify=cfg.verify_ssl,
        )

    except requests.exceptions.ProxyError as e:
        raise TransportError(str(e), StatusCode.TRANSPORT_PROXY_ERROR) from e

    except requests.exceptions.SSLError as e:
        raise TransportError(str(e), StatusCode.TRANSPORT_SSL_ERROR) from e

    except requests.exceptions.Timeout as e:
        raise TransportError(str(e), StatusCode.TRANSPORT_TIMEOUT) from e

    except requests.exceptions.ConnectionError as e:
        msg = str(e).lower()
        if "name or service not known" in msg:
            raise TransportError(str(e), StatusCode.TRANSPORT_DNS_FAILURE) from e
        raise TransportError(str(e), StatusCode.TRANSPORT_CONNECTION_FAILED) from e

    except Exception as e:
        raise TransportError(str(e), StatusCode.API_UNEXPECTED_RESPONSE) from e

    http_status = resp.status_code

    if http_status == 200:
        return

    if http_status == 401:
        raise TransportError("Unauthorized", StatusCode.API_UNAUTHORIZED, http_status)

    if http_status == 403:
        raise TransportError("Forbidden", StatusCode.API_FORBIDDEN, http_status)

    if http_status == 404:
        raise TransportError("Not found", StatusCode.API_NOT_FOUND, http_status)

    if http_status == 429:
        raise TransportError("Rate limited", StatusCode.API_RATE_LIMITED, http_status)

    if 500 <= http_status <= 599:
        raise TransportError("Server error", StatusCode.API_SERVER_ERROR, http_status)

    raise TransportError(
        f"Unexpected HTTP status {http_status}",
        StatusCode.API_UNEXPECTED_RESPONSE,
        http_status,
    )
