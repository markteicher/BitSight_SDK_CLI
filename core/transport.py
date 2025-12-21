#!/usr/bin/env python3

import logging
import socket
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests
from requests import Session


@dataclass(frozen=True)
class TransportConfig:
    base_url: str
    api_key: str
    timeout: int = 60

    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

    verify_ssl: bool = True


class TransportError(Exception):
    def __init__(self, message: str, status_code, http_status: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code
        self.http_status = http_status


def _normalize_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("base_url must be a non-empty string")
    base_url = base_url.strip().rstrip("/")
    return base_url


def _validate_proxy_config(cfg: TransportConfig) -> None:
    if cfg.proxy_url is None and (cfg.proxy_username or cfg.proxy_password):
        raise ValueError("proxy_username/proxy_password provided but proxy_url is missing")

    if cfg.proxy_url is None:
        return

    parsed = urlparse(cfg.proxy_url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("proxy_url must start with http:// or https://")

    if not parsed.hostname:
        raise ValueError("proxy_url missing hostname")

    if (cfg.proxy_username is None) != (cfg.proxy_password is None):
        raise ValueError("proxy_username and proxy_password must be provided together")


def _build_proxies(cfg: TransportConfig) -> Optional[Dict[str, str]]:
    if not cfg.proxy_url:
        return None

    parsed = urlparse(cfg.proxy_url)

    # If proxy auth is provided, inject it into the netloc
    if cfg.proxy_username and cfg.proxy_password:
        netloc = f"{cfg.proxy_username}:{cfg.proxy_password}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        parsed = parsed._replace(netloc=netloc)

    proxy = urlunparse(parsed)
    return {"http": proxy, "https": proxy}


def build_session(cfg: TransportConfig) -> Tuple[Session, Optional[Dict[str, str]]]:
    _validate_proxy_config(cfg)

    session = requests.Session()
    proxies = _build_proxies(cfg)

    # Do not set auth headers globally; BitSight uses Basic Auth (api_key, "")
    # Auth is applied per-request to avoid cross-target leakage.
    return session, proxies


def validate_bitsight_api(
    session: Session,
    cfg: TransportConfig,
    proxies: Optional[Dict[str, str]],
    status_codes_module_path: str = "core.status_codes",
) -> None:
    """
    Validates network path + proxy + TLS + BitSight auth deterministically.

    Uses GET /ratings/v1/current-ratings?limit=1&offset=0
    (HEAD is not assumed supported.)

    Raises TransportError with a StatusCode on failure.
    """
    # Late import so this file stays self-contained & does not hard-couple at import time.
    status_mod = __import__(status_codes_module_path, fromlist=["StatusCode"])
    StatusCode = getattr(status_mod, "StatusCode")

    base_url = _normalize_base_url(cfg.base_url)
    if not cfg.api_key or not isinstance(cfg.api_key, str) or not cfg.api_key.strip():
        raise TransportError("API key missing", StatusCode.CONFIG_INVALID)

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
            headers={"Accept": "application/json"},
        )

    except requests.exceptions.ProxyError as e:
        raise TransportError(f"Proxy error: {e}", StatusCode.API_CONNECTION_FAILED) from e

    except requests.exceptions.SSLError as e:
        raise TransportError(f"SSL error: {e}", StatusCode.API_SSL_ERROR) from e

    except requests.exceptions.Timeout as e:
        raise TransportError(f"API timeout: {e}", StatusCode.API_TIMEOUT) from e

    except requests.exceptions.ConnectionError as e:
        # Try to distinguish DNS failure (best-effort, deterministic enough for ops)
        # If we cannot, treat as connection failed.
        msg = str(e)
        if "Name or service not known" in msg or "Temporary failure in name resolution" in msg:
            raise TransportError(f"DNS failure: {e}", StatusCode.API_DNS_FAILURE) from e
        raise TransportError(f"Connection failed: {e}", StatusCode.API_CONNECTION_FAILED) from e

    except Exception as e:
        raise TransportError(f"Unexpected transport failure: {e}", StatusCode.API_UNEXPECTED_RESPONSE) from e

    http_status = resp.status_code

    if http_status == 200:
        logging.info("API validation succeeded (HTTP 200)")
        return

    if http_status == 401:
        raise TransportError("Unauthorized (HTTP 401)", StatusCode.AUTH_FAILED, http_status=http_status)

    if http_status == 403:
        raise TransportError("Permission denied (HTTP 403)", StatusCode.PERMISSION_DENIED, http_status=http_status)

    if http_status == 404:
        raise TransportError("Endpoint not found (HTTP 404)", StatusCode.API_NOT_FOUND, http_status=http_status)

    if http_status == 429:
        raise TransportError("Rate limited (HTTP 429)", StatusCode.API_RATE_LIMITED, http_status=http_status)

    if 500 <= http_status <= 599:
        raise TransportError(f"Server error (HTTP {http_status})", StatusCode.API_SERVER_ERROR, http_status=http_status)

    raise TransportError(
        f"Unexpected HTTP status (HTTP {http_status})",
        StatusCode.API_UNEXPECTED_RESPONSE,
        http_status=http_status,
    )
