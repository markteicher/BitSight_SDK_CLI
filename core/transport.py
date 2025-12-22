#!/usr/bin/env python3

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse, urlunparse, quote

import requests
from requests import Session

from core.status_codes import StatusCode


@dataclass(frozen=True)
class TransportConfig:
    base_url: str
    api_key: str
    timeout: int = 60

    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

    verify_ssl: bool = True
    user_agent: str = "BitSight_SDK_CLI/1.0"


class TransportError(Exception):
    def __init__(
        self,
        message: str,
        status_code: StatusCode,
        http_status: Optional[int] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.http_status = http_status


def _normalize_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("base_url must be a non-empty string")
    return base_url.strip().rstrip("/")


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

    if cfg.proxy_username is not None and cfg.proxy_password is not None:
        # URL-encode credentials (combat-safe; avoids breaking proxy URL parsing)
        u = quote(cfg.proxy_username, safe="")
        p = quote(cfg.proxy_password, safe="")
        host = parsed.hostname or ""
        netloc = f"{u}:{p}@{host}"
        if parsed.port:
            netloc += f":{parsed.port}"
        parsed = parsed._replace(netloc=netloc)

    proxy = urlunparse(parsed)
    return {"http": proxy, "https": proxy}


def build_session(cfg: TransportConfig) -> Tuple[Session, Optional[Dict[str, str]]]:
    _validate_proxy_config(cfg)

    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": cfg.user_agent,
        }
    )

    proxies = _build_proxies(cfg)
    return session, proxies


def _map_requests_exception(exc: Exception) -> StatusCode:
    if isinstance(exc, requests.exceptions.ProxyError):
        return StatusCode.TRANSPORT_PROXY_ERROR
    if isinstance(exc, requests.exceptions.SSLError):
        return StatusCode.TRANSPORT_SSL_ERROR
    if isinstance(exc, requests.exceptions.Timeout):
        return StatusCode.TRANSPORT_TIMEOUT
    if isinstance(exc, requests.exceptions.ConnectionError):
        msg = str(exc).lower()
        if "name or service not known" in msg or "temporary failure in name resolution" in msg:
            return StatusCode.TRANSPORT_DNS_FAILURE
        return StatusCode.TRANSPORT_CONNECTION_FAILED
    return StatusCode.TRANSPORT_UNKNOWN


def validate_bitsight_api(
    session: Session,
    cfg: TransportConfig,
    proxies: Optional[Dict[str, str]],
) -> None:
    """
    Validate BitSight API connectivity and authentication.

    Behavior:
    - Validates against the root first (matches your Pass-1 reality).
    - If root returns 404/405, falls back to /ratings/v1/current-ratings with limit=1.
    - Raises TransportError with a StatusCode on failure.
    """

    base_url = _normalize_base_url(cfg.base_url)

    if not cfg.api_key or not cfg.api_key.strip():
        raise TransportError("API key missing", StatusCode.AUTH_API_KEY_MISSING)

    def _do_get(url: str, params: Optional[Dict[str, int]] = None) -> requests.Response:
        try:
            return session.get(
                url,
                params=params,
                auth=(cfg.api_key, ""),
                timeout=cfg.timeout,
                proxies=proxies,
                verify=cfg.verify_ssl,
            )
        except Exception as e:
            raise TransportError(str(e), _map_requests_exception(e)) from e

    # 1) Root validation (preferred)
    root_url = f"{base_url}/"
    logging.info("Validating BitSight API connectivity (root) | url=%s", root_url)
    resp = _do_get(root_url)

    # Some environments may return 404/405 at root even when auth is valid.
    if resp.status_code in (404, 405):
        fallback_url = f"{base_url}/ratings/v1/current-ratings"
        logging.info("Root validation not supported; falling back | url=%s", fallback_url)
        resp = _do_get(fallback_url, params={"limit": 1, "offset": 0})

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
