#!/usr/bin/env python3

import logging
from typing import Iterator, Dict, Any, Optional

import requests
from requests import Session

from core.status_codes import StatusCode
from core.transport import TransportError


class BitSightIngestBase:
    """
    Base helper for BitSight ingestion modules.

    Responsibilities:
    - Own a requests.Session
    - Execute authenticated GET requests
    - Provide deterministic pagination via links.next
    """

    def __init__(
        self,
        *,
        session: Optional[Session],
        base_url: str,
        api_key: str,
        timeout: int = 60,
        proxies: Optional[Dict[str, str]] = None,
        verify_ssl: bool = True,
    ):
        if not base_url or not isinstance(base_url, str):
            raise TransportError(
                "Invalid base_url",
                StatusCode.CONFIG_INVALID,
            )

        if not api_key or not isinstance(api_key, str):
            raise TransportError(
                "API key missing",
                StatusCode.AUTH_API_KEY_MISSING,
            )

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.proxies = proxies
        self.verify_ssl = verify_ssl

        self.session = session or requests.Session()

    # ---------------------------------------------------------
    # HTTP
    # ---------------------------------------------------------
    def request(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a single GET request against the BitSight API.
        """

        url = f"{self.base_url}{path}"
        logging.debug("Requesting %s", url)

        try:
            resp = self.session.get(
                url,
                params=params,
                auth=(self.api_key, ""),
                timeout=self.timeout,
                proxies=self.proxies,
                verify=self.verify_ssl,
                headers={"Accept": "application/json"},
            )

        except requests.exceptions.Timeout as e:
            raise TransportError(str(e), StatusCode.TRANSPORT_TIMEOUT) from e

        except requests.exceptions.ProxyError as e:
            raise TransportError(str(e), StatusCode.TRANSPORT_PROXY_ERROR) from e

        except requests.exceptions.SSLError as e:
            raise TransportError(str(e), StatusCode.TRANSPORT_SSL_ERROR) from e

        except requests.exceptions.ConnectionError as e:
            raise TransportError(str(e), StatusCode.TRANSPORT_CONNECTION_FAILED) from e

        except Exception as e:
            raise TransportError(str(e), StatusCode.TRANSPORT_UNKNOWN) from e

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 401:
            raise TransportError("Unauthorized", StatusCode.API_UNAUTHORIZED, resp.status_code)

        if resp.status_code == 403:
            raise TransportError("Forbidden", StatusCode.API_FORBIDDEN, resp.status_code)

        if resp.status_code == 404:
            raise TransportError("Not found", StatusCode.API_NOT_FOUND, resp.status_code)

        if resp.status_code == 429:
            raise TransportError("Rate limited", StatusCode.API_RATE_LIMITED, resp.status_code)

        if 500 <= resp.status_code <= 599:
            raise TransportError("Server error", StatusCode.API_SERVER_ERROR, resp.status_code)

        raise TransportError(
            f"Unexpected HTTP status {resp.status_code}",
            StatusCode.API_UNEXPECTED_RESPONSE,
            resp.status_code,
        )

    # ---------------------------------------------------------
    # Pagination
    # ---------------------------------------------------------
    def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterate deterministically through a paginated BitSight endpoint.
        """

        current_path = path
        current_params = params or {}

        while True:
            payload = self.request(current_path, params=current_params)

            results = payload.get("results", [])
            for item in results:
                yield item

            links = payload.get("links") or {}
            next_link = links.get("next")

            if not next_link:
                return

            # BitSight pagination encodes offset/limit in the next URL
            if next_link.startswith(self.base_url):
                current_path = next_link[len(self.base_url):]
                current_params = None
            else:
                current_path = next_link
                current_params = None
