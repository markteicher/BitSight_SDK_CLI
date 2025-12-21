#!/usr/bin/env python3

import logging
import requests
from typing import Iterator, Dict, Any, Optional
from urllib.parse import urlparse

from core.status_codes import StatusCode
from core.transport import TransportError


class BitSightIngestBase:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        proxies: Optional[Dict[str, str]] = None,
        timeout: int = 60,
    ):
        if not api_key or not isinstance(api_key, str):
            raise ValueError("api_key must be a non-empty string")

        if not base_url or not isinstance(base_url, str):
            raise ValueError("base_url must be a non-empty string")

        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.proxies = proxies

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
        })

    def request(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        logging.debug("Requesting %s", url)

        try:
            resp = self.session.get(
                url,
                params=params,
                auth=(self.api_key, ""),
                timeout=self.timeout,
                proxies=self.proxies,
            )
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.Timeout as e:
            raise TransportError(str(e), StatusCode.TRANSPORT_TIMEOUT) from e

        except requests.exceptions.ConnectionError as e:
            raise TransportError(str(e), StatusCode.TRANSPORT_CONNECTION_FAILED) from e

        except requests.exceptions.HTTPError as e:
            http_status = e.response.status_code if e.response else None

            if http_status == 401:
                raise TransportError("Unauthorized", StatusCode.API_UNAUTHORIZED, http_status)
            if http_status == 403:
                raise TransportError("Forbidden", StatusCode.API_FORBIDDEN, http_status)
            if http_status == 404:
                raise TransportError("Not found", StatusCode.API_NOT_FOUND, http_status)
            if http_status == 429:
                raise TransportError("Rate limited", StatusCode.API_RATE_LIMITED, http_status)
            if 500 <= (http_status or 0) <= 599:
                raise TransportError("Server error", StatusCode.API_SERVER_ERROR, http_status)

            raise TransportError(
                "HTTP error",
                StatusCode.API_UNEXPECTED_RESPONSE,
                http_status,
            ) from e

    def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        params = params or {}

        while True:
            data = self.request(path, params=params)

            results = data.get("results", [])
            for item in results:
                yield item

            links = data.get("links") or {}
            next_link = links.get("next")
            if not next_link:
                break

            parsed = urlparse(next_link)
            path = parsed.path
            params = dict(
                (kv.split("=", 1) if "=" in kv else (kv, None))
                for kv in parsed.query.split("&")
                if kv
            )
