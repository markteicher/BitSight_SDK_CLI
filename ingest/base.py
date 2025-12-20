import logging
import requests
from typing import Iterator, Dict, Any


class BitSightIngestBase:
    def __init__(self, api_key: str, base_url: str, proxies=None, timeout=60):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.proxies = proxies or {}

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        })

    def request(self, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        logging.debug("Requesting %s", url)

        resp = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
            proxies=self.proxies
        )
        resp.raise_for_status()
        return resp.json()

    def paginate(self, path: str, params: Dict[str, Any] = None) -> Iterator[Dict[str, Any]]:
        params = params or {}

        while True:
            data = self.request(path, params=params)

            for item in data.get("results", []):
                yield item

            next_link = data.get("links", {}).get("next")
            if not next_link:
                break

            # BitSight pagination uses offset/limit in the next URL
            params = None
            path = next_link.replace(self.base_url, "")
