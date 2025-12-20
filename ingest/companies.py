#!/usr/bin/env python3

import logging
import os
from typing import Optional, Dict, Any, Iterable

from ingest.base import BitSightIngestBase


class CompaniesIngest(BitSightIngestBase):
    ENDPOINT_PATH = "/ratings/v1/companies"

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        proxies: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ):
        api_key = api_key or os.getenv("BITSIGHT_API_KEY")
        base_url = base_url or os.getenv("BITSIGHT_BASE_URL", "https://api.bitsighttech.com")
        timeout = timeout if timeout is not None else int(os.getenv("BITSIGHT_TIMEOUT", "60"))

        if proxies is None:
            proxy_url = os.getenv("BITSIGHT_PROXY_URL")
            if proxy_url:
                proxies = {"http": proxy_url, "https": proxy_url}

        if not api_key:
            raise ValueError("Missing API key. Set BITSIGHT_API_KEY or pass api_key=...")

        super().__init__(
            api_key=api_key,
            base_url=base_url,
            proxies=proxies,
            timeout=timeout,
        )

    def run(self) -> None:
        logging.info("Ingesting companies")

        count = 0
        for company in self._iter_companies():
            self._handle_company(company)
            count += 1

        logging.info("Companies ingestion completed. Count=%d", count)

    def _iter_companies(self) -> Iterable[Dict[str, Any]]:
        return self.paginate(self.ENDPOINT_PATH)

    def _handle_company(self, company: Dict[str, Any]) -> None:
        logging.debug(
            "Company retrieved guid=%s name=%s",
            company.get("guid"),
            company.get("name"),
        )
