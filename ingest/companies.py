#!/usr/bin/env python3

import logging
import os
from typing import Optional, Dict, Any, Iterable

from ingest.base import BitSightIngestBase


class CompaniesIngest(BitSightIngestBase):
    """
    Ingest Companies
    Endpoint: GET /ratings/v1/companies
    """

    ENDPOINT_PATH = "/ratings/v1/companies"

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        proxies: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ):
        # Deterministic config sourcing for now:
        # - explicit args win
        # - else environment variables
        api_key = api_key or os.getenv("BITSIGHT_API_KEY")
        base_url = base_url or os.getenv("BITSIGHT_BASE_URL", "https://api.bitsighttech.com")
        timeout = timeout if timeout is not None else int(os.getenv("BITSIGHT_TIMEOUT", "60"))

        # Optional proxy support via env until config layer is wired
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
        logging.info("Ingesting companies from %s", self.ENDPOINT_PATH)

        retrieved = 0
        processed = 0

        for company in self._iter_companies():
            retrieved += 1
            self._process_company(company)
            processed += 1

        logging.info("Companies ingestion complete. Retrieved=%d Processed=%d", retrieved, processed)

    def _iter_companies(self) -> Iterable[Dict[str, Any]]:
        # Pagination handled by BitSightIngestBase.paginate()
        return self.paginate(self.ENDPOINT_PATH)

    def _process_company(self, company: Dict[str, Any]) -> None:
        # This is where DB upsert will go later.
        # For now: deterministic logging only (no prints).
        guid = company.get("guid")
        name = company.get("name")
        logging.debug("Company: guid=%s name=%s", guid, name)
