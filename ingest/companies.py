#!/usr/bin/env python3

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

from ingest.base import BitSightIngestBase
from db.mssql import MSSQLDatabase


class CompaniesIngest(BitSightIngestBase):
    ENDPOINT_PATH = "/ratings/v1/companies"

    def __init__(self):
        super().__init__(
            api_key=os.environ["BITSIGHT_API_KEY"],
            base_url=os.environ.get("BITSIGHT_BASE_URL", "https://api.bitsighttech.com"),
        )

        self.db = MSSQLDatabase(
            server=os.environ["MSSQL_SERVER"],
            database=os.environ["MSSQL_DATABASE"],
            username=os.environ["MSSQL_USERNAME"],
            password=os.environ["MSSQL_PASSWORD"],
        )

    def run(self) -> None:
        logging.info("Ingesting BitSight companies into MSSQL")

        sql = """
        MERGE dbo.bitsight_companies AS target
        USING (SELECT ? AS company_guid) AS source
        ON target.company_guid = source.company_guid
        WHEN MATCHED THEN
            UPDATE SET
                name = ?,
                domain = ?,
                industry = ?,
                sub_industry = ?,
                country = ?,
                added_date = ?,
                rating = ?,
                ingested_at = ?,
                raw_payload = ?
        WHEN NOT MATCHED THEN
            INSERT (
                company_guid, name, domain, industry, sub_industry,
                country, added_date, rating, ingested_at, raw_payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        now = datetime.utcnow()

        try:
            for company in self.paginate(self.ENDPOINT_PATH):
                params = self._build_params(company, now)
                self.db.execute(sql, params)

            self.db.commit()
            logging.info("Companies ingestion committed successfully")

        except Exception:
            self.db.rollback()
            logging.exception("Companies ingestion failed — transaction rolled back")
            raise

        finally:
            self.db.close()

    def _build_params(self, company: Dict[str, Any], now: datetime):
        guid = company.get("guid")

        return (
            guid,
            company.get("name"),
            company.get("domain"),
            company.get("industry", {}).get("name") if company.get("industry") else None,
            company.get("sub_industry", {}).get("name") if company.get("sub_industry") else None,
            company.get("country"),
            company.get("added_date"),
            company.get("rating"),
            now,
            json.dumps(company),

            # INSERT params
            guid,
            company.get("name"),
            company.get("domain"),
            company.get("industry", {}).get("name") if company.get("industry") else None,
            company.get("sub_industry", {}).get("name") if company.get("sub_industry") else None,
            company.get("country"),
            company.get("added_date"),
            company.get("rating"),
            now,
            json.dumps(company),
        )
