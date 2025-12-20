#!/usr/bin/env python3

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

from ingest.base import BitSightIngestBase
from db.mssql import MSSQLDatabase


class CurrentRatingsIngest(BitSightIngestBase):
    ENDPOINT_PATH = "/ratings/v1/current-ratings"

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
        logging.info("Ingesting BitSight current ratings")

        merge_sql = """
        MERGE dbo.bitsight_current_ratings AS target
        USING (SELECT ? AS company_guid) AS source
        ON target.company_guid = source.company_guid
        WHEN MATCHED THEN
            UPDATE SET
                company_name = ?,
                rating = ?,
                rating_date = ?,
                rating_level = ?,
                industry_name = ?,
                industry_slug = ?,
                sub_industry_name = ?,
                sub_industry_slug = ?,
                network_size_v4 = ?,
                ingested_at = ?,
                raw_payload = ?
        WHEN NOT MATCHED THEN
            INSERT (
                company_guid,
                company_name,
                rating,
                rating_date,
                rating_level,
                industry_name,
                industry_slug,
                sub_industry_name,
                sub_industry_slug,
                network_size_v4,
                ingested_at,
                raw_payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        now = datetime.utcnow()

        try:
            count = 0
            for record in self.paginate(self.ENDPOINT_PATH):
                params = self._build_params(record, now)
                self.db.execute(merge_sql, params)
                count += 1

            self.db.commit()
            logging.info("Current ratings ingestion committed. Records=%d", count)

        except Exception:
            self.db.rollback()
            logging.exception("Current ratings ingestion failed — rolled back")
            raise

        finally:
            self.db.close()

    @staticmethod
    def _build_params(record: Dict[str, Any], now: datetime):
        company = record.get("company") or {}
        industry = record.get("industry") or {}
        sub_industry = record.get("sub_industry") or {}

        guid = company.get("guid")
        if not guid:
            raise ValueError("Missing required field: company.guid")

        raw_payload = json.dumps(record, ensure_ascii=False)

        return (
            # MERGE key
            guid,

            # UPDATE values
            company.get("name"),
            record.get("rating"),
            record.get("rating_date"),
            record.get("rating_level"),
            industry.get("name"),
            industry.get("slug"),
            sub_industry.get("name"),
            sub_industry.get("slug"),
            record.get("network_size_v4"),
            now,
            raw_payload,

            # INSERT values
            guid,
            company.get("name"),
            record.get("rating"),
            record.get("rating_date"),
            record.get("rating_level"),
            industry.get("name"),
            industry.get("slug"),
            sub_industry.get("name"),
            sub_industry.get("slug"),
            record.get("network_size_v4"),
            now,
            raw_payload,
        )
