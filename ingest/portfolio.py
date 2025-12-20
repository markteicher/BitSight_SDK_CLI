#!/usr/bin/env python3

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from ingest.base import BitSightIngestBase
from db.mssql import MSSQLDatabase


class PortfolioIngest(BitSightIngestBase):
    ENDPOINT_PATH = "/ratings/v2/portfolio"

    def __init__(self):
        super().__init__(
            api_key=os.environ["BITSIGHT_API_KEY"],
            base_url=os.environ.get("BITSIGHT_BASE_URL", "https://api.bitsighttech.com"),
            proxies=self._env_proxies(),
            timeout=int(os.environ.get("BITSIGHT_TIMEOUT", "60")),
        )

        self.db = MSSQLDatabase(
            server=os.environ["MSSQL_SERVER"],
            database=os.environ["MSSQL_DATABASE"],
            username=os.environ["MSSQL_USERNAME"],
            password=os.environ["MSSQL_PASSWORD"],
        )

    @staticmethod
    def _env_proxies() -> Optional[Dict[str, str]]:
        proxy_url = os.environ.get("BITSIGHT_PROXY_URL")
        if not proxy_url:
            return None
        return {"http": proxy_url, "https": proxy_url}

    def run(self) -> None:
        logging.info("Ingesting BitSight portfolio into MSSQL")

        merge_sql = """
        MERGE dbo.bitsight_portfolio AS target
        USING (SELECT ? AS guid) AS source
        ON target.guid = source.guid
        WHEN MATCHED THEN
            UPDATE SET
                custom_id = ?,
                name = ?,
                shortname = ?,
                network_size_v4 = ?,
                rating = ?,
                rating_date = ?,
                added_date = ?,
                industry_name = ?,
                industry_slug = ?,
                sub_industry_name = ?,
                sub_industry_slug = ?,
                type_json = ?,
                logo = ?,
                sparkline = ?,
                subscription_type_name = ?,
                subscription_type_slug = ?,
                primary_domain = ?,
                display_url = ?,
                tier = ?,
                tier_name = ?,
                life_cycle_name = ?,
                life_cycle_slug = ?,
                relationship_name = ?,
                relationship_slug = ?,
                ingested_at = ?,
                raw_payload = ?
        WHEN NOT MATCHED THEN
            INSERT (
                guid,
                custom_id,
                name,
                shortname,
                network_size_v4,
                rating,
                rating_date,
                added_date,
                industry_name,
                industry_slug,
                sub_industry_name,
                sub_industry_slug,
                type_json,
                logo,
                sparkline,
                subscription_type_name,
                subscription_type_slug,
                primary_domain,
                display_url,
                tier,
                tier_name,
                life_cycle_name,
                life_cycle_slug,
                relationship_name,
                relationship_slug,
                ingested_at,
                raw_payload
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            );
        """

        now = datetime.utcnow()

        try:
            count = 0
            for rec in self.paginate(self.ENDPOINT_PATH):
                params = self._params_for_merge(rec, now)
                self.db.execute(merge_sql, params)
                count += 1

            self.db.commit()
            logging.info("Portfolio ingestion committed. Records=%d", count)

        except Exception:
            self.db.rollback()
            logging.exception("Portfolio ingestion failed — rolled back")
            raise

        finally:
            self.db.close()

    @staticmethod
    def _params_for_merge(rec: Dict[str, Any], now: datetime):
        industry = rec.get("industry") or {}
        sub_industry = rec.get("sub_industry") or {}
        subscription_type = rec.get("subscription_type") or {}
        life_cycle = rec.get("life_cycle") or {}
        relationship = rec.get("relationship") or {}

        guid = rec.get("guid")
        if not guid:
            raise ValueError("Portfolio record missing required field: guid")

        type_json = json.dumps(rec.get("type")) if "type" in rec else None
        raw_payload = json.dumps(rec, ensure_ascii=False)

        # MERGE source guid
        p = [
            guid,
            rec.get("custom_id"),
            rec.get("name"),
            rec.get("shortname"),
            rec.get("network_size_v4"),
            rec.get("rating"),
            rec.get("rating_date"),
            rec.get("added_date"),
            industry.get("name"),
            industry.get("slug"),
            sub_industry.get("name"),
            sub_industry.get("slug"),
            type_json,
            rec.get("logo"),
            rec.get("sparkline"),
            subscription_type.get("name"),
            subscription_type.get("slug"),
            rec.get("primary_domain"),
            rec.get("display_url"),
            rec.get("tier"),
            rec.get("tier_name"),
            life_cycle.get("name"),
            life_cycle.get("slug"),
            relationship.get("name"),
            relationship.get("slug"),
            now,
            raw_payload,
        ]

        # INSERT values (full row)
        p.extend([
            guid,
            rec.get("custom_id"),
            rec.get("name"),
            rec.get("shortname"),
            rec.get("network_size_v4"),
            rec.get("rating"),
            rec.get("rating_date"),
            rec.get("added_date"),
            industry.get("name"),
            industry.get("slug"),
            sub_industry.get("name"),
            sub_industry.get("slug"),
            type_json,
            rec.get("logo"),
            rec.get("sparkline"),
            subscription_type.get("name"),
            subscription_type.get("slug"),
            rec.get("primary_domain"),
            rec.get("display_url"),
            rec.get("tier"),
            rec.get("tier_name"),
            life_cycle.get("name"),
            life_cycle.get("slug"),
            relationship.get("name"),
            relationship.get("slug"),
            now,
            raw_payload,
        ])

        return tuple(p)
