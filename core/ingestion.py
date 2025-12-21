#!/usr/bin/env python3

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List, Any


@dataclass
class IngestionExecution:
    endpoint: str
    method: str = "GET"
    company_guid: Optional[str] = None

    http_statuses: Dict[int, int] = field(default_factory=dict)
    retries: int = 0
    rate_limited: bool = False

    pages_fetched: int = 0
    records_fetched: int = 0

    records_written: int = 0
    records_skipped: int = 0
    records_failed: int = 0

    started_at: datetime = field(default_factory=lambda: datetime.utcnow())
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    success: bool = False
    failure_reason: Optional[str] = None

    errors: List[Dict[str, Any]] = field(default_factory=list)

    def bump_status(self, status_code: int) -> None:
        self.http_statuses[status_code] = self.http_statuses.get(status_code, 0) + 1
        if status_code == 429:
            self.rate_limited = True

    def finish_success(self) -> None:
        self.completed_at = datetime.utcnow()
        self.duration_seconds = (self.completed_at - self.started_at).total_seconds()
        self.success = True

    def finish_failure(self, reason: str) -> None:
        self.completed_at = datetime.utcnow()
        self.duration_seconds = (self.completed_at - self.started_at).total_seconds()
        self.success = False
        self.failure_reason = reason
