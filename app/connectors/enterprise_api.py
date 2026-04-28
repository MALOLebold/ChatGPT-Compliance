from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

from app.config import Settings
from app.connectors.base import ComplianceSource
from app.models import RawComplianceRecord


class EnterpriseComplianceAPIError(RuntimeError):
    """Raised when the Enterprise Compliance Platform request fails."""


class EnterpriseComplianceAPIConnector(ComplianceSource):
    """
    Generic connector for the ChatGPT Enterprise Compliance Platform.

    The exact route and payload shape vary by workspace access and are controlled
    through environment variables. This class isolates that uncertainty so the
    rest of the reporting pipeline stays stable.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def fetch_records(
        self,
        start_date: date,
        end_date: date,
        input_file: Path | None = None,
    ) -> list[RawComplianceRecord]:
        if not self.settings.enterprise_base_url or not self.settings.enterprise_api_key:
            raise EnterpriseComplianceAPIError(
                "OPENAI_COMPLIANCE_BASE_URL and OPENAI_COMPLIANCE_API_KEY must be configured."
            )

        response = requests.get(
            f"{self.settings.enterprise_base_url.rstrip('/')}"
            f"{self.settings.enterprise_events_path}",
            headers={
                "Authorization": f"Bearer {self.settings.enterprise_api_key}",
                "Accept": "application/json",
            },
            params={
                "start_time": start_date.isoformat(),
                "end_time": end_date.isoformat(),
            },
            timeout=self.settings.enterprise_timeout_seconds,
        )

        if response.status_code >= 400:
            raise EnterpriseComplianceAPIError(
                f"Enterprise API request failed with status {response.status_code}: {response.text}"
            )

        payload = response.json()
        records = payload.get("data") or payload.get("records") or payload
        if not isinstance(records, list):
            raise EnterpriseComplianceAPIError(
                "Enterprise API response did not contain a list under 'data' or 'records'."
            )

        return [self._normalize_record(record) for record in records]

    def _normalize_record(self, record: dict[str, Any]) -> RawComplianceRecord:
        raw_payload = json.dumps(record, sort_keys=True)
        source_record_id = (
            record.get("source_record_id")
            or record.get("id")
            or record.get("event_id")
            or hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()
        )
        timestamp = (
            record.get("prompt_timestamp")
            or record.get("timestamp")
            or record.get("created_at")
            or datetime.now(timezone.utc).isoformat()
        )

        return RawComplianceRecord(
            source_record_id=str(source_record_id),
            workspace_id=str(record.get("workspace_id", "unknown")),
            user_id=str(record.get("user_id", "unknown")),
            conversation_id=str(record.get("conversation_id", "unknown")),
            message_id=str(record.get("message_id", record.get("id", "unknown"))),
            prompt_text=str(
                record.get("prompt_text")
                or record.get("message_text")
                or record.get("input_text")
                or ""
            ),
            prompt_timestamp=timestamp,
            raw_payload=record,
        )
