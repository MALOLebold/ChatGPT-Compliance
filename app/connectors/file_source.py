from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from app.connectors.base import ComplianceSource
from app.models import RawComplianceRecord


class FileComplianceSource(ComplianceSource):
    """Reads compliance records from local JSON or NDJSON for testing and backfills."""

    def fetch_records(
        self,
        start_date: date,
        end_date: date,
        input_file: Path | None = None,
    ) -> list[RawComplianceRecord]:
        if input_file is None:
            raise ValueError("An --input-file path is required when using the file source.")
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        raw_items = self._load_records(input_file)
        filtered: list[RawComplianceRecord] = []
        for item in raw_items:
            normalized = self._normalize_record(item)
            prompt_dt = datetime.fromisoformat(
                normalized.prompt_timestamp.replace("Z", "+00:00")
            ).date()
            if start_date <= prompt_dt <= end_date:
                filtered.append(normalized)
        return filtered

    def _load_records(self, input_file: Path) -> list[dict[str, Any]]:
        if input_file.suffix.lower() == ".json":
            return json.loads(input_file.read_text(encoding="utf-8"))

        if input_file.suffix.lower() in {".ndjson", ".jsonl"}:
            records: list[dict[str, Any]] = []
            for line in input_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    records.append(json.loads(line))
            return records

        raise ValueError("Input file must be .json, .ndjson, or .jsonl")

    def _normalize_record(self, record: dict[str, Any]) -> RawComplianceRecord:
        raw_payload = json.dumps(record, sort_keys=True)
        source_record_id = (
            record.get("source_record_id")
            or record.get("id")
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
            workspace_id=str(record.get("workspace_id", "workspace-demo")),
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
