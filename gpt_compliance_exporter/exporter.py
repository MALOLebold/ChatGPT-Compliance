from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .client import ComplianceClient, scope_type_for_principal
from .extraction import extract_prompts


@dataclass(frozen=True)
class ExportConfig:
    principal_id: str
    event_type: str
    out_dir: Path
    days: int = 30
    limit: int = 100
    after: Optional[str] = None


@dataclass(frozen=True)
class ExportResult:
    out_dir: Path
    manifest_path: Path
    raw_path: Path
    prompts_path: Path
    manifest: Dict[str, Any]


def export_logs(
    config: ExportConfig,
    *,
    client: ComplianceClient,
    now: Optional[datetime] = None,
) -> ExportResult:
    if not config.principal_id:
        raise ValueError("principal_id is required")
    if not config.event_type:
        raise ValueError("event_type is required")
    if config.days <= 0:
        raise ValueError("days must be greater than zero")
    if config.limit <= 0:
        raise ValueError("limit must be greater than zero")

    run_now = _coerce_utc(now or datetime.now(timezone.utc))
    after = config.after or isoformat_utc(run_now - timedelta(days=config.days))
    out_dir = Path(config.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path = out_dir / "raw.jsonl"
    prompts_path = out_dir / "prompts.jsonl"
    manifest_path = out_dir / "manifest.json"
    warnings: List[str] = []
    pages_fetched = 0
    log_files_listed = 0
    log_files_downloaded = 0
    raw_records_written = 0
    prompt_records_written = 0
    current_after = after

    with raw_path.open("w", encoding="utf-8") as raw_file, prompts_path.open("w", encoding="utf-8") as prompts_file:
        while True:
            pages_fetched += 1
            page = client.list_logs(
                principal_id=config.principal_id,
                event_type=config.event_type,
                limit=config.limit,
                after=current_after,
            )
            entries = page.get("data") or []
            if not isinstance(entries, list):
                raise ValueError("Compliance Logs list response field 'data' must be a list")

            for entry in entries:
                if not isinstance(entry, dict):
                    warnings.append("Skipped non-object log listing entry")
                    continue
                log_id = entry.get("id")
                if not log_id:
                    warnings.append("Skipped log listing entry without an id")
                    continue

                log_files_listed += 1
                body = client.download_log(principal_id=config.principal_id, log_id=str(log_id))
                log_files_downloaded += 1
                raw_records, decode_warnings = decode_log_payload(body, source_log_id=str(log_id))
                warnings.extend(decode_warnings)

                for raw_record in raw_records:
                    write_jsonl(raw_file, raw_record)
                    raw_records_written += 1
                    prompts = extract_prompts(raw_record, source_log_id=str(log_id))
                    if not prompts:
                        warnings.append(f"No user prompt extracted from log id {log_id}")
                    for prompt in prompts:
                        write_jsonl(prompts_file, prompt)
                        prompt_records_written += 1

            has_more = bool(page.get("has_more"))
            next_after = page.get("last_end_time")
            if has_more and not next_after:
                raise ValueError("Compliance Logs page had has_more=true but no last_end_time")
            if has_more:
                current_after = str(next_after)
                continue
            if next_after:
                current_after = str(next_after)
            break

    manifest: Dict[str, Any] = {
        "generated_at": isoformat_utc(run_now),
        "principal_id": config.principal_id,
        "scope_type": scope_type_for_principal(config.principal_id),
        "event_type": config.event_type,
        "after": after,
        "completed_through": current_after,
        "days": config.days if config.after is None else None,
        "limit": config.limit,
        "pages_fetched": pages_fetched,
        "log_files_listed": log_files_listed,
        "log_files_downloaded": log_files_downloaded,
        "raw_records_written": raw_records_written,
        "prompt_records_written": prompt_records_written,
        "warnings": warnings,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return ExportResult(
        out_dir=out_dir,
        manifest_path=manifest_path,
        raw_path=raw_path,
        prompts_path=prompts_path,
        manifest=manifest,
    )


def decode_log_payload(body: str, *, source_log_id: str) -> Tuple[List[Any], List[str]]:
    stripped = body.strip()
    if not stripped:
        return [], [f"Downloaded log id {source_log_id} was empty"]

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return _decode_jsonl_payload(stripped, source_log_id=source_log_id)

    if isinstance(parsed, list):
        return parsed, []
    return [parsed], []


def _decode_jsonl_payload(body: str, *, source_log_id: str) -> Tuple[List[Any], List[str]]:
    records: List[Any] = []
    warnings: List[str] = []
    for line_number, line in enumerate(body.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            warnings.append(f"Could not parse line {line_number} from log id {source_log_id}; stored as raw text")
            records.append({"_unparsed_log_line": line, "source_log_id": source_log_id, "line_number": line_number})
    return records, warnings


def write_jsonl(file_obj: Any, record: Any) -> None:
    file_obj.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def isoformat_utc(value: datetime) -> str:
    value = _coerce_utc(value).replace(microsecond=0)
    return value.isoformat().replace("+00:00", "Z")


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
