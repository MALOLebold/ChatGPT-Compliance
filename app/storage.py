from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

from app.models import EvaluatedComplianceRecord, PipelineRunSummary, RawComplianceRecord


class SQLiteStore:
    """Restricted local store for raw records, evaluated records, and run logs."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _initialize(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS raw_records (
                    source_record_id TEXT PRIMARY KEY,
                    workspace_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    prompt_text TEXT NOT NULL,
                    prompt_timestamp TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    reporting_period TEXT NOT NULL,
                    raw_payload_hash TEXT NOT NULL,
                    raw_payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS evaluated_records (
                    source_record_id TEXT NOT NULL,
                    report_period TEXT NOT NULL,
                    prompt_timestamp TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    prompt_text TEXT NOT NULL,
                    openai_policy_matches TEXT NOT NULL,
                    company_policy_matches TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    disposition TEXT NOT NULL,
                    review_status TEXT NOT NULL,
                    review_notes TEXT NOT NULL,
                    report_included INTEGER NOT NULL,
                    PRIMARY KEY (source_record_id, report_period)
                );

                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id TEXT PRIMARY KEY,
                    period TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    records_pulled INTEGER NOT NULL,
                    records_inserted INTEGER NOT NULL,
                    duplicates_skipped INTEGER NOT NULL,
                    evaluated_records INTEGER NOT NULL,
                    no_match_count INTEGER NOT NULL,
                    openai_flag_count INTEGER NOT NULL,
                    company_flag_count INTEGER NOT NULL,
                    dual_flag_count INTEGER NOT NULL,
                    failures INTEGER NOT NULL,
                    detail_report_path TEXT,
                    summary_report_path TEXT
                );
                """
            )

    def start_run(self, period: str, start_date: date, end_date: date, source: str) -> str:
        run_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, period, start_date, end_date, source, status, started_at,
                    records_pulled, records_inserted, duplicates_skipped, evaluated_records,
                    no_match_count, openai_flag_count, company_flag_count, dual_flag_count,
                    failures
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                """,
                (
                    run_id,
                    period,
                    start_date.isoformat(),
                    end_date.isoformat(),
                    source,
                    "running",
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        return run_id

    def save_raw_records(
        self,
        records: list[RawComplianceRecord],
        reporting_period: str,
    ) -> tuple[int, int]:
        inserted = 0
        duplicates = 0
        with sqlite3.connect(self.db_path) as conn:
            for record in records:
                raw_payload_json = json.dumps(record.raw_payload, sort_keys=True)
                raw_payload_hash = hashlib.sha256(
                    raw_payload_json.encode("utf-8")
                ).hexdigest()
                try:
                    conn.execute(
                        """
                        INSERT INTO raw_records (
                            source_record_id, workspace_id, user_id, conversation_id,
                            message_id, prompt_text, prompt_timestamp, ingested_at,
                            reporting_period, raw_payload_hash, raw_payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record.source_record_id,
                            record.workspace_id,
                            record.user_id,
                            record.conversation_id,
                            record.message_id,
                            record.prompt_text,
                            record.prompt_timestamp,
                            datetime.now(timezone.utc).isoformat(),
                            reporting_period,
                            raw_payload_hash,
                            raw_payload_json,
                        ),
                    )
                    inserted += 1
                except sqlite3.IntegrityError:
                    duplicates += 1
        return inserted, duplicates

    def save_evaluated_records(self, records: list[EvaluatedComplianceRecord]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            for record in records:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO evaluated_records (
                        source_record_id, report_period, prompt_timestamp, user_id,
                        conversation_id, prompt_text, openai_policy_matches,
                        company_policy_matches, severity, disposition, review_status,
                        review_notes, report_included
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.source_record_id,
                        record.report_period,
                        record.prompt_timestamp,
                        record.user_id,
                        record.conversation_id,
                        record.prompt_text,
                        json.dumps([asdict(match) for match in record.openai_policy_matches]),
                        json.dumps([asdict(match) for match in record.company_policy_matches]),
                        record.severity,
                        record.disposition,
                        record.review_status,
                        record.review_notes,
                        1 if record.report_included else 0,
                    ),
                )

    def complete_run(self, summary: PipelineRunSummary) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE pipeline_runs
                SET status = ?, completed_at = ?, records_pulled = ?, records_inserted = ?,
                    duplicates_skipped = ?, evaluated_records = ?, no_match_count = ?,
                    openai_flag_count = ?, company_flag_count = ?, dual_flag_count = ?,
                    failures = ?, detail_report_path = ?, summary_report_path = ?
                WHERE run_id = ?
                """,
                (
                    "completed",
                    datetime.now(timezone.utc).isoformat(),
                    summary.records_pulled,
                    summary.records_inserted,
                    summary.duplicates_skipped,
                    summary.evaluated_records,
                    summary.no_match_count,
                    summary.openai_flag_count,
                    summary.company_flag_count,
                    summary.dual_flag_count,
                    summary.failures,
                    summary.detail_report_path,
                    summary.summary_report_path,
                    summary.run_id,
                ),
            )
