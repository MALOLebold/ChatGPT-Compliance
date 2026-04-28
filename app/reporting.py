from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from app.models import EvaluatedComplianceRecord, PipelineRunSummary


class ReportBuilder:
    """Creates machine-readable detail reports and human-readable summaries."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def build(
        self,
        records: list[EvaluatedComplianceRecord],
        summary: PipelineRunSummary,
    ) -> tuple[Path, Path]:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        detail_path = self.output_dir / f"{summary.period}_{stamp}_detail.csv"
        summary_path = self.output_dir / f"{summary.period}_{stamp}_summary.json"

        summary.detail_report_path = str(detail_path)
        summary.summary_report_path = str(summary_path)
        self._write_detail_report(detail_path, records)
        self._write_summary_report(summary_path, records, summary)
        return detail_path, summary_path

    def _write_detail_report(
        self,
        path: Path,
        records: list[EvaluatedComplianceRecord],
    ) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "report_period",
                    "source_record_id",
                    "prompt_timestamp",
                    "user_id",
                    "conversation_id",
                    "prompt_text",
                    "openai_policy_matches",
                    "company_policy_matches",
                    "severity",
                    "disposition",
                    "review_status",
                    "review_notes",
                ],
            )
            writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        "report_period": record.report_period,
                        "source_record_id": record.source_record_id,
                        "prompt_timestamp": record.prompt_timestamp,
                        "user_id": record.user_id,
                        "conversation_id": record.conversation_id,
                        "prompt_text": record.prompt_text,
                        "openai_policy_matches": json.dumps(
                            [asdict(match) for match in record.openai_policy_matches]
                        ),
                        "company_policy_matches": json.dumps(
                            [asdict(match) for match in record.company_policy_matches]
                        ),
                        "severity": record.severity,
                        "disposition": record.disposition,
                        "review_status": record.review_status,
                        "review_notes": record.review_notes,
                    }
                )

    def _write_summary_report(
        self,
        path: Path,
        records: list[EvaluatedComplianceRecord],
        summary: PipelineRunSummary,
    ) -> None:
        severity_counts = Counter(record.severity for record in records)
        user_counts = Counter(record.user_id for record in records)

        payload = {
            "run_summary": asdict(summary),
            "summary_metrics": {
                "total_prompts_reviewed": len(records),
                "prompts_with_no_matches": sum(
                    1 for record in records if record.disposition == "no_match"
                ),
                "prompts_flagged_for_openai_policy_review": sum(
                    1
                    for record in records
                    if record.disposition == "potential_openai_policy_issue"
                ),
                "prompts_flagged_for_company_policy_review": sum(
                    1
                    for record in records
                    if record.disposition == "potential_company_policy_issue"
                ),
                "prompts_flagged_for_both": sum(
                    1 for record in records if record.disposition == "potential_both"
                ),
                "counts_by_severity": dict(severity_counts),
                "counts_by_user": dict(user_counts),
            },
        }

        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
