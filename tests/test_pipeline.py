from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest import TestCase
import uuid

from app.config import Settings
from app.pipeline import ComplianceReportingPipeline


class PipelineTests(TestCase):
    def test_file_source_pipeline_generates_reports_and_deduplicates(self) -> None:
        base = Path("test_output") / str(uuid.uuid4())
        base.mkdir(parents=True, exist_ok=True)
        settings = Settings(
            database_path=base / "compliance.db",
            log_file_path=base / "app.log",
            report_output_dir=base / "reports",
            openai_rules_path=Path("data/policies/openai_policy_rules.json"),
            company_rules_path=Path("data/policies/company_policy_rules.json"),
            enterprise_base_url=None,
            enterprise_api_key=None,
            enterprise_events_path="/v1/compliance/logs/conversations",
            enterprise_timeout_seconds=30,
        )
        pipeline = ComplianceReportingPipeline(settings=settings)
        sample_file = Path("data/sample_exports/chatgpt_enterprise_logs.ndjson")

        first = pipeline.run(
            period="weekly",
            start_date=date(2026, 4, 20),
            end_date=date(2026, 4, 26),
            source="file",
            input_file=sample_file,
        )
        second = pipeline.run(
            period="weekly",
            start_date=date(2026, 4, 20),
            end_date=date(2026, 4, 26),
            source="file",
            input_file=sample_file,
        )

        self.assertEqual(first.records_pulled, 4)
        self.assertEqual(first.records_inserted, 4)
        self.assertEqual(first.dual_flag_count, 1)
        self.assertEqual(first.company_flag_count, 1)
        self.assertTrue(Path(first.detail_report_path).exists())
        self.assertTrue(Path(first.summary_report_path).exists())
        self.assertEqual(second.records_inserted, 0)
        self.assertEqual(second.duplicates_skipped, 4)
        self.assertTrue((base / "compliance.db").exists())
