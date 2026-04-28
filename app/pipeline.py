from __future__ import annotations

from datetime import date
from pathlib import Path

from app.config import Settings
from app.connectors.enterprise_api import EnterpriseComplianceAPIConnector
from app.connectors.file_source import FileComplianceSource
from app.models import PipelineRunSummary
from app.policy_engine import PolicyEvaluator, PolicyLibrary
from app.reporting import ReportBuilder
from app.storage import SQLiteStore
from app.utils.logger import get_logger


class ComplianceReportingPipeline:
    """Coordinates ingestion, evaluation, persistence, and report export."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.logger = get_logger(settings.log_file_path)
        self.store = SQLiteStore(settings.database_path)
        self.evaluator = PolicyEvaluator(
            openai_library=PolicyLibrary(settings.openai_rules_path, label="openai"),
            company_library=PolicyLibrary(settings.company_rules_path, label="company"),
        )
        self.report_builder = ReportBuilder(settings.report_output_dir)

    def seed_sample_file(self, sample_path: Path) -> None:
        self.logger.info("Sample file available at %s", sample_path)

    def run(
        self,
        period: str,
        start_date: date,
        end_date: date,
        source: str,
        input_file: Path | None = None,
    ) -> PipelineRunSummary:
        reporting_period = f"{period}:{start_date.isoformat()}:{end_date.isoformat()}"
        run_id = self.store.start_run(period=period, start_date=start_date, end_date=end_date, source=source)
        self.logger.info(
            "Starting compliance reporting run %s for %s (%s to %s) via %s",
            run_id,
            period,
            start_date,
            end_date,
            source,
        )

        connector = self._select_connector(source)
        records = connector.fetch_records(start_date=start_date, end_date=end_date, input_file=input_file)

        inserted, duplicates = self.store.save_raw_records(records, reporting_period)
        evaluated_records = [
            self.evaluator.evaluate(record=record, report_period=reporting_period)
            for record in records
        ]
        self.store.save_evaluated_records(evaluated_records)

        summary = PipelineRunSummary(
            run_id=run_id,
            period=period,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            source=source,
            records_pulled=len(records),
            records_inserted=inserted,
            duplicates_skipped=duplicates,
            evaluated_records=len(evaluated_records),
            no_match_count=sum(1 for item in evaluated_records if item.disposition == "no_match"),
            openai_flag_count=sum(
                1
                for item in evaluated_records
                if item.disposition == "potential_openai_policy_issue"
            ),
            company_flag_count=sum(
                1
                for item in evaluated_records
                if item.disposition == "potential_company_policy_issue"
            ),
            dual_flag_count=sum(
                1 for item in evaluated_records if item.disposition == "potential_both"
            ),
            failures=0,
            detail_report_path="",
            summary_report_path="",
        )
        detail_path, summary_path = self.report_builder.build(evaluated_records, summary)
        summary.detail_report_path = str(detail_path)
        summary.summary_report_path = str(summary_path)
        self.store.complete_run(summary)

        self.logger.info(
            "Completed run %s. Pulled=%s inserted=%s duplicates=%s evaluated=%s",
            run_id,
            summary.records_pulled,
            summary.records_inserted,
            summary.duplicates_skipped,
            summary.evaluated_records,
        )
        return summary

    def _select_connector(self, source: str):
        if source == "enterprise_api":
            return EnterpriseComplianceAPIConnector(self.settings)
        if source == "file":
            return FileComplianceSource()
        raise ValueError(f"Unsupported source: {source}")
