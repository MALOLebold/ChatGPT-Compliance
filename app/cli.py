from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

from app.config import Settings
from app.pipeline import ComplianceReportingPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run weekly/monthly ChatGPT Enterprise compliance reporting."
    )
    parser.add_argument(
        "command",
        choices=["run", "seed-sample"],
        help="Pipeline command to execute.",
    )
    parser.add_argument(
        "--period",
        choices=["weekly", "monthly"],
        default="weekly",
        help="Reporting cadence for this run.",
    )
    parser.add_argument(
        "--start-date",
        help="Inclusive start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        help="Inclusive end date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--source",
        choices=["enterprise_api", "file"],
        default="file",
        help="Source connector to use for ingestion.",
    )
    parser.add_argument(
        "--input-file",
        help="Path to a JSON or NDJSON file when using the file source.",
    )
    parser.add_argument(
        "--output-dir",
        help="Override the report output directory.",
    )
    return parser


def parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def default_window(period: str) -> tuple[date, date]:
    today = date.today()
    if period == "weekly":
        end = today - timedelta(days=1)
        start = end - timedelta(days=6)
        return start, end

    first_of_current_month = today.replace(day=1)
    end = first_of_current_month - timedelta(days=1)
    start = end.replace(day=1)
    return start, end


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings.from_env()

    if args.output_dir:
        settings.report_output_dir = Path(args.output_dir)

    pipeline = ComplianceReportingPipeline(settings=settings)

    if args.command == "seed-sample":
        sample_path = Path("data/sample_exports/chatgpt_enterprise_logs.ndjson")
        pipeline.seed_sample_file(sample_path)
        return

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if start_date is None or end_date is None:
        start_date, end_date = default_window(args.period)

    pipeline.run(
        period=args.period,
        start_date=start_date,
        end_date=end_date,
        source=args.source,
        input_file=Path(args.input_file) if args.input_file else None,
    )


if __name__ == "__main__":
    main()
