from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, Sequence

from .client import ComplianceAPIError, ComplianceClient
from .exporter import ExportConfig, export_logs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gpt_compliance_exporter",
        description="Export ChatGPT Enterprise Compliance Logs and extracted prompts.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export raw logs and prompt records.")
    export_parser.add_argument("--principal-id", required=True, help="Workspace ID or org-... organization ID.")
    export_parser.add_argument("--event-type", required=True, help="Compliance Logs event type to export.")
    export_parser.add_argument("--out-dir", default="exports", type=Path, help="Directory for output files.")
    export_parser.add_argument("--days", default=30, type=int, help="Days to look back when --after is omitted.")
    export_parser.add_argument("--after", help="ISO-8601 timestamp to use as the initial Compliance Logs cursor.")
    export_parser.add_argument("--limit", default=100, type=int, help="Page size for Compliance Logs listings.")
    export_parser.add_argument(
        "--api-base",
        default="https://api.chatgpt.com/v1/compliance",
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "export":
        return _run_export(args)

    parser.error(f"unknown command: {args.command}")
    return 2


def _run_export(args: argparse.Namespace) -> int:
    api_key = os.environ.get("COMPLIANCE_API_KEY")
    if not api_key:
        print("COMPLIANCE_API_KEY environment variable is required.", file=sys.stderr)
        return 2

    config = ExportConfig(
        principal_id=args.principal_id,
        event_type=args.event_type,
        out_dir=args.out_dir,
        days=args.days,
        limit=args.limit,
        after=args.after,
    )
    client = ComplianceClient(api_key=api_key, api_base=args.api_base)

    try:
        result = export_logs(config, client=client)
    except ComplianceAPIError as exc:
        print(str(exc), file=sys.stderr)
        if exc.body:
            print(exc.body, file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(f"Wrote raw logs to {result.raw_path}")
    print(f"Wrote prompts to {result.prompts_path}")
    print(f"Wrote manifest to {result.manifest_path}")
    print(f"Prompt records: {result.manifest['prompt_records_written']}")
    return 0
