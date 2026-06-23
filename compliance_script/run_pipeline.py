from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from gpt_compliance_exporter.client import API_BASE, ComplianceAPIError, ComplianceClient
from gpt_compliance_exporter.exporter import ExportConfig, ExportResult, export_logs

from .microsoft_cloud import (
    ClientCredentials,
    MicrosoftCloudClient,
    MicrosoftCloudError,
    PowerBIRefreshConfig,
    SharePointUploadConfig,
)
from .scan_prompt_compliance import DEFAULT_EXCERPT_CHARS, export_results, scan_raw_jsonl


DEFAULT_EVENT_TYPE = "CONVERSATION_MESSAGE"
DEFAULT_SHAREPOINT_FILENAME = "compliance_findings.xlsx"


class PipelineConfigurationError(ValueError):
    """Raised when the pipeline is missing required runtime configuration."""


@dataclass(frozen=True)
class PipelineConfig:
    principal_id: str
    event_type: str
    export_dir: Path
    scan_out_dir: Path
    sharepoint_site_id: Optional[str] = None
    sharepoint_site_url: Optional[str] = None
    sharepoint_drive_id: Optional[str] = None
    sharepoint_drive_name: str = "Documents"
    sharepoint_folder: str = ""
    sharepoint_filename: str = DEFAULT_SHAREPOINT_FILENAME
    powerbi_workspace_id: Optional[str] = None
    powerbi_dataset_id: Optional[str] = None
    powerbi_notify_option: Optional[str] = None
    microsoft_tenant_id: Optional[str] = None
    microsoft_client_id: Optional[str] = None
    microsoft_client_secret: Optional[str] = None
    days: int = 30
    limit: int = 100
    after: Optional[str] = None
    api_base: str = API_BASE
    redact_prompt: bool = False
    excerpt_chars: int = DEFAULT_EXCERPT_CHARS


@dataclass(frozen=True)
class PipelineResult:
    export_result: ExportResult
    scan_summary: Dict[str, Any]
    scan_paths: Dict[str, Path]
    sharepoint_upload: Dict[str, Any]
    powerbi_refresh: Optional[Dict[str, Any]]


def run_pipeline(
    config: PipelineConfig,
    *,
    api_key: Optional[str] = None,
    client: Optional[ComplianceClient] = None,
    cloud_client: Optional[MicrosoftCloudClient] = None,
) -> PipelineResult:
    effective_api_key = api_key if api_key is not None else os.environ.get("COMPLIANCE_API_KEY")
    if not effective_api_key:
        raise PipelineConfigurationError("COMPLIANCE_API_KEY environment variable is required.")

    _validate_cloud_config(config)

    compliance_client = client or ComplianceClient(api_key=effective_api_key, api_base=config.api_base)
    export_result = export_logs(
        ExportConfig(
            principal_id=config.principal_id,
            event_type=config.event_type,
            out_dir=config.export_dir,
            days=config.days,
            limit=config.limit,
            after=config.after,
        ),
        client=compliance_client,
    )

    scan_summary = scan_raw_jsonl(
        export_result.raw_path,
        redact_prompt=config.redact_prompt,
        excerpt_chars=config.excerpt_chars,
    )
    scan_paths = export_results(scan_summary, config.scan_out_dir)

    microsoft_client = cloud_client or MicrosoftCloudClient(_microsoft_credentials(config))
    sharepoint_upload = microsoft_client.upload_xlsx_to_sharepoint(
        SharePointUploadConfig(
            site_id=config.sharepoint_site_id,
            site_url=config.sharepoint_site_url,
            drive_id=config.sharepoint_drive_id,
            drive_name=config.sharepoint_drive_name,
            folder_path=config.sharepoint_folder,
            filename=config.sharepoint_filename,
        ),
        scan_paths["findings_xlsx"],
    )

    powerbi_refresh = None
    if config.powerbi_workspace_id and config.powerbi_dataset_id:
        powerbi_refresh = microsoft_client.trigger_powerbi_refresh(
            PowerBIRefreshConfig(
                workspace_id=config.powerbi_workspace_id,
                dataset_id=config.powerbi_dataset_id,
                notify_option=config.powerbi_notify_option,
            )
        )

    return PipelineResult(
        export_result=export_result,
        scan_summary=scan_summary,
        scan_paths=scan_paths,
        sharepoint_upload=sharepoint_upload,
        powerbi_refresh=powerbi_refresh,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export ChatGPT Compliance Logs, scan prompts, upload the XLSX to SharePoint, and optionally refresh Power BI.",
    )
    parser.add_argument("--principal-id", required=True, help="ChatGPT workspace ID or org-... organization ID.")
    parser.add_argument(
        "--event-type",
        default=DEFAULT_EVENT_TYPE,
        help=f"Compliance Logs event type to export. Defaults to {DEFAULT_EVENT_TYPE}.",
    )
    parser.add_argument("--export-dir", default=Path("exports"), type=Path, help="Directory for exporter outputs.")
    parser.add_argument(
        "--scan-out-dir",
        default=Path("compliance_script/output"),
        type=Path,
        help="Directory for compliance scanner outputs.",
    )
    parser.add_argument("--sharepoint-site-id", help="Microsoft Graph SharePoint site ID.")
    parser.add_argument(
        "--sharepoint-site-url",
        help="SharePoint site URL, used to resolve the site ID when --sharepoint-site-id is omitted.",
    )
    parser.add_argument("--sharepoint-drive-id", help="Microsoft Graph drive/document library ID.")
    parser.add_argument(
        "--sharepoint-drive-name",
        default="Documents",
        help="SharePoint document library name to resolve when --sharepoint-drive-id is omitted.",
    )
    parser.add_argument(
        "--sharepoint-folder",
        default="",
        help="Folder path inside the SharePoint document library.",
    )
    parser.add_argument(
        "--sharepoint-filename",
        default=DEFAULT_SHAREPOINT_FILENAME,
        help=f"Uploaded workbook file name. Defaults to {DEFAULT_SHAREPOINT_FILENAME}.",
    )
    parser.add_argument("--powerbi-workspace-id", help="Power BI workspace/group ID for optional refresh.")
    parser.add_argument("--powerbi-dataset-id", help="Power BI dataset/semantic model ID for optional refresh.")
    parser.add_argument(
        "--powerbi-notify-option",
        choices=("NoNotification", "MailOnFailure", "MailOnCompletion"),
        help="Optional Power BI notifyOption payload value.",
    )
    parser.add_argument(
        "--microsoft-tenant-id",
        help="Microsoft tenant ID. Defaults to MICROSOFT_TENANT_ID or AZURE_TENANT_ID.",
    )
    parser.add_argument(
        "--microsoft-client-id",
        help="Entra app client ID. Defaults to MICROSOFT_CLIENT_ID or AZURE_CLIENT_ID.",
    )
    parser.add_argument(
        "--microsoft-client-secret",
        help="Entra app client secret. Prefer MICROSOFT_CLIENT_SECRET or AZURE_CLIENT_SECRET instead of passing this on the command line.",
    )
    parser.add_argument("--days", default=30, type=int, help="Days to look back when --after is omitted.")
    parser.add_argument("--after", help="ISO-8601 timestamp to use as the initial Compliance Logs cursor.")
    parser.add_argument("--limit", default=100, type=int, help="Page size for Compliance Logs listings.")
    parser.add_argument(
        "--redact-prompt",
        action="store_true",
        help="Store redacted prompt excerpts instead of full prompt text in scanner outputs.",
    )
    parser.add_argument(
        "--excerpt-chars",
        default=DEFAULT_EXCERPT_CHARS,
        type=int,
        help="Maximum redacted excerpt length when --redact-prompt is used.",
    )
    parser.add_argument("--api-base", default=API_BASE, help=argparse.SUPPRESS)
    return parser


def config_from_args(args: argparse.Namespace) -> PipelineConfig:
    return PipelineConfig(
        principal_id=args.principal_id,
        event_type=args.event_type,
        export_dir=args.export_dir,
        scan_out_dir=args.scan_out_dir,
        sharepoint_site_id=args.sharepoint_site_id,
        sharepoint_site_url=args.sharepoint_site_url,
        sharepoint_drive_id=args.sharepoint_drive_id,
        sharepoint_drive_name=args.sharepoint_drive_name,
        sharepoint_folder=args.sharepoint_folder,
        sharepoint_filename=args.sharepoint_filename,
        powerbi_workspace_id=args.powerbi_workspace_id,
        powerbi_dataset_id=args.powerbi_dataset_id,
        powerbi_notify_option=args.powerbi_notify_option,
        microsoft_tenant_id=args.microsoft_tenant_id,
        microsoft_client_id=args.microsoft_client_id,
        microsoft_client_secret=args.microsoft_client_secret,
        days=args.days,
        limit=args.limit,
        after=args.after,
        api_base=args.api_base,
        redact_prompt=args.redact_prompt,
        excerpt_chars=args.excerpt_chars,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        result = run_pipeline(config_from_args(args))
    except PipelineConfigurationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except ComplianceAPIError as exc:
        print(str(exc), file=sys.stderr)
        if exc.body:
            print(exc.body, file=sys.stderr)
        return 1
    except MicrosoftCloudError as exc:
        print(str(exc), file=sys.stderr)
        if exc.body:
            print(exc.body, file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    summary = result.scan_summary
    print(f"Wrote raw logs to {result.export_result.raw_path}")
    print(f"Wrote scanner workbook to {result.scan_paths['findings_xlsx']}")
    print(f"Uploaded workbook to SharePoint as {result.sharepoint_upload.get('name', '<unknown>')}")
    if result.powerbi_refresh is not None:
        print("Triggered Power BI refresh")
    print(f"Prompt records reviewed: {summary['prompt_records_seen']}")
    print(f"Flagged prompts: {summary['flagged_prompts']}")
    return 0


def _validate_cloud_config(config: PipelineConfig) -> None:
    if not (config.sharepoint_site_id or config.sharepoint_site_url):
        raise PipelineConfigurationError("--sharepoint-site-id or --sharepoint-site-url is required.")
    if not (config.sharepoint_drive_id or config.sharepoint_drive_name):
        raise PipelineConfigurationError("--sharepoint-drive-id or --sharepoint-drive-name is required.")
    if bool(config.powerbi_workspace_id) != bool(config.powerbi_dataset_id):
        raise PipelineConfigurationError("--powerbi-workspace-id and --powerbi-dataset-id must be supplied together.")
    _microsoft_credentials(config)


def _microsoft_credentials(config: PipelineConfig) -> ClientCredentials:
    tenant_id = config.microsoft_tenant_id or os.environ.get("MICROSOFT_TENANT_ID") or os.environ.get("AZURE_TENANT_ID")
    client_id = config.microsoft_client_id or os.environ.get("MICROSOFT_CLIENT_ID") or os.environ.get("AZURE_CLIENT_ID")
    client_secret = (
        config.microsoft_client_secret
        or os.environ.get("MICROSOFT_CLIENT_SECRET")
        or os.environ.get("AZURE_CLIENT_SECRET")
    )
    missing = [
        name
        for name, value in (
            ("MICROSOFT_TENANT_ID", tenant_id),
            ("MICROSOFT_CLIENT_ID", client_id),
            ("MICROSOFT_CLIENT_SECRET", client_secret),
        )
        if not value
    ]
    if missing:
        raise PipelineConfigurationError(f"Missing Microsoft cloud credential values: {', '.join(missing)}.")
    return ClientCredentials(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)


if __name__ == "__main__":
    raise SystemExit(main())
