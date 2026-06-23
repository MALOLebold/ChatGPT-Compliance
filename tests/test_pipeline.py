import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from compliance_script.run_pipeline import DEFAULT_EVENT_TYPE, PipelineConfig, build_parser, main, run_pipeline


def prompt_record(content: str):
    return {
        "event_id": "evt_1",
        "actor": {"type": "user", "user_id": "user_1", "user_email": "user@example.test"},
        "timestamp": "2026-06-12T13:00:00Z",
        "conversation": {"id": "conv_1"},
        "message": {
            "id": "msg_1",
            "author": {"type": "user"},
            "content": {"type": "text", "value": content},
            "created_at": "2026-06-12T12:59:59Z",
        },
    }


class FakeComplianceClient:
    def __init__(self, content: str = "Write a client meeting agenda."):
        self.content = content
        self.list_calls = []

    def list_logs(self, *, principal_id, event_type, limit, after):
        self.list_calls.append(
            {
                "principal_id": principal_id,
                "event_type": event_type,
                "limit": limit,
                "after": after,
            }
        )
        return {
            "data": [{"id": "log_1"}],
            "has_more": False,
            "last_end_time": "2026-06-12T13:00:00Z",
        }

    def download_log(self, *, principal_id, log_id):
        return json.dumps(prompt_record(self.content))


class FakeCloudClient:
    def __init__(self):
        self.uploads = []
        self.refreshes = []

    def upload_xlsx_to_sharepoint(self, config, xlsx_path):
        self.uploads.append((config, xlsx_path))
        return {"id": "item_1", "name": config.filename}

    def trigger_powerbi_refresh(self, config):
        self.refreshes.append(config)
        return {"status": 202, "location": "https://example.test/refresh"}


class PipelineTests(unittest.TestCase):
    def test_parser_uses_conversation_message_default(self):
        args = build_parser().parse_args(
            [
                "--principal-id",
                "workspace-123",
                "--sharepoint-site-id",
                "site-123",
            ]
        )

        self.assertEqual(args.event_type, DEFAULT_EVENT_TYPE)
        self.assertEqual(args.days, 30)
        self.assertEqual(args.limit, 100)
        self.assertEqual(args.sharepoint_filename, "compliance_findings.xlsx")

    def test_missing_api_key_returns_configuration_error(self):
        stderr = io.StringIO()
        with patch.dict(
            "os.environ",
            {
                "MICROSOFT_TENANT_ID": "tenant",
                "MICROSOFT_CLIENT_ID": "client",
                "MICROSOFT_CLIENT_SECRET": "secret-value",
            },
            clear=True,
        ), redirect_stderr(stderr):
            code = main(
                [
                    "--principal-id",
                    "workspace-123",
                    "--sharepoint-site-id",
                    "site-123",
                ]
            )

        self.assertEqual(code, 2)
        self.assertIn("COMPLIANCE_API_KEY", stderr.getvalue())
        self.assertNotIn("secret-value", stderr.getvalue())

    def test_missing_microsoft_credentials_returns_configuration_error_without_secret(self):
        stderr = io.StringIO()
        with patch.dict("os.environ", {"COMPLIANCE_API_KEY": "compliance-key"}, clear=True), redirect_stderr(stderr):
            code = main(
                [
                    "--principal-id",
                    "workspace-123",
                    "--sharepoint-site-id",
                    "site-123",
                ]
            )

        self.assertEqual(code, 2)
        self.assertIn("MICROSOFT_TENANT_ID", stderr.getvalue())
        self.assertIn("MICROSOFT_CLIENT_ID", stderr.getvalue())
        self.assertIn("MICROSOFT_CLIENT_SECRET", stderr.getvalue())
        self.assertNotIn("compliance-key", stderr.getvalue())

    def test_run_pipeline_writes_outputs_and_uploads_workbook(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            root = Path(tmpdir)
            cloud_client = FakeCloudClient()
            config = PipelineConfig(
                principal_id="workspace-123",
                event_type=DEFAULT_EVENT_TYPE,
                export_dir=root / "exports",
                scan_out_dir=root / "scan",
                sharepoint_site_id="site-123",
                sharepoint_drive_id="drive-123",
                sharepoint_folder="ChatGPT Compliance",
                microsoft_tenant_id="tenant",
                microsoft_client_id="client",
                microsoft_client_secret="secret",
            )

            result = run_pipeline(config, api_key="test-key", client=FakeComplianceClient(), cloud_client=cloud_client)

            self.assertTrue(result.export_result.raw_path.exists())
            self.assertTrue(result.scan_paths["findings_xlsx"].exists())
            self.assertEqual(result.scan_summary["prompt_records_seen"], 1)
            self.assertEqual(result.scan_summary["flagged_prompts"], 0)
            self.assertEqual(len(cloud_client.uploads), 1)
            self.assertEqual(cloud_client.uploads[0][0].filename, "compliance_findings.xlsx")
            self.assertEqual(result.sharepoint_upload["name"], "compliance_findings.xlsx")
            self.assertIsNone(result.powerbi_refresh)

    def test_flagged_prompts_do_not_fail_pipeline(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            root = Path(tmpdir)
            cloud_client = FakeCloudClient()
            config = PipelineConfig(
                principal_id="workspace-123",
                event_type=DEFAULT_EVENT_TYPE,
                export_dir=root / "exports",
                scan_out_dir=root / "scan",
                sharepoint_site_id="site-123",
                sharepoint_drive_id="drive-123",
                microsoft_tenant_id="tenant",
                microsoft_client_id="client",
                microsoft_client_secret="secret",
            )

            result = run_pipeline(
                config,
                api_key="test-key",
                client=FakeComplianceClient("Analyze this client's 1040 with SSN 123-45-6789."),
                cloud_client=cloud_client,
            )

            self.assertEqual(result.scan_summary["flagged_prompts"], 1)
            self.assertEqual(len(cloud_client.uploads), 1)

    def test_powerbi_refresh_runs_after_sharepoint_upload_when_configured(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            root = Path(tmpdir)
            cloud_client = FakeCloudClient()
            config = PipelineConfig(
                principal_id="workspace-123",
                event_type=DEFAULT_EVENT_TYPE,
                export_dir=root / "exports",
                scan_out_dir=root / "scan",
                sharepoint_site_id="site-123",
                sharepoint_drive_id="drive-123",
                powerbi_workspace_id="workspace-id",
                powerbi_dataset_id="dataset-id",
                microsoft_tenant_id="tenant",
                microsoft_client_id="client",
                microsoft_client_secret="secret",
            )

            result = run_pipeline(config, api_key="test-key", client=FakeComplianceClient(), cloud_client=cloud_client)

            self.assertEqual(len(cloud_client.uploads), 1)
            self.assertEqual(len(cloud_client.refreshes), 1)
            self.assertEqual(cloud_client.refreshes[0].workspace_id, "workspace-id")
            self.assertEqual(cloud_client.refreshes[0].dataset_id, "dataset-id")
            self.assertEqual(result.powerbi_refresh["status"], 202)


if __name__ == "__main__":
    unittest.main()
