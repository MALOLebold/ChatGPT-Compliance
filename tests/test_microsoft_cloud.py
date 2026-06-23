import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs

from compliance_script.microsoft_cloud import (
    ClientCredentials,
    HttpResponse,
    MicrosoftCloudClient,
    MicrosoftCloudError,
    PowerBIRefreshConfig,
    SharePointUploadConfig,
)


class FakeHttp:
    def __init__(self):
        self.calls = []

    def __call__(self, method, url, *, headers=None, data=None, timeout_seconds=60):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers or {},
                "data": data,
                "timeout_seconds": timeout_seconds,
            }
        )
        if "oauth2/v2.0/token" in url:
            return HttpResponse(200, json.dumps({"access_token": "token-123"}).encode("utf-8"), {})
        if url.endswith("/sites/tenant.sharepoint.com:/sites/Compliance"):
            return HttpResponse(200, json.dumps({"id": "site-123"}).encode("utf-8"), {})
        if url.endswith("/sites/site-123/drives"):
            return HttpResponse(
                200,
                json.dumps({"value": [{"id": "drive-123", "name": "Documents"}]}).encode("utf-8"),
                {},
            )
        if "/root:/" in url and url.endswith(":/content"):
            return HttpResponse(201, json.dumps({"id": "item-123", "name": "compliance_findings.xlsx"}).encode("utf-8"), {})
        if "api.powerbi.com" in url:
            return HttpResponse(202, b"", {"Location": "https://example.test/refresh", "x-ms-request-id": "req-123"})
        return HttpResponse(404, b"not found", {})


class MicrosoftCloudTests(unittest.TestCase):
    def test_upload_xlsx_uses_graph_drive_path(self):
        fake_http = FakeHttp()
        client = MicrosoftCloudClient(
            ClientCredentials(tenant_id="tenant", client_id="client", client_secret="secret-value"),
            http_request=fake_http,
        )
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            workbook = Path(tmpdir) / "compliance_findings.xlsx"
            workbook.write_bytes(b"xlsx bytes")

            result = client.upload_xlsx_to_sharepoint(
                SharePointUploadConfig(
                    site_id="site-123",
                    drive_id="drive-123",
                    folder_path="ChatGPT Compliance",
                    filename="compliance_findings.xlsx",
                ),
                workbook,
            )

        self.assertEqual(result["id"], "item-123")
        token_call = fake_http.calls[0]
        upload_call = fake_http.calls[1]
        self.assertEqual(token_call["method"], "POST")
        token_body = parse_qs(token_call["data"].decode("utf-8"))
        self.assertEqual(token_body["scope"], ["https://graph.microsoft.com/.default"])
        self.assertEqual(token_body["client_id"], ["client"])
        self.assertEqual(token_body["client_secret"], ["secret-value"])
        self.assertEqual(upload_call["method"], "PUT")
        self.assertEqual(
            upload_call["url"],
            "https://graph.microsoft.com/v1.0/drives/drive-123/root:/ChatGPT%20Compliance/compliance_findings.xlsx:/content",
        )
        self.assertEqual(upload_call["headers"]["Authorization"], "Bearer token-123")
        self.assertEqual(upload_call["data"], b"xlsx bytes")

    def test_upload_can_resolve_site_and_drive(self):
        fake_http = FakeHttp()
        client = MicrosoftCloudClient(
            ClientCredentials(tenant_id="tenant", client_id="client", client_secret="secret-value"),
            http_request=fake_http,
        )
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            workbook = Path(tmpdir) / "compliance_findings.xlsx"
            workbook.write_bytes(b"xlsx bytes")

            result = client.upload_xlsx_to_sharepoint(
                SharePointUploadConfig(
                    site_url="https://tenant.sharepoint.com/sites/Compliance",
                    drive_name="Documents",
                    filename="compliance_findings.xlsx",
                ),
                workbook,
            )

        self.assertEqual(result["name"], "compliance_findings.xlsx")
        self.assertEqual(fake_http.calls[1]["method"], "GET")
        self.assertIn("/sites/tenant.sharepoint.com:/sites/Compliance", fake_http.calls[1]["url"])
        self.assertEqual(fake_http.calls[2]["method"], "GET")
        self.assertIn("/sites/site-123/drives", fake_http.calls[2]["url"])

    def test_powerbi_refresh_uses_refresh_endpoint(self):
        fake_http = FakeHttp()
        client = MicrosoftCloudClient(
            ClientCredentials(tenant_id="tenant", client_id="client", client_secret="secret-value"),
            http_request=fake_http,
        )

        result = client.trigger_powerbi_refresh(PowerBIRefreshConfig(workspace_id="group-123", dataset_id="dataset-123"))

        self.assertEqual(result["status"], 202)
        refresh_call = fake_http.calls[1]
        self.assertEqual(refresh_call["method"], "POST")
        self.assertEqual(
            refresh_call["url"],
            "https://api.powerbi.com/v1.0/myorg/groups/group-123/datasets/dataset-123/refreshes",
        )
        self.assertEqual(refresh_call["headers"]["Authorization"], "Bearer token-123")
        self.assertEqual(json.loads(refresh_call["data"].decode("utf-8")), {})

    def test_invalid_sharepoint_filename_is_rejected(self):
        fake_http = FakeHttp()
        client = MicrosoftCloudClient(
            ClientCredentials(tenant_id="tenant", client_id="client", client_secret="secret-value"),
            http_request=fake_http,
        )
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            workbook = Path(tmpdir) / "compliance_findings.xlsx"
            workbook.write_bytes(b"xlsx bytes")

            with self.assertRaises(MicrosoftCloudError):
                client.upload_xlsx_to_sharepoint(
                    SharePointUploadConfig(site_id="site-123", drive_id="drive-123", filename="bad/name.xlsx"),
                    workbook,
                )


if __name__ == "__main__":
    unittest.main()
