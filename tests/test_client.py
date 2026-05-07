import unittest

from gpt_compliance_exporter.client import ComplianceClient, scope_segment_for_principal, scope_type_for_principal


class ClientTests(unittest.TestCase):
    def test_scope_for_workspace_id(self):
        self.assertEqual(scope_segment_for_principal("workspace-123"), "workspaces")
        self.assertEqual(scope_type_for_principal("workspace-123"), "workspace")

    def test_scope_for_org_id(self):
        self.assertEqual(scope_segment_for_principal("org-abc"), "organizations")
        self.assertEqual(scope_type_for_principal("org-abc"), "organization")

    def test_build_url_for_workspace(self):
        client = ComplianceClient(api_key="test", api_base="https://example.test/v1/compliance")
        url = client.build_url(
            "workspace-123",
            "logs",
            {"limit": 100, "event_type": "CONVERSATION", "after": "2026-01-01T00:00:00Z"},
        )
        self.assertEqual(
            url,
            "https://example.test/v1/compliance/workspaces/workspace-123/logs"
            "?limit=100&event_type=CONVERSATION&after=2026-01-01T00%3A00%3A00Z",
        )

    def test_build_url_for_organization(self):
        client = ComplianceClient(api_key="test", api_base="https://example.test/v1/compliance")
        self.assertEqual(
            client.build_url("org-abc", "logs/log_1"),
            "https://example.test/v1/compliance/organizations/org-abc/logs/log_1",
        )


if __name__ == "__main__":
    unittest.main()
