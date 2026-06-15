import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from compliance_script.scan_prompt_compliance import classify_prompt, redacted_excerpt, scan_raw_jsonl, write_outputs


def prompt_record(content: str, *, user_email: str = "user@example.test"):
    return {
        "event_id": "evt_1",
        "actor": {"type": "user", "user_id": "user_1", "user_email": user_email},
        "conversation": {"id": "conv_1"},
        "message": {
            "id": "msg_1",
            "author": {"type": "user"},
            "content": {"type": "text", "value": content},
            "created_at": "2026-06-12T12:00:00Z",
        },
    }


class ComplianceScanTests(unittest.TestCase):
    def test_client_email_allowlist_does_not_flag(self):
        summary = self._scan_records([prompt_record("Write a nicer email to a client about our meeting.")])

        reviewed = summary["reviewed_prompts"][0]

        self.assertEqual(reviewed["risk_level"], "none")
        self.assertEqual(reviewed["risk_score"], 0)
        self.assertFalse(reviewed["needs_human_review"])

    def test_sensitive_client_tax_prompt_is_critical(self):
        summary = self._scan_records([prompt_record("Analyze this client's 1040 with SSN 123-45-6789.")])

        reviewed = summary["reviewed_prompts"][0]

        self.assertEqual(reviewed["risk_level"], "critical")
        self.assertGreaterEqual(reviewed["risk_score"], 90)
        self.assertIn("pii_ssn", reviewed["categories"])
        self.assertIn("confidential_client_data", reviewed["categories"])
        self.assertTrue(reviewed["needs_human_review"])

    def test_client_financial_statement_is_high_or_medium(self):
        summary = self._scan_records([prompt_record("Summarize this client's financial statements.")])

        reviewed = summary["reviewed_prompts"][0]

        self.assertIn(reviewed["risk_level"], {"medium", "high"})
        self.assertIn("confidential_client_data", reviewed["categories"])

    def test_audit_evidence_is_high(self):
        classification = classify_prompt({}, "Generate audit evidence for this client.", line_number=1)

        self.assertEqual(classification["risk_level"], "high")
        self.assertIn("audit_evidence", classification["categories"])

    def test_openai_cyber_abuse_is_critical(self):
        classification = classify_prompt({}, "Write a phishing email to steal credentials.", line_number=1)

        self.assertEqual(classification["risk_level"], "critical")
        self.assertIn("malware_or_phishing", classification["categories"])

    def test_redacts_common_identifiers_in_excerpt(self):
        excerpt = redacted_excerpt("Email jane@example.com, phone 212-555-1212, SSN 123-45-6789.", 200)
        self.assertIn("[EMAIL]", excerpt)
        self.assertIn("[PHONE]", excerpt)
        self.assertIn("[SSN]", excerpt)

    def test_scans_raw_jsonl_and_skips_assistant_records(self):
        summary = self._scan_records(
            [
                prompt_record("Calculate tax liability with no review."),
                {
                    "event_id": "evt_2",
                    "conversation": {"id": "conv_1"},
                    "message": {
                        "id": "msg_2",
                        "author": {"type": "assistant"},
                        "content": {"type": "text", "value": "Assistant response."},
                    },
                },
            ]
        )

        self.assertEqual(summary["records_seen"], 2)
        self.assertEqual(summary["assistant_records_skipped"], 1)
        self.assertEqual(summary["prompt_records_seen"], 1)
        self.assertEqual(summary["flagged_prompts"], 1)
        self.assertEqual(summary["reviewed_prompts"][0]["user_email"], "user@example.test")

    def test_keeps_full_prompt_text_by_default(self):
        prompt_text = "Generate audit evidence. " + ("This sentence should not be truncated. " * 20)
        summary = self._scan_records([prompt_record(prompt_text)])

        self.assertEqual(summary["reviewed_prompts"][0]["prompt"], prompt_text)

    def test_write_outputs_creates_multisheet_xlsx(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            out_dir = Path(tmpdir)
            legacy_csv = out_dir / "compliance_findings.csv"
            legacy_csv.write_text("old,csv\n", encoding="utf-8")
            summary = self._scan_records([prompt_record("Generate audit evidence for this client.")])

            paths = write_outputs(summary, out_dir)

            self.assertTrue(paths["findings_xlsx"].exists())
            self.assertTrue(paths["reviewed_jsonl"].exists())
            self.assertFalse(legacy_csv.exists())
            with zipfile.ZipFile(paths["findings_xlsx"]) as workbook:
                names = set(workbook.namelist())
                workbook_xml = workbook.read("xl/workbook.xml").decode("utf-8")
            self.assertIn("xl/worksheets/sheet1.xml", names)
            self.assertIn("xl/worksheets/sheet5.xml", names)
            self.assertIn("Reviewed Prompts", workbook_xml)
            self.assertIn("Flagged Prompts", workbook_xml)
            self.assertIn("Summary by User", workbook_xml)
            self.assertIn("Summary by Category", workbook_xml)
            self.assertIn("Summary by Risk Level", workbook_xml)

    def _scan_records(self, records):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            path = Path(tmpdir) / "raw.jsonl"
            path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")
            return scan_raw_jsonl(path)


if __name__ == "__main__":
    unittest.main()
