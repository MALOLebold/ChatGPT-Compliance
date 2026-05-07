import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from gpt_compliance_exporter.exporter import ExportConfig, decode_log_payload, export_logs, isoformat_utc


class FakeClient:
    def __init__(self):
        self.pages = [
            {
                "data": [{"id": "log_1"}],
                "has_more": True,
                "last_end_time": "2026-05-01T01:00:00Z",
            },
            {
                "data": [{"id": "log_2"}],
                "has_more": False,
                "last_end_time": "2026-05-01T02:00:00Z",
            },
        ]
        self.after_values = []

    def list_logs(self, *, principal_id, event_type, limit, after):
        self.after_values.append(after)
        return self.pages.pop(0)

    def download_log(self, *, principal_id, log_id):
        if log_id == "log_1":
            return json.dumps(
                {
                    "event_id": "evt_1",
                    "conversation_id": "conv_1",
                    "message": {"id": "msg_1", "role": "user", "content": "hello"},
                }
            )
        return json.dumps(
            {
                "event_id": "evt_2",
                "conversation_id": "conv_2",
                "message": {"id": "msg_2", "role": "assistant", "content": "hi"},
            }
        )


class ExporterTests(unittest.TestCase):
    def test_isoformat_utc(self):
        value = datetime(2026, 5, 7, 12, 30, 5, tzinfo=timezone.utc)
        self.assertEqual(isoformat_utc(value), "2026-05-07T12:30:05Z")

    def test_decode_json_object_payload(self):
        records, warnings = decode_log_payload('{"role":"user","content":"hello"}', source_log_id="log_1")
        self.assertEqual(records, [{"role": "user", "content": "hello"}])
        self.assertEqual(warnings, [])

    def test_decode_jsonl_payload(self):
        records, warnings = decode_log_payload('{"a":1}\n{"b":2}\n', source_log_id="log_1")
        self.assertEqual(records, [{"a": 1}, {"b": 2}])
        self.assertEqual(warnings, [])

    def test_export_logs_writes_outputs_and_paginates(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmpdir:
            client = FakeClient()
            result = export_logs(
                ExportConfig(
                    principal_id="workspace-123",
                    event_type="CONVERSATION",
                    out_dir=Path(tmpdir),
                    days=30,
                    limit=100,
                ),
                client=client,
                now=datetime(2026, 5, 7, 0, 0, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(client.after_values, ["2026-04-07T00:00:00Z", "2026-05-01T01:00:00Z"])
            self.assertTrue(result.raw_path.exists())
            self.assertTrue(result.prompts_path.exists())
            self.assertTrue(result.manifest_path.exists())

            raw_records = [json.loads(line) for line in result.raw_path.read_text(encoding="utf-8").splitlines()]
            prompt_records = [json.loads(line) for line in result.prompts_path.read_text(encoding="utf-8").splitlines()]
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

            self.assertEqual(len(raw_records), 2)
            self.assertEqual(len(prompt_records), 1)
            self.assertEqual(prompt_records[0]["content"], "hello")
            self.assertEqual(manifest["scope_type"], "workspace")
            self.assertEqual(manifest["raw_records_written"], 2)
            self.assertEqual(manifest["prompt_records_written"], 1)


if __name__ == "__main__":
    unittest.main()
