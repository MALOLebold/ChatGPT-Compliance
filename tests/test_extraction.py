import unittest

from gpt_compliance_exporter.extraction import extract_prompts


class ExtractionTests(unittest.TestCase):
    def test_extracts_simple_role_content_prompt(self):
        raw = {
            "id": "evt_1",
            "conversation_id": "conv_1",
            "user_id": "user_1",
            "created_at": "2026-05-01T12:00:00Z",
            "message": {
                "id": "msg_1",
                "role": "user",
                "content": "hello",
            },
        }

        prompts = extract_prompts(raw, source_log_id="log_1")

        self.assertEqual(len(prompts), 1)
        self.assertEqual(prompts[0]["source_log_id"], "log_1")
        self.assertEqual(prompts[0]["conversation_id"], "conv_1")
        self.assertEqual(prompts[0]["message_id"], "msg_1")
        self.assertEqual(prompts[0]["user_id"], "user_1")
        self.assertEqual(prompts[0]["content"], "hello")
        self.assertEqual(prompts[0]["role"], "user")

    def test_extracts_chatgpt_author_parts_shape(self):
        raw = {
            "event_id": "evt_1",
            "conversation": {"id": "conv_1"},
            "message": {
                "id": "msg_1",
                "author": {"role": "user", "id": "user_1"},
                "content": {"content_type": "text", "parts": ["What is policy?"]},
                "create_time": 1770000000,
            },
        }

        prompts = extract_prompts(raw, source_log_id="log_1")

        self.assertEqual(len(prompts), 1)
        self.assertEqual(prompts[0]["event_id"], "evt_1")
        self.assertEqual(prompts[0]["conversation_id"], "conv_1")
        self.assertEqual(prompts[0]["created_at"], 1770000000)
        self.assertEqual(prompts[0]["content"], "What is policy?")

    def test_ignores_assistant_messages(self):
        raw = {"role": "assistant", "content": "response"}

        prompts = extract_prompts(raw, source_log_id="log_1")

        self.assertEqual(prompts, [])

    def test_keeps_repeated_content_without_message_ids(self):
        raw = [
            {"role": "user", "content": "yes"},
            {"role": "user", "content": "yes"},
        ]

        prompts = extract_prompts(raw, source_log_id="log_1")

        self.assertEqual(len(prompts), 2)


if __name__ == "__main__":
    unittest.main()
