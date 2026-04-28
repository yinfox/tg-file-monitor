import json
import os
import tempfile
import unittest
from unittest.mock import patch

import app.app as app_module


class SelfServiceRecordHistoryTestCase(unittest.TestCase):
    def _write_results(self, file_path: str, payload: dict) -> None:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def test_recent_records_only_include_success_and_deleted(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result_file = os.path.join(tmp_dir, "self_service_results.json")
            self._write_results(
                result_file,
                {
                    "rid-success": {"status": "success", "message": "ok", "updated_at": 2000},
                    "rid-deleted": {"status": "deleted", "message": "deleted", "updated_at": 3000},
                    "rid-error": {"status": "error", "message": "error", "updated_at": 4000},
                    "rid-processing": {"status": "processing", "message": "processing", "updated_at": 5000},
                    "rid-partial": {"status": "partial", "message": "partial", "updated_at": 6000},
                },
            )

            with patch.object(app_module, "SELF_SERVICE_RESULT_FILE", result_file):
                records = app_module._list_self_service_results(limit=20)

        self.assertEqual([item["request_id"] for item in records], ["rid-deleted", "rid-success"])
        self.assertEqual([item["status"] for item in records], ["deleted", "success"])

    def test_public_records_only_include_successful_items_for_current_binding(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result_file = os.path.join(tmp_dir, "self_service_results.json")
            self._write_results(
                result_file,
                {
                    "rid-a": {
                        "status": "success",
                        "message": "ok",
                        "updated_at": 1000,
                        "emby_user_id": "user-1",
                        "requester_account": "tester",
                    },
                    "rid-b": {
                        "status": "error",
                        "message": "bad",
                        "updated_at": 2000,
                        "emby_user_id": "user-1",
                        "requester_account": "tester",
                    },
                    "rid-c": {
                        "status": "deleted",
                        "message": "removed",
                        "updated_at": 3000,
                        "emby_user_id": "user-1",
                        "requester_account": "tester",
                    },
                    "rid-d": {
                        "status": "success",
                        "message": "other user",
                        "updated_at": 4000,
                        "emby_user_id": "user-2",
                        "requester_account": "other",
                    },
                },
            )

            with patch.object(app_module, "SELF_SERVICE_RESULT_FILE", result_file):
                records = app_module._list_self_service_results_for_binding(
                    {"id": "user-1", "account": "tester"},
                    limit=20,
                )

        self.assertEqual([item["request_id"] for item in records], ["rid-c", "rid-a"])
        self.assertEqual([item["status"] for item in records], ["deleted", "success"])


if __name__ == "__main__":
    unittest.main()
