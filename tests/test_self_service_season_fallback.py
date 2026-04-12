import unittest
from unittest.mock import patch

import app.app as app_module


class SelfServiceSeasonFallbackTestCase(unittest.TestCase):
    def test_prioritize_candidates_keeps_unknown_when_season_strict(self):
        candidates = [
            "ABCDEFGHIJKLMNOP",
            "QRSTUVWXYZABCDEF",
        ]

        result = app_module._prioritize_hdhive_candidates(
            candidates,
            base_url="https://hdhive.com",
            season=[1],
            season_strict=True,
            max_items=5,
        )

        self.assertEqual(result, candidates)

    def test_run_self_service_strict_season_unknown_fallback_continues(self):
        payload = {
            "request_id": "rid-season-fallback",
            "query": "诊疗中",
            "title": "诊疗中",
            "notify_targets": ["@tester"],
            "max_results": 5,
            "hdhive_cookie": "",
            "base_url": "https://hdhive.com",
            "type": "电视剧",
            "year": "",
            "note": "",
            "hdhive_url": "",
            "use_open_api": True,
            "hdhive_open_api_key": "api-key",
            "tmdb_id": "12345",
            "unlock_threshold": 0,
            "open_api_direct_unlock": False,
            "storage_mode": "115",
            "dolby_preference": "any",
            "season": "S01",
            "resolution": "",
            "advanced_mode": False,
        }

        with patch("app.app._append_self_service_log"), \
             patch("app.app._hdhive_open_api_resources", return_value={
                 "success": True,
                 "data": [
                     {
                         "slug": "ABCDEFGHIJKLMNOP",
                         "title": "诊疗中",
                         "unlock_points": 0,
                     }
                 ],
             }), \
             patch("app.app._hdhive_open_api_unlock", return_value={
                 "success": True,
                 "data": {
                     "full_url": "https://115.com/s/demo?password=abcd",
                 },
             }), \
             patch("app.app._try_115_share_transfer", return_value=(True, "transfer_ok")), \
             patch("app.app._set_self_service_result") as set_result, \
             patch("app.app._self_service_notify_result"):
            app_module._run_self_service_request(payload)

        set_result.assert_called()
        rid, status, message, detail = set_result.call_args[0][:4]
        self.assertEqual(rid, "rid-season-fallback")
        self.assertEqual(status, "success")
        self.assertIn("资源已入库", message)
        self.assertIn("来源: Open API 搜索", detail)

    def test_run_self_service_strict_season_explicit_mismatch_still_fails(self):
        payload = {
            "request_id": "rid-season-mismatch",
            "query": "诊疗中",
            "title": "诊疗中",
            "notify_targets": ["@tester"],
            "max_results": 5,
            "hdhive_cookie": "",
            "base_url": "https://hdhive.com",
            "type": "电视剧",
            "year": "",
            "note": "",
            "hdhive_url": "",
            "use_open_api": True,
            "hdhive_open_api_key": "api-key",
            "tmdb_id": "12345",
            "unlock_threshold": 0,
            "open_api_direct_unlock": False,
            "storage_mode": "115",
            "dolby_preference": "any",
            "season": "S01",
            "resolution": "",
            "advanced_mode": False,
        }

        with patch("app.app._append_self_service_log"), \
             patch("app.app._hdhive_open_api_resources", return_value={
                 "success": True,
                 "data": [
                     {
                         "slug": "ABCDEFGHIJKLMNOP",
                         "title": "诊疗中 第二季",
                         "season": 2,
                         "unlock_points": 0,
                     }
                 ],
             }), \
             patch("app.app._hdhive_open_api_unlock") as open_api_unlock, \
             patch("app.app._try_115_share_transfer") as transfer, \
             patch("app.app._set_self_service_result") as set_result, \
             patch("app.app._self_service_notify_result"):
            app_module._run_self_service_request(payload)

        set_result.assert_called()
        rid, status, _message, detail = set_result.call_args[0][:4]
        self.assertEqual(rid, "rid-season-mismatch")
        self.assertEqual(status, "error")
        self.assertIn("未找到所选季", detail)
        open_api_unlock.assert_not_called()
        transfer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
