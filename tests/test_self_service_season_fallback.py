import unittest
from unittest.mock import Mock, patch

import app.app as app_module


class SelfServiceSeasonFallbackTestCase(unittest.TestCase):
    def setUp(self):
        try:
            app_module._HDHIVE_OPEN_API_RATE_LIMIT_CACHE.clear()
        except Exception:
            pass

    def test_extract_season_candidates_supports_s_prefixed_range(self):
        result = app_module._extract_season_candidates("Slow Horses S01-S04 完结季")
        self.assertEqual(result, [1, 2, 3, 4])

    def test_extract_season_candidates_supports_full_total_seasons(self):
        result = app_module._extract_season_candidates("Slow Horses 全4季 4K")
        self.assertEqual(result, [1, 2, 3, 4])

    def test_extract_season_candidates_supports_plain_range_with_season_suffix(self):
        result = app_module._extract_season_candidates("Slow Horses 1-4季全")
        self.assertEqual(result, [1, 2, 3, 4])

    def test_resource_match_season_open_ended_pack_returns_unknown(self):
        item = {
            "title": "Slow Horses S01-S完结季",
            "season": 1,
        }
        result = app_module._resource_match_season(item, 3)
        self.assertIsNone(result)

    def test_resource_match_season_full_total_seasons_matches_requested(self):
        item = {
            "title": "Slow Horses 全4季 4K",
        }
        result = app_module._resource_match_season(item, 3)
        self.assertTrue(result)

    def test_open_api_rate_limit_allows_three_requests_per_minute(self):
        url = "https://hdhive.com/api/open/ping"
        api_key = "api-key"

        wait1 = app_module._reserve_hdhive_open_api_slot(url, api_key, now_ts=100.0)
        wait2 = app_module._reserve_hdhive_open_api_slot(url, api_key, now_ts=110.0)
        wait3 = app_module._reserve_hdhive_open_api_slot(url, api_key, now_ts=120.0)
        wait4 = app_module._reserve_hdhive_open_api_slot(url, api_key, now_ts=130.0)
        wait5 = app_module._reserve_hdhive_open_api_slot(url, api_key, now_ts=161.0)

        self.assertEqual(wait1, 0.0)
        self.assertEqual(wait2, 0.0)
        self.assertEqual(wait3, 0.0)
        self.assertGreater(wait4, 0.0)
        self.assertEqual(wait5, 0.0)

    def test_open_api_request_retries_once_when_429(self):
        too_many = Mock()
        too_many.status_code = 429
        too_many.headers = {"Retry-After": "2"}
        too_many.text = '{"success": false, "message": "too many requests"}'
        too_many.json.return_value = {"success": False, "code": "429", "message": "too many requests"}

        success = Mock()
        success.status_code = 200
        success.headers = {"Content-Type": "application/json"}
        success.text = '{"success": true}'
        success.json.return_value = {"success": True, "data": {"ok": True}}

        with patch("app.app._wait_for_hdhive_open_api_slot"), \
             patch("app.app.time.sleep") as sleep_mock, \
             patch("app.app._requests_request", side_effect=[too_many, success]) as req_mock:
            result = app_module._hdhive_open_api_request(
                "GET",
                "https://hdhive.com/api/open/ping",
                "api-key",
            )

        self.assertTrue(bool(result.get("success")))
        self.assertEqual(req_mock.call_count, 2)
        sleep_mock.assert_called_once_with(2)

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

    def test_run_self_service_strict_season_open_ended_pack_continues(self):
        payload = {
            "request_id": "rid-season-open-ended",
            "query": "Slow Horses",
            "title": "Slow Horses",
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
            "tmdb_id": "84773",
            "unlock_threshold": 0,
            "open_api_direct_unlock": False,
            "storage_mode": "115",
            "dolby_preference": "any",
            "season": "S03",
            "resolution": "",
            "advanced_mode": False,
        }

        with patch("app.app._append_self_service_log"), \
             patch("app.app._hdhive_open_api_resources", return_value={
                 "success": True,
                 "data": [
                     {
                         "slug": "ABCDEFGHIJKLMNOP",
                         "title": "Slow Horses S01-S完结季",
                         "season": 1,
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
        self.assertEqual(rid, "rid-season-open-ended")
        self.assertEqual(status, "success")
        self.assertIn("资源已入库", message)
        self.assertIn("来源: Open API 搜索", detail)


if __name__ == "__main__":
    unittest.main()
