import unittest
from unittest.mock import Mock, patch

from scripts.update_drama_calendar_env import (
    _collect_added_titles,
    _guard_moviepilot_tv_subscription_states,
    _resolve_tmdb_tv_identity_for_sync,
    _sync_moviepilot_subscriptions,
)


def _mock_json_response(status_code, payload=None, text=""):
    resp = Mock()
    resp.status_code = status_code
    if isinstance(payload, Exception):
        resp.json.side_effect = payload
    else:
        resp.json.return_value = payload
    resp.text = text
    return resp


class DramaCalendarMoviePilotSyncTestCase(unittest.TestCase):
    def test_collect_added_titles_returns_only_new_items(self):
        added = _collect_added_titles(
            ["剧A", "剧B", "剧B"],
            ["剧A", "剧C", "剧D", "剧C"],
        )
        self.assertEqual(added, ["剧C", "剧D"])

    def test_sync_skips_when_disabled(self):
        result = _sync_moviepilot_subscriptions(
            ["示例剧"],
            enabled=False,
            base_url="http://127.0.0.1:3000",
            api_token="token",
            timeout=10,
        )

        self.assertTrue(result.get("skipped"))
        self.assertEqual(result.get("message"), "未启用 moviepilot-sync")

    def test_sync_skips_when_missing_required_config(self):
        result = _sync_moviepilot_subscriptions(
            ["示例剧"],
            enabled=True,
            base_url="",
            api_token="",
            timeout=10,
        )
        self.assertTrue(result.get("skipped"))
        self.assertIn("moviepilot-url", str(result.get("message")))

    @patch("scripts.update_drama_calendar_env.requests.post")
    def test_sync_counts_added_existing_and_failed(self, mock_post):
        mock_post.side_effect = [
            _mock_json_response(200, {"success": True, "message": "新增订阅成功"}),
            _mock_json_response(200, {"success": True, "message": "订阅已存在"}),
            _mock_json_response(500, {"detail": "服务异常"}),
        ]

        result = _sync_moviepilot_subscriptions(
            ["剧A", "剧A", "剧B", "剧C"],
            enabled=True,
            base_url="127.0.0.1:3000",
            api_token="abc123",
            timeout=12,
        )

        self.assertFalse(result.get("skipped"))
        self.assertEqual(result.get("attempted"), 3)
        self.assertEqual(result.get("added"), 1)
        self.assertEqual(result.get("existing"), 1)
        self.assertEqual(result.get("failed"), 1)
        self.assertEqual(mock_post.call_count, 3)

        first_call = mock_post.call_args_list[0]
        self.assertEqual(first_call.kwargs.get("params"), {"token": "abc123"})
        self.assertEqual(first_call.kwargs.get("timeout"), 12)
        self.assertEqual(
            first_call.args[0],
            "http://127.0.0.1:3000/api/v1/subscribe/",
        )

    @patch("scripts.update_drama_calendar_env.requests.post")
    @patch("scripts.update_drama_calendar_env._tmdb_request_get")
    def test_sync_uses_tmdbid_when_tmdb_match(self, mock_tmdb_get, mock_post):
        mock_tmdb_get.return_value = _mock_json_response(
            200,
            {
                "results": [
                    {
                        "id": 95299,
                        "name": "白莲花度假村",
                        "original_name": "The White Lotus",
                        "first_air_date": "2021-07-11",
                        "origin_country": ["US"],
                        "popularity": 99,
                    }
                ]
            },
        )
        mock_post.return_value = _mock_json_response(200, {"success": True, "message": "新增订阅成功"})

        result = _sync_moviepilot_subscriptions(
            ["白莲花度假村"],
            enabled=True,
            base_url="127.0.0.1:3000",
            api_token="abc123",
            timeout=12,
            tmdb_api_key="tmdb-key",
            tmdb_language="zh-CN",
            tmdb_region="US",
            tmdb_timeout=8,
            tmdb_year_tolerance=2,
            tmdb_min_score=10,
        )

        self.assertFalse(result.get("skipped"))
        self.assertEqual(result.get("attempted"), 1)
        self.assertEqual(result.get("tmdb_enriched"), 1)
        self.assertEqual(result.get("failed"), 0)

        first_call = mock_post.call_args_list[0]
        payload = first_call.kwargs.get("json") or {}
        self.assertEqual(payload.get("tmdbid"), 95299)
        self.assertEqual(payload.get("name"), "白莲花度假村")
        self.assertEqual(payload.get("year"), "2021")

    @patch("scripts.update_drama_calendar_env.requests.post")
    @patch("scripts.update_drama_calendar_env._tmdb_request_get")
    def test_sync_fallbacks_when_tmdb_no_match(self, mock_tmdb_get, mock_post):
        mock_tmdb_get.return_value = _mock_json_response(200, {"results": []})
        mock_post.return_value = _mock_json_response(200, {"success": True, "message": "新增订阅成功"})

        result = _sync_moviepilot_subscriptions(
            ["未知剧名"],
            enabled=True,
            base_url="127.0.0.1:3000",
            api_token="abc123",
            timeout=12,
            tmdb_api_key="tmdb-key",
        )

        self.assertFalse(result.get("skipped"))
        self.assertEqual(result.get("attempted"), 1)
        self.assertEqual(result.get("tmdb_enriched"), 0)
        self.assertEqual(result.get("tmdb_miss"), 1)
        self.assertEqual(result.get("failed"), 0)

        first_call = mock_post.call_args_list[0]
        payload = first_call.kwargs.get("json") or {}
        self.assertEqual(payload.get("name"), "未知剧名")
        self.assertNotIn("tmdbid", payload)

    @patch("scripts.update_drama_calendar_env.requests.post")
    @patch("scripts.update_drama_calendar_env._tmdb_request_get")
    def test_sync_uses_movie_payload_when_tv_miss_but_movie_match(self, mock_tmdb_get, mock_post):
        mock_tmdb_get.side_effect = [
            _mock_json_response(200, {"results": []}),
            _mock_json_response(
                200,
                {
                    "results": [
                        {
                            "id": 1029575,
                            "title": "超级马力欧兄弟大电影",
                            "original_title": "The Super Mario Bros. Movie",
                            "release_date": "2023-04-05",
                            "origin_country": ["US"],
                            "popularity": 100,
                        }
                    ]
                },
            ),
        ]
        mock_post.return_value = _mock_json_response(200, {"success": True, "message": "新增订阅成功"})

        result = _sync_moviepilot_subscriptions(
            ["超级马力欧银河大电影"],
            enabled=True,
            base_url="127.0.0.1:3000",
            api_token="abc123",
            timeout=12,
            tmdb_api_key="tmdb-key",
            tmdb_region="US",
            tmdb_min_score=10,
        )

        self.assertFalse(result.get("skipped"))
        self.assertEqual(result.get("attempted"), 1)
        self.assertEqual(result.get("tmdb_enriched"), 1)
        self.assertEqual(result.get("failed"), 0)

        payload = mock_post.call_args_list[0].kwargs.get("json") or {}
        self.assertEqual(payload.get("type"), "电影")
        self.assertEqual(payload.get("tmdbid"), 1029575)
        self.assertEqual(payload.get("year"), "2023")

    @patch("scripts.update_drama_calendar_env._tmdb_request_get")
    def test_resolve_tmdb_tv_identity_returns_match_payload(self, mock_tmdb_get):
        mock_tmdb_get.return_value = _mock_json_response(
            200,
            {
                "results": [
                    {
                        "id": 131927,
                        "name": "人生切割术",
                        "original_name": "Severance",
                        "first_air_date": "2022-02-17",
                        "origin_country": ["US"],
                        "popularity": 130,
                    }
                ]
            },
        )

        payload, note = _resolve_tmdb_tv_identity_for_sync(
            "人生切割术",
            api_key="tmdb-key",
            language="zh-CN",
            region="US",
            timeout=8,
            year_tolerance=2,
            min_confidence_score=10,
        )

        self.assertIsInstance(payload, dict)
        self.assertEqual(payload.get("tmdbid"), 131927)
        self.assertEqual(payload.get("year"), "2022")
        self.assertIn("tmdbid=", note)

    def test_guard_skips_when_disabled(self):
        result = _guard_moviepilot_tv_subscription_states(
            ["示例剧"],
            enabled=False,
            base_url="http://127.0.0.1:3000",
            api_token="token",
            timeout=10,
        )

        self.assertTrue(result.get("skipped"))
        self.assertEqual(result.get("message"), "未启用 moviepilot-sync")

    @patch("scripts.update_drama_calendar_env.requests.put")
    @patch("scripts.update_drama_calendar_env.requests.get")
    def test_guard_sets_pending_and_running_by_total_episode(self, mock_get, mock_put):
        mock_get.return_value = _mock_json_response(
            200,
            {
                "data": [
                    {"id": 101, "name": "剧A", "type": "电视剧", "state": "R", "total_episode": 1},
                    {"id": 102, "name": "剧B", "type": "电视剧", "state": "P", "total_episode": 2},
                    {"id": 103, "name": "电影C", "type": "电影", "state": "R", "total_episode": 1},
                ]
            },
        )
        mock_put.side_effect = [
            _mock_json_response(200, {"success": True, "message": "ok"}),
            _mock_json_response(200, {"success": True, "message": "ok"}),
        ]

        result = _guard_moviepilot_tv_subscription_states(
            ["剧A", "剧B", "剧X"],
            enabled=True,
            base_url="127.0.0.1:3000",
            api_token="abc123",
            timeout=12,
            pending_total_episode_le=1,
            resume_total_episode_gt=1,
        )

        self.assertFalse(result.get("skipped"))
        self.assertEqual(result.get("attempted"), 2)
        self.assertEqual(result.get("set_pending"), 1)
        self.assertEqual(result.get("set_running"), 1)
        self.assertEqual(result.get("failed"), 0)
        self.assertEqual(mock_put.call_count, 2)

        first_put = mock_put.call_args_list[0]
        second_put = mock_put.call_args_list[1]
        self.assertEqual(first_put.kwargs.get("params"), {"token": "abc123", "state": "P"})
        self.assertEqual(second_put.kwargs.get("params"), {"token": "abc123", "state": "R"})


if __name__ == "__main__":
    unittest.main()
