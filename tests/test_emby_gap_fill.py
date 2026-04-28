import datetime
import unittest
from unittest.mock import patch
import threading

from app.app import (
    _extract_emby_missing_seasons,
    _extract_emby_missing_seasons_by_index_gap,
    _extract_emby_missing_seasons_from_season_items,
    _count_emby_present_episodes_by_season,
    _emby_fetch_episode_items,
    _normalize_emby_gap_fill_config,
    _emby_fetch_series_items,
    _prune_emby_gap_missing_items,
    _submit_emby_gap_item,
    _run_emby_gap_fill_once,
    _tmdb_expected_episode_count_for_season,
    _update_emby_gap_fill_state_from_result,
)


class EmbyGapFillTestCase(unittest.TestCase):
    def _build_base_config(self, max_missing_requests: int = 2, cooldown_hours: int = 24):
        return {
            "hdhive_auto_unlock_points_threshold": 0,
            "drama_calendar": {
                "tmdb_api_key": "",
                "emby_gap_fill": {
                    "enabled": True,
                    "base_url": "http://127.0.0.1:8096",
                    "api_key": "emby-key",
                    "user_id": "",
                    "library_ids": "",
                    "only_aired": True,
                    "max_series": 20,
                    "max_missing_requests": max_missing_requests,
                    "request_cooldown_hours": cooldown_hours,
                    "auto_sync_interval_minutes": 360,
                    "auto_sync_cron_expr": "",
                    "notify_user_ids": "",
                },
            },
        }

    def _runtime(self):
        return {
            "enabled": True,
            "targets": ["@emby_bot"],
            "notify_targets": ["@emby_bot"],
            "effective_targets": ["@emby_bot"],
            "max_results": 5,
            "hdhive_cookie": "cookie=a",
            "hdhive_api_key": "open-api-key",
            "use_open_api": True,
            "allow_open_api_direct": True,
            "storage_mode": "115",
            "base_url": "https://hdhive.com",
            "tmdb_api_key": "",
        }

    def test_extract_emby_missing_seasons_filters_future_and_non_missing_items(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        past = (now - datetime.timedelta(days=1)).isoformat().replace("+00:00", "Z")
        future = (now + datetime.timedelta(days=3)).isoformat().replace("+00:00", "Z")
        items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": past},
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": future},
            {"IsMissing": True, "ParentIndexNumber": 2},
            {"IsMissing": False, "ParentIndexNumber": 3, "LocationType": "FileSystem"},
        ]

        result = _extract_emby_missing_seasons(items, only_aired=True)
        self.assertEqual(result, {1: 1, 2: 1})

    def test_extract_emby_missing_seasons_ignores_entries_without_missing_markers(self):
        items = [
            {"IsMissing": None, "LocationType": None, "ParentIndexNumber": 1, "PremiereDate": "2025-03-03T00:00:00.0000000Z"},
            {"IsMissing": None, "LocationType": "", "ParentIndexNumber": 1, "PremiereDate": "2025-03-04T00:00:00.0000000Z"},
            {"ParentIndexNumber": 1, "PremiereDate": "2025-03-05T00:00:00.0000000Z"},
        ]
        result = _extract_emby_missing_seasons(items, only_aired=True)
        self.assertEqual(result, {})

    def test_extract_emby_missing_seasons_by_index_gap_detects_leading_missing_episode(self):
        items = [
            {"ParentIndexNumber": 1, "IndexNumber": 2, "Name": "第 2 集"},
            {"ParentIndexNumber": 1, "IndexNumber": 3, "Name": "第 3 集"},
        ]
        result = _extract_emby_missing_seasons_by_index_gap(items, only_aired=False)
        self.assertEqual(result, {1: 1})

    def test_extract_emby_missing_seasons_by_index_gap_infers_fully_missing_prior_seasons(self):
        items = [
            {"ParentIndexNumber": 8, "IndexNumber": 1, "Name": "第 1 集"},
            {"ParentIndexNumber": 8, "IndexNumber": 2, "Name": "第 2 集"},
        ]
        result = _extract_emby_missing_seasons_by_index_gap(items, only_aired=False)
        expected = {season: 1 for season in range(1, 8)}
        self.assertEqual(result, expected)

    def test_extract_emby_missing_seasons_by_index_gap_skips_large_leading_absolute_numbering(self):
        items = [
            {"ParentIndexNumber": 1, "IndexNumber": 892, "Name": "第 892 集"},
            {"ParentIndexNumber": 1, "IndexNumber": 893, "Name": "第 893 集"},
        ]
        result = _extract_emby_missing_seasons_by_index_gap(items, only_aired=False)
        self.assertEqual(result, {})

    def test_extract_emby_missing_seasons_by_index_gap_ignores_known_future_holes(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        past = (now - datetime.timedelta(days=1)).isoformat().replace("+00:00", "Z")
        future = (now + datetime.timedelta(days=3)).isoformat().replace("+00:00", "Z")
        items = [
            {"ParentIndexNumber": 1, "IndexNumber": 1, "PremiereDate": past},
            {"ParentIndexNumber": 1, "IndexNumber": 2, "PremiereDate": past},
            {"ParentIndexNumber": 1, "IndexNumber": 3, "PremiereDate": future},
            {"ParentIndexNumber": 1, "IndexNumber": 4, "PremiereDate": future},
            {"ParentIndexNumber": 1, "IndexNumber": 5, "PremiereDate": past},
        ]
        result = _extract_emby_missing_seasons_by_index_gap(items, only_aired=True)
        self.assertEqual(result, {})

    def test_extract_emby_missing_seasons_from_season_items_detects_virtual_or_empty_seasons(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        past = (now - datetime.timedelta(days=1)).isoformat().replace("+00:00", "Z")
        future = (now + datetime.timedelta(days=7)).isoformat().replace("+00:00", "Z")
        season_items = [
            {"IndexNumber": 1, "Name": "Season 1", "ChildCount": 10, "LocationType": "FileSystem", "IsMissing": False, "PremiereDate": past},
            {"IndexNumber": 2, "Name": "Season 2", "ChildCount": 0, "LocationType": "FileSystem", "IsMissing": False, "PremiereDate": past},
            {"IndexNumber": 3, "Name": "Season 3", "LocationType": "Virtual", "IsMissing": None, "PremiereDate": past},
            {"IndexNumber": 4, "Name": "Season 4", "ChildCount": 0, "LocationType": "Virtual", "IsMissing": True, "PremiereDate": future},
        ]
        result = _extract_emby_missing_seasons_from_season_items(season_items, only_aired=True)
        self.assertEqual(result, {2: 1, 3: 1})

    def test_count_emby_present_episodes_by_season_skips_missing_placeholders(self):
        items = [
            {"ParentIndexNumber": 1, "IndexNumber": 1, "IsMissing": False, "LocationType": "FileSystem"},
            {"ParentIndexNumber": 1, "IndexNumber": 2, "IsMissing": True, "LocationType": "Virtual"},
            {"ParentIndexNumber": 1, "IndexNumber": 3, "IsMissing": None, "LocationType": "missing"},
            {"ParentIndexNumber": 1, "IndexNumber": 4, "IsMissing": None, "LocationType": "FileSystem"},
        ]
        result = _count_emby_present_episodes_by_season(items, only_aired=False)
        self.assertEqual(result, {1: 2})

    def test_normalize_emby_gap_fill_config_blank_max_series_means_unlimited(self):
        normalized = _normalize_emby_gap_fill_config({"max_series": ""})
        self.assertEqual(normalized.get("max_series"), 0)

    def test_normalize_emby_gap_fill_config_series_workers_clamped(self):
        normalized_high = _normalize_emby_gap_fill_config({"series_workers": 99})
        normalized_low = _normalize_emby_gap_fill_config({"series_workers": 0})
        self.assertEqual(normalized_high.get("series_workers"), 12)
        self.assertEqual(normalized_low.get("series_workers"), 1)

    def test_normalize_emby_gap_fill_config_allows_zero_cooldown(self):
        normalized = _normalize_emby_gap_fill_config({"request_cooldown_hours": 0})
        self.assertEqual(normalized.get("request_cooldown_hours"), 0)

    def test_normalize_emby_gap_fill_config_keeps_ignore_list(self):
        normalized = _normalize_emby_gap_fill_config({"ignore_list": " series-1:S01 \n示例剧 "})
        self.assertEqual(normalized.get("ignore_list"), "series-1:S01 \n示例剧")

    def test_normalize_emby_gap_fill_config_tmdb_validate_defaults_true(self):
        normalized = _normalize_emby_gap_fill_config({})
        self.assertTrue(bool(normalized.get("tmdb_validate")))

    def test_normalize_emby_gap_fill_config_tmdb_validate_can_be_disabled(self):
        normalized = _normalize_emby_gap_fill_config({"tmdb_validate": "off"})
        self.assertFalse(bool(normalized.get("tmdb_validate")))

    def test_normalize_emby_gap_fill_config_strict_missing_markers_default_false(self):
        normalized = _normalize_emby_gap_fill_config({})
        self.assertFalse(bool(normalized.get("strict_missing_markers_only")))

    def test_normalize_emby_gap_fill_config_strict_missing_markers_can_be_enabled(self):
        normalized = _normalize_emby_gap_fill_config({"strict_missing_markers_only": "on"})
        self.assertTrue(bool(normalized.get("strict_missing_markers_only")))

    def test_tmdb_expected_episode_count_only_aired_uses_episode_air_dates(self):
        season_detail = {
            "air_date": "2026-01-01",
            "episode_count": 10,
            "episodes": [
                {"episode_number": 1, "air_date": "2026-01-01"},
                {"episode_number": 2, "air_date": "2099-01-01"},
                {"episode_number": 3, "air_date": None},
            ],
        }
        result = _tmdb_expected_episode_count_for_season(season_detail, only_aired=True)
        self.assertEqual(result, 1)

    def test_tmdb_expected_episode_count_only_aired_avoids_full_count_without_air_dates(self):
        season_detail = {
            "air_date": "2026-01-01",
            "episode_count": 12,
            "episodes": [
                {"episode_number": 1, "air_date": None},
                {"episode_number": 2, "air_date": ""},
            ],
        }
        result = _tmdb_expected_episode_count_for_season(season_detail, only_aired=True)
        self.assertIsNone(result)

    def test_emby_fetch_series_items_unlimited_uses_pagination(self):
        calls = []

        def _fake_emby_request_json(_base_url, _api_key, _path, params=None, timeout=25):
            params = params or {}
            start_index = int(params.get("StartIndex", "0") or 0)
            calls.append(start_index)
            if start_index == 0:
                return True, {
                    "Items": [
                        {"Id": "s1", "Name": "A"},
                        {"Id": "s2", "Name": "B"},
                    ],
                    "TotalRecordCount": 3,
                }, ""
            if start_index == 2:
                return True, {
                    "Items": [
                        {"Id": "s3", "Name": "C"},
                    ],
                    "TotalRecordCount": 3,
                }, ""
            return True, {"Items": [], "TotalRecordCount": 3}, ""

        with patch("app.app._emby_request_json", side_effect=_fake_emby_request_json):
            items, err = _emby_fetch_series_items(
                base_url="http://127.0.0.1:8096",
                api_key="emby-key",
                user_id="user-1",
                library_ids=[],
                max_series=0,
            )

        self.assertEqual(err, "")
        self.assertEqual(len(items), 3)
        self.assertEqual(calls[:2], [0, 2])

    def test_emby_fetch_series_items_stops_when_pages_repeat(self):
        calls = []
        repeated_items = [{"Id": f"s{i}", "Name": f"Series{i}"} for i in range(1, 201)]

        def _fake_emby_request_json(_base_url, _api_key, _path, params=None, timeout=25):
            params = params or {}
            start_index = int(params.get("StartIndex", "0") or 0)
            calls.append(start_index)
            return True, {
                "Items": repeated_items,
            }, ""

        with patch("app.app._emby_request_json", side_effect=_fake_emby_request_json):
            items, err = _emby_fetch_series_items(
                base_url="http://127.0.0.1:8096",
                api_key="emby-key",
                user_id="user-1",
                library_ids=[],
                max_series=0,
            )

        self.assertEqual(err, "")
        self.assertEqual(len(items), 200)
        self.assertEqual(calls, [0, 200, 400])

    def test_emby_fetch_episode_items_paginates_when_more_than_one_thousand(self):
        calls = []

        def _fake_emby_request_json(_base_url, _api_key, _path, params=None, timeout=25):
            params = params or {}
            start_index = int(params.get("StartIndex", "0") or 0)
            calls.append(start_index)
            if start_index == 0:
                return True, {
                    "Items": [{"Id": f"e{i}", "Name": f"E{i}", "ParentIndexNumber": 1, "IndexNumber": i} for i in range(1, 1001)],
                    "TotalRecordCount": 1050,
                }, ""
            if start_index == 1000:
                return True, {
                    "Items": [{"Id": f"e{i}", "Name": f"E{i}", "ParentIndexNumber": 1, "IndexNumber": i} for i in range(1001, 1051)],
                    "TotalRecordCount": 1050,
                }, ""
            return True, {"Items": [], "TotalRecordCount": 1050}, ""

        with patch("app.app._emby_request_json", side_effect=_fake_emby_request_json):
            items, err = _emby_fetch_episode_items(
                base_url="http://127.0.0.1:8096",
                api_key="emby-key",
                user_id="user-1",
                series_id="series-1",
                is_missing=None,
            )

        self.assertEqual(err, "")
        self.assertEqual(len(items), 1050)
        self.assertEqual(calls[:2], [0, 1000])

    def test_run_emby_gap_fill_scheduler_submits_all_candidates(self):
        config = self._build_base_config(max_missing_requests=1)
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-02T00:00:00Z"},
            {"IsMissing": True, "ParentIndexNumber": 2, "PremiereDate": "2026-01-03T00:00:00Z"},
        ]

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("submitted"), 2)
        self.assertEqual(run_request.call_count, 2)
        submitted_seasons = [call.args[0].get("season") for call in run_request.call_args_list]
        self.assertEqual(submitted_seasons, ["S01", "S02"])

    def test_run_emby_gap_fill_manual_submits_all_candidates(self):
        config = self._build_base_config(max_missing_requests=1)
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
            {"IsMissing": True, "ParentIndexNumber": 2, "PremiereDate": "2026-01-03T00:00:00Z"},
        ]

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="manual")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("submitted"), 2)
        self.assertEqual(run_request.call_count, 2)

    def test_run_emby_gap_fill_apply_publishes_missing_list_during_running(self):
        config = self._build_base_config(max_missing_requests=1)
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
            {"IsMissing": True, "ParentIndexNumber": 2, "PremiereDate": "2026-01-02T00:00:00Z"},
        ]

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._set_emby_gap_fill_scheduler_state") as set_state, \
             patch("app.app._run_self_service_request"):
            result = _run_emby_gap_fill_once(config, trigger="manual")

        self.assertEqual(result.get("status"), "success")
        list_publish_calls = [
            call.kwargs for call in set_state.call_args_list
            if isinstance(call.kwargs.get("last_missing_items"), list)
        ]
        self.assertTrue(list_publish_calls)
        latest_list = list_publish_calls[-1]["last_missing_items"]
        self.assertEqual(len(latest_list), 2)
        self.assertEqual(latest_list[0].get("season_label"), "S01")
        self.assertEqual(latest_list[1].get("season_label"), "S02")

    def test_run_emby_gap_fill_scheduler_falls_back_to_season_items_when_missing_episode_list_empty(self):
        config = self._build_base_config(max_missing_requests=1)
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        season_items = [
            {"IndexNumber": 1, "Name": "Season 1", "ChildCount": 12, "LocationType": "FileSystem", "IsMissing": False},
            {"IndexNumber": 2, "Name": "Season 2", "ChildCount": 0, "LocationType": "Virtual", "IsMissing": True},
            {"IndexNumber": 3, "Name": "Season 3", "ChildCount": 0, "LocationType": "Virtual", "IsMissing": True},
        ]

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=([], "")), \
             patch("app.app._emby_fetch_season_items", return_value=(season_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 2)
        self.assertEqual(result.get("summary", {}).get("submitted"), 2)
        submitted_seasons = [call.args[0].get("season") for call in run_request.call_args_list]
        self.assertEqual(submitted_seasons, ["S02", "S03"])

    def test_run_emby_gap_fill_strict_mode_skips_season_level_inference(self):
        config = self._build_base_config(max_missing_requests=1)
        config["drama_calendar"]["emby_gap_fill"]["strict_missing_markers_only"] = True
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        season_items = [
            {"IndexNumber": 2, "Name": "Season 2", "ChildCount": 0, "LocationType": "Virtual", "IsMissing": True},
        ]

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=([], "")), \
             patch("app.app._emby_fetch_season_items", return_value=(season_items, "")) as fetch_seasons, \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 0)
        self.assertEqual(result.get("summary", {}).get("submitted"), 0)
        fetch_seasons.assert_not_called()
        run_request.assert_not_called()

    def test_run_emby_gap_fill_strict_mode_skips_index_gap_inference(self):
        config = self._build_base_config(max_missing_requests=1)
        config["drama_calendar"]["emby_gap_fill"]["strict_missing_markers_only"] = True
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items_without_markers = [
            {"Name": "第 1 集", "ParentIndexNumber": 1, "IndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z", "IsMissing": None, "LocationType": None},
            {"Name": "第 3 集", "ParentIndexNumber": 1, "IndexNumber": 3, "PremiereDate": "2026-01-02T00:00:00Z", "IsMissing": None, "LocationType": None},
        ]

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items_without_markers, "")), \
             patch("app.app._emby_fetch_episode_items") as fetch_all_episodes, \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 0)
        self.assertEqual(result.get("summary", {}).get("submitted"), 0)
        fetch_all_episodes.assert_not_called()
        run_request.assert_not_called()

    def test_run_emby_gap_fill_scheduler_tmdb_conflict_skips_submit(self):
        config = self._build_base_config(max_missing_requests=1)
        config["drama_calendar"]["tmdb_api_key"] = "tmdb-key"
        series_items = [
            {
                "Id": "series-1",
                "Name": "示例剧",
                "ProductionYear": 2026,
                "ProviderIds": {"Tmdb": "12345"},
            }
        ]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "IndexNumber": 2, "PremiereDate": "2026-01-01T00:00:00Z"},
        ]
        present_items = [
            {"ParentIndexNumber": 1, "IndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z", "IsMissing": False, "LocationType": "FileSystem"},
            {"ParentIndexNumber": 1, "IndexNumber": 2, "PremiereDate": "2026-01-02T00:00:00Z", "IsMissing": False, "LocationType": "FileSystem"},
        ]
        season_detail = {
            "episodes": [
                {"episode_number": 1, "air_date": "2025-01-01"},
                {"episode_number": 2, "air_date": "2025-01-02"},
            ]
        }

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._emby_fetch_episode_items", return_value=(present_items, "")), \
             patch("app.app._tmdb_fetch_tv_detail", return_value={"number_of_seasons": 1}), \
             patch("app.app._tmdb_fetch_tv_season_detail", return_value=season_detail), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "warning")
        self.assertEqual(result.get("summary", {}).get("missing"), 0)
        self.assertEqual(result.get("summary", {}).get("submitted"), 0)
        self.assertEqual(result.get("summary", {}).get("tmdb_checked_series"), 1)
        self.assertEqual(result.get("summary", {}).get("tmdb_conflicts"), 1)
        run_request.assert_not_called()

    def test_run_emby_gap_fill_scheduler_tmdb_confirmed_missing_submits(self):
        config = self._build_base_config(max_missing_requests=1)
        config["drama_calendar"]["tmdb_api_key"] = "tmdb-key"
        series_items = [
            {
                "Id": "series-1",
                "Name": "示例剧",
                "ProductionYear": 2026,
                "ProviderIds": {"Tmdb": "12345"},
            }
        ]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "IndexNumber": 2, "PremiereDate": "2026-01-01T00:00:00Z"},
        ]
        present_items = [
            {"ParentIndexNumber": 1, "IndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z", "IsMissing": False, "LocationType": "FileSystem"},
        ]
        season_detail = {
            "episodes": [
                {"episode_number": 1, "air_date": "2025-01-01"},
                {"episode_number": 2, "air_date": "2025-01-02"},
            ]
        }

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._emby_fetch_episode_items", return_value=(present_items, "")), \
             patch("app.app._tmdb_fetch_tv_detail", return_value={"number_of_seasons": 1}), \
             patch("app.app._tmdb_fetch_tv_season_detail", return_value=season_detail), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 1)
        self.assertEqual(result.get("summary", {}).get("submitted"), 1)
        self.assertEqual(result.get("summary", {}).get("tmdb_checked_series"), 1)
        self.assertEqual(result.get("summary", {}).get("tmdb_conflicts"), 0)
        run_request.assert_called_once()

    def test_run_emby_gap_fill_scheduler_tmdb_only_aired_caps_unreleased_missing_count(self):
        config = self._build_base_config(max_missing_requests=1)
        config["drama_calendar"]["tmdb_api_key"] = "tmdb-key"
        series_items = [
            {
                "Id": "series-1",
                "Name": "示例剧",
                "ProductionYear": 2026,
                "ProviderIds": {"Tmdb": "12345"},
            }
        ]
        # Emby may return placeholder missing entries without reliable premiere dates,
        # including unaired episodes.
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "IndexNumber": 10},
            {"IsMissing": True, "ParentIndexNumber": 1, "IndexNumber": 11},
            {"IsMissing": True, "ParentIndexNumber": 1, "IndexNumber": 12},
            {"IsMissing": True, "ParentIndexNumber": 1, "IndexNumber": 13},
        ]
        present_items = [
            {"ParentIndexNumber": 1, "IndexNumber": ep, "PremiereDate": "2025-01-01T00:00:00Z", "IsMissing": False, "LocationType": "FileSystem"}
            for ep in range(1, 10)
        ]
        season_detail = {
            "episodes": [
                {"episode_number": ep, "air_date": "2025-01-01"} for ep in range(1, 11)
            ] + [
                {"episode_number": ep, "air_date": "2099-01-01"} for ep in range(11, 14)
            ]
        }

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._emby_fetch_episode_items", return_value=(present_items, "")), \
             patch("app.app._tmdb_fetch_tv_detail", return_value={"number_of_seasons": 1}), \
             patch("app.app._tmdb_fetch_tv_season_detail", return_value=season_detail), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 1)
        self.assertEqual(result.get("summary", {}).get("submitted"), 1)
        missing_items_out = result.get("missing_items") or []
        self.assertEqual(len(missing_items_out), 1)
        self.assertEqual(int(missing_items_out[0].get("missing_episodes") or 0), 1)
        submitted_payload = run_request.call_args[0][0]
        self.assertEqual(int(submitted_payload.get("missing_episodes") or 0), 1)

    def test_run_emby_gap_fill_scheduler_tmdb_present_compare_uses_unfiltered_episode_list(self):
        config = self._build_base_config(max_missing_requests=1)
        config["drama_calendar"]["tmdb_api_key"] = "tmdb-key"
        series_items = [
            {
                "Id": "series-1",
                "Name": "示例剧",
                "ProductionYear": 2026,
                "ProviderIds": {"Tmdb": "12345"},
            }
        ]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "IndexNumber": 2, "PremiereDate": "2026-01-01T00:00:00Z"},
        ]
        season_detail = {
            "episodes": [
                {"episode_number": 1, "air_date": "2025-01-01"},
                {"episode_number": 2, "air_date": "2025-01-02"},
            ]
        }
        present_calls = []

        def _fake_fetch_episode_items(base_url, api_key, user_id, series_id, *, is_missing=None):
            present_calls.append(is_missing)
            items = [
                {"ParentIndexNumber": 1, "IndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z", "IsMissing": False, "LocationType": "FileSystem"},
            ]
            return items, ""

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._emby_fetch_episode_items", side_effect=_fake_fetch_episode_items), \
             patch("app.app._tmdb_fetch_tv_detail", return_value={"number_of_seasons": 1}), \
             patch("app.app._tmdb_fetch_tv_season_detail", return_value=season_detail), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertIn(None, present_calls)
        run_request.assert_called_once()

    def test_run_emby_gap_fill_scheduler_tmdb_infers_missing_full_seasons_even_when_emby_reports_none(self):
        config = self._build_base_config(max_missing_requests=1)
        config["drama_calendar"]["tmdb_api_key"] = "tmdb-key"
        series_items = [
            {
                "Id": "series-1",
                "Name": "投行风云",
                "ProductionYear": 2026,
                "ProviderIds": {"Tmdb": "12345"},
            }
        ]
        missing_items = []
        season_items = [
            {"IndexNumber": 1, "Name": "Season 1", "ChildCount": 8, "LocationType": "FileSystem", "IsMissing": False},
        ]
        present_items = [
            {"ParentIndexNumber": 1, "IndexNumber": 1, "PremiereDate": "2025-01-01T00:00:00Z", "IsMissing": False, "LocationType": "FileSystem"},
        ]
        season_detail_s1 = {"episodes": [{"episode_number": 1, "air_date": "2025-01-01"}]}
        season_detail_s2 = {"episodes": [{"episode_number": 1, "air_date": "2025-01-01"}]}
        season_detail_s3 = {"episodes": [{"episode_number": 1, "air_date": "2025-01-01"}]}

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._emby_fetch_season_items", return_value=(season_items, "")), \
             patch("app.app._emby_fetch_episode_items", return_value=(present_items, "")), \
             patch("app.app._tmdb_fetch_tv_detail", return_value={"number_of_seasons": 3, "seasons": [{"season_number": 1}, {"season_number": 2}, {"season_number": 3}]}), \
             patch("app.app._tmdb_fetch_tv_season_detail", side_effect=[season_detail_s1, season_detail_s2, season_detail_s3]), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 2)
        self.assertEqual(result.get("summary", {}).get("submitted"), 2)
        submitted_seasons = [call.args[0].get("season") for call in run_request.call_args_list]
        self.assertEqual(submitted_seasons, ["S02", "S03"])

    def test_run_emby_gap_fill_scheduler_skips_ignored_season(self):
        config = self._build_base_config(max_missing_requests=2)
        config["drama_calendar"]["emby_gap_fill"]["ignore_list"] = "series-1:S01"
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
            {"IsMissing": True, "ParentIndexNumber": 2, "PremiereDate": "2026-01-02T00:00:00Z"},
        ]

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("ignored"), 1)
        self.assertEqual(result.get("summary", {}).get("submitted"), 1)
        self.assertEqual(run_request.call_count, 1)
        payload = run_request.call_args[0][0]
        self.assertEqual(payload.get("season"), "S02")

    def test_run_emby_gap_fill_scheduler_skips_ignored_series(self):
        config = self._build_base_config(max_missing_requests=2)
        config["drama_calendar"]["emby_gap_fill"]["ignore_list"] = "示例剧"
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
        ]

        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 0)
        self.assertEqual(result.get("summary", {}).get("submitted"), 0)
        run_request.assert_not_called()

    def test_run_emby_gap_fill_respects_cooldown(self):
        config = self._build_base_config(max_missing_requests=2, cooldown_hours=24)
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
        ]
        state = {
            "requested": {
                "series-1:S01": 99_000,
            },
            "last_run": {},
        }

        with patch("app.app.time.time", return_value=100_000), \
             patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value=state), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("submitted"), 0)
        self.assertEqual(result.get("summary", {}).get("cooldown_skipped"), 1)
        run_request.assert_not_called()

    def test_run_emby_gap_fill_cooldown_zero_submits_recent_same_season(self):
        config = self._build_base_config(max_missing_requests=2, cooldown_hours=0)
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
        ]
        state = {
            "requested": {
                "series-1:S01": 99_900,
            },
            "last_run": {},
        }

        with patch("app.app.time.time", return_value=100_000), \
             patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value=state), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._enqueue_message"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="scheduler")

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("submitted"), 1)
        self.assertEqual(result.get("summary", {}).get("cooldown_skipped"), 0)
        run_request.assert_called_once()

    def test_run_emby_gap_fill_preview_lists_missing_without_submit(self):
        config = self._build_base_config(max_missing_requests=1, cooldown_hours=24)
        config["drama_calendar"]["emby_gap_fill"]["enabled"] = False
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
            {"IsMissing": True, "ParentIndexNumber": 2, "PremiereDate": "2026-01-02T00:00:00Z"},
        ]
        state = {
            "requested": {
                "series-1:S02": 99_000,
            },
            "last_run": {},
        }

        with patch("app.app.time.time", return_value=100_000), \
             patch("app.app._get_self_service_runtime", return_value={"enabled": False}), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value=state), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="manual_preview", preview_only=True)

        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 2)
        self.assertEqual(result.get("summary", {}).get("submitted"), 0)
        self.assertEqual(result.get("summary", {}).get("pending"), 1)
        self.assertEqual(result.get("summary", {}).get("cooldown_skipped"), 1)
        run_request.assert_not_called()

        missing_list = result.get("missing_items", [])
        self.assertEqual(len(missing_list), 2)
        season_map = {item.get("season_label"): item for item in missing_list}
        self.assertIn("S01", season_map)
        self.assertIn("S02", season_map)
        self.assertFalse(bool(season_map["S01"].get("cooldown_active")))
        self.assertTrue(bool(season_map["S02"].get("cooldown_active")))

    def test_run_emby_gap_fill_preview_fallbacks_to_index_gap_when_missing_marker_unavailable(self):
        config = self._build_base_config(max_missing_requests=1, cooldown_hours=24)
        config["drama_calendar"]["emby_gap_fill"]["enabled"] = False
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items_without_markers = [
            {
                "Name": "第 1 集",
                "ParentIndexNumber": 1,
                "IndexNumber": 1,
                "PremiereDate": "2026-01-01T00:00:00Z",
                "IsMissing": None,
                "LocationType": None,
            },
            {
                "Name": "第 3 集",
                "ParentIndexNumber": 1,
                "IndexNumber": 3,
                "PremiereDate": "2026-01-02T00:00:00Z",
                "IsMissing": None,
                "LocationType": None,
            },
        ]

        with patch("app.app._get_self_service_runtime", return_value={"enabled": False}), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items_without_markers, "")), \
             patch("app.app._emby_fetch_episode_items", return_value=(missing_items_without_markers, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="manual_preview", preview_only=True)

        self.assertEqual(result.get("status"), "warning")
        self.assertEqual(result.get("summary", {}).get("missing"), 1)
        self.assertEqual(result.get("summary", {}).get("pending"), 1)
        self.assertEqual(result.get("summary", {}).get("inferred_missing"), 1)
        self.assertTrue(bool(result.get("missing_filter_ignored")))
        self.assertIn("按集号断档推断", result.get("message", ""))
        run_request.assert_not_called()

    def test_run_emby_gap_fill_preview_filter_probe_is_per_series(self):
        config = self._build_base_config(max_missing_requests=1, cooldown_hours=24)
        config["drama_calendar"]["emby_gap_fill"]["enabled"] = False
        series_items = [
            {"Id": "series-1", "Name": "第一部剧", "ProductionYear": 2026},
            {"Id": "series-2", "Name": "第二部剧", "ProductionYear": 2026},
        ]
        missing_no_marker_full = [
            {
                "Id": "s1e1",
                "Name": "第 1 集",
                "ParentIndexNumber": 1,
                "IndexNumber": 1,
                "PremiereDate": "2026-01-01T00:00:00Z",
                "IsMissing": None,
                "LocationType": None,
            },
            {
                "Id": "s1e2",
                "Name": "第 2 集",
                "ParentIndexNumber": 1,
                "IndexNumber": 2,
                "PremiereDate": "2026-01-02T00:00:00Z",
                "IsMissing": None,
                "LocationType": None,
            },
        ]
        missing_no_marker_gap = [
            {
                "Id": "s2e1",
                "Name": "第 1 集",
                "ParentIndexNumber": 1,
                "IndexNumber": 1,
                "PremiereDate": "2026-01-01T00:00:00Z",
                "IsMissing": None,
                "LocationType": None,
            },
            {
                "Id": "s2e3",
                "Name": "第 3 集",
                "ParentIndexNumber": 1,
                "IndexNumber": 3,
                "PremiereDate": "2026-01-02T00:00:00Z",
                "IsMissing": None,
                "LocationType": None,
            },
        ]
        present_partial = [dict(missing_no_marker_full[0])]

        def _fake_missing(_base_url, _api_key, _user_id, sid):
            if sid == "series-1":
                return missing_no_marker_full, ""
            return missing_no_marker_gap, ""

        def _fake_fetch_episode(*args, **kwargs):
            sid = str(kwargs.get("series_id") or "")
            if not sid and len(args) >= 4:
                sid = str(args[3] or "")
            is_missing = kwargs.get("is_missing", None)
            if is_missing is False and sid == "series-1":
                return present_partial, ""
            if is_missing is False and sid == "series-2":
                return missing_no_marker_gap, ""
            if sid == "series-1":
                return missing_no_marker_full, ""
            return missing_no_marker_gap, ""

        with patch("app.app._get_self_service_runtime", return_value={"enabled": False}), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", side_effect=_fake_missing), \
             patch("app.app._emby_fetch_episode_items", side_effect=_fake_fetch_episode), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data"), \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="manual_preview", preview_only=True)

        self.assertEqual(result.get("status"), "warning")
        self.assertEqual(result.get("summary", {}).get("missing"), 1)
        self.assertEqual(result.get("summary", {}).get("pending"), 1)
        self.assertEqual(result.get("summary", {}).get("inferred_missing"), 1)
        self.assertTrue(bool(result.get("missing_filter_ignored")))
        self.assertEqual(int(result.get("missing_filter_ignored_series") or 0), 1)
        self.assertIn("按集号断档推断", result.get("message", ""))
        missing_list = result.get("missing_items", [])
        self.assertEqual(len(missing_list), 1)
        self.assertEqual(missing_list[0].get("title"), "第二部剧")
        run_request.assert_not_called()

    def test_run_emby_gap_fill_preview_still_runs_when_lock_busy(self):
        config = self._build_base_config(max_missing_requests=1, cooldown_hours=24)
        config["drama_calendar"]["emby_gap_fill"]["enabled"] = False
        series_items = [{"Id": "series-1", "Name": "示例剧", "ProductionYear": 2026}]
        missing_items = [
            {"IsMissing": True, "ParentIndexNumber": 1, "PremiereDate": "2026-01-01T00:00:00Z"},
        ]
        lock = threading.Lock()
        self.assertTrue(lock.acquire(blocking=False))

        with patch("app.app._EMBY_GAP_FILL_RUN_LOCK", lock), \
             patch("app.app._get_self_service_runtime", return_value={"enabled": False}), \
             patch("app.app._emby_resolve_user_id", return_value=("user-1", "")), \
             patch("app.app._emby_fetch_series_items", return_value=(series_items, "")), \
             patch("app.app._emby_fetch_missing_episode_items", return_value=(missing_items, "")), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data") as save_state, \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _run_emby_gap_fill_once(config, trigger="manual_preview", preview_only=True)

        lock.release()
        self.assertEqual(result.get("status"), "success")
        self.assertEqual(result.get("summary", {}).get("missing"), 1)
        run_request.assert_not_called()
        save_state.assert_not_called()

    def test_submit_emby_gap_item_success(self):
        config = self._build_base_config(max_missing_requests=1, cooldown_hours=24)
        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data") as save_state, \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _submit_emby_gap_item(
                config,
                series_id="series-1",
                title="示例剧",
                season=1,
                missing_episodes=2,
                year="2026",
            )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(result.get("status"), "success")
        submitted_item = result.get("submitted_item") if isinstance(result.get("submitted_item"), dict) else {}
        self.assertEqual(submitted_item.get("series_id"), "series-1")
        self.assertEqual(submitted_item.get("season"), 1)
        run_request.assert_called_once()
        save_state.assert_called_once()

    def test_prune_emby_gap_missing_items_filters_submitted(self):
        missing_items = [
            {"series_id": "series-1", "season": 1, "season_label": "S01", "title": "示例剧 A"},
            {"series_id": "series-2", "season": 2, "season_label": "S02", "title": "示例剧 B"},
        ]
        submitted_items = [
            {"series_id": "series-1", "season": 1, "season_label": "S01", "title": "示例剧 A"},
        ]

        pruned, removed = _prune_emby_gap_missing_items(missing_items, submitted_items)
        self.assertEqual(removed, 1)
        self.assertEqual(len(pruned), 1)
        self.assertEqual(pruned[0].get("series_id"), "series-2")

    def test_update_emby_gap_fill_state_from_result_prunes_submitted_missing_items(self):
        result = {
            "status": "success",
            "message": "done",
            "summary": {"scanned": 2, "missing": 2, "submitted": 1, "pending": 1},
            "missing_items": [
                {"series_id": "series-1", "season": 1, "season_label": "S01", "title": "示例剧 A"},
                {"series_id": "series-2", "season": 1, "season_label": "S01", "title": "示例剧 B"},
            ],
            "submitted_items": [
                {"series_id": "series-1", "season": 1, "season_label": "S01", "title": "示例剧 A"},
            ],
        }
        with patch("app.app.get_emby_gap_fill_scheduler_state", return_value={"schedule_mode": "interval", "next_run_at": "未启用", "enabled": True}), \
             patch("app.app._set_emby_gap_fill_scheduler_state") as set_state:
            _update_emby_gap_fill_state_from_result(result)

        kwargs = set_state.call_args.kwargs
        last_missing_items = kwargs.get("last_missing_items") if isinstance(kwargs.get("last_missing_items"), list) else []
        self.assertEqual(len(last_missing_items), 1)
        self.assertEqual(last_missing_items[0].get("series_id"), "series-2")

    def test_update_emby_gap_fill_state_from_result_keeps_previous_list_on_suspicious_empty_scan(self):
        previous_missing_items = [
            {"series_id": "series-1", "season": 1, "season_label": "S01", "title": "示例剧 A"},
        ]
        result = {
            "status": "success",
            "message": "done",
            "summary": {
                "scanned": 488,
                "missing": 0,
                "submitted": 0,
                "pending": 0,
                "ignored": 0,
                "inferred_missing": 0,
                "series_with_missing": 0,
                "cooldown_skipped": 0,
                "api_errors": 0,
                "submit_errors": 0,
                "tmdb_checked_series": 0,
                "tmdb_conflicts": 0,
            },
            "missing_items": [],
            "submitted_items": [],
            "strict_missing_markers_only": False,
            "tmdb_validation_enabled": True,
        }
        with patch(
            "app.app.get_emby_gap_fill_scheduler_state",
            return_value={
                "schedule_mode": "interval",
                "next_run_at": "未启用",
                "enabled": True,
                "last_missing_items": previous_missing_items,
            },
        ), patch("app.app._set_emby_gap_fill_scheduler_state") as set_state, patch("app.app._append_emby_gap_fill_log"):
            status, message = _update_emby_gap_fill_state_from_result(result)

        kwargs = set_state.call_args.kwargs
        last_missing_items = kwargs.get("last_missing_items") if isinstance(kwargs.get("last_missing_items"), list) else []
        self.assertEqual(len(last_missing_items), 1)
        self.assertEqual(last_missing_items[0].get("series_id"), "series-1")
        self.assertEqual(status, "warning")
        self.assertEqual(kwargs.get("last_status"), "warning")
        self.assertIn("已保留上次缺失列表", message)
        self.assertIn("已保留上次缺失列表", str(kwargs.get("last_message") or ""))

    def test_submit_emby_gap_item_respects_cooldown(self):
        config = self._build_base_config(max_missing_requests=1, cooldown_hours=24)
        state = {"requested": {"series-1:S01": 99_500}, "last_run": {}}
        with patch("app.app.time.time", return_value=100_000), \
             patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._load_emby_gap_fill_state_data", return_value=state), \
             patch("app.app._save_emby_gap_fill_state_data") as save_state, \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _submit_emby_gap_item(
                config,
                series_id="series-1",
                title="示例剧",
                season=1,
                missing_episodes=2,
                year="2026",
            )

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(result.get("status"), "warning")
        self.assertIn("冷却中", result.get("message", ""))
        run_request.assert_not_called()
        save_state.assert_not_called()

    def test_submit_emby_gap_item_respects_ignore_list(self):
        config = self._build_base_config(max_missing_requests=1, cooldown_hours=24)
        config["drama_calendar"]["emby_gap_fill"]["ignore_list"] = "series-1:S01"
        with patch("app.app._get_self_service_runtime", return_value=self._runtime()), \
             patch("app.app._load_emby_gap_fill_state_data", return_value={"requested": {}, "last_run": {}}), \
             patch("app.app._save_emby_gap_fill_state_data") as save_state, \
             patch("app.app._append_emby_gap_fill_log"), \
             patch("app.app._set_self_service_result"), \
             patch("app.app._run_self_service_request") as run_request:
            result = _submit_emby_gap_item(
                config,
                series_id="series-1",
                title="示例剧",
                season=1,
                missing_episodes=2,
                year="2026",
            )

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(result.get("status"), "warning")
        self.assertIn("忽略名单", result.get("message", ""))
        run_request.assert_not_called()
        save_state.assert_not_called()


if __name__ == "__main__":
    unittest.main()
