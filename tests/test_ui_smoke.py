import unittest
import time
from unittest.mock import patch

from app.app import app


class UISmokeTestCase(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def _login(self):
        with self.client.session_transaction() as sess:
            sess["logged_in"] = True

    def test_public_pages_render(self):
        checks = [
            ("/login", 200, "Telegram 登录 / 认证"),
            ("/web_login", 200, "Web 管理登录"),
            ("/self_service_public", 200, "自助观影公共提交"),
        ]

        for path, expected_status, marker in checks:
            with self.subTest(path=path):
                response = self.client.get(path, follow_redirects=False)
                body = response.get_data(as_text=True)
                self.assertEqual(response.status_code, expected_status)
                self.assertIn(marker, body)

    def test_public_page_has_emby_login_fields(self):
        response = self.client.get("/self_service_public", follow_redirects=False)
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('name="emby_username"', body)
        self.assertIn('name="emby_password"', body)
        self.assertIn("Emby 账户", body)
        self.assertIn("Emby 登录", body)

    def test_self_service_result_modal_present(self):
        self._login()
        private_resp = self.client.get("/self_service", follow_redirects=False)
        private_body = private_resp.get_data(as_text=True)
        self.assertEqual(private_resp.status_code, 200)
        self.assertIn('id="selfServiceResultModal"', private_body)
        self.assertIn('id="selfServiceOpenResultBtn"', private_body)

        public_resp = self.client.get("/self_service_public", follow_redirects=False)
        public_body = public_resp.get_data(as_text=True)
        self.assertEqual(public_resp.status_code, 200)
        self.assertIn('id="selfServiceResultModal"', public_body)

    def test_public_submit_requires_emby_login(self):
        cfg = {
            "self_service_public_enabled": True,
            "self_service_public_access_key": "",
            "self_service_public_rate_limit": {"enabled": False, "window_seconds": 60, "max_requests": 5},
        }
        runtime = {
            "enabled": True,
            "targets": ["@admin"],
            "notify_targets": ["@admin"],
            "effective_targets": ["@admin"],
            "max_results": 5,
            "hdhive_cookie": "",
            "hdhive_api_key": "api-key",
            "use_open_api": True,
            "allow_open_api_direct": False,
            "storage_mode": "any",
            "base_url": "https://hdhive.com",
            "tmdb_api_key": "",
        }
        with patch("app.app.load_config", return_value=cfg), \
             patch("app.app._get_self_service_runtime", return_value=runtime), \
             patch("app.app._check_public_rate_limit", return_value=(True, 0)), \
             patch("app.app._submit_self_service_request") as submit_mock:
            response = self.client.post(
                "/self_service_public",
                data={"title": "示例片名", "type": "电影"},
                follow_redirects=True,
            )
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("请先使用 Emby 账号登录", body)
        submit_mock.assert_not_called()

    def test_public_submit_after_emby_login_calls_submit(self):
        cfg = {
            "self_service_public_enabled": True,
            "self_service_public_access_key": "",
            "self_service_public_rate_limit": {"enabled": False, "window_seconds": 60, "max_requests": 5},
            "drama_calendar": {
                "emby_gap_fill": {
                    "enabled": True,
                    "base_url": "http://127.0.0.1:8096",
                    "api_key": "emby-key",
                }
            },
        }
        runtime = {
            "enabled": True,
            "targets": ["@admin"],
            "notify_targets": ["@admin"],
            "effective_targets": ["@admin"],
            "max_results": 5,
            "hdhive_cookie": "",
            "hdhive_api_key": "api-key",
            "use_open_api": True,
            "allow_open_api_direct": False,
            "storage_mode": "any",
            "base_url": "https://hdhive.com",
            "tmdb_api_key": "",
        }
        binding = {"account": "tester", "name": "tester", "id": "user-1", "verified": True}

        with patch("app.app.load_config", return_value=cfg), \
             patch("app.app._get_self_service_runtime", return_value=runtime), \
             patch("app.app._emby_authenticate_public_user", return_value=(binding, "")):
            login_resp = self.client.post(
                "/self_service_public",
                data={"action": "public_login", "emby_username": "tester", "emby_password": "secret"},
                follow_redirects=True,
            )
        self.assertEqual(login_resp.status_code, 200)

        with patch("app.app.load_config", return_value=cfg), \
             patch("app.app._get_self_service_runtime", return_value=runtime), \
             patch("app.app._check_public_rate_limit", return_value=(True, 0)), \
             patch("app.app._submit_self_service_request", return_value=app.response_class("ok", status=200)) as submit_mock:
            submit_resp = self.client.post(
                "/self_service_public",
                data={"action": "submit", "title": "示例片名", "type": "电影"},
                follow_redirects=False,
            )

        self.assertEqual(submit_resp.status_code, 200)
        submit_mock.assert_called_once()
        kwargs = submit_mock.call_args.kwargs
        self.assertEqual(kwargs.get("source_label"), "公共入口")
        self.assertIsInstance(kwargs.get("requester_binding_override"), dict)
        self.assertEqual(kwargs.get("requester_binding_override", {}).get("account"), "tester")

    def test_public_page_shows_current_user_recent_records(self):
        cfg = {
            "self_service_public_enabled": True,
            "self_service_public_access_key": "",
            "self_service_public_rate_limit": {"enabled": False, "window_seconds": 60, "max_requests": 5},
            "drama_calendar": {
                "emby_gap_fill": {
                    "enabled": True,
                    "base_url": "http://127.0.0.1:8096",
                    "api_key": "emby-key",
                }
            },
        }
        runtime = {
            "enabled": True,
            "targets": ["@admin"],
            "notify_targets": ["@admin"],
            "effective_targets": ["@admin"],
            "max_results": 5,
            "hdhive_cookie": "",
            "hdhive_api_key": "api-key",
            "use_open_api": True,
            "allow_open_api_direct": False,
            "storage_mode": "any",
            "base_url": "https://hdhive.com",
            "tmdb_api_key": "",
        }
        with self.client.session_transaction() as sess:
            sess["self_service_public_emby_auth"] = {
                "account": "tester",
                "name": "tester",
                "id": "user-1",
                "verified": True,
                "at": time.time(),
            }

        recent = [{
            "request_id": "rid-1",
            "status": "success",
            "message": "✅ 资源已入库，请等待3-5分钟后进入服务器观看。",
            "detail": "片名: 示例片名",
            "updated_at": time.time(),
            "updated_at_str": "2026-04-21 00:00:00",
        }]
        with patch("app.app.load_config", return_value=cfg), \
             patch("app.app._get_self_service_runtime", return_value=runtime), \
             patch("app.app._list_self_service_results_for_binding", return_value=recent):
            response = self.client.get("/self_service_public", follow_redirects=False)
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("我的申请记录", body)
        self.assertIn("示例片名", body)
        self.assertIn('data-self-service-open-rid="rid-1"', body)

    def test_public_delete_success_record(self):
        with self.client.session_transaction() as sess:
            sess["self_service_public_emby_auth"] = {
                "account": "tester",
                "name": "tester",
                "id": "user-1",
                "verified": True,
                "at": time.time(),
            }
        delete_payload = {
            "ok": True,
            "item_id": "item-1",
            "item_name": "示例片名",
            "item_year": "2024",
            "title": "示例片名",
            "year": "2024",
            "type": "电影",
        }
        with patch("app.app.load_config", return_value={}), \
             patch("app.app._get_self_service_result_for_context", return_value={"status": "success", "requester_account": "tester"}), \
             patch("app.app._emby_delete_media_for_result", return_value=delete_payload) as delete_mock, \
             patch("app.app._set_self_service_result") as set_result_mock:
            response = self.client.post("/self_service_public_delete", data={"rid": "rid-success"}, follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        delete_mock.assert_called_once()
        set_result_mock.assert_called_once()

    def test_public_delete_non_success_record_rejected(self):
        with self.client.session_transaction() as sess:
            sess["self_service_public_emby_auth"] = {
                "account": "tester",
                "name": "tester",
                "id": "user-1",
                "verified": True,
                "at": time.time(),
            }
        with patch("app.app._get_self_service_result_for_context", return_value={"status": "processing"}), \
             patch("app.app._emby_delete_media_for_result") as delete_mock:
            response = self.client.post("/self_service_public_delete", data={"rid": "rid-running"}, follow_redirects=True)
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("仅可删除申请成功的资源记录", body)
        delete_mock.assert_not_called()

    def test_verify_code_redirects_without_auth_flow(self):
        response = self.client.get("/verify_code", follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers.get("Location", "").endswith("/login"))

    def test_private_pages_render_when_logged_in(self):
        self._login()
        checks = [
            ("/", "控制台"),
            ("/config", "Telegram 配置中心"),
            ("/proxy_config", "代理设置"),
            ("/file_config", "文件同步配置中心"),
            ("/drama_calendar", "追剧配置中心"),
            ("/monitor_log", "Telegram 日志中心"),
            ("/file_monitor_log", "文件同步日志中心"),
            ("/drama_calendar_log", "追剧日志中心"),
            ("/downloader", "万能下载工具"),
            ("/self_service", "自助观影中心"),
        ]

        for path, marker in checks:
            with self.subTest(path=path):
                response = self.client.get(path, follow_redirects=False)
                body = response.get_data(as_text=True)
                self.assertEqual(response.status_code, 200)
                self.assertIn(marker, body)

    def test_logged_in_json_endpoints_return_success(self):
        self._login()

        get_checks = [
            "/monitor_log_data",
            "/file_monitor_log_data",
            "/drama_calendar_log_data",
            "/downloader/log_data",
            "/self_service_result",
        ]
        for path in get_checks:
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.is_json)

        response = self.client.post("/api/browse_dir", json={"path": "."})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.is_json)
        payload = response.get_json()
        self.assertIsInstance(payload, dict)
        self.assertIn("items", payload)


if __name__ == "__main__":
    unittest.main()
