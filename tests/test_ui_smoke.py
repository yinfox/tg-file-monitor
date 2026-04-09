import unittest

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
