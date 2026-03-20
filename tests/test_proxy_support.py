import unittest
from unittest.mock import patch
import tempfile
from unittest.mock import AsyncMock

import telegram_monitor
from app.app import app, _build_proxy_url_from_config, _build_tmdb_proxy_env, _try_115_share_transfer


class ProxySupportTestCase(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def _login(self):
        with self.client.session_transaction() as sess:
            sess["logged_in"] = True

    def test_proxy_url_builder_encodes_credentials(self):
        config = {
            "proxy": {
                "service": {
                    "addr": "127.0.0.1",
                    "port": "7890",
                    "username": "user@example.com",
                    "password": "p@ss:word",
                }
            }
        }
        expected = "http://user%40example.com:p%40ss%3Aword@127.0.0.1:7890"
        self.assertEqual(_build_proxy_url_from_config(config, "service"), expected)
        self.assertEqual(telegram_monitor._build_proxy_url_from_config(config, "service"), expected)

    def test_config_page_renders_proxy_settings_section(self):
        self._login()
        response = self.client.get("/config", follow_redirects=False)
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("代理设置", body)
        self.assertIn("/proxy_config", body)

    def test_proxy_config_page_renders_three_proxy_forms(self):
        self._login()
        response = self.client.get("/proxy_config", follow_redirects=False)
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("保存 Telegram 代理", body)
        self.assertIn("保存 HDHive / TMDB 代理", body)
        self.assertIn("保存下载器代理", body)

    def test_tmdb_proxy_env_builder_exports_proxy_url(self):
        config = {"proxy": {"service": {"addr": "127.0.0.1", "port": "7890"}}}
        env = _build_tmdb_proxy_env(config)
        self.assertEqual(env.get("DRAMA_TMDB_PROXY_URL"), "http://127.0.0.1:7890")

    def test_115_transfer_does_not_pass_proxy_to_client(self):
        payload = {
            "115_cookie": "cookie=value",
            "115_target_cid": "123456",
        }
        fake_client = type("FakeClient", (), {
            "receive_share_link": lambda self, real_url, target_cid="", accept_all=False: {"success": True}
        })()
        with patch("app.app.Client115", return_value=fake_client) as client_cls:
            ok, reason = _try_115_share_transfer("https://115cdn.com/s/test?password=abcd", payload)
        self.assertTrue(ok)
        self.assertEqual(reason, "transfer_ok")
        client_cls.assert_called_once_with(cookie="cookie=value")

    def test_web_downloader_uses_only_downloader_proxy_scope(self):
        self._login()
        with tempfile.TemporaryDirectory() as tmpdir:
            scheduled = []
            def _close_task(coro):
                scheduled.append(coro)
                coro.close()
            with patch("app.app.load_config", return_value={
                    "downloader": {},
                    "proxy": {
                        "telegram": {"addr": "127.0.0.2", "port": "8899"},
                        "service": {"addr": "127.0.0.3", "port": "9900"},
                        "downloader": {"addr": "127.0.0.1", "port": "7890"},
                    },
                }), \
                 patch("app.app.save_config"), \
                 patch("app.app.asyncio.create_task", side_effect=_close_task), \
                 patch("app.app.downloader.download_task", new_callable=AsyncMock) as download_task:
                response = self.client.post(
                    "/api/download",
                    data={
                        "url": "https://example.com/video",
                        "output_dir": tmpdir,
                        "quality_mode": "balanced_hd",
                    },
                )

            self.assertEqual(response.status_code, 200)
            download_task.assert_called_once()
            self.assertEqual(download_task.call_args.args[4], "http://127.0.0.1:7890")
            self.assertEqual(len(scheduled), 1)


if __name__ == "__main__":
    unittest.main()
