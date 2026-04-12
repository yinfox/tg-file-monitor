import unittest
from unittest.mock import patch
import tempfile
from unittest.mock import AsyncMock
from unittest.mock import Mock

import telegram_monitor
from app.app import app, _build_proxy_url_from_config, _build_tmdb_proxy_env, _try_115_share_transfer
from app.api_115 import Client115
from app.proxy_helpers import build_telethon_proxy_from_scope_config


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

    def test_proxy_url_builder_respects_scheme_in_addr(self):
        config = {
            "proxy": {
                "service": {
                    "addr": "socks5://127.0.0.1",
                    "port": "7890",
                }
            }
        }
        expected = "socks5://127.0.0.1:7890"
        self.assertEqual(_build_proxy_url_from_config(config, "service"), expected)
        self.assertEqual(telegram_monitor._build_proxy_url_from_config(config, "service"), expected)

    def test_telethon_proxy_builder_defaults_to_socks5_and_parses_http_scheme(self):
        plain = build_telethon_proxy_from_scope_config({
            "addr": "127.0.0.1",
            "port": "7890",
            "username": "",
            "password": "",
        })
        self.assertEqual(plain, ("socks5", "127.0.0.1", 7890, True, None, None))

        with_http = build_telethon_proxy_from_scope_config({
            "addr": "http://127.0.0.1",
            "port": "8080",
            "username": "u",
            "password": "p",
        })
        self.assertEqual(with_http, ("http", "127.0.0.1", 8080, True, "u", "p"))

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

    def test_115_uploadinfo_errno_99_returns_relogin_message(self):
        client = Client115(cookie="cookie=value")
        fake_p115 = Mock()
        fake_p115.upload_key.return_value = {
            "state": False,
            "error": "请重新登录",
            "errno": 99,
            "request": "/android/2.0/user/upload_key",
            "data": [],
        }
        fake_p115.upload_info.return_value = {
            "state": False,
            "error": "请重新登录",
            "errno": 99,
            "request": "/app/uploadinfo",
            "data": [],
        }
        client.p115_client = fake_p115
        result = client.check_file_exists(
            "9e5198f1f78afb17f373f1e8f5ffa857888352fe",
            879039017,
            "test.mp4",
            target="U_1_123456",
            file_path=__file__,
        )
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "COOKIE_RELOGIN_REQUIRED")
        self.assertIn("重新登录 115", result["message"])

    def test_115_second_transfer_reuse_uses_native_upload_init(self):
        client = Client115(cookie="cookie=value")
        fake_p115 = Mock()
        fake_p115.upload_key.return_value = {"state": True, "data": {"userkey": "abc"}}
        fake_p115.upload_init.return_value = {
            "state": True,
            "status": 2,
            "reuse": True,
            "data": {"file_id": "123", "pick_code": "pick"},
        }
        client.p115_client = fake_p115
        result = client.check_file_exists(
            "9e5198f1f78afb17f373f1e8f5ffa857888352fe",
            879039017,
            "test.mp4",
            target="U_1_123456",
            file_path=__file__,
        )
        self.assertTrue(result["success"])
        self.assertTrue(result["already_exists"])
        fake_p115.upload_key.assert_called_once()
        fake_p115.upload_init.assert_called_once()

    def test_115_second_transfer_falls_back_to_upload_info_when_upload_key_fails(self):
        client = Client115(cookie="cookie=value")
        fake_p115 = Mock()
        fake_p115.upload_key.return_value = {
            "state": False,
            "error": "temporary unavailable",
            "errno": 500,
        }
        fake_p115.upload_info.return_value = {
            "state": True,
            "userkey": "fallback-key",
            "userid": "123456",
        }
        fake_p115.upload_init.return_value = {
            "state": True,
            "status": 2,
            "data": {"file_id": "123", "pick_code": "pick"},
        }
        client.p115_client = fake_p115
        result = client.check_file_exists(
            "9e5198f1f78afb17f373f1e8f5ffa857888352fe",
            879039017,
            "test.mp4",
            target="U_1_123456",
            file_path=__file__,
        )
        self.assertTrue(result["success"])
        self.assertTrue(result["already_exists"])
        fake_p115.upload_key.assert_called_once()
        fake_p115.upload_info.assert_called_once()
        fake_p115.upload_init.assert_called_once()

    def test_115_second_transfer_falls_back_to_upload_info_when_upload_key_raises(self):
        client = Client115(cookie="cookie=value")
        fake_p115 = Mock()
        fake_p115.upload_key.side_effect = RuntimeError(
            "code=405 method='GET' url='https://proapi.115.com/android/2.0/user/upload_key' "
            "reason='Method Not Allowed'"
        )
        fake_p115.upload_info.return_value = {
            "state": True,
            "userkey": "fallback-key",
            "userid": "123456",
        }
        fake_p115.upload_init.return_value = {
            "state": True,
            "status": 2,
            "data": {"file_id": "123", "pick_code": "pick"},
        }
        client.p115_client = fake_p115
        result = client.check_file_exists(
            "9e5198f1f78afb17f373f1e8f5ffa857888352fe",
            879039017,
            "test.mp4",
            target="U_1_123456",
            file_path=__file__,
        )
        self.assertTrue(result["success"])
        self.assertTrue(result["already_exists"])
        self.assertEqual(fake_p115.upload_key.call_count, 1)
        self.assertEqual(fake_p115.upload_info.call_count, 1)
        fake_p115.upload_init.assert_called_once()

    def test_115_upload_chain_falls_back_to_upload_info_when_upload_key_raises(self):
        client = Client115(cookie="cookie=value")
        fake_p115 = Mock()
        fake_p115.upload_key.side_effect = RuntimeError(
            "code=405 method='GET' url='https://proapi.115.com/android/2.0/user/upload_key' "
            "reason='Method Not Allowed'"
        )
        fake_p115.upload_info.return_value = {
            "state": True,
            "userkey": "fallback-key",
            "userid": "123456",
        }
        fake_p115.upload_init.return_value = {
            "state": True,
            "status": 1,
            "reuse": False,
            "data": {},
        }
        client.p115_client = fake_p115
        result = client.test_upload_chain(target="U_1_123456")
        self.assertTrue(result["success"])
        self.assertEqual(result["credential_source"], "upload_info")
        self.assertEqual(fake_p115.upload_key.call_count, 1)
        self.assertEqual(fake_p115.upload_info.call_count, 1)
        fake_p115.upload_init.assert_called_once()

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

    def test_115_upload_chain_api_returns_json(self):
        self._login()

        fake_client = Mock()
        fake_client.test_upload_chain.return_value = {
            "success": True,
            "message": "上传链路可用：已通过初始化，可进入真实上传",
            "credential_source": "upload_key",
        }

        with patch("app.app.Client115", return_value=fake_client):
            response = self.client.post(
                "/api/115/test_upload_chain",
                json={
                    "cookie_115": "cookie=value",
                    "target_115_cid": "123456",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.is_json)
        payload = response.get_json()
        self.assertTrue(payload.get("success"))
        self.assertIn("上传链路可用", payload.get("message", ""))


if __name__ == "__main__":
    unittest.main()
