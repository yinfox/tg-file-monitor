import asyncio
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


def _install_telegram_monitor_import_stubs():
    try:
        import telethon  # noqa: F401
    except ModuleNotFoundError:
        telethon = types.ModuleType("telethon")
        telethon.sync = types.ModuleType("telethon.sync")
        telethon.sync.TelegramClient = object
        telethon.events = types.ModuleType("telethon.events")
        telethon.events.NewMessage = lambda *args, **kwargs: None
        telethon.errors = types.ModuleType("telethon.errors")

        class FloodWaitError(Exception):
            def __init__(self, seconds=0):
                super().__init__(f"Flood wait: {seconds}")
                self.seconds = seconds

        telethon.errors.FloodWaitError = FloodWaitError
        telethon.tl = types.ModuleType("telethon.tl")
        telethon.tl.types = types.ModuleType("telethon.tl.types")

        class MessageMediaPhoto:
            pass

        class MessageMediaDocument:
            pass

        class MessageMediaWebPage:
            pass

        telethon.tl.types.MessageMediaPhoto = MessageMediaPhoto
        telethon.tl.types.MessageMediaDocument = MessageMediaDocument
        telethon.tl.types.MessageMediaWebPage = MessageMediaWebPage
        sys.modules["telethon"] = telethon
        sys.modules["telethon.sync"] = telethon.sync
        sys.modules["telethon.events"] = telethon.events
        sys.modules["telethon.errors"] = telethon.errors
        sys.modules["telethon.tl"] = telethon.tl
        sys.modules["telethon.tl.types"] = telethon.tl.types

    try:
        import dotenv  # noqa: F401
    except ModuleNotFoundError:
        dotenv = types.ModuleType("dotenv")
        dotenv.load_dotenv = lambda *args, **kwargs: False
        sys.modules["dotenv"] = dotenv


_install_telegram_monitor_import_stubs()

import telegram_monitor

from telegram_monitor import _should_send_copy_instead_of_forward


class HDHiveForwardingDecisionTestCase(unittest.TestCase):
    def test_hdhive_hit_always_uses_rewritten_copy(self):
        self.assertTrue(
            _should_send_copy_instead_of_forward(
                media_type_detected="video",
                convert_hdhive_enabled=False,
                has_hdhive=True,
            )
        )

    def test_convert_hdhive_without_hit_keeps_original_forward_for_media(self):
        self.assertFalse(
            _should_send_copy_instead_of_forward(
                media_type_detected="video",
                convert_hdhive_enabled=True,
                has_hdhive=False,
            )
        )

    def test_convert_hdhive_without_hit_keeps_original_forward_for_text(self):
        self.assertFalse(
            _should_send_copy_instead_of_forward(
                media_type_detected="text",
                convert_hdhive_enabled=True,
                has_hdhive=False,
            )
        )

    def test_plain_text_without_conversion_still_uses_send_message(self):
        self.assertTrue(
            _should_send_copy_instead_of_forward(
                media_type_detected="text",
                convert_hdhive_enabled=False,
                has_hdhive=False,
            )
        )

    def test_plain_media_without_conversion_keeps_forward(self):
        self.assertFalse(
            _should_send_copy_instead_of_forward(
                media_type_detected="video",
                convert_hdhive_enabled=False,
                has_hdhive=False,
            )
        )

    def test_extract_hdhive_urls_matches_new_resource_format(self):
        url = "https://hdhive.com/resource/ABCDEFGHIJKLMNOP"
        self.assertEqual(
            telegram_monitor._extract_hdhive_urls_from_text(f"资源链接：{url}"),
            [url],
        )

    def test_extract_hdhive_urls_still_matches_legacy_resource_format(self):
        url = "https://hdhive.com/resource/115/ABCDEFGHIJKLMNOP"
        self.assertEqual(
            telegram_monitor._extract_hdhive_urls_from_text(f"资源链接：{url}"),
            [url],
        )

    def test_convert_hdhive_new_resource_format_rewrites_message(self):
        source_url = "https://hdhive.com/resource/ABCDEFGHIJKLMNOP"
        real_url = "https://115.com/s/example?password=abcd"
        msg = SimpleNamespace(message=f"资源链接：{source_url}", entities=[], media=None, reply_markup=None)

        with patch(
            "telegram_monitor.resolve_hdhive_115_url_with_note",
            new=AsyncMock(return_value=(real_url, "解析成功")),
        ) as resolver:
            changed, new_text = asyncio.run(telegram_monitor.convert_message_hdhive_links(msg))

        self.assertTrue(changed)
        resolver.assert_awaited_once_with(source_url)
        self.assertIn(real_url, new_text)
        self.assertNotIn(source_url, new_text)

    def test_resolve_with_note_returns_free_resource_url_without_unlock(self):
        source_url = "https://hdhive.com/resource/115/ABCDEFGHIJKLMNOP"
        real_url = "https://115cdn.com/s/example?password=abcd"
        url_info = {
            "url": real_url,
            "access_code": "",
            "unlock_points": 0,
        }

        with patch.object(telegram_monitor, "current_config", {}, create=True), \
             patch("telegram_monitor._get_hdhive_open_api_key", return_value=""), \
             patch("telegram_monitor._get_hdhive_cookie_header", return_value="foo=bar"), \
             patch("telegram_monitor._hdhive_go_api_get_url_info_sync", return_value=url_info), \
             patch("telegram_monitor._hdhive_unlock_resource_sync") as unlock:
            resolved, note = telegram_monitor._resolve_hdhive_115_url_with_note_sync(source_url)

        self.assertEqual(resolved, real_url)
        self.assertEqual(note, "免积分资源，解析成功")
        unlock.assert_not_called()

    def test_resolve_with_note_prefers_open_api_free_resource_when_direct_unlock_disabled(self):
        source_url = "https://hdhive.com/resource/115/ABCDEFGHIJKLMNOP"
        real_url = "https://115cdn.com/s/open-api-free?password=abcd"
        detail_resp = {
            "data": {
                "url": real_url,
                "access_code": "",
                "unlock_points": 0,
            }
        }

        with patch.object(
            telegram_monitor,
            "current_config",
            {"hdhive_open_api_direct_unlock": False, "hdhive_auto_unlock_points_threshold": 0},
            create=True,
        ), \
             patch("telegram_monitor._get_hdhive_open_api_key", return_value="api-key"), \
             patch("telegram_monitor._hdhive_open_api_resource_detail", return_value=detail_resp), \
             patch("telegram_monitor._get_hdhive_cookie_header", return_value=""), \
             patch("telegram_monitor._hdhive_open_api_unlock") as open_api_unlock, \
             patch("telegram_monitor._hdhive_go_api_get_url_info_sync") as cookie_resolver:
            resolved, note = telegram_monitor._resolve_hdhive_115_url_with_note_sync(source_url)

        self.assertEqual(resolved, real_url)
        self.assertEqual(note, "Open API：免积分资源，解析成功")
        open_api_unlock.assert_not_called()
        cookie_resolver.assert_not_called()

    def test_open_api_request_surfaces_cloudflare_html_error(self):
        fake_response = Mock()
        fake_response.status_code = 403
        fake_response.headers = {"Content-Type": "text/html; charset=UTF-8"}
        fake_response.text = "<html><title>Attention Required! | Cloudflare</title></html>"
        fake_response.json.side_effect = ValueError("not json")

        with patch("telegram_monitor._requests_request", return_value=fake_response):
            result = telegram_monitor._hdhive_open_api_request(
                "GET",
                "https://hdhive.com/api/open/resources/detail/movie",
                "api-key",
            )

        self.assertFalse(result["success"])
        self.assertEqual(result["code"], "403")
        self.assertEqual(result["message"], "Cloudflare challenge blocked request")

    def test_open_api_resource_detail_returns_more_informative_fallback_error(self):
        with patch(
            "telegram_monitor._hdhive_open_api_request",
            side_effect=[
                {"success": False, "code": "404", "message": "404 page not found"},
                {"success": False, "code": "400", "message": "参数错误", "description": "type 必须是 movie 或 tv"},
            ],
        ):
            result = telegram_monitor._hdhive_open_api_resource_detail(
                "https://hdhive.com",
                "api-key",
                "ABCDEFGHIJKLMNOP",
            )

        self.assertFalse(result["success"])
        self.assertEqual(result["code"], "400")
        self.assertEqual(result["description"], "type 必须是 movie 或 tv")

    def test_resolve_with_note_keeps_open_api_fallback_reason_when_cookie_succeeds(self):
        source_url = "https://hdhive.com/resource/115/ABCDEFGHIJKLMNOP"
        cookie_url_info = {
            "url": "https://115cdn.com/s/cookie-fallback?password=abcd",
            "access_code": "",
            "unlock_points": 0,
        }
        detail_resp = {
            "success": False,
            "code": "400",
            "message": "参数错误",
            "description": "type 必须是 movie 或 tv",
        }

        with patch.object(
            telegram_monitor,
            "current_config",
            {"hdhive_open_api_direct_unlock": False, "hdhive_auto_unlock_points_threshold": 0},
            create=True,
        ), \
             patch("telegram_monitor._get_hdhive_open_api_key", return_value="api-key"), \
             patch("telegram_monitor._hdhive_open_api_resource_detail", return_value=detail_resp), \
             patch("telegram_monitor._get_hdhive_cookie_header", return_value="foo=bar"), \
             patch("telegram_monitor._hdhive_go_api_get_url_info_sync", return_value=cookie_url_info):
            resolved, note = telegram_monitor._resolve_hdhive_115_url_with_note_sync(source_url)

        self.assertEqual(resolved, "https://115cdn.com/s/cookie-fallback?password=abcd")
        self.assertIn("Open API：详情接口不可用", note)
        self.assertIn("免积分资源，解析成功", note)

    def test_resolve_with_note_uses_open_api_unlock_when_detail_fails_and_direct_unlock_enabled(self):
        source_url = "https://hdhive.com/resource/115/ABCDEFGHIJKLMNOP"
        detail_resp = {
            "success": False,
            "code": "400",
            "message": "参数错误",
            "description": "type 必须是 movie 或 tv",
        }
        unlock_resp = {
            "success": True,
            "message": "免费资源",
            "data": {
                "url": "https://115cdn.com/s/open-api-unlock?password=abcd",
                "full_url": "https://115cdn.com/s/open-api-unlock?password=abcd",
                "access_code": "",
                "already_owned": False,
            },
        }

        with patch.object(
            telegram_monitor,
            "current_config",
            {"hdhive_open_api_direct_unlock": True, "hdhive_auto_unlock_points_threshold": 0},
            create=True,
        ), \
             patch("telegram_monitor._get_hdhive_open_api_key", return_value="api-key"), \
             patch("telegram_monitor._hdhive_open_api_resource_detail", return_value=detail_resp), \
             patch("telegram_monitor._hdhive_open_api_unlock", return_value=unlock_resp) as open_api_unlock, \
             patch("telegram_monitor._get_hdhive_cookie_header", return_value=""), \
             patch("telegram_monitor._hdhive_go_api_get_url_info_sync") as cookie_resolver:
            resolved, note = telegram_monitor._resolve_hdhive_115_url_with_note_sync(source_url)

        self.assertEqual(resolved, "https://115cdn.com/s/open-api-unlock?password=abcd")
        self.assertIn("Open API：详情接口不可用", note)
        self.assertIn("Open API：免费资源", note)
        open_api_unlock.assert_called_once_with("https://hdhive.com", "api-key", "ABCDEFGHIJKLMNOP")
        cookie_resolver.assert_not_called()


if __name__ == "__main__":
    unittest.main()
