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
    def setUp(self):
        try:
            telegram_monitor._HDHIVE_OPEN_API_RATE_LIMIT_CACHE.clear()
        except Exception:
            pass

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

    def test_split_media_caption_and_followups_keeps_short_text_intact(self):
        caption, followups = telegram_monitor._split_media_caption_and_followups(
            "短说明",
            caption_limit=20,
            message_limit=50,
        )
        self.assertEqual(caption, "短说明")
        self.assertEqual(followups, [])

    def test_split_media_caption_and_followups_preserves_all_ed2k_links(self):
        links = [
            f"ed2k://|file|Example.S01E{i:02d}.mkv|123456|ABCDEF1234567890ABCDEF12345678{i:02d}|/"
            for i in range(1, 9)
        ]
        text = "资源合集\n" + "\n".join(links)
        caption, followups = telegram_monitor._split_media_caption_and_followups(
            text,
            caption_limit=120,
            message_limit=160,
        )

        self.assertLessEqual(len(caption), 120)
        self.assertTrue(followups)
        self.assertTrue(all(len(chunk) <= 160 for chunk in followups))

        merged = "\n".join([caption] + followups)
        for link in links:
            self.assertIn(link, merged)

    def test_split_text_for_telegram_messages_hard_splits_long_token(self):
        long_token = "ed2k://|file|" + ("A" * 300) + "|123456|HASHVALUE|/"
        chunks = telegram_monitor._split_text_for_telegram_messages(long_token, max_length=80)
        self.assertTrue(chunks)
        self.assertTrue(all(len(chunk) <= 80 for chunk in chunks))
        self.assertEqual("".join(chunks), long_token)

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

    def test_build_hdhive_full_url_supports_ed2k(self):
        ed2k_url = "ed2k://|file|Example.S01E01.mkv|123456|ABCDEF1234567890ABCDEF1234567890|/"
        self.assertEqual(
            telegram_monitor._build_hdhive_full_url({"full_url": ed2k_url}),
            ed2k_url,
        )
        self.assertEqual(
            telegram_monitor._build_hdhive_full_url({"url": ed2k_url, "access_code": "ignored"}),
            ed2k_url,
        )

    def test_resolve_with_note_prefers_open_api_ed2k_free_resource(self):
        source_url = "https://hdhive.com/resource/115/ABCDEFGHIJKLMNOP"
        ed2k_url = "ed2k://|file|Example.S01E01.mkv|123456|ABCDEF1234567890ABCDEF1234567890|/"
        detail_resp = {
            "data": {
                "url": ed2k_url,
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

        self.assertEqual(resolved, ed2k_url)
        self.assertEqual(note, "Open API：免积分资源，解析成功")
        open_api_unlock.assert_not_called()
        cookie_resolver.assert_not_called()

    def test_open_api_rate_limit_allows_three_requests_per_minute(self):
        url = "https://hdhive.com/api/open/ping"
        api_key = "api-key"

        wait1 = telegram_monitor._reserve_hdhive_open_api_slot(url, api_key, now_ts=100.0)
        wait2 = telegram_monitor._reserve_hdhive_open_api_slot(url, api_key, now_ts=110.0)
        wait3 = telegram_monitor._reserve_hdhive_open_api_slot(url, api_key, now_ts=120.0)
        wait4 = telegram_monitor._reserve_hdhive_open_api_slot(url, api_key, now_ts=130.0)
        wait5 = telegram_monitor._reserve_hdhive_open_api_slot(url, api_key, now_ts=161.0)

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

        with patch("telegram_monitor._wait_for_hdhive_open_api_slot"), \
             patch("telegram_monitor.time.sleep") as sleep_mock, \
             patch("telegram_monitor._requests_request", side_effect=[too_many, success]) as req_mock:
            result = telegram_monitor._hdhive_open_api_request(
                "GET",
                "https://hdhive.com/api/open/ping",
                "api-key",
            )

        self.assertTrue(bool(result.get("success")))
        self.assertEqual(req_mock.call_count, 2)
        sleep_mock.assert_called_once_with(2)

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
