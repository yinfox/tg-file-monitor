import unittest
from unittest.mock import patch

from app.app import (
    _HDHIVE_CHECKIN_ENDPOINT_404_MESSAGE,
    _extract_hdhive_checkin_action_id_from_js,
    _extract_hdhive_next_chunk_paths,
    _hdhive_do_checkin,
    _hdhive_do_checkin_via_server_action,
)


class HDHiveCheckinTestCase(unittest.TestCase):
    def test_extract_checkin_action_id_supports_legacy_server_reference_syntax(self):
        js_text = 'let x=createServerReference("409539c7faa0ad25d3e3e8c21465c10661896ca5a2",callServer,void 0,findSourceMapURL,"checkIn");'
        self.assertEqual(
            _extract_hdhive_checkin_action_id_from_js(js_text),
            "409539c7faa0ad25d3e3e8c21465c10661896ca5a2",
        )

    def test_extract_checkin_action_id_supports_current_wrapped_server_reference_syntax(self):
        js_text = 'let j=(0,k.createServerReference)("40efbc107064215e9eff178b0466274739ba7d9cb4",k.callServer,void 0,k.findSourceMapURL,"checkIn");'
        self.assertEqual(
            _extract_hdhive_checkin_action_id_from_js(js_text),
            "40efbc107064215e9eff178b0466274739ba7d9cb4",
        )

    def test_extract_next_chunk_paths_includes_flight_embedded_static_chunks(self):
        html_text = '1d:I[80733,["6300","static/chunks/6300-39e322369d1d0d1c.js","4944","static/chunks/app/(app)/layout-324227863d0b7ccc.js"],"default"]'
        paths = _extract_hdhive_next_chunk_paths(html_text)
        self.assertIn("/_next/static/chunks/6300-39e322369d1d0d1c.js", paths)
        self.assertIn("/_next/static/chunks/app/(app)/layout-324227863d0b7ccc.js", paths)

    def test_server_action_404_retries_with_forced_refresh(self):
        action_force_flags = []
        tree_force_flags = []

        def fake_refresh(_base_url, _cookie="", force_refresh=False):
            action_force_flags.append(force_refresh)
            return "action-refreshed" if force_refresh else "action-stale"

        def fake_tree(_base_url, _cookie="", force_refresh=False):
            tree_force_flags.append(force_refresh)
            return '["root"]'

        with patch("app.app._refresh_hdhive_checkin_action_id_if_needed", side_effect=fake_refresh), \
             patch("app.app._get_hdhive_home_router_state_tree_json", side_effect=fake_tree), \
             patch(
                 "app.app._hdhive_next_action_call_sync",
                 side_effect=[
                     (None, 404, "404 Not Found"),
                     ({"response": {"message": "签到成功", "points": 88}}, 200, '1:{"response":{"message":"签到成功","points":88}}'),
                 ],
             ):
            ok, msg, points = _hdhive_do_checkin_via_server_action("https://hdhive.com", "foo=bar", "normal")

        self.assertTrue(ok)
        self.assertEqual(msg, "签到成功")
        self.assertEqual(points, 88)
        self.assertEqual(action_force_flags, [False, True])
        self.assertEqual(tree_force_flags, [False, True])

    def test_legacy_checkin_404_returns_informative_message(self):
        with patch("app.app._hdhive_do_checkin_via_server_action", return_value=(False, "签到响应异常", None)), \
             patch("app.app._hdhive_request_json", side_effect=[(None, "404 Not Found", 404)] * 6), \
             patch("app.app._hdhive_fetch_points", return_value=None):
            ok, msg, points = _hdhive_do_checkin("https://hdhive.com", "foo=bar", "normal", {})

        self.assertFalse(ok)
        self.assertEqual(msg, _HDHIVE_CHECKIN_ENDPOINT_404_MESSAGE)
        self.assertIsNone(points)


if __name__ == "__main__":
    unittest.main()
