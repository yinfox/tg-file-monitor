import unittest

from app import bot_monitor


class BotMonitorShareLinkTestCase(unittest.TestCase):
    def test_non_115_url_with_hyphenated_query_is_not_detected_as_115_share_code(self):
        text = "https://youtu.be/dQw4w9WgXcQ?si=abcdefgh-ijkl"
        self.assertEqual(bot_monitor._extract_115_share_link(text), "")

    def test_bare_115_share_code_is_still_supported(self):
        text = "115分享码 abcdefgh-ijkl"
        self.assertEqual(bot_monitor._extract_115_share_link(text), "abcdefgh-ijkl")


if __name__ == "__main__":
    unittest.main()
