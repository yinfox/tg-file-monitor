import unittest

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


if __name__ == "__main__":
    unittest.main()
