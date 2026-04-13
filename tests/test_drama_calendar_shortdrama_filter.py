import unittest

from scripts.update_drama_calendar_env import extract_calendar_title_states


class DramaCalendarShortDramaFilterTestCase(unittest.TestCase):
    def test_extract_calendar_title_states_skips_short_drama_sections(self):
        text = """
网络短剧追更日历
《短剧甲》上线
《短剧乙》开播
网络剧追更日历
《长剧甲》上线
网络电影追更日历
《电影甲》开播
        """.strip()

        titles, target_lines, title_states = extract_calendar_title_states(
            text=text,
            include_keywords=["上线", "开播"],
            post_date=None,
            finish_keywords=["完结", "收官", "大结局"],
            finish_exclude_keywords=["未完结"],
        )

        self.assertIn("长剧甲", titles)
        self.assertIn("电影甲", titles)
        self.assertNotIn("短剧甲", titles)
        self.assertNotIn("短剧乙", titles)
        self.assertIn("长剧甲", title_states)
        self.assertIn("电影甲", title_states)
        self.assertNotIn("短剧甲", title_states)
        self.assertNotIn("短剧乙", title_states)
        joined_lines = "\n".join(target_lines)
        self.assertNotIn("短剧甲", joined_lines)
        self.assertNotIn("短剧乙", joined_lines)

    def test_extract_calendar_title_states_skips_inline_short_drama_line(self):
        text = "网络短剧追更日历：《短剧丙》上线\n《长剧乙》上线"

        titles, _target_lines, _title_states = extract_calendar_title_states(
            text=text,
            include_keywords=["上线", "开播"],
            post_date=None,
            finish_keywords=["完结", "收官", "大结局"],
            finish_exclude_keywords=["未完结"],
        )

        self.assertIn("长剧乙", titles)
        self.assertNotIn("短剧丙", titles)

    def test_extract_calendar_title_states_includes_long_drama_update_lines(self):
        text = """
追剧日历
【电视剧更新概述】5部长剧更新
《长剧丙》更新至11-12集
【网络短剧追更日历】6部短剧更新
《短剧丁》更新至9-10集
        """.strip()

        titles, target_lines, title_states = extract_calendar_title_states(
            text=text,
            include_keywords=["上线", "开播"],
            post_date=None,
            finish_keywords=["完结", "收官", "大结局"],
            finish_exclude_keywords=["未完结"],
        )

        self.assertIn("长剧丙", titles)
        self.assertIn("长剧丙", title_states)
        self.assertNotIn("短剧丁", titles)
        self.assertNotIn("短剧丁", title_states)
        joined_lines = "\n".join(target_lines)
        self.assertIn("长剧丙", joined_lines)
        self.assertNotIn("短剧丁", joined_lines)

    def test_extract_calendar_title_states_resets_after_short_drama_section(self):
        text = """
【网络短剧追更日历】短剧更新
《短剧甲》上线
【近期待播或待定档的新剧】
《长剧甲》有望近期开播
        """.strip()

        titles, target_lines, title_states = extract_calendar_title_states(
            text=text,
            include_keywords=["上线", "开播"],
            post_date=None,
            finish_keywords=["完结", "收官", "大结局"],
            finish_exclude_keywords=["未完结"],
        )

        self.assertNotIn("短剧甲", titles)
        self.assertNotIn("短剧甲", title_states)
        self.assertIn("长剧甲", titles)
        self.assertIn("长剧甲", title_states)
        joined_lines = "\n".join(target_lines)
        self.assertNotIn("短剧甲", joined_lines)
        self.assertIn("长剧甲", joined_lines)

    def test_extract_calendar_title_states_skips_short_drama_single_line_hint(self):
        text = """
【近期待播或待定档的新剧】
景研竣、崔宝月主演的短剧《侦宋》4月14日桃厂上线
白宇帆、于文文主演的电视剧《高兴》有望近期桃厂上线
        """.strip()

        titles, target_lines, title_states = extract_calendar_title_states(
            text=text,
            include_keywords=["上线", "开播"],
            post_date=None,
            finish_keywords=["完结", "收官", "大结局"],
            finish_exclude_keywords=["未完结"],
        )

        self.assertNotIn("侦宋", titles)
        self.assertNotIn("侦宋", title_states)
        self.assertIn("高兴", titles)
        self.assertIn("高兴", title_states)
        joined_lines = "\n".join(target_lines)
        self.assertNotIn("侦宋", joined_lines)
        self.assertIn("高兴", joined_lines)


if __name__ == "__main__":
    unittest.main()
