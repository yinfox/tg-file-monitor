import unittest
import asyncio
from unittest.mock import patch

import telegram_monitor


class _DummyReplyTo:
    def __init__(self, msg_id):
        self.reply_to_msg_id = msg_id
        self.reply_to_top_id = None


class _DummySender:
    def __init__(self, first_name="", last_name="", username=""):
        self.first_name = first_name
        self.last_name = last_name
        self.username = username
        self.title = ""


class _DummyMsg:
    def __init__(self, msg_id, text, *, reply_to=None, out=False, sender=None):
        self.id = msg_id
        self.message = text
        self.reply_to = reply_to
        self.out = out
        self.sender = sender
        self.post_author = None
        self.sender_name = None
        self.buttons = None
        self.clicked = []

    async def click(self, *args, **kwargs):
        self.clicked.append((args, kwargs))
        return True


class _DummyButton:
    def __init__(self, text):
        self.text = text


class _DummyClient:
    def __init__(self, messages):
        self._messages = messages
        self.sent_messages = []

    async def get_messages(self, chat_id, limit=100):
        return self._messages

    async def send_message(self, target, message, **kwargs):
        self.sent_messages.append((target, message, kwargs))
        return True


class _DummyEvent:
    def __init__(self, chat_id=-100):
        self.chat_id = chat_id
        self.out = False
        self.chat = None


class RedPacketCaptchaTestCase(unittest.TestCase):
    def test_normalize_captcha_code(self):
        self.assertEqual(telegram_monitor._normalize_captcha_code(" a1 b2 c3 "), "a1b2c3")
        self.assertEqual(telegram_monitor._normalize_captcha_code("验证码: 12-AB-34"), "12AB34")
        self.assertEqual(telegram_monitor._normalize_captcha_code("无有效口令"), "")

    def test_captcha_prompt_detection(self):
        text = "回复本消息并发送图中完整验证码即可抢红包"
        keywords = ["回复本消息", "验证码", "口令红包"]
        self.assertTrue(telegram_monitor._looks_like_redpacket_captcha_prompt(text, keywords))
        self.assertFalse(telegram_monitor._looks_like_redpacket_captcha_prompt("发送验证码即可", keywords))
        self.assertFalse(telegram_monitor._looks_like_redpacket_captcha_prompt("回复本消息就行", keywords))

    def test_auto_click_rules_are_poetry_only(self):
        rules = telegram_monitor._get_auto_click_rules(
            {
                "auto_click_redpacket": True,
                "auto_click_keywords": ["发了一个红包"],
                "auto_click_button_texts": ["抢红包"],
                "auto_click_captcha_reply": True,
                "auto_click_captcha_keywords": ["回复本消息", "验证码"],
                "auto_click_notify_targets": ["123456"],
            }
        )
        self.assertEqual(rules.get("keywords"), [])
        self.assertEqual(rules.get("button_texts"), [])
        self.assertFalse(rules.get("captcha_reply_enabled"))
        self.assertTrue(rules.get("captcha_reply_from_success_only"))
        self.assertEqual(rules.get("captcha_keywords"), [])
        self.assertEqual(rules.get("notify_targets"), ["123456"])

    def test_auto_click_rules_normalize_risk_settings(self):
        rules = telegram_monitor._get_auto_click_rules(
            {
                "auto_click_redpacket": True,
                "auto_click_risk_control_enabled": True,
                "auto_click_min_interval_seconds": "30",
                "auto_click_hourly_limit": "5",
                "auto_click_random_delay_seconds": "1.5",
            }
        )
        self.assertEqual(
            rules.get("risk"),
            {
                "enabled": True,
                "min_interval_seconds": 30.0,
                "hourly_limit": 5,
                "random_delay_seconds": 1.5,
            },
        )

    def test_auto_click_rules_normalize_delay(self):
        rules = telegram_monitor._get_auto_click_rules(
            {"auto_click_redpacket": True, "auto_click_delay_seconds": "1.5"}
        )
        self.assertEqual(rules.get("delay_seconds"), 1.5)

    def test_auto_click_rules_ignore_legacy_captcha_settings(self):
        rules = telegram_monitor._get_auto_click_rules(
            {
                "auto_click_redpacket": True,
                "auto_click_captcha_reply": True,
                "auto_click_captcha_keywords": ["回复本消息", "验证码"],
            }
        )
        self.assertFalse(rules.get("captcha_reply_enabled"))
        self.assertEqual(rules.get("captcha_keywords"), [])

    def test_solve_poetry_redpacket_answer_from_new_quiz(self):
        text = "📜 诗词填空：\n《春晓》唐·孟浩然：__来风雨声，花落知多少。"
        answer = telegram_monitor._solve_poetry_redpacket_answer(text, ["巷", "夜", "江", "九"])
        self.assertEqual(answer, "夜")

    def test_solve_poetry_redpacket_answer_uses_question_bank(self):
        text = "📜 诗词填空：欲穷千里目，更上一层__。"

        def fake_phrase_exists(phrase):
            return phrase == "更上一层楼"

        with patch.object(telegram_monitor, "_poetry_phrase_exists", side_effect=fake_phrase_exists):
            answer = telegram_monitor._solve_poetry_redpacket_answer(text, ["天", "台", "楼", "云"])

        self.assertEqual(answer, "楼")

    def test_parse_poetry_quiz_button_options(self):
        msg = _DummyMsg(100, "诗词填空")
        msg.buttons = [
            [_DummyButton("A. 巷"), _DummyButton("B. 夜")],
            [_DummyButton("C. 江"), _DummyButton("D. 九")],
        ]
        options = telegram_monitor._parse_poetry_quiz_button_options(msg)
        self.assertEqual(options["夜"], (0, 1, "B. 夜"))

    def test_auto_click_buttons_waits_before_click(self):
        msg = _DummyMsg(100, "📜 诗词填空：\n《春晓》唐·孟浩然：__来风雨声，花落知多少。")
        msg.buttons = [
            [_DummyButton("A. 巷"), _DummyButton("B. 夜")],
            [_DummyButton("C. 江"), _DummyButton("D. 九")],
        ]
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        entry = {
            "auto_click_redpacket": True,
            "auto_click_delay_seconds": "0.5",
        }
        telegram_monitor.AUTO_CLICK_HISTORY.clear()
        with patch.object(telegram_monitor.asyncio, "sleep", side_effect=fake_sleep):
            clicked = asyncio.run(
                telegram_monitor._maybe_auto_click_buttons(
                    _DummyEvent(),
                    msg,
                    entry,
                    msg.message,
                )
            )

        self.assertTrue(clicked)
        self.assertEqual(sleeps, [0.5])
        self.assertEqual(len(msg.clicked), 1)

    def test_auto_click_risk_random_delay_is_added(self):
        msg = _DummyMsg(100, "📜 诗词填空：\n《春晓》唐·孟浩然：__来风雨声，花落知多少。")
        msg.buttons = [
            [_DummyButton("A. 巷"), _DummyButton("B. 夜")],
            [_DummyButton("C. 江"), _DummyButton("D. 九")],
        ]
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        entry = {
            "auto_click_redpacket": True,
            "auto_click_delay_seconds": "0.5",
            "auto_click_risk_control_enabled": True,
            "auto_click_random_delay_seconds": "0.4",
        }
        telegram_monitor.AUTO_CLICK_HISTORY.clear()
        telegram_monitor.AUTO_CLICK_RISK_HISTORY.clear()
        with patch.object(telegram_monitor.random, "uniform", return_value=0.25), \
             patch.object(telegram_monitor.asyncio, "sleep", side_effect=fake_sleep):
            clicked = asyncio.run(
                telegram_monitor._maybe_auto_click_buttons(
                    _DummyEvent(),
                    msg,
                    entry,
                    msg.message,
                )
            )

        self.assertTrue(clicked)
        self.assertEqual(sleeps, [0.75])
        self.assertEqual(len(msg.clicked), 1)

    def test_auto_click_risk_min_interval_blocks_second_click(self):
        msg1 = _DummyMsg(100, "📜 诗词填空：\n《春晓》唐·孟浩然：__来风雨声，花落知多少。")
        msg2 = _DummyMsg(101, msg1.message)
        buttons = [
            [_DummyButton("A. 巷"), _DummyButton("B. 夜")],
            [_DummyButton("C. 江"), _DummyButton("D. 九")],
        ]
        msg1.buttons = buttons
        msg2.buttons = buttons
        entry = {
            "auto_click_redpacket": True,
            "auto_click_risk_control_enabled": True,
            "auto_click_min_interval_seconds": "60",
        }
        telegram_monitor.AUTO_CLICK_HISTORY.clear()
        telegram_monitor.AUTO_CLICK_RISK_HISTORY.clear()

        first = asyncio.run(
            telegram_monitor._maybe_auto_click_buttons(_DummyEvent(), msg1, entry, msg1.message)
        )
        second = asyncio.run(
            telegram_monitor._maybe_auto_click_buttons(_DummyEvent(), msg2, entry, msg2.message)
        )

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(msg1.clicked), 1)
        self.assertEqual(len(msg2.clicked), 0)

    def test_auto_click_risk_hourly_limit_blocks_second_click(self):
        msg1 = _DummyMsg(100, "📜 诗词填空：\n《春晓》唐·孟浩然：__来风雨声，花落知多少。")
        msg2 = _DummyMsg(101, msg1.message)
        buttons = [
            [_DummyButton("A. 巷"), _DummyButton("B. 夜")],
            [_DummyButton("C. 江"), _DummyButton("D. 九")],
        ]
        msg1.buttons = buttons
        msg2.buttons = buttons
        entry = {
            "auto_click_redpacket": True,
            "auto_click_risk_control_enabled": True,
            "auto_click_hourly_limit": "1",
        }
        telegram_monitor.AUTO_CLICK_HISTORY.clear()
        telegram_monitor.AUTO_CLICK_RISK_HISTORY.clear()

        first = asyncio.run(
            telegram_monitor._maybe_auto_click_buttons(_DummyEvent(), msg1, entry, msg1.message)
        )
        second = asyncio.run(
            telegram_monitor._maybe_auto_click_buttons(_DummyEvent(), msg2, entry, msg2.message)
        )

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(msg1.clicked), 1)
        self.assertEqual(len(msg2.clicked), 0)

    def test_legacy_keyword_auto_click_is_ignored(self):
        msg = _DummyMsg(100, "发了一个红包")
        msg.buttons = [[_DummyButton("抢红包")]]
        dummy_client = _DummyClient([])
        entry = {
            "auto_click_keywords": ["发了一个红包"],
            "auto_click_button_texts": ["抢红包"],
            "auto_click_notify_targets": ["123456"],
        }
        telegram_monitor.AUTO_CLICK_HISTORY.clear()
        with patch.object(telegram_monitor, "client", dummy_client):
            clicked = asyncio.run(
                telegram_monitor._maybe_auto_click_buttons(
                    _DummyEvent(),
                    msg,
                    entry,
                    msg.message,
                )
            )

        self.assertFalse(clicked)
        self.assertEqual(dummy_client.sent_messages, [])

    def test_poetry_auto_click_sends_notify(self):
        msg = _DummyMsg(100, "📜 诗词填空：\n《春晓》唐·孟浩然：__来风雨声，花落知多少。")
        msg.buttons = [
            [_DummyButton("A. 巷"), _DummyButton("B. 夜")],
            [_DummyButton("C. 江"), _DummyButton("D. 九")],
        ]
        dummy_client = _DummyClient([])
        entry = {
            "auto_click_redpacket": True,
            "auto_click_notify_targets": ["123456"],
        }
        telegram_monitor.AUTO_CLICK_HISTORY.clear()
        with patch.object(telegram_monitor, "client", dummy_client):
            clicked = asyncio.run(
                telegram_monitor._maybe_auto_click_buttons(
                    _DummyEvent(),
                    msg,
                    entry,
                    msg.message,
                )
            )

        self.assertTrue(clicked)
        self.assertEqual(len(dummy_client.sent_messages), 1)
        target, payload, _ = dummy_client.sent_messages[0]
        self.assertEqual(target, "123456")
        self.assertIn("已自动点击诗词红包答案: 夜", payload)

    def test_extract_captcha_code_uses_vote(self):
        variants = [("raw", b"a"), ("gray", b"b")]
        engines = [("main", "E1"), ("old", "E2")]

        def fake_run(engine, image_bytes, *, png_fix):
            mapping = {
                ("E1", b"a", True): "xyp8",
                ("E1", b"b", False): "xyp8",
                ("E2", b"b", True): "xyp8",
                ("E2", b"a", False): "xyz9",
            }
            return mapping.get((engine, image_bytes, png_fix), "")

        with patch.object(telegram_monitor, "_get_captcha_ocr_engines", return_value=engines), \
             patch.object(telegram_monitor, "_build_captcha_image_variants", return_value=variants), \
             patch.object(telegram_monitor, "_run_captcha_ocr_once", side_effect=fake_run):
            code, summary = telegram_monitor._extract_captcha_code_from_image(b"raw-image")

        self.assertEqual(code, "xyp8")
        self.assertIn("picked=xyp8", summary)

    def test_extract_captcha_code_returns_no_candidate_summary(self):
        variants = [("raw", b"a")]
        engines = [("main", "E1")]

        with patch.object(telegram_monitor, "_get_captcha_ocr_engines", return_value=engines), \
             patch.object(telegram_monitor, "_build_captcha_image_variants", return_value=variants), \
             patch.object(telegram_monitor, "_run_captcha_ocr_once", return_value="***"):
            code, summary = telegram_monitor._extract_captcha_code_from_image(b"raw-image")

        self.assertEqual(code, "")
        self.assertIn("no_candidate", summary)

    def test_expand_confusion_candidates(self):
        expanded = telegram_monitor._expand_captcha_confusion_candidates(["HS8"], max_extra=20)
        self.assertIn("HS8", expanded)
        self.assertIn("NY8", expanded)

    def test_feedback_classifier(self):
        self.assertEqual(telegram_monitor._classify_captcha_reply_feedback("验证码错误，请重试"), "rejected")
        self.assertEqual(telegram_monitor._classify_captcha_reply_feedback("系统提示：口令不对"), "rejected")
        self.assertEqual(telegram_monitor._classify_captcha_reply_feedback("恭喜领取成功"), "accepted")
        self.assertEqual(telegram_monitor._classify_captcha_reply_feedback("zhou jun 的口令红包已抢完"), "closed")
        self.assertEqual(telegram_monitor._classify_captcha_reply_feedback("🎉 Mark 抢到了 2 积分!"), "accepted")
        self.assertEqual(telegram_monitor._classify_captcha_reply_feedback("处理中"), "unknown")

    def test_extract_captcha_code_from_text(self):
        self.assertEqual(telegram_monitor._extract_captcha_code_from_text("h5mso"), "h5mso")
        self.assertEqual(telegram_monitor._extract_captcha_code_from_text("验证码： h5 mso"), "h5mso")
        self.assertEqual(telegram_monitor._extract_captcha_code_from_text("@alice h5mso"), "h5mso")
        self.assertEqual(telegram_monitor._extract_captcha_code_from_text("无效"), "")

    def test_extract_redpacket_winner_name_tokens(self):
        text = (
            "📋 领取记录:\n"
            "- Alice: 20积分\n"
            "- 王五 : 10积分\n"
            "🎁 剩余 8 个"
        )
        winners = telegram_monitor._extract_redpacket_winner_name_tokens(text)
        self.assertIn("alice", winners)
        self.assertIn("王五", winners)

    def test_extract_inline_redpacket_success_name_tokens(self):
        text = "🎉 Mark 抢到了 2 积分!"
        winners = telegram_monitor._extract_inline_redpacket_success_name_tokens(text)
        self.assertIn("mark", winners)

    def test_auto_captcha_reply_slot(self):
        telegram_monitor.AUTO_CAPTCHA_REPLY_INFLIGHT.clear()
        self.assertTrue(telegram_monitor._try_acquire_auto_captcha_reply_slot(-100, 123))
        self.assertFalse(telegram_monitor._try_acquire_auto_captcha_reply_slot(-100, 123))
        telegram_monitor._release_auto_captcha_reply_slot(-100, 123)
        self.assertTrue(telegram_monitor._try_acquire_auto_captcha_reply_slot(-100, 123))

    def test_captcha_notify_dedup(self):
        telegram_monitor.AUTO_CAPTCHA_NOTIFY_HISTORY.clear()
        self.assertFalse(
            telegram_monitor._captcha_notify_recently(-100, 1, "wait_success", ttl_seconds=60)
        )
        telegram_monitor._mark_captcha_notify_sent(-100, 1, "wait_success")
        self.assertTrue(
            telegram_monitor._captcha_notify_recently(-100, 1, "wait_success", ttl_seconds=60)
        )

    def test_extract_reply_based_candidates_success_only(self):
        parent_msg_id = 100
        messages = [
            _DummyMsg(
                201,
                "h5mso",
                reply_to=_DummyReplyTo(parent_msg_id),
                sender=_DummySender(first_name="Alice"),
            ),
            _DummyMsg(
                202,
                "abcde",
                reply_to=_DummyReplyTo(parent_msg_id),
                sender=_DummySender(first_name="Bob"),
            ),
            _DummyMsg(203, "验证码错误，请重试", reply_to=_DummyReplyTo(202)),
        ]
        parent_text = "📋 领取记录:\n- Alice: 20积分\n🎁 剩余 8 个"
        dummy_client = _DummyClient(messages)

        with patch.object(telegram_monitor, "client", dummy_client):
            candidates, detail = asyncio.run(
                telegram_monitor._extract_reply_based_captcha_candidates(
                    -100,
                    parent_msg_id,
                    parent_message_text=parent_text,
                    success_only=True,
                )
            )

        self.assertEqual(candidates, ["h5mso"])
        self.assertEqual(detail.get("success_code_total"), 1)
        self.assertEqual(detail.get("rejected_code_total"), 1)

    def test_extract_reply_based_candidates_success_only_from_inline_success_text(self):
        parent_msg_id = 100
        messages = [
            _DummyMsg(
                301,
                "lz56ik",
                reply_to=_DummyReplyTo(parent_msg_id),
                sender=_DummySender(first_name="Mark"),
            ),
            _DummyMsg(
                302,
                "abcde",
                reply_to=_DummyReplyTo(parent_msg_id),
                sender=_DummySender(first_name="Bob"),
            ),
            _DummyMsg(303, "🎉 Mark 抢到了 2 积分!"),
        ]
        dummy_client = _DummyClient(messages)

        with patch.object(telegram_monitor, "client", dummy_client):
            candidates, detail = asyncio.run(
                telegram_monitor._extract_reply_based_captcha_candidates(
                    -100,
                    parent_msg_id,
                    parent_message_text="回复本消息发送验证码",
                    success_only=True,
                )
            )

        self.assertEqual(candidates, ["lz56ik"])
        self.assertEqual(detail.get("accepted_code_total"), 1)

    def test_extract_reply_based_candidates_success_only_fallback_non_rejected(self):
        parent_msg_id = 100
        messages = [
            _DummyMsg(
                401,
                "h5mso",
                reply_to=_DummyReplyTo(parent_msg_id),
                sender=_DummySender(first_name="Alice"),
            ),
            _DummyMsg(
                402,
                "abcde",
                reply_to=_DummyReplyTo(parent_msg_id),
                sender=_DummySender(first_name="Bob"),
            ),
            _DummyMsg(
                403,
                "系统提示：口令不对，请重新输入",
                reply_to=_DummyReplyTo(402),
            ),
            _DummyMsg(
                404,
                "h5mso",
                reply_to=_DummyReplyTo(parent_msg_id),
                sender=_DummySender(first_name="Carol"),
            ),
        ]
        dummy_client = _DummyClient(messages)

        with patch.object(telegram_monitor, "client", dummy_client):
            candidates, detail = asyncio.run(
                telegram_monitor._extract_reply_based_captcha_candidates(
                    -100,
                    parent_msg_id,
                    parent_message_text="回复本消息发送验证码",
                    success_only=True,
                )
            )

        self.assertEqual(candidates, ["h5mso"])
        self.assertEqual(detail.get("success_code_total"), 0)
        self.assertEqual(detail.get("rejected_code_total"), 1)
        self.assertTrue(detail.get("fallback_non_rejected"))

    def test_wait_reply_based_candidates_polls_until_available(self):
        calls = []

        async def fake_extract(*args, **kwargs):
            calls.append(kwargs)
            detail = {"reply_total": len(calls)}
            if len(calls) < 2:
                return [], detail
            return ["h5mso"], detail

        async def fake_sleep(_seconds):
            return None

        with patch.object(telegram_monitor, "_extract_reply_based_captcha_candidates", side_effect=fake_extract), \
             patch.object(telegram_monitor.asyncio, "sleep", side_effect=fake_sleep):
            candidates, detail = asyncio.run(
                telegram_monitor._wait_reply_based_captcha_candidates(
                    -100,
                    100,
                    parent_message_text="回复本消息发送验证码",
                    success_only=True,
                    timeout_seconds=1.0,
                    poll_seconds=0.01,
                )
            )

        self.assertEqual(candidates, ["h5mso"])
        self.assertEqual(len(calls), 2)
        self.assertEqual(detail.get("reply_total"), 2)

    def test_legacy_auto_reply_config_is_ignored(self):
        msg = _DummyMsg(100, "回复本消息并发送图中完整验证码即可抢红包")
        msg.photo = object()
        rules = {
            "auto_click_captcha_reply": True,
            "auto_click_captcha_reply_from_success_only": True,
            "auto_click_captcha_keywords": ["回复本消息", "验证码"],
        }

        async def fake_extract(*args, **kwargs):
            return [], {"reply_total": 0}

        with patch.object(telegram_monitor, "_extract_reply_based_captcha_candidates", side_effect=fake_extract) as extract_mock, \
             patch.object(telegram_monitor, "_wait_reply_based_captcha_candidates") as wait_mock, \
             patch.object(telegram_monitor, "_send_captcha_manual_notify") as notify_mock:
            replied = asyncio.run(
                telegram_monitor._maybe_auto_reply_redpacket_captcha(
                    _DummyEvent(),
                    msg,
                    rules,
                    msg.message,
                    source="new_message",
                )
            )

        self.assertFalse(replied)
        extract_mock.assert_not_called()
        wait_mock.assert_not_called()
        notify_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
