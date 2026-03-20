import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import telegram_monitor


class _FakeClient:
    def __init__(self, *, connected=False, authorized=True, run_error=None):
        self.connected = connected
        self.authorized = authorized
        self.run_error = run_error
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.run_calls = 0

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connect_calls += 1
        self.connected = True

    async def is_user_authorized(self):
        return self.authorized

    async def disconnect(self):
        self.disconnect_calls += 1
        self.connected = False

    async def run_until_disconnected(self):
        self.run_calls += 1
        if self.run_error is not None:
            self.connected = False
            raise self.run_error


class TelegramMonitorReconnectTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_client = telegram_monitor.client
        self.original_config = telegram_monitor.current_config
        self.original_lock = telegram_monitor._CLIENT_CONNECT_LOCK

    def tearDown(self):
        telegram_monitor.client = self.original_client
        telegram_monitor.current_config = self.original_config
        telegram_monitor._CLIENT_CONNECT_LOCK = self.original_lock

    async def test_ensure_client_connected_reuses_existing_disconnected_client(self):
        fake_client = _FakeClient(connected=False, authorized=True)
        telegram_monitor.client = fake_client
        telegram_monitor.current_config = {
            "telegram": {"session_name": "telegram_monitor"},
            "proxy": {},
        }
        telegram_monitor._CLIENT_CONNECT_LOCK = None

        with patch.dict(
            "os.environ",
            {"TELEGRAM_API_ID": "12345", "TELEGRAM_API_HASH": "hash-value"},
            clear=False,
        ), patch.object(
            telegram_monitor,
            "TelegramClient",
            side_effect=AssertionError("should reuse existing client"),
        ):
            ok = await telegram_monitor.ensure_client_connected()

        self.assertTrue(ok)
        self.assertIs(telegram_monitor.client, fake_client)
        self.assertEqual(fake_client.connect_calls, 1)
        self.assertEqual(fake_client.disconnect_calls, 0)

    async def test_keep_client_connected_handles_run_until_disconnected_error_and_retries(self):
        fake_client = _FakeClient(
            connected=True,
            authorized=True,
            run_error=RuntimeError("socket closed"),
        )
        telegram_monitor.client = fake_client

        async def fake_sleep(_seconds):
            raise asyncio.CancelledError()

        with patch.object(
            telegram_monitor,
            "ensure_client_connected",
            AsyncMock(return_value=True),
        ), patch.object(
            telegram_monitor,
            "_register_event_handlers",
            lambda: None,
        ), patch.object(
            telegram_monitor.asyncio,
            "sleep",
            fake_sleep,
        ), patch.object(
            telegram_monitor.traceback,
            "print_exc",
            lambda: None,
        ):
            with self.assertRaises(asyncio.CancelledError):
                await telegram_monitor.keep_client_connected()

        self.assertEqual(fake_client.run_calls, 1)


if __name__ == "__main__":
    unittest.main()
