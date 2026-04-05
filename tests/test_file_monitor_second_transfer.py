import os
import tempfile
import unittest
from unittest.mock import patch

import file_monitor


class _ExceptionClient:
    def check_file_exists(self, *args, **kwargs):
        raise RuntimeError("115 temporary failure")


class _HitClient:
    def __init__(self, check_result, copy_result=None):
        self.check_result = check_result
        self.copy_result = copy_result or {}
        self.copy_calls = []

    def check_file_exists(self, *args, **kwargs):
        return dict(self.check_result)

    def copy_file_to_folder(self, **kwargs):
        self.copy_calls.append(kwargs)
        return dict(self.copy_result)


class FileMonitorSecondTransferTestCase(unittest.TestCase):
    def _make_file(self, directory: str, name: str, content: bytes) -> str:
        path = os.path.join(directory, name)
        with open(path, "wb") as f:
            f.write(content)
        return path

    def test_mid_copy_check_exception_falls_back_to_local_copy(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as dst_dir:
            content = b"abcdef123456"
            src = self._make_file(src_dir, "sample.bin", content)
            sha1 = file_monitor.compute_sha1(src)

            with patch.object(file_monitor, "log_message"):
                ok = file_monitor.perform_local_action(
                    src,
                    "sample.bin",
                    dst_dir,
                    "copy",
                    "rename",
                    enable_mid_copy_check=True,
                    client_115=_ExceptionClient(),
                    file_sha1=sha1,
                    file_size=len(content),
                    target="U_1_123",
                    check_interval=0,
                    chunk_size=1,
                    delete_source_after_transfer=False,
                )

            self.assertTrue(ok)
            self.assertTrue(os.path.exists(src))
            copied = os.path.join(dst_dir, "sample.bin")
            self.assertTrue(os.path.exists(copied))
            with open(copied, "rb") as f:
                self.assertEqual(f.read(), content)

    def test_mid_copy_hit_copy_and_delete_uses_115_completion_before_deleting_source(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as dst_dir:
            content = b"abcdefghij"
            src = self._make_file(src_dir, "episode.mkv", content)
            sha1 = file_monitor.compute_sha1(src)
            client = _HitClient(
                check_result={
                    "success": True,
                    "can_transfer": True,
                    "already_exists": True,
                    "transferred": False,
                    "file_id": "fid-123",
                },
                copy_result={
                    "success": True,
                    "transferred": True,
                },
            )

            with patch.object(file_monitor, "log_message"):
                ok = file_monitor.perform_local_action(
                    src,
                    "episode.mkv",
                    dst_dir,
                    "copy_and_delete",
                    "rename",
                    enable_mid_copy_check=True,
                    client_115=client,
                    file_sha1=sha1,
                    file_size=len(content),
                    target="U_1_999",
                    check_interval=0,
                    chunk_size=1,
                    delete_source_after_transfer=False,
                )

            self.assertTrue(ok)
            self.assertFalse(os.path.exists(src))
            self.assertEqual(len(client.copy_calls), 1)
            self.assertEqual(client.copy_calls[0]["target_cid"], "999")
            self.assertEqual(client.copy_calls[0]["file_name"], "episode.mkv")

    def test_mid_copy_hit_with_115_completion_failure_falls_back_to_local_copy(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as dst_dir:
            content = b"abcdefghij1234567890"
            src = self._make_file(src_dir, "movie.mp4", content)
            sha1 = file_monitor.compute_sha1(src)
            client = _HitClient(
                check_result={
                    "success": True,
                    "can_transfer": True,
                    "already_exists": True,
                    "transferred": False,
                    "file_id": "fid-456",
                },
                copy_result={
                    "success": False,
                    "message": "copy failed",
                },
            )

            with patch.object(file_monitor, "log_message"):
                ok = file_monitor.perform_local_action(
                    src,
                    "movie.mp4",
                    dst_dir,
                    "copy",
                    "rename",
                    enable_mid_copy_check=True,
                    client_115=client,
                    file_sha1=sha1,
                    file_size=len(content),
                    target="U_1_321",
                    check_interval=0,
                    chunk_size=1,
                    delete_source_after_transfer=False,
                )

            self.assertTrue(ok)
            self.assertTrue(os.path.exists(src))
            copied = os.path.join(dst_dir, "movie.mp4")
            self.assertTrue(os.path.exists(copied))
            with open(copied, "rb") as f:
                self.assertEqual(f.read(), content)
            self.assertEqual(len(client.copy_calls), 1)


if __name__ == "__main__":
    unittest.main()
