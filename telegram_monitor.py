import os
import json
import asyncio
import time
import traceback
import sys
import sqlite3
import re
import shutil
from collections import deque
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict
import html
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from telethon.sync import TelegramClient
from telethon import events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage
from telethon.errors import FloodWaitError
from dotenv import load_dotenv

# Add app directory to path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Paths & Config ---
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
if not os.path.exists(CONFIG_DIR):
    CONFIG_DIR = os.path.abspath('config')

CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')
MESSAGE_QUEUE_FILE = os.path.join(CONFIG_DIR, 'message_queue.json')
DOWNLOAD_RISK_STATS_FILE = os.path.join(CONFIG_DIR, 'download_risk_stats.json')

# Load environment variables (optional)
try:
    load_dotenv(os.path.join(CONFIG_DIR, '.env'))
except Exception:
    pass


DEBUG_MODE = False


def log_message(message: str, level: str = "INFO"):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    if level == "DEBUG" and not DEBUG_MODE:
        return
    print(f"[{ts}] [{level}] {message}", flush=True)


def debug_log(message: str):
    log_message(message, level="DEBUG")


def trace_log(message: str):
    enabled = False
    try:
        enabled = bool(current_config.get('trace_media_detection', False))
    except Exception:
        enabled = False
    if enabled:
        log_message(message, level="INFO")


def get_extension_from_mime(mime_type: str) -> str:
    """根据MIME类型返回正确的文件扩展名"""
    mime_map = {
        'video/mp4': '.mp4',
        'video/webm': '.webm',
        'video/x-matroska': '.mkv',
        'video/quicktime': '.mov',
        'video/x-msvideo': '.avi',
        'video/x-flv': '.flv',
        'video/mpeg': '.mpeg',
        'audio/mpeg': '.mp3',
        'audio/ogg': '.ogg',
        'audio/wav': '.wav',
        'audio/x-m4a': '.m4a',
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'image/gif': '.gif',
        'image/webp': '.webp',
    }
    
    if mime_type in mime_map:
        return mime_map[mime_type]
    
    # 降级方案：从MIME类型中提取扩展名
    if '/' in mime_type:
        ext = mime_type.split('/')[-1]
        # 处理特殊格式
        if ext.startswith('x-'):
            ext = ext[2:]
        return f'.{ext}'
    
    return ''


def _extract_first_url_from_text(text: str) -> str:
    if not text:
        return ''
    m = REAL_URL_RE.search(text)
    return (m.group(0).strip() if m else '')


def _extract_video_resolution_text(msg) -> str:
    document = _get_primary_document(msg)

    def _fmt(w, h):
        try:
            iw = int(w)
            ih = int(h)
            if iw > 0 and ih > 0:
                return f"{iw}x{ih}"
        except Exception:
            pass
        return ''

    if getattr(msg, 'video', None):
        w = getattr(msg.video, 'w', None) or getattr(msg.video, 'width', None)
        h = getattr(msg.video, 'h', None) or getattr(msg.video, 'height', None)
        txt = _fmt(w, h)
        if txt:
            return txt

    if document:
        for attr in getattr(document, 'attributes', []) or []:
            if attr.__class__.__name__ == 'DocumentAttributeVideo':
                txt = _fmt(getattr(attr, 'w', None), getattr(attr, 'h', None))
                if txt:
                    return txt

    return ''


def _extract_title_for_download(msg, original_filename: str) -> str:
    title = (getattr(msg, 'message', None) or '').strip()
    if title:
        title = re.sub(r'\s+', ' ', title)
        return title[:120]

    if original_filename:
        return os.path.splitext(os.path.basename(original_filename))[0][:120]

    return ''


def create_progress_callback(file_path: str, media_type: str):
    """创建下载进度回调函数，用于大文件下载 - 单行更新进度"""
    last_log_time = [0]
    last_log_percent = [0]
    
    def progress_callback(current, total):
        # 每1秒或每5%更新一次进度（Web UI 会自动替换同一行）
        current_time = time.time()
        percent = (current / total * 100) if total > 0 else 0
        
        time_threshold = current_time - last_log_time[0] >= 1
        percent_threshold = percent - last_log_percent[0] >= 5
        is_complete = percent >= 100
        
        if time_threshold or percent_threshold or is_complete:
            size_mb = current / (1024 * 1024)
            total_mb = total / (1024 * 1024)
            
            # ANSI 颜色代码：青色醒目进度
            cyan = "\033[96m"
            green = "\033[92m"
            reset = "\033[0m"
            
            if is_complete:
                # 完成时用绿色
                log_message(f"{green}下载完成 [{media_type}]: {size_mb:.1f}MB / {total_mb:.1f}MB (100%){reset}")
            else:
                # 进行中用青色
                log_message(f"{cyan}下载进度 [{media_type}]: {size_mb:.1f}MB / {total_mb:.1f}MB ({percent:.1f}%){reset}")
            
            last_log_time[0] = current_time
            last_log_percent[0] = percent
    
    return progress_callback


def load_config() -> dict:
    default_config = {
        "telegram": {
            "api_id": None,
            "api_hash": "",
            "session_name": "telegram_monitor",
        },
        "restricted_channels": [],
        "proxy": {},
        "debug_mode": False,
        "trace_media_detection": False,
        "hdhive_cookie": "",
        "hdhive_auto_unlock_points_threshold": 0,
        "download_risk_control": {
            "enabled": True,
            "per_channel_max_downloads_per_minute": 6,
            "duplicate_cooldown_seconds": 300,
            "max_single_file_size_mb": 4096,
            "min_free_space_gb": 5,
            "download_timeout_dynamic_enabled": True,
            "download_timeout_base_seconds": 1800,
            "download_timeout_max_seconds": 10800,
            "download_timeout_buffer_seconds": 300,
            "download_timeout_min_speed_mb_s": 1.0,
        },
    }

    if not os.path.exists(CONFIG_FILE):
        return default_config
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
        if not isinstance(config, dict):
            return default_config
        if 'telegram' not in config:
            config['telegram'] = default_config['telegram']
        if 'restricted_channels' not in config:
            config['restricted_channels'] = []
        if 'proxy' not in config:
            config['proxy'] = {}
        if 'trace_media_detection' not in config:
            config['trace_media_detection'] = False
        if 'debug_mode' not in config:
            config['debug_mode'] = False
        if 'hdhive_cookie' not in config:
            config['hdhive_cookie'] = ''
        if 'hdhive_auto_unlock_points_threshold' not in config:
            config['hdhive_auto_unlock_points_threshold'] = 0
        if 'download_risk_control' not in config or not isinstance(config.get('download_risk_control'), dict):
            config['download_risk_control'] = default_config['download_risk_control']
        else:
            merged_risk = default_config['download_risk_control'].copy()
            merged_risk.update(config.get('download_risk_control') or {})
            config['download_risk_control'] = merged_risk
        return config
    except Exception:
        return default_config


def save_config(config: dict):
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


# --- Regex & Types ---
REAL_URL_RE = re.compile(r"https?://[^\s\"<>']+", re.IGNORECASE)
HDHIVE_115_URL_RE = re.compile(
    r"https?://(?:www\.)?hdhive\.com/resource/115/[0-9a-fA-F]{32}",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class HdhiveLinkHit:
    hdhive_url: str
    display_text: str
    source: str


def _extract_hdhive_slug(hdhive_url: str) -> Optional[str]:
    if not hdhive_url:
        return None
    m = re.search(r"/resource/115/([0-9a-fA-F]{32})", hdhive_url)
    if m:
        return m.group(1)
    # Best-effort fallback
    tail = hdhive_url.rstrip('/').split('/')[-1]
    tail = tail.split('?', 1)[0].split('#', 1)[0]
    return tail or None


# --- HDHive Server Actions ---
# Default action IDs discovered from public Next.js chunks (may change with deployments)
HDHIVE_ACTION_DECRYPT_ID_DEFAULT = "40c9c3d9fd41a3ddb01539b93b112ebf0dd6e5f98f"
HDHIVE_ACTION_ENCRYPTE_ID_DEFAULT = "4009ae744a7d94ccc9b0f0ff4e3f5bc55d39a111ad"

HDHIVE_ACTION_DECRYPT_ID = HDHIVE_ACTION_DECRYPT_ID_DEFAULT
HDHIVE_ACTION_ENCRYPTE_ID = HDHIVE_ACTION_ENCRYPTE_ID_DEFAULT

_HDHIVE_ACTION_IDS_LAST_REFRESH_TS = 0.0
_HDHIVE_ACTION_IDS_REFRESH_TTL_SECONDS = 6 * 60 * 60


def _refresh_hdhive_action_ids_if_needed():
    global HDHIVE_ACTION_DECRYPT_ID, HDHIVE_ACTION_ENCRYPTE_ID, _HDHIVE_ACTION_IDS_LAST_REFRESH_TS

    now = time.time()
    if _HDHIVE_ACTION_IDS_LAST_REFRESH_TS and (now - _HDHIVE_ACTION_IDS_LAST_REFRESH_TS) < _HDHIVE_ACTION_IDS_REFRESH_TTL_SECONDS:
        return

    _HDHIVE_ACTION_IDS_LAST_REFRESH_TS = now
    try:
        html_text = requests.get(
            "https://hdhive.com/",
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        ).text
        chunk_paths = sorted(set(re.findall(r"(/_next/static/[^\"<> ]+\\.js)", html_text)))
        if not chunk_paths:
            return

        decrypt_id = None
        encrypte_id = None
        pat_decrypt = re.compile(r"createServerReference\(\"([0-9a-f]{40,})\"[^\)]*\"decrypt\"\)")
        pat_encrypte = re.compile(r"createServerReference\(\"([0-9a-f]{40,})\"[^\)]*\"encrypte\"\)")

        for p in chunk_paths[:120]:
            js_text = requests.get(
                f"https://hdhive.com{p}",
                timeout=15,
                headers={"User-Agent": "Mozilla/5.0"},
            ).text
            if decrypt_id is None:
                m = pat_decrypt.search(js_text)
                if m:
                    decrypt_id = m.group(1)
            if encrypte_id is None:
                m = pat_encrypte.search(js_text)
                if m:
                    encrypte_id = m.group(1)
            if decrypt_id and encrypte_id:
                break

        if decrypt_id:
            HDHIVE_ACTION_DECRYPT_ID = decrypt_id
        if encrypte_id:
            HDHIVE_ACTION_ENCRYPTE_ID = encrypte_id
    except Exception:
        # Keep defaults on any failure
        return


_HDHIVE_NEXT_ROUTER_STATE_TREE_JSON = "[]"


_hdhive_resolve_cache: Dict[str, Tuple[float, Optional[str]]] = {}
_HDHIVE_CACHE_TTL_SECONDS = 6 * 60 * 60
_HDHIVE_NEGATIVE_CACHE_TTL_SECONDS = 5 * 60
_album_folder_cache: Dict[int, str] = {}

_download_attempt_timestamps: Dict[int, deque] = {}
_download_dedup_cache: Dict[str, float] = {}
_download_risk_stats = {
    'blocked_total': 0,
    'reasons': {},
    'last_blocked_reason': '',
    'last_blocked_at': '',
}


def _persist_download_risk_stats():
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(DOWNLOAD_RISK_STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(_download_risk_stats, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _record_download_risk_block(reason: str):
    reason_key = (reason or 'unknown').strip()
    _download_risk_stats['blocked_total'] = int(_download_risk_stats.get('blocked_total', 0)) + 1
    reasons = _download_risk_stats.setdefault('reasons', {})
    reasons[reason_key] = int(reasons.get(reason_key, 0)) + 1
    _download_risk_stats['last_blocked_reason'] = reason_key
    _download_risk_stats['last_blocked_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
    _persist_download_risk_stats()


def _get_download_risk_control_config() -> Dict[str, float]:
    risk_cfg = (current_config or {}).get('download_risk_control', {}) if isinstance(current_config, dict) else {}
    if not isinstance(risk_cfg, dict):
        risk_cfg = {}

    enabled = bool(risk_cfg.get('enabled', True))
    per_channel_max = int(risk_cfg.get('per_channel_max_downloads_per_minute', 6) or 0)
    duplicate_cooldown = int(risk_cfg.get('duplicate_cooldown_seconds', 300) or 0)
    max_single_file_size_mb = int(risk_cfg.get('max_single_file_size_mb', 4096) or 0)
    min_free_space_gb = float(risk_cfg.get('min_free_space_gb', 5) or 0)
    timeout_dynamic_enabled = bool(risk_cfg.get('download_timeout_dynamic_enabled', True))
    timeout_base_seconds = int(risk_cfg.get('download_timeout_base_seconds', 1800) or 0)
    timeout_max_seconds = int(risk_cfg.get('download_timeout_max_seconds', 10800) or 0)
    timeout_buffer_seconds = int(risk_cfg.get('download_timeout_buffer_seconds', 300) or 0)
    timeout_min_speed_mb_s = float(risk_cfg.get('download_timeout_min_speed_mb_s', 1.0) or 0)

    return {
        'enabled': enabled,
        'per_channel_max_downloads_per_minute': max(0, per_channel_max),
        'duplicate_cooldown_seconds': max(0, duplicate_cooldown),
        'max_single_file_size_mb': max(0, max_single_file_size_mb),
        'min_free_space_gb': max(0.0, min_free_space_gb),
        'download_timeout_dynamic_enabled': timeout_dynamic_enabled,
        'download_timeout_base_seconds': max(60, timeout_base_seconds),
        'download_timeout_max_seconds': max(60, timeout_max_seconds),
        'download_timeout_buffer_seconds': max(0, timeout_buffer_seconds),
        'download_timeout_min_speed_mb_s': max(0.1, timeout_min_speed_mb_s),
    }


def _compute_download_timeout_seconds(expected_size: int) -> int:
    cfg = _get_download_risk_control_config()
    base_timeout = int(cfg.get('download_timeout_base_seconds', 1800) or 1800)
    max_timeout = int(cfg.get('download_timeout_max_seconds', 10800) or 10800)
    buffer_seconds = int(cfg.get('download_timeout_buffer_seconds', 300) or 300)
    min_speed_mb_s = float(cfg.get('download_timeout_min_speed_mb_s', 1.0) or 1.0)
    dynamic_enabled = bool(cfg.get('download_timeout_dynamic_enabled', True))

    if max_timeout < base_timeout:
        max_timeout = base_timeout

    if (not dynamic_enabled) or expected_size <= 0:
        return max(60, min(base_timeout, max_timeout))

    expected_seconds = int((expected_size / (1024 * 1024)) / min_speed_mb_s) + buffer_seconds
    timeout = max(base_timeout, expected_seconds)
    timeout = min(timeout, max_timeout)
    return max(60, timeout)


def _build_download_fingerprint(msg, restricted_channel_id: int) -> str:
    document = getattr(msg, 'document', None)
    if document and getattr(document, 'id', None):
        return f"ch:{restricted_channel_id}:doc:{document.id}:{getattr(document, 'size', 0)}"

    video = getattr(msg, 'video', None)
    if video and getattr(video, 'id', None):
        return f"ch:{restricted_channel_id}:video:{video.id}:{getattr(video, 'size', 0)}"

    photo = getattr(msg, 'photo', None)
    if photo and getattr(photo, 'id', None):
        return f"ch:{restricted_channel_id}:photo:{photo.id}"

    grouped_id = getattr(msg, 'grouped_id', None)
    if grouped_id:
        return f"ch:{restricted_channel_id}:group:{grouped_id}:msg:{msg.id}"

    return f"ch:{restricted_channel_id}:msg:{msg.id}"


def _cleanup_download_risk_state(now_ts: float, window_seconds: int, dedup_cooldown_seconds: int):
    for channel_id in list(_download_attempt_timestamps.keys()):
        ts_queue = _download_attempt_timestamps.get(channel_id)
        if not ts_queue:
            _download_attempt_timestamps.pop(channel_id, None)
            continue
        while ts_queue and (now_ts - ts_queue[0]) > window_seconds:
            ts_queue.popleft()
        if not ts_queue:
            _download_attempt_timestamps.pop(channel_id, None)

    if dedup_cooldown_seconds <= 0:
        return
    expire_before = now_ts - dedup_cooldown_seconds
    for fp in list(_download_dedup_cache.keys()):
        if _download_dedup_cache.get(fp, 0) < expire_before:
            _download_dedup_cache.pop(fp, None)


def _check_download_risk_controls(*, restricted_channel_id: int, download_directory: str, msg, expected_size: int) -> Tuple[bool, str, int]:
    cfg = _get_download_risk_control_config()
    if not cfg.get('enabled'):
        return True, '', 0

    now_ts = time.time()
    window_seconds = 60
    per_channel_max = int(cfg.get('per_channel_max_downloads_per_minute', 0) or 0)
    dedup_cooldown_seconds = int(cfg.get('duplicate_cooldown_seconds', 0) or 0)
    max_single_file_size_mb = int(cfg.get('max_single_file_size_mb', 0) or 0)
    min_free_space_gb = float(cfg.get('min_free_space_gb', 0) or 0)

    _cleanup_download_risk_state(now_ts, window_seconds, dedup_cooldown_seconds)

    def blocked(reason: str, retry_after: int = 0) -> Tuple[bool, str, int]:
        _record_download_risk_block(reason)
        return False, reason, max(0, int(retry_after or 0))

    if max_single_file_size_mb > 0 and expected_size > 0:
        max_size_bytes = max_single_file_size_mb * 1024 * 1024
        if expected_size > max_size_bytes:
            return blocked(f"文件体积超限: {expected_size / (1024 * 1024):.1f}MB > {max_single_file_size_mb}MB")

    if min_free_space_gb > 0 and download_directory:
        try:
            _, _, free_bytes = shutil.disk_usage(download_directory)
            min_free_bytes = int(min_free_space_gb * 1024 * 1024 * 1024)
            if free_bytes < min_free_bytes:
                return blocked(
                    f"磁盘剩余空间不足: {free_bytes / (1024 ** 3):.2f}GB < {min_free_space_gb:.2f}GB"
                )
        except Exception as e:
            log_message(f"下载风控: 磁盘空间检查失败，继续下载。原因: {e}", level="DEBUG")

    if per_channel_max > 0:
        channel_queue = _download_attempt_timestamps.setdefault(restricted_channel_id, deque())
        while channel_queue and (now_ts - channel_queue[0]) > window_seconds:
            channel_queue.popleft()
        if len(channel_queue) >= per_channel_max:
            oldest_ts = channel_queue[0] if channel_queue else now_ts
            retry_after = max(1, int(window_seconds - (now_ts - oldest_ts)) + 1)
            return blocked(f"频道每分钟下载上限触发: {len(channel_queue)}/{per_channel_max}", retry_after=retry_after)

    if dedup_cooldown_seconds > 0:
        fingerprint = _build_download_fingerprint(msg, restricted_channel_id)
        last_ts = _download_dedup_cache.get(fingerprint)
        if last_ts and (now_ts - last_ts) < dedup_cooldown_seconds:
            remaining = max(1, int(dedup_cooldown_seconds - (now_ts - last_ts)) + 1)
            return blocked(f"重复下载冷却中，剩余 {remaining}s", retry_after=remaining)
        _download_dedup_cache[fingerprint] = now_ts

    if per_channel_max > 0:
        _download_attempt_timestamps[restricted_channel_id].append(now_ts)

    return True, '', 0


def _resolve_non_conflicting_path(file_path: str, msg_id: int) -> str:
    """避免同名覆盖：若目标文件已存在，附加消息ID和序号。"""
    if not os.path.exists(file_path):
        return file_path

    directory, filename = os.path.split(file_path)
    base, ext = os.path.splitext(filename)

    candidate = os.path.join(directory, f"{base}_msg{msg_id}{ext}")
    if not os.path.exists(candidate):
        return candidate

    index = 1
    while True:
        candidate = os.path.join(directory, f"{base}_msg{msg_id}_{index}{ext}")
        if not os.path.exists(candidate):
            return candidate
        index += 1


def _extract_document_filename(msg) -> str:
    document = _get_primary_document(msg)
    if not document:
        return ''
    try:
        for attr in getattr(document, 'attributes', []) or []:
            file_name = getattr(attr, 'file_name', None)
            if file_name:
                return str(file_name)
    except Exception:
        pass
    return ''


def _get_primary_document(msg):
    document = getattr(msg, 'document', None)
    if document:
        return document

    media = getattr(msg, 'media', None)
    if isinstance(media, MessageMediaDocument):
        return getattr(media, 'document', None)

    if isinstance(media, MessageMediaWebPage):
        webpage = getattr(media, 'webpage', None)
        return getattr(webpage, 'document', None)

    return None


def _is_video_like_message(msg) -> bool:
    if getattr(msg, 'video', None) or getattr(msg, 'video_note', None) or getattr(msg, 'gif', None):
        return True

    document = _get_primary_document(msg)

    if not document:
        return False

    mime_type = (getattr(document, 'mime_type', '') or '').lower()
    if mime_type.startswith('video/'):
        return True

    try:
        for attr in getattr(document, 'attributes', []) or []:
            if attr.__class__.__name__ == 'DocumentAttributeVideo':
                return True
            if attr.__class__.__name__ == 'DocumentAttributeAnimated':
                return True
    except Exception:
        pass

    filename = _extract_document_filename(msg).lower()
    if filename:
        video_exts = ('.mp4', '.mkv', '.mov', '.webm', '.avi', '.flv', '.mpeg', '.mpg', '.m4v', '.3gp', '.gif')
        if filename.endswith(video_exts):
            return True

    if mime_type in ('image/gif', 'application/x-mpegurl'):
        return True

    return False


def _build_media_trace(msg) -> str:
    """构建可读的媒体追踪信息，便于定位漏检。"""
    try:
        media = getattr(msg, 'media', None)
        media_cls = media.__class__.__name__ if media else 'None'
        document = _get_primary_document(msg)
        mime_type = (getattr(document, 'mime_type', '') or '') if document else ''
        attributes = []
        if document and getattr(document, 'attributes', None):
            attributes = [a.__class__.__name__ for a in document.attributes]
        file_name = _extract_document_filename(msg)

        return (
            f"media={media_cls}, mime={mime_type or '-'}, attrs={attributes or []}, "
            f"file={file_name or '-'}, flags=(video={bool(getattr(msg, 'video', None))},"
            f" video_note={bool(getattr(msg, 'video_note', None))}, gif={bool(getattr(msg, 'gif', None))},"
            f" photo={bool(getattr(msg, 'photo', None))}, audio={bool(getattr(msg, 'audio', None) or getattr(msg, 'voice', None))})"
        )
    except Exception as e:
        return f"trace_build_failed={e}"


def _get_hdhive_cookie_header() -> str:
    try:
        cookie = (current_config or {}).get('hdhive_cookie', '')  # type: ignore[name-defined]
        return (cookie or '').strip()
    except Exception:
        return ''


def _parse_next_action_rsc_result(text: str):
    """Parse Next.js Server Action (text/x-component) response.

    We care about the line like:
      1:{...}
    or:
      1:"..."
    """
    if not text:
        return None
    for line in text.splitlines():
        if not line:
            continue
        if line.startswith('1:'):
            payload = line[2:]
            try:
                return json.loads(payload)
            except Exception:
                return payload
    return None


def _hdhive_next_action_call_sync(
    *,
    cookie_header: str,
    action_id: str,
    page_path: str,
    router_state_tree_json: str,
    action_args,
):
    """Call a Next.js Server Action used by HDHive.

    Important: action_args must be passed as JSON array string (encodeReply output for simple values).
    We verified `decrypt` works with body: json.dumps([ciphertext]).
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Cookie': cookie_header,
        'Accept': 'text/x-component',
        'next-action': action_id,
        'next-router-state-tree': router_state_tree_json,
        'next-url': page_path,
        'Content-Type': 'text/plain;charset=UTF-8',
    }

    # Next.js adds a cache-busting param `_rsc`; value doesn't matter.
    rsc_url = f"https://hdhive.com/?_rsc={int(time.time() * 1000)}"
    body = json.dumps(action_args, ensure_ascii=False)
    resp = requests.post(rsc_url, data=body.encode('utf-8'), timeout=25, headers=headers)
    return _parse_next_action_rsc_result(resp.text)


def _hdhive_decrypt_sync(cookie_header: str, slug: str, ciphertext: str):
    _refresh_hdhive_action_ids_if_needed()
    page_path = f"/resource/115/{slug}"
    router_state_tree_json = _HDHIVE_NEXT_ROUTER_STATE_TREE_JSON
    return _hdhive_next_action_call_sync(
        cookie_header=cookie_header,
        action_id=HDHIVE_ACTION_DECRYPT_ID,
        page_path=page_path,
        router_state_tree_json=router_state_tree_json,
        action_args=[ciphertext],
    )


def _hdhive_encrypte_sync(cookie_header: str, slug: str, plaintext_json: str):
    _refresh_hdhive_action_ids_if_needed()
    page_path = f"/resource/115/{slug}"
    router_state_tree_json = _HDHIVE_NEXT_ROUTER_STATE_TREE_JSON
    return _hdhive_next_action_call_sync(
        cookie_header=cookie_header,
        action_id=HDHIVE_ACTION_ENCRYPTE_ID,
        page_path=page_path,
        router_state_tree_json=router_state_tree_json,
        action_args=[plaintext_json],
    )


def _hdhive_go_api_get_url_info_sync(cookie_header: str, slug: str) -> Optional[dict]:
    # Build encrypted query from {slug, utctimestamp}
    payload = json.dumps({
        'slug': slug,
        'utctimestamp': int(time.time()),
    }, ensure_ascii=False)
    encrypted_query = _hdhive_encrypte_sync(cookie_header, slug, payload)
    if not isinstance(encrypted_query, str) or not encrypted_query:
        return None

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json,*/*',
        'Cookie': cookie_header,
    }
    url = f"https://hdhive.com/go-api/customer/resources/{slug}/url"
    resp = requests.get(url, params={'query': encrypted_query}, timeout=25, headers=headers)
    try:
        raw = resp.json()
    except Exception:
        return None
    ciphertext = raw.get('data') if isinstance(raw, dict) else None
    if not ciphertext:
        return None
    decrypted = _hdhive_decrypt_sync(cookie_header, slug, ciphertext)
    return decrypted if isinstance(decrypted, dict) else None


def _hdhive_go_api_unlock_sync(cookie_header: str, slug: str) -> Optional[dict]:
    # Frontend sends encrypted body {data: encrypte(JSON({utctimestamp}))}
    payload = json.dumps({'utctimestamp': int(time.time())}, ensure_ascii=False)
    encrypted_body = _hdhive_encrypte_sync(cookie_header, slug, payload)

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json,*/*',
        'Cookie': cookie_header,
    }
    url = f"https://hdhive.com/go-api/customer/resources/{slug}/unlock"
    if isinstance(encrypted_body, str) and encrypted_body:
        resp = requests.post(url, json={'data': encrypted_body}, timeout=25, headers=headers)
    else:
        resp = requests.post(url, timeout=25, headers=headers)
    try:
        raw = resp.json()
    except Exception:
        return None
    ciphertext = raw.get('data') if isinstance(raw, dict) else None
    if not ciphertext:
        return None
    decrypted = _hdhive_decrypt_sync(cookie_header, slug, ciphertext)
    return decrypted if isinstance(decrypted, dict) else None


def _normalize_115_url(u: str) -> str:
    if not u:
        return u
    u = html.unescape(u).strip()
    # Some strings may contain trailing fragments/artifacts from templates
    u = u.replace('\\', '')
    while u.endswith('&#') or u.endswith('&'):
        u = u[:-1]
    if u.endswith('#') and '?#' not in u:
        u = u[:-1]

    # Normalize query param name: access_code -> password
    try:
        parsed = urlparse(u)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        if 'access_code' in qs and 'password' not in qs:
            qs['password'] = qs.pop('access_code')
            new_query = urlencode(qs, doseq=True)
            u = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        # best-effort fallback
        if 'access_code=' in u and 'password=' not in u:
            u = u.replace('access_code=', 'password=')

    return u


def _extract_hdhive_urls_from_text(text: str) -> List[str]:
    if not text:
        return []
    return list(dict.fromkeys(m.group(0) for m in HDHIVE_115_URL_RE.finditer(text)))


def _extract_hdhive_hits_from_message(msg) -> List[HdhiveLinkHit]:
    hits: List[HdhiveLinkHit] = []
    text = getattr(msg, 'message', None) or ""

    for url in _extract_hdhive_urls_from_text(text):
        hits.append(HdhiveLinkHit(hdhive_url=url, display_text=url, source='text'))

    entities = getattr(msg, 'entities', None) or []
    for ent in entities:
        url = getattr(ent, 'url', None)
        if not url or not HDHIVE_115_URL_RE.search(url):
            continue
        try:
            offset = int(getattr(ent, 'offset', 0))
            length = int(getattr(ent, 'length', 0))
            display = text[offset:offset + length] if length else url
        except Exception:
            display = url
        hits.append(HdhiveLinkHit(hdhive_url=url, display_text=display or url, source='entity'))

    reply_markup = getattr(msg, 'reply_markup', None)
    rows = getattr(reply_markup, 'rows', None) if reply_markup else None
    if rows:
        for row in rows:
            buttons = getattr(row, 'buttons', None) or []
            for btn in buttons:
                btn_url = getattr(btn, 'url', None)
                if not btn_url or not HDHIVE_115_URL_RE.search(btn_url):
                    continue
                btn_text = getattr(btn, 'text', None) or btn_url
                hits.append(HdhiveLinkHit(hdhive_url=btn_url, display_text=btn_text, source='button'))

    # de-dup, keep first display/source
    seen: set[str] = set()
    deduped: List[HdhiveLinkHit] = []
    for hit in hits:
        if hit.hdhive_url in seen:
            continue
        seen.add(hit.hdhive_url)
        deduped.append(hit)
    return deduped


def _pick_real_url_from_response(resp: requests.Response) -> Optional[str]:
    try:
        # If redirected off HDHive, the final URL is usually the real one.
        if resp.url and ('/login' in resp.url.lower() or 'redirect=' in resp.url.lower()):
            return None
        if resp.url and not HDHIVE_115_URL_RE.search(resp.url) and 'hdhive.com' not in resp.url.lower():
            return resp.url
    except Exception:
        pass

    content_type = (resp.headers.get('Content-Type') or '').lower()
    text = ''
    if 'application/json' in content_type:
        try:
            payload = resp.json()
            # Heuristic: search any string value that looks like a URL
            stack = [payload]
            while stack:
                item = stack.pop()
                if isinstance(item, dict):
                    for v in item.values():
                        stack.append(v)
                elif isinstance(item, list):
                    stack.extend(item)
                elif isinstance(item, str):
                    for m in REAL_URL_RE.finditer(item):
                        u = m.group(0)
                        if '115.com' in u or '115cdn' in u:
                            return u
        except Exception:
            pass
    else:
        try:
            text = resp.text or ''
        except Exception:
            text = ''

    if text:
        # Next.js unauthenticated pages return an inlined redirect payload.
        if 'NEXT_REDIRECT' in text and '/login?redirect=' in text:
            return None

        candidates = []
        for m in REAL_URL_RE.finditer(text):
            u = m.group(0)
            if '115.com' in u or '115cdn' in u:
                candidates.append(u)
        if candidates:
            return candidates[0]

    return None


def _resolve_hdhive_115_url_sync(hdhive_url: str) -> Optional[str]:
    now = time.time()
    cached = _hdhive_resolve_cache.get(hdhive_url)
    if cached:
        ts, val = cached
        ttl = _HDHIVE_CACHE_TTL_SECONDS if val else _HDHIVE_NEGATIVE_CACHE_TTL_SECONDS
        if (now - ts) < ttl:
            return val

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7',
    }

    cookie = _get_hdhive_cookie_header()
    if cookie:
        headers['Cookie'] = cookie

    real_url: Optional[str] = None
    try:
        session = requests.Session()
        resp = session.get(hdhive_url, allow_redirects=True, timeout=20, headers=headers)
        real_url = _pick_real_url_from_response(resp)
    except Exception:
        real_url = None

    debug_log(f"[HDHive] requests resolver: {hdhive_url} -> {real_url}")

    # Stable path: use official go-api + Next.js server action decrypt/encrypte
    if not real_url and cookie:
        try:
            slug = _extract_hdhive_slug(hdhive_url) or hdhive_url.rstrip('/').split('/')[-1]
            url_info = _hdhive_go_api_get_url_info_sync(cookie, slug)
            threshold = 0
            try:
                threshold = int((current_config or {}).get('hdhive_auto_unlock_points_threshold', 0) or 0)  # type: ignore[name-defined]
            except Exception:
                threshold = 0

            if isinstance(url_info, dict):
                unlock_points_raw = url_info.get('unlock_points')
                try:
                    unlock_points = int(unlock_points_raw) if unlock_points_raw is not None else None
                except Exception:
                    unlock_points = None

                full_url = url_info.get('full_url')
                url = url_info.get('url')
                access_code = url_info.get('access_code')

                if isinstance(full_url, str) and full_url.startswith('http'):
                    real_url = _normalize_115_url(full_url)
                elif isinstance(url, str) and url.startswith('http'):
                    # Some payloads require access_code/password to be appended.
                    if isinstance(access_code, str) and access_code and access_code not in url:
                        sep = '&' if '?' in url else '?'
                        real_url = _normalize_115_url(f"{url}{sep}password={access_code}")
                    else:
                        real_url = _normalize_115_url(url)

                # Auto-unlock when unlock_points is known and <= threshold (threshold=0 means only free)
                should_try_unlock = False
                if unlock_points is not None:
                    if unlock_points == 0:
                        # Free unlock should be safe, and may be required to obtain full_url.
                        should_try_unlock = True
                    elif threshold > 0 and unlock_points <= threshold:
                        should_try_unlock = True

                if unlock_points is not None and unlock_points > 0 and threshold >= 0 and unlock_points > threshold:
                    log_message(f"警告: HDHive 资源需要 {unlock_points} 积分，超过自动解锁阈值 {threshold}，跳过自动解锁。")

                # If we got a URL but it might be the pre-unlock placeholder, unlocking still yields full_url.
                if should_try_unlock:
                    log_message(f"INFO: 正在尝试自动解锁 HDHive 资源 (所需积分: {unlock_points if unlock_points is not None else '未知'}, 阈值: {threshold})")
                    unlocked = _hdhive_go_api_unlock_sync(cookie, slug)
                    if isinstance(unlocked, dict):
                        full_url2 = unlocked.get('full_url')
                        url2 = unlocked.get('url')
                        access_code2 = unlocked.get('access_code')
                        if isinstance(full_url2, str) and full_url2.startswith('http'):
                            real_url = _normalize_115_url(full_url2)
                        elif isinstance(url2, str) and url2.startswith('http'):
                            if isinstance(access_code2, str) and access_code2 and access_code2 not in url2:
                                sep = '&' if '?' in url2 else '?'
                                real_url = _normalize_115_url(f"{url2}{sep}password={access_code2}")
                            else:
                                real_url = _normalize_115_url(url2)

                        if real_url:
                            log_message(f"成功: HDHive 解锁成功，真实链接: {real_url}")
                    else:
                        log_message("警告: HDHive 自动解锁失败（可能积分不足/资源失效/登录态异常）。")
        except Exception as e:
            debug_log(f"[HDHive] go-api/action resolver failed: {e}")

    debug_log(f"[HDHive] final resolver: {hdhive_url} -> {real_url}")

    if real_url:
        log_message(f"成功: HDHive 真实链接: {real_url}")

    _hdhive_resolve_cache[hdhive_url] = (now, real_url)
    return real_url


async def resolve_hdhive_115_url(hdhive_url: str) -> Optional[str]:
    to_thread = getattr(asyncio, 'to_thread', None)
    if to_thread:
        return await to_thread(_resolve_hdhive_115_url_sync, hdhive_url)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _resolve_hdhive_115_url_sync, hdhive_url)


def _resolve_hdhive_115_url_with_note_sync(hdhive_url: str) -> Tuple[Optional[str], str]:
    """Resolve HDHive url and return a friendly note for forwarding messages."""
    slug = _extract_hdhive_slug(hdhive_url) or hdhive_url.rstrip('/').split('/')[-1]
    cookie = _get_hdhive_cookie_header()
    if not cookie:
        return None, "未配置 HDHive Cookie，无法解析/解锁"

    threshold = 0
    try:
        threshold = int((current_config or {}).get('hdhive_auto_unlock_points_threshold', 0) or 0)  # type: ignore[name-defined]
    except Exception:
        threshold = 0

    # 1) Try read url info
    url_info = None
    try:
        url_info = _hdhive_go_api_get_url_info_sync(cookie, slug)
    except Exception:
        url_info = None

    unlock_points = None
    already_owned = False
    if isinstance(url_info, dict):
        try:
            if url_info.get('unlock_points') is not None:
                unlock_points = int(url_info.get('unlock_points'))
        except Exception:
            unlock_points = None
        already_owned = bool(url_info.get('already_owned', False))

        full_url = url_info.get('full_url')
        url = url_info.get('url')
        access_code = url_info.get('access_code')

        if isinstance(full_url, str) and full_url.startswith('http'):
            real = _normalize_115_url(full_url)
            return real, "已解锁，解析成功"

        if isinstance(url, str) and url.startswith('http') and already_owned:
            if isinstance(access_code, str) and access_code and access_code not in url:
                sep = '&' if '?' in url else '?'
                real = _normalize_115_url(f"{url}{sep}password={access_code}")
            else:
                real = _normalize_115_url(url)
            return real, "已解锁，解析成功"

        # Locked case: has points requirement but not owned and no full_url
        if unlock_points is not None and unlock_points > 0 and not already_owned:
            if unlock_points > threshold:
                return None, f"需要 {unlock_points} 积分 > 阈值 {threshold}，未自动解锁"

    # 2) Attempt unlock if allowed (free or within threshold)
    should_unlock = False
    if unlock_points is None:
        # If we can't read points, do not auto-spend by default
        should_unlock = False
    elif unlock_points == 0:
        should_unlock = True
    elif threshold > 0 and unlock_points <= threshold:
        should_unlock = True

    if should_unlock:
        unlocked = None
        try:
            unlocked = _hdhive_go_api_unlock_sync(cookie, slug)
        except Exception:
            unlocked = None
        if isinstance(unlocked, dict):
            full_url2 = unlocked.get('full_url')
            url2 = unlocked.get('url')
            access_code2 = unlocked.get('access_code')
            if isinstance(full_url2, str) and full_url2.startswith('http'):
                real = _normalize_115_url(full_url2)
                if unlock_points == 0:
                    return real, "自动解锁成功(0积分)"
                return real, f"自动解锁成功(消耗 {unlock_points} 积分)"
            if isinstance(url2, str) and url2.startswith('http'):
                if isinstance(access_code2, str) and access_code2 and access_code2 not in url2:
                    sep = '&' if '?' in url2 else '?'
                    real = _normalize_115_url(f"{url2}{sep}password={access_code2}")
                else:
                    real = _normalize_115_url(url2)
                if unlock_points == 0:
                    return real, "自动解锁成功(0积分)"
                return real, f"自动解锁成功(消耗 {unlock_points} 积分)"
        return None, "尝试自动解锁失败(可能积分不足/资源失效/登录态异常)"

    # 3) Fallback: use existing resolver (may succeed via redirects for some cases)
    real = _resolve_hdhive_115_url_sync(hdhive_url)
    if real:
        return _normalize_115_url(real), "解析成功"

    if unlock_points is not None and unlock_points > 0:
        return None, f"需要 {unlock_points} 积分，未解锁"
    return None, "解析失败"


async def resolve_hdhive_115_url_with_note(hdhive_url: str) -> Tuple[Optional[str], str]:
    to_thread = getattr(asyncio, 'to_thread', None)
    if to_thread:
        return await to_thread(_resolve_hdhive_115_url_with_note_sync, hdhive_url)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _resolve_hdhive_115_url_with_note_sync, hdhive_url)


async def convert_message_hdhive_links(msg) -> Tuple[bool, str]:
    """If message contains HDHive 115 links (explicit/implicit/buttons), resolve and convert.

    Returns: (changed, new_text)
    """
    original_text = getattr(msg, 'message', None) or ""
    hits = _extract_hdhive_hits_from_message(msg)
    if not hits:
        return False, original_text

    resolved_map: Dict[str, Optional[str]] = {}
    note_map: Dict[str, str] = {}
    for hit in hits:
        if hit.hdhive_url in resolved_map:
            continue
        real_url, note = await resolve_hdhive_115_url_with_note(hit.hdhive_url)
        resolved_map[hit.hdhive_url] = real_url
        note_map[hit.hdhive_url] = note

    new_text = original_text
    # Replace explicit occurrences in the visible text
    for hdhive_url, real_url in resolved_map.items():
        if real_url:
            new_text = new_text.replace(hdhive_url, real_url)

    # Append a single prominent summary block, avoiding repeating URLs that already appear in message text.
    summary_lines: List[str] = []
    for hit in hits:
        note = (note_map.get(hit.hdhive_url) or '').strip() or '已处理'
        real = resolved_map.get(hit.hdhive_url)

        if real:
            # If real link is already visible in message (explicit replacement), do not repeat it.
            if real in new_text:
                summary_lines.append(f"✅ HDHive：{note}")
            else:
                summary_lines.append(f"✅ 真实链接（{note}）：{real}")
        else:
            summary_lines.append(f"⚠️ 未解析（{note}）：{hit.hdhive_url}")

    summary_lines = list(dict.fromkeys(summary_lines))
    if summary_lines:
        new_text = (new_text + "\n\n【HDHive解析】\n" + "\n".join(summary_lines)).strip()

    return True, new_text

async def message_queue_loop():
    """Background loop to send pending messages from app.py"""
    log_message("消息发送队列轮询已启动。")
    while True:
        try:
            if os.path.exists(MESSAGE_QUEUE_FILE):
                pending = []
                # Use a lock-free approach for simplicity, but we should be careful
                # We'll read, clear the file, then process
                try:
                    with open(MESSAGE_QUEUE_FILE, 'r+', encoding='utf-8') as f:
                        content = f.read().strip()
                        if content:
                            pending = json.loads(content)
                            f.seek(0)
                            f.truncate()
                except Exception as e:
                    log_message(f"读取消息队列失败: {e}")
                
                if pending:
                    for msg in pending:
                        chat_id = msg.get('chat_id')
                        text = msg.get('text')
                        if chat_id and text:
                            try:
                                log_message(f"发送队列消息到 {chat_id}...")
                                await client.send_message(chat_id, text)
                            except Exception as e:
                                log_message(f"发送队列消息失败: {e}")
            
            await asyncio.sleep(2)
        except Exception as e:
            log_message(f"消息队列循环异常: {e}")
            await asyncio.sleep(5)

# Global client and configuration
client = None
current_config = load_config()

# Semaphore for concurrency limiting
# Limit to 2 concurrent "expensive" operations (download/forward)
concurrency_semaphore = asyncio.Semaphore(2)

async def ensure_client_connected():
    """Ensures the Telethon client is connected and authenticated."""
    global client, current_config

    # Prioritize environment variables
    api_id_env = os.environ.get('TELEGRAM_API_ID')
    api_hash_env = os.environ.get('TELEGRAM_API_HASH')

    if api_id_env and api_hash_env:
        api_id = int(api_id_env)
        api_hash = api_hash_env
        log_message("使用环境变量中的 Telegram API 凭据。" )
    else:
        api_id = current_config['telegram'].get('api_id')
        api_hash = current_config['telegram'].get('api_hash')
        if api_id or api_hash:
            log_message("警告: 建议将 Telegram API 凭据存储在环境变量中而不是 config.json 中。" )

    # Construct full path for session file
    session_file_path = os.path.join(CONFIG_DIR, current_config['telegram'].get('session_name', 'telegram_monitor'))
    
    if not api_id or not api_hash:
        log_message("Telegram API 凭据未设置。无法启动监控。" )
        return False

    # If client exists and is connected, and authorized, return true
    if client and client.is_connected():
        try:
            if await client.is_user_authorized():
                return True
            else:
                log_message("现有客户端未授权。" )
                await client.disconnect() # Disconnect unauthorized client
        except FloodWaitError as e:
            log_message(f"遇到 FloodWaitError，等待 {e.seconds} 秒...")
            await asyncio.sleep(e.seconds)
            return False # Try again after waiting
        except Exception as e:
            log_message(f"检查客户端授权状态失败: {e}")
            traceback.print_exc()
            if client and client.is_connected():
                await client.disconnect()
            return False

    # Attempt to create and connect client
    proxy_config = current_config.get('proxy', {})
    
    # Pre-configure the session database with WAL mode before creating client
    db_file = session_file_path + '.session'
    try:
        conn = sqlite3.connect(db_file, timeout=10)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=10000')
        conn.commit()
        conn.close()
        log_message("✓ Session数据库已配置为WAL模式")
    except Exception as e:
        log_message(f"警告: 无法预配置数据库 (将继续): {e}")
    
    client_args = {'session': session_file_path, 'api_id': api_id, 'api_hash': api_hash}
    
    if proxy_config and proxy_config.get('addr') and proxy_config.get('port'):
        try:
            proxy_addr = proxy_config['addr']
            proxy_port = int(proxy_config['port'])
            proxy_username = proxy_config.get('username')
            proxy_password = proxy_config.get('password')

            log_message(f"使用代理：{proxy_addr}:{proxy_port}")
            client_args['proxy'] = (proxy_addr, proxy_port, proxy_username, proxy_password)
        except Exception as e:
            log_message(f"解析代理配置失败，将不使用代理: {e}")
            traceback.print_exc()

    client = TelegramClient(**client_args) # Modified to use full path and proxy
    try:
        log_message("尝试连接到 Telegram...")
        await client.connect()
        if not await client.is_user_authorized():
            log_message("Telethon 客户端未授权。请通过 Web UI 进行认证。" )
            await client.disconnect()
            return False
        log_message("Telethon 客户端已连接并授权。" )
        return True
    except FloodWaitError as e:
        log_message(f"遇到 FloodWaitError，等待 {e.seconds} 秒...")
        await asyncio.sleep(e.seconds)
        if client and client.is_connected():
            await client.disconnect() # Disconnect to ensure clean state after flood wait
        return False
    except Exception as e:
        log_message(f"连接或授权 Telethon 客户端失败: {e}")
        traceback.print_exc() # Add this line
        if client and client.is_connected():
            await client.disconnect()
        return False

async def reliable_action(action_name, coro_func, *args, **kwargs):
    """
    Executes a coroutine with automatic retry on FloodWaitError.
    Uses a semaphore to limit global concurrency.
    Includes a timeout mechanism to prevent indefinite hanging (e.g. stalled downloads).
    """
    timeout = kwargs.pop('timeout', 1800) # Default timeout 30 minutes
    max_retries = 5
    attempt = 0
    
    async with concurrency_semaphore:
        while attempt < max_retries:
            try:
                # Wrap the coroutine with a timeout
                return await asyncio.wait_for(coro_func(*args, **kwargs), timeout=timeout)
            except FloodWaitError as e:
                attempt += 1
                wait_time = e.seconds + 5 # Add a small buffer of 5 seconds
                log_message(f"[{action_name}] 遇到 FloodWaitError。等待 {wait_time} 秒后重试 (尝试次数: {attempt}/{max_retries})...")
                await asyncio.sleep(wait_time)
            except asyncio.TimeoutError:
                log_message(f"[{action_name}] 操作超时 ({timeout}秒)。放弃本次尝试。")
                # Usually timeout means network stuck, retrying immediately might help or might not.
                # Let's count it as a failure for now to release semaphore.
                return None 
            except Exception as e:
                log_message(f"[{action_name}] 发生错误: {e}")
                # traceback.print_exc() # Reduce noise for known errors
                # For non-flood errors, we stop retrying to avoid infinite loops on hard failures.
                raise e 
        
        log_message(f"[{action_name}] 超过最大重试次数，放弃操作。")
        return None

def extract_keywords(text, limit=30):
    """
    极简提取关键词 - 去除停用词和描述词，只保留核心内容。
    常见停用词：的、和、是、了、在、有、也、被、以、为、与、并、或、等
    """
    import re
    import emoji
    
    if not text:
        return ""
    
    # 1. Remove URLs
    text = re.sub(r'http[s]?://\S+', '', text)
    text = re.sub(r'www\.\S+', '', text)
    text = re.sub(r't\.me/\S+', '', text)

    # 2. Remove User Mentions (@username)
    text = re.sub(r'@\w+', '', text)

    # 3. Remove Emojis
    try:
        text = emoji.replace_emoji(text, replace='')
    except:
        pass

    # 4. Remove illegal chars
    text = re.sub(r'[\\/*?:"<>|]', '', text)
    
    # 5. Remove blacklist keywords
    filename_blacklist = current_config.get('filename_blacklist', [])
    for keyword in filename_blacklist:
        try:
            if keyword:
                text = re.sub(re.escape(keyword), '', text, flags=re.IGNORECASE)
        except:
            pass
    
    # 6. Normalize separators and tokenize
    # 目标：尽量保持语义词组，避免“硬截断”导致断句断词
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r'[，。！？、；：,!.?;:【】\[\]()（）“”"\'<>《》·•…—\-]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    stopwords = {
        '的', '和', '是', '了', '在', '有', '也', '被', '以', '为', '与', '并', '或', '等',
        '这', '那', '就', '还', '把', '给', '向', '一个', '一些', '我们', '你们', '他们'
    }

    # 词元提取：英文/数字串 + 连续中文串(>=2)
    tokens = re.findall(r'[A-Za-z0-9]+|[\u4e00-\u9fff]{2,}', text)
    if not tokens:
        # 兜底：若无法分词，至少返回清洗后的整句（不再硬截词）
        return text[:limit].strip() if len(text) <= limit else text[:limit].rstrip()

    filtered_tokens = []
    seen = set()
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        if token in stopwords:
            continue
        if len(token) == 1 and re.match(r'[\u4e00-\u9fff]', token):
            continue
        if token.lower() in {'tg', 'telegram', '频道', '视频', '图片'}:
            # 通用噪声词降权（可根据需要保留）
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        filtered_tokens.append(token)

    if not filtered_tokens:
        filtered_tokens = tokens[:]

    # 7. 按词元拼接到 limit，避免中间截断
    result_tokens = []
    current_len = 0
    for token in filtered_tokens:
        add_len = len(token) if not result_tokens else len(token) + 1  # +1 for underscore
        if current_len + add_len > limit:
            break
        result_tokens.append(token)
        current_len += add_len

    if not result_tokens:
        # 极端情况下取首词前缀
        token = filtered_tokens[0]
        return token[:limit].strip()

    return '_'.join(result_tokens).strip('_ ').strip()

def sanitize_filename(text, limit=60):
    """
    Sanitizes a string to be safe for filenames.
    Removes illegal chars, emojis, URLs, mentions, and promotional spam.
    Truncates to a specified limit.
    """
    import re
    import emoji # Make sure to import emoji if not top-level
    
    if not text:
        return ""
    
    # 1. Remove URLs
    text = re.sub(r'http[s]?://\S+', '', text)
    text = re.sub(r'www\.\S+', '', text)
    text = re.sub(r't\.me/\S+', '', text)

    # 2. Remove User Mentions (@username)
    text = re.sub(r'@\w+', '', text)

    # 3. Remove Spam Phrases (Regex for common promotional text)
    # spam_patterns removed as per user request to use only custom blacklist
    pass
    filename_blacklist = current_config.get('filename_blacklist', [])
    for keyword in filename_blacklist:
        try:
            if keyword:
                text = re.sub(re.escape(keyword), '', text, flags=re.IGNORECASE)
        except:
            pass

    # 5. Remove Emojis
    try:
        text = emoji.replace_emoji(text, replace='')
    except:
        pass # If emoji module fails or not loaded

    # 5. Simple Illegal Char Removal
    # Windows: \ / : * ? " < > |
    text = re.sub(r'[\\/*?:"<>|]', '', text)
    
    # 6. Collapse multiple spaces and trim
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 7. Truncate
    if len(text) > limit:
        text = text[:limit].rstrip()
        
    return text.strip()

def match_keyword(pattern, text):
    """Checks if a pattern (regex or string) matches the text."""
    if not pattern:
        return False
    if not text:
        text = ""  # Ensure text is at least an empty string
        
    pattern_str = str(pattern).strip()
    text_str = str(text)

    try:
        # 1. 优先尝试完全匹配字符串（去空格）
        if pattern_str.lower() in text_str.lower():
            return True
            
        # 2. 尝试作为正则表达式匹配
        # 注意：如果 pattern 本身不是合法的正则，re.search 会抛出异常进入 except
        import re
        if re.search(pattern_str, text_str, re.IGNORECASE | re.DOTALL):
            return True
    except Exception:
        # 如果正则解析失败，上面已经做了字符串包含检查，这里直接返回 False
        pass
    return False

# --- Message Handler ---
async def new_message_handler(event):
    try:
        chat_title = "Unknown"
        if hasattr(event, 'chat') and hasattr(event.chat, 'title'):
            chat_title = event.chat.title
        debug_log(f" 收到原始消息 - 来源: {chat_title} (ID: {event.chat_id}) | MsgID: {event.message.id}")
    except Exception as e:
        debug_log(f" 解析消息来源时报错: {e}")

    msg = event.message
    if not msg:
        debug_log(" 消息内容为空，忽略")
        return

    # --- 全局黑名单检查 ---
    global_blacklist = current_config.get('global_blacklist_keywords', [])
    if global_blacklist and msg.message:
        for keyword in global_blacklist:
            if match_keyword(keyword, msg.message):
                debug_log(f" 触发全局黑名单关键词 '{keyword}'，跳过消息 {msg.id}")
                return

    # Check for restricted channels first
    debug_log(f" 当前配置中有 {len(current_config.get('restricted_channels', []))} 个关注频道")
    for restricted_entry in current_config.get('restricted_channels', []):
        restricted_channel_id = restricted_entry.get('channel_id')
        try:
            restricted_channel_id = int(restricted_channel_id)
        except (ValueError, TypeError):
            continue

        if event.chat_id == restricted_channel_id:
            debug_log(f" 命中关注频道: {chat_title} ({restricted_channel_id})")
            
            # --- Auto-update Channel Name if Empty ---
            if not restricted_entry.get('channel_name') and chat_title != "Unknown":
                try:
                    # Find index in actual config list
                    for i, entry in enumerate(current_config.get('restricted_channels', [])):
                        if int(entry.get('channel_id')) == event.chat_id:
                            current_config['restricted_channels'][i]['channel_name'] = chat_title
                            save_config(current_config)
                            log_message(f"已自动补全频道名称: {chat_title}")
                            break
                except Exception as e:
                    debug_log(f" 自动补全名称失败: {e}")

            msg = event.message
            if not msg:
                debug_log(" 消息内容为空，忽略")
                return

            media_trace = _build_media_trace(msg)

            message_text_for_filter = msg.message or ""
            if not message_text_for_filter and msg.grouped_id:
                try:
                    nearby_msgs = await client.get_messages(
                        event.chat_id,
                        limit=50,
                        min_id=max(msg.id - 20, 0),
                        max_id=msg.id + 20
                    )
                    nearby_msgs = list(nearby_msgs) if nearby_msgs else []
                    for m in nearby_msgs:
                        if m.grouped_id == msg.grouped_id and m.message:
                            message_text_for_filter = m.message
                            break
                except Exception:
                    pass

            # --- Blacklist Check ---
            blacklist = restricted_entry.get('blacklist_keywords', [])
            if blacklist and message_text_for_filter:
                for keyword in blacklist:
                    if match_keyword(keyword, message_text_for_filter):
                        if getattr(msg, 'media', None):
                            trace_log(
                                f"[TRACE_SKIP] ch={restricted_channel_id} msg={msg.id} reason=blacklist keyword='{keyword}' | {media_trace}",
                            )
                        debug_log(f"在频道 {restricted_channel_id} 中检测到黑名单关键词或正则 '{keyword}'，跳过。")
                        return

            # --- Whitelist Check ---
            whitelist = restricted_entry.get('whitelist_keywords', [])
            if whitelist:
                found_whitelist = False
                if message_text_for_filter:
                    for keyword in whitelist:
                        if match_keyword(keyword, message_text_for_filter):
                            found_whitelist = True
                            break
                if not found_whitelist:
                    if getattr(msg, 'media', None):
                        trace_log(
                            f"[TRACE_SKIP] ch={restricted_channel_id} msg={msg.id} reason=whitelist_miss | {media_trace}",
                        )
                    debug_log(f"在频道 {restricted_channel_id} 中未检测到白名单关键词或正则，跳过消息 {msg.id}。")
                    return

            # --- Detect Message Type ---
            media_type_detected = 'text'
            debug_log(f" 原始媒体: {media_trace}")

            if _is_video_like_message(msg):
                media_type_detected = 'video'
            elif msg.photo:
                media_type_detected = 'photo'
            elif msg.audio or msg.voice:
                media_type_detected = 'audio'
            elif isinstance(msg.media, MessageMediaDocument):
                media_type_detected = 'document'
            
            debug_log(f" 消息类型: {media_type_detected}")
            
            # --- Check Monitor Type ---
            monitor_types = restricted_entry.get('monitor_types')
            if isinstance(monitor_types, list) and monitor_types:
                normalized_types = [str(t).strip().lower() for t in monitor_types if str(t).strip()]
            else:
                single_type = str(restricted_entry.get('monitor_type', 'all')).strip().lower()
                normalized_types = [single_type] if single_type else ['all']

            if 'all' not in normalized_types and media_type_detected not in normalized_types:
                if getattr(msg, 'media', None):
                    trace_log(
                        f"[TRACE_SKIP] ch={restricted_channel_id} msg={msg.id} reason=monitor_type_mismatch "
                        f"configured={normalized_types} detected={media_type_detected} | {media_trace}"
                    )
                debug_log(
                    f" 频道监控类型为 '{','.join(normalized_types)}'，"
                    f"与消息类型 '{media_type_detected}' 不匹配，跳过。"
                )
                return

            # --- Enable Forward/Download Flags ---
            download_directory = restricted_entry.get('download_directory', '').strip()
            should_forward = restricted_entry.get('keep_video_message', False)
            # If download_directory is set, we consider download enabled for non-text media
            should_download = bool(download_directory) and media_type_detected != 'text'

            # Special case: if message contains HDHive 115 links (explicit/implicit/buttons),
            # always send the resolved real link(s) to configured target users.
            try:
                target_user_ids_for_hdhive = restricted_entry.get('target_user_ids', [])
                if target_user_ids_for_hdhive and _extract_hdhive_hits_from_message(msg):
                    should_forward = True
            except Exception:
                pass
            
            debug_log(f" 配置读取 - 转发开关(keep_video_message): {should_forward}, 下载目录: '{download_directory}'")
            debug_log(f" 最终判定 - 下载: {should_download}, 转发: {should_forward}")
            if getattr(msg, 'media', None):
                trace_log(
                    f"[TRACE_DETECT] ch={restricted_channel_id} msg={msg.id} detected={media_type_detected} "
                    f"download={should_download} forward={should_forward} | {media_trace}"
                )
            
            # --- Decision Logic ---
            # Use the flags we already determined based on monitor_type
            is_monitored = True # We already filtered by monitor_type above

            if not should_download and not should_forward:
                 if getattr(msg, 'media', None):
                     trace_log(
                         f"[TRACE_SKIP] ch={restricted_channel_id} msg={msg.id} reason=no_action "
                         f"detected={media_type_detected} | {media_trace}"
                     )
                 debug_log(f" 消息 {msg.id} 既不需要下载也不需要转发，跳过")
                 return

            log_message(f"在频道 {restricted_channel_id} 中检测到 {media_type_detected} 消息 (ID: {msg.id})。")

            # --- 1. Execute Download ---
            if should_download:
                if download_directory:
                    # Ensure the download directory exists
                    os.makedirs(download_directory, exist_ok=True)
                    # Smart filename generation
                    base_name = str(event.message.id)
                    download_target = msg
                    primary_document = _get_primary_document(msg)
                    if isinstance(getattr(msg, 'media', None), MessageMediaWebPage) and primary_document:
                        download_target = primary_document
                    original_ext = ".mp4" # Default fallback
                    original_filename = None  # 保存原始文件名
                    expected_size = 0  # 预期文件大小
                    
                    if media_type_detected == 'photo':
                        original_ext = ".jpg"
                    elif media_type_detected == 'audio':
                        original_ext = ".mp3"
                    
                    # 优先从document属性中获取原始文件名和扩展名
                    if primary_document:
                        expected_size = primary_document.size if hasattr(primary_document, 'size') else 0
                        for attr in getattr(primary_document, 'attributes', []) or []:
                            if hasattr(attr, 'file_name') and attr.file_name:
                                original_filename = attr.file_name
                                # 从原始文件名提取扩展名
                                if '.' in original_filename:
                                    original_ext = os.path.splitext(original_filename)[1].lower()
                                break
                    
                    # 使用MIME类型识别扩展名（仅在没有原始文件名时）
                    if not original_filename:
                        if msg.video and hasattr(msg.video, 'mime_type') and msg.video.mime_type:
                            ext = get_extension_from_mime(msg.video.mime_type)
                            if ext:
                                original_ext = ext
                            expected_size = msg.video.size if hasattr(msg.video, 'size') else 0
                        elif primary_document and hasattr(primary_document, 'mime_type') and primary_document.mime_type:
                            ext = get_extension_from_mime(primary_document.mime_type)
                            if ext:
                                original_ext = ext

                    final_filename = ""
                    final_folder_path = download_directory

                    # Check for Album/Grouped Media
                    if msg.grouped_id:
                        try:
                            search_min_id = msg.id - 10
                            search_max_id = msg.id + 10
                            nearby_msgs = await client.get_messages(
                                event.chat_id, 
                                limit=50,
                                min_id=search_min_id if search_min_id > 0 else 0, 
                                max_id=search_max_id
                            )
                            if nearby_msgs is None:
                                nearby_msgs = []
                            else:
                                nearby_msgs = list(nearby_msgs)
                            album_msgs = [m for m in nearby_msgs if m.grouped_id == msg.grouped_id]
                            if not any(m.id == msg.id for m in album_msgs):
                                album_msgs.append(msg)
                            album_msgs.sort(key=lambda m: m.id)
                            
                            album_caption = ""
                            for m in album_msgs:
                                if m.message:
                                    album_caption = m.message
                                    break
                            
                            grouped_id_key = int(msg.grouped_id)
                            folder_name = _album_folder_cache.get(grouped_id_key, "")
                            if not folder_name:
                                folder_name = str(msg.grouped_id)
                                if album_caption:
                                    # 极简风格：只保留关键词，去除停用词
                                    keywords = extract_keywords(album_caption, limit=30)
                                    if keywords:
                                        folder_name = keywords
                                _album_folder_cache[grouped_id_key] = folder_name
                            
                            final_folder_path = os.path.join(download_directory, folder_name)
                            os.makedirs(final_folder_path, exist_ok=True)

                            try:
                                # 按文件类型分别计数：图片单独编号，视频单独编号
                                type_sequence = []
                                for m in album_msgs:
                                    m_ext = ".jpg"  # Default for photo
                                    if m.video:
                                        m_ext = ".mp4"  # Default for video
                                    elif m.document:
                                        if hasattr(m.document, 'mime_type') and m.document.mime_type:
                                            m_ext = "." + m.document.mime_type.split('/')[-1]
                                    # 只有相同扩展名的才计入同一序列
                                    if m_ext == original_ext:
                                        type_sequence.append(m.id)
                                
                                if msg.id in type_sequence:
                                    index = type_sequence.index(msg.id) + 1
                                else:
                                    index = len(type_sequence) + 1
                            except ValueError:
                                index = msg.id
                            
                            # File inherits folder name for better organization
                            final_filename = f"{folder_name}_{index}{original_ext}"
                            log_message(f"检测到相册消息 (Group: {msg.grouped_id})。按类型编号 - {media_type_detected}: {index}。归档至: '{folder_name}/{final_filename}'")
                        except Exception as e:
                            log_message(f"处理相册逻辑失败: {e}。")
                    
                    if not final_filename:
                        try:
                            # 优先级调整：消息文本 > 原始文件名 > fallback
                            potential_name = msg.message or ""
                            if potential_name:
                                # 优先使用消息文本作为文件名
                                clean_name = sanitize_filename(potential_name, limit=60)
                                if clean_name:
                                    final_filename = f"{clean_name}{original_ext}"
                                else:
                                    final_filename = f"{base_name}{original_ext}"
                            elif original_filename:
                                # 其次使用原始文件名（保持原始扩展名）
                                clean_name = sanitize_filename(os.path.splitext(original_filename)[0], limit=60)
                                if clean_name:
                                    final_filename = f"{clean_name}{original_ext}"
                                else:
                                    final_filename = f"{base_name}{original_ext}"
                            else:
                                # 最后使用默认名称
                                final_filename = f"{base_name}{original_ext}"
                        except Exception as e:
                            final_filename = f"{base_name}{original_ext}"

                    file_path = os.path.join(final_folder_path, final_filename)
                    safe_file_path = _resolve_non_conflicting_path(file_path, msg.id)
                    if safe_file_path != file_path:
                        log_message(f"检测到同名文件，改用防覆盖路径: {os.path.basename(safe_file_path)}")
                    file_path = safe_file_path
                    size_info = f" (预计 {expected_size / (1024*1024):.1f}MB)" if expected_size > 0 else ""
                    source_url = _extract_first_url_from_text(msg.message or "")
                    source_title = _extract_title_for_download(msg, original_filename or "")
                    source_resolution = _extract_video_resolution_text(msg)
                    details = []
                    if source_url:
                        details.append(f"原链接: {source_url}")
                    if source_title:
                        details.append(f"标题: {source_title}")
                    if source_resolution:
                        details.append(f"分辨率: {source_resolution}")
                    detail_text = f" | {' | '.join(details)}" if details else ""
                    log_message(f"开始下载 {media_type_detected}{size_info} 到 {file_path}...{detail_text}")

                    while True:
                        can_download, risk_reason, retry_after = _check_download_risk_controls(
                            restricted_channel_id=restricted_channel_id,
                            download_directory=final_folder_path,
                            msg=msg,
                            expected_size=expected_size,
                        )
                        if can_download:
                            break

                        if retry_after > 0:
                            log_message(f"触发下载风控，已进入队列等待 {retry_after}s 后重试: {risk_reason}")
                            await asyncio.sleep(retry_after)
                            continue

                        log_message(f"触发下载风控，已跳过下载: {risk_reason}")
                        downloaded_file = None
                        break

                    if can_download:
                        try:
                            # 为大文件（>50MB）添加进度回调
                            progress_cb = None
                            if expected_size > 50 * 1024 * 1024:  # 50MB
                                progress_cb = create_progress_callback(file_path, media_type_detected)
                            download_timeout = _compute_download_timeout_seconds(expected_size)
                            log_message(
                                f"下载超时设置 [{media_type_detected}]: {download_timeout}s "
                                f"(文件大小: {expected_size / (1024 * 1024):.1f}MB)"
                            )
                            
                            downloaded_file = await reliable_action(
                                f"下载 {media_type_detected} {msg.id}",
                                client.download_media,
                                download_target,
                                file=file_path,
                                progress_callback=progress_cb,
                                timeout=download_timeout,
                            )
                            
                            if downloaded_file:
                                # 验证文件完整性
                                actual_size = os.path.getsize(downloaded_file) if os.path.exists(downloaded_file) else 0
                                completion_details = []
                                if source_url:
                                    completion_details.append(f"原链接: {source_url}")
                                if source_title:
                                    completion_details.append(f"标题: {source_title}")
                                if source_resolution:
                                    completion_details.append(f"分辨率: {source_resolution}")
                                completion_detail_text = f" | {' | '.join(completion_details)}" if completion_details else ""
                                if expected_size > 0 and actual_size > 0:
                                    size_diff_percent = abs(actual_size - expected_size) / expected_size * 100
                                    if size_diff_percent > 5:  # 大小差异超过5%
                                        log_message(f"警告: 下载文件大小异常 - 预期{expected_size/(1024*1024):.1f}MB，实际{actual_size/(1024*1024):.1f}MB (差异{size_diff_percent:.1f}%)")
                                    else:
                                        log_message(f"{media_type_detected} 已成功下载到 {downloaded_file} ({actual_size/(1024*1024):.1f}MB)。{completion_detail_text}")
                                else:
                                    log_message(f"{media_type_detected} 已成功下载到 {downloaded_file}。{completion_detail_text}")
                            else:
                                log_message(f"{media_type_detected} 下载失败。")
                        except Exception as e:
                            log_message(f"{media_type_detected} 下载异常: {e}")
                else:
                    log_message(f"跳过下载 {media_type_detected}，因为未配置下载目录。")

            # --- 2. Execute Forward ---
            if should_forward:
                target_user_ids = restricted_entry.get('target_user_ids', [])
                if not target_user_ids:
                    log_message(f"跳过转发，因为未配置目标用户。")
                else:
                    log_message(f"正在尝试将 {media_type_detected} 消息发送到 {len(target_user_ids)} 个目标。")
                    
                    final_msg_text = msg.message or ""
                    has_hdhive, converted_text = await convert_message_hdhive_links(msg)
                    if has_hdhive:
                        final_msg_text = converted_text
                    
                    for user_id in target_user_ids:
                        try:
                            target_entity = await client.get_entity(user_id)
                            
                            # Use send_message for text, forward or send_file for media
                            if has_hdhive or restricted_entry.get('convert_hdhive', False) or media_type_detected == 'text':
                                if media_type_detected == 'text':
                                    await reliable_action(
                                        f"发送文字消息到 {user_id}",
                                        client.send_message,
                                        target_entity,
                                        final_msg_text
                                    )
                                else:
                                    # Media message with potentially modified caption
                                    await reliable_action(
                                        f"发送媒体消息到 {user_id}",
                                        client.send_file,
                                        target_entity,
                                        msg.media,
                                        caption=final_msg_text
                                    )
                            else:
                                # Normal forward (preserves forward tag)
                                await reliable_action(
                                    f"转发消息到 {user_id}",
                                    client.forward_messages,
                                    target_entity,
                                    event.message
                                )
                            log_message(f"成功发送/转发消息到 {user_id}。")
                        except Exception as e:
                            log_message(f"发送消息到 {user_id} 失败: {e}")
    
            return

async def monitor_config_changes():
    global current_config, DEBUG_MODE
    last_config_mtime = 0
    initialized = False  # 用于记录是否已显示初始化日志
    last_heartbeat_ts = 0.0
    
    while True:
        try:
            # Heartbeat log - DEBUG模式下每60秒显示一次，非DEBUG模式下只显示一次
            if DEBUG_MODE:
                now_ts = time.time()
                if now_ts - last_heartbeat_ts >= 60:
                    debug_log(f"监控运行中... 当前关注 {len(current_config.get('restricted_channels', []))} 个频道。")
                    last_heartbeat_ts = now_ts
            elif not initialized:
                log_message(f"监控运行中... 当前关注 {len(current_config.get('restricted_channels', []))} 个频道。")
                initialized = True
            
            # Check for config changes
            if os.path.exists(CONFIG_FILE):
                mtime = os.path.getmtime(CONFIG_FILE)
                if mtime > last_config_mtime:
                    try:
                        new_config = load_config()
                        # Only update if config has actually changed
                        if new_config != current_config:
                            current_config = new_config
                            # Update Debug Mode
                            DEBUG_MODE = current_config.get('debug_mode', False)
                            initialized = False  # 重置标志，便于在模式切换时重新显示初始化日志
                            
                            last_config_mtime = mtime
                            log_message("config.json 已更新，重新加载配置。" )
                            
                            # --- 自动补全缺失的频道名称 ---
                            updated_any_name = False
                            for i, entry in enumerate(current_config.get('restricted_channels', [])):
                                if not entry.get('channel_name'):
                                    try:
                                        cid = int(entry.get('channel_id'))
                                        entity = await client.get_entity(cid)
                                        title = getattr(entity, 'title', str(cid))
                                        current_config['restricted_channels'][i]['channel_name'] = title
                                        log_message(f"检测到新频道 {cid}，自动补全名称: {title}")
                                        updated_any_name = True
                                    except Exception as e:
                                        debug_log(f"后台自动获取名称失败 ({entry.get('channel_id')}): {e}")
                            
                            if updated_any_name:
                                save_config(current_config)
                                last_config_mtime = os.path.getmtime(CONFIG_FILE) # 更新 mtime 避免重复加载
                            
                            log_message(f"当前监控 {len(current_config.get('restricted_channels', []))} 个频道。" )
                            debug_log(f"Debug 模式: {'开启' if DEBUG_MODE else '关闭'}")
                    except Exception as e:
                        log_message(f"虽然检测到配置变更，但重新加载失败: {e}")
        except Exception as e:
            log_message(f"监控配置文件变更时发生错误: {e}")
                 
        await asyncio.sleep(5) # Check for config changes every 5 seconds

async def main():
    global client
    
    # 1. First, ensure the client is connected and authorized
    # This also sets the global 'client' variable
    if not await ensure_client_connected():
        log_message("Telegram 客户端未连接或未授权，无法启动监控。" )
        return

    # 2. Add event handler after client is connected
    client.add_event_handler(new_message_handler, events.NewMessage())
    log_message(f"已成功连接并授权 Telegram 客户端。")
    
    # --- DIAGNOSIS: List all available dialogs ---
    try:
        debug_log("[DIAG] 正在获取账号可见的频道列表...")
        async for dialog in client.iter_dialogs(limit=50):
            if dialog.is_channel or dialog.is_group:
                debug_log(f"[DIAG] 发现频道/群组: {dialog.name} | ID: {dialog.id}")
    except Exception as e:
        debug_log(f"[DIAG] 获取频道列表失败: {e}")
    
    # --- Proactive Channel Name Update on Startup ---
    log_message("正在主动更新频道名称信息...")
    config_updated = False
    import json # Ensure json is imported
    
    # Create a unified list of channels to check (both forward and restricted)
    # But usually we only care about restricted ones for names in config
    channels_to_check = current_config.get('restricted_channels', [])
    
    for i, entry in enumerate(channels_to_check):
        channel_id = entry.get('channel_id')
        # Check if we should update (missing name or force check on startup)
        # Let's check even if name exists, to keep it fresh
        try:
            # We need to ensure channel_id is int
            cid = int(channel_id)
            entity = await client.get_entity(cid)
            title = getattr(entity, 'title', str(cid))
            
            if entry.get('channel_name') != title:
                 current_config['restricted_channels'][i]['channel_name'] = title
                 log_message(f"已更新频道 {cid} 名称: {title}")
                 config_updated = True
        except Exception as e:
            log_message(f"无法获取频道 {channel_id} 的名称: {e}")
            
    if config_updated:
        try:
            config_path = 'config/config.json'
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(current_config, f, indent=4, ensure_ascii=False)
            log_message("频道名称已更新并保存到配置。")
        except Exception as e:
            log_message(f"保存更新后的配置失败: {e}")

    log_message(f"开始监控 {len(current_config.get('restricted_channels', []))} 个频道。")

    try:
        # 3. Run loops
        await asyncio.gather(
            monitor_config_changes(), # Monitors config file for changes
            # periodic_checkin_loop(),   # HDHive 签到功能已移除
            message_queue_loop(),      # Sends messages requested by app.py
            # resource_request_loop(),   # HDHive 功能已移除
            client.run_until_disconnected() # Keeps Telethon client running
        )
    except Exception as e:
        log_message(f"监控主循环发生错误: {e}")
        traceback.print_exc()
    finally:
        # Ensure client disconnects cleanly if main loop exits
        if client and client.is_connected():
            await client.disconnect()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_message("用户中断，程序退出。" )
    except Exception as e:
        log_message(f"发生未预期错误: {e}")
        traceback.print_exc() # Add this line
    finally:
        # Client disconnection is now handled within main() function's finally block
        # Only print status here
        if client and client.is_connected():
            log_message("Telethon 客户端已断开连接。" )
        elif client:
            log_message("Telethon 客户端未连接。" )
