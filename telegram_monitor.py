import os
import json
import asyncio
import time
import traceback
import sys
import sqlite3
import re
import codecs
import shutil
from functools import lru_cache
from collections import deque
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict
from types import SimpleNamespace
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
APP_DIR = os.path.join(BASE_DIR, 'app')
if APP_DIR not in sys.path:
    sys.path.append(APP_DIR)
from proxy_helpers import (
    build_proxy_url_from_scope_config,
    build_requests_proxies_from_scope_config,
    build_telethon_proxy_from_scope_config,
    extract_proxy_scope_config,
    normalize_proxy_scope,
)

# --- Paths & Config ---
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
if not os.path.exists(CONFIG_DIR):
    CONFIG_DIR = os.path.abspath('config')

CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')
TV_CHANNEL_FILTERS_FILE = os.path.join(CONFIG_DIR, 'tvchannel_filters.json')
MESSAGE_QUEUE_FILE = os.path.join(CONFIG_DIR, 'message_queue.json')
DOWNLOAD_RISK_STATS_FILE = os.path.join(CONFIG_DIR, 'download_risk_stats.json')
DOWNLOAD_QUEUE_STATS_FILE = os.path.join(CONFIG_DIR, 'download_queue_stats.json')

# Load environment variables (optional)
try:
    load_dotenv(os.path.join(CONFIG_DIR, '.env'))
except Exception:
    pass


DEBUG_MODE = False
_CHANNEL_FILTERS_CACHE = None
_CHANNEL_FILTERS_MTIME = None
_CHANNEL_FILTERS_LAST_CHECK = 0.0
_CHANNEL_FILTERS_CHECK_INTERVAL = 2.0
_LAST_GOOD_CONFIG = None
_HANDLER_CLIENT = None
STARTUP_TV_WHITELIST_SCAN_LIMIT = 20


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
    global _LAST_GOOD_CONFIG
    default_config = {
        "telegram": {
            "api_id": None,
            "api_hash": "",
            "session_name": "telegram_monitor",
        },
        "download_concurrency": 2,
        "download_queue_maxsize": 200,
        "download_queue_alert": {
            "enabled": False,
            "threshold": 100,
            "cooldown_seconds": 600,
            "notify_user_ids": "",
        },
        "download_queue_throttle": {
            "enabled": True,
            "threshold": 100,
            "recover_threshold": 0,
            "min_concurrency": 1,
        },
        "startup_tv_whitelist_scan_limit": STARTUP_TV_WHITELIST_SCAN_LIMIT,
        "restricted_channels": [],
        "proxy": {
            "telegram": {},
            "service": {},
            "downloader": {},
        },
        "debug_mode": False,
        "trace_media_detection": False,
        "hdhive_base_url": "https://hdhive.com",
        "hdhive_open_api_key": "",
        "hdhive_open_api_direct_unlock": False,
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
        _LAST_GOOD_CONFIG = default_config
        return default_config
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
        if not isinstance(config, dict):
            _LAST_GOOD_CONFIG = default_config
            return default_config
        if 'telegram' not in config:
            config['telegram'] = default_config['telegram']
        if 'download_concurrency' not in config:
            config['download_concurrency'] = default_config['download_concurrency']
        if 'download_queue_maxsize' not in config:
            config['download_queue_maxsize'] = default_config['download_queue_maxsize']
        if 'download_queue_alert' not in config or not isinstance(config.get('download_queue_alert'), dict):
            config['download_queue_alert'] = default_config['download_queue_alert']
        else:
            merged_alert = default_config['download_queue_alert'].copy()
            merged_alert.update(config.get('download_queue_alert') or {})
            config['download_queue_alert'] = merged_alert
        if 'download_queue_throttle' not in config or not isinstance(config.get('download_queue_throttle'), dict):
            config['download_queue_throttle'] = default_config['download_queue_throttle']
        else:
            merged_throttle = default_config['download_queue_throttle'].copy()
            merged_throttle.update(config.get('download_queue_throttle') or {})
            config['download_queue_throttle'] = merged_throttle
        if 'startup_tv_whitelist_scan_limit' not in config:
            config['startup_tv_whitelist_scan_limit'] = default_config['startup_tv_whitelist_scan_limit']
        if 'restricted_channels' not in config:
            config['restricted_channels'] = []
        if 'proxy' not in config:
            config['proxy'] = json.loads(json.dumps(default_config['proxy']))
        else:
            raw_proxy = config.get('proxy')
            if not isinstance(raw_proxy, dict):
                config['proxy'] = json.loads(json.dumps(default_config['proxy']))
            else:
                legacy_keys = {"addr", "port", "username", "password"}
                if legacy_keys & set(raw_proxy.keys()):
                    legacy_proxy = {
                        "addr": str(raw_proxy.get("addr") or "").strip(),
                        "port": str(raw_proxy.get("port") or "").strip(),
                        "username": str(raw_proxy.get("username") or "").strip(),
                        "password": str(raw_proxy.get("password") or "").strip(),
                    }
                    config['proxy'] = {
                        "telegram": dict(legacy_proxy),
                        "service": dict(legacy_proxy),
                        "downloader": {},
                    }
                else:
                    normalized_proxy = json.loads(json.dumps(default_config['proxy']))
                    for scope in ("telegram", "service", "downloader"):
                        scope_cfg = raw_proxy.get(scope)
                        if not isinstance(scope_cfg, dict):
                            continue
                        normalized_proxy[scope] = {
                            "addr": str(scope_cfg.get("addr") or "").strip(),
                            "port": str(scope_cfg.get("port") or "").strip(),
                            "username": str(scope_cfg.get("username") or "").strip(),
                            "password": str(scope_cfg.get("password") or "").strip(),
                        }
                    config['proxy'] = normalized_proxy
        if 'trace_media_detection' not in config:
            config['trace_media_detection'] = False
        if 'debug_mode' not in config:
            config['debug_mode'] = False
        if 'hdhive_base_url' not in config:
            config['hdhive_base_url'] = 'https://hdhive.com'
        if 'hdhive_open_api_key' not in config:
            config['hdhive_open_api_key'] = ''
        if 'hdhive_open_api_direct_unlock' not in config:
            config['hdhive_open_api_direct_unlock'] = False
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
        _LAST_GOOD_CONFIG = config
        return config
    except Exception as e:
        if _LAST_GOOD_CONFIG is not None:
            log_message(f"读取 config.json 失败，使用上次有效配置: {e}")
            return _LAST_GOOD_CONFIG
        _LAST_GOOD_CONFIG = default_config
        return default_config


def _normalize_proxy_scope(scope: Optional[str]) -> str:
    return normalize_proxy_scope(scope)


def _get_proxy_scope_config(config: Optional[dict] = None, scope: Optional[str] = None) -> dict:
    cfg = config if isinstance(config, dict) else load_config()
    return extract_proxy_scope_config(cfg, scope, allow_legacy_proxy=True)


def _build_proxy_url_from_config(config: Optional[dict] = None, scope: Optional[str] = None) -> Optional[str]:
    proxy_cfg = _get_proxy_scope_config(config, scope)
    return build_proxy_url_from_scope_config(proxy_cfg)


def _build_requests_proxies(config: Optional[dict] = None, scope: Optional[str] = None) -> Optional[dict]:
    proxy_cfg = _get_proxy_scope_config(config, scope)
    return build_requests_proxies_from_scope_config(proxy_cfg)


def _build_telethon_proxy_from_config(config: Optional[dict] = None) -> Optional[tuple]:
    proxy_cfg = _get_proxy_scope_config(config, "telegram")
    return build_telethon_proxy_from_scope_config(proxy_cfg)


def _requests_request(method: str, url: str, *, config: Optional[dict] = None, proxy_scope: Optional[str] = None, **kwargs):
    if proxy_scope and 'proxies' not in kwargs:
        proxies = _build_requests_proxies(config, proxy_scope)
        if proxies:
            kwargs['proxies'] = proxies
    return requests.request(method, url, **kwargs)


def _requests_get(url: str, *, config: Optional[dict] = None, proxy_scope: Optional[str] = None, **kwargs):
    return _requests_request('GET', url, config=config, proxy_scope=proxy_scope, **kwargs)


def _requests_post(url: str, *, config: Optional[dict] = None, proxy_scope: Optional[str] = None, **kwargs):
    return _requests_request('POST', url, config=config, proxy_scope=proxy_scope, **kwargs)


def _atomic_write_json(path: str, data: dict, *, indent: int = 4) -> None:
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}.{int(time.time() * 1000)}"
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def save_config(config: dict):
    _atomic_write_json(CONFIG_FILE, config, indent=4)


def _default_channel_filters():
    return {
        "global": {
            "whitelist": [],
            "blacklist": [],
        },
        "drama": {
            "whitelist": [],
        },
        "channels": {},
    }


def load_channel_filters():
    """Load channel filter config from config/tvchannel_filters.json (auto-create if missing)."""
    global _CHANNEL_FILTERS_CACHE, _CHANNEL_FILTERS_MTIME, _CHANNEL_FILTERS_LAST_CHECK
    now = time.monotonic()
    if _CHANNEL_FILTERS_CACHE is not None and (now - _CHANNEL_FILTERS_LAST_CHECK) < _CHANNEL_FILTERS_CHECK_INTERVAL:
        return _CHANNEL_FILTERS_CACHE
    _CHANNEL_FILTERS_LAST_CHECK = now

    try:
        mtime = os.path.getmtime(TV_CHANNEL_FILTERS_FILE)
    except Exception:
        mtime = None

    if _CHANNEL_FILTERS_CACHE is not None and mtime == _CHANNEL_FILTERS_MTIME:
        return _CHANNEL_FILTERS_CACHE

    if not os.path.exists(TV_CHANNEL_FILTERS_FILE):
        try:
            os.makedirs(os.path.dirname(TV_CHANNEL_FILTERS_FILE) or '.', exist_ok=True)
            with open(TV_CHANNEL_FILTERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(_default_channel_filters(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            debug_log(f"写入 tvchannel_filters.json 失败: {e}")
            _CHANNEL_FILTERS_CACHE = _default_channel_filters()
            _CHANNEL_FILTERS_MTIME = mtime
            return _CHANNEL_FILTERS_CACHE

    try:
        with open(TV_CHANNEL_FILTERS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("invalid channel_filters format")
    except Exception as e:
        debug_log(f"读取 tvchannel_filters.json 失败: {e}")
        if _CHANNEL_FILTERS_CACHE is not None:
            return _CHANNEL_FILTERS_CACHE
        data = _default_channel_filters()

    if "global" not in data or not isinstance(data.get("global"), dict):
        data["global"] = {"whitelist": [], "blacklist": []}
    if "drama" not in data or not isinstance(data.get("drama"), dict):
        data["drama"] = {"whitelist": []}
    if "channels" not in data or not isinstance(data.get("channels"), dict):
        data["channels"] = {}

    _CHANNEL_FILTERS_CACHE = data
    _CHANNEL_FILTERS_MTIME = mtime
    return data


# --- Regex & Types ---
REAL_URL_RE = re.compile(r"https?://[^\s\"<>']+", re.IGNORECASE)
HDHIVE_115_URL_RE = re.compile(
    # Support both legacy /resource/115/<slug> and current /resource/<slug> links.
    r"(?:https?://)?(?:www\.)?[^/\s]*hdhive\.[a-z]{2,}/resource/(?:115/)?[0-9A-Za-z]{16,64}",
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
    m = re.search(r"/resource/(?:115/)?([0-9A-Za-z]{16,64})(?:[/?#]|$)", hdhive_url)
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
_HDHIVE_ROUTER_STATE_CACHE: Dict[str, Tuple[float, str]] = {}
_HDHIVE_ROUTER_STATE_TTL_SECONDS = 6 * 60 * 60
_HDHIVE_UNLOCK_ACTION_ID: Optional[str] = None
_HDHIVE_UNLOCK_ACTION_ID_LAST_REFRESH_TS = 0.0
_HDHIVE_UNLOCK_ACTION_ID_TTL_SECONDS = 6 * 60 * 60


def _refresh_hdhive_action_ids_if_needed():
    global HDHIVE_ACTION_DECRYPT_ID, HDHIVE_ACTION_ENCRYPTE_ID, _HDHIVE_ACTION_IDS_LAST_REFRESH_TS

    now = time.time()
    needs_refresh = (
        HDHIVE_ACTION_DECRYPT_ID == HDHIVE_ACTION_DECRYPT_ID_DEFAULT
        or HDHIVE_ACTION_ENCRYPTE_ID == HDHIVE_ACTION_ENCRYPTE_ID_DEFAULT
    )
    if not needs_refresh and _HDHIVE_ACTION_IDS_LAST_REFRESH_TS and (now - _HDHIVE_ACTION_IDS_LAST_REFRESH_TS) < _HDHIVE_ACTION_IDS_REFRESH_TTL_SECONDS:
        return
    try:
        html_text = _requests_get(
            "https://hdhive.com/",
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"},
            proxy_scope="service",
        ).text
    except Exception:
        return

    try:
        chunk_paths = sorted(set(re.findall(r"(/_next/static/[^\"<> ]+\.js)", html_text)))
        if not chunk_paths:
            # Fallback: broader match for any .js under /_next/static
            chunk_paths = sorted(set(re.findall(r"(/_next/static/[^\s\"'<>]+?\.js)", html_text, re.IGNORECASE)))
        if not chunk_paths:
            snippet = (html_text[:160] if isinstance(html_text, str) else "").replace("\n", " ")
            plain_count = html_text.count("/_next/static/") if isinstance(html_text, str) else 0
            debug_log(
                f"[HDHive] action id refresh: no chunk paths found (len={len(html_text)} "
                f"plain_count={plain_count}). Snippet: {snippet}"
            )
            return

        decrypt_id = None
        encrypte_id = None
        # Support multiple minified forms:
        # 1) createServerReference("id", ..., "decrypt")
        # 2) createServerReference)("id", ..., "decrypt")
        # 3) (0,t.createServerReference)("id", ..., "encrypte")
        # Also allow single quotes.
        pat_decrypt = re.compile(
            r"createServerReference[^\(]*\(\s*[\"']([0-9a-f]{40,})[\"'][^\)]*[\"']decrypt[\"']"
        )
        pat_encrypte = re.compile(
            r"createServerReference[^\(]*\(\s*[\"']([0-9a-f]{40,})[\"'][^\)]*[\"']encrypte[\"']"
        )
        pat_encrypt = re.compile(
            r"createServerReference[^\(]*\(\s*[\"']([0-9a-f]{40,})[\"'][^\)]*[\"']encrypt[\"']"
        )

        for p in chunk_paths[:200]:
            try:
                js_text = _requests_get(
                    f"https://hdhive.com{p}",
                    timeout=20,
                    headers={"User-Agent": "Mozilla/5.0"},
                    proxy_scope="service",
                ).text
            except Exception:
                continue
            if decrypt_id is None:
                m = pat_decrypt.search(js_text)
                if m:
                    decrypt_id = m.group(1)
            if encrypte_id is None:
                m = pat_encrypte.search(js_text)
                if m:
                    encrypte_id = m.group(1)
            if encrypte_id is None:
                m = pat_encrypt.search(js_text)
                if m:
                    encrypte_id = m.group(1)
            if decrypt_id and encrypte_id:
                break

        refreshed = False
        if decrypt_id:
            HDHIVE_ACTION_DECRYPT_ID = decrypt_id
            refreshed = True
        if encrypte_id:
            HDHIVE_ACTION_ENCRYPTE_ID = encrypte_id
            refreshed = True
        if not refreshed:
            debug_log("[HDHive] action id refresh: no action ids found in chunks")
        if refreshed:
            _HDHIVE_ACTION_IDS_LAST_REFRESH_TS = now
            debug_log(
                f"[HDHive] action ids refreshed decrypt={HDHIVE_ACTION_DECRYPT_ID[:8]} "
                f"encrypte={HDHIVE_ACTION_ENCRYPTE_ID[:8]}"
            )
    except Exception:
        # Keep defaults on any failure
        return


def _refresh_hdhive_unlock_action_id_if_needed(slug: str) -> Optional[str]:
    global _HDHIVE_UNLOCK_ACTION_ID, _HDHIVE_UNLOCK_ACTION_ID_LAST_REFRESH_TS
    now = time.time()
    if (
        _HDHIVE_UNLOCK_ACTION_ID
        and _HDHIVE_UNLOCK_ACTION_ID_LAST_REFRESH_TS
        and (now - _HDHIVE_UNLOCK_ACTION_ID_LAST_REFRESH_TS) < _HDHIVE_UNLOCK_ACTION_ID_TTL_SECONDS
    ):
        return _HDHIVE_UNLOCK_ACTION_ID

    try:
        html_text = _requests_get(
            f"https://hdhive.com/resource/115/{slug}",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
            proxy_scope="service",
        ).text
    except Exception:
        return _HDHIVE_UNLOCK_ACTION_ID

    try:
        # Extract chunk paths from resource page
        chunk_paths = sorted(set(re.findall(r"(/_next/static/[^\"<> ]+\.js)", html_text)))
        if not chunk_paths:
            return _HDHIVE_UNLOCK_ACTION_ID

        pat_action = re.compile(
            r"createServerReference[^\(]*\(\s*[\"']([0-9a-f]{40,})[\"'][^\)]*?[\"']([A-Za-z0-9_]+)[\"']"
        )
        unlock_id = None
        for p in chunk_paths[:200]:
            try:
                js_text = _requests_get(
                    f"https://hdhive.com{p}",
                    timeout=20,
                    headers={"User-Agent": "Mozilla/5.0"},
                    proxy_scope="service",
                ).text
            except Exception:
                continue
            for m in pat_action.finditer(js_text):
                action_id = m.group(1)
                action_name = m.group(2)
                if action_name == "unlockResource":
                    unlock_id = action_id
                    break
            if unlock_id:
                break

        if unlock_id:
            _HDHIVE_UNLOCK_ACTION_ID = unlock_id
            _HDHIVE_UNLOCK_ACTION_ID_LAST_REFRESH_TS = now
            debug_log(f"[HDHive] unlock action id refreshed {unlock_id[:8]}")
        return _HDHIVE_UNLOCK_ACTION_ID
    except Exception:
        return _HDHIVE_UNLOCK_ACTION_ID


_HDHIVE_NEXT_ROUTER_STATE_TREE_JSON = "[]"


def _get_hdhive_router_state_tree_json(slug: str) -> str:
    if not slug:
        return _HDHIVE_NEXT_ROUTER_STATE_TREE_JSON

    now = time.time()
    cached = _HDHIVE_ROUTER_STATE_CACHE.get(slug)
    if cached:
        ts, tree_json = cached
        if (now - ts) < _HDHIVE_ROUTER_STATE_TTL_SECONDS and tree_json:
            return tree_json

    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        for attempt in range(2):
            html_text = _requests_get(
                f"https://hdhive.com/resource/115/{slug}",
                timeout=30,
                headers=headers,
                proxy_scope="service",
            ).text
            # New Next.js format: embedded flight data in self.__next_f.push([1,"..."])
            parts = re.findall(r'self\.__next_f\.push\(\[1,"([\s\S]*?)"\]\)', html_text)
            # Fallback to legacy pattern if new format is not found
            if not parts:
                parts = re.findall(r"__next_f\.push\(\[\d+,\\\"(.*?)\\\"\\]\)", html_text)
            if not parts:
                continue

            joined = "\n".join(parts)
            try:
                joined = joined.encode("utf-8").decode("unicode_escape")
            except Exception:
                pass

            for line in joined.splitlines():
                if not line.startswith("0:"):
                    continue
                payload = line[2:]
                try:
                    data = json.loads(payload)
                except Exception:
                    continue
                f = data.get("f") if isinstance(data, dict) else None
                if not (isinstance(f, list) and f):
                    continue
                first = f[0]
                if not (isinstance(first, list) and first):
                    continue
                tree = first[0]
                tree_json = json.dumps(tree, ensure_ascii=False)
                _HDHIVE_ROUTER_STATE_CACHE[slug] = (now, tree_json)
                return tree_json
    except Exception:
        return _HDHIVE_NEXT_ROUTER_STATE_TREE_JSON

    return _HDHIVE_NEXT_ROUTER_STATE_TREE_JSON


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

_download_queue_stats = {
    'queue_size': 0,
    'maxsize': 0,
    'queued_total': 0,
    'dropped_total': 0,
    'last_queued_at': '',
    'last_dropped_at': '',
    'last_drop_reason': '',
    'last_alert_at': '',
    'config_concurrency': 0,
    'effective_concurrency': 0,
    'throttle_active': False,
    'throttle_target_concurrency': 0,
    'throttle_threshold': 0,
    'throttle_recover_threshold': 0,
    'last_throttle_at': '',
    'last_throttle_reason': '',
    'updated_at': '',
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


def _persist_download_queue_stats():
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(DOWNLOAD_QUEUE_STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(_download_queue_stats, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _update_download_queue_stats(
    action: str,
    *,
    queue_size: Optional[int] = None,
    reason: str = "",
    apply_throttle: bool = True,
) -> None:
    if queue_size is None:
        try:
            queue_size = download_queue.qsize()
        except Exception:
            queue_size = 0
    _download_queue_stats['queue_size'] = int(queue_size or 0)
    _download_queue_stats['maxsize'] = int(DOWNLOAD_QUEUE_MAXSIZE or 0)
    now_str = time.strftime('%Y-%m-%d %H:%M:%S')
    action = (action or '').strip().lower()
    if action == 'enqueue':
        _download_queue_stats['queued_total'] = int(_download_queue_stats.get('queued_total', 0)) + 1
        _download_queue_stats['last_queued_at'] = now_str
    elif action == 'drop':
        _download_queue_stats['dropped_total'] = int(_download_queue_stats.get('dropped_total', 0)) + 1
        _download_queue_stats['last_dropped_at'] = now_str
        _download_queue_stats['last_drop_reason'] = (reason or '').strip()
    if apply_throttle:
        try:
            _maybe_auto_throttle(int(queue_size or 0))
        except Exception:
            pass

    throttle_cfg = _get_download_queue_throttle_config(current_config if isinstance(current_config, dict) else {})
    _download_queue_stats['config_concurrency'] = int(CONFIG_DOWNLOAD_CONCURRENCY or 0)
    _download_queue_stats['effective_concurrency'] = int(DOWNLOAD_CONCURRENCY or 0)
    _download_queue_stats['throttle_active'] = bool(
        DOWNLOAD_CONCURRENCY_OVERRIDE is not None and DOWNLOAD_CONCURRENCY_OVERRIDE < CONFIG_DOWNLOAD_CONCURRENCY
    )
    _download_queue_stats['throttle_target_concurrency'] = int(DOWNLOAD_CONCURRENCY_OVERRIDE or 0)
    _download_queue_stats['throttle_threshold'] = int(throttle_cfg.get('threshold', 0) or 0)
    _download_queue_stats['throttle_recover_threshold'] = int(throttle_cfg.get('recover_threshold', 0) or 0)
    _download_queue_stats['updated_at'] = now_str
    _persist_download_queue_stats()


def _parse_notify_targets(raw: str) -> List:
    items = []
    for part in re.split(r'[,\n]+', str(raw or '')):
        value = (part or '').strip()
        if not value:
            continue
        if value.isdigit():
            items.append(int(value))
        else:
            items.append(value)
    return items


def _get_download_queue_alert_config(cfg: dict) -> dict:
    alert_cfg = {}
    if isinstance(cfg, dict):
        alert_cfg = cfg.get('download_queue_alert') if isinstance(cfg.get('download_queue_alert'), dict) else {}
    enabled = bool(alert_cfg.get('enabled', False))
    threshold = int(alert_cfg.get('threshold', 100) or 100)
    cooldown = int(alert_cfg.get('cooldown_seconds', 600) or 600)
    notify_ids_raw = (alert_cfg.get('notify_user_ids') or '').strip()
    if not notify_ids_raw:
        notify_ids_raw = (cfg.get('self_service_notify_user_ids') or cfg.get('self_service_target_user_ids') or '').strip() if isinstance(cfg, dict) else ''
    targets = _parse_notify_targets(notify_ids_raw)
    return {
        'enabled': enabled,
        'threshold': max(1, threshold),
        'cooldown_seconds': max(30, cooldown),
        'targets': targets,
    }


def _get_download_queue_throttle_config(cfg: dict) -> dict:
    throttle_cfg = {}
    if isinstance(cfg, dict):
        throttle_cfg = cfg.get('download_queue_throttle') if isinstance(cfg.get('download_queue_throttle'), dict) else {}

    enabled = bool(throttle_cfg.get('enabled', True))
    threshold = int(throttle_cfg.get('threshold', 0) or 0)
    if threshold <= 0:
        threshold = int(_get_download_queue_alert_config(cfg).get('threshold', 100) or 100)

    min_concurrency = int(throttle_cfg.get('min_concurrency', 1) or 1)
    recover_threshold = int(throttle_cfg.get('recover_threshold', 0) or 0)
    if recover_threshold <= 0 or recover_threshold >= threshold:
        recover_threshold = max(1, int(threshold * 0.7))
        if recover_threshold >= threshold:
            recover_threshold = max(1, threshold - 1)

    return {
        'enabled': enabled,
        'threshold': max(1, threshold),
        'recover_threshold': max(0, recover_threshold),
        'min_concurrency': max(1, min_concurrency),
    }


def _compute_throttled_concurrency(queue_size: int, base_concurrency: int, threshold: int, min_concurrency: int) -> Optional[int]:
    if queue_size < threshold:
        return None
    if base_concurrency <= 1:
        return None
    if threshold <= 0:
        return None
    desired = int((base_concurrency * threshold) // max(queue_size, 1))
    desired = max(min_concurrency, min(base_concurrency, desired))
    if desired >= base_concurrency:
        return None
    return max(1, desired)


def _maybe_auto_throttle(queue_size: int) -> None:
    cfg = _get_download_queue_throttle_config(current_config if isinstance(current_config, dict) else {})
    if not cfg.get('enabled'):
        if DOWNLOAD_CONCURRENCY_OVERRIDE is not None:
            _set_download_concurrency_override(
                None,
                queue_size=queue_size,
                threshold=int(cfg.get('threshold', 0) or 0),
                announce=False,
                reason="自动降速已关闭",
                persist_stats=False,
            )
        return

    base = int(CONFIG_DOWNLOAD_CONCURRENCY or 0)
    threshold = int(cfg.get('threshold', 0) or 0)
    recover_threshold = int(cfg.get('recover_threshold', 0) or 0)
    min_concurrency = int(cfg.get('min_concurrency', 1) or 1)

    if base <= 1:
        if DOWNLOAD_CONCURRENCY_OVERRIDE is not None:
            _set_download_concurrency_override(
                None,
                queue_size=queue_size,
                threshold=threshold,
                announce=False,
                reason="并发已是最低值",
                persist_stats=False,
            )
        return

    if queue_size >= threshold:
        desired = _compute_throttled_concurrency(queue_size, base, threshold, min_concurrency)
        if desired is None:
            if DOWNLOAD_CONCURRENCY_OVERRIDE is not None:
                _set_download_concurrency_override(
                    None,
                    queue_size=queue_size,
                    threshold=threshold,
                    announce=False,
                    reason="无需降速",
                    persist_stats=False,
                )
            return
        if DOWNLOAD_CONCURRENCY_OVERRIDE != desired:
            _set_download_concurrency_override(
                desired,
                queue_size=queue_size,
                threshold=threshold,
                announce=True,
                reason=f"队列积压 {queue_size}/{DOWNLOAD_QUEUE_MAXSIZE}",
                persist_stats=False,
            )
        return

    if queue_size <= recover_threshold and DOWNLOAD_CONCURRENCY_OVERRIDE is not None:
        _set_download_concurrency_override(
            None,
            queue_size=queue_size,
            threshold=threshold,
            announce=True,
            reason=f"队列回落 {queue_size}/{DOWNLOAD_QUEUE_MAXSIZE}",
            persist_stats=False,
        )


def _schedule_download_queue_alert(queue_size: int) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    cfg = _get_download_queue_alert_config(current_config if isinstance(current_config, dict) else {})
    if not cfg.get('enabled'):
        return
    if queue_size < cfg.get('threshold', 0):
        return
    targets = cfg.get('targets') or []
    if not targets:
        return
    now_ts = time.time()
    last_alert_ts = float(_download_queue_stats.get('last_alert_ts', 0) or 0)
    if now_ts - last_alert_ts < cfg.get('cooldown_seconds', 600):
        return
    _download_queue_stats['last_alert_ts'] = now_ts
    _download_queue_stats['last_alert_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
    _persist_download_queue_stats()
    message = (
        f"⚠️ 下载队列积压: {queue_size}/{DOWNLOAD_QUEUE_MAXSIZE}\n"
        f"时间: {_download_queue_stats.get('last_alert_at')}"
    )
    loop.create_task(_send_download_queue_alert(targets, message))


async def _send_download_queue_alert(targets: List, message: str) -> None:
    if not targets:
        return
    if not client or not client.is_connected():
        return
    for tid in targets:
        try:
            await client.send_message(tid, message)
        except Exception as e:
            log_message(f"下载队列告警发送失败: target={tid} err={e}")


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


def _normalize_hdhive_cookie(raw_cookie: str) -> str:
    if not raw_cookie:
        return ''
    cleaned = str(raw_cookie).replace('\r', ';').replace('\n', ';').replace('\t', ';')
    cleaned = re.sub(r';{2,}', ';', cleaned)
    return cleaned.strip(' ;')


def _get_hdhive_cookie_header() -> str:
    try:
        cookie = (current_config or {}).get('hdhive_cookie', '')  # type: ignore[name-defined]
        return _normalize_hdhive_cookie(cookie)
    except Exception:
        return ''


def _get_hdhive_base_url() -> str:
    try:
        base_url = (current_config or {}).get('hdhive_base_url', '')  # type: ignore[name-defined]
    except Exception:
        base_url = ''
    base_url = (base_url or 'https://hdhive.com').strip()
    if not base_url:
        base_url = 'https://hdhive.com'
    return base_url.rstrip('/')


def _get_hdhive_open_api_key() -> str:
    try:
        return ((current_config or {}).get('hdhive_open_api_key', '') or '').strip()  # type: ignore[name-defined]
    except Exception:
        return ''


def _hdhive_open_api_request(method: str, url: str, api_key: str, json_body: dict = None, timeout: int = 20) -> dict:
    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json",
    }
    if method.upper() in ("POST", "PATCH"):
        headers["Content-Type"] = "application/json"
    try:
        resp = _requests_request(method, url, headers=headers, json=json_body, timeout=timeout, proxy_scope="service")
    except Exception as e:
        return {"success": False, "code": "NETWORK_ERROR", "message": f"{type(e).__name__}: {e}"}
    try:
        data = resp.json()
    except Exception:
        data = None
    if isinstance(data, dict):
        return data

    try:
        raw_text = (resp.text or "").strip()
    except Exception:
        raw_text = ""
    compact_text = re.sub(r"\s+", " ", raw_text)[:240]
    if "Attention Required!" in compact_text and "Cloudflare" in compact_text:
        compact_text = "Cloudflare challenge blocked request"
    if not compact_text:
        compact_text = "Invalid JSON response"
    return {"success": False, "code": str(resp.status_code), "message": compact_text}


def _hdhive_open_api_unlock(base_url: str, api_key: str, slug: str) -> dict:
    api_base = base_url.rstrip("/") + "/api/open"
    url = f"{api_base}/resources/unlock"
    return _hdhive_open_api_request("POST", url, api_key, json_body={"slug": slug})


def _hdhive_open_api_resource_detail(base_url: str, api_key: str, slug: str) -> dict:
    api_base = base_url.rstrip("/") + "/api/open"
    legacy_url = f"{api_base}/resources/{slug}"
    legacy_resp = _hdhive_open_api_request("GET", legacy_url, api_key)
    if isinstance(legacy_resp, dict) and legacy_resp.get("success") is True:
        return legacy_resp

    detail_url = f"{api_base}/resources/detail/{slug}"
    detail_resp = _hdhive_open_api_request("GET", detail_url, api_key)
    if isinstance(detail_resp, dict) and detail_resp.get("success") is True:
        return detail_resp

    legacy_msg = str((legacy_resp or {}).get("message") or "").strip()
    detail_msg = str((detail_resp or {}).get("message") or (detail_resp or {}).get("description") or "").strip()
    if detail_msg and detail_msg != "Invalid JSON response":
        return detail_resp
    if legacy_msg:
        return legacy_resp
    return detail_resp if isinstance(detail_resp, dict) else legacy_resp


def _hdhive_open_api_extract_unlock_points(data: dict) -> Optional[int]:
    if not isinstance(data, dict):
        return None
    for key in ("unlock_points", "unlockPoints", "points", "unlock_point", "unlockPoint"):
        if key in data:
            try:
                return int(data.get(key))
            except Exception:
                return None
    unlock_info = data.get("unlock") if isinstance(data.get("unlock"), dict) else None
    if unlock_info:
        for key in ("points", "unlock_points", "unlockPoints"):
            if key in unlock_info:
                try:
                    return int(unlock_info.get(key))
                except Exception:
                    return None
    return None


def _build_hdhive_full_url(data: dict) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    full_url = data.get("full_url") or ""
    if isinstance(full_url, str) and full_url.startswith("http"):
        return _normalize_115_url(full_url)
    url = data.get("url") or ""
    access_code = data.get("access_code")
    if isinstance(url, str) and url.startswith("http"):
        if isinstance(access_code, str) and access_code and access_code not in url:
            sep = '&' if '?' in url else '?'
            url = f"{url}{sep}password={access_code}"
        return _normalize_115_url(url)
    return None

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
        'Origin': 'https://hdhive.com',
        'Referer': f'https://hdhive.com{page_path}',
    }

    # Next.js adds a cache-busting param `_rsc`; value doesn't matter.
    rsc_token = int(time.time() * 1000)
    body = json.dumps(action_args, ensure_ascii=False)
    rsc_url = f"https://hdhive.com/?_rsc={rsc_token}"
    resp = _requests_post(rsc_url, data=body.encode('utf-8'), timeout=25, headers=headers, proxy_scope="service")
    parsed = _parse_next_action_rsc_result(resp.text)
    if parsed is not None:
        return parsed
    # Fallback: try page path as endpoint (some deployments require scoped URL)
    try:
        alt_url = f"https://hdhive.com{page_path}?_rsc={rsc_token}"
        resp2 = _requests_post(alt_url, data=body.encode('utf-8'), timeout=25, headers=headers, proxy_scope="service")
        return _parse_next_action_rsc_result(resp2.text)
    except Exception:
        return None


def _force_refresh_hdhive_state(slug: str):
    global _HDHIVE_ACTION_IDS_LAST_REFRESH_TS
    _HDHIVE_ACTION_IDS_LAST_REFRESH_TS = 0
    try:
        _HDHIVE_ROUTER_STATE_CACHE.pop(slug, None)
    except Exception:
        pass
    _refresh_hdhive_action_ids_if_needed()


def _hdhive_decrypt_sync(cookie_header: str, slug: str, ciphertext: str):
    _refresh_hdhive_action_ids_if_needed()
    page_path = f"/resource/115/{slug}"
    router_state_tree_json = _get_hdhive_router_state_tree_json(slug)
    result = _hdhive_next_action_call_sync(
        cookie_header=cookie_header,
        action_id=HDHIVE_ACTION_DECRYPT_ID,
        page_path=page_path,
        router_state_tree_json=router_state_tree_json,
        action_args=[ciphertext],
    )
    if result is None:
        _force_refresh_hdhive_state(slug)
        router_state_tree_json = _get_hdhive_router_state_tree_json(slug)
        result = _hdhive_next_action_call_sync(
            cookie_header=cookie_header,
            action_id=HDHIVE_ACTION_DECRYPT_ID,
            page_path=page_path,
            router_state_tree_json=router_state_tree_json,
            action_args=[ciphertext],
        )
    return result


def _hdhive_encrypte_sync(cookie_header: str, slug: str, plaintext_json: str):
    _refresh_hdhive_action_ids_if_needed()
    page_path = f"/resource/115/{slug}"
    router_state_tree_json = _get_hdhive_router_state_tree_json(slug)
    result = _hdhive_next_action_call_sync(
        cookie_header=cookie_header,
        action_id=HDHIVE_ACTION_ENCRYPTE_ID,
        page_path=page_path,
        router_state_tree_json=router_state_tree_json,
        action_args=[plaintext_json],
    )
    if result is None:
        _force_refresh_hdhive_state(slug)
        router_state_tree_json = _get_hdhive_router_state_tree_json(slug)
        result = _hdhive_next_action_call_sync(
            cookie_header=cookie_header,
            action_id=HDHIVE_ACTION_ENCRYPTE_ID,
            page_path=page_path,
            router_state_tree_json=router_state_tree_json,
            action_args=[plaintext_json],
        )
    return result


def _hdhive_go_api_get_url_info_sync(cookie_header: str, slug: str) -> Optional[dict]:
    # Build encrypted query from {slug, utctimestamp}
    payload = json.dumps({
        'slug': slug,
        'utctimestamp': int(time.time()),
    }, ensure_ascii=False)
    encrypted_query = _hdhive_encrypte_sync(cookie_header, slug, payload)
    if not isinstance(encrypted_query, str) or not encrypted_query:
        return {
            "__error": "加密参数生成失败"
        }

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json,*/*',
        'Cookie': cookie_header,
        'Origin': 'https://hdhive.com',
        'Referer': f'https://hdhive.com/resource/115/{slug}',
    }
    url = f"https://hdhive.com/go-api/customer/resources/{slug}/url"
    resp = _requests_get(url, params={'query': encrypted_query}, timeout=25, headers=headers, proxy_scope="service")
    try:
        raw = resp.json()
    except Exception:
        return None
    if isinstance(raw, dict) and raw.get('success') is False:
        msg = raw.get('message') or raw.get('description') or "go-api error"
        data = {"__error": msg}
        try:
            m = re.search(r"(\d+)\s*积分", str(msg))
            if m:
                data["__unlock_points"] = int(m.group(1))
        except Exception:
            pass
        return data
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
        'Origin': 'https://hdhive.com',
        'Referer': f'https://hdhive.com/resource/115/{slug}',
    }
    url = f"https://hdhive.com/go-api/customer/resources/{slug}/unlock"
    if isinstance(encrypted_body, str) and encrypted_body:
        resp = _requests_post(url, json={'data': encrypted_body}, timeout=25, headers=headers, proxy_scope="service")
    else:
        return {
            "__error": "加密参数生成失败"
        }
    try:
        raw = resp.json()
    except Exception:
        return None
    if isinstance(raw, dict) and raw.get('success') is False:
        return {
            "__error": raw.get('message') or raw.get('description') or "go-api error"
        }
    ciphertext = raw.get('data') if isinstance(raw, dict) else None
    if not ciphertext:
        return None
    decrypted = _hdhive_decrypt_sync(cookie_header, slug, ciphertext)
    return decrypted if isinstance(decrypted, dict) else None


def _hdhive_unlock_resource_sync(cookie_header: str, slug: str) -> Optional[dict]:
    """Unlock via Next.js server action (unlockResource)."""
    unlock_id = _refresh_hdhive_unlock_action_id_if_needed(slug)
    if not unlock_id:
        return {
            "__error": "无法获取解锁 action id"
        }
    page_path = f"/resource/115/{slug}"
    router_state_tree_json = _get_hdhive_router_state_tree_json(slug)
    result = _hdhive_next_action_call_sync(
        cookie_header=cookie_header,
        action_id=unlock_id,
        page_path=page_path,
        router_state_tree_json=router_state_tree_json,
        action_args=[slug],
    )
    # Expected shape: {"response": {"success": True/False, "data": {...}, "message": "...", ...}}
    if isinstance(result, dict) and isinstance(result.get("response"), dict):
        resp = result.get("response") or {}
        if resp.get("success") is True:
            data = resp.get("data") or {}
            if isinstance(data, dict):
                return data
            return {"__error": "解锁返回格式异常"}
        msg = resp.get("message") or resp.get("description") or "unlock error"
        return {"__error": msg}
    return {"__error": "解锁响应异常"}


def _normalize_115_url(u: str) -> str:
    if not u:
        return u
    u = html.unescape(u).strip()
    # Some strings may contain trailing fragments/artifacts from templates
    u = u.replace('\\', '')
    # Strip trailing status suffix like ;307 (often appended by redirectors)
    # Apply for both plain URLs and ones with query/fragment.
    u = re.sub(r';\d+(?=[?#]|$)', '', u)
    while u.endswith('&#') or u.endswith('&'):
        u = u[:-1]
    if u.endswith('#') and '?#' not in u:
        u = u[:-1]
    # Clean stray trailing semicolons
    u = re.sub(r';+$', '', u)

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
    urls: List[str] = []
    for m in HDHIVE_115_URL_RE.finditer(text):
        u = m.group(0)
        if not u:
            continue
        if not u.lower().startswith('http'):
            u = f"https://{u}"
        urls.append(u)
    return list(dict.fromkeys(urls))


def _extract_hdhive_hits_from_message(msg) -> List[HdhiveLinkHit]:
    hits: List[HdhiveLinkHit] = []
    text = getattr(msg, 'message', None) or ""

    for url in _extract_hdhive_urls_from_text(text):
        hits.append(HdhiveLinkHit(hdhive_url=url, display_text=url, source='text'))

    entities = getattr(msg, 'entities', None) or []
    for ent in entities:
        url = getattr(ent, 'url', None)
        try:
            offset = int(getattr(ent, 'offset', 0))
            length = int(getattr(ent, 'length', 0))
            display = text[offset:offset + length] if length else url
        except Exception:
            display = url
        candidate = url or display
        if not candidate:
            continue
        if not HDHIVE_115_URL_RE.search(candidate):
            continue
        if not candidate.lower().startswith('http'):
            candidate = f"https://{candidate}"
        hits.append(HdhiveLinkHit(hdhive_url=candidate, display_text=display or candidate, source='entity'))

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
                if not btn_url.lower().startswith('http'):
                    btn_url = f"https://{btn_url}"
                hits.append(HdhiveLinkHit(hdhive_url=btn_url, display_text=btn_text, source='button'))

    # Webpage preview URL (when message text is empty but has a link preview)
    try:
        if isinstance(getattr(msg, 'media', None), MessageMediaWebPage):
            wp = getattr(msg.media, 'webpage', None)
            wp_url = getattr(wp, 'url', None) if wp else None
            wp_display = getattr(wp, 'display_url', None) if wp else None
            for candidate in (wp_url, wp_display):
                if candidate and HDHIVE_115_URL_RE.search(candidate):
                    if not candidate.lower().startswith('http'):
                        candidate = f"https://{candidate}"
                    hits.append(HdhiveLinkHit(hdhive_url=candidate, display_text=candidate, source='webpage'))
                    break
    except Exception:
        pass

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
            if '115.com' in resp.url or '115cdn' in resp.url:
                return _normalize_115_url(resp.url)
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
                            return _normalize_115_url(u)
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
            return _normalize_115_url(candidates[0])

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

            locked_required = False
            unlock_failed = False

            api_error = None
            unlock_points = None
            if isinstance(url_info, dict) and url_info.get("__error"):
                api_error = str(url_info.get("__error") or "").strip()
                try:
                    if url_info.get("__unlock_points") is not None:
                        unlock_points = int(url_info.get("__unlock_points"))
                except Exception:
                    unlock_points = unlock_points

            if isinstance(url_info, dict):
                unlock_points_raw = url_info.get('unlock_points')
                try:
                    unlock_points = int(unlock_points_raw) if unlock_points_raw is not None else None
                except Exception:
                    unlock_points = None

                full_url = url_info.get('full_url')
                url = url_info.get('url')
                access_code = url_info.get('access_code')
                already_owned = bool(url_info.get('already_owned', False))

                if unlock_points is not None and unlock_points > 0 and not already_owned:
                    locked_required = True

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
                    unlocked = _hdhive_unlock_resource_sync(cookie, slug)
                    if isinstance(unlocked, dict) and unlocked.get("__error"):
                        log_message(f"警告: HDHive 自动解锁失败（{unlocked.get('__error')}）")
                        unlock_failed = True
                    elif isinstance(unlocked, dict):
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
                            unlock_failed = True
                    else:
                        log_message("警告: HDHive 自动解锁失败（可能积分不足/资源失效/登录态异常）。")
                        unlock_failed = True

                # If resource is locked and unlock did not succeed, avoid returning placeholder links
                if locked_required and (unlock_failed or not should_try_unlock):
                    real_url = None

                # If API error occurred and no unlock points, avoid trusting redirect placeholders
                if api_error and unlock_points is None:
                    real_url = None
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
    open_api_key = _get_hdhive_open_api_key()
    base_url = _get_hdhive_base_url()
    open_api_note = ""
    allow_open_api_direct = False
    try:
        allow_open_api_direct = bool((current_config or {}).get('hdhive_open_api_direct_unlock', False))  # type: ignore[name-defined]
    except Exception:
        allow_open_api_direct = False
    if open_api_key:
        threshold = 0
        try:
            threshold = int((current_config or {}).get('hdhive_auto_unlock_points_threshold', 0) or 0)  # type: ignore[name-defined]
        except Exception:
            threshold = 0
        skip_open_api_unlock = False
        allow_direct_unlock_without_detail = False
        detail_resp = _hdhive_open_api_resource_detail(base_url, open_api_key, slug)
        detail_data = detail_resp.get("data") if isinstance(detail_resp, dict) else None
        unlock_points = None
        already_owned = False
        if isinstance(detail_data, dict):
            real = _build_hdhive_full_url(detail_data)
            already_owned = bool(detail_data.get("already_owned") or detail_data.get("is_unlocked"))
            if real and already_owned:
                return real, "Open API：已拥有该资源"
            unlock_points = _hdhive_open_api_extract_unlock_points(detail_data)
            if real and unlock_points == 0:
                return real, "Open API：免积分资源，解析成功"
        else:
            detail_code = str((detail_resp or {}).get("code") or "").strip()
            detail_msg = str((detail_resp or {}).get("description") or (detail_resp or {}).get("message") or "").strip()
            detail_reason = " ".join(part for part in (detail_code, detail_msg) if part).strip()
            if allow_open_api_direct:
                allow_direct_unlock_without_detail = True
                if detail_reason:
                    open_api_note = f"Open API：详情接口不可用（{detail_reason}），尝试直接解锁"
                else:
                    open_api_note = "Open API：详情接口不可用，尝试直接解锁"
            else:
                if detail_reason:
                    open_api_note = f"Open API：详情接口不可用（{detail_reason}），改用 Cookie 判定"
                else:
                    open_api_note = "Open API：未获取积分信息，改用 Cookie 判定"
                skip_open_api_unlock = True

        should_try_open_api_unlock = False
        if not skip_open_api_unlock:
            if allow_direct_unlock_without_detail:
                should_try_open_api_unlock = True
            elif already_owned:
                should_try_open_api_unlock = True
            elif unlock_points is None:
                open_api_note = f"Open API：未获取积分信息，已跳过解锁（阈值 {threshold}）"
                skip_open_api_unlock = True
            elif unlock_points == 0:
                should_try_open_api_unlock = True
            elif not allow_open_api_direct:
                open_api_note = "Open API 直链解锁已关闭"
                skip_open_api_unlock = True
            elif unlock_points > threshold:
                open_api_note = f"Open API：需要 {unlock_points} 积分 > 阈值 {threshold}，已跳过解锁"
                skip_open_api_unlock = True
            else:
                should_try_open_api_unlock = True

        if skip_open_api_unlock:
            # fallback to cookie-based resolve below
            pass
        elif should_try_open_api_unlock:
            resp = _hdhive_open_api_unlock(base_url, open_api_key, slug)
            if isinstance(resp, dict) and resp.get("success") is True:
                data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                real = _build_hdhive_full_url(data)
                already_owned = bool(data.get("already_owned", False)) if isinstance(data, dict) else False
                msg = str(resp.get("message") or "解锁成功")
                if real:
                    if already_owned:
                        return real, "Open API：已拥有该资源"
                    if allow_direct_unlock_without_detail:
                        return real, f"{open_api_note}；Open API：{msg}"
                    return real, f"Open API：{msg}"
                open_api_note = f"Open API：{msg}，但未返回链接"
            else:
                code = str((resp or {}).get("code") or "")
                msg = str((resp or {}).get("message") or (resp or {}).get("description") or "解锁失败")
                if code.upper() == "INSUFFICIENT_POINTS" or code == "402":
                    return None, f"Open API：积分不足（{msg}）"
                if code == "404":
                    return None, f"Open API：资源不存在（{msg}）"
                if code == "400":
                    return None, f"Open API：资源参数无效（{msg}）"
                if code == "401":
                    open_api_note = f"Open API：认证失败（{msg}）"
                elif code == "NETWORK_ERROR":
                    open_api_note = f"Open API：网络错误（{msg}）"
                else:
                    open_api_note = f"Open API：解锁失败（{msg}）"

    cookie = _get_hdhive_cookie_header()
    if not cookie:
        if open_api_note:
            return None, f"{open_api_note}，且未配置 HDHive Cookie"
        return None, "未配置 HDHive Cookie，无法解析/解锁"

    threshold = 0
    try:
        threshold = int((current_config or {}).get('hdhive_auto_unlock_points_threshold', 0) or 0)  # type: ignore[name-defined]
    except Exception:
        threshold = 0

    # 1) Try read url info (retry once when "not found")
    url_info = None
    api_error = None
    unlock_points = None
    already_owned = False
    locked_required = False

    for attempt in range(2):
        try:
            url_info = _hdhive_go_api_get_url_info_sync(cookie, slug)
        except Exception:
            url_info = None
        api_error = None
        unlock_points = None
        already_owned = False
        locked_required = False

        if isinstance(url_info, dict):
            if url_info.get("__error"):
                api_error = str(url_info.get("__error") or "").strip()
                # Try extract points from error message
                try:
                    if url_info.get("__unlock_points") is not None:
                        unlock_points = int(url_info.get("__unlock_points"))
                except Exception:
                    unlock_points = unlock_points
            try:
                if url_info.get('unlock_points') is not None:
                    unlock_points = int(url_info.get('unlock_points'))
            except Exception:
                unlock_points = None
            already_owned = bool(url_info.get('already_owned', False))
            if unlock_points is not None and unlock_points > 0 and not already_owned:
                locked_required = True

            full_url = url_info.get('full_url')
            url = url_info.get('url')
            access_code = url_info.get('access_code')

            if isinstance(full_url, str) and full_url.startswith('http'):
                real = _normalize_115_url(full_url)
                note = "已解锁，解析成功"
                if open_api_note:
                    note = f"{open_api_note}；{note}"
                return real, note

            if isinstance(url, str) and url.startswith('http'):
                if isinstance(access_code, str) and access_code and access_code not in url:
                    sep = '&' if '?' in url else '?'
                    real = _normalize_115_url(f"{url}{sep}password={access_code}")
                else:
                    real = _normalize_115_url(url)
                if already_owned:
                    note = "已解锁，解析成功"
                    if open_api_note:
                        note = f"{open_api_note}；{note}"
                    return real, note
                if unlock_points == 0:
                    note = "免积分资源，解析成功"
                    if open_api_note:
                        note = f"{open_api_note}；{note}"
                    return real, note

            # Locked case: has points requirement but not owned and no full_url
            if unlock_points is not None and unlock_points > 0 and not already_owned:
                if unlock_points > threshold:
                    return None, f"需要 {unlock_points} 积分 > 阈值 {threshold}，未自动解锁"

        if api_error and unlock_points is None and "找不到记录" in api_error and attempt == 0:
            _force_refresh_hdhive_state(slug)
            continue
        break

    if api_error and unlock_points is None:
        note = f"解析失败（{api_error}，可能登录失效/接口变更）"
        if open_api_note:
            note = f"{open_api_note}；{note}"
        return None, note

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
            unlocked = _hdhive_unlock_resource_sync(cookie, slug)
        except Exception:
            unlocked = None
        if isinstance(unlocked, dict) and unlocked.get("__error"):
            return None, f"尝试自动解锁失败（{unlocked.get('__error')}）"
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
        note = "解析失败（可能登录失效/接口变更）"
        if open_api_note:
            note = f"{open_api_note}；{note}"
        return None, note


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
        # If text or web preview hints HDHive but regex didn't match, log for diagnostics
        hint_sources = []
        if original_text:
            hint_sources.append(original_text)
        try:
            if isinstance(getattr(msg, 'media', None), MessageMediaWebPage):
                wp = getattr(msg.media, 'webpage', None)
                wp_url = getattr(wp, 'url', None) if wp else None
                wp_display = getattr(wp, 'display_url', None) if wp else None
                if wp_url:
                    hint_sources.append(str(wp_url))
                if wp_display:
                    hint_sources.append(str(wp_display))
        except Exception:
            pass
        hint_text = " | ".join(hint_sources)
        if 'hdhive' in hint_text.lower() or '/resource/' in hint_text.lower():
            log_message(f"[HDHive] 未匹配到链接，text='{original_text[:200]}'")
        return False, original_text

    log_message(f"[HDHive] 命中 {len(hits)} 个链接，开始解析。")

    resolved_map: Dict[str, Optional[str]] = {}
    note_map: Dict[str, str] = {}
    for hit in hits:
        if hit.hdhive_url in resolved_map:
            continue
        real_url, note = await resolve_hdhive_115_url_with_note(hit.hdhive_url)
        log_message(f"[HDHive] 解析结果: {hit.hdhive_url} -> {real_url or 'None'} | {note}")
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
            # Avoid repeating the original URL if it already exists in the message text.
            if hit.hdhive_url and hit.hdhive_url in new_text:
                summary_lines.append(f"⚠️ 未解析（{note}）")
            else:
                summary_lines.append(f"⚠️ 未解析（{note}）：{hit.hdhive_url}")

    summary_lines = list(dict.fromkeys(summary_lines))
    if summary_lines:
        new_text = (new_text + "\n\n【HDHive解析】\n" + "\n".join(summary_lines)).strip()

    return True, new_text


def _should_send_copy_instead_of_forward(
    media_type_detected: str,
    convert_hdhive_enabled: bool,
    has_hdhive: bool,
) -> bool:
    """Decide whether to resend a rewritten copy instead of forwarding the original message.

    When HDHive conversion is enabled but the message does not actually contain an HDHive
    link, we should preserve the original forward behavior so non-HDHive links/buttons
    are not stripped by send_message/send_file.
    """
    if has_hdhive:
        return True
    if convert_hdhive_enabled:
        return False
    return media_type_detected == 'text'

async def message_queue_loop():
    """Background loop to send pending messages from app.py"""
    log_message("消息发送队列轮询已启动。")
    while True:
        try:
            try:
                sender_mode = str((current_config or {}).get('self_service_notify_sender', 'telegram_monitor')).lower()
            except Exception:
                sender_mode = 'telegram_monitor'
            if sender_mode not in ('telegram_monitor', 'userbot', 'telegram', 'tg'):
                await asyncio.sleep(3)
                continue

            if client is None or not client.is_connected():
                await asyncio.sleep(2)
                continue

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
_CLIENT_CONNECT_LOCK = None


def _get_client_connect_lock() -> asyncio.Lock:
    global _CLIENT_CONNECT_LOCK
    if _CLIENT_CONNECT_LOCK is None:
        _CLIENT_CONNECT_LOCK = asyncio.Lock()
    return _CLIENT_CONNECT_LOCK


async def _disconnect_client_safely(target_client) -> None:
    if target_client is None:
        return
    try:
        await target_client.disconnect()
    except Exception as e:
        debug_log(f"断开 Telegram 客户端时忽略异常: {e}")

# Semaphore for concurrency limiting
# Limit concurrent "expensive" operations (download/forward)
def _resolve_download_concurrency(cfg: dict) -> int:
    raw = None
    if isinstance(cfg, dict):
        raw = cfg.get('download_concurrency')
    if raw is None:
        raw = os.environ.get('TELEGRAM_DOWNLOAD_CONCURRENCY') or os.environ.get('DOWNLOAD_CONCURRENCY')
    try:
        val = int(raw)
    except Exception:
        val = 2
    return max(1, min(val, 8))


CONFIG_DOWNLOAD_CONCURRENCY = _resolve_download_concurrency(current_config)
DOWNLOAD_CONCURRENCY = CONFIG_DOWNLOAD_CONCURRENCY
DOWNLOAD_CONCURRENCY_OVERRIDE: Optional[int] = None
concurrency_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
download_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)


def _set_effective_download_concurrency(new_val: int, *, announce: bool = False, reason: str = "") -> None:
    global DOWNLOAD_CONCURRENCY, concurrency_semaphore, download_semaphore
    try:
        new_val = int(new_val)
    except Exception:
        new_val = 1
    new_val = max(1, new_val)
    if new_val == DOWNLOAD_CONCURRENCY:
        return
    DOWNLOAD_CONCURRENCY = new_val
    concurrency_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    download_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    _ensure_download_workers(DOWNLOAD_CONCURRENCY)
    if announce:
        suffix = f"{reason}" if reason else ""
        log_message(f"下载并发已更新: {DOWNLOAD_CONCURRENCY}{suffix}")


def _set_download_concurrency_override(
    new_override: Optional[int],
    *,
    queue_size: int = 0,
    threshold: int = 0,
    announce: bool = True,
    reason: str = "",
    persist_stats: bool = True,
) -> None:
    global DOWNLOAD_CONCURRENCY_OVERRIDE
    prev_override = DOWNLOAD_CONCURRENCY_OVERRIDE
    prev_effective = DOWNLOAD_CONCURRENCY

    if new_override is not None:
        try:
            new_override = int(new_override)
        except Exception:
            new_override = None
    if new_override is not None:
        new_override = max(1, new_override)
        if new_override > CONFIG_DOWNLOAD_CONCURRENCY:
            new_override = CONFIG_DOWNLOAD_CONCURRENCY

    if new_override == prev_override:
        return

    now_str = time.strftime('%Y-%m-%d %H:%M:%S')

    if new_override is None:
        DOWNLOAD_CONCURRENCY_OVERRIDE = None
        _set_effective_download_concurrency(CONFIG_DOWNLOAD_CONCURRENCY, announce=False)
        _download_queue_stats['last_throttle_at'] = now_str
        _download_queue_stats['last_throttle_reason'] = reason or "队列恢复"
        if announce and prev_override is not None:
            log_message(f"队列恢复，下载并发已恢复: {prev_effective} -> {DOWNLOAD_CONCURRENCY}")
    else:
        DOWNLOAD_CONCURRENCY_OVERRIDE = new_override
        new_effective = min(CONFIG_DOWNLOAD_CONCURRENCY, new_override)
        _set_effective_download_concurrency(new_effective, announce=False)
        _download_queue_stats['last_throttle_at'] = now_str
        if reason:
            _download_queue_stats['last_throttle_reason'] = reason
        else:
            _download_queue_stats['last_throttle_reason'] = f"队列积压 {queue_size}/{DOWNLOAD_QUEUE_MAXSIZE}"
        if announce:
            log_message(
                f"队列积压触发降速: {prev_effective} -> {DOWNLOAD_CONCURRENCY} "
                f"(queue={queue_size}/{DOWNLOAD_QUEUE_MAXSIZE}, threshold={threshold})"
            )

    if persist_stats:
        _update_download_queue_stats('concurrency', apply_throttle=False)


def _resolve_download_queue_maxsize(cfg: dict) -> int:
    raw = None
    if isinstance(cfg, dict):
        raw = cfg.get('download_queue_maxsize')
    if raw is None:
        raw = os.environ.get('TELEGRAM_DOWNLOAD_QUEUE_MAXSIZE') or os.environ.get('DOWNLOAD_QUEUE_MAXSIZE')
    try:
        val = int(raw)
    except Exception:
        val = 200
    return max(10, min(val, 5000))


def _apply_download_queue_config(cfg: dict, *, announce: bool = False) -> None:
    global DOWNLOAD_QUEUE_MAXSIZE, download_queue
    new_val = _resolve_download_queue_maxsize(cfg)
    if new_val == DOWNLOAD_QUEUE_MAXSIZE:
        return
    DOWNLOAD_QUEUE_MAXSIZE = new_val
    try:
        download_queue._maxsize = DOWNLOAD_QUEUE_MAXSIZE
    except Exception:
        pass
    _update_download_queue_stats('resize')
    if announce:
        log_message(f"下载队列上限已更新: {DOWNLOAD_QUEUE_MAXSIZE}")


DOWNLOAD_QUEUE_MAXSIZE = _resolve_download_queue_maxsize(current_config)
download_queue: asyncio.Queue = asyncio.Queue(maxsize=DOWNLOAD_QUEUE_MAXSIZE)
_download_workers: list = []
_update_download_queue_stats('init', queue_size=0)


def _ensure_download_workers(target_count: int) -> None:
    if target_count <= 0:
        target_count = 1
    # Only create/cancel workers when event loop is running.
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    current = len(_download_workers)
    if current < target_count:
        for idx in range(current + 1, target_count + 1):
            task = asyncio.create_task(_download_worker(idx))
            _download_workers.append(task)
        log_message(f"下载队列工作线程已扩展为 {target_count} 个")
    elif current > target_count:
        for _ in range(current - target_count):
            task = _download_workers.pop()
            task.cancel()
        log_message(f"下载队列工作线程已收缩为 {target_count} 个")


def _apply_download_concurrency(cfg: dict, *, announce: bool = False) -> None:
    global CONFIG_DOWNLOAD_CONCURRENCY, DOWNLOAD_CONCURRENCY_OVERRIDE
    new_val = _resolve_download_concurrency(cfg)
    if new_val == CONFIG_DOWNLOAD_CONCURRENCY and DOWNLOAD_CONCURRENCY_OVERRIDE is None:
        return
    CONFIG_DOWNLOAD_CONCURRENCY = new_val
    if DOWNLOAD_CONCURRENCY_OVERRIDE is not None and DOWNLOAD_CONCURRENCY_OVERRIDE > CONFIG_DOWNLOAD_CONCURRENCY:
        DOWNLOAD_CONCURRENCY_OVERRIDE = CONFIG_DOWNLOAD_CONCURRENCY
    effective = CONFIG_DOWNLOAD_CONCURRENCY
    reason = ""
    if DOWNLOAD_CONCURRENCY_OVERRIDE is not None:
        effective = min(CONFIG_DOWNLOAD_CONCURRENCY, DOWNLOAD_CONCURRENCY_OVERRIDE)
        if DOWNLOAD_CONCURRENCY_OVERRIDE < CONFIG_DOWNLOAD_CONCURRENCY:
            reason = " (队列积压降速中)"
    _set_effective_download_concurrency(effective, announce=announce, reason=reason)
    _update_download_queue_stats('concurrency', apply_throttle=False)


def _resolve_startup_tv_whitelist_scan_limit(cfg: dict) -> int:
    raw = None
    if isinstance(cfg, dict):
        raw = cfg.get('startup_tv_whitelist_scan_limit')
    if raw is None:
        raw = os.environ.get('STARTUP_TV_WHITELIST_SCAN_LIMIT') or os.environ.get('TG_STARTUP_TV_WHITELIST_SCAN_LIMIT')
    try:
        val = int(raw)
    except Exception:
        val = STARTUP_TV_WHITELIST_SCAN_LIMIT
    return max(0, min(val, 200))

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

    async with _get_client_connect_lock():
        # If client exists, always prefer reconnecting the same instance first.
        if client is not None:
            try:
                if client.is_connected():
                    if await client.is_user_authorized():
                        return True
                    log_message("现有客户端未授权。" )
                    await _disconnect_client_safely(client)
                    client = None
                else:
                    log_message("检测到现有 Telegram 客户端已断开，尝试复用会话重连...")
                    await client.connect()
                    if not await client.is_user_authorized():
                        log_message("Telethon 客户端未授权。请通过 Web UI 进行认证。" )
                        await _disconnect_client_safely(client)
                        client = None
                        return False
                    log_message("Telethon 客户端已连接并授权。" )
                    return True
            except FloodWaitError as e:
                log_message(f"遇到 FloodWaitError，等待 {e.seconds} 秒...")
                await asyncio.sleep(e.seconds)
                return False
            except Exception as e:
                log_message(f"复用现有 Telegram 会话重连失败: {e}")
                traceback.print_exc()
                stale_client = client
                client = None
                await _disconnect_client_safely(stale_client)

        # Attempt to create and connect client
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

        telethon_proxy = _build_telethon_proxy_from_config(current_config)
        if telethon_proxy:
            try:
                log_message(f"使用 Telegram 代理：{telethon_proxy[0]}:{telethon_proxy[1]}")
                client_args['proxy'] = telethon_proxy
            except Exception as e:
                log_message(f"解析 Telegram 代理配置失败，将不使用代理: {e}")
                traceback.print_exc()

        new_client = TelegramClient(**client_args) # Modified to use full path and proxy
        client = new_client
        try:
            log_message("尝试连接到 Telegram...")
            await new_client.connect()
            if not await new_client.is_user_authorized():
                log_message("Telethon 客户端未授权。请通过 Web UI 进行认证。" )
                await _disconnect_client_safely(new_client)
                if client is new_client:
                    client = None
                return False
            log_message("Telethon 客户端已连接并授权。" )
            return True
        except FloodWaitError as e:
            log_message(f"遇到 FloodWaitError，等待 {e.seconds} 秒...")
            await asyncio.sleep(e.seconds)
            await _disconnect_client_safely(new_client)
            if client is new_client:
                client = None
            return False
        except Exception as e:
            log_message(f"连接或授权 Telethon 客户端失败: {e}")
            traceback.print_exc() # Add this line
            await _disconnect_client_safely(new_client)
            if client is new_client:
                client = None
            return False

def _register_event_handlers():
    global _HANDLER_CLIENT
    if client is None:
        return
    if _HANDLER_CLIENT is client:
        return
    try:
        if _HANDLER_CLIENT is not None:
            _HANDLER_CLIENT.remove_event_handler(new_message_handler)
    except Exception:
        pass
    client.add_event_handler(new_message_handler, events.NewMessage())
    _HANDLER_CLIENT = client


async def keep_client_connected():
    reconnect_delay = 5
    while True:
        try:
            ok = await ensure_client_connected()
            if not ok:
                log_message(f"Telegram 客户端暂不可用，{reconnect_delay} 秒后重试。")
                await asyncio.sleep(reconnect_delay)
                continue

            _register_event_handlers()
            active_client = client
            try:
                await active_client.run_until_disconnected()
                log_message("Telegram 长连接已断开，准备自动重连...")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log_message(f"Telegram 长连接异常断开，将自动重连: {e}")
                traceback.print_exc()
            finally:
                if active_client is not None and active_client.is_connected():
                    await _disconnect_client_safely(active_client)
        except Exception as e:
            log_message(f"重连检查失败: {e}")
            traceback.print_exc()
        await asyncio.sleep(reconnect_delay)

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


async def reliable_download_action(action_name, coro_func, *args, **kwargs):
    """
    Executes a coroutine with automatic retry on FloodWaitError.
    Uses a dedicated download semaphore to avoid blocking message forwarding.
    Includes a timeout mechanism to prevent indefinite hanging.
    """
    timeout = kwargs.pop('timeout', 1800)
    max_retries = 5
    attempt = 0

    async with download_semaphore:
        while attempt < max_retries:
            try:
                return await asyncio.wait_for(coro_func(*args, **kwargs), timeout=timeout)
            except FloodWaitError as e:
                attempt += 1
                wait_time = e.seconds + 5
                log_message(f"[{action_name}] 遇到 FloodWaitError。等待 {wait_time} 秒后重试 (尝试次数: {attempt}/{max_retries})...")
                await asyncio.sleep(wait_time)
            except asyncio.TimeoutError:
                log_message(f"[{action_name}] 操作超时 ({timeout}秒)。放弃本次尝试。")
                return None
            except Exception as e:
                log_message(f"[{action_name}] 发生错误: {e}")
                raise e

        log_message(f"[{action_name}] 超过最大重试次数，放弃操作。")
        return None


def _enqueue_download_task(task: dict) -> tuple[bool, str]:
    if download_queue is None:
        return False, "queue_not_ready"
    try:
        download_queue.put_nowait(task)
        _update_download_queue_stats('enqueue')
        _schedule_download_queue_alert(download_queue.qsize())
        return True, "queued"
    except asyncio.QueueFull:
        _update_download_queue_stats('drop', reason='queue_full')
        return False, "queue_full"


async def _download_worker(worker_id: int):
    log_message(f"下载队列 Worker#{worker_id} 已启动")
    while True:
        try:
            task = await download_queue.get()
            _update_download_queue_stats('dequeue')
            try:
                await _handle_download_task(task)
            except Exception as e:
                log_message(f"下载队列 Worker#{worker_id} 处理异常: {e}")
            finally:
                download_queue.task_done()
        except asyncio.CancelledError:
            log_message(f"下载队列 Worker#{worker_id} 已停止")
            break
        except Exception as e:
            log_message(f"下载队列 Worker#{worker_id} 循环异常: {e}")
            await asyncio.sleep(1)


async def _handle_download_task(task: dict) -> None:
    msg = task.get('msg')
    if not msg:
        return
    restricted_entry = task.get('restricted_entry') or {}
    restricted_channel_id = task.get('restricted_channel_id')
    media_type_detected = task.get('media_type_detected') or 'unknown'
    chat_id = task.get('chat_id')

    download_directory = (task.get('download_directory') or restricted_entry.get('download_directory') or '').strip()
    if not download_directory:
        log_message(f"跳过下载 {media_type_detected}，因为未配置下载目录。")
        return

    try:
        os.makedirs(download_directory, exist_ok=True)
    except Exception as e:
        log_message(f"创建下载目录失败: {download_directory} err={e}")
        return

    # Smart filename generation
    base_name = str(msg.id)
    download_target = msg
    primary_document = _get_primary_document(msg)
    if isinstance(getattr(msg, 'media', None), MessageMediaWebPage) and primary_document:
        download_target = primary_document
    original_ext = ".mp4"
    original_filename = None
    expected_size = 0

    if media_type_detected == 'photo':
        original_ext = ".jpg"
    elif media_type_detected == 'audio':
        original_ext = ".mp3"

    if primary_document:
        expected_size = primary_document.size if hasattr(primary_document, 'size') else 0
        for attr in getattr(primary_document, 'attributes', []) or []:
            if hasattr(attr, 'file_name') and attr.file_name:
                original_filename = attr.file_name
                if '.' in original_filename:
                    original_ext = os.path.splitext(original_filename)[1].lower()
                break

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
                chat_id,
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
                    keywords = extract_keywords(album_caption, limit=30)
                    if keywords:
                        folder_name = keywords
                _album_folder_cache[grouped_id_key] = folder_name

            final_folder_path = os.path.join(download_directory, folder_name)
            os.makedirs(final_folder_path, exist_ok=True)

            try:
                type_sequence = []
                for m in album_msgs:
                    m_ext = ".jpg"
                    if m.video:
                        m_ext = ".mp4"
                    elif m.document:
                        if hasattr(m.document, 'mime_type') and m.document.mime_type:
                            m_ext = "." + m.document.mime_type.split('/')[-1]
                    if m_ext == original_ext:
                        type_sequence.append(m.id)

                if msg.id in type_sequence:
                    index = type_sequence.index(msg.id) + 1
                else:
                    index = len(type_sequence) + 1
            except ValueError:
                index = msg.id

            final_filename = f"{folder_name}_{index}{original_ext}"
            log_message(
                f"检测到相册消息 (Group: {msg.grouped_id})。按类型编号 - {media_type_detected}: {index}。"
                f"归档至: '{folder_name}/{final_filename}'"
            )
        except Exception as e:
            final_filename = f"{base_name}{original_ext}"
    if not final_filename:
        try:
            potential_name = msg.message or ""
            if potential_name:
                clean_name = sanitize_filename(potential_name, limit=60)
                if clean_name:
                    final_filename = f"{clean_name}{original_ext}"
                else:
                    final_filename = f"{base_name}{original_ext}"
            elif original_filename:
                clean_name = sanitize_filename(os.path.splitext(original_filename)[0], limit=60)
                if clean_name:
                    final_filename = f"{clean_name}{original_ext}"
                else:
                    final_filename = f"{base_name}{original_ext}"
            else:
                final_filename = f"{base_name}{original_ext}"
        except Exception:
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
        return

    try:
        progress_cb = None
        if expected_size > 50 * 1024 * 1024:
            progress_cb = create_progress_callback(file_path, media_type_detected)
        download_timeout = _compute_download_timeout_seconds(expected_size)
        log_message(
            f"下载超时设置 [{media_type_detected}]: {download_timeout}s "
            f"(文件大小: {expected_size / (1024 * 1024):.1f}MB)"
        )

        downloaded_file = await reliable_download_action(
            f"下载 {media_type_detected} {msg.id}",
            client.download_media,
            download_target,
            file=file_path,
            progress_callback=progress_cb,
            timeout=download_timeout,
        )

        if downloaded_file:
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
                if size_diff_percent > 5:
                    log_message(
                        f"警告: 下载文件大小异常 - 预期{expected_size/(1024*1024):.1f}MB，"
                        f"实际{actual_size/(1024*1024):.1f}MB (差异{size_diff_percent:.1f}%)"
                    )
                else:
                    log_message(
                        f"{media_type_detected} 已成功下载到 {downloaded_file} "
                        f"({actual_size/(1024*1024):.1f}MB)。{completion_detail_text}"
                    )
            else:
                log_message(f"{media_type_detected} 已成功下载到 {downloaded_file}。{completion_detail_text}")
        else:
            log_message(f"{media_type_detected} 下载失败。")
    except Exception as e:
        log_message(f"{media_type_detected} 下载异常: {e}")

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

@lru_cache(maxsize=1024)
def _compile_keyword_regex(pattern_str: str):
    try:
        return re.compile(pattern_str, re.IGNORECASE | re.DOTALL)
    except Exception:
        return None


def match_keyword(pattern, text, text_lower: Optional[str] = None):
    """Checks if a pattern (regex or string) matches the text."""
    if not pattern:
        return False
    if text is None:
        text = ""  # Ensure text is at least an empty string

    pattern_str = str(pattern).strip()
    if not pattern_str:
        return False
    text_str = str(text)
    if text_lower is None:
        text_lower = text_str.lower()

    try:
        # 1. 优先尝试完全匹配字符串（忽略大小写）
        if pattern_str.lower() in text_lower:
            return True

        # 2. 尝试作为正则表达式匹配
        regex = _compile_keyword_regex(pattern_str)
        if regex and regex.search(text_str):
            return True
    except Exception:
        # 如果正则解析失败，上面已经做了字符串包含检查，这里直接返回 False
        pass
    return False


AUTO_CLICK_HISTORY: Dict[Tuple[int, int], float] = {}
AUTO_CLICK_HISTORY_TTL_SECONDS = 30


def _normalize_keyword_list(value) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = [v.strip() for v in value.split(',')]
    else:
        items = [str(value).strip()]
    return [str(v).strip() for v in items if str(v).strip()]


def _get_auto_click_rules(restricted_entry: dict) -> Dict[str, List[str]]:
    keywords = _normalize_keyword_list(restricted_entry.get('auto_click_keywords'))
    button_texts = _normalize_keyword_list(restricted_entry.get('auto_click_button_texts'))
    notify_targets = _normalize_keyword_list(restricted_entry.get('auto_click_notify_targets'))

    # Backward-compatible quick toggle: auto_click_redpacket=true uses default keyword
    if restricted_entry.get('auto_click_redpacket') and not keywords:
        keywords = ['发了一个红包']

    return {
        "keywords": keywords,
        "button_texts": button_texts,
        "notify_targets": notify_targets,
    }


async def _extract_message_text_for_filter(msg, chat_id: int) -> str:
    message_text_for_filter = msg.message or ""
    if message_text_for_filter:
        return message_text_for_filter
    if not msg.grouped_id:
        return ""
    try:
        nearby_msgs = await client.get_messages(
            chat_id,
            limit=50,
            min_id=max(msg.id - 20, 0),
            max_id=msg.id + 20
        )
        nearby_msgs = list(nearby_msgs) if nearby_msgs else []
        for m in nearby_msgs:
            if m.grouped_id == msg.grouped_id and m.message:
                return m.message or ""
    except Exception:
        pass
    return ""


def _build_tv_whitelist_for_channel(channel_id: int, channel_filters_cfg: dict) -> List[str]:
    if not isinstance(channel_filters_cfg, dict):
        return []
    channel_filters = channel_filters_cfg.get('channels', {}) if isinstance(channel_filters_cfg, dict) else {}
    channel_rule = channel_filters.get(str(channel_id), {}) if isinstance(channel_filters, dict) else {}
    global_rule = channel_filters_cfg.get('global', {}) if isinstance(channel_filters_cfg, dict) else {}
    drama_rule = channel_filters_cfg.get('drama', {}) if isinstance(channel_filters_cfg, dict) else {}
    return (
        _normalize_keyword_list(global_rule.get('whitelist'))
        + _normalize_keyword_list(channel_rule.get('whitelist'))
        + _normalize_keyword_list(drama_rule.get('whitelist'))
    )


async def _startup_scan_tv_whitelist(limit: int = STARTUP_TV_WHITELIST_SCAN_LIMIT) -> None:
    if client is None:
        return
    restricted_channels = current_config.get('restricted_channels', [])
    if not restricted_channels:
        return
    channel_filters_cfg = load_channel_filters()

    for restricted_entry in restricted_channels:
        if not restricted_entry.get('use_tvchannel_filters'):
            continue
        restricted_channel_id = restricted_entry.get('channel_id')
        try:
            restricted_channel_id = int(restricted_channel_id)
        except (ValueError, TypeError):
            continue

        tv_whitelist = _build_tv_whitelist_for_channel(restricted_channel_id, channel_filters_cfg)
        if not tv_whitelist:
            continue

        try:
            recent_msgs = await client.get_messages(restricted_channel_id, limit=limit)
        except Exception as e:
            log_message(f"获取频道 {restricted_channel_id} 最近消息失败: {e}")
            continue

        if not recent_msgs:
            continue
        for msg in reversed(list(recent_msgs)):
            if not msg:
                continue
            message_text_for_filter = await _extract_message_text_for_filter(msg, restricted_channel_id)
            if not message_text_for_filter:
                continue
            message_text_lower = message_text_for_filter.lower()
            if not any(match_keyword(keyword, message_text_for_filter, message_text_lower) for keyword in tv_whitelist):
                continue
            event = SimpleNamespace(chat_id=restricted_channel_id, message=msg, chat=getattr(msg, 'chat', None))
            try:
                await new_message_handler(event, backfill=True)
            except Exception as e:
                log_message(f"回溯处理消息失败: ch={restricted_channel_id} msg={getattr(msg, 'id', '-')}, err={e}")


def _auto_click_recently(chat_id: int, msg_id: int) -> bool:
    now = time.time()
    # purge old entries
    stale_keys = []
    for k, ts in AUTO_CLICK_HISTORY.items():
        if now - ts > AUTO_CLICK_HISTORY_TTL_SECONDS:
            stale_keys.append(k)
    for k in stale_keys:
        AUTO_CLICK_HISTORY.pop(k, None)

    key = (chat_id, msg_id)
    ts = AUTO_CLICK_HISTORY.get(key)
    return bool(ts and (now - ts) < AUTO_CLICK_HISTORY_TTL_SECONDS)


def _mark_auto_clicked(chat_id: int, msg_id: int) -> None:
    AUTO_CLICK_HISTORY[(chat_id, msg_id)] = time.time()


def _iter_message_buttons(msg):
    buttons = getattr(msg, 'buttons', None)
    if not buttons:
        reply_markup = getattr(msg, 'reply_markup', None)
        rows = getattr(reply_markup, 'rows', None) if reply_markup else None
        if rows:
            buttons = []
            for row in rows:
                row_buttons = getattr(row, 'buttons', None) or []
                buttons.append(row_buttons)
    if not buttons:
        return
    for r_idx, row in enumerate(buttons):
        for c_idx, btn in enumerate(row or []):
            yield r_idx, c_idx, btn


def _build_message_link(event, msg) -> str:
    msg_id = getattr(msg, 'id', None)
    if not msg_id:
        return ''
    username = None
    try:
        username = getattr(event.chat, 'username', None)
    except Exception:
        username = None
    if username:
        return f"https://t.me/{username}/{msg_id}"

    chat_id = getattr(event, 'chat_id', None)
    try:
        chat_id = int(chat_id)
    except Exception:
        chat_id = None
    if chat_id is None:
        return ''
    chat_id_str = str(chat_id)
    if chat_id_str.startswith('-100') and len(chat_id_str) > 4:
        return f"https://t.me/c/{chat_id_str[4:]}/{msg_id}"
    return ''


async def _maybe_auto_click_buttons(
    event,
    msg,
    restricted_entry: dict,
    message_text_for_filter: str,
    *,
    fast_mode: bool = False,
) -> bool:
    rules = _get_auto_click_rules(restricted_entry)
    keywords = rules.get('keywords') or []
    button_texts = rules.get('button_texts') or []
    if not keywords and not button_texts:
        return False

    # Only handle incoming messages
    if getattr(event, 'out', False):
        return False

    if not fast_mode:
        if not message_text_for_filter:
            return False

        matched = False
        message_text_lower = message_text_for_filter.lower()
        for keyword in keywords:
            if match_keyword(keyword, message_text_for_filter, message_text_lower):
                matched = True
                break
        if not matched:
            return False

    chat_id = getattr(event, 'chat_id', None)
    msg_id = getattr(msg, 'id', None)
    if chat_id is None or msg_id is None:
        return False

    if _auto_click_recently(chat_id, msg_id):
        debug_log(f" 自动点击已触发过，跳过: ch={chat_id} msg={msg_id}")
        return False

    candidate = None

    if fast_mode:
        message_text_lower = (message_text_for_filter or '').lower()
        if button_texts:
            for r_idx, c_idx, btn in _iter_message_buttons(msg):
                btn_text = getattr(btn, 'text', None) or ''
                for bt in button_texts:
                    if match_keyword(bt, btn_text):
                        candidate = (r_idx, c_idx, btn_text)
                        break
                if candidate:
                    break

        if not candidate and keywords and message_text_for_filter:
            for keyword in keywords:
                if match_keyword(keyword, message_text_for_filter, message_text_lower):
                    for r_idx, c_idx, btn in _iter_message_buttons(msg):
                        btn_text = getattr(btn, 'text', None) or ''
                        candidate = (r_idx, c_idx, btn_text)
                        break
                    break
    else:
        for r_idx, c_idx, btn in _iter_message_buttons(msg):
            btn_text = getattr(btn, 'text', None) or ''
            if button_texts:
                for bt in button_texts:
                    if match_keyword(bt, btn_text):
                        candidate = (r_idx, c_idx, btn_text)
                        break
            else:
                candidate = (r_idx, c_idx, btn_text)
            if candidate:
                break

    if not candidate:
        debug_log(f" 自动点击未找到按钮: ch={chat_id} msg={msg_id}")
        return False

    try:
        r_idx, c_idx, btn_text = candidate
        try:
            await msg.click(i=r_idx, j=c_idx)
        except TypeError:
            try:
                await msg.click(row=r_idx, column=c_idx)
            except TypeError:
                await msg.click(r_idx, c_idx)
        _mark_auto_clicked(chat_id, msg_id)
        log_message(f"已自动点击按钮: ch={chat_id} msg={msg_id} btn='{btn_text}'")

        notify_targets = rules.get('notify_targets') or []
        if notify_targets:
            chat_title = None
            try:
                chat_title = getattr(event.chat, 'title', None)
            except Exception:
                chat_title = None
            msg_link = _build_message_link(event, msg)
            base_text = f"已自动点击红包按钮\n群: {chat_title or chat_id}\n消息ID: {msg_id}\n按钮: {btn_text or '-'}"
            if msg_link:
                base_text += f"\n链接: {msg_link}"
            for target in notify_targets:
                try:
                    await client.send_message(target, base_text)
                except Exception as e:
                    log_message(f"自动点击通知失败: target={target} err={e}")
        return True
    except Exception as e:
        log_message(f"自动点击失败: ch={chat_id} msg={msg_id} err={e}")
    return False

# --- Message Handler ---
async def new_message_handler(event, *, backfill: bool = False):
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
        msg_text = msg.message
        msg_text_lower = msg_text.lower()
        for keyword in global_blacklist:
            if match_keyword(keyword, msg_text, msg_text_lower):
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
            auto_click_fast = bool(restricted_entry.get('auto_click_fast', False))
            auto_click_fast_skip = bool(restricted_entry.get('auto_click_fast_skip_processing', False))
            auto_click_clicked = False
            if auto_click_fast and not backfill:
                try:
                    auto_click_clicked = await _maybe_auto_click_buttons(
                        event,
                        msg,
                        restricted_entry,
                        message_text_for_filter,
                        fast_mode=True,
                    )
                except Exception as e:
                    debug_log(f" 自动点击(极速)处理异常: {e}")
                if auto_click_clicked and auto_click_fast_skip:
                    return

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

            force_forward_all = bool(restricted_entry.get('force_forward_all', False))
            message_text_lower = message_text_for_filter.lower() if message_text_for_filter else ""

            # --- Auto Click Buttons (e.g., 红包) ---
            if not backfill:
                try:
                    await _maybe_auto_click_buttons(event, msg, restricted_entry, message_text_for_filter)
                except Exception as e:
                    debug_log(f" 自动点击处理异常: {e}")

            if not force_forward_all:
                use_tv_filters = bool(restricted_entry.get('use_tvchannel_filters'))
                extra_blacklist = []
                extra_whitelist = []
                tv_drama_whitelist = []
                if use_tv_filters:
                    channel_filters_cfg = load_channel_filters()
                    channel_filters = channel_filters_cfg.get('channels', {}) if isinstance(channel_filters_cfg, dict) else {}
                    channel_rule = channel_filters.get(str(restricted_channel_id), {}) if isinstance(channel_filters, dict) else {}
                    global_rule = channel_filters_cfg.get('global', {}) if isinstance(channel_filters_cfg, dict) else {}
                    drama_rule = channel_filters_cfg.get('drama', {}) if isinstance(channel_filters_cfg, dict) else {}

                    extra_blacklist = _normalize_keyword_list(global_rule.get('blacklist')) + _normalize_keyword_list(channel_rule.get('blacklist'))
                    extra_whitelist = (
                        _normalize_keyword_list(global_rule.get('whitelist'))
                        + _normalize_keyword_list(channel_rule.get('whitelist'))
                        + _normalize_keyword_list(drama_rule.get('whitelist'))
                    )
                    tv_drama_whitelist = _normalize_keyword_list(drama_rule.get('whitelist'))

                # --- Blacklist Check ---
                blacklist = _normalize_keyword_list(restricted_entry.get('blacklist_keywords')) + extra_blacklist
                if blacklist and message_text_for_filter:
                    for keyword in blacklist:
                        if match_keyword(keyword, message_text_for_filter, message_text_lower):
                            if getattr(msg, 'media', None):
                                trace_log(
                                    f"[TRACE_SKIP] ch={restricted_channel_id} msg={msg.id} reason=blacklist keyword='{keyword}' | {media_trace}",
                                )
                            debug_log(f"在频道 {restricted_channel_id} 中检测到黑名单关键词或正则 '{keyword}'，跳过。")
                            return

                # --- Whitelist Check ---
                whitelist = _normalize_keyword_list(restricted_entry.get('whitelist_keywords')) + extra_whitelist
                tv_whitelist_hit = False
                if whitelist:
                    found_whitelist = False
                    if message_text_for_filter:
                        for keyword in whitelist:
                            if match_keyword(keyword, message_text_for_filter, message_text_lower):
                                found_whitelist = True
                                break
                        if tv_drama_whitelist:
                            for keyword in tv_drama_whitelist:
                                if match_keyword(keyword, message_text_for_filter, message_text_lower):
                                    tv_whitelist_hit = True
                                    break
                    if not found_whitelist:
                        if getattr(msg, 'media', None):
                            trace_log(
                                f"[TRACE_SKIP] ch={restricted_channel_id} msg={msg.id} reason=whitelist_miss | {media_trace}",
                            )
                        debug_log(f"在频道 {restricted_channel_id} 中未检测到白名单关键词或正则，跳过消息 {msg.id}。")
                        return
            else:
                debug_log("已开启强制转发，跳过黑白名单过滤。")

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

            # --- Pre-check HDHive hits (used to override monitor type mismatch) ---
            target_user_ids_for_hdhive = restricted_entry.get('target_user_ids', [])
            has_hdhive_hit = False
            try:
                if target_user_ids_for_hdhive:
                    has_hdhive_hit = bool(_extract_hdhive_hits_from_message(msg))
            except Exception:
                has_hdhive_hit = False
            
            # --- Check Monitor Type ---
            monitor_types = restricted_entry.get('monitor_types')
            if isinstance(monitor_types, list) and monitor_types:
                normalized_types = [str(t).strip().lower() for t in monitor_types if str(t).strip()]
            else:
                single_type = str(restricted_entry.get('monitor_type', 'all')).strip().lower()
                normalized_types = [single_type] if single_type else ['all']

            type_allowed = ('all' in normalized_types) or (media_type_detected in normalized_types)
            if not type_allowed:
                if has_hdhive_hit:
                    debug_log(
                        f" 频道监控类型为 '{','.join(normalized_types)}'，"
                        f"但检测到 HDHive 链接，继续处理。"
                    )
                elif tv_whitelist_hit:
                    debug_log(
                        f" 频道监控类型为 '{','.join(normalized_types)}'，"
                        f"但 TV 白名单已命中，继续处理。"
                    )
                elif force_forward_all:
                    debug_log(
                        f" 频道监控类型为 '{','.join(normalized_types)}'，"
                        f"但已开启强制转发，继续处理。"
                    )
                else:
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
            forward_only = bool(restricted_entry.get('forward_only', False))

            # Special case: if message contains HDHive 115 links (explicit/implicit/buttons),
            # always send the resolved real link(s) to configured target users.
            if has_hdhive_hit:
                should_forward = True
            if force_forward_all:
                should_forward = True

            if forward_only:
                should_forward = True
                should_download = False
            elif not type_allowed and not has_hdhive_hit:
                # Force-forwarded messages should not trigger downloads for mismatched types.
                should_download = False
            
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

            # --- 1. Queue Download (non-blocking) ---
            if should_download:
                task = {
                    "msg": msg,
                    "chat_id": event.chat_id,
                    "restricted_channel_id": restricted_channel_id,
                    "restricted_entry": dict(restricted_entry or {}),
                    "media_type_detected": media_type_detected,
                    "download_directory": download_directory,
                }
                queued, reason = _enqueue_download_task(task)
                if queued:
                    log_message(
                        f"下载任务已入队 ch={restricted_channel_id} msg={msg.id} "
                        f"queue={download_queue.qsize()}/{DOWNLOAD_QUEUE_MAXSIZE}"
                    )
                else:
                    log_message(
                        f"下载队列已满，跳过下载 ch={restricted_channel_id} msg={msg.id} reason={reason}"
                    )

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
                    convert_hdhive_enabled = bool(restricted_entry.get('convert_hdhive', False))
                    should_send_copy = _should_send_copy_instead_of_forward(
                        media_type_detected,
                        convert_hdhive_enabled,
                        has_hdhive,
                    )
                    
                    for user_id in target_user_ids:
                        try:
                            target_entity = await client.get_entity(user_id)
                            
                            # Use send_message for text, forward or send_file for media
                            if should_send_copy:
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
                                # Normal forward preserves the original forward tag and non-HDHive links/buttons.
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
                            _apply_download_concurrency(current_config, announce=True)
                            _apply_download_queue_config(current_config, announce=True)
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
    _register_event_handlers()
    log_message(f"已成功连接并授权 Telegram 客户端。")
    _ensure_download_workers(DOWNLOAD_CONCURRENCY)
    
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
            save_config(current_config)
            log_message("频道名称已更新并保存到配置。")
        except Exception as e:
            log_message(f"保存更新后的配置失败: {e}")

    log_message(f"开始监控 {len(current_config.get('restricted_channels', []))} 个频道。")
    startup_scan_limit = _resolve_startup_tv_whitelist_scan_limit(current_config)
    if startup_scan_limit > 0:
        log_message(f"启动回溯检测：最近 {startup_scan_limit} 条 TV 白名单消息（仅限启用 TV 过滤的频道）")
        try:
            await _startup_scan_tv_whitelist(limit=startup_scan_limit)
        except Exception as e:
            log_message(f"启动回溯检测失败: {e}")
    else:
        log_message("启动回溯检测已关闭。")

    try:
        # 3. Run loops
        await asyncio.gather(
            monitor_config_changes(), # Monitors config file for changes
            # periodic_checkin_loop(),   # HDHive 签到功能已移除
            message_queue_loop(),      # Sends messages requested by app.py
            keep_client_connected(),   # Keeps the Telegram long connection alive and reconnects on disconnect
            # resource_request_loop(),   # HDHive 功能已移除
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
