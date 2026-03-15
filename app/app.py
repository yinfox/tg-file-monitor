# app/app.py
import os
import json
import asyncio
import subprocess
import threading
import html # Import for escaping HTML
import inspect # Import for iscoroutinefunction
import sys
import logging
import uuid
import time
import re
import datetime
import shutil
import requests
from urllib.parse import quote_plus
from typing import Tuple
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, send_file
from telethon.sync import TelegramClient # Using sync version for simpler Flask integration
from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError, PhoneCodeInvalidError
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from dotenv import load_dotenv
from croniter import croniter

# Add current directory to path so we can import modules from app/
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from downloader_module import downloader

app = Flask(__name__)
# Stable secret key for v0.4.6
app.secret_key = "tg-file-monitor-v0.4.6-rapid-upload-key"

VERSION = "0.4.78"
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)
DRAMA_CALENDAR_SCRIPT = os.path.join(ROOT_DIR, 'scripts', 'update_drama_calendar_env.py')

# --- Configuration Management ---
CONFIG_DIR = 'config' # Define the config directory
CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')
LOG_DIR = os.path.join(CONFIG_DIR, 'logs')
DOWNLOAD_RISK_STATS_FILE = os.path.join(CONFIG_DIR, 'download_risk_stats.json')
DRAMA_CALENDAR_LOG_FILE = os.path.join(LOG_DIR, 'drama_calendar.log')
DRAMA_CALENDAR_STATE_FILE = os.path.join(CONFIG_DIR, 'drama_calendar_state.json')
DRAMA_RUN_BUSY_MESSAGE = '当前已有追剧任务在运行（可能是自动调度），请稍后重试。'
MESSAGE_QUEUE_FILE = os.path.join(CONFIG_DIR, 'message_queue.json')
SELF_SERVICE_LOG_FILE = os.path.join(LOG_DIR, 'self_service.log')

# Log performance safeguards
LOG_TAIL_LINES = 200
LOG_TAIL_MAX_BYTES = 2 * 1024 * 1024
LOG_FILE_MAX_BYTES = 50 * 1024 * 1024
LOG_FILE_TRIM_BYTES = 5 * 1024 * 1024
LOG_FILE_SIZE_CHECK_INTERVAL = 15
LOG_REFRESH_INTERVAL_DEFAULT = 10

_MESSAGE_QUEUE_LOCK = threading.Lock()
_PUBLIC_RATE_LIMIT_LOCK = threading.Lock()
_PUBLIC_RATE_LIMIT_CACHE = {}

# Load environment variables from config/.env
try:
    load_dotenv(os.path.join(CONFIG_DIR, '.env'))
except Exception:
    pass

TELEGRAM_SESSION_NAME = "telegram_monitor" # This must match what telegram_monitor.py uses

@app.context_processor
def inject_version():
    return dict(version=VERSION)

def load_config():
    """Loads configuration from config.json with environment variable overrides."""
    default_config = {
        "telegram": {
            "api_id": None,
            "api_hash": None,
            "session_name": TELEGRAM_SESSION_NAME
        },
        "web_auth": {
            "username": "admin",
            "password_hash": None
        },
        "channels_to_forward": [],
        "file_monitoring_tasks": [],
        "file_monitor_scan_interval": 5,
        "file_monitor_dir_state_check_interval": 0,
        "download_concurrency": 2,
        "log_refresh_interval_seconds": LOG_REFRESH_INTERVAL_DEFAULT,
        "log_auto_refresh_enabled": False,
        "log_tail_lines": LOG_TAIL_LINES,
        "log_tail_max_bytes": LOG_TAIL_MAX_BYTES,
        "self_service_enabled": False,
        "self_service_target_user_ids": "",
        "self_service_search_max_results": 5,
        "self_service_cookie_check_mode": "warn",
        "self_service_use_open_api": False,
        "self_service_storage_mode": "any",
        "self_service_public_enabled": False,
        "self_service_public_access_key": "",
        "self_service_public_rate_limit": {
            "enabled": True,
            "window_seconds": 300,
            "max_requests": 3
        },
        "hdhive_base_url": "https://hdhive.com",
        "hdhive_open_api_key": "",
        "hdhive_open_api_direct_unlock": False,
        "allowed_browse_path": os.getcwd(),
        "restricted_channels": [],
        "proxy": {},
        "115_cookie": "",
        "bot": {"token": ""},
        "debug_mode": False,
        "trace_media_detection": False,
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
            "download_timeout_min_speed_mb_s": 1.0
        },
        "drama_calendar": {
            "source": "calendar",
            "home_url": "https://blog.922928.de/",
            "post_url": "",
            "calendar_whitelist_keywords": "",
            "calendar_blacklist_keywords": "",
            "maoyan_url": "https://piaofang.maoyan.com/box-office?ver=normal",
            "maoyan_top_n": 0,
            "include_maoyan_web_heat": True,
            "maoyan_web_heat_url": "https://piaofang.maoyan.com/web-heat",
            "maoyan_web_heat_top_n": 0,
            "maoyan_whitelist_keywords": "",
            "maoyan_blacklist_keywords": "",
            "douban_url": "https://m.douban.com/subject_collection/tv_american",
            "douban_top_n": 0,
            "douban_asia_top_n": 0,
            "douban_domestic_top_n": 0,
            "douban_variety_top_n": 0,
            "douban_animation_top_n": 0,
            "douban_whitelist_keywords": "",
            "douban_blacklist_keywords": "",
            "douban_asia_whitelist_keywords": "",
            "douban_asia_blacklist_keywords": "",
            "douban_domestic_whitelist_keywords": "",
            "douban_domestic_blacklist_keywords": "",
            "douban_variety_whitelist_keywords": "",
            "douban_variety_blacklist_keywords": "",
            "douban_animation_whitelist_keywords": "",
            "douban_animation_blacklist_keywords": "",
            "remove_movie_premiere_after_days": 365,
            "remove_finished_after_days": -1,
            "line_keywords": "上线,开播",
            "title_alias_map": "",
            "env_files": "",
            "env_key": "DRAMA_CALENDAR_REGEX",
            "backup_before_write": True,
            "append_to_whitelist": True,
            "managed_scope_source_only": True,
            "auto_sync_enabled": False,
            "auto_sync_interval_minutes": 60,
            "auto_sync_cron_expr": "",
            "finish_detect_mode": "hybrid",
            "tmdb_api_key": "",
            "tmdb_language": "zh-CN",
            "tmdb_region": "CN"
        }
    }
    if not os.path.exists(CONFIG_FILE):
        config = default_config
    else:
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if "allowed_browse_path" not in config:
                    config["allowed_browse_path"] = os.getcwd()
                if "restricted_channels" not in config:
                    config["restricted_channels"] = []
                if "file_monitor_scan_interval" not in config:
                    config["file_monitor_scan_interval"] = default_config["file_monitor_scan_interval"]
                if "file_monitor_dir_state_check_interval" not in config:
                    config["file_monitor_dir_state_check_interval"] = default_config["file_monitor_dir_state_check_interval"]
                if "download_concurrency" not in config:
                    config["download_concurrency"] = default_config["download_concurrency"]
                if "log_refresh_interval_seconds" not in config:
                    config["log_refresh_interval_seconds"] = default_config["log_refresh_interval_seconds"]
                if "log_auto_refresh_enabled" not in config:
                    config["log_auto_refresh_enabled"] = default_config["log_auto_refresh_enabled"]
                if "log_tail_lines" not in config:
                    config["log_tail_lines"] = default_config["log_tail_lines"]
                if "log_tail_max_bytes" not in config:
                    config["log_tail_max_bytes"] = default_config["log_tail_max_bytes"]
                if "self_service_enabled" not in config:
                    config["self_service_enabled"] = default_config["self_service_enabled"]
                if "self_service_target_user_ids" not in config:
                    config["self_service_target_user_ids"] = default_config["self_service_target_user_ids"]
                if "self_service_search_max_results" not in config:
                    config["self_service_search_max_results"] = default_config["self_service_search_max_results"]
                if "self_service_cookie_check_mode" not in config:
                    config["self_service_cookie_check_mode"] = default_config["self_service_cookie_check_mode"]
                if "self_service_use_open_api" not in config:
                    config["self_service_use_open_api"] = default_config["self_service_use_open_api"]
                if "self_service_storage_mode" not in config:
                    legacy_only_115 = bool(config.get("self_service_only_115", False))
                    config["self_service_storage_mode"] = "115" if legacy_only_115 else default_config["self_service_storage_mode"]
                else:
                    mode = str(config.get("self_service_storage_mode") or "any").lower()
                    if mode not in ("any", "115", "123", "115_123"):
                        config["self_service_storage_mode"] = default_config["self_service_storage_mode"]
                if "self_service_public_enabled" not in config:
                    config["self_service_public_enabled"] = default_config["self_service_public_enabled"]
                if "self_service_public_access_key" not in config:
                    config["self_service_public_access_key"] = default_config["self_service_public_access_key"]
                if "self_service_public_rate_limit" not in config or not isinstance(config.get("self_service_public_rate_limit"), dict):
                    config["self_service_public_rate_limit"] = default_config["self_service_public_rate_limit"].copy()
                else:
                    merged_public_rate = default_config["self_service_public_rate_limit"].copy()
                    merged_public_rate.update(config.get("self_service_public_rate_limit") or {})
                    config["self_service_public_rate_limit"] = merged_public_rate
                if "hdhive_base_url" not in config:
                    config["hdhive_base_url"] = default_config["hdhive_base_url"]
                if "hdhive_open_api_key" not in config:
                    config["hdhive_open_api_key"] = default_config["hdhive_open_api_key"]
                if "hdhive_open_api_direct_unlock" not in config:
                    config["hdhive_open_api_direct_unlock"] = default_config["hdhive_open_api_direct_unlock"]
                if "proxy" not in config:
                    config["proxy"] = {}
                if "trace_media_detection" not in config:
                    config["trace_media_detection"] = False
                if "download_risk_control" not in config or not isinstance(config.get("download_risk_control"), dict):
                    config["download_risk_control"] = default_config["download_risk_control"]
                else:
                    merged_risk = default_config["download_risk_control"].copy()
                    merged_risk.update(config.get("download_risk_control") or {})
                    config["download_risk_control"] = merged_risk
                if "drama_calendar" not in config or not isinstance(config.get("drama_calendar"), dict):
                    config["drama_calendar"] = default_config["drama_calendar"].copy()
                else:
                    merged_drama = default_config["drama_calendar"].copy()
                    raw_drama = config.get("drama_calendar") or {}
                    merged_drama.update(raw_drama)
                    for key in (
                        "douban_asia_top_n",
                        "douban_domestic_top_n",
                        "douban_variety_top_n",
                        "douban_animation_top_n",
                    ):
                        if key not in raw_drama:
                            merged_drama[key] = int(merged_drama.get("douban_top_n", 0) or 0)
                    merged_drama["douban_url"] = _normalize_douban_collection_url(
                        str(merged_drama.get("douban_url") or ""),
                        fallback_url=default_config["drama_calendar"]["douban_url"],
                    )
                    config["drama_calendar"] = merged_drama
        except json.JSONDecodeError:
            flash(f"错误: 无法解析 {CONFIG_FILE}。请检查文件格式。", "error")
            config = default_config
    
    # ==================== 环境变量覆盖（敏感信息） ====================
    # Telegram API 凭证
    if os.environ.get('TELEGRAM_API_ID'):
        try:
            config['telegram']['api_id'] = int(os.environ.get('TELEGRAM_API_ID'))
        except ValueError:
            pass
    
    if os.environ.get('TELEGRAM_API_HASH'):
        config['telegram']['api_hash'] = os.environ.get('TELEGRAM_API_HASH')
    
    # Telegram Bot Token: prefer config value so Web updates can take effect directly.
    # Env var remains a fallback when config token is missing.
    env_bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    if env_bot_token:
        if 'bot' not in config:
            config['bot'] = {}
        current_bot_token = (config.get('bot', {}).get('token') or '').strip()
        if not current_bot_token:
            config['bot']['token'] = env_bot_token
    
    # 115 Cookie
    if os.environ.get('COOKIE_115'):
        config['115_cookie'] = os.environ.get('COOKIE_115')
    elif os.environ.get('WEB_115_COOKIE'):
        config['115_cookie'] = os.environ.get('WEB_115_COOKIE')
    
    # 移除 HDHive 配置
    
    return config

def save_config(config):
    """Saves configuration to config.json."""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


def _parse_env_files(raw_env_files: str):
    if not raw_env_files:
        return []
    result = []
    for p in re.split(r'[\n,]+', raw_env_files):
        item = (p or '').strip()
        if item:
            result.append(item)
    return result


def _parse_env_keys(raw_env_key: str):
    if not raw_env_key:
        return []
    result = []
    seen = set()
    for p in re.split(r'[\n,]+', raw_env_key):
        item = (p or '').strip()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


DRAMA_SOURCE_CHOICES = (
    'calendar',
    'maoyan',
    'douban',
    'douban_asia',
    'douban_domestic',
    'douban_variety',
    'douban_animation',
    'all',
)


def _normalize_drama_sources(raw_sources):
    if isinstance(raw_sources, str):
        candidates = [item.strip() for item in raw_sources.split(',')]
    elif isinstance(raw_sources, (list, tuple, set)):
        candidates = [str(item).strip() for item in raw_sources]
    else:
        candidates = []

    normalized = []
    seen = set()
    for item in candidates:
        if not item or item not in DRAMA_SOURCE_CHOICES:
            continue
        if item == 'all':
            return ['all']
        if item not in seen:
            normalized.append(item)
            seen.add(item)
    return normalized or ['calendar']


def _drama_sources_csv(raw_sources) -> str:
    return ','.join(_normalize_drama_sources(raw_sources))


def _normalize_douban_collection_url(raw_url: str, fallback_url: str = 'https://m.douban.com/subject_collection/tv_american') -> str:
    text = (raw_url or '').strip()
    if not text:
        return fallback_url

    url_matches = re.findall(r'https?://m\.douban\.com/subject_collection/[^,\s?#]+', text, flags=re.IGNORECASE)
    if url_matches:
        return url_matches[0]

    slug_match = re.search(r'/subject_collection/([^,\s/?#]+)', text)
    if slug_match:
        return f'https://m.douban.com/subject_collection/{slug_match.group(1).strip()}'

    return fallback_url


def _strip_env_quotes(v: str) -> str:
    v = (v or '').strip()
    if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
        return v[1:-1]
    return v


def _detect_env_quote_style(v: str) -> str:
    s = (v or '').strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return 'double'
    if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
        return 'single'
    return 'bare'


def _format_env_value(value: str, style: str) -> str:
    if style == 'single':
        safe = (value or '').replace("'", "'\\''")
        return f"'{safe}'"
    if style == 'bare':
        return value or ''
    safe = (value or '').replace('"', '\\"')
    return f'"{safe}"'


def _update_env_key_value(env_content: str, key: str, value: str) -> str:
    lines = (env_content or '').splitlines()
    updated = False
    new_lines = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith('#') or '=' not in line:
            new_lines.append(line)
            continue

        left, raw_v = line.split('=', 1)
        if left.strip() == key:
            quote_style = _detect_env_quote_style(raw_v)
            new_lines.append(f"{left}={_format_env_value(value, quote_style)}")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        if new_lines and new_lines[-1].strip() != '':
            new_lines.append('')
        new_lines.append(f'{key}="{value}"')

    return '\n'.join(new_lines) + '\n'


def _make_env_backup_if_needed(env_path: str) -> str:
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"{env_path}.bak_{ts}"
    shutil.copy2(env_path, backup_path)
    return backup_path


def _extract_env_value_by_key(env_content: str, key: str) -> str:
    for line in (env_content or '').splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#') or '=' not in line:
            continue
        left, raw_v = line.split('=', 1)
        if left.strip() == key:
            return _strip_env_quotes(raw_v)
    return ''


def _normalize_title_for_match(title: str) -> str:
    t = (title or '').strip().lower()
    t = re.sub(r'[\s\-_:：·\.\,，。\(\)\[\]【】]+', '', t)
    return t


def _clean_extracted_title(title: str) -> str:
    s = (title or '').strip()
    if not s:
        return ''
    s = html.unescape(s)
    s = s.replace('\u3000', ' ')
    s = re.sub(r'\s+第[一二三四五六七八九十百零两0-9]+季$', '', s).strip()
    s = re.sub(r'[`~!@#$%^&*=|\\\\/;\"\'<>,.?•…\\-—_]', '', s)
    s = re.sub(r'[！＠＃￥％……＆＊［\\[\\]］【】｛{}｝「」『』《》〈〉〔〕〖〗“”‘’、，。；？～｜×✕]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _extract_titles_from_regex_value(regex_value: str) -> list:
    if not regex_value:
        return []
    found = []
    seen = set()
    for token in re.findall(r'\^\(\?=\.\*(.*?)\)\.\*\$', regex_value):
        title = re.sub(r'\\(.)', r'\1', token).strip()
        title = _clean_extracted_title(title)
        if not title or title in seen:
            continue
        seen.add(title)
        found.append(title)
    return found


def _escape_title_for_regex(title: str) -> str:
    return re.sub(r'([.^$*+?{}\\[\\]\\\\|()])', r'\\\\\\1', title)


def _build_regex_from_titles(titles: list) -> str:
    if not titles:
        return ''
    unique_titles = []
    seen_norm = set()
    for t in titles:
        s = (t or '').strip()
        if not s:
            continue
        norm = _normalize_title_for_match(s)
        if not norm or norm in seen_norm:
            continue
        seen_norm.add(norm)
        unique_titles.append(s)
    escaped = [_escape_title_for_regex(t) for t in unique_titles]
    if not escaped:
        return ''
    escaped.sort(key=len, reverse=True)
    return '|'.join([f'^(?=.*{item}).*$' for item in escaped])


def _load_drama_calendar_state() -> dict:
    if not DRAMA_CALENDAR_STATE_FILE or not os.path.exists(DRAMA_CALENDAR_STATE_FILE):
        return {'records': []}
    try:
        with open(DRAMA_CALENDAR_STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get('records'), list):
            return data
    except Exception:
        pass
    return {'records': []}


def _save_drama_calendar_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(DRAMA_CALENDAR_STATE_FILE) or '.', exist_ok=True)
        with open(DRAMA_CALENDAR_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _merge_regex_values(existing_value: str, add_value: str, remove_values: list) -> str:
    existing_titles = _extract_titles_from_regex_value(existing_value)
    remove_norms = set()
    for old_value in remove_values or []:
        for title in _extract_titles_from_regex_value(old_value):
            norm = _normalize_title_for_match(title)
            if norm:
                remove_norms.add(norm)

    kept_titles = []
    kept_norms = set()
    for title in existing_titles:
        norm = _normalize_title_for_match(title)
        if not norm or norm in remove_norms or norm in kept_norms:
            continue
        kept_titles.append(title)
        kept_norms.add(norm)

    for title in _extract_titles_from_regex_value(add_value):
        norm = _normalize_title_for_match(title)
        if not norm or norm in kept_norms:
            continue
        kept_titles.append(title)
        kept_norms.add(norm)

    return _build_regex_from_titles(kept_titles)


def _replace_managed_append_value_in_memory(
    env_content: str,
    key: str,
    source_tag: str,
    env_path: str,
    new_value: str,
    records: list,
    managed_scope: str,
) -> tuple:
    target_env = os.path.abspath(env_path)
    old_values = []
    new_records = []
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        rec_env = os.path.abspath(str(rec.get('env_path') or ''))
        rec_key = str(rec.get('key') or '')
        rec_source = str(rec.get('source') or '')
        rec_value = str(rec.get('value') or '')
        should_replace = False
        if managed_scope == 'key':
            should_replace = (rec_env == target_env and rec_key == key)
        else:
            should_replace = (rec_env == target_env and rec_key == key and rec_source == source_tag)

        if should_replace:
            if rec_value:
                old_values.append(rec_value)
            continue
        new_records.append(rec)

    lines = (env_content or '').splitlines()
    replaced_lines = []
    found_key = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('#') or '=' not in line:
            replaced_lines.append(line)
            continue
        left, raw_v = line.split('=', 1)
        if left.strip() != key:
            replaced_lines.append(line)
            continue
        found_key = True
        quote_style = _detect_env_quote_style(raw_v)
        current = _strip_env_quotes(raw_v)
        merged = _merge_regex_values(current, new_value, old_values)
        replaced_lines.append(f"{left}={_format_env_value(merged, quote_style)}")

    if not found_key:
        if replaced_lines and replaced_lines[-1].strip() != '':
            replaced_lines.append('')
        replaced_lines.append(f'{key}="{new_value}"')

    new_records.append(
        {
            'env_path': target_env,
            'key': key,
            'source': source_tag,
            'value': new_value,
            'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    )
    return '\n'.join(replaced_lines) + '\n', new_records


def _label_for_source_tag(source_tag: str) -> str:
    base_labels = {
        'calendar': '追剧日历',
        'maoyan': '猫眼',
        'douban': '豆瓣美剧',
        'douban_asia': '豆瓣日韩',
        'douban_domestic': '豆瓣国产剧',
        'douban_variety': '豆瓣综艺',
        'douban_animation': '豆瓣动漫',
        'all': '全部来源',
        'manual': '手动/未归档',
        'key': '全部自动值',
    }
    tag = (source_tag or '').strip()
    if not tag:
        return '未知来源'
    if tag in base_labels:
        return base_labels[tag]
    if ',' in tag:
        parts = [p.strip() for p in tag.split(',') if p.strip()]
        labels = [base_labels.get(p, p) for p in parts]
        return '合并：' + ' + '.join(labels)
    return f'来源：{tag}'


def _build_env_edit_source_view(drama_cfg: dict) -> tuple:
    env_files_list = _parse_env_files(drama_cfg.get('env_files', ''))
    env_keys = _parse_env_keys(str(drama_cfg.get('env_key') or 'DRAMA_CALENDAR_REGEX')) or ['DRAMA_CALENDAR_REGEX']
    env_key = env_keys[0]
    state = _load_drama_calendar_state()
    records = state.get('records') if isinstance(state, dict) else []
    if not isinstance(records, list):
        records = []
    source_titles_state = state.get('source_titles') if isinstance(state, dict) else None
    if not isinstance(source_titles_state, dict):
        source_titles_state = {}

    sources_map = {}
    env_errors = []
    group_index = 0

    def _ensure_source(tag: str) -> dict:
        if tag not in sources_map:
            sources_map[tag] = {
                'tag': tag,
                'label': _label_for_source_tag(tag),
                'env_groups': [],
            }
        return sources_map[tag]

    for env_path in env_files_list:
        abs_env = os.path.abspath(env_path)
        if not os.path.exists(abs_env):
            env_errors.append({'env_display': env_path, 'error': '目标 .env 不存在'})
            continue

        try:
            with open(abs_env, 'r', encoding='utf-8') as f:
                env_content = f.read()
        except Exception:
            env_errors.append({'env_display': env_path, 'error': '读取 .env 失败'})
            continue

        current_value = _extract_env_value_by_key(env_content, env_key)
        current_titles = _extract_titles_from_regex_value(current_value)
        current_norms = set()
        for t in current_titles:
            norm = _normalize_title_for_match(t)
            if norm:
                current_norms.add(norm)
        used_norms = set()
        source_titles_map = {}

        source_map_for_env = {}
        if source_titles_state:
            env_entry = source_titles_state.get(abs_env)
            if isinstance(env_entry, dict):
                key_entry = env_entry.get(env_key)
                if isinstance(key_entry, dict):
                    source_map_for_env = key_entry

        if source_map_for_env:
            for source_tag, titles in source_map_for_env.items():
                if not isinstance(titles, list):
                    continue
                cleaned = []
                for t in titles:
                    norm = _normalize_title_for_match(t)
                    if not norm or norm in used_norms or norm not in current_norms:
                        continue
                    used_norms.add(norm)
                    cleaned.append(t)
                if cleaned:
                    source_titles_map.setdefault(source_tag, []).extend(cleaned)
        else:
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                rec_env = os.path.abspath(str(rec.get('env_path') or ''))
                rec_key = str(rec.get('key') or '')
                if rec_env != abs_env or rec_key != env_key:
                    continue
                source_tag = str(rec.get('source') or '').strip() or 'unknown'
                value = str(rec.get('value') or '')
                titles = _extract_titles_from_regex_value(value)
                cleaned = []
                for t in titles:
                    norm = _normalize_title_for_match(t)
                    if not norm or norm in used_norms:
                        continue
                    used_norms.add(norm)
                    cleaned.append(t)
                if cleaned:
                    source_titles_map.setdefault(source_tag, []).extend(cleaned)

        untracked = []
        for t in current_titles:
            norm = _normalize_title_for_match(t)
            if norm and norm not in used_norms:
                untracked.append(t)
        if untracked:
            source_titles_map.setdefault('manual', []).extend(untracked)

        for tag, titles in source_titles_map.items():
            if not titles:
                continue
            group_index += 1
            source_entry = _ensure_source(tag)
            source_entry['env_groups'].append(
                {
                    'id': f'g{group_index}',
                    'tag': tag,
                    'env_path': abs_env,
                    'env_display': env_path,
                    'titles': titles,
                }
            )

    order = [
        'calendar',
        'maoyan',
        'douban',
        'douban_asia',
        'douban_domestic',
        'douban_variety',
        'douban_animation',
        'all',
        'manual',
        'key',
        'unknown',
    ]
    def _source_sort(item: dict) -> tuple:
        tag = item.get('tag')
        return (order.index(tag) if tag in order else len(order), item.get('label') or '')

    sources = sorted(sources_map.values(), key=_source_sort)
    for source in sources:
        source['env_groups'].sort(key=lambda g: g.get('env_display') or '')

    return sources, env_errors


def _clear_drama_calendar_state_records(env_path: str, key: str) -> None:
    if not DRAMA_CALENDAR_STATE_FILE or not os.path.exists(DRAMA_CALENDAR_STATE_FILE):
        return
    try:
        with open(DRAMA_CALENDAR_STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
        records = state.get('records') if isinstance(state, dict) else None
        if not isinstance(records, list):
            return
        filtered = []
        target_env = os.path.abspath(env_path)
        for rec in records:
            if not isinstance(rec, dict):
                continue
            rec_env = os.path.abspath(str(rec.get('env_path') or ''))
            rec_key = str(rec.get('key') or '')
            if rec_env == target_env and rec_key == key:
                continue
            filtered.append(rec)
        state['records'] = filtered
        source_titles = state.get('source_titles') if isinstance(state, dict) else None
        if isinstance(source_titles, dict):
            env_entry = source_titles.get(target_env)
            if isinstance(env_entry, dict) and key in env_entry:
                env_entry.pop(key, None)
                source_titles[target_env] = env_entry
            state['source_titles'] = source_titles
        with open(DRAMA_CALENDAR_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _clear_drama_calendar_env_values(drama_cfg: dict):
    env_files_list = _parse_env_files(drama_cfg.get('env_files', ''))
    if not env_files_list:
        return False, [], ['请先在追剧日历配置中填写至少一个 .env 路径。']

    env_keys = _parse_env_keys(str(drama_cfg.get('env_key') or 'DRAMA_CALENDAR_REGEX')) or ['DRAMA_CALENDAR_REGEX']
    success_paths = []
    errors = []

    for env_path in env_files_list:
        abs_env_path = os.path.abspath(env_path)
        try:
            original = ''
            env_exists = os.path.exists(abs_env_path)
            if env_exists:
                with open(abs_env_path, 'r', encoding='utf-8') as f:
                    original = f.read()
            updated = original
            for env_key in env_keys:
                updated = _update_env_key_value(updated, env_key, '')

            os.makedirs(os.path.dirname(abs_env_path) or '.', exist_ok=True)
            if env_exists:
                emergency_backup = f"{abs_env_path}.autosnap_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                shutil.copy2(abs_env_path, emergency_backup)
            if env_exists and bool(drama_cfg.get('backup_before_write', False)):
                _make_env_backup_if_needed(abs_env_path)
            with open(abs_env_path, 'w', encoding='utf-8') as f:
                f.write(updated)
            for env_key in env_keys:
                _clear_drama_calendar_state_records(abs_env_path, env_key)
            success_paths.append(abs_env_path)
        except Exception as e:
            errors.append(f'{abs_env_path}: {e}')

    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    keys_label = ",".join(env_keys)
    log_lines = [
        f'[{ts}] [INFO] [DRAMA] 清空环境变量 key={keys_label} success={len(success_paths)} errors={len(errors)}',
    ]
    for path in success_paths[:20]:
        log_lines.append(f'[{ts}] [INFO] [DRAMA][CLEAR] {path}')
    for item in errors[:20]:
        log_lines.append(f'[{ts}] [ERROR] [DRAMA][CLEAR] {item}')
    _append_drama_calendar_log(log_lines)
    return len(errors) == 0, success_paths, errors


def _estimate_drama_run_timeout(drama_cfg: dict) -> int:
    sources = _normalize_drama_sources(drama_cfg.get('source'))
    source_count = 7 if 'all' in sources else len(sources)
    finish_mode = str(drama_cfg.get('finish_detect_mode') or 'hybrid').strip().lower()
    has_tmdb_key = bool((drama_cfg.get('tmdb_api_key') or os.environ.get('TMDB_API_KEY') or '').strip())
    remove_finished_days = int(drama_cfg.get('remove_finished_after_days', -1) or -1)
    remove_movie_days = int(drama_cfg.get('remove_movie_premiere_after_days', 365) if drama_cfg.get('remove_movie_premiere_after_days', 365) is not None else 365)

    timeout_seconds = 180
    if source_count >= 3:
        timeout_seconds = max(timeout_seconds, 300)
    if source_count >= 5 or 'all' in sources:
        timeout_seconds = max(timeout_seconds, 600)
    if has_tmdb_key and finish_mode in ('tmdb', 'hybrid') and remove_finished_days >= 0:
        timeout_seconds = max(timeout_seconds, 420 if source_count < 5 else 600)
    if has_tmdb_key and remove_movie_days >= 0 and ('maoyan' in sources or 'all' in sources):
        timeout_seconds = max(timeout_seconds, 420 if source_count < 5 else 600)
    return timeout_seconds


def _decode_subprocess_output(raw: bytes) -> str:
    data = raw or b''
    for encoding in ('utf-8', 'utf-8-sig', 'gb18030'):
        try:
            return data.decode(encoding)
        except Exception:
            continue
    return data.decode('utf-8', errors='replace')


def _select_drama_log_lines(lines, *, max_lines: int = 120):
    cleaned = [str(ln).strip() for ln in (lines or []) if str(ln).strip()]
    if len(cleaned) <= max_lines:
        return cleaned

    noisy_markers = (
        'TMDB 缓存命中',
        'TMDB 发起请求',
        'TMDB 电影缓存命中',
        'TMDB 电影发起请求',
    )
    important_markers = (
        '[INFO] 数据源:',
        '[INFO] 来源链接:',
        '[INFO] 提取剧名数:',
        '[INFO] 已移除完结剧:',
        '[INFO] 已移除超期电影:',
        '[INFO] TMDB 补充判定为完结:',
        '[INFO] 已跳过历史已监控剧名:',
        '[INFO] 生成正则:',
        '[INFO] 本次无新增剧名可写入',
        '[OK] 已写入',
        '[DRY-RUN] 将更新',
        '[WARN] 未提取到任何剧名',
        '[INFO] 猫眼提取前N:',
        '[INFO] 豆瓣前N:',
        '[INFO] 豆瓣日韩前N:',
        '[INFO] 豆瓣国产剧前N:',
        '[INFO] 豆瓣综艺前N:',
        '[INFO] 豆瓣动漫前N:',
        '[INFO] 完结判定模式:',
        '[INFO] 完结移除天数:',
        '[INFO] 电影首映剔除阈值:',
    )

    selected_idx = set()

    for idx, line in enumerate(cleaned):
        if any(marker in line for marker in important_markers):
            selected_idx.add(idx)
            for follow_idx in range(idx + 1, min(len(cleaned), idx + 6)):
                if cleaned[follow_idx].lstrip().startswith('- '):
                    selected_idx.add(follow_idx)
                else:
                    break

    non_noisy_idx = [idx for idx, line in enumerate(cleaned) if not any(marker in line for marker in noisy_markers)]
    for idx in non_noisy_idx[:20]:
        selected_idx.add(idx)
    for idx in non_noisy_idx[-40:]:
        selected_idx.add(idx)

    ordered = [cleaned[idx] for idx in sorted(selected_idx)]
    if len(ordered) > max_lines:
        return ordered[:max_lines]
    return ordered


def _summarize_drama_output_lines(lines, *, prefer_write: bool = False, max_parts: int = 4) -> str:
    cleaned = [str(ln).strip() for ln in (lines or []) if str(ln).strip()]
    if not cleaned:
        return ''

    priority_markers = [
        '[OK] 已写入',
        '[INFO] 提取剧名数:',
        '[INFO] 已移除完结剧:',
        '[INFO] 已移除超期电影:',
        '[INFO] TMDB 补充判定为完结:',
        '[INFO] 已跳过历史已监控剧名:',
        '[INFO] 本次无新增剧名可写入',
        '[WARN] 未提取到任何剧名',
        '[INFO] 数据源:',
        '[INFO] 来源链接:',
    ]
    if prefer_write:
        priority_markers = ['[OK] 已写入', '[INFO] 本次无新增剧名可写入'] + [m for m in priority_markers if m not in ('[OK] 已写入', '[INFO] 本次无新增剧名可写入')]

    picked: List[str] = []
    seen: Set[str] = set()
    for marker in priority_markers:
        for line in cleaned:
            if marker in line and line not in seen:
                picked.append(line)
                seen.add(line)
                break
        if len(picked) >= max_parts:
            break

    if len(picked) < max_parts:
        noisy_markers = (
            'TMDB 缓存命中',
            'TMDB 发起请求',
            'TMDB 电影缓存命中',
            'TMDB 电影发起请求',
        )
        for line in cleaned:
            if line in seen:
                continue
            if any(marker in line for marker in noisy_markers):
                continue
            picked.append(line)
            seen.add(line)
            if len(picked) >= max_parts:
                break

    return ' | '.join(picked[:max_parts]).strip()


def _extract_drama_count(lines, marker: str) -> int:
    for line in (lines or []):
        if marker not in str(line):
            continue
        m = re.search(r':\s*(\d+)', str(line))
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return 0
    return 0


def _build_drama_summary_items(output_lines, err_lines, *, ok: bool, dry_run: bool) -> dict:
    lines = [str(ln).strip() for ln in (output_lines or []) if str(ln).strip()]
    errs = [str(ln).strip() for ln in (err_lines or []) if str(ln).strip()]

    extracted = _extract_drama_count(lines, '[INFO] 提取剧名数:')
    removed_finished = _extract_drama_count(lines, '[INFO] 已移除完结剧:')
    removed_movies = _extract_drama_count(lines, '[INFO] 已移除超期电影:')
    skipped_existing = _extract_drama_count(lines, '[INFO] 已跳过历史已监控剧名:')

    if ok:
        if any('[OK] 已写入' in line for line in lines):
            env_write_count = sum(1 for line in lines if '[OK] 已写入' in line)
            result_text = f'已写入 {env_write_count} 个 .env'
        elif any('本次无新增剧名可写入' in line for line in lines):
            result_text = '无新增，已跳过写入'
        elif dry_run and any('[DRY-RUN] 将更新' in line for line in lines):
            preview_count = sum(1 for line in lines if '[DRY-RUN] 将更新' in line)
            result_text = f'预览 {preview_count} 个 .env'
        else:
            result_text = '执行成功'
    else:
        result_text = _summarize_drama_output_lines(errs or lines, prefer_write=not dry_run, max_parts=2) or '执行失败'

    removed_parts = []
    if removed_finished:
        removed_parts.append(f'完结 {removed_finished}')
    if removed_movies:
        removed_parts.append(f'电影 {removed_movies}')
    if skipped_existing:
        removed_parts.append(f'历史 {skipped_existing}')
    removed_text = ' / '.join(removed_parts) if removed_parts else '无'

    return {
        'extract': (str(extracted) if extracted else '0'),
        'removed': removed_text,
        'result': result_text,
    }


def _append_drama_calendar_log(lines):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(DRAMA_CALENDAR_LOG_FILE, 'a', encoding='utf-8') as f:
            for line in (lines or []):
                f.write(str(line).rstrip('\n') + '\n')
    except Exception:
        pass


def _read_drama_calendar_log_lines(max_lines: int = 2000):
    try:
        if not os.path.exists(DRAMA_CALENDAR_LOG_FILE):
            return []
        lines = _tail_file_lines(DRAMA_CALENDAR_LOG_FILE, max_lines=max_lines)
        return lines
    except Exception:
        return []


def _run_drama_calendar_update(drama_cfg: dict, dry_run: bool = True, trigger: str = 'manual'):
    if not os.path.exists(DRAMA_CALENDAR_SCRIPT):
        return False, "", f"脚本不存在: {DRAMA_CALENDAR_SCRIPT}"

    env_files_list = _parse_env_files(drama_cfg.get('env_files', ''))
    if not env_files_list:
        return False, "", "请先在追剧日历配置中填写至少一个 .env 路径。"

    if not _DRAMA_RUN_LOCK.acquire(blocking=False):
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        _append_drama_calendar_log([
            f'[{ts}] [WARN] [DRAMA] 忽略执行，已有任务运行中 trigger={trigger}',
        ])
        return False, '', DRAMA_RUN_BUSY_MESSAGE

    cmd = [
        sys.executable,
        DRAMA_CALENDAR_SCRIPT,
        '--source', _drama_sources_csv(drama_cfg.get('source')),
        '--home-url', (drama_cfg.get('home_url') or 'https://blog.922928.de/').strip(),
        '--calendar-whitelist-keywords', (drama_cfg.get('calendar_whitelist_keywords') or '').strip(),
        '--calendar-blacklist-keywords', (drama_cfg.get('calendar_blacklist_keywords') or '').strip(),
        '--maoyan-url', (drama_cfg.get('maoyan_url') or 'https://piaofang.maoyan.com/box-office?ver=normal').strip(),
        '--maoyan-top-n', str(int(drama_cfg.get('maoyan_top_n', 0) or 0)),
        '--maoyan-web-heat-url', (drama_cfg.get('maoyan_web_heat_url') or 'https://piaofang.maoyan.com/web-heat').strip(),
        '--maoyan-web-heat-top-n', str(int(drama_cfg.get('maoyan_web_heat_top_n', 0) or 0)),
        '--maoyan-whitelist-keywords', (drama_cfg.get('maoyan_whitelist_keywords') or '').strip(),
        '--maoyan-blacklist-keywords', (drama_cfg.get('maoyan_blacklist_keywords') or '').strip(),
        '--douban-url', _normalize_douban_collection_url(
            str(drama_cfg.get('douban_url') or ''),
            fallback_url='https://m.douban.com/subject_collection/tv_american',
        ),
        '--douban-top-n', str(int(drama_cfg.get('douban_top_n', 0) or 0)),
        '--douban-asia-top-n', str(int(drama_cfg.get('douban_asia_top_n', drama_cfg.get('douban_top_n', 0)) or 0)),
        '--douban-domestic-top-n', str(int(drama_cfg.get('douban_domestic_top_n', drama_cfg.get('douban_top_n', 0)) or 0)),
        '--douban-variety-top-n', str(int(drama_cfg.get('douban_variety_top_n', drama_cfg.get('douban_top_n', 0)) or 0)),
        '--douban-animation-top-n', str(int(drama_cfg.get('douban_animation_top_n', drama_cfg.get('douban_top_n', 0)) or 0)),
        '--douban-whitelist-keywords', (drama_cfg.get('douban_whitelist_keywords') or '').strip(),
        '--douban-blacklist-keywords', (drama_cfg.get('douban_blacklist_keywords') or '').strip(),
        '--douban-asia-whitelist-keywords', (drama_cfg.get('douban_asia_whitelist_keywords') or '').strip(),
        '--douban-asia-blacklist-keywords', (drama_cfg.get('douban_asia_blacklist_keywords') or '').strip(),
        '--douban-domestic-whitelist-keywords', (drama_cfg.get('douban_domestic_whitelist_keywords') or '').strip(),
        '--douban-domestic-blacklist-keywords', (drama_cfg.get('douban_domestic_blacklist_keywords') or '').strip(),
        '--douban-variety-whitelist-keywords', (drama_cfg.get('douban_variety_whitelist_keywords') or '').strip(),
        '--douban-variety-blacklist-keywords', (drama_cfg.get('douban_variety_blacklist_keywords') or '').strip(),
        '--douban-animation-whitelist-keywords', (drama_cfg.get('douban_animation_whitelist_keywords') or '').strip(),
        '--douban-animation-blacklist-keywords', (drama_cfg.get('douban_animation_blacklist_keywords') or '').strip(),
        '--remove-movie-premiere-after-days', str(int(drama_cfg.get('remove_movie_premiere_after_days', 365) if drama_cfg.get('remove_movie_premiere_after_days', 365) is not None else 365)),
        '--remove-finished-after-days', str(int(drama_cfg.get('remove_finished_after_days', -1) if drama_cfg.get('remove_finished_after_days', -1) is not None else -1)),
        '--finish-detect-mode', (drama_cfg.get('finish_detect_mode') or 'hybrid').strip(),
        '--line-keywords', (drama_cfg.get('line_keywords') or '上线,开播').strip(),
        '--title-alias-map', (drama_cfg.get('title_alias_map') or '').strip(),
        '--env-files', ','.join(env_files_list),
        '--env-key', (drama_cfg.get('env_key') or 'DRAMA_CALENDAR_REGEX').strip(),
        '--managed-scope', ('source' if bool(drama_cfg.get('managed_scope_source_only', True)) else 'key'),
    ]

    tmdb_api_key = (drama_cfg.get('tmdb_api_key') or os.environ.get('TMDB_API_KEY') or '').strip()
    if tmdb_api_key:
        cmd.extend(['--tmdb-api-key', tmdb_api_key])
    tmdb_language = (drama_cfg.get('tmdb_language') or 'zh-CN').strip() or 'zh-CN'
    tmdb_region = (drama_cfg.get('tmdb_region') or 'CN').strip() or 'CN'
    try:
        tmdb_year_tolerance = max(0, int(drama_cfg.get('tmdb_year_tolerance', 2) or 2))
    except Exception:
        tmdb_year_tolerance = 2
    try:
        tmdb_min_score = max(1, int(drama_cfg.get('tmdb_min_score', 70) or 70))
    except Exception:
        tmdb_min_score = 70
    cmd.extend(['--tmdb-language', tmdb_language, '--tmdb-region', tmdb_region])
    cmd.extend(['--tmdb-year-tolerance', str(tmdb_year_tolerance), '--tmdb-min-score', str(tmdb_min_score)])

    post_url = (drama_cfg.get('post_url') or '').strip()
    if post_url:
        cmd.extend(['--post-url', post_url])
    if bool(drama_cfg.get('include_maoyan_web_heat', True)):
        cmd.append('--include-maoyan-web-heat')
    if (not dry_run) and bool(drama_cfg.get('backup_before_write', False)):
        cmd.append('--backup')
    if bool(drama_cfg.get('append_to_whitelist', True)):
        cmd.append('--append')
    if dry_run:
        cmd.append('--dry-run')

    started_at = time.strftime('%Y-%m-%d %H:%M:%S')
    source = _drama_sources_csv(drama_cfg.get('source'))
    run_label = '预览' if dry_run else '写入'
    _append_drama_calendar_log([
        f"[{started_at}] [INFO] [DRAMA] 开始执行 ({run_label}) trigger={trigger} source={source} env_files={len(env_files_list)}",
    ])

    try:
        timeout_seconds = _estimate_drama_run_timeout(drama_cfg)
        proc = subprocess.run(
            cmd,
            cwd=ROOT_DIR,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
        finished_at = time.strftime('%Y-%m-%d %H:%M:%S')
        level = 'INFO' if proc.returncode == 0 else 'ERROR'
        summary = f"[{finished_at}] [{level}] [DRAMA] 执行结束 exit={proc.returncode} trigger={trigger} mode={run_label}"
        log_lines = [summary]
        stdout_text = _decode_subprocess_output(proc.stdout)
        stderr_text = _decode_subprocess_output(proc.stderr)
        out_lines = [ln for ln in (stdout_text or '').splitlines() if ln.strip()]
        err_lines = [ln for ln in (stderr_text or '').splitlines() if ln.strip()]
        for ln in _select_drama_log_lines(out_lines, max_lines=120):
            log_lines.append(f"[{finished_at}] [INFO] [DRAMA][OUT] {ln}")
        for ln in err_lines[:120]:
            log_lines.append(f"[{finished_at}] [ERROR] [DRAMA][ERR] {ln}")
        _append_drama_calendar_log(log_lines)
        return proc.returncode == 0, stdout_text, stderr_text
    except subprocess.TimeoutExpired:
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        _append_drama_calendar_log([
            f"[{ts}] [ERROR] [DRAMA] 执行超时 ({timeout_seconds}s) trigger={trigger} mode={run_label}",
        ])
        return False, '', f'执行超时（{timeout_seconds}秒），请检查网络连通性后重试。'
    except Exception as e:
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        _append_drama_calendar_log([
            f"[{ts}] [ERROR] [DRAMA] 执行异常 trigger={trigger} mode={run_label}: {e}",
        ])
        return False, '', f'执行异常: {e}'
    finally:
        _DRAMA_RUN_LOCK.release()


_DRAMA_SCHEDULER_STOP_EVENT = threading.Event()
_DRAMA_SCHEDULER_THREAD = None
_DRAMA_SCHEDULER_LOCK = threading.Lock()
_DRAMA_RUN_LOCK = threading.Lock()
_DRAMA_SCHEDULER_STATE = {
    'enabled': False,
    'running': False,
    'next_run_at': '未启用',
    'last_run_at': '',
    'last_status': '',
    'last_message': '',
    'last_summary': {
        'extract': '',
        'removed': '',
        'result': '',
    },
    'schedule_mode': 'interval',
}


def _format_scheduler_ts(ts: float) -> str:
    if not ts or ts <= 0:
        return ''
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))


def _set_drama_scheduler_state(**kwargs):
    with _DRAMA_SCHEDULER_LOCK:
        _DRAMA_SCHEDULER_STATE.update(kwargs)


def get_drama_scheduler_state() -> dict:
    _ensure_drama_scheduler_started()
    with _DRAMA_SCHEDULER_LOCK:
        return dict(_DRAMA_SCHEDULER_STATE)


def _normalize_cron_expr(expr: str) -> str:
    return (expr or '').strip()


def _ensure_drama_scheduler_started():
    if _DRAMA_SCHEDULER_THREAD and _DRAMA_SCHEDULER_THREAD.is_alive():
        return
    try:
        config = load_config()
        cfg = config.get('drama_calendar', {}) if isinstance(config, dict) else {}
        enabled = bool(cfg.get('auto_sync_enabled', False))
        cron_expr = _normalize_cron_expr(str(cfg.get('auto_sync_cron_expr', '') or ''))
        if enabled:
            start_drama_scheduler()
            _set_drama_scheduler_state(
                enabled=True,
                running=False,
                schedule_mode=('cron' if cron_expr else 'interval'),
                next_run_at='即将启动',
            )
        else:
            _set_drama_scheduler_state(enabled=False, running=False, schedule_mode='interval', next_run_at='未启用')
    except Exception:
        pass


def _cron_expr_valid(expr: str) -> bool:
    e = _normalize_cron_expr(expr)
    if not e:
        return False
    try:
        return bool(croniter.is_valid(e))
    except Exception:
        return False


def _next_run_by_cron(expr: str, now_ts: float) -> float:
    base_dt = datetime.datetime.fromtimestamp(now_ts)
    it = croniter(_normalize_cron_expr(expr), base_dt)
    return float(it.get_next(float))


def _tail_file_lines(path: str, max_lines: int = LOG_TAIL_LINES, max_bytes: int = LOG_TAIL_MAX_BYTES):
    if max_lines <= 0:
        return []
    try:
        file_size = os.path.getsize(path)
    except Exception:
        return []

    if file_size <= max_bytes:
        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                return [line.rstrip('\n') for line in f]
        except Exception:
            return []

    chunk_size = 64 * 1024
    buffer = b''
    try:
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell()
            while pos > 0 and buffer.count(b'\n') <= max_lines and len(buffer) < max_bytes:
                read_size = chunk_size if pos >= chunk_size else pos
                pos -= read_size
                f.seek(pos)
                buffer = f.read(read_size) + buffer
    except Exception:
        return []

    lines = buffer.splitlines()[-max_lines:]
    return [line.decode('utf-8', errors='replace') for line in lines]


def _append_self_service_log(lines):
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(SELF_SERVICE_LOG_FILE, 'a', encoding='utf-8') as f:
            for line in (lines or []):
                f.write(str(line).rstrip('\n') + '\n')
    except Exception:
        pass


def _extract_next_data_json(html_text: str):
    if not html_text:
        return None
    try:
        m = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html_text,
            flags=re.S | re.IGNORECASE,
        )
        if not m:
            return None
        raw = m.group(1).strip()
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


def _normalize_hdhive_cookie(raw_cookie: str) -> str:
    if not raw_cookie:
        return ''
    cleaned = str(raw_cookie).replace('\r', ';').replace('\n', ';').replace('\t', ';')
    cleaned = re.sub(r';{2,}', ';', cleaned)
    return cleaned.strip(' ;')


def _collect_hdhive_urls_from_text(text: str, base_url: str, results: list, max_results: int) -> None:
    if not text:
        return
    pattern_full = re.compile(r"/resource/(?:115/)?[0-9A-Za-z]{16,64}", re.IGNORECASE)
    slug_pattern = re.compile(r"resource[^0-9A-Za-z]{0,20}([0-9A-Za-z]{16,64})", re.IGNORECASE)
    json_slug_pattern = re.compile(r'"(?:slug|resourceId|resource_id|id)"\s*:\s*"([0-9A-Za-z]{16,64})"', re.IGNORECASE)

    def _add_url(path: str):
        full_url = f"{base_url}{path}"
        if full_url not in results:
            results.append(full_url)

    for path in pattern_full.findall(text):
        _add_url(path)
        if len(results) >= max_results:
            return
    for slug in slug_pattern.findall(text):
        # Unknown variant: try both /resource/<slug> and /resource/115/<slug>
        _add_url(f"/resource/{slug}")
        if len(results) >= max_results:
            return
        _add_url(f"/resource/115/{slug}")
        if len(results) >= max_results:
            return
    for slug in json_slug_pattern.findall(text):
        _add_url(f"/resource/{slug}")
        if len(results) >= max_results:
            return
        _add_url(f"/resource/115/{slug}")
        if len(results) >= max_results:
            return


def _walk_json_for_hdhive_urls(obj, base_url: str, results: list, max_results: int) -> None:
    if isinstance(obj, dict):
        for v in obj.values():
            _walk_json_for_hdhive_urls(v, base_url, results, max_results)
    elif isinstance(obj, list):
        for v in obj:
            _walk_json_for_hdhive_urls(v, base_url, results, max_results)
    elif isinstance(obj, str):
        if '/resource/115/' in obj or len(obj) >= 16:
            _collect_hdhive_urls_from_text(obj, base_url, results, max_results)


def _collect_from_next_f(html_text: str, base_url: str, results: list, max_results: int) -> None:
    if not html_text:
        return
    try:
        parts = re.findall(r'self\.__next_f\.push\(\[1,"([\s\S]*?)"\]\)', html_text)
        if not parts:
            parts = re.findall(r'__next_f\.push\(\[\d+,\\\"(.*?)\\\"\\]\)', html_text)
        if not parts:
            return
        joined = "\n".join(parts)
        try:
            joined = joined.encode("utf-8").decode("unicode_escape")
        except Exception:
            pass
        _collect_hdhive_urls_from_text(joined, base_url, results, max_results)
    except Exception:
        return


def _collect_tmdb_urls_from_text(text: str, base_url: str, tmdb_urls: list, max_results: int) -> None:
    if not text:
        return
    pattern = re.compile(r"/tmdb/(movie|tv)/(\d+)", re.IGNORECASE)
    for m in pattern.finditer(text):
        path = m.group(0)
        full_url = f"{base_url}{path}"
        if full_url not in tmdb_urls:
            tmdb_urls.append(full_url)
            if len(tmdb_urls) >= max_results:
                return


def _collect_tmdb_ids_from_json(obj, tmdb_ids: list, max_results: int) -> None:
    if len(tmdb_ids) >= max_results:
        return
    if isinstance(obj, dict):
        # Common keys
        tmdb_id = obj.get("tmdbId") or obj.get("tmdb_id") or obj.get("tmdbID")
        media_type = obj.get("mediaType") or obj.get("media_type") or obj.get("type")
        if tmdb_id:
            try:
                tmdb_id = int(tmdb_id)
            except Exception:
                tmdb_id = None
        if tmdb_id:
            mt = str(media_type or "").lower()
            if mt in ("tv", "series", "电视剧", "tvshow"):
                mt = "tv"
            elif mt in ("movie", "film", "电影"):
                mt = "movie"
            else:
                # Unknown, default tv
                mt = "tv"
            key = f"{mt}:{tmdb_id}"
            if key not in tmdb_ids:
                tmdb_ids.append(key)
                if len(tmdb_ids) >= max_results:
                    return
        for v in obj.values():
            _collect_tmdb_ids_from_json(v, tmdb_ids, max_results)
    elif isinstance(obj, list):
        for v in obj:
            _collect_tmdb_ids_from_json(v, tmdb_ids, max_results)


def _fetch_hdhive_urls_from_url(
    url: str,
    headers: dict,
    base_url: str,
    results: list,
    max_results: int,
    debug_info: list,
    tmdb_urls: list = None,
    tmdb_ids: list = None,
) -> None:
    try:
        resp = requests.get(url, timeout=20, headers=headers)
    except Exception as e:
        debug_info.append(f"url={url} error={type(e).__name__}")
        return
    debug_info.append(f"url={url} status={getattr(resp, 'status_code', 'NA')}")
    if resp.status_code != 200 or not resp.text:
        return
    _collect_hdhive_urls_from_text(resp.text, base_url, results, max_results)
    if tmdb_urls is not None:
        _collect_tmdb_urls_from_text(resp.text, base_url, tmdb_urls, max_results)
    if tmdb_ids is not None:
        _collect_tmdb_ids_from_json(resp.text, tmdb_ids, max_results)
    if len(results) >= max_results:
        return
    _collect_from_next_f(resp.text, base_url, results, max_results)
    if tmdb_urls is not None:
        _collect_tmdb_urls_from_text(resp.text, base_url, tmdb_urls, max_results)
    if tmdb_ids is not None:
        _collect_tmdb_ids_from_json(resp.text, tmdb_ids, max_results)
    if len(results) >= max_results:
        return
    next_data = _extract_next_data_json(resp.text)
    if next_data:
        _walk_json_for_hdhive_urls(next_data, base_url, results, max_results)
        if len(results) >= max_results:
            return

    # Try JSON payload
    try:
        if resp.headers.get('Content-Type', '').startswith('application/json') or resp.text.strip().startswith('{'):
            data = resp.json()
            _walk_json_for_hdhive_urls(data, base_url, results, max_results)
            if tmdb_urls is not None:
                try:
                    _collect_tmdb_urls_from_text(json.dumps(data, ensure_ascii=False), base_url, tmdb_urls, max_results)
                except Exception:
                    pass
            if tmdb_ids is not None:
                _collect_tmdb_ids_from_json(data, tmdb_ids, max_results)
    except Exception:
        pass


def _tmdb_search_ids(api_key: str, query: str, year: str, media_type: str, language: str = "zh-CN") -> list:
    api_key = (api_key or '').strip()
    if not api_key or not query:
        return []
    base = "https://api.themoviedb.org/3/search"
    url = f"{base}/{media_type}"
    params = {"api_key": api_key, "query": query, "language": language}
    if year:
        if media_type == "movie":
            params["year"] = year
        else:
            params["first_air_date_year"] = year
    try:
        resp = requests.get(url, params=params, timeout=20)
        data = resp.json() if resp.status_code == 200 else None
    except Exception:
        data = None
    if not isinstance(data, dict):
        return []
    results = data.get("results") if isinstance(data.get("results"), list) else []
    ids = []
    for item in results:
        if isinstance(item, dict) and item.get("id"):
            ids.append(int(item.get("id")))
    return ids


def _test_hdhive_cookie(base_url: str, cookie: str) -> dict:
    base_url = (base_url or "https://hdhive.com").rstrip('/')
    cookie = _normalize_hdhive_cookie(cookie or '')
    if not cookie:
        return {"success": False, "message": "未配置 Cookie"}

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Origin": base_url,
        "Referer": f"{base_url}/",
        "Cookie": cookie,
    }

    checks = [
        ("home", f"{base_url}/"),
        ("profile", f"{base_url}/profile"),
        ("points", f"{base_url}/go-api/customer/points"),
        ("info", f"{base_url}/go-api/customer/info"),
        ("user", f"{base_url}/go-api/customer"),
    ]
    details = []
    for name, url in checks:
        try:
            resp = requests.get(url, timeout=15, headers=headers)
        except Exception as e:
            details.append(f"{name}: {type(e).__name__}")
            continue
        details.append(f"{name}:{resp.status_code}")
        text = resp.text or ''
        content_type = resp.headers.get('Content-Type', '')
        if 'application/json' in content_type or text.strip().startswith('{'):
            try:
                data = resp.json()
            except Exception:
                data = None
            if isinstance(data, dict):
                if data.get('success') is True:
                    return {"success": True, "message": "Cookie 有效 (API 成功)", "details": details}
                msg = str(data.get('message') or data.get('description') or '')
                if msg and ('未登录' in msg or 'login' in msg.lower()):
                    continue
                if data.get('data') is not None or data.get('user') is not None:
                    return {"success": True, "message": "Cookie 有效 (API 返回数据)", "details": details}
        else:
            if any(token in text for token in ("退出登录", "个人中心", "账户中心", "logout")):
                return {"success": True, "message": "Cookie 可能有效 (页面已登录)", "details": details}

    return {"success": False, "message": "Cookie 无效或登录失效", "details": details}


def _parse_target_user_ids(raw) -> list:
    if raw is None:
        return []
    items = []
    if isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        text = str(raw)
        items = re.split(r'[\n,]+', text)
    result = []
    for item in items:
        s = str(item or '').strip()
        if not s:
            continue
        if s.lstrip('-').isdigit():
            try:
                result.append(int(s))
                continue
            except Exception:
                pass
        result.append(s)
    return result


def _resolve_client_ip(req) -> str:
    try:
        forwarded = (req.headers.get('X-Forwarded-For') or '').strip()
        if forwarded:
            return forwarded.split(',')[0].strip() or (req.remote_addr or 'unknown')
        return req.remote_addr or 'unknown'
    except Exception:
        return 'unknown'


def _check_public_rate_limit(req, cfg: dict) -> Tuple[bool, int]:
    if not isinstance(cfg, dict):
        return True, 0
    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        return True, 0
    try:
        window_seconds = int(cfg.get("window_seconds", 300) or 300)
    except Exception:
        window_seconds = 300
    try:
        max_requests = int(cfg.get("max_requests", 3) or 3)
    except Exception:
        max_requests = 3

    window_seconds = max(10, min(window_seconds, 24 * 60 * 60))
    max_requests = max(1, min(max_requests, 100))

    ip = _resolve_client_ip(req)
    now = time.time()
    cutoff = now - window_seconds

    with _PUBLIC_RATE_LIMIT_LOCK:
        history = _PUBLIC_RATE_LIMIT_CACHE.get(ip, [])
        history = [ts for ts in history if ts >= cutoff]
        if len(history) >= max_requests:
            oldest = min(history) if history else now
            retry_after = int(max(1, window_seconds - (now - oldest)))
            _PUBLIC_RATE_LIMIT_CACHE[ip] = history
            return False, retry_after
        history.append(now)
        _PUBLIC_RATE_LIMIT_CACHE[ip] = history

    return True, 0


def _hdhive_open_api_request(method: str, url: str, api_key: str, json_body: dict = None, timeout: int = 20) -> dict:
    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json",
    }
    if method.upper() in ("POST", "PATCH"):
        headers["Content-Type"] = "application/json"
    try:
        resp = requests.request(method, url, headers=headers, json=json_body, timeout=timeout)
    except Exception as e:
        return {"success": False, "code": "NETWORK_ERROR", "message": f"{type(e).__name__}: {e}"}
    try:
        data = resp.json()
    except Exception:
        data = None
    if isinstance(data, dict):
        return data
    return {"success": False, "code": str(resp.status_code), "message": "Invalid JSON response"}


def _hdhive_open_api_resources(base_url: str, api_key: str, media_type: str, tmdb_id: str) -> dict:
    api_base = base_url.rstrip("/") + "/api/open"
    url = f"{api_base}/resources/{media_type}/{tmdb_id}"
    return _hdhive_open_api_request("GET", url, api_key)


def _hdhive_open_api_ping(base_url: str, api_key: str) -> dict:
    api_base = base_url.rstrip("/") + "/api/open"
    url = f"{api_base}/ping"
    return _hdhive_open_api_request("GET", url, api_key)


def _hdhive_open_api_unlock(base_url: str, api_key: str, slug: str) -> dict:
    api_base = base_url.rstrip("/") + "/api/open"
    url = f"{api_base}/resources/unlock"
    return _hdhive_open_api_request("POST", url, api_key, json_body={"slug": slug})


def _resource_unlock_points(item: dict) -> int:
    pts = item.get("unlock_points") if isinstance(item, dict) else None
    try:
        return int(pts) if pts is not None else 0
    except Exception:
        return 0


def _sort_hdhive_resources(resources: list) -> list:
    if not resources:
        return []

    def _score(item):
        points = _resource_unlock_points(item)
        unlocked = bool(item.get("is_unlocked", False))
        official = bool(item.get("is_official", False))
        return (
            0 if unlocked else 1,
            points,
            0 if official else 1,
        )

    return sorted(resources, key=_score)


def _pick_hdhive_resource(resources: list, threshold: int) -> dict:
    if not resources:
        return {}

    resources_sorted = _sort_hdhive_resources(resources)
    # Prefer already unlocked
    for item in resources_sorted:
        if item.get("is_unlocked"):
            return item
    # Then affordable
    for item in resources_sorted:
        points = _resource_unlock_points(item)
        if points <= threshold:
            return item
    return {}


def _format_resource_line(item: dict) -> str:
    title = item.get("title") or "-"
    points = item.get("unlock_points")
    try:
        points = int(points) if points is not None else 0
    except Exception:
        points = 0
    unlocked = "已解锁" if item.get("is_unlocked") else f"{points}积分"
    official = "官方" if item.get("is_official") else "普通"
    resolution = ",".join(item.get("video_resolution") or [])
    return f"{title} | {unlocked} | {official} | {resolution}"


def _is_115_url(url: str) -> bool:
    if not url:
        return False
    lower = str(url).lower()
    return ("115.com" in lower) or ("115cdn" in lower)


def _is_123_url(url: str) -> bool:
    if not url:
        return False
    lower = str(url).lower()
    return ("123pan.com" in lower) or ("123pan.cn" in lower)


def _storage_mode_label(mode: str) -> str:
    if mode == "115":
        return "115"
    if mode == "123":
        return "123"
    if mode == "115_123":
        return "115/123"
    return "不限"


def _match_storage_mode(url: str, mode: str) -> bool:
    mode = (mode or "any").lower()
    if mode == "any":
        return True
    if mode == "115":
        return _is_115_url(url)
    if mode == "123":
        return _is_123_url(url)
    if mode == "115_123":
        return _is_115_url(url) or _is_123_url(url)
    return True


def _enqueue_message(chat_id, text: str) -> bool:
    if not chat_id or not text:
        return False
    record = {"chat_id": chat_id, "text": text}
    with _MESSAGE_QUEUE_LOCK:
        pending = []
        try:
            if os.path.exists(MESSAGE_QUEUE_FILE):
                with open(MESSAGE_QUEUE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    pending = data
        except Exception:
            pending = []
        pending.append(record)
        tmp_path = MESSAGE_QUEUE_FILE + '.tmp'
        try:
            os.makedirs(os.path.dirname(MESSAGE_QUEUE_FILE) or '.', exist_ok=True)
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(pending, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, MESSAGE_QUEUE_FILE)
            return True
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
    return False


def _search_hdhive_resource_urls(query: str, max_results: int = 5, cookie_header: str = "", base_url: str = "https://hdhive.com"):
    query = (query or "").strip()
    if not query:
        return [], [], [], []
    base_url = (base_url or "https://hdhive.com").rstrip('/')
    encoded = quote_plus(query)
    search_urls = [
        f"{base_url}/search?keyword={encoded}",
        f"{base_url}/search?q={encoded}",
        f"{base_url}/search?query={encoded}",
        f"{base_url}/?keyword={encoded}",
        f"{base_url}/?q={encoded}",
        f"{base_url}/search/{encoded}",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": f"{base_url}/",
    }
    json_headers = dict(headers)
    json_headers["Accept"] = "application/json, text/plain, */*"
    if cookie_header:
        headers["Cookie"] = cookie_header
        json_headers["Cookie"] = cookie_header

    debug_info = []

    results = []
    tmdb_urls = []
    tmdb_ids = []
    for url in search_urls:
        _fetch_hdhive_urls_from_url(
            url,
            headers,
            base_url,
            results,
            max_results,
            debug_info,
            tmdb_urls=tmdb_urls,
            tmdb_ids=tmdb_ids,
        )
        if len(results) >= max_results:
            return results, debug_info, tmdb_urls, tmdb_ids

        try:
            html_text = requests.get(url, timeout=20, headers=headers).text
        except Exception:
            html_text = ""
        next_data = _extract_next_data_json(html_text)
        if next_data:
            debug_info.append("next_data=found")
            _walk_json_for_hdhive_urls(next_data, base_url, results, max_results)
        else:
            debug_info.append("next_data=missing")
        if len(results) >= max_results:
            return results, debug_info, tmdb_urls, tmdb_ids

        # Try Next.js data endpoint
        try:
            build_id = None
            page_path = "/search"
            if isinstance(next_data, dict):
                build_id = next_data.get("buildId") or next_data.get("build_id")
                page_path = next_data.get("page") or page_path
            if not build_id and html_text:
                build_id_match = re.search(r'"buildId"\s*:\s*"([^"]+)"', html_text)
                if build_id_match:
                    build_id = build_id_match.group(1)
            if build_id:
                debug_info.append(f"build_id={build_id}")
                if not str(page_path).startswith("/"):
                    page_path = f"/{page_path}"
                data_urls = [
                    f"{base_url}/_next/data/{build_id}{page_path}.json?keyword={encoded}",
                    f"{base_url}/_next/data/{build_id}{page_path}.json?q={encoded}",
                    f"{base_url}/_next/data/{build_id}{page_path}.json?query={encoded}",
                ]
                for durl in data_urls:
                    try:
                        dresp = requests.get(durl, timeout=20, headers=headers)
                    except Exception:
                        continue
                    debug_info.append(f"data_url={durl} status={getattr(dresp, 'status_code', 'NA')}")
                    if dresp.status_code != 200 or not dresp.text:
                        continue
                    raw = dresp.text.strip()
                    if raw.startswith(")]}'"):
                        raw = raw.split("\n", 1)[-1]
                    try:
                        data = json.loads(raw)
                    except Exception:
                        data = None
                    if data is not None:
                        _walk_json_for_hdhive_urls(data, base_url, results, max_results)
                        _collect_tmdb_urls_from_text(raw, base_url, tmdb_urls, max_results)
                        _collect_tmdb_ids_from_json(data, tmdb_ids, max_results)
                        if len(results) >= max_results:
                            return results, debug_info, tmdb_urls, tmdb_ids
        except Exception:
            pass

        # Try x-nextjs-data header for JSON payload
        try:
            headers_json = dict(headers)
            headers_json["x-nextjs-data"] = "1"
            jresp = requests.get(url, timeout=20, headers=headers_json)
            debug_info.append(f"nextjs_data={getattr(jresp, 'status_code', 'NA')}")
            if jresp.status_code == 200 and jresp.text:
                raw = jresp.text.strip()
                if raw.startswith(")]}'"):
                    raw = raw.split("\n", 1)[-1]
                try:
                    data = json.loads(raw)
                except Exception as json_err:
                    data = None
                    debug_info.append(f"nextjs_json=err:{type(json_err).__name__}")
                if data is not None:
                    debug_info.append("nextjs_json=ok")
                    _walk_json_for_hdhive_urls(data, base_url, results, max_results)
                    _collect_tmdb_urls_from_text(raw, base_url, tmdb_urls, max_results)
                    _collect_tmdb_ids_from_json(data, tmdb_ids, max_results)
                    if len(results) >= max_results:
                        return results, debug_info, tmdb_urls, tmdb_ids
        except Exception:
            pass

        # Try common JSON API endpoints (best-effort)
        api_urls = [
            f"{base_url}/api/search?keyword={encoded}",
            f"{base_url}/api/search?q={encoded}",
            f"{base_url}/api/search?query={encoded}",
            f"{base_url}/api/resources/search?keyword={encoded}",
            f"{base_url}/api/resource/search?keyword={encoded}",
            f"{base_url}/go-api/search?keyword={encoded}",
            f"{base_url}/go-api/search?q={encoded}",
        ]
        for api_url in api_urls:
            try:
                aresp = requests.get(api_url, timeout=20, headers=json_headers)
            except Exception:
                continue
            debug_info.append(f"api_url={api_url} status={getattr(aresp, 'status_code', 'NA')}")
            if aresp.status_code != 200 or not aresp.text:
                continue
            raw = aresp.text.strip()
            if raw.startswith(")]}'"):
                raw = raw.split("\n", 1)[-1]
            try:
                data = json.loads(raw)
            except Exception as json_err:
                debug_info.append(f"api_json=err:{type(json_err).__name__}")
                data = None
            if data is not None:
                debug_info.append("api_json=ok")
                _walk_json_for_hdhive_urls(data, base_url, results, max_results)
                _collect_tmdb_urls_from_text(raw, base_url, tmdb_urls, max_results)
                _collect_tmdb_ids_from_json(data, tmdb_ids, max_results)
                if len(results) >= max_results:
                    return results, debug_info, tmdb_urls, tmdb_ids

        # Try RSC parameter (some pages require _rsc)
        try:
            rsc_url = f"{url}{'&' if '?' in url else '?'}_rsc={int(time.time() * 1000)}"
            rresp = requests.get(rsc_url, timeout=20, headers=headers)
            debug_info.append(f"rsc_url={rsc_url} status={getattr(rresp, 'status_code', 'NA')}")
            if rresp.status_code == 200 and rresp.text:
                _collect_hdhive_urls_from_text(rresp.text, base_url, results, max_results)
                _collect_from_next_f(rresp.text, base_url, results, max_results)
                _collect_tmdb_urls_from_text(rresp.text, base_url, tmdb_urls, max_results)
                if len(results) >= max_results:
                    return results, debug_info, tmdb_urls, tmdb_ids
        except Exception:
            pass

        if not results and (tmdb_urls or tmdb_ids):
            for tmdb_url in tmdb_urls[:10]:
                _fetch_hdhive_urls_from_url(tmdb_url, headers, base_url, results, max_results, debug_info)
                if len(results) >= max_results:
                    return results, debug_info, tmdb_urls, tmdb_ids
            for key in tmdb_ids[:10]:
                try:
                    mt, tid = key.split(":", 1)
                except Exception:
                    continue
                tmdb_url = f"{base_url}/tmdb/{mt}/{tid}?_rsc={int(time.time() * 1000)}"
                _fetch_hdhive_urls_from_url(tmdb_url, headers, base_url, results, max_results, debug_info)
                if len(results) >= max_results:
                    return results, debug_info, tmdb_urls, tmdb_ids

        if results:
            break
    return results, debug_info, tmdb_urls, tmdb_ids


def _run_self_service_request(payload: dict) -> None:
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    query = payload.get("query", "")
    title = payload.get("title", "") or ""
    target_ids = payload.get("targets", [])
    max_results = int(payload.get("max_results", 5) or 5)
    max_results = max(1, min(max_results, 20))
    hdhive_cookie = payload.get("hdhive_cookie", "") or ""
    base_url = payload.get("base_url", "") or "https://hdhive.com"
    request_type = payload.get("type", "")
    request_year = payload.get("year", "")
    request_note = payload.get("note", "")
    direct_url = (payload.get("hdhive_url", "") or "").strip()
    use_open_api = bool(payload.get("use_open_api"))
    open_api_key = (payload.get("hdhive_open_api_key") or "").strip()
    tmdb_id_input = (payload.get("tmdb_id") or "").strip()
    unlock_threshold = int(payload.get("unlock_threshold", 0) or 0)
    allow_open_api_direct = bool(payload.get("open_api_direct_unlock", False))
    storage_mode = str(payload.get("storage_mode", "any") or "any").lower()
    if storage_mode not in ("any", "115", "123", "115_123"):
        storage_mode = "any"
    storage_label = _storage_mode_label(storage_mode)

    if use_open_api and direct_url and not allow_open_api_direct:
        use_open_api = False

    _append_self_service_log([f"[{ts}] [INFO] submit query={query} targets={target_ids}"])

    if not target_ids:
        _append_self_service_log([f"[{ts}] [WARN] no targets configured"])
        return

    if use_open_api:
        if not open_api_key:
            msg = "❌ 未配置 HDHive Open API Key，无法使用 API 搜索"
            for tid in target_ids:
                _enqueue_message(tid, msg)
            _append_self_service_log([f"[{ts}] [WARN] open_api missing api key"])
            return

        # Direct resource slug unlock
        if direct_url:
            slug = direct_url.rstrip('/').split('/')[-1]
            unlock_resp = _hdhive_open_api_unlock(base_url, open_api_key, slug)
            if not unlock_resp.get("success"):
                msg = f"❌ 资源解锁失败: {unlock_resp.get('message') or unlock_resp.get('description')}"
                for tid in target_ids:
                    _enqueue_message(tid, msg)
                _append_self_service_log([f"[{ts}] [WARN] open_api unlock failed slug={slug}"])
                return
            data = unlock_resp.get("data") if isinstance(unlock_resp, dict) else {}
            full_url = ""
            if isinstance(data, dict):
                full_url = data.get("full_url") or ""
                if not full_url and data.get("url"):
                    url = data.get("url")
                    access_code = data.get("access_code")
                    if access_code:
                        sep = "&" if "?" in url else "?"
                        full_url = f"{url}{sep}password={access_code}"
                    else:
                        full_url = url
            if full_url and not _match_storage_mode(full_url, storage_mode):
                msg_lines = [
                    f"❌ 自助观影申请仅支持 {storage_label} 网盘",
                    f"关键词: {query}",
                    f"Slug: {slug}",
                    f"解锁结果: {unlock_resp.get('message') or 'success'}",
                    "链接类型: 不符合当前网盘设置（已过滤）",
                ]
                msg = "\n".join(msg_lines)
                for tid in target_ids:
                    _enqueue_message(tid, msg)
                _append_self_service_log([f"[{ts}] [WARN] open_api non-115 filtered slug={slug}"])
                return
            msg_lines = [
                "🎬 自助观影申请结果（Open API）",
                f"关键词: {query}",
                f"Slug: {slug}",
                f"解锁结果: {unlock_resp.get('message') or 'success'}",
            ]
            msg_lines.append(f"链接: {full_url or '未返回'}")
            msg = "\n".join(msg_lines)
            for tid in target_ids:
                _enqueue_message(tid, msg)
            _append_self_service_log([f"[{ts}] [INFO] open_api success slug={slug}"])
            return

        media_type = ""
        if request_type in ("电影", "movie"):
            media_type = "movie"
        elif request_type in ("电视剧", "tv"):
            media_type = "tv"

        tmdb_ids = []
        if tmdb_id_input:
            tmdb_ids = [tmdb_id_input]
        else:
            tmdb_key = (payload.get("tmdb_api_key") or "").strip()
            if tmdb_key and title:
                if media_type:
                    tmdb_ids = [str(x) for x in _tmdb_search_ids(tmdb_key, title, request_year, media_type)]
                else:
                    tmdb_ids = [str(x) for x in _tmdb_search_ids(tmdb_key, title, request_year, "tv")]
                    if len(tmdb_ids) < 3:
                        tmdb_ids += [str(x) for x in _tmdb_search_ids(tmdb_key, title, request_year, "movie")]

        if not tmdb_ids:
            msg_lines = [
                "❌ 自助观影申请未找到 TMDB ID",
                f"关键词: {query}",
                "请在表单中填写 TMDB ID 或配置 TMDB API Key。",
            ]
            msg = "\n".join(msg_lines)
            for tid in target_ids:
                _enqueue_message(tid, msg)
            _append_self_service_log([f"[{ts}] [WARN] open_api no tmdb id query={query}"])
            return

        collected_resources = []
        for tid in tmdb_ids[:5]:
            mt_candidates = [media_type] if media_type else ["tv", "movie"]
            for mt in mt_candidates:
                resp = _hdhive_open_api_resources(base_url, open_api_key, mt, tid)
                if not resp.get("success"):
                    continue
                data = resp.get("data") if isinstance(resp, dict) else None
                if isinstance(data, list) and data:
                    for item in data:
                        if isinstance(item, dict):
                            item["_tmdb_id"] = tid
                            item["_media_type"] = mt
                            collected_resources.append(item)
            if collected_resources:
                break

        if not collected_resources:
            msg_lines = [
                "❌ 自助观影申请未找到资源",
                f"关键词: {query}",
                f"TMDB ID: {', '.join(tmdb_ids[:5])}",
            ]
            msg = "\n".join(msg_lines)
            for tid in target_ids:
                _enqueue_message(tid, msg)
            _append_self_service_log([f"[{ts}] [WARN] open_api no resources query={query}"])
            return

        candidate_pool = []
        resources_sorted = _sort_hdhive_resources(collected_resources)
        for item in resources_sorted:
            if item.get("is_unlocked"):
                candidate_pool.append(item)
        for item in resources_sorted:
            if item in candidate_pool:
                continue
            points = _resource_unlock_points(item)
            if points <= unlock_threshold:
                candidate_pool.append(item)

        if not candidate_pool:
            msg_lines = [
                "❌ 未找到可自动解锁的资源",
                f"关键词: {query}",
                f"阈值: {unlock_threshold} 积分",
                "候选资源:",
            ]
            for item in collected_resources[:max_results]:
                msg_lines.append(f"- {_format_resource_line(item)}")
            msg = "\n".join(msg_lines)
            for tid in target_ids:
                _enqueue_message(tid, msg)
            _append_self_service_log([f"[{ts}] [WARN] open_api no affordable resources query={query}"])
            return

        picked = None
        picked_slug = ""
        picked_resp = None
        picked_full_url = ""
        filtered_count = 0
        for item in candidate_pool[:max_results]:
            slug = item.get("slug") or ""
            if not slug:
                continue
            unlock_resp = _hdhive_open_api_unlock(base_url, open_api_key, slug)
            if not unlock_resp.get("success"):
                continue
            data = unlock_resp.get("data") if isinstance(unlock_resp, dict) else {}
            full_url = ""
            if isinstance(data, dict):
                full_url = data.get("full_url") or ""
                if not full_url and data.get("url"):
                    url = data.get("url")
                    access_code = data.get("access_code")
                    if access_code:
                        sep = "&" if "?" in url else "?"
                        full_url = f"{url}{sep}password={access_code}"
                    else:
                        full_url = url
            if full_url and not _match_storage_mode(full_url, storage_mode):
                filtered_count += 1
                continue
            picked = item
            picked_slug = slug
            picked_resp = unlock_resp
            picked_full_url = full_url
            break

        if not picked:
            msg_lines = [
                "❌ 未找到可用资源",
                f"关键词: {query}",
                f"阈值: {unlock_threshold} 积分",
            ]
            if storage_mode != "any":
                msg_lines.append(f"说明: 已过滤非 {storage_label} 网盘资源")
            msg_lines.append("候选资源:")
            for item in collected_resources[:max_results]:
                msg_lines.append(f"- {_format_resource_line(item)}")
            msg = "\n".join(msg_lines)
            for tid in target_ids:
                _enqueue_message(tid, msg)
            _append_self_service_log([f"[{ts}] [WARN] open_api no usable resources query={query} filtered={filtered_count}"])
            return

        msg_lines = [
            "🎬 自助观影申请结果（Open API）",
            f"关键词: {query}",
            f"资源: {picked.get('title') or '-'}",
            f"Slug: {picked_slug}",
            f"解锁结果: {picked_resp.get('message') or 'success'}",
        ]
        msg_lines.append(f"链接: {picked_full_url or '未返回'}")
        msg = "\n".join(msg_lines)
        for tid in target_ids:
            _enqueue_message(tid, msg)
        _append_self_service_log([f"[{ts}] [INFO] open_api success slug={picked_slug}"])
        return

    candidates = []
    debug_lines = []
    used_query = query
    tmdb_urls = []
    tmdb_ids = []

    if direct_url:
        normalized = direct_url
        if not normalized.startswith("http"):
            normalized = f"{base_url.rstrip('/')}/resource/115/{normalized.strip('/')}"
        candidates = [normalized]
        used_query = query or title or direct_url
    else:
        query_candidates = []
        if query:
            query_candidates.append(query)
        if title:
            combos = []
            if request_year:
                combos.append(f"{title} {request_year}")
            if request_type:
                combos.append(f"{title} {request_type}")
            combos.append(title)
            for item in combos:
                if item and item not in query_candidates:
                    query_candidates.append(item)

        for q in query_candidates:
            candidates, debug_info, found_tmdb_urls, found_tmdb_ids = _search_hdhive_resource_urls(
                q,
                max_results=max_results,
                cookie_header=hdhive_cookie,
                base_url=base_url,
            )
            for item in (found_tmdb_urls or []):
                if item not in tmdb_urls:
                    tmdb_urls.append(item)
            for item in (found_tmdb_ids or []):
                if item not in tmdb_ids:
                    tmdb_ids.append(item)
            if debug_info:
                debug_lines.extend(debug_info[:8])
            if candidates:
                used_query = q
                break

        # TMDB fallback
        if not candidates:
            tmdb_key = ""
            try:
                tmdb_key = (payload.get("tmdb_api_key") or "").strip()
            except Exception:
                tmdb_key = ""
            tmdb_ids = []
            if tmdb_key and title:
                if request_type == "电影":
                    tmdb_ids = _tmdb_search_ids(tmdb_key, title, request_year, "movie")
                elif request_type == "电视剧":
                    tmdb_ids = _tmdb_search_ids(tmdb_key, title, request_year, "tv")
                else:
                    tmdb_ids = _tmdb_search_ids(tmdb_key, title, request_year, "tv")
                    if len(tmdb_ids) < 3:
                        tmdb_ids += _tmdb_search_ids(tmdb_key, title, request_year, "movie")
            if tmdb_ids:
                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Referer": f"{base_url}/",
                }
                if hdhive_cookie:
                    headers["Cookie"] = hdhive_cookie
                for tid in tmdb_ids[:5]:
                    for prefix in ("movie", "tv"):
                        tmdb_url = f"{base_url}/tmdb/{prefix}/{tid}?_rsc={int(time.time() * 1000)}"
                        _fetch_hdhive_urls_from_url(tmdb_url, headers, base_url, candidates, max_results, debug_lines)
                        if candidates:
                            used_query = f"tmdb:{prefix}:{tid}"
                            break
                    if candidates:
                        break
    if not candidates:
        msg_lines = [
            "❌ 自助观影申请未找到匹配资源",
            f"关键词: {query}",
        ]
        if request_type:
            msg_lines.append(f"类型: {request_type}")
        if request_year:
            msg_lines.append(f"年份: {request_year}")
        if request_note:
            msg_lines.append(f"备注: {request_note}")
        msg = "\n".join(msg_lines)
        for tid in target_ids:
            _enqueue_message(tid, msg)
        log_line = f"[{ts}] [WARN] no results for query={query}"
        if debug_lines:
            _append_self_service_log([log_line] + [f"[{ts}] [DEBUG] {line}" for line in debug_lines])
        else:
            _append_self_service_log([log_line])
        return

    primary = candidates[0]
    real_url = None
    note = ""
    candidate_notes = []
    try:
        import telegram_monitor as tg_monitor
        tg_monitor.current_config = tg_monitor.load_config()
        for idx, cand in enumerate(candidates[:max_results], start=1):
            real_url, note = tg_monitor._resolve_hdhive_115_url_with_note_sync(cand)
            if real_url and not _match_storage_mode(real_url, storage_mode):
                real_url = None
                if storage_mode != "any":
                    note = f"非 {storage_label} 网盘已过滤"
            candidate_notes.append((cand, note))
            if real_url:
                primary = cand
                break

        if not real_url and (tmdb_urls or tmdb_ids):
            extra_candidates = []
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": f"{base_url}/",
            }
            if hdhive_cookie:
                headers["Cookie"] = hdhive_cookie
            for tmdb_url in tmdb_urls[:10]:
                _fetch_hdhive_urls_from_url(tmdb_url, headers, base_url, extra_candidates, max_results, debug_lines)
            for key in tmdb_ids[:10]:
                try:
                    mt, tid = key.split(":", 1)
                except Exception:
                    continue
                tmdb_url = f"{base_url}/tmdb/{mt}/{tid}?_rsc={int(time.time() * 1000)}"
                _fetch_hdhive_urls_from_url(tmdb_url, headers, base_url, extra_candidates, max_results, debug_lines)
            tried = {url for url, _ in candidate_notes}
            for cand in extra_candidates:
                if cand in tried:
                    continue
                real_url, note = tg_monitor._resolve_hdhive_115_url_with_note_sync(cand)
                if real_url and not _match_storage_mode(real_url, storage_mode):
                    real_url = None
                    if storage_mode != "any":
                        note = f"非 {storage_label} 网盘已过滤"
                candidate_notes.append((cand, note))
                if real_url:
                    primary = cand
                    break
    except Exception as e:
        real_url = None
        note = f"解析异常: {e}"
        candidate_notes.append((primary, note))

    msg_lines = [
        "🎬 自助观影申请结果",
        f"关键词: {query}",
    ]
    if request_type:
        msg_lines.append(f"类型: {request_type}")
    if request_year:
        msg_lines.append(f"年份: {request_year}")
    if request_note:
        msg_lines.append(f"备注: {request_note}")
    msg_lines.append(f"影巢资源: {primary}")
    if real_url:
        msg_lines.append(f"网盘链接: {real_url}")
        if note:
            msg_lines.append(f"说明: {note}")
    else:
        msg_lines.append("网盘链接: 未解析")
        if candidate_notes:
            msg_lines.append("候选解析结果:")
            for idx, (url, cnote) in enumerate(candidate_notes[:max_results], start=1):
                msg_lines.append(f"{idx}. {url} | {cnote or '未解析'}")
        elif note:
            msg_lines.append(f"说明: {note}")
    if len(candidates) > 1:
        msg_lines.append("候选资源:")
        for idx, url in enumerate(candidates[:max_results], start=1):
            msg_lines.append(f"{idx}. {url}")

    msg = "\n".join(msg_lines)
    for tid in target_ids:
        _enqueue_message(tid, msg)

    _append_self_service_log([f"[{ts}] [INFO] done query={used_query} primary={primary} ok={bool(real_url)} note={note}"])


def _resolve_log_view_config(cfg: dict):
    interval = cfg.get("log_refresh_interval_seconds", LOG_REFRESH_INTERVAL_DEFAULT)
    try:
        interval = int(interval or LOG_REFRESH_INTERVAL_DEFAULT)
    except Exception:
        interval = LOG_REFRESH_INTERVAL_DEFAULT
    interval = max(2, min(interval, 60))

    auto_refresh = bool(cfg.get("log_auto_refresh_enabled", False))

    max_lines = cfg.get("log_tail_lines", LOG_TAIL_LINES)
    try:
        max_lines = int(max_lines or LOG_TAIL_LINES)
    except Exception:
        max_lines = LOG_TAIL_LINES
    max_lines = max(50, min(max_lines, 2000))

    max_bytes = cfg.get("log_tail_max_bytes", LOG_TAIL_MAX_BYTES)
    try:
        max_bytes = int(max_bytes or LOG_TAIL_MAX_BYTES)
    except Exception:
        max_bytes = LOG_TAIL_MAX_BYTES
    max_bytes = max(256 * 1024, min(max_bytes, 10 * 1024 * 1024))

    return interval, auto_refresh, max_lines, max_bytes


def _drama_scheduler_loop():
    next_run_ts = 0.0
    last_signature = None

    while not _DRAMA_SCHEDULER_STOP_EVENT.is_set():
        config = load_config()
        cfg = config.get('drama_calendar', {}) if isinstance(config, dict) else {}
        enabled = bool(cfg.get('auto_sync_enabled', False))
        try:
            interval_minutes = int(cfg.get('auto_sync_interval_minutes', 60) or 60)
        except Exception:
            interval_minutes = 60
        interval_minutes = max(1, interval_minutes)
        cron_expr = _normalize_cron_expr(str(cfg.get('auto_sync_cron_expr', '') or ''))
        use_cron = bool(cron_expr)

        signature = (
            enabled,
            interval_minutes,
            cron_expr,
            _drama_sources_csv(cfg.get('source', 'calendar')),
            str(cfg.get('env_key', 'DRAMA_CALENDAR_REGEX')),
            str(cfg.get('env_files', '')),
            bool(cfg.get('include_maoyan_web_heat', True)),
            int(cfg.get('maoyan_top_n', 0) or 0),
            int(cfg.get('maoyan_web_heat_top_n', 0) or 0),
        )

        if signature != last_signature:
            last_signature = signature
            if enabled:
                next_run_ts = time.time() + 5

        if not enabled:
            _set_drama_scheduler_state(enabled=False, running=False, next_run_at='未启用', schedule_mode='interval')
            _DRAMA_SCHEDULER_STOP_EVENT.wait(2)
            continue

        if use_cron and not _cron_expr_valid(cron_expr):
            _set_drama_scheduler_state(
                enabled=True,
                running=False,
                schedule_mode='cron',
                last_status='error',
                last_message=f'Cron 表达式无效: {cron_expr}',
                next_run_at='Cron 表达式无效',
            )
            _DRAMA_SCHEDULER_STOP_EVENT.wait(5)
            continue

        now = time.time()
        if next_run_ts <= 0:
            if use_cron:
                next_run_ts = _next_run_by_cron(cron_expr, now)
            else:
                next_run_ts = now + 5

        if now < next_run_ts:
            _set_drama_scheduler_state(
                enabled=True,
                running=False,
                schedule_mode=('cron' if use_cron else 'interval'),
                next_run_at=_format_scheduler_ts(next_run_ts),
            )
            _DRAMA_SCHEDULER_STOP_EVENT.wait(min(5, max(1, int(next_run_ts - now))))
            continue

        _set_drama_scheduler_state(enabled=True, running=True)
        ok, out, err = _run_drama_calendar_update(cfg, dry_run=False, trigger='scheduler')
        output_lines = [ln for ln in (out or '').splitlines() if ln.strip()]
        err_lines = [ln for ln in (err or '').splitlines() if ln.strip()]
        summary_items = _build_drama_summary_items(output_lines, err_lines, ok=ok, dry_run=False)
        if ok:
            summary = _summarize_drama_output_lines(output_lines, prefer_write=True, max_parts=4) or '自动执行成功'
            status = 'success'
        elif (err or '').strip() == DRAMA_RUN_BUSY_MESSAGE:
            summary = '已有任务在运行，已跳过本次自动调度'
            status = 'success'
            summary_items = {'extract': '', 'removed': '', 'result': '已有任务运行，已跳过'}
        else:
            summary_out = ' | '.join(output_lines[:3]) if output_lines else ''
            summary_err = ' | '.join(err_lines[:3]) if err_lines else '未知错误'
            summary = (summary_out + ' ' + summary_err).strip()
            status = 'error'

        if use_cron:
            next_run_ts = _next_run_by_cron(cron_expr, time.time())
        else:
            next_run_ts = time.time() + interval_minutes * 60
        _set_drama_scheduler_state(
            enabled=True,
            running=False,
            last_run_at=_format_scheduler_ts(time.time()),
            last_status=status,
            last_message=summary,
            last_summary=summary_items,
            schedule_mode=('cron' if use_cron else 'interval'),
            next_run_at=_format_scheduler_ts(next_run_ts),
        )

    _set_drama_scheduler_state(running=False, next_run_at='已停止')


def start_drama_scheduler():
    global _DRAMA_SCHEDULER_THREAD
    if _DRAMA_SCHEDULER_THREAD and _DRAMA_SCHEDULER_THREAD.is_alive():
        return
    _DRAMA_SCHEDULER_STOP_EVENT.clear()
    _DRAMA_SCHEDULER_THREAD = threading.Thread(
        target=_drama_scheduler_loop,
        name='drama-calendar-scheduler',
        daemon=True,
    )
    _DRAMA_SCHEDULER_THREAD.start()


def load_download_risk_stats():
    default_stats = {
        "blocked_total": 0,
        "reasons": {},
        "last_blocked_reason": "",
        "last_blocked_at": ""
    }
    try:
        if not os.path.exists(DOWNLOAD_RISK_STATS_FILE):
            return default_stats
        with open(DOWNLOAD_RISK_STATS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return default_stats
        merged = default_stats.copy()
        merged.update(data)
        if not isinstance(merged.get('reasons'), dict):
            merged['reasons'] = {}
        return merged
    except Exception:
        return default_stats


def clear_download_risk_stats():
    try:
        data = {
            "blocked_total": 0,
            "reasons": {},
            "last_blocked_reason": "",
            "last_blocked_at": ""
        }
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(DOWNLOAD_RISK_STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

# Global Telethon client instance for authentication process in Flask
# This will be short-lived for auth and not the long-running monitor
phone_number_for_auth = None # To store phone number between code request and verification
code_hash_for_auth = None # To store code hash between code request and verification

# --- Web Authentication Decorator ---
def login_required(f):
    is_async_func = inspect.iscoroutinefunction(f)

    @wraps(f)
    async def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            flash('请先登录以访问此页面。', 'warning')
            return redirect(url_for('web_login'))
        
        if is_async_func:
            return await f(*args, **kwargs)
        else:
            # If f is synchronous, call it directly.
            # Flask's async context will handle the Response object.
            return f(*args, **kwargs)
            
    return decorated_function

# --- Process Management Logic ---
class ProcessManager:
    def __init__(self, script_path, name, log_buffer_size=200):
        self.script_path = script_path
        self.name = name
        self.process = None
        self.log_buffer = []
        self.log_buffer_size = log_buffer_size
        self.log_file_path = os.path.join(LOG_DIR, f"{self.name.replace(' ', '_')}.log")
        self._log_file_lock = threading.Lock()
        self._log_cache_mtime = None
        self._log_cache_size = None
        self._log_cache_lines = None
        self._log_cache_max_lines = LOG_TAIL_LINES
        self._log_cache_max_bytes = LOG_TAIL_MAX_BYTES
        self._render_cache_key = None
        self._render_cache_output = None
        self._last_size_check = 0.0

    def _append_to_log_file(self, entry):
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            with self._log_file_lock:
                with open(self.log_file_path, 'a', encoding='utf-8') as f:
                    f.write(entry + "\n")
                self._trim_log_file_if_needed()
        except Exception:
            pass

    def _trim_log_file_if_needed(self):
        now = time.monotonic()
        if now - self._last_size_check < LOG_FILE_SIZE_CHECK_INTERVAL:
            return
        self._last_size_check = now
        try:
            size = os.path.getsize(self.log_file_path)
        except Exception:
            return
        if size <= LOG_FILE_MAX_BYTES:
            return

        keep_bytes = min(LOG_FILE_TRIM_BYTES, size)
        try:
            with open(self.log_file_path, 'rb') as f:
                if keep_bytes < size:
                    f.seek(-keep_bytes, os.SEEK_END)
                data = f.read()
            with open(self.log_file_path, 'wb') as f:
                f.write(data)
            self._log_cache_mtime = None
            self._log_cache_size = None
            self._log_cache_lines = None
        except Exception:
            pass

    def _current_log_cache_key(self):
        try:
            if os.path.exists(self.log_file_path):
                mtime = os.path.getmtime(self.log_file_path)
                size = os.path.getsize(self.log_file_path)
                return ("file", int(mtime * 1000), size)
        except Exception:
            pass
        return ("buffer", len(self.log_buffer))

    def get_log_cache_key(self, max_lines: int = LOG_TAIL_LINES, max_bytes: int = LOG_TAIL_MAX_BYTES) -> str:
        key = self._current_log_cache_key()
        return f"{key[0]}:{key[1]}:{key[2] if len(key) > 2 else ''}:{max_lines}:{max_bytes}"

    def get_full_log_lines(self, max_lines: int = LOG_TAIL_LINES, max_bytes: int = LOG_TAIL_MAX_BYTES):
        """读取日志，进度行只保留最新一条"""
        lines = []
        try:
            if os.path.exists(self.log_file_path):
                mtime = os.path.getmtime(self.log_file_path)
                size = os.path.getsize(self.log_file_path)
                if (
                    self._log_cache_lines is not None
                    and self._log_cache_mtime == mtime
                    and self._log_cache_size == size
                    and self._log_cache_max_lines == max_lines
                    and self._log_cache_max_bytes == max_bytes
                ):
                    lines = list(self._log_cache_lines)
                else:
                    self._trim_log_file_if_needed()
                    lines = _tail_file_lines(self.log_file_path, max_lines=max_lines, max_bytes=max_bytes)
                    self._log_cache_mtime = mtime
                    self._log_cache_size = size
                    self._log_cache_lines = list(lines)
                    self._log_cache_max_lines = max_lines
                    self._log_cache_max_bytes = max_bytes
        except Exception:
            pass
        if not lines:
            lines = list(self.log_buffer)
        
        # 合并连续的进度日志，只保留最新一条
        result = []
        for line in lines:
            is_progress = '下载进度' in line
            if is_progress and result and '下载进度' in result[-1]:
                result[-1] = line  # 替换上一条进度
            else:
                result.append(line)
        return result

    def get_colored_log_output(self, colorizer, empty_text: str = "暂无日志。", max_lines: int = LOG_TAIL_LINES, max_bytes: int = LOG_TAIL_MAX_BYTES) -> str:
        cache_key = self._current_log_cache_key()

        render_key = (cache_key, max_lines, max_bytes)
        if self._render_cache_key == render_key and self._render_cache_output is not None:
            return self._render_cache_output

        lines = self.get_full_log_lines(max_lines=max_lines, max_bytes=max_bytes)
        if not lines:
            output = empty_text
        else:
            colored_log_lines = [colorizer(line) for line in lines]
            output = "\n".join(reversed(colored_log_lines))

        self._render_cache_key = render_key
        self._render_cache_output = output
        return output

    def clear_logs(self):
        self.log_buffer.clear()
        try:
            if os.path.exists(self.log_file_path):
                with open(self.log_file_path, 'w', encoding='utf-8') as f:
                    f.write('')
        except Exception:
            pass

    def start(self):
        if self.process and self.process.poll() is None:
            return True
        try:
            self.log_buffer.clear()
            self.process = subprocess.Popen(
                [sys.executable, '-u', self.script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            threading.Thread(target=self._read_output, args=(self.process.stdout, False), daemon=True).start()
            threading.Thread(target=self._read_output, args=(self.process.stderr, True), daemon=True).start()
            print(f"{self.name} 已启动。")
            return True
        except Exception as e:
            print(f"启动 {self.name} 失败: {e}")
            return False

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None
            return True
        return False

    def status(self):
        if not self.process: return "未启动"
        ret = self.process.poll()
        if ret is None: return "运行中"
        return f"已停止 (Exit Code: {ret})"

    def _read_output(self, pipe, is_error):
        for line in iter(pipe.readline, ''):
            if line:
                entry = f"ERROR: {line.strip()}" if is_error else line.strip()
                if self.script_path == 'app/bot_monitor.py':
                    from downloader_module import downloader
                    downloader.log(line.strip())
                
                # 检测是否为进度日志（单行更新）
                is_progress_log = '下载进度' in entry or '下载完成' in entry
                
                if is_progress_log and len(self.log_buffer) > 0:
                    # 检查最后一条是否也是进度日志
                    last_entry = self.log_buffer[-1]
                    if '下载进度' in last_entry:
                        # 替换最后一条进度日志
                        self.log_buffer[-1] = entry
                    else:
                        # 最后一条不是进度，正常追加
                        self.log_buffer.append(entry)
                else:
                    # 非进度日志或 buffer 为空，正常追加
                    self.log_buffer.append(entry)
                
                # 写入文件时始终追加（保留完整历史）
                self._append_to_log_file(entry)
                
                if len(self.log_buffer) > self.log_buffer_size:
                    self.log_buffer.pop(0)
        pipe.close()

# Initialize managers
tg_monitor_mgr = ProcessManager('telegram_monitor.py', 'Telegram 监控程序')
file_monitor_mgr = ProcessManager('file_monitor.py', '文件监控程序')
bot_monitor_mgr = ProcessManager('app/bot_monitor.py', 'Bot 监控程序')

# Define buffers and processes for backward compatibility with templates and routes
monitor_log_buffer = tg_monitor_mgr.log_buffer
file_monitor_log_buffer = file_monitor_mgr.log_buffer

def _has_file_monitor_tasks(cfg: dict) -> bool:
    tasks = (cfg or {}).get('file_monitoring_tasks', [])
    if not isinstance(tasks, list):
        return False
    for task in tasks:
        if isinstance(task, dict) and (task.get('source_dir') or task.get('destination_dir')):
            return True
    return False

def start_monitor_process(): return tg_monitor_mgr.start()
def stop_monitor_process():
    if tg_monitor_mgr.stop():
        flash("Telegram 监控程序已停止。", "info")
        return True
    return False
def get_monitor_status(): return tg_monitor_mgr.status()

def start_file_monitor_process():
    cfg = load_config()
    if not _has_file_monitor_tasks(cfg):
        # No tasks: keep process stopped to save CPU.
        if file_monitor_mgr.process and file_monitor_mgr.process.poll() is None:
            file_monitor_mgr.stop()
        return False
    return file_monitor_mgr.start()
def stop_file_monitor_process():
    if file_monitor_mgr.stop():
        flash("文件监控程序已停止。", "info")
        return True
    return False
def get_file_monitor_status(): return file_monitor_mgr.status()

def start_bot_monitor_process(): return bot_monitor_mgr.start()
def stop_bot_monitor_process():
    if bot_monitor_mgr.stop():
        flash("Bot 监控程序已停止。", "info")
        return True
    return False
def get_bot_monitor_status(): return bot_monitor_mgr.status()

def colorize_log_line(line):
    """Applies CSS classes to log line based on keywords for colorization, supports ANSI color codes."""
    import uuid
    
    # ANSI color code mapping to HTML colors
    ansi_to_html = {
        '\033[0m': '</span>',       # Reset
        '\033[90m': '<span style="color: #888;">',   # Gray (DEBUG)
        '\033[91m': '<span style="color: #dc3545;">',  # Red (ERROR)
        '\033[92m': '<span style="color: #28a745;">',  # Green (Success)
        '\033[93m': '<span style="color: #ffc107;">',  # Yellow (Warning)
        '\033[96m': '<span style="color: #17a2b8;">',  # Cyan (Progress/秒传)
    }
    
    if '\x1b[' not in line:
        return html.escape(line)

    # Step 1: Replace ANSI codes with unique markers before HTML escaping
    markers = {}
    for ansi_code, html_tag in ansi_to_html.items():
        if ansi_code in line:
            marker = f"___ANSI_{uuid.uuid4().hex}___"
            markers[marker] = html_tag
            line = line.replace(ansi_code, marker)
    
    # Step 2: Escape HTML to prevent XSS
    line = html.escape(line)
    
    # Step 3: Restore markers with HTML tags
    for marker, html_tag in markers.items():
        line = line.replace(marker, html_tag)

    # Step 4: Highlight URLs
    try:
        line = re.sub(
            r'(https?://[^\s<]+)',
            r'<span class="text-warning fw-bold text-decoration-underline">\1</span>',
            line,
            flags=re.IGNORECASE,
        )
    except Exception:
        pass
    
    # Step 5: Apply additional colorization based on keywords (only if no ANSI color detected)
    has_ansi_color = any(marker in line for marker in markers.keys()) or '<span style="color:' in line
    if not has_ansi_color:
        if "ERROR:" in line or "失败:" in line or "失败。" in line:
            return f'<span class="text-danger fw-bold">{line}</span>'
        elif "成功" in line or "已启动" in line:
            return f'<span class="text-success fw-bold">{line}</span>'
        elif "警告" in line or "warning" in line:
            return f'<span class="text-warning fw-bold">{line}</span>'
        elif "INFO:" in line or "info" in line or "正在连接" in line or "已更新" in line:
            return f'<span class="text-info fw-bold">{line}</span>'
    
    return line


# --- Flask Routes ---
@app.route('/')
@login_required
async def index():
    config = load_config()
    download_risk_stats = load_download_risk_stats()
    api_id = config['telegram'].get('api_id')
    api_hash = config['telegram'].get('api_hash')
    bot_token = config.get('bot', {}).get('token')
    
    api_id_env = os.environ.get('TELEGRAM_API_ID')
    api_hash_env = os.environ.get('TELEGRAM_API_HASH')
    bot_token_env = os.environ.get('TELEGRAM_BOT_TOKEN')
    
    # 检查凭证是否直接写在 config.json 中
    # 注意：load_config() 已经尝试用环境变量覆盖了，所以如果 api_id 有值且不是来自环境变量，
    # 那它一定是从 config.json 中读取的基础值且没有对应的环境变量覆盖。
    
    # 这里的逻辑稍作调整：如果环境变量缺失任何一个，且 config.json 中有任何一个值，则提醒
    if not (api_id_env and api_hash_env):
        if api_id or api_hash:
             flash("警告: 建议将 Telegram API 凭据存储在环境变量中而不是 config.json 中。", "warning")
    
    if not bot_token_env and bot_token:
        flash("提示: 建议将 Bot Token 也存储在环境变量 (TELEGRAM_BOT_TOKEN) 中。", "info")

    auth_status = "未认证"
    session_file = os.path.join(CONFIG_DIR, TELEGRAM_SESSION_NAME + '.session') # Updated path
    if os.path.exists(session_file):
        auth_status = "已认证 (会话文件存在)"
    
    return render_template('index.html', 
                                  config=config, 
                                  auth_status=auth_status,
                                  monitor_status=get_monitor_status(),
                                  file_monitor_status=get_file_monitor_status(),
                                  bot_monitor_status=get_bot_monitor_status(),
                                  download_risk_stats=download_risk_stats)

@app.route('/toggle_debug', methods=['POST'])
@login_required
def toggle_debug():
    config = load_config()
    debug_mode = request.form.get('debug_mode') == 'on'
    config['debug_mode'] = debug_mode
    save_config(config)

    if get_monitor_status() == "运行中":
        stop_monitor_process()
        start_monitor_process()

    flash(f"DEBUG 模式已{'开启' if debug_mode else '关闭'}", "success")
    return redirect(url_for('index'))

@app.route('/login', methods=['GET', 'POST'])
async def login():
    global phone_number_for_auth, code_hash_for_auth
    config = load_config()

    api_id_env = os.environ.get('TELEGRAM_API_ID')
    api_hash_env = os.environ.get('TELEGRAM_API_HASH')

    if api_id_env and api_hash_env:
        config['telegram']['api_id'] = int(api_id_env)
        config['telegram']['api_hash'] = api_hash_env
        flash("正在使用环境变量中的 Telegram API 凭据。", "info")

    if request.method == 'POST':
        api_id_input = request.form['api_id']
        api_hash_input = request.form['api_hash']
        phone = request.form['phone']

        try:
            # Save to config only if not using env vars
            if not (api_id_env and api_hash_env):
                config['telegram']['api_id'] = int(api_id_input)
                config['telegram']['api_hash'] = api_hash_input
                save_config(config)

            # Initialize client for authentication
            session_file_path = os.path.join(CONFIG_DIR, TELEGRAM_SESSION_NAME) # Updated path
            flask_telethon_client = TelegramClient(session_file_path, int(api_id_input), api_hash_input) # Updated session_name
            await flask_telethon_client.connect()

            # Send code request
            send_code_result = await flask_telethon_client.send_code_request(phone)
            
            phone_number_for_auth = phone
            code_hash_for_auth = send_code_result.phone_code_hash
            
            flash(f"验证码已发送到 {phone}，请检查您的 Telegram 应用。", "info")
            
            # Crucially, disconnect the client after sending code request.
            # The session file is already created.
            await flask_telethon_client.disconnect()
            flask_telethon_client = None # Clear global reference
            
            return redirect(url_for('verify_code'))

        except Exception as e:
            flash(f"登录失败: {e}", "error")
            if flask_telethon_client:
                await flask_telethon_client.disconnect()
            flask_telethon_client = None
            return redirect(url_for('login'))
    
    return render_template('login.html', config=config, api_id_env=api_id_env, api_hash_env=api_hash_env)

@app.route('/verify_code', methods=['GET', 'POST'])
async def verify_code():
    global phone_number_for_auth, code_hash_for_auth # No longer need flask_telethon_client globally here

    if not phone_number_for_auth or not code_hash_for_auth:
        flash("请先进行登录。", "warning")
        return redirect(url_for('login'))

    config = load_config()
    api_id_env = os.environ.get('TELEGRAM_API_ID')
    api_hash_env = os.environ.get('TELEGRAM_API_HASH')

    if api_id_env and api_hash_env:
        api_id = int(api_id_env)
        api_hash = api_hash_env
    else:
        api_id = config['telegram'].get('api_id')
        api_hash = config['telegram'].get('api_hash')
    
    session_name = config['telegram'].get('session_name', TELEGRAM_SESSION_NAME)

    # Ensure API credentials are set
    if not api_id or not api_hash:
        flash("Telegram API 凭据未设置。", "error")
        return redirect(url_for('login'))

    # Re-instantiate client, it should load the session created in login
    session_file_path = os.path.join(CONFIG_DIR, session_name) # Updated path
    client_for_verify = TelegramClient(session_file_path, api_id, api_hash) # Updated session_name
    
    if request.method == 'POST':
        code = request.form['code']
        password = request.form.get('password') # For 2FA

        try:
            await client_for_verify.connect() # Connect the new client

            if password:
                await client_for_verify.sign_in(password=password)
            else:
                await client_for_verify.sign_in(phone=phone_number_for_auth, phone_code_hash=code_hash_for_auth, code=code)
            
            # Authentication successful
            flash("Telegram 认证成功！", "success")
            
            # Disconnect the client used for auth
            await client_for_verify.disconnect()
            # Clear global auth state variables
            phone_number_for_auth = None
            code_hash_for_auth = None

            return redirect(url_for('index'))

        except PhoneCodeExpiredError:
            flash("验证码已过期，请重新发送请求。", "error")
            await client_for_verify.disconnect() # Disconnect on error
            phone_number_for_auth = None
            code_hash_for_auth = None
            return redirect(url_for('login'))
        except PhoneCodeInvalidError:
            flash("验证码错误，请重新输入。", "error")
            # Don't disconnect, user might re-enter code
            if client_for_verify.is_connected():
                await client_for_verify.disconnect()
            return render_template('verify_code.html', show_password_input=True)
        except SessionPasswordNeededError:
            flash("您的账户启用了两步验证。请输入密码。", "info")
            # Don't disconnect, user needs to enter password
            if client_for_verify.is_connected():
                await client_for_verify.disconnect()
            return render_template('verify_code.html', show_password_input=True)
        except Exception as e:
            flash(f"认证失败: {e}", "error")
            if client_for_verify.is_connected():
                await client_for_verify.disconnect() # Disconnect on other errors
            phone_number_for_auth = None
            code_hash_for_auth = None
            return redirect(url_for('login'))
    
    # Ensure client is disconnected if GET request without post
    if client_for_verify.is_connected():
        await client_for_verify.disconnect()
    
    return render_template('verify_code.html')

@app.route('/logout')
@login_required
async def logout():
    session_file = os.path.join(CONFIG_DIR, TELEGRAM_SESSION_NAME + '.session') # Updated path
    if os.path.exists(session_file):
        os.remove(session_file)
        flash("会话文件已清除，您已登出。", "info")
    else:
        flash("没有找到会话文件。", "warning")
    
    # Also stop monitor process if running
    stop_monitor_process()

    return redirect(url_for('index'))

@app.route('/config', methods=['GET', 'POST'])
@login_required
def manage_config():
    config = load_config()
    if request.method == 'POST':
        action = request.form.get('action')
        should_restart_monitor = True
        if action == 'add':
            source_channel = request.form['source_channel']
            target_users = request.form['target_users']
            try:
                source_channel_id = int(source_channel)
                target_user_ids = [uid.strip() for uid in target_users.split(',') if uid.strip()]
                
                # Check if already exists
                if any(entry['source_channel_id'] == source_channel_id for entry in config['channels_to_forward']):
                    flash(f"频道 {source_channel_id} 已存在。", "warning")
                else:
                    config['channels_to_forward'].append({
                        "source_channel_id": source_channel_id,
                        "target_user_ids": target_user_ids
                    })
                    save_config(config)
                    flash("配置添加成功！", "success")
            except ValueError:
                flash("频道 ID 必须是整数；用户 ID 可以是整数或用户名。", "error")
            
        elif action == 'delete':
            channel_id_to_delete = int(request.form['channel_id'])
            config['channels_to_forward'] = [
                entry for entry in config['channels_to_forward'] 
                if entry['source_channel_id'] != channel_id_to_delete
            ]
            save_config(config)
            flash("配置删除成功！", "success")
        
        elif action == 'add_restricted':
            channel_id = request.form['restricted_channel_id']
            download_directory = request.form.get('download_directory', '').strip()
            download_video = request.form.get('download_video') == 'on'
            keep_video_message = request.form.get('keep_video_message') == 'on'
            convert_hdhive = request.form.get('convert_hdhive') == 'on'
            target_user_ids_restricted = request.form['target_user_ids_restricted']
            group_name = request.form.get('group_name', '默认分组').strip()
            channel_name = request.form.get('channel_name', '').strip()
            old_channel_id = request.form.get('old_restricted_channel_id', '').strip()
            
            # 获取关键字和监控类型
            blacklist_keywords = [k.strip() for k in request.form.get('blacklist_keywords', '').split(',') if k.strip()]
            whitelist_keywords = [k.strip() for k in request.form.get('whitelist_keywords', '').split(',') if k.strip()]
            auto_click_redpacket = request.form.get('auto_click_redpacket') == 'on'
            auto_click_keywords = [k.strip() for k in request.form.get('auto_click_keywords', '').split(',') if k.strip()]
            auto_click_button_texts = [k.strip() for k in request.form.get('auto_click_button_texts', '').split(',') if k.strip()]
            auto_click_notify_targets = [k.strip() for k in request.form.get('auto_click_notify_targets', '').split(',') if k.strip()]
            monitor_types = request.form.getlist('monitor_types')
            if not monitor_types:
                monitor_types = ['video']  # 默认监控视频
            
            # 调试日志
            print(f"DEBUG: channel_id={channel_id}, old_channel_id='{old_channel_id}', keep_video_message={keep_video_message}")
            
            try:
                channel_id_int = int(channel_id)
                target_user_ids_list = [uid.strip() for uid in target_user_ids_restricted.split(',') if uid.strip()]

                auto_click_enabled = auto_click_redpacket or bool(auto_click_keywords)

                # 如果不是转发模式，检查下载目录必须存在
                if not keep_video_message and (not download_directory or not os.path.isdir(download_directory)):
                    # 如果 monitor_types 只包含 'text'，则不需要下载目录
                    if monitor_types != ['text']:
                        # 仅做红包/自动点击且未配置下载目录时，自动降级为文本监控
                        if auto_click_enabled and monitor_types == ['video']:
                            monitor_types = ['text']
                            flash("未指定下载目录，已自动切换为文本监控（用于红包自动点击）。", "info")
                        else:
                            flash("下载模式必须指定有效的下载目录。", "error")
                            return redirect(url_for('manage_config'))

                # Check if this is an update (old_channel_id is set)
                if old_channel_id:
                    old_channel_id_int = int(old_channel_id)
                    # Check if channel_id changed and new_id already exists
                    if channel_id_int != old_channel_id_int:
                        if any(entry['channel_id'] == channel_id_int for entry in config['restricted_channels']):
                            flash(f"受限频道 {channel_id_int} 已存在。", "warning")
                            return redirect(url_for('manage_config'))
                    
                    # Update existing channel
                    found = False
                    for entry in config['restricted_channels']:
                        if entry['channel_id'] == old_channel_id_int:
                            entry['channel_id'] = channel_id_int
                            entry['download_directory'] = download_directory
                            entry['download_video'] = download_video
                            entry['keep_video_message'] = keep_video_message
                            entry['convert_hdhive'] = convert_hdhive
                            entry['target_user_ids'] = target_user_ids_list
                            entry['group_name'] = group_name
                            entry['channel_name'] = channel_name
                            entry['blacklist_keywords'] = blacklist_keywords
                            entry['whitelist_keywords'] = whitelist_keywords
                            entry['auto_click_redpacket'] = auto_click_redpacket
                            entry['auto_click_keywords'] = auto_click_keywords
                            entry['auto_click_button_texts'] = auto_click_button_texts
                            entry['auto_click_notify_targets'] = auto_click_notify_targets
                            entry['monitor_types'] = monitor_types
                            found = True
                            break
                    
                    if found:
                        # Update group_order if new group
                        if group_name not in config.get('group_order', []):
                            config.setdefault('group_order', []).append(group_name)
                        save_config(config)
                        flash("受限频道配置已更新！", "success")
                        return redirect(url_for('manage_config'))
                    else:
                        flash("未找到要更新的频道配置。", "error")
                        return redirect(url_for('manage_config'))
                else:
                    # Add new channel
                    if any(entry['channel_id'] == channel_id_int for entry in config['restricted_channels']):
                        flash(f"受限频道 {channel_id_int} 已存在。", "warning")
                        return redirect(url_for('manage_config'))
                    
                    config['restricted_channels'].append({
                        "channel_id": channel_id_int,
                        "download_directory": download_directory,
                        "download_video": download_video,
                        "keep_video_message": keep_video_message,
                        "convert_hdhive": convert_hdhive,
                        "target_user_ids": target_user_ids_list,
                        "group_name": group_name,
                        "channel_name": channel_name,
                        "blacklist_keywords": blacklist_keywords,
                        "whitelist_keywords": whitelist_keywords,
                        "auto_click_redpacket": auto_click_redpacket,
                        "auto_click_keywords": auto_click_keywords,
                        "auto_click_button_texts": auto_click_button_texts,
                        "auto_click_notify_targets": auto_click_notify_targets,
                        "monitor_types": monitor_types
                    })
                    # Update group_order if new group
                    if group_name not in config.get('group_order', []):
                        config.setdefault('group_order', []).append(group_name)
                    save_config(config)
                    flash("受限频道配置添加成功！", "success")
                    return redirect(url_for('manage_config'))
            except ValueError as e:
                flash(f"频道 ID 必须是整数。错误: {str(e)}", "error")

        elif action == 'delete_restricted':
            channel_id_to_delete = int(request.form['restricted_channel_id'])
            config['restricted_channels'] = [
                entry for entry in config['restricted_channels'] 
                if entry['channel_id'] != channel_id_to_delete
            ]
            save_config(config)
            flash("受限频道配置删除成功！", "success")
        
        elif action == 'update_proxy':
            proxy_addr = request.form.get('proxy_addr')
            proxy_port = request.form.get('proxy_port')
            proxy_username = request.form.get('proxy_username')
            proxy_password = request.form.get('proxy_password')

            config['proxy'] = {
                "addr": proxy_addr,
                "port": proxy_port,
                "username": proxy_username,
                "password": proxy_password
            }
            save_config(config)
            flash("代理配置已更新！", "success")

        elif action == 'update_hdhive_cookie':
            hdhive_cookie = (request.form.get('hdhive_cookie') or '').strip()
            threshold_raw = (request.form.get('hdhive_auto_unlock_points_threshold') or '').strip()
            hdhive_base_url = (request.form.get('hdhive_base_url') or '').strip()
            hdhive_open_api_key = (request.form.get('hdhive_open_api_key') or '').strip()
            hdhive_open_api_direct_unlock = (request.form.get('hdhive_open_api_direct_unlock') or 'off').strip().lower() == 'on'
            try:
                threshold = int(threshold_raw) if threshold_raw != '' else 0
                if threshold < 0:
                    threshold = 0
            except ValueError:
                threshold = 0
                flash("自动解锁阈值必须是整数，已回退为 0。", "warning")

            config['hdhive_auto_unlock_points_threshold'] = threshold
            if hdhive_cookie:
                config['hdhive_cookie'] = hdhive_cookie
                flash("HDHive Cookie 已更新！", "success")
            else:
                # Allow clearing
                if 'hdhive_cookie' in config:
                    config.pop('hdhive_cookie', None)
                flash("HDHive Cookie 已清除。", "info")

            if hdhive_base_url:
                config['hdhive_base_url'] = hdhive_base_url
            if hdhive_open_api_key:
                config['hdhive_open_api_key'] = hdhive_open_api_key
            config['hdhive_open_api_direct_unlock'] = hdhive_open_api_direct_unlock
            save_config(config)

        elif action == 'update_self_service':
            enabled = request.form.get('self_service_enabled') == 'on'
            public_enabled = request.form.get('self_service_public_enabled') == 'on'
            public_access_key = (request.form.get('self_service_public_access_key') or '').strip()
            public_rate_enabled = request.form.get('self_service_public_rate_limit_enabled') == 'on'
            public_rate_window_raw = (request.form.get('self_service_public_rate_limit_window_seconds') or '').strip()
            public_rate_max_raw = (request.form.get('self_service_public_rate_limit_max_requests') or '').strip()
            target_user_ids = (request.form.get('self_service_target_user_ids') or '').strip()
            storage_mode = (request.form.get('self_service_storage_mode') or 'any').strip().lower()
            if storage_mode not in ('any', '115', '123', '115_123'):
                storage_mode = 'any'
            max_results_raw = (request.form.get('self_service_search_max_results') or '').strip()
            cookie_check_mode = (request.form.get('self_service_cookie_check_mode') or 'warn').strip().lower()
            use_open_api = (request.form.get('self_service_use_open_api') or 'off').strip().lower() == 'on'
            try:
                max_results = int(max_results_raw) if max_results_raw != '' else 5
            except Exception:
                max_results = 5
            max_results = max(1, min(max_results, 20))
            if cookie_check_mode not in ('strict', 'warn', 'off'):
                cookie_check_mode = 'warn'
            try:
                public_rate_window = int(public_rate_window_raw) if public_rate_window_raw != '' else 300
            except Exception:
                public_rate_window = 300
            try:
                public_rate_max = int(public_rate_max_raw) if public_rate_max_raw != '' else 3
            except Exception:
                public_rate_max = 3
            public_rate_window = max(10, min(public_rate_window, 24 * 60 * 60))
            public_rate_max = max(1, min(public_rate_max, 100))

            config['self_service_enabled'] = enabled
            config['self_service_public_enabled'] = public_enabled
            config['self_service_public_access_key'] = public_access_key
            config['self_service_public_rate_limit'] = {
                "enabled": public_rate_enabled,
                "window_seconds": public_rate_window,
                "max_requests": public_rate_max,
            }
            config['self_service_target_user_ids'] = target_user_ids
            config['self_service_storage_mode'] = storage_mode
            config['self_service_search_max_results'] = max_results
            config['self_service_cookie_check_mode'] = cookie_check_mode
            config['self_service_use_open_api'] = use_open_api
            save_config(config)
            flash("自助观影申请配置已更新！", "success")

        elif action == 'clear_proxy':
            config['proxy'] = {}
            save_config(config)
            flash("代理配置已清除！", "info")

        elif action == 'reorder_groups':
            group_order_str = request.form.get('group_order', '[]')
            try:
                group_order = json.loads(group_order_str)
                config['group_order'] = group_order
                save_config(config)
                return jsonify({"success": True})
            except json.JSONDecodeError as e:
                return jsonify({"error": f"JSON解析错误: {str(e)}"}), 400

        elif action == 'update_bot':
            bot_token = (request.form.get('bot_token') or '').strip()
            if 'bot' not in config:
                config['bot'] = {}
            config['bot']['token'] = bot_token
            save_config(config)
            if get_bot_monitor_status() == "运行中":
                stop_bot_monitor_process()
                start_bot_monitor_process()
                flash("Bot Token 已更新，Bot 监控已自动重启并应用新配置。", "success")
            else:
                flash("Bot Token 已更新！启动 Bot 监控后将使用新配置。", "success")

        elif action == 'update_filename_blacklist':
            blacklist_str = request.form.get('filename_blacklist_keywords', '')
            blacklist = [k.strip() for k in blacklist_str.split(',') if k.strip()]
            config['filename_blacklist'] = blacklist
            save_config(config)
            flash("文件名清理频率配置已更新！", "success")

        elif action == 'update_global_blacklist':
            global_blacklist_str = request.form.get('global_blacklist_keywords', '')
            global_blacklist = [k.strip() for k in global_blacklist_str.split(',') if k.strip()]
            config['global_blacklist_keywords'] = global_blacklist
            save_config(config)
            flash("全局黑名单关键词已更新！", "success")

        elif action == 'bulk_manage_restricted':
            sub_action = request.form.get('sub_action')
            selected_ids_str = request.form.get('selected_ids', '[]')
            try:
                selected_ids = json.loads(selected_ids_str)
                selected_ids = [int(sid) for sid in selected_ids]
            except:
                flash("选择的频道 ID 无效。", "error")
                return redirect(url_for('manage_config'))

            if sub_action == 'delete':
                config['restricted_channels'] = [
                    entry for entry in config['restricted_channels'] 
                    if entry['channel_id'] not in selected_ids
                ]
                save_config(config)
                flash(f"已批量删除 {len(selected_ids)} 个频道配置。", "success")
            elif sub_action == 'move_group':
                target_group = request.form.get('target_group')
                if not target_group:
                    flash("请选择目标分组。", "warning")
                    return redirect(url_for('manage_config'))
                
                # Handle new group creation
                if target_group == 'NEW_GROUP':
                    flash("请先在单个频道的编辑中手动创建新分组。", "info")
                    return redirect(url_for('manage_config'))

                count = 0
                for entry in config['restricted_channels']:
                    if entry['channel_id'] in selected_ids:
                        entry['group_name'] = target_group
                        count += 1
                
                if count > 0:
                    save_config(config)
                    flash(f"已将 {count} 个频道移动到分组: {target_group}", "success")
                else:
                    flash("未找到匹配的待移动频道。", "warning")

        # After config changes, restart monitor if it was running
        if should_restart_monitor and get_monitor_status() == "运行中":
            stop_monitor_process()
            start_monitor_process()

        return redirect(url_for('manage_config'))

    return render_template('config.html', config=config)


@app.route('/drama_calendar', methods=['GET', 'POST'])
@login_required
def drama_calendar_settings():
    config = load_config()
    if request.method == 'POST':
        action_values = request.form.getlist('action')
        action = (action_values[-1] if action_values else request.form.get('action') or '').strip()

        def _drama_cfg_from_form(base: dict) -> dict:
            drama = dict(base or {})
            drama['source'] = _normalize_drama_sources(request.form.getlist('drama_source'))
            drama['home_url'] = (request.form.get('drama_home_url') or '').strip() or 'https://blog.922928.de/'
            drama['post_url'] = (request.form.get('drama_post_url') or '').strip()
            drama['calendar_whitelist_keywords'] = (request.form.get('drama_calendar_whitelist_keywords') or '').strip()
            drama['calendar_blacklist_keywords'] = (request.form.get('drama_calendar_blacklist_keywords') or '').strip()
            drama['maoyan_url'] = (request.form.get('drama_maoyan_url') or '').strip() or 'https://piaofang.maoyan.com/box-office?ver=normal'
            try:
                drama['maoyan_top_n'] = max(0, int((request.form.get('drama_maoyan_top_n') or '0').strip() or 0))
            except Exception:
                drama['maoyan_top_n'] = 0
            drama['include_maoyan_web_heat'] = request.form.get('drama_include_maoyan_web_heat') == 'on'
            drama['maoyan_web_heat_url'] = (request.form.get('drama_maoyan_web_heat_url') or '').strip() or 'https://piaofang.maoyan.com/web-heat'
            try:
                drama['maoyan_web_heat_top_n'] = max(0, int((request.form.get('drama_maoyan_web_heat_top_n') or '0').strip() or 0))
            except Exception:
                drama['maoyan_web_heat_top_n'] = 0
            drama['maoyan_whitelist_keywords'] = (request.form.get('drama_maoyan_whitelist_keywords') or '').strip()
            drama['maoyan_blacklist_keywords'] = (request.form.get('drama_maoyan_blacklist_keywords') or '').strip()
            drama['douban_url'] = _normalize_douban_collection_url(
                (request.form.get('drama_douban_url') or '').strip(),
                fallback_url='https://m.douban.com/subject_collection/tv_american',
            )
            raw_douban_top_n = request.form.get('drama_douban_top_n')
            if raw_douban_top_n is None:
                drama['douban_top_n'] = int(drama.get('douban_top_n', 0) or 0)
            else:
                try:
                    drama['douban_top_n'] = max(0, int((raw_douban_top_n or '0').strip() or 0))
                except Exception:
                    drama['douban_top_n'] = int(drama.get('douban_top_n', 0) or 0)
            def _parse_optional_top_n(field_name: str, fallback_key: str) -> int:
                raw_value = request.form.get(field_name)
                if raw_value is None:
                    return int(drama.get(fallback_key, drama.get('douban_top_n', 0)) or 0)
                try:
                    return max(0, int((raw_value or '0').strip() or 0))
                except Exception:
                    return int(drama.get(fallback_key, drama.get('douban_top_n', 0)) or 0)
            drama['douban_asia_top_n'] = _parse_optional_top_n('drama_douban_asia_top_n', 'douban_asia_top_n')
            drama['douban_domestic_top_n'] = _parse_optional_top_n('drama_douban_domestic_top_n', 'douban_domestic_top_n')
            drama['douban_variety_top_n'] = _parse_optional_top_n('drama_douban_variety_top_n', 'douban_variety_top_n')
            drama['douban_animation_top_n'] = _parse_optional_top_n('drama_douban_animation_top_n', 'douban_animation_top_n')
            drama['douban_whitelist_keywords'] = (request.form.get('drama_douban_whitelist_keywords') or '').strip()
            drama['douban_blacklist_keywords'] = (request.form.get('drama_douban_blacklist_keywords') or '').strip()
            drama['douban_asia_whitelist_keywords'] = (request.form.get('drama_douban_asia_whitelist_keywords') or '').strip()
            drama['douban_asia_blacklist_keywords'] = (request.form.get('drama_douban_asia_blacklist_keywords') or '').strip()
            drama['douban_domestic_whitelist_keywords'] = (request.form.get('drama_douban_domestic_whitelist_keywords') or '').strip()
            drama['douban_domestic_blacklist_keywords'] = (request.form.get('drama_douban_domestic_blacklist_keywords') or '').strip()
            drama['douban_variety_whitelist_keywords'] = (request.form.get('drama_douban_variety_whitelist_keywords') or '').strip()
            drama['douban_variety_blacklist_keywords'] = (request.form.get('drama_douban_variety_blacklist_keywords') or '').strip()
            drama['douban_animation_whitelist_keywords'] = (request.form.get('drama_douban_animation_whitelist_keywords') or '').strip()
            drama['douban_animation_blacklist_keywords'] = (request.form.get('drama_douban_animation_blacklist_keywords') or '').strip()
            try:
                drama['remove_movie_premiere_after_days'] = int((request.form.get('drama_remove_movie_premiere_after_days') or '365').strip() or 365)
            except Exception:
                drama['remove_movie_premiere_after_days'] = 365
            try:
                drama['remove_finished_after_days'] = int((request.form.get('drama_remove_finished_after_days') or '-1').strip() or -1)
            except Exception:
                drama['remove_finished_after_days'] = -1
            drama['line_keywords'] = (request.form.get('drama_line_keywords') or '').strip() or '上线,开播'
            drama['title_alias_map'] = (request.form.get('drama_title_alias_map') or '').strip()
            drama['env_files'] = (request.form.get('drama_env_files') or '').strip()
            drama['env_key'] = (request.form.get('drama_env_key') or '').strip() or 'DRAMA_CALENDAR_REGEX'
            drama['backup_before_write'] = request.form.get('drama_backup_before_write') == 'on'
            drama['append_to_whitelist'] = request.form.get('drama_append_to_whitelist') == 'on'
            drama['managed_scope_source_only'] = request.form.get('drama_managed_scope_source_only') == 'on'
            finish_mode = (request.form.get('drama_finish_detect_mode') or 'hybrid').strip().lower()
            if finish_mode not in ('keyword', 'tmdb', 'hybrid'):
                finish_mode = 'hybrid'
            drama['finish_detect_mode'] = finish_mode
            drama['tmdb_api_key'] = (request.form.get('drama_tmdb_api_key') or '').strip()
            drama['tmdb_language'] = (request.form.get('drama_tmdb_language') or '').strip() or 'zh-CN'
            drama['tmdb_region'] = (request.form.get('drama_tmdb_region') or '').strip() or 'CN'
            try:
                drama['tmdb_year_tolerance'] = max(0, int((request.form.get('drama_tmdb_year_tolerance') or '2').strip() or 2))
            except Exception:
                drama['tmdb_year_tolerance'] = 2
            try:
                drama['tmdb_min_score'] = max(1, int((request.form.get('drama_tmdb_min_score') or '70').strip() or 70))
            except Exception:
                drama['tmdb_min_score'] = 70
            drama['auto_sync_enabled'] = request.form.get('drama_auto_sync_enabled') == 'on'
            try:
                drama['auto_sync_interval_minutes'] = max(1, int((request.form.get('drama_auto_sync_interval_minutes') or '60').strip() or 60))
            except Exception:
                drama['auto_sync_interval_minutes'] = 60
            cron_expr = (request.form.get('drama_auto_sync_cron_expr') or '').strip()
            if cron_expr and not _cron_expr_valid(cron_expr):
                flash('Cron 表达式无效，已自动改为按分钟间隔执行。', 'warning')
                cron_expr = ''
            drama['auto_sync_cron_expr'] = cron_expr
            return drama

        if action == 'update_drama_calendar_settings':
            drama = _drama_cfg_from_form(config.get('drama_calendar', {}))
            config['drama_calendar'] = drama
            save_config(config)
            _ensure_drama_scheduler_started()
            auto_tip = '已开启' if bool(drama.get('auto_sync_enabled')) else '未开启'
            if drama.get('auto_sync_cron_expr'):
                plan_tip = f"Cron: {drama.get('auto_sync_cron_expr')}"
            else:
                plan_tip = f"间隔 {int(drama.get('auto_sync_interval_minutes', 60) or 60)} 分钟"
            flash(f'追剧日历配置已保存。自动抓取：{auto_tip}（{plan_tip}）', 'success')

        elif action == 'update_drama_env_titles':
            drama_cfg = config.get('drama_calendar', {})
            env_files_list = _parse_env_files(drama_cfg.get('env_files', ''))
            env_key = (drama_cfg.get('env_key') or 'DRAMA_CALENDAR_REGEX').strip() or 'DRAMA_CALENDAR_REGEX'
            if not env_files_list:
                flash('请先在追剧日历配置中填写至少一个 .env 路径。', 'error')
            else:
                allowed = {os.path.abspath(p): p for p in env_files_list}
                group_ids = request.form.getlist('env_group_id')
                if not group_ids:
                    flash('没有可更新的条目。', 'warning')
                else:
                    updates_by_env = {}
                    totals_by_env = {}
                    source_titles_updates = {}
                    for gid in group_ids:
                        tag = (request.form.get(f'group_tag_{gid}') or '').strip() or 'manual'
                        env_path = (request.form.get(f'group_env_{gid}') or '').strip()
                        if not env_path:
                            continue
                        env_abs = os.path.abspath(env_path)
                        if env_abs not in allowed:
                            continue
                        orig_list = request.form.getlist(f'orig_{gid}[]')
                        title_list = request.form.getlist(f'title_{gid}[]')
                        remove_list = set(request.form.getlist(f'remove_{gid}[]'))
                        new_titles = []
                        seen_norm = set()
                        totals = totals_by_env.setdefault(env_abs, {'before': 0, 'after': 0})
                        totals['before'] += len(orig_list)
                        for orig, new in zip(orig_list, title_list):
                            if orig in remove_list:
                                continue
                            cleaned = (new or '').strip()
                            if not cleaned:
                                continue
                            norm = _normalize_title_for_match(cleaned)
                            if not norm or norm in seen_norm:
                                continue
                            seen_norm.add(norm)
                            new_titles.append(cleaned)
                        totals['after'] += len(new_titles)
                        updates_by_env.setdefault(env_abs, []).append(
                            (tag, _build_regex_from_titles(new_titles), _build_regex_from_titles(orig_list))
                        )
                        source_titles_updates.setdefault(env_abs, {})[tag] = new_titles

                    if not updates_by_env:
                        flash('没有可更新的条目。', 'warning')
                    else:
                        managed_scope = 'source' if bool(drama_cfg.get('managed_scope_source_only', True)) else 'key'
                        state = _load_drama_calendar_state()
                        records = state.get('records') if isinstance(state, dict) else []
                        if not isinstance(records, list):
                            records = []
                        source_titles_state = state.get('source_titles') if isinstance(state, dict) else None
                        if not isinstance(source_titles_state, dict):
                            source_titles_state = {}
                        had_change = False

                        for env_abs, updates in updates_by_env.items():
                            if not os.path.exists(env_abs):
                                flash(f'目标 .env 不存在：{env_abs}', 'error')
                                continue
                            try:
                                with open(env_abs, 'r', encoding='utf-8') as f:
                                    original = f.read()
                            except Exception as e:
                                flash(f'读取 .env 失败：{env_abs} ({e})', 'error')
                                continue

                            if managed_scope == 'key':
                                combined_titles = []
                                seen_norm = set()
                                for _, regex_value, _ in updates:
                                    for title in _extract_titles_from_regex_value(regex_value):
                                        norm = _normalize_title_for_match(title)
                                        if not norm or norm in seen_norm:
                                            continue
                                        seen_norm.add(norm)
                                        combined_titles.append(title)
                                current_value = _extract_env_value_by_key(original, env_key)
                                updates = [('key', _build_regex_from_titles(combined_titles), current_value)]

                            updated = original
                            for tag, regex_value, old_regex in updates:
                                target_env_abs = os.path.abspath(env_abs)
                                has_record = False
                                for rec in records:
                                    if not isinstance(rec, dict):
                                        continue
                                    rec_env = os.path.abspath(str(rec.get('env_path') or ''))
                                    rec_key = str(rec.get('key') or '')
                                    rec_source = str(rec.get('source') or '')
                                    if managed_scope == 'key':
                                        if rec_env == target_env_abs and rec_key == env_key:
                                            has_record = True
                                            break
                                    else:
                                        if rec_env == target_env_abs and rec_key == env_key and rec_source == tag:
                                            has_record = True
                                            break
                                if not has_record and old_regex:
                                    records.append(
                                        {
                                            'env_path': target_env_abs,
                                            'key': env_key,
                                            'source': tag,
                                            'value': old_regex,
                                            'updated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        }
                                    )
                                updated, records = _replace_managed_append_value_in_memory(
                                    env_content=updated,
                                    key=env_key,
                                    source_tag=tag,
                                    env_path=env_abs,
                                    new_value=regex_value,
                                    records=records,
                                    managed_scope=managed_scope,
                                )

                            if updated == original:
                                flash(f'未检测到变更：{env_abs}', 'info')
                                continue

                            try:
                                emergency_backup = f"{env_abs}.autosnap_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                                shutil.copy2(env_abs, emergency_backup)
                                if bool(drama_cfg.get('backup_before_write', False)):
                                    _make_env_backup_if_needed(env_abs)
                                with open(env_abs, 'w', encoding='utf-8') as f:
                                    f.write(updated)
                                had_change = True
                                totals = totals_by_env.get(os.path.abspath(env_abs), {'before': 0, 'after': 0})
                                flash(
                                    f'已更新 .env：{env_abs}（{totals.get("before", 0)} → {totals.get("after", 0)}）',
                                    'success',
                                )
                                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                _append_drama_calendar_log([
                                    f'[{ts}] [INFO] [DRAMA][ENV-EDIT] {env_abs} key={env_key} {totals.get("before", 0)}->{totals.get("after", 0)}',
                                ])
                            except Exception as e:
                                flash(f'写入 .env 失败：{env_abs} ({e})', 'error')

                        state['records'] = records
                        for env_abs, per_source in source_titles_updates.items():
                            env_entry = source_titles_state.get(env_abs)
                            if not isinstance(env_entry, dict):
                                env_entry = {}
                            key_entry = env_entry.get(env_key)
                            if not isinstance(key_entry, dict):
                                key_entry = {}
                            for tag, titles in per_source.items():
                                key_entry[tag] = list(titles or [])
                            env_entry[env_key] = key_entry
                            source_titles_state[env_abs] = env_entry
                        state['source_titles'] = source_titles_state
                        _save_drama_calendar_state(state)
                        if not had_change:
                            flash('未检测到需要写入的变更。', 'info')

        elif action == 'run_drama_calendar':
            drama_cfg = config.get('drama_calendar', {})
            if request.form.get('drama_source') is not None:
                drama_cfg = _drama_cfg_from_form(drama_cfg)
                config['drama_calendar'] = drama_cfg
                save_config(config)
            run_mode = (request.form.get('run_mode') or 'preview').strip().lower()
            dry_run = run_mode != 'apply'
            ok, out, err = _run_drama_calendar_update(drama_cfg, dry_run=dry_run, trigger='manual')
            output_lines = [ln for ln in (out or '').splitlines() if ln.strip()]
            err_lines = [ln for ln in (err or '').splitlines() if ln.strip()]
            summary_items = _build_drama_summary_items(output_lines, err_lines, ok=ok, dry_run=dry_run)

            if ok:
                label = '预览成功' if dry_run else '写入成功'
                if dry_run:
                    summary = _summarize_drama_output_lines(output_lines, prefer_write=False, max_parts=6) or '无输出'
                else:
                    summary = _summarize_drama_output_lines(output_lines, prefer_write=True, max_parts=6) or '无输出'
                flash(f'{label}：{summary}', 'success')
                _set_drama_scheduler_state(
                    last_run_at=_format_scheduler_ts(time.time()),
                    last_status='success',
                    last_message=summary,
                    last_summary=summary_items,
                )
            elif (err or '').strip() == DRAMA_RUN_BUSY_MESSAGE:
                flash(DRAMA_RUN_BUSY_MESSAGE, 'warning')
                _set_drama_scheduler_state(
                    last_run_at=_format_scheduler_ts(time.time()),
                    last_status='warning',
                    last_message=DRAMA_RUN_BUSY_MESSAGE,
                    last_summary={'extract': '', 'removed': '', 'result': '已有任务运行'},
                )
            else:
                summary_out = ' | '.join(output_lines[:4]) if output_lines else ''
                summary_err = ' | '.join(err_lines[:4]) if err_lines else '未知错误'
                flash(f'追剧日历执行失败：{summary_out} {summary_err}'.strip(), 'error')
                _set_drama_scheduler_state(
                    last_run_at=_format_scheduler_ts(time.time()),
                    last_status='error',
                    last_message=(summary_out + ' ' + summary_err).strip(),
                    last_summary=summary_items,
                )

        elif action == 'clear_drama_calendar_env':
            drama_cfg = config.get('drama_calendar', {})
            if request.form.get('drama_source') is not None:
                drama_cfg = _drama_cfg_from_form(drama_cfg)
                config['drama_calendar'] = drama_cfg
                save_config(config)
            ok, success_paths, errors = _clear_drama_calendar_env_values(drama_cfg)
            env_key = (drama_cfg.get('env_key') or 'DRAMA_CALENDAR_REGEX').strip() or 'DRAMA_CALENDAR_REGEX'
            if ok:
                flash(f'已清空 {len(success_paths)} 个 .env 中的变量内容：{env_key}', 'success')
            elif success_paths:
                flash(f'已清空 {len(success_paths)} 个 .env，但仍有 {len(errors)} 个失败：{env_key}', 'warning')
            else:
                flash(f'清空失败：{" | ".join(errors[:3])}', 'error')

        return redirect(url_for('drama_calendar_settings'))

    env_edit_sources, env_edit_errors = _build_env_edit_source_view(config.get('drama_calendar', {}))
    return render_template(
        'drama_calendar.html',
        config=config,
        scheduler_state=get_drama_scheduler_state(),
        env_edit_sources=env_edit_sources,
        env_edit_errors=env_edit_errors,
    )

@app.route('/monitor_action', methods=['POST'])
@login_required
def monitor_action():
    action = request.form['action']
    if action == 'start':
        start_monitor_process()
    elif action == 'stop':
        stop_monitor_process()
    
    return redirect(url_for('index'))

@app.route('/monitor_log')
@login_required
def monitor_log():
    cfg = load_config()
    interval, auto_refresh, max_lines, max_bytes = _resolve_log_view_config(cfg)
    log_output = tg_monitor_mgr.get_colored_log_output(
        colorize_log_line,
        empty_text="暂无日志。",
        max_lines=max_lines,
        max_bytes=max_bytes,
    )
    return render_template(
        'log.html',
        log_output=log_output,
        refresh_interval=interval,
        auto_refresh=auto_refresh,
    )

@app.route('/monitor_log_data')
@login_required
def monitor_log_data():
    cfg = load_config()
    _, _, max_lines, max_bytes = _resolve_log_view_config(cfg)
    last_key = (request.args.get("last_key") or "").strip()
    current_key = tg_monitor_mgr.get_log_cache_key(max_lines=max_lines, max_bytes=max_bytes)
    if last_key and last_key == current_key:
        return jsonify({"changed": False, "key": current_key})
    log_output = tg_monitor_mgr.get_colored_log_output(
        colorize_log_line,
        empty_text="暂无日志。",
        max_lines=max_lines,
        max_bytes=max_bytes,
    )
    return jsonify({"changed": True, "key": current_key, "log_output": log_output})


@app.route('/drama_calendar_log')
@login_required
def drama_calendar_log():
    log_lines = _read_drama_calendar_log_lines()
    colored_log_lines = [colorize_log_line(line) for line in log_lines]
    log_output = "\n".join(reversed(colored_log_lines)) if colored_log_lines else "暂无追剧日志。"
    return render_template('drama_calendar_log.html', log_output=log_output)

@app.route('/file_config', methods=['GET', 'POST'])
@login_required
def manage_file_config():
    config = load_config()
    download_risk_stats = load_download_risk_stats()
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add':
            source_dir = os.path.abspath(request.form['source_dir'])
            destination_dir = os.path.abspath(request.form['destination_dir'])
            action_type = request.form['action_type']
            stable_time = request.form.get('stable_time', 10)
            dir_stable_time = request.form.get('dir_stable_time', 30)
            enable_second_transfer = request.form.get('enable_second_transfer') == 'on'
            enable_mid_copy_check = request.form.get('enable_mid_copy_check') == 'on'
            mid_copy_check_interval = request.form.get('mid_copy_check_interval', 30)
            mid_copy_chunk_size_mb = request.form.get('mid_copy_chunk_size_mb', 8)
            target_cid = (request.form.get('target_cid') or '').strip()
            
            if source_dir == destination_dir:
                flash("源目录和目标目录不能相同。", "error")
                return redirect(url_for('manage_file_config'))

            try:
                stable_time = int(stable_time)
                dir_stable_time = int(dir_stable_time)
                mid_copy_check_interval = int(mid_copy_check_interval)
                mid_copy_chunk_size_mb = int(mid_copy_chunk_size_mb)
                if dir_stable_time < 1:
                    raise ValueError("dir_stable_time must be >= 1")
                if mid_copy_check_interval < 1:
                    raise ValueError("mid_copy_check_interval must be >= 1")
                if mid_copy_chunk_size_mb < 1:
                    raise ValueError("mid_copy_chunk_size_mb must be >= 1")
                delete_source_after_transfer = request.form.get('delete_source_after_transfer') == 'on'
                old_source_dir = request.form.get('old_source_dir') or ''
                old_source_dir = os.path.abspath(old_source_dir) if old_source_dir else ''

                if old_source_dir:
                    if source_dir != old_source_dir and any(
                        task['source_dir'] == source_dir for task in config['file_monitoring_tasks']
                    ):
                        flash(f"源目录 '{source_dir}' 已存在于监控任务中。", "warning")
                    else:
                        updated = False
                        for task in config['file_monitoring_tasks']:
                            if task['source_dir'] == old_source_dir:
                                task.update({
                                    "source_dir": source_dir,
                                    "destination_dir": destination_dir,
                                    "action": action_type,
                                    "stable_time": stable_time,
                                    "dir_stable_time": dir_stable_time,
                                    "handle_duplicate": request.form['handle_duplicate'],
                                    "enable_second_transfer": enable_second_transfer,
                                    "enable_mid_copy_check": enable_mid_copy_check,
                                    "mid_copy_check_interval": mid_copy_check_interval,
                                    "mid_copy_chunk_size": mid_copy_chunk_size_mb * 1024 * 1024,
                                    "target_cid": target_cid,
                                    "delete_source_after_transfer": delete_source_after_transfer
                                })
                                updated = True
                                break
                        if updated:
                            save_config(config)
                            flash("文件监控任务更新成功！", "success")
                        else:
                            flash("未找到要更新的任务，已改为新增。", "warning")
                            config['file_monitoring_tasks'].append({
                                "source_dir": source_dir,
                                "destination_dir": destination_dir,
                                "action": action_type,
                                "stable_time": stable_time,
                                "dir_stable_time": dir_stable_time,
                                "handle_duplicate": request.form['handle_duplicate'],
                                "enable_second_transfer": enable_second_transfer,
                                "enable_mid_copy_check": enable_mid_copy_check,
                                "mid_copy_check_interval": mid_copy_check_interval,
                                "mid_copy_chunk_size": mid_copy_chunk_size_mb * 1024 * 1024,
                                "target_cid": target_cid,
                                "delete_source_after_transfer": delete_source_after_transfer
                            })
                            save_config(config)
                            flash("文件监控任务添加成功！", "success")
                else:
                    # Check if source_dir already exists in tasks
                    if any(task['source_dir'] == source_dir for task in config['file_monitoring_tasks']):
                        flash(f"源目录 '{source_dir}' 已存在于监控任务中。", "warning")
                    else:
                        config['file_monitoring_tasks'].append({
                            "source_dir": source_dir,
                            "destination_dir": destination_dir,
                            "action": action_type,
                            "stable_time": stable_time,
                            "dir_stable_time": dir_stable_time,
                            "handle_duplicate": request.form['handle_duplicate'],
                            "enable_second_transfer": enable_second_transfer,
                            "enable_mid_copy_check": enable_mid_copy_check,
                            "mid_copy_check_interval": mid_copy_check_interval,
                            "mid_copy_chunk_size": mid_copy_chunk_size_mb * 1024 * 1024,
                            "target_cid": target_cid,
                            "delete_source_after_transfer": delete_source_after_transfer
                        })
                        save_config(config)
                        flash("文件监控任务添加成功！", "success")
            except ValueError:
                    flash("稳定时间、目录稳定时间、检测间隔和块大小必须是整数，且大于等于 1。", "error")
            
        elif action == 'delete':
            source_dir_to_delete = request.form['source_dir']
            config['file_monitoring_tasks'] = [
                task for task in config['file_monitoring_tasks'] 
                if task['source_dir'] != source_dir_to_delete
            ]
            save_config(config)
            flash("文件监控任务删除成功！", "success")
        
        elif action == 'update_global':
            allowed_browse_path = request.form.get('allowed_browse_path')
            debug_mode = request.form.get('debug_mode') == 'on'
            trace_media_detection = request.form.get('trace_media_detection') == 'on'
            config['debug_mode'] = debug_mode
            config['trace_media_detection'] = trace_media_detection
            if os.path.isdir(allowed_browse_path):
                config['allowed_browse_path'] = os.path.abspath(allowed_browse_path)
                save_config(config)
                flash(
                    f"全局设置已更新！DEBUG 模式: {'[开启]' if debug_mode else '[关闭]'}，"
                    f"媒体追踪日志: {'[开启]' if trace_media_detection else '[关闭]'}",
                    "success"
                )
            else:
                flash("无效的目录路径。", "error")

        elif action == 'update_115':
            cookie_115 = (request.form.get('cookie_115') or '').strip()
            config['115_cookie'] = cookie_115
            config['web_115_cookie'] = cookie_115
            save_config(config)
            flash("115 Cookie 已更新！", "success")

        elif action == 'update_download_risk_control':
            try:
                enabled = request.form.get('risk_enabled') == 'on'
                per_channel_max = int(request.form.get('per_channel_max_downloads_per_minute', 6))
                duplicate_cooldown_seconds = int(request.form.get('duplicate_cooldown_seconds', 300))
                max_single_file_size_mb = int(request.form.get('max_single_file_size_mb', 4096))
                min_free_space_gb = float(request.form.get('min_free_space_gb', 5))

                if per_channel_max < 0 or duplicate_cooldown_seconds < 0 or max_single_file_size_mb < 0 or min_free_space_gb < 0:
                    raise ValueError

                config['download_risk_control'] = {
                    'enabled': enabled,
                    'per_channel_max_downloads_per_minute': per_channel_max,
                    'duplicate_cooldown_seconds': duplicate_cooldown_seconds,
                    'max_single_file_size_mb': max_single_file_size_mb,
                    'min_free_space_gb': min_free_space_gb,
                }
                save_config(config)
                flash("下载风控设置已更新！", "success")
            except ValueError:
                flash("下载风控参数必须是大于等于 0 的数字。", "error")

        # After config changes, restart monitor if it was running
        if get_file_monitor_status() == "运行中":
            if _has_file_monitor_tasks(config):
                stop_file_monitor_process()
                start_file_monitor_process()
            else:
                stop_file_monitor_process()

        return redirect(url_for('manage_file_config'))

    return render_template('file_config.html', config=config, download_risk_stats=download_risk_stats)

@app.route('/file_monitor_action', methods=['POST'])
@login_required
def file_monitor_action():
    action = request.form['action']
    if action == 'start':
        if not _has_file_monitor_tasks(load_config()):
            flash("未配置文件监控任务，无法启动。", "warning")
        else:
            start_file_monitor_process()
    elif action == 'stop':
        stop_file_monitor_process()
    
    return redirect(url_for('index'))

@app.route('/file_monitor_log')
@login_required
def file_monitor_log():
    cfg = load_config()
    interval, auto_refresh, max_lines, max_bytes = _resolve_log_view_config(cfg)
    log_output = file_monitor_mgr.get_colored_log_output(
        colorize_log_line,
        empty_text="暂无日志。",
        max_lines=max_lines,
        max_bytes=max_bytes,
    )
    return render_template(
        'file_monitor_log.html',
        log_output=log_output,
        refresh_interval=interval,
        auto_refresh=auto_refresh,
    )

@app.route('/file_monitor_log_data')
@login_required
def file_monitor_log_data():
    cfg = load_config()
    _, _, max_lines, max_bytes = _resolve_log_view_config(cfg)
    last_key = (request.args.get("last_key") or "").strip()
    current_key = file_monitor_mgr.get_log_cache_key(max_lines=max_lines, max_bytes=max_bytes)
    if last_key and last_key == current_key:
        return jsonify({"changed": False, "key": current_key})
    log_output = file_monitor_mgr.get_colored_log_output(
        colorize_log_line,
        empty_text="暂无日志。",
        max_lines=max_lines,
        max_bytes=max_bytes,
    )
    return jsonify({"changed": True, "key": current_key, "log_output": log_output})

@app.route('/clear_monitor_log', methods=['POST'])
@login_required
def clear_monitor_log():
    tg_monitor_mgr.clear_logs()
    flash("Telegram 监控日志已清除。", "info")
    return redirect(url_for('monitor_log'))


@app.route('/clear_drama_calendar_log', methods=['POST'])
@login_required
def clear_drama_calendar_log():
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(DRAMA_CALENDAR_LOG_FILE, 'w', encoding='utf-8') as f:
            f.write('')
        flash("追剧日志已清除。", "info")
    except Exception:
        flash("追剧日志清除失败。", "error")
    return redirect(url_for('drama_calendar_log'))

@app.route('/clear_file_monitor_log', methods=['POST'])
@login_required
def clear_file_monitor_log():
    file_monitor_mgr.clear_logs()
    flash("文件监控日志已清除。", "info")
    return redirect(url_for('file_monitor_log'))


@app.route('/clear_download_risk_stats', methods=['POST'])
@login_required
def clear_download_risk_stats_route():
    if clear_download_risk_stats():
        flash("下载风控统计已清零。", "success")
    else:
        flash("下载风控统计清零失败。", "error")
    return redirect(url_for('index'))

@app.route('/download_monitor_log')
@login_required
def download_monitor_log():
    if not os.path.exists(tg_monitor_mgr.log_file_path):
        flash("暂无 Telegram 监控日志可下载。", "warning")
        return redirect(url_for('monitor_log'))
    return send_file(
        tg_monitor_mgr.log_file_path,
        as_attachment=True,
        download_name=f"telegram_monitor_{time.strftime('%Y%m%d_%H%M%S')}.log",
        mimetype='text/plain'
    )


@app.route('/download_drama_calendar_log')
@login_required
def download_drama_calendar_log():
    if not os.path.exists(DRAMA_CALENDAR_LOG_FILE):
        flash("暂无追剧日志可下载。", "warning")
        return redirect(url_for('drama_calendar_log'))
    return send_file(
        DRAMA_CALENDAR_LOG_FILE,
        as_attachment=True,
        download_name=f"drama_calendar_{time.strftime('%Y%m%d_%H%M%S')}.log",
        mimetype='text/plain'
    )

@app.route('/download_file_monitor_log')
@login_required
def download_file_monitor_log():
    if not os.path.exists(file_monitor_mgr.log_file_path):
        flash("暂无文件监控日志可下载。", "warning")
        return redirect(url_for('file_monitor_log'))
    return send_file(
        file_monitor_mgr.log_file_path,
        as_attachment=True,
        download_name=f"file_monitor_{time.strftime('%Y%m%d_%H%M%S')}.log",
        mimetype='text/plain'
    )

@app.route('/bot_monitor_action', methods=['POST'])
@login_required
def bot_monitor_action():
    action = request.form['action']
    if action == 'start':
        start_bot_monitor_process()
    elif action == 'stop':
        stop_bot_monitor_process()
    
    return redirect(url_for('index'))

@app.route('/api/get_channel_info', methods=['POST'])
@login_required
async def api_get_channel_info():
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"error": "无效的 JSON 请求"}), 400
            
        channel_id = data.get('channel_id')
        if not channel_id:
            return jsonify({"error": "缺少频道 ID"}), 400
        
        config = load_config()
        # 兼容环境变量和配置文件
        api_id = os.environ.get('TELEGRAM_API_ID') or config['telegram'].get('api_id')
        api_hash = os.environ.get('TELEGRAM_API_HASH') or config['telegram'].get('api_hash')
        
        if not api_id or not api_hash:
            return jsonify({"error": "未配置 API ID 或 API Hash"}), 400
            
        session_name = config['telegram'].get('session_name', TELEGRAM_SESSION_NAME)
        # 确保路径正确，如果是相对路径，基于项目的 config 目录
        if not os.path.isabs(CONFIG_DIR):
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            actual_config_dir = os.path.join(project_root, 'config')
        else:
            actual_config_dir = CONFIG_DIR
            
        session_file_path = os.path.join(actual_config_dir, session_name)
        
        # 检查会话文件是否存在
        if not os.path.exists(session_file_path + '.session'):
            return jsonify({"error": "Telegram 会话文件不存在，请先登录认证"}), 404

        client = TelegramClient(session_file_path, int(api_id), api_hash)
        try:
            # 尝试连接，设置等待时间以防卡死
            await asyncio.wait_for(client.connect(), timeout=10)
            
            if not await client.is_user_authorized():
                await client.disconnect()
                return jsonify({"error": "Telegram 未认证，请重新登录"}), 401
                
            try:
                cid = int(channel_id)
                entity = await client.get_entity(cid)
                title = getattr(entity, 'title', None) or getattr(entity, 'username', None) or str(cid)
                await client.disconnect()
                return jsonify({"success": True, "title": title})
            except Exception as e:
                await client.disconnect()
                return jsonify({"error": f"获取频道信息失败: {str(e)}"}), 500
        except asyncio.TimeoutError:
            return jsonify({"error": "连接 Telegram 超时，可能是网络问题或数据库被占用 (请尝试停止监控进程再试)"}), 504
        except Exception as e:
            error_msg = str(e)
            if "database is locked" in error_msg.lower():
                return jsonify({"error": "数据库已锁定。请先停止监控主程序，然后再获取名称 (同一个会话不能同时被两个进程打开)"}), 503
            return jsonify({"error": f"Telegram 连接错误: {error_msg}"}), 500
    except Exception as e:
        # 最后的保底捕获，确保返回的是 JSON 而不是 HTML 错误页
        return jsonify({"error": f"服务器内部错误: {str(e)}"}), 500

@app.route('/downloader')
@login_required
def downloader_page():
    config = load_config()
    # Use project root/downloads as default if not set
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_path = config.get('downloader', {}).get('default_path')
    quality_mode = config.get('downloader', {}).get('quality_mode', 'balanced_hd')
    if quality_mode not in ('super_fast_720p', 'fast_compatible', 'balanced_hd', 'ultra_quality'):
        quality_mode = 'balanced_hd'
    if not default_path:
        default_path = os.path.join(project_root, 'downloads')
    return render_template('downloader.html', default_path=default_path, quality_mode=quality_mode)

@app.route('/downloader/log')
@login_required
def downloader_log():
    return jsonify({"logs": downloader.download_logs})

@app.route('/downloader/clear_log', methods=['POST'])
@login_required
def downloader_clear_log():
    downloader.clear_logs()
    return jsonify({"success": True})


@app.route('/self_service', methods=['GET', 'POST'])
@login_required
def self_service_request():
    config = load_config()
    enabled = bool(config.get("self_service_enabled", False))
    targets = _parse_target_user_ids(config.get("self_service_target_user_ids", ""))
    max_results = int(config.get("self_service_search_max_results", 5) or 5)
    max_results = max(1, min(max_results, 20))
    hdhive_cookie = (config.get("hdhive_cookie") or "").strip()
    hdhive_api_key = (config.get("hdhive_open_api_key") or "").strip()
    use_open_api = bool(config.get("self_service_use_open_api", False))
    allow_open_api_direct = bool(config.get("hdhive_open_api_direct_unlock", False))
    storage_mode = str(config.get("self_service_storage_mode", "any") or "any").lower()

    if request.method == 'POST':
        hdhive_url = (request.form.get('hdhive_url') or '').strip()
        effective_use_open_api = use_open_api
        if not enabled:
            flash("自助观影申请功能未启用，请先在配置页开启。", "warning")
            return redirect(url_for('self_service_request'))
        if use_open_api and hdhive_url and not allow_open_api_direct:
            if not hdhive_cookie:
                flash("Open API 直链解锁已关闭，且未配置 Cookie，无法处理直链。", "error")
                return redirect(url_for('self_service_request'))
            effective_use_open_api = False
            flash("Open API 直链解锁已关闭，本次已改用 Cookie 解析。", "info")
        if effective_use_open_api:
            if not hdhive_api_key:
                flash("未配置 HDHive Open API Key，无法使用 API 搜索。", "error")
                return redirect(url_for('self_service_request'))
        else:
            if not hdhive_cookie:
                flash("未配置 HDHive Cookie，无法自动解锁/解析。", "error")
                return redirect(url_for('self_service_request'))
        if not targets:
            flash("未配置接收用户，请先在配置页填写目标用户 ID。", "error")
            return redirect(url_for('self_service_request'))

        if not effective_use_open_api:
            cookie_check_mode = str(config.get("self_service_cookie_check_mode", "warn") or "warn").lower()
            if cookie_check_mode not in ("strict", "warn", "off"):
                cookie_check_mode = "warn"
            if cookie_check_mode != "off":
                cookie_check = _test_hdhive_cookie((config.get("hdhive_base_url") or "https://hdhive.com"), hdhive_cookie)
                if not cookie_check.get("success"):
                    msg = cookie_check.get("message") or "Cookie 无效或登录失效"
                    if cookie_check_mode == "strict":
                        flash(f"HDHive Cookie 无效：{msg}，请先更新 Cookie。", "error")
                        return redirect(url_for('self_service_request'))
                    flash(f"HDHive Cookie 可能无效：{msg}，仍尝试提交。", "warning")
        title = (request.form.get('title') or '').strip()
        if not title:
            if not hdhive_url:
                flash("请填写影片或电视剧名称。", "warning")
                return redirect(url_for('self_service_request'))

        request_type = (request.form.get('type') or '').strip()
        tmdb_id = (request.form.get('tmdb_id') or '').strip()
        request_year = (request.form.get('year') or '').strip()
        request_note = (request.form.get('note') or '').strip()

        query_parts = [title]
        if request_year:
            query_parts.append(request_year)
        if request_type:
            query_parts.append(request_type)
        query = " ".join([p for p in query_parts if p])

        tmdb_key = ""
        try:
            tmdb_key = (config.get("drama_calendar", {}).get("tmdb_api_key") or "").strip()
        except Exception:
            tmdb_key = ""

        payload = {
            "query": query,
            "title": title,
            "targets": targets,
            "max_results": max_results,
            "hdhive_cookie": hdhive_cookie,
            "base_url": (config.get("hdhive_base_url") or "https://hdhive.com"),
            "type": request_type,
            "year": request_year,
            "note": request_note,
            "hdhive_url": hdhive_url,
            "tmdb_api_key": tmdb_key,
            "tmdb_id": tmdb_id,
            "use_open_api": effective_use_open_api,
            "hdhive_open_api_key": hdhive_api_key,
            "open_api_direct_unlock": allow_open_api_direct,
            "unlock_threshold": int(config.get("hdhive_auto_unlock_points_threshold", 0) or 0),
            "storage_mode": storage_mode,
        }
        threading.Thread(target=_run_self_service_request, args=(payload,), daemon=True).start()
        flash("已提交申请，后台处理中。解析结果将发送到指定用户。", "success")
        return redirect(url_for('self_service_request'))

    return render_template(
        'self_service_request.html',
        enabled=enabled,
        target_display=", ".join([str(t) for t in targets]) if targets else "未配置",
        max_results=max_results,
        has_cookie=bool(hdhive_cookie),
        has_open_api_key=bool(hdhive_api_key),
        use_open_api=use_open_api,
    )

@app.route('/self_service_public', methods=['GET', 'POST'])
def self_service_public():
    config = load_config()
    enabled = bool(config.get("self_service_enabled", False))
    public_enabled = bool(config.get("self_service_public_enabled", False))
    public_access_key = (config.get("self_service_public_access_key") or "").strip()
    targets = _parse_target_user_ids(config.get("self_service_target_user_ids", ""))
    max_results = int(config.get("self_service_search_max_results", 5) or 5)
    max_results = max(1, min(max_results, 20))
    hdhive_cookie = (config.get("hdhive_cookie") or "").strip()
    hdhive_api_key = (config.get("hdhive_open_api_key") or "").strip()
    use_open_api = bool(config.get("self_service_use_open_api", False))
    allow_open_api_direct = bool(config.get("hdhive_open_api_direct_unlock", False))
    storage_mode = str(config.get("self_service_storage_mode", "any") or "any").lower()

    if request.method == 'POST':
        if not enabled:
            flash("自助观影申请功能未启用，请联系管理员。", "warning")
            return redirect(url_for('self_service_public'))
        if not public_enabled:
            flash("公共提交入口未启用，请联系管理员。", "warning")
            return redirect(url_for('self_service_public'))
        if public_access_key:
            provided_key = (request.form.get('access_key') or '').strip()
            if provided_key != public_access_key:
                flash("访问口令错误。", "error")
                return redirect(url_for('self_service_public'))
        rate_cfg = config.get("self_service_public_rate_limit") if isinstance(config.get("self_service_public_rate_limit"), dict) else {}
        allowed, retry_after = _check_public_rate_limit(request, rate_cfg)
        if not allowed:
            flash(f"请求过于频繁，请在 {retry_after} 秒后再试。", "warning")
            return redirect(url_for('self_service_public'))

        hdhive_url = (request.form.get('hdhive_url') or '').strip()
        effective_use_open_api = use_open_api
        if use_open_api and hdhive_url and not allow_open_api_direct:
            if not hdhive_cookie:
                flash("Open API 直链解锁已关闭，且未配置 Cookie，无法处理直链。", "error")
                return redirect(url_for('self_service_public'))
            effective_use_open_api = False
            flash("Open API 直链解锁已关闭，本次已改用 Cookie 解析。", "info")

        if effective_use_open_api:
            if not hdhive_api_key:
                flash("未配置 HDHive Open API Key，无法使用 API 搜索。", "error")
                return redirect(url_for('self_service_public'))
        else:
            if not hdhive_cookie:
                flash("未配置 HDHive Cookie，无法自动解锁/解析。", "error")
                return redirect(url_for('self_service_public'))

        if not targets:
            flash("未配置接收用户，请联系管理员。", "error")
            return redirect(url_for('self_service_public'))

        if not effective_use_open_api:
            cookie_check_mode = str(config.get("self_service_cookie_check_mode", "warn") or "warn").lower()
            if cookie_check_mode not in ("strict", "warn", "off"):
                cookie_check_mode = "warn"
            if cookie_check_mode != "off":
                cookie_check = _test_hdhive_cookie((config.get("hdhive_base_url") or "https://hdhive.com"), hdhive_cookie)
                if not cookie_check.get("success"):
                    msg = cookie_check.get("message") or "Cookie 无效或登录失效"
                    if cookie_check_mode == "strict":
                        flash(f"HDHive Cookie 无效：{msg}，请联系管理员。", "error")
                        return redirect(url_for('self_service_public'))
                    flash(f"HDHive Cookie 可能无效：{msg}，仍尝试提交。", "warning")

        title = (request.form.get('title') or '').strip()
        if not title:
            if not hdhive_url:
                flash("请填写影片或电视剧名称。", "warning")
                return redirect(url_for('self_service_public'))

        request_type = (request.form.get('type') or '').strip()
        tmdb_id = (request.form.get('tmdb_id') or '').strip()
        request_year = (request.form.get('year') or '').strip()
        request_note = (request.form.get('note') or '').strip()

        query_parts = [title]
        if request_year:
            query_parts.append(request_year)
        if request_type:
            query_parts.append(request_type)
        query = " ".join([p for p in query_parts if p])

        tmdb_key = ""
        try:
            tmdb_key = (config.get("drama_calendar", {}).get("tmdb_api_key") or "").strip()
        except Exception:
            tmdb_key = ""

        payload = {
            "query": query,
            "title": title,
            "targets": targets,
            "max_results": max_results,
            "hdhive_cookie": hdhive_cookie,
            "base_url": (config.get("hdhive_base_url") or "https://hdhive.com"),
            "type": request_type,
            "year": request_year,
            "note": request_note,
            "hdhive_url": hdhive_url,
            "tmdb_api_key": tmdb_key,
            "tmdb_id": tmdb_id,
            "use_open_api": effective_use_open_api,
            "hdhive_open_api_key": hdhive_api_key,
            "open_api_direct_unlock": allow_open_api_direct,
            "unlock_threshold": int(config.get("hdhive_auto_unlock_points_threshold", 0) or 0),
            "storage_mode": storage_mode,
        }
        threading.Thread(target=_run_self_service_request, args=(payload,), daemon=True).start()
        flash("已提交申请，后台处理中。解析结果将发送给管理员。", "success")
        return redirect(url_for('self_service_public'))

    return render_template(
        'self_service_public.html',
        enabled=enabled,
        public_enabled=public_enabled,
        require_access_key=bool(public_access_key),
        max_results=max_results,
        has_cookie=bool(hdhive_cookie),
        has_open_api_key=bool(hdhive_api_key),
        use_open_api=use_open_api,
        target_display="管理员" if targets else "未配置",
    )

@app.route('/api/download', methods=['POST'])
@login_required
async def api_download():
    url = request.form.get('url')
    output_dir = request.form.get('output_dir')
    browser = request.form.get('browser')
    quality_mode = (request.form.get('quality_mode') or 'balanced_hd').strip()
    if quality_mode not in ('super_fast_720p', 'fast_compatible', 'balanced_hd', 'ultra_quality'):
        quality_mode = 'balanced_hd'
    cookie_file = request.files.get('cookie_file')
    
    if not url or not output_dir:
        return jsonify({"error": "Missing parameters"}), 400

    output_dir = os.path.abspath(os.path.expanduser(output_dir.strip()))
    if not output_dir:
        return jsonify({"error": "保存目录不能为空"}), 400

    try:
        os.makedirs(output_dir, exist_ok=True)
        write_test = os.path.join(output_dir, '.write_test')
        with open(write_test, 'w', encoding='utf-8') as f:
            f.write('ok')
        os.remove(write_test)
    except Exception as e:
        return jsonify({"error": f"保存目录不可写: {e}"}), 400
        
    # Save cookie file if provided
    cookie_path = None
    if cookie_file:
        os.makedirs('temp_cookies', exist_ok=True)
        cookie_path = os.path.join('temp_cookies', f"cookie_{uuid.uuid4()}.txt")
        cookie_file.save(cookie_path)
    
    config = load_config()
    downloader_cfg = config.setdefault('downloader', {})
    if downloader_cfg.get('default_path') != output_dir:
        downloader_cfg['default_path'] = output_dir
    if downloader_cfg.get('quality_mode') != quality_mode:
        downloader_cfg['quality_mode'] = quality_mode
    save_config(config)

    p_cfg = config.get("proxy", {})
    proxy_url = None
    if p_cfg.get("addr") and p_cfg.get("port"):
        proxy_url = f"http://{p_cfg.get('username') + ':' + p_cfg.get('password') + '@' if p_cfg.get('username') else ''}{p_cfg.get('addr')}:{p_cfg.get('port')}"

    # Run download in background
    asyncio.create_task(downloader.download_task(url, output_dir, cookie_path, browser, proxy_url))
    
    return jsonify({"success": True})


@app.route('/api/downloader/default_path', methods=['POST'])
@login_required
def api_set_downloader_default_path():
    data = request.get_json(silent=True) or {}
    output_dir = str(data.get('output_dir', '')).strip()
    quality_mode = str(data.get('quality_mode', 'balanced_hd')).strip()
    if quality_mode not in ('fast_compatible', 'balanced_hd', 'ultra_quality'):
        quality_mode = 'balanced_hd'

    normalized_output_dir = None
    if output_dir:
        normalized_output_dir = os.path.abspath(os.path.expanduser(output_dir))
        try:
            os.makedirs(normalized_output_dir, exist_ok=True)
            write_test = os.path.join(normalized_output_dir, '.write_test')
            with open(write_test, 'w', encoding='utf-8') as f:
                f.write('ok')
            os.remove(write_test)
        except Exception as e:
            return jsonify({"error": f"保存目录不可写: {e}"}), 400

    config = load_config()
    downloader_cfg = config.setdefault('downloader', {})
    if normalized_output_dir:
        downloader_cfg['default_path'] = normalized_output_dir
    downloader_cfg['quality_mode'] = quality_mode
    save_config(config)

    return jsonify({
        "success": True,
        "default_path": downloader_cfg.get('default_path', ''),
        "quality_mode": quality_mode,
    })

# HDHive 资源请求功能已移除

@app.route('/submit_resource_request', methods=['POST'])
@login_required
def submit_resource_request():
    flash("HDHive 资源请求功能已移除", "error")
    return redirect(url_for('index'))

# 所有相关 API 端点已移除

# --- API Routes ---
@app.route('/api/browse_dir', methods=['POST'])
@login_required
def api_browse_dir():
    config = load_config()
    allowed_path = os.path.abspath(config.get("allowed_browse_path", os.getcwd()))

    data = request.get_json()
    current_path = data.get('path')
    if not current_path:
        current_path = allowed_path

    # Normalize path. This will resolve '..' and ensure an absolute path.
    abs_path = os.path.abspath(current_path)

    # Security check: Ensure the requested path is within the allowed base path
    if not abs_path.startswith(allowed_path):
        return jsonify({"error": "Access denied: Path is outside the allowed browsing area."} ), 403

    if not os.path.isdir(abs_path):
        return jsonify({"error": "Path is not a valid directory or does not exist."} ), 400

    items = []
    try:
        # Add parent directory navigation, but don't allow going above the allowed_path
        if abs_path != allowed_path:
            parent_path = os.path.dirname(abs_path)
            items.append({
                "name": "..",
                "type": "dir",
                "path": parent_path
            })

        for entry in os.listdir(abs_path):
            entry_path = os.path.join(abs_path, entry)
            if os.path.isdir(entry_path):
                items.append({
                    "name": entry,
                    "type": "dir",
                    "path": entry_path
                })
    except PermissionError:
        return jsonify({"error": "Permission denied to access this directory."} ), 403
    except Exception as e:
        return jsonify({"error": f"Failed to list directory: {e}"} ), 500

    return jsonify({"path": abs_path, "items": items})


@app.route('/api/hdhive/test_cookie', methods=['POST'])
@login_required
def api_hdhive_test_cookie():
    config = load_config()
    base_url = (config.get('hdhive_base_url') or 'https://hdhive.com').strip()
    api_key = (config.get('hdhive_open_api_key') or '').strip()
    if api_key:
        ping = _hdhive_open_api_ping(base_url, api_key)
        if isinstance(ping, dict) and ping.get("success") is True:
            details = []
            data = ping.get("data") if isinstance(ping.get("data"), dict) else {}
            if data:
                if data.get("api_key_id") is not None:
                    details.append(f"api_key_id={data.get('api_key_id')}")
                if data.get("name"):
                    details.append(f"name={data.get('name')}")
            return jsonify({"success": True, "message": "Open API Key 有效", "details": details})
        msg = (ping.get("message") if isinstance(ping, dict) else None) or "Open API Key 无效"
        code = (ping.get("code") if isinstance(ping, dict) else None) or ""
        detail = f"code={code}" if code else "code=unknown"
        return jsonify({"success": False, "message": msg, "details": [detail]})

    cookie = (config.get('hdhive_cookie') or '').strip()
    result = _test_hdhive_cookie(base_url, cookie)
    return jsonify(result)


@app.route('/web_login', methods=['GET', 'POST'])
async def web_login():
    config = load_config()
    web_auth = config.get('web_auth', {})
    
    # If no password is set, allow setting it
    if not web_auth.get('password_hash'):
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            # Use 'admin' as default username for initial setup if not configured
            expected_username = web_auth.get('username', 'admin') 
            if username == expected_username and password:
                web_auth['password_hash'] = generate_password_hash(password)
                web_auth['username'] = expected_username # Ensure username is saved
                config['web_auth'] = web_auth
                save_config(config)
                flash('初始密码已设置成功，请登录。', 'success')
                return redirect(url_for('web_login'))
            else:
                flash('请提供有效的用户名和密码来设置初始密码。', 'error')
        return render_template('web_setup_password.html', username=web_auth.get('username', 'admin'))

    # If password is set, handle login
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if username == web_auth.get('username') and check_password_hash(web_auth['password_hash'], password):
            session['logged_in'] = True
            flash('登录成功！', 'success')
            return redirect(url_for('index'))
        else:
            flash('用户名或密码错误。', 'error')
    
    return render_template('web_login.html', username=web_auth.get('username', 'admin'))

@app.route('/web_logout')
def web_logout():
    session.pop('logged_in', None)
    flash('您已成功退出登录。', 'info')
    return redirect(url_for('web_login'))

@app.route('/test_accordion')
def test_accordion():
    """Simple test page for Bootstrap Accordion functionality"""
    return render_template('test_accordion.html')


# --- Main execution ---
if __name__ == "__main__":
    config = load_config()
    debug_mode = bool(config.get('debug_mode', False))
    # Allow env override
    if os.environ.get('FLASK_DEBUG') in ('1', 'true', 'True', 'yes', 'on'):
        debug_mode = True

    # When reloader is enabled, Flask starts a parent process and a child process.
    # Only start monitor subprocesses in the child (WERKZEUG_RUN_MAIN==true),
    # otherwise they will be started twice and Telethon's SQLite session will lock.
    should_start_monitors = (not debug_mode) or (os.environ.get("WERKZEUG_RUN_MAIN") == "true")
    if should_start_monitors:
        start_monitor_process()
        start_file_monitor_process()
        start_bot_monitor_process()
        start_drama_scheduler()

    app.run(host="0.0.0.0", port=5001, debug=debug_mode, use_reloader=debug_mode)
