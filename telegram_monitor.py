import os
import json
import asyncio
import time
import traceback
import sys
import sqlite3
import re
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict
import html
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from telethon.sync import TelegramClient
from telethon import events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
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
    print(f"[{ts}] [{level}] {message}")


def debug_log(message: str):
    log_message(message, level="DEBUG")


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
        "hdhive_cookie": "",
        "hdhive_auto_unlock_points_threshold": 0,
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
        if 'debug_mode' not in config:
            config['debug_mode'] = False
        if 'hdhive_cookie' not in config:
            config['hdhive_cookie'] = ''
        if 'hdhive_auto_unlock_points_threshold' not in config:
            config['hdhive_auto_unlock_points_threshold'] = 0
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

            # --- Blacklist Check ---
            blacklist = restricted_entry.get('blacklist_keywords', [])
            if blacklist and msg.message:
                for keyword in blacklist:
                    if match_keyword(keyword, msg.message):
                        debug_log(f"在频道 {restricted_channel_id} 中检测到黑名单关键词或正则 '{keyword}'，跳过。")
                        return

            # --- Whitelist Check ---
            whitelist = restricted_entry.get('whitelist_keywords', [])
            if whitelist:
                found_whitelist = False
                if msg.message:
                    for keyword in whitelist:
                        if match_keyword(keyword, msg.message):
                            found_whitelist = True
                            break
                if not found_whitelist:
                    debug_log(f"在频道 {restricted_channel_id} 中未检测到白名单关键词或正则，跳过消息 {msg.id}。")
                    return

            # --- Detect Message Type ---
            media_type_detected = 'text'
            if msg.video:
                media_type_detected = 'video'
            elif isinstance(msg.media, MessageMediaDocument) and msg.media.document.mime_type.startswith('video/'):
                media_type_detected = 'video'
            elif msg.photo:
                media_type_detected = 'photo'
            elif msg.audio or msg.voice:
                media_type_detected = 'audio'
            elif isinstance(msg.media, MessageMediaDocument):
                media_type_detected = 'document'
            
            debug_log(f" 消息类型: {media_type_detected}")
            
            # --- Check Monitor Type ---
            monitor_type = restricted_entry.get('monitor_type', 'all')
            if monitor_type != 'all' and monitor_type != media_type_detected:
                debug_log(f" 频道监控类型为 '{monitor_type}'，与消息类型 '{media_type_detected}' 不匹配，跳过。")
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
            
            # --- Decision Logic ---
            # Use the flags we already determined based on monitor_type
            is_monitored = True # We already filtered by monitor_type above

            if not should_download and not should_forward:
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
                    original_ext = ".mp4" # Default fallback
                    if media_type_detected == 'photo':
                        original_ext = ".jpg"
                    elif media_type_detected == 'audio':
                        original_ext = ".mp3"
                    
                    if msg.video and hasattr(msg.video, 'mime_type') and msg.video.mime_type:
                        original_ext = "." + msg.video.mime_type.split('/')[-1]
                    elif isinstance(msg.media, MessageMediaDocument) and hasattr(msg.media.document, 'mime_type') and msg.media.document.mime_type:
                        original_ext = "." + msg.media.document.mime_type.split('/')[-1]

                    final_filename = ""
                    final_folder_path = download_directory

                    # Check for Album/Grouped Media
                    if msg.grouped_id:
                        try:
                            search_min_id = msg.id - 10
                            search_max_id = msg.id + 10
                            nearby_msgs = await client.get_messages(
                                event.chat_id, 
                                min_id=search_min_id if search_min_id > 0 else 0, 
                                max_id=search_max_id
                            )
                            album_msgs = [m for m in nearby_msgs if m.grouped_id == msg.grouped_id]
                            if not any(m.id == msg.id for m in album_msgs):
                                album_msgs.append(msg)
                            album_msgs.sort(key=lambda m: m.id)
                            
                            album_caption = ""
                            for m in album_msgs:
                                if m.message:
                                    album_caption = m.message
                                    break
                            
                            folder_name = str(msg.grouped_id)
                            if album_caption:
                                sanitized_caption = sanitize_filename(album_caption, limit=60)
                                if sanitized_caption:
                                    folder_name = sanitized_caption
                            
                            final_folder_path = os.path.join(download_directory, folder_name)
                            os.makedirs(final_folder_path, exist_ok=True)

                            try:
                                index = [m.id for m in album_msgs].index(msg.id) + 1
                            except ValueError:
                                index = msg.id
                            
                            final_filename = f"{index}{original_ext}"
                            log_message(f"检测到相册消息 (Group: {msg.grouped_id})。归档至: '{folder_name}/{final_filename}'")
                        except Exception as e:
                            log_message(f"处理相册逻辑失败: {e}。")
                    
                    if not final_filename:
                        try:
                            potential_name = msg.message or "" 
                            if not potential_name and msg.document:
                                for attr in msg.document.attributes:
                                    if hasattr(attr, 'file_name') and attr.file_name:
                                        potential_name = attr.file_name
                                        break
                            
                            if potential_name:
                                clean_name = sanitize_filename(potential_name, limit=60)
                                if clean_name: 
                                    final_filename = f"{clean_name}{original_ext}"
                                else:
                                    final_filename = f"{base_name}{original_ext}"
                            else:
                                final_filename = f"{base_name}{original_ext}"
                        except Exception as e:
                            final_filename = f"{base_name}{original_ext}"

                    file_path = os.path.join(final_folder_path, final_filename)
                    log_message(f"开始下载 {media_type_detected} 到 {file_path}...")
                    
                    try:
                        downloaded_file = await reliable_action(
                            f"下载 {media_type_detected} {msg.id}",
                            client.download_media,
                            msg,
                            file=file_path
                        )
                        if downloaded_file:
                            log_message(f"{media_type_detected} 已成功下载到 {downloaded_file}。")
                        else:
                            log_message(f"{media_type_detected} 下载失败。")
                    except Exception:
                        pass
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
    while True:
        try:
            # Heartbeat log
            log_message(f"监控运行中... 当前关注 {len(current_config.get('restricted_channels', []))} 个频道。")
            
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
