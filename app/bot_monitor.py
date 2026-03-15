import os
import json
import logging
import asyncio
import sqlite3
import time
import fcntl
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from telethon import TelegramClient, events, functions, types
from telethon.sessions import SQLiteSession
import sys
from dotenv import load_dotenv
from urllib.parse import urlparse

# Add current directory to sys.path to find downloader_module when running from app directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import re

from downloader_module import downloader

# Setup logging after config is loaded
def setup_logging():
    config = load_config()
    debug_mode = config.get('debug_mode', False)
    logging.basicConfig(
        level=logging.DEBUG if debug_mode else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True # Force override existing config
    )

logger = logging.getLogger(__name__)

# Get the project root directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Try to find config in BASE_DIR/config or ./config
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
if not os.path.exists(CONFIG_DIR):
    CONFIG_DIR = os.path.abspath('config')

CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')
TELEGRAM_SESSION_NAME = "bot_session"
BOT_LOCK_FILE = os.path.join(CONFIG_DIR, 'bot_monitor.lock')

# Load environment variables
load_dotenv(os.path.join(CONFIG_DIR, '.env'))

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
        if not isinstance(config, dict):
            logger.error("Invalid config format: top-level JSON must be an object")
            return {}
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}


def _token_fingerprint(token: str) -> str:
    """Return a short non-reversible fingerprint for safe diagnostics."""
    if not token:
        return "empty"
    digest = hashlib.sha1(token.encode('utf-8')).hexdigest()[:10]
    return f"sha1:{digest},len:{len(token)}"


def _is_telegram_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = (parsed.netloc or '').lower()
    return host in ("t.me", "telegram.me", "telegram.dog")


def _extract_bot_id_from_token(bot_token: str) -> Optional[int]:
    """Extract bot id prefix from token like '<bot_id>:<secret>' for validation."""
    if not bot_token or ':' not in bot_token:
        return None
    head = (bot_token or '').split(':', 1)[0].strip()
    if not head.isdigit():
        return None
    try:
        return int(head)
    except Exception:
        return None


def resolve_writable_download_dir(config):
    """Return a writable download path, falling back to project downloads dir."""
    configured_path = config.get('downloader', {}).get('default_path') or os.path.join(BASE_DIR, 'downloads')
    candidate = os.path.abspath(os.path.expanduser(configured_path))

    def _is_writable_dir(path):
        try:
            os.makedirs(path, exist_ok=True)
            test_file = os.path.join(path, '.write_test')
            with open(test_file, 'w', encoding='utf-8') as f:
                f.write('ok')
            os.remove(test_file)
            return True
        except Exception:
            return False

    if _is_writable_dir(candidate):
        return candidate, None

    fallback = os.path.join(BASE_DIR, 'downloads')
    if _is_writable_dir(fallback):
        return fallback, candidate

    return None, candidate


def resolve_youtube_quality_mode(config):
    """Pick YouTube quality mode for bot link downloads.

    Defaults to ultra_quality to avoid unnecessary 1080p caps on 4K sources.
    """
    downloader_cfg = (config or {}).get('downloader', {}) if isinstance(config, dict) else {}
    if not isinstance(downloader_cfg, dict):
        downloader_cfg = {}

    mode = (downloader_cfg.get('youtube_quality_mode') or 'ultra_quality').strip()
    if mode in {'super_fast_720p', 'fast_compatible', 'balanced_hd', 'ultra_quality'}:
        return mode
    return 'ultra_quality'


def normalize_quality_mode(mode: str, default: str = 'balanced_hd') -> str:
    allowed = {'super_fast_720p', 'fast_compatible', 'balanced_hd', 'ultra_quality'}
    normalized = (mode or '').strip()
    return normalized if normalized in allowed else default


def normalize_upload_mode(mode: str, default: str = 'transcode') -> str:
    allowed = {'transcode', 'original', 'document'}
    normalized = (mode or '').strip().lower()
    return normalized if normalized in allowed else default


def is_streaming_compatible_media(probe: Optional[dict]) -> bool:
    if not isinstance(probe, dict):
        return False
    video_stream = probe.get('video_stream')
    if not isinstance(video_stream, dict):
        return False

    vcodec = (probe.get('vcodec') or '').lower()
    pix_fmt = (probe.get('pix_fmt') or '').lower()
    rotation = int(probe.get('rotation') or 0) % 360
    sar = str(probe.get('sar') or '').strip()

    if vcodec != 'h264':
        return False
    if pix_fmt not in ('yuv420p', 'yuvj420p'):
        return False
    if rotation != 0:
        return False
    if sar and sar not in ('1:1', 'N/A', 'unknown', '0:1'):
        return False
    return True


def acquire_single_instance_lock():
    """Ensure only one bot_monitor process can run at a time."""
    os.makedirs(CONFIG_DIR, exist_ok=True)
    lock_fp = open(BOT_LOCK_FILE, 'w', encoding='utf-8')
    try:
        fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fp.write(str(os.getpid()))
        lock_fp.flush()
        return lock_fp
    except BlockingIOError:
        try:
            lock_fp.close()
        except Exception:
            pass
        return None

def merge_cookies_logic(existing_path, new_path):
    """
    Merges new cookies into existing cookies.
    - Standardizes both files to Netscape format using downloader._prepare_cookies.
    - If a domain exists in new cookies, all cookies for that domain in existing file are removed.
    - Appends new cookies.
    """
    # 1. Standardize new cookies
    validated_new_path = downloader._prepare_cookies(new_path)
    
    new_cookies_lines = []
    new_domains = set()
    
    try:
        with open(validated_new_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split('\t')
                # Netscape: domain flag path secure expiration name value (7 fields)
                if len(parts) >= 6: 
                    domain = parts[0]
                    new_domains.add(domain)
                    new_cookies_lines.append(line)
    except Exception as e:
        downloader.log(f"Error parsing new cookies: {e}", "error")
        # Cleanup temp
        if validated_new_path != new_path and os.path.exists(validated_new_path):
            try: os.remove(validated_new_path)
            except: pass
        return False, str(e)

    # 2. Read and Filter Existing Cookies
    existing_cookies_lines = []
    if os.path.exists(existing_path):
        validated_existing_path = downloader._prepare_cookies(existing_path)
        try:
             with open(validated_existing_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    parts = line.split('\t')
                    if len(parts) >= 6:
                        domain = parts[0]
                        if domain in new_domains:
                            continue # Skip old cookie if domain is in new update
                        existing_cookies_lines.append(line)
        except Exception as e:
             downloader.log(f"Error parsing existing cookies: {e}", "warning")
             # Proceed with empty existing if corrupt
        
        # Clean up temp existing if created
        if validated_existing_path != existing_path and os.path.exists(validated_existing_path):
            try: os.remove(validated_existing_path)
            except: pass

    # 3. Write Merged Result
    try:
        with open(existing_path, 'w', encoding='utf-8') as f:
            f.write("# Netscape HTTP Cookie File\n")
            f.write(f"# Updated by bot_monitor.py at {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for line in existing_cookies_lines:
                f.write(line + "\n")
            for line in new_cookies_lines:
                f.write(line + "\n")
        
        # Cleanup temp new
        if validated_new_path != new_path and os.path.exists(validated_new_path):
            try: os.remove(validated_new_path)
            except: pass
            
        return True, f"Updated {len(new_cookies_lines)} cookies. Overwrote {len(new_domains)} domains."
    except Exception as e:
        downloader.log(f"Error writing merged cookies: {e}", "error")
        return False, str(e)

async def main():
    lock_fp = acquire_single_instance_lock()
    if lock_fp is None:
        downloader.log("检测到已有 bot_monitor 实例在运行，当前进程退出以避免 session 锁冲突。", "warning")
        return

    config = load_config()
    
    # Priority: Config > Env fallback
    api_id = os.environ.get('TELEGRAM_API_ID') or config.get('telegram', {}).get('api_id')
    api_hash = os.environ.get('TELEGRAM_API_HASH') or config.get('telegram', {}).get('api_hash')
    config_bot_token = (config.get('bot', {}).get('token') or '').strip()
    env_bot_token = (os.environ.get('TELEGRAM_BOT_TOKEN') or '').strip()
    bot_token = config_bot_token or env_bot_token
    bot_token_source = 'config.json' if config_bot_token else ('env' if env_bot_token else 'none')

    downloader.log(
        f"Bot token source={bot_token_source}, fingerprint={_token_fingerprint(bot_token)}",
        "info"
    )

    if not api_id or not api_hash or not bot_token:
        downloader.log("Bot credentials (API ID, Hash, or Token) missing. Bot monitor will not start.", "warning")
        return

    # Ensure integer
    try:
        api_id = int(api_id)
    except (ValueError, TypeError):
        logger.error(f"API ID must be an integer. Got: {api_id}")
        return

    session_path = os.path.join(CONFIG_DIR, TELEGRAM_SESSION_NAME)
    
    # Pre-configure the database file with WAL mode before creating session
    db_file = session_path + '.session'
    try:
        # Open database and set WAL mode before Telethon uses it
        conn = sqlite3.connect(db_file, timeout=10)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=10000')
        conn.commit()
        conn.close()
        downloader.log("Pre-configured session database with WAL mode")
    except Exception as e:
        downloader.log(f"Warning: Could not pre-configure database: {e}", "warning")
    
    # Now create the Telethon session
    client = TelegramClient(session_path, api_id, api_hash)

    # State management for interactive flows
    waiting_for_cookies = set()
    waiting_for_download_path = set()

    def save_runtime_config(cfg: dict) -> bool:
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(cfg, f, indent=4, ensure_ascii=False)
            return True
        except Exception as e:
            downloader.log(f"保存配置失败: {e}", "error")
            return False

    def ensure_downloader_config(cfg: dict) -> dict:
        if not isinstance(cfg, dict):
            cfg = {}
        downloader_cfg = cfg.get('downloader')
        if not isinstance(downloader_cfg, dict):
            downloader_cfg = {}
            cfg['downloader'] = downloader_cfg
        return downloader_cfg

    def quality_mode_label(mode: str) -> str:
        labels = {
            'super_fast_720p': '超快 720p',
            'fast_compatible': '快速兼容 1080p',
            'balanced_hd': '均衡高清',
            'ultra_quality': '超清优先',
        }
        return labels.get(mode, mode or '未设置')

    def upload_mode_label(mode: str) -> str:
        labels = {
            'transcode': '兼容转码上传',
            'original': '原码直传',
            'document': '原码文件发送',
        }
        return labels.get(mode, mode or '未设置')

    def build_download_settings_text(cfg: dict) -> str:
        downloader_cfg = ensure_downloader_config(cfg)
        default_path = (downloader_cfg.get('default_path') or '').strip() or os.path.join(BASE_DIR, 'downloads')
        general_mode = normalize_quality_mode(
            downloader_cfg.get('quality_mode', 'balanced_hd'),
            default='balanced_hd'
        )
        yt_mode = normalize_quality_mode(
            downloader_cfg.get('youtube_quality_mode', 'ultra_quality'),
            default='ultra_quality',
        )
        upload_mode = normalize_upload_mode(
            downloader_cfg.get('upload_mode', 'transcode'),
            default='transcode',
        )
        return (
            "⚙️ **下载设置**\n"
            f"- 下载目录: `{default_path}`\n"
            f"- 通用画质(非YouTube): `{general_mode}` ({quality_mode_label(general_mode)})\n"
            f"- YouTube画质覆盖: `{yt_mode}` ({quality_mode_label(yt_mode)})\n\n"
            f"- 上传策略: `{upload_mode}` ({upload_mode_label(upload_mode)})\n\n"
            "点击按钮可直接修改。"
        )

    def build_download_settings_buttons():
        return [
            [
                types.KeyboardButtonCallback("YT: 超快720p", data=b'dl_yt_super_fast_720p'),
                types.KeyboardButtonCallback("YT: 快速1080p", data=b'dl_yt_fast_compatible'),
            ],
            [
                types.KeyboardButtonCallback("YT: 均衡高清", data=b'dl_yt_balanced_hd'),
                types.KeyboardButtonCallback("YT: 超清优先", data=b'dl_yt_ultra_quality'),
            ],
            [
                types.KeyboardButtonCallback("通用: 超快720p", data=b'dl_general_super_fast_720p'),
                types.KeyboardButtonCallback("通用: 均衡高清", data=b'dl_general_balanced_hd'),
            ],
            [
                types.KeyboardButtonCallback("通用: 快速1080p", data=b'dl_general_fast_compatible'),
                types.KeyboardButtonCallback("通用: 超清优先", data=b'dl_general_ultra_quality'),
            ],
            [
                types.KeyboardButtonCallback("设置下载目录", data=b'dl_set_path'),
                types.KeyboardButtonCallback("刷新设置", data=b'dl_settings'),
            ],
            [
                types.KeyboardButtonCallback("上传: 兼容转码", data=b'dl_upload_transcode'),
                types.KeyboardButtonCallback("上传: 原码直传", data=b'dl_upload_original'),
            ],
            [
                types.KeyboardButtonCallback("上传: 原码文件发送", data=b'dl_upload_document'),
            ],
        ]

    def locate_cookies_file(cfg) -> Tuple[Optional[str], List[str]]:
        configured = (cfg.get('downloader', {}) or {}).get('cookies_file')
        candidates: List[str] = []
        if configured:
            candidates.append(os.path.abspath(os.path.expanduser(configured)))
        candidates.append(os.path.join(CONFIG_DIR, 'cookies.txt'))
        candidates.append(os.path.abspath('cookies.txt'))

        seen = set()
        paths: List[str] = []
        for p in candidates:
            if p and p not in seen:
                seen.add(p)
                paths.append(p)

        for p in paths:
            if os.path.exists(p):
                return p, paths
        return None, paths

    def inspect_cookies_file(path: str) -> Dict[str, object]:
        now_ts = int(time.time())
        total_lines = 0
        valid_lines = 0
        malformed_lines = 0
        expired_lines = 0
        domains: Set[str] = set()
        cookie_names: Set[str] = set()

        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for raw in f:
                line = (raw or '').strip()
                if not line or line.startswith('#'):
                    continue

                total_lines += 1
                parts = line.split('\t')
                if len(parts) < 7:
                    malformed_lines += 1
                    continue

                valid_lines += 1
                domain = (parts[0] or '').strip().lower()
                name = (parts[5] or '').strip()
                expires = (parts[4] or '').strip()

                if domain:
                    domains.add(domain)
                if name:
                    cookie_names.add(name)

                try:
                    exp_ts = int(expires)
                    if exp_ts > 0 and exp_ts < now_ts:
                        expired_lines += 1
                except Exception:
                    pass

        yt_domains = {d for d in domains if 'youtube.com' in d or 'youtu.be' in d}
        google_domains = {d for d in domains if 'google.com' in d}
        key_login_cookies = {
            'SID', 'HSID', 'SSID', 'APISID', 'SAPISID',
            '__Secure-1PSID', '__Secure-3PSID', 'LOGIN_INFO'
        }
        present_key = sorted(list(key_login_cookies.intersection(cookie_names)))

        return {
            'total_lines': total_lines,
            'valid_lines': valid_lines,
            'malformed_lines': malformed_lines,
            'expired_lines': expired_lines,
            'domains': domains,
            'yt_domains': yt_domains,
            'google_domains': google_domains,
            'cookie_names': cookie_names,
            'present_key': present_key,
        }

    def build_cookies_status_text(cfg):
        found, _ = locate_cookies_file(cfg)

        if not found:
            return (
                "🍪 **Cookies 状态**\n"
                "- 状态: 未找到 cookies 文件\n"
                "- 建议: 发送 `/start` -> 点击 `🍪 更新 Cookies` 上传 `cookies.txt`"
            )

        try:
            st = os.stat(found)
            size_kb = st.st_size / 1024
            mtime = datetime.fromtimestamp(st.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            size_kb = 0
            mtime = '未知'

        hint = "✅ 文件存在，可用于下载测试"
        if size_kb < 1:
            hint = "⚠️ 文件过小，可能不是有效导出的 Cookies"

        return (
            "🍪 **Cookies 状态**\n"
            f"- 路径: `{found}`\n"
            f"- 大小: `{size_kb:.1f} KB`\n"
            f"- 修改时间: `{mtime}`\n"
            f"- 检查结果: {hint}"
        )

    def build_cookies_check_text(cfg):
        found, searched_paths = locate_cookies_file(cfg)
        if not found:
            searched = '\n'.join([f"  - `{p}`" for p in searched_paths]) or "  - (无)"
            return (
                "🍪 **Cookies 深度检查**\n"
                "- 结果: 未找到 cookies 文件\n"
                "- 已检查路径:\n"
                f"{searched}\n"
                "- 建议: 发送 `/start` -> 点击 `🍪 更新 Cookies` 上传最新 `cookies.txt`"
            )

        try:
            st = os.stat(found)
            info = inspect_cookies_file(found)
        except Exception as e:
            return (
                "🍪 **Cookies 深度检查**\n"
                f"- 路径: `{found}`\n"
                f"- 结果: 读取失败 `{e}`"
            )

        valid_lines = int(info['valid_lines'])
        malformed_lines = int(info['malformed_lines'])
        expired_lines = int(info['expired_lines'])
        yt_domains = sorted(list(info['yt_domains']))
        google_domains = sorted(list(info['google_domains']))
        present_key = info['present_key']

        verdict = "✅ 基本可用"
        if valid_lines == 0:
            verdict = "❌ 无有效 Netscape Cookie 记录"
        elif not yt_domains and not google_domains:
            verdict = "⚠️ 缺少 YouTube/Google 域名记录"
        elif len(present_key) < 2:
            verdict = "⚠️ 登录态关键信息偏少，可能仍会触发风控"

        return (
            "🍪 **Cookies 深度检查**\n"
            f"- 路径: `{found}`\n"
            f"- 文件大小: `{st.st_size / 1024:.1f} KB`\n"
            f"- 有效记录: `{valid_lines}`\n"
            f"- 格式异常行: `{malformed_lines}`\n"
            f"- 已过期记录: `{expired_lines}`\n"
            f"- YouTube域: `{len(yt_domains)}`\n"
            f"- Google域: `{len(google_domains)}`\n"
            f"- 关键登录Cookie: `{', '.join(present_key) if present_key else '未检测到'}`\n"
            f"- 结论: {verdict}\n"
            "- 提示: 若仍下载失败，请在浏览器保持登录后重新导出 cookies。"
        )

    def has_strong_youtube_cookies(cfg) -> bool:
        """Heuristic: enough evidence that login cookies are present."""
        found, _ = locate_cookies_file(cfg)
        if not found:
            return False
        try:
            info = inspect_cookies_file(found)
        except Exception:
            return False

        yt_domains = set(info.get('yt_domains') or [])
        google_domains = set(info.get('google_domains') or [])
        present_key = set(info.get('present_key') or [])

        has_domain = bool(yt_domains or google_domains)
        # At least 2 key cookies usually means usable login state exists.
        return has_domain and len(present_key) >= 2

    # --- Command Handlers ---
    @client.on(events.NewMessage(pattern='/start'))
    async def start_handler(event):
        welcome_text = (
            "👋 **你好！我是您的文件监控助手。**\n\n"
            "我可以帮您下载视频（支持 YouTube、TikTok、X/Twitter、Facebook、Instagram 等）。\n\n"
            "👇 **点击下方按钮开始交互**"
        )
        buttons = [
            [types.KeyboardButtonCallback("🍪 更新 Cookies", data=b'update_cookies')],
            [types.KeyboardButtonCallback("❓ 使用帮助", data=b'help_menu')]
        ]
        # Note: KeyboardButtonCallback is for Inline keyboards usually sent via buttons=... in client.send_message
        # In Telethon events.reply(buttons=...) works if it's inline.
        # Strict types: buttons should be list of lists of Button.inline(...)
        await event.reply(welcome_text, buttons=[
            [types.KeyboardButtonCallback("🍪 更新 Cookies", data=b'update_cookies')],
            [types.KeyboardButtonCallback("⚙️ 下载设置", data=b'dl_settings')],
            [types.KeyboardButtonCallback("🗑 清除记录", data=b'clear_history'), types.KeyboardButtonCallback("❓ 使用帮助", data=b'help_menu')]
        ])

    @client.on(events.CallbackQuery(data=b'clear_history'))
    async def clear_history_callback_handler(event):
        await event.answer("正在清理...")
        chat_id = event.chat_id
        
        # 1. Clear Internal Logs
        downloader.clear_logs()
        
        # 2. Delete Chat History
        # Bots generally cannot fetch history (GetHistoryRequest) to find message IDs to delete.
        # They can only delete messages if they already know the IDs.
        # Since we don't track IO, we cannot bulk delete past messages safely.
        
        # Send confirmation
        await client.send_message(chat_id, "✅ **日志已清除**\n(由于 Telegram 限制，机器人无法删除历史聊天记录，请您手动清理)")
        
        # Send Start Menu again to refresh UI
        # Construct a fake event or just send the text/buttons directly
        welcome_text = (
            "👋 **你好！我是您的文件监控助手。**\n\n"
            "我可以帮您下载视频（支持 YouTube、TikTok、X/Twitter、Facebook、Instagram 等）。\n\n"
            "👇 **点击下方按钮开始交互**"
        )
        await client.send_message(chat_id, welcome_text, buttons=[
            [types.KeyboardButtonCallback("🍪 更新 Cookies", data=b'update_cookies')],
            [types.KeyboardButtonCallback("⚙️ 下载设置", data=b'dl_settings')],
            [types.KeyboardButtonCallback("🗑 清除记录", data=b'clear_history'), types.KeyboardButtonCallback("❓ 使用帮助", data=b'help_menu')]
        ])

    @client.on(events.NewMessage(pattern='/download_settings'))
    async def download_settings_handler(event):
        cfg = load_config()
        await event.reply(build_download_settings_text(cfg), buttons=build_download_settings_buttons())

    @client.on(events.CallbackQuery(data=b'dl_settings'))
    async def download_settings_callback_handler(event):
        await event.answer("已刷新")
        cfg = load_config()
        await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())

    @client.on(events.CallbackQuery(data=b'dl_set_path'))
    async def download_set_path_callback_handler(event):
        waiting_for_download_path.add(event.sender_id)
        await event.answer()
        await event.respond(
            "请发送新的下载目录绝对路径，例如：`/app/downloads`\n"
            "发送 `/cancel` 可取消。"
        )

    @client.on(events.CallbackQuery(data=b'dl_yt_super_fast_720p'))
    async def download_set_yt_super_fast_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['youtube_quality_mode'] = 'super_fast_720p'
        if save_runtime_config(cfg):
            await event.answer("YouTube 模式已设为超快720p")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_yt_fast_compatible'))
    async def download_set_yt_fast_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['youtube_quality_mode'] = 'fast_compatible'
        if save_runtime_config(cfg):
            await event.answer("YouTube 模式已设为快速1080p")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_yt_balanced_hd'))
    async def download_set_yt_balanced_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['youtube_quality_mode'] = 'balanced_hd'
        if save_runtime_config(cfg):
            await event.answer("YouTube 模式已设为均衡高清")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_yt_ultra_quality'))
    async def download_set_yt_ultra_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['youtube_quality_mode'] = 'ultra_quality'
        if save_runtime_config(cfg):
            await event.answer("YouTube 模式已设为超清优先")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_general_super_fast_720p'))
    async def download_set_general_super_fast_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['quality_mode'] = 'super_fast_720p'
        if save_runtime_config(cfg):
            await event.answer("通用模式已设为超快720p")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_general_balanced_hd'))
    async def download_set_general_balanced_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['quality_mode'] = 'balanced_hd'
        if save_runtime_config(cfg):
            await event.answer("通用模式已设为均衡高清")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_general_fast_compatible'))
    async def download_set_general_fast_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['quality_mode'] = 'fast_compatible'
        if save_runtime_config(cfg):
            await event.answer("通用模式已设为快速1080p")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_general_ultra_quality'))
    async def download_set_general_ultra_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['quality_mode'] = 'ultra_quality'
        if save_runtime_config(cfg):
            await event.answer("通用模式已设为超清优先")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_upload_transcode'))
    async def download_set_upload_transcode_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['upload_mode'] = 'transcode'
        if save_runtime_config(cfg):
            await event.answer("上传策略已设为兼容转码")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_upload_original'))
    async def download_set_upload_original_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['upload_mode'] = 'original'
        if save_runtime_config(cfg):
            await event.answer("上传策略已设为原码直传")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'dl_upload_document'))
    async def download_set_upload_document_handler(event):
        cfg = load_config()
        downloader_cfg = ensure_downloader_config(cfg)
        downloader_cfg['upload_mode'] = 'document'
        if save_runtime_config(cfg):
            await event.answer("上传策略已设为原码文件发送")
            await event.respond(build_download_settings_text(cfg), buttons=build_download_settings_buttons())
        else:
            await event.answer("保存失败", alert=True)

    @client.on(events.CallbackQuery(data=b'update_cookies'))
    async def cookie_callback_handler(event):
        sender_id = event.sender_id
        waiting_for_cookies.add(sender_id)
        await event.answer()
        await event.respond("请现在发送 `cookies.txt` 文件，或者直接粘贴 Cookies 内容发送给我。\n(发送 /cancel 可取消操作)")

    @client.on(events.CallbackQuery(data=b'help_menu'))
    async def help_callback_handler(event):
        await event.answer()
        help_text = (
            "🛠 **帮助菜单**\n\n"
            "1. **发送链接**: 直接粘贴 URL。\n"
            "2. **更新 Cookies**: 点击菜单按钮后发送文件或文本。\n"
            "3. **下载设置**: 发送 `/download_settings` 或点 `⚙️ 下载设置`\n"
            "4. **检查 Cookies**: 发送 `/cookies_status`\n"
            "5. **深度检查 Cookies**: 发送 `/cookies_check`\n"
            "6. **搜寻影视**: 发送 `/movie <剧名>` (例如: `/movie 金玉满堂`)\n"
        )
        await event.respond(help_text)

    @client.on(events.NewMessage(pattern='/cancel'))
    async def cancel_handler(event):
        sender_id = event.sender_id
        was_waiting = False
        if sender_id in waiting_for_cookies:
            waiting_for_cookies.remove(sender_id)
            was_waiting = True
        if sender_id in waiting_for_download_path:
            waiting_for_download_path.remove(sender_id)
            was_waiting = True

        if was_waiting:
            await event.reply("❌ 操作已取消。")
        else:
            await event.reply("当前没有进行中的操作。")

    @client.on(events.NewMessage(pattern='/help'))
    async def help_handler(event):
        # reuse help logic or redirect
        await help_callback_handler(event) # This might fail if event types differ (NewMessage vs CallbackQuery)
        # So just copy text or make a common func. Simpler to just reply here.
        help_text = (
            "🛠 **帮助菜单**\n\n"
            "1. **发送链接**: 直接粘贴 URL。\n"
            "2. **更新 Cookies**: 输入 /start 点击按钮。\n"
            "3. **下载设置**: 发送 `/download_settings`\n"
            "4. **检查 Cookies**: 发送 `/cookies_status`\n"
            "5. **深度检查 Cookies**: 发送 `/cookies_check`\n"
            "6. **搜寻影视**: 发送 `/movie <剧名>`\n"
        )
        await event.reply(help_text)

    @client.on(events.NewMessage(pattern='/cookies_status'))
    async def cookies_status_handler(event):
        cfg = load_config()
        await event.reply(build_cookies_status_text(cfg))

    @client.on(events.NewMessage(pattern='/cookies_check'))
    async def cookies_check_handler(event):
        cfg = load_config()
        await event.reply(build_cookies_check_text(cfg))


    @client.on(events.NewMessage(pattern='/movie'))
    async def movie_handler(event):
        # Extract title: /movie <title>
        command_parts = event.text.split(' ', 1)
        if len(command_parts) < 2:
            await event.reply("🔎 **影巢搜索**\n使用方法: `/movie <影视名称>`\n例如: `/movie 奥本海默`")
            return
        
        title = command_parts[1].strip()
        if not title:
            await event.reply("❌ 请输入影视名称。")
            return

        # Load config and add to resource_requests
        import uuid
        import time
        
        config = load_config()
        resource_requests = config.get("resource_requests", [])
        
        new_req = {
            "id": str(uuid.uuid4()),
            "title": title,
            "status": "pending",
            "result": "",
            "error": "",
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "chat_id": event.chat_id
        }
        
        resource_requests.insert(0, new_req)
        config["resource_requests"] = resource_requests
        
        # Save config
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            
            await event.reply(f"🎬 **影视资源申请已提交**\n名称: {title}\n状态: 排队中...\n\n系统搜索完成后将给您发送结果。")
            logger.info(f"Bot: Added resource request '{title}' via /movie command.")
        except Exception as e:
            logger.error(f"Bot: Failed to save resource request: {e}")
            await event.reply(f"❌ 提交失败: {str(e)}")

    @client.on(events.NewMessage(pattern='/status'))
    async def status_handler(event):
        # We can expand this to show queue stats later
        await event.reply("✅ **服务运行正常**\n等待任务中...")

    @client.on(events.NewMessage(incoming=True))
    async def message_handler(event):
        # Reload config to get the latest settings (e.g., download path)
        config = load_config()
        
        if not event.is_private:
            return

        text = (event.text or '').strip()
        
        # Ignore commands handled by other handlers
        if text.startswith('/'):
            return

        # Ignore auto-click notifications to avoid triggering downloader
        if text.startswith("已自动点击红包按钮"):
            return

        sender_id = event.sender_id
        
        # --- 1. Check for Cookies Content (Direct Text) ---
        # Heuristic: starts with # Netscape OR contains typical cookie fields OR specific domains
        is_cookie = False
        if text.startswith("# Netscape"):
            is_cookie = True
        elif len(text) > 50 and ("\t" in text or "    " in text): # Looks like columnar data
             if "TRUE" in text or "FALSE" in text or ".instagram.com" in text or ".youtube.com" in text or "google.com" in text:
                 is_cookie = True
        # Also check for Raw Header (key=value; key2=value2)
        elif len(text) > 50 and ';' in text and '=' in text and ("instagram.com" in text or "youtube.com" in text or "Cookie:" in text):
             is_cookie = True
        
        if is_cookie:
             try:
                # Sanitize cookies: Try to convert space-separated to tab-separated if needed
                # We keep this sanitization step because _prepare_cookies doesn't fix space-separated Netscape
                lines = text.strip().splitlines()
                sanitized_lines = []
                for line in lines:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        sanitized_lines.append(line)
                        continue
                    
                    if '\t' in line:
                        sanitized_lines.append(line)
                    elif ';' in line and '=' in line and '\t' not in line:
                         # Raw Header format? Leave for _prepare_cookies to handle if it matches heuristic there
                         # Just append line
                         sanitized_lines.append(line)
                    else:
                        # Try to fix space-separated lines
                        parts = line.split()
                        # Netscape format has at least 7 fields
                        if len(parts) >= 6:
                            if parts[1].upper() in ['TRUE', 'FALSE'] and parts[3].upper() in ['TRUE', 'FALSE']:
                                fixed_line = "\t".join(parts[:6])
                                if len(parts) > 6:
                                    fixed_line += "\t" + " ".join(parts[6:])
                                else:
                                    fixed_line += "\t" 
                                sanitized_lines.append(fixed_line)
                            else:
                                sanitized_lines.append(line)
                        else:
                            sanitized_lines.append(line)

                # Save sanitized text to TEMP file
                temp_text_path = os.path.join(CONFIG_DIR, f'temp_cookies_text_{int(time.time())}.txt')
                with open(temp_text_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(sanitized_lines))
                
                # Merge
                cookies_path = os.path.join(CONFIG_DIR, 'cookies.txt')
                success, msg = merge_cookies_logic(cookies_path, temp_text_path)
                
                # Cleanup temp
                if os.path.exists(temp_text_path):
                    os.remove(temp_text_path)

                if success:
                    await event.reply(f"✅ Cookies 合并成功！\n{msg}")
                    downloader.log(f"Merged cookies from text message by user {sender_id}")
                else:
                    await event.reply(f"❌ Cookies 合并失败: {msg}")

                # Clear waiting state if it exists
                if sender_id in waiting_for_cookies:
                    waiting_for_cookies.remove(sender_id)
                return
             except Exception as e:
                downloader.log(f"Failed to process text cookies: {e}", "error")
                await event.reply(f"❌ 处理失败: {e}")
             return

        # --- 2. Interactive Flow: Waiting for File/Confirmation ---
        if sender_id in waiting_for_cookies:
            # Check for File
            if event.file:
                 # Check extension or just try
                 file_name = event.file.name or "unknown_file"
                 await event.reply("收到文件，正在处理...")
                 try:
                     temp_file_path = os.path.join(CONFIG_DIR, f'temp_cookies_file_{int(time.time())}_{file_name}')
                     await event.download_media(file=temp_file_path)
                     
                     cookies_path = os.path.join(CONFIG_DIR, 'cookies.txt')
                     success, msg = merge_cookies_logic(cookies_path, temp_file_path)
                     
                     # Cleanup temp
                     if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)

                     if success:
                         await event.reply(f"✅ Cookies 文件合并成功！\n{msg}\n文件: {cookies_path}")
                         downloader.log(f"Merged cookies from file by user {sender_id}")
                         waiting_for_cookies.remove(sender_id)
                     else:
                        await event.reply(f"❌ Cookies 合并失败: {msg}")

                     return
                 except Exception as e:
                     downloader.log(f"Failed to process cookies file: {e}", "error")
                     await event.reply(f"❌ 处理失败: {e}")
                     return
            
            # Check for ANY Text if waiting (Force Accept) - Reuse logic from above logic?
            # Actually the "Is Cookie" check above handles it. 
            # If text didn't trigger "is_cookie" but we are waiting, maybe it matches JSON/Header but failed regex?
            # Let's try to process it anyway if we are waiting.
            if text and not text.startswith('/'):
                 try:
                    # Same logic as above for text
                    # We can just copy paste the temp file writing and merging
                    temp_text_path = os.path.join(CONFIG_DIR, f'temp_cookies_text_force_{int(time.time())}.txt')
                    with open(temp_text_path, 'w', encoding='utf-8') as f:
                        f.write(text) # Just write raw text, merge_cookies_logic will try _prepare_cookies which handles JSON/Header

                    cookies_path = os.path.join(CONFIG_DIR, 'cookies.txt')
                    success, msg = merge_cookies_logic(cookies_path, temp_text_path)
                    
                    if os.path.exists(temp_text_path):
                        os.remove(temp_text_path)

                    if success:
                         await event.reply(f"✅ (交互模式) Cookies 合并成功！\n{msg}")
                         waiting_for_cookies.remove(sender_id)
                         return
                    else:
                         # If fail, maybe tell user to correct format
                         await event.reply(f"❌ 无法识别有效的 Cookies 格式 (JSON/Netscape/Header)。\n错误: {msg}")
                         return

                 except Exception as e:
                    await event.reply(f"❌ 保存失败: {e}")
                    return

            await event.reply("请发送 `cookies.txt` 文件或粘贴 Cookies 内容。\n发送 /cancel 取消。")
            return

        # --- 3. Normal Flow: URLs ---
        # If user sends a file but not waiting for cookies, ignore or hint
        if sender_id in waiting_for_download_path:
            new_path = (text or '').strip()
            if not new_path:
                await event.reply("❌ 路径不能为空，请重新发送。")
                return

            if not os.path.isabs(new_path):
                await event.reply("❌ 请输入绝对路径，例如：`/app/downloads`")
                return

            try:
                os.makedirs(new_path, exist_ok=True)
                test_file = os.path.join(new_path, '.write_test')
                with open(test_file, 'w', encoding='utf-8') as f:
                    f.write('ok')
                os.remove(test_file)
            except Exception as e:
                await event.reply(f"❌ 目录不可写: `{new_path}`\n原因: `{e}`")
                return

            cfg = load_config()
            downloader_cfg = ensure_downloader_config(cfg)
            downloader_cfg['default_path'] = new_path
            if save_runtime_config(cfg):
                waiting_for_download_path.discard(sender_id)
                await event.reply(
                    "✅ 下载目录已更新。\n"
                    + build_download_settings_text(cfg),
                    buttons=build_download_settings_buttons()
                )
            else:
                await event.reply("❌ 保存失败，请稍后重试。")
            return

        if event.file:
            await event.reply("请先通过 /start 菜单点击 '🍪 更新 Cookies' 按钮后再发送文件。")
            return

        url_match = re.search(r'https?://\S+', text)
        if url_match:
            url = url_match.group(0).rstrip(')>,.;!\"\'')
            if _is_telegram_url(url):
                await event.reply("⚠️ Telegram 链接不支持下载，已忽略。")
                return
            # sender = await event.get_sender() # Might fail if user restricted privacy? just use sender_id
            downloader.log(f"Received URL from {sender_id}: {url}")
            # await event.reply(f"📥 **已加入下载队列**\n链接: {url}")
            
            msg = await event.reply("收到链接，正在准备下载...")
            
            default_path, bad_path = resolve_writable_download_dir(config)
            if bad_path:
                downloader.log(
                    f"配置下载目录不可写: {bad_path}，自动回退到项目目录 downloads/",
                    "warning"
                )
            if not default_path:
                await msg.edit("❌ **下载失败**\n原因: `下载目录不可写，请检查 downloader.default_path 权限配置。`")
                return
            
            try:
                # Check if it looks like a supported URL (basic check)
                if "youtube.com" in url or "youtu.be" in url or "instagram.com" in url:
                    pass
                else:
                    # await event.reply("这看起来像是一个链接，但我不仅确认支持 YouTube 或 Instagram。尝试下载中...")
                    pass

                cookies_file = config.get('downloader', {}).get('cookies_file')
                cookies_from_browser = config.get('downloader', {}).get('cookies_from_browser')
                
                # Try to auto-detect cookies.txt in config dir or root if not specified
                if not cookies_file:
                     possible_paths = [
                         os.path.join(CONFIG_DIR, 'cookies.txt'),
                         'cookies.txt'
                     ]
                     for p in possible_paths:
                         if os.path.exists(p):
                             cookies_file = os.path.abspath(p)
                             break

                proxy_config = config.get('proxy', {})
                proxy_url = None
                if proxy_config.get('addr') and proxy_config.get('port'):
                    addr = proxy_config['addr']
                    port = proxy_config['port']
                    user = proxy_config.get('username')
                    pwd = proxy_config.get('password')
                    if user and pwd:
                        proxy_url = f"http://{user}:{pwd}@{addr}:{port}"
                    else:
                        proxy_url = f"http://{addr}:{port}"

                youtube_quality_mode = None
                if "youtube.com" in url or "youtu.be" in url:
                    youtube_quality_mode = resolve_youtube_quality_mode(config)
                    downloader.log(f"YouTube 下载启用画质覆盖: {youtube_quality_mode}", "info")

                downloader_cfg = config.get('downloader', {}) if isinstance(config, dict) else {}
                if not isinstance(downloader_cfg, dict):
                    downloader_cfg = {}
                upload_mode = normalize_upload_mode(
                    downloader_cfg.get('upload_mode', 'transcode'),
                    default='transcode',
                )
                use_transcode_upload = (upload_mode == 'transcode')
                force_document_mode = (upload_mode == 'document')
                downloader.log(f"上传策略: {upload_mode}", "info")

                download_started_at = time.perf_counter()
                filename = await downloader.download_task(
                    url,
                    default_path,
                    cookies_file,
                    cookies_from_browser,
                    proxy_url,
                    youtube_quality_mode,
                    False,
                )
                download_elapsed = time.perf_counter() - download_started_at
                
                if filename and os.path.exists(filename):
                    original_download_path = filename
                    original_probe = None
                    original_streaming_compatible = False
                    try:
                        original_probe = downloader._probe_media_streams(original_download_path)
                        original_streaming_compatible = is_streaming_compatible_media(original_probe)
                    except Exception:
                        original_probe = None
                        original_streaming_compatible = False

                    compat_elapsed = 0.0
                    transcoded_for_upload = False
                    part_size_kb = 512
                    max_upload_parts = 4000
                    max_upload_bytes = part_size_kb * 1024 * max_upload_parts

                    async def _run_compatibility_pass(current_file: str):
                        started_at = time.perf_counter()
                        fixed_path = await asyncio.get_running_loop().run_in_executor(
                            downloader.executor,
                            downloader._maybe_make_telegram_compatible,
                            current_file,
                        )
                        elapsed = time.perf_counter() - started_at
                        if fixed_path and os.path.exists(fixed_path):
                            changed = os.path.abspath(fixed_path) != os.path.abspath(current_file)
                            return fixed_path, elapsed, changed
                        return current_file, elapsed, False

                    if use_transcode_upload:
                        # Final guard: local files (or edge outputs) should still pass compatibility check.
                        try:
                            filename, compat_elapsed, transcoded_for_upload = await _run_compatibility_pass(filename)
                        except Exception as _fix_err:
                            downloader.log(f"Upload compatibility precheck failed, fallback to original file: {_fix_err}", "warning")

                        # In transcode mode, if original media is not stream-compatible but transcode did not produce
                        # a converted file, do not continue with a misleading "upload success" flow.
                        if (not original_streaming_compatible) and (not transcoded_for_upload):
                            await msg.edit(
                                "❌ **上传已中止（转码失败）**\n"
                                "当前源视频不兼容 Telegram 流媒体播放，且兼容转码未成功。\n"
                                "请重试，或在下载设置中切换为 `上传: 原码直传`。"
                            )
                            downloader.log(
                                f"转码模式下中止上传: 源文件不兼容(vcodec={(original_probe or {}).get('vcodec')}, pix_fmt={(original_probe or {}).get('pix_fmt')}) 且转码失败。",
                                "warning",
                            )
                            return

                    try:
                        file_size_bytes = os.path.getsize(filename)
                    except Exception:
                        file_size_bytes = 0

                    if file_size_bytes > max_upload_bytes and upload_mode == 'original':
                        downloader.log(
                            f"原码文件过大({file_size_bytes} bytes)，超过 Telegram 分片上限，尝试先转码压缩...",
                            "warning",
                        )
                        try:
                            filename, extra_compat_elapsed, changed = await _run_compatibility_pass(filename)
                            compat_elapsed += extra_compat_elapsed
                            transcoded_for_upload = transcoded_for_upload or changed
                            file_size_bytes = os.path.getsize(filename)
                        except Exception as _oversize_fix_err:
                            downloader.log(f"超限转码尝试失败: {_oversize_fix_err}", "warning")

                    if file_size_bytes > max_upload_bytes:
                        size_gb = file_size_bytes / (1024 * 1024 * 1024)
                        limit_gb = max_upload_bytes / (1024 * 1024 * 1024)
                        await msg.edit(
                            "❌ **上传失败（文件过大）**\n"
                            f"当前文件大小约 `{size_gb:.2f} GB`，超过当前可上传上限约 `{limit_gb:.2f} GB`。\n"
                            "建议切换到较低画质（如 1080p）或启用兼容转码后重试。"
                        )
                        return

                    download_meta = downloader.get_last_download_metadata()
                    source_url = (download_meta.get('source_url') or url or '').strip()
                    source_title = (download_meta.get('title') or '').strip()
                    source_resolution = (download_meta.get('resolution') or '').strip()

                    lines = [
                        "✅ **下载完成**",
                        f"文件: `{os.path.basename(filename)}`",
                    ]
                    if source_url:
                        lines.append(f"原链接: {source_url}")
                    if source_title:
                        lines.append(f"标题: {source_title}")
                    if source_resolution:
                        lines.append(f"分辨率: {source_resolution}")
                    lines.append("正在上传...")

                    await msg.edit("\n".join(lines))

                    upload_caption_parts = ["下载完成"]
                    if source_title:
                        upload_caption_parts.append(f"标题: {source_title}")
                    if source_resolution:
                        upload_caption_parts.append(f"分辨率: {source_resolution}")

                    upload_attributes = None
                    final_probe = None
                    force_document_upload = force_document_mode
                    supports_streaming_upload = not force_document_mode
                    try:
                        final_probe = downloader._probe_media_streams(filename)
                        if final_probe and final_probe.get('video_stream'):
                            probe_w = int(final_probe.get('width') or 0)
                            probe_h = int(final_probe.get('height') or 0)
                            probe_rot = int(final_probe.get('rotation') or 0) % 360
                            if probe_rot in (90, 270):
                                probe_w, probe_h = probe_h, probe_w

                            stream_duration = (final_probe.get('video_stream') or {}).get('duration')
                            duration_seconds = int(round(float(stream_duration or 0))) if stream_duration else 0
                            if duration_seconds <= 0:
                                duration_seconds = 1

                            if probe_w > 0 and probe_h > 0:
                                upload_attributes = [
                                    types.DocumentAttributeVideo(
                                        duration=duration_seconds,
                                        w=probe_w,
                                        h=probe_h,
                                        supports_streaming=True,
                                    )
                                ]

                            if force_document_mode:
                                upload_attributes = None
                            elif not is_streaming_compatible_media(final_probe):
                                force_document_upload = True
                                supports_streaming_upload = False
                                upload_attributes = None
                    except Exception as attr_err:
                        downloader.log(f"Upload video attribute probe failed, fallback to auto attributes: {attr_err}", "warning")

                    if force_document_upload and not force_document_mode:
                        downloader.log(
                            f"检测到原码可能不兼容 Telegram 流媒体播放(vcodec={(final_probe or {}).get('vcodec')}, pix_fmt={(final_probe or {}).get('pix_fmt')})，改为文件发送避免只有声音无画面。",
                            "warning",
                        )
                    elif force_document_mode:
                        downloader.log("当前上传策略为原码文件发送，已强制使用 document 方式上传。", "info")

                    upload_started_at = time.perf_counter()
                    try:
                        await client.send_file(
                            event.chat_id,
                            filename,
                            caption="\n".join(upload_caption_parts),
                            attributes=upload_attributes,
                            force_document=force_document_upload,
                            supports_streaming=supports_streaming_upload,
                            part_size_kb=part_size_kb,
                        )
                    except Exception as upload_err:
                        err_text = str(upload_err)
                        if 'SaveBigFilePartRequest' in err_text or 'number of file parts is invalid' in err_text.lower():
                            size_gb = (os.path.getsize(filename) / (1024 * 1024 * 1024)) if os.path.exists(filename) else 0
                            await msg.edit(
                                "❌ **上传失败（文件过大）**\n"
                                f"当前文件大小约 `{size_gb:.2f} GB`，超出当前分片上传限制。\n"
                                "请降低下载画质（建议 1080p）后重试。"
                            )
                            downloader.log(f"上传失败(分片上限): {upload_err}", "warning")
                            return
                        raise
                    upload_elapsed = time.perf_counter() - upload_started_at

                    # Keep original source file; only cleanup uploaded transcode artifact.
                    if (
                        transcoded_for_upload
                        and os.path.abspath(filename) != os.path.abspath(original_download_path)
                        and os.path.exists(filename)
                    ):
                        try:
                            os.remove(filename)
                            downloader.log(
                                f"上传成功后已清理转码文件: {os.path.basename(filename)}",
                                "info",
                            )
                        except Exception as cleanup_err:
                            downloader.log(f"转码文件清理失败: {cleanup_err}", "warning")

                    downloader.log(
                        f"任务耗时统计: download={download_elapsed:.2f}s, compat={compat_elapsed:.2f}s, "
                        f"upload={upload_elapsed:.2f}s, upload_mode={upload_mode}, transcoded={transcoded_for_upload}, "
                        f"file={os.path.basename(filename)}",
                        "info",
                    )

                    # Cleanup: Delete status message AND user's original message
                    try:
                        await msg.delete()
                        await event.delete()
                    except:
                        pass
                else:
                    last_error = (downloader.get_last_error() or "").lower()
                    if 'ffmpeg' in last_error:
                        await msg.edit(
                            "❌ **下载失败（缺少 FFmpeg）**\n"
                            "你当前已启用“最高画质”策略，但服务器未安装 FFmpeg，无法合并最佳音视频流。\n"
                            "请先安装 FFmpeg 后重试。"
                        )
                        return

                    cookies_required = downloader.is_youtube_cookies_required_error()
                    ip_or_client_limited = downloader.is_youtube_client_or_ip_limited_error()
                    strong_cookies = has_strong_youtube_cookies(config)

                    # If we already have strong cookies but still hit Sign-in style failures,
                    # it is commonly an IP/client risk-control issue rather than missing cookies.
                    if ip_or_client_limited or (cookies_required and strong_cookies):
                        js_runtime_hint = ""
                        if downloader.is_js_runtime_missing():
                            js_runtime_hint = "\n4. 当前服务器缺少 JS 运行时（deno/node），先安装后再试"
                        await msg.edit(
                            "❌ **下载失败（YouTube 风控/出口受限）**\n"
                            "当前 Cookies 已检测到登录态，但仍被 YouTube 限制。\n"
                            "建议依次尝试：\n"
                            "1. 临时关闭代理后重试\n"
                            "2. 更换网络出口/IP（优先住宅网络）\n"
                            "3. 使用该出口网络重新登录并导出 Cookies"
                            f"{js_runtime_hint}"
                        )
                        return

                    if cookies_required:
                        await msg.edit(
                            "❌ **下载失败（YouTube 需要登录态）**\n"
                            "请先更新 Cookies：\n"
                            "1. 发送 `/start`\n"
                            "2. 点击 `🍪 更新 Cookies`\n"
                            "3. 上传可用的 `cookies.txt`（含 YouTube 登录态）\n"
                            "4. 重新发送链接"
                        )
                        return

                    reason = downloader.get_last_error() or "未拿到可下载的视频流，可能需要代理或有效 Cookies。"
                    await msg.edit(f"❌ **下载失败**\n原因: `{reason}`")
            except Exception as e:
                downloader.log(f"Bot processing error: {e}", "error")
                await msg.edit(f"发生错误: {str(e)}")
        else:
            await event.reply("请发送有效的视频链接（支持 YouTube/Instagram 等）。")

    # Ensure existing session matches the current bot token.
    # Otherwise Telethon may ignore the provided bot token and keep old auth.
    expected_bot_id = _extract_bot_id_from_token(bot_token)
    try:
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            current_id = int(getattr(me, 'id', 0) or 0)
            is_bot = bool(getattr(me, 'bot', False))

            must_reset = False
            reset_reason = ""
            if not is_bot:
                must_reset = True
                reset_reason = "session is bound to a user account"
            elif expected_bot_id and current_id and current_id != expected_bot_id:
                must_reset = True
                reset_reason = f"session bot id {current_id} != token bot id {expected_bot_id}"

            if must_reset:
                downloader.log(
                    f"Detected stale bot session ({reset_reason}), rebuilding bot session...",
                    "warning"
                )
                await client.disconnect()
                for suffix in ('.session', '.session-journal', '.session-shm', '.session-wal'):
                    try:
                        p = session_path + suffix
                        if os.path.exists(p):
                            os.remove(p)
                    except Exception as e:
                        downloader.log(f"Failed to remove old session file: {e}", "warning")
                client = TelegramClient(session_path, api_id, api_hash)
        else:
            await client.disconnect()
    except Exception as e:
        downloader.log(f"Session precheck failed, continue with fresh login attempt: {e}", "warning")
        try:
            await client.disconnect()
        except Exception:
            pass

    downloader.log("Starting Bot...")
    # Retry logic for database lock
    max_retries = 10
    for i in range(max_retries):
        try:
            await client.start(bot_token=bot_token)
            downloader.log("✓ Bot started successfully with WAL mode enabled")
            break
        except Exception as e:
            if "database is locked" in str(e).lower() and i < max_retries - 1:
                wait_time = min(3 * (i + 1), 15)  # Progressive backoff, max 15s
                downloader.log(f"Database locked, retrying {i+1}/{max_retries} in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                downloader.log(f"Failed to start bot after {max_retries} retries: {e}", "error")
                raise e
    
    # Set Bot Menu Commands
    try:
        await client(functions.bots.SetBotCommandsRequest(
            commands=[
                types.BotCommand("start", "开始菜单"),
                types.BotCommand("cookies_status", "检查 Cookies 状态"),
                types.BotCommand("cookies_check", "深度检查 Cookies 可用性"),
                types.BotCommand("movie", "影巢资源搜寻"),
                types.BotCommand("help", "使用帮助"),
                types.BotCommand("status", "检查服务状态"),
            ],
            scope=types.BotCommandScopeDefault(),
            lang_code=""
        ))
        downloader.log("Bot commands menu set successfully.")
    except Exception as e:
        downloader.log(f"Failed to set bot commands: {e}", "error")

    downloader.log("Bot is running...")
    await client.run_until_disconnected()

if __name__ == '__main__':
    setup_logging()
    asyncio.run(main())
