import os
import json
import logging
import asyncio
import sqlite3
from telethon import TelegramClient, events, functions, types
from telethon.sessions import SQLiteSession
import sys
from dotenv import load_dotenv

# Add current directory to sys.path to find downloader_module when running from app directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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

# Load environment variables
load_dotenv(os.path.join(CONFIG_DIR, '.env'))

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}

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
    config = load_config()
    
    # Priority: Env Vars > Config
    api_id = os.environ.get('TELEGRAM_API_ID') or config.get('telegram', {}).get('api_id')
    api_hash = os.environ.get('TELEGRAM_API_HASH') or config.get('telegram', {}).get('api_hash')
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN') or config.get('bot', {}).get('token')

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

    # --- Command Handlers ---
    @client.on(events.NewMessage(pattern='/start'))
    async def start_handler(event):
        welcome_text = (
            "👋 **你好！我是您的文件监控助手。**\n\n"
            "我可以帮您下载视频（支持 YouTube, Instagram 等）。\n\n"
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
            "我可以帮您下载视频（支持 YouTube, Instagram 等）。\n\n"
            "👇 **点击下方按钮开始交互**"
        )
        await client.send_message(chat_id, welcome_text, buttons=[
            [types.KeyboardButtonCallback("🍪 更新 Cookies", data=b'update_cookies')],
            [types.KeyboardButtonCallback("🗑 清除记录", data=b'clear_history'), types.KeyboardButtonCallback("❓ 使用帮助", data=b'help_menu')]
        ])

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
            "3. **搜寻影视**: 发送 `/movie <剧名>` (例如: `/movie 金玉满堂`)\n"
        )
        await event.respond(help_text)

    @client.on(events.NewMessage(pattern='/cancel'))
    async def cancel_handler(event):
        sender_id = event.sender_id
        if sender_id in waiting_for_cookies:
            waiting_for_cookies.remove(sender_id)
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
            "3. **搜寻影视**: 发送 `/movie <剧名>`\n"
        )
        await event.reply(help_text)


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
        
        # Ignore commands handled by other handlers
        if event.text.startswith('/'):
            return

        sender_id = event.sender_id
        text = event.text.strip()
        
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
        if event.file:
            await event.reply("请先通过 /start 菜单点击 '🍪 更新 Cookies' 按钮后再发送文件。")
            return

        if text.startswith('http://') or text.startswith('https://'):
            url = text
            # sender = await event.get_sender() # Might fail if user restricted privacy? just use sender_id
            downloader.log(f"Received URL from {sender_id}: {url}")
            # await event.reply(f"📥 **已加入下载队列**\n链接: {url}")
            
            msg = await event.reply("收到链接，正在准备下载...")
            
            default_path = config.get('downloader', {}).get('default_path', os.path.join(BASE_DIR, 'downloads'))
            
            # Use downloader module to download
            # Since download_video is blocking, run in executor
            loop = asyncio.get_running_loop()
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

                filename = await loop.run_in_executor(downloader.executor, downloader.download_video, url, default_path, cookies_file, cookies_from_browser, proxy_url)
                
                if filename and os.path.exists(filename):
                     await msg.edit(f"✅ **下载完成**\n`{os.path.basename(filename)}`\n正在上传...")
                     await client.send_file(event.chat_id, filename, caption="下载完成")
                     
                     # Cleanup: Delete status message AND user's original message
                     try:
                         await msg.delete()
                         await event.delete()
                     except:
                         pass
                else:
                    await msg.edit("❌ **下载失败**\n请检查链接或日志。")
            except Exception as e:
                downloader.log(f"Bot processing error: {e}", "error")
                await msg.edit(f"发生错误: {str(e)}")

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
