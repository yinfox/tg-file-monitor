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
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, send_file
from telethon.sync import TelegramClient # Using sync version for simpler Flask integration
from telethon.errors import SessionPasswordNeededError, PhoneCodeExpiredError, PhoneCodeInvalidError
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from dotenv import load_dotenv

# Add current directory to path so we can import modules from app/
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from downloader_module import downloader

app = Flask(__name__)
# Stable secret key for v0.4.6
app.secret_key = "tg-file-monitor-v0.4.6-rapid-upload-key"

VERSION = "0.4.26"

# --- Configuration Management ---
CONFIG_DIR = 'config' # Define the config directory
CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')
LOG_DIR = os.path.join(CONFIG_DIR, 'logs')
DOWNLOAD_RISK_STATS_FILE = os.path.join(CONFIG_DIR, 'download_risk_stats.json')

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
            "min_free_space_gb": 5
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

    def _append_to_log_file(self, entry):
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            with self._log_file_lock:
                with open(self.log_file_path, 'a', encoding='utf-8') as f:
                    f.write(entry + "\n")
        except Exception:
            pass

    def get_full_log_lines(self):
        """读取日志，进度行只保留最新一条"""
        lines = []
        try:
            if os.path.exists(self.log_file_path):
                with open(self.log_file_path, 'r', encoding='utf-8') as f:
                    lines = [line.rstrip('\n') for line in f.readlines()]
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

def start_monitor_process(): return tg_monitor_mgr.start()
def stop_monitor_process():
    if tg_monitor_mgr.stop():
        flash("Telegram 监控程序已停止。", "info")
        return True
    return False
def get_monitor_status(): return tg_monitor_mgr.status()

def start_file_monitor_process(): return file_monitor_mgr.start()
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
            monitor_types = request.form.getlist('monitor_types')
            if not monitor_types:
                monitor_types = ['video']  # 默认监控视频
            
            # 调试日志
            print(f"DEBUG: channel_id={channel_id}, old_channel_id='{old_channel_id}', keep_video_message={keep_video_message}")
            
            try:
                channel_id_int = int(channel_id)
                target_user_ids_list = [uid.strip() for uid in target_user_ids_restricted.split(',') if uid.strip()]

                # 如果不是转发模式，检查下载目录必须存在
                if not keep_video_message and (not download_directory or not os.path.isdir(download_directory)):
                    # 如果 monitor_types 只包含 'text'，则不需要下载目录
                    if monitor_types != ['text']:
                        flash(f"下载模式必须指定有效的下载目录。", "error")
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
            save_config(config)

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
        if get_monitor_status() == "运行中":
            stop_monitor_process()
            start_monitor_process()

        return redirect(url_for('manage_config'))

    return render_template('config.html', config=config)

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
    log_lines = tg_monitor_mgr.get_full_log_lines()
    colored_log_lines = [colorize_log_line(line) for line in log_lines]
    log_output = "\n".join(reversed(colored_log_lines)) if colored_log_lines else "暂无日志。"
    return render_template('log.html', log_output=log_output)

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
            stop_file_monitor_process()
            start_file_monitor_process()

        return redirect(url_for('manage_file_config'))

    return render_template('file_config.html', config=config, download_risk_stats=download_risk_stats)

@app.route('/file_monitor_action', methods=['POST'])
@login_required
def file_monitor_action():
    action = request.form['action']
    if action == 'start':
        start_file_monitor_process()
    elif action == 'stop':
        stop_file_monitor_process()
    
    return redirect(url_for('index'))

@app.route('/file_monitor_log')
@login_required
def file_monitor_log():
    log_lines = file_monitor_mgr.get_full_log_lines()
    colored_log_lines = [colorize_log_line(line) for line in log_lines]
    log_output = "\n".join(reversed(colored_log_lines)) if colored_log_lines else "暂无日志。"
    return render_template('file_monitor_log.html', log_output=log_output)

@app.route('/clear_monitor_log', methods=['POST'])
@login_required
def clear_monitor_log():
    tg_monitor_mgr.clear_logs()
    flash("Telegram 监控日志已清除。", "info")
    return redirect(url_for('monitor_log'))

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
    if not default_path:
        default_path = os.path.join(project_root, 'downloads')
    return render_template('downloader.html', default_path=default_path)

@app.route('/downloader/log')
@login_required
def downloader_log():
    return jsonify({"logs": downloader.download_logs})

@app.route('/downloader/clear_log', methods=['POST'])
@login_required
def downloader_clear_log():
    downloader.clear_logs()
    return jsonify({"success": True})

@app.route('/api/download', methods=['POST'])
@login_required
async def api_download():
    url = request.form.get('url')
    output_dir = request.form.get('output_dir')
    browser = request.form.get('browser')
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

    if not output_dir:
        return jsonify({"error": "保存目录不能为空"}), 400

    output_dir = os.path.abspath(os.path.expanduser(output_dir))

    try:
        os.makedirs(output_dir, exist_ok=True)
        write_test = os.path.join(output_dir, '.write_test')
        with open(write_test, 'w', encoding='utf-8') as f:
            f.write('ok')
        os.remove(write_test)
    except Exception as e:
        return jsonify({"error": f"保存目录不可写: {e}"}), 400

    config = load_config()
    downloader_cfg = config.setdefault('downloader', {})
    downloader_cfg['default_path'] = output_dir
    save_config(config)

    return jsonify({"success": True, "default_path": output_dir})

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

    app.run(host="0.0.0.0", port=5001, debug=debug_mode, use_reloader=debug_mode)
