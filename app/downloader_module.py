import yt_dlp
import os
import asyncio
import logging
import json
from concurrent.futures import ThreadPoolExecutor

# Global logging configuration setup
def setup_logging():
    debug_mode = load_debug_mode()
    logging.basicConfig(
        level=logging.DEBUG if debug_mode else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True
    )

logger = logging.getLogger(__name__)

CONFIG_FILE = 'config/config.json'

def load_debug_mode():
    """从配置文件加载 debug_mode 设置"""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config.get('debug_mode', False)
    except:
        pass
    return False

class Downloader:
    def __init__(self):
        self.download_logs = []
        self.executor = ThreadPoolExecutor(max_workers=2) # Limit concurrent downloads

    def _log_hook(self, d):
        if d['status'] == 'downloading':
            try:
                p = d.get('_percent_str', 'N/A').replace('%','')
                self.log(f"正在下载: {p}% - 剩余 {d.get('_eta_str', 'N/A')}", "info")
            except:
                pass
        elif d['status'] == 'finished':
            self.log(f"下载完成: {d['filename']}", "success")
        elif d['status'] == 'error':
             self.log(f"错误: {d}", "error")

    def log(self, message, level="info"):
        """Adds a log message to the buffer."""
        # 如果是 debug 级别的日志，检查 debug_mode 是否开启
        if level == "debug" and not load_debug_mode():
            return  # debug_mode 关闭时不记录 debug 日志
        
        # Clean color codes if any (yt-dlp might output them)
        import re
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        clean_message = ansi_escape.sub('', message)
        
        import sys
        
        entry = f"[{level.upper()}] {clean_message}"
        # Use logger.info/error based on level or just print to stderr
        if level == "error":
            logger.error(clean_message)
        elif level == "warning":
            logger.warning(clean_message)
        elif level == "debug":
            logger.debug(clean_message)
        else:
            logger.info(clean_message)

        # Force flush stdout/stderr just in case
        sys.stderr.flush()
        
        self.download_logs.append(entry)
        if len(self.download_logs) > 200:
            self.download_logs.pop(0)

    def clear_logs(self):
        """Clears the log buffer."""
        self.download_logs = []

    def _prepare_cookies(self, cookies_file):
        """
        Check if cookies file is JSON or Raw Header string and convert to Netscape if needed.
        Returns path to a usable cookie file (original or temporary).
        """
        import json
        import time
        
        try:
            with open(cookies_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                
            # Case 1: JSON Format
            if content.startswith('[') or content.startswith('{'):
                try:
                    cookies_data = json.loads(content)
                except json.JSONDecodeError:
                    pass # Not valid JSON, fall through
                else:
                    # If it's a dict (Selenium/EditThisCookie format sometimes), wrap in list if needed or extract
                    if isinstance(cookies_data, dict):
                        if 'cookies' in cookies_data:
                            cookies_data = cookies_data['cookies']
                        else:
                            cookies_data = [cookies_data] 

                    if isinstance(cookies_data, list):
                        # Convert to Netscape format
                        netscape_lines = ["# Netscape HTTP Cookie File"]
                        for cookie in cookies_data:
                            domain = cookie.get('domain', '')
                            flag = 'TRUE' if domain.startswith('.') else 'FALSE'
                            path = cookie.get('path', '/')
                            secure = 'TRUE' if cookie.get('secure', False) else 'FALSE'
                            expires = cookie.get('expiry', cookie.get('expirationDate', 0))
                            expires = int(expires) if expires else 0
                            name = cookie.get('name', '')
                            value = cookie.get('value', '')
                            netscape_lines.append(f"{domain}\t{flag}\t{path}\t{secure}\t{expires}\t{name}\t{value}")
                        
                        return self._write_temp_cookies(cookies_file, netscape_lines)

            # Case 2: Raw Cookie Header String (e.g. "key=value; key2=value2")
            # Heuristic: Check for semicolons and equals, but no tabs (which Netscape has)
            if ';' in content and '=' in content and '\t' not in content:
                # Naive parsing of "Cookie: name=value; name2=value2"
                # We need a domain to create a Netscape file. We'll use a catch-all or specific ones if known.
                # Since we don't know the exact domain, we can try adding for common social media domains or use .instagram.com as a fallback if seemingly related.
                
                # Check keywords or assume general compatibility
                target_domains = ['.instagram.com', '.youtube.com', '.google.com']
                netscape_lines = ["# Netscape HTTP Cookie File"]
                
                # Remove "Cookie: " prefix if present
                if content.lower().startswith("cookie:"):
                    content = content[7:].strip()
                
                # Split by semicolon
                pairs = [p.strip() for p in content.split(';') if p.strip()]
                
                timestamp = int(time.time()) + 31536000 # Valid for 1 year
                
                for pair in pairs:
                    if '=' not in pair: continue
                    name, value = pair.split('=', 1)
                    name = name.strip()
                    value = value.strip()
                    
                    # We add entries for all likely domains to ensure coverage
                    for domain in target_domains:
                        # domain flag path secure expiration name value
                        netscape_lines.append(f"{domain}\tTRUE\t/\tTRUE\t{timestamp}\t{name}\t{value}")
                
                self.log("Detected raw Cookie header string. Converted to Netscape format.", "info")
                return self._write_temp_cookies(cookies_file, netscape_lines)

            return cookies_file # Looks like Netscape (tabs present) or unknown
        except Exception as e:
            self.log(f"检查 Cookies 格式时出错: {e}", "warning")
            return cookies_file

    def _write_temp_cookies(self, original_file, lines):
        import time
        temp_dir = os.path.dirname(original_file)
        temp_name = f"cookies_netscape_{int(time.time())}.txt"
        temp_path = os.path.join(temp_dir, temp_name)
        with open(temp_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        self.log(f"已生成临时 Cookies 文件: {temp_path}", "info")
        return temp_path

    def download_video(self, url, output_dir, cookies_file=None, cookies_from_browser=None, proxy=None):
        """
        Downloads a video using yt-dlp synchronously. 
        Should be run in a separate thread/executor.
        Returns the path of the downloaded file (or one of them).
        """
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
            except Exception as e:
                self.log(f"无法创建目录 {output_dir}: {e}", "error")
                return None
        
        import shutil
        has_ffmpeg = shutil.which('ffmpeg') is not None

        # Absolute compatibility format for Telegram (H.264 + AAC)
        # We explicitly prefer avc1 (H.264) and mp4a (AAC)
        format_spec = 'bestvideo[vcodec^=avc1]+bestaudio[acodec^=mp4a]/best[vcodec^=avc1]/best'
        merge_format = 'mp4'

        if not has_ffmpeg:
            self.log("未找到 FFmpeg。降级为单文件格式以确保音画同步。", "warning")
            format_spec = 'best' 
            merge_format = None

        # Base options
        base_ydl_opts = {
            'outtmpl': os.path.join(output_dir, '%(title)s [%(id)s].%(ext)s'),
            'progress_hooks': [self._log_hook],
            'logger': self, 
            'restrictfilenames': True, 
            'nocheckcertificate': True, 
            'ignoreerrors': True,
            'format': format_spec,
            'merge_output_format': merge_format,
            'postprocessors': [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }],
            'http_headers': {
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            },
        }

        if proxy:
            base_ydl_opts['proxy'] = proxy
            self.log(f"正在配置代理: {proxy}", "info")
        else:
            self.log("未配置代理，尝试直连。", "warning")

        # Handle Cookies
        final_cookies_path = None
        if cookies_file and os.path.exists(cookies_file):
            final_cookies_path = self._prepare_cookies(cookies_file)
            base_ydl_opts['cookiefile'] = final_cookies_path
            self.log(f"使用 Cookies: {os.path.basename(cookies_file)}", "info")
        
        if cookies_from_browser:
            base_ydl_opts['cookiesfrombrowser'] = (cookies_from_browser,)

        # Define client combinations to try for YouTube
        youtube_client_combos = [
            ['ios'],
            ['web_embedded', 'ios'],
            ['android'],
            ['mweb', 'ios'],
            ['tv', 'web_creator']
        ]

        try:
            self.log(f"开始分析链接 {url}...", "info")
            
            # Non-YouTube
            if "youtube.com" not in url and "youtu.be" not in url:
                if "instagram.com" in url or "tiktok.com" in url:
                    base_ydl_opts.setdefault('http_headers', {})['Referer'] = 'https://www.instagram.com/'
                    base_ydl_opts.setdefault('http_headers', {})['User-Agent'] = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1'
                
                with yt_dlp.YoutubeDL(base_ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    return self._process_info(ydl, info)

            # YouTube
            last_error = None
            for combo in youtube_client_combos:
                self.log(f"尝试组合: {combo}", "info")
                # Shallow copy is safer for objects like logger
                current_opts = base_ydl_opts.copy()
                current_opts['extractor_args'] = {
                    'youtube': {
                        'player_client': combo,
                        'player_skip': ['webpage', 'configs']
                    }
                }
                
                try:
                    with yt_dlp.YoutubeDL(current_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                        if info:
                            return self._process_info(ydl, info)
                except Exception as e:
                    last_error = e
                    if "Sign in to confirm" in str(e):
                        self.log(f"组合 {combo} 被拦截 (Sign in)。", "warning")
                        continue
                    else:
                        self.log(f"组合 {combo} 失败: {e}", "warning")
                        continue
            
            if last_error:
                raise last_error
            return None

        except Exception as e:
            self.log(f"下载最终失败: {e}", "error")
            return None
        finally:
            if final_cookies_path and final_cookies_path != cookies_file and os.path.exists(final_cookies_path):
                try: os.remove(final_cookies_path)
                except: pass

    def _process_info(self, ydl, info):
        if info is None:
            return None
        filename = ydl.prepare_filename(info)
        if os.path.exists(filename):
            return filename
        base, ext = os.path.splitext(filename)
        if ext != '.mp4':
            mp4_filename = base + '.mp4'
            if os.path.exists(mp4_filename):
                return mp4_filename
        return filename

    # Redirect methods for yt-dlp logger interface
    def debug(self, msg):
        # Filter out too verbose debug info if needed
        if not msg.startswith('[debug] '):
             self.log(msg, "debug")
    
    def info(self, msg):
        self.log(msg, "info")

    def warning(self, msg):
        self.log(msg, "warning")

    def error(self, msg):
        self.log(msg, "error")

    async def download_task(self, url, output_dir, cookies_file=None, cookies_from_browser=None, proxy=None):
        """Async wrapper for download_video."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self.download_video, url, output_dir, cookies_file, cookies_from_browser, proxy)

# Global instance
downloader = Downloader()
