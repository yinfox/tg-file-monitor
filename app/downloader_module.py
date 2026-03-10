import yt_dlp
import os
import asyncio
import logging
import json
import subprocess
import shutil
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
        self.last_error_message = ""
        self.js_runtime_missing = False
        self.last_cookies_supplied = False
        self.ffmpeg_path = None
        self.ffprobe_path = None

    def _ensure_ffmpeg_tools(self):
        import shutil

        if self.ffmpeg_path and os.path.exists(self.ffmpeg_path):
            return self.ffmpeg_path, self.ffprobe_path

        ffmpeg_path = shutil.which('ffmpeg')
        ffprobe_path = shutil.which('ffprobe')

        if not ffmpeg_path:
            try:
                import imageio_ffmpeg
                ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
            except Exception:
                ffmpeg_path = None

        if ffmpeg_path and not ffprobe_path:
            try:
                sibling_ffprobe = os.path.join(os.path.dirname(ffmpeg_path), 'ffprobe')
                if os.path.exists(sibling_ffprobe):
                    ffprobe_path = sibling_ffprobe
            except Exception:
                pass

        self.ffmpeg_path = ffmpeg_path
        self.ffprobe_path = ffprobe_path
        return ffmpeg_path, ffprobe_path

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
        if 'no supported javascript runtime could be found' in clean_message.lower():
            self.js_runtime_missing = True
        # Use logger.info/error based on level or just print to stderr
        if level == "error":
            logger.error(clean_message)
        elif level == "warning":
            logger.warning(clean_message)
        elif level == "debug":
            logger.debug(clean_message)
        else:
            logger.info(clean_message)

        if level == "error":
            self.last_error_message = clean_message

        # Force flush stdout/stderr just in case
        sys.stderr.flush()
        
        self.download_logs.append(entry)
        if len(self.download_logs) > 200:
            self.download_logs.pop(0)

    def clear_logs(self):
        """Clears the log buffer."""
        self.download_logs = []
        self.js_runtime_missing = False
        self.last_cookies_supplied = False

    def get_last_error(self):
        return self.last_error_message

    def is_youtube_cookies_required_error(self):
        msg = (self.last_error_message or '').lower()
        return (
            'sign in to confirm you\'re not a bot' in msg
            or 'use --cookies-from-browser or --cookies' in msg
            or 'cookies for the authentication' in msg
            or 'cookies 可能已失效' in msg
            or 'cookies may be expired' in msg
        )

    def is_youtube_client_or_ip_limited_error(self):
        msg = (self.last_error_message or '').lower()
        return (
            'requested format is not available' in msg
            or 'only images are available for download' in msg
            or 'http error 403' in msg
            or 'error code: 152' in msg
            or 'po token' in msg
        )

    def is_js_runtime_missing(self):
        return bool(self.js_runtime_missing)

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

        # Reset per-run state regardless of output_dir existence.
        self.last_error_message = ""
        self.js_runtime_missing = False
        self.last_cookies_supplied = False
        
        ffmpeg_path, ffprobe_path = self._ensure_ffmpeg_tools()
        if ffmpeg_path:
            self.log(f"使用 FFmpeg: {ffmpeg_path}", "info")
        if ffprobe_path:
            self.log(f"使用 FFprobe: {ffprobe_path}", "debug")

        has_ffmpeg = bool(ffmpeg_path)

        # Configure JS runtimes explicitly for yt-dlp EJS path (deno preferred, node fallback).
        js_runtimes_config = {}
        deno_path = shutil.which('deno') or os.path.expanduser('~/.deno/bin/deno')
        if deno_path and os.path.exists(deno_path):
            js_runtimes_config['deno'] = {'path': deno_path}
        node_path = shutil.which('node')
        if node_path:
            js_runtimes_config['node'] = {'path': node_path}

        # Quality profiles:
        # - fast_compatible: prioritize Telegram compatibility and upload speed.
        # - balanced_hd: higher quality with compatibility fallback.
        # - ultra_quality: quality-first with compatibility still preferred first.
        quality_mode = self._get_quality_mode_from_config()
        format_spec = self._build_format_spec(quality_mode)
        self.log(f"下载画质模式: {quality_mode}", "info")
        merge_format = 'mp4'

        if not has_ffmpeg:
            self.log(
                "未检测到 FFmpeg，无法保证最高画质。请先安装 FFmpeg 后再下载。",
                "error",
            )
            return None

        # Base options
        base_ydl_opts = {
            'outtmpl': os.path.join(output_dir, '%(title)s [%(id)s].%(ext)s'),
            'progress_hooks': [self._log_hook],
            'logger': self, 
            'restrictfilenames': True, 
            'nocheckcertificate': True, 
            'ignoreerrors': False,
            'format': format_spec,
            'http_headers': {
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            },
            'noplaylist': True,
            'remote_components': {'ejs:github', 'ejs:npm'},
        }

        # Pin ffmpeg location (system or bundled) to guarantee merge capability.
        base_ydl_opts['ffmpeg_location'] = ffmpeg_path
        if js_runtimes_config:
            base_ydl_opts['js_runtimes'] = js_runtimes_config
            runtime_desc = ', '.join(
                f"{name}:{cfg.get('path')}" for name, cfg in js_runtimes_config.items()
            )
            self.log(f"启用 JS runtime: {runtime_desc}", "info")

        # Only enable ffmpeg-dependent output merge/conversion when ffmpeg exists.
        if merge_format:
            base_ydl_opts['merge_output_format'] = merge_format
            # Keep highest quality by merging/remuxing only; avoid extra re-encode convertor.

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
            self.last_cookies_supplied = True

        if cookies_from_browser:
            base_ydl_opts['cookiesfrombrowser'] = (cookies_from_browser,)
            self.last_cookies_supplied = True

        # Define client combinations to try for YouTube
        youtube_client_combos = [
            None,
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
                self.log(f"尝试组合: {combo or 'default'}", "info")
                # Shallow copy is safer for objects like logger
                current_opts = base_ydl_opts.copy()
                if combo:
                    # Keep extractor args conservative: forcing player_skip(webpage,configs)
                    # may require extra account signals (e.g. data_sync_id) and reduce robustness.
                    current_opts['extractor_args'] = {
                        'youtube': {
                            'player_client': combo,
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
                        self.log(f"组合 {combo} 失败: {type(e).__name__}: {e!r}", "warning")
                        continue

            # Fallback 1: retry once with relaxed YouTube defaults to avoid player-client/PO-token issues.
            if last_error:
                self.log("YouTube 首轮失败，尝试默认客户端重试（保持最高画质策略）...", "warning")
                relaxed_opts = base_ydl_opts.copy()
                try:
                    with yt_dlp.YoutubeDL(relaxed_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                        if info:
                            return self._process_info(ydl, info)
                except Exception as e:
                    last_error = e
                    self.log(f"默认客户端重试失败: {type(e).__name__}: {e!r}", "warning")

            # Fallback 2: retry without proxy/cookies to avoid bad cookie/proxy failures.
            if last_error and (proxy or cookies_file or cookies_from_browser):
                self.log("YouTube 首轮失败，降级重试（不使用代理/Cookies）...", "warning")
                fallback_opts = base_ydl_opts.copy()
                fallback_opts.pop('proxy', None)
                fallback_opts.pop('cookiefile', None)
                fallback_opts.pop('cookiesfrombrowser', None)

                try:
                    with yt_dlp.YoutubeDL(fallback_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                        if info:
                            return self._process_info(ydl, info)
                except Exception as e:
                    last_error = e
                    self.log(f"降级直连失败: {type(e).__name__}: {e!r}", "warning")
            
            if last_error:
                raise last_error
            return None

        except Exception as e:
            if (
                "Sign in to confirm you're not a bot" in str(e)
                or "Use --cookies-from-browser or --cookies" in str(e)
            ):
                if self.last_cookies_supplied:
                    self.log(
                        "YouTube 仍要求登录验证：已检测到 Cookies，但 Cookies 可能已失效或当前出口/IP 被风控。"
                        "请重新导出最新 Cookies 后重试，必要时更换网络出口。",
                        "error"
                    )
                else:
                    self.log(
                        "YouTube 需要登录态 Cookies 才能继续下载。请先更新 cookies.txt 后重试。",
                        "error"
                    )
            else:
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

    def _probe_media_streams(self, file_path):
        """Use ffprobe to inspect media streams for Telegram compatibility decisions."""
        _, ffprobe_path = self._ensure_ffmpeg_tools()
        ffprobe = ffprobe_path or 'ffprobe'
        cmd = [
            ffprobe,
            '-v', 'error',
            '-print_format', 'json',
            '-show_streams',
            '-show_format',
            file_path,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout or '{}')
            streams = data.get('streams') or []

            video_stream = None
            audio_stream = None
            for s in streams:
                codec_type = (s.get('codec_type') or '').lower()
                if codec_type == 'video' and video_stream is None:
                    video_stream = s
                elif codec_type == 'audio' and audio_stream is None:
                    audio_stream = s

            return {
                'video_stream': video_stream,
                'audio_stream': audio_stream,
                'vcodec': (video_stream or {}).get('codec_name'),
                'pix_fmt': (video_stream or {}).get('pix_fmt'),
                'sar': (video_stream or {}).get('sample_aspect_ratio'),
                'acodec': (audio_stream or {}).get('codec_name'),
            }
        except Exception as e:
            self.log(f"ffprobe 分析失败，跳过兼容性转码: {e}", "warning")
            return None

    def _maybe_make_telegram_compatible(self, file_path):
        """Transcode to Telegram-friendly H.264/AAC MP4 when stream codec is likely incompatible."""
        if not file_path or not os.path.exists(file_path):
            return file_path

        probe = self._probe_media_streams(file_path)
        if not probe:
            return file_path

        video_stream = probe.get('video_stream')
        audio_stream = probe.get('audio_stream')
        vcodec = (probe.get('vcodec') or '').lower()
        pix_fmt = (probe.get('pix_fmt') or '').lower()
        sar = str(probe.get('sar') or '').strip()

        if not video_stream:
            self.log("检测到文件无视频流，无法修复为可播放视频。", "warning")
            return file_path

        # Telegram clients are most stable with H.264 + yuv420p in MP4.
        sar_is_non_square = bool(sar) and sar not in ('1:1', '0:1', 'N/A', 'unknown')
        needs_transcode = (vcodec != 'h264') or (pix_fmt not in ('yuv420p', 'yuvj420p')) or sar_is_non_square
        if not needs_transcode:
            return file_path

        base, _ = os.path.splitext(file_path)
        fixed_path = base + '.tgfix.mp4'

        cmd = [
            self.ffmpeg_path or 'ffmpeg', '-y',
            '-i', file_path,
            '-map', '0:v:0',
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-crf', '20',
            # Preserve displayed aspect ratio and normalize to square pixels for Telegram players.
            '-vf', "scale='trunc(iw*sar/2)*2':'trunc(ih/2)*2',setsar=1",
            '-pix_fmt', 'yuv420p',
            '-movflags', '+faststart',
        ]

        if audio_stream:
            cmd.extend(['-map', '0:a:0', '-c:a', 'aac', '-b:a', '192k'])
        else:
            cmd.extend(['-an'])

        cmd.append(fixed_path)

        try:
            self.log(
                f"检测到视频兼容性风险(vcodec={vcodec or 'unknown'}, pix_fmt={pix_fmt or 'unknown'}, sar={sar or 'unknown'})，开始转码为 Telegram 兼容格式...",
                "warning"
            )
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            if os.path.exists(fixed_path) and os.path.getsize(fixed_path) > 0:
                self.log(f"兼容性转码完成: {os.path.basename(fixed_path)}", "success")
                return fixed_path
            self.log("兼容性转码未产出有效文件，继续使用原文件。", "warning")
            return file_path
        except Exception as e:
            self.log(f"兼容性转码失败，继续使用原文件: {e}", "warning")
            return file_path

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

    def _get_quality_mode_from_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                mode = ((cfg.get('downloader', {}) or {}).get('quality_mode') or '').strip()
                if mode in {'fast_compatible', 'balanced_hd', 'ultra_quality'}:
                    return mode
        except Exception:
            pass
        return 'balanced_hd'

    def _build_format_spec(self, quality_mode):
        if quality_mode == 'fast_compatible':
            return (
                'bestvideo[height<=1080][vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a]/'
                'bestvideo[height<=1080][vcodec^=avc1]+bestaudio[ext=m4a]/'
                'best[height<=1080][ext=mp4]/best[height<=1080]'
            )

        if quality_mode == 'ultra_quality':
            return (
                'bestvideo[height<=4320][vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a]/'
                'bestvideo[height<=4320][vcodec^=avc1]+bestaudio[ext=m4a]/'
                'bestvideo[height<=4320]+bestaudio/best[height<=4320]/best'
            )

        # balanced_hd
        return (
            'bestvideo[height<=2160][vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a]/'
            'bestvideo[height<=2160][vcodec^=avc1]+bestaudio[ext=m4a]/'
            'bestvideo[height<=2160]+bestaudio/best[height<=2160]/best'
        )

    async def download_task(self, url, output_dir, cookies_file=None, cookies_from_browser=None, proxy=None):
        """Async wrapper for download_video."""
        loop = asyncio.get_running_loop()
        downloaded = await loop.run_in_executor(
            self.executor,
            self.download_video,
            url,
            output_dir,
            cookies_file,
            cookies_from_browser,
            proxy,
        )
        if not downloaded:
            return downloaded
        return await loop.run_in_executor(self.executor, self._maybe_make_telegram_compatible, downloaded)

# Global instance
downloader = Downloader()
