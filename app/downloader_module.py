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
        self.last_download_metadata = {}

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
        self.last_download_metadata = {}

    def get_last_download_metadata(self):
        return dict(self.last_download_metadata or {})

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

    def _normalize_quality_mode(self, mode):
        normalized = (mode or '').strip()
        if normalized in {'super_fast_720p', 'fast_compatible', 'balanced_hd', 'ultra_quality'}:
            return normalized
        return ''

    def download_video(self, url, output_dir, cookies_file=None, cookies_from_browser=None, proxy=None, quality_mode_override=None):
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
        # - super_fast_720p: smallest practical output for fastest Telegram upload.
        # - fast_compatible: prioritize Telegram compatibility and upload speed.
        # - balanced_hd: quality-first up to 2K with compatibility fallback.
        # - ultra_quality: quality-first up to 4K with compatibility fallback.
        quality_mode = self._normalize_quality_mode(quality_mode_override) or self._get_quality_mode_from_config()
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

        try:
            requested_formats = info.get('requested_formats')
            if isinstance(requested_formats, list) and requested_formats:
                selected = []
                for fmt in requested_formats:
                    if not isinstance(fmt, dict):
                        continue
                    fid = str(fmt.get('format_id') or '?')
                    fext = str(fmt.get('ext') or '?')
                    fw = fmt.get('width') or '?'
                    fh = fmt.get('height') or '?'
                    vcodec = str(fmt.get('vcodec') or '?')
                    acodec = str(fmt.get('acodec') or '?')
                    selected.append(f"{fid}:{fext}:{fw}x{fh}:v={vcodec}:a={acodec}")
                if selected:
                    self.log(f"yt-dlp 选中流: {' | '.join(selected)}", "info")
        except Exception:
            pass

        self.last_download_metadata = self._extract_download_metadata(info)

        filename = ydl.prepare_filename(info)
        if os.path.exists(filename):
            return filename
        base, ext = os.path.splitext(filename)
        if ext != '.mp4':
            mp4_filename = base + '.mp4'
            if os.path.exists(mp4_filename):
                return mp4_filename
        return filename

    def _extract_download_metadata(self, info):
        if not isinstance(info, dict):
            return {}

        def _as_positive_int(value):
            try:
                iv = int(value)
                return iv if iv > 0 else None
            except Exception:
                return None

        title = str(info.get('title') or '').strip()
        source_url = str(
            info.get('webpage_url')
            or info.get('original_url')
            or info.get('url')
            or ''
        ).strip()

        width = _as_positive_int(info.get('width'))
        height = _as_positive_int(info.get('height'))

        requested_formats = info.get('requested_formats')
        if isinstance(requested_formats, list):
            for fmt in requested_formats:
                if not isinstance(fmt, dict):
                    continue
                f_w = _as_positive_int(fmt.get('width'))
                f_h = _as_positive_int(fmt.get('height'))
                vcodec = str(fmt.get('vcodec') or '').lower()
                if f_w and f_h and vcodec not in ('none', ''):
                    width = f_w
                    height = f_h
                    break

        resolution = ''
        if width and height:
            resolution = f"{width}x{height}"
        else:
            format_note = str(info.get('format_note') or '').strip()
            resolution = format_note if format_note else ''

        return {
            'title': title,
            'source_url': source_url,
            'resolution': resolution,
        }

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
                'dar': (video_stream or {}).get('display_aspect_ratio'),
                'width': (video_stream or {}).get('width'),
                'height': (video_stream or {}).get('height'),
                'rotation': self._extract_rotation_degrees(video_stream),
                'acodec': (audio_stream or {}).get('codec_name'),
            }
        except Exception as e:
            self.log(f"ffprobe 分析失败，跳过兼容性转码: {e}", "warning")
            return None

    def _extract_rotation_degrees(self, video_stream):
        if not isinstance(video_stream, dict):
            return 0

        def _to_int(v):
            try:
                return int(round(float(v)))
            except Exception:
                return 0

        tags = video_stream.get('tags') or {}
        if isinstance(tags, dict) and 'rotate' in tags:
            deg = _to_int(tags.get('rotate'))
            if deg:
                return deg

        for side_data in video_stream.get('side_data_list') or []:
            if not isinstance(side_data, dict):
                continue
            if 'rotation' in side_data:
                deg = _to_int(side_data.get('rotation'))
                if deg:
                    return deg

        return 0

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
        dar = str(probe.get('dar') or '').strip()
        width = int(probe.get('width') or 0)
        height = int(probe.get('height') or 0)
        rotation_degrees = int(probe.get('rotation') or 0)

        if not video_stream:
            self.log("检测到文件无视频流，无法修复为可播放视频。", "warning")
            return file_path

        def _parse_ratio(ratio_text):
            t = str(ratio_text or '').strip()
            if not t or t in ('N/A', 'unknown', '0:1'):
                return None
            sep = ':' if ':' in t else ('/' if '/' in t else None)
            if sep:
                try:
                    left, right = t.split(sep, 1)
                    left_v = float(left)
                    right_v = float(right)
                    if right_v == 0:
                        return None
                    return left_v / right_v
                except Exception:
                    return None
            try:
                value = float(t)
                return value if value > 0 else None
            except Exception:
                return None

        def _round_even(value):
            try:
                v = int(round(float(value)))
            except Exception:
                v = 0
            if v < 2:
                v = 2
            if v % 2 != 0:
                v += 1
            return v

        def _is_reasonable_ratio(value, *, low=0.25, high=4.0):
            if value is None:
                return False
            try:
                v = float(value)
            except Exception:
                return False
            return low <= v <= high

        def _normalized_rotation_deg(value):
            try:
                return int(value) % 360
            except Exception:
                return 0

        def _rotation_filter_and_dimensions(w, h, rot):
            rot_n = _normalized_rotation_deg(rot)
            if rot_n == 90:
                return 'transpose=clock', h, w
            if rot_n == 270:
                return 'transpose=cclock', h, w
            if rot_n == 180:
                return 'hflip,vflip', w, h
            return '', w, h

        def _display_ratio_from_dimensions(w, h, s_ratio):
            if w <= 0 or h <= 0:
                return None
            ratio = float(w) / float(h)
            if _is_reasonable_ratio(s_ratio):
                ratio = ratio * float(s_ratio)
            return ratio if _is_reasonable_ratio(ratio, low=0.2, high=5.0) else None

        def _cap_dimensions_for_telegram(w, h, max_long_side=1920):
            if not w or not h:
                return w, h
            long_side = max(w, h)
            if long_side <= max_long_side:
                return w, h
            scale_ratio = float(max_long_side) / float(long_side)
            return _round_even(float(w) * scale_ratio), _round_even(float(h) * scale_ratio)

        def _build_ratio_locked_filter(target_ratio, fallback_w, fallback_h):
            safe_w = _round_even(fallback_w or width or 1280)
            safe_h = _round_even(fallback_h or height or 720)
            safe_w, safe_h = _cap_dimensions_for_telegram(safe_w, safe_h)

            if not _is_reasonable_ratio(target_ratio, low=0.2, high=5.0):
                return f"scale={safe_w}:{safe_h},setsar=1", safe_w, safe_h

            by_width_h = _round_even(float(safe_w) / float(target_ratio))
            by_height_w = _round_even(float(safe_h) * float(target_ratio))

            cand1_w, cand1_h = safe_w, max(2, by_width_h)
            cand2_w, cand2_h = max(2, by_height_w), safe_h

            err1 = abs((float(cand1_w) / float(cand1_h)) - float(target_ratio))
            err2 = abs((float(cand2_w) / float(cand2_h)) - float(target_ratio))

            if err1 <= err2:
                target_w_local, target_h_local = cand1_w, cand1_h
            else:
                target_w_local, target_h_local = cand2_w, cand2_h

            return (
                f"scale={target_w_local}:{target_h_local},setsar=1",
                target_w_local,
                target_h_local,
            )

        def _build_safe_filter(rotate_filter='', src_w=0, src_h=0):
            # Build deterministic scale target in Python.
            # Important: do NOT trust DAR for scaling, because bad DAR metadata can permanently bake stretch.
            filter_parts = []
            if rotate_filter:
                filter_parts.append(rotate_filter)

            if src_w <= 0 or src_h <= 0:
                filter_parts.append("scale='trunc(iw/2)*2':'trunc(ih/2)*2'")
                filter_parts.append('setsar=1')
                return ','.join(filter_parts), None, None

            if _is_reasonable_ratio(sar_ratio) and abs(float(sar_ratio) - 1.0) > 0.01:
                display_w = float(src_w) * sar_ratio
                display_h = float(src_h)
            else:
                display_w = float(src_w)
                display_h = float(src_h)

            target_w = _round_even(display_w)
            target_h = _round_even(display_h)
            target_w, target_h = _cap_dimensions_for_telegram(target_w, target_h)
            filter_parts.append(f"scale={target_w}:{target_h}")
            filter_parts.append('setsar=1')
            filt = ','.join(filter_parts)
            return filt, target_w, target_h

        def _build_capped_filter(max_long_side, rotate_filter='', src_w=0, src_h=0):
            if not src_w or not src_h or max_long_side <= 0:
                return _build_safe_filter(rotate_filter, src_w, src_h)

            long_side = max(src_w, src_h)
            if long_side <= max_long_side:
                return _build_safe_filter(rotate_filter, src_w, src_h)

            scale_ratio = float(max_long_side) / float(long_side)
            target_w_local = _round_even(float(src_w) * scale_ratio)
            target_h_local = _round_even(float(src_h) * scale_ratio)

            filter_parts = []
            if rotate_filter:
                filter_parts.append(rotate_filter)
            filter_parts.append(f"scale={target_w_local}:{target_h_local}")
            filter_parts.append('setsar=1')
            return ','.join(filter_parts), target_w_local, target_h_local

        # Telegram clients are most stable with H.264 + yuv420p and square pixels in MP4.
        sar_ratio = _parse_ratio(sar)
        dar_ratio = _parse_ratio(dar)
        rotate_filter, effective_w, effective_h = _rotation_filter_and_dimensions(
            width,
            height,
            rotation_degrees,
        )
        coded_ratio = (float(effective_w) / float(effective_h)) if effective_w > 0 and effective_h > 0 else None

        sar_is_non_square = bool(sar_ratio and abs(sar_ratio - 1.0) > 0.01)
        dar_mismatch_coded = bool(
            dar_ratio and coded_ratio and abs(dar_ratio - coded_ratio) / coded_ratio > 0.03
        )

        needs_transcode = (
            (vcodec != 'h264')
            or (pix_fmt not in ('yuv420p', 'yuvj420p'))
            or sar_is_non_square
            or dar_mismatch_coded
            or (_normalized_rotation_deg(rotation_degrees) != 0)
        )
        if not needs_transcode:
            return file_path

        safe_filter, target_w, target_h = _build_safe_filter(rotate_filter, effective_w, effective_h)
        source_display_ratio = _display_ratio_from_dimensions(effective_w, effective_h, sar_ratio)

        base, _ = os.path.splitext(file_path)
        fixed_path = base + '.tgfix.mp4'

        def _build_transcode_cmd(vf_filter, output_path, preset='veryfast', crf='20'):
            cmd_local = [
                self.ffmpeg_path or 'ffmpeg', '-y',
                '-noautorotate',
                '-i', file_path,
                '-map', '0:v:0',
                '-c:v', 'libx264',
                '-preset', preset,
                '-crf', str(crf),
                # Normalize dimensions and clear ratio metadata for stable Telegram playback.
                '-vf', vf_filter,
                '-pix_fmt', 'yuv420p',
                '-profile:v', 'high',
                '-metadata:s:v:0', 'rotate=0',
                '-movflags', '+faststart',
            ]

            if audio_stream:
                cmd_local.extend(['-map', '0:a:0', '-c:a', 'aac', '-b:a', '192k'])
            else:
                cmd_local.extend(['-an'])

            cmd_local.append(output_path)
            return cmd_local

        cmd = _build_transcode_cmd(safe_filter, fixed_path)

        try:
            target_desc = f"{target_w}x{target_h}" if target_w and target_h else 'auto-even'
            self.log(
                f"检测到视频兼容性风险(vcodec={vcodec or 'unknown'}, pix_fmt={pix_fmt or 'unknown'}, sar={sar or 'unknown'}, dar={dar or 'unknown'}, rotate={rotation_degrees}, target={target_desc})，开始转码为 Telegram 兼容格式...",
                "warning"
            )
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            if os.path.exists(fixed_path) and os.path.getsize(fixed_path) > 0:
                fixed_probe = self._probe_media_streams(fixed_path)
                if fixed_probe and source_display_ratio:
                    out_w = int(fixed_probe.get('width') or 0)
                    out_h = int(fixed_probe.get('height') or 0)
                    out_sar = _parse_ratio(fixed_probe.get('sar'))
                    fixed_display_ratio = _display_ratio_from_dimensions(out_w, out_h, out_sar)
                    ratio_drift = None
                    if fixed_display_ratio and source_display_ratio:
                        ratio_drift = abs(fixed_display_ratio - source_display_ratio) / source_display_ratio

                    if ratio_drift is not None and ratio_drift > 0.03:
                        locked_filter, lock_w, lock_h = _build_ratio_locked_filter(
                            source_display_ratio,
                            target_w,
                            target_h,
                        )
                        retry_path = base + '.tgfix2.mp4'
                        retry_cmd = _build_transcode_cmd(locked_filter, retry_path)

                        self.log(
                            f"检测到转码后比例偏差({ratio_drift * 100:.2f}%)，启用二次比例锁定转码({lock_w}x{lock_h})...",
                            "warning",
                        )

                        try:
                            subprocess.run(retry_cmd, check=True, capture_output=True, text=True)
                            if os.path.exists(retry_path) and os.path.getsize(retry_path) > 0:
                                self.log(f"二次比例锁定转码完成: {os.path.basename(retry_path)}", "success")
                                return retry_path
                            self.log("二次比例锁定未产出有效文件，回退首轮转码结果。", "warning")
                        except subprocess.CalledProcessError as retry_error:
                            retry_err = (retry_error.stderr or retry_error.stdout or '').strip()
                            if retry_err:
                                tail = '\n'.join(retry_err.splitlines()[-8:])
                                self.log(f"二次比例锁定转码失败，ffmpeg 输出(末尾): {tail}", "warning")
                            self.log(f"二次比例锁定转码失败，回退首轮转码结果: {retry_error}", "warning")
                        except Exception as retry_error:
                            self.log(f"二次比例锁定转码失败，回退首轮转码结果: {retry_error}", "warning")

                self.log(f"兼容性转码完成: {os.path.basename(fixed_path)}", "success")
                return fixed_path
            self.log("兼容性转码未产出有效文件，继续使用原文件。", "warning")
            return file_path
        except subprocess.CalledProcessError as e:
            ffmpeg_err = (e.stderr or e.stdout or '').strip()
            if ffmpeg_err:
                tail = '\n'.join(ffmpeg_err.splitlines()[-10:])
                self.log(f"兼容性转码失败，ffmpeg 输出(末尾): {tail}", "warning")

            # Fallback ladder for difficult sources (AV1/HDR/4K): simplify params and progressively cap size.
            fallback_profiles = [
                ('简化参数重试', 0, 'superfast', '22'),
                ('降级到 2160 长边', 2160, 'superfast', '23'),
                ('降级到 1080 长边', 1080, 'veryfast', '23'),
            ]

            for label, cap_side, preset, crf in fallback_profiles:
                fallback_filter, cap_w, cap_h = _build_capped_filter(cap_side, rotate_filter, effective_w, effective_h)
                if fallback_filter == safe_filter and preset == 'veryfast' and str(crf) == '20':
                    continue

                suffix = f"cap{cap_side}" if cap_side else 'retry'
                fallback_path = base + f'.tgfix_{suffix}.mp4'
                fallback_cmd = _build_transcode_cmd(fallback_filter, fallback_path, preset=preset, crf=crf)
                try:
                    self.log(
                        f"兼容性转码开始保底重试: {label} ({cap_w}x{cap_h}, preset={preset}, crf={crf})",
                        "warning",
                    )
                    subprocess.run(fallback_cmd, check=True, capture_output=True, text=True)
                    if os.path.exists(fallback_path) and os.path.getsize(fallback_path) > 0:
                        self.log(f"兼容性保底转码成功: {os.path.basename(fallback_path)}", "success")
                        return fallback_path
                except subprocess.CalledProcessError as fallback_err:
                    fallback_text = (fallback_err.stderr or fallback_err.stdout or '').strip()
                    if fallback_text:
                        tail = '\n'.join(fallback_text.splitlines()[-8:])
                        self.log(f"兼容性保底转码失败({label})，ffmpeg 输出(末尾): {tail}", "warning")
                except Exception as fallback_err:
                    self.log(f"兼容性保底转码失败({label}): {fallback_err}", "warning")

            self.log(f"兼容性转码失败，继续使用原文件: {e}", "warning")
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
                if mode in {'super_fast_720p', 'fast_compatible', 'balanced_hd', 'ultra_quality'}:
                    return mode
        except Exception:
            pass
        return 'balanced_hd'

    def _build_format_spec(self, quality_mode):
        if quality_mode == 'super_fast_720p':
            return (
                'bestvideo[height<=720][vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a]/'
                'bestvideo[height<=720][vcodec^=avc1]+bestaudio[ext=m4a]/'
                'best[height<=720][ext=mp4]/best[height<=720]'
            )

        if quality_mode == 'fast_compatible':
            return (
                'bestvideo[height<=1080][height>=720][vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a]/'
                'bestvideo[height<=1080][vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a]/'
                'bestvideo[height<=1080][vcodec^=avc1]+bestaudio[ext=m4a]/'
                'bestvideo[height<=1080]+bestaudio/'
                'best[height<=1080][ext=mp4]/best[height<=1080]'
            )

        if quality_mode == 'ultra_quality':
            return (
                'bestvideo[height<=4320]+bestaudio/best[height<=4320]/'
                'bestvideo[height<=4320][vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a]/'
                'bestvideo[height<=4320][vcodec^=avc1]+bestaudio[ext=m4a]/best'
            )

        # balanced_hd
        return (
            'bestvideo[height<=2160]+bestaudio/best[height<=2160]/'
            'bestvideo[height<=2160][vcodec^=avc1][ext=mp4]+bestaudio[acodec^=mp4a]/'
            'bestvideo[height<=2160][vcodec^=avc1]+bestaudio[ext=m4a]/best'
        )

    async def download_task(
        self,
        url,
        output_dir,
        cookies_file=None,
        cookies_from_browser=None,
        proxy=None,
        quality_mode_override=None,
        ensure_telegram_compatible=True,
    ):
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
            quality_mode_override,
        )
        if not downloaded:
            return downloaded
        if ensure_telegram_compatible:
            return await loop.run_in_executor(self.executor, self._maybe_make_telegram_compatible, downloaded)
        return downloaded

# Global instance
downloader = Downloader()
