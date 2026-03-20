# 📡 Telegram File Monitor

<div align="center">

**A full-featured Telegram message and file monitoring system**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-0.4.90-orange.svg)](.)

</div>

## ✨ Features

### 📱 Telegram Monitoring
- 🔄 Real-time channel monitoring
- 📤 Smart message forwarding
- 📥 Automatic media/file downloads
- 🤖 Bot link download flow: send a video-platform URL to the bot, auto-download, then upload back to TG
- ☁️ 115 share transfer: save 115 share links to a target CID via bot DM
- 🎯 Keyword and regex filtering
- 🛡️ Download risk controls (per-channel rate limit, dedup cooldown, file-size and free-space guards)
- ⏱️ Dynamic download timeout based on file size (with cap/buffer and timeout logs)

### 📂 File Monitoring
- 👀 Local directory watch
- ☁️ Auto upload to Telegram
- 🔁 Copy/Move sync modes
- ✅ Stability checks for safer transfers

### 🌐 Web Console
- Modern responsive UI
- Visual configuration pages
- Real-time logs
- Login protection

### 📺 Drama Calendar & Regex Sync
- Multi-source ingestion (`calendar`, `maoyan`, `douban`, web-heat, and merged `all` mode)
- Automatic scheduling with interval mode and Cron expressions
- Finished-show filtering via keyword / TMDB / hybrid strategies (with year tolerance and confidence score)
- Movie age pruning for Maoyan source using TMDB premiere dates (customizable day threshold)
- Dedicated drama logs with view / download / clear actions

## 🚀 Quick Start

### Requirements

- Python 3.8+
- Telegram API credentials ([my.telegram.org](https://my.telegram.org))

### Local Setup

```bash
# 1) Create and activate virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate

# 2) Install dependencies
pip install -r requirements.txt

# 3) Configure API credentials (config/.env)
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash

# 4) Start web app
python app/app.py

# 5) Open http://localhost:5001/web_login
# First visit will guide you to set initial password (default user: admin)
```

### Docker Setup

```bash
docker compose up -d
```

Latest image: `y1nf0x/tg-file-monitor:0.5.35`

Upgrade example:

```bash
docker compose pull
docker compose up -d
```

To pin a version, update `docker-compose.yml` with:

```
y1nf0x/tg-file-monitor:0.5.35
```

### ☁️ 115 Share Transfer (Bot)

Prerequisites:
- Save `115 Cookie` in the web config
- (Optional) Set a default **115 target CID**

Usage (DM the bot):
```
https://115.com/s/xxxx?password=yyyy cid=123456
```
or
```
xxxx-yyyy cid=123456
```

Notes:
- If the share contains multiple items, the bot will list them; reply with numbers or `all`
- Hidden/inline links, webpage preview links, and messages with images are supported
- DM only (group messages are ignored)

### 📺 Channel Monitor Regex (Built-in Config)

No external `.env` is required. Use the internal config file `config/tvchannel_filters.json` to define global/channel-level allow/deny patterns (regex supported).

Example:
```json
{
  "global": {
    "whitelist": ["^(?=.*Linan).*", "S\\d{2}E\\d{2}"],
    "blacklist": ["trailer", "behind the scenes"]
  },
  "channels": {
    "-1001234567890": {
      "whitelist": ["4K", "WEB-DL"],
      "blacklist": ["sample"]
    }
  }
}
```

Notes:
- `global`: applies to all monitored channels
- `channels`: per-channel rules (key is the channel ID)
- Blacklist hit skips; if any whitelist exists, message must match

Usage:
- In "TG collection settings", enable **Use TV channel filters** per channel
```

## 🖥️ VPS Notes

### Basic Checks

```bash
# Time sync (important for Telethon)
timedatectl status

# Open web port if needed
sudo ufw allow 5001/tcp
```

### Runtime Recommendations

- Prefer Docker/Compose for easier restart and rollback.
- If running via Python directly, use systemd to keep services alive.
- Do not let multiple processes share the same `.session` file.

### Upgrade

```bash
git pull
docker compose up -d --build
```

## 📖 Usage

### First-Time Setup

1. Open Web UI: `http://localhost:5001/web_login`
2. Complete Telegram authentication (phone + code)
3. Add channels to monitor
4. Start monitor from Web UI
5. Check live logs

### Example: Channel Forwarding

- Source channel ID: `-1001234567890` (must be negative)
- Target users: `user1,user2` (comma-separated)

### Example: File Monitoring Task

- Source dir: `/path/to/watch`
- Destination dir: `/path/to/sync`
- Mode: `copy` / `move`
- Stable time: e.g. `10` seconds

### Example: Bot Link Download to TG

- Send a supported video-platform URL to your Telegram bot
- The bot resolves and downloads the media automatically
- After completion, the media is uploaded back to Telegram with progress/timeout logs

## 🔐 Security Best Practices

- Do not commit `config/config.json`, `config/.env`, `*.session`.
- Keep API credentials in environment variables or `.env`.
- Put Web UI behind reverse proxy in production.
- Enable HTTPS (Nginx + Let's Encrypt recommended).

## 💡 FAQ

### Q: Telegram auth fails. What should I check?

1. Verify `API_ID` and `API_HASH`.
2. Check network/proxy configuration.
3. Ensure phone number format is correct.

### Q: How to get a channel ID?

1. Forward a message to [@userinfobot](https://t.me/userinfobot).
2. Read returned ID (usually negative for channels).

### Q: I see `Server sent a very old message with ID ...`.

Common causes: clock skew, NTP issues, or stale session files.

Try:
1. `timedatectl status`
2. `sudo timedatectl set-ntp true`
3. Stop related processes, backup/remove `config/*.session*`
4. Re-authenticate Telegram session

### Q: `database is locked` error when starting bot/monitor.

Try:
1. Stop all related processes
2. Run: `bash fix_database_lock.sh`
3. Start services again and inspect logs

## 🛠️ Tech Stack

- Backend: Python, Flask, Telethon
- Frontend: Bootstrap 5, JavaScript
- Config storage: JSON
- Deployment: Docker, Docker Compose

## 📊 Release History

### v0.5.35 (2026-03-20) - Current
- ✅ Public self-service entry no longer shows the public-entry status block
- ✅ The public request form is rebalanced for both mobile and desktop, with a tighter first screen and cleaner desktop layout

### v0.5.34 (2026-03-20)
- ✅ Telegram monitor no longer exits its main loop after transient Telethon connection drops
- ✅ Reconnect now reuses the existing Telethon session first, reducing session database lock conflicts

### v0.5.33 (2026-03-19)
- ✅ Unified all four homepage runtime cards into the same "mini field cards + status badges" layout
- ✅ Telegram, file sync, bot, and drama scheduler states now share a cleaner hierarchy and handle long status text better

### v0.5.30 (2026-03-19)
- ✅ Restored HDHive auto sign-in using the site's current Server Action flow
- ✅ Restored current HDHive points from account-page data
- ✅ Config page adds a manual points refresh action and updated points timestamp
- ✅ Bot adds `/hdhive_checkin`, `/hdhive_points`, and `/start` shortcuts

### v0.5.29 (2026-03-19)
- ✅ Added `猎杀史卡佩塔` to the drama whitelist

### v0.5.22 (2026-03-18)
- ✅ Bot can resolve HDHive links and transfer to 115 (supports `cid` for direct transfer)
- ✅ HDHive/115 link messages no longer trigger the “update cookies” prompt

### v0.5.21 (2026-03-18)
- ✅ Home overview adds TG/Bot capture details (last link/download/transcode/upload/error)
- ✅ Overview stats window is configurable and switchable (All/5/10/30/60 minutes)

### v0.5.20 (2026-03-18)
- ✅ Log page shows download queue backlog and concurrency in real time
- ✅ Auto-throttle download concurrency when queue exceeds threshold, auto-recover on cooldown
- ✅ New queue auto-throttle settings (threshold/recover threshold/min concurrency)

### v0.5.19 (2026-03-17)
- ✅ Maoyan web movies now use the official API (avoid grabbing box-office titles)
- ✅ Web movies: add “days since release” removal threshold (independent of TMDB finish)
- ✅ Drama whitelist preserves per-source groups (web movies editable separately)

### v0.5.17 (2026-03-17)
- ✅ Added drama source: Maoyan web heat (web movies)

### v0.5.16 (2026-03-17)
- ✅ Custom category layout polish in drama whitelist editor

### v0.5.15 (2026-03-17)
- ✅ Drama whitelist editor: custom categories (manual keywords, unaffected by refresh)

### v0.5.14 (2026-03-17)
- ✅ Self-service form hint: leave season empty = all seasons

### v0.5.13 (2026-03-17)
- ✅ Multi-season input supported (e.g. `1,2` / `1-2` / `S01,S02`)
- ✅ Strict multi-season matching to avoid unlocking the wrong season

### v0.5.12 (2026-03-17)
- ✅ Season matching now scans nested fields/filenames to identify S01/S02

### v0.5.11 (2026-03-17)
- ✅ Improved season hints and clearer failure message when season info is missing

### v0.5.10 (2026-03-17)
- ✅ Auto-reconnect for TG monitor to avoid long-run stalls
- ✅ More robust config/TV whitelist loading (atomic writes + fallback to last good config)
- ✅ Startup TV-whitelist backfill (configurable limit, default 20; only for channels using TV filters)

### v0.5.08 (2026-03-16)
- ✅ Self-service season matching: support titles like `S01+S02`, `S01-02`, `S01E02`

### v0.5.06 (2026-03-16)
- Self-service adds optional season & resolution inputs
- Season selection is strict to avoid jumping seasons
- Resolution preference participates in sorting and filtering

### v0.5.05 (2026-03-16)
- Self-service resource selection: prefer official items, then lower points

### v0.5.04 (2026-03-16)
- Fast red packet click mode: prefer button text and skip context fetch
- Optional "skip follow-up processing" after click for maximum speed
- HDHive cookie test/monitoring: unlock-points fallback, force cookie test, sync test result to status
- Self-service UI: recent list on the right, details collapsed, limit display count
- Drama scheduler log now keeps only the latest run

### v0.5.14 (2026-03-17) - Current
- ✅ Self-service form hint: leave season empty = all seasons

### v0.5.13 (2026-03-17)
- ✅ Multi-season input supported (e.g. `1,2` / `1-2` / `S01,S02`)
- ✅ Strict multi-season matching to avoid unlocking the wrong season

### v0.5.12 (2026-03-17)
- ✅ Season matching now scans nested fields/filenames to identify S01/S02

### v0.5.11 (2026-03-17)
- ✅ Improved season hints and clearer failure message when season info is missing

### v0.4.90 (2026-03-15)
- Channel monitor uses built-in `tvchannel_filters.json` (regex allow/deny)
- Drama regex no longer depends on external `.env`

### v0.4.89 (2026-03-15)
- Simplify self-service request fields and highlight key inputs
- Self-service result now uses a unified success/failure message

### v0.4.88 (2026-03-15)
- Fix bot monitor startup failure (indentation error)

### v0.4.87 (2026-03-15)
- Detect hidden/inline 115 share links (including webpage preview URLs)

### v0.4.86 (2026-03-15)
- Allow 115 transfers from messages that include images (skip file-only blocking)

### v0.4.85 (2026-03-15)
- Bot can save 115 share links to a target CID (supports multi-item selection)
- Web config adds a default 115 target CID

### v0.4.84 (2026-03-15)
- Fix Threads `unknown_video` extension causing document uploads by remuxing/transcoding to mp4
- Add ffprobe format_name for smarter container fixes

### v0.4.83 (2026-03-15)
- Threads: prefer embed mp4, fallback to yt-dlp for compatibility
- Threads filenames use "title + shortcode" to avoid collisions
- Stronger upload compatibility transcode to reduce document uploads
- Downloader page shows last task title/resolution/filename/source

### v0.4.82 (2026-03-15)
- Download Threads direct mp4 without yt-dlp
- Ensure mp4 filename (avoid unknown_video)

### v0.4.81 (2026-03-15)
- Improve Threads embed parsing (escaped URLs + optional cookies)
- Clear error when Threads post has no video

### v0.4.80 (2026-03-15)
- Skip native yt-dlp for Threads and go straight to embed direct mp4
- Remove Unsupported URL noise for Threads downloads

### v0.4.79 (2026-03-15)
- Treat Threads direct mp4 as streaming-compatible to avoid failed transcode
- Tag Threads source in download metadata for upload flow

### v0.4.78 (2026-03-15)
- Extract Threads direct mp4 from embed page (works without native yt-dlp support)
- Validate direct video URL before download

### v0.4.77 (2026-03-15)
- Force Threads URLs to `/t/` short links in bot flow
- Prefer `/t/` candidates when resolving Threads URLs

### v0.4.76 (2026-03-15)
- Threads fallback URL support (`/t/` short links) to reduce Unsupported URL errors
- Bump minimum yt-dlp version for latest site support

### v0.4.75 (2026-03-15)
- Normalize Threads URLs (threads.com -> threads.net, strip tracking params)
- Deduplicate bot download replies within 15s for the same user+URL

### v0.4.74 (2026-03-15)
- Threads link download support (threads.com / threads.net)
- Auto-click red packet notifications no longer trigger downloader flow
- Telegram links are ignored to prevent accidental download attempts

### v0.4.73 (2026-03-15)
- Public self-service submission page (optional access key)
- Public endpoint rate limiting (IP-based, configurable window/limit)
- Storage filter options for self-service: any / 115 / 123 / 115+123
- Open API direct-unlock toggle to avoid bypassing point thresholds
- Open API Key test now prioritizes ping verification

### v0.4.72 (2026-03-14)
- Log page performance optimizations: tail-only reads, render caching, manual refresh, configurable refresh interval
- Auto-refresh disabled by default to reduce Web UI load
- File monitor scan interval configurable; no-task mode skips process startup
- Configurable download concurrency to avoid CPU spikes

### v0.4.71 (2026-03-14)
- Added a dedicated Red Packet settings card (auto-click toggle, keywords, button text, notify targets)
- Auto-switch to `text` monitoring when enabling red packet/auto-click without a download directory
- Fixed auto-click compatibility with Telethon `Message.click` argument names (`i/j` vs `row/column`)

### v0.4.67 (2026-03-13)
- Fixed Drama regex append behavior so appended rules are rebuilt as `|`-joined regex instead of comma-separated fragments
- Normalized legacy escaped title tokens during append merges, preventing stale patterns like `\\(` and `\\)` from spreading

### v0.4.66 (2026-03-12)
- Reworked the Drama Calendar page with multi-select sources, source tabs, a compact layout, and a top-right scheduler status card
- Fixed Drama Calendar TMDB routing and Douban source normalization, and added run concurrency protection plus dynamic timeouts
- Added structured run summaries, clearer Drama logs, and a one-click action to clear target env variable contents

### v0.4.64 (2026-03-12)
- Upgraded download timeout strategy with size-based dynamic timeout (with cap and buffer) to reduce false timeouts on large files
- Added download timeout observability log per task for easier troubleshooting

### v0.4.63 (2026-03-11)
- Fixed Drama Calendar runs being marked as failure when there are no new titles to write (now treated as successful skip)

### v0.4.62 (2026-03-11)
- Added configurable movie premiere age removal in Drama Calendar settings (default: 365 days, `-1` to disable)
- Added TMDB-based auto-removal for over-age movie titles from Maoyan source

### v0.4.61 (2026-03-11)
- Fixed manual Preview/Apply action handling on Drama Calendar page (resolved form action conflict)

### v0.4.60 (2026-03-11)
- Improved dedup: previously monitored titles are no longer appended repeatedly
- Improved TMDB observability logs: enabled state, cache hit, outbound request, and skip reasons

## 📄 License

MIT License
