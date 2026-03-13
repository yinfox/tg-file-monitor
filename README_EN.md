# 📡 Telegram File Monitor

<div align="center">

**A full-featured Telegram message and file monitoring system**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-0.4.67-orange.svg)](.)

</div>

## ✨ Features

### 📱 Telegram Monitoring
- 🔄 Real-time channel monitoring
- 📤 Smart message forwarding
- 📥 Automatic media/file downloads
- 🤖 Bot link download flow: send a video-platform URL to the bot, auto-download, then upload back to TG
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

### v0.4.67 (2026-03-13) - Current
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
