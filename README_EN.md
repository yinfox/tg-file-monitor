# 📡 Telegram File Monitor

<div align="center">

**A full-featured Telegram message and file monitoring system**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-0.4.18-orange.svg)](.)

</div>

## ✨ Features

### 📱 Telegram Monitoring
- 🔄 Real-time channel monitoring
- 📤 Smart message forwarding
- 📥 Automatic media/file downloads
- 🎯 Keyword and regex filtering

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

## 📄 License

MIT License
