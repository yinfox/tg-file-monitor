# 📡 Telegram 文件监控系统

<div align="center">

**功能完善的 Telegram 消息和文件监控管理系统**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-0.3.8-orange.svg)](.)

</div>

## ✨ 核心功能

### 📱 Telegram 监控
- 🔄 **频道消息监控** - 实时监控多个 Telegram 频道
- 📤 **自动转发** - 智能转发消息给指定用户
- 📥 **文件下载** - 自动下载频道文件和视频
- 🎯 **关键词过滤** - 正则表达式内容过滤

### 📂 文件监控
- 👀 **目录监控** - 实时监控本地文件变化
- ☁️ **自动上传** - 新文件自动上传到 Telegram
- 🔁 **文件同步** - 支持复制/移动模式
- ✅ **智能验证** - 中途检查保证传输完整性

### 🌐 Web 管理界面
- 💎 **现代化 UI** - 渐变配色，响应式设计
- ⚙️ **可视化配置** - 无需编辑配置文件
- 📊 **实时日志** - 彩色日志，方便调试
- 🔐 **安全认证** - 用户登录保护

## 🚀 快速开始

### 📋 环境要求

- Python 3.8+
- Telegram API 凭据（[获取地址](https://my.telegram.org)）

### ⚡ 5分钟部署

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置API（在config/.env中）
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash

# 3. 启动Web界面
python app/app.py

# 4. 访问 http://localhost:5001/web_login
# 首次访问会引导设置初始密码（用户名默认为 admin）
```

### 🐳 Docker 一键部署

```bash
docker-compose up -d
```

## 📖 使用指南

### 第一次使用

1. **登录 Web 界面** (`http://localhost:5001/web_login`)
2. **Telegram 认证** - 输入手机号，填写验证码
3. **配置频道** - 添加要监控的频道 ID
4. **启动监控** - 点击"启动监控"按钮
5. **查看日志** - 实时查看运行状态

### 配置频道转发

```
配置页面 → Telegram 监控配置
- 源频道 ID: -1001234567890  (必须是负数)
- 目标用户 ID: user1,user2    (用逗号分隔)
```

### 配置文件监控

```
文件监控 → 任务管理 → 添加任务
- 源目录: /path/to/watch
- 目标目录: /path/to/sync
- 操作模式: 复制 / 移动
- 稳定时间: 10 (秒，等待文件写入完成)
```

## 📁 项目结构

```
tg-file-monitor/
├── app/
│   ├── app.py                 # Flask 主程序
│   ├── bot_monitor.py         # Bot 监控
│   ├── downloader_module.py   # 文件下载器
│   ├── api_115.py             # 115网盘集成
│   └── templates/             # HTML 模板
├── config/
│   ├── config.json            # 配置文件
│   └── .env                   # API 凭据
├── telegram_monitor.py        # Telegram 监控
├── file_monitor.py            # 文件监控
└── requirements.txt           # 依赖列表
```

## 🔧 高级配置

### 代理设置

Web 界面 → 配置 → 代理配置

```json
{
  "addr": "127.0.0.1",
  "port": "7890",
  "username": "",
  "password": ""
}
```

### 文件名黑名单

自动清理文件名中的推广信息：

```
配置 → 文件名黑名单 → 添加关键词
例如: "频道名称", "t.me/xxx"
```

## 💡 常见问题

<details>
<summary><b>Q: Telegram 认证失败怎么办？</b></summary>

**解决方案：**
1. 确认 API_ID 和 API_HASH 正确
2. 检查网络连接（可能需要代理）
3. 确保手机号格式正确（+86xxxxxxxxxx）
</details>

<details>
<summary><b>Q: 如何获取频道 ID？</b></summary>

**方法：**
1. 转发频道消息给 [@userinfobot](https://t.me/userinfobot)
2. Bot 会显示频道 ID（负数，如 -1001234567890）
3. 或使用 Telegram Desktop 查看频道链接
</details>

<details>
<summary><b>Q: 文件监控不生效？</b></summary>

**检查项：**
1. 源目录路径是否正确
2. 是否有读取权限
3. 稳定时间是否设置合理
4. 查看"文件监控日志"了解详情
</details>

## 🛠️ 技术栈

| 类别 | 技术 |
|------|------|
| 后端 | Python 3.8+, Flask, Telethon |
| 前端 | Bootstrap 5, JavaScript |
| 存储 | JSON 配置 |
| 部署 | Docker, Docker Compose |

## 📊 版本历史

### v0.3.1 (2026-02-11) - 当前版本
- ✅ 完整的 Web 管理界面
- ✅ 渐变色现代化 UI
- ✅ Telegram/文件/Bot 三大监控模块
- ✅ Docker 一键部署
- ✅ 完善的错误处理

## 🗺️ 开发路线图

- [ ] 多云存储支持（阿里云OSS、腾讯云COS）
- [ ] 消息模板自定义
- [ ] Webhook 集成
- [ ] 性能优化和缓存机制
- [ ] 多语言支持

## 🤝 贡献

欢迎提交 Issue 和 Pull Request!

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE)

## ⭐ Star History

如果这个项目对你有帮助，请给个 Star ⭐

---

<div align="center">
Made with ❤️ by Telegram File Monitor Team
</div>
