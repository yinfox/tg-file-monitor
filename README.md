# 📡 Telegram 文件监控系统

<div align="center">

**功能完善的 Telegram 消息和文件监控管理系统**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-0.4.29-orange.svg)](.)

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
# 1. 创建并激活虚拟环境（推荐）
python3 -m venv .venv
source .venv/bin/activate

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置API（在config/.env中）
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash

# 4. 启动Web界面
python app/app.py

# 5. 访问 http://localhost:5001/web_login
# 首次访问会引导设置初始密码（用户名默认为 admin）
```

### 🐳 Docker 一键部署

```bash
docker compose up -d
```

## 🖥️ VPS 部署建议

### 基础检查

```bash
# 时间同步（Telethon 强依赖）
timedatectl status

# 防火墙确保放行 Web 端口
sudo ufw allow 5001/tcp
```

### 进程运行建议

- 优先使用 Docker/Compose 运行，便于重启与回滚。
- 若使用 Python 直接运行，建议配合 systemd 守护进程，避免 SSH 断开导致服务退出。
- 不要让多个进程同时占用同一个 `.session` 文件。

### 升级建议

```bash
git pull
docker compose up -d --build
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

## 🔐 安全建议

- 不要提交 `config/config.json`、`config/.env`、`*.session` 到 GitHub。
- API 凭据优先放在环境变量或 `.env`，避免硬编码。
- 建议将 Web 管理面板放在反向代理后，并限制访问来源 IP。
- 生产环境建议开启 HTTPS（如 Nginx + Let's Encrypt）。

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

<details>
<summary><b>Q: 出现 <code>Server sent a very old message with ID ...</code> 怎么办？</b></summary>

**原因：**
常见于系统时间漂移、NTP 未同步、或旧会话文件异常。

**处理步骤：**
1. 检查时间同步：`timedatectl status`
2. 开启 NTP：`sudo timedatectl set-ntp true`
3. 停止相关进程后重建 session（备份并删除 `config/*.session*`）
4. 重新登录 Telegram 授权
</details>

<details>
<summary><b>Q: Bot/监控启动提示数据库锁定（database is locked）？</b></summary>

**处理方式：**
1. 停止所有相关进程（Web / monitor / bot）
2. 执行仓库内脚本：`bash fix_database_lock.sh`
3. 再次启动服务并观察日志
</details>

## 🛠️ 技术栈

| 类别 | 技术 |
|------|------|
| 后端 | Python 3.8+, Flask, Telethon |
| 前端 | Bootstrap 5, JavaScript |
| 存储 | JSON 配置 |
| 部署 | Docker, Docker Compose |

## 📊 版本历史

### v0.4.29 (2026-03-10) - 当前版本
- ✅ 修复下载页保存设置交互：按钮文案与状态统一为“保存下载设置（含画质）”
- ✅ 修复画质独立保存体验：移除目录输入框 `required` 校验干扰
- ✅ 增强下载后兼容探测稳定性：统一复用已解析的 `ffmpeg/ffprobe` 路径，避免部分环境下探测/转码未生效

### v0.4.28 (2026-03-10)
- ✅ 修复下载器设置保存体验：画质模式可独立保存，不再依赖先执行下载任务
- ✅ 新增 Telegram 视频兼容兜底：下载后自动探测并在必要时转码为 `H.264 + AAC + yuv420p + faststart`
- ✅ 上传侧增强：显式按视频语义发送，降低“00:00/只有声音”概率

### v0.4.27 (2026-03-10)
- ✅ 新增下载画质模式可选：`极速兼容 / 高清平衡 / 超清优先`
- ✅ Web 下载页支持直接切换并保存画质模式，无需手改配置文件
- ✅ 下载策略改为按模式动态选择 yt-dlp format，兼顾清晰度、兼容性与上传速度

### v0.4.26 (2026-03-10)
- ✅ YouTube 下载格式优化：优先 `H.264(avc1)+AAC(mp4a)` 并限制 `<=1080p`，降低 Telegram 兼容性问题
- ✅ 修复“上传后只有声音无画面”高频场景：避免选择 AV1/VP9 等部分客户端兼容较差的视频流
- ✅ 上传参数优化：启用 `supports_streaming` 并调整分片，改善上传与在线播放体验

### v0.4.25 (2026-03-10)
- ✅ 修复 Bot Token 切换后仍复用旧会话问题：启动前校验 `bot_session` 身份
- ✅ 自动重建失配会话：当会话绑定用户账号或 Bot ID 与 token 不一致时，自动清理并重登
- ✅ 避免 Telethon 忽略新 `bot_token`，确保 Bot 指令菜单与 `/start` 对应新 Bot 生效

### v0.4.24 (2026-03-10)
- ✅ 修复 Bot 启动日志崩溃：`load_config()` 返回值异常导致的 `NoneType.get` 报错
- ✅ 加固配置读取：配置格式异常时安全回退，避免启动链路中断

### v0.4.23 (2026-03-10)
- ✅ Bot Token 生效逻辑优化：Web 配置优先，环境变量仅作为兜底
- ✅ Web 端更新 Bot Token 后自动重启 Bot 监控进程，修改后立即生效
- ✅ 新增 Bot 启动诊断日志：输出 token 来源与脱敏指纹，便于排查配置覆盖

### v0.4.22 (2026-03-09)
- ✅ 优化 YouTube 失败诊断：区分“未提供 Cookies”与“已提供但可能失效/出口受限”
- ✅ Bot 提示更准确：避免始终提示重新上传 Cookies，优先引导排查网络出口/IP 风控

### v0.4.21 (2026-03-09)
- ✅ 修复 Web 端版本显示未同步问题：统一更新页面版本变量为 `0.4.21`
- ✅ 补丁发布：同步 Docker 镜像与页面显示版本，避免“镜像版本/页面版本”不一致

### v0.4.20 (2026-03-09)
- ✅ Docker 发布链路增强：补齐 `deno` 安装依赖 `unzip`，避免镜像构建中断
- ✅ 运行环境补齐：镜像内置 `nodejs`/`npm`/`deno` + `ffmpeg`，提升 yt-dlp 可用性
- ✅ Python 依赖完善：新增 `imageio-ffmpeg`，在受限环境提供 ffmpeg 兜底
- ✅ 监控日志体验优化：`telegram_monitor.py` 日志输出实时 `flush`
- ✅ 新增发布脚本：`scripts/docker_release.sh`、`scripts/netcup_deploy.sh`

### v0.4.18 (2026-03-08) - 当前版本
- ✅ 下载风控优化：限频/冷却触发时进入队列等待并自动重试
- ✅ 不再直接跳过可重试下载任务，避免消息丢下载
- ✅ 不可重试场景（体积超限/磁盘不足）仍按策略跳过

### v0.4.17 (2026-03-08)
- ✅ 优化文件监控心跳日志：任务数未变化时不重复刷屏
- ✅ 保留异常与错误日志完整输出，排障信息不丢失
- ✅ 日志更简洁：默认仅关键状态变化时输出

### v0.4.16 (2026-03-08)
- ✅ 修复下载进度日志多行刷屏问题：Web UI 始终只显示 1 条进度
- ✅ 进度日志单行刷新，完成后自动替换为完成消息
- ✅ ANSI 颜色渲染完善：青色进度、绿色完成、红色错误等

### v0.4.15 (2026-03-08)
- ✅ 下载进度日志单行刷新：Web UI 中始终只显示 1 条进度，不再刷屏
- ✅ 完善 ANSI 颜色渲染：Web UI 正确显示青色进度、绿色完成等彩色日志
- ✅ 优化日志体验：进度流畅更新（1s/5%），完成后自动替换为完成消息

### v0.4.14 (2026-03-08)
- ✅ 优化文件监控日志格式：添加 ANSI 颜色支持
- ✅ 成功/失败/警告等日志关键词彩色高亮显示
- ✅ 提升日志可读性，便于快速定位问题

### v0.4.13 (2026-03-08)
- ✅ 修复文件命名优先级问题：消息文本 > 原始文件名 > fallback
- ✅ 避免使用带黑名单关键词的视频原始文件名
- ✅ 用户自定义消息描述优先级最高，符合实际使用习惯

### v0.4.12 (2026-03-08)
- ✅ 新增媒体精确追踪日志开关（trace_media_detection，默认关闭）
- ✅ TRACE_DETECT / TRACE_SKIP 改为可控输出，便于按需排障
- ✅ Web 文件配置页新增 TRACE 开关并支持热生效

### v0.4.11 (2026-03-08)
- ✅ 修复相册多视频下载同名覆盖导致丢文件
- ✅ 增强视频识别（mime/属性/扩展名）
- ✅ 修复 monitor_types 列表配置兼容与相册文案白名单匹配

### v0.4.10 (2026-03-08)
- ✅ 新增下载风控（限流/重复冷却/体积与磁盘保护）
- ✅ Web 可配置下载风控参数
- ✅ 首页与文件监控页新增风控拦截统计与清零

### v0.4.9 (2026-03-08)
- ✅ 修复目录写入未完成被提前转存的问题
- ✅ 新增目录稳定时间（dir_stable_time）配置

### v0.3.1 (2026-02-11)
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
