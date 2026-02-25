#!/bin/bash
# 修复数据库锁定问题 - 清理并重新初始化session

echo "=== 🔧 修复数据库锁定问题 ==="
echo ""

cd "$(dirname "$0")/config"

echo "1️⃣ 停止所有相关进程..."
pkill -f "telegram_monitor.py"
pkill -f "app.py"
pkill -f "bot_monitor.py"
sleep 2

echo ""
echo "2️⃣ 备份现有session文件..."
mkdir -p session_backup_$(date +%Y%m%d_%H%M%S)
cp *.session* session_backup_$(date +%Y%m%d_%H%M%S)/ 2>/dev/null || true

echo ""
echo "3️⃣ 转换session到WAL模式..."
for db in *.session; do
    if [ -f "$db" ]; then
        echo "  处理: $db"
        sqlite3 "$db" "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;"
        # 删除旧的journal文件
        rm -f "${db}-journal"
        echo "  ✓ 已转换为WAL模式"
    fi
done

echo ""
echo "4️⃣ 验证配置..."
for db in *.session; do
    if [ -f "$db" ]; then
        mode=$(sqlite3 "$db" "PRAGMA journal_mode;")
        echo "  $db: $mode"
    fi
done

echo ""
echo "✅ 修复完成！"
echo ""
echo "现在可以重启应用了："
echo "  cd .."
echo "  source venv/bin/activate"
echo "  python app/app.py"
