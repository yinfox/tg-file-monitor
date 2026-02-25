# file_monitor.py
import os
import json
import time
import shutil
import traceback
import sys
import hashlib
from dotenv import load_dotenv

# 直接使用项目本地的 115 API 客户端（不要回退到其他项目路径）
try:
    from app.api_115 import Client115
except Exception:
    # 若本地客户端不可用，则禁用 115 功能
    Client115 = None

# --- Paths & Config ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 尝试定位 config 目录
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
if not os.path.exists(CONFIG_DIR):
    CONFIG_DIR = os.path.abspath('config')

CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')

# 加载环境变量 (优先加载 config/.env)
try:
    load_dotenv(os.path.join(CONFIG_DIR, '.env'))
except Exception:
    pass

# 全局 DEBUG 标志
DEBUG_MODE = False

def load_config():
    if not os.path.exists(CONFIG_FILE): return {"file_monitoring_tasks": [], "debug_mode": False}
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
            # 环境变量覆盖（敏感信息优先）
            if os.environ.get('COOKIE_115'):
                 config['115_cookie'] = os.environ.get('COOKIE_115')
            elif os.environ.get('WEB_115_COOKIE'):
                 config['115_cookie'] = os.environ.get('WEB_115_COOKIE')
            
            # 读取 debug 模式设置
            global DEBUG_MODE
            DEBUG_MODE = config.get('debug_mode', False)
            return config
    except: return {"file_monitoring_tasks": [], "debug_mode": False}

def log_message(message, level="INFO"):
    """输出日志，level 可以是 INFO, DEBUG, ERROR"""
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    if level == "DEBUG" and not DEBUG_MODE:
        return  # DEBUG 模式关闭时不输出 DEBUG 日志
    print(f"[{timestamp}] [{level}] {message}")

def debug_log(message):
    """便捷的 DEBUG 日志函数"""
    log_message(message, level="DEBUG")


def compute_sha1(file_path, chunk_size=8 * 1024 * 1024):
    """按块计算文件的 SHA1 哈希值，默认 8MB 块大小。"""
    h = hashlib.sha1()
    try:
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                h.update(data)
        return h.hexdigest()
    except Exception:
        return None

def resolve_destination_path(destination_dir, filename, handle_duplicate="rename"):
    os.makedirs(destination_dir, exist_ok=True)
    base, ext = os.path.splitext(filename)
    candidate = os.path.join(destination_dir, filename)
    if not os.path.exists(candidate):
        return candidate
    if handle_duplicate != "rename":
        return candidate
    index = 1
    while True:
        new_name = f"{base} ({index}){ext}"
        candidate = os.path.join(destination_dir, new_name)
        if not os.path.exists(candidate):
            return candidate
        index += 1

def copy_with_mid_check(filepath, filename, destination_dir, handle_duplicate, client_115, file_sha1, file_size, target, check_interval, chunk_size):
    target_path = resolve_destination_path(destination_dir, filename, handle_duplicate)
    temp_path = f"{target_path}.part"
    last_check = time.time()

    try:
        with open(filepath, 'rb') as src, open(temp_path, 'wb') as dst:
            while True:
                data = src.read(chunk_size)
                if not data:
                    break
                dst.write(data)

                now = time.time()
                if now - last_check >= check_interval:
                    last_check = now
                    check = client_115.check_file_exists(
                        file_sha1,
                        file_size,
                        filename,
                        target=target,
                        file_path=filepath
                    )
                    if check.get('success') and check.get('can_transfer') and check.get('already_exists'):
                        log_message(f"⚡ 复制过程中检测到可秒传，停止本地复制: {filename}")
                        dst.close()
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass
                        return {"stopped": True, "check": check}

        os.replace(temp_path, target_path)
        return {"stopped": False, "target_path": target_path}
    except Exception as e:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass
        raise e

def perform_local_action(filepath, filename, destination_dir, action_type, handle_duplicate, enable_mid_copy_check=False, client_115=None, file_sha1=None, file_size=None, target=None, check_interval=30, chunk_size=8 * 1024 * 1024, delete_source_after_transfer=False):
    if not destination_dir:
        log_message(f"⚠️  未配置本地目标目录，跳过 {action_type} 操作: {filename}")
        return False
    try:
        use_mid_check = (
            enable_mid_copy_check
            and client_115
            and file_sha1
            and file_size
            and target
            and action_type in ("copy", "move")
        )

        if use_mid_check:
            result = copy_with_mid_check(
                filepath,
                filename,
                destination_dir,
                handle_duplicate,
                client_115,
                file_sha1,
                file_size,
                target,
                check_interval,
                chunk_size
            )
            if result.get("stopped"):
                log_message(f"✅ 已停止本地复制（秒传可用）: {filename}")
                # 即使停止了复制，如果是移动或设置了删除源，也要删除
                should_delete_stopped = (action_type in ["move", "copy_and_delete"]) or delete_source_after_transfer
                if should_delete_stopped and os.path.exists(filepath):
                    if os.path.isdir(filepath):
                        shutil.rmtree(filepath)
                    else:
                        os.remove(filepath)
                    log_message(f"🗑️  秒传成功及停止本地复制后已删除源: {filename}")
                return True
            target_path = result.get("target_path")
        else:
            target_path = resolve_destination_path(destination_dir, filename, handle_duplicate)
            if action_type == "move":
                shutil.move(filepath, target_path)
            else:
                if os.path.isdir(filepath):
                    shutil.copytree(filepath, target_path)
                else:
                    shutil.copy2(filepath, target_path)

        # 核心逻辑：如果是 'copy_and_delete' 或者是 'copy' 且配置了“删除源文件”，则执行删除
        should_delete = (action_type == "copy_and_delete") or (action_type == "copy" and delete_source_after_transfer)
        if should_delete and os.path.exists(filepath):
            is_dir_now = os.path.isdir(filepath)
            if is_dir_now:
                shutil.rmtree(filepath)
            else:
                os.remove(filepath)
            log_message(f"🗑️  已根据策略 ( {action_type} ) 成功删除源{'目录' if is_dir_now else '文件'}: {filename}")

        log_message(f"✅ 本地{('移动' if action_type == 'move' else '复制')}成功: {filename} -> {target_path}")
        return True
    except Exception as e:
        log_message(f"❌ 本地{action_type}失败: {filename}\n原因: {e}", level="ERROR")
        return False

def monitor_files():
    log_message("文件监控程序已启动。")
    log_message(f"DEBUG 模式: {'[开启]' if DEBUG_MODE else '[关闭]'}")
    monitored_file_states = {}
    
    # 115 客户端（如果配置中提供 cookie 会初始化）
    client_115 = None

    while True:
        config = load_config()
        tasks = config.get("file_monitoring_tasks", [])
        debug_log(f"当前监控任务数: {len(tasks)}")
        # 尝试从配置读取115 cookie
        cookie_115 = config.get('115_cookie') or config.get('web_115_cookie')
        if cookie_115 and Client115:
            if not client_115 or client_115.cookie != cookie_115:
                client_115 = Client115(cookie=cookie_115)
                debug_log("115 客户端已初始化")
        
        for task in tasks:
            source_dir = task.get("source_dir")
            destination_dir = task.get("destination_dir")
            action_type = task.get("action")
            handle_duplicate = task.get("handle_duplicate", "rename")
            enable_mid_copy_check = task.get("enable_mid_copy_check", False)
            mid_copy_check_interval = task.get("mid_copy_check_interval", 30)
            mid_copy_chunk_size = task.get("mid_copy_chunk_size", 8 * 1024 * 1024)
            
            if not source_dir or not os.path.isdir(source_dir): continue

            for filename in os.listdir(source_dir):
                if filename.startswith('.'): continue
                filepath = os.path.join(source_dir, filename)
                
                # 检查是文件还是文件夹
                is_file = os.path.isfile(filepath)
                is_dir = os.path.isdir(filepath)
                if not is_file and not is_dir: continue # 跳过管道等特殊文件
                
                # 对于文件使用大小检测稳定性，对于文件夹使用修改时间作为近似稳定性检测
                current_state = os.path.getsize(filepath) if is_file else os.path.getmtime(filepath)

                if filepath not in monitored_file_states:
                    monitored_file_states[filepath] = {
                        "state": current_state, 
                        "time": time.time(), 
                        "is_dir": is_dir,
                        "last_check": 0, 
                        "last_success": 0
                    }
                    debug_log(f"新{'目录' if is_dir else '文件'}进入监控: {filename} ({'mtime' if is_dir else 'size'}: {current_state})")
                    continue
                
                # 跳过已完成的文件
                if monitored_file_states[filepath].get("completed"):
                    continue
                
                prev = monitored_file_states[filepath]
                if current_state == prev['state'] and (time.time() - prev['time']) >= task.get('stable_time', 10):
                    debug_log(f"检测到稳定{'目录' if is_dir else '文件'}: {filename}")
                    debug_log(f"信息: state={current_state}, stable_time={time.time() - prev['time']:.1f}s")

                    enable_second_transfer = task.get('enable_second_transfer', True)
                    target_cid = (task.get('target_cid') or '').strip()
                    target = f"U_1_{target_cid}" if target_cid else task.get('target', 'U_1_0')
                    
                    # 文件夹无法秒传，跳过 115 秒传逻辑直接本地同步
                    if is_dir or not client_115 or not enable_second_transfer:
                        log_message(f"{'目录跳过秒传' if is_dir else '秒传被禁用 or 未配置'}，执行本地 {action_type} 操作: {filename}")
                        if perform_local_action(
                            filepath,
                            filename,
                            destination_dir,
                            action_type,
                            handle_duplicate,
                            enable_mid_copy_check=enable_mid_copy_check if not is_dir else False,
                            client_115=client_115,
                            file_sha1=None,
                            file_size=current_state if is_file else 0,
                            target=None,
                            check_interval=mid_copy_check_interval,
                            chunk_size=mid_copy_chunk_size,
                            delete_source_after_transfer=task.get('delete_source_after_transfer', False)
                        ):
                            monitored_file_states[filepath] = {"completed": True}
                        continue
                    
                    try:
                        cooldown = task.get('second_transfer_cooldown', 300)
                        now = time.time()
                        state = monitored_file_states.get(filepath, {})
                        
                        if now - state.get('last_check', 0) < cooldown:
                            remaining = int(cooldown - (now - state.get('last_check', 0)))
                            debug_log(f"⏰ 秒传冷却中（剩余{remaining}秒），等待: {filename}")
                            continue
                        
                        # 执行秒传检查（只有文件能进这里，is_dir已经在上方过滤了）
                        debug_log(f"🔍 检查秒传: {filename}")
                        debug_log(f"计算 SHA1: {filename}")
                        sha1 = compute_sha1(filepath)
                        debug_log(f"SHA1: {sha1}, 文件大小: {current_state} bytes, 目标: {target}")
                        check = client_115.check_file_exists(
                            sha1, 
                            current_state, 
                            filename, 
                            target=target,
                            file_path=filepath  # 传入文件路径用于秒传上传
                        )
                        monitored_file_states[filepath]['last_check'] = now
                        debug_log(f"秒传检查结果: {check}")
                        # ...
                        
                        # 秒传成功（文件已在115服务器）
                        if check.get('success') and check.get('can_transfer') and check.get('already_exists'):
                            delete_source_after_transfer = task.get('delete_source_after_transfer', False)
                            # 检查是否已经传输到目标文件夹
                            if check.get('transferred'):
                                # 新的秒传方式：upload_file已经直接传到目标文件夹了
                                log_message(f"✅ 秒传成功！文件已秒传到目标文件夹: {filename} ({action_type})")
                                monitored_file_states[filepath] = {"completed": True}
                                
                                # 核心逻辑：如果是 'move' / 'copy_and_delete' 或者是 'copy' 且配置了“删除源文件”，则执行删除
                                should_delete_after_115 = (action_type in ["move", "copy_and_delete"]) or delete_source_after_transfer
                                if should_delete_after_115 and os.path.exists(filepath):
                                    try:
                                        os.remove(filepath)
                                        log_message(f"🗑️  已根据策略 ( {action_type} ) 删除源文件: {filename}")
                                        debug_log(f"删除文件路径: {filepath}")
                                    except Exception as e:
                                        log_message(f"⚠️  根据策略删除源文件失败: {filename}, 原因: {e}", level="ERROR")
                            else:
                                # 旧方式：需要复制
                                log_message(f"✅ 秒传成功，文件已在115: {filename} ({action_type})")
                                
                                # 获取file_id并复制到目标文件夹
                                file_id = check.get('file_id')
                                if file_id:
                                    log_message(f"📋 找到File ID: {file_id}，开始复制到目标文件夹...")
                                    
                                    # 调用copy_file_to_folder将文件添加到目标文件夹
                                    copy_result = client_115.copy_file_to_folder(
                                        file_id=file_id,
                                        target_cid=target_cid,  # 使用原始CID
                                        file_name=filename
                                    )
                                    
                                    if copy_result.get('success') and copy_result.get('transferred'):
                                        log_message(f"✅ 完整秒传成功！文件已添加到目标文件夹: {filename}")
                                        monitored_file_states[filepath] = {"completed": True}
                                        
                                        # 秒传成功后删除源文件
                                        if should_delete_after_115 and os.path.exists(filepath):
                                            try:
                                                os.remove(filepath)
                                                log_message(f"🗑️  已根据策略 ( {action_type} ) 删除源文件: {filename}")
                                                debug_log(f"删除文件路径: {filepath}")
                                            except Exception as e:
                                                log_message(f"⚠️  根据策略删除源文件失败: {filename}, 原因: {e}", level="ERROR")
                                    else:
                                        error_msg = copy_result.get('message', '未知错误')
                                        log_message(f"❌ 文件复制失败: {filename}\n原因: {error_msg}", level="ERROR")
                                        log_message(f"⚠️  文件ID已找到但未能添加到目标文件夹，保留源文件")
                                else:
                                    log_message(f"⚠️  秒传成功但未返回file_id，保留源文件: {filename}")
                                
                        else:
                            # 秒传失败（文件不在115），执行本地同步
                            log_message(f"⚠️  秒传失败，执行本地 {action_type} 操作: {filename}")
                            if perform_local_action(
                                filepath,
                                filename,
                                destination_dir,
                                action_type,
                                handle_duplicate,
                                enable_mid_copy_check=enable_mid_copy_check,
                                client_115=client_115,
                                file_sha1=sha1,
                                file_size=current_state,
                                target=target,
                                check_interval=mid_copy_check_interval,
                                chunk_size=mid_copy_chunk_size,
                                delete_source_after_transfer=task.get('delete_source_after_transfer', False)
                            ):
                                monitored_file_states[filepath] = {"completed": True}
                    except Exception as e:
                        log_message(f"秒传检查异常，保留源文件: {e}")
                else:
                    # 更新状态，如果状态没变则保持 initial time
                    monitored_file_states[filepath] = {
                        "state": current_state, 
                        "time": prev['time'] if current_state == prev['state'] else time.time(),
                        "is_dir": is_dir,
                        "last_check": prev.get('last_check', 0),
                        "last_success": prev.get('last_success', 0)
                    }
        
        time.sleep(5)

if __name__ == '__main__':
    monitor_files()
