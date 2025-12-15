import os
import sys
import datetime
import traceback
import subprocess

# 引入之前的模块
# 确保 sync_watchlist.py 和 data_engine.py 在同一目录下
try:
    from sync_watchlist import update_config_watchlist
    from data_engine import StockDataEngine
    import config
    import importlib
except ImportError as e:
    print(f"❌ 模块导入失败: {e}")
    sys.exit(1)

# ================= 配置区 =================
# 你的 CSV 文件夹路径 (脚本会自动找最新的 CSV 同步)
CSV_FOLDER_PATH = "/Users/liuyuming/Desktop/agent" 
# ========================================

def send_notification(title, message):
    """发送 macOS 系统通知"""
    try:
        # 使用 AppleScript 发送桌面通知
        script = f'display notification "{message}" with title "{title}" sound name "Glass"'
        subprocess.run(["osascript", "-e", script])
        print(f"🔔 [通知已发送] {title}: {message}")
    except Exception as e:
        print(f"⚠️ 通知发送失败: {e}")

def find_latest_csv(folder_path):
    """找到文件夹里最新修改的 csv 文件"""
    try:
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv') and '交易记录' in f]
        if not files:
            return None
        # 按修改时间排序，取最新的
        latest_file = max(files, key=os.path.getmtime)
        return latest_file
    except Exception:
        return None

def main():
    start_time = datetime.datetime.now()
    log_msg = []
    
    try:
        print("🚀 === 每日自动更新任务开始 ===")
        
        # --- 1. 自动同步 Watchlist (可选) ---
        latest_csv = find_latest_csv(CSV_FOLDER_PATH)
        if latest_csv:
            print(f"📂 发现最新交易记录: {os.path.basename(latest_csv)}")
            update_config_watchlist(latest_csv)
            # 重新加载 config 以生效
            importlib.reload(config)
            log_msg.append(f"✅ 同步自选股: {os.path.basename(latest_csv)}")
        else:
            print("ℹ️ 未找到交易记录CSV，跳过同步。")
        
        # --- 2. 启动数据引擎 ---
        engine = StockDataEngine()
        
        # 更新日线
        print("📅 更新日线数据...")
        engine.update_all() # 你的日线更新函数名可能是 update_all 或 update_daily_data，请核对
        
        # 更新分钟线 (智能增量模式)
        print("⏱️ 更新分钟数据...")
        engine.update_minute_data()
        
        engine.close()
        
        duration = datetime.datetime.now() - start_time
        success_msg = f"耗时 {duration.seconds} 秒 | 股票池 {len(config.WATCHLIST)} 只"
        log_msg.append("✅ 数据更新完成")
        
        print("✨ === 任务圆满结束 ===")
        
        # 发送成功通知
        final_text = "\n".join(log_msg)
        send_notification("股票数据更新成功", f"{success_msg}")

    except Exception as e:
        # 捕获所有错误
        error_msg = str(e)
        traceback.print_exc()
        
        # 发送失败通知
        send_notification("❌ 股票更新失败", f"错误: {error_msg}")

if __name__ == "__main__":
    # 切换到脚本所在目录 (防止 crontab 路径错误)
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()