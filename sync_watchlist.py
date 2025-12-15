import pandas as pd
import config
import re
import os
import json

def update_config_watchlist(csv_path):
    """
    读取 CSV 中的交易标的，自动更新到 config.py 的 WATCHLIST 中
    """
    print(f"📂 正在检查新股票: {csv_path} ...")
    
    # 1. 读取 CSV 中的股票代码
    try:
        df = pd.read_csv(csv_path)
        if '交易标的' not in df.columns:
            print("❌ CSV 文件中未找到 '交易标的' 列，跳过更新。")
            return
        
        # 提取股票代码，去重，并转为大写
        new_tickers = set(df['交易标的'].dropna().unique())
        # 剔除可能存在的非股票字符（视情况而定）
        new_tickers = {x.strip().upper() for x in new_tickers if isinstance(x, str)}
        
    except Exception as e:
        print(f"❌ 读取 CSV 失败: {e}")
        return

    # 2. 读取 config.py 中现有的 WATCHLIST
    current_watchlist = set(config.WATCHLIST)
    
    # 3. 找出新增的股票 (差集)
    diff = new_tickers - current_watchlist
    
    if not diff:
        print("✅ 没有发现新股票，config.py 无需更新。")
        return

    print(f"🆕 发现 {len(diff)} 只新股票: {diff}")
    
    # 合并并排序 (保持列表整洁)
    final_list = sorted(list(current_watchlist.union(new_tickers)))

    # 4. 原地修改 config.py 文件
    config_path = "config.py"
    
    with open(config_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 使用 json.dumps 将列表转换为格式化的字符串
    # ensure_ascii=False 允许中文注释(虽然这里是股票代码)
    # indent=4 让生成的列表换行缩进，更美观
    list_str = json.dumps(final_list, indent=4).replace('"', "'") # 把双引号换成单引号，符合Python习惯
    
    # 构造新的 WATCHLIST 字符串
    new_block = f"WATCHLIST = {list_str}"

    # === 正则替换 ===
    # 匹配模式：WATCHLIST = [ ... ] (支持多行)
    # re.DOTALL 让 . 能够匹配换行符
    pattern = r"WATCHLIST\s*=\s*\[.*?\]"
    
    if re.search(pattern, content, re.DOTALL):
        new_content = re.sub(pattern, new_block, content, flags=re.DOTALL)
        
        # 写入文件
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
            
        print(f"🚀 已成功将 {len(diff)} 只新股票写入 {config_path}！")
        print(f"📊 当前监控总数: {len(final_list)}")
        
        # 提示用户重新加载
        print("⚠️ 注意: 请重启主程序以加载新的配置。")
    else:
        print("❌ 未能在 config.py 中找到 WATCHLIST 变量，请手动检查文件格式。")

if __name__ == "__main__":
    # 在这里指定你要扫描的 CSV 文件路径
    target_csv = "robin交易记录 - 多空对冲AI识别.csv"
    
    if os.path.exists(target_csv):
        update_config_watchlist(target_csv)
    else:
        print(f"❌ 找不到文件: {target_csv}")