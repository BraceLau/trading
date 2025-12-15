import pandas as pd
import sqlite3
import pytz
import os
from datetime import datetime, timedelta
import config  
from data_engine import StockDataEngine 

# 结果保存路径
OUTPUT_FILE = "交易复盘_全量报告.csv"

class TradeReviewer:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.conn = sqlite3.connect(config.DB_NAME)
        self.tz_cn = pytz.timezone('Asia/Shanghai')
        self.tz_us = pytz.timezone('America/New_York')
        
        # 设定脏数据过滤阈值 (20%)
        self.bad_tick_threshold = 0.20

    def _convert_time(self, time_str):
        try:
            dt_cn = datetime.strptime(time_str, "%Y/%m/%d %H:%M")
            dt_cn = self.tz_cn.localize(dt_cn)
            dt_us = dt_cn.astimezone(self.tz_us)
            return dt_us
        except:
            return None

    def _generate_fingerprint(self, row):
        return f"{row['交易标的']}_{row['交易方向']}_{row['交易价格']}_{row['交易时间']}"

    def get_processed_fingerprints(self):
        if not os.path.exists(OUTPUT_FILE):
            return set()
        try:
            df_existing = pd.read_csv(OUTPUT_FILE)
            if '指纹' not in df_existing.columns:
                return set()
            return set(df_existing['指纹'].astype(str))
        except:
            return set()

    def load_and_sync_data(self):
        print(f"📂 读取交易记录: {self.csv_path} ...")
        self.df_trades = pd.read_csv(self.csv_path)
        
        # =========================================================
        # 🔥 修改核心：设定固定的复盘起始日
        # =========================================================
        start_date_str = "2025-11-16"
        
        # 1. 解析日期
        cutoff_time = datetime.strptime(start_date_str, "%Y-%m-%d")
        
        # 2. 赋予时区 (美东时间 00:00:00)
        cutoff_time = self.tz_us.localize(cutoff_time)
        
        now_us = datetime.now(self.tz_us)
        
        print(f"📅 当前美东时间: {now_us.strftime('%Y-%m-%d %H:%M')}")
        print(f"🏁 复盘起始日期: {cutoff_time.strftime('%Y-%m-%d')} (固定)")
        print(f"✂️ 将忽略 {start_date_str} 之前的所有旧交易")
        
        # =========================================================

        # === 增量筛选：剔除已分析过的 & 太久远的 ===
        processed_fingerprints = self.get_processed_fingerprints()
        new_trades = []
        
        for index, row in self.df_trades.iterrows():
            # 1. 时间筛选：只看固定日期之后的
            dt_us = self._convert_time(row['交易时间'])
            if not dt_us or dt_us < cutoff_time:
                continue 
            
            # 2. 指纹筛选：只看没分析过的
            fp = self._generate_fingerprint(row)
            if fp not in processed_fingerprints:
                row['指纹'] = fp
                row['dt_us'] = dt_us 
                new_trades.append(row)
        
        self.df_new = pd.DataFrame(new_trades)
        
        if self.df_new.empty:
            print("✅ 无新增交易，指定日期后的交易均已复盘。")
            return

        print(f"🆕 发现 {len(self.df_new)} 条待分析交易...")

        # === 同步数据 ===
        # Yahoo 限制: 只能下最近60天的2m数据
        # 只有当新交易在这个范围内时，才去尝试下载
        download_cutoff = now_us - timedelta(days=60)
        tickers_to_sync = set()
        
        for index, row in self.df_new.iterrows():
            if row['dt_us'] > download_cutoff:
                tickers_to_sync.add(row['交易标的'])

        if tickers_to_sync:
            print(f"⬇️ 尝试同步 {len(tickers_to_sync)} 只股票数据...")
            engine = StockDataEngine()
            engine.update_minute_data(target_tickers=list(tickers_to_sync))
            engine.close()

    def analyze(self):
        if not hasattr(self, 'df_new') or self.df_new.empty:
            return pd.DataFrame()

        print("🔎 开始增量分析 (严格日内模式 | 剔除脏数据)...")
        results = []

        for index, row in self.df_new.iterrows():
            ticker = row['交易标的']
            action = row['交易方向']
            exec_price = row['交易价格']
            dt_us = row['dt_us']
            
            table_name = f"stock_2m_{ticker.replace('-', '_')}"
            try:
                # 1. 检查表
                check_cursor = self.conn.cursor()
                check_cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                if not check_cursor.fetchone():
                    results.append(self._make_result(row, dt_us, "无历史数据", 0, 0, None, None, None))
                    continue

                # 2. 只查当天
                trade_date_str = dt_us.strftime('%Y-%m-%d')
                query = f"""
                    SELECT * FROM {table_name} 
                    WHERE substr(Datetime, 1, 10) = '{trade_date_str}'
                """
                df_kline = pd.read_sql(query, self.conn, parse_dates=['Datetime'])
                
                if df_kline.empty:
                    results.append(self._make_result(row, dt_us, "数据缺失", 0, 0, None, None, None))
                    continue
                
                if df_kline['Datetime'].dt.tz is None:
                     df_kline['Datetime'] = df_kline['Datetime'].dt.tz_localize(self.tz_us)
                else:
                     df_kline['Datetime'] = df_kline['Datetime'].dt.tz_convert(self.tz_us)

                # 3. 筛选后续行情
                df_future = df_kline[df_kline['Datetime'] > dt_us]
                
                if df_future.empty:
                    results.append(self._make_result(row, dt_us, "无后续行情(尾盘)", 0, 0, None, None, None))
                    continue

                # 4. 脏数据清洗 (20% 阈值)
                lower_bound = exec_price * (1 - self.bad_tick_threshold)
                upper_bound = exec_price * (1 + self.bad_tick_threshold)

                df_future = df_future[
                    (df_future['Low'] > lower_bound) & 
                    (df_future['High'] < upper_bound)
                ]

                if df_future.empty:
                    results.append(self._make_result(row, dt_us, "数据异常(已清洗)", 0, 0, None, None, None))
                    continue

                # 5. 寻找最优
                better_price_found = False
                best_price = exec_price
                diff = 0
                best_time = None
                first_better_time = None
                
                if action == 'B':
                    min_price = df_future['Low'].min()
                    if min_price < exec_price:
                        better_price_found = True
                        best_price = min_price
                        diff = exec_price - min_price
                        best_time = df_future.loc[df_future['Low'].idxmin()]['Datetime']
                        better_rows = df_future[df_future['Low'] < exec_price]
                        if not better_rows.empty:
                            first_better_time = better_rows.iloc[0]['Datetime']

                elif action == 'S':
                    max_price = df_future['High'].max()
                    if max_price > exec_price:
                        better_price_found = True
                        best_price = max_price
                        diff = max_price - exec_price
                        best_time = df_future.loc[df_future['High'].idxmax()]['Datetime']
                        better_rows = df_future[df_future['High'] > exec_price]
                        if not better_rows.empty:
                            first_better_time = better_rows.iloc[0]['Datetime']

                status = "❌ 过早行动" if better_price_found else "✅ 完美操作"
                pct = (diff / exec_price * 100) if better_price_found else 0
                
                results.append(self._make_result(
                    row, dt_us, status, best_price, pct, 
                    best_time, dt_us, first_better_time
                ))

            except Exception as e:
                results.append(self._make_result(row, dt_us, f"错误: {str(e)}", 0, 0, None, None, None))

        return pd.DataFrame(results)

    def _calculate_duration(self, start_time, end_time):
        if not start_time or not end_time: return "-"
        delta_seconds = (end_time - start_time).total_seconds()
        if delta_seconds <= 0: return "+0m"
        hours, remainder = divmod(delta_seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        return f"+{int(hours)}h {int(minutes)}m" if hours > 0 else f"+{int(minutes)}m"

    def _make_result(self, row, dt_us, status, best_price, diff_pct, best_time, trade_time, first_better_time):
        res = {
            '日期': dt_us.strftime('%m-%d') if dt_us else row['交易时间'][:10],
            '标的': row['交易标的'],
            '方向': row['交易方向'],
            '实际成交': row['交易价格'],
            '实际时间': dt_us.strftime('%H:%M') if dt_us else "-",
            '指纹': row['指纹'] 
        }

        if best_time and trade_time:
            wait_for_best = self._calculate_duration(trade_time, best_time)
            wait_for_first = self._calculate_duration(trade_time, first_better_time) if first_better_time else "-"
            
            res.update({
                '最短等待': wait_for_first,
                '最短等待时间点': first_better_time.strftime('%H:%M') if first_better_time else "-",
                '最佳等待': wait_for_best,
                '最优时间点': best_time.strftime('%H:%M'),
                '最优价格': round(best_price, 2),
                '错失空间%': round(diff_pct, 2),
            })
        else:
            res.update({
                '最短等待': "-", '最短等待时间点': "-",
                '最佳等待': "-", '最优时间点': "-",
                '最优价格': "-", '错失空间%': 0,
            })

        res['评估'] = status
        res['原始时间'] = row['交易时间']
        return res
    
    def close(self):
        self.conn.close()

if __name__ == "__main__":
    csv_file = "robin交易记录 - 多空对冲AI识别.csv"
    
    reviewer = TradeReviewer(csv_file)
    reviewer.load_and_sync_data()
    df_new_result = reviewer.analyze()
    
    if not df_new_result.empty:
        significant = df_new_result[df_new_result['错失空间%'] > 0.0].sort_values(by='错失空间%', ascending=False)
        print("\n📊 [本次增量复盘结果 (2025-11-27后)]")
        print("-" * 140)
        cols = ['日期', '标的', '方向', '实际成交', '实际时间', '最短等待', '最优时间点', '错失空间%']
        print(significant[cols].head(5).to_string(index=False))
        print("-" * 140)
        
        file_exists = os.path.exists(OUTPUT_FILE)
        df_new_result.to_csv(OUTPUT_FILE, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
        print(f"\n✅ 已追加 {len(df_new_result)} 条记录")
    else:
        print("\n💤 无需处理")
    
    reviewer.close()