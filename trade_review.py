import pandas as pd
import numpy as np
import sqlite3
import config
from datetime import datetime, timedelta
import pytz
from data_engine import StockDataEngine 

class TradeReviewer:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.conn = sqlite3.connect(config.DB_NAME)
        # 时区定义
        self.tz_cn = pytz.timezone('Asia/Shanghai')
        self.tz_us = pytz.timezone('America/New_York')

    def _convert_time(self, time_str):
        """解析中国时间字符串 -> 美东时间 (带时区)"""
        try:
            dt_cn = datetime.strptime(time_str, "%Y/%m/%d %H:%M")
            dt_cn = self.tz_cn.localize(dt_cn)
            dt_us = dt_cn.astimezone(self.tz_us)
            return dt_us
        except:
            return None

    def load_and_sync_data(self):
        print(f"📂 读取交易记录: {self.csv_path} ...")
        self.df_trades = pd.read_csv(self.csv_path)
        
        # === 1. 预处理：转换时间并过滤最近7天 ===
        # 计算7天前的截止时间 (美东时间)
        now_us = datetime.now(self.tz_us)
        cutoff_time = now_us - timedelta(days=7)
        
        print(f"📅 当前美东时间: {now_us.strftime('%Y-%m-%d %H:%M')}")
        print(f"✂️ 过滤截止时间: {cutoff_time.strftime('%Y-%m-%d %H:%M')} (只保留此后的交易)")

        # 临时列表保存有效交易
        valid_trades = []
        
        for index, row in self.df_trades.iterrows():
            dt_us = self._convert_time(row['交易时间'])
            if dt_us and dt_us > cutoff_time:
                # 把转换好的美东时间存进去，方便后面用
                row['dt_us'] = dt_us
                valid_trades.append(row)
        
        self.df_valid = pd.DataFrame(valid_trades)
        
        if self.df_valid.empty:
            print("⚠️ 警告: 最近7天内没有发现交易记录。")
            return

        print(f"✅ 过滤完成: 原记录 {len(self.df_trades)} 条 -> 有效记录 {len(self.df_valid)} 条")

        # === 2. 提取需要下载的股票代码 ===
        unique_tickers = self.df_valid['交易标的'].unique().tolist()
        
        # === 3. 调用数据引擎 (只下载缺失的) ===
        engine = StockDataEngine()
        engine.update_minute_data(target_tickers=unique_tickers)
        engine.close()

    def analyze(self):
        if not hasattr(self, 'df_valid') or self.df_valid.empty:
            return pd.DataFrame()

        print("🔎 开始回溯分析 (仅向后查找)...")
        results = []

        for index, row in self.df_valid.iterrows():
            ticker = row['交易标的']
            action = row['交易方向']
            exec_price = row['交易价格']
            trade_time = row['dt_us'] # 已经是美东时间对象
            
            # 读取分钟线
            table_name = f"stock_1m_{ticker.replace('-', '_')}"
            try:
                query = f"SELECT * FROM {table_name}"
                df_kline = pd.read_sql(query, self.conn, parse_dates=['Datetime'])
                
                if df_kline.empty:
                    results.append(self._make_result(row, "无数据", 0, 0, None, None))
                    continue
                
                # 时区标准化 (确保数据库读出来的时间也有时区)
                if df_kline['Datetime'].dt.tz is None:
                     df_kline['Datetime'] = df_kline['Datetime'].dt.tz_localize(self.tz_us)
                else:
                     df_kline['Datetime'] = df_kline['Datetime'].dt.tz_convert(self.tz_us)

                # === 核心逻辑：只看交易之后的时间 ===
                # 1. 筛选当天的K线 (避免跨日对比)
                trade_date = trade_time.date()
                df_today = df_kline[df_kline['Datetime'].dt.date == trade_date]
                
                # 2. 筛选交易时间之后的K线
                df_future = df_today[df_today['Datetime'] > trade_time]
                
                if df_future.empty:
                    results.append(self._make_result(row, "无后续行情(尾盘)", 0, 0, None, None))
                    continue

                # === 寻找最佳价格 ===
                better_price_found = False
                best_price = 0
                diff = 0
                best_time = None
                
                if action == 'B': # 买入：找之后的最低价 (Low)
                    min_price = df_future['Low'].min()
                    # 找到最小值发生的第一行
                    best_row = df_future.loc[df_future['Low'].idxmin()]
                    
                    if min_price < exec_price:
                        better_price_found = True
                        best_price = min_price
                        diff = exec_price - min_price
                        best_time = best_row['Datetime']
                        
                elif action == 'S': # 卖出：找之后的最高价 (High)
                    max_price = df_future['High'].max()
                    best_row = df_future.loc[df_future['High'].idxmax()]
                    
                    if max_price > exec_price:
                        better_price_found = True
                        best_price = max_price
                        diff = max_price - exec_price
                        best_time = best_row['Datetime']

                # 记录
                results.append(self._make_result(row, 
                                                 "❌ 哪怕再等一会" if better_price_found else "✅ 卖在最高/买在最低", 
                                                 best_price if better_price_found else exec_price, 
                                                 (diff / exec_price * 100) if better_price_found else 0,
                                                 best_time if better_price_found else trade_time,
                                                 trade_time)) # 传入实际交易时间用于计算间隔

            except Exception as e:
                print(f"Error {ticker}: {e}")

        return pd.DataFrame(results)

    def _make_result(self, row, status, best_price, diff_pct, best_time, trade_time):
        # 计算时间间隔
        interval_str = "-"
        best_time_str = "-"
        
        if best_time and trade_time and best_time != trade_time:
            # 计算秒数差
            delta_seconds = (best_time - trade_time).total_seconds()
            hours, remainder = divmod(delta_seconds, 3600)
            minutes, _ = divmod(remainder, 60)
            
            # 格式化间隔: "+1h 30m"
            if hours > 0:
                interval_str = f"+{int(hours)}h {int(minutes)}m"
            else:
                interval_str = f"+{int(minutes)}m"
            
            best_time_str = best_time.strftime('%H:%M')

        return {
            '日期': row['dt_us'].strftime('%m-%d'),
            '标的': row['交易标的'],
            '方向': row['交易方向'],
            '实际成交': row['交易价格'],
            '实际时间': row['dt_us'].strftime('%H:%M'),
            '最优价格': round(best_price, 2),
            '最优时间': best_time_str,
            '需等待时长': interval_str, # 新增列：如果等一会，需要等多久
            '错失空间%': round(diff_pct, 2),
            '评估': status
        }
    
    def close(self):
        self.conn.close()

if __name__ == "__main__":
    csv_file = "robin交易记录 - 多空对冲AI识别.csv"
    
    reviewer = TradeReviewer(csv_file)
    reviewer.load_and_sync_data()
    df_result = reviewer.analyze()
    
    if not df_result.empty:
        # 筛选出真正有优化空间的 (错失空间 > 0.5% 才有分析意义，太小的可能是噪音)
        significant_misses = df_result[df_result['错失空间%'] > 0.0].sort_values(by='错失空间%', ascending=False)
        
        print("\n📊 [交易复盘 - 最佳时间点分析]")
        print("-" * 100)
        # 调整列顺序，更符合阅读习惯
        cols = ['日期', '标的', '方向', '实际成交', '实际时间', '最优价格', '最优时间', '需等待时长', '错失空间%']
        print(significant_misses[cols].head(15).to_string(index=False))
        print("-" * 100)
        
        df_result.to_csv("交易复盘_时间优化版.csv", index=False, encoding='utf-8-sig')
        print("✅ 结果已保存")
    
    reviewer.close()