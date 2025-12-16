import pandas as pd
import sqlite3
import pytz
import os
from datetime import datetime, timedelta
import config 

class ArbitrageEngine:
    def __init__(self):
        """
        全能套利分析引擎 (全字段完整版)
        包含：
        1. 价格维：成交价 -> 入场价 -> 离场价
        2. 时间维：成交时间 -> 入场间隔 -> 持有时长 -> 离场间隔
        3. 结果维：套利空间%、形态描述
        """
        self.conn = sqlite3.connect(config.DB_NAME)
        self.tz_cn = pytz.timezone('Asia/Shanghai')
        self.tz_us = pytz.timezone('America/New_York')
        self.bad_tick_threshold = 0.20

    def _convert_time(self, time_str):
        try:
            if isinstance(time_str, pd.Timestamp):
                return time_str.tz_convert(self.tz_us)
            
            dt_cn = datetime.strptime(time_str, "%Y/%m/%d %H:%M")
            dt_cn = self.tz_cn.localize(dt_cn)
            dt_us = dt_cn.astimezone(self.tz_us)
            return dt_us
        except:
            return None

    def _calc_duration(self, start, end):
        """计算时间间隔，返回 +12m 格式"""
        if not start or not end: return "-"
        delta = (end - start).total_seconds()
        if delta < 0: return "+0m"
        
        hours, remainder = divmod(delta, 3600)
        minutes, _ = divmod(remainder, 60)
        
        if hours > 0:
            return f"+{int(hours)}h{int(minutes)}m"
        else:
            return f"+{int(minutes)}m"

    def get_market_data(self, ticker, trade_time):
        table_name = f"stock_2m_{ticker.replace('-', '_')}"
        trade_date_str = trade_time.strftime('%Y-%m-%d')
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                return pd.DataFrame()

            query = f"SELECT * FROM {table_name} WHERE substr(Datetime, 1, 10) = '{trade_date_str}'"
            df = pd.read_sql(query, self.conn, parse_dates=['Datetime'])
            
            if df.empty:
                return pd.DataFrame()

            if df['Datetime'].dt.tz is None:
                df['Datetime'] = df['Datetime'].dt.tz_localize(self.tz_us)
            else:
                df['Datetime'] = df['Datetime'].dt.tz_convert(self.tz_us)
                
            return df
        except Exception as e:
            return pd.DataFrame()

    def run_analysis(self, csv_path, start_date="2025-11-27", window_minutes=30):
        print(f"🚀 [全能分析引擎] 启动 | 窗口: {window_minutes}分钟 | 起始: {start_date}")
        
        df_trades = pd.read_csv(csv_path)
        cutoff_time = self.tz_us.localize(datetime.strptime(start_date, "%Y-%m-%d"))
        
        results = []

        for index, row in df_trades.iterrows():
            ticker = row['交易标的']
            action = row['交易方向']
            price = row['交易价格']
            time_str = row['交易时间']
            
            dt_us = self._convert_time(time_str)
            if not dt_us or dt_us < cutoff_time:
                continue

            # 1. 获取行情
            df_kline = self.get_market_data(ticker, dt_us)
            if df_kline.empty:
                continue
            
            # 2. 截取 X分钟 窗口
            window_end = dt_us + timedelta(minutes=window_minutes)
            df_window = df_kline[
                (df_kline['Datetime'] > dt_us) & 
                (df_kline['Datetime'] <= window_end)
            ]
            
            if df_window.empty:
                continue

            # 3. 脏数据过滤
            lower = price * (1 - self.bad_tick_threshold)
            upper = price * (1 + self.bad_tick_threshold)
            df_window = df_window[(df_window['Low'] > lower) & (df_window['High'] < upper)]
            
            if df_window.empty:
                continue

            # =========================================================
            # PART A: 顺势波动统计
            # =========================================================
            trend_extreme = price
            trend_pct = 0
            trend_time_str = "-"
            trend_wait_str = "-"
            
            if action == 'B':
                # 买入看涨
                max_idx = df_window['High'].idxmax()
                trend_extreme = df_window.loc[max_idx]['High']
                trend_time_obj = df_window.loc[max_idx]['Datetime']
                if trend_extreme > price:
                    trend_pct = (trend_extreme - price) / price * 100
                    trend_time_str = trend_time_obj.strftime('%H:%M')
                    trend_wait_str = self._calc_duration(dt_us, trend_time_obj)
            elif action == 'S':
                # 卖出看跌
                min_idx = df_window['Low'].idxmin()
                trend_extreme = df_window.loc[min_idx]['Low']
                trend_time_obj = df_window.loc[min_idx]['Datetime']
                if trend_extreme < price:
                    trend_pct = (price - trend_extreme) / price * 100 
                    trend_time_str = trend_time_obj.strftime('%H:%M')
                    trend_wait_str = self._calc_duration(dt_us, trend_time_obj)

            # =========================================================
            # PART B: 逆势波段套利
            # =========================================================
            arb_entry = 0
            arb_exit = 0
            arb_pct = 0
            arb_note = ""
            
            # 时间字段
            entry_time_str = "-"
            exit_time_str = "-"
            entry_gap_str = "-" # 入场间隔
            exit_gap_str = "-"  # 离场间隔
            hold_str = "-"      # 持仓时长

            if action == 'S': 
                # 卖出 -> 找地板(Min) -> 找天花板(Max)
                min_idx = df_window['Low'].idxmin()
                arb_entry = df_window.loc[min_idx]['Low']
                entry_time = df_window.loc[min_idx]['Datetime']
                
                entry_time_str = entry_time.strftime('%H:%M')
                entry_gap_str = self._calc_duration(dt_us, entry_time)
                
                df_after = df_window[df_window['Datetime'] > entry_time]
                
                if not df_after.empty:
                    max_idx_after = df_after['High'].idxmax()
                    arb_exit = df_after.loc[max_idx_after]['High']
                    exit_time = df_after.loc[max_idx_after]['Datetime']
                    
                    arb_pct = ((arb_exit - arb_entry) / arb_entry) * 100
                    arb_note = "触底反弹"
                    
                    exit_time_str = exit_time.strftime('%H:%M')
                    hold_str = self._calc_duration(entry_time, exit_time)
                    exit_gap_str = self._calc_duration(dt_us, exit_time)
                else:
                    arb_exit = arb_entry
                    arb_note = "单边下跌"

            elif action == 'B':
                # 买入 -> 找天花板(Max) -> 找地板(Min)
                max_idx = df_window['High'].idxmax()
                arb_entry = df_window.loc[max_idx]['High']
                entry_time = df_window.loc[max_idx]['Datetime']
                
                entry_time_str = entry_time.strftime('%H:%M')
                entry_gap_str = self._calc_duration(dt_us, entry_time)
                
                df_after = df_window[df_window['Datetime'] > entry_time]
                
                if not df_after.empty:
                    min_idx_after = df_after['Low'].idxmin()
                    arb_exit = df_after.loc[min_idx_after]['Low']
                    exit_time = df_after.loc[min_idx_after]['Datetime']
                    
                    arb_pct = ((arb_entry - arb_exit) / arb_entry) * 100
                    arb_note = "冲高回落"
                    
                    exit_time_str = exit_time.strftime('%H:%M')
                    hold_str = self._calc_duration(entry_time, exit_time)
                    exit_gap_str = self._calc_duration(dt_us, exit_time)
                else:
                    arb_exit = arb_entry
                    arb_note = "单边上涨"

            # =========================================================

            results.append({
                '日期': dt_us.strftime('%m-%d'),
                '标的': ticker,
                '方向': action,
                '成交时间': dt_us.strftime('%H:%M'),
                '成交价': price, # 1. 实际成交价格
                
                # 顺势
                '顺势空间%': round(trend_pct, 2),
                
                # 逆势套利 - 价格
                '入场价格': round(arb_entry, 2), # 2. 入场价格
                '离场价格': round(arb_exit, 2),  # 3. 离场价格
                '波段套利%': round(arb_pct, 2),
                
                # 逆势套利 - 时间
                '入场间隔': entry_gap_str,
                '波段持有': hold_str,
                '离场间隔': exit_gap_str,
                
                # 形态
                '波段形态': arb_note, # 4. 波段形态
                
                # 辅助排序
                '_排序时间戳': dt_us 
            })

        df_res = pd.DataFrame(results)
        
        if not df_res.empty:
            df_res = df_res.sort_values(by='_排序时间戳', ascending=False)
            df_res = df_res.drop(columns=['_排序时间戳'])
            
        return df_res

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    csv_file = "robin交易记录 - 多空对冲AI识别.csv"
    engine = ArbitrageEngine()
    
    # 运行分析
    df = engine.run_analysis(csv_file, start_date="2025-10-15", window_minutes=60)
    
    if not df.empty:
        print(f"\n📊 [全能分析报告 - 含价格与形态]")
        print("-" * 160)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1500)
        
        # 打印列配置：涵盖你要求的所有信息
        cols = [
            '日期', '标的', '方向', 
            '成交价', '成交时间', 
            '入场间隔', '入场价格', 
            '离场价格', '离场间隔', 
            '波段套利%', '波段形态'
        ]
        print(df[cols].head(15).to_string(index=False))
        print("-" * 160)
        
        df.to_csv("全能分析报告.csv", index=False, encoding='utf-8-sig')
        print("✅ 结果已保存至: 全能分析报告.csv")
    else:
        print("无数据")
    
    engine.close()