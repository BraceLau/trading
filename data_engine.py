import yfinance as yf
import pandas as pd
import sqlite3
import os
import time
import config  # 直接引用同目录下的 config.py

# ==========================================
# 代理设置 (按需开启，如果不需要请注释掉)
# ==========================================
proxy = "http://127.0.0.1:7890"
os.environ['HTTP_PROXY'] = proxy
os.environ['HTTPS_PROXY'] = proxy

class StockDataEngine:
    def __init__(self):
        # 使用 config.DB_NAME 连接数据库
        self.conn = sqlite3.connect(config.DB_NAME)

    def _flatten_columns(self, df):
        """处理 yfinance 的 MultiIndex 列名 (Price, Ticker) -> Price"""
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df

    def get_db_last_timestamp(self, table_name):
        """获取数据库中某张表的最晚时间戳"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                return None
            
            query = f"SELECT MAX(Datetime) FROM {table_name}"
            result = pd.read_sql(query, self.conn)
            last_time = result.iloc[0, 0]
            
            if last_time:
                return pd.to_datetime(last_time)
            return None
        except Exception:
            return None

    def _calculate_indicators(self, df):
        """日线指标计算 (仅用于 update_all)"""
        if len(df) < 2: return df
        # 简单示例，如需完整指标请把之前的代码贴回来
        for span in [5, 10, 20, 60, 120, 200]:
            df[f'EMA{span}'] = df['Close'].ewm(span=span, adjust=False).mean()
        # ... 其他指标逻辑 ...
        return df

    def update_all(self):
        """
        [日线更新] 保持不变，用于长期趋势分析
        """
        print(f"🔄 [日线更新] 正在批量更新 {len(config.WATCHLIST)} 只股票...")
        try:
            tickers_str = " ".join(config.WATCHLIST)
            # 日线数据量小，直接下 2 年
            all_data = yf.download(tickers_str, period="2y", interval="1d", group_by='ticker', auto_adjust=True, progress=True)
            
            if all_data.empty: return

            for ticker in config.WATCHLIST:
                try:
                    if ticker not in all_data.columns.levels[0]: continue
                    df = all_data[ticker].copy()
                    if df.empty: continue
                    
                    df = df[df['Volume'] > 0].copy()
                    df = self._flatten_columns(df)
                    
                    # 日线我们通常需要计算指标
                    df = self._calculate_indicators(df) 
                    
                    df.reset_index(inplace=True)
                    df['Ticker'] = ticker
                    df.columns = [str(c).replace(' ', '_') for c in df.columns]
                    
                    table_name = f"stock_{ticker.replace('-', '_')}"
                    df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                except:
                    continue
            print("✅ 日线数据更新完成！")
        except Exception as e:
            print(f"❌ 批量下载严重错误: {e}")

    def update_minute_data(self, target_tickers=None):
        """
        [2分钟级智能更新]
        1. 新股票 -> 下载 60天 (充分利用 2m 优势，最大化历史回溯)
        2. 老股票 -> 下载 5天 (即使隔个周末也没事，且速度快)
        """
        if target_tickers is None:
            download_list = config.WATCHLIST
        else:
            download_list = target_tickers

        print(f"⏱️ [2分钟线更新] 准备扫描 {len(download_list)} 只股票...")
        
        for ticker in download_list:
            # 🔥 改动1: 表名变成 stock_2m_
            table_name = f"stock_2m_{ticker.replace('-', '_')}"
            
            try:
                last_db_time = self.get_db_last_timestamp(table_name)
                
                # 🔥 改动2: 动态周期选择
                if last_db_time is None:
                    # Case A: 新股票
                    # yfinance 2m 数据最多支持回溯 60天，我们直接拉满
                    download_period = "60d" 
                    is_new_stock = True
                else:
                    # Case B: 老股票
                    # 为了防止周末漏数据，或者你隔了几天没跑，每次更新回看 5天
                    # 这样比 1d 安全，比 60d 快得多
                    download_period = "5d" 
                    is_new_stock = False

                # 🔥 改动3: interval="2m"
                # print(f"   Downloading {ticker} (2m, {download_period})...")
                df = yf.download(ticker, period=download_period, interval="2m", auto_adjust=True, progress=False)
                
                if df.empty:
                    print(f"   ⚠️ {ticker} 暂无数据")
                    continue

                # 数据清洗
                df = self._flatten_columns(df)
                df = df[df['Volume'] > 0].copy()
                
                # 格式化
                df.reset_index(inplace=True)
                df['Ticker'] = ticker
                
                # 统一时间列名
                if 'Date' in df.columns:
                    df.rename(columns={'Date': 'Datetime'}, inplace=True)
                elif 'index' in df.columns:
                     df.rename(columns={'index': 'Datetime'}, inplace=True)
                
                df.columns = [str(c).replace(' ', '_') for c in df.columns]

                # 入库逻辑
                if is_new_stock:
                    print(f"   📝 [新收录] {ticker}: 下载60天(2m) -> 写入 {len(df)} 条")
                    df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                else:
                    # 增量更新：先确保时区对齐
                    if df['Datetime'].dt.tz is not None and last_db_time.tzinfo is None:
                        last_db_time = last_db_time.tz_localize(df['Datetime'].dt.tz)
                    
                    # 只保留比数据库新的数据
                    new_data = df[df['Datetime'] > last_db_time].copy()
                    
                    if not new_data.empty:
                        print(f"   ➕ [更新] {ticker}: 追加 {len(new_data)} 条新数据")
                        new_data.to_sql(table_name, self.conn, if_exists='append', index=False)
                    else:
                        # 这种情况很正常（比如盘前刚跑过一次，或者今天休市）
                        pass

                # 稍微限流，防止请求过快
                time.sleep(0.2) 
                
            except Exception as e:
                print(f"❌ {ticker} 更新失败: {e}")
        
        print("✅ 所有 2分钟线 更新完成！")

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    engine = StockDataEngine()
    
    # 1. 更新日线 (带指标)
    engine.update_all()
    
    # 2. 更新分钟线 (2分钟级，智能增量)
    engine.update_minute_data()
    
    engine.close()