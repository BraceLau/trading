import yfinance as yf
import pandas as pd
import sqlite3
import os
import config  # 导入配置

proxy = "http://127.0.0.1:7890"
os.environ['HTTP_PROXY'] = proxy
os.environ['HTTPS_PROXY'] = proxy

class StockDataEngine:
    def __init__(self):
        self.conn = sqlite3.connect(config.DB_NAME)

    def _calculate_indicators(self, df):
        """
        纯 Pandas 实现版：无需安装 pandas_ta
        包含：EMA, MACD, RSI, KDJ, Bollinger Bands, ATR, OBV
        """
        # 1. 基础均线 (EMA)
        for span in [5, 10, 20, 60, 120, 200]:
            df[f'EMA{span}'] = df['Close'].ewm(span=span, adjust=False).mean()
        
        # 2. 基础涨幅
        for days in [5, 10, 20, 60, 120, 200]:
            df[f'Return_{days}d'] = df['Close'].pct_change(periods=days)

        # --- 手写高级指标 ---

        # 3. MACD (12, 26, 9)
        # DIF (快线) = EMA12 - EMA26
        ema12 = df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        # DEA (慢线/信号线) = MACD的EMA9
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        # Histogram (柱状图) = (DIF - DEA) * 2
        df['MACD_Hist'] = (df['MACD'] - df['MACD_Signal']) * 2

        # 4. RSI (14)
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))

        # 5. Bollinger Bands (20, 2)
        # 中轨 = 20日简单移动平均 (SMA)
        df['BBM'] = df['Close'].rolling(window=20).mean()
        # 标准差
        std = df['Close'].rolling(window=20).std()
        # 上轨 = 中轨 + 2*std
        df['BBU'] = df['BBM'] + 2 * std
        # 下轨 = 中轨 - 2*std
        df['BBL'] = df['BBM'] - 2 * std

        # 6. ATR (14) - 平均真实波幅
        # TR = Max(High-Low, abs(High-PrevClose), abs(Low-PrevClose))
        prev_close = df['Close'].shift(1)
        tr1 = df['High'] - df['Low']
        tr2 = (df['High'] - prev_close).abs()
        tr3 = (df['Low'] - prev_close).abs()
        # 取三者最大值
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        # ATR = TR的14日移动平均
        df['ATR'] = tr.ewm(alpha=1/14, adjust=False).mean()

        # 7. KDJ (9, 3, 3)
        low_min = df['Low'].rolling(window=9).min()
        high_max = df['High'].rolling(window=9).max()
        # RSV
        rsv = (df['Close'] - low_min) / (high_max - low_min) * 100
        # 处理除零异常 (fillna)
        rsv = rsv.fillna(0)
        
        df['K'] = rsv.ewm(alpha=1/3, adjust=False).mean()
        df['D'] = df['K'].ewm(alpha=1/3, adjust=False).mean()
        df['J'] = 3 * df['K'] - 2 * df['D']

        # 8. OBV (能量潮)
        # 如果今天收盘 > 昨天收盘，OBV = 昨天OBV + 今天成交量
        # 如果今天收盘 < 昨天收盘，OBV = 昨天OBV - 今天成交量
        obv_val = pd.Series(0, index=df.index)
        change = df['Close'].diff()
        # sign: 涨为1，跌为-1，平为0
        direction = change.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        df['OBV'] = (direction * df['Volume']).cumsum()

        return df

    def update_all(self):
        print(f"🔄 正在批量更新 {len(config.WATCHLIST)} 只股票数据...")
        
        # 1. 批量下载 (核心优化：一次请求搞定所有)
        # group_by='ticker' 会让返回的数据结构更清晰
        try:
            # 把列表转成字符串 "AAPL MSFT NVDA"
            tickers_str = " ".join(config.WATCHLIST)
            
            # 一次性下载所有数据
            all_data = yf.download(tickers_str, period="2y", interval="1d", group_by='ticker', auto_adjust=True, progress=True)
            
            if all_data.empty:
                print("❌ 下载失败: 数据为空")
                return

            # 2. 遍历处理并存库
            for ticker in config.WATCHLIST:
                try:
                    # 从大表中提取单只股票的数据
                    # 注意：如果某只股票停牌或没数据，这里可能会报错，加个 try
                    if ticker not in all_data.columns.levels[0]:
                        continue
                        
                    df = all_data[ticker].copy()
                    
                    if df.empty: continue
                    
                    # 只有在这里才进行清洗和计算
                    df = df[df['Volume'] > 0].copy()
                    df = self._calculate_indicators(df)
                    df.reset_index(inplace=True)
                    df['Ticker'] = ticker
                    df.columns = [str(c).replace(' ', '_') for c in df.columns]
                    
                    table_name = f"stock_{ticker.replace('-', '_')}"
                    df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                    
                except Exception as inner_e:
                    print(f"⚠️ 处理 {ticker} 时出错: {inner_e}")
            
            print("✅ 所有数据更新完成！")
            
        except Exception as e:
            print(f"❌ 批量下载严重错误: {e}")
            return

    def get_latest_data(self, ticker):
        table_name = f"stock_{ticker.replace('-', '_')}"
        try:
            query = f"SELECT * FROM {table_name} ORDER BY Date DESC LIMIT 1"
            return pd.read_sql(query, self.conn).iloc[0]
        except:
            return None
    
    def close(self):
        self.conn.close()