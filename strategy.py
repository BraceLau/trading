import pandas as pd
import numpy as np
import sqlite3
import config
import yfinance as yf
from scipy.signal import argrelextrema

class StrategyRunner:
    def __init__(self):
        """初始化：连接数据库"""
        self.conn = sqlite3.connect(config.DB_NAME)

    def _get_all_tables(self):
        """内部工具：获取数据库中所有股票表名"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'stock_%'")
        return [row[0] for row in cursor.fetchall()]

    def _get_latest_row(self, table_name):
        """内部工具：快速获取某只股票最新的一行数据"""
        try:
            # 取最新的一行，包含所有计算好的指标 (EMA, Return等)
            query = f"SELECT * FROM {table_name} ORDER BY Date DESC LIMIT 1"
            df = pd.read_sql(query, self.conn)
            if not df.empty:
                return df.iloc[0]
        except:
            pass
        return None

    # ==========================================
    # 策略 1: 寻找近期涨幅榜 (Momentum)
    # ==========================================
    def run_top_gainers(self, days=20, top_n=10):
        """
        筛选策略：涨幅榜
        :param days: 周期 (5, 20, 60...)
        :param top_n: 返回前几名
        """
        results = []
        tables = self._get_all_tables()
        col_name = f"Return_{days}d"

        print(f"🔎 [策略执行] 扫描 {days} 日涨幅榜...")

        for table in tables:
            # 兼容性处理：如果数据库没存 Ticker 列，则从表名提取
            ticker_name = table.replace('stock_', '').replace('_', '-')
            
            row = self._get_latest_row(table)
            
            # 确保数据存在且涨幅不为空
            if row is not None and pd.notna(row.get(col_name)):
                # 如果数据库里有 Ticker 列，优先用数据库里的
                if 'Ticker' in row and pd.notna(row['Ticker']):
                    ticker_name = row['Ticker']

                current_close = row['Close']
                return_rate = row[col_name] # 例如 0.20 代表 20%
                
                # 🔥 核心修改：通过涨幅反推 X 天前的价格
                # 公式：旧价格 = 现价 / (1 + 涨幅)
                prev_close = current_close / (1 + return_rate)

                results.append({
                    'Ticker': ticker_name,
                    'Close': current_close,       # 当前价格
                    'Prev_Close': prev_close,     # X天前价格 (新增)
                    'Score': return_rate,         # 涨幅
                    'Strategy': f'Top Gainers ({days}d)'
                })

        # 排序并取前 N 名
        df = pd.DataFrame(results)
        if df.empty: return []
        
        # 按涨幅降序排列
        df = df.sort_values(by='Score', ascending=False).head(top_n)
        return df.to_dict('records')

    # ==========================================
    # 策略 2: 均线回调买入 (已更新：显示现价)
    # ==========================================
    def run_ema_pullback(self, tolerance=0.015):
        results = []
        tables = self._get_all_tables()
        
        print(f"🔎 [策略执行] 扫描均线回调机会...")

        for table in tables:
            # 提取 Ticker (兼容旧数据)
            ticker_name = table.replace('stock_', '').replace('_', '-')
            
            row = self._get_latest_row(table)
            if row is None: continue

            # 优先使用数据库里的 Ticker 列，如果没有则用表名提取的
            if 'Ticker' in row and pd.notna(row['Ticker']):
                ticker_name = row['Ticker']

            close = row['Close']
            ema200 = row.get('EMA200')

            # 1. 趋势过滤：只看多头排列 (股价 > 年线)
            if ema200 is None or close < ema200:
                continue

            # 2. 检查回调
            matched_ema = None
            matched_val = 0 # 用于记录具体均线数值
            
            for span in [20, 60, 120]:
                ema_val = row.get(f'EMA{span}')
                if ema_val:
                    upper = ema_val * (1 + tolerance)
                    lower = ema_val * (1 - tolerance)
                    if lower <= close <= upper:
                        matched_ema = f"EMA{span}"
                        matched_val = ema_val
                        break
            
            if matched_ema:
                # 计算乖离率 (当前价格相对于均线的百分比差异)
                diff_pct = (close - matched_val) / matched_val
                
                results.append({
                    'Ticker': ticker_name,
                    'Close': close,
                    'Score': row.get('Return_20d', 0),
                    'Strategy': 'EMA Pullback',
                    # 🔥 修改点：在这里加上了“现价”信息
                    'Detail': f"现价 ${close:.2f} (偏离 {diff_pct:+.2%}) -> 支撑于 {matched_ema} (${matched_val:.2f})"
                })

        return results

    # ==========================================
    # 策略 3: 均线多头排列 (Strong Trend)
    # ==========================================
    def run_strong_trend(self):
        """
        筛选策略：EMA20 > EMA60 > EMA120 > EMA200 (超强趋势)
        """
        results = []
        tables = self._get_all_tables()
        print(f"🔎 [策略执行] 扫描超强多头排列...")

        for table in tables:
            row = self._get_latest_row(table)
            if row is None: continue

            try:
                # 必须所有均线都有值
                if (row['EMA20'] > row['EMA60'] > row['EMA120'] > row['EMA200']):
                    # 且当前价格在所有均线之上
                    if row['Close'] > row['EMA20']:
                        results.append({
                            'Ticker': row['Ticker'],
                            'Close': row['Close'],
                            'Score': row['Return_60d'],
                            'Strategy': 'Strong Trend',
                            'Detail': '均线完美多头排列'
                        })
            except:
                continue
                
        return results
    
    # ==========================================
    # 策略 4: MACD 底背离 + KDJ 金叉 (高胜率共振)
    # ==========================================
    def run_macd_divergence_kdj(self):
        results = []
        tables = self._get_all_tables()
        print("🔎 [策略执行] 扫描 MACD底背离 + KDJ金叉 共振机会...")

        for table in tables:
            # 1. 获取最近 60 天数据 (需要历史数据来判断背离)
            try:
                query = f"SELECT * FROM {table} ORDER BY Date DESC LIMIT 60"
                df = pd.read_sql(query, self.conn)
                
                if len(df) < 30: continue # 新股数据太少，跳过
                
                # 数据库读出来是倒序的(最新在最前)，反转为正序(时间从左到右)方便计算
                df = df.iloc[::-1].reset_index(drop=True)
            except:
                continue

            # 提取 Ticker
            ticker_name = df['Ticker'].iloc[-1] if 'Ticker' in df.columns else table.replace('stock_', '').replace('_', '-')

            # === 第一步：检查 KDJ 金叉 (战术信号) ===
            # 逻辑：今天 K > D，且昨天 K < D (或非常接近)
            # 注意：K和D是最后两行
            curr_k, curr_d = df['K'].iloc[-1], df['D'].iloc[-1]
            prev_k, prev_d = df['K'].iloc[-2], df['D'].iloc[-2]

            # 判定金叉：今天 K在D上，且 (昨天K在D下 或 昨天K,D还没拉开差距)
            is_gold_cross = (curr_k > curr_d) and (prev_k < prev_d)
            
            # 增加一个过滤器：金叉最好发生在低位 (例如 K < 50)，高位金叉可能是诱多
            if not (is_gold_cross and curr_k < 50):
                continue 

            # === 第二步：检查 MACD 底背离 (战略信号) ===
            # 定义背离：股价创新低，但 MACD 没有创新低
            
            # 选取最近 20 天的窗口
            window = 20
            recent_df = df.iloc[-window:]
            
            # 1. 找到这20天内的股价最低点
            min_price = recent_df['Low'].min()
            min_price_idx = recent_df['Low'].idxmin()
            
            # 2. 找到这20天内的 MACD (DIF线) 最低点
            min_macd = recent_df['MACD'].min()
            min_macd_idx = recent_df['MACD'].idxmin()
            
            # 3. 判定逻辑
            # A. 股价最低点必须发生在最近 (比如最近 3-5 天内)，说明刚刚经历下跌
            price_is_new_low = (len(df) - 1 - min_price_idx) <= 5
            
            # B. MACD 的最低点必须发生在比较久之前 (比如 5 天以前)
            # 这意味着最近股价跌了，但 MACD 没跟着跌到新低
            macd_bottom_was_earlier = (min_price_idx - min_macd_idx) > 3
            
            # C. 再次确认：当前 MACD 值明显高于之前的最低 MACD 值
            # 这里的 MACD 列对应 DIF 快线
            current_macd_higher = df['MACD'].iloc[-1] > min_macd
            
            # D. 甚至可以要求 MACD 也是金叉状态 (DIF > DEA)
            macd_gold = df['MACD'].iloc[-1] > df['MACD_Signal'].iloc[-1]

            if price_is_new_low and macd_bottom_was_earlier and current_macd_higher and macd_gold:
                results.append({
                    'Ticker': ticker_name,
                    'Close': df['Close'].iloc[-1],
                    'Score': curr_k, # 用K值作为排序参考
                    'Strategy': 'MACD Div + KDJ Cross',
                    'Detail': f"MACD底背离 (MACD底在{(len(df)-1-min_macd_idx)}天前) + KDJ低位金叉"
                })

        return results

    def close(self):
        self.conn.close()

class MarketPhaseScanner:
    def __init__(self):
        self.conn = sqlite3.connect(config.DB_NAME)

    def _get_all_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'stock_%'")
        return [row[0] for row in cursor.fetchall()]

    def analyze_phase(self):
        results = []
        tables = self._get_all_tables()
        print("🔎 [策略执行] 正在全市场扫描，判断个股所处阶段 (左侧/右侧/震荡)...")

        for table in tables:
            try:
                # 获取最近 60 天数据 (判断趋势需要一段时间)
                df = pd.read_sql(f"SELECT * FROM {table} ORDER BY Date DESC LIMIT 60", self.conn)
                if len(df) < 50: continue 
                
                # 转为正序
                df = df.iloc[::-1].reset_index(drop=True)
                
                # 提取数据
                row = df.iloc[-1]
                ticker = row['Ticker'] if 'Ticker' in row else table.replace('stock_', '').replace('_', '-')
                
                # 必须包含均线数据
                if pd.isna(row.get('EMA20')) or pd.isna(row.get('EMA120')):
                    continue

                close = row['Close']
                ema20 = row['EMA20']
                ema60 = row['EMA60']
                ema120 = row['EMA120']
                
                # ==========================================
                # 🔥 核心打分逻辑 (Score System)
                # ==========================================
                score = 0
                
                # 1. 价格位置 (Price Location)
                if close > ema20: score += 1
                elif close < ema20: score -= 1
                
                if close > ema60: score += 1
                elif close < ema60: score -= 1

                if close > ema120: score += 1 # 站稳半年线是很重要的右侧信号
                elif close < ema120: score -= 1

                # 2. 均线排列 (MA Alignment)
                if ema20 > ema60: score += 1
                elif ema20 < ema60: score -= 1
                
                if ema60 > ema120: score += 1
                elif ema60 < ema120: score -= 1
                
                # 3. 趋势斜率 (MA Slope) - 判断是走平还是发散
                # 计算 EMA60 今天的涨跌幅
                prev_ema60 = df.iloc[-2]['EMA60']
                slope = (ema60 - prev_ema60) / prev_ema60
                
                is_flat = abs(slope) < 0.0005 # 如果斜率非常小，说明均线走平 -> 震荡

                # ==========================================
                # ⚖️ 阶段判定
                # ==========================================
                phase = "未知"
                advice = "观望"
                color = "⚪"
                
                # 定义布林带带宽 (Bandwidth) - 辅助判断震荡
                # Bandwidth = (上轨 - 下轨) / 中轨
                # 如果你的数据库里没算布林带，这里可以用 (High20 - Low20) / Close 估算
                
                if is_flat or (abs(score) <= 1):
                    phase = "🟡 震荡整理 (Consolidation)"
                    advice = "高抛低吸 / 等待突破"
                    color = "🟡"
                elif score >= 4:
                    phase = "🟢 强势右侧 (Strong Uptrend)"
                    advice = "持有 / 回调EMA20买入"
                    color = "🟢"
                elif score >= 2:
                    phase = "📈 弱势右侧 (Weak Uptrend)"
                    advice = "谨慎做多"
                    color = "📈"
                elif score <= -4:
                    phase = "🔴 极度左侧 (Strong Downtrend)"
                    advice = "空仓 / 反弹做空"
                    color = "🔴"
                elif score <= -2:
                    phase = "📉 弱势左侧 (Weak Downtrend)"
                    advice = "勿抄底"
                    color = "📉"
                
                results.append({
                    'Ticker': ticker,
                    'Close': close,
                    'Score': score,
                    'Phase': phase,
                    'Advice': advice,
                    'Color': color
                })

            except Exception as e:
                continue

        # 按分数排序：从最强右侧 到 最强左侧
        return sorted(results, key=lambda x: x['Score'], reverse=True)

    def close(self):
        self.conn.close()

class ReversalScanner:
    def __init__(self):
        self.conn = sqlite3.connect(config.DB_NAME)

    def _get_all_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'stock_%'")
        return [row[0] for row in cursor.fetchall()]

    def run_short_term_reversal(self):
        results = []
        tables = self._get_all_tables()
        print("🔎 [策略执行] 扫描‘超短线5/10日反转’形态...")

        for table in tables:
            try:
                # 只需要最近 30 天数据即可
                df = pd.read_sql(f"SELECT * FROM {table} ORDER BY Date DESC LIMIT 10", self.conn)
                if len(df) < 5: continue
                
                # 转正序
                df = df.iloc[::-1].reset_index(drop=True)
                row = df.iloc[-1]
                ticker = row['Ticker'] if 'Ticker' in row else table.replace('stock_', '').replace('_', '-')
                
                # 检查是否有 EMA5 和 EMA10 列
                if pd.isna(row.get('EMA5')) or pd.isna(row.get('EMA10')): 
                    continue

                close = row['Close']
                ema5 = row['EMA5']   # 快线 (攻击线)
                ema10 = row['EMA10'] # 慢线 (操盘线)

                # === 核心逻辑 (5日/10日版本) ===
                
                # 1. 价格站上 10日线 (短线生命线)
                if not (close > ema10): 
                    continue

                # 2. 5日线金叉10日线 (刚刚启动)
                # 判定：EMA5 > EMA10 且 两者距离非常近 (3%以内)
                bias = (ema5 - ema10) / ema10
                
                is_just_crossed = (0 < bias < 0.03) 
                
                if not is_just_crossed:
                    continue

                # 3. 拒绝高位接盘 (短线)
                # 判定：当前价格 距离 过去20天最低价 涨幅不超过 15%
                # 如果短线已经涨了20%以上再金叉，通常是鱼尾行情
                lowest_price = df['Low'].min()
                gain_from_bottom = (close - lowest_price) / lowest_price
                
                # if gain_from_bottom > 0.15: 
                #     continue 

                # 4. (可选) 昨天的 EMA5 还在 EMA10 下方 (确认是今天刚金叉)
                # prev_row = df.iloc[-2]
                # if prev_row['EMA5'] > prev_row['EMA10']: continue

                results.append({
                    'Ticker': ticker,
                    'Close': close,
                    'Score': gain_from_bottom, 
                    'Strategy': '5/10 Day Reversal',
                    'Detail': f"站上EMA10 + 5日线金叉 (距20日底 +{gain_from_bottom:.1%})"
                })

            except Exception as e:
                continue

        return results

class TrendlineScanner:
    def __init__(self):
        self.conn = sqlite3.connect(config.DB_NAME)
        
        # ================= 配置参数 =================
        self.use_log_scale = True     # 🔥 开启对数坐标 (关键修改)
        
        self.lookback_days = 120      
        self.peak_order = 3           
        self.min_dist_between_pts = 5 
        self.breakout_threshold = 1.002
        # ===========================================

    def _get_all_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'stock_%'")
        return [row[0] for row in cursor.fetchall()]

    def run_trendline_breakout(self):
        results = []
        tables = self._get_all_tables()
        mode_str = "对数Log" if self.use_log_scale else "普通Linear"
        print(f"🔎 [策略执行] 扫描‘长期下降趋势线突破’ ({mode_str}模式)...")

        for table in tables:
            try:
                # 1. 获取数据
                df = pd.read_sql(f"SELECT * FROM {table} ORDER BY Date DESC LIMIT {self.lookback_days}", self.conn, parse_dates=['Date'])
                if len(df) < 100: continue 
                
                df = df.iloc[::-1].reset_index(drop=True)
                ticker = df['Ticker'].iloc[-1] if 'Ticker' in df.columns else table.replace('stock_', '').replace('_', '-')
                
                # 🔥 关键步骤：转换到对数空间
                if self.use_log_scale:
                    # 使用 np.log 处理价格
                    # 所有的画线逻辑都在 log_highs 上进行
                    raw_highs = df['High'].values
                    highs = np.log(raw_highs) 
                    
                    raw_closes = df['Close'].values
                    closes = np.log(raw_closes)
                else:
                    highs = df['High'].values
                    closes = df['Close'].values

                current_idx = len(df) - 1
                
                # 2. 寻找波峰 (在对数空间找波峰，其实位置和普通空间一样，但数值不同)
                peak_indexes = argrelextrema(highs, np.greater, order=self.peak_order)[0]
                if len(peak_indexes) < 2: continue

                best_breakout = None

                # 3. 遍历寻找锚点
                for i in range(len(peak_indexes)):
                    idx_a = peak_indexes[i]
                    price_a = highs[idx_a] # 注意：这里的 price_a 是对数值 (如 4.56)
                    
                    if (current_idx - idx_a) < 30: continue
                    
                    for j in range(i + 1, len(peak_indexes)):
                        idx_b = peak_indexes[j]
                        price_b = highs[idx_b]
                        
                        if (idx_b - idx_a) < self.min_dist_between_pts: continue
                        if price_b >= price_a: continue # 下降趋势
                        
                        # === 建立方程 (对数空间) ===
                        # log(y) = kx + b
                        slope = (price_b - price_a) / (idx_b - idx_a)
                        intercept = price_a - slope * idx_a
                        
                        # === 天花板测试 ===
                        check_range = np.arange(idx_a + 1, current_idx) 
                        if len(check_range) == 0: continue

                        line_values = slope * check_range + intercept
                        actual_highs = highs[check_range]
                        
                        violations = np.sum(actual_highs > line_values)
                        violation_rate = violations / len(check_range)
                        
                        if violation_rate > 0.05: continue

                        # === 判断突破 ===
                        log_resistance_now = slope * current_idx + intercept
                        log_close_now = closes[-1]
                        
                        # 判定条件：log(Close) > log(Resistance) + 阈值
                        # 注意：对数空间的加减，对应原始空间的乘除
                        # log(A) > log(B) + log(1.005)  =>  A > B * 1.005
                        threshold_log = np.log(self.breakout_threshold)
                        
                        if log_close_now > log_resistance_now + threshold_log:
                            
                            duration = current_idx - idx_a
                            date_a = df['Date'].iloc[idx_a].strftime('%Y-%m-%d')
                            date_b = df['Date'].iloc[idx_b].strftime('%Y-%m-%d')
                            
                            # 🔥 还原显示价格 (从 Log 变回 $)
                            # 为了显示给人类看，必须用 np.exp 还原
                            real_price_a = np.exp(price_a) if self.use_log_scale else price_a
                            real_price_b = np.exp(price_b) if self.use_log_scale else price_b
                            real_resistance = np.exp(log_resistance_now) if self.use_log_scale else log_resistance_now
                            real_close = np.exp(log_close_now) if self.use_log_scale else log_close_now
                            
                            if best_breakout is None or duration > best_breakout['Duration']:
                                best_breakout = {
                                    'Ticker': ticker,
                                    'Close': real_close,
                                    'Resistance': real_resistance, # 这是对数趋势线对应的今日阻力位
                                    'Duration': duration,
                                    'PointA': f"{date_a} (${real_price_a:.2f})",
                                    'PointB': f"{date_b} (${real_price_b:.2f})",
                                    'Detail': f"突破 {duration}天 对数趋势线"
                                }
                
                if best_breakout:
                    results.append(best_breakout)

            except Exception:
                continue

        return results
    

class HighWinRateScanner:
    def __init__(self):
        self.conn = sqlite3.connect(config.DB_NAME)

    def _get_all_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'stock_%'")
        return [row[0] for row in cursor.fetchall()]

    def get_realtime_market_cap(self, ticker):
        """联网获取最新市值 (单位: 亿)"""
        try:
            info = yf.Ticker(ticker).info
            mkt_cap = info.get('marketCap', 0)
            return mkt_cap / 100000000 # 换算成“亿”
        except:
            return 0

    def run(self):
        results = []
        tables = self._get_all_tables()
        print("🔎 [策略执行] 扫描‘高胜率超跌’ (RSI<30 + ATR>4% + 市值>200亿)...")

        for table in tables:
            try:
                # 1. 获取数据 (至少需要20天计算ATR14)
                df = pd.read_sql(f"SELECT * FROM {table} ORDER BY Date DESC LIMIT 30", self.conn)
                if len(df) < 20: continue
                
                # 转正序
                df = df.iloc[::-1].reset_index(drop=True)
                ticker = df['Ticker'].iloc[-1] if 'Ticker' in df.columns else table.replace('stock_', '').replace('_', '-')
                
                row = df.iloc[-1]
                
                # ==========================================
                # 🛑 第一道关卡：技术指标 (本地计算，极快)
                # ==========================================
                
                # 1. 检查 RSI (超跌)
                # 如果数据库里没有 RSI 列，或者值为 NaN，跳过
                if 'RSI' not in row or pd.isna(row['RSI']): continue
                
                rsi = row['RSI']
                if rsi >= 30: continue # 只看 RSI < 30
                
                # 2. 检查 ATR% (高波动)
                # ATR通常是绝对值，需要除以股价转为百分比
                # 如果数据库没有 ATR，这里简单手算一下 ATR14 的近似值
                if 'ATR' in row and pd.notna(row['ATR']):
                    atr_val = row['ATR']
                else:
                    # 简易补救：计算最近14天的波动均值
                    df['TR'] = df[['High', 'Close']].max(axis=1) - df[['Low', 'Close']].min(axis=1)
                    atr_val = df['TR'].tail(14).mean()
                
                close_price = row['Close']
                atr_pct = (atr_val / close_price) * 100
                
                # 核心条件：波动率必须大于 4% (说明股性活)
                if atr_pct <= 4.0: continue

                # ==========================================
                # 🛑 第二道关卡：市值过滤 (联网查询，较慢)
                # ==========================================
                # 能走到这一步的股票已经很少了，所以这里联网查不耗时
                print(f"   >>> 正在核验 {ticker} 市值...")
                market_cap_亿 = self.get_realtime_market_cap(ticker)
                
                if market_cap_亿 < 200: 
                    # print(f"       市值不足 ({market_cap_亿:.0f}亿), 剔除.")
                    continue

                # ==========================================
                # ✅ 全部通关，加入结果
                # ==========================================
                results.append({
                    'Ticker': ticker,
                    'Close': close_price,
                    'RSI': rsi,
                    'ATR_Pct': atr_pct,
                    'MarketCap': market_cap_亿,
                    'Strategy': 'High Win Rate Dip',
                    'Detail': f"RSI={rsi:.1f} (超跌) | ATR={atr_pct:.1f}% (活跃) | 市值={market_cap_亿:.0f}亿"
                })

            except Exception as e:
                # print(f"Error {ticker}: {e}")
                continue

        # 按 RSI 从低到高排序 (越低越超跌)
        return sorted(results, key=lambda x: x['RSI'])