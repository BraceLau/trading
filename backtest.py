import pandas as pd
import sqlite3
import config
import matplotlib.pyplot as plt

class BacktestEngine:
    def __init__(self, initial_capital=100000):
        self.conn = sqlite3.connect(config.DB_NAME)
        self.initial_capital = initial_capital
        self.balance = initial_capital
        self.positions = {} # 持仓记录
        self.trade_log = [] # 交易日志

    def get_history(self, ticker):
        """读取单只股票的完整历史数据"""
        try:
            table_name = f"stock_{ticker.replace('-', '_')}"
            # 按日期正序排列 (旧 -> 新)
            df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY Date ASC", self.conn, parse_dates=['Date'])
            return df
        except:
            return pd.DataFrame()

    # ====================================================
    # 策略逻辑定义 (在这里把 strategy.py 的逻辑翻译成单行判断)
    # ====================================================
    def strategy_ema_pullback(self, row, prev_row=None):
        """
        策略：EMA多头排列 + 回调买入
        返回: 'BUY', 'SELL', or None
        """
        # 1. 必须有数据
        if row['EMA200'] is None or pd.isna(row['EMA200']):
            return None
            
        close = row['Close']
        ema20 = row['EMA20']
        ema60 = row['EMA60']
        ema200 = row['EMA200']

        # 买入条件
        # A. 大趋势向上 (收盘 > 年线)
        trend_up = close > ema200
        
        # B. 回调触碰 EMA20 (允许 1.5% 误差)
        tolerance = 0.015
        touch_ema20 = (ema20 * (1 - tolerance)) <= close <= (ema20 * (1 + tolerance))
        
        # C. 简单的出场条件 (止盈止损)
        # 这里我们只负责发买入信号，卖出逻辑由引擎统一管理(如持有10天或止损)
        
        if trend_up and touch_ema20:
            return 'BUY'
        
        return None

    # ====================================================
    # 核心回测循环
    # ====================================================
    def run_backtest(self, ticker, stop_loss_pct=0.05, take_profit_pct=0.10, hold_days=10):
        """
        对单只股票进行回测
        :param stop_loss_pct: 止损 (如 0.05 代表 5%)
        :param take_profit_pct: 止盈 (如 0.10 代表 10%)
        :param hold_days: 最大持仓天数 (时间止损)
        """
        df = self.get_history(ticker)
        if df.empty:
            print(f"⚠️ {ticker} 无数据，跳过")
            return

        print(f"🔄 正在回测 {ticker} ({len(df)} 天数据)...")
        
        in_position = False
        entry_price = 0
        entry_date = None
        days_held = 0
        
        # 遍历每一天
        for i in range(1, len(df)):
            today = df.iloc[i]
            yesterday = df.iloc[i-1]
            current_price = today['Close']
            date = today['Date']

            # --- 如果持有仓位，检查是否卖出 ---
            if in_position:
                days_held += 1
                
                # 计算当前收益率
                pct_change = (current_price - entry_price) / entry_price
                
                exit_reason = None
                if pct_change <= -stop_loss_pct:
                    exit_reason = "止损"
                elif pct_change >= take_profit_pct:
                    exit_reason = "止盈"
                elif days_held >= hold_days:
                    exit_reason = "时间到期"
                
                if exit_reason:
                    # 执行卖出
                    pnl = (current_price - entry_price) # 每股盈亏
                    return_rate = pnl / entry_price
                    
                    self.trade_log.append({
                        'Ticker': ticker,
                        'Entry_Date': entry_date,
                        'Exit_Date': date,
                        'Entry_Price': entry_price,
                        'Exit_Price': current_price,
                        'Reason': exit_reason,
                        'Return': return_rate
                    })
                    
                    in_position = False
                    days_held = 0

            # --- 如果空仓，检查是否买入 ---
            else:
                signal = self.strategy_ema_pullback(today, yesterday)
                if signal == 'BUY':
                    # 执行买入
                    in_position = True
                    entry_price = current_price
                    entry_date = date
                    days_held = 0

    def print_performance(self):
        if not self.trade_log:
            print("⚠️ 期间未触发任何交易。")
            return

        df_trades = pd.DataFrame(self.trade_log)
        
        # --- 1. 基础统计 ---
        total_trades = len(df_trades)
        wins = df_trades[df_trades['Return'] > 0]
        losses = df_trades[df_trades['Return'] <= 0]
        
        # 胜率
        win_rate = len(wins) / total_trades if total_trades > 0 else 0
        
        # 盈亏比 (避免除以0)
        avg_win = wins['Return'].mean() if not wins.empty else 0
        avg_loss = abs(losses['Return'].mean()) if not losses.empty else 0
        pl_ratio = avg_win / avg_loss if avg_loss > 0 else 0
        
        # --- 2. 资金曲线与回撤计算 ---
        # 假设每次全仓交易 (复利计算)
        df_trades['Equity'] = (1 + df_trades['Return']).cumprod() * self.initial_capital
        
        # 计算最大回撤 (Max Drawdown)
        # 累计最大值
        df_trades['Peak'] = df_trades['Equity'].cummax()
        # 当前回撤幅度
        df_trades['Drawdown'] = (df_trades['Equity'] - df_trades['Peak']) / df_trades['Peak']
        max_drawdown = df_trades['Drawdown'].min() # 这是一个负数，如 -0.15
        
        # --- 3. 夏普比率 (简化估算) ---
        # 这里基于“每笔交易”计算，严格来说应该基于“每日净值”计算
        risk_free_rate = 0.04 # 假设无风险利率 4%
        mean_return = df_trades['Return'].mean()
        std_return = df_trades['Return'].std()
        
        # 这是一个粗略的每笔交易夏普，年化需要乘以 sqrt(交易频率)
        # 这里仅作参考
        sharpe_ratio = (mean_return - (risk_free_rate/252)) / std_return if std_return > 0 else 0

        # --- 4. 打印专业报告 ---
        print("\n" + "="*50)
        print("📊 全面回测分析报告 (Advanced)")
        print("="*50)
        print(f"💰 最终资金:   ${df_trades['Equity'].iloc[-1]:.2f} (初始 ${self.initial_capital})")
        print(f"📈 累计收益:   {(df_trades['Equity'].iloc[-1]/self.initial_capital - 1):.2%}")
        print("-" * 50)
        print(f"🛡️ 最大回撤:   {max_drawdown:.2%} (最重要风险指标!)")
        print(f"⚖️ 夏普比率:   {sharpe_ratio:.2f}")
        print("-" * 50)
        print(f"🎲 胜率:       {win_rate:.2%}")
        print(f"🤝 盈亏比:     {pl_ratio:.2f} (平均赚 {avg_win:.1%} / 亏 {avg_loss:.1%})")
        print(f"🔢 交易次数:   {total_trades}")
        print("="*50)

        # 绘图
        plt.figure(figsize=(12, 8))
        
        # 子图1: 资金曲线
        plt.subplot(2, 1, 1)
        plt.plot(df_trades['Equity'], label='Strategy Equity', color='blue')
        plt.title('Equity Curve (Compound)')
        plt.grid(True)
        
        # 子图2: 回撤曲线
        plt.subplot(2, 1, 2)
        plt.fill_between(range(len(df_trades)), df_trades['Drawdown'], 0, color='red', alpha=0.3)
        plt.plot(df_trades['Drawdown'], color='red', label='Drawdown')
        plt.title('Drawdown (%)')
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig('backtest_advanced.png')
        print("✅ 高级图表已保存为 backtest_advanced.png")

# =========================================
# 运行脚本
# =========================================
if __name__ == "__main__":
    # 1. 初始化回测引擎
    tester = BacktestEngine()
    
    # 2. 选择要回测的股票 (可以是整个 config.WATCHLIST)
    # 这里先拿 NVDA 和 TSLA 跑跑看
    test_tickers = [
    "NVDA", "TSLA", "AAPL", "MSFT", "AMD", "COIN", "MSTR", 
    "GOOGL", "AMZN", "META", "LITE", "ORCL", 'NBIS', 'CRWV',
    'CLS', 'CRDO', 'ALAB', 'RKLB', 'ASTS', 'MU', 'SNDK', 'INTC',
    'OKLO', 'CCJ', 'BE', 'APP', 'VST', 'GEV', 'AVGO', 'TSM', 'AMD',
    'STX', 'WDC', 'FLNC', 'SMR', 'CIEN', 'COHR', 'UBER', 'HOOD', 'MSTR',
    'CRCL', 'ONDS']
    
    for t in test_tickers:
        tester.run_backtest(t, stop_loss_pct=0.08, take_profit_pct=0.15, hold_days=20)
        
    # 3. 打印结果
    tester.print_performance()