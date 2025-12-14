import pandas as pd
import sqlite3
import config
import matplotlib.pyplot as plt
import numpy as np

class ReversalStrategyBacktest:
    def __init__(self, initial_capital=100000):
        self.conn = sqlite3.connect(config.DB_NAME)
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self.positions = {} 
        self.trade_log = []
        self.history_equity = []
        
        # 策略参数
        self.max_pos_pct = 0.20   # 最大仓位 20%
        self.stop_loss_pct = 0.05 # 固定止损 5%

    def load_data(self):
        print("⏳ 正在加载全市场数据...")
        self.market_data = {}
        all_dates = set()
        
        for ticker in config.WATCHLIST:
            try:
                # 必须按时间正序加载
                table_name = f"stock_{ticker.replace('-', '_')}"
                df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY Date ASC", self.conn, parse_dates=['Date'])
                if not df.empty:
                    df.set_index('Date', inplace=True)
                    self.market_data[ticker] = df
                    all_dates.update(df.index)
            except: continue
            
        self.timeline = sorted(list(all_dates))
        print(f"✅ 数据加载完毕，共 {len(self.market_data)} 只股票")

    def run(self):
        self.load_data()
        print("🚀 开始回测策略: [5/10日金叉 + RSI超卖 + KDJ金叉]...")
        
        for i, date in enumerate(self.timeline):
            # 必须从第2天开始(需要比较昨天)
            if i < 5: continue 
            
            prev_date = self.timeline[i-1]
            daily_equity = self.cash
            
            # --- 1. 持仓管理 (卖出逻辑) ---
            for ticker in list(self.positions.keys()):
                df = self.market_data.get(ticker)
                if date not in df.index: 
                    # 停牌时更新市值
                    daily_equity += self.positions[ticker]['qty'] * self.positions[ticker]['last_price']
                    continue
                
                row = df.loc[date]
                pos = self.positions[ticker]
                price = row['Close']
                
                # 更新市值
                daily_equity += pos['qty'] * price
                self.positions[ticker]['last_price'] = price
                
                # 计算当前收益率
                pnl_pct = (price - pos['entry_price']) / pos['entry_price']
                
                # A. 止损 (Hard Stop): 亏损 5%
                if pnl_pct <= -self.stop_loss_pct:
                    self._sell(date, ticker, price, f"止损触少(-5%)")
                    continue
                
                # B. 动态止盈策略
                # 逻辑：如果曾经盈利超过 5%，则止损线上移至 成本价 (保本)
                if pos['max_pnl'] > 0.05 and pnl_pct < 0.01:
                    self._sell(date, ticker, price, "保本离场")
                    continue
                    
                # 逻辑：跌破 5日线 止盈 (短线战法核心)
                # 只有当盈利状态下，跌破5日线才卖出；亏损时由止损保护
                if pd.notna(row.get('EMA5')) and price < row['EMA5'] and pnl_pct > 0:
                     self._sell(date, ticker, price, "跌破EMA5止盈")
                     continue

                # 更新持仓最高收益 (用于触发保本逻辑)
                if pnl_pct > pos['max_pnl']:
                    self.positions[ticker]['max_pnl'] = pnl_pct

            # --- 2. 开仓管理 (买入逻辑) ---
            # 只有现金够买至少一只股票时才扫描
            if self.cash > self.initial_capital * 0.05:
                for ticker in self.market_data:
                    if ticker in self.positions: continue
                    
                    df = self.market_data.get(ticker)
                    if date not in df.index or prev_date not in df.index: continue
                    
                    curr = df.loc[date]
                    prev = df.loc[prev_date]
                    
                    # 确保所有指标都存在
                    if pd.isna(curr.get('EMA5')) or pd.isna(curr.get('EMA10')) or \
                       pd.isna(curr.get('RSI')) or pd.isna(curr.get('K')) or pd.isna(curr.get('D')):
                        continue

                    # === 核心策略逻辑 ===
                    
                    # 条件1: 股价5日线上穿10日线 (金叉)
                    # 判定：今天 5>10 且 昨天 5<=10
                    ma_cross = (curr['EMA5'] > curr['EMA10']) and (prev['EMA5'] <= prev['EMA10'])
                    
                    # 条件2: RSI出现超卖
                    # 注意：通常MA金叉时价格已经涨起来了，RSI可能已经回到40-50了
                    # 所以我们判定：过去5天内，RSI曾经低于 35
                    # 获取过去5天数据
                    recent_rsi = df.loc[:date].tail(5)['RSI']
                    rsi_oversold = (recent_rsi < 35).any()
                    
                    # 条件3: KDJ出现反转 (金叉)
                    # 判定：今天 K > D (或者 J 向上拐头)
                    kdj_up = (curr['K'] > curr['D'])
                    
                    if ma_cross and rsi_oversold and kdj_up:
                        self._buy(date, ticker, curr['Close'])

            self.history_equity.append({'Date': date, 'Total_Equity': daily_equity})

    def _buy(self, date, ticker, price):
        # 仓位控制：不超过总资金的 20%
        # 计算当前总资产 (现金 + 持仓市值)
        total_asset = self.cash + sum(p['qty'] * p['last_price'] for p in self.positions.values())
        
        target_pos_value = total_asset * self.max_pos_pct
        
        # 实际买入金额 (不能超过现金)
        invest_amt = min(self.cash, target_pos_value)
        
        if invest_amt > 500: # 最小交易额
            qty = invest_amt / price
            self.cash -= invest_amt
            self.positions[ticker] = {
                'qty': qty,
                'entry_price': price,
                'last_price': price,
                'max_pnl': 0 # 记录最大浮盈
            }
            self.trade_log.append({
                'Date': date, 'Ticker': ticker, 'Action': 'BUY', 
                'Price': price, 'Reason': 'MA5/10金叉+RSI超卖'
            })

    def _sell(self, date, ticker, price, reason):
        pos = self.positions[ticker]
        market_val = pos['qty'] * price
        self.cash += market_val
        pnl = (price - pos['entry_price']) / pos['entry_price']
        
        self.trade_log.append({
            'Date': date, 'Ticker': ticker, 'Action': 'SELL', 
            'Price': price, 'Reason': reason, 'PnL': pnl
        })
        del self.positions[ticker]

    def report(self):
        df_eq = pd.DataFrame(self.history_equity).set_index('Date')
        final_ret = (df_eq['Total_Equity'].iloc[-1] / self.initial_capital) - 1
        
        # 计算回撤
        df_eq['Peak'] = df_eq['Total_Equity'].cummax()
        df_eq['Drawdown'] = (df_eq['Total_Equity'] - df_eq['Peak']) / df_eq['Peak']
        max_dd = df_eq['Drawdown'].min()

        print("\n" + "="*40)
        print(f"📊 策略回测报告 (Reversal Strategy)")
        print("="*40)
        print(f"最终收益: {final_ret:.2%}")
        print(f"最大回撤: {max_dd:.2%}")
        
        # 交易统计
        df_trades = pd.DataFrame(self.trade_log)
        if not df_trades.empty:
            sells = df_trades[df_trades['Action'] == 'SELL']
            wins = sells[sells['PnL'] > 0]
            win_rate = len(wins) / len(sells) if len(sells) > 0 else 0
            print(f"交易次数: {len(sells)}")
            print(f"胜率: {win_rate:.2%}")
            print("最近5笔交易:")
            print(sells[['Date', 'Ticker', 'Reason', 'PnL']].tail(5))
        
        # 绘图
        plt.figure(figsize=(10, 6))
        plt.subplot(2,1,1)
        plt.plot(df_eq['Total_Equity'])
        plt.title('Equity Curve')
        plt.grid()
        plt.subplot(2,1,2)
        plt.plot(df_eq['Drawdown'], color='red')
        plt.title('Drawdown')
        plt.grid()
        plt.tight_layout()
        plt.savefig('strategy_reversal.png')
        print("✅ 结果已保存为 strategy_reversal.png")

if __name__ == "__main__":
    bot = ReversalStrategyBacktest()
    bot.run()
    bot.report()