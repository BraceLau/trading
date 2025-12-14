import pandas as pd
import sqlite3
import config
import matplotlib.pyplot as plt
import numpy as np
import yfinance as yf

class PortfolioBacktestPro:
    def __init__(self, initial_capital=100000):
        self.conn = sqlite3.connect(config.DB_NAME)
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self.positions = {} 
        self.trade_log = []
        self.history_equity = []
        
        # 风控参数
        self.risk_per_trade = 0.015  # 每笔交易最大亏损风险 (1.5%)
        self.atr_multiplier = 2.5    # 止损宽度 (2.5倍 ATR)

    def load_data_and_benchmark(self):
        """加载个股数据 + 大盘指数(SPY)"""
        print("⏳ 正在加载个股数据...")
        self.market_data = {}
        all_dates = set()
        
        # 1. 加载个股
        for ticker in config.WATCHLIST:
            try:
                table_name = f"stock_{ticker.replace('-', '_')}"
                df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY Date ASC", self.conn, parse_dates=['Date'])
                if not df.empty:
                    df.set_index('Date', inplace=True)
                    self.market_data[ticker] = df
                    all_dates.update(df.index)
            except: continue
            
        # 2. 临时下载 SPY 大盘数据作为“红绿灯”
        print("🚦 正在获取 SPY 大盘数据用于风控...")
        self.spy = yf.download("SPY", period="2y", interval="1d", auto_adjust=True, progress=False)
        if isinstance(self.spy.columns, pd.MultiIndex):
            self.spy.columns = self.spy.columns.get_level_values(0)
        self.spy['MA200'] = self.spy['Close'].rolling(200).mean()
        
        self.timeline = sorted(list(all_dates))

    def get_spy_trend(self, date):
        """判断大盘环境: True=牛市(可开仓), False=熊市(只卖不买)"""
        if date not in self.spy.index:
            # 如果对应日期没有SPY数据(比如假期差异)，往前找最近的一天
            try:
                idx = self.spy.index.get_indexer([date], method='pad')[0]
                row = self.spy.iloc[idx]
            except:
                return True # 默认允许
        else:
            row = self.spy.loc[date]
            
        # 只有当 SPY > 200日均线时，才允许做多
        if pd.notna(row['MA200']) and row['Close'] < row['MA200']:
            return False
        return True

    def run(self):
        self.load_data_and_benchmark()
        print("🚀 开始 Pro 版回测 (含大盘风控 + ATR仓位管理)...")
        
        for date in self.timeline:
            daily_portfolio_value = self.cash
            
            # 1. 处理持仓 (卖出逻辑)
            for ticker in list(self.positions.keys()):
                df = self.market_data.get(ticker)
                if date not in df.index: continue
                row = df.loc[date]
                pos = self.positions[ticker]
                
                price = row['Close']
                daily_portfolio_value += pos['qty'] * price
                
                # --- 止损逻辑 (基于 ATR 的硬止损) ---
                # 如果价格跌破了我们开仓时设定的止损价
                if price < pos['stop_loss_price']:
                    self.cash += pos['qty'] * price
                    pnl = (price - pos['entry_price']) / pos['entry_price']
                    self.trade_log.append({'Date':date, 'Ticker':ticker, 'Action':'SELL', 'Reason':'ATR止损', 'PnL':pnl})
                    del self.positions[ticker]
                    continue
                
                # --- 移动止盈 (Trailing Stop) ---
                # 如果从持仓后的最高点回撤超过 3倍 ATR (或者固定比例)，也卖出
                # 这里简单演示：价格涨破均线后又跌破 EMA20
                if price < row['EMA20'] and price > pos['entry_price']:
                     self.cash += pos['qty'] * price
                     pnl = (price - pos['entry_price']) / pos['entry_price']
                     self.trade_log.append({'Date':date, 'Ticker':ticker, 'Action':'SELL', 'Reason':'趋势止盈', 'PnL':pnl})
                     del self.positions[ticker]

            # 2. 开仓逻辑 (买入)
            # 【风控核心】先看大盘脸色！
            is_bull_market = self.get_spy_trend(date)
            
            if is_bull_market: 
                for ticker in self.market_data:
                    if ticker in self.positions: continue
                    
                    df = self.market_data.get(ticker)
                    if date not in df.index: continue
                    row = df.loc[date]
                    
                    # 策略：EMA多头排列 + RSI回调
                    if (row['Close'] > row['EMA60']) and (row['RSI'] < 55):
                        
                        # 【仓位管理核心】根据 ATR 计算买多少股
                        # 我们希望这笔交易最多只亏损总账户的 1.5%
                        atr = row['ATR'] if pd.notna(row.get('ATR')) else row['Close']*0.02
                        
                        risk_amount = daily_portfolio_value * self.risk_per_trade # 比如 10万 * 1.5% = 1500元风险预算
                        stop_loss_dist = atr * self.atr_multiplier # 止损距离 = 2.5 * ATR
                        
                        # 应该买的股数 = 风险预算 / 每股止损距离
                        # 例如：风险1500元，每股止损30元，那就买 50股
                        shares_to_buy = risk_amount / stop_loss_dist
                        
                        # 必须有足够的现金
                        cost = shares_to_buy * row['Close']
                        if self.cash >= cost and cost > 500:
                            self.cash -= cost
                            self.positions[ticker] = {
                                'qty': shares_to_buy,
                                'entry_price': row['Close'],
                                'stop_loss_price': row['Close'] - stop_loss_dist # 记录固定的止损价
                            }
                            self.trade_log.append({'Date':date, 'Ticker':ticker, 'Action':'BUY', 'Reason':'Trend+ATR', 'PnL':0})

            self.history_equity.append({'Date': date, 'Total_Equity': daily_portfolio_value})

    def report(self):
            print("\n📊 正在生成专业回测报告...")
            
            # 1. 整理策略数据
            df_eq = pd.DataFrame(self.history_equity).set_index('Date')
            
            # 计算策略累积收益率 (从 0% 开始)
            df_eq['Strategy_Return'] = (df_eq['Total_Equity'] / self.initial_capital) - 1
            
            # 计算回撤
            df_eq['Peak'] = df_eq['Total_Equity'].cummax()
            df_eq['Drawdown'] = (df_eq['Total_Equity'] - df_eq['Peak']) / df_eq['Peak']
            
            # 2. 获取基准数据 (SPY) 用于对比
            print("📥 下载基准指数 (SPY) 进行对比...")
            try:
                start_date = df_eq.index[0]
                end_date = df_eq.index[-1]
                
                # 下载 SPY
                spy = yf.download("SPY", start=start_date, end=end_date, progress=False, auto_adjust=True)
                
                # 修复多层索引问题 (同之前的 Fix)
                if isinstance(spy.columns, pd.MultiIndex):
                    spy.columns = spy.columns.get_level_values(0)
                
                # 计算 SPY 累积收益率 (归一化，让它和策略同一天从 0% 起跑)
                # 逻辑: (今天收盘 / 第一天收盘) - 1
                first_price = spy['Close'].iloc[0]
                spy['Benchmark_Return'] = (spy['Close'] / first_price) - 1
                
                # 合并到一张表 (按日期对齐)
                df_final = df_eq.join(spy['Benchmark_Return'], how='left')
                # 填充空值 (防止SPY某天没数据导致断线)
                df_final['Benchmark_Return'] = df_final['Benchmark_Return'].ffill()
                
            except Exception as e:
                print(f"⚠️ 基准数据下载失败: {e}，将只画策略曲线。")
                df_final = df_eq
                df_final['Benchmark_Return'] = 0 # 没下到就画条直线

            # 3. 计算核心指标
            total_ret = df_eq['Strategy_Return'].iloc[-1]
            max_dd = df_eq['Drawdown'].min()
            
            # 胜率计算
            df_trades = pd.DataFrame(self.trade_log)
            win_rate = 0
            if not df_trades.empty:
                sell_trades = df_trades[df_trades['Action'] == 'SELL']
                if len(sell_trades) > 0:
                    win_rate = len(sell_trades[sell_trades['PnL'] > 0]) / len(sell_trades)

            # 4. 打印文字报告
            print("-" * 50)
            print(f"✅ 回测结束")
            print(f"💰 初始资金: ${self.initial_capital:,.2f}")
            print(f"💰 最终资金: ${df_eq['Total_Equity'].iloc[-1]:,.2f}")
            print(f"📈 策略收益: {total_ret:.2%} (大盘: {df_final['Benchmark_Return'].iloc[-1]:.2%})")
            print(f"🛡️ 最大回撤: {max_dd:.2%}")
            print(f"🎲 交易胜率: {win_rate:.2%} (共 {len(df_trades)} 笔交易)")
            print("-" * 50)

            # 5. 画图 (上图：收益对比，下图：回撤)
            plt.figure(figsize=(12, 8))
            
            # 子图 1: 收益率曲线
            ax1 = plt.subplot(2, 1, 1)
            ax1.plot(df_final.index, df_final['Strategy_Return'], color='#1f77b4', linewidth=2, label='My AI Agent')
            ax1.plot(df_final.index, df_final['Benchmark_Return'], color='gray', linestyle='--', linewidth=1, label='S&P 500 (SPY)')
            
            # 填充绿色/红色区域 (赚钱是绿色，亏钱是红色)
            ax1.fill_between(df_final.index, df_final['Strategy_Return'], 0, where=(df_final['Strategy_Return']>=0), color='green', alpha=0.1)
            ax1.fill_between(df_final.index, df_final['Strategy_Return'], 0, where=(df_final['Strategy_Return']<0), color='red', alpha=0.1)
            
            ax1.set_title('Cumulative Return: Strategy vs Benchmark', fontsize=12, fontweight='bold')
            ax1.grid(True, alpha=0.3)
            ax1.legend(loc='upper left')
            # 设置Y轴显示百分比
            import matplotlib.ticker as mtick
            ax1.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

            # 子图 2: 回撤曲线 (深水区)
            ax2 = plt.subplot(2, 1, 2, sharex=ax1) # 共享X轴
            ax2.plot(df_final.index, df_final['Drawdown'], color='#d62728', linewidth=1, label='Drawdown')
            ax2.fill_between(df_final.index, df_final['Drawdown'], 0, color='#d62728', alpha=0.2)
            
            ax2.set_title(f'Drawdown (Max: {max_dd:.2%})', fontsize=12)
            ax2.grid(True, alpha=0.3)
            ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
            
            plt.tight_layout()
            plt.savefig('backtest_performance.png', dpi=300) # 保存高清图
            print("🖼️ 图表已保存为: backtest_performance.png")
            # plt.show() # 如果是在服务器跑，这一行注释掉；本地跑可以打开


if __name__ == "__main__":
    bot = PortfolioBacktestPro()
    bot.run()
    bot.report()