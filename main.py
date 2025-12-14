from data_engine import StockDataEngine
from strategy import StrategyRunner, MarketPhaseScanner, ReversalScanner, TrendlineScanner, HighWinRateScanner
import config

def main():
    # 1. 更新数据 (DataEngine 代码略...)
    engine = StockDataEngine()
    engine.update_all() 

    # 2. 初始化策略运行器
    runner = StrategyRunner()
    
    # === 运行策略 A: 找涨幅榜 ===
    top_gainers = runner.run_top_gainers(days=20, top_n=10)
    
    # === 运行策略 B: 找回调机会 ===
    pullbacks = runner.run_ema_pullback()
    
    # === 运行策略 C: 找最强趋势 ===
    trends = runner.run_strong_trend()
    
    # === 运行策略 D: 共振策略 ===
    combo_signals = runner.run_macd_divergence_kdj()
    
    runner.close() # 记得关闭数据库连接

    # 3. 汇总结果并展示 (或者发给 AI)
    print("\n" + "="*40)
    print("📊 今日策略扫描汇总")
    print("="*40)

    if top_gainers:
        print(f"\n🔥 [涨幅榜 TOP 5]")
        # 打印表头，让显示更整齐
        print(f"   {'代码':<6} | {'涨幅':<7} | {'起涨价':<10} -> {'现价':<10}")
        print("   " + "-" * 45)
        
        for item in top_gainers:
            # 使用 <6, <10 这种语法来控制对齐，保证看起来像表格一样工整
            print(f"   {item['Ticker']:<6} | +{item['Score']:<6.2%} | ${item['Prev_Close']:<9.2f} -> ${item['Close']:<9.2f}")

    if pullbacks:
        print(f"\n📉 [回调买点监控]")
        for item in pullbacks:
            print(f"   {item['Ticker']}: {item['Detail']}")
            
    if trends:
        print(f"\n🚀 [强势多头排列] (共{len(trends)}只)")
        # 仅打印前3只示例
        for item in trends:
            print(f"   {item['Ticker']}")

    if combo_signals:
        print(f"\n💎 [MACD底背离 + KDJ金叉 共振] (极高价值)")
        print(f"   {'代码':<6} | {'现价':<10} | {'详情'}")
        print("   " + "-" * 50)
        for item in combo_signals:
            print(f"   {item['Ticker']:<6} | ${item['Close']:<9.2f} | {item['Detail']}")

    # 4. (可选) 将这些 list 传给 ai_analyst.generate_report(...)

    # === 运行策略 F: 市场阶段扫描 ===
    phase_scanner = MarketPhaseScanner()
    market_status = phase_scanner.analyze_phase()
    
    print(f"\n🌍 [全市场阶段扫描结果]")
    print(f"{'代码':<6} | {'现价':<8} | {'综合评分':<8} | {'所处阶段':<20} | {'操作建议'}")
    print("-" * 80)
    
    # 打印前 5 个 (最强右侧)
    for item in market_status[:5]:
        print(f"{item['Ticker']:<6} | ${item['Close']:<7.2f} | {item['Score']:<8} | {item['Phase']:<20} | {item['Advice']}")
    
    print("." * 80)
    
    # 打印中间 3 个 (震荡股)
    mid = len(market_status) // 2
    for item in market_status[mid-1:mid+2]:
        print(f"{item['Ticker']:<6} | ${item['Close']:<7.2f} | {item['Score']:<8} | {item['Phase']:<20} | {item['Advice']}")

    print("." * 80)
    
    # 打印后 5 个 (最惨左侧)
    for item in market_status[-5:]:
        print(f"{item['Ticker']:<6} | ${item['Close']:<7.2f} | {item['Score']:<8} | {item['Phase']:<20} | {item['Advice']}")

    phase_scanner.close()

    # === 运行策略 G: 底部反转启动 ===
    rev_scanner = ReversalScanner()
    # 调用新的短线方法
    short_reversals = rev_scanner.run_short_term_reversal()
    
    if short_reversals:
        print(f"\n⚡ [超短线反转] (5日/10日金叉启动)")
        print(f"   {'代码':<6} | {'现价':<10} | {'详情'}")
        print("   " + "-" * 60)
        for item in short_reversals:
            print(f"   {item['Ticker']:<6} | ${item['Close']:<9.2f} | {item['Detail']}")

    rev_scanner.conn.close()
    
    # === 运行策略 ===
    scanner = TrendlineScanner()
    breakouts = scanner.run_trendline_breakout()
    
    if breakouts:
        print(f"\n📐 [长期趋势突破] (基于远端高点画线)")
        print(f"   {'代码':<6} | {'压制时长':<8} | {'现价/阻力':<18} | {'关键锚点 (A -> B)'}")
        print("   " + "-" * 90)
        
        for item in breakouts:
            price_info = f"${item['Close']:.2f} / ${item['Resistance']:.2f}"
            points = f"{item['PointA']} -> {item['PointB']}"
            
            print(f"   {item['Ticker']:<6} | {item['Duration']}天     | {price_info:<18} | {points}")

    # === 运行策略 L: 高胜率超跌 ===
    hw_scanner = HighWinRateScanner()
    opportunities = hw_scanner.run()
    
    if opportunities:
        print(f"\n🏆 [高胜率黄金坑] (RSI<30, ATR>4%, 市值>200亿)")
        print(f"   {'代码':<6} | {'RSI':<5} | {'波动率':<6} | {'市值(亿)':<8} | {'建议持有'}")
        print("   " + "-" * 65)
        
        for item in opportunities:
            print(f"   {item['Ticker']:<6} | {item['RSI']:<5.1f} | {item['ATR_Pct']:<5.1f}% | {item['MarketCap']:<8.0f} | 2周左右")
    else:
        print("\n😴 [高胜率策略] 今日无符合条件的标的 (机会稀缺，耐心等待)")

if __name__ == "__main__":
    main()