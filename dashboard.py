import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from strategy import StrategyRunner
from data_engine import StockDataEngine
import config

# ================= 页面配置 =================
st.set_page_config(
    page_title="SmartTrader AI 监控台",
    page_icon="📈",
    layout="wide"  # 宽屏模式，看表格更舒服
)

# ================= 侧边栏：控制区 =================
st.sidebar.title("🚀 控制台")
st.sidebar.info("数据源: 本地 SQLite")

# 重新加载数据的按钮
if st.sidebar.button("🔄 立即运行数据更新"):
    with st.spinner("正在连接 Yahoo Finance 更新数据..."):
        engine = StockDataEngine()
        engine.update_all()
        engine.close()
    st.sidebar.success("数据更新完毕！请刷新页面。")

# 股票选择器 (用于画图)
st.sidebar.markdown("---")
st.sidebar.subheader("📊 个股 K 线分析")
selected_ticker = st.sidebar.selectbox("选择股票查看详情:", config.WATCHLIST)

# ================= 主页面：策略扫描结果 =================
st.title("📈 SmartTrader AI 量化看板")

# 初始化策略运行器
runner = StrategyRunner()

# 创建两列布局
col1, col2 = st.columns(2)

with col1:
    st.subheader("🔥 20日涨幅榜 (Top Gainers)")
    # 获取数据
    top_gainers = runner.run_top_gainers(days=20, top_n=10)
    if top_gainers:
        df_gainers = pd.DataFrame(top_gainers)
        # 美化表格显示
        st.dataframe(
            df_gainers[['Ticker', 'Close', 'Score']],
            column_config={
                "Ticker": "股票代码",
                "Close": st.column_config.NumberColumn("现价", format="$%.2f"),
                "Score": st.column_config.NumberColumn("20日涨幅", format="%.2f%%")
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("暂无数据")

with col2:
    st.subheader("📉 均线回调监控 (Pullbacks)")
    pullbacks = runner.run_ema_pullback()
    if pullbacks:
        df_pullback = pd.DataFrame(pullbacks)
        # 只要展示关键信息
        st.dataframe(
            df_pullback[['Ticker', 'Detail']],
            column_config={
                "Ticker": "股票代码",
                "Detail": "信号详情"
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.success("今日无回调买入信号，市场强势或处于空头。")

runner.close()

# ================= 下方：交互式 K 线图 =================
st.markdown("---")
st.subheader(f"🕯️ {selected_ticker} 技术走势图")

# 获取历史数据用于画图
engine = StockDataEngine()
# 直接写 SQL 读全部历史
try:
    df_hist = pd.read_sql(
        f"SELECT * FROM stock_{selected_ticker.replace('-', '_')} ORDER BY Date ASC", 
        engine.conn,
        parse_dates=['Date']
    )
    
    # 为了画图清晰，只取最近 1 年
    df_chart = df_hist.tail(250).reset_index(drop=True)

    # 使用 Plotly 画专业的 K 线图
    fig = go.Figure()

    # 1. 画 K 线
    fig.add_trace(go.Candlestick(
        x=df_chart['Date'],
        open=df_chart['Open'],
        high=df_chart['High'],
        low=df_chart['Low'],
        close=df_chart['Close'],
        name='K Line'
    ))

    # 2. 画均线 (EMA20 黄色, EMA60 蓝色)
    if 'EMA20' in df_chart.columns:
        fig.add_trace(go.Scatter(x=df_chart['Date'], y=df_chart['EMA20'], mode='lines', name='EMA20', line=dict(color='orange', width=1)))
    
    if 'EMA60' in df_chart.columns:
        fig.add_trace(go.Scatter(x=df_chart['Date'], y=df_chart['EMA60'], mode='lines', name='EMA60', line=dict(color='blue', width=1)))
        
    if 'EMA200' in df_chart.columns:
        fig.add_trace(go.Scatter(x=df_chart['Date'], y=df_chart['EMA200'], mode='lines', name='EMA200', line=dict(color='purple', width=2)))

    # 设置布局：去掉周末空缺，增加滑动条
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=600,
        title=f"{selected_ticker} Price vs EMA",
        template="plotly_dark" # 暗黑模式，很专业
    )

    # 显示图表
    st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"无法读取 {selected_ticker} 的数据，请先运行数据更新。错误: {e}")

engine.close()