import os

# 1. 你的自选股池
WATCHLIST = [
    "NVDA", "TSLA", "AAPL", "MSFT", "AMD", "COIN", "MSTR", 
    "GOOGL", "AMZN", "META", "LITE", "ORCL", 'NBIS', 'CRWV',
    'CLS', 'CRDO', 'ALAB', 'RKLB', 'ASTS', 'MU', 'SNDK', 'INTC',
    'OKLO', 'CCJ', 'BE', 'APP', 'VST', 'GEV', 'AVGO', 'TSM', 'AMD',
    'STX', 'WDC', 'FLNC', 'SMR', 'CIEN', 'COHR', 'UBER', 'HOOD', 'MSTR',
    'CRCL', 'ONDS', 'CEG', 'VRT', 'TLN', 'RDW', 'ANET', 'FN', 'IONQ', 'RGTI',
    'PLTR', 'NFLX', 'EL'
]

# 2. Gemini API Key (建议从环境变量获取，或者暂时填在这里)
# ⚠️ 注意：不要将含 Key 的代码上传到 GitHub
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "你的_GEMINI_API_KEY_粘贴在这里")

# 3. 策略参数
# 判定回调的误差范围 (1.5%)
TOLERANCE = 0.015 
# 数据库名称
DB_NAME = "stock_data.db"