import sqlite3 
import pandas as pd

# 1. 连接数据库
conn = sqlite3.connect('stock_data.db')

# 2. 构造查询语句 (SQL)
# 假设我们要读 NVDA 的数据，表名通常是 stock_NVDA
ticker = "NVDA"
table_name = f"stock_{ticker}" 

# 3. 读取数据
# parse_dates=['Date'] 的作用是把字符串格式的日期自动转回时间格式
df = pd.read_sql(f"SELECT * FROM {table_name}", conn, parse_dates=['Date'])

# 4. 打印看看
print(f"成功读取 {len(df)} 行数据")
print(df.head())     # 看前5行
print(df.tail())     # 看最后5行

# 5. 关闭连接
conn.close()