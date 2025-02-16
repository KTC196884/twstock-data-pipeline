from pathlib import Path
import pandas as pd
from config import TWSTK_1mk_PATH, TRADING_DAYS_PATH, TWSTK_INFO_PATH

#%%
parquet_files = list(TWSTK_1mk_PATH.glob("*.parquet"))

for file in parquet_files:
    stock_code = file.stem  # 取得股票代號（檔名去掉 .parquet）
    df = pd.read_parquet(file)
    print(f"Stock Code: {stock_code}")
    print(df)

#%%
trading_days = pd.read_parquet(TRADING_DAYS_PATH)
print(trading_days)

#%%
twstk_info = pd.read_parquet(TWSTK_INFO_PATH)
print(twstk_info)