from pathlib import Path
from datetime import date

# === 路徑及常數設定 ===
TWSTK_INFO_PATH = Path('THE_PATH_OF_twstk_info.parquet')
TRADINGDAY_LIST_PATH = Path('THE_PATH_OF_tradingday_list.npy')

# START_DATE = '2018-12-07' # shioaji api 1mk 資料起始日
START_DATE = '2024-12-15'
END_DATE = date.today().strftime("%Y-%m-%d")

REMAINING_BYTES_THRESHOLD = 1e6
SLEEP_TIME = 0.25

# STOCK_IDX_START, STOCK_IDX_END = (0, 2170)
STOCK_IDX_START, STOCK_IDX_END = (0, 2170)

# === API KEY ===
API_KEY = 'YOUR_API_KEY'
SECRET_KEY = 'YOUR_SECRET_KEY'
