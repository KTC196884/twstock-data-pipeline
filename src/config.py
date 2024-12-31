from pathlib import Path
from datetime import date

# === 路徑及常數設定 ===
DATA_FOLDER = Path("/Users/jack/Documents/Quant/basic_data/TW_stock_1mk")
TWSTK_INFO_PATH = Path('THE_PATH_OF_TWstk_info.parquet')
TRADINGDAY_LIST_PATH = Path('/Users/jack/Documents/Quant/basic_data/tradingday_list.npy')

# START_DATE = '2018-12-07'
START_DATE = '2024-12-30'
END_DATE = date.today().strftime("%Y-%m-%d")

REMAINING_BYTES_THRESHOLD = 1e6
SLEEP_TIME = 0.25

STOCK_IDX_START, STOCK_IDX_END = (1268, 2170)

# === API KEY ===
API_KEY = 'YOUR_API_KEY'
SECRET_KEY = 'YOUR_SECRET_KEY'
