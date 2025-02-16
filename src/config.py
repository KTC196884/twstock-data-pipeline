from pathlib import Path
from datetime import date

# === 路徑及常數設定 ===
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 資料與日誌目錄
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

# 個別檔案/資料夾路徑
TWSTK_INFO_PATH = DATA_DIR / "twstk_info.parquet"
TRADING_DAYS_PATH = DATA_DIR / "trading_days.parquet"
TWSTK_1mk_PATH = DATA_DIR / "twstk_1mk"

# 日誌檔位置
LOG_FILE_PATH = LOGS_DIR / "twstk_1mk_fetching.log"

#
START_DATE = '2025-02-01' # START_DATE must >= '2018-12-07'
END_DATE = date.today().strftime("%Y-%m-%d")

#
REMAINING_BYTES_THRESHOLD = 1e6
SLEEP_TIME = 0.25

#
STOCK_IDX_START, STOCK_IDX_END = (0, 10)

# === API KEY ===
API_KEY = 'YOUR_API_KEY'
SECRET_KEY = 'YOUR_SECRET_KEY'
