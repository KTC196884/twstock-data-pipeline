from pathlib import Path
from datetime import date, timedelta

# === 路徑及常數設定 ===
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 資料與日誌目錄
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

# 個別檔案/資料夾路徑
TWSTK_INFO_PATH = DATA_DIR / "twstk_info.parquet"
TRADING_DAYS_PATH = DATA_DIR / "trading_days.parquet"
TWSTK_1mk_PATH = DATA_DIR / "twstk_1mk"
TWSTK_PROGRESS_PATH = DATA_DIR / "twstk_progress.pkl"

# 日誌檔位置
LOG_FILE_PATH = LOGS_DIR / "twstk_1mk_fetching.log"

#
START_DATE = '2018-12-07' # START_DATE must >= '2018-12-07'
END_DATE = date.today().strftime("%Y-%m-%d")
# END_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

#
REMAINING_BYTES_THRESHOLD = 1e6
SLEEP_TIME = 0.25

# === API KEY ===
API_KEY = 'YOUR API KEY'
SECRET_KEY = 'YOUR SECRET KEY'
