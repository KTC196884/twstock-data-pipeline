import pandas as pd

from construct_trading_days import get_trading_days
from construct_twstk_info import get_twstk_info
from construct_twstk_1mk import get_twstk_1mk

from config import (
    TRADING_DAYS_PATH,
    TWSTK_INFO_PATH,
    LOG_FILE_PATH,
    DATA_DIR,
    TWSTK_1mk_PATH,
)

def setup_directories():
    """
    在開始前檢查/建立 data/ 和 logs/ 資料夾，以及 twstk_1mk 目錄。
    """
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        print(f"[setup_directories] Created data/ at {DATA_DIR}")
    log_dir = LOG_FILE_PATH.parent
    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)
        print(f"[setup_directories] Created logs/ at {log_dir}")
    
    if not TWSTK_1mk_PATH.exists():
        TWSTK_1mk_PATH.mkdir(parents=True, exist_ok=True)
        print(f"[setup_directories] Created twstk_1mk/ at {TWSTK_1mk_PATH}")

def main():
    setup_directories()
    get_trading_days()
    print(pd.read_parquet(TRADING_DAYS_PATH))
    get_twstk_info()
    print(pd.read_parquet(TWSTK_INFO_PATH))
    get_twstk_1mk()
    
if __name__ == '__main__':
    main()
