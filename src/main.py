import sys
import logging
from pathlib import Path

from config import (
    TWSTK_INFO_PATH,
    TRADINGDAY_LIST_PATH,
    START_DATE,
    END_DATE,
    STOCK_IDX_START,
    STOCK_IDX_END,
    API_KEY,
    SECRET_KEY
)
from shioaji_client import (
    login_shioaji,
    check_api_limit
)
from data_handler import (
    load_twstk_info,
    load_tradingday_list,
    update_stock
)

#%%
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler('twstk_1mk_fetching.log'),
            logging.StreamHandler()
        ]
    )

def setup_twstk_directory() -> Path:
    """
    Sets up the directory structure for the project.
    Ensures the twstk_1mk directory exists within the project root.
    """
    # project root: 上一層
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    twstk_dir = PROJECT_ROOT / 'twstk_1mk'

    if not twstk_dir.exists():
        twstk_dir.mkdir(parents=True, exist_ok=True)
        logging.info('twstk_1mk did not exist and has been created')
    return twstk_dir

#%%
def main():
    setup_logging()
    logging.info(f'Starting [{STOCK_IDX_START}]-[{STOCK_IDX_END - 1}] {START_DATE} to {END_DATE}')
    
    # 建立或確認資料夾存在
    twstk_dir = setup_twstk_directory()
    
    # 登入 Shioaji
    api = login_shioaji(API_KEY, SECRET_KEY)
    
    # 讀取股票代碼清單 & 交易日清單
    twstk_info = load_twstk_info(TWSTK_INFO_PATH)
    tradingday_list = load_tradingday_list(TRADINGDAY_LIST_PATH, START_DATE, END_DATE)
    
    # 逐檔股票進行資料更新
    for stk_idx in range(STOCK_IDX_START, STOCK_IDX_END):
        if not check_api_limit(api):
            return 1
        
        try:
            update_stock(api, stk_idx, twstk_info, tradingday_list, twstk_dir)
        except Exception as e:
            logging.error(f"Error processing [{stk_idx}]{twstk_info[stk_idx]}: {e}")
            continue
    return 0

if __name__ == '__main__':
    sys.exit(main())
