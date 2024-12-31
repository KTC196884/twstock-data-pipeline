import sys
import logging
import numpy as np

from config import (
    DATA_FOLDER,
    TWSTK_INFO_PATH,
    TRADINGDAY_LIST_PATH,
    START_DATE,
    END_DATE,
    STOCK_IDX_START,
    STOCK_IDX_END,
    API_KEY,
    SECRET_KEY,
    REMAINING_BYTES_THRESHOLD
)
from shioaji_client import login_shioaji
from data_handler import (
    load_TWstk_info,
    load_tradingday_list,
    load_stk_from_api,
    load_stk_from_local,
    group_dates,
    concat_stk_data
)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler('TWstk_1mk_fetching.log'),
            logging.StreamHandler()
        ]
    )

def check_api_limit(api) -> bool:
    remaining = api.usage().remaining_bytes
    logging.info(f'API Remaining bytes: {remaining}')
    if remaining < REMAINING_BYTES_THRESHOLD:
        logging.warning('API usage limit reached')
        return False
    return True

def update_stock(
        api,
        stk_idx: int,
        TWstk_info: np.ndarray,
        tradingday_list: np.ndarray
    ) -> None:
    stk_code = TWstk_info[stk_idx]
    stk_path = DATA_FOLDER / f'{stk_code}.parquet'
    
    if not stk_path.exists():
        stk_df = load_stk_from_api(api, stk_code, START_DATE, END_DATE)
        stk_df.to_parquet(stk_path, index=False)
        logging.info(f'[{stk_idx}]{stk_code} downloaded directly from API\n')
        return

    existing_df = load_stk_from_local(stk_path)
    existing_dates = np.unique(existing_df['ts'].dt.normalize().to_numpy(dtype='datetime64[D]'))
    missing_dates = np.setdiff1d(tradingday_list, existing_dates)
    
    if missing_dates.size == 0:
        logging.info(f'[{stk_idx}]{stk_code} up to date\n')
        return
        
    for start_str, end_str in group_dates(missing_dates, tradingday_list):
        new_df = load_stk_from_api(api, stk_code, start_str, end_str)
        combined_df = concat_stk_data(existing_df, new_df)
        combined_df.to_parquet(stk_path, index=False)
        if start_str == end_str:
            logging.info(f'[{stk_idx}]{stk_code} updated {start_str}')
        else:
            logging.info(f'[{stk_idx}]{stk_code} updated {start_str} to {end_str}')
    logging.info(f'[{stk_idx}]{stk_code} up to date\n')

def main():
    setup_logging()
    logging.info(f'Starting [{STOCK_IDX_START}]-[{STOCK_IDX_END-1}] {START_DATE} to {END_DATE}')
    
    api = login_shioaji(API_KEY, SECRET_KEY)
    TWstk_info = load_TWstk_info(TWSTK_INFO_PATH)
    tradingday_list = load_tradingday_list(TRADINGDAY_LIST_PATH, START_DATE, END_DATE)
    
    for stk_idx in range(STOCK_IDX_START, STOCK_IDX_END):
        if not check_api_limit(api):
            return 1
        
        try:
            update_stock(api, stk_idx, TWstk_info, tradingday_list)
        except Exception as e:
            logging.error(f"Error processing [{stk_idx}]{TWstk_info[stk_idx]}: {e}")
            continue
    return 0

if __name__ == '__main__':
    sys.exit(main())
        
