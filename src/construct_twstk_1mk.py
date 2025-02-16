import time
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import shioaji as sj

from config import (
    REMAINING_BYTES_THRESHOLD,
    SLEEP_TIME,
    LOG_FILE_PATH,
    TWSTK_1mk_PATH,
    API_KEY,
    SECRET_KEY,
    TWSTK_INFO_PATH,
    TRADING_DAYS_PATH,
    START_DATE,
    END_DATE,
    STOCK_IDX_START,
    STOCK_IDX_END
)

def load_twstk_code(twstk_info_path: Path) -> pd.Series:
    twstk_info = pd.read_parquet(twstk_info_path)
    return twstk_info['證券代號'].astype(str)

def load_trading_days(
    trading_days_path: Path,
    start_date: str,
    end_date: str
) -> pd.Series:
    trading_days = pd.read_parquet(trading_days_path)['date']
    
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    trading_days = trading_days[
        (trading_days >= start_date) & (trading_days <= end_date)
    ]
    
    trading_days = trading_days.reset_index(drop=True)
    
    logging.info(f'Loaded {len(trading_days)} trading days from {start_date} to {end_date}.')
    
    return trading_days

def load_stk_from_api(
    api: sj.Shioaji,
    stk_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    '''
    Fetch kbars for a given stock from Shioaji within the specified date range,
    then return as a DataFrame.
    '''
    kbars = api.kbars(
        contract=api.Contracts.Stocks[stk_code],
        start=start_date,
        end=end_date
    )
    time.sleep(SLEEP_TIME)
    stk_df = pd.DataFrame({**kbars})
    stk_df['ts'] = pd.to_datetime(stk_df['ts'])
    stk_df.sort_values(by='ts', inplace=True)
    stk_df = stk_df[['ts', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount']]
    return stk_df

def load_stk_from_local(stk_path: Path) -> pd.DataFrame:
    existing_df = pd.read_parquet(stk_path)
    existing_df['ts'] = pd.to_datetime(existing_df['ts'])
    existing_df.sort_values(by='ts', inplace=True)
    return existing_df

def concat_stk_data(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined.drop_duplicates(subset=['ts'], keep='last', inplace=True)
    combined.sort_values(by='ts', inplace=True)
    combined = combined[['ts', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount']]
    return combined

def group_dates(
    missing_dates: list, 
    trading_days: pd.Series
) -> list[tuple[str, str]]:
    '''
    Group consecutive missing dates and return as (start_date, end_date) tuples.

    Parameters
    ----------
    missing_dates : list
        A list of missing dates, whose elements can be datetime.date or strings in a comparable format.
    trading_days : pd.Series
        Trading day info, which should be a sorted date series (datetime.date or comparable type).

    Returns
    -------
    list of tuple(str, str)
        For example:
        missing_dates = [2024-12-01, 2024-12-02, 2024-12-04]
        -> [('2024-12-01', '2024-12-02'), ('2024-12-04', '2024-12-04')]
    '''
    # 檢查若無缺失日期，直接回傳空清單
    if not missing_dates:
        return []

    # 若 trading_days 尚未排序，先進行排序 (通常交易日應該是已排序)
    trading_days = trading_days.sort_values().reset_index(drop=True)

    # 建立日期 -> index 的對照表，用以檢查是否為連續交易日
    tradingday_index_map = {date: idx for idx, date in enumerate(trading_days)}

    # 確認 missing_dates 為有序：先轉為 set 去重再排
    sorted_missing = sorted(set(missing_dates))

    # 開始分組
    ranges = []
    start = prev = sorted_missing[0]

    for curr in sorted_missing[1:]:
        # 若 curr 或 prev 不在交易日資訊中，可視情況選擇報錯或略過
        if curr not in tradingday_index_map or prev not in tradingday_index_map:
            # 這裡以「直接結束前一段，並將新日期當作新的段落」的方式處理
            ranges.append((str(start), str(prev)))
            start = prev = curr
            continue

        # 依交易日位置判斷是否連續 (index 差為 1)
        if tradingday_index_map[curr] == tradingday_index_map[prev] + 1:
            # 連續則更新 prev 指向當前日期
            prev = curr
        else:
            # 不連續則結束上一段，開啟新段落
            ranges.append((str(start), str(prev)))
            start = prev = curr

    # 最後一段
    ranges.append((str(start), str(prev)))
    return ranges

def login_shioaji(api_key, secret_key):
    api = sj.Shioaji(simulation=True)
    api.login(api_key=api_key, secret_key=secret_key)
    logging.info('API successfully accessed')
    return api

def check_api_limit(api) -> bool:
    remaining = api.usage().remaining_bytes
    logging.info(f'API Remaining bytes: {remaining}')
    if remaining < REMAINING_BYTES_THRESHOLD:
        logging.warning('API usage limit reached')
        return False
    return True

def setup_logging():
    """
    設定 logging 輸出到指定的檔案與 console。
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            # 將日誌輸出到 config.py 中定義的 LOG_FILE_PATH
            logging.FileHandler(str(LOG_FILE_PATH)),
            logging.StreamHandler()
        ]
    )

def get_twstk_1mk() -> None:
    setup_logging()
    logging.info(f'Starting [{STOCK_IDX_START}]-[{STOCK_IDX_END - 1}] {START_DATE} to {END_DATE}')
    
    api = login_shioaji(API_KEY, SECRET_KEY)
    twstk_codes = load_twstk_code(TWSTK_INFO_PATH)
    trading_days = load_trading_days(TRADING_DAYS_PATH, START_DATE, END_DATE)
    
    # 逐檔股票進行資料更新
    for stk_idx in range(STOCK_IDX_START, STOCK_IDX_END):
        if not check_api_limit(api):
            return 1
        stk_code = twstk_codes[stk_idx]
        try:
            stk_path = TWSTK_1mk_PATH / f'{stk_code}.parquet'
            
            # If no local file, fetch directly for the entire date range
            if not stk_path.exists():
                stk_df = load_stk_from_api(api, stk_code, START_DATE, END_DATE)
                stk_df.to_parquet(stk_path, index=False)
                logging.info(f'[{stk_idx}]{stk_code} downloaded directly from API\n')
                continue
            
            #
            existing_df = load_stk_from_local(stk_path)
            existing_dates = existing_df['ts'].dt.date
            missing_dates = sorted(set(trading_days) - set(existing_dates))
            
            # If no missing dates, no action needed
            if not missing_dates:
                logging.info(f'[{stk_idx}]{stk_code} is up-to-date')
                continue
            
            # Group missing dates into consecutive ranges
            date_ranges = group_dates(missing_dates, trading_days)
            for start_str, end_str in date_ranges:
                new_df = load_stk_from_api(api, stk_code, start_str, end_str)
                existing_df = concat_stk_data(existing_df, new_df)
                if start_str == end_str:
                    logging.info(f'[{stk_idx}]{stk_code} updated {start_str}')
                else:
                    logging.info(f'[{stk_idx}]{stk_code} updated {start_str} to {end_str}')
            existing_df.to_parquet(stk_path, index=False)
            logging.info(f'[{stk_idx}]{stk_code} final update complete\n')
        except Exception as e:
            logging.error(f"Error processing [{stk_idx}]{stk_code}: {e}")
            continue
    
    logging.info('Terminated')
    return None

if __name__ == '__main__':
    get_twstk_1mk()