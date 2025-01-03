import time
import pandas as pd
import numpy as np
from pathlib import Path
import logging

from config import (
    START_DATE,
    END_DATE,
    SLEEP_TIME
)

def load_twstk_info(
        twstk_info_path: Path
    ) -> np.array:
    twstk_info = pd.read_parquet(twstk_info_path)
    return np.array(twstk_info['code']).astype('str')

def load_tradingday_list(
        tradingday_list_path: Path,
        start_date: str,
        end_date: str
    ) -> list:
    """
    載入交易日清單，並依照所需期間篩選、排序
    """
    tradingday_list = np.load(tradingday_list_path, allow_pickle=True)
    
    start_date = np.datetime64(start_date)
    end_date = np.datetime64(end_date)
    
    tradingday_list = tradingday_list[
        (tradingday_list >= start_date) & (tradingday_list <= end_date)
    ]
    # logging.info("Loaded trading days.")
    return tradingday_list

def load_stk_from_api(
        api,
        stk_code,
        start_date,
        end_date,
    ) -> pd.DataFrame:
    """
    從 Shioaji 抓取股票 kbars 並儲存至本地
    """
    kbars = api.kbars(
        contract=api.Contracts.Stocks[stk_code],
        start=start_date,
        end=end_date
    )
    time.sleep(SLEEP_TIME)
    stk_df = pd.DataFrame({**kbars})
    stk_df["ts"] = pd.to_datetime(stk_df["ts"])
    stk_df.sort_values(by="ts", inplace=True)
    stk_df = stk_df[["ts", "Open", "High", "Low", "Close", "Volume", "Amount"]]
    return stk_df

def load_stk_from_local(
        stk_path: Path
    ) -> pd.DataFrame():
    existing_df = pd.read_parquet(stk_path)
    existing_df['ts'] = pd.to_datetime(existing_df['ts'])
    existing_df.sort_values(by='ts', inplace=True)
    return existing_df


def concat_stk_data(
        old_df: pd.DataFrame,
        new_df: pd.DataFrame
    ) -> pd.DataFrame:
    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined.drop_duplicates(subset=["ts"], keep="last", inplace=True)
    combined.sort_values(by="ts", inplace=True)
    combined = combined[["ts", "Open", "High", "Low", "Close", "Volume", "Amount"]]
    return combined

def group_dates(dates, tradingday_list):
    """
    將缺失日期按照連續性群組
    """
    tradingday_list_index = {date: idx for idx, date in enumerate(tradingday_list)}
    ranges = []
    dates = sorted(dates)
    start = prev = dates[0]
    for curr in dates[1:]:
        if tradingday_list_index[curr] == tradingday_list_index[prev] + 1:
            prev = curr
        else:
            ranges.append((str(start), str(prev)))
            start = prev = curr
    ranges.append((str(start), str(prev)))
    return ranges

def update_stock(
        api,
        stk_idx: int,
        twstk_info: np.ndarray,
        tradingday_list: np.ndarray,
        twstk_dir: Path
    ) -> None:
    stk_code = twstk_info[stk_idx]
    stk_path = twstk_dir / f'{stk_code}.parquet'
    
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
