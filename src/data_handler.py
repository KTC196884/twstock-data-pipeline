import time
import pandas as pd
import numpy as np
from pathlib import Path

from config import SLEEP_TIME

def load_TWstk_info(
        TWstk_info_path: Path
    ) -> np.array:
    TWstk_info = pd.read_parquet(TWstk_info_path)
    return np.array(TWstk_info['code']).astype('str')

def load_stk_from_api(
        api,
        code,
        start_date,
        end_date,
    ) -> pd.DataFrame:
    """
    從 Shioaji 抓取股票 kbars 並儲存至本地
    """
    kbars = api.kbars(
        contract=api.Contracts.Stocks[code],
        start=start_date,
        end=end_date
    )
    time.sleep(SLEEP_TIME)
    df = pd.DataFrame({**kbars})
    df["ts"] = pd.to_datetime(df["ts"])
    df.sort_values(by="ts", inplace=True)
    df = df[["ts", "Open", "High", "Low", "Close", "Volume", "Amount"]]
    return df

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