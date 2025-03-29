import pandas as pd
from datetime import date, timedelta
import pandas_market_calendars as mcal
from config import TRADING_DAYS_PATH

def get_twstk_open_days_df(start_date: str, end_date: str) -> pd.DataFrame:
    xtai = mcal.get_calendar('XTAI')
    schedule = xtai.schedule(start_date=start_date, end_date=end_date)
    trading_days = pd.DataFrame(schedule.index, columns=['date'])
    return trading_days

def get_trading_days(start: str = "2018-01-01", end: str = None) -> None:
    """
    取得台股交易日，並輸出 parquet 檔
    """
    if end is None:
        end = date.today().strftime("%Y-%m-%d")

    # 取得交易日
    trading_days = get_twstk_open_days_df(start, end)
    
    # 只保留日期
    trading_days["date"] = pd.to_datetime(trading_days["date"]).dt.date
    
    # 臨時休市日（颱風假等）
    UNSCHEDULED_CLOSED = [
        "2019-08-09",
        "2019-09-30",
        "2022-02-04", # 春節補假但不開市
        "2023-01-18", # 春節，僅交割
        "2023-08-03",
        "2024-02-06", # 春節，僅交割
        "2024-02-07", # 春節，僅交割
        "2024-07-24",
        "2024-07-25",
        "2024-10-02",
        "2024-10-03",
        "2024-10-31",
        "2025-01-23", # 春節，僅交割
        "2025-01-24" # 春節，僅交割
    ]
    # 永豐資料沒有以下四個該有的日期
    # "2019-02-20",
    # "2019-02-21",
    # "2019-02-22",
    # "2019-05-16",
    
    # 移除休市日
    closed_dates = pd.to_datetime(UNSCHEDULED_CLOSED).date
    trading_days = trading_days[~trading_days["date"].isin(closed_dates)]
    
    # 新增 "2018-12-22" 這天
    extra_date = pd.to_datetime(["2018-12-22"]).date
    
    trading_days = pd.concat([trading_days, pd.DataFrame(extra_date, columns=["date"])], ignore_index=True)
    trading_days = trading_days.sort_values(by="date").reset_index(drop=True)
    trading_days.to_parquet(TRADING_DAYS_PATH, index=False)
    
if __name__ == "__main__":
    start = "2018-01-01"
    # end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    end = date.today().strftime('%Y-%m-%d')
    get_trading_days(start, end)
