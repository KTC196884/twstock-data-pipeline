import pandas as pd
from datetime import date
import pandas_market_calendars as mcal
from config import TRADING_DAYS_PATH

def get_twstk_open_days_df(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Retrieve a DataFrame of trading days within a specified date range
    using the XTAI calendar from pandas_market_calendars.

    Parameters
    ----------
    start_date : str
        The start date in "YYYY-MM-DD" format (e.g., "2023-01-01").
    end_date : str
        The end date in "YYYY-MM-DD" format (e.g., "2023-12-31").

    Returns
    -------
    pd.DataFrame
        A DataFrame with a DatetimeIndex of trading days and columns:
        ["market_open", "market_close"], both converted to "Asia/Taipei" timezone.
    """
    xtai = mcal.get_calendar('XTAI')
    
    schedule = xtai.schedule(start_date=start_date, end_date=end_date)
    
    schedule["market_open"] = schedule["market_open"].dt.tz_convert("Asia/Taipei")
    schedule["market_close"] = schedule["market_close"].dt.tz_convert("Asia/Taipei")
    
    return schedule

def get_trading_days(start: str = "2018-01-01", end: str = None) -> None:
    """
    取得台股交易日，並輸出 parquet 檔
    """
    if end is None:
        end = date.today().strftime("%Y-%m-%d")

    # 取得交易日
    trading_days = get_twstk_open_days_df(start, end) \
                      .reset_index() \
                      .rename(columns={'index': 'date'})
    
    # 只保留日期
    trading_days["date"] = pd.to_datetime(trading_days["date"]).dt.date
    
    # 臨時休市日（颱風假）
    UNSCHEDULED_CLOSURES = [
        "2019-08-09",
        "2019-09-30",
        "2023-08-03",
        "2024-07-24",
        "2024-07-25",
        "2024-10-02",
        "2024-10-03",
        "2024-10-31"
    ]
    
    # 移除休市日
    closure_dates = pd.to_datetime(UNSCHEDULED_CLOSURES).date
    trading_days = trading_days[~trading_days["date"].isin(closure_dates)]
    
    # 儲存 parquet
    trading_days.to_parquet(TRADING_DAYS_PATH, index=False)
    
if __name__ == "__main__":
    start = "2018-01-01"
    end   = date.today().strftime("%Y-%m-%d")
    get_trading_days(start, end)
    


