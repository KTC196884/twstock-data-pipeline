import time
import pandas as pd
import pickle
import logging
from datetime import datetime, timedelta
import shioaji as sj

from config import (
    REMAINING_BYTES_THRESHOLD,
    SLEEP_TIME,
    LOG_FILE_PATH,
    TWSTK_1mk_PATH,
    API_KEY,
    SECRET_KEY,
    TWSTK_INFO_PATH,
    TWSTK_PROGRESS_PATH,
    START_DATE,
    END_DATE
)

def load_stk_from_api(api: sj.Shioaji, stk_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch stock data for the specified date range via API.
    """
    try:
        kbars = api.kbars(
            contract=api.Contracts.Stocks[stk_code],
            start=start_date,
            end=end_date
        )
        time.sleep(SLEEP_TIME)
        stk_df = pd.DataFrame({**kbars})
        stk_df['ts'] = pd.to_datetime(stk_df['ts'])
        stk_df.sort_values(by='ts', inplace=True)
        return stk_df[['ts', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount']]
    except Exception as e:
        logging.exception(f"Failed to fetch data for {stk_code}: {e}")
        raise

def concat_stk_data(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined.drop_duplicates(subset=['ts'], keep='last', inplace=True)
    combined.sort_values(by='ts', inplace=True)
    return combined[['ts', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount']]

def setup_logging():
    """
    Set up logging to output to both a file and the console.
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

def save_twstk_progress(progress: dict) -> None:
    """
    Save the progress dictionary to a file for resuming later.
    """
    try:
        with open(TWSTK_PROGRESS_PATH, 'wb') as f:
            pickle.dump(progress, f)
    except Exception as e:
        logging.exception(f"Failed to save progress file: {e}")
        raise

def get_twstk_1mk() -> int:
    """
    Main function to download or update one-minute stock data within the set date range.
    Returns 1 if API usage limit is reached or login fails; otherwise, returns 0.
    """
    setup_logging()
    logging.info(f'Starting data fetch: {START_DATE} to {END_DATE}')
    
    try:
        api = sj.Shioaji(simulation=True)
        api.login(api_key=API_KEY, secret_key=SECRET_KEY)
        logging.info('Successfully connected to API')
    except Exception as e:
        logging.exception(f"API login failed: {e}")
        return 1

    # Read stock codes
    twstk_codes = pd.read_parquet(TWSTK_INFO_PATH)['證券代號'].astype(str)
    
    start_date_dt = datetime.strptime(START_DATE, '%Y-%m-%d').date()
    end_date_dt = datetime.strptime(END_DATE, '%Y-%m-%d').date()
    
    # Read the progress from the previous run; if not available, initialize as empty dict
    try:
        with open(TWSTK_PROGRESS_PATH, 'rb') as f:
            twstk_progress = pickle.load(f)
    except Exception as e:
        logging.warning(f"TWSTK_PROGRESS not found or corrupted. Initializing progress dictionary. {e}")
        twstk_progress = {}

    for stk_code in twstk_codes:
        usage_remaining = api.usage().remaining_bytes
        logging.info(f'[{stk_code}] API remaining: {usage_remaining} bytes')
        
        if usage_remaining < REMAINING_BYTES_THRESHOLD:
            logging.warning('API usage limit reached')
            return 1
        
        stk_path = TWSTK_1mk_PATH / f'{stk_code}.parquet'
        
        # Since twstk_progress now stores datetime.date objects, we can directly unpack
        progress_range = twstk_progress.get(stk_code)
        if progress_range:
            progress_start, progress_end = progress_range
        else:
            progress_start, progress_end = None, None
        
        # If the data file does not exist, download the full range directly
        if not stk_path.exists():
            try:
                stk_df = load_stk_from_api(api, stk_code, START_DATE, END_DATE)
            except Exception as e:
                logging.error(f"Error fetching data for {stk_code}: {e}")
                continue
            stk_df.to_parquet(stk_path, index=False)
            twstk_progress[stk_code] = (start_date_dt, end_date_dt)
            save_twstk_progress(twstk_progress)
            logging.info(f'[{stk_code}] Downloaded directly')
            continue
        
        # If the stored progress already covers the desired date range, skip updating
        if progress_range and start_date_dt >= progress_start and end_date_dt <= progress_end:
            logging.info(f'[{stk_code}] Data is already complete')
            continue
        
        # Read existing data and update missing parts based on progress
        existing_df = pd.read_parquet(stk_path)
        
        # Fetch missing beginning period if needed
        if not progress_range or start_date_dt < progress_start:
            fetch_end = (progress_start - timedelta(days=1)).strftime("%Y-%m-%d") if progress_range else END_DATE
            try:
                new_df = load_stk_from_api(api, stk_code, START_DATE, fetch_end)
            except Exception as e:
                logging.error(f"Error fetching data for {stk_code}: {e}")
                continue
            existing_df = concat_stk_data(existing_df, new_df)
        
        # Fetch missing ending period if needed
        if not progress_range or end_date_dt > progress_end:
            fetch_start = (progress_end + timedelta(days=1)).strftime("%Y-%m-%d") if progress_range else START_DATE
            try:
                new_df = load_stk_from_api(api, stk_code, fetch_start, END_DATE)
            except Exception as e:
                logging.error(f"Error fetching data for {stk_code}: {e}")
                continue
            existing_df = concat_stk_data(existing_df, new_df)
        
        # Update progress and save the updated data file
        twstk_progress[stk_code] = (start_date_dt, end_date_dt)
        save_twstk_progress(twstk_progress)
        existing_df.to_parquet(stk_path, index=False)
        logging.info(f'[{stk_code}] Data update complete')
    
    save_twstk_progress(twstk_progress)
    logging.info('All processing completed')
    
    return 0

if __name__ == '__main__':
    get_twstk_1mk()
    
