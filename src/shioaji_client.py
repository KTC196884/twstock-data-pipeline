import shioaji as sj
import logging

from config import REMAINING_BYTES_THRESHOLD

def login_shioaji(api_key, secret_key):
    '''
    建立並登入 Shioaji 連線
    '''
    api = sj.Shioaji(simulation=True)
    api.login(api_key=api_key, secret_key=secret_key)
    logging.info('API successfully accessed')
    return api

def check_api_limit(api) -> bool:
    '''
    檢查 API 用量
    '''
    remaining = api.usage().remaining_bytes
    logging.info(f'API Remaining bytes: {remaining}')
    if remaining < REMAINING_BYTES_THRESHOLD:
        logging.warning('API usage limit reached')
        return False
    return True
