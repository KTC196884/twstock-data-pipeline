import time
import sys
import shioaji as sj

from config import REMAINING_BYTES_THRESHOLD, SLEEP_TIME

def login_shioaji(api_key, secret_key):
    """
    建立並登入 Shioaji 連線
    """
    api = sj.Shioaji(simulation=True)
    api.login(api_key=api_key, secret_key=secret_key)
    return api

def remaining_api_usage(api):
    """
    回傳目前剩餘的 API bytes
    """
    usage = api.usage()
    return usage.remaining_bytes

def ensure_api_usage(api):
    """
    檢查 API bytes 是否不足，如不足則結束程式
    """
    if remaining_api_usage(api) <= REMAINING_BYTES_THRESHOLD:
        sys.exit()
    else:
        time.sleep(SLEEP_TIME)
