import shioaji as sj

def login_shioaji(api_key, secret_key):
    """
    建立並登入 Shioaji 連線
    """
    api = sj.Shioaji(simulation=True)
    api.login(api_key=api_key, secret_key=secret_key)
    return api
