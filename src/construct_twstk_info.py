import requests
from bs4 import BeautifulSoup
import pandas as pd
from config import TWSTK_INFO_PATH

def fetch_twse_tpex_data():
    """
    從 TWSE (上市) 及 TPEx (上櫃) 的網站抓取證券資料，回傳合併後的 pandas.DataFrame。
    """
    urls = {
        '上市': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2',
        '上櫃': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4',
        '興櫃': 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=5'
    }
    
    all_data = []

    for market_type, url in urls.items():
        print(f'抓取 {market_type} 資料中... URL: {url}')
        resp = requests.get(url)
        resp.encoding = 'big5'

        soup = BeautifulSoup(resp.text, 'html.parser')
        tables = soup.find_all('table')

        for table in tables:
            rows = table.find_all('tr')
            # 從第二行開始，根據網頁格式可能需要調整
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) < 6:
                    continue

                # 格式假設:
                # 0: 有價證券代號及名稱 (例如 "2330 台積電")
                # 1: ISIN Code
                # 2: 上市(櫃)日/發行日
                # 3: 市場別
                # 4: 產業別
                # 5: CFICode
                # 6: 備註 (若有)
                security_name_col = cols[0].text.strip()
                isin_code_col     = cols[1].text.strip()
                listing_date_col  = cols[2].text.strip()
                market_col        = cols[3].text.strip()
                industry_col      = cols[4].text.strip()
                cfi_code_col      = cols[5].text.strip() if len(cols) > 5 else ''
                remark_col        = cols[6].text.strip() if len(cols) > 6 else ''

                # 排除空白或格式不符的資料
                if not security_name_col or not isin_code_col:
                    continue

                all_data.append([
                    security_name_col,  # 原始「有價證券代號及名稱」
                    isin_code_col,
                    listing_date_col,
                    market_col,
                    industry_col,
                    cfi_code_col,
                    remark_col,
                    market_type        # 標記來源：上市或上櫃
                ])

    columns = [
        '有價證券代號及名稱',
        'ISIN Code',
        '上市(櫃)日',
        '市場別',
        '產業別',
        'CFICode',
        '備註',
        '交易板'
    ]
    df = pd.DataFrame(all_data, columns=columns)
    
    # 移除重複資料（視需求調整）
    df.drop_duplicates(subset=['有價證券代號及名稱', 'ISIN Code'], keep='first', inplace=True)
    df = df[df['有價證券代號及名稱'] != ''].reset_index(drop=True)
    
    return df


def classify_by_cficode(cfi_code: str) -> str:
    """
    根據 CFICode 判斷種類:
      - 'CE' → ETF
      - 'CM' → ETN
      - 'ESV' → 普通股
      - 'ED' → TDR
      - 其他 → 未分類
    """
    cfi = cfi_code.strip().upper()
    if cfi.startswith('CE'):
        return 'ETF'
    elif cfi.startswith('CM'):
        return 'ETN'
    elif cfi.startswith('ESV'):
        return '普通股'
    elif cfi.startswith('ED'):
        return 'TDR'
    else:
        return '未分類'

def get_twstk_info() -> None:
    # 1. 抓取資料
    df = fetch_twse_tpex_data()
    
    # 2. 根據 CFICode 分類
    df['種類'] = df['CFICode'].apply(classify_by_cficode)
    
    # 3. 過濾只保留普通股、ETF、ETN、TDR
    allowed_types = ['普通股', 'ETF', 'ETN', 'TDR']
    df = df[df['種類'].isin(allowed_types)].copy()
    
    # 4. 用正則表達式分離「證券代號」與「證券名稱」
    split_df = df['有價證券代號及名稱'].str.extract(r'^(?P<證券代號>\S+)\s+(?P<證券名稱>.+)$')
    df['證券代號'] = split_df['證券代號']
    df['證券名稱'] = split_df['證券名稱']
    
    # 5. 指定排序順序：主要順序為 普通股 → TDR → ETF → ETN；同一種類中，上市在前、上櫃在後
    type_order_map = {
        '普通股': 1,
        'TDR': 2,
        'ETF': 3,
        'ETN': 4
    }
    market_order_map = {
        '上市': 1,
        '上櫃': 2,
        '興櫃': 3
    }
    df['type_order'] = df['種類'].map(type_order_map).fillna(999)
    df['market_order'] = df['交易板'].map(market_order_map).fillna(999)
    
    df_sorted = df.sort_values(by=['type_order', 'market_order']).reset_index(drop=True)
    
    final_columns = [
        '證券代號',
        '證券名稱',
        '種類', # ETF、ETN、普通股、TDR
        '交易板', # ['上市' '上櫃']
        'ISIN Code',
        '上市(櫃)日',
        '市場別', # ['上市' '上市臺灣創新板' '上櫃']
        '產業別',
        'CFICode',
        '備註'
    ]
    twstk_info = df_sorted[final_columns]
    
    twstk_info.loc[:, '產業別'] = twstk_info.apply(
        lambda row: row['種類'] if pd.isna(row['產業別']) or row['產業別'].strip() == '' else row['產業別'],
        axis=1
    )
    
    print(twstk_info)
    twstk_info.to_parquet(TWSTK_INFO_PATH, index=False)

    return None

if __name__ == "__main__":
    get_twstk_info()
