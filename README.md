# TWSTK 1-minute Kbars Fetcher

使用 [Shioaji](https://sinotrade.github.io) API 來取得台股 1 minute kbars 資料，並儲存為 Parquet 格式。  
也提供自動檢查遺漏交易日並補齊資料的機制。

## 專案結構
```bash
twstk_1mk_fetcher/
├── src/
│   ├── config.py
│   ├── logger.py
│   ├── shioaji_client.py
│   ├── data_handler.py
│   └── main.py
├── TWstk_1mk/
│   └── ... (股票 1mk 文件)
├── .gitignore
├── README.md
└── requirements.txt
```
- src/：放置程式碼檔案。
- TWstk_1mk/：存放 1 分線資料的目錄。
- .gitignore：忽略不必要上傳的檔案。
- requirements.txt：使用套件清單，可透過 ``pip install -r requirements.txt`` 安裝。

## 安裝方式
1. Clone 此 project:
   ```bash
   git clone https://github.com/<yourname>/twstk_1mk_fetcher.git
   cd twstk_1mk_fetcher
   ```
2. 建立並啟用虛擬環境（建議使用 ``venv`` 或其他工具）：
   ```bash
   python -m venv env
   source env/bin/activate      # Mac/Linux
   # or
   env\Scripts\activate.bat     # Windows
   ```
4. 安裝所需套件：
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

## 使用說明
1. 請編輯 ``src/config.py`` 並填入 API_KEY 與 SECRET_KEY，以及想要抓取的日期區間等設定。
2. 執行程式：
   ```bash
   python src/main.py
   ```
3. 抓取完成之後，可以在 ``TWstk_1mk/`` 下找到對應的 Parquet 檔案。
