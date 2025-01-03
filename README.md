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
