# twstock-data-pipeline

**twstock-data-pipeline** project 目前涵蓋以下功能：
1. 下載並建立 `trading_days.parquet`，內容包含台股交易日及市場開盤/收盤時間。
2. 下載並建立 `twstk_info.parquet`，內容包含台股中的 *普通股、TDR、ETF、ETN* （不含牛熊證等）之基本資訊（證券代號、名稱、產業別、上市日等）。
3. 透過 [Shioaji API](https://github.com/Sinotrade/Shioaji) 下載上述股票的 1 minute OHLCV & count，儲存在 `data/twstk_1mk/`。

---

## Project 結構
```
twstock-data-pipeline/
│
├─ .gitignore
├─ requirements.txt
├─ README.md
│
├─ src/
│   ├─ __init__.py
│   ├─ config.py
│   ├─ main.py
│   ├─ test.py
│   ├─ construct_trading_days.py
│   ├─ construct_twstk_info.py
│   ├─ construct_twstk_1mk.py
│   └─ ...
│
├─ data/
│   ├─ trading_days.parquet
│   ├─ twstk_info.parquet
│   └─ twstk_1mk/  (此資料夾內放各股 1 分鐘 K)
│
└─ logs/
    └─ twstk_1mk_fetching.log
```

### 主要程式功能簡介

- **`construct_trading_days.py`**  
  利用金融市場開休市資料，建立台股交易日清單，並輸出 `trading_days.parquet`。

- **`construct_twstk_info.py`**  
  透過公開資訊或證交所資料，下載所有上市櫃股票的基本訊息，輸出 `twstk_info.parquet`。

- **`construct_twstk_1mk.py`**  
  透過 Shioaji API，抓取特定股票在指定日期區間內的 1 分鐘 K 線，儲存至 `data/twstk_1mk/` 資料夾。

- **`main.py`**  
  串接並執行上述三個模組的主流程，可依需要選擇呼叫哪幾個功能。

- **`test.py`**  
  對各模組的核心函式進行基本測試，確保程式正常運行。

- **`config.py`**  
  儲存各種參數設定，例如 API 金鑰、資料輸出路徑、開始/結束日期、需要下載的股票範圍等。

---

## 安裝與使用說明

1. **下載或 Clone 專案**
   ```bash
   git clone https://github.com/yourname/twstock-data-pipeline.git
   cd twstock-data-pipeline
   ```
2. **安裝套件**
   - 建議先建立虛擬環境（使用 conda 或 venv 均可。
   - 接著安裝：
     ```bash
     pip install -r requirements.txt
     ```
3. **設定參數**
   打開 ``src/config.py``，填入所需的 API_KEY、SECRET_KEY、資料儲存路徑、下載股票範圍等等。
4. **執行主程式**
   ```
   python src/main.py
   ```
   - 預設會執行下載交易日、股票資訊與 1mk 資料的流程。
   - 下載過程中產生的 parquet 檔案和日誌檔會分別放在 ``data/`` 和 ``logs/`` 目錄下。
  
---

## 進階說明 / Roadmap
- 資料庫整合：未來可考慮將所有資料寫入 PostgreSQL 或其他資料庫。
- 容器化：使用 Docker 來打包執行環境，方便在不同機器上運行。
- 自動排程：配合 CI/CD（如 GitHub Actions），定時執行更新。

---

## 聯絡方式
- 專案作者：KTC196884 (陳冠廷 Jack)
- Email: adams60304@gmail.com

