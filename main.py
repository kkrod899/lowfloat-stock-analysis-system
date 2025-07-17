import os
import datetime as dt
import pandas as pd
import requests
import yfinance as yf
import json
from io import StringIO, BytesIO

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# 0) 定数 -----------------------------
PRICE_MIN, PRICE_MAX = 0.5, 5
FLOAT_MAX_M           = 10
TP_PCT, SL_PCT        = 0.10, 0.05
DRIVE_FOLDER_ID       = "10MiYwSllMX-KhzmeoZitWMXf2_QEq6BG"
HOOK                  = os.getenv("DISCORD_HOOK")
SCOPES                = ['https://www.googleapis.com/auth/drive']

# 1) Google Drive 認証 & サービス準備 -----------------
sa_json_string = os.getenv("GDRIVE_SA_JSON")
if not sa_json_string:
    raise ValueError("Secret GDRIVE_SA_JSON not found.")
sa_dict = json.loads(sa_json_string)

credentials = Credentials.from_service_account_info(sa_dict, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

# --- Google Drive 操作関数 (新ライブラリ版) ---
def find_file_id(name, folder_id):
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    response = drive_service.files().list(q=query, spaces='drive', fields='files(id)').execute()
    files = response.get('files', [])
    return files[0]['id'] if files else None

def upload_csv(df, name, folder_id):
    csv_data = df.to_csv(index=False).encode('utf-8')
    media = MediaIoBaseUpload(BytesIO(csv_data), mimetype='text/csv', resumable=True)
    file_metadata = {'name': name, 'parents': [folder_id]}
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"Uploaded '{name}' to Google Drive.")

def download_csv(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return pd.read_csv(StringIO(fh.getvalue().decode('utf-8')))

def append_csv(df_to_append, name, folder_id):
    file_id = find_file_id(name, folder_id)
    if not file_id:
        upload_csv(df_to_append, name, folder_id)
    else:
        df_old = download_csv(file_id)
        df_new = pd.concat([df_old, df_to_append], ignore_index=True)
        drive_service.files().delete(fileId=file_id).execute()
        upload_csv(df_new, name, folder_id)
    print(f"Updated '{name}' in Google Drive.")


# 2) Step 判定 & 実行 -------------------------
UTC_NOW = dt.datetime.utcnow()
is_after_market_close = UTC_NOW.time() > dt.time(20, 5) # 16:05 ET

if is_after_market_close:
    print("Step-A: Scraping Finviz...")
    try:
        url = "https://finviz.com/screener.ashx?v=111&f=ta_perf_d100o&o=-change"
        html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
        df = pd.read_html(html)[-2]
        df.columns = df.iloc[0]
        df = df.drop(0)
        file_name = f"prev100_{UTC_NOW.date().isoformat()}.csv"
        upload_csv(df, file_name, DRIVE_FOLDER_ID)
    except Exception as e:
        print(f"Step-A failed: {e}")
else:
    print("Step-B: Day-2 simulation...")
    try:
        yesterday = (UTC_NOW.date() - dt.timedelta(days=1)).isoformat()
        file_name = f"prev100_{yesterday}.csv"
        file_id = find_file_id(file_name, DRIVE_FOLDER_ID)
        if not file_id:
            raise FileNotFoundError(f"'{file_name}' not found in Google Drive.")
        
        df = download_csv(file_id)
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df['Float'] = df['Float'].str.replace('M','', regex=False).astype(float)
        
        df_watch = df.query(f"{PRICE_MIN} <= Price <= {PRICE_MAX} and Float <= {FLOAT_MAX_M}").nlargest(10, 'Price')
        
        if not df_watch.empty:
            table = "\n".join(f"{t:<6}  ${p:<5.2f} Float:{f:.1f}M"
                              for t, p, f in zip(df_watch.Ticker, df_watch.Price, df_watch.Float))
            requests.post(HOOK, json={"username": "Day-2 Watch", "content": "```" + table + "```"})

            # ★★★ここからがデータリッチ化対応のブロック★★★
            rows = []
            result_columns = ["date", "ticker", "open", "high", "low", "pnl", "max_gain_pct", "max_loss_pct"]

            for t in df_watch.Ticker:
                try:
                    # yfinanceからデータを取得
                    data = yf.download(t, period="1d", interval="1m", progress=False)
                    if data.empty: continue

                    # OHLC（始値・高値・安値）を取得
                    o, h, l = data.iloc[0]['Open'], data['High'].max(), data['Low'].min()
                    
                    # 最大上昇率と最大下落率を計算
                    max_gain_pct = ((h - o) / o) * 100 if o > 0 else 0
                    max_loss_pct = ((l - o) / o) * 100 if o > 0 else 0

                    # P/Lを判定
                    tp_hit = h >= o * (1 + TP_PCT)
                    sl_hit = l <= o * (1 - SL_PCT)
                    pnl = 10 if tp_hit else (-5 if sl_hit else 0)
                    
                    # 記録するデータをリストに追加
                    rows.append([
                        dt.date.today().isoformat(), t, o, h, l, pnl, 
                        round(max_gain_pct, 2), round(max_loss_pct, 2)
                    ])
                except Exception as e:
                    print(f"Ticker {t} error: {e}")
            
            if rows:
                # 新しい列名を使ってDataFrameを作成し、CSVに追記
                df_results = pd.DataFrame(rows, columns=result_columns)
                append_csv(df_results, "results.csv", DRIVE_FOLDER_ID)
            # ★★★ここまで★★★
        else:
            print("No stocks matched the criteria.")

    except Exception as e:
        print(f"Step-B failed: {e}")
