import os
import datetime as dt
import pandas as pd
import requests
import yfinance as yf
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import json

# 0) 定数 -----------------------------
PRICE_MIN, PRICE_MAX = 0.5, 5
FLOAT_MAX_M           = 10
TP_PCT, SL_PCT        = 0.10, 0.05            # +10%, -5%
DRIVE_FOLDER_ID       = "Y10MiYwSllMX-KhzmeoZitWMXf2_QEq6BG"  # ← ★★★ここの書き換えが必要です！★★★
HOOK                  = os.getenv("DISCORD_HOOK")

# 1) Google Drive 認証 -----------------
# GitHub Secretsからサービスアカウント情報を読み込む
sa_json_string = os.getenv("GDRIVE_SA_JSON")
if not sa_json_string:
    raise ValueError("Secret GDRIVE_SA_JSON not found. Please set it in GitHub Secrets.")
sa_dict = json.loads(sa_json_string)

g_auth = GoogleAuth()
scope = ["https://www.googleapis.com/auth/drive"]
g_auth.auth_method = 'service'
g_auth.credentials = g_auth.get_credentials_from_service_account(
    service_account_dict=sa_dict,
    scopes=scope
)
drive = GoogleDrive(g_auth)

# --- Google Drive 操作関数 ---
def drive_find_file_in_folder(folder_id, title):
    q = f"'{folder_id}' in parents and title='{title}' and trashed=false"
    file_list = drive.ListFile({'q': q}).GetList()
    return file_list[0] if file_list else None

def drive_upload_csv(df, folder_id, file_name):
    file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
    file.SetContentString(df.to_csv(index=False))
    file.Upload()
    print(f"Uploaded '{file_name}' to Google Drive.")

def drive_append_csv(rows, folder_id, file_name="results.csv"):
    file = drive_find_file_in_folder(folder_id, file_name)
    df_to_append = pd.DataFrame(rows, columns=["date", "ticker", "open", "high", "low", "pnl"])

    if not file:
        drive_upload_csv(df_to_append, folder_id, file_name)
    else:
        # download_urlが使えないことがあるので、より確実な方法に変更
        content = file.GetContentString()
        from io import StringIO
        df_old = pd.read_csv(StringIO(content))
        df_new = pd.concat([df_old, df_to_append], ignore_index=True)
        file.SetContentString(df_new.to_csv(index=False))
        file.Upload()
        print(f"Appended {len(rows)} rows to '{file_name}'.")

# 2) Step 判定 -------------------------
UTC_NOW = dt.datetime.utcnow()
is_after_close = UTC_NOW.time() > dt.time(20, 5) # 20:05 UTC=16:05 ET
today_iso = UTC_NOW.date().isoformat()
yesterday_iso = (UTC_NOW.date() - dt.timedelta(days=1)).isoformat()
csv_prev_name = f"prev100_{today_iso}.csv" # 引け後に作るファイル名
csv_to_read_name = f"prev100_{yesterday_iso}.csv" # 翌朝読むファイル名

# 3-A) 引け後処理 (Step-A) ----------------------
if is_after_close:
    print("Step-A: Scraping Finviz and saving to Drive...")
    try:
        url = ("https://finviz.com/screener.ashx?v=111&f=ta_perf_d100o&o=-change") # Today+100%
        html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
        df = pd.read_html(html)[-2] # Finvizのテーブルは末尾から2番目が安定
        df.columns = df.iloc[0]
        df = df.drop(0)
        drive_upload_csv(df, DRIVE_FOLDER_ID, csv_prev_name)
    except Exception as e:
        print(f"Step-A failed: {e}")

# 3-B) 翌朝処理 (Step-B) ------------------------
else:
    print("Step-B: Day-2 simulation...")
    try:
        file = drive_find_file_in_folder(DRIVE_FOLDER_ID, csv_to_read_name)
        if not file:
            raise FileNotFoundError(f"{csv_to_read_name} not found in Google Drive.")
        
        content = file.GetContentString()
        from io import StringIO
        df = pd.read_csv(StringIO(content))
        
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df['Float'] = df['Float'].str.replace('M','').astype(float)

        df_filtered = (df.query(f"{PRICE_MIN} <= Price <= {PRICE_MAX}")
                         .query("`Float` <= @FLOAT_MAX_M")
                         .nlargest(10, 'Price'))
        tickers = df_filtered.Ticker.to_list()

        # Discord 表通知
        if not df_filtered.empty:
            table = "\n".join(f"{t:<6}  ${p:<5.2f} Float:{f:.1f}M"
                              for t, p, f in zip(df_filtered.Ticker, df_filtered.Price, df_filtered.Float))
            requests.post(HOOK, json={"username": "Day-2 Watch", "content": "```" + table + "```"})

        rows = []
        for t in tickers:
            try:
                data = yf.download(t, period="2d", interval="1m", progress=False)
                today_data = data[data.index.date == dt.date.today()]
                if today_data.empty: continue
                o = today_data.iloc[0]['Open']
                h = today_data['High'].max()
                l = today_data['Low'].min()
                tp_hit = h >= o * (1 + TP_PCT)
                sl_hit = l <= o * (1 - SL_PCT)
                # 1分足の時系列データがないので、暫定で利確優先ルール
                pnl = 10 if tp_hit else (-5 if sl_hit else 0)
                rows.append([dt.date.today().isoformat(), t, o, h, l, pnl])
            except Exception as e:
                print(f"Ticker {t} error: {e}")
        
        if rows:
            drive_append_csv(rows, DRIVE_FOLDER_ID)

    except Exception as e:
        print(f"Step-B failed: {e}")
