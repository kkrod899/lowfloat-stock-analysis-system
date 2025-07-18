# 必要なライブラリをまとめてインポート
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

# ===================================================================
# 0) 定数（チューニングする時は、主にこの部分を書き換えます）
# ===================================================================
PRICE_MIN, PRICE_MAX = 0.5, 5   # 絞り込む株価の範囲（ドル）
FLOAT_MAX_M           = 10      # 絞り込む浮動株の上限（百万株）
TP_PCT, SL_PCT        = 0.10, 0.05  # 利確+10%, 損切り-5%
DRIVE_FOLDER_ID       = "10MiYwSllMX-KhzmeoZitWMXf2_QEq6BG" # あなたのGoogle DriveフォルダID
HOOK                  = os.getenv("DISCORD_HOOK")
SCOPES                = ['https://www.googleapis.com/auth/drive']

# ===================================================================
# 1) Google Drive 認証 & サービス準備
# ===================================================================
sa_json_string = os.getenv("GDRIVE_SA_JSON")
if not sa_json_string:
    raise ValueError("GitHubのSecretsにGDRIVE_SA_JSONが見つかりません。")
sa_dict = json.loads(sa_json_string)

credentials = Credentials.from_service_account_info(sa_dict, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

# --- Google Drive 操作のための便利関数 ---
def find_file_id(name, folder_id):
    """指定された名前のファイルをフォルダ内から探し、IDを返す"""
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    response = drive_service.files().list(q=query, spaces='drive', fields='files(id)').execute()
    files = response.get('files', [])
    return files[0]['id'] if files else None

def upload_csv(df, name, folder_id):
    """DataFrameをCSVとしてGoogle Driveにアップロードする"""
    csv_data = df.to_csv(index=False).encode('utf-8')
    media = MediaIoBaseUpload(BytesIO(csv_data), mimetype='text/csv', resumable=True)
    file_metadata = {'name': name, 'parents': [folder_id]}
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"'{name}' をGoogle Driveにアップロードしました。")

def download_csv(file_id):
    """ファイルIDを指定して、CSVをダウンロードしDataFrameとして返す"""
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return pd.read_csv(StringIO(fh.getvalue().decode('utf-8')))

def append_csv(df_to_append, name, folder_id):
    """既存のCSVに新しいデータを追記する"""
    file_id = find_file_id(name, folder_id)
    if not file_id:
        upload_csv(df_to_append, name, folder_id)
    else:
        df_old = download_csv(file_id)
        df_new = pd.concat([df_old, df_to_append], ignore_index=True)
        drive_service.files().delete(fileId=file_id).execute()
        upload_csv(df_new, name, folder_id)
    print(f"'{name}' を更新しました。")

# ===================================================================
# 2) Step 判定 & 実行
# ===================================================================
manual_step = os.getenv("MANUAL_STEP") # 手動実行の選択を取得

# デフォルトは時間ベースで判定
is_step_a = False
if not manual_step:
    # スケジュール実行の場合：時間で判断 (UTC 20:00-21:00 = 日本時間 朝5:00-6:00)
    UTC_NOW = dt.datetime.utcnow()
    is_step_a = dt.time(20, 0) <= UTC_NOW.time() < dt.time(21, 0)
else:
    # 手動実行の場合：入力で判断
    is_step_a = (manual_step.upper() == 'A')

if is_step_a:
    # === Step-A (引け後処理) の実行 ===
    print("仕事A：Finvizからデータを取得します...")
    try:
        url = "https://finviz.com/screener.ashx?v=111&f=ta_perf_d100o&o=-change"
        html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
        df = pd.read_html(html)[-2]
        df.columns = df.iloc[0]
        df = df.drop(0)
        file_name = f"prev100_{dt.date.today().isoformat()}.csv"
        upload_csv(df, file_name, DRIVE_FOLDER_ID)
    except Exception as e:
        print(f"仕事Aでエラーが発生しました: {e}")
else:
    # === Step-B (翌朝処理) の実行 ===
    print("仕事B：Discord通知と仮想取引を開始します...")
    try:
        # 昨日の日付のファイルを探す
        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        file_name = f"prev100_{yesterday}.csv"
        file_id = find_file_id(file_name, DRIVE_FOLDER_ID)
        
        if not file_id:
            raise FileNotFoundError(f"'{file_name}' がGoogle Driveに見つかりません。(仕事Aがまだ実行されていないか、日付がずれている可能性があります)")
        
        # 昨日のデータをダウンロードして、条件で絞り込む
        df = download_csv(file_id)
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df['Float'] = df['Float'].str.replace('M','', regex=False).astype(float)
        
        df_watch = df.query(f"{PRICE_MIN} <= Price <= {PRICE_MAX} and Float <= {FLOAT_MAX_M}").nlargest(10, 'Price')
        
        if not df_watch.empty:
            # Discordに監視リストを通知
            table = "\n".join(f"{t:<6}  ${p:<5.2f} Float:{f:.1f}M"
                              for t, p, f in zip(df_watch.Ticker, df_watch.Price, df_watch.Float))
            requests.post(HOOK, json={"username": "Day-2 Watch", "content": "```" + table + "```"})

            # 仮想取引と結果記録（データリッチ化対応）
            rows = []
            result_columns = ["date", "ticker", "open", "high", "low", "pnl", "max_gain_pct", "max_loss_pct"]

            for t in df_watch.Ticker:
                try:
                    data = yf.download(t, period="1d", interval="1m", progress=False)
                    if data.empty: continue
                    o, h, l = data.iloc[0]['Open'], data['High'].max(), data['Low'].min()
                    
                    max_gain_pct = ((h - o) / o) * 100 if o > 0 else 0
                    max_loss_pct = ((l - o) / o) * 100 if o > 0 else 0

                    tp_hit = h >= o * (1 + TP_PCT)
                    sl_hit = l <= o * (1 - SL_PCT)
                    pnl = 10 if tp_hit else (-5 if sl_hit else 0)
                    
                    rows.append([
                        dt.date.today().isoformat(), t, o, h, l, pnl, 
                        round(max_gain_pct, 2), round(max_loss_pct, 2)
                    ])
                except Exception as e:
                    print(f"銘柄 {t} の処理中にエラー: {e}")
            
            if rows:
                df_results = pd.DataFrame(rows, columns=result_columns)
                append_csv(df_results, "results.csv", DRIVE_FOLDER_ID)
        else:
            print("監視対象となる銘柄が見つかりませんでした。")

    except Exception as e:
        print(f"仕事Bでエラーが発生しました: {e}")
