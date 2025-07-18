import os
import datetime as dt
import pandas as pd
import requests
import yfinance as yf
import json # jsonはDiscord通知で使う可能性があります
from io import StringIO

# ===================================================================
# 0) 定数
# ===================================================================
PRICE_MIN, PRICE_MAX = 0.5, 5   # 絞り込む株価の範囲（ドル）
FLOAT_MAX_M           = 10      # 絞り込む浮動株の上限（百万株）
TP_PCT, SL_PCT        = 0.10, 0.05  # 利確+10%, 損切り-5%
HOOK                  = os.getenv("DISCORD_HOOK")
# Artifactsを保存するためのフォルダ
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
        html_content = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
        
        # StringIOを使って、pandasの警告を抑制しつつHTMLを読み込む
        all_tables = pd.read_html(StringIO(html_content))
        
        # 銘柄データが含まれるテーブルを特定（通常、末尾から2番目）
        df = all_tables[-2]
        
        # ★★★ここがヘッダー修正部分★★★
        # 1行目をヘッダーとして設定し、その行をデータから削除する
        df.columns = df.iloc[0]
        df = df.drop(0).reset_index(drop=True)
        # ★★★ここまで★★★
        
        file_name = f"prev100_{dt.date.today().isoformat()}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        
        # index=Falseを指定して、行番号がCSVに含まれないようにする
        df.to_csv(file_path, index=False)
        print(f"'{file_name}' をヘッダー付きでローカルに保存しました。")
        
    except Exception as e:
        print(f"仕事Aでエラーが発生しました: {e}")
else:
    # === Step-B (翌朝処理) の実行 ===
    # このステップはプランBの次の段階で実装します。
    # 今はまず仕事Aを完璧に動かすことに集中します。
    print("仕事Bは、次のステップで実装します。")
    print("（現在は仕事Aのテスト期間です）")
