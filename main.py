import os
import datetime as dt
import pandas as pd
import requests
import yfinance as yf
from io import StringIO

# 0) 定数
PRICE_MIN, PRICE_MAX = 0.5, 5
FLOAT_MAX_M           = 10
TP_PCT, SL_PCT        = 0.10, 0.05
HOOK                  = os.getenv("DISCORD_HOOK")
# Artifactsを保存するためのフォルダ
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 2) Step 判定 & 実行
manual_step = os.getenv("MANUAL_STEP")

is_step_a = False
if not manual_step:
    UTC_NOW = dt.datetime.utcnow()
    is_step_a = dt.time(20, 0) <= UTC_NOW.time() < dt.time(21, 0)
else:
    is_step_a = (manual_step.upper() == 'A')

if is_step_a:
    print("仕事A：Finvizからデータを取得します...")
    try:
        url = "https://finviz.com/screener.ashx?v=111&f=ta_perf_d100o&o=-change"
        html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
        df = pd.read_html(StringIO(html))[-2] # StringIOで警告を抑制
        df.columns = df.iloc[0]
        df = df.drop(0)
        file_name = f"prev100_{dt.date.today().isoformat()}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        df.to_csv(file_path, index=False)
        print(f"'{file_name}' をローカルに保存しました。")
    except Exception as e:
        print(f"仕事Aでエラーが発生しました: {e}")
else:
    # このステップはプランBではまだ実装しません。まずはAを確実に動かします。
    print("仕事BはプランBでは後で実装します。")
