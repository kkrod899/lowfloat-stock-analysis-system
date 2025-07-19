import os
import datetime as dt
import pandas as pd
import requests
import yfinance as yf
import json
from io import StringIO
import glob

# ===================================================================
# 0) 定数
# ===================================================================
PRICE_MIN, PRICE_MAX = 0.5, 5
FLOAT_MAX_M           = 10
TP_PCT, SL_PCT        = 0.10, 0.05
HOOK                  = os.getenv("DISCORD_HOOK")
OUTPUT_DIR            = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===================================================================
# 1) Step 判定 & 実行
# ===================================================================
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
        df = pd.read_html(StringIO(html))[-2]
        
        df.columns = df.iloc[0] # 1行目をヘッダーにする
        df = df.drop(0).reset_index(drop=True)
        
        file_name = f"prev100_{dt.date.today().isoformat()}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        df.to_csv(file_path, index=False)
        print(f"'{file_name}' をローカルに保存しました。")
    except Exception as e:
        print(f"仕事Aでエラーが発生しました: {e}")
else:
    print("仕事B：Discord通知と仮想取引を開始します...")
    try:
        watchlist_files = glob.glob(os.path.join(OUTPUT_DIR, "prev100_*.csv"))
        if not watchlist_files:
            print("処理対象のウォッチリストファイルが見つかりません。"); exit()
        
        latest_file = max(watchlist_files, key=os.path.getctime)
        print(f"最新のウォッチリスト '{os.path.basename(latest_file)}' を読み込みます。")
        df = pd.read_csv(latest_file)
        
        # --- ★★★ここからが最終FIXロジック★★★ ---
        # Price列のデータ型を数値に変換。変換できないものはNaNになり、後で削除される
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df.dropna(subset=['Price'], inplace=True) # Priceが数値でない行は削除

        # まずはPriceで絞り込む
        df_watch = df[df['Price'].between(PRICE_MIN, PRICE_MAX)].copy()

        # Float列が存在するかどうかを安全にチェック
        if 'Float' in df_watch.columns:
            print("'Float'列が見つかりました。Floatでさらに絞り込みます。")
            df_watch['Float'] = df_watch['Float'].astype(str).str.replace('M','', regex=False)
            df_watch['Float'] = pd.to_numeric(df_watch['Float'], errors='coerce')
            df_watch.dropna(subset=['Float'], inplace=True)
            # Floatの条件でさらに絞り込む
            df_watch = df_watch[df_watch['Float'] <= FLOAT_MAX_M]
        else:
            print("警告: 'Float'列が見つかりません。Priceでのみ絞り込みます。")
        
        # 最後に、株価の高い順に上位10件を取得
        df_watch = df_watch.nlargest(10, 'Price')
        # --- ★★★ここまで★★★ ---

        if not df_watch.empty:
            print(f"{len(df_watch)}件の銘柄をDiscordに通知します...")
            table_rows = []
            has_float_column = 'Float' in df_watch.columns
            for index, row in df_watch.iterrows():
                if has_float_column:
                    table_rows.append(f"{row['Ticker']:<6}  ${row['Price']:<5.2f} Float:{row['Float']:.1f}M")
                else:
                    table_rows.append(f"{row['Ticker']:<6}  ${row['Price']:<5.2f}")
            
            requests.post(HOOK, json={"username": "Day-2 Watch", "content": "```" + "\n".join(table_rows) + "```"})

            # (これ以降のシミュレーション部分は変更なし)
            rows = []
            result_columns = ["date", "ticker", "open", "high", "low", "pnl", "max_gain_pct", "max_loss_pct"]
            for t in df_watch['Ticker']:
                try:
                    data = yf.download(t, period="1d", interval="1m", progress=False)
                    if data.empty: continue
                    o, h, l = data.iloc[0]['Open'], data['High'].max(), data['Low'].min()
                    max_gain_pct = ((h - o) / o) * 100 if o > 0 else 0
                    max_loss_pct = ((l - o) / o) * 100 if o > 0 else 0
                    tp_hit = h >= o * (1 + TP_PCT)
                    sl_hit = l <= o * (1 - SL_PCT)
                    pnl = 10 if tp_hit else (-5 if sl_hit else 0)
                    rows.append([dt.date.today().isoformat(),t,o,h,l,pnl,round(max_gain_pct, 2),round(max_loss_pct, 2)])
                except Exception as e:
                    print(f"銘柄 {t} の処理中にエラー: {e}")
            if rows:
                df_results = pd.DataFrame(rows, columns=result_columns)
                results_path = os.path.join(OUTPUT_DIR, "results.csv")
                if os.path.exists(results_path):
                    df_old = pd.read_csv(results_path)
                    df_all = pd.concat([df_old, df_results], ignore_index=True)
                else:
                    df_all = df_results
                df_all.to_csv(results_path, index=False)
                print(f"results.csv に {len(rows)}件のデータを保存しました。")
        else:
            print("監視対象となる銘柄が見つかりませんでした。")
    except Exception as e:
        print(f"仕事Bでエラーが発生しました: {e}")
