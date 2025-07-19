import os
import datetime as dt
import pandas as pd
import requests
import yfinance as yf
import json
from io import StringIO
import glob
from bs4 import BeautifulSoup # 新しいライブラリ

# ===================================================================
# 0) 定数
# ===================================================================
PRICE_MIN, PRICE_MAX = 0.5, 5
HOOK                  = os.getenv("DISCORD_HOOK")
OUTPUT_DIR            = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
TP_PCT, SL_PCT        = 0.10, 0.05

# ===================================================================
# 1) Step 判定 & 実行
# ===================================================================
manual_step = os.getenv("MANUAL_STEP")
is_step_a = (manual_step.upper() == 'A') if manual_step else (dt.time(20, 0) <= dt.datetime.utcnow().time() < dt.time(21, 0))

if is_step_a:
    print("仕事A：Finvizからデータを取得します...")
    try:
        url = "https://finviz.com/screener.ashx?v=111&f=ta_perf_d100o&o=-change"
        html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
        soup = BeautifulSoup(html, 'html.parser')
        
        # HTMLから直接テーブルデータを抽出する
        table = soup.find('table', {'class': 'screener_table'})
        rows = []
        # ヘッダー行を抽出
        headers = [header.text.strip() for header in table.find_all('th')]
        
        for row in table.find_all('tr')[1:]: # ヘッダー行を除いてループ
            cols = [col.text.strip() for col in row.find_all('td')]
            if len(cols) == len(headers):
                rows.append(cols)
        
        df = pd.DataFrame(rows, columns=headers)

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

        # これ以降のコードは、ヘッダーが正しく設定されていれば動くはず
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df.dropna(subset=['Price'], inplace=True)
        df_watch = df[df['Price'].between(PRICE_MIN, PRICE_MAX)].nlargest(10, 'Price')
        
        if not df_watch.empty:
            print(f"{len(df_watch)}件の銘柄をDiscordに通知します...")
            table_rows = [f"{row['Ticker']:<6}  ${row['Price']:<5.2f}" for index, row in df_watch.iterrows()]
            requests.post(HOOK, json={"username": "Day-2 Watch", "content": "```" + "\n".join(table_rows) + "```"})
            
            # シミュレーション部分 (変更なし)
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
