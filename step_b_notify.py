import os
import datetime as dt
import pandas as pd
import requests
import json
import glob
import time
import pytz # ★★★ タイムゾーンライブラリをインポート ★★★

# ===================================================================
# 0) 定数
# ===================================================================
PRICE_MIN, PRICE_MAX = 0.1, 5
FLOAT_MAX_M           = 50
TP_PCT, SL_PCT        = 0.10, 0.05
HOOK                  = os.getenv("DISCORD_HOOK")
API_KEY               = os.getenv("ALPHA_VANTAGE_API_KEY")
OUTPUT_DIR            = "output"

print("仕事B：Discord通知と仮想取引を開始します...")
try:
    # --- 準備：最新のウォッチリストファイルを探す ---
    watchlist_files = glob.glob(os.path.join(OUTPUT_DIR, "prev100_*.csv"))
    if not watchlist_files:
        print("処理対象のウォッチリストファイルが見つかりません。")
        exit()
    
    latest_file = max(watchlist_files, key=os.path.getctime)
    print(f"最新のウォッチリスト '{os.path.basename(latest_file)}' を読み込みます。")
    df = pd.read_csv(latest_file)
    
    # --- 銘柄の絞り込み ---
    # Finvizの列名'Shs Float'を、コード内で使いやすい'Float'に統一
    if 'Shs Float' in df.columns:
        df.rename(columns={'Shs Float': 'Float'}, inplace=True)
        
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df.dropna(subset=['Price'], inplace=True)
    
    df_watch = df[df['Price'].between(PRICE_MIN, PRICE_MAX)].copy()
    has_float_column = 'Float' in df_watch.columns.tolist()
    
    if has_float_column:
        df_watch['Float'] = pd.to_numeric(df_watch['Float'].astype(str).str.replace('M','', regex=False), errors='coerce')
        df_watch.dropna(subset=['Float'], inplace=True)
        df_watch = df_watch[df_watch['Float'] <= FLOAT_MAX_M]
    
    df_watch = df_watch.nlargest(10, 'Price')
    
    # --- Discord通知 & シミュレーション ---
    if not df_watch.empty:
        print(f"{len(df_watch)}件の銘柄をDiscordに通知します...")
        table_rows = []
        for index, row in df_watch.iterrows():
            if has_float_column and pd.notna(row['Float']):
                table_rows.append(f"{row['Ticker']:<6}  ${row['Price']:<5.2f} Float:{row['Float']:.1f}M")
            else:
                table_rows.append(f"{row['Ticker']:<6}  ${row['Price']:<5.2f}")
        
        requests.post(HOOK, json={"username": "Day-2 Watch", "content": "```" + "\n".join(table_rows) + "```"})
        
        rows = []
        result_columns = ["date", "ticker", "open", "high", "low", "pnl", "max_gain_pct", "max_loss_pct"]
        
        print("シミュレーションを開始します (Data source: Alpha Vantage)...")
        for t in df_watch['Ticker']:
            try:
                # Alpha Vantage APIで1分足データを取得
                url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={t}&interval=1min&apikey={API_KEY}&outputsize=full'
                r = requests.get(url)
                data_json = r.json()

                if 'Time Series (1min)' not in data_json:
                    print(f"銘柄 {t} のデータ取得に失敗(Alpha Vantage)。エラー: {data_json.get('Note', 'Unknown error')}")
                    time.sleep(15)
                    continue

                df_price = pd.DataFrame.from_dict(data_json['Time Series (1min)'], orient='index')
                df_price.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                df_price = df_price.astype(float)
                
                # --- ★★★ここからがタイムゾーン修正の核心部分★★★ ---
                # タイムスタンプのインデックスをdatetime型に変換し、米国東部時間(ET)として解釈させる
                df_price.index = pd.to_datetime(df_price.index).tz_localize('America/New_York')

                # 現在の米国東部時間の日付を取得する
                et_timezone = pytz.timezone('America/New_York')
                today_et = dt.datetime.now(et_timezone).date()

                # 正しいタイムゾーンで日付を比較し、当日データのみを抽出
                df_today = df_price[df_price.index.date == today_et].sort_index()
                # --- ★★★ここまで★★★ ---

                if df_today.empty:
                    print(f"銘柄 {t} の当日データがありませんでした。")
                    continue

                o, h, l = df_today['Open'].iloc[0], df_today['High'].max(), df_today['Low'].min()
                
                max_gain_pct = ((h - o) / o) * 100 if o > 0 else 0
                max_loss_pct = ((l - o) / o) * 100 if o > 0 else 0
                tp_hit = h >= o * (1 + TP_PCT)
                sl_hit = l <= o * (1 - SL_PCT)
                pnl = 10 if tp_hit else (-5 if sl_hit else 0)
                
                rows.append([dt.date.today().isoformat(),t,o,h,l,pnl,round(max_gain_pct, 2),round(max_loss_pct, 2)])

                print(f"銘柄 {t} 処理完了。15秒待機します...")
                time.sleep(15)

            except Exception as e:
                print(f"銘柄 {t} の処理中にエラー: {e}")
                continue
        
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

    print("仕事B 完了。")

except Exception as e:
    print(f"仕事Bでエラーが発生しました: {e}")
    exit(1)
