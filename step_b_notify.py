import os
import datetime as dt
import pandas as pd
import requests
import json
import glob
from io import StringIO
import time

# (定数は変更なし)
PRICE_MIN, PRICE_MAX = 0.5, 5
FLOAT_MAX_M           = 10
TP_PCT, SL_PCT        = 0.10, 0.05
HOOK                  = os.getenv("DISCORD_HOOK")
API_KEY               = os.getenv("ALPHA_VANTAGE_API_KEY") # ★★★APIキーを読み込む★★★
OUTPUT_DIR            = "output"

print("仕事B：Discord通知と仮想取引を開始します...")
try:
    # (ファイル検索、絞り込み、Discord通知までは変更なし)
    # ...
    
    # --- シミュレーション部分を、Alpha Vantageを使うように全面改訂 ---
    if not df_watch.empty:
        # (Discord通知のコードはここに移動)
        
        rows = []
        result_columns = ["date", "ticker", "open", "high", "low", "pnl", "max_gain_pct", "max_loss_pct"]
        
        print("シミュレーションを開始します (Data source: Alpha Vantage)...")
        for t in df_watch['Ticker']:
            try:
                # --- ★★★ Alpha Vantage APIで1分足データを取得 ★★★ ---
                url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={t}&interval=1min&apikey={API_KEY}&outputsize=full'
                r = requests.get(url)
                data_json = r.json()

                # 'Time Series (1min)'が存在するかチェック
                if 'Time Series (1min)' not in data_json:
                    print(f"銘柄 {t} のデータ取得に失敗(Alpha Vantage)。エラー: {data_json.get('Note', 'Unknown error')}")
                    continue

                # JSONデータをpandas DataFrameに変換
                df_price = pd.DataFrame.from_dict(data_json['Time Series (1min)'], orient='index')
                df_price.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                df_price = df_price.astype(float)
                # 日付でフィルタリング（米国東部時間基準）
                today_et = (dt.datetime.utcnow() - dt.timedelta(hours=4)).date()
                df_price.index = pd.to_datetime(df_price.index)
                df_today = df_price[df_price.index.date == today_et].sort_index()

                if df_today.empty:
                    print(f"銘柄 {t} の当日データがありませんでした。")
                    continue
                # --- ★★★ ここまでがAlpha Vantageの処理 ★★★ ---

                o, h, l = df_today['Open'].iloc[0], df_today['High'].max(), df_today['Low'].min()
                
                # (以降の計算は変更なし)
                max_gain_pct = ((h - o) / o) * 100 if o > 0 else 0
                max_loss_pct = ((l - o) / o) * 100 if o > 0 else 0
                tp_hit = h >= o * (1 + TP_PCT)
                sl_hit = l <= o * (1 - SL_PCT)
                pnl = 10 if tp_hit else (-5 if sl_hit else 0)
                rows.append([dt.date.today().isoformat(),t,o,h,l,pnl,round(max_gain_pct, 2),round(max_loss_pct, 2)])

                # API制限を避けるため、15秒待機
                time.sleep(15)

            except Exception as e:
                print(f"銘柄 {t} の処理中にエラー: {e}")
                continue
        
        # (結果の保存部分は変更なし)
        # ...
