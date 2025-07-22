import os
import datetime as dt
import pandas as pd
import requests
import json
import glob
import time
import pytz
import pandas_market_calendars as mcal

# ===================================================================
# 0) 定数
# ===================================================================
TP_PCT, SL_PCT        = 0.10, 0.05
API_KEY               = os.getenv("ALPHA_VANTAGE_API_KEY")
OUTPUT_DIR            = "output"

print("仕事B：シミュレーションを開始します...")
try:
    # --- 準備：検証すべき「前営業日」のファイルを探す ---
    nyse = mcal.get_calendar('NYSE')
    # 「今日」の米国日付を取得
    et_timezone = pytz.timezone('America/New_York')
    today_et = dt.datetime.now(et_timezone).date()

    # 「今日」の直近の営業日（＝シミュレーション対象日）を取得
    schedule = nyse.schedule(start_date=today_et, end_date=today_et)
    if schedule.empty:
        print(f"本日({today_et})は市場休場日のため、シミュレーションはありません。"); exit()
    
    # 監視リストが作られた「前営業日」の日付を取得
    recent_trading_days = nyse.schedule(start_date=today_et - dt.timedelta(days=7), end_date=today_et)
    if len(recent_trading_days) < 2:
        print("前営業日のデータが見つかりません。"); exit()
    prev_trading_day = recent_trading_days.index[-2].date()

    file_name = f"watchlist_{prev_trading_day.isoformat()}.csv"
    file_path = os.path.join(OUTPUT_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"'{file_name}' が見つかりません。"); exit()
        
    print(f"'{os.path.basename(file_path)}' の銘柄を、本日({today_et})の市場で検証します。")
    df_watch = pd.read_csv(file_path)
    
    # --- シミュレーション ---
    if not df_watch.empty:
        rows = []
        result_columns = ["date", "ticker", "open", "high", "low", "pnl", "max_gain_pct", "max_loss_pct"]
        
        for t in df_watch['Ticker']:
            try:
                url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={t}&interval=1min&apikey={API_KEY}&outputsize=full'
                r = requests.get(url); data_json = r.json()
                if 'Time Series (1min)' not in data_json:
                    print(f"銘柄 {t} データ取得失敗: {data_json.get('Note', 'Unknown error')}"); time.sleep(15); continue
                
                df_price = pd.DataFrame.from_dict(data_json['Time Series (1min)'], orient='index')
                df_price.columns = ['Open', 'High', 'Low', 'Close', 'Volume']; df_price = df_price.astype(float)
                
                df_price.index = pd.to_datetime(df_price.index).tz_localize('America/New_York')
                df_today = df_price[df_price.index.date == today_et].sort_index()

                if df_today.empty:
                    print(f"銘柄 {t} の当日データなし"); continue

                o, h, l = df_today['Open'].iloc[0], df_today['High'].max(), df_today['Low'].min()
                max_gain_pct = ((h - o) / o) * 100 if o > 0 else 0; max_loss_pct = ((l - o) / o) * 100 if o > 0 else 0
                tp_hit = h >= o * (1 + TP_PCT); sl_hit = l <= o * (1 - SL_PCT)
                pnl = 10 if tp_hit else (-5 if sl_hit else 0)
                rows.append([today_et.isoformat(),t,o,h,l,pnl,round(max_gain_pct, 2),round(max_loss_pct, 2)])
                print(f"銘柄 {t} 処理完了。15秒待機..."); time.sleep(15)
            except Exception as e:
                print(f"銘柄 {t} の処理中にエラー: {e}"); continue
        
        if rows:
            df_results = pd.DataFrame(rows, columns=result_columns)
            results_path = os.path.join(OUTPUT_DIR, "results.csv")
            if os.path.exists(results_path):
                df_old = pd.read_csv(results_path); df_all = pd.concat([df_old, df_results], ignore_index=True)
            else: df_all = df_results
            df_all.to_csv(results_path, index=False)
            print(f"results.csv に {len(rows)}件保存。")
    else:
        print("監視対象銘柄なし。")

    print("仕事B 完了。")
except Exception as e:
    print(f"仕事Bでエラーが発生: {e}"); exit(1)
