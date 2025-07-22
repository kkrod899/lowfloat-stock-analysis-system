import os, datetime as dt, pandas as pd, requests, json, glob, time, pytz
import pandas_market_calendars as mcal

# ===================================================================
# 0) 定数
# ===================================================================
TP_PCT, SL_PCT = 0.10, 0.05
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
OUTPUT_DIR = "output"

# ===================================================================
# 1) メイン処理
# ===================================================================
print("仕事B：シミュレーションを開始します...")
try:
    # --- 実行日の市場が開いているかチェック ---
    nyse = mcal.get_calendar('NYSE')
    et_timezone = pytz.timezone('America/New_York')
    today_et = dt.datetime.now(et_timezone).date()

    schedule = nyse.schedule(start_date=today_et, end_date=today_et)
    if schedule.empty:
        print(f"本日({today_et})は市場休場日のためシミュレーションはありません。")
        exit()
    
    # --- 処理対象のウォッチリストファイルを探す ---
    # 【修正】日付を計算する代わりに、globでファイルを直接検索する
    watchlist_files = glob.glob(os.path.join(OUTPUT_DIR, "watchlist_*.csv"))
    
    if not watchlist_files:
        print("処理対象のウォッチリストファイル (watchlist_*.csv) が見つかりません。")
        exit()
        
    # 最新のウォッチリストファイルを取得（複数ある場合を考慮）
    file_path = max(watchlist_files, key=os.path.getctime)

    print(f"'{os.path.basename(file_path)}' の銘柄を、本日({today_et})の市場で検証します。")
    df_watch = pd.read_csv(file_path)
    
    if not df_watch.empty:
        # --- 過去の結果を読み込む ---
        results_path = os.path.join(OUTPUT_DIR, "results.csv")
        if os.path.exists(results_path):
            df_old_results = pd.read_csv(results_path)
        else:
            df_old_results = pd.DataFrame()

        # --- シミュレーション実行 ---
        new_rows = []
        # 【修正】結果に保存するカラムを、新しいwatchlistに合わせて拡張
        # 元のwatchlistの情報も引き継ぐ
        result_columns = df_watch.columns.tolist() + ["sim_date", "open", "high", "low", "pnl", "max_gain_pct", "max_loss_pct"]
        
        for index, watch_row in df_watch.iterrows():
            ticker = watch_row['Ticker']
            try:
                # APIコール
                url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min&apikey={API_KEY}&outputsize=full'
                r = requests.get(url)
                r.raise_for_status() # HTTPエラーがあればここで例外を発生させる
                data_json = r.json()
                
                # データ取得失敗時のハンドリング
                if 'Time Series (1min)' not in data_json:
                    error_message = data_json.get('Note', data_json.get('Error Message', 'Unknown error'))
                    print(f"銘柄 {ticker} データ取得失敗: {error_message}")
                    # API制限に達した場合は15秒待機
                    if 'Note' in data_json:
                        time.sleep(15)
                    continue
                
                # 価格データをDataFrameに変換
                df_price = pd.DataFrame.from_dict(data_json['Time Series (1min)'], orient='index')
                df_price.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                df_price = df_price.astype(float)
                
                # タイムゾーンを正しく設定
                df_price.index = pd.to_datetime(df_price.index).tz_localize('America/New_York')
                
                # 今日のデータのみを抽出
                df_today = df_price[df_price.index.date == today_et].sort_index()

                if df_today.empty:
                    print(f"銘柄 {ticker} の当日データなし")
                    continue

                # シミュレーション計算
                o, h, l = df_today['Open'].iloc[0], df_today['High'].max(), df_today['Low'].min()
                max_gain_pct = ((h - o) / o) * 100 if o > 0 else 0
                max_loss_pct = ((l - o) / o) * 100 if o > 0 else 0
                tp_hit = h >= o * (1 + TP_PCT)
                sl_hit = l <= o * (1 - SL_PCT)
                pnl = 10 if tp_hit else (-5 if sl_hit else 0)
                
                # 保存する行データを作成
                new_row_data = watch_row.to_dict()
                new_row_data.update({
                    "sim_date": today_et.isoformat(),
                    "open": o, "high": h, "low": l, "pnl": pnl,
                    "max_gain_pct": round(max_gain_pct, 2),
                    "max_loss_pct": round(max_loss_pct, 2)
                })
                new_rows.append(new_row_data)

                print(f"銘柄 {ticker} 処理完了。API制限のため15秒待機...")
                time.sleep(15) # API制限を避けるための待機

            except requests.exceptions.RequestException as e:
                print(f"銘柄 {ticker} のAPIリクエスト中にエラー: {e}")
                continue
            except Exception as e:
                print(f"銘柄 {ticker} の処理中に予期せぬエラー: {e}")
                continue
        
        # --- 結果を保存 ---
        if new_rows:
            df_new_results = pd.DataFrame(new_rows)
            # 新しい結果と古い結果を結合
            df_all = pd.concat([df_old_results, df_new_results], ignore_index=True)
            # 不要なカラムを削除（もしあれば）
            df_all = df_all.loc[:, ~df_all.columns.str.contains('^Unnamed')]
            df_all.to_csv(results_path, index=False)
            print(f"results.csv に {len(new_rows)}件の新規データを保存しました。合計: {len(df_all)}件")

    else:
        print("監視対象銘柄なし。")

    print("仕事B 完了。")

except Exception as e:
    print(f"仕事Bでエラーが発生: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
