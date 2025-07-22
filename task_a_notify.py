import os
import datetime as dt
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import pandas_market_calendars as mcal
import re
import traceback # エラー詳細表示のため

# ===================================================================
# 0) 定数
# ===================================================================
PRICE_MIN, PRICE_MAX = 0.1, 5
FLOAT_MAX_M           = 50
TOP_N_RESULTS         = 10
HOOK                  = os.getenv("DISCORD_HOOK")
OUTPUT_DIR            = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_numeric_value(value):
    if isinstance(value, str):
        value = value.strip()
        if value == '-': return None
        value = value.replace('%', '')
        multipliers = {'K': 1e3, 'M': 1e6, 'B': 1e9}
        for suffix, multiplier in multipliers.items():
            if value.endswith(suffix):
                return pd.to_numeric(value[:-1], errors='coerce') * multiplier
        return pd.to_numeric(value, errors='coerce')
    return value

print("仕事A：データ取得とDiscord通知を開始します...")
try:
    # --- Finvizからデータを取得 ---
    url = "https://finviz.com/screener.ashx?v=152&o=-change&c=0,1,2,3,4,6,8,9,25,61,63,64,65,66"
    html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
    soup = BeautifulSoup(html, 'html.parser')
    
    table = soup.find('table', {'class': 'screener_table'})
    header_rows = table.find_all('tr', {'valign': 'middle'})
    headers = [header.text.strip() for header in header_rows[0].find_all('td')]

    rows = []
    # 【修正】データ行のセレクタをより堅牢に
    data_rows = table.find_all('tr', class_=re.compile(r"table-(dark|light)-row-"))
    for row in data_rows:
        cols = [col.text.strip() for col in row.find_all('td')]
        if len(cols) == len(headers):
            rows.append(cols)
    df = pd.DataFrame(rows, columns=headers)

    # --- データのクレンジングと型変換 ---
    # ### ここからが重要な修正箇所 ###
    
    # 1. まず、カラム名を正規化・統一する
    #    Finviz側の微妙な名前変更（例: 'P/E ' -> 'P/E'）に対応
    df.columns = [col.strip() for col in df.columns]
    
    #    分析しやすいようにカラム名をリネーム
    rename_map = {
        'Shs Float': 'Float',
        'Market Cap': 'MarketCap',
        'Fwd P/E': 'FwdPE',
        'Rel Volume': 'RelVolume',
        'Avg Volume': 'AvgVolume'
    }
    # 存在するカラムだけリネーム
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
    
    # 2. 次に、数値カラムをクレンジングする
    #    リネーム後の名前で指定
    numeric_cols = [
        'MarketCap', 'P/E', 'FwdPE', 'PEG', 'Float', 'Gap', 
        'AvgVolume', 'RelVolume', 'Price', 'Change'
    ]
    
    for col in [c for c in numeric_cols if c in df.columns]:
        df[col] = df[col].apply(clean_numeric_value)
    
    # ### 修正ここまで ###

    # --- 銘柄の絞り込み ---
    # 【修正】必須カラムが存在するかチェックしてからdropnaを実行
    required_cols = ['Price', 'Float']
    if not all(col in df.columns for col in required_cols):
        raise KeyError(f"必須カラム {required_cols} のいずれかがDataFrameに存在しません。Finvizの仕様変更の可能性があります。")

    df.dropna(subset=required_cols, inplace=True)
    
    # 条件でフィルタリング
    df_watch = df[
        (df['Price'].between(PRICE_MIN, PRICE_MAX)) &
        (df['Float'] <= FLOAT_MAX_M * 1e6)
    ].copy()
    
    # Change(変化率)の降順でソートし、上位N件を取得
    if 'Change' in df_watch.columns:
        df_watch = df_watch.sort_values(by='Change', ascending=False).head(TOP_N_RESULTS)
    else:
        df_watch = df_watch.head(TOP_N_RESULTS)

    # --- Discord通知 & ファイル保存 ---
    if not df_watch.empty:
        print(f"{len(df_watch)}件の銘柄をDiscordに通知します...")
        
        # (市場日の取得ロジックは変更なし)
        nyse = mcal.get_calendar('NYSE')
        today_utc = dt.date.today()
        schedule = nyse.schedule(start_date=today_utc - dt.timedelta(days=7), end_date=today_utc)
        market_date = schedule.index[-1].date() if not schedule.empty else today_utc - dt.timedelta(days=1)
        if schedule.empty:
            print("警告: 直近の営業日が見つかりませんでした。前日の日付を使用します。")
        
        message_header = f"--- {market_date.strftime('%Y-%m-%d')} 市場後 / 本日の監視銘柄 ---"
        
        # (Discordメッセージの作成部分は変更なし)
        table_rows = []
        for _, row in df_watch.iterrows():
            change_str = f"{row.get('Change', 0):.2f}%"
            float_str = f"{row.get('Float', 0)/1e6:.1f}M"
            rel_vol_str = f"{row.get('RelVolume', 0):.2f}"
            
            table_rows.append(
                f"{row['Ticker']:<7} ${row['Price']:<5.2f} Chg:{change_str:<8} Float:{float_str:<8} RVol:{rel_vol_str}"
            )
        
        requests.post(HOOK, json={"username": "Day-2 Watch", "content": f"```{message_header}\n" + "\n".join(table_rows) + "```"})
        
        file_name = f"watchlist_{market_date.isoformat()}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        df_watch.to_csv(file_path, index=False)
        print(f"'{file_name}' に全 {len(df_watch.columns)} カラムを保存しました。")
    else:
        print("監視対象となる銘柄が見つかりませんでした。")
        
    print("仕事A 完了。")
    
except Exception as e:
    print(f"仕事Aでエラーが発生しました: {e}")
    traceback.print_exc()
    exit(1)
