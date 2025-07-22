import os
import datetime as dt
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import pandas_market_calendars as mcal
import re
import traceback

# ===================================================================
# 0) 定数
# ===================================================================
PRICE_MIN, PRICE_MAX = 0.1, 5
FLOAT_MAX_M           = 50
TOP_N_RESULTS         = 10
HOOK                  = os.getenv("DISCORD_HOOK")
OUTPUT_DIR            = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===================================================================
# 1) ヘルパー関数
# ===================================================================
def clean_numeric_value(value):
    if isinstance(value, str):
        value = value.strip()
        if value == '-': return None
        value = value.replace('%', '')
        multipliers = {'K': 1e3, 'M': 1e6, 'B': 1e9}
        for suffix, multiplier in multipliers.items():
            if value.endswith(suffix):
                try: return float(value[:-1]) * multiplier
                except ValueError: return None
        try: return float(value)
        except ValueError: return None
    return value

# ===================================================================
# 2) メイン処理
# ===================================================================
print("仕事A：データ取得とDiscord通知を開始します...")
try:
    # --- Finvizからデータを取得 ---
    # 【修正】カラムを明示的に指定する元のURL形式に戻す。Volume(13)も追加。
    url = "https://finviz.com/screener.ashx?v=152&o=-change&c=0,1,2,3,4,6,8,9,13,25,61,63,64,65,66"
    print(f"URLからデータを取得します: {url}")

    html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
    soup = BeautifulSoup(html, 'html.parser')
    
    table = soup.find('table', class_='screener_table')
    if not table:
        raise ValueError("Finvizのスクリーナーテーブルが見つかりませんでした。")

    # ヘッダーを取得
    headers = [th.get_text(strip=True) for th in table.find_all('th')]
    # 'Shs Float' を 'Float' にリネームするロジック
    if 'Float' not in headers and 'Shs Float' in headers:
        headers[headers.index('Shs Float')] = 'Float'

    # データ行を取得
    rows = []
    # aタグのテキストのみを取得するよう修正
    for tr in table.find_all('tr')[1:]:
        # 各セル内のaタグのテキストを取得。aタグがなければtdのテキストを取得。
        cols = [td.find('a').get_text(strip=True) if td.find('a') else td.get_text(strip=True) for td in tr.find_all('td')]
        if len(cols) == len(headers):
            rows.append(cols)

    if not rows:
        print("監視対象となる銘柄が見つかりませんでした。")
        exit(0)

    df = pd.DataFrame(rows, columns=headers)
    
    # --- データのクレンジングと型変換 ---
    numeric_cols = [
        'Market Cap', 'P/E', 'Fwd P/E', 'PEG', 'Float', 'Gap', 
        'Avg Volume', 'Rel Volume', 'Price', 'Change', 'Volume'
    ]
    for col in [c for c in numeric_cols if c in df.columns]:
        df[col] = df[col].apply(clean_numeric_value)
    
    # --- 銘柄の絞り込み ---
    required_cols = ['Price', 'Float']
    if not all(col in df.columns for col in required_cols):
        print("取得したカラム:", df.columns.tolist())
        raise KeyError(f"必須カラム {required_cols} のいずれかが見つかりません。")

    df.dropna(subset=required_cols, inplace=True)

    df_watch = df[
        (df['Price'].between(PRICE_MIN, PRICE_MAX)) &
        (df['Float'] <= FLOAT_MAX_M * 1e6)
    ].copy()
    
    # Changeの降順でソートし、上位N件を取得
    if 'Change' in df_watch.columns:
        df_watch = df_watch.sort_values(by='Change', ascending=False).head(TOP_N_RESULTS)
    else:
        df_watch = df_watch.head(TOP_N_RESULTS)

    # --- Discord通知 & ファイル保存 ---
    if not df_watch.empty:
        print(f"{len(df_watch)}件の銘柄をDiscordに通知します...")
        
        nyse = mcal.get_calendar('NYSE')
        today_utc = dt.date.today()
        schedule = nyse.schedule(start_date=today_utc - dt.timedelta(days=7), end_date=today_utc)
        market_date = schedule.index[-1].date() if not schedule.empty else today_utc - dt.timedelta(days=1)
        
        message_header = f"--- {market_date.strftime('%Y-%m-%d')} 市場後 / 本日の監視銘柄 ---"
        
        table_rows = []
        # 'Rel Volume'をリネームマップに合わせて修正
        rename_map = {'Rel Volume': 'RelVolume'}
        df_watch.rename(columns=rename_map, inplace=True)
        
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
        print("絞り込み後の監視対象銘柄はありませんでした。")
        
    print("仕事A 完了。")
    
except Exception as e:
    print(f"仕事Aでエラーが発生しました: {e}")
    traceback.print_exc()
    exit(1)
