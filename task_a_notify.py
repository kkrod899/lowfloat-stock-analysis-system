import os
import datetime as dt
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import pandas_market_calendars as mcal
import re # ### 追加 ### 正規表現ライブラリをインポート

# ===================================================================
# 0) 定数
# ===================================================================
# --- スクリーニング条件 ---
PRICE_MIN, PRICE_MAX = 0.1, 5
FLOAT_MAX_M           = 50 # 単位は百万ドル (M)

# --- その他設定 ---
TOP_N_RESULTS         = 10 # 最終的に何銘柄に絞るか
HOOK                  = os.getenv("DISCORD_HOOK")
OUTPUT_DIR            = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ### 追加 ### データクレンジング用の関数
def clean_numeric_value(value):
    """'1.23M', '4.56B', '7.89%'のような文字列を数値に変換する"""
    if isinstance(value, str):
        value = value.strip()
        if value == '-': return None # ハイフンは欠損値として扱う
        
        # パーセント記号を削除
        value = value.replace('%', '')
        
        # K, M, B を乗数に変換
        multipliers = {'K': 1e3, 'M': 1e6, 'B': 1e9}
        for suffix, multiplier in multipliers.items():
            if value.endswith(suffix):
                return pd.to_numeric(value[:-1], errors='coerce') * multiplier
        
        return pd.to_numeric(value, errors='coerce')
    return value

print("仕事A：データ取得とDiscord通知を開始します...")
try:
    # --- Finvizからデータを取得 ---
    # ### 変更 ### ご指定のURLに更新
    url = "https://finviz.com/screener.ashx?v=152&o=-change&c=0,1,2,3,4,6,8,9,25,61,63,64,65,66"
    html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
    soup = BeautifulSoup(html, 'html.parser')
    
    table = soup.find('table', {'class': 'screener_table'})
    
    # ### 変更 ### ヘッダー名をFinvizの表示に合わせる
    # ヘッダーが複数行になっている場合を考慮して取得
    header_rows = table.find_all('tr', {'valign': 'middle'})
    headers = [header.text.strip() for header in header_rows[0].find_all('td')]

    rows = []
    for row in table.find_all('tr', {'valign': 'top'})[1:]: # データ行は 'valign="top"'
        cols = [col.text.strip() for col in row.find_all('td')]
        if len(cols) == len(headers):
            rows.append(cols)
    df = pd.DataFrame(rows, columns=headers)

    # --- データのクレンジングと型変換 ---
    # ### 変更/追加 ### 取得した全カラムを適切な型に変換
    
    # 変換対象のカラムを指定
    numeric_cols = [
        'Market Cap', 'P/E', 'Fwd P/E', 'PEG', 'Shs Float', 'Gap', 
        'Avg Volume', 'Rel Volume', 'Price', 'Change'
    ]
    
    # 存在するカラムのみを対象に処理
    for col in [c for c in numeric_cols if c in df.columns]:
        # 'Shs Float' は 'Float' にリネームして処理
        if col == 'Shs Float':
            df.rename(columns={'Shs Float': 'Float'}, inplace=True)
            df['Float'] = df['Float'].apply(clean_numeric_value)
        else:
            df[col] = df[col].apply(clean_numeric_value)

    # --- 銘柄の絞り込み ---
    # 欠損値処理
    df.dropna(subset=['Price', 'Float'], inplace=True)
    
    # 条件でフィルタリング
    df_watch = df[
        (df['Price'].between(PRICE_MIN, PRICE_MAX)) &
        (df['Float'] <= FLOAT_MAX_M * 1e6) # FloatはM(10^6)単位に変換済みのため
    ].copy()
    
    # Change(変化率)の降順でソートし、上位N件を取得
    df_watch = df_watch.sort_values(by='Change', ascending=False).head(TOP_N_RESULTS)

    # --- Discord通知 & ファイル保存 ---
    if not df_watch.empty:
        print(f"{len(df_watch)}件の銘柄をDiscordに通知します...")
        
        # --- 正しい市場日を取得するロジック (ここは変更なし) ---
        nyse = mcal.get_calendar('NYSE')
        today_utc = dt.date.today()
        schedule = nyse.schedule(start_date=today_utc - dt.timedelta(days=7), end_date=today_utc)
        
        if schedule.empty:
            print("警告: 直近の営業日が見つかりませんでした。前日の日付を使用します。")
            market_date = today_utc - dt.timedelta(days=1)
        else:
            market_date = schedule.index[-1].date()
        
        message_header = f"--- {market_date.strftime('%Y-%m-%d')} 市場後 / 本日の監視銘柄 ---"
        
        # ### 変更 ### Discord通知のメッセージ内容を更新
        table_rows = []
        # 表示したい情報を選択
        for _, row in df_watch.iterrows():
            change_str = f"{row['Change']:.2f}%" if pd.notna(row['Change']) else "N/A"
            float_str = f"{row['Float']/1e6:.1f}M" if pd.notna(row['Float']) else "N/A"
            rel_vol_str = f"{row['Rel Volume']:.2f}" if pd.notna(row['Rel Volume']) else "N/A"
            
            table_rows.append(
                f"{row['Ticker']:<7} ${row['Price']:<5.2f} Chg:{change_str:<8} Float:{float_str:<8} RVol:{rel_vol_str}"
            )
        
        requests.post(HOOK, json={"username": "Day-2 Watch", "content": f"```{message_header}\n" + "\n".join(table_rows) + "```"})
        
        # 保存するファイル名も、正確な「市場日」に合わせる
        file_name = f"watchlist_{market_date.isoformat()}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        
        # ### 変更 ### 全てのカラムをCSVに保存
        df_watch.to_csv(file_path, index=False)
        print(f"'{file_name}' に全 {len(df_watch.columns)} カラムを保存しました。")
    else:
        print("監視対象となる銘柄が見つかりませんでした。")
        
    print("仕事A 完了。")
    
except Exception as e:
    print(f"仕事Aでエラーが発生しました: {e}")
    # ### 追加 ### 詳細なエラー情報を表示
    import traceback
    traceback.print_exc()
    exit(1)
