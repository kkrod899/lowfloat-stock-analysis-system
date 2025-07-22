import os, datetime as dt, pandas as pd, requests, json
from bs4 import BeautifulSoup

# 定数
PRICE_MIN, PRICE_MAX = 0.1, 5
FLOAT_MAX_M           = 50
HOOK                  = os.getenv("DISCORD_HOOK")
OUTPUT_DIR            = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("仕事A：データ取得とDiscord通知を開始します...")
try:
    url = "https://finviz.com/screener.ashx?v=152&o=-change&c=0,1,2,6,8,9,65"
    html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
    soup = BeautifulSoup(html, 'html.parser'); table = soup.find('table', {'class': 'screener_table'})
    headers = [header.text.strip() for header in table.find_all('th')]
    rows = []
    for row in table.find_all('tr')[1:]:
        cols = [col.text.strip() for col in row.find_all('td')]
        if len(cols) == len(headers):
            rows.append(cols)
    df = pd.DataFrame(rows, columns=headers)
    
    df.rename(columns={'Shs Float': 'Float'}, inplace=True)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce'); df.dropna(subset=['Price'], inplace=True)
    df_watch = df[df['Price'].between(PRICE_MIN, PRICE_MAX)].copy()
    has_float_column = 'Float' in df_watch.columns.tolist()
    if has_float_column:
        df_watch['Float'] = pd.to_numeric(df_watch['Float'].astype(str).str.replace('M','', regex=False), errors='coerce')
        df_watch.dropna(subset=['Float'], inplace=True)
        df_watch = df_watch[df_watch['Float'] <= FLOAT_MAX_M]
    df_watch = df_watch.nlargest(10, 'Price')

    if not df_watch.empty:
        print(f"{len(df_watch)}件の銘柄をDiscordに通知します...")
        market_date = (dt.datetime.utcnow() - dt.timedelta(hours=4)).date()
        message_header = f"--- {market_date.strftime('%Y-%m-%d')} 市場後 / 翌営業日の監視銘柄 ---"
        table_rows = []
        for index, row in df_watch.iterrows():
            if has_float_column and pd.notna(row['Float']):
                table_rows.append(f"{row['Ticker']:<6}  ${row['Price']:<5.2f} Float:{row['Float']:.1f}M")
            else:
                table_rows.append(f"{row['Ticker']:<6}  ${row['Price']:<5.2f}")
        requests.post(HOOK, json={"username": "Day-2 Watch", "content": f"```{message_header}\n" + "\n".join(table_rows) + "```"})
        
        file_name = f"watchlist_{market_date.isoformat()}.csv"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        df_watch.to_csv(file_path, index=False)
        print(f"'{file_name}' を保存しました。")
    else:
        print("監視対象となる銘柄が見つかりませんでした。")
    print("仕事A 完了。")
except Exception as e:
    print(f"仕事Aでエラーが発生しました: {e}"); exit(1)
