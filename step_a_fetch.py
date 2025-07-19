# 必要なライブラリ
import os
import datetime as dt
import pandas as pd
import requests
from bs4 import BeautifulSoup

# 保存先フォルダの準備
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("仕事A：Finvizからデータを取得します...")
try:
    # FinvizのURL
    url = "https://finviz.com/screener.ashx?v=111&f=ta_perf_d100o&o=-change"
    
    # BeautifulSoupを使って、HTMLからテーブルを正確に抽出
    html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text
    soup = BeautifulSoup(html, 'html.parser')
    
    table = soup.find('table', {'class': 'screener_table'})
    headers = [header.text.strip() for header in table.find_all('th')]
    rows = []
    for row in table.find_all('tr')[1:]: # ヘッダー行を除いてループ
        cols = [col.text.strip() for col in row.find_all('td')]
        if len(cols) == len(headers):
            rows.append(cols)
    
    # 抽出したデータでDataFrameを作成
    df = pd.DataFrame(rows, columns=headers)

    # ファイルを作成して保存
    file_name = f"prev100_{dt.date.today().isoformat()}.csv"
    file_path = os.path.join(OUTPUT_DIR, file_name)
    df.to_csv(file_path, index=False)
    
    print(f"'{file_name}' をローカルに保存しました。")
    print("仕事A 完了。")

except Exception as e:
    print(f"仕事Aでエラーが発生しました: {e}")
    # エラーが発生した場合は、0以外のコードで終了し、GitHub Actionsに失敗を伝える
    exit(1)
