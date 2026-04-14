import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime

# ---------------------------------------------------------
# 1. 払い戻しテーブルをパースする関数
# ---------------------------------------------------------
def scrape_haraimodoshi(race_id):
    """
    指定されたレースIDのraceresult.htmlにアクセスし、
    払い戻しデータをスクレイピングしてDataFrameを返す。
    """
    url = f"https://www.keibalab.jp/db/race/{race_id}/raceresult.html"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 404:
            return None
        res.raise_for_status()
    except Exception as e:
        return None

    soup = BeautifulSoup(res.content, 'html.parser')
    
    # 払い戻しのブロックを取得
    haraimodoshi_div = soup.find('div', class_='haraimodoshi')
    if not haraimodoshi_div:
        return None
        
    table = haraimodoshi_div.find('table')
    if not table:
        return None

    payouts_data = []
    
    # テーブルの各行(tr)をループ
    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        
        # 1行の中に左側(例:単勝)と右側(例:馬単)の2つのブロックが入るため、3つずつ処理
        for i in range(0, len(tds), 3):
            # 要素が3つ揃っているか確認 (券種, 組番, 払戻金)
            if i + 2 < len(tds):
                bet_type = tds[i].get_text(strip=True)
                
                # <br>で区切られている複数要素(複勝やワイドなど)を個別のリストに分割
                numbers = list(tds[i+1].stripped_strings)
                prices = list(tds[i+2].stripped_strings)
                
                # 取得した組番と払戻金のペアをデータリストに追加
                for num, price in zip(numbers, prices):
                    # 「円」や「カンマ」を除去して数値に変換しやすい状態にする
                    clean_price = price.replace(',', '').replace('円', '')
                    
                    payouts_data.append({
                        'レースID': race_id,
                        '券種': bet_type,
                        '組番': num,
                        '払戻金': clean_price
                    })

    # データがあればDataFrameに変換して返す
    if payouts_data:
        return pd.DataFrame(payouts_data)
    else:
        return None


# ---------------------------------------------------------
# 2. 指定した日付の全レースを取得するロジック
# ---------------------------------------------------------
def scrape_races_for_date(date_str, all_data):
    # JRA競馬場コード (01:札幌 ～ 10:小倉)
    course_codes = [f"{i:02d}" for i in range(1, 11)]
    races_found_today = 0
    
    for course in course_codes:
        # まず「1レース目」が存在するかで、その競馬場での開催有無をチェック
        first_race_id = f"{date_str}{course}01"
        df_first = scrape_haraimodoshi(first_race_id)
        
        if df_first is not None and not df_first.empty:
            df_first.insert(1, '開催日', date_str)
            all_data.append(df_first)
            races_found_today += 1
            
            # 2レース目 〜 12レース目まで順番に取得
            for r in range(2, 13):
                race_id = f"{date_str}{course}{r:02d}"
                df_r = scrape_haraimodoshi(race_id)
                
                if df_r is not None and not df_r.empty:
                    df_r.insert(1, '開催日', date_str)
                    all_data.append(df_r)
                    races_found_today += 1
                
                time.sleep(1.0) # サーバー負荷対策
                
        # 1レース目がない＝その競馬場での開催はないので、次の競馬場へ
        time.sleep(0.5)
                
    return races_found_today


# ---------------------------------------------------------
# 3. 年間実行メインロジック
# ---------------------------------------------------------
def scrape_entire_year_payouts(year):
    print(f"[{year}年] 払い戻しデータ全日スクレイピングを開始します...")
    all_data = []
    
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 1, 10)
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        print(f"検索中: {date_str} ...", end="\r")
        
        races_found = scrape_races_for_date(date_str, all_data)
        
        if races_found > 0:
            print(f"  => {date_str}: 計 {races_found} レースの払い戻しを取得しました。")
            
        current_date += datetime.timedelta(days=1)
        
        # 1ヶ月ごとに中間保存（クラッシュ対策）
        if current_date.day == 1 and all_data:
            temp_df = pd.concat(all_data, ignore_index=True)
            temp_df.to_csv(f"payout_data_{year}_temp.csv", index=False, encoding='utf-8-sig')

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        filename = f"payout_data_{year}.csv"
        # 払戻金列を数値型にしておく
        final_df['払戻金'] = pd.to_numeric(final_df['払戻金'], errors='coerce')
        
        final_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n完了: {filename} に全 {len(final_df)} 行の払い戻しデータを保存しました。")
    else:
        print("\nデータが1件も取得できませんでした。")

if __name__ == "__main__":
    # 例: 2026年分を実行 (過去の年に変更して実行してください)
    TARGET_YEAR = 2026
    scrape_entire_year_payouts(TARGET_YEAR)
