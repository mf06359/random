import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime

# ---------------------------------------------------------
# 1. <a>タグからID（馬、騎手、調教師）を安全に抽出するヘルパー関数
# ---------------------------------------------------------
def get_id_from_href(td_element):
    a_tag = td_element.find('a')
    if a_tag and 'href' in a_tag.attrs:
        # 例: /db/horse/2023100583/ -> 2023100583 を取得
        return a_tag['href'].strip('/').split('/')[-1]
    return ""

# ---------------------------------------------------------
# 2. レース結果テーブルをパースする関数
# ---------------------------------------------------------
def scrape_race_results(race_id):
    """
    指定されたレースIDのraceresult.htmlにアクセスし、
    レース結果（着順、タイム、上がり等）を抽出してDataFrameを返す。
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
    
    # レース結果のテーブルを取得
    table = soup.find('table', class_='resulttable')
    if not table:
        return None

    results_data = []
    
    # tbody内の各行(tr)をループ
    tbody = table.find('tbody')
    if not tbody:
        tbody = table
        
    for tr in tbody.find_all('tr', recursive=False):
        tds = tr.find_all('td', recursive=False)
        
        # 最低でも馬体重(15列目=index 14)まである行のみ処理
        if len(tds) >= 15:
            row_data = {
                'レースID': race_id,
                '着順': tds[0].text.strip(),
                '枠番': tds[1].text.strip(),
                '馬番': tds[2].text.strip(),
                '馬名': tds[3].text.strip(),
                '馬ID': get_id_from_href(tds[3]),
                '性齢': tds[4].text.strip(),
                '斤量': tds[5].text.strip(),
                '騎手': tds[6].text.strip(),
                '騎手ID': get_id_from_href(tds[6]),
                '人気': tds[7].text.strip(),
                '単勝オッズ': tds[8].text.strip(),
                'タイム': tds[9].text.strip(),
                '着差': tds[10].text.strip(),
                '通過順': tds[11].text.strip(),
                '上がり3F': tds[12].text.strip(),
                '調教師': tds[13].text.strip(),
                '調教師ID': get_id_from_href(tds[13]),
                '馬体重': tds[14].text.strip()
            }
            results_data.append(row_data)

    if results_data:
        return pd.DataFrame(results_data)
    else:
        return None

# ---------------------------------------------------------
# 3. 指定した日付の全レースを取得するロジック
# ---------------------------------------------------------
def scrape_results_for_date(date_str, all_data):
    # JRA競馬場コード (01:札幌 ～ 10:小倉)
    course_codes = [f"{i:02d}" for i in range(1, 11)]
    races_found_today = 0
    
    for course in course_codes:
        # まず「1レース目」が存在するかで、その競馬場での開催有無をチェック
        first_race_id = f"{date_str}{course}01"
        df_first = scrape_race_results(first_race_id)
        
        if df_first is not None and not df_first.empty:
            df_first.insert(1, '開催日', date_str)
            all_data.append(df_first)
            races_found_today += 1
            
            # 2レース目 〜 12レース目まで順番に取得
            for r in range(2, 13):
                race_id = f"{date_str}{course}{r:02d}"
                df_r = scrape_race_results(race_id)
                
                if df_r is not None and not df_r.empty:
                    df_r.insert(1, '開催日', date_str)
                    all_data.append(df_r)
                    races_found_today += 1
                
                time.sleep(1.0) # サーバー負荷対策
                
        # 1レース目がない＝その競馬場での開催はないので次へ
        time.sleep(0.5)
                
    return races_found_today

# ---------------------------------------------------------
# 4. 年間実行メインロジック
# ---------------------------------------------------------
def scrape_entire_year_results(year):
    print(f"[{year}年] レース成績データ全日スクレイピングを開始します...")
    all_data = []
    
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 1, 7)
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        print(f"検索中: {date_str} ...", end="\r")
        
        races_found = scrape_results_for_date(date_str, all_data)
        
        if races_found > 0:
            print(f"  => {date_str}: 計 {races_found} レースの成績を取得しました。")
            
        current_date += datetime.timedelta(days=1)
        
        # 1ヶ月ごとに中間保存（クラッシュ対策）
        if current_date.day == 1 and all_data:
            temp_df = pd.concat(all_data, ignore_index=True)
            temp_df.to_csv(f"race_results_{year}_temp.csv", index=False, encoding='utf-8-sig')

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        filename = f"race_results_{year}.csv"
        
        # AIで扱いやすいように一部列を数値化
        numeric_cols = ['枠番', '馬番', '斤量', '人気', '単勝オッズ', '上がり3F']
        for col in numeric_cols:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
            
        final_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n完了: {filename} に全 {len(final_df)} 行の成績データを保存しました。")
    else:
        print("\nデータが1件も取得できませんでした。")

if __name__ == "__main__":
    # 例: 2026年分を実行 (過去の年に変更して実行してください)
    TARGET_YEAR = 2026
    scrape_entire_year_results(TARGET_YEAR)
