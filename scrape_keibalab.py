import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime

# ---------------------------------------------------------
# 1. 前走詳細パース関数 (変更なし)
# ---------------------------------------------------------
def parse_zensou(td, zensou_idx):
    prefix = f"{zensou_idx}走前_"
    data = {}
    table = td.find('table', class_='zensouTable')
    if not table: return data
    
    rows = table.find_all('tr')
    if len(rows) > 0:
        daybaba = rows[0].find('ul', class_='daybaba')
        if daybaba:
            lis = daybaba.find_all('li')
            if len(lis) >= 3:
                data[prefix + '開催'] = lis[0].text.strip()
                data[prefix + '日付'] = lis[1].text.strip()
                data[prefix + 'コース条件'] = lis[2].text.strip()
        cyaku = rows[0].find('p', class_='cyakuJun')
        if cyaku: data[prefix + '着順'] = cyaku.text.strip()
            
    if len(rows) > 1:
        zensoname = rows[1].find('div', class_='zensoname')
        if zensoname: data[prefix + 'レース名'] = zensoname.text.strip()
            
    if len(rows) > 2:
        spans = rows[2].find_all('span')
        if len(spans) >= 4:
            data[prefix + '人気'] = spans[0].text.strip()
            data[prefix + 'タイム'] = spans[1].text.strip()
            data[prefix + '上がり3F'] = spans[2].text.strip()
            data[prefix + 'ペース'] = spans[3].text.strip()
            
    if len(rows) > 4: data[prefix + '通過順'] = rows[4].text.strip()
    if len(rows) > 7: data[prefix + '勝馬差'] = rows[7].text.strip()

    return data

# ---------------------------------------------------------
# 2. レース単位のスクレイピング (変更なし)
# ---------------------------------------------------------
def scrape_race(race_id):
    url = f"https://www.keibalab.jp/db/race/{race_id}/umabashira.html"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        # 404エラー（ページが存在しない）場合はNoneを返す
        if res.status_code == 404:
            return None
        res.raise_for_status()
    except: 
        return None
        
    soup = BeautifulSoup(res.content, 'html.parser')
    table = soup.find('table', class_='megamoriTable')
    if not table: return None
        
    rows = (table.find('tbody') or table).find_all('tr', recursive=False)
    
    num_horses = 0
    for row in rows:
        th = row.find('th')
        if th and '馬番' in th.text:
            num_horses = len(row.find_all('td', recursive=False))
            break
    if num_horses == 0: return None
        
    horses_data = [{} for _ in range(num_horses)]
    
    for row in rows:
        th = row.find('th')
        if not th: continue
        header = th.text.strip().replace('\n', '')
        tds = row.find_all('td', recursive=False)
        
        if len(tds) == num_horses:
            for i, td in enumerate(tds):
                if '枠番' in header: horses_data[i]['枠番'] = td.text.strip()
                elif '馬番' in header: horses_data[i]['馬番'] = td.text.strip()
                elif '馬　名' in header:
                    a = td.find('a', class_='bamei')
                    if a:
                        horses_data[i]['馬名'] = a.text.strip()
                        horses_data[i]['馬ID'] = a.get('href', '').split('/')[-2]
                elif '性·齢' in header: horses_data[i]['性齢'] = td.text.strip()
                elif '単勝' in header:
                    sp = td.find_all('span')
                    if len(sp) >= 2:
                        horses_data[i]['単勝オッズ'] = sp[0].text.strip()
                        horses_data[i]['人気'] = sp[1].text.strip().strip('()')
                elif '斤量' in header: horses_data[i]['斤量'] = td.text.strip()

        cl = row.get('class', [])
        z_cl = [c for c in cl if c.startswith('zensou')]
        if z_cl:
            idx = int(z_cl[0].replace('zensou', ''))
            horse_tds = [td for td in tds if 'BeforRaces' not in td.get('class', [])]
            for i, td in enumerate(horse_tds[:num_horses]):
                horses_data[i].update(parse_zensou(td, idx))

    horses_data.reverse()
    return pd.DataFrame(horses_data)

# ---------------------------------------------------------
# 3. 指定した日付の全レースをID生成して取得するロジック
# ---------------------------------------------------------
def scrape_races_for_date(date_str, all_data):
    # JRA競馬場コード (01:札幌 ～ 10:小倉)
    course_codes = [f"{i:02d}" for i in range(1, 11)]
    races_found_today = 0
    
    for course in course_codes:
        # まず「1レース目」が存在するかで、その競馬場での開催有無をチェック
        first_race_id = f"{date_str}{course}01"
        df_first = scrape_race(first_race_id)
        
        if df_first is not None and not df_first.empty:
            print(f"    -> {course}競馬場の開催を確認。1〜12Rを取得します...")
            
            # 1レース目のデータを追加
            df_first.insert(0, 'レースID', first_race_id)
            df_first.insert(1, '開催日', date_str)
            all_data.append(df_first)
            races_found_today += 1
            time.sleep(1.5) # サーバー負荷対策
            
            # 2レース目 〜 12レース目まで順番に取得
            for r in range(2, 13):
                race_id = f"{date_str}{course}{r:02d}"
                df_r = scrape_race(race_id)
                
                if df_r is not None and not df_r.empty:
                    df_r.insert(0, 'レースID', race_id)
                    df_r.insert(1, '開催日', date_str)
                    all_data.append(df_r)
                    races_found_today += 1
                
                time.sleep(1.5) # サーバー負荷対策
                
    return races_found_today

# ---------------------------------------------------------
# 4. 年間実行メインロジック
# ---------------------------------------------------------
def scrape_entire_year(year):
    print(f"[{year}年] 全日スクレイピングを開始します（ID連結方式）...")
    all_data = []
    
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 1, 10)
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        print(f"検索中: {date_str} ...")
        
        races_found = scrape_races_for_date(date_str, all_data)
        
        if races_found > 0:
            print(f"  => {date_str} は計 {races_found} レースのデータを取得しました。")
        else:
            # 開催がない日はすぐに次へ（1レース目のチェック10回のみで済む）
            pass
            
        current_date += datetime.timedelta(days=1)
        
        # 1ヶ月ごとに中間保存（クラッシュ対策）
        if current_date.day == 1 and all_data:
            pd.concat(all_data).to_csv(f"keiba_{year}_temp.csv", index=False, encoding='utf-8-sig')

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        filename = f"keiba_data_{year}.csv"
        final_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n完了: {filename} に全 {len(final_df)} 行のデータを保存しました。")
    else:
        print("\nデータが1件も取得できませんでした。")

if __name__ == "__main__":
    # 例: 2026年分を実行
    scrape_entire_year(2026)

