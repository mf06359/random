import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from datetime import datetime, timedelta

# ==================== 設定項目 ====================
START_DATE = "20240105"  # 取得開始日 (YYYYMMDD)
END_DATE   = "20240108"  # 取得終了日 (YYYYMMDD)
# 競馬場コード (01:札幌, 02:函館, 03:福島, 04:新潟, 05:東京, 06:中山, 07:中京, 08:京都, 09:阪神, 10:小倉)
PLACE_CODES = ["06", "08"] 
OUTPUT_FILE = "keiba_data_complete.csv"
# =================================================

def get_race_data(race_id):
    """特定のレースIDから全着順データを取得する"""
    url = f"https://www.keibalab.jp/db/race/{race_id}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            return None
        
        # 競馬ラボはUTF-8
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.text, "html.parser")
        
        # --- レース名の取得 (Titleタグから正確に抽出) ---
        # 例: 「中山金杯【2010年1月5日中山11R】...」から「中山金杯」を抜き出す
        full_title = soup.title.get_text(strip=True) if soup.title else ""
        if "【" in full_title:
            race_name = full_title.split("【")[0].strip()
        else:
            # Titleが取れない場合の予備
            race_name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Unknown"

        # 結果テーブルのtbodyを取得
        table_body = soup.select_one(".DbTable.resulttable tbody")
        if not table_body:
            return None

        race_results = []
        rows = table_body.find_all("tr")
        
        for row in rows:
            cols = row.find_all("td")
            # 必要な列が揃っているか確認 (rowspanの列を除外)
            if len(cols) < 15:
                continue
            
            # 各列のデータ抽出
            data = {
                "race_id": race_id,
                "race_name": race_name,
                "rank": cols[0].get_text(strip=True),
                "waku": cols[1].get_text(strip=True),
                "umaban": cols[2].get_text(strip=True),
                "horse": cols[3].find("a").get_text(strip=True) if cols[3].find("a") else cols[3].get_text(strip=True),
                "age_sex": cols[4].get_text(strip=True),
                "weight": cols[5].get_text(strip=True),
                "jockey": cols[6].find("a").get_text(strip=True) if cols[6].find("a") else cols[6].get_text(strip=True),
                "pop": cols[7].get_text(strip=True),
                "odds": cols[8].get_text(strip=True),
                "time": cols[9].get_text(strip=True),
                "margin": cols[10].get_text(strip=True),
                "passage": cols[11].get_text(strip=True),
                "last_3f": cols[12].get_text(strip=True),
                "trainer": cols[13].find("a").get_text(strip=True) if cols[13].find("a") else cols[13].get_text(strip=True),
                "horse_weight": cols[14].get_text(strip=True),
            }
            race_results.append(data)
            
        return race_results

    except Exception as e:
        print(f"\n[Error] Race ID {race_id}: {e}")
        return None

def main():
    # 日付フォーマットの修正 (余計なdを削除)
    try:
        start_dt = datetime.strptime(START_DATE, "%Y%m%d")
        end_dt = datetime.strptime(END_DATE, "%Y%m%d")
    except ValueError as e:
        print(f"日付設定エラー: {e}")
        return

    current_dt = start_dt
    
    print(f"Script Start: {START_DATE} to {END_DATE}")
    print(f"Saving to: {OUTPUT_FILE}")
    print("-" * 40)

    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y%m%d")
        print(f"\nDate: {date_str}")
        
        for place in PLACE_CODES:
            found_at_least_one = False
            for race_num in range(1, 13):
                race_id = f"{date_str}{place}{race_num:02d}"
                
                # 進捗表示
                print(f"  Fetching: {race_id}...", end="\r")
                
                result = get_race_data(race_id)
                
                if result:
                    # CSVへ書き込み (毎回保存することでクラッシュ対策)
                    df = pd.DataFrame(result)
                    file_exists = os.path.isfile(OUTPUT_FILE)
                    df.to_csv(OUTPUT_FILE, mode='a', header=not file_exists, index=False, encoding="utf-8-sig")
                    
                    found_at_least_one = True
                    time.sleep(1.2)  # サーバー負荷対策
                else:
                    # 1Rもない場合はその会場は開催なしと判断
                    if race_num == 1:
                        break
                    # レース番号が途切れたら終了
                    continue
            
            if found_at_least_one:
                print(f"  Place {place}: Done.           ")

        current_dt += timedelta(days=1)

    print("\n" + "=" * 40)
    print("All tasks completed successfully.")

if __name__ == "__main__":
    START_DATES = [
        "20090101",
        "20100101",
        "20110101",
        "20120101",
        "20130101",
        "20140101",
        "20150101",
        "20160101",
        "20170101",
        "20180101",
        "20190101",
        "20200101",
        "20210101",
        "20220101",
        "20230101",
        "20240101",
        "20250101",
            ]
    END_DATES = [
        "20091231",
        "20101231",
        "20111231",
        "20121231",
        "20131231",
        "20141231",
        "20151231",
        "20161231",
        "20171231",
        "20181231",
        "20191231",
        "20201231",
        "20211231",
        "20221231",
        "20231231",
        "20241231",
        "20251231",
            ]
    main()
