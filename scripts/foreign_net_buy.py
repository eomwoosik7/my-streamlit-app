import datetime
from pykrx import stock
import pandas as pd
import os

today = datetime.date.today()
dfs = []
collected_days = 0
days_back = 0

data_dir = r"C:\Users\ws\Desktop\Python\Project_Hermes5\data"
os.makedirs(data_dir, exist_ok=True)  # 폴더 생성

while collected_days < 3:
    day = today - datetime.timedelta(days=days_back)
    date_str = day.strftime("%Y%m%d")
    
    df_net = stock.get_market_net_purchases_of_equities(date_str, date_str, "ALL", "외국인")
    
    if not df_net.empty:
        df_net['날짜'] = date_str
        dfs.append(df_net)
        collected_days += 1
    else:
        print(f"{date_str} 데이터 없음 - 스킵")
    
    days_back += 1
    if days_back > 30:
        break

if collected_days < 3:
    print("충분한 데이터 없음")
else:
    latest_date = dfs[0]['날짜'].iloc[0]
    df_cap = stock.get_market_cap(latest_date)
    top_tickers = df_cap.sort_values(by='시가총액', ascending=False).index[:1000]
    
    filtered_dfs = []
    for df in dfs:
        df_top = df[df.index.isin(top_tickers)]
        filtered_dfs.append(df_top)
    
    df_combined = pd.concat(filtered_dfs)
    df_combined_sorted = df_combined.sort_values(by=['날짜', '순매수거래량'], ascending=[False, False])
    
    if not df_combined_sorted.empty:
        df_combined_sorted = df_combined_sorted.reset_index()
        df_combined_sorted['티커'] = df_combined_sorted['티커'].astype(str)
    else:
        print("필터링 후 데이터 없음")
    
    file_path = os.path.join(data_dir, 'foreign_net_buy_daily_top_1000.csv')
    df_combined_sorted.to_csv(file_path, encoding='utf-8-sig', index=False)
    print(f"CSV 파일 저장 완료: {file_path}")