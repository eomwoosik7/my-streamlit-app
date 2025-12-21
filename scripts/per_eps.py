import datetime
from pykrx import stock
import pandas as pd
import os

# 오늘 날짜를 평일로 조정 (주말이면 금요일로 이동)
today = datetime.date.today()
if today.weekday() >= 5:  # 5: 토요일, 6: 일요일
    days_back = today.weekday() - 4  # 토요일:1, 일요일:2만큼 이전 (금요일)
    today -= datetime.timedelta(days=days_back)

dfs = []
collected_days = 0
days_back = 0

data_dir = r"C:\Users\ws\Desktop\Python\Project_Hermes5\data"
os.makedirs(data_dir, exist_ok=True)  # 폴더 생성

while collected_days < 1:
    day = today - datetime.timedelta(days=days_back)
    date_str = day.strftime("%Y%m%d")
    
    df_fund = stock.get_market_fundamental_by_ticker(date_str, "ALL")
    
    if not df_fund.empty:
        df_fund['날짜'] = date_str
        dfs.append(df_fund)
        collected_days += 1
    else:
        print(f"{date_str} 데이터 없음 - 스킵")
    
    days_back += 1
    if days_back > 30:
        break

if collected_days < 1:
    print("데이터 없음")
else:
    latest_date = dfs[0]['날짜'].iloc[0]
    df_cap = stock.get_market_cap(latest_date)
    top_tickers = df_cap.sort_values(by='시가총액', ascending=False).index[:1000]
    
    filtered_dfs = []
    for df in dfs:
        df_top = df[df.index.isin(top_tickers)]
        filtered_dfs.append(df_top)
    
    df_combined = pd.concat(filtered_dfs)
    
    df_combined = df_combined.reset_index()
    df_combined['티커'] = df_combined['티커'].astype(str)
    df_combined['종목명'] = df_combined['티커'].apply(lambda x: stock.get_market_ticker_name(x))
    
    columns = ['티커', '종목명'] + [col for col in df_combined.columns if col not in ['티커', '종목명']]
    df_combined = df_combined[columns]
    
    file_path = os.path.join(data_dir, 'per_eps_top_1000.csv')
    df_combined.to_csv(file_path, encoding='utf-8-sig', index=False)
    print(f"CSV 파일 저장 완료: {file_path}")