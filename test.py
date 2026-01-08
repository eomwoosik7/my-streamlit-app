import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time

# 데이터 저장 디렉토리
data_dir = r"C:\Users\ws\Desktop\Python\Project_Hermes5\data"
os.makedirs(data_dir, exist_ok=True)

# 섹터별 대표 ETF
sector_etfs = {
    'Information Technology': {'US': 'XLK', 'KR': '139260', 'kr_name': 'TIGER 200 IT'},
    'Consumer Discretionary': {'US': 'XLY', 'KR': '139290', 'kr_name': 'TIGER 200 경기소비재'},
    'Communication Services': {'US': 'XLC', 'KR': '228810', 'kr_name': 'TIGER 미디어컨텐츠'},
    'Health Care': {'US': 'XLV', 'KR': '143860', 'kr_name': 'TIGER 헬스케어'},
    'Consumer Staples': {'US': 'XLP', 'KR': '266410', 'kr_name': 'KODEX 필수소비재'},
    'Financials': {'US': 'XLF', 'KR': '139270', 'kr_name': 'TIGER 200 금융'},
    'Energy': {'US': 'XLE', 'KR': '117680', 'kr_name': 'KODEX 에너지화학'},
    'Industrials': {'US': 'XLI', 'KR': '117700', 'kr_name': 'KODEX 산업재'},
    'Materials': {'US': 'XLB', 'KR': '117690', 'kr_name': 'KODEX 소재산업'},
    'Utilities': {'US': 'XLU', 'KR': '404650', 'kr_name': 'TIGER KRX 기후변화솔루션'},
    'Real Estate': {'US': 'XLRE', 'KR': '329200', 'kr_name': 'TIGER 리츠부동산인프라'}
}

def get_kr_etf_trend(code, name):
    """KR ETF 1개월 수익률 크롤링 (네이버)"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # "1개월 수익률" 찾기
        text = soup.get_text()
        match = re.search(r'1개월\s*수익률\s*([+\-]?[\d,.]+)%', text)
        if match:
            rate = float(match.group(1).replace(',', ''))
            trend = '상승' if rate > 0 else '하락'
            return f"{trend}({rate:+.2f}%) {name}"
    except:
        pass
    return None

def get_us_etf_trend(ticker):
    """US ETF 1개월 수익률 계산 (yfinance)"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=35)
        
        etf = yf.Ticker(ticker)
        hist = etf.history(start=start_date, end=end_date)
        
        if hist.empty or len(hist) < 2:
            return None
        
        latest_close = hist['Close'].iloc[-1]
        month_ago_close = hist['Close'].iloc[0]
        change_rate = ((latest_close - month_ago_close) / month_ago_close) * 100
        
        trend = '상승' if change_rate > 0 else '하락'
        return f"{trend}({change_rate:+.2f}%) {ticker}"
    except Exception as e:
        print(f"      US {ticker} 에러: {type(e).__name__} - {str(e)[:50]}")
        return None

# 메인 실행
print("\n📈 섹터 ETF 트렌드 수집 중...")
sector_trends = []
for sector, etfs in sector_etfs.items():
    print(f"  {sector} 수집 중...")
    
    # US (yfinance)
    us_trend = get_us_etf_trend(etfs['US'])
    if us_trend:
        sector_trends.append({
            'sector': sector,
            'market': 'US',
            'trend_display': us_trend
        })
        print(f"    US: {us_trend}")
    else:
        print(f"    US: ❌ 실패")
    
    # KR (네이버 크롤링)
    kr_trend = get_kr_etf_trend(etfs['KR'], etfs['kr_name'])
    if kr_trend:
        sector_trends.append({
            'sector': sector,
            'market': 'KR',
            'trend_display': kr_trend
        })
        print(f"    KR: {kr_trend}")
    else:
        print(f"    KR: ❌ 실패")
    
    time.sleep(0.3)  # 서버 부하 방지

df_sector_trends = pd.DataFrame(sector_trends)
sector_trend_path = os.path.join(data_dir, 'sector_etf_trends.csv')
df_sector_trends.to_csv(sector_trend_path, encoding='utf-8-sig', index=False)
print(f"\n✅ 섹터 트렌드: {sector_trend_path}")
print(f"   수집: {len(df_sector_trends)}개 (US: {len(df_sector_trends[df_sector_trends['market']=='US'])}개, KR: {len(df_sector_trends[df_sector_trends['market']=='KR'])}개)")

# 샘플 출력
print("\n[섹터 ETF 트렌드 샘플]")
print(df_sector_trends.head(6).to_string(index=False))