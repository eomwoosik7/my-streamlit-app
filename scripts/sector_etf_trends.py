import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

data_dir = r"C:\Users\ws\Desktop\Python\Project_Hermes5\data"
os.makedirs(data_dir, exist_ok=True)

# 섹터별 대표 ETF (한글 섹터명 → 영문 매칭용)
sector_etfs = {
    'Information Technology': {'US': 'XLK', 'KR': '381170.KS', 'name': '정보기술'},
    'Consumer Discretionary': {'US': 'XLY', 'KR': '228810.KS', 'name': '임의소비재'},
    'Communication Services': {'US': 'XLC', 'KR': '228810.KS', 'name': '통신서비스'},
    'Health Care': {'US': 'XLV', 'KR': '143860.KS', 'name': '헬스케어'},
    'Consumer Staples': {'US': 'XLP', 'KR': '139270.KS', 'name': '필수소비재'},
    'Financials': {'US': 'XLF', 'KR': '157500.KS', 'name': '금융'},
    'Energy': {'US': 'XLE', 'KR': '305080.KS', 'name': '에너지'},
    'Industrials': {'US': 'XLI', 'KR': '117700.KS', 'name': '산업재'},
    'Materials': {'US': 'XLB', 'KR': '117690.KS', 'name': '소재'},
    'Utilities': {'US': 'XLU', 'KR': '404650.KS', 'name': '유틸리티'},
    'Real Estate': {'US': 'XLRE', 'KR': '329200.KS', 'name': '부동산'}
}

def get_etf_data(ticker, market='US'):
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=35)
        
        etf = yf.Ticker(ticker)
        hist = etf.history(start=start_date, end=end_date)
        
        if hist.empty or len(hist) < 2:
            print(f"{ticker} 데이터 없음")
            return None
        
        latest_close = hist['Close'].iloc[-1]
        month_ago_close = hist['Close'].iloc[0]
        change_rate = ((latest_close - month_ago_close) / month_ago_close) * 100
        
        trend = '상승' if change_rate > 0 else '하락' if change_rate < 0 else '보합'
        
        # ETF 이름 가져오기
        info = etf.info
        etf_name = info.get('shortName') or info.get('longName') or ticker
        
        # KR ETF 이름 간소화
        if market == 'KR':
            etf_name = etf_name.replace(' ETF', '').replace('ETF', '').strip()
            words = etf_name.split()
            if len(words) > 2:
                etf_name = ' '.join(words[:2])
        else:
            # US는 티커만 사용
            etf_name = ticker
        
        return {
            'sector': None,
            'market': market,
            'change_rate': round(change_rate, 2),
            'trend': trend,
            'etf_name': etf_name,
            'trend_display': f"{trend}({change_rate:+.2f}%) {etf_name}"
        }
    except Exception as e:
        print(f"{ticker} 오류: {e}")
        return None

def main():
    print("섹터 ETF 트렌드 수집 시작...")
    results = []
    
    for sector, etfs in sector_etfs.items():
        kr_name = etfs['name']
        print(f"{kr_name} ({sector}) 처리 중...")
        
        # US ETF
        us_data = get_etf_data(etfs['US'], 'US')
        if us_data:
            us_data['sector'] = sector
            results.append(us_data)
            print(f"  US ({etfs['US']}): {us_data['trend_display']}")
        
        # KR ETF
        kr_data = get_etf_data(etfs['KR'], 'KR')
        if kr_data:
            kr_data['sector'] = sector
            results.append(kr_data)
            kr_ticker_display = etfs['KR'].replace('.KS', '')
            print(f"  KR ({kr_ticker_display}): {kr_data['trend_display']}")
    
    df = pd.DataFrame(results)
    
    if not df.empty:
        df = df[['sector', 'market', 'trend_display']]
        
        file_path = os.path.join(data_dir, 'sector_etf_trends.csv')
        df.to_csv(file_path, encoding='utf-8-sig', index=False)
        print(f"\n✅ CSV 저장: {file_path}")
        print(f"총 {len(df)}개 섹터 트렌드 저장")
        
        # 샘플 출력
        print("\n=== 저장된 데이터 샘플 ===")
        print(df.head(6).to_string(index=False))
        
        # 요약 통계
        us_rising = len(df[(df['market'] == 'US') & (df['trend_display'].str.contains('상승'))])
        kr_rising = len(df[(df['market'] == 'KR') & (df['trend_display'].str.contains('상승'))])
        print(f"\n=== 요약 ===")
        print(f"US 상승 섹터: {us_rising}/11")
        print(f"KR 상승 섹터: {kr_rising}/11")
    else:
        print("⚠️ 데이터 수집 실패")

if __name__ == '__main__':
    main()