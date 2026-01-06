import yfinance as yf
from datetime import datetime, timedelta

def test_us_etf(ticker):
    """US ETF yfinance 테스트"""
    print("="*60)
    print(f"US ETF 테스트: {ticker}")
    print("="*60)
    
    try:
        print(f"티커: {ticker}")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=35)
        print(f"기간: {start_date.date()} ~ {end_date.date()}")
        
        print("\n데이터 다운로드 중...")
        etf = yf.Ticker(ticker)
        hist = etf.history(start=start_date, end=end_date)
        
        print(f"데이터 행 수: {len(hist)}")
        
        if hist.empty:
            print("❌ 데이터 없음 (empty)")
            return
        
        if len(hist) < 2:
            print(f"❌ 데이터 부족 (len={len(hist)})")
            return
        
        print("\n최근 5일 데이터:")
        print(hist.tail(5)[['Close']])
        
        latest_close = hist['Close'].iloc[-1]
        month_ago_close = hist['Close'].iloc[0]
        change_rate = ((latest_close - month_ago_close) / month_ago_close) * 100
        
        print(f"\n1개월 전: ${month_ago_close:.2f}")
        print(f"현재: ${latest_close:.2f}")
        print(f"수익률: {change_rate:+.2f}%")
        
        trend = '상승' if change_rate > 0 else '하락'
        result = f"{trend}({change_rate:+.2f}%) {ticker}"
        print(f"\n✅ 결과: {result}")
        
    except Exception as e:
        print(f"\n❌ 에러 발생: {type(e).__name__}")
        print(f"상세: {str(e)}")
        import traceback
        traceback.print_exc()

# 테스트
test_us_etf('XLK')
print("\n\n")
test_us_etf('XLE')
print("\n\n")
test_us_etf('XLF')