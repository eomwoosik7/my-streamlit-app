import yfinance as yf
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json
import time
import shutil
import numpy as np

DATA_DIR = os.getenv('DATA_DIR', './data')
os.makedirs(DATA_DIR, exist_ok=True)

META_DIR = os.path.join(DATA_DIR, 'meta')
DB_PATH = os.path.join(META_DIR, 'universe.db')
RESULTS_PATH = os.path.join(META_DIR, 'backtest.db')

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("universe.db 삭제 완료!")

if os.path.exists(RESULTS_PATH):
    os.remove(RESULTS_PATH)
    print("screener_results.parquet 삭제 완료!")

# ✅ 오늘 날짜를 평일로 조정
today = datetime.now()
if today.weekday() == 5:  # 토요일
    today -= timedelta(days=1)
    print(f"⚠️ 토요일 → 금요일로 조정: {today.strftime('%Y-%m-%d')}")
elif today.weekday() == 6:  # 일요일
    today -= timedelta(days=2)
    print(f"⚠️ 일요일 → 금요일로 조정: {today.strftime('%Y-%m-%d')}")

def get_kr_tickers():
    """FinanceDataReader로 KRX 상장 종목 조회"""
    try:
        print("📊 KRX 종목 리스트 조회 중...")
        
        df_krx = fdr.StockListing('KRX')
        
        if df_krx.empty:
            print("🚨 KRX 데이터 조회 실패")
            return [], pd.DataFrame(), None
        
        print(f"📋 실제 컬럼명: {df_krx.columns.tolist()}")
        print(f"📊 샘플 데이터:\n{df_krx.head(3)}")
        
        cap_col = None
        possible_names = ['MarketCap', 'Market Cap', 'Marcap', '시가총액', 'CapSize']
        
        for col_name in possible_names:
            if col_name in df_krx.columns:
                cap_col = col_name
                print(f"✅ 시가총액 컬럼 발견: {cap_col}")
                break
        
        if cap_col is None:
            print("⚠️ 시가총액 컬럼 없음 - 종목코드 순으로 상위 1000개")
            df_kr = df_krx.head(1000)
        else:
            df_krx[cap_col] = pd.to_numeric(df_krx[cap_col], errors='coerce').fillna(0)
            df_kr = df_krx.sort_values(cap_col, ascending=False).head(1000)
        
        kr_tickers = df_kr['Code'].tolist()
        date_str = today.strftime('%Y%m%d')
        
        print(f"✅ KR 상위 1000: {len(kr_tickers)}개 (날짜: {date_str})")
        print(f"샘플: {kr_tickers[:5]}")
        
        return kr_tickers, df_kr, date_str
        
    except Exception as e:
        print(f"❌ KRX 종목 조회 실패: {e}")
        import traceback
        traceback.print_exc()
        return [], pd.DataFrame(), None

def get_us_symbols():
    """US Russell 1000 종목 조회"""
    url = 'https://en.wikipedia.org/wiki/Russell_1000_Index'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')
        
        for table in tables:
            if 'Symbol' in str(table):
                df_us = pd.read_html(str(table))[0]
                us_symbols = df_us['Symbol'].str.replace('.', '-', regex=False).tolist()
                print(f"✅ US 상위 {len(us_symbols)}개 로드 (Russell 1000)")
                return us_symbols, df_us
        
        print("❌ US 테이블 찾기 실패")
        return [], pd.DataFrame()
    except Exception as e:
        print(f"❌ US 심볼 로드 실패: {e}")
        return [], pd.DataFrame()

def fetch_us_single(symbol, start_date):
    """US 일봉 다운로드"""
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(start=start_date, end=today, interval="1d")
        if data.empty:
            return
        daily_dir = os.path.join(DATA_DIR, 'us_daily')
        os.makedirs(daily_dir, exist_ok=True)
        data.to_csv(os.path.join(daily_dir, f"{symbol}.csv"), encoding='utf-8-sig')
    except Exception as e:
        print(f"❌ {symbol} 오류: {e}")

def fetch_kr_single(ticker, start_date):
    """FinanceDataReader로 KR 일봉 다운로드"""
    try:
        if not ticker or len(ticker) != 6 or not ticker.isdigit():
            print(f"⚠️ 잘못된 티커 형식: {ticker}")
            return False
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        data = fdr.DataReader(ticker, start=start_dt, end=today)
        
        if data.empty:
            print(f"⚠️ {ticker} 데이터 없음")
            return False
        
        data = data.rename(columns={
            '시가': 'Open', '고가': 'High', '저가': 'Low',
            '종가': 'Close', '거래량': 'Volume'
        })
        
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        available_cols = [col for col in required_cols if col in data.columns]
        
        if not available_cols:
            print(f"⚠️ {ticker} 필수 컬럼 없음")
            return False
            
        data = data[available_cols]
        
        daily_dir = os.path.join(DATA_DIR, 'kr_daily')
        os.makedirs(daily_dir, exist_ok=True)
        data.to_csv(os.path.join(daily_dir, f"{ticker}.csv"), encoding='utf-8-sig')
        
        return True
        
    except Exception as e:
        print(f"⚠️ {ticker} 다운로드 실패: {e}")
        return False

def get_kr_meta_single(ticker, df_kr):
    """KR 메타 정보 추출 (FinanceDataReader 기반)"""
    cap = 0.0
    name = "N/A"
    per = 0.0
    eps = 0.0
    close_price = 0.0
    
    try:
        if ticker in df_kr['Code'].values:
            row = df_kr[df_kr['Code'] == ticker].iloc[0]
            
            name = row.get('Name', 'N/A')
            
            cap_col = None
            for col_name in ['MarketCap', 'Market Cap', 'Marcap', '시가총액', 'CapSize']:
                if col_name in df_kr.columns:
                    cap_col = col_name
                    break
            
            if cap_col:
                cap = float(row.get(cap_col, 0))
            
            if 'Close' in df_kr.columns:
                close_price = float(row.get('Close', 0))
                
    except Exception as e:
        print(f"⚠️ {ticker} 메타 추출 실패: {e}")
    
    return ticker, cap, name, per, eps, close_price

def get_us_meta_single(symbol, df_us):
    """US 메타 정보 추출"""
    cap = 0.0
    name = "N/A"
    per = 0.0
    eps = 0.0
    close_price = 0.0
    sector = "N/A"
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        name = info.get('longName') or info.get('shortName') or "N/A"
        per = round(info.get('trailingPE') or info.get('forwardPE') or 0.0, 2)
        eps = round(info.get('trailingEps') or info.get('forwardEps') or 0.0, 2)

        shares = info.get('sharesOutstanding')
        if shares and shares > 0:
            hist = ticker.history(start=today - timedelta(days=5), end=today)
            if not hist.empty:
                cap = shares * hist['Close'].iloc[-1]
                close_price = hist['Close'].iloc[-1]
        
        symbol_dot = symbol.replace('-', '.')
        if 'Symbol' in df_us.columns and 'GICS Sector' in df_us.columns:
            matching = df_us[df_us['Symbol'] == symbol_dot]
            if not matching.empty:
                sector = matching['GICS Sector'].iloc[0]
    except:
        pass
    return symbol, float(cap), name, per, eps, float(close_price), sector

if __name__ == '__main__':
    print(f"🗓️ 작업 기준일: {today.strftime('%Y-%m-%d %A')}")
    
    # 기존 데이터 삭제
    for folder in ['kr_daily', 'us_daily']:
        path = os.path.join(DATA_DIR, folder)
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"🗑️ {folder} 폴더 삭제 완료")
        os.makedirs(path, exist_ok=True)
    
    # 메타 파일 로드
    meta_dir = os.path.join(DATA_DIR, 'meta')
    os.makedirs(meta_dir, exist_ok=True)
    meta_file = os.path.join(meta_dir, 'tickers_meta.json')
    
    if os.path.exists(meta_file):
        with open(meta_file, 'r', encoding='utf-8') as f:
            old_meta = json.load(f)
        print("📂 기존 meta.json 로드 완료")
    else:
        old_meta = {'KR': {}, 'US': {}}
        print("📝 기존 meta.json 없음 → 새로 생성")
    
    start_date = (today - timedelta(days=730)).strftime('%Y-%m-%d')
    today_str = today.strftime('%Y-%m-%d')

    # ✅ 시가총액 미수집 종목 기록용 리스트
    cap_failed_list = []
    
    # ====================================================
    # KR 데이터 수집
    # ====================================================
    print("\n" + "="*50)
    print("🇰🇷 KR 데이터 수집 시작")
    print("="*50)
    kr_tickers, df_kr, kr_date_str = get_kr_tickers()
    
    # ====================================================
    # US 데이터 수집
    # ====================================================
    print("\n" + "="*50)
    print("🇺🇸 US 데이터 수집 시작")
    print("="*50)
    us_symbols, df_us = get_us_symbols()

    # US 일봉 다운로드
    if us_symbols:
        print("\n📥 US 일봉 다운로드 시작")
        with Pool(4) as pool:
            pool.starmap(fetch_us_single, [(s, start_date) for s in us_symbols])

    # KR 일봉 다운로드
    if kr_tickers:
        print("\n📥 KR 일봉 다운로드 시작")
        print(f"총 {len(kr_tickers)}개 종목 처리 예정")
        
        success_count = 0
        fail_count = 0
        
        for i in range(0, len(kr_tickers), 100):
            batch = kr_tickers[i:i+100]
            batch_success = 0
            
            print(f"\n배치 {i//100 + 1}: {i}~{min(i+100, len(kr_tickers))} 처리 중...")
            
            for ticker in batch:
                if fetch_kr_single(ticker, start_date):
                    batch_success += 1
                    success_count += 1
                else:
                    fail_count += 1
                
                if (success_count + fail_count) % 10 == 0:
                    print(f"진행: {success_count + fail_count}/{len(kr_tickers)} (성공: {success_count}, 실패: {fail_count})")
            
            print(f"✅ 배치 완료: {batch_success}/{len(batch)} 성공")
            time.sleep(3)
        
        print(f"\n✅ KR 일봉 다운로드 완료: 성공 {success_count}개, 실패 {fail_count}개")
    
    # ====================================================
    # KR 메타 업데이트
    # ====================================================
    kr_meta = old_meta.get('KR', {})
    if kr_tickers and not df_kr.empty:
        print("\n📊 KR 메타 수집 시작")
        
        batch_size = 200
        for i in tqdm(range(0, len(kr_tickers), batch_size)):
            batch_tickers = kr_tickers[i:i+batch_size]
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = executor.map(
                    lambda t: get_kr_meta_single(t, df_kr),
                    batch_tickers
                )
            for ticker, cap, name, per, eps, close_price in results:
                old_data = kr_meta.get(ticker, {})

                # ✅ 시가총액 미수집 기록
                if cap == 0:
                    cap_failed_list.append({
                        'market': 'KR',
                        'symbol': ticker,
                        'name': name if name != "N/A" else old_data.get('name', "N/A"),
                        'date': today_str
                    })

                kr_meta[ticker] = {
                    'name':       name if name != "N/A" else old_data.get('name', "N/A"),
                    'cap':        cap if cap > 0 else old_data.get('cap', 0.0),
                    'cap_status': today_str,  # ✅ 항상 오늘 날짜
                    'per':        per,
                    'eps':        eps,
                    'close':      close_price if close_price > 0 else old_data.get('close', 0.0)
                }
            time.sleep(5)

    # ====================================================
    # US 메타 업데이트
    # ====================================================
    us_meta = old_meta.get('US', {})
    if us_symbols:
        print("\n📊 US 메타 수집 시작")
        batch_size = 200
        for i in tqdm(range(0, len(us_symbols), batch_size)):
            batch_symbols = us_symbols[i:i+batch_size]
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = executor.map(lambda s: get_us_meta_single(s, df_us), batch_symbols)
            for symbol, new_cap, name, per, eps, close_price, sector in results:
                old_data = us_meta.get(symbol, {})

                # ✅ 시가총액 미수집 기록
                if new_cap == 0:
                    cap_failed_list.append({
                        'market': 'US',
                        'symbol': symbol,
                        'name': name if name != "N/A" else old_data.get('name', "N/A"),
                        'date': today_str
                    })

                us_meta[symbol] = {
                    'name':       name if name != "N/A" else old_data.get('name', "N/A"),
                    'cap':        new_cap if new_cap > 0 else old_data.get('cap', 0.0),
                    'cap_status': today_str,  # ✅ 항상 오늘 날짜
                    'per':        per if per != 0.0 else old_data.get('per', 0.0),
                    'eps':        eps if eps != 0.0 else old_data.get('eps', 0.0),
                    'close':      close_price if close_price > 0 else old_data.get('close', 0.0),
                    'sector':     sector if sector != "N/A" else old_data.get('sector', "N/A")
                }
            time.sleep(30)

    # ====================================================
    # ✅ 시가총액 미수집 종목 CSV 저장
    # ====================================================
    if cap_failed_list:
        df_failed = pd.DataFrame(cap_failed_list)
        failed_path = os.path.join(DATA_DIR, 'cap_failed.csv')
        df_failed.to_csv(failed_path, index=False, encoding='utf-8-sig')
        print(f"\n⚠️ 시가총액 미수집 종목: {len(cap_failed_list)}개 → {failed_path}")
        print(f"   KR: {len(df_failed[df_failed['market']=='KR'])}개 | US: {len(df_failed[df_failed['market']=='US'])}개")
    else:
        print("\n✅ 시가총액 미수집 종목 없음")

    # ====================================================
    # JSON 저장
    # ====================================================
    def convert_np(obj):
        if isinstance(obj, (np.integer, np.floating)):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: convert_np(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [convert_np(i) for i in obj]
        return obj

    kr_meta = convert_np(kr_meta)
    us_meta = convert_np(us_meta)

    with open(meta_file, 'w', encoding='utf-8') as f:
        json.dump({'KR': kr_meta, 'US': us_meta}, f, ensure_ascii=False, indent=2)

    print("\n" + "="*50)
    print("✅ 모든 작업 완료!")
    print(f"📁 저장 위치: {meta_file}")
    print(f"📊 KR: {len(kr_meta)}개 | US: {len(us_meta)}개")
    print("="*50)
    print("\n⚠️ 중요 알림:")
    print("1. PER/EPS는 FinanceDataReader에서 제공하지 않습니다")
    print("2. 외국인 순매수 데이터도 수집 불가능합니다")
    print("3. 섹터 정보는 별도 처리가 필요합니다")