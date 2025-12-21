import yfinance as yf
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from pykrx import stock
from datetime import datetime, timedelta
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm  # 진행바 추가
import json
import time
import shutil
import numpy as np  # json 저장용 추가

DATA_DIR = os.getenv('DATA_DIR', './data')
os.makedirs(DATA_DIR, exist_ok=True)

# 추가: 시작 시 DB와 스크리너 결과 파일 삭제
META_DIR = os.path.join(DATA_DIR, 'meta')
DB_PATH = os.path.join(META_DIR, 'universe.db')
RESULTS_PATH = os.path.join(META_DIR, 'backtest.db')

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("universe.db 삭제 완료!")

if os.path.exists(RESULTS_PATH):
    os.remove(RESULTS_PATH)
    print("screener_results.parquet 삭제 완료!")

# 오늘 날짜를 평일로 조정 (주말이면 금요일로 이동)
today = datetime.now()
if today.weekday() >= 5:  # 5: 토요일, 6: 일요일
    days_back = today.weekday() - 4  # 토요일:1, 일요일:2만큼 이전 (금요일)
    today -= timedelta(days=days_back)

def get_kr_tickers():
    df_kr = None
    date_str = None
    temp_date = today  # 조정된 today 사용
    for attempt in range(10):
        date_str = temp_date.strftime('%Y%m%d')
        try:
            df_kr = stock.get_market_cap_by_ticker(date_str)
            if not df_kr.empty and len(df_kr) > 0:
                print(f"KR 데이터 로드 성공: {date_str} (상위 1000개)")
                break
            else:
                print(f"빈 데이터: {date_str} – 전날 시도")
        except Exception as e:
            print(f"{date_str} 에러: {e} – 전날 시도")
        temp_date -= timedelta(days=1)
    
    if df_kr is None or df_kr.empty:
        print("KR 데이터 수집 실패 – 기본 티커 리스트 사용")
        return [], pd.DataFrame(), None
    
    df_kr['시가총액'] = pd.to_numeric(df_kr['시가총액'].astype(str).str.replace(',', '').str.replace('-', '0'), errors='coerce').fillna(0)
    kr_tickers = df_kr.sort_values('시가총액', ascending=False).head(1000).index.tolist()
    print(f"KR 상위 1000: {len(kr_tickers)}개 로드 (날짜: {date_str})")
    return kr_tickers, df_kr, date_str

def get_us_symbols():
    url = 'https://en.wikipedia.org/wiki/Russell_1000_Index'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')
        # "Components" 테이블 찾기 (컬럼에 'Symbol' 포함)
        for table in tables:
            if 'Symbol' in str(table):  # 'Ticker symbol' → 'Symbol'로 되돌림
                df_us = pd.read_html(str(table))[0]
                us_symbols = df_us['Symbol'].str.replace('.', '-', regex=False).tolist()  # 컬럼 'Symbol'로 접근
                print(f"US 상위 {len(us_symbols)}개 로드 (Russell 1000)")
                return us_symbols, df_us  # df_us 반환 추가 (섹터 사용)
        print("US 테이블 찾기 실패 – 빈 리스트 반환")
        return [], pd.DataFrame()
    except Exception as e:
        print(f"US 심볼 로드 실패: {e}")
        return [], pd.DataFrame()

def fetch_us_single(symbol, start_date):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(start=start_date, end=today, interval="1d")  # end=today로 변경
        if data.empty:
            print(f"{symbol} 데이터 없음")
            return
        daily_dir = os.path.join(DATA_DIR, 'us_daily')
        os.makedirs(daily_dir, exist_ok=True)
        data.to_csv(os.path.join(daily_dir, f"{symbol}.csv"), encoding='utf-8-sig')
        print(f"{symbol} US 일봉 저장 완료")
    except Exception as e:
        print(f"{symbol} 오류: {e}")

def fetch_kr_single(ticker, start_date):
    try:
        start_ymd = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')
        end_ymd = today.strftime('%Y%m%d')  # end_ymd=today로 변경
        data = stock.get_market_ohlcv_by_date(start_ymd, end_ymd, ticker)
        if data.empty:
            return
        data = data.rename(columns={'시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'})
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
        daily_dir = os.path.join(DATA_DIR, 'kr_daily')
        os.makedirs(daily_dir, exist_ok=True)
        data.to_csv(os.path.join(daily_dir, f"{ticker}.csv"), encoding='utf-8-sig')
        print(f"{ticker} KR 일봉 저장 완료")
    except Exception as e:
        print(f"{ticker} 오류: {e}")

def get_kr_meta_single(ticker, df_kr, fundamental, ohlcv, kr_date_str):
    cap = 0.0
    name = "N/A"
    per = 0.0
    eps = 0.0
    close_price = 0.0
    sector = "N/A"
    cap_status = "N/A"
    try:
        new_cap = float(df_kr.loc[ticker, '시가총액']) if ticker in df_kr.index else 0.0
        if new_cap > 0:
            cap = new_cap
            cap_status = datetime.strptime(kr_date_str, '%Y%m%d').strftime('%Y-%m-%d')
        name = stock.get_market_ticker_name(ticker) or "N/A"
        per = round(fundamental.loc[ticker, 'PER'], 2) if ticker in fundamental.index and not pd.isna(fundamental.loc[ticker, 'PER']) else 0.0
        eps = round(fundamental.loc[ticker, 'EPS'], 2) if ticker in fundamental.index and not pd.isna(fundamental.loc[ticker, 'EPS']) else 0.0
        close_price = float(ohlcv.loc[ticker, '종가']) if ticker in ohlcv.index else 0.0
        # 섹터 추가
        kr_ticker = f"{ticker}.KS"
        sector = yf.Ticker(kr_ticker).info.get('sector', 'N/A')
    except:
        pass
    return ticker, cap, name, per, eps, close_price, sector, cap_status

def get_us_meta_single(symbol, df_us):
    cap = 0.0
    name = "N/A"
    per = 0.0
    eps = 0.0
    close_price = 0.0  # 종가 추가
    sector = "N/A"  # 섹터 추가
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        name = info.get('longName') or info.get('shortName') or "N/A"
        per = round(info.get('trailingPE') or info.get('forwardPE') or 0.0, 2)
        eps = round(info.get('trailingEps') or info.get('forwardEps') or 0.0, 2)

        shares = info.get('sharesOutstanding')
        if shares and shares > 0:
            hist = ticker.history(start=today - timedelta(days=5), end=today)  # 최근 5일로 변경 (주말 피함)
            if not hist.empty:
                cap = shares * hist['Close'].iloc[-1]
                close_price = hist['Close'].iloc[-1]  # 최근 종가 저장
        
        # 섹터: Wikipedia df_us에서 가져오기 (Symbol '.'으로 복원)
        symbol_dot = symbol.replace('-', '.')
        if 'Symbol' in df_us.columns and 'GICS Sector' in df_us.columns:
            matching = df_us[df_us['Symbol'] == symbol_dot]
            if not matching.empty:
                sector = matching['GICS Sector'].iloc[0]
    except:
        pass
    return symbol, float(cap), name, per, eps, float(close_price), sector

if __name__ == '__main__':
    for folder in ['kr_daily', 'us_daily']:
        path = os.path.join(DATA_DIR, folder)
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"{folder} 폴더 완전 삭제 완료!")
        os.makedirs(path, exist_ok=True)
    
    meta_dir = os.path.join(DATA_DIR, 'meta')
    os.makedirs(meta_dir, exist_ok=True)
    meta_file = os.path.join(meta_dir, 'tickers_meta.json')
    
    if os.path.exists(meta_file):
        with open(meta_file, 'r', encoding='utf-8') as f:
            old_meta = json.load(f)
        print("기존 meta.json 로드 완료")
    else:
        old_meta = {'KR': {}, 'US': {}}
        print("기존 meta.json 없음 – 새로 생성")
    
    start_date = (today - timedelta(days=730)).strftime('%Y-%m-%d')  # start_date도 today 기반
    kr_tickers, df_kr, kr_date_str = get_kr_tickers()
    us_symbols, df_us = get_us_symbols()  # df_us 추가

    print("US 일봉 다운로드 시작")
    with Pool(4) as pool:
        pool.starmap(fetch_us_single, [(s, start_date) for s in us_symbols])

    print("KR 일봉 다운로드 시작")
    for i in range(0, len(kr_tickers), 100):
        batch = kr_tickers[i:i+100]
        with Pool(4) as pool:
            pool.starmap(fetch_kr_single, [(t, start_date) for t in batch])
        time.sleep(2)
    
    # KR 메타 업데이트 (배치 처리 추가)
    kr_meta = old_meta.get('KR', {})
    if kr_tickers and not df_kr.empty:
        try:
            fundamental = stock.get_market_fundamental_by_ticker(kr_date_str)
            ohlcv = stock.get_market_ohlcv_by_ticker(kr_date_str, market="ALL")
        except:
            fundamental = pd.DataFrame()
            ohlcv = pd.DataFrame()
            print("KR fundamental/OHLCV 로드 실패 – 기존 데이터 유지")
        
        print("KR 메타 수집 시작")
        batch_size = 200
        for i in tqdm(range(0, len(kr_tickers), batch_size)):
            batch_tickers = kr_tickers[i:i+batch_size]
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = executor.map(lambda t: get_kr_meta_single(t, df_kr, fundamental, ohlcv, kr_date_str), batch_tickers)
            for ticker, cap, name, per, eps, close_price, sector, cap_status in results:
                old_cap = kr_meta.get(ticker, {}).get('cap', 0.0)
                old_cap_status = kr_meta.get(ticker, {}).get('cap_status', "N/A")
                old_name = kr_meta.get(ticker, {}).get('name', "N/A")
                old_per = kr_meta.get(ticker, {}).get('per', 0.0)
                old_eps = kr_meta.get(ticker, {}).get('eps', 0.0)
                old_close = kr_meta.get(ticker, {}).get('close', 0.0)
                old_sector = kr_meta.get(ticker, {}).get('sector', "N/A")
                
                cap_val = cap if cap > 0 else old_cap
                cap_status_val = cap_status if cap > 0 else old_cap_status
                name_val = name if name != "N/A" else old_name
                per_val = per if per != 0.0 else old_per
                eps_val = eps if eps != 0.0 else old_eps
                close_val = close_price if close_price > 0 else old_close
                sector_val = sector if sector != "N/A" else old_sector
                
                kr_meta[ticker] = {
                    'name': name_val, 'cap': cap_val, 'cap_status': cap_status_val,
                    'per': per_val, 'eps': eps_val, 'close': close_val, 'sector': sector_val
                }
            time.sleep(30)  # yfinance rate limit 방지
    else:
        print("KR 티커 로드 실패 – 기존 KR meta 유지")

    # US 메타 업데이트 (기존)
    us_meta = old_meta.get('US', {})
    us_cap_date = today.strftime('%Y-%m-%d')
    if us_symbols:
        print("US 메타 수집 시작")
        batch_size = 200
        for i in tqdm(range(0, len(us_symbols), batch_size)):
            batch_symbols = us_symbols[i:i+batch_size]
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = executor.map(lambda s: get_us_meta_single(s, df_us), batch_symbols)
            for symbol, new_cap, name, per, eps, close_price, sector in results:
                old_cap = us_meta.get(symbol, {}).get('cap', 0.0)
                if new_cap > 0:
                    cap = new_cap
                    cap_status = us_cap_date
                else:
                    cap = old_cap
                    cap_status = us_meta.get(symbol, {}).get('cap_status', "N/A")
                if name == "N/A":
                    name = us_meta.get(symbol, {}).get('name', "N/A")
                if per == 0.0:
                    per = us_meta.get(symbol, {}).get('per', 0.0)
                if eps == 0.0:
                    eps = us_meta.get(symbol, {}).get('eps', 0.0)
                close_val = close_price if close_price > 0 else us_meta.get(symbol, {}).get('close', 0.0)
                old_sector = us_meta.get(symbol, {}).get('sector', "N/A")
                sector_val = sector if sector != "N/A" else old_sector
                us_meta[symbol] = {'name': name, 'cap': cap, 'cap_status': cap_status, 'per': per, 'eps': eps, 'close': close_val, 'sector': sector_val}
            time.sleep(30)
    else:
        print("US 심볼 로드 실패 – 기존 US meta 유지")

    # json 저장 전 numpy int → float 변환 (에러 해결)
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

    print("모든 작업 완료! daily 새로 생성, meta 업데이트됨")
    print(f"저장 위치: {meta_file}")