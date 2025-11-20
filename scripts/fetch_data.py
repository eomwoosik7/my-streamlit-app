import yfinance as yf
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from pykrx import stock
from datetime import datetime, timedelta
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
import json
import time
import shutil
import numpy as np  # json 저장용 추가

DATA_DIR = os.getenv('DATA_DIR', './data')
os.makedirs(DATA_DIR, exist_ok=True)

def get_kr_tickers():
    today = datetime.now()
    df_kr = None
    date_str = None
    for attempt in range(10):
        date_str = today.strftime('%Y%m%d')
        try:
            df_kr = stock.get_market_cap_by_ticker(date_str)
            if not df_kr.empty and len(df_kr) > 0:
                print(f"KR 데이터 로드 성공: {date_str} (상위 1000개)")
                break
            else:
                print(f"빈 데이터: {date_str} – 전날 시도")
        except Exception as e:
            print(f"{date_str} 에러: {e} – 전날 시도")
        today -= timedelta(days=1)
    
    if df_kr is None or df_kr.empty:
        print("KR 데이터 수집 실패 – 기본 티커 리스트 사용")
        return [], pd.DataFrame(), None
    
    df_kr['시가총액'] = pd.to_numeric(df_kr['시가총액'].astype(str).str.replace(',', '').str.replace('-', '0'), errors='coerce').fillna(0)
    kr_tickers = df_kr.sort_values('시가총액', ascending=False).head(1000).index.tolist()
    print(f"KR 상위 1000: {len(kr_tickers)}개 로드 (날짜: {date_str})")
    return kr_tickers, df_kr, date_str

def get_us_symbols():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'constituents'})
        df_us = pd.read_html(str(table))[0]
        us_symbols = df_us['Symbol'].str.replace('.', '-', regex=False).tolist()
        print(f"US 상위 {len(us_symbols)}개 로드")
        return us_symbols
    except Exception as e:
        print(f"US 심볼 로드 실패: {e}")
        return []

def fetch_us_single(symbol, start_date):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(start=start_date, end=datetime.now(), interval="1d")
        if data.empty:
            print(f"{symbol} 데이터 없음")
            return
        daily_dir = os.path.join(DATA_DIR, 'us_daily')
        os.makedirs(daily_dir, exist_ok=True)
        data.to_parquet(os.path.join(daily_dir, f"{symbol}.parquet"))
        print(f"{symbol} US 일봉 저장 완료")
    except Exception as e:
        print(f"{symbol} 오류: {e}")

def fetch_kr_single(ticker, start_date):
    try:
        start_ymd = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')
        end_ymd = datetime.now().strftime('%Y%m%d')
        data = stock.get_market_ohlcv_by_date(start_ymd, end_ymd, ticker)
        if data.empty:
            return
        data = data.rename(columns={'시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'})
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
        daily_dir = os.path.join(DATA_DIR, 'kr_daily')
        os.makedirs(daily_dir, exist_ok=True)
        data.to_parquet(os.path.join(daily_dir, f"{ticker}.parquet"))
        print(f"{ticker} KR 일봉 저장 완료")
    except Exception as e:
        print(f"{ticker} 오류: {e}")

def get_us_meta_single(symbol):
    cap = 0.0
    name = "N/A"
    per = 0.0
    eps = 0.0
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        name = info.get('longName') or info.get('shortName') or "N/A"
        per = round(info.get('trailingPE') or info.get('forwardPE') or 0.0, 2)
        eps = round(info.get('trailingEps') or info.get('forwardEps') or 0.0, 2)

        shares = info.get('sharesOutstanding')
        if shares and shares > 0:
            hist = ticker.history(period="2d")
            if not hist.empty:
                cap = shares * hist['Close'].iloc[-1]
    except:
        pass
    return symbol, float(cap), name, per, eps

if __name__ == '__main__':
    for folder in ['kr_daily', 'us_daily']:
        path = os.path.join(DATA_DIR, folder)
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"{folder} 폴더 완전 삭제 완료!")
        os.makedirs(path, exist_ok=True)
    
    # meta 폴더는 삭제하지 않고 유지
    meta_dir = os.path.join(DATA_DIR, 'meta')
    os.makedirs(meta_dir, exist_ok=True)
    meta_file = os.path.join(meta_dir, 'tickers_meta.json')
    
    # 기존 meta.json 로드 (없으면 빈 dict)
    if os.path.exists(meta_file):
        with open(meta_file, 'r', encoding='utf-8') as f:
            old_meta = json.load(f)
        print("기존 meta.json 로드 완료")
    else:
        old_meta = {'KR': {}, 'US': {}}
        print("기존 meta.json 없음 – 새로 생성")
    
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    kr_tickers, df_kr, kr_date_str = get_kr_tickers()
    us_symbols = get_us_symbols()

    print("US 일봉 다운로드 시작")
    with Pool(4) as pool:
        pool.starmap(fetch_us_single, [(s, start_date) for s in us_symbols])

    print("KR 일봉 다운로드 시작")
    for i in range(0, len(kr_tickers), 100):
        batch = kr_tickers[i:i+100]
        with Pool(4) as pool:
            pool.starmap(fetch_kr_single, [(t, start_date) for t in batch])
        time.sleep(2)
    
    # KR 메타 업데이트
    kr_meta = old_meta.get('KR', {})
    if kr_tickers and not df_kr.empty:
        try:
            fundamental = stock.get_market_fundamental_by_ticker(kr_date_str)
        except:
            fundamental = pd.DataFrame()
            print("KR fundamental 로드 실패 – 기존 데이터 유지")
        for ticker in kr_tickers:
            try:
                cap = float(df_kr.loc[ticker, '시가총액']) if ticker in df_kr.index else kr_meta.get(ticker, {}).get('cap', 0.0)
                name = stock.get_market_ticker_name(ticker) or kr_meta.get(ticker, {}).get('name', "N/A")
                per = round(fundamental.loc[ticker, 'PER'], 2) if ticker in fundamental.index and not pd.isna(fundamental.loc[ticker, 'PER']) else kr_meta.get(ticker, {}).get('per', 0.0)
                eps = round(fundamental.loc[ticker, 'EPS'], 2) if ticker in fundamental.index and not pd.isna(fundamental.loc[ticker, 'EPS']) else kr_meta.get(ticker, {}).get('eps', 0.0)
                kr_meta[ticker] = {'name': name, 'cap': cap, 'per': per, 'eps': eps}
            except Exception as e:
                print(f"KR {ticker} 업데이트 실패: {e} – 기존 유지")
                if ticker in old_meta.get('KR', {}):
                    kr_meta[ticker] = old_meta['KR'][ticker]
    else:
        print("KR 티커 로드 실패 – 기존 KR meta 유지")

    # US 메타 업데이트
    us_meta = old_meta.get('US', {})
    if us_symbols:
        print("US 메타 수집 시작 (shares×close + PER + EPS)")
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(get_us_meta_single, us_symbols)
        for symbol, cap, name, per, eps in results:
            try:
                if cap == 0.0:
                    cap = us_meta.get(symbol, {}).get('cap', 0.0)
                if name == "N/A":
                    name = us_meta.get(symbol, {}).get('name', "N/A")
                if per == 0.0:
                    per = us_meta.get(symbol, {}).get('per', 0.0)
                if eps == 0.0:
                    eps = us_meta.get(symbol, {}).get('eps', 0.0)
                us_meta[symbol] = {'name': name, 'cap': cap, 'per': per, 'eps': eps}
            except Exception as e:
                print(f"US {symbol} 업데이트 실패: {e} – 기존 유지")
                if symbol in old_meta.get('US', {}):
                    us_meta[symbol] = old_meta['US'][symbol]
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