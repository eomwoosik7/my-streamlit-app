import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json
import time
import shutil
import numpy as np
import ast

DATA_DIR = os.getenv('DATA_DIR', './data')
os.makedirs(DATA_DIR, exist_ok=True)

META_DIR = os.path.join(DATA_DIR, 'meta')
DB_PATH = os.path.join(META_DIR, 'universe.db')
RESULTS_PATH = os.path.join(META_DIR, 'backtest.db')

SHORT_FOLDER = os.path.join(DATA_DIR, 'short_term_results')
MID_FOLDER = os.path.join(DATA_DIR, 'screener_results')

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


def load_etf_codes():
    """
    kr_stock_sectors.csv 가 있으면 Sector == 'ETF' 인 종목코드 set 반환
    없으면 빈 set 반환 (전체 수집)
    """
    sector_path = os.path.join(DATA_DIR, 'kr_stock_sectors.csv')
    if not os.path.exists(sector_path):
        print("ℹ️ kr_stock_sectors.csv 없음 → ETF 필터 없이 전체 수집")
        return set()

    try:
        df = pd.read_csv(sector_path, encoding='utf-8-sig', dtype={'종목코드': str})
        df['종목코드'] = df['종목코드'].str.zfill(6)
        etf_codes = set(df[df['Sector'] == 'ETF']['종목코드'].tolist())
        print(f"ℹ️ kr_stock_sectors.csv 로드 완료 → ETF {len(etf_codes)}개 제외 예정")
        return etf_codes
    except Exception as e:
        print(f"⚠️ kr_stock_sectors.csv 로드 실패: {e} → ETF 필터 없이 전체 수집")
        return set()


def _fetch_market_tickers(sosok):
    """
    네이버 금융 시가총액 페이지에서 전체 종목 수집
    sosok=0 → KOSPI, sosok=1 → KOSDAQ
    ETF 필터 없음 (전종목 수집)
    """
    market_name = 'KOSPI' if sosok == 0 else 'KOSDAQ'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://finance.naver.com/'
    }

    stocks = []
    page = 1

    while True:
        url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}'
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = 'euc-kr'
        soup = BeautifulSoup(res.text, 'html.parser')

        pager = soup.find('td', class_='pgRR')
        if pager is None:
            break

        last_page = int(pager.find('a')['href'].split('page=')[-1])

        table = soup.find('table', class_='type_2')
        if table is None:
            break

        for row in table.find_all('tr'):
            link = row.find('a', class_='tltle')
            if link is None:
                continue

            href = link.get('href', '')
            if 'code=' not in href:
                continue

            code = href.split('code=')[-1]
            name = link.text.strip()

            tds = row.find_all('td')

            close = 0
            if len(tds) >= 3:
                try:
                    close_text = tds[2].text.strip().replace(',', '')
                    close = int(close_text) if close_text.isdigit() else 0
                except:
                    close = 0

            cap = 0
            if len(tds) >= 7:
                try:
                    cap_text = tds[6].text.strip().replace(',', '')
                    cap = int(cap_text) if cap_text.isdigit() else 0
                except:
                    cap = 0

            if code and len(code) == 6 and code.isdigit():
                stocks.append({
                    'Code': code,
                    'Name': name,
                    'MarketCap': cap,
                    'Close': close,
                    'Market': market_name
                })

        print(f"  {market_name} {page}/{last_page} 페이지 수집 중...")

        if page >= last_page:
            break

        page += 1
        time.sleep(0.3)

    print(f"✅ {market_name} 전체 수집 완료: {len(stocks)}개")
    return stocks


def get_kospi_tickers(etf_codes=set()):
    """코스피 전체 종목 조회 (ETF 제외)"""
    try:
        print("📊 KOSPI 전체 종목 조회 중...")
        stocks = _fetch_market_tickers(sosok=0)

        if not stocks:
            print("🚨 KOSPI 종목 조회 실패")
            return [], pd.DataFrame(), None

        df = pd.DataFrame(stocks)
        df['MarketCap'] = pd.to_numeric(df['MarketCap'], errors='coerce').fillna(0)
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce').fillna(0)
        df = df.drop_duplicates('Code').reset_index(drop=True)

        # ETF 제외 (kr_stock_sectors.csv 있을 때만)
        if etf_codes:
            before = len(df)
            df = df[~df['Code'].isin(etf_codes)].reset_index(drop=True)
            print(f"ℹ️ KOSPI ETF 제외: {before}개 → {len(df)}개")

        tickers = df['Code'].tolist()
        date_str = today.strftime('%Y%m%d')

        print(f"✅ KOSPI: {len(tickers)}개 (날짜: {date_str})")
        print(f"샘플: {tickers[:5]}")

        return tickers, df, date_str

    except Exception as e:
        print(f"❌ KOSPI 종목 조회 실패: {e}")
        import traceback
        traceback.print_exc()
        return [], pd.DataFrame(), None


def get_kosdaq_tickers(etf_codes=set()):
    """코스닥 전체 종목 조회 (ETF 제외)"""
    try:
        print("📊 KOSDAQ 전체 종목 조회 중...")
        stocks = _fetch_market_tickers(sosok=1)

        if not stocks:
            print("🚨 KOSDAQ 종목 조회 실패")
            return [], pd.DataFrame(), None

        df = pd.DataFrame(stocks)
        df['MarketCap'] = pd.to_numeric(df['MarketCap'], errors='coerce').fillna(0)
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce').fillna(0)
        df = df.drop_duplicates('Code').reset_index(drop=True)

        # ETF 제외 (kr_stock_sectors.csv 있을 때만)
        if etf_codes:
            before = len(df)
            df = df[~df['Code'].isin(etf_codes)].reset_index(drop=True)
            print(f"ℹ️ KOSDAQ ETF 제외: {before}개 → {len(df)}개")

        tickers = df['Code'].tolist()
        date_str = today.strftime('%Y%m%d')

        print(f"✅ KOSDAQ: {len(tickers)}개 (날짜: {date_str})")
        print(f"샘플: {tickers[:5]}")

        return tickers, df, date_str

    except Exception as e:
        print(f"❌ KOSDAQ 종목 조회 실패: {e}")
        import traceback
        traceback.print_exc()
        return [], pd.DataFrame(), None


def fetch_kr_single(ticker, start_date, market='KOSPI'):
    """
    네이버 금융 siseJson API로 KR 일봉 다운로드 (수정주가 기준)
    market: 'KOSPI' or 'KOSDAQ' → 저장 폴더 분리
    """
    try:
        if not ticker or len(ticker) != 6 or not ticker.isdigit():
            print(f"⚠️ 잘못된 티커 형식: {ticker}")
            return False

        start_str = start_date.replace('-', '')
        end_str = today.strftime('%Y%m%d')

        url = (
            f'https://api.finance.naver.com/siseJson.naver'
            f'?symbol={ticker}&requestType=1'
            f'&startTime={start_str}&endTime={end_str}&timeframe=day'
        )

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.naver.com/'
        }

        res = requests.get(url, headers=headers, timeout=10)

        if res.status_code != 200:
            return False

        text = res.text.strip()
        if not text or text == '[]':
            return False

        raw = ast.literal_eval(text)

        rows = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 6:
                continue
            try:
                date_str_item = str(item[0])
                if len(date_str_item) != 8 or not date_str_item.isdigit():
                    continue
                rows.append({
                    'Date':   pd.to_datetime(date_str_item, format='%Y%m%d'),
                    'Open':   int(item[1]) if item[1] is not None else None,
                    'High':   int(item[2]) if item[2] is not None else None,
                    'Low':    int(item[3]) if item[3] is not None else None,
                    'Close':  int(item[4]) if item[4] is not None else None,
                    'Volume': int(item[5]) if item[5] is not None else None,
                })
            except:
                continue

        if not rows:
            return False

        df = pd.DataFrame(rows).dropna()
        df = df.set_index('Date').sort_index()
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]

        # KOSPI / KOSDAQ 폴더 분리 저장
        daily_dir = os.path.join(DATA_DIR, 'kr_daily', market.lower())
        os.makedirs(daily_dir, exist_ok=True)
        df.to_csv(os.path.join(daily_dir, f"{ticker}.csv"), encoding='utf-8-sig')

        return True

    except Exception as e:
        print(f"⚠️ {ticker} 다운로드 실패: {e}")
        return False


def get_kr_meta_single(ticker, df_kr):
    """KR 메타 정보 추출 (네이버 금융 기반)"""
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


# ====================================================
# ✅ 백테스트 누락 종목 보완 함수
# ====================================================

def get_backtest_symbols():
    """
    short_term_results, screener_results 폴더에서
    백테스트 대상 KR 종목 symbol 전체 목록 추출
    """
    symbols = set()
    for folder in [SHORT_FOLDER, MID_FOLDER]:
        if not os.path.exists(folder):
            continue
        for file in os.listdir(folder):
            if not file.endswith('.csv'):
                continue
            try:
                df = pd.read_csv(
                    os.path.join(folder, file),
                    dtype={'symbol': str},
                    usecols=['symbol', 'market']
                )
                kr_df = df[df['market'].isin(['KR', 'KOSPI', 'KOSDAQ'])]
                for sym in kr_df['symbol'].tolist():
                    symbols.add(str(sym).zfill(6))
            except Exception as e:
                print(f"⚠️ {file} 읽기 실패: {e}")
    return list(symbols)


def get_naver_stock_info(ticker):
    """
    네이버 금융에서 종목명, 현재가, 시가총액 크롤링
    전체 종목 수집 후에도 누락된 종목의 메타 보완용
    """
    result = {'name': 'N/A', 'close': 0.0, 'cap': 0.0}
    try:
        import re
        url = f"https://finance.naver.com/item/main.nhn?code={ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.naver.com/'
        }
        res = requests.get(url, headers=headers, timeout=5)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        # 종목명
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.text.strip()
            name_part = title_text.split(':')[0].strip()
            if name_part:
                result['name'] = name_part

        # 현재가
        price_tag = soup.find('p', class_='no_today')
        if price_tag:
            blind_tag = price_tag.find('span', class_='blind')
            if blind_tag:
                try:
                    result['close'] = float(blind_tag.text.strip().replace(',', ''))
                except:
                    pass

        # 시가총액 (억원 단위)
        text = soup.get_text()
        cap_match = re.search(r'시가총액\s*([\d,]+)\s*억원', text)
        if cap_match:
            try:
                result['cap'] = float(cap_match.group(1).replace(',', ''))
            except:
                pass

    except Exception as e:
        print(f"⚠️ {ticker} 네이버 크롤링 실패: {e}")

    return result


def supplement_backtest_symbols(all_tickers_set, kospi_meta, kosdaq_meta, start_date, today_str):
    """
    백테스트 대상 종목 중 오늘 수집 목록에서 누락된 종목을
    찾아서 일봉 다운로드 + 메타 보완
    (전체 수집 후에도 상장폐지/거래정지 등으로 누락될 수 있음)
    """
    print("\n" + "="*50)
    print("🔍 백테스트 누락 종목 보완 시작")
    print("="*50)

    backtest_symbols = get_backtest_symbols()
    if not backtest_symbols:
        print("⚠️ 백테스트 대상 종목 없음 (스킵)")
        return kospi_meta, kosdaq_meta

    print(f"📋 백테스트 대상 종목: {len(backtest_symbols)}개")

    missing_symbols = [s for s in backtest_symbols if s not in all_tickers_set]

    if not missing_symbols:
        print("✅ 누락 종목 없음")
        return kospi_meta, kosdaq_meta

    print(f"⚠️ 누락 종목: {len(missing_symbols)}개")
    for s in missing_symbols:
        name = kospi_meta.get(s, kosdaq_meta.get(s, {})).get('name', 'N/A')
        print(f"   - {s} ({name})")

    # 누락 종목 일봉 다운로드 (어느 시장인지 모르므로 kospi 폴더에 저장)
    print(f"\n📥 누락 종목 일봉 다운로드 시작...")
    dl_success = 0
    dl_fail = 0
    for symbol in missing_symbols:
        # KOSPI 메타에 있으면 kospi, 없으면 kosdaq 폴더
        market = 'KOSPI' if symbol in kospi_meta else 'KOSDAQ'
        if fetch_kr_single(symbol, start_date, market=market):
            dl_success += 1
            print(f"   ✅ {symbol} ({market}) 다운로드 완료")
        else:
            dl_fail += 1
            print(f"   ❌ {symbol} 다운로드 실패")
        time.sleep(0.3)

    print(f"\n✅ 일봉 다운로드 완료: 성공 {dl_success}개 / 실패 {dl_fail}개")

    # 누락 종목 메타 보완
    print(f"\n📊 누락 종목 메타 보완 시작...")
    for symbol in missing_symbols:
        # KOSPI/KOSDAQ 중 어디 있는지 확인
        if symbol in kospi_meta:
            target_meta = kospi_meta
        elif symbol in kosdaq_meta:
            target_meta = kosdaq_meta
        else:
            # 어디도 없으면 KOSPI로 기본 처리
            target_meta = kospi_meta

        old_data = target_meta.get(symbol, {})

        if old_data.get('cap_status') == today_str and old_data.get('cap', 0) > 0:
            print(f"   ⏭️ {symbol} 메타 이미 최신 (스킵)")
            continue

        info = get_naver_stock_info(symbol)
        print(f"   📌 {symbol} → 종목명: {info['name']}, 종가: {info['close']:,.0f}, 시총: {info['cap']:,.0f}억")

        target_meta[symbol] = {
            'name':         info['name'] if info['name'] != 'N/A' else old_data.get('name', 'N/A'),
            'cap':          info['cap'] if info['cap'] > 0 else old_data.get('cap', 0.0),
            'cap_status':   today_str,
            'per':          old_data.get('per', 0.0),
            'eps':          old_data.get('eps', 0.0),
            'close':        info['close'] if info['close'] > 0 else old_data.get('close', 0.0),
            'sector':       old_data.get('sector', 'N/A'),
            'sector_trend': old_data.get('sector_trend', 'N/A'),
        }
        time.sleep(0.3)

    print(f"\n✅ 백테스트 누락 종목 보완 완료!")
    return kospi_meta, kosdaq_meta


if __name__ == '__main__':
    print(f"🗓️ 작업 기준일: {today.strftime('%Y-%m-%d %A')}")

    # 기존 데이터 삭제 (kospi / kosdaq 폴더 각각)
    for folder in ['kr_daily/kospi', 'kr_daily/kosdaq']:
        path = os.path.join(DATA_DIR, folder)
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                print(f"🗑️ {folder} 폴더 삭제 완료")
            except PermissionError as e:
                print(f"⚠️ {folder} 폴더 삭제 실패 (점유 중): {e}")
                print(f"   → 기존 파일 유지하고 계속 진행합니다")
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
        old_meta = {'KOSPI': {}, 'KOSDAQ': {}}
        print("📝 기존 meta.json 없음 → 새로 생성")

    start_date = (today - timedelta(days=730)).strftime('%Y-%m-%d')
    today_str = today.strftime('%Y-%m-%d')

    cap_failed_list = []

    # ====================================================
    # ETF 코드 로드 (kr_stock_sectors.csv 있으면 필터링)
    # ====================================================
    etf_codes = load_etf_codes()

    # ====================================================
    # KOSPI 데이터 수집
    # ====================================================
    print("\n" + "="*50)
    print("🇰🇷 KOSPI 데이터 수집 시작")
    print("="*50)
    kospi_tickers, df_kospi, kospi_date_str = get_kospi_tickers(etf_codes)

    if kospi_tickers:
        print(f"\n📥 KOSPI 일봉 다운로드 시작 (총 {len(kospi_tickers)}개)")
        success_count = 0
        fail_count = 0

        for i in range(0, len(kospi_tickers), 100):
            batch = kospi_tickers[i:i+100]
            batch_success = 0

            print(f"\n배치 {i//100 + 1}: {i}~{min(i+100, len(kospi_tickers))} 처리 중...")

            for ticker in batch:
                if fetch_kr_single(ticker, start_date, market='KOSPI'):
                    batch_success += 1
                    success_count += 1
                else:
                    fail_count += 1

                if (success_count + fail_count) % 10 == 0:
                    print(f"진행: {success_count + fail_count}/{len(kospi_tickers)} (성공: {success_count}, 실패: {fail_count})")

            print(f"✅ 배치 완료: {batch_success}/{len(batch)} 성공")
            time.sleep(3)

        print(f"\n✅ KOSPI 일봉 완료: 성공 {success_count}개, 실패 {fail_count}개")

    # KOSPI 메타 업데이트
    kospi_meta = old_meta.get('KOSPI', {})
    if kospi_tickers and not df_kospi.empty:
        print("\n📊 KOSPI 메타 수집 시작")
        batch_size = 200
        for i in tqdm(range(0, len(kospi_tickers), batch_size)):
            batch_tickers = kospi_tickers[i:i+batch_size]
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = executor.map(
                    lambda t: get_kr_meta_single(t, df_kospi),
                    batch_tickers
                )
            for ticker, cap, name, per, eps, close_price in results:
                old_data = kospi_meta.get(ticker, {})

                if cap == 0:
                    cap_failed_list.append({
                        'market': 'KOSPI',
                        'symbol': ticker,
                        'name': name if name != "N/A" else old_data.get('name', "N/A"),
                        'date': today_str
                    })

                kospi_meta[ticker] = {
                    'name':       name if name != "N/A" else old_data.get('name', "N/A"),
                    'cap':        cap if cap > 0 else old_data.get('cap', 0.0),
                    'cap_status': today_str,
                    'per':        per,
                    'eps':        eps,
                    'close':      close_price if close_price > 0 else old_data.get('close', 0.0)
                }
            time.sleep(5)

    # ====================================================
    # KOSDAQ 데이터 수집
    # ====================================================
    print("\n" + "="*50)
    print("🇰🇷 KOSDAQ 데이터 수집 시작")
    print("="*50)
    kosdaq_tickers, df_kosdaq, kosdaq_date_str = get_kosdaq_tickers(etf_codes)

    if kosdaq_tickers:
        print(f"\n📥 KOSDAQ 일봉 다운로드 시작 (총 {len(kosdaq_tickers)}개)")
        success_count = 0
        fail_count = 0

        for i in range(0, len(kosdaq_tickers), 100):
            batch = kosdaq_tickers[i:i+100]
            batch_success = 0

            print(f"\n배치 {i//100 + 1}: {i}~{min(i+100, len(kosdaq_tickers))} 처리 중...")

            for ticker in batch:
                if fetch_kr_single(ticker, start_date, market='KOSDAQ'):
                    batch_success += 1
                    success_count += 1
                else:
                    fail_count += 1

                if (success_count + fail_count) % 10 == 0:
                    print(f"진행: {success_count + fail_count}/{len(kosdaq_tickers)} (성공: {success_count}, 실패: {fail_count})")

            print(f"✅ 배치 완료: {batch_success}/{len(batch)} 성공")
            time.sleep(3)

        print(f"\n✅ KOSDAQ 일봉 완료: 성공 {success_count}개, 실패 {fail_count}개")

    # KOSDAQ 메타 업데이트
    kosdaq_meta = old_meta.get('KOSDAQ', {})
    if kosdaq_tickers and not df_kosdaq.empty:
        print("\n📊 KOSDAQ 메타 수집 시작")
        batch_size = 200
        for i in tqdm(range(0, len(kosdaq_tickers), batch_size)):
            batch_tickers = kosdaq_tickers[i:i+batch_size]
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = executor.map(
                    lambda t: get_kr_meta_single(t, df_kosdaq),
                    batch_tickers
                )
            for ticker, cap, name, per, eps, close_price in results:
                old_data = kosdaq_meta.get(ticker, {})

                if cap == 0:
                    cap_failed_list.append({
                        'market': 'KOSDAQ',
                        'symbol': ticker,
                        'name': name if name != "N/A" else old_data.get('name', "N/A"),
                        'date': today_str
                    })

                kosdaq_meta[ticker] = {
                    'name':       name if name != "N/A" else old_data.get('name', "N/A"),
                    'cap':        cap if cap > 0 else old_data.get('cap', 0.0),
                    'cap_status': today_str,
                    'per':        per,
                    'eps':        eps,
                    'close':      close_price if close_price > 0 else old_data.get('close', 0.0)
                }
            time.sleep(5)

    # ====================================================
    # ✅ 백테스트 누락 종목 보완
    # ====================================================
    all_tickers_set = set(str(t).zfill(6) for t in kospi_tickers + kosdaq_tickers)
    kospi_meta, kosdaq_meta = supplement_backtest_symbols(
        all_tickers_set, kospi_meta, kosdaq_meta, start_date, today_str
    )

    # ====================================================
    # 시가총액 미수집 종목 CSV 저장
    # ====================================================
    if cap_failed_list:
        df_failed = pd.DataFrame(cap_failed_list)
        failed_path = os.path.join(DATA_DIR, 'cap_failed.csv')
        df_failed.to_csv(failed_path, index=False, encoding='utf-8-sig')
        print(f"\n⚠️ 시가총액 미수집 종목: {len(cap_failed_list)}개 → {failed_path}")
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

    kospi_meta = convert_np(kospi_meta)
    kosdaq_meta = convert_np(kosdaq_meta)

    with open(meta_file, 'w', encoding='utf-8') as f:
        json.dump({'KOSPI': kospi_meta, 'KOSDAQ': kosdaq_meta}, f, ensure_ascii=False, indent=2)

    print("\n" + "="*50)
    print("✅ 모든 작업 완료!")
    print(f"📁 저장 위치: {meta_file}")
    print(f"📊 KOSPI: {len(kospi_meta)}개")
    print(f"📊 KOSDAQ: {len(kosdaq_meta)}개")
    print(f"📊 전체: {len(kospi_meta) + len(kosdaq_meta)}개")
    print("="*50)