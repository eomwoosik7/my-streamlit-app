import duckdb
import pandas as pd
import os
import json
import numpy as np
from pykrx import stock
import yfinance as yf
import sys
from datetime import datetime, timedelta
import time
import traceback

DATA_DIR = os.getenv('DATA_DIR', './data')
os.makedirs(DATA_DIR, exist_ok=True)
META_DIR = os.path.join(DATA_DIR, 'meta')
os.makedirs(META_DIR, exist_ok=True)
DB_PATH = os.path.join(META_DIR, 'universe.db')
BACKTEST_DB_PATH = os.path.join(META_DIR, 'backtest.db')  # 새 백테스팅 DB
BACKTEST_CSV_PATH = os.path.join(DATA_DIR, 'backtest_results.csv')  # 백테스팅 CSV

# 폴더 생성 (CSV 저장용)
LONG_FOLDER = os.path.join(DATA_DIR, 'long_term_results')
SHORT_FOLDER = os.path.join(DATA_DIR, 'short_term_results')
MID_FOLDER = os.path.join(DATA_DIR, 'screener_results')  # 중기
os.makedirs(LONG_FOLDER, exist_ok=True)
os.makedirs(SHORT_FOLDER, exist_ok=True)
os.makedirs(MID_FOLDER, exist_ok=True)

# meta.json 로드
META_FILE = os.path.join(META_DIR, 'tickers_meta.json')

def load_meta():
    if os.path.exists(META_FILE):
        with open(META_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        print("메타 파일 없음 – 빈 딕트 반환")
        return {'KR': {}, 'US': {}}

def add_close_price(df):
    if df.empty or 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()
    df['close'] = np.nan
    for idx, row in df.iterrows():
        symbol = row['symbol']
        market = row['market']
        meta_dict = meta.get(market, {}).get(symbol, {})
        close_price = meta_dict.get('close', 0.0)
        df.at[idx, 'close'] = close_price
    return df

con = None

def ensure_db_exists():
    if not os.path.exists(DB_PATH):
        con_temp = duckdb.connect(DB_PATH, read_only=False)
        con_temp.execute("""
            CREATE TABLE IF NOT EXISTS indicators (
                symbol VARCHAR PRIMARY KEY,
                market VARCHAR,
                name VARCHAR,
                rsi_d TEXT,
                macd_d TEXT,
                signal_d TEXT,
                obv_d TEXT,
                signal_obv_d TEXT,
                market_cap DOUBLE,
                avg_trading_value_20d DOUBLE,
                today_trading_value DOUBLE,
                turnover DOUBLE,
                per DOUBLE,
                eps DOUBLE,
                cap_status VARCHAR,  # 추가
                sector VARCHAR  # 추가
            )
        """)
        con_temp.close()
        print(f"DB 생성 완료: {DB_PATH}")

try:
    ensure_db_exists()
    con = duckdb.connect(DB_PATH)
    print("DB 연결 성공!")
except duckdb.IOException:
    print("DB 파일 잠김 – 5초 대기 후 재시도")
    time.sleep(5)
    ensure_db_exists()
    con = duckdb.connect(DB_PATH)

def run_screener(top_n=50, use_us=True, use_kr=True):
    try:
        print("스크리너 시작...")
        row_count = con.execute("SELECT COUNT(*) FROM indicators").fetchone()[0]
        print(f"DB 행 수: {row_count}")
        
        df = con.execute("SELECT * FROM indicators").fetchdf()
        print(f"전체 데이터 로드: {len(df)}행")

        def parse_json_array(col, num_vals=3):
            def safe_parse(x):
                if pd.isna(x) or not isinstance(x, str) or len(x) <= 2:
                    return [0.0] * num_vals
                try:
                    arr = json.loads(x)
                    return [float(v) if isinstance(v, (int, float)) else 0.0 for v in arr[:num_vals]]
                except:
                    return [0.0] * num_vals
            parsed = df[col].apply(safe_parse).apply(pd.Series)
            return parsed.iloc[:, :num_vals]

        rsi_parsed = parse_json_array('rsi_d', 3)
        df['rsi_d_2ago'] = rsi_parsed[0]
        df['rsi_d_1ago'] = rsi_parsed[1]
        df['rsi_d_latest'] = rsi_parsed[2]

        obv_parsed = parse_json_array('obv_d', 3)
        df['obv_latest'] = obv_parsed[0]   
        df['obv_1ago'] = obv_parsed[1]

        signal_obv_parsed = parse_json_array('signal_obv_d', 3)
        df['signal_obv_latest'] = signal_obv_parsed[0]
        df['signal_obv_1ago'] = signal_obv_parsed[1]

        if 'per' not in df.columns:
            df['per'] = 0.0
        if 'eps' not in df.columns:
            df['eps'] = 0.0
        if 'sector' not in df.columns:  # sector 없으면 N/A 추가
            df['sector'] = 'N/A'

        market_filter = df['market'].isin(
            ['US'] if use_us and not use_kr else
            ['KR'] if use_kr and not use_us else
            ['US', 'KR']
        )
        df_filtered = df[market_filter].copy()

        obv_bullish = (df_filtered['obv_latest'] > df_filtered['signal_obv_latest']) & \
                      (df_filtered['obv_1ago'] <= df_filtered['signal_obv_1ago'])

        rsi_3up = (df_filtered['rsi_d_2ago'] < df_filtered['rsi_d_1ago']) & \
                  (df_filtered['rsi_d_1ago'] < df_filtered['rsi_d_latest']) & \
                  (df_filtered['rsi_d_latest'] <= 50)

        rsi_3down = (df_filtered['rsi_d_2ago'] > df_filtered['rsi_d_1ago']) & \
                    (df_filtered['rsi_d_1ago'] > df_filtered['rsi_d_latest']) & \
                    (df_filtered['rsi_d_latest'] <= 50)

        trading_high = df_filtered['today_trading_value'] > 1.5 * df_filtered['avg_trading_value_20d']

        per_filter = (df_filtered['per'] >= 3) & (df_filtered['per'] <= 30) & (df_filtered['eps'] > 0)

        liquidity_filter = (
            ((df_filtered['market'] == 'US') & (df_filtered['market_cap'] >= 2000000000.0)) |
            ((df_filtered['market'] == 'KR') & (df_filtered['market_cap'] >= 200000000000.0))
        )

        # 영업일 조정 (주말 → 금요일)
        today = datetime.now()
        if today.weekday() >= 5:  # 5: 토요일, 6: 일요일
            days_back = today.weekday() - 4  # 토요일:1, 일요일:2만큼 이전 (금요일)
            today -= timedelta(days=days_back)
        today_str = today.strftime('%Y-%m-%d')

        # 장타: OBV 상승 + RSI 하강 + EPS/PER + 유동성
        long_results = df_filtered[obv_bullish & rsi_3down & per_filter & liquidity_filter].copy()
        long_results = long_results.sort_values('rsi_d_latest')
        long_results = add_close_price(long_results)  # close 추가
        # symbol 형식 보장 (CSV 저장 전)
        long_results['symbol'] = long_results.apply(lambda row: str(row['symbol']).zfill(6) if row['market'] == 'KR' else str(row['symbol']), axis=1)
        long_csv_path = os.path.join(LONG_FOLDER, f"{today_str}_long.csv")
        long_results.to_csv(long_csv_path, index=False, encoding='utf-8-sig')
        print(f"\n장타 완료! 총 {len(long_results)}개 종목 선정 (CSV: {long_csv_path})")
        if not long_results.empty:
            print(long_results[['symbol', 'name', 'rsi_d_latest', 'per', 'eps', 'market', 'cap_status', 'sector']].to_string(index=False))  # sector 추가

        # 단타: OBV 상승 + RSI 상승 + 거래대금 + 유동성
        short_results = df_filtered[obv_bullish & rsi_3up & trading_high & liquidity_filter].copy()
        short_results = short_results.sort_values('rsi_d_latest')
        short_results = add_close_price(short_results)  # close 추가
        # symbol 형식 보장 (CSV 저장 전)
        short_results['symbol'] = short_results.apply(lambda row: str(row['symbol']).zfill(6) if row['market'] == 'KR' else str(row['symbol']), axis=1)
        short_csv_path = os.path.join(SHORT_FOLDER, f"{today_str}_short.csv")
        short_results.to_csv(short_csv_path, index=False, encoding='utf-8-sig')
        print(f"\n단타 완료! 총 {len(short_results)}개 종목 선정 (CSV: {short_csv_path})")
        if not short_results.empty:
            print(short_results[['symbol', 'name', 'rsi_d_latest', 'today_trading_value', 'market', 'cap_status', 'sector']].to_string(index=False))  # sector 추가

        # 중기: OBV 상승 + RSI 상승 + EPS/PER + 유동성 (기존 results)
        mid_results = df_filtered[obv_bullish & rsi_3up & per_filter & liquidity_filter].copy()
        mid_results = mid_results.sort_values('rsi_d_latest')
        mid_results = add_close_price(mid_results)  # close 추가
        # round(2) 루프 제거: 반올림 방지
        # symbol 형식 보장 (CSV 저장 전)
        mid_results['symbol'] = mid_results.apply(lambda row: str(row['symbol']).zfill(6) if row['market'] == 'KR' else str(row['symbol']), axis=1)
        mid_csv_path = os.path.join(MID_FOLDER, f"{today_str}_mid.csv")
        mid_results.to_csv(mid_csv_path, index=False, encoding='utf-8-sig')

        print(f"\n중기 스크리너 완료! 총 {len(mid_results)}개 종목 선정 (CSV: {mid_csv_path})")
        if not mid_results.empty:
            print(mid_results[['symbol', 'name', 'rsi_d_latest', 'per', 'eps', 'market', 'cap_status', 'sector']].to_string(index=False))  # cap_status, sector 추가

        # 백테스팅 DB 생성
        create_backtest_db()

        return mid_results  # 기존 반환 유지

    except Exception as e:
        print(f"스크리너 에러: {e}")
        traceback.print_exc()
        return pd.DataFrame()

# 폴더 내 모든 CSV 로드 및 종합
def load_all_csv_from_folder(folder_path, result_type):
    all_df = pd.DataFrame()
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path, dtype={'symbol': str})  # symbol을 str로 유지
            # date 컬럼 추가 안 함 (제거)
            df['type'] = result_type
            all_df = pd.concat([all_df, df], ignore_index=True)
    return all_df

# 백테스팅 DB 생성
def create_backtest_db():
    # 각 폴더 CSV 종합
    long_df = load_all_csv_from_folder(LONG_FOLDER, 'long')
    short_df = load_all_csv_from_folder(SHORT_FOLDER, 'short')
    mid_df = load_all_csv_from_folder(MID_FOLDER, 'mid')

    backtest_df = pd.concat([long_df, short_df, mid_df], ignore_index=True)

    # meta 로드
    meta = load_meta()

    # 추가 컬럼
    backtest_df['latest_close'] = 0.0
    backtest_df['latest_update'] = 'N/A'
    backtest_df['change_rate'] = 0.0
    backtest_df['sector'] = 'N/A'  # sector 추가

    for idx, row in backtest_df.iterrows():
        symbol = row['symbol']
        market = row['market']
        if market == 'KR':
            symbol = str(symbol).zfill(6)  # KR 티커 6자리 leading zero 보장
        meta_dict = meta.get(market, {}).get(symbol, {})
        latest_close = meta_dict.get('close', 0.0)
        latest_update = meta_dict.get('cap_status', 'N/A')
        sector_val = meta_dict.get('sector', 'N/A')  # sector 불러오기

        # 과거 close (CSV에 저장된 close 사용)
        past_close = row.get('close', 0.0)
        change_rate = ((latest_close - past_close) / past_close * 100) if past_close != 0 else 0.0
        change_rate = round(change_rate, 2)  # 소숫점 둘째 자리

        backtest_df.at[idx, 'latest_close'] = latest_close
        backtest_df.at[idx, 'latest_update'] = latest_update
        backtest_df.at[idx, 'change_rate'] = change_rate
        backtest_df.at[idx, 'sector'] = sector_val  # sector 저장

    # DB 저장 (기존 테이블 drop 후 생성)
    con_back = duckdb.connect(BACKTEST_DB_PATH)
    con_back.execute("DROP TABLE IF EXISTS backtest")
    con_back.execute("CREATE TABLE backtest AS SELECT * FROM backtest_df")
    con_back.close()

    # CSV 저장
    backtest_df.to_csv(BACKTEST_CSV_PATH, index=False, encoding='utf-8-sig')

    print(f"백테스팅 DB 생성 완료: {BACKTEST_DB_PATH}")
    print(f"백테스팅 CSV 저장 완료: {BACKTEST_CSV_PATH}")

if __name__ == "__main__":
    use_us = sys.argv[1].lower() == 'true' if len(sys.argv) > 1 else True
    use_kr = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else True
    top_n = int(sys.argv[3]) if len(sys.argv) > 3 else 50

    print(f"스크리너 실행 → US: {use_us}, KR: {use_kr}, Top {top_n}개")
    results = run_screener(top_n, use_us, use_kr)
    
    if con:
        con.close()
    print("스크리너 종료!")