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
BACKTEST_DB_PATH = os.path.join(META_DIR, 'backtest.db')
BACKTEST_CSV_PATH = os.path.join(DATA_DIR, 'backtest_results.csv')

SHORT_FOLDER = os.path.join(DATA_DIR, 'short_term_results')
MID_FOLDER = os.path.join(DATA_DIR, 'screener_results')
SELL_FOLDER = os.path.join(DATA_DIR, 'sell_signals')
os.makedirs(SHORT_FOLDER, exist_ok=True)
os.makedirs(MID_FOLDER, exist_ok=True)
os.makedirs(SELL_FOLDER, exist_ok=True)

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
                signal_obv_9d TEXT,
                signal_obv_20d TEXT,
                market_cap DOUBLE,
                avg_trading_value_20d DOUBLE,
                today_trading_value DOUBLE,
                turnover DOUBLE,
                per DOUBLE,
                eps DOUBLE,
                cap_status VARCHAR,
                upper_closes INTEGER,
                lower_closes INTEGER,
                sector VARCHAR,
                sector_trend VARCHAR,
                ma20 TEXT,
                ma50 TEXT,
                ma200 TEXT,
                break_20high INTEGER,
                close_d TEXT
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
        
        if row_count == 0:
            print("❌ DB가 비어있습니다! compute_indicators.py를 먼저 실행하세요.")
            return pd.DataFrame()
        
        df = con.execute("SELECT * FROM indicators").fetchdf()
        print(f"전체 데이터 로드: {len(df)}행")

        # JSON 파싱
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

        # RSI 파싱
        rsi_parsed = parse_json_array('rsi_d', 3)
        df['rsi_d_2ago'] = rsi_parsed[0]
        df['rsi_d_1ago'] = rsi_parsed[1]
        df['rsi_d_latest'] = rsi_parsed[2]

        # OBV 파싱
        obv_parsed = parse_json_array('obv_d', 3)
        df['obv_latest'] = obv_parsed[0]
        df['obv_1ago'] = obv_parsed[1]
        df['obv_2ago'] = obv_parsed[2]

        # OBV 9일 평균 파싱
        signal_obv_9_parsed = parse_json_array('signal_obv_9d', 3)
        df['signal_obv_9_latest'] = signal_obv_9_parsed[0]
        df['signal_obv_9_1ago'] = signal_obv_9_parsed[1]

        # OBV 20일 평균 파싱 (4일치)
        signal_obv_20_parsed = parse_json_array('signal_obv_20d', 4)
        df['signal_obv_20_latest'] = signal_obv_20_parsed[0]
        df['signal_obv_20_1ago'] = signal_obv_20_parsed[1]
        df['signal_obv_20_2ago'] = signal_obv_20_parsed[2]
        df['signal_obv_20_3ago'] = signal_obv_20_parsed[3]

        # 종가 파싱
        close_parsed = parse_json_array('close_d', 3)
        df['close_today'] = close_parsed[0]
        df['close_yesterday'] = close_parsed[1]
        df['close_2ago'] = close_parsed[2]

        # MA20 파싱
        ma20_parsed = parse_json_array('ma20', 3)
        df['ma20_today'] = ma20_parsed[0]
        df['ma20_yesterday'] = ma20_parsed[1]
        df['ma20_2ago'] = ma20_parsed[2]

        # MA50 파싱
        ma50_parsed = parse_json_array('ma50', 3)
        df['ma50_today'] = ma50_parsed[0]
        df['ma50_yesterday'] = ma50_parsed[1]
        df['ma50_2ago'] = ma50_parsed[2]

        # MA200 파싱
        ma200_parsed = parse_json_array('ma200', 3)
        df['ma200_today'] = ma200_parsed[0]
        df['ma200_yesterday'] = ma200_parsed[1]
        df['ma200_2ago'] = ma200_parsed[2]

        if 'per' not in df.columns:
            df['per'] = 0.0
        if 'eps' not in df.columns:
            df['eps'] = 0.0
        if 'sector' not in df.columns:
            df['sector'] = 'N/A'
        if 'sector_trend' not in df.columns:
            df['sector_trend'] = 'N/A'

        market_filter = df['market'].isin(
            ['US'] if use_us and not use_kr else
            ['KR'] if use_kr and not use_us else
            ['US', 'KR']
        )
        df_filtered = df[market_filter].copy()

        # 영업일 조정
        today = datetime.now()
        if today.weekday() >= 5:
            days_back = today.weekday() - 4
            today -= timedelta(days=days_back)
        today_str = today.strftime('%Y-%m-%d')

        save_columns = ['symbol', 'market', 'name', 'rsi_d', 'macd_d', 'signal_d', 'obv_d', 
                       'signal_obv_9d', 'signal_obv_20d', 'market_cap', 'avg_trading_value_20d', 
                       'today_trading_value', 'turnover', 'per', 'eps', 'cap_status', 
                       'upper_closes', 'lower_closes', 'sector', 'sector_trend',
                       'ma20', 'ma50', 'ma200', 'break_20high', 'close_d', 'close']

        # ========================================
        # 🟨 중기 (3개월) - 파동 초입 모멘텀
        # ========================================
        print("\n🟨 중기 스크리닝 시작...")
        
        rsi_rising_mid = (
            (df_filtered['rsi_d_2ago'] < df_filtered['rsi_d_1ago']) & 
            (df_filtered['rsi_d_1ago'] < df_filtered['rsi_d_latest']) & 
            (df_filtered['rsi_d_latest'] >= 40) & 
            (df_filtered['rsi_d_latest'] <= 60)
        )
        
        golden_cross = df_filtered['ma50_today'] > df_filtered['ma200_today']
        
        obv_above_20ma = df_filtered['obv_latest'] > df_filtered['signal_obv_20_latest']
        obv_20ma_rising = df_filtered['signal_obv_20_latest'] > df_filtered['signal_obv_20_3ago']
        
        obv_cross_3d = (
            ((df_filtered['obv_2ago'] <= df_filtered['signal_obv_20_2ago']) & 
             (df_filtered['obv_latest'] > df_filtered['signal_obv_20_latest'])) |
            ((df_filtered['obv_1ago'] <= df_filtered['signal_obv_20_1ago']) & 
             (df_filtered['obv_latest'] > df_filtered['signal_obv_20_latest']))
        )
        
        obv_condition_mid = obv_above_20ma & (obv_20ma_rising | obv_cross_3d)
        
        # ✅ 1. 오늘 거래대금이 20일 평균 이상 추가
        trading_avg_or_above = df_filtered['today_trading_value'] >= df_filtered['avg_trading_value_20d']
        
        # ✅ 중기 필수 조건 종합 (거래대금 조건 추가)
        mid_filter = rsi_rising_mid & golden_cross & obv_condition_mid & trading_avg_or_above
        
        mid_results = df_filtered[mid_filter].copy()
        
        if mid_results.empty:
            print("⚠️ 중기 필터링 결과 없음 - 스킵")
        else:
            mid_results = mid_results.sort_values('rsi_d_latest')
            
            mid_results['symbol'] = mid_results.apply(
                lambda row: str(row['symbol']).zfill(6) if row['market'] == 'KR' else str(row['symbol']), axis=1
            )
            
            mid_results = add_close_price(mid_results)
            
            if 'close' not in mid_results.columns:
                print("❌ close 컬럼 추가 실패!")
                mid_results['close'] = 0.0
            
            mid_csv_path = os.path.join(MID_FOLDER, f"{today_str}_mid.csv")
            mid_results[save_columns].to_csv(mid_csv_path, index=False, encoding='utf-8-sig')
            print(f"✅ 중기 완료! 총 {len(mid_results)}개 종목 선정 (CSV: {mid_csv_path})")

        # ========================================
        # 🟥 단기 (1개월) - 내일 급등 후보
        # ========================================
        print("\n🟥 단기 스크리닝 시작...")
        
        obv_bullish_short = (
            (df_filtered['obv_latest'] > df_filtered['signal_obv_9_latest']) & 
            (df_filtered['obv_1ago'] <= df_filtered['signal_obv_9_1ago'])
        )
        
        trading_surge = df_filtered['today_trading_value'] >= 2.0 * df_filtered['avg_trading_value_20d']
        
        ma20_breakout = (
            (df_filtered['close_today'] > df_filtered['ma20_today']) & 
            (df_filtered['close_yesterday'] <= df_filtered['ma20_yesterday'])
        )
        
        break_condition = (df_filtered['break_20high'] == 1) | ma20_breakout
        
        short_filter = obv_bullish_short & trading_surge & break_condition
        
        short_results = df_filtered[short_filter].copy()
        
        if short_results.empty:
            print("⚠️ 단기 필터링 결과 없음 - 스킵")
        else:
            short_results = short_results.sort_values('rsi_d_latest')
            
            short_results['symbol'] = short_results.apply(
                lambda row: str(row['symbol']).zfill(6) if row['market'] == 'KR' else str(row['symbol']), axis=1
            )
            
            short_results = add_close_price(short_results)
            
            if 'close' not in short_results.columns:
                print("❌ close 컬럼 추가 실패!")
                short_results['close'] = 0.0
            
            short_csv_path = os.path.join(SHORT_FOLDER, f"{today_str}_short.csv")
            short_results[save_columns].to_csv(short_csv_path, index=False, encoding='utf-8-sig')
            print(f"✅ 단기 완료! 총 {len(short_results)}개 종목 선정 (CSV: {short_csv_path})")

        # ========================================
        # 🟪 매도시점 - 이익 실현 or 손절
        # ========================================
        print("\n🟪 매도시점 스크리닝 시작...")
        
        # 1️⃣ RSI 과열 (70 이상)
        rsi_overheated = df_filtered['rsi_d_latest'] >= 70
        
        # 2️⃣ OBV 하락 크로스
        obv_bearish = (
            (df_filtered['obv_latest'] < df_filtered['signal_obv_9_latest']) & 
            (df_filtered['obv_1ago'] >= df_filtered['signal_obv_9_1ago'])
        )
        
        # 3️⃣ RSI 하강 지속
        rsi_falling = (
            (df_filtered['rsi_d_2ago'] > df_filtered['rsi_d_1ago']) & 
            (df_filtered['rsi_d_1ago'] > df_filtered['rsi_d_latest']) & 
            (df_filtered['rsi_d_latest'] <= 50)
        )
        
        sell_filter = rsi_overheated | obv_bearish | rsi_falling
        
        sell_results = df_filtered[sell_filter].copy()
        
        if sell_results.empty:
            print("⚠️ 매도시점 필터링 결과 없음 - 스킵")
        else:
            sell_results = sell_results.sort_values('rsi_d_latest', ascending=False)
            
            sell_results['symbol'] = sell_results.apply(
                lambda row: str(row['symbol']).zfill(6) if row['market'] == 'KR' else str(row['symbol']), axis=1
            )
            
            sell_results = add_close_price(sell_results)
            
            if 'close' not in sell_results.columns:
                print("❌ close 컬럼 추가 실패!")
                sell_results['close'] = 0.0
            
            # ✅ 2. 날짜 prefix 제거 (항상 sell.csv로 덮어쓰기)
            sell_csv_path = os.path.join(SELL_FOLDER, "sell.csv")
            sell_results[save_columns].to_csv(sell_csv_path, index=False, encoding='utf-8-sig')
            print(f"✅ 매도시점 완료! 총 {len(sell_results)}개 종목 선정 (CSV: {sell_csv_path})")

        # 백테스팅 DB 생성
        create_backtest_db()

        return mid_results

    except Exception as e:
        print(f"스크리너 에러: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def load_all_csv_from_folder(folder_path, result_type):
    all_df = pd.DataFrame()
    if not os.path.exists(folder_path):
        return all_df
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path, dtype={'symbol': str})
            df['type'] = result_type
            all_df = pd.concat([all_df, df], ignore_index=True)
    return all_df

def create_backtest_db():
    # ✅ 3. 매도는 백테스트에 포함 안 함 (단기/중기만)
    short_df = load_all_csv_from_folder(SHORT_FOLDER, 'short')
    mid_df = load_all_csv_from_folder(MID_FOLDER, 'mid')

    backtest_df = pd.concat([short_df, mid_df], ignore_index=True)

    meta = load_meta()

    backtest_df['latest_close'] = 0.0
    backtest_df['latest_update'] = 'N/A'
    backtest_df['change_rate'] = 0.0

    for idx, row in backtest_df.iterrows():
        symbol = row['symbol']
        market = row['market']
        if market == 'KR':
            symbol = str(symbol).zfill(6)
        meta_dict = meta.get(market, {}).get(symbol, {})
        latest_close = meta_dict.get('close', 0.0)
        latest_update = meta_dict.get('cap_status', 'N/A')

        past_close = row.get('close', 0.0)
        change_rate = ((latest_close - past_close) / past_close * 100) if past_close != 0 else 0.0
        change_rate = round(change_rate, 2)

        backtest_df.at[idx, 'latest_close'] = latest_close
        backtest_df.at[idx, 'latest_update'] = latest_update
        backtest_df.at[idx, 'change_rate'] = change_rate

    con_back = duckdb.connect(BACKTEST_DB_PATH)
    con_back.execute("DROP TABLE IF EXISTS backtest")
    con_back.execute("CREATE TABLE backtest AS SELECT * FROM backtest_df")
    con_back.close()

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