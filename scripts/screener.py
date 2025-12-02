import duckdb
import pandas as pd
import os
import json
import numpy as np
from pykrx import stock
import yfinance as yf
import sys
from datetime import datetime
import time
import traceback

DATA_DIR = os.getenv('DATA_DIR', './data')
os.makedirs(DATA_DIR, exist_ok=True)
META_DIR = os.path.join(DATA_DIR, 'meta')
os.makedirs(META_DIR, exist_ok=True)
DB_PATH = os.path.join(META_DIR, 'universe.db')

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
                cap_status VARCHAR  # 추가
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

        # 장타: OBV 상승 + RSI 하강 + EPS/PER + 유동성
        long_results = df_filtered[obv_bullish & rsi_3down & per_filter & liquidity_filter].copy()
        long_results = long_results.sort_values('rsi_d_latest').head(top_n)
        long_results_path = os.path.join(META_DIR, 'long_term_results.parquet')
        long_results.to_parquet(long_results_path)
        print(f"\n장타 완료! 총 {len(long_results)}개 종목 선정")
        if not long_results.empty:
            print(long_results[['symbol', 'name', 'rsi_d_latest', 'per', 'eps', 'market', 'cap_status']].to_string(index=False))

        # 단타: OBV 상승 + RSI 상승 + 거래대금 + 유동성
        short_results = df_filtered[obv_bullish & rsi_3up & trading_high & liquidity_filter].copy()
        short_results = short_results.sort_values('rsi_d_latest').head(top_n)
        short_results_path = os.path.join(META_DIR, 'short_term_results.parquet')
        short_results.to_parquet(short_results_path)
        print(f"\n단타 완료! 총 {len(short_results)}개 종목 선정")
        if not short_results.empty:
            print(short_results[['symbol', 'name', 'rsi_d_latest', 'today_trading_value', 'market', 'cap_status']].to_string(index=False))

        # 기존 screener_results도 유지 (필요시)
        results = df_filtered[obv_bullish & rsi_3up & per_filter & liquidity_filter].copy()
        results = results.sort_values('rsi_d_latest').head(top_n)
        numeric_cols = results.select_dtypes(include=['float64']).columns
        for col in numeric_cols:
            results[col] = results[col].round(2)
        results_path = os.path.join(META_DIR, 'screener_results.parquet')
        results.to_parquet(results_path)

        print(f"\n기존 스크리너 완료! 총 {len(results)}개 종목 선정")
        if not results.empty:
            print(results[['symbol', 'name', 'rsi_d_latest', 'per', 'eps', 'market', 'cap_status']].to_string(index=False))  # cap_status 추가

        return results

    except Exception as e:
        print(f"스크리너 에러: {e}")
        traceback.print_exc()
        return pd.DataFrame()

if __name__ == "__main__":
    use_us = sys.argv[1].lower() == 'true' if len(sys.argv) > 1 else True
    use_kr = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else True
    top_n = int(sys.argv[3]) if len(sys.argv) > 3 else 50

    print(f"스크리너 실행 → US: {use_us}, KR: {use_kr}, Top {top_n}개")
    results = run_screener(top_n, use_us, use_kr)
    
    if con:
        con.close()
    print("스크리너 종료!")