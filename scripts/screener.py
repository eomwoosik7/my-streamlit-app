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
                eps DOUBLE
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

        # JSON 안전 파싱 함수
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

        # RSI 파싱 (3일)
        rsi_parsed = parse_json_array('rsi_d', 3)
        df['rsi_d_2ago'] = rsi_parsed[0]
        df['rsi_d_1ago'] = rsi_parsed[1]
        df['rsi_d_latest'] = rsi_parsed[2]

        # OBV 파싱
        obv_parsed = parse_json_array('obv_d', 3)
        df['obv_latest'] = obv_parsed[0]   # 최신값 (compute에서 reverse 했음)
        df['obv_1ago'] = obv_parsed[1]

        signal_obv_parsed = parse_json_array('signal_obv_d', 3)
        df['signal_obv_latest'] = signal_obv_parsed[0]
        df['signal_obv_1ago'] = signal_obv_parsed[1]

        # PER/EPS는 DB에 이미 있음 (compute_indicators.py에서 추가됨)
        # 만약 없으면 0으로 처리
        if 'per' not in df.columns:
            df['per'] = 0.0
        if 'eps' not in df.columns:
            df['eps'] = 0.0

        # 시장 필터링
        market_filter = df['market'].isin(
            ['US'] if use_us and not use_kr else
            ['KR'] if use_kr and not use_us else
            ['US', 'KR']
        )
        df_filtered = df[market_filter].copy()

        # 핵심 조건 3개만 적용
        obv_bullish = (df_filtered['obv_latest'] > df_filtered['signal_obv_latest']) & \
                      (df_filtered['obv_1ago'] <= df_filtered['signal_obv_1ago'])

        rsi_3up = (df_filtered['rsi_d_2ago'] < df_filtered['rsi_d_1ago']) & \
                  (df_filtered['rsi_d_1ago'] < df_filtered['rsi_d_latest']) & \
                  (df_filtered['rsi_d_latest'] <= 50)

        per_filter = (df_filtered['per'] >= 3) & (df_filtered['per'] <= 30) & (df_filtered['eps'] > 0)

        # 최종 필터링 (유동성 조건 완전 제거!)
        results = df_filtered[obv_bullish & rsi_3up & per_filter].copy()

        # RSI 낮은 순 정렬 → 상위 top_n개
        results = results.sort_values('rsi_d_latest').head(top_n)

        # 소수점 정리
        numeric_cols = results.select_dtypes(include=['float64']).columns
        for col in numeric_cols:
            results[col] = results[col].round(2)

        # 결과 저장 및 출력
        results_path = os.path.join(META_DIR, 'screener_results.parquet')
        results.to_parquet(results_path)

        print(f"\n스크리너 완료! 총 {len(results)}개 종목 선정")
        if not results.empty:
            print(results[['symbol', 'name', 'rsi_d_latest', 'per', 'eps', 'market']].to_string(index=False))
        else:
            print("조건에 맞는 종목이 없습니다.")

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