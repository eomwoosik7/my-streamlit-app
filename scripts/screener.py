import duckdb
import pandas as pd
import os
import json
import numpy as np
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

# 테스트 탭용 CSV 경로
BACKTEST_TEST_CSV_PATH = os.path.join(DATA_DIR, 'backtest_test.csv')

SHORT_FOLDER = os.path.join(DATA_DIR, 'short_term_results')
MID_FOLDER = os.path.join(DATA_DIR, 'screener_results')
SELL_FOLDER = os.path.join(DATA_DIR, 'sell_signals')
os.makedirs(SHORT_FOLDER, exist_ok=True)
os.makedirs(MID_FOLDER, exist_ok=True)
os.makedirs(SELL_FOLDER, exist_ok=True)

META_FILE = os.path.join(META_DIR, 'tickers_meta.json')

# KR 계열 market 값 (기존 KR + 새 KOSPI/KOSDAQ 모두 포함)
KR_MARKETS = {'KR', 'KOSPI', 'KOSDAQ'}


def load_meta():
    if os.path.exists(META_FILE):
        with open(META_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        print("메타 파일 없음 – 빈 딕트 반환")
        return {'KOSPI': {}, 'KOSDAQ': {}}


def get_meta_info(meta, symbol, market):
    """
    market 값에 따라 메타 조회
    - KOSPI/KOSDAQ → 해당 키에서 직접 조회
    - KR (구버전) → KOSPI 먼저, 없으면 KOSDAQ fallback
    """
    if market in ('KOSPI', 'KOSDAQ'):
        return meta.get(market, {}).get(symbol, {})
    else:
        # 구버전 KR: KOSPI → KOSDAQ → KR 순서로 fallback
        info = meta.get('KOSPI', {}).get(symbol, {})
        if not info:
            info = meta.get('KOSDAQ', {}).get(symbol, {})
        if not info:
            info = meta.get('KR', {}).get(symbol, {})
        return info


def get_daily_path(symbol, market):
    """
    market 값에 따라 일봉 CSV 경로 반환
    - KOSPI → kr_daily/kospi/{symbol}.csv
    - KOSDAQ → kr_daily/kosdaq/{symbol}.csv
    - KR (구버전) → kr_daily/{symbol}.csv (기존 경로)
    """
    if market == 'KOSPI':
        return os.path.join(DATA_DIR, 'kr_daily', 'kospi', f"{symbol}.csv")
    elif market == 'KOSDAQ':
        return os.path.join(DATA_DIR, 'kr_daily', 'kosdaq', f"{symbol}.csv")
    else:
        # 구버전 KR: 기존 루트 경로 → 없으면 kospi/kosdaq 순서로 fallback
        root_path = os.path.join(DATA_DIR, 'kr_daily', f"{symbol}.csv")
        if os.path.exists(root_path):
            return root_path
        kospi_path = os.path.join(DATA_DIR, 'kr_daily', 'kospi', f"{symbol}.csv")
        if os.path.exists(kospi_path):
            return kospi_path
        kosdaq_path = os.path.join(DATA_DIR, 'kr_daily', 'kosdaq', f"{symbol}.csv")
        if os.path.exists(kosdaq_path):
            return kosdaq_path
        return root_path  # 없어도 원래 경로 반환 (에러 메시지용)


def add_close_price(df):
    if df.empty or 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()
    df['close'] = np.nan
    for idx, row in df.iterrows():
        symbol = row['symbol']
        market = row['market']
        meta_dict = get_meta_info(meta, symbol, market)
        close_price = meta_dict.get('close', 0.0)
        df.at[idx, 'close'] = close_price
    return df


def get_historical_close(symbol, market, target_date):
    """CSV 파일에서 특정 날짜의 종가 조회"""
    try:
        daily_path = get_daily_path(symbol, market)

        if not os.path.exists(daily_path):
            print(f"⚠️ 파일 없음: {symbol} ({market})")
            return None

        df = pd.read_csv(daily_path, index_col=0, parse_dates=True)
        df = df.rename(columns={
            '시가': 'Open', '고가': 'High', '저가': 'Low',
            '종가': 'Close', '거래량': 'Volume'
        })

        target_str = target_date.strftime('%Y-%m-%d')
        df.index = pd.to_datetime(df.index).strftime('%Y-%m-%d')

        if target_str in df.index:
            close_price = df.loc[target_str, 'Close']
            return float(close_price)

        valid_dates = [d for d in df.index if d <= target_str]
        if valid_dates:
            closest_date = valid_dates[-1]
            close_price = df.loc[closest_date, 'Close']
            print(f"ℹ️ {symbol} 목표일 {target_str} → 이전 영업일 {closest_date} 사용")
            return float(close_price)

        print(f"⚠️ {symbol} 목표일 {target_str} 이전 데이터 없음")
        return None

    except Exception as e:
        print(f"⚠️ 종가 조회 실패: {symbol} ({market}) - {target_date.strftime('%Y-%m-%d')} - {e}")
        return None


def get_closes_in_range(symbol, market, base_date, target_date):
    """
    기준일(base_date) 다음날부터 목표일(target_date)까지의
    날짜별 종가를 DataFrame으로 반환
    반환: DataFrame with columns ['date', 'close'] (날짜 오름차순)
    """
    try:
        daily_path = get_daily_path(symbol, market)

        if not os.path.exists(daily_path):
            return pd.DataFrame(columns=['date', 'close'])

        df = pd.read_csv(daily_path, index_col=0, parse_dates=True)
        df = df.rename(columns={
            '시가': 'Open', '고가': 'High', '저가': 'Low',
            '종가': 'Close', '거래량': 'Volume'
        })

        df.index = pd.to_datetime(df.index)
        df = df.sort_index()

        base_str = base_date.strftime('%Y-%m-%d')
        target_str = target_date.strftime('%Y-%m-%d')
        df_range = df[(df.index > base_str) & (df.index <= target_str)][['Close']].copy()
        df_range = df_range.reset_index()
        df_range.columns = ['date', 'close']
        df_range['date'] = df_range['date'].dt.strftime('%Y-%m-%d')

        return df_range

    except Exception as e:
        print(f"⚠️ 구간 종가 조회 실패: {symbol} ({market}) - {e}")
        return pd.DataFrame(columns=['date', 'close'])


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


def run_screener(top_n=50, use_kr=True):
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

        # KR 계열 전체 필터 (KR + KOSPI + KOSDAQ)
        df_filtered = df[df['market'].isin(KR_MARKETS)].copy()
        print(f"KR 계열 필터 후: {len(df_filtered)}행 (KR/KOSPI/KOSDAQ 포함)")

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
        # 단기 스크리닝
        # ========================================
        print("\n단기 스크리닝 시작...")
        short_conditions = (
            # OBV 상승 크로스
            (df_filtered['obv_latest'] > df_filtered['signal_obv_9_latest']) &
            (df_filtered['obv_1ago'] <= df_filtered['signal_obv_9_1ago']) &
            # 거래대금 급증
            (df_filtered['today_trading_value'] >= 2.0 * df_filtered['avg_trading_value_20d']) &
            # 돌파
            ((df_filtered['break_20high'] == 1) |
             ((df_filtered['close_today'] > df_filtered['ma20_today']) &
              (df_filtered['close_yesterday'] <= df_filtered['ma20_yesterday'])))
        )

        short_results = df_filtered[short_conditions].copy()
        short_results = add_close_price(short_results)

        if not short_results.empty:
            short_results = short_results.sort_values('market_cap', ascending=False).head(top_n)
            short_results_save = short_results[save_columns].copy()
            short_file = os.path.join(SHORT_FOLDER, f'{today_str}_short.csv')
            short_results_save.to_csv(short_file, index=False, encoding='utf-8-sig')
            print(f"✅ 단기 결과: {len(short_results)}개 → {short_file}")
        else:
            print("⚠️ 단기 조건 만족 종목 없음")

        # ========================================
        # 중기 스크리닝
        # ========================================
        print("\n중기 스크리닝 시작...")
        mid_conditions = (
            # RSI 상승 (40~60)
            (df_filtered['rsi_d_2ago'] < df_filtered['rsi_d_1ago']) &
            (df_filtered['rsi_d_1ago'] < df_filtered['rsi_d_latest']) &
            (df_filtered['rsi_d_latest'] >= 40) &
            (df_filtered['rsi_d_latest'] <= 60) &
            # OBV 우상향/크로스
            (df_filtered['obv_latest'] > df_filtered['signal_obv_20_latest']) &
            (
                (df_filtered['signal_obv_20_latest'] > df_filtered['signal_obv_20_3ago']) |
                ((df_filtered['obv_2ago'] <= df_filtered['signal_obv_20_2ago']) &
                 (df_filtered['obv_latest'] > df_filtered['signal_obv_20_latest'])) |
                ((df_filtered['obv_1ago'] <= df_filtered['signal_obv_20_1ago']) &
                 (df_filtered['obv_latest'] > df_filtered['signal_obv_20_latest']))
            ) &
            # 골든크로스
            (df_filtered['ma50_today'] > df_filtered['ma200_today']) &
            # 거래대금
            (df_filtered['today_trading_value'] >= df_filtered['avg_trading_value_20d'])
        )

        mid_results = df_filtered[mid_conditions].copy()
        mid_results = add_close_price(mid_results)

        if not mid_results.empty:
            mid_results = mid_results.sort_values('market_cap', ascending=False).head(top_n)
            mid_results_save = mid_results[save_columns].copy()
            mid_file = os.path.join(MID_FOLDER, f'{today_str}_mid.csv')
            mid_results_save.to_csv(mid_file, index=False, encoding='utf-8-sig')
            print(f"✅ 중기 결과: {len(mid_results)}개 → {mid_file}")
        else:
            print("⚠️ 중기 조건 만족 종목 없음")

        # ========================================
        # 매도 스크리닝
        # ========================================
        print("\n매도 스크리닝 시작...")
        sell_conditions = (
            # RSI 과열
            (df_filtered['rsi_d_latest'] >= 70) |
            # OBV 하락 크로스
            ((df_filtered['obv_latest'] < df_filtered['signal_obv_9_latest']) &
             (df_filtered['obv_1ago'] >= df_filtered['signal_obv_9_1ago'])) |
            # RSI 하강
            ((df_filtered['rsi_d_2ago'] > df_filtered['rsi_d_1ago']) &
             (df_filtered['rsi_d_1ago'] > df_filtered['rsi_d_latest']) &
             (df_filtered['rsi_d_latest'] <= 50))
        )

        sell_results = df_filtered[sell_conditions].copy()
        sell_results = add_close_price(sell_results)

        if not sell_results.empty:
            sell_results = sell_results.sort_values('market_cap', ascending=False).head(top_n)
            sell_results_save = sell_results[save_columns].copy()
            sell_file = os.path.join(SELL_FOLDER, f'{today_str}_sell_signals.csv')
            sell_results_save.to_csv(sell_file, index=False, encoding='utf-8-sig')
            print(f"✅ 매도 결과: {len(sell_results)}개 → {sell_file}")
        else:
            print("⚠️ 매도 조건 만족 종목 없음")

        # 백테스팅 DB 생성
        create_backtest_db()

        # 테스트 탭용 데이터 생성
        create_backtest_test()

        return pd.DataFrame()

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

            # KR 계열 전체 필터 (KR + KOSPI + KOSDAQ)
            if 'market' in df.columns:
                df = df[df['market'].isin(KR_MARKETS)].copy()

            if df.empty:
                continue

            df['symbol'] = df['symbol'].str.zfill(6)
            all_df = pd.concat([all_df, df], ignore_index=True)
    return all_df


def create_backtest_db():
    print("\n" + "="*60)
    print("📊 백테스트 DB 생성 중...")
    print("="*60)

    short_df = load_all_csv_from_folder(SHORT_FOLDER, 'short')
    mid_df = load_all_csv_from_folder(MID_FOLDER, 'mid')

    print(f"\n📁 로드된 데이터:")
    print(f"   - 단기 CSV: {len(short_df)}행")
    print(f"   - 중기 CSV: {len(mid_df)}행")

    all_df = pd.concat([short_df, mid_df], ignore_index=True)

    if not all_df.empty and 'market' in all_df.columns and 'symbol' in all_df.columns:
        all_df['symbol'] = all_df['symbol'].astype(str)
        kr_mask = all_df['market'].isin(KR_MARKETS)
        all_df.loc[kr_mask, 'symbol'] = all_df.loc[kr_mask, 'symbol'].str.zfill(6)
        print(f"   ✅ 한국 종목 symbol 형식 통일 완료")

    if all_df.empty:
        print("⚠️ 백테스트할 데이터 없음")
        return

    print(f"   - 전체: {len(all_df)}행")

    # 기존 완료 데이터 로드 (중복 체크용)
    completed_csv_path = os.path.join(DATA_DIR, 'backtest_completed.csv')
    existing_completed_set = set()

    if os.path.exists(completed_csv_path):
        try:
            existing_completed = pd.read_csv(completed_csv_path, dtype={'symbol': str})
            kr_mask = existing_completed['market'].isin(KR_MARKETS)
            existing_completed.loc[kr_mask, 'symbol'] = existing_completed.loc[kr_mask, 'symbol'].str.zfill(6)
            for _, row in existing_completed.iterrows():
                key = f"{str(row['symbol'])}_{str(row['market'])}_{str(row['type'])}_{str(row['base_date'])}"
                existing_completed_set.add(key)
            print(f"\n📂 기존 완료 데이터: {len(existing_completed)}개")
            print(f"   (고유 키: {len(existing_completed_set)}개)")
        except Exception as e:
            print(f"⚠️ 기존 완료 데이터 로드 실패: {e}")
    else:
        print(f"\n📂 기존 완료 데이터: 없음 (신규 생성)")

    print(f"\n📋 샘플 데이터 (첫 3행):")
    for i, row in all_df.head(3).iterrows():
        print(f"   [{i}] {row.get('symbol', 'N/A')} | {row.get('market', 'N/A')} | cap_status: {row.get('cap_status', 'N/A')} | type: {row.get('type', 'N/A')}")

    meta = load_meta()
    today = datetime.now()
    print(f"\n📅 오늘 날짜: {today.strftime('%Y-%m-%d')}")

    pending_list = []
    completed_list = []
    skip_count = 0
    already_completed_count = 0

    for idx, row in all_df.iterrows():
        symbol = row['symbol']
        market = row['market']
        result_type = row['type']

        # 1. 기준일 파싱
        base_date_str = row.get('cap_status', 'N/A')
        try:
            base_date = datetime.strptime(base_date_str, '%Y-%m-%d')
        except Exception as e:
            print(f"⚠️ [{idx}] 날짜 파싱 실패: {symbol} ({market}) - cap_status: '{base_date_str}' - 에러: {e}")
            skip_count += 1
            continue

        # 2. 목표일 계산
        if result_type == 'short':
            target_date = base_date + timedelta(days=30)
        elif result_type == 'mid':
            target_date = base_date + timedelta(days=90)
        else:
            print(f"⚠️ [{idx}] 알 수 없는 타입: {symbol} ({market}) - type: '{result_type}'")
            skip_count += 1
            continue

        # 3. 완료 여부 확인
        days_elapsed = (today - base_date).days
        is_completed = today >= target_date

        # 4. symbol_key 생성
        symbol_key = str(symbol).zfill(6)
        check_key = f"{symbol_key}_{market}_{result_type}_{base_date_str}"

        if is_completed and check_key in existing_completed_set:
            already_completed_count += 1
            if already_completed_count <= 5:
                print(f"⏭️ [{idx}] {symbol_key} ({market}) {result_type} {base_date_str} - 이미 완료됨, 스킵")
            continue

        if idx < 5:
            print(f"\n🔍 [{idx}] {symbol_key} ({market}) - {result_type}")
            print(f"    기준일: {base_date.strftime('%Y-%m-%d')}")
            print(f"    목표일: {target_date.strftime('%Y-%m-%d')} ({'+30일' if result_type == 'short' else '+90일'})")
            print(f"    경과일: {days_elapsed}일")
            print(f"    완료여부: {'✅ 완료' if is_completed else '⏳ 대기 중'}")

        # 5. 메타 및 기준일 종가
        meta_dict = get_meta_info(meta, symbol_key, market)
        base_close = row.get('close', 0.0)

        if is_completed:
            target_close = get_historical_close(symbol_key, market, target_date)

            if target_close is None:
                print(f"⚠️ [{idx}] {symbol_key} - CSV에 목표일({target_date.strftime('%Y-%m-%d')}) 데이터 없음")
                print(f"    → 현재 종가로 대체 (부정확할 수 있음)")
                target_close = meta_dict.get('close', 0.0)

                if target_close == 0.0:
                    print(f"    → 메타에도 종가 없음, 대기 중으로 유지")
                    is_completed = False

            if is_completed:
                current_close = target_close
                current_update = target_date.strftime('%Y-%m-%d')

                if idx < 5:
                    print(f"    목표일 종가: {current_close:,.0f}")

        if not is_completed:
            current_close = meta_dict.get('close', 0.0)
            current_update = meta_dict.get('cap_status', 'N/A')

        # 6. 데이터 구성
        record = {
            'symbol': symbol_key,
            'market': market,
            'name': row.get('name', 'N/A'),
            'sector': row.get('sector', 'N/A'),
            'sector_trend': row.get('sector_trend', 'N/A'),
            'type': result_type,
            'base_date': base_date_str,
            'target_date': target_date.strftime('%Y-%m-%d'),
            'days_elapsed': days_elapsed,
            'close': base_close,
            'market_cap': row.get('market_cap', 0.0),
            'avg_trading_value_20d': row.get('avg_trading_value_20d', 0.0),
            'today_trading_value': row.get('today_trading_value', 0.0),
            'turnover': row.get('turnover', 0.0),
            'per': row.get('per', 0.0),
            'eps': row.get('eps', 0.0),
            'cap_status': base_date_str,
            'upper_closes': row.get('upper_closes', 0),
            'lower_closes': row.get('lower_closes', 0),
            'rsi_d': row.get('rsi_d', '[]'),
            'macd_d': row.get('macd_d', '[]'),
            'signal_d': row.get('signal_d', '[]'),
            'obv_d': row.get('obv_d', '[]'),
            'signal_obv_9d': row.get('signal_obv_9d', '[]'),
            'signal_obv_20d': row.get('signal_obv_20d', '[]'),
            'ma20': row.get('ma20', '[]'),
            'ma50': row.get('ma50', '[]'),
            'ma200': row.get('ma200', '[]'),
            'break_20high': row.get('break_20high', 0),
            'close_d': row.get('close_d', '[]'),
        }

        # 7. 완료 여부에 따라 분류
        if is_completed:
            record['latest_close'] = current_close
            record['latest_update'] = current_update
            change_rate = ((current_close - base_close) / base_close * 100) if base_close != 0 else 0.0
            record['change_rate'] = round(change_rate, 2)
            completed_list.append(record)
        else:
            record['latest_close'] = current_close
            record['latest_update'] = current_update
            change_rate = ((current_close - base_close) / base_close * 100) if base_close != 0 else 0.0
            record['change_rate'] = round(change_rate, 2)
            pending_list.append(record)

    # 8. 데이터프레임 생성
    pending_df = pd.DataFrame(pending_list)
    completed_df = pd.DataFrame(completed_list)

    print(f"\n" + "="*60)
    print(f"📊 분류 결과:")
    print(f"   - 스킵됨 (파싱 실패): {skip_count}개")
    print(f"   - 이미 완료됨 (중복 스킵): {already_completed_count}개")
    print(f"   - 대기 중: {len(pending_df)}개")
    print(f"   - 신규 완료: {len(completed_df)}개")
    print("="*60)

    # 9. DB 및 CSV 저장
    if not pending_df.empty:
        con_back = duckdb.connect(BACKTEST_DB_PATH)
        con_back.execute("DROP TABLE IF EXISTS backtest")
        con_back.execute("CREATE TABLE backtest AS SELECT * FROM pending_df")
        con_back.close()

        pending_df.to_csv(BACKTEST_CSV_PATH, index=False, encoding='utf-8-sig')
        print(f"\n✅ 백테스트 대기 중: {len(pending_df)}개 종목")
        print(f"   📄 {BACKTEST_CSV_PATH}")
    else:
        print("\n⚠️ 대기 중인 백테스트 종목 없음")

    if not completed_df.empty:
        if os.path.exists(completed_csv_path):
            existing_completed = pd.read_csv(completed_csv_path, dtype={'symbol': str})
            kr_mask = existing_completed['market'].isin(KR_MARKETS)
            existing_completed.loc[kr_mask, 'symbol'] = existing_completed.loc[kr_mask, 'symbol'].str.zfill(6)

            combined = pd.concat([existing_completed, completed_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['symbol', 'market', 'type', 'base_date'], keep='last')
            combined.to_csv(completed_csv_path, index=False, encoding='utf-8-sig', quoting=1)

            print(f"\n✅ 백테스트 완료: {len(completed_df)}개 종목 추가 (총 {len(combined)}개)")
            print(f"   📄 {completed_csv_path}")
        else:
            completed_df.to_csv(completed_csv_path, index=False, encoding='utf-8-sig', quoting=1)
            print(f"\n✅ 백테스트 완료: {len(completed_df)}개 종목 (신규)")
            print(f"   📄 {completed_csv_path}")

    # 10. 통계 출력
    print(f"\n" + "="*60)
    print(f"📊 백테스트 요약")
    print(f"   - 대기 중: {len(pending_df)}개")
    if not pending_df.empty:
        print(f"     · 단기(1개월): {len(pending_df[pending_df['type'] == 'short'])}개")
        print(f"     · 중기(3개월): {len(pending_df[pending_df['type'] == 'mid'])}개")
    print(f"   - 신규 완료: {len(completed_df)}개")
    if not completed_df.empty:
        print(f"     · 단기(1개월): {len(completed_df[completed_df['type'] == 'short'])}개")
        print(f"     · 중기(3개월): {len(completed_df[completed_df['type'] == 'mid'])}개")
        avg_return = completed_df['change_rate'].mean()
        win_rate = (completed_df['change_rate'] > 0).sum() / len(completed_df) * 100
        print(f"   - 평균 수익률: {avg_return:.2f}%")
        print(f"   - 승률: {win_rate:.1f}%")

    if os.path.exists(completed_csv_path):
        all_completed = pd.read_csv(completed_csv_path, dtype={'symbol': str})
        print(f"\n   📊 전체 완료 통계 (누적):")
        print(f"     · 총 완료: {len(all_completed)}개")
        print(f"     · 단기(1개월): {len(all_completed[all_completed['type'] == 'short'])}개")
        print(f"     · 중기(3개월): {len(all_completed[all_completed['type'] == 'mid'])}개")
        if len(all_completed) > 0:
            all_avg_return = all_completed['change_rate'].mean()
            all_win_rate = (all_completed['change_rate'] > 0).sum() / len(all_completed) * 100
            print(f"     · 평균 수익률: {all_avg_return:.2f}%")
            print(f"     · 승률: {all_win_rate:.1f}%")

    print("="*60 + "\n")


def create_backtest_test():
    """
    테스트 탭용 데이터 생성 → backtest_test.csv
    기준일로부터 +5%, +10% 첫 달성일과 최종일 종가를 기록
    """
    print("\n" + "="*60)
    print("🧪 테스트 탭 데이터 생성 중 (backtest_test.csv)...")
    print("="*60)

    short_df = load_all_csv_from_folder(SHORT_FOLDER, 'short')
    mid_df = load_all_csv_from_folder(MID_FOLDER, 'mid')
    all_df = pd.concat([short_df, mid_df], ignore_index=True)

    if not all_df.empty and 'market' in all_df.columns and 'symbol' in all_df.columns:
        all_df['symbol'] = all_df['symbol'].astype(str)
        kr_mask = all_df['market'].isin(KR_MARKETS)
        all_df.loc[kr_mask, 'symbol'] = all_df.loc[kr_mask, 'symbol'].str.zfill(6)

    if all_df.empty:
        print("⚠️ 테스트 탭 생성할 데이터 없음")
        return

    print(f"   - 전체 입력: {len(all_df)}행")

    # 기존 backtest_test.csv 로드 (중복 체크용)
    existing_test_set = set()
    existing_test_df = pd.DataFrame()

    if os.path.exists(BACKTEST_TEST_CSV_PATH):
        try:
            existing_test_df = pd.read_csv(BACKTEST_TEST_CSV_PATH, dtype={'symbol': str})
            kr_mask = existing_test_df['market'].isin(KR_MARKETS)
            existing_test_df.loc[kr_mask, 'symbol'] = existing_test_df.loc[kr_mask, 'symbol'].str.zfill(6)
            for _, row in existing_test_df.iterrows():
                key = f"{str(row['symbol'])}_{str(row['market'])}_{str(row['type'])}_{str(row['base_date'])}"
                existing_test_set.add(key)
            print(f"   - 기존 테스트 데이터: {len(existing_test_df)}개")
        except Exception as e:
            print(f"⚠️ 기존 테스트 데이터 로드 실패: {e}")
    else:
        print(f"   - 기존 테스트 데이터: 없음 (신규 생성)")

    today = datetime.now()
    new_records = []
    update_keys = []
    skip_count = 0

    for idx, row in all_df.iterrows():
        symbol = row['symbol']
        market = row['market']
        result_type = row['type']

        # 1. 기준일 파싱
        base_date_str = row.get('cap_status', 'N/A')
        try:
            base_date = datetime.strptime(base_date_str, '%Y-%m-%d')
        except Exception as e:
            print(f"⚠️ [{idx}] 날짜 파싱 실패: {symbol} ({market}) - '{base_date_str}' - {e}")
            skip_count += 1
            continue

        # 2. 목표일 계산
        if result_type == 'short':
            target_date = base_date + timedelta(days=30)
        elif result_type == 'mid':
            target_date = base_date + timedelta(days=90)
        else:
            skip_count += 1
            continue

        # 3. symbol_key 통일
        symbol_key = str(symbol).zfill(6)
        check_key = f"{symbol_key}_{market}_{result_type}_{base_date_str}"
        is_completed = today >= target_date

        # 이미 완료 처리된 항목 스킵
        if check_key in existing_test_set:
            if is_completed and not existing_test_df.empty:
                existing_row = existing_test_df[
                    (existing_test_df['symbol'] == symbol_key) &
                    (existing_test_df['market'] == market) &
                    (existing_test_df['type'] == result_type) &
                    (existing_test_df['base_date'] == base_date_str)
                ]
                if not existing_row.empty:
                    if int(existing_row.iloc[0].get('is_completed', 0)) == 1:
                        continue
                    else:
                        update_keys.append(check_key)
                else:
                    continue
            else:
                continue

        # 4. 기준가
        base_close = float(row.get('close', 0.0))
        if base_close == 0.0:
            skip_count += 1
            continue

        # 5. 기준일 이후 일별 종가 조회
        df_range = get_closes_in_range(symbol_key, market, base_date, target_date)

        # 6. +5%, +10% 달성일 탐색
        date_5pct = ''
        date_10pct = ''
        price_5pct = base_close * 1.05
        price_10pct = base_close * 1.10

        for _, price_row in df_range.iterrows():
            close_val = float(price_row['close'])
            date_val = str(price_row['date'])
            if date_5pct == '' and close_val >= price_5pct:
                date_5pct = date_val
            if date_10pct == '' and close_val >= price_10pct:
                date_10pct = date_val
            if date_5pct != '' and date_10pct != '':
                break

        # 7. 최종일 종가 (완료된 경우만)
        final_close = ''
        final_change_rate = ''

        if is_completed:
            fc = get_historical_close(symbol_key, market, target_date)
            if fc is not None and fc != 0.0:
                final_close = fc
                final_change_rate = round(((fc - base_close) / base_close) * 100, 2)

        # 8. 레코드 구성
        record = {
            'symbol': symbol_key,
            'market': market,
            'name': row.get('name', 'N/A'),
            'sector': row.get('sector', 'N/A'),
            'type': result_type,
            'base_date': base_date_str,
            'target_date': target_date.strftime('%Y-%m-%d'),
            'base_close': base_close,
            'date_5pct': date_5pct,
            'date_10pct': date_10pct,
            'final_close': final_close,
            'final_change_rate': final_change_rate,
            'is_completed': 1 if is_completed and final_close != '' else 0,
        }
        new_records.append(record)

    # 9. 기존 데이터 + 신규 데이터 병합 저장
    new_df = pd.DataFrame(new_records)

    if not new_df.empty or not existing_test_df.empty:
        if not existing_test_df.empty and update_keys:
            existing_test_df['_key'] = (
                existing_test_df['symbol'].astype(str) + '_' +
                existing_test_df['market'].astype(str) + '_' +
                existing_test_df['type'].astype(str) + '_' +
                existing_test_df['base_date'].astype(str)
            )
            existing_test_df = existing_test_df[~existing_test_df['_key'].isin(update_keys)]
            existing_test_df = existing_test_df.drop(columns=['_key'])

        combined = pd.concat([existing_test_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=['symbol', 'market', 'type', 'base_date'], keep='last'
        )
        combined.to_csv(BACKTEST_TEST_CSV_PATH, index=False, encoding='utf-8-sig', quoting=1)
        print(f"\n✅ 테스트 탭 저장 완료: {len(combined)}개 종목")
        print(f"   - 신규/업데이트: {len(new_df)}개")
        print(f"   - 완료됨: {len(combined[combined['is_completed'] == 1])}개")
        print(f"   - 대기 중: {len(combined[combined['is_completed'] == 0])}개")
        print(f"   📄 {BACKTEST_TEST_CSV_PATH}")

        done = combined[combined['is_completed'] == 1].copy()
        if len(done) > 0:
            done['final_change_rate'] = pd.to_numeric(done['final_change_rate'], errors='coerce')
            avg_r = done['final_change_rate'].mean()
            win_r = (done['final_change_rate'] > 0).sum() / len(done) * 100
            cnt_5 = (done['date_5pct'] != '').sum()
            cnt_10 = (done['date_10pct'] != '').sum()
            print(f"\n   📊 완료 통계:")
            print(f"     · 평균 수익률: {avg_r:.2f}%")
            print(f"     · 승률: {win_r:.1f}%")
            print(f"     · +5% 달성: {cnt_5}개 ({cnt_5/len(done)*100:.1f}%)")
            print(f"     · +10% 달성: {cnt_10}개 ({cnt_10/len(done)*100:.1f}%)")
    else:
        print("⚠️ 테스트 탭 저장할 데이터 없음")

    if skip_count > 0:
        print(f"   - 스킵됨: {skip_count}개")

    print("="*60 + "\n")


if __name__ == "__main__":
    use_kr = sys.argv[1].lower() == 'true' if len(sys.argv) > 1 else True
    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else 50

    print(f"스크리너 실행 → KR: {use_kr}, Top {top_n}개")
    results = run_screener(top_n, use_kr)

    if con:
        con.close()
    print("\n🎉 스크리너 종료!")