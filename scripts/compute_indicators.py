import pandas_ta as ta
import pandas as pd
import duckdb
import os
import json
from datetime import datetime
from multiprocessing import Pool
import time

DATA_DIR = os.getenv('DATA_DIR', './data')
os.makedirs(DATA_DIR, exist_ok=True)
META_DIR = os.path.join(DATA_DIR, 'meta')
os.makedirs(META_DIR, exist_ok=True)
DB_PATH = os.path.join(META_DIR, 'universe.db')
META_FILE = os.path.join(META_DIR, 'tickers_meta.json')

def load_meta():
    """메타 데이터 로드"""
    if os.path.exists(META_FILE):
        with open(META_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        print("메타 파일 없음 – fetch_data.py 먼저 실행하세요!")
        return {'KOSPI': {}, 'KOSDAQ': {}}

def compute_indicators_wrapper(args):
    symbol, market = args
    return compute_indicators(symbol, market)

def compute_indicators(symbol, market='KOSPI'):
    try:
        # market 기준으로 폴더 분기
        # KOSPI → kr_daily/kospi/
        # KOSDAQ → kr_daily/kosdaq/
        market_folder = market.lower()
        daily_path = os.path.join(DATA_DIR, 'kr_daily', market_folder, f"{symbol}.csv")

        if not os.path.exists(daily_path):
            print(f"{symbol} ({market}) 데이터 없음 – 스킵")
            return None

        df_daily = pd.read_csv(daily_path, index_col=0)

        # 한글 컬럼 영어로 변경
        df_daily = df_daily.rename(columns={
            '시가': 'Open', '고가': 'High', '저가': 'Low',
            '종가': 'Close', '거래량': 'Volume'
        })
        close_col = 'Close'
        vol_col = 'Volume'
        high_col = 'High'
        low_col = 'Low'

        # 기술적 지표 계산
        df_daily['RSI_D'] = ta.rsi(close=df_daily[close_col], length=14)
        macd_df = ta.macd(close=df_daily[close_col], fast=12, slow=26)
        df_daily['MACD'] = macd_df['MACD_12_26_9']
        df_daily['MACD_SIGNAL'] = macd_df['MACDs_12_26_9']
        df_daily['OBV'] = ta.obv(close=df_daily[close_col], volume=df_daily[vol_col])
        df_daily['OBV_SIGNAL_9'] = ta.sma(close=df_daily['OBV'], length=9)
        df_daily['OBV_SIGNAL_20'] = ta.sma(close=df_daily['OBV'], length=20)

        recent_d_rsi = df_daily['RSI_D'].tail(3).round(2).tolist()
        recent_macd = df_daily['MACD'].tail(3).round(4).tolist()
        recent_signal = df_daily['MACD_SIGNAL'].tail(3).round(4).tolist()
        recent_obv = df_daily['OBV'].tail(3).round(0).tolist()[::-1]
        recent_obv_signal_9 = df_daily['OBV_SIGNAL_9'].tail(3).round(0).tolist()[::-1]

        # signal_obv_20d 4일치
        recent_obv_signal_20 = df_daily['OBV_SIGNAL_20'].tail(4).round(0).tolist()[::-1]

        # MA 계산
        df_daily['MA20'] = ta.sma(close=df_daily[close_col], length=20)
        df_daily['MA50'] = ta.sma(close=df_daily[close_col], length=50)
        df_daily['MA200'] = ta.sma(close=df_daily[close_col], length=200)

        # 종가 3일치
        recent_close = df_daily[close_col].tail(3).round(2).tolist()[::-1]

        # MA 3일치
        recent_ma20 = df_daily['MA20'].tail(3).round(2).tolist()[::-1]
        recent_ma50 = df_daily['MA50'].tail(3).round(2).tolist()[::-1]
        recent_ma200 = df_daily['MA200'].tail(3).round(2).tolist()[::-1]

        # 20일 고가 돌파 여부 (오늘 제외, 이전 20일)
        if len(df_daily) >= 21:
            high_20d = df_daily[close_col].iloc[-21:-1].max()
            today_close = df_daily[close_col].iloc[-1]
            break_20high = 1 if today_close > high_20d else 0
        else:
            break_20high = 0

        # 메타에서 정보 가져오기 (KOSPI/KOSDAQ 키 기준)
        meta = load_meta()
        meta_dict = meta.get(market, {})
        name_val = meta_dict.get(symbol, {}).get('name', 'N/A')
        market_cap = meta_dict.get(symbol, {}).get('cap', 0.0)
        cap_status = meta_dict.get(symbol, {}).get('cap_status', "기존")
        per_val = meta_dict.get(symbol, {}).get('per', 0.0)
        eps_val = meta_dict.get(symbol, {}).get('eps', 0.0)
        sector_val = meta_dict.get(symbol, {}).get('sector', 'N/A')
        sector_trend_val = meta_dict.get(symbol, {}).get('sector_trend', 'N/A')

        df_daily['TradingValue'] = df_daily[close_col] * df_daily[vol_col]
        avg_20d = df_daily['TradingValue'].tail(20).mean()
        today_trading = df_daily['TradingValue'].iloc[-1]

        # 회전율: 억원 → 원 변환
        turnover = today_trading / (market_cap * 1e8) if market_cap > 0 else 0

        # 캔들 위치 계산 (최근 5일 상단/하단 마감)
        n = 5
        df_daily['candle_pos'] = (df_daily[close_col] - df_daily[low_col]) / (df_daily[high_col] - df_daily[low_col]).replace(0, float('nan'))
        upper_closes = (df_daily['candle_pos'].tail(n) > 0.7).sum()
        lower_closes = (df_daily['candle_pos'].tail(n) < 0.3).sum()

        # 25개 값 반환
        return (symbol, market, name_val,
                json.dumps(recent_d_rsi),
                json.dumps(recent_macd), json.dumps(recent_signal),
                json.dumps(recent_obv),
                json.dumps(recent_obv_signal_9),
                json.dumps(recent_obv_signal_20),
                float(market_cap), float(avg_20d), float(today_trading), float(turnover),
                float(per_val), float(eps_val), cap_status,
                int(upper_closes), int(lower_closes),
                sector_val, sector_trend_val,
                json.dumps(recent_ma20),
                json.dumps(recent_ma50),
                json.dumps(recent_ma200),
                int(break_20high),
                json.dumps(recent_close))

    except Exception as e:
        print(f"{symbol} ({market}) 에러: {e} – 스킵")
        return None

if __name__ == '__main__':
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

    con = None
    try:
        ensure_db_exists()
        con = duckdb.connect(DB_PATH)
    except duckdb.IOException:
        print("DB 파일 잠김 – 5초 대기 후 재시도")
        time.sleep(5)
        ensure_db_exists()
        con = duckdb.connect(DB_PATH)

    con.execute("DROP TABLE IF EXISTS indicators")
    con.execute("""
        CREATE TABLE indicators (
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

    meta = load_meta()

    # KOSPI + KOSDAQ 티커 각각 로드 (sector == 'ETF' 제외)
    kospi_tickers = [
        code for code, info in meta.get('KOSPI', {}).items()
        if info.get('sector', 'N/A') != 'ETF'
    ]
    kosdaq_tickers = [
        code for code, info in meta.get('KOSDAQ', {}).items()
        if info.get('sector', 'N/A') != 'ETF'
    ]

    # 구버전 KR 키 호환 처리
    kr_tickers = [
        code for code, info in meta.get('KR', {}).items()
        if info.get('sector', 'N/A') != 'ETF'
    ]
    if kr_tickers:
        print(f"⚠️ 구버전 KR 키 감지: {len(kr_tickers)}개 → KOSPI로 처리")

    print(f"KOSPI: {len(kospi_tickers)}개 / KOSDAQ: {len(kosdaq_tickers)}개 로드 (ETF 제외)")

    num_processes = 4
    print(f"멀티프로세싱 시작: {num_processes} 프로세스 사용")

    # KOSPI + KOSDAQ + 구버전 KR(KOSPI 폴더로 처리) args 생성
    all_args = (
        [(ticker, 'KOSPI') for ticker in kospi_tickers] +
        [(ticker, 'KOSDAQ') for ticker in kosdaq_tickers] +
        [(ticker, 'KOSPI') for ticker in kr_tickers]  # 구버전 호환
    )

    with Pool(num_processes) as pool:
        all_results_raw = pool.map(compute_indicators_wrapper, all_args)

    all_results = [r for r in all_results_raw if r is not None]
    print(f"유효 결과: {len(all_results)}개")

    for row in all_results:
        con.execute(
            "INSERT OR REPLACE INTO indicators VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row
        )

    total = con.execute("SELECT COUNT(*) FROM indicators").fetchone()[0]
    kospi_count = con.execute("SELECT COUNT(*) FROM indicators WHERE market = 'KOSPI'").fetchone()[0]
    kosdaq_count = con.execute("SELECT COUNT(*) FROM indicators WHERE market = 'KOSDAQ'").fetchone()[0]
    print(f"전체 완료! KOSPI: {kospi_count}개 / KOSDAQ: {kosdaq_count}개 / 합계: {total}개")

    # CSV 내보내기 (확인용)
    df = con.execute("SELECT * FROM indicators").fetchdf()
    csv_path = os.path.join(DATA_DIR, "indicators.csv")
    df.to_csv(csv_path, encoding='utf-8-sig', index=False)
    print(f"CSV 저장 완료: {csv_path}")

    con.close()