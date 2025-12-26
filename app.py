import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import json
import subprocess
import pandas_ta as ta
from pykrx import stock
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
import warnings

def get_sector_trend_color(trend_text):
    """
    섹터트렌드 텍스트에서 퍼센트를 추출하여 배경색 반환
    
    Args:
        trend_text: "섹터트렌드: 상승(+4.01%) TIGER 200 금융" 형식의 텍스트
    
    Returns:
        배경색 RGB 값 또는 None
    """
    import re
    
    if not trend_text or pd.isna(trend_text):
        return None
    
    # 퍼센트 값 추출 (예: "+4.01%" 또는 "-2.50%")
    match = re.search(r'([+-]?\d+\.?\d*)%', str(trend_text))
    
    if not match:
        return None
    
    try:
        percent = float(match.group(1))
    except:
        return None
    
    # 색상 단계 정의 (±15% 기준, 3%씩 5단계)
    # 빨간색 계열 (플러스)
    if percent >= 12:  # 12% ~ 15%+
        return "rgba(220, 38, 38, 0.30)"  # 가장 진한 빨간색 (#dc2626)
    elif percent >= 9:  # 9% ~ 12%
        return "rgba(220, 38, 38, 0.25)"
    elif percent >= 6:  # 6% ~ 9%
        return "rgba(239, 68, 68, 0.2)"  # 진한 빨간색 (#ef4444)
    elif percent >= 3:  # 3% ~ 6%
        return "rgba(248, 113, 113, 0.15)"  # 중간 빨간색 (#f87171)
    elif percent > 0:  # 0% ~ 3%
        return "rgba(252, 165, 165, 0.1)"  # 연한 빨간색 (#fca5a5)
    
    # 파란색 계열 (마이너스)
    elif percent <= -12:  # -15% ~ -12%
        return "rgba(37, 99, 235, 0.30)"  # 가장 진한 파란색 (#2563eb)
    elif percent <= -9:  # -12% ~ -9%
        return "rgba(37, 99, 235, 0.25)"
    elif percent <= -6:  # -9% ~ -6%
        return "rgba(59, 130, 246, 0.20)"  # 진한 파란색 (#3b82f6)
    elif percent <= -3:  # -6% ~ -3%
        return "rgba(96, 165, 250, 0.15)"  # 중간 파란색 (#60a5fa)
    elif percent < 0:  # -3% ~ 0%
        return "rgba(147, 197, 253, 0.1)"  # 연한 파란색 (#93c5fd)
    
    # 0%
    else:
        return None


# 캐시 클리어
st.cache_data.clear()
st.cache_resource.clear()

# 페이지 설정
st.set_page_config(page_title="Trading Copilot 🚀", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    /* 전체 폰트 크기 축소 */
    html, body, [class*="css"] {
        font-size: 13px !important;
    }
    
    /* 전체 배경 - 불투명 */
    .main {
        background: var(--background-color) !important;
    }
    
    /* 사이드바 - 불투명 배경 (JavaScript로 강제 적용) */
    [data-testid="stSidebar"] {
        border-right: 1px solid rgba(128,128,128,.2) !important;
        overflow-y: auto !important;
    }

    /* 사이드바 텍스트 명확하게 */
    [data-testid="stSidebar"] * {
        color: var(--text-color) !important;
    }

    [data-testid="stSidebar"] label {
        color: var(--text-color) !important;
        opacity: 1 !important;
    }

    [data-testid="stSidebar"] p {
        color: var(--text-color) !important;
    }

    [data-testid="stSidebar"] h2, 
    [data-testid="stSidebar"] h3, 
    [data-testid="stSidebar"] h4 {
        color: var(--text-color) !important;
    }

    /* 사이드바 버튼 텍스트 */
    [data-testid="stSidebar"] .stButton button {
        color: var(--text-color) !important;
    }

    /* 사이드바 체크박스, 라디오 텍스트 */
    [data-testid="stSidebar"] .stCheckbox label,
    [data-testid="stSidebar"] .stRadio label {
        color: var(--text-color) !important;
        font-weight: 500 !important;
    }

    /* 비활성화된 버튼 시각화 강화 */
    [data-testid="stSidebar"] .stButton button:disabled {
        opacity: 0.4 !important;
        background: rgba(128, 128, 128, 0.1) !important;
        border: 1px dashed rgba(128, 128, 128, 0.3) !important;
        cursor: not-allowed !important;
        color: rgba(128, 128, 128, 0.5) !important;
    }
    
    [data-testid="stSidebar"] .stButton button:disabled:hover {
        background: rgba(128, 128, 128, 0.15) !important;
        transform: none !important;
    }            
            
    /* 사이드바 selectbox 텍스트 */
    [data-testid="stSidebar"] .stSelectbox label {
        color: var(--text-color) !important;
        font-weight: 600 !important;
    }
    
    /* 사이드바 모든 요소 간격 대폭 축소 */
    [data-testid="stSidebar"] .element-container {
        margin: 0 !important;
        padding: 0 !important;
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
        margin: 0 !important;
        padding: 0 !important;
    }
    
    [data-testid="stSidebar"] .stCheckbox {
        margin: 0 !important;
        padding: 0px 8px !important;
        border: none !important;
        background: transparent !important;
    }
    
    [data-testid="stSidebar"] .stSelectbox {
        margin: 0.05rem 0 !important;
    }
    
    [data-testid="stSidebar"] .stRadio {
        margin: 0.05rem 0 !important;
    }
    
    [data-testid="stSidebar"] hr {
        margin: 0.1rem 0 !important;
    }
    
    /* 사이드바 체크박스 라벨 중앙 정렬 */
    [data-testid="stSidebar"] .stCheckbox label {
        display: flex !important;
        align-items: center !important;
        padding: 0px 0 !important;
        margin: 0 !important;
    }
    
    [data-testid="stSidebar"] .stCheckbox > div {
        padding: 0 !important;
        margin: 0 !important;
    }
    
    /* 체크박스 간격 더 좁히기 */
    [data-testid="stSidebar"] .stCheckbox + .stCheckbox {
        margin-top: -10px !important;
    }
    
    /* 사이드바 버튼 텍스트 중앙 정렬 */
    [data-testid="stSidebar"] .stButton button {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }
    
    /* 익스팬더 내부 간격 축소 */
    .streamlit-expander {
        margin: 0 !important;
        padding: 0 !important;
    }
    
    .streamlit-expanderHeader {
        padding: 2px 8px !important;
        margin: 0 !important;
        font-size: 0.85rem !important;
    }
    
    .streamlit-expanderContent {
        padding: 2px 8px !important;
        margin: 0 !important;
    }
    
    .streamlit-expanderContent .stCheckbox {
        margin-top: -5px !important;
    }
    
    /* 헤더 스타일 */
    h1 {
        font-weight: 800;
        letter-spacing: 0.2px;
        font-size: 1.8rem !important;
    }
    
    h2 {
        font-weight: 800;
        letter-spacing: 0.2px;
        font-size: 1.4rem !important;
    }
    
    h3 {
        font-weight: 800;
        letter-spacing: 0.2px;
        font-size: 1.1rem !important;
    }
    
    h4 {
        font-weight: 700;
        letter-spacing: 0.2px;
        font-size: 0.95rem !important;
        margin-bottom: 0.4rem !important;
    }
    
    /* 메트릭 카드 스타일 */
    [data-testid="stMetricValue"] {
        font-size: 1.3rem !important;
        font-weight: 1000;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem !important;
    }
    
    /* 버튼 스타일 */
    .stButton>button {
        border-radius: 12px;
        font-weight: 900;
        font-size: 0.85rem !important;
        transition: 0.15s ease;
        padding: 0.35rem 0.7rem;
    }

    /* Primary 버튼 색상 연하게 (사이드바용) */
    [data-testid="stSidebar"] .stButton button[kind="primary"] {
        background-color: rgba(239, 68, 68, 0.7) !important;  /* 빨간색 50% 투명도 */
        border-color: rgba(239, 68, 68, 0.5) !important;
    }

    [data-testid="stSidebar"] .stButton button[kind="primary"]:hover {
        background-color: rgba(239, 68, 68, 0.8) !important;  /* 호버 시 70% */
        border-color: rgba(239, 68, 68, 0.7) !important;
    }

    [data-testid="stSidebar"] .stButton button[kind="primary"]:active {
        background-color: rgba(239, 68, 68, 0.9) !important;  /* 클릭 시 90% */
        border-color: rgba(239, 68, 68, 0.9) !important;
    }
    
    /* 체크박스 스타일 */
    .stCheckbox {
        padding: 5px 8px;
        border-radius: 12px;
        margin-bottom: 0.2rem;
        font-size: 0.85rem !important;
    }
    
    /* 셀렉트박스 스타일 */
    .stSelectbox>div>div {
        border-radius: 12px;
        font-size: 0.85rem !important;
    }
    
    /* 라디오 버튼 스타일 */
    .stRadio > div {
        gap: 0.2rem !important;
    }
    
    /* 데이터프레임 스타일 */
    [data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
        font-size: 0.8rem !important;
    }
    
    /* 탭 스타일 */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 999px;
        padding: 6px 10px;
        font-weight: 1000;
        font-size: 0.8rem !important;
    }
    
    /* 입력 필드 */
    .stTextInput>div>div>input {
        border-radius: 12px;
        font-size: 0.85rem !important;
    }
    
    /* 정보 박스 */
    .stInfo {
        border-radius: 14px;
        font-size: 0.85rem !important;
    }
    
    /* 경고 박스 */
    .stWarning {
        border-radius: 14px;
        font-size: 0.85rem !important;
    }
    
    /* 익스팬더 */
    .streamlit-expanderHeader {
        font-size: 0.85rem !important;
    }
    
    /* 구분선 간격 축소 */
    hr {
        margin: 0.5rem 0 !important;
    }

    /* 비활성화된 체크박스 시각화 강화 */
    [data-testid="stSidebar"] .stCheckbox:has(input:disabled) {
        opacity: 0.4 !important;
        background: rgba(128, 128, 128, 0.1) !important;
        border: 1px dashed rgba(128, 128, 128, 0.3) !important;
        pointer-events: none !important;
    }
    
    [data-testid="stSidebar"] .stCheckbox:has(input:disabled) label {
        cursor: not-allowed !important;
        color: rgba(128, 128, 128, 0.5) !important;
    }
    
    /* 비활성화된 체크박스 호버 시 */
    [data-testid="stSidebar"] .stCheckbox:has(input:disabled):hover {
        background: rgba(128, 128, 128, 0.15) !important;
    }
    
    /* 비활성화 안내 메시지 스타일 */
    .filter-disabled-notice {
        background: rgba(255, 193, 7, 0.1) !important;
        border-left: 3px solid #ffc107 !important;
        padding: 8px 12px !important;
        border-radius: 8px !important;
        margin: 8px 0 !important;
        font-size: 0.75rem !important;
        color: var(--text-color) !important;
    }
            
</style>

<script>
// 사이드바 배경을 완전 불투명하게 강제 설정
(function() {
    function fixSidebarBackground() {
        const sidebar = document.querySelector('[data-testid="stSidebar"]');
        if (!sidebar) {
            setTimeout(fixSidebarBackground, 100);
            return;
        }
        
        const root = document.documentElement;
        const bgColor = getComputedStyle(root).getPropertyValue('--secondary-background-color').trim();
        
        // rgba를 rgb로 변환 (투명도 제거)
        const match = bgColor.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
        if (match) {
            const opaqueColor = `rgb(${match[1]}, ${match[2]}, ${match[3]})`;
            sidebar.style.backgroundColor = opaqueColor;
        } else {
            // 폴백: 직접 값 설정
            const isDark = getComputedStyle(root).getPropertyValue('--text-color').includes('250');
            sidebar.style.backgroundColor = isDark ? '#0e1117' : '#ffffff';
        }
    }
    
    // 초기 실행
    fixSidebarBackground();
    
    // 테마 변경 감지
    const observer = new MutationObserver(fixSidebarBackground);
    observer.observe(document.documentElement, { 
        attributes: true, 
        attributeFilter: ['data-theme', 'class', 'style'] 
    });
    
    // 페이지 로드 후에도 재확인
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', fixSidebarBackground);
    }
})();
</script>
""", unsafe_allow_html=True)

warnings.filterwarnings("ignore", message=".*keyword arguments.*deprecated.*config.*")
warnings.filterwarnings("ignore", category=FutureWarning, message=".*to_pydatetime.*")
warnings.filterwarnings("ignore", category=UserWarning, module="pykrx")

@st.cache_data
def load_data():
    DB_PATH = "data/meta/universe.db"
    if not os.path.exists(DB_PATH):
        st.warning("데이터 없음 – 배치 실행하세요.")
        return pd.DataFrame()
    con = duckdb.connect(DB_PATH, read_only=True)
    df_ind = con.execute("SELECT * FROM indicators").fetchdf()
    con.close()
    return df_ind

def get_db_connection():
    DB_PATH = "data/meta/universe.db"
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data(ttl=3600)
def add_names(df):
    if df.empty or 'symbol' not in df.columns:
        return df
    con = get_db_connection()
    try:
        symbols = df['symbol'].tolist()
        query = f"SELECT symbol, name FROM indicators WHERE symbol IN ({','.join(['?'] * len(symbols))})"
        name_df = con.execute(query, symbols).fetchdf()
        name_dict = dict(zip(name_df['symbol'], name_df['name']))
        df = df.copy()
        df['name'] = df['symbol'].map(name_dict).fillna('N/A')
        return df
    except Exception as e:
        st.warning(f"이름 로드 에러: {e} – 기본값 사용")
        df = df.copy()
        df['name'] = 'N/A'
        return df
    finally:
        con.close()

@st.cache_data
def load_meta():
    META_FILE = "data/meta/tickers_meta.json"
    if os.path.exists(META_FILE):
        with open(META_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'KR': {}, 'US': {}}

@st.cache_data(ttl=3600)
def add_foreign_net_buy(df):
    if 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()
    df['foreign_net_buy_3ago'] = np.nan
    df['foreign_net_buy_2ago'] = np.nan
    df['foreign_net_buy_1ago'] = np.nan
    if df.empty:
        return df
    for idx, row in df.iterrows():
        symbol = row['symbol']
        market = row['market']
        meta_dict = meta.get(market, {}).get(symbol, {})
        fnb = meta_dict.get('foreign_net_buy', [0, 0, 0])
        df.at[idx, 'foreign_net_buy_3ago'] = fnb[2] if len(fnb) > 2 else 0
        df.at[idx, 'foreign_net_buy_2ago'] = fnb[1] if len(fnb) > 1 else 0
        df.at[idx, 'foreign_net_buy_1ago'] = fnb[0] if len(fnb) > 0 else 0
    return df

@st.cache_data(ttl=3600)
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

def run_screener_query(con, filter_condition="all", use_us=True, use_kr=True, top_n=None, additional_filter=None):
    try:
        con.execute("SELECT 1").fetchone()
    except:
        con = get_db_connection()
        st.session_state.con = con
    
    market_filter = "market = 'US'" if use_us and not use_kr else "market = 'KR'" if use_kr and not use_us else "market IN ('US', 'KR')"
    
    if filter_condition == "obv":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago)"
    elif filter_condition == "rsi_up":
        condition = "(rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest) AND rsi_d_latest <= 50"
    elif filter_condition == "rsi_down":
        condition = "(rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest) AND rsi_d_latest <= 50"
    elif filter_condition == "trading_volume":
        condition = "today_trading_value > 1.5 * avg_trading_value_20d"
    elif filter_condition == "all":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AND (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50)"
    elif filter_condition == "eps_per_only":
        condition = "1=1"
    elif filter_condition == "short_term":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AND (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50) AND (today_trading_value > 1.5 * avg_trading_value_20d)"
    elif filter_condition == "mid_term":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AND (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50)"
    elif filter_condition == "long_term":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AND (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50)"
    
    liquidity = """
    AND market_cap >= CASE WHEN market = 'US' THEN 2000000000.0 ELSE 200000000000.0 END
    """
    
    additional_condition = ""
    if additional_filter == "eps_per":
        additional_condition = " AND eps > 0 AND per >= 3 AND per <= 30"
    
    query = f"""
    WITH parsed AS (
        SELECT symbol, market,
            rsi_d, macd_d, signal_d, obv_d, signal_obv_d, market_cap, avg_trading_value_20d, today_trading_value, turnover,
            per, eps, cap_status, upper_closes, lower_closes, sector, sector_trend,
            CAST(json_extract(rsi_d, '$[0]') AS DOUBLE) AS rsi_d_2ago,
            CAST(json_extract(rsi_d, '$[1]') AS DOUBLE) AS rsi_d_1ago,
            CAST(json_extract(rsi_d, '$[2]') AS DOUBLE) AS rsi_d_latest,
            CAST(json_extract(macd_d, '$[2]') AS DOUBLE) AS macd_latest,
            CAST(json_extract(signal_d, '$[2]') AS DOUBLE) AS signal_latest,
            CAST(json_extract(obv_d, '$[1]') AS DOUBLE) AS obv_1ago,
            CAST(json_extract(obv_d, '$[0]') AS DOUBLE) AS obv_latest,
            CAST(json_extract(signal_obv_d, '$[1]') AS DOUBLE) AS signal_obv_1ago,
            CAST(json_extract(signal_obv_d, '$[0]') AS DOUBLE) AS signal_obv_latest
        FROM indicators
    )
    SELECT symbol, market,
        rsi_d AS rsi_d_array,
        macd_d AS macd_array,
        signal_d AS signal_array,
        obv_d AS obv_array,
        signal_obv_d AS signal_obv_array,
        market_cap, avg_trading_value_20d, today_trading_value, turnover,
        per, eps, cap_status, upper_closes, lower_closes, sector, sector_trend,
        rsi_d_2ago, rsi_d_1ago, rsi_d_latest,
        macd_latest, signal_latest,
        obv_latest, signal_obv_latest,
        obv_1ago, signal_obv_1ago,
        (obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AS obv_bullish_cross,
        (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3up,
        (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3down,
        (today_trading_value > 1.5 * avg_trading_value_20d) AS trading_high
    FROM parsed
    WHERE {market_filter}
      AND {condition}
      {liquidity}
      {additional_condition}
    ORDER BY rsi_d_latest ASC
    """
    df = con.execute(query).fetchdf()
    if top_n:
        df = df.head(top_n)
    return df

def format_dataframe(df, market_type):
    if market_type == 'KR':
        df = df.rename(columns={
            '시가총액': '시가총액 (KRW 억원)',
            '20일평균거래대금': '20일평균거래대금 (KRW 억원)',
            '오늘거래대금': '오늘거래대금 (KRW 억원)',
            '회전율': '회전율 (%)',
            'PER_TTM': 'PER_TTM (x)',
            '종가': '종가 (KRW)',
            '외국인순매수_3일전': '외국인순매수_3일전 (주)',
            '외국인순매수_2일전': '외국인순매수_2일전 (주)',
            '외국인순매수_1일전': '외국인순매수_1일전 (주)',
            'sector': '섹터',
            'sector_trend': '섹터트렌드',
        })
    elif market_type == 'US':
        df = df.rename(columns={
            '시가총액': '시가총액 (USD M)',
            '20일평균거래대금': '20일평균거래대금 (USD M)',
            '오늘거래대금': '오늘거래대금 (USD M)',
            '회전율': '회전율 (%)',
            'PER_TTM': 'PER_TTM (x)',
            '종가': '종가 (USD)',
            '외국인순매수_3일전': '외국인순매수_3일전 (N/A)',
            '외국인순매수_2일전': '외국인순매수_2일전 (N/A)',
            '외국인순매수_1일전': '외국인순매수_1일전 (N/A)',
            'sector': '섹터',
            'sector_trend': '섹터트렌드',
        })

    def safe_float(x):
        return float(x) if pd.notna(x) else 0.0

    if '시가총액 (KRW 억원)' in df.columns or '시가총액 (USD M)' in df.columns:
        col_name = df.columns[df.columns.str.startswith('시가총액 (')][0]
        df[col_name] = df[col_name].apply(safe_float)
        if market_type == 'KR':
            df[col_name] = df[col_name] / 1e8
        else:
            df[col_name] = df[col_name] / 1e6

    if '20일평균거래대금 (KRW 억원)' in df.columns or '20일평균거래대금 (USD M)' in df.columns:
        col_name = df.columns[df.columns.str.startswith('20일평균거래대금 (')][0]
        df[col_name] = df[col_name].apply(safe_float)
        if market_type == 'KR':
            df[col_name] = df[col_name] / 1e8
        else:
            df[col_name] = df[col_name] / 1e6

    if '오늘거래대금 (KRW 억원)' in df.columns or '오늘거래대금 (USD M)' in df.columns:
        col_name = df.columns[df.columns.str.startswith('오늘거래대금 (')][0]
        df[col_name] = df[col_name].apply(safe_float)
        if market_type == 'KR':
            df[col_name] = df[col_name] / 1e8
        else:
            df[col_name] = df[col_name] / 1e6

    if '회전율 (%)' in df.columns:
        df['회전율 (%)'] = df['회전율 (%)'].apply(safe_float) * 100

    if 'PER_TTM (x)' in df.columns:
        df['PER_TTM (x)'] = df['PER_TTM (x)'].apply(safe_float)

    if 'EPS_TTM' in df.columns:
        df['EPS_TTM'] = df['EPS_TTM'].apply(safe_float)

    if 'RSI_3일_2ago' in df.columns:
        df['RSI_3일_2ago'] = df['RSI_3일_2ago'].apply(safe_float)

    if 'RSI_3일_1ago' in df.columns:
        df['RSI_3일_1ago'] = df['RSI_3일_1ago'].apply(safe_float)

    if 'RSI_3일_latest' in df.columns:
        df['RSI_3일_latest'] = df['RSI_3일_latest'].apply(safe_float)

    if '종가 (KRW)' in df.columns or '종가 (USD)' in df.columns:
        col_name = df.columns[df.columns.str.startswith('종가 (')][0]
        df[col_name] = df[col_name].apply(safe_float)

    foreign_cols = [col for col in df.columns if col.startswith('외국인순매수_')]
    for col in foreign_cols:
        df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else 0)

    def bool_fmt(x):
        return '✅' if x else '❌'

    bool_cols = ['OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(bool_fmt)

    numeric_cols = df.select_dtypes(include='float').columns
    numeric_cols = numeric_cols.drop('회전율 (%)', errors='ignore')
    df[numeric_cols] = df[numeric_cols].round(2)

    return df

def show_chart(symbol, market, chart_type):
    """차트 표시 함수"""
    base_dir = "data"
    daily_path = os.path.join(base_dir, ('us_daily' if market == 'US' else 'kr_daily'), f"{symbol}.csv")
    
    if not os.path.exists(daily_path):
        st.warning("데이터 없음")
        return
    
    df_chart = pd.read_csv(daily_path, index_col=0)
    if market == 'KR':
        df_chart = df_chart.rename(columns={'시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'})
    
    close_col = 'Close'
    vol_col = 'Volume'
    
    if close_col in df_chart.columns:
        df_chart[close_col] = df_chart[close_col].round(2)
    
    if chart_type == "종가":
        fig = px.line(df_chart, x=df_chart.index, y=close_col, title=f"{symbol} Close")
        fig.update_traces(name='Close', showlegend=True, line=dict(color='#2563eb', width=2))
        fig.update_layout(
            height=350,
            template="plotly"  # 자동 테마 적용
        )
        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False}, theme="streamlit")
        
    elif chart_type == "MACD":
        macd_df = ta.macd(df_chart[close_col], fast=12, slow=26)
        macd = macd_df['MACD_12_26_9']
        signal = macd_df['MACDs_12_26_9']
        hist = macd_df['MACDh_12_26_9']
        df_macd = pd.DataFrame({'Date': df_chart.index, 'MACD': macd, 'Signal': signal, 'Hist': hist}).dropna()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_macd['Date'], y=df_macd['MACD'], name='MACD', line=dict(color='#2563eb', width=2)))
        fig.add_trace(go.Scatter(x=df_macd['Date'], y=df_macd['Signal'], name='Signal', line=dict(color='#dc2626', width=2)))
        fig.add_trace(go.Bar(x=df_macd['Date'], y=df_macd['Hist'], name='Histogram', marker_color='#059669'))
        fig.update_layout(
            height=350,
            title="MACD",
            template="plotly"  # 자동 테마 적용
        )
        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False}, theme="streamlit")
        
    elif chart_type == "OBV":
        obv = ta.obv(df_chart[close_col], df_chart[vol_col])
        obv_signal = ta.sma(obv, length=9)
        df_obv = pd.DataFrame({'Date': df_chart.index, 'OBV': obv, 'OBV_SIGNAL': obv_signal}).dropna()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_obv['Date'], y=df_obv['OBV'], name='OBV', line=dict(color='#059669', width=2)))
        fig.add_trace(go.Scatter(x=df_obv['Date'], y=df_obv['OBV_SIGNAL'], name='OBV Signal', line=dict(color='#f59e0b', width=2)))
        fig.update_layout(
            height=350,
            title="OBV",
            template="plotly"  # 자동 테마 적용
        )
        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False}, theme="streamlit")
        
    elif chart_type == "RSI":
        rsi = ta.rsi(df_chart[close_col], length=14)
        df_rsi = pd.DataFrame({'Date': df_chart.index, 'RSI': rsi}).dropna()
        
        fig = px.line(df_rsi, x='Date', y='RSI', title="RSI")
        fig.add_hline(y=30, line_dash="dot", line_color="#dc2626", annotation_text="OverSold (30)", annotation_position="bottom right")
        fig.add_hline(y=70, line_dash="dot", line_color="#dc2626", annotation_text="OverBought (70)", annotation_position="top right")
        fig.update_traces(name='RSI', showlegend=True, line=dict(color='#8b5cf6', width=2))
        fig.update_layout(
            height=350,
            template="plotly"  # 자동 테마 적용
        )
        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False}, theme="streamlit")

def get_indicator_data(symbol, market):
    con = get_db_connection()
    query = """
    WITH parsed AS (
        SELECT 
            rsi_d, macd_d, signal_d, obv_d, signal_obv_d, market_cap, avg_trading_value_20d, today_trading_value, turnover,
            per, eps, cap_status, upper_closes, lower_closes, sector, sector_trend,
            CAST(json_extract(rsi_d, '$[0]') AS DOUBLE) AS rsi_d_2ago,
            CAST(json_extract(rsi_d, '$[1]') AS DOUBLE) AS rsi_d_1ago,
            CAST(json_extract(rsi_d, '$[2]') AS DOUBLE) AS rsi_d_latest,
            CAST(json_extract(macd_d, '$[2]') AS DOUBLE) AS macd_latest,
            CAST(json_extract(signal_d, '$[2]') AS DOUBLE) AS signal_latest,
            CAST(json_extract(obv_d, '$[1]') AS DOUBLE) AS obv_1ago,
            CAST(json_extract(obv_d, '$[0]') AS DOUBLE) AS obv_latest,
            CAST(json_extract(signal_obv_d, '$[1]') AS DOUBLE) AS signal_obv_1ago,
            CAST(json_extract(signal_obv_d, '$[0]') AS DOUBLE) AS signal_obv_latest
        FROM indicators
        WHERE symbol = ? AND market = ?
    )
    SELECT 
        rsi_d_2ago, rsi_d_1ago, rsi_d_latest,
        market_cap, avg_trading_value_20d, today_trading_value, turnover,
        per, eps, upper_closes, lower_closes, sector, sector_trend,
        (obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AS obv_bullish_cross,
        (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3up,
        (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3down,
        (today_trading_value > 1.5 * avg_trading_value_20d) AS trading_high
    FROM parsed
    """
    df = con.execute(query, [symbol, market]).fetchdf()
    con.close()
    if not df.empty:
        return df.iloc[0]
    return None

# 세션 상태 초기화
if 'selected_symbol' not in st.session_state:
    st.session_state.selected_symbol = None
if 'selected_market' not in st.session_state:
    st.session_state.selected_market = None
if 'con' not in st.session_state:
    st.session_state.con = None
if 'filter_results' not in st.session_state:
    st.session_state.filter_results = pd.DataFrame()
if 'last_selected' not in st.session_state:
    st.session_state.last_selected = None
if 'kr_editor_state' not in st.session_state:
    st.session_state.kr_editor_state = None
if 'us_editor_state' not in st.session_state:
    st.session_state.us_editor_state = None

# 초기화 플래그 추가
if 'reset_filters' not in st.session_state:
    st.session_state.reset_filters = False

# 필터 체크박스 상태 초기화 (위젯 생성 전에 처리)
if st.session_state.reset_filters:
    # 위젯이 생성되기 전에 key 값 삭제
    for key in ['obv', 'rsi_up', 'eps_per', 'trading', 'foreign', 'candle', 'sector']:
        if key in st.session_state:
            del st.session_state[key]
    st.session_state.reset_filters = False  # 플래그 리셋

# 기본값 설정 (삭제된 경우에만 적용됨)
if 'obv' not in st.session_state:
    st.session_state.obv = False
if 'rsi_up' not in st.session_state:
    st.session_state.rsi_up = False
if 'eps_per' not in st.session_state:
    st.session_state.eps_per = False
if 'trading' not in st.session_state:
    st.session_state.trading = False
if 'foreign' not in st.session_state:
    st.session_state.foreign = False
if 'candle' not in st.session_state:
    st.session_state.candle = False
if 'sector' not in st.session_state:
    st.session_state.sector = False

# 데이터 로드
df_ind = load_data()
con = get_db_connection()

# 사이드바 구성 (간격 대폭 축소)
with st.sidebar:
    st.markdown("<h2 style='font-size: 1.8rem; margin-bottom: 0;'>🚀 Trading Copilot</h2>", unsafe_allow_html=True)
    st.markdown("---")
    
    # 시장 선택
    st.markdown("#### 시장 · 기간")
    market = st.selectbox("시장", ["모두", "KR", "US"], label_visibility="collapsed")
    
    # 기간 선택
    period = st.radio(
        "기간",
        ["전체", "단기", "중기", "장기", "백데이터"],
        horizontal=False,
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # 필터 (전체일 때만 활성화)
    st.markdown("#### 필터(기본)")

    # 비활성화 안내 메시지 추가 (여기부터)
    if period != "전체":
        st.markdown(f"""
        <div class="filter-disabled-notice">
            ⚠️ <strong>{period}</strong> 는 고정 필터 항목 입니다.<br>
                필터를 사용하려면 <strong>'전체'</strong>를 선택하세요.
        </div>
        """, unsafe_allow_html=True)
    filter_disabled = period != "전체"

    obv_apply = st.checkbox("OBV 상승 크로스", disabled=filter_disabled, key="obv")
    rsi_up_apply = st.checkbox("RSI 상승 지속", disabled=filter_disabled, key="rsi_up")
    eps_per_apply = st.checkbox("EPS & PER", disabled=filter_disabled, key="eps_per")
    trading_apply = st.checkbox("거래대금", disabled=filter_disabled, key="trading")

    st.markdown("---")

    # 필터 추가
    with st.expander("필터(추가)", expanded=True):
        foreign_apply = st.checkbox("외국인 순매수(국내전용)", disabled=filter_disabled, key="foreign")
        candle_apply = st.checkbox("캔들", disabled=filter_disabled, key="candle")
        sector_trend_apply = st.checkbox("섹터트렌드(해외전용)", disabled=filter_disabled, key="sector")

    st.markdown("---")

    # 버튼
    col1, col2 = st.columns(2)
    with col1:
        apply_btn = st.button("🔍 검색 적용", width='stretch', type="primary", disabled=filter_disabled)
    with col2:
        reset_btn = st.button("초기화", width='stretch', disabled=filter_disabled)

    st.markdown("---")
    
    # 로그 항목
    with st.expander("📋 사용설명서", expanded=False):
        
        st.markdown("### 📋 필터 조건 및 알고리즘 설명서")
        st.markdown("""
        ### 🎯 이 도구의 목적
        어려운 데이터 대신 **직관적인 지표**로 종목을 찾는 '주식 나침반'이에요. 
        시장의 흐름과 회사의 건강 상태를 분석해 여러분의 소중한 시간을 아끼고 데이터에 기반한 똑똑한 투자를 돕습니다.
        
        > **기본 필터(Liquidity)**: 모든 종목은 **시가총액**이 일정 수준 이상(KR: 2,000억 원 / US: 20억 달러)인 종목만 선별하여 안전성을 더했습니다.

        ---

        ### 🛠 탭별 활용법
        - **🔍 필터 탭**: "내 취향대로 찾기!" 아래 8가지 조건을 직접 조합해 나만의 유망주를 걸러낼 수 있어요.
        - **📊 KR/US 탭**: "시간 절약형!" 미리 검증된 필터로 자동 선별된 리스트를 즉시 확인하세요.
        - **📈 백테스팅 탭**: "전략 검증!" 내가 고른 필터가 과거에는 실제로 얼마나 수익을 냈는지 확인해 보세요.

        ---

        ### 💡 8가지 핵심 필터 작동 원리 (알고리즘)

        ### 1. 🌊 OBV 상승 크로스
        * **알고리즘**: `오늘 OBV > 신호선(9일 평균)` 이고 `어제 OBV <= 신호선`일 때
        * **설명**: 주가는 가만히 있어도 '누적 거래량'이 평균치를 뚫고 올라오면 세력이 움직이는 신호로 봅니다.
        * **한줄요약**: **"진짜 매수 에너지가 폭발하기 시작한 순간!"**

        ### 2. ⚡ RSI 상승 지속
        * **알고리즘**: `3일 연속 RSI 상승` 및 `현재 RSI 50 이하`
        * **설명**: 심리 지표가 바닥권에서 3일째 꾸준히 올라오며 '회복'하는 단계의 종목을 찾습니다.
        * **한줄요약**: **"차갑게 식었던 열기가 따뜻하게 살아나는 바닥 탈출 신호!"**

        ### 3. 📉 RSI 하강 지속
        * **알고리즘**: `3일 연속 RSI 하락` 및 `현재 RSI 50 이하`
        * **설명**: 주가의 기세가 3일째 힘없이 꺾이고 있는 상태입니다. 하락 추세를 주의해야 합니다.
        * **한줄요약**: **"매수세가 점차 위축되며 힘이 빠지고 있는 구간!"**

        ### 4. 💎 EPS & PER
        * **알고리즘**: `순이익(EPS) > 0` 이고 `PER이 3~30 사이`
        * **설명**: 적자 회사는 버리고, 이익 대비 주가가 합리적인(가성비 좋은) 종목만 고릅니다.
        * **한줄요약**: **"실적은 튼튼한데 가격은 아직 저렴한 '알짜' 종목!"**

        ### 5. 💰 거래대금 급증
        * **알고리즘**: `오늘 거래대금 > 최근 20일 평균의 1.5배`
        * **설명**: 평소보다 1.5배 이상의 큰돈이 몰렸다는 건 시장의 강력한 관심을 받고 있다는 증거입니다.
        * **한줄요약**: **"오늘 사람들의 돈과 관심이 가장 많이 쏠린 핫플 종목!"**

        ### 6. 🌏 외국인 순매수 (KR 전용)
        * **알고리즘**: `최근 2일 연속 외국인 순매수량 > 0`
        * **설명**: 시장의 큰손인 외국인 투자자들이 이틀 연속으로 '줍줍'하고 있는 종목을 추적합니다.
        * **한줄요약**: **"정보력 빠른 외국인 형님들이 이틀째 사고 있는 종목!"**

        ### 7. 🕯️ 캔들 패턴 (상단 마감)
        * **알고리즘**: `최근 5일 중 3일 이상 상단 마감` (캔들 위치 > 0.7)
        * **설명**: 장 막판까지 매수세가 강해 캔들 윗부분에서 가격이 끝나는 날이 많은 종목입니다.
        * **한줄요약**: **"뒷심이 좋아 종가가 항상 높게 형성되는 기세 좋은 종목!"**

        ### 8. 🏘️ 섹터 트렌드 (US 전용)
        * **알고리즘**: `속한 섹터 ETF의 수익률이 상승(+)` 중일 때
        * **설명**: 개별 주식뿐만 아니라 그 업종 전체가 유행을 타고 있는지 체크하여 성공 확률을 높입니다.
        * **한줄요약**: **"지금 가장 유행하는 동네(업종)에 있는 종목!"**
        """)

# 필터 적용 로직 (이전과 동일 - 생략)
if period == "전체":
    if apply_btn or reset_btn:
        if reset_btn:
            # 결과 데이터 초기화
            st.session_state.filter_results = pd.DataFrame()
            
            # 선택된 종목 초기화
            st.session_state.selected_symbol = None
            st.session_state.selected_market = None
            st.session_state.last_selected = None
            
            # 초기화 플래그 설정 (위젯 생성 전에 처리됨)
            st.session_state.reset_filters = True
            
            st.rerun()  # UI 새로고침
        else:
            use_us = market in ["모두", "US"]
            use_kr = market in ["모두", "KR"]
            
            condition = "rsi_d_latest == rsi_d_latest"
            if obv_apply:
                condition += " and (obv_latest > signal_obv_latest and obv_1ago <= signal_obv_1ago)"
            if rsi_up_apply:
                condition += " and (rsi_d_2ago < rsi_d_1ago and rsi_d_1ago < rsi_d_latest and rsi_d_latest <= 50)"
            if eps_per_apply:
                condition += " and eps > 0 and per >= 3 and per <= 30"
            if trading_apply:
                condition += " and today_trading_value > 1.5 * avg_trading_value_20d"
            
            df_filter = run_screener_query(con, filter_condition="eps_per_only", use_us=use_us, use_kr=use_kr)
            df_filter = df_filter.query(condition)
            
            df_filter = add_foreign_net_buy(df_filter)
            
            if foreign_apply and not df_filter.empty and 'foreign_net_buy_1ago' in df_filter.columns:
                df_filter = df_filter[(df_filter['foreign_net_buy_1ago'] > 0) & (df_filter['foreign_net_buy_2ago'] > 0)]
            
            if candle_apply and not df_filter.empty and 'upper_closes' in df_filter.columns:
                df_filter = df_filter[df_filter['upper_closes'] >= 3]
            
            if sector_trend_apply and not df_filter.empty and 'sector_trend' in df_filter.columns:
                df_filter = df_filter[(df_filter['market'] == 'US') & (df_filter['sector_trend'].str.contains('+', na=False, regex=False))]
            
            df_filter = add_names(df_filter)
            df_filter = add_close_price(df_filter)
            
            if not df_filter.empty:
                df_filter['foreign_positive'] = ((df_filter['foreign_net_buy_1ago'] > 0) & (df_filter['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
                df_filter['candle_upper_3'] = (df_filter['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
                df_filter['sector_trend_check'] = df_filter['sector_trend'].apply(lambda x: '✅' if '+' in str(x) else '❌' if '-' in str(x) else 'N/A')
                df_filter['eps_positive'] = df_filter['eps'] > 0
                df_filter['per_range'] = (df_filter['per'] >= 3) & (df_filter['per'] <= 30)
                
                df_filter = df_filter.rename(columns={
                    'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'sector': '섹터', 'sector_trend': '섹터트렌드',
                    'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest',
                    'close': '종가',
                    'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                    'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                    'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                    'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들',
                    'sector_trend_check': '섹터트렌드체크', 'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
                })
                
                # 시가총액 기준 내림차순 정렬
                df_filter = df_filter.sort_values('시가총액', ascending=False)

                df_kr = df_filter[df_filter['시장'] == 'KR'].copy() if '시장' in df_filter.columns else pd.DataFrame()
                df_us = df_filter[df_filter['시장'] == 'US'].copy() if '시장' in df_filter.columns else pd.DataFrame()
                
                if not df_kr.empty:
                    df_kr = format_dataframe(df_kr, 'KR')
                if not df_us.empty:
                    df_us = format_dataframe(df_us, 'US')
                
                st.session_state.filter_results = pd.concat([df_kr, df_us], ignore_index=True)
            else:
                st.session_state.filter_results = pd.DataFrame()
    
    df_display = st.session_state.filter_results
    
elif period == "단기":
    use_us = market in ["모두", "US"]
    use_kr = market in ["모두", "KR"]
    df_result = run_screener_query(con, "short_term", use_us=use_us, use_kr=use_kr)
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    
    if not df_result.empty:
        df_result['foreign_positive'] = ((df_result['foreign_net_buy_1ago'] > 0) & (df_result['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
        df_result['candle_upper_3'] = (df_result['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
        df_result['sector_trend_check'] = df_result['sector_trend'].apply(lambda x: '✅' if '+' in str(x) else '❌' if '-' in str(x) else 'N/A')
        df_result['eps_positive'] = df_result['eps'] > 0
        df_result['per_range'] = (df_result['per'] >= 3) & (df_result['per'] <= 30)
        
        df_result = df_result.rename(columns={
            'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'sector': '섹터', 'sector_trend': '섹터트렌드',
            'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest',
            'close': '종가',
            'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
            'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
            'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
            'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들',
            'sector_trend_check': '섹터트렌드체크', 'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
        })
        
        # 시가총액 기준 내림차순 정렬
        df_result = df_result.sort_values('시가총액', ascending=False)

        df_kr = df_result[df_result['시장'] == 'KR'].copy() if '시장' in df_result.columns else pd.DataFrame()
        df_us = df_result[df_result['시장'] == 'US'].copy() if '시장' in df_result.columns else pd.DataFrame()
        
        if not df_kr.empty:
            df_kr = format_dataframe(df_kr, 'KR')
        if not df_us.empty:
            df_us = format_dataframe(df_us, 'US')
        
        df_display = pd.concat([df_kr, df_us], ignore_index=True)
    else:
        df_display = pd.DataFrame()
        
elif period == "중기":
    use_us = market in ["모두", "US"]
    use_kr = market in ["모두", "KR"]
    df_result = run_screener_query(con, "mid_term", use_us=use_us, use_kr=use_kr, additional_filter="eps_per")
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    
    if not df_result.empty:
        df_result['foreign_positive'] = ((df_result['foreign_net_buy_1ago'] > 0) & (df_result['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
        df_result['candle_upper_3'] = (df_result['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
        df_result['sector_trend_check'] = df_result['sector_trend'].apply(lambda x: '✅' if '+' in str(x) else '❌' if '-' in str(x) else 'N/A')
        df_result['eps_positive'] = df_result['eps'] > 0
        df_result['per_range'] = (df_result['per'] >= 3) & (df_result['per'] <= 30)
        
        df_result = df_result.rename(columns={
            'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'sector': '섹터', 'sector_trend': '섹터트렌드',
            'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest',
            'close': '종가',
            'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
            'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
            'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
            'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들',
            'sector_trend_check': '섹터트렌드체크', 'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
        })
        
        # 시가총액 기준 내림차순 정렬
        df_result = df_result.sort_values('시가총액', ascending=False)

        df_kr = df_result[df_result['시장'] == 'KR'].copy() if '시장' in df_result.columns else pd.DataFrame()
        df_us = df_result[df_result['시장'] == 'US'].copy() if '시장' in df_result.columns else pd.DataFrame()
        
        if not df_kr.empty:
            df_kr = format_dataframe(df_kr, 'KR')
        if not df_us.empty:
            df_us = format_dataframe(df_us, 'US')
        
        df_display = pd.concat([df_kr, df_us], ignore_index=True)
    else:
        df_display = pd.DataFrame()
        
elif period == "장기":
    use_us = market in ["모두", "US"]
    use_kr = market in ["모두", "KR"]
    df_result = run_screener_query(con, "long_term", use_us=use_us, use_kr=use_kr, additional_filter="eps_per")
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    
    if not df_result.empty:
        df_result['foreign_positive'] = ((df_result['foreign_net_buy_1ago'] > 0) & (df_result['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
        df_result['candle_upper_3'] = (df_result['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
        df_result['sector_trend_check'] = df_result['sector_trend'].apply(lambda x: '✅' if '+' in str(x) else '❌' if '-' in str(x) else 'N/A')
        df_result['eps_positive'] = df_result['eps'] > 0
        df_result['per_range'] = (df_result['per'] >= 3) & (df_result['per'] <= 30)
        
        df_result = df_result.rename(columns={
            'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'sector': '섹터', 'sector_trend': '섹터트렌드',
            'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest',
            'close': '종가',
            'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
            'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
            'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
            'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들',
            'sector_trend_check': '섹터트렌드체크', 'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
        })
        
        # 시가총액 기준 내림차순 정렬
        df_result = df_result.sort_values('시가총액', ascending=False)

        df_kr = df_result[df_result['시장'] == 'KR'].copy() if '시장' in df_result.columns else pd.DataFrame()
        df_us = df_result[df_result['시장'] == 'US'].copy() if '시장' in df_result.columns else pd.DataFrame()
        
        if not df_kr.empty:
            df_kr = format_dataframe(df_kr, 'KR')
        if not df_us.empty:
            df_us = format_dataframe(df_us, 'US')
        
        df_display = pd.concat([df_kr, df_us], ignore_index=True)
    else:
        df_display = pd.DataFrame()

elif period == "백데이터":
    BACKTEST_DB_PATH = "data/meta/backtest.db"
    if not os.path.exists(BACKTEST_DB_PATH):
        st.warning("백테스팅 DB 없음 – 배치 실행하세요.")
        df_display = pd.DataFrame()
    else:
        con_back = duckdb.connect(BACKTEST_DB_PATH, read_only=True)
        df_back = con_back.execute("SELECT * FROM backtest").fetchdf()
        con_back.close()
        
        if not df_back.empty:
            if market == "KR":
                df_back = df_back[df_back['market'] == 'KR']
            elif market == "US":
                df_back = df_back[df_back['market'] == 'US']
            
            df_back['symbol'] = df_back.apply(lambda row: str(row['symbol']).zfill(6) if row['market'] == 'KR' else str(row['symbol']), axis=1)
            
            rename_dict = {
                'symbol': '종목코드',
                'name': '회사명',
                'sector': '섹터',
                'sector_trend': '섹터트렌드',
                'market': '시장',
                'close': '종가',
                'market_cap': '시가총액',
                'cap_status': '업데이트',
                'latest_close': '최신종가',
                'latest_update': '최신업데이트',
                'change_rate': '변동율%'
            }
            
            df_back = df_back.rename(columns=rename_dict)
            df_back = df_back.sort_values('업데이트', ascending=False)
            
            df_kr = df_back[df_back['시장'] == 'KR'].copy() if '시장' in df_back.columns else pd.DataFrame()
            df_us = df_back[df_back['시장'] == 'US'].copy() if '시장' in df_back.columns else pd.DataFrame()
            
            if not df_kr.empty:
                df_kr = format_dataframe(df_kr, 'KR')
            if not df_us.empty:
                df_us = format_dataframe(df_us, 'US')
            
            df_display = pd.concat([df_kr, df_us], ignore_index=True)
        else:
            df_display = pd.DataFrame()

# 배치 날짜 로드
log_time_file = "logs/batch_time.txt"
batch_time = ""
if os.path.exists(log_time_file):
    with open(log_time_file, "r") as f:
        batch_time = f.read().strip()

# 활성화된 필터 목록 생성
active_filters = []
if not df_display.empty:
    if period == "전체":
        if obv_apply:
            active_filters.append("OBV 상승")
        if rsi_up_apply:
            active_filters.append("RSI 상승")
        if eps_per_apply:
            active_filters.append("EPS & PER")
        if trading_apply:
            active_filters.append("거래대금")
        if foreign_apply:
            active_filters.append("외국인 순매수")
        if candle_apply:
            active_filters.append("캔들")
        if sector_trend_apply:
            active_filters.append("섹터트렌드")
    else:
        active_filters.append(f"{period} 전략")

# 상단 정보 박스 (테두리 제거 + 폰트 증가)
st.markdown(f"""
<div style='
    background: var(--secondary-background-color); 
    padding: 20px 24px; 
    border-radius: 24px; 
    border: 1px solid rgba(128,128,128,.15); 
    box-shadow: 0 6px 18px rgba(0,0,0,.08); 
    margin-bottom: 20px;
'>
    <div style='display: flex; align-items: center; justify-content: space-between;'>
        <h2 style='margin: 0; font-size: 1.8rem; font-weight: 800;'>오늘의 후보</h2>
        <div style='text-align: center;'>
            <div style='font-size: 0.85rem; opacity: 0.6; margin-bottom: 6px;'>마지막 갱신</div>
            <div style='font-size: 1.05rem; font-weight: 800; color: #8b5cf6;'>{batch_time if batch_time else 'N/A'}</div>
        </div>
    </div>
    <div style='display: flex; gap: 16px; flex-wrap: wrap; align-items: center; margin-top: 16px;'>
        <div>
            <span style='font-size: 0.95rem; opacity: 0.6;'>후보 수: </span>
            <span style='font-size: 1.6rem; font-weight: 1000; color: #2563eb;'>{len(df_display) if not df_display.empty else 0}</span>
        </div>
        <div>
            <span style='font-size: 0.95rem; opacity: 0.6;'>시장: </span>
            <span style='font-size: 1.6rem; font-weight: 1000; color: #059669;'>{market}</span>
        </div>
        <div>
            <span style='font-size: 0.95rem; opacity: 0.6;'>적용된 필터: </span>
            <span style='font-size: 1.05rem; font-weight: 800;'>{' · '.join(active_filters) if active_filters else '없음'}</span>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# 메인 콘텐츠 (1:1 비율)
col_left, col_right = st.columns([1, 1], gap="large")

with col_left:
    st.markdown("### 결과 리스트")
    
    if not df_display.empty:
        # 축약된 컬럼만 표시
        display_cols = ['종목코드', '시장', '회사명', '섹터', '섹터트렌드']
        
        # 종가와 시가총액 - KR과 US 모두 체크
        for col in ['종가 (KRW)', '종가 (USD)', '시가총액 (KRW 억원)', '시가총액 (USD M)']:
            if col in df_display.columns:
                display_cols.append(col)
        
        # 체크박스 8개 항목
        check_cols = ['OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들', '섹터트렌드체크']
        for col in check_cols:
            if col in df_display.columns:
                display_cols.append(col)
        
        if period == "백데이터":
            back_cols = ['업데이트', '최신종가', '최신업데이트', '변동율%']
            for col in back_cols:
                if col in df_display.columns:
                    display_cols.append(col)
        
        # 실제로 존재하는 컬럼만 필터링
        display_cols = [col for col in display_cols if col in df_display.columns]
        
        # 검색 기능
        search_term = st.text_input("🔍 종목 검색", placeholder="코드 또는 회사명 입력", key="main_search")
        
        if search_term:
            mask = (df_display['종목코드'].astype(str).str.contains(search_term, case=False, na=False)) | \
                   (df_display['회사명'].astype(str).str.contains(search_term, case=False, na=False))
            df_filtered = df_display[mask]
        else:
            df_filtered = df_display
        
        # KR과 US 테이블 구분
        df_kr_filtered = df_filtered[df_filtered['시장'] == 'KR'] if '시장' in df_filtered.columns else pd.DataFrame()
        df_us_filtered = df_filtered[df_filtered['시장'] == 'US'] if '시장' in df_filtered.columns else pd.DataFrame()
        
        if not df_kr_filtered.empty:
            # KR 통계 계산 (백데이터만)
            if period == "백데이터":
                kr_total = len(df_kr_filtered)
                kr_up = len(df_kr_filtered[df_kr_filtered['변동율%'] > 0]) if '변동율%' in df_kr_filtered.columns else 0
                kr_down = len(df_kr_filtered[df_kr_filtered['변동율%'] < 0]) if '변동율%' in df_kr_filtered.columns else 0
                kr_stats = f"총 종목수: {kr_total} · 상승: {kr_up} · 하락: {kr_down}"
            else:
                kr_stats = ""
            
            # CSV용 컬럼 순서 정의
            csv_columns_kr = ['종목코드', '시장', '회사명', '섹터', '섹터트렌드', '종가 (KRW)', '시가총액 (KRW 억원)',
                            'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', 
                            '외국인 순매수', '캔들', '섹터트렌드체크',
                            'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest',
                            '20일평균거래대금 (KRW 억원)', '오늘거래대금 (KRW 억원)', '회전율 (%)',
                            'PER_TTM (x)', 'EPS_TTM',
                            '외국인순매수_3일전 (주)', '외국인순매수_2일전 (주)', '외국인순매수_1일전 (주)',
                            '캔들(상단)', '캔들(하단)', '업데이트']
            
            if period == "백데이터":
                csv_columns_kr.extend(['최신종가', '최신업데이트', '변동율%'])
            
            # 실제 존재하는 컬럼만 선택
            csv_columns_kr = [col for col in csv_columns_kr if col in df_kr_filtered.columns]
            df_kr_csv = df_kr_filtered[csv_columns_kr]
            csv_kr = df_kr_csv.to_csv(index=False).encode('utf-8-sig')
            
            col_kr_header1, col_kr_header2, col_kr_header3 = st.columns([1, 2, 1])
            with col_kr_header1:
                st.markdown("#### 국내 (KR)")
            with col_kr_header2:
                if kr_stats:
                    st.markdown(f"**{kr_stats}**")
            with col_kr_header3:
                st.download_button(
                    label="💾 Data Download",
                    data=csv_kr,
                    file_name=f'kr_stocks_{period}.csv',
                    mime='text/csv',
                    key=f"download_kr_{period}"
                )
          
            # KR 전용 컬럼
            kr_display_cols = [col for col in display_cols if '(USD' not in col]
            
            # 동적 높이 계산 (10개 이상이면 스크롤)
            kr_count = len(df_kr_filtered)
            kr_height = min(kr_count, 10) * 30 + 30
            
            # 테이블 데이터 준비 (섹터트렌드 포함)
            df_kr_display_full = df_kr_filtered[kr_display_cols].copy().reset_index(drop=True)

            # 섹터트렌드 임시 저장
            kr_sector_trends = df_kr_display_full['섹터트렌드'].copy() if '섹터트렌드' in df_kr_display_full.columns else None

            # 표시용 데이터 (섹터트렌드 제외)
            df_kr_display = df_kr_display_full.drop(columns=['섹터트렌드'], errors='ignore')

            # KR 테이블 key - 현재 선택이 KR이 아니면 리셋
            kr_key = f"kr_dataframe_{period}"

            # 섹터트렌드 기반 행 배경색 적용
            def apply_kr_row_style(row):
                styles = []
                bg_color = None
                
                # 행 인덱스로 섹터트렌드 가져오기
                if kr_sector_trends is not None and row.name < len(kr_sector_trends):
                    if pd.notna(kr_sector_trends.iloc[row.name]):
                        bg_color = get_sector_trend_color(kr_sector_trends.iloc[row.name])
                
                # 모든 컬럼에 동일한 배경색 적용
                for _ in row.index:
                    if bg_color:
                        styles.append(f'background-color: {bg_color}')
                    else:
                        styles.append('')
                
                return styles

            # 스타일 적용
            styled_kr = df_kr_display.style.apply(apply_kr_row_style, axis=1)

            # 숫자 포맷 설정
            format_dict = {}
            for col in df_kr_display.columns:
                if df_kr_display[col].dtype in ['int64', 'float64']:
                    if col == '종가 (KRW)':
                        format_dict[col] = '{:,.0f}'
                    elif col == '종가 (USD)':
                        format_dict[col] = '${:,.2f}'
                    elif '시가총액' in col:
                        format_dict[col] = '{:,.2f}'
                    elif '거래대금' in col:
                        format_dict[col] = '{:,.2f}'
                    elif '회전율' in col:
                        format_dict[col] = '{:.2f}'
                    elif col in ['RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest']:
                        format_dict[col] = '{:.2f}'
                    elif col in ['PER_TTM (x)', 'EPS_TTM']:
                        format_dict[col] = '{:.2f}'
                    elif col == '변동율%':
                        format_dict[col] = '{:.2f}'
                    else:
                        format_dict[col] = '{:,.2f}'

            if format_dict:
                styled_kr = styled_kr.format(format_dict, na_rep='')

            # 데이터프레임 표시
            event_kr = st.dataframe(
                styled_kr,
                on_select="rerun",
                selection_mode="single-row",
                hide_index=True,
                width='stretch',
                height=kr_height,
                key=kr_key,  # ← 동적 key!
                column_config={
                    "종목코드": st.column_config.Column(width=50),
                    "시장": st.column_config.Column(width=40),
                    "회사명": st.column_config.Column(width="small"),
                    "섹터": st.column_config.Column(width="small"),
                    "종가 (KRW)": st.column_config.Column(width="small"),
                    "시가총액 (KRW 억원)": st.column_config.Column(width="small"),
                    "OBV_상승": st.column_config.Column(width=40),
                    "RSI_3상승": st.column_config.Column(width=40),
                    "RSI_3하강": st.column_config.Column(width=40),
                    "거래대금_상승": st.column_config.Column(width=40),
                    "EPS > 0": st.column_config.Column(width=40),
                    "3<=PER<=30": st.column_config.Column(width=40),
                    "외국인 순매수": st.column_config.Column(width=40),
                    "캔들": st.column_config.Column(width=40),
                    "섹터트렌드체크": st.column_config.Column(width=40),
                }
            )

            # 선택된 행 처리
            if event_kr.selection.rows:
                selected_idx = event_kr.selection.rows[0]
                new_symbol = df_kr_display.iloc[selected_idx]['종목코드']
                
                # 항상 업데이트 (단, rerun은 변경시만)
                if new_symbol != st.session_state.selected_symbol or st.session_state.selected_market != 'KR':
                    st.session_state.selected_symbol = new_symbol
                    st.session_state.selected_market = 'KR'
                    st.rerun()

        
        if not df_us_filtered.empty:
            # US 통계 계산 (백데이터만)
            if period == "백데이터":
                us_total = len(df_us_filtered)
                us_up = len(df_us_filtered[df_us_filtered['변동율%'] > 0]) if '변동율%' in df_us_filtered.columns else 0
                us_down = len(df_us_filtered[df_us_filtered['변동율%'] < 0]) if '변동율%' in df_us_filtered.columns else 0
                us_stats = f"총 종목수: {us_total} · 상승: {us_up} · 하락: {us_down}"
            else:
                us_stats = ""
            
            # CSV용 컬럼 순서 정의
            csv_columns_us = ['종목코드', '시장', '회사명', '섹터', '섹터트렌드', '종가 (USD)', '시가총액 (USD M)',
                            'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', 
                            '외국인 순매수', '캔들', '섹터트렌드체크',
                            'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest',
                            '20일평균거래대금 (USD M)', '오늘거래대금 (USD M)', '회전율 (%)',
                            'PER_TTM (x)', 'EPS_TTM',
                            '캔들(상단)', '캔들(하단)', '업데이트']
            
            if period == "백데이터":
                csv_columns_us.extend(['최신종가', '최신업데이트', '변동율%'])
            
            # 실제 존재하는 컬럼만 선택
            csv_columns_us = [col for col in csv_columns_us if col in df_us_filtered.columns]
            df_us_csv = df_us_filtered[csv_columns_us]
            csv_us = df_us_csv.to_csv(index=False).encode('utf-8-sig')
            
            col_us_header1, col_us_header2, col_us_header3 = st.columns([1, 2, 1])
            with col_us_header1:
                st.markdown("#### 해외 (US)")
            with col_us_header2:
                if us_stats:
                    st.markdown(f"**{us_stats}**")
            with col_us_header3:
                st.download_button(
                    label="💾 Data Download",
                    data=csv_us,
                    file_name=f'us_stocks_{period}.csv',
                    mime='text/csv',
                    key=f"download_us_{period}"
                )
            
            # US 전용 컬럼
            us_display_cols = [col for col in display_cols if '(KRW' not in col and '(주)' not in col]
            
            # 동적 높이 계산
            us_count = len(df_us_filtered)
            us_height = min(us_count, 10) * 30 + 30
            
            # 테이블 데이터 준비 (섹터트렌드 포함)
            df_us_display_full = df_us_filtered[us_display_cols].copy().reset_index(drop=True)

            # 섹터트렌드 임시 저장
            us_sector_trends = df_us_display_full['섹터트렌드'].copy() if '섹터트렌드' in df_us_display_full.columns else None

            # 표시용 데이터 (섹터트렌드 제외)
            df_us_display = df_us_display_full.drop(columns=['섹터트렌드'], errors='ignore')

            # US 테이블 key - KR이 선택되면 리셋 (US 선택 시에는 유지)
            us_key = f"us_dataframe_{period}"

            # 섹터트렌드 기반 행 배경색 적용
            def apply_us_row_style(row):
                styles = []
                bg_color = None
                
                # 행 인덱스로 섹터트렌드 가져오기
                if us_sector_trends is not None and row.name < len(us_sector_trends):
                    if pd.notna(us_sector_trends.iloc[row.name]):
                        bg_color = get_sector_trend_color(us_sector_trends.iloc[row.name])
                
                # 모든 컬럼에 동일한 배경색 적용
                for _ in row.index:
                    if bg_color:
                        styles.append(f'background-color: {bg_color}')
                    else:
                        styles.append('')
                
                return styles

            # 스타일 적용
            styled_us = df_us_display.style.apply(apply_us_row_style, axis=1)

            # 숫자 포맷 설정
            format_dict = {}
            for col in df_us_display.columns:
                if df_us_display[col].dtype in ['int64', 'float64']:
                    if col == '종가 (KRW)':
                        format_dict[col] = '{:,.0f}'
                    elif col == '종가 (USD)':
                        format_dict[col] = '${:,.2f}'
                    elif '시가총액' in col:
                        format_dict[col] = '{:,.2f}'
                    elif '거래대금' in col:
                        format_dict[col] = '{:,.2f}'
                    elif '회전율' in col:
                        format_dict[col] = '{:.2f}'
                    elif col in ['RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest']:
                        format_dict[col] = '{:.2f}'
                    elif col in ['PER_TTM (x)', 'EPS_TTM']:
                        format_dict[col] = '{:.2f}'
                    elif col == '변동율%':
                        format_dict[col] = '{:.2f}'
                    else:
                        format_dict[col] = '{:,.2f}'

            if format_dict:
                styled_us = styled_us.format(format_dict, na_rep='')

            # 데이터프레임 표시
            event_us = st.dataframe(
                styled_us,
                on_select="rerun",
                selection_mode="single-row",
                hide_index=True,
                width='stretch',
                height=us_height,
                key=us_key,  # ← 동적 key!
                column_config={
                    "종목코드": st.column_config.Column(width=50),
                    "시장": st.column_config.Column(width=40),
                    "회사명": st.column_config.Column(width="small"),
                    "섹터": st.column_config.Column(width="small"),
                    "종가 (USD)": st.column_config.Column(width="small"),
                    "시가총액 (USD M)": st.column_config.Column(width="small"),
                    "OBV_상승": st.column_config.Column(width=40),
                    "RSI_3상승": st.column_config.Column(width=40),
                    "RSI_3하강": st.column_config.Column(width=40),
                    "거래대금_상승": st.column_config.Column(width=40),
                    "EPS > 0": st.column_config.Column(width=40),
                    "3<=PER<=30": st.column_config.Column(width=40),
                    "외국인 순매수": st.column_config.Column(width=40),
                    "캔들": st.column_config.Column(width=40),
                    "섹터트렌드체크": st.column_config.Column(width=40),
                }
            )

            # 선택된 행 처리  
            if event_us.selection.rows:
                selected_idx = event_us.selection.rows[0]
                new_symbol = df_us_display.iloc[selected_idx]['종목코드']
                
                # 항상 업데이트 (단, rerun은 변경시만)
                if new_symbol != st.session_state.selected_symbol or st.session_state.selected_market != 'US':
                    st.session_state.selected_symbol = new_symbol
                    st.session_state.selected_market = 'US'
                    st.rerun()
        
        if df_kr_filtered.empty and df_us_filtered.empty:
            st.info("조건에 맞는 종목이 없습니다.")
        
        if period == "백데이터":
            st.markdown("---")
    else:
        st.info("조건에 맞는 종목이 없습니다.")

with col_right:
    st.markdown("### 자세히 보기")
    
    if st.session_state.selected_symbol and st.session_state.selected_market:
        symbol = st.session_state.selected_symbol
        market = st.session_state.selected_market
        
        # 선택된 종목 정보
        if not df_display.empty:
            selected_data = df_display[df_display['종목코드'] == symbol]
            
            if not selected_data.empty:
                row = selected_data.iloc[0]
                
                # 백데이터일 경우 추가 지표 로드
                if period == "백데이터":
                    ind_data = get_indicator_data(symbol, market)
                    if ind_data is not None:
                        row = pd.concat([row, ind_data])
                
                # 기본 정보
                st.markdown(f"**종목**: {row['회사명']}")
                st.markdown(f"**코드**: {symbol} · **시장**: {market} · **섹터**: {row.get('섹터', 'N/A')}")
                
                if '섹터트렌드' in row:
                    trend_text = row['섹터트렌드']
                    bg_color = get_sector_trend_color(trend_text)
                    
                    if bg_color:
                        # 배경색이 있는 경우
                        st.markdown(
                            f"<div style='background-color: {bg_color}; padding: 8px 12px; border-radius: 6px; margin: 4px 0;'>"
                            f"<strong>섹터트렌드</strong>: {trend_text}"
                            f"</div>",
                            unsafe_allow_html=True
                        )
                    else:
                        # 배경색이 없는 경우 (기존 스타일 유지)
                        st.markdown(f"**섹터트렌드**: {trend_text}")
                
                st.markdown("---")
                
                # KPI 그리드
                st.markdown("#### 주요 지표")
                
                kpi_col1, kpi_col2 = st.columns(2)
                
                with kpi_col1:
                    # RSI 3일 데이터
                    if all(k in row for k in ['RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest']):
                        st.metric(
                            "RSI 3일 데이터", 
                            f"{row['RSI_3일_2ago']:.2f} / {row['RSI_3일_1ago']:.2f} / {row['RSI_3일_latest']:.2f}"
                        )
                    
                    # 종가
                    if '종가 (KRW)' in row and pd.notna(row['종가 (KRW)']):
                        st.metric("종가", f"{row['종가 (KRW)']:,.0f} 원")
                    elif '종가 (USD)' in row and pd.notna(row['종가 (USD)']):
                        st.metric("종가", f"${row['종가 (USD)']:,.2f}")
                    
                    # 시가총액
                    if '시가총액 (KRW 억원)' in row and pd.notna(row['시가총액 (KRW 억원)']):
                        st.metric("시가총액", f"{row['시가총액 (KRW 억원)']:,.0f} 억원")
                    elif '시가총액 (USD M)' in row and pd.notna(row['시가총액 (USD M)']):
                        st.metric("시가총액", f"${row['시가총액 (USD M)']:,.2f}M")
                    
                    # PER / EPS
                    if 'PER_TTM (x)' in row and 'EPS_TTM' in row:
                        st.metric("PER / EPS", f"{row['PER_TTM (x)']:.2f} / {row['EPS_TTM']:.2f}")
                
                with kpi_col2:
                    # 거래대금 정보
                    if market == 'KR':
                        if all(k in row for k in ['20일평균거래대금 (KRW 억원)', '오늘거래대금 (KRW 억원)', '회전율 (%)']):
                            avg_val = f"{row['20일평균거래대금 (KRW 억원)']:,.0f}억원"
                            today_val = f"{row['오늘거래대금 (KRW 억원)']:,.0f}억원"
                            turnover_val = f"{row['회전율 (%)']:.2f}%"
                            st.metric(
                                "20일평균 / 오늘 / 회전율",
                                f"{avg_val} / {today_val} / {turnover_val}"
                            )
                    else:
                        if all(k in row for k in ['20일평균거래대금 (USD M)', '오늘거래대금 (USD M)', '회전율 (%)']):
                            st.metric(
                                "20일평균 / 오늘 / 회전율",
                                f"${row['20일평균거래대금 (USD M)']:,.2f}M / ${row['오늘거래대금 (USD M)']:,.2f}M / {row['회전율 (%)']:.2f}%"
                            )
                    
                    # 캔들 (상단 빨간색, 하단 파란색)
                    if '캔들(상단)' in row and '캔들(하단)' in row:
                        upper = int(row['캔들(상단)'])
                        lower = int(row['캔들(하단)'])
                        st.markdown(f"**캔들 (상단/하단)**")
                        st.markdown(f"<span style='color: #dc2626; font-size: 1.3rem; font-weight: 1000;'>{upper}</span> / <span style='color: #2563eb; font-size: 1.3rem; font-weight: 1000;'>{lower}</span>", unsafe_allow_html=True)
                    
                    # 외국인 순매수 3일 데이터 (플러스 빨간색, 마이너스 파란색)
                    if market == 'KR':
                        if all(k in row for k in ['외국인순매수_3일전 (주)', '외국인순매수_2일전 (주)', '외국인순매수_1일전 (주)']):
                            f3 = int(row['외국인순매수_3일전 (주)'])
                            f2 = int(row['외국인순매수_2일전 (주)'])
                            f1 = int(row['외국인순매수_1일전 (주)'])
                            
                            def format_foreign(val):
                                if val > 0:
                                    return f"<span style='color: #dc2626;'>{val:,}</span>"
                                elif val < 0:
                                    return f"<span style='color: #2563eb;'>{val:,}</span>"
                                else:
                                    return f"{val:,}"
                            
                            st.markdown("**외국인 순매수 (3일/2일/1일)**")
                            st.markdown(f"<span style='font-size: 1.1rem; font-weight: 800;'>{format_foreign(f3)} / {format_foreign(f2)} / {format_foreign(f1)}</span>", unsafe_allow_html=True)
                    
                    # OBV 상승
                    if 'OBV_상승' in row:
                        st.metric("OBV 상승", row['OBV_상승'])
                
                st.markdown("---")
                
                # 차트 탭
                chart_tab1, chart_tab2, chart_tab3, chart_tab4 = st.tabs(["종가", "MACD", "OBV", "RSI"])
                
                with chart_tab1:
                    show_chart(symbol, market, "종가")
                
                with chart_tab2:
                    show_chart(symbol, market, "MACD")
                
                with chart_tab3:
                    show_chart(symbol, market, "OBV")
                
                with chart_tab4:
                    show_chart(symbol, market, "RSI")
    else:
        st.info("왼쪽 테이블에서 종목을 선택하세요.")

# 연결 종료
if hasattr(st.session_state, 'con') and st.session_state.con:
    try:
        st.session_state.con.close()
    except:
        pass