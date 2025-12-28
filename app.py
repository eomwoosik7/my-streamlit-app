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

def get_sector_check(trend_text):
    """
    업종 트렌드가 플러스면 ✅, 마이너스면 ❌ 반환
    
    Args:
        trend_text: "상승(+4.01%) TIGER 200 금융" 형식의 텍스트
    
    Returns:
        '✅' 또는 '❌'
    """
    import re
    
    if pd.isna(trend_text) or trend_text == 'N/A':
        return '❌'
    
    # 퍼센트 값 추출 (예: "+4.01%" 또는 "-2.50%")
    match = re.search(r'([+-]?\d+\.?\d*)%', str(trend_text))
    
    if match:
        try:
            percent = float(match.group(1))
            return '✅' if percent > 0 else '❌'
        except:
            return '❌'
    
    return '❌'

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
    """외국인 순매수 5일치 + 합산값 추가"""
    if 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()
    df['foreign_net_buy_5ago'] = np.nan
    df['foreign_net_buy_4ago'] = np.nan
    df['foreign_net_buy_3ago'] = np.nan
    df['foreign_net_buy_2ago'] = np.nan
    df['foreign_net_buy_1ago'] = np.nan
    df['foreign_net_buy_sum'] = np.nan
    if df.empty:
        return df
    for idx, row in df.iterrows():
        symbol = row['symbol']
        market = row['market']
        meta_dict = meta.get(market, {}).get(symbol, {})
        fnb = meta_dict.get('foreign_net_buy', [0, 0, 0, 0, 0])
        # fnb는 최근부터 [0, 1, 2, 3, 4] 순서
        df.at[idx, 'foreign_net_buy_1ago'] = fnb[0] if len(fnb) > 0 else 0
        df.at[idx, 'foreign_net_buy_2ago'] = fnb[1] if len(fnb) > 1 else 0
        df.at[idx, 'foreign_net_buy_3ago'] = fnb[2] if len(fnb) > 2 else 0
        df.at[idx, 'foreign_net_buy_4ago'] = fnb[3] if len(fnb) > 3 else 0
        df.at[idx, 'foreign_net_buy_5ago'] = fnb[4] if len(fnb) > 4 else 0
        df.at[idx, 'foreign_net_buy_sum'] = sum(fnb)
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
    
# ========== 매수신호 계산 함수 ==========
def parse_json_col(df, col_name, num_vals=3):
    """JSON 컬럼 파싱"""
    if col_name not in df.columns:
        return pd.DataFrame([[0.0] * num_vals] * len(df))
    
    def safe_parse(x):
        if pd.isna(x) or not isinstance(x, str) or len(x) <= 2:
            return [0.0] * num_vals
        try:
            arr = json.loads(x)
            return [float(v) if isinstance(v, (int, float)) else 0.0 for v in arr[:num_vals]]
        except:
            return [0.0] * num_vals
    
    parsed = df[col_name].apply(safe_parse).apply(pd.Series)
    return parsed.iloc[:, :num_vals]

def calculate_buy_signals(df):
    """매수/매도 신호 점수 계산"""
    if df.empty:
        return df
    
    import re
    df = df.copy()
    
    # ========== 공통 계산 ==========
    # 외국인 순매수
    if 'foreign_net_buy_sum' in df.columns:
        df['foreign_sum'] = df['foreign_net_buy_sum']
    else:
        df['foreign_sum'] = 0
    
    # ✅ 캔들 (수정됨!)
    if 'upper_closes' in df.columns and 'lower_closes' in df.columns:
        df['candle_bullish'] = df['upper_closes'] > df['lower_closes']  # 매수: 상단 > 하단
        df['candle_bearish'] = df['lower_closes'] >= df['upper_closes']  # 매도: 하단 >= 상단
    else:
        df['candle_bullish'] = False
        df['candle_bearish'] = False
    
    # 섹터 트렌드
    def check_sector_positive(trend_text):
        if pd.isna(trend_text):
            return False
        match = re.search(r'([+-]?\d+\.?\d*)%', str(trend_text))
        if match:
            try:
                return float(match.group(1)) > 0
            except:
                return False
        return False
    
    def check_sector_negative(trend_text):
        if pd.isna(trend_text):
            return False
        match = re.search(r'([+-]?\d+\.?\d*)%', str(trend_text))
        if match:
            try:
                return float(match.group(1)) < 0
            except:
                return False
        return False
    
    if 'sector_trend' in df.columns:
        df['sector_positive'] = df['sector_trend'].apply(check_sector_positive)
        df['sector_negative'] = df['sector_trend'].apply(check_sector_negative)
    else:
        df['sector_positive'] = False
        df['sector_negative'] = False
    
    # ========== 단기 매수신호 (6점) ==========
    df['short_obv_cross'] = df.get('obv_bullish_cross', False)
    df['short_trading'] = df.get('trading_surge_2x', False)
    df['short_break'] = df.get('breakout', False)
    df['short_foreign'] = df['foreign_sum'] > 0
    df['short_candle'] = df['candle_bullish']
    df['short_sector'] = df['sector_positive']
    
    df['단기매수신호'] = (
        df['short_obv_cross'].astype(int) +
        df['short_trading'].astype(int) +
        df['short_break'].astype(int) +
        df['short_foreign'].astype(int) +
        df['short_candle'].astype(int) +
        df['short_sector'].astype(int)
    )
    
    # ========== 중기 매수신호 (7점) ==========
    df['mid_rsi'] = df.get('rsi_3up', False)
    df['mid_obv'] = df.get('obv_mid_condition', False)
    df['mid_golden'] = df.get('ma50_above_200', False)
    df['mid_trading'] = df.get('trading_above_avg', False)
    df['mid_foreign'] = df['foreign_sum'] > 0
    df['mid_candle'] = df['candle_bullish']
    df['mid_sector'] = df['sector_positive']
    
    df['중기매수신호'] = (
        df['mid_rsi'].astype(int) +
        df['mid_obv'].astype(int) +
        df['mid_golden'].astype(int) +
        df['mid_trading'].astype(int) +
        df['mid_foreign'].astype(int) +
        df['mid_candle'].astype(int) +
        df['mid_sector'].astype(int)
    )
    
    # ========== 매도신호 (6점) ==========
    df['sell_rsi_overbought'] = df.get('rsi_overbought', False)  # 1. RSI 과열
    df['sell_rsi_down'] = df.get('rsi_3down', False)  # 2. RSI 하강
    df['sell_obv_cross'] = df.get('obv_bearish_cross', False)  # 3. OBV 하락 크로스
    df['sell_foreign'] = df['foreign_sum'] < 0  # 4. 외국인 순매도 (마이너스)
    df['sell_candle'] = df['candle_bearish']  # 5. 캔들 (하단 >= 상단) ✅ 수정됨!
    df['sell_sector'] = df['sector_negative']  # 6. 섹터 약세 (마이너스)
    
    df['매도신호'] = (
        df['sell_rsi_overbought'].astype(int) +
        df['sell_rsi_down'].astype(int) +
        df['sell_obv_cross'].astype(int) +
        df['sell_foreign'].astype(int) +
        df['sell_candle'].astype(int) +
        df['sell_sector'].astype(int)
    )
    
    return df

def format_buy_signal(score, signal_type):
    """매수신호 점수 포맷팅"""
    if pd.isna(score):
        return ''
    score = int(score)
    
    if signal_type == 'short':
        # 단기 (기간 탭용): 🟣 6, 🔵 5, 🟢 0~4
        if score == 6:
            return f'🟣 {score}점'
        elif score == 5:
            return f'🔵 {score}점'
        else:
            return f'🟢 {score}점'
    
    elif signal_type == 'mid':
        # 중기 (기간 탭용): 🟣 7, 🔵 6, 🟢 0~5
        if score == 7:
            return f'🟣 {score}점'
        elif score == 6:
            return f'🔵 {score}점'
        else:
            return f'🟢 {score}점'
    
    elif signal_type == 'all_short':
        # 전체 단기: 🟣 6, 🔵 5, 🟢 3~4, 🟡 2, 🔴 0~1
        if score == 6:
            return f'🟣 {score}점'
        elif score == 5:
            return f'🔵 {score}점'
        elif score >= 3:
            return f'🟢 {score}점'
        elif score == 2:
            return f'🟡 {score}점'
        else:
            return f'🔴 {score}점'
    
    elif signal_type == 'all_mid':
        # 전체 중기: 🟣 7, 🔵 6, 🟢 4~5, 🟡 2~3, 🔴 0~1
        if score == 7:
            return f'🟣 {score}점'
        elif score == 6:
            return f'🔵 {score}점'
        elif score >= 4:
            return f'🟢 {score}점'
        elif score >= 2:
            return f'🟡 {score}점'
        else:
            return f'🔴 {score}점'
    
    return str(score)
# ========== 여기까지 매수신호 ==========

def run_screener_query(con, filter_condition="all", use_us=True, use_kr=True, top_n=None, additional_filters=None):
    """
    스크리너 쿼리 실행 함수
    
    주의: compute_indicators.py에서 생성된 전체 데이터 개수와 
    이 함수가 반환하는 데이터 개수는 다를 수 있습니다.
    
    이유: 시가총액 필터 (KR: 2,000억 원 이상, US: 20억 달러 이상)가 
    자동으로 적용되기 때문입니다.
    """
    try:
        con.execute("SELECT 1").fetchone()
    except:
        con = get_db_connection()
        st.session_state.con = con
    
    market_filter = "market = 'US'" if use_us and not use_kr else "market = 'KR'" if use_kr and not use_us else "market IN ('US', 'KR')"
    
    # 기본 조건 (단기/중기/매도 전략)
    if filter_condition == "short_term":
        # 단기: OBV 상승 크로스(9일) AND 거래대금 급증(2배) AND 돌파
        condition = """(obv_latest > signal_obv_9_latest AND obv_1ago <= signal_obv_9_1ago) 
                       AND (today_trading_value >= 2.0 * avg_trading_value_20d)
                       AND (break_20high = 1 OR (close_latest > ma20_latest AND close_1ago <= ma20_1ago))"""
    elif filter_condition == "mid_term":
        # 중기: RSI 상승(40-60) AND OBV 우상향/크로스(20일) AND 골든크로스 AND 거래대금(평균이상)
        condition = """(rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest >= 40 AND rsi_d_latest <= 60)
                       AND (obv_latest > signal_obv_20_latest AND 
                            (signal_obv_20_latest > signal_obv_20_3ago OR 
                             (obv_2ago <= signal_obv_20_2ago AND obv_latest > signal_obv_20_latest) OR
                             (obv_1ago <= signal_obv_20_1ago AND obv_latest > signal_obv_20_latest)))
                       AND (ma50_latest > ma200_latest)
                       AND (today_trading_value >= avg_trading_value_20d)"""
    elif filter_condition == "sell":
        # 매도: RSI 과열(70 이상) OR OBV 하락 크로스(9일) OR RSI 하강 지속
        condition = """(rsi_d_latest >= 70)
                       OR (obv_latest < signal_obv_9_latest AND obv_1ago >= signal_obv_9_1ago)
                       OR (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50)"""
    elif filter_condition == "all":
        # 전체: 필터 없음 (모든 종목)
        condition = "1=1"
    else:
        condition = "1=1"
    
    liquidity = """
    AND market_cap >= CASE WHEN market = 'US' THEN 2000000000.0 ELSE 200000000000.0 END
    """
    
    # 추가 필터 적용
    additional_condition = ""
    if additional_filters:
        for key, value in additional_filters.items():
            if value:  # True인 경우만 적용
                if key == "foreign":
                    # 외국인 순매수: 5일치 합산 > 0 (메타에서 처리하므로 여기서는 스킵)
                    pass
                elif key == "candle":
                    # 캔들: 최근 5일 중 3일 이상 상단 마감
                    additional_condition += " AND upper_closes >= 3"
    
    query = f"""
    WITH parsed AS (
        SELECT symbol, market,
            rsi_d, macd_d, signal_d, obv_d, signal_obv_9d, signal_obv_20d, market_cap, avg_trading_value_20d, today_trading_value, turnover,
            per, eps, cap_status, upper_closes, lower_closes, sector, sector_trend,
            ma20, ma50, ma200, break_20high, close_d,
            CAST(json_extract(rsi_d, '$[0]') AS DOUBLE) AS rsi_d_2ago,
            CAST(json_extract(rsi_d, '$[1]') AS DOUBLE) AS rsi_d_1ago,
            CAST(json_extract(rsi_d, '$[2]') AS DOUBLE) AS rsi_d_latest,
            CAST(json_extract(macd_d, '$[2]') AS DOUBLE) AS macd_latest,
            CAST(json_extract(signal_d, '$[2]') AS DOUBLE) AS signal_latest,
            CAST(json_extract(obv_d, '$[2]') AS DOUBLE) AS obv_2ago,
            CAST(json_extract(obv_d, '$[1]') AS DOUBLE) AS obv_1ago,
            CAST(json_extract(obv_d, '$[0]') AS DOUBLE) AS obv_latest,
            CAST(json_extract(signal_obv_9d, '$[1]') AS DOUBLE) AS signal_obv_9_1ago,
            CAST(json_extract(signal_obv_9d, '$[0]') AS DOUBLE) AS signal_obv_9_latest,
            CAST(json_extract(signal_obv_20d, '$[0]') AS DOUBLE) AS signal_obv_20_latest,
            CAST(json_extract(signal_obv_20d, '$[1]') AS DOUBLE) AS signal_obv_20_1ago,
            CAST(json_extract(signal_obv_20d, '$[2]') AS DOUBLE) AS signal_obv_20_2ago,
            CAST(json_extract(signal_obv_20d, '$[3]') AS DOUBLE) AS signal_obv_20_3ago,
            CAST(json_extract(close_d, '$[1]') AS DOUBLE) AS close_1ago,
            CAST(json_extract(close_d, '$[0]') AS DOUBLE) AS close_latest,
            CAST(json_extract(ma20, '$[1]') AS DOUBLE) AS ma20_1ago,
            CAST(json_extract(ma20, '$[0]') AS DOUBLE) AS ma20_latest,
            CAST(json_extract(ma50, '$[0]') AS DOUBLE) AS ma50_latest,
            CAST(json_extract(ma200, '$[0]') AS DOUBLE) AS ma200_latest
        FROM indicators
    )
    SELECT symbol, market,
        rsi_d AS rsi_d_array,
        macd_d AS macd_array,
        signal_d AS signal_array,
        obv_d AS obv_array,
        signal_obv_9d AS signal_obv_9_array,
        signal_obv_20d AS signal_obv_20_array,
        market_cap, avg_trading_value_20d, today_trading_value, turnover,
        per, eps, cap_status, upper_closes, lower_closes, sector, sector_trend,
        rsi_d_2ago, rsi_d_1ago, rsi_d_latest,
        macd_latest, signal_latest,
        obv_latest, signal_obv_9_latest, signal_obv_20_latest,
        obv_1ago, signal_obv_9_1ago,
        close_latest, close_1ago,
        ma20_latest, ma20_1ago, ma50_latest, ma200_latest, break_20high,
        (obv_latest > signal_obv_9_latest AND obv_1ago <= signal_obv_9_1ago) AS obv_bullish_cross,
        (today_trading_value > 2.0 * avg_trading_value_20d) AS trading_surge_2x,
        (break_20high = 1 OR (close_latest > ma20_latest AND close_1ago <= ma20_1ago)) AS breakout,
        (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest >= 40 AND rsi_d_latest <= 60) AS rsi_3up,
        (obv_latest > signal_obv_20_latest AND 
         (signal_obv_20_latest > signal_obv_20_3ago OR 
          (obv_2ago <= signal_obv_20_2ago AND obv_latest > signal_obv_20_latest) OR
          (obv_1ago <= signal_obv_20_1ago AND obv_latest > signal_obv_20_latest))) AS obv_mid_condition,
        (obv_latest > signal_obv_20_latest) AS obv_uptrend,
        (ma50_latest > ma200_latest) AS ma50_above_200,
        (today_trading_value >= avg_trading_value_20d) AS trading_above_avg,
        (rsi_d_latest >= 70) AS rsi_overbought,
        (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3down,
        (obv_latest < signal_obv_9_latest AND obv_1ago >= signal_obv_9_1ago) AS obv_bearish_cross
    FROM parsed
    WHERE {market_filter}
      AND {condition}
      {liquidity}
      {additional_condition}
    ORDER BY market_cap DESC
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
            '외국인순매수_5일전': '외국인순매수_5일전 (주)',
            '외국인순매수_4일전': '외국인순매수_4일전 (주)',
            '외국인순매수_3일전': '외국인순매수_3일전 (주)',
            '외국인순매수_2일전': '외국인순매수_2일전 (주)',
            '외국인순매수_1일전': '외국인순매수_1일전 (주)',
            '외국인순매수_합산': '외국인순매수_합산 (주)',
            'sector': '업종',
            'sector_trend': '업종트렌드',
        })
    elif market_type == 'US':
        df = df.rename(columns={
            '시가총액': '시가총액 (USD M)',
            '20일평균거래대금': '20일평균거래대금 (USD M)',
            '오늘거래대금': '오늘거래대금 (USD M)',
            '회전율': '회전율 (%)',
            'PER_TTM': 'PER_TTM (x)',
            '종가': '종가 (USD)',
            '외국인순매수_5일전': '외국인순매수_5일전 (N/A)',
            '외국인순매수_4일전': '외국인순매수_4일전 (N/A)',
            '외국인순매수_3일전': '외국인순매수_3일전 (N/A)',
            '외국인순매수_2일전': '외국인순매수_2일전 (N/A)',
            '외국인순매수_1일전': '외국인순매수_1일전 (N/A)',
            '외국인순매수_합산': '외국인순매수_합산 (N/A)',
            'sector': '업종',
            'sector_trend': '업종트렌드',
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

    bool_cols = [col for col in df.columns if col in [
        'OBV 상승 크로스', '거래대금 급증(20일평균2배)', '돌파(20일 고가 or MA20 상향)',
        'RSI 상승', 'OBV 우상향/크로스', '50MA > 200MA', '거래대금(20평균이상)',
        'RSI 과열(70 이상)', 'RSI 하강 지속', 'OBV 하락 크로스'
        # '외국인 순매수', '캔들', '섹터', '매도신호'는 이미 변환되어 있으므로 제외
    ]]
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
            template="plotly"
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False}, theme="streamlit")
        
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
            template="plotly"
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False}, theme="streamlit")
        
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
            template="plotly"
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False}, theme="streamlit")
        
    elif chart_type == "RSI":
        rsi = ta.rsi(df_chart[close_col], length=14)
        df_rsi = pd.DataFrame({'Date': df_chart.index, 'RSI': rsi}).dropna()
        
        fig = px.line(df_rsi, x='Date', y='RSI', title="RSI")
        fig.add_hline(y=30, line_dash="dot", line_color="#dc2626", annotation_text="OverSold (30)", annotation_position="bottom right")
        fig.add_hline(y=70, line_dash="dot", line_color="#dc2626", annotation_text="OverBought (70)", annotation_position="top right")
        fig.update_traces(name='RSI', showlegend=True, line=dict(color='#8b5cf6', width=2))
        fig.update_layout(
            height=350,
            template="plotly"
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False}, theme="streamlit")

def get_indicator_data(symbol, market):
    con = get_db_connection()
    query = """
    WITH parsed AS (
        SELECT 
            rsi_d, macd_d, signal_d, obv_d, signal_obv_9d, market_cap, avg_trading_value_20d, today_trading_value, turnover,
            per, eps, cap_status, upper_closes, lower_closes, sector, sector_trend,
            ma20, ma50, ma200, break_20high, close_d,
            CAST(json_extract(rsi_d, '$[0]') AS DOUBLE) AS rsi_d_2ago,
            CAST(json_extract(rsi_d, '$[1]') AS DOUBLE) AS rsi_d_1ago,
            CAST(json_extract(rsi_d, '$[2]') AS DOUBLE) AS rsi_d_latest,
            CAST(json_extract(macd_d, '$[2]') AS DOUBLE) AS macd_latest,
            CAST(json_extract(signal_d, '$[2]') AS DOUBLE) AS signal_latest,
            CAST(json_extract(obv_d, '$[1]') AS DOUBLE) AS obv_1ago,
            CAST(json_extract(obv_d, '$[0]') AS DOUBLE) AS obv_latest,
            CAST(json_extract(signal_obv_9d, '$[1]') AS DOUBLE) AS signal_obv_1ago,
            CAST(json_extract(signal_obv_9d, '$[0]') AS DOUBLE) AS signal_obv_latest,
            CAST(json_extract(close_d, '$[1]') AS DOUBLE) AS close_1ago,
            CAST(json_extract(close_d, '$[0]') AS DOUBLE) AS close_latest,
            CAST(json_extract(ma20, '$[1]') AS DOUBLE) AS ma20_1ago,
            CAST(json_extract(ma20, '$[0]') AS DOUBLE) AS ma20_latest,
            CAST(json_extract(ma50, '$[0]') AS DOUBLE) AS ma50_latest,
            CAST(json_extract(ma200, '$[0]') AS DOUBLE) AS ma200_latest
        FROM indicators
        WHERE symbol = ? AND market = ?
    )
    SELECT 
        rsi_d_2ago, rsi_d_1ago, rsi_d_latest,
        market_cap, avg_trading_value_20d, today_trading_value, turnover,
        per, eps, upper_closes, lower_closes, sector, sector_trend,
        ma20_latest, ma200_latest,
        (obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AS obv_bullish_cross,
        (today_trading_value > 2.0 * avg_trading_value_20d) AS trading_surge_2x,
        (break_20high = 1 OR (close_latest > ma20_latest AND close_1ago <= ma20_1ago)) AS breakout,
        NOT (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50) AS rsi_not_3down,
        (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3up,
        (obv_latest > signal_obv_latest) AS obv_uptrend,
        (ma50_latest > ma200_latest) AS ma50_above_200,
        (today_trading_value >= avg_trading_value_20d) AS trading_above_avg,
        (rsi_d_latest >= 70) AS rsi_overbought,
        (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3down,
        (obv_latest < signal_obv_latest AND obv_1ago >= signal_obv_1ago) AS obv_bearish_cross,
        (ma50_latest < ma200_latest) AS ma50_below_200,
        (today_trading_value <= 0.5 * avg_trading_value_20d) AS trading_below_half
    FROM parsed
    """
    df = con.execute(query, [symbol, market]).fetchdf()
    con.close()
    if not df.empty:
        series = df.iloc[0]
        # RSI 컬럼 이름 변경 (백데이터 탭에서 표시용)
        series = series.rename({
            'rsi_d_2ago': 'RSI_3일_2ago',
            'rsi_d_1ago': 'RSI_3일_1ago',
            'rsi_d_latest': 'RSI_3일_latest'
        })
        return series
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

# 페이지네이션 상태 추가
if 'kr_page' not in st.session_state:
    st.session_state.kr_page = 0
if 'us_page' not in st.session_state:
    st.session_state.us_page = 0

# ✅ 정렬 상태 추가 (복수 컬럼 정렬)
if 'kr_sort_rules' not in st.session_state:
    st.session_state.kr_sort_rules = [
        {'column': '시가총액 (KRW 억원)', 'ascending': False}
    ]
if 'us_sort_rules' not in st.session_state:
    st.session_state.us_sort_rules = [
        {'column': '시가총액 (USD M)', 'ascending': False}
    ]

# ✅ 마지막 기간 저장 (탭 변경 감지용)
if 'last_period' not in st.session_state:
    st.session_state.last_period = None

# 초기화 플래그 추가
if 'reset_filters' not in st.session_state:
    st.session_state.reset_filters = False

# 필터 체크박스 상태 초기화
if st.session_state.reset_filters:
    filter_keys = [
        'short_obv', 'short_trading', 'short_break',
        'mid_rsi', 'mid_obv', 'mid_golden', 'mid_trading',
        'foreign', 'candle'
    ]
    for key in filter_keys:
        if key in st.session_state:
            del st.session_state[key]
    st.session_state.reset_filters = False

# 기본값 설정
filter_defaults = {
    'short_obv': False, 'short_trading': False, 'short_break': False,
    'mid_rsi': False, 'mid_obv': False, 'mid_golden': False, 'mid_trading': False,
    'foreign': False, 'candle': False
}
for key, default_val in filter_defaults.items():
    if key not in st.session_state:
        st.session_state[key] = default_val

# 데이터 로드
df_ind = load_data()
con = get_db_connection()

# 사이드바 구성
with st.sidebar:
    st.markdown("<h2 style='font-size: 1.8rem; margin-bottom: 0;'>🚀 Trading Copilot</h2>", unsafe_allow_html=True)
    st.markdown("---")
    
    # 시장 선택
    st.markdown("#### 시장 · 기간")
    market = st.selectbox("시장", ["모두", "KR", "US"], label_visibility="collapsed")
    
    # 기간 선택 (장기 → 매도)
    period = st.radio(
        "기간",
        ["전체", "단기", "중기", "매도", "백데이터"],
        horizontal=False,
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # 필터 익스팬더 (전체만 활성화)
    filter_disabled = period != "전체"
    
    if period == "단기":
        with st.expander("필터(단기) - 자동 적용됨", expanded=True):
            st.markdown("""
            ✅ OBV 상승 크로스  
            ✅ 거래대금 급증(20일평균2배)  
            ✅ 돌파(20일 고가 or MA20 상향)  
                        ➕외국인 순매수(5일)  
                        ➕캔들(5일)  
                        ➕섹터 트렌드
            """)
    elif period == "중기":
        with st.expander("필터(중기) - 자동 적용됨", expanded=True):
            st.markdown("""
            ✅ RSI 상승  
            ✅ OBV 우상향/크로스  
            ✅ 50MA > 200MA  
            ✅ 거래대금(20평균이상)  
                        ➕외국인 순매수(5일)  
                        ➕캔들(5일)  
                        ➕섹터 트렌드
            """)
    elif period == "매도":
        with st.expander("필터(매도) - 자동 적용됨", expanded=True):
            st.markdown("""
            ✅ RSI 과열 (70 이상)  
            ✅ OBV 하락 크로스  
            ✅ RSI 하강 지속  
                        ➕외국인 순매수(리버스)  
                        ➕캔들(리버스)  
                        ➕섹터 트렌드(리버스)
            """)
    elif period == "백데이터":
        st.markdown("")
    
    # 필터 선택 (전체만 활성화)
    if period != "전체":
        st.markdown("""
        <div class="filter-disabled-notice">
            ⚠️ 필터를 사용하려면 <strong>'전체'</strong>를 선택하세요.
        </div>
        """, unsafe_allow_html=True)
    
    # 필터(단기)
    with st.expander("필터(단기)", expanded=False):
        short_obv = st.checkbox("OBV 상승 크로스", disabled=filter_disabled, key="short_obv")
        short_trading = st.checkbox("거래대금 급증(20일평균2배)", disabled=filter_disabled, key="short_trading")
        short_break = st.checkbox("돌파(20일 고가 or MA20 상향)", disabled=filter_disabled, key="short_break")
    
    # 필터(중기)
    with st.expander("필터(중기)", expanded=False):
        mid_rsi = st.checkbox("RSI 상승", disabled=filter_disabled, key="mid_rsi")
        mid_obv = st.checkbox("OBV 우상향/크로스", disabled=filter_disabled, key="mid_obv")
        mid_golden = st.checkbox("50MA > 200MA", disabled=filter_disabled, key="mid_golden")
        mid_trading = st.checkbox("거래대금(20평균이상)", disabled=filter_disabled, key="mid_trading")
    
    # 필터(참고)
    with st.expander("필터(참고)", expanded=False):
        foreign_apply = st.checkbox("외국인 순매수(5일 합산 > 0)", disabled=filter_disabled, key="foreign")
        candle_apply = st.checkbox("캔들(5일중 상단 > 하단)", disabled=filter_disabled, key="candle")
    
    st.markdown("---")
    
    # 버튼 (전체만 활성화)
    col1, col2 = st.columns(2)
    with col1:
        apply_btn = st.button("🔍 검색 적용", use_container_width=True, type="primary", disabled=filter_disabled)
    with col2:
        reset_btn = st.button("초기화", use_container_width=True, disabled=filter_disabled)
    
    st.markdown("---")
    
    # 사용설명서
    with st.expander("📋 사용설명서", expanded=False):
        st.markdown("""

    ## 📍 이 앱의 목적

    **차트 신호로 '좋아 보이는 종목'과 '조심해야 할 종목'을 골라주는 도구**입니다.

    - ✅ 살 만한 종목을 빠르게 찾기
    - ⚠️ 보유 종목의 위험 신호 확인
    - 🔁 과거 전략 성과로 신뢰도 점검

    ---

    ## 🧭 필터 전략

    ### 🌐 전체 (필터) - 내 입맛대로 종목 찾기!  
    1. **9개 필터** → AND조건으로 동작  
    2. **단기신호**  
   단기신호(6점) : 단기 필터(3) → 각 1+점  
    - 외국인 순매도 +, 캔들 상승, 섹터 강세 → 각 +1점  
    - 🟣 6점 : 매수 고려  
    - 🔵 5점 : 안정  
    - 🟢 3~4점 : 관심  
    - 🟡 2점 : 주의  
    - 🔴 0~2점 : 매수 제외                    
                     
    3. **중기신호**  
    중기신호(7점) : 중기 필터(4) 각 → 1+점  
    - 외국인 순매도 +, 캔들 상승, 섹터 강세 → 각 +1점  
    - 🟣 7점 : 매수 고려  
    - 🔵 6점 : 안정  
    - 🟢 3~4점 : 관심  
    - 🟡 1~2점 : 주의  
    - 🔴 0~1점 : 매수 제외                
        
    ---              
                    
    ### ⚡ 단기 (3개 AND) - 급등 가능성 찾기
    1. **OBV 상승 크로스** → 돈이 갑자기 들어오기 시작
    2. **거래대금 급증 (평균 2배)** → 사람들이 몰림
    3. **가격 돌파** → 20일 고가 또는 MA20 위로 돌파

    → **돈 + 관심 + 돌파 = 단기 급등 확률 ↑**

    4. 단기매수점수(6점) : 단기 필터(3) 각 → 1+점  
    - 외국인 순매도 +, 캔들 상승, 섹터 강세 → 각 +1점  
    - 🟣 6점 : 매수 고려  
    - 🔵 5점 : 안정  
    - 🟢 3~4점 : 관심                      

    ---

    ### 🌳 중기 (4개 AND) - 안정적인 상승 흐름
    1. **RSI 3일 상승 (40~60)** → 바닥에서 회복 중
    2. **OBV 우상향** → 돈이 꾸준히 유입
    3. **50MA > 200MA** → 골든크로스 (중기 상승 추세)
    4. **거래대금 평균 이상** → 관심이 계속 유지됨

    → **추세 + 유입 + 회복 = 중기 안정 상승**

    5. 중기매수점수(7점) : 중기 필터(4) 각 → 1+점
    - 외국인 순매도 +, 캔들 상승, 섹터 강세 → 각 +1점
    - 🟣 7점 : 매수 고려  
    - 🔵 6점 : 안정
    - 🟢 4~5점 : 관심      

    ---

    ### 🛑 매도 (하나라도 OR) - 위험 신호 감지
    1. **RSI ≥ 70** → 과열 구간
    2. **OBV 하락 크로스** → 돈이 빠져나가기 시작
    3. **RSI 3일 하락 (≤50)** → 매수 심리 꺾임
    4. **매도신호** → 보유한 종목의 매도 타이밍을 확인하세요 !  
    - 리버스 : 외국인 순매도 -, 캔들 하단 마감, 섹터 약세 → 각 +1점
    - 🟢 0~2점 : 안정  
    - 🟡 3~4점 : 주의  
    - 🔴 5~6점 : 매도 강하게 고려

    ---

    ### 📊 백데이터 - 고정 필터 성능 검증  
    1. **변동율** → 필터링된 종목의 성능 검증       

    """)

    with st.expander("📘 주식 데이터 설명", expanded=False):
        st.markdown("""
            
    1. **RSI (0~100)**: 주가가 과열인지, 너무 빠졌는지 보는 지표  
    - 70↑ : 과열 구간 (너무 많이 오른 상태) ⚠️  
    - 40~60 : 회복 시작 구간 (관심)  
    - 30↓ : 과매도 구간 (너무 많이 떨어진 상태)

    2. **MA (이동평균)**: 최근 N일 평균 가격으로 보는 추세선  
    - 종가 > MA20 : 단기 상승 흐름 시작  
    - MA50 > MA200 : 중기 상승 추세 (골든크로스)  
    - MA50 < MA200 : 중기 하락 추세 (데드크로스)

    3. **OBV**: 거래량으로 돈의 흐름을 보는 지표  
    - OBV 상승 크로스 : 갑자기 돈 유입 시작  
    - OBV 우상향 : 돈이 꾸준히 들어오는 중  
    - OBV 하락 크로스 : 돈이 빠져나가기 시작 ⚠️

    4. **거래대금**: 하루 동안 거래된 총 금액 (관심도)  
    - 많을수록 : 사람들이 많이 보는 종목 👀  
    - 오늘 > 20일 평균 : 관심 증가 신호

    5. **회전율**: 주식이 얼마나 '바쁘게' 사고팔리는지  
    - 높음 : 매매 활발, 변동 큼 (단기용)  
    - 낮음 : 거래 한산, 비교적 안정 (중기용)

    6. **외국인 순매수**: 외국인 투자자 자금 유입 여부  
    - + : 외국인이 더 많이 삼 → 긍정 신호  
    - - : 외국인이 더 많이 팜 → 주의  
    - 5일 합산 기준으로 판단

    7. **캔들**: 하루 동안 매수·매도 힘의 결과  
    - 상단 > 하단 : 매수 힘이 더 강함  
    - 상단 ≤ 하단 : 매도 힘이 더 강함  
    - 상단 마감 : 종가가 상위 70% → 강한 마감  
    - 하단 마감 : 종가가 하위 30% → 약한 마감

    8. **업종**: 이 회사가 속한 산업(업종) 분위기  
    - 같은 업종 종목들은 함께 움직이는 경향  
    - 최근 20일 등락률(%) 표시  
    - + : 업종 강세 🔴 / - : 업종 약세 🔵

    9. **EPS**: 주당순이익 (1주당 얼마나 버는지)  
    - 회사의 '돈 버는 실력'  
    - 높을수록, 꾸준히 늘수록 좋음

    10. **PER**: 주가수익비율 (실력 대비 가격표)  
    - 주가 ÷ EPS  
    - 낮음 : 상대적으로 저렴  
    - 높음 : 비싸거나 기대가 큼  
    - 같은 업종끼리 비교
                    
        """)        
# 필터 적용 로직
# 필터 적용 로직
if period == "전체":
    if apply_btn or reset_btn:
        if reset_btn:
            st.session_state.filter_results = pd.DataFrame()
            st.session_state.selected_symbol = None
            st.session_state.selected_market = None
            st.session_state.last_selected = None
            st.session_state.reset_filters = True
            # ✅ 페이지네이션 리셋 추가
            st.session_state.kr_page = 0
            st.session_state.us_page = 0
            st.rerun()
        else:
            use_us = market in ["모두", "US"]
            use_kr = market in ["모두", "KR"]
            
            # 필터 조건 조합 (모두 AND 조건)
            filter_parts = []
            
            # 단기 필터
            if st.session_state.short_obv:
                filter_parts.append("(obv_latest > signal_obv_9_latest AND obv_1ago <= signal_obv_9_1ago)")
            if st.session_state.short_trading:
                filter_parts.append("(today_trading_value >= 2.0 * avg_trading_value_20d)")
            if st.session_state.short_break:
                filter_parts.append("(break_20high = 1 OR (close_latest > ma20_latest AND close_1ago <= ma20_1ago))")
            
            # 중기 필터 (signal_obv_20d 사용)
            if st.session_state.mid_rsi:
                filter_parts.append("(rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest >= 40 AND rsi_d_latest <= 60)")
            if st.session_state.mid_obv:
                filter_parts.append("""(obv_latest > signal_obv_20_latest AND 
                                        (signal_obv_20_latest > signal_obv_20_3ago OR 
                                         (obv_2ago <= signal_obv_20_2ago AND obv_latest > signal_obv_20_latest) OR
                                         (obv_1ago <= signal_obv_20_1ago AND obv_latest > signal_obv_20_latest)))""")
            if st.session_state.mid_golden:
                filter_parts.append("(ma50_latest > ma200_latest)")
            if st.session_state.mid_trading:
                filter_parts.append("(today_trading_value >= avg_trading_value_20d)")
            
            # 조건 조합 (AND 연결)
            if filter_parts:
                combined_condition = " AND ".join(filter_parts)
            else:
                combined_condition = "1=1"  # 조건 없으면 전체
            
            additional_filters = {
                "foreign": st.session_state.foreign,
                "candle": st.session_state.candle
            }
            
            # 커스텀 쿼리 실행
            try:
                con.execute("SELECT 1").fetchone()
            except:
                con = get_db_connection()
                st.session_state.con = con
            
            market_filter = "market = 'US'" if use_us and not use_kr else "market = 'KR'" if use_kr and not use_us else "market IN ('US', 'KR')"
            
            liquidity = """
            AND market_cap >= CASE WHEN market = 'US' THEN 2000000000.0 ELSE 200000000000.0 END
            """
            
            # 추가 필터 적용
            additional_condition = ""
            if additional_filters:
                for key, value in additional_filters.items():
                    if value:
                        if key == "candle":
                            additional_condition += " AND upper_closes >= 3"
            
            query = f"""
            WITH parsed AS (
                SELECT symbol, market,
                    rsi_d, macd_d, signal_d, obv_d, signal_obv_9d, signal_obv_20d, market_cap, avg_trading_value_20d, today_trading_value, turnover,
                    per, eps, cap_status, upper_closes, lower_closes, sector, sector_trend,
                    ma20, ma50, ma200, break_20high, close_d,
                    CAST(json_extract(rsi_d, '$[0]') AS DOUBLE) AS rsi_d_2ago,
                    CAST(json_extract(rsi_d, '$[1]') AS DOUBLE) AS rsi_d_1ago,
                    CAST(json_extract(rsi_d, '$[2]') AS DOUBLE) AS rsi_d_latest,
                    CAST(json_extract(macd_d, '$[2]') AS DOUBLE) AS macd_latest,
                    CAST(json_extract(signal_d, '$[2]') AS DOUBLE) AS signal_latest,
                    CAST(json_extract(obv_d, '$[1]') AS DOUBLE) AS obv_1ago,
                    CAST(json_extract(obv_d, '$[0]') AS DOUBLE) AS obv_latest,
                    CAST(json_extract(obv_d, '$[2]') AS DOUBLE) AS obv_2ago,
                    CAST(json_extract(signal_obv_9d, '$[1]') AS DOUBLE) AS signal_obv_9_1ago,
                    CAST(json_extract(signal_obv_9d, '$[0]') AS DOUBLE) AS signal_obv_9_latest,
                    CAST(json_extract(signal_obv_20d, '$[0]') AS DOUBLE) AS signal_obv_20_latest,
                    CAST(json_extract(signal_obv_20d, '$[1]') AS DOUBLE) AS signal_obv_20_1ago,
                    CAST(json_extract(signal_obv_20d, '$[2]') AS DOUBLE) AS signal_obv_20_2ago,
                    CAST(json_extract(signal_obv_20d, '$[3]') AS DOUBLE) AS signal_obv_20_3ago,
                    CAST(json_extract(close_d, '$[1]') AS DOUBLE) AS close_1ago,
                    CAST(json_extract(close_d, '$[0]') AS DOUBLE) AS close_latest,
                    CAST(json_extract(ma20, '$[1]') AS DOUBLE) AS ma20_1ago,
                    CAST(json_extract(ma20, '$[0]') AS DOUBLE) AS ma20_latest,
                    CAST(json_extract(ma50, '$[0]') AS DOUBLE) AS ma50_latest,
                    CAST(json_extract(ma200, '$[0]') AS DOUBLE) AS ma200_latest
                FROM indicators
            )
            SELECT symbol, market,
                rsi_d AS rsi_d_array,
                macd_d AS macd_array,
                signal_d AS signal_array,
                obv_d AS obv_array,
                signal_obv_9d AS signal_obv_9_array,
                signal_obv_20d AS signal_obv_20_array,
                market_cap, avg_trading_value_20d, today_trading_value, turnover,
                per, eps, cap_status, upper_closes, lower_closes, sector, sector_trend,
                rsi_d_2ago, rsi_d_1ago, rsi_d_latest,
                macd_latest, signal_latest,
                obv_latest, signal_obv_9_latest, signal_obv_20_latest,
                obv_1ago, signal_obv_9_1ago,
                obv_2ago, signal_obv_20_1ago, signal_obv_20_2ago, signal_obv_20_3ago,
                close_latest, close_1ago,
                ma20_latest, ma20_1ago, ma50_latest, ma200_latest, break_20high,
                (obv_latest > signal_obv_9_latest AND obv_1ago <= signal_obv_9_1ago) AS obv_bullish_cross,
                (today_trading_value > 2.0 * avg_trading_value_20d) AS trading_surge_2x,
                (break_20high = 1 OR (close_latest > ma20_latest AND close_1ago <= ma20_1ago)) AS breakout,
                (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest >= 40 AND rsi_d_latest <= 60) AS rsi_3up,
                (obv_latest > signal_obv_20_latest AND 
                 (signal_obv_20_latest > signal_obv_20_3ago OR 
                  (obv_2ago <= signal_obv_20_2ago AND obv_latest > signal_obv_20_latest) OR
                  (obv_1ago <= signal_obv_20_1ago AND obv_latest > signal_obv_20_latest))) AS obv_mid_condition,
                (obv_latest > signal_obv_20_latest) AS obv_uptrend,
                (ma50_latest > ma200_latest) AS ma50_above_200,
                (today_trading_value >= avg_trading_value_20d) AS trading_above_avg,
                (rsi_d_latest >= 70) AS rsi_overbought,
                (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3down,
                (obv_latest < signal_obv_9_latest AND obv_1ago >= signal_obv_9_1ago) AS obv_bearish_cross
            FROM parsed
            WHERE {market_filter}
              AND ({combined_condition})
              {liquidity}
              {additional_condition}
            ORDER BY market_cap DESC
            """
            df_filter = con.execute(query).fetchdf()
            
            df_filter = add_foreign_net_buy(df_filter)
            
            # 외국인 순매수 필터 적용 (5일 합산 > 0)
            if st.session_state.foreign and not df_filter.empty and 'foreign_net_buy_sum' in df_filter.columns:
                df_filter = df_filter[df_filter['foreign_net_buy_sum'] > 0]
            
            df_filter = add_names(df_filter)
            df_filter = add_close_price(df_filter)
            
            # ========== 1단계: 매수신호 계산 ==========
            df_filter = calculate_buy_signals(df_filter)
            
            if not df_filter.empty:
                # ========== 2단계: 점수 계산에 사용한 값을 직접 재사용 (중복 방지) ==========
                # ✅ short_foreign, short_candle, short_sector를 그대로 ✅/❌로 변환
                if 'short_foreign' in df_filter.columns:
                    df_filter['_외국인_순매수'] = df_filter['short_foreign'].apply(lambda x: '✅' if x else '❌')
                else:
                    df_filter['_외국인_순매수'] = '❌'
                
                if 'short_candle' in df_filter.columns:
                    df_filter['_캔들'] = df_filter['short_candle'].apply(lambda x: '✅' if x else '❌')
                else:
                    df_filter['_캔들'] = '❌'
                
                if 'short_sector' in df_filter.columns:
                    df_filter['_섹터'] = df_filter['short_sector'].apply(lambda x: '✅' if x else '❌')
                else:
                    df_filter['_섹터'] = '❌'
                
                # ========== 3단계: 매수신호 포맷팅 ==========
                df_filter['단기매수신호_fmt'] = df_filter['단기매수신호'].apply(lambda x: format_buy_signal(x, 'all_short'))
                df_filter['중기매수신호_fmt'] = df_filter['중기매수신호'].apply(lambda x: format_buy_signal(x, 'all_mid'))
                
                # ========== 4단계: rename ==========
                df_filter = df_filter.rename(columns={
                    'symbol': '종목코드', 
                    'market': '시장', 
                    'name': '회사명', 
                    'sector': '업종', 
                    'sector_trend': '업종트렌드',
                    'close': '종가',
                    'market_cap': '시가총액',
                    'avg_trading_value_20d': '20일평균거래대금',
                    'today_trading_value': '오늘거래대금',
                    'turnover': '회전율',
                    'per': 'PER_TTM',
                    'eps': 'EPS_TTM',
                    'foreign_net_buy_5ago': '외국인순매수_5일전',
                    'foreign_net_buy_4ago': '외국인순매수_4일전',
                    'foreign_net_buy_3ago': '외국인순매수_3일전',
                    'foreign_net_buy_2ago': '외국인순매수_2일전',
                    'foreign_net_buy_1ago': '외국인순매수_1일전',
                    'foreign_net_buy_sum': '외국인순매수_합산',
                    'cap_status': '업데이트',
                    '_외국인_순매수': '외국인 순매수',
                    '_캔들': '캔들',
                    '_섹터': '섹터',
                    '단기매수신호_fmt': '단기신호',
                    '중기매수신호_fmt': '중기신호',
                    'rsi_d_2ago': 'RSI_3일_2ago',
                    'rsi_d_1ago': 'RSI_3일_1ago',
                    'rsi_d_latest': 'RSI_3일_latest',
                    'upper_closes': '캔들(상단)',
                    'lower_closes': '캔들(하단)',
                    'obv_bullish_cross': 'OBV 상승 크로스',
                    'trading_surge_2x': '거래대금 급증(20일평균2배)',
                    'breakout': '돌파(20일 고가 or MA20 상향)',
                    'rsi_3up': 'RSI 상승',
                    'obv_mid_condition': 'OBV 우상향/크로스',
                    'ma50_above_200': '50MA > 200MA',
                    'trading_above_avg': '거래대금(20평균이상)',
                    'rsi_overbought': 'RSI 과열(70 이상)',
                    'rsi_3down': 'RSI 하강 지속'
                })
                
                # ========== 5단계: 불필요한 컬럼 삭제 (중복 방지) ==========
                drop_cols = [
                    'short_obv_cross', 'short_trading', 'short_break', 'short_foreign', 'short_candle', 'short_sector',
                    'mid_rsi', 'mid_obv', 'mid_golden', 'mid_trading', 'mid_foreign', 'mid_candle', 'mid_sector',
                    '단기매수신호', '중기매수신호'
                ]
                df_filter = df_filter.drop(columns=[col for col in drop_cols if col in df_filter.columns], errors='ignore')
                
                # ========== 6단계: 정렬 및 시장별 분리 ==========
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

                # ✅ 페이지 리셋 추가
                st.session_state.kr_page = 0
                st.session_state.us_page = 0
    
    df_display = st.session_state.filter_results

elif period == "단기":
    use_us = market in ["모두", "US"]
    use_kr = market in ["모두", "KR"]
    df_result = run_screener_query(con, "short_term", use_us=use_us, use_kr=use_kr)
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    
    if not df_result.empty:
        df_result = calculate_buy_signals(df_result)
        
        # ✅ mid_foreign, mid_candle, mid_sector를 직접 재사용
        if 'mid_foreign' in df_result.columns:
            df_result['_외국인_순매수'] = df_result['mid_foreign'].apply(lambda x: '✅' if x else '❌')
        else:
            df_result['_외국인_순매수'] = '❌'
        
        if 'mid_candle' in df_result.columns:
            df_result['_캔들'] = df_result['mid_candle'].apply(lambda x: '✅' if x else '❌')
        else:
            df_result['_캔들'] = '❌'
        
        if 'mid_sector' in df_result.columns:
            df_result['_섹터'] = df_result['mid_sector'].apply(lambda x: '✅' if x else '❌')
        else:
            df_result['_섹터'] = '❌'
        
        # ========== 3단계: rename ==========
        df_result = df_result.rename(columns={
            'symbol': '종목코드', 
            'market': '시장', 
            'name': '회사명', 
            'sector': '업종',
            'sector_trend': '업종트렌드',
            'close': '종가',
            'market_cap': '시가총액',
            'avg_trading_value_20d': '20일평균거래대금',
            'today_trading_value': '오늘거래대금',
            'turnover': '회전율',
            'per': 'PER_TTM',
            'eps': 'EPS_TTM',
            'obv_bullish_cross': 'OBV 상승 크로스',
            'trading_surge_2x': '거래대금 급증(20일평균2배)',
            'breakout': '돌파(20일 고가 or MA20 상향)',
            'foreign_net_buy_5ago': '외국인순매수_5일전',
            'foreign_net_buy_4ago': '외국인순매수_4일전',
            'foreign_net_buy_3ago': '외국인순매수_3일전',
            'foreign_net_buy_2ago': '외국인순매수_2일전',
            'foreign_net_buy_1ago': '외국인순매수_1일전',
            'foreign_net_buy_sum': '외국인순매수_합산',
            '_외국인_순매수': '외국인 순매수',
            '_캔들': '캔들',
            '_섹터': '섹터',
            'rsi_d_2ago': 'RSI_3일_2ago',
            'rsi_d_1ago': 'RSI_3일_1ago',
            'rsi_d_latest': 'RSI_3일_latest',
            'upper_closes': '캔들(상단)',
            'lower_closes': '캔들(하단)'
        })
        
        # ========== 4단계: 매수신호 포맷팅 ==========
        if '단기매수신호' in df_result.columns:
            df_result['단기매수신호'] = df_result['단기매수신호'].apply(lambda x: format_buy_signal(x, 'short'))
        
        # ========== 5단계: 불필요한 컬럼 삭제 ==========
        drop_cols = ['short_obv_cross', 'short_trading', 'short_break', 'short_foreign', 'short_candle', 'short_sector']
        df_result = df_result.drop(columns=[col for col in drop_cols if col in df_result.columns], errors='ignore')
        
        # ========== 6단계: 정렬 및 시장별 분리 ==========
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
    df_result = run_screener_query(con, "mid_term", use_us=use_us, use_kr=use_kr)
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    
    if not df_result.empty:
        # ========== 1단계: 매수신호 계산 ==========
        df_result = calculate_buy_signals(df_result)

        # ========== 2단계: 점수 계산에 사용한 값을 재사용 ==========
        if 'mid_foreign' in df_result.columns:
            df_result['_외국인_순매수'] = df_result['mid_foreign'].apply(lambda x: '✅' if x else '❌')

        if 'mid_candle' in df_result.columns:
            df_result['_캔들'] = df_result['mid_candle'].apply(lambda x: '✅' if x else '❌')

        if 'mid_sector' in df_result.columns:
            df_result['_섹터'] = df_result['mid_sector'].apply(lambda x: '✅' if x else '❌')
        
        # ========== 3단계: rename ==========
        df_result = df_result.rename(columns={
            'symbol': '종목코드', 
            'market': '시장', 
            'name': '회사명', 
            'sector': '업종', 
            'sector_trend': '업종트렌드',
            'close': '종가',
            'market_cap': '시가총액',
            'avg_trading_value_20d': '20일평균거래대금',
            'today_trading_value': '오늘거래대금',
            'turnover': '회전율',
            'per': 'PER_TTM',
            'eps': 'EPS_TTM',
            'rsi_3up': 'RSI 상승',
            'obv_mid_condition': 'OBV 우상향/크로스',
            'ma50_above_200': '50MA > 200MA',
            'trading_above_avg': '거래대금(20평균이상)',
            'foreign_net_buy_5ago': '외국인순매수_5일전',
            'foreign_net_buy_4ago': '외국인순매수_4일전',
            'foreign_net_buy_3ago': '외국인순매수_3일전',
            'foreign_net_buy_2ago': '외국인순매수_2일전',
            'foreign_net_buy_1ago': '외국인순매수_1일전',
            'foreign_net_buy_sum': '외국인순매수_합산',
            '_외국인_순매수': '외국인 순매수',
            '_캔들': '캔들',
            '_섹터': '섹터',
            'rsi_d_2ago': 'RSI_3일_2ago',
            'rsi_d_1ago': 'RSI_3일_1ago',
            'rsi_d_latest': 'RSI_3일_latest',
            'upper_closes': '캔들(상단)',
            'lower_closes': '캔들(하단)'
        })
        
        # ========== 4단계: 매수신호 포맷팅 ==========
        if '중기매수신호' in df_result.columns:
            df_result['중기매수신호'] = df_result['중기매수신호'].apply(lambda x: format_buy_signal(x, 'mid'))
        
        # ========== 5단계: 불필요한 컬럼 삭제 ==========
        drop_cols = ['mid_rsi', 'mid_obv', 'mid_golden', 'mid_trading', 'mid_foreign', 'mid_candle', 'mid_sector']
        df_result = df_result.drop(columns=[col for col in drop_cols if col in df_result.columns], errors='ignore')
        
        # ========== 6단계: 정렬 및 시장별 분리 ==========
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

elif period == "매도":
    use_us = market in ["모두", "US"]
    use_kr = market in ["모두", "KR"]
    df_result = run_screener_query(con, "sell", use_us=use_us, use_kr=use_kr)
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    
    if not df_result.empty:
        # ========== 1단계: 매도신호 계산 ==========
        df_result = calculate_buy_signals(df_result)
        
        # ========== 2단계: 점수 계산값 재사용 (중복 방지) ==========
        if 'sell_foreign' in df_result.columns:
            df_result['_외국인_순매수_리버스'] = df_result['sell_foreign'].apply(lambda x: '✅' if x else '❌')
        else:
            df_result['_외국인_순매수_리버스'] = '❌'
        
        if 'sell_candle' in df_result.columns:
            df_result['_캔들_리버스'] = df_result['sell_candle'].apply(lambda x: '✅' if x else '❌')
        else:
            df_result['_캔들_리버스'] = '❌'
        
        if 'sell_sector' in df_result.columns:
            df_result['_섹터_리버스'] = df_result['sell_sector'].apply(lambda x: '✅' if x else '❌')
        else:
            df_result['_섹터_리버스'] = '❌'
        
        # ========== 3단계: 매도신호 포맷팅 + 원본 삭제 ==========
        if '매도신호' in df_result.columns:
            df_result['매도신호_fmt'] = df_result['매도신호'].apply(
                lambda x: f'🟢 {x}점' if x <= 2 else f'🟡 {x}점' if x <= 4 else f'🔴 {x}점'
            )
            # ✅ 원본 '매도신호' 삭제 (rename 전)
            df_result = df_result.drop(columns=['매도신호'])
        
        # ========== 4단계: rename ==========
        df_result = df_result.rename(columns={
            'symbol': '종목코드', 
            'market': '시장', 
            'name': '회사명', 
            'sector': '업종', 
            'sector_trend': '업종트렌드',
            'close': '종가',
            'market_cap': '시가총액',
            'avg_trading_value_20d': '20일평균거래대금',
            'today_trading_value': '오늘거래대금',
            'turnover': '회전율',
            'per': 'PER_TTM',
            'eps': 'EPS_TTM',
            'rsi_overbought': 'RSI 과열(70 이상)',
            'rsi_3down': 'RSI 하강 지속',
            'obv_bearish_cross': 'OBV 하락 크로스',
            'foreign_net_buy_5ago': '외국인순매수_5일전',
            'foreign_net_buy_4ago': '외국인순매수_4일전',
            'foreign_net_buy_3ago': '외국인순매수_3일전',
            'foreign_net_buy_2ago': '외국인순매수_2일전',
            'foreign_net_buy_1ago': '외국인순매수_1일전',
            'foreign_net_buy_sum': '외국인순매수_합산',
            '_외국인_순매수_리버스': '외국인 순매수(리버스)',
            '_캔들_리버스': '캔들(리버스)',
            '_섹터_리버스': '섹터(리버스)',
            '매도신호_fmt': '매도신호',  # ← 이제 안전
            'rsi_d_2ago': 'RSI_3일_2ago',
            'rsi_d_1ago': 'RSI_3일_1ago',
            'rsi_d_latest': 'RSI_3일_latest',
            'upper_closes': '캔들(상단)',
            'lower_closes': '캔들(하단)'
        })
        
        # ========== 5단계: 불필요한 컬럼 삭제 ==========
        drop_cols = ['sell_rsi_overbought', 'sell_rsi_down', 'sell_obv_cross', 'sell_foreign', 'sell_candle', 'sell_sector']
        df_result = df_result.drop(columns=[col for col in drop_cols if col in df_result.columns], errors='ignore')
        
        # ========== 6단계: 정렬 및 시장별 분리 ==========
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
            
            # 매도 신호 추가 (매도 기간 종목과 비교)
            use_us_sell = market in ["모두", "US"]
            use_kr_sell = market in ["모두", "KR"]
            df_sell = run_screener_query(con, "sell", use_us=use_us_sell, use_kr=use_kr_sell)

            if not df_sell.empty:
                # 매도 종목의 세부 데이터 가져오기
                df_sell = add_foreign_net_buy(df_sell)
                
                # ✅ calculate_buy_signals로 매도신호 계산
                df_sell = calculate_buy_signals(df_sell)
                
                # ✅ 매도 종목 딕셔너리 생성 (symbol을 key로)
                sell_dict = {}
                for idx, row in df_sell.iterrows():
                    symbol = row['symbol']
                    score = row.get('매도신호', 0)  # ✅ 계산된 점수 직접 사용
                    sell_dict[symbol] = score
                
                # 매도신호 동그라미 + 점수로 표시
                def get_sell_signal(symbol):
                    if symbol in sell_dict:
                        score = sell_dict[symbol]
                        if score <= 2:
                            return f'🟢 {score}점'
                        elif score <= 4:
                            return f'🟡 {score}점'
                        else:
                            return f'🔴 {score}점'
                    return '⚪ 0점'  # 매도 종목 아님
                
                df_back['매도신호'] = df_back['symbol'].apply(get_sell_signal)
            else:
                df_back['매도신호'] = '⚪'

            # DB에서 가져온 타입을 한글로 변환
            if 'type' in df_back.columns:
                type_mapping = {
                    'short': '단기',
                    'mid': '중기',
                    'short_mid': '단기+중기',
                    'short+mid': '단기+중기'
                }
                df_back['type'] = df_back['type'].map(type_mapping).fillna(df_back['type'])

            df_back = add_foreign_net_buy(df_back)
            
            # 외국인 순매수 필터 적용 (필터(참고) 활성화 시)
            if apply_btn and foreign_apply and 'foreign_net_buy_sum' in df_back.columns:
                df_back = df_back[df_back['foreign_net_buy_sum'] > 0]
            
            # 캔들 필터 적용
            if apply_btn and candle_apply and 'upper_closes' in df_back.columns:
                df_back = df_back[df_back['upper_closes'] >= 3]
            
            if not df_back.empty:
                # 외국인 순매수: 5일 합산 > 0이면 ✅, 아니면 ❌
                df_back['foreign_positive'] = df_back['foreign_net_buy_sum'].apply(lambda x: '✅' if x > 0 else '❌')
                # 캔들: 5일 중 3일 이상이면 ✅, 아니면 ❌
                df_back['candle_upper_3'] = df_back['upper_closes'].apply(lambda x: '✅' if x >= 3 else '❌')
                
                rename_dict = {
                    'symbol': '종목코드',
                    'name': '회사명',
                    'sector': '업종',
                    'sector_trend': '업종트렌드',
                    'market': '시장',
                    'close': '종가',
                    'market_cap': '시가총액',
                    'avg_trading_value_20d': '20일평균거래대금',
                    'today_trading_value': '오늘거래대금',
                    'turnover': '회전율',
                    'per': 'PER_TTM',
                    'eps': 'EPS_TTM',
                    'cap_status': '업데이트',
                    'type': '타입',
                    'latest_close': '최신종가',
                    'latest_update': '최신업데이트',
                    'change_rate': '변동율%',
                    'foreign_net_buy_5ago': '외국인순매수_5일전',
                    'foreign_net_buy_4ago': '외국인순매수_4일전',
                    'foreign_net_buy_3ago': '외국인순매수_3일전',
                    'foreign_net_buy_2ago': '외국인순매수_2일전',
                    'foreign_net_buy_1ago': '외국인순매수_1일전',
                    'foreign_net_buy_sum': '외국인순매수_합산',
                    'foreign_positive': '외국인 순매수',
                    'candle_upper_3': '캔들',
                    'rsi_d_2ago': 'RSI_3일_2ago',
                    'rsi_d_1ago': 'RSI_3일_1ago',
                    'rsi_d_latest': 'RSI_3일_latest',
                    'upper_closes': '캔들(상단)',
                    'lower_closes': '캔들(하단)'
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
        short_filters = []
        if st.session_state.short_obv:
            short_filters.append("OBV 상승 크로스")
        if st.session_state.short_trading:
            short_filters.append("거래대금 급증")
        if st.session_state.short_break:
            short_filters.append("돌파")
        if short_filters:
            active_filters.append(f"단기({', '.join(short_filters)})")
        
        mid_filters = []
        if st.session_state.mid_rsi:
            mid_filters.append("RSI 상승")
        if st.session_state.mid_obv:
            mid_filters.append("OBV 우상향")
        if st.session_state.mid_golden:
            mid_filters.append("골든크로스")
        if st.session_state.mid_trading:
            mid_filters.append("거래대금")
        if mid_filters:
            active_filters.append(f"중기({', '.join(mid_filters)})")
        
        if st.session_state.foreign:
            active_filters.append("외국인 순매수")
        if st.session_state.candle:
            active_filters.append("캔들")
    elif period in ["단기", "중기", "매도"]:
        active_filters.append(f"{period} 전략")
    elif period == "백데이터":
        if apply_btn:
            if st.session_state.foreign:
                active_filters.append("외국인 순매수")
            if st.session_state.candle:
                active_filters.append("캔들")

# 상단 정보 박스
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
    # ✅ 탭 변경 감지 및 페이지네이션 + 정렬 리셋
    if st.session_state.last_period != period:
        st.session_state.kr_page = 0
        st.session_state.us_page = 0
        # ✅ 정렬도 초기값으로 리셋
        st.session_state.kr_sort_column = '시가총액 (KRW 억원)'
        st.session_state.kr_sort_ascending = False
        st.session_state.us_sort_column = '시가총액 (USD M)'
        st.session_state.us_sort_ascending = False
        st.session_state.last_period = period

    if not df_display.empty:
        # 기간별 표시 컬럼 설정
        if period == "단기":
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '종가 (USD)', '시가총액 (KRW 억원)', '시가총액 (USD M)']:
                if col in df_display.columns:
                    display_cols.append(col)
            # 단기매수신호 추가
            if '단기매수신호' in df_display.columns:
                display_cols.append('단기매수신호')
            check_cols = ['OBV 상승 크로스', '거래대금 급증(20일평균2배)', '돌파(20일 고가 or MA20 상향)', '외국인 순매수', '캔들', '섹터']
            for col in check_cols:
                if col in df_display.columns:
                    display_cols.append(col)
        
        elif period == "중기":
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '종가 (USD)', '시가총액 (KRW 억원)', '시가총액 (USD M)']:
                if col in df_display.columns:
                    display_cols.append(col)
            # 중기매수신호 추가
            if '중기매수신호' in df_display.columns:
                display_cols.append('중기매수신호')
            check_cols = ['RSI 상승', 'OBV 우상향/크로스', '50MA > 200MA', '거래대금(20평균이상)', '외국인 순매수', '캔들', '섹터']
            for col in check_cols:
                if col in df_display.columns:
                    display_cols.append(col)
        
        elif period == "매도":
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '종가 (USD)', '시가총액 (KRW 억원)', '시가총액 (USD M)']:
                if col in df_display.columns:
                    display_cols.append(col)
            # 매도신호 추가
            if '매도신호' in df_display.columns:
                display_cols.append('매도신호')
            check_cols = ['RSI 과열(70 이상)', 'RSI 하강 지속', 'OBV 하락 크로스', '외국인 순매수(리버스)', '캔들(리버스)', '섹터(리버스)']
            for col in check_cols:
                if col in df_display.columns:
                    display_cols.append(col)
        
        elif period == "백데이터":
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '종가 (USD)', '시가총액 (KRW 억원)', '시가총액 (USD M)']:
                if col in df_display.columns:
                    display_cols.append(col)
            back_cols = ['업데이트', '타입', '최신종가', '최신업데이트', '변동율%', '매도신호']
            for col in back_cols:
                if col in df_display.columns:
                    display_cols.append(col)
            # 외국인, 캔들 체크박스 활성화 시 표시
            if apply_btn:
                if foreign_apply and '외국인 순매수' in df_display.columns:
                    display_cols.append('외국인 순매수')
                if candle_apply and '캔들' in df_display.columns:
                    display_cols.append('캔들')
        
        else:  # 전체
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '종가 (USD)', '시가총액 (KRW 억원)', '시가총액 (USD M)']:
                if col in df_display.columns:
                    display_cols.append(col)
            # 단기신호, 중기신호 추가
            if '단기신호' in df_display.columns:
                display_cols.append('단기신호')
            if '중기신호' in df_display.columns:
                display_cols.append('중기신호')
            # 9개 필터 항목 표시
            check_cols = [
                # 단기 필터
                'OBV 상승 크로스', '거래대금 급증(20일평균2배)', '돌파(20일 고가 or MA20 상향)',
                # 중기 필터
                'RSI 상승', 'OBV 우상향/크로스', '50MA > 200MA', '거래대금(20평균이상)',
                # 참고 필터
                '외국인 순매수', '캔들', '섹터'
            ]
            for col in check_cols:
                if col in df_display.columns:
                    display_cols.append(col)
        
        # 실제 존재하는 컬럼만 필터링
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
        
        # ========== KR 테이블 (페이지네이션) ==========
        if not df_kr_filtered.empty:
            # 페이지네이션 설정
            ITEMS_PER_PAGE = 100
            kr_total = len(df_kr_filtered)
            kr_total_pages = (kr_total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            
            # KR 통계 계산
            if period == "백데이터":
                kr_up = len(df_kr_filtered[df_kr_filtered['변동율%'] > 0]) if '변동율%' in df_kr_filtered.columns else 0
                kr_down = len(df_kr_filtered[df_kr_filtered['변동율%'] < 0]) if '변동율%' in df_kr_filtered.columns else 0
                kr_stats = f"총 종목수: {kr_total} · 상승: {kr_up} · 하락: {kr_down}"
            else:
                kr_stats = f"총 종목수: {kr_total}"
            
            # CSV용 컬럼 (전체 데이터)
            csv_columns_kr = display_cols.copy()
            df_kr_csv = df_kr_filtered[csv_columns_kr]
            csv_kr = df_kr_csv.to_csv(index=False).encode('utf-8-sig')
            
            # 헤더 + 정렬 UI
            col_kr_header1, col_kr_header2, col_kr_header3, col_kr_header4 = st.columns([1, 1.5, 1.5, 1])
            
            with col_kr_header1:
                st.markdown("#### 국내 (KR)")
            
            with col_kr_header2:
                st.markdown(f"**{kr_stats}**")
            
            with col_kr_header3:
                # 정렬 컬럼 선택 (복수 선택 가능)
                kr_display_cols = [col for col in display_cols if '(USD' not in col]
                sort_options = [col for col in kr_display_cols if col not in ['종목코드', '시장', '회사명', '업종', '업종트렌드']]
                if not sort_options:
                    sort_options = ['시가총액 (KRW 억원)']
                
                # 기본값: 시가총액
                if st.session_state.kr_sort_column not in sort_options:
                    st.session_state.kr_sort_column = '시가총액 (KRW 억원)' if '시가총액 (KRW 억원)' in sort_options else sort_options[0]
                
                # ✅ 복수 선택 가능한 정렬
                selected_sort = st.selectbox(
                    "정렬 (1순위)",
                    options=sort_options,
                    index=sort_options.index(st.session_state.kr_sort_column) if st.session_state.kr_sort_column in sort_options else 0,
                    key=f"kr_sort_col_{period}",
                    label_visibility="collapsed"
                )
                
                if selected_sort != st.session_state.kr_sort_column:
                    st.session_state.kr_sort_column = selected_sort
                    st.session_state.kr_page = 0
                    st.rerun()
            
            with col_kr_header4:
                col_sort_btn, col_download = st.columns([1, 1])
                
                with col_sort_btn:
                    # 오름차순/내림차순 토글
                    sort_icon = "🔼" if st.session_state.kr_sort_ascending else "🔽"
                    if st.button(sort_icon, key=f"kr_sort_dir_{period}", use_container_width=True):
                        st.session_state.kr_sort_ascending = not st.session_state.kr_sort_ascending
                        st.session_state.kr_page = 0
                        st.rerun()
                
                with col_download:
                    st.download_button(
                        label="💾",
                        data=csv_kr,
                        file_name=f'kr_stocks_{period}.csv',
                        mime='text/csv',
                        key=f"download_kr_{period}",
                        use_container_width=True
                    )
                        # 기본값: 시가총액 내림차순
            sort_by = [st.session_state.kr_sort_column]
            ascending = [st.session_state.kr_sort_ascending]
            
            # 2순위: 항상 시가총액으로 정렬 (1순위가 시가총액이 아닐 때)
            if st.session_state.kr_sort_column != '시가총액 (KRW 억원)' and '시가총액 (KRW 억원)' in df_kr_filtered.columns:
                sort_by.append('시가총액 (KRW 억원)')
                ascending.append(False)
            
            # 정렬 실행
            if all(col in df_kr_filtered.columns for col in sort_by):
                df_kr_filtered = df_kr_filtered.sort_values(
                    by=sort_by,
                    ascending=ascending
                )

            # ✅ 정렬 후 페이지 슬라이싱
            start_idx = st.session_state.kr_page * ITEMS_PER_PAGE
            end_idx = min(start_idx + ITEMS_PER_PAGE, kr_total)
            df_kr_page = df_kr_filtered.iloc[start_idx:end_idx].copy()
            
            # KR 전용 컬럼
            kr_display_cols = [col for col in display_cols if '(USD' not in col and '(N/A)' not in col]
            
            # ✅ 동적 높이 계산 (기존 방식 유지)
            kr_count = len(df_kr_filtered)
            kr_height = min(kr_count, 10) * 30 + 30
            
            # 테이블 데이터 준비
            df_kr_display_full = df_kr_page[kr_display_cols].copy().reset_index(drop=True)
            kr_sector_trends = df_kr_display_full['업종트렌드'].copy() if '업종트렌드' in df_kr_display_full.columns else None
            df_kr_display = df_kr_display_full.drop(columns=['업종트렌드'], errors='ignore')
            
            kr_key = f"kr_dataframe_{period}_page_{st.session_state.kr_page}"
            
            # 스타일 적용
            def apply_kr_row_style(row):
                styles = []
                bg_color = None
                if kr_sector_trends is not None and row.name < len(kr_sector_trends):
                    if pd.notna(kr_sector_trends.iloc[row.name]):
                        bg_color = get_sector_trend_color(kr_sector_trends.iloc[row.name])
                for _ in row.index:
                    if bg_color:
                        styles.append(f'background-color: {bg_color}')
                    else:
                        styles.append('')
                return styles
            
            styled_kr = df_kr_display.style.apply(apply_kr_row_style, axis=1)
            
            # 숫자 포맷 설정
            format_dict = {}
            for col in df_kr_display.columns:
                if df_kr_display[col].dtype in ['int64', 'float64']:
                    if col == '종가 (KRW)':
                        format_dict[col] = '{:,.0f}'
                    elif '시가총액' in col:
                        format_dict[col] = '{:,.2f}'
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
                use_container_width=True,
                height=kr_height,
                key=kr_key,
                column_config={
                    "종목코드": st.column_config.Column(width=50),
                    "시장": st.column_config.Column(width=40),
                    "회사명": st.column_config.Column(width="small"),
                    "업종": st.column_config.Column(width="small"),
                    "업종트렌드": st.column_config.Column(width="small"),
                    "종가 (KRW)": st.column_config.Column(width="small"),
                    "시가총액 (KRW 억원)": st.column_config.Column(width="small"),
                    "단기매수신호": st.column_config.Column(width=60),
                    "중기매수신호": st.column_config.Column(width=60),
                    "단기신호": st.column_config.Column(width=60),
                    "중기신호": st.column_config.Column(width=60),
                    "OBV 상승 크로스": st.column_config.Column(width=40),
                    "거래대금 급증(20일평균2배)": st.column_config.Column(width=40),
                    "돌파(20일 고가 or MA20 상향)": st.column_config.Column(width=40),
                    "RSI 상승": st.column_config.Column(width=40),
                    "OBV 우상향/크로스": st.column_config.Column(width=40),
                    "50MA > 200MA": st.column_config.Column(width=40),
                    "거래대금(20평균이상)": st.column_config.Column(width=40),
                    "RSI 과열(70 이상)": st.column_config.Column(width=40),
                    "RSI 하강 지속": st.column_config.Column(width=40),
                    "OBV 하락 크로스": st.column_config.Column(width=40),
                    "외국인 순매수(리버스)": st.column_config.Column(width=40),
                    "캔들(리버스)": st.column_config.Column(width=40),
                    "섹터(리버스)": st.column_config.Column(width=40),
                    "외국인 순매수": st.column_config.Column(width=40),
                    "캔들": st.column_config.Column(width=40),
                    "섹터": st.column_config.Column(width=40),
                    "업데이트": st.column_config.Column(width=60),
                    "타입": st.column_config.Column(width=50),
                    "최신종가": st.column_config.Column(width=60),
                    "최신업데이트": st.column_config.Column(width=60),
                    "변동율%": st.column_config.Column(width=40),
                    "매도신호": st.column_config.Column(width=60),
                }
            )
            
            # ✅ 페이지네이션 UI (테이블 아래)
            if kr_total_pages > 1:
                col_prev, col_page_info, col_next = st.columns([0.4, 3, 0.4])
                
                with col_prev:
                    if st.button("◀ 이전", key=f"kr_prev_{period}", disabled=st.session_state.kr_page == 0, use_container_width=True):
                        st.session_state.kr_page -= 1
                        st.rerun()
                
                with col_page_info:
                    st.markdown(
                        f"<div style='text-align: center; padding: 8px; font-weight: 600;'>"
                        f"{st.session_state.kr_page + 1} / {kr_total_pages} "
                        f"({start_idx + 1}-{end_idx} / {kr_total})"
                        f"</div>",
                        unsafe_allow_html=True
                    )
                
                with col_next:
                    if st.button("다음 ▶", key=f"kr_next_{period}", disabled=st.session_state.kr_page >= kr_total_pages - 1, use_container_width=True):
                        st.session_state.kr_page += 1
                        st.rerun()
            
            # 선택된 행 처리
            if event_kr.selection.rows:
                selected_idx = event_kr.selection.rows[0]
                actual_idx = start_idx + selected_idx
                new_symbol = df_kr_filtered.iloc[actual_idx]['종목코드']
                if new_symbol != st.session_state.selected_symbol or st.session_state.selected_market != 'KR':
                    st.session_state.selected_symbol = new_symbol
                    st.session_state.selected_market = 'KR'
                    st.rerun()
        
        # ========== US 테이블 (페이지네이션) ==========
        if not df_us_filtered.empty:
            # 페이지네이션 설정
            ITEMS_PER_PAGE = 100
            us_total = len(df_us_filtered)
            us_total_pages = (us_total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            
            # US 통계 계산
            if period == "백데이터":
                us_up = len(df_us_filtered[df_us_filtered['변동율%'] > 0]) if '변동율%' in df_us_filtered.columns else 0
                us_down = len(df_us_filtered[df_us_filtered['변동율%'] < 0]) if '변동율%' in df_us_filtered.columns else 0
                us_stats = f"총 종목수: {us_total} · 상승: {us_up} · 하락: {us_down}"
            else:
                us_stats = f"총 종목수: {us_total}"
            
            # CSV용 컬럼 (전체 데이터)
            csv_columns_us = display_cols.copy()
            df_us_csv = df_us_filtered[csv_columns_us]
            csv_us = df_us_csv.to_csv(index=False).encode('utf-8-sig')
            
            # 헤더 + 정렬 UI
            col_us_header1, col_us_header2, col_us_header3, col_us_header4 = st.columns([1, 1.5, 1.5, 1])
            
            with col_us_header1:
                st.markdown("#### 해외 (US)")
            
            with col_us_header2:
                st.markdown(f"**{us_stats}**")
            
            with col_us_header3:
                # 정렬 컬럼 선택 (복수 선택 가능)
                us_display_cols = [col for col in display_cols if '(KRW' not in col]
                sort_options = [col for col in us_display_cols if col not in ['종목코드', '시장', '회사명', '업종', '업종트렌드']]
                if not sort_options:
                    sort_options = ['시가총액 (USD M)']
                
                # 기본값: 시가총액
                if st.session_state.us_sort_column not in sort_options:
                    st.session_state.us_sort_column = '시가총액 (USD M)' if '시가총액 (USD M)' in sort_options else sort_options[0]
                
                # ✅ 복수 선택 가능한 정렬
                selected_sort = st.selectbox(
                    "정렬 (1순위)",
                    options=sort_options,
                    index=sort_options.index(st.session_state.us_sort_column) if st.session_state.us_sort_column in sort_options else 0,
                    key=f"us_sort_col_{period}",
                    label_visibility="collapsed"
                )
                
                if selected_sort != st.session_state.us_sort_column:
                    st.session_state.us_sort_column = selected_sort
                    st.session_state.us_page = 0
                    st.rerun()
            
            with col_us_header4:
                col_sort_btn, col_download = st.columns([1, 1])
                
                with col_sort_btn:
                    # 오름차순/내림차순 토글
                    sort_icon = "🔼" if st.session_state.us_sort_ascending else "🔽"
                    if st.button(sort_icon, key=f"us_sort_dir_{period}", use_container_width=True):
                        st.session_state.us_sort_ascending = not st.session_state.us_sort_ascending
                        st.session_state.us_page = 0
                        st.rerun()
                
                with col_download:
                    st.download_button(
                        label="💾",
                        data=csv_us,
                        file_name=f'us_stocks_{period}.csv',
                        mime='text/csv',
                        key=f"download_us_{period}",
                        use_container_width=True
                    )

            # 기본값: 시가총액 내림차순
            sort_by = [st.session_state.us_sort_column]
            ascending = [st.session_state.us_sort_ascending]
            
            # 2순위: 항상 시가총액으로 정렬 (1순위가 시가총액이 아닐 때)
            if st.session_state.us_sort_column != '시가총액 (USD M)' and '시가총액 (USD M)' in df_us_filtered.columns:
                sort_by.append('시가총액 (USD M)')
                ascending.append(False)
            
            # 정렬 실행
            if all(col in df_us_filtered.columns for col in sort_by):
                df_us_filtered = df_us_filtered.sort_values(
                    by=sort_by,
                    ascending=ascending
                )
            
            # ✅ 정렬 후 페이지 슬라이싱
            start_idx = st.session_state.us_page * ITEMS_PER_PAGE
            end_idx = min(start_idx + ITEMS_PER_PAGE, us_total)
            df_us_page = df_us_filtered.iloc[start_idx:end_idx].copy()
            
            # US 전용 컬럼
            us_display_cols = [col for col in display_cols if '(KRW' not in col and '(주)' not in col]
            
            # ✅ 동적 높이 계산 (기존 방식 유지)
            us_count = len(df_us_filtered)
            us_height = min(us_count, 10) * 30 + 30
            
            # 테이블 데이터 준비
            df_us_display_full = df_us_page[us_display_cols].copy().reset_index(drop=True)
            us_sector_trends = df_us_display_full['업종트렌드'].copy() if '업종트렌드' in df_us_display_full.columns else None
            df_us_display = df_us_display_full.drop(columns=['업종트렌드'], errors='ignore')
            
            us_key = f"us_dataframe_{period}_page_{st.session_state.us_page}"
            
            # 스타일 적용
            def apply_us_row_style(row):
                styles = []
                bg_color = None
                if us_sector_trends is not None and row.name < len(us_sector_trends):
                    if pd.notna(us_sector_trends.iloc[row.name]):
                        bg_color = get_sector_trend_color(us_sector_trends.iloc[row.name])
                for _ in row.index:
                    if bg_color:
                        styles.append(f'background-color: {bg_color}')
                    else:
                        styles.append('')
                return styles
            
            styled_us = df_us_display.style.apply(apply_us_row_style, axis=1)
            
            # 숫자 포맷 설정
            format_dict = {}
            for col in df_us_display.columns:
                if df_us_display[col].dtype in ['int64', 'float64']:
                    if col == '종가 (USD)':
                        format_dict[col] = '${:,.2f}'
                    elif '시가총액' in col:
                        format_dict[col] = '{:,.2f}'
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
                use_container_width=True,
                height=us_height,
                key=us_key,
                column_config={
                    "종목코드": st.column_config.Column(width=50),
                    "시장": st.column_config.Column(width=40),
                    "회사명": st.column_config.Column(width="small"),
                    "업종": st.column_config.Column(width="small"),
                    "업종트렌드": st.column_config.Column(width="small"),
                    "종가 (USD)": st.column_config.Column(width="small"),
                    "시가총액 (USD M)": st.column_config.Column(width="small"),
                    "단기매수신호": st.column_config.Column(width=60),
                    "중기매수신호": st.column_config.Column(width=60),
                    "단기신호": st.column_config.Column(width=60),
                    "중기신호": st.column_config.Column(width=60),
                    "OBV 상승 크로스": st.column_config.Column(width=40),
                    "거래대금 급증(20일평균2배)": st.column_config.Column(width=40),
                    "돌파(20일 고가 or MA20 상향)": st.column_config.Column(width=40),
                    "RSI 상승": st.column_config.Column(width=40),
                    "OBV 우상향/크로스": st.column_config.Column(width=40),
                    "50MA > 200MA": st.column_config.Column(width=40),
                    "거래대금(20평균이상)": st.column_config.Column(width=40),
                    "RSI 과열(70 이상)": st.column_config.Column(width=40),
                    "RSI 하강 지속": st.column_config.Column(width=40),
                    "OBV 하락 크로스": st.column_config.Column(width=40),
                    "외국인 순매수(리버스)": st.column_config.Column(width=40),
                    "캔들(리버스)": st.column_config.Column(width=40),
                    "섹터(리버스)": st.column_config.Column(width=40),
                    "외국인 순매수": st.column_config.Column(width=40),
                    "캔들": st.column_config.Column(width=40),
                    "섹터": st.column_config.Column(width=40),
                    "업데이트": st.column_config.Column(width=60),
                    "타입": st.column_config.Column(width=50),
                    "최신종가": st.column_config.Column(width=60),
                    "최신업데이트": st.column_config.Column(width=60),
                    "변동율%": st.column_config.Column(width=40),
                    "매도신호": st.column_config.Column(width=60),
                }
            )
            
            # ✅ 페이지네이션 UI (테이블 아래)
            if us_total_pages > 1:
                col_prev, col_page_info, col_next = st.columns([0.4, 3, 0.4])
                
                with col_prev:
                    if st.button("◀ 이전", key=f"us_prev_{period}", disabled=st.session_state.us_page == 0, use_container_width=True):
                        st.session_state.us_page -= 1
                        st.rerun()
                
                with col_page_info:
                    st.markdown(
                        f"<div style='text-align: center; padding: 8px; font-weight: 600;'>"
                        f"{st.session_state.us_page + 1} / {us_total_pages} "
                        f"({start_idx + 1}-{end_idx} / {us_total})"
                        f"</div>",
                        unsafe_allow_html=True
                    )
                
                with col_next:
                    if st.button("다음 ▶", key=f"us_next_{period}", disabled=st.session_state.us_page >= us_total_pages - 1, use_container_width=True):
                        st.session_state.us_page += 1
                        st.rerun()
            
            # 선택된 행 처리
            if event_us.selection.rows:
                selected_idx = event_us.selection.rows[0]
                actual_idx = start_idx + selected_idx
                new_symbol = df_us_filtered.iloc[actual_idx]['종목코드']
                if new_symbol != st.session_state.selected_symbol or st.session_state.selected_market != 'US':
                    st.session_state.selected_symbol = new_symbol
                    st.session_state.selected_market = 'US'
                    st.rerun()
        
        if df_kr_filtered.empty and df_us_filtered.empty:
            st.info("조건에 맞는 종목이 없습니다.")
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
                st.markdown(f"**코드**: {symbol} · **시장**: {market} · **업종**: {row.get('업종', 'N/A')}")
                
                if '업종트렌드' in row:
                    trend_text = row['업종트렌드']
                    bg_color = get_sector_trend_color(trend_text)
                    
                    if bg_color:
                        st.markdown(
                            f"<div style='background-color: {bg_color}; padding: 8px 12px; border-radius: 6px; margin: 4px 0;'>"
                            f"<strong>업종트렌드</strong>: {trend_text}"
                            f"</div>",
                            unsafe_allow_html=True
                        )
                    else:
                        st.markdown(f"**업종트렌드**: {trend_text}")
                
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
                    
                    # MA20 / MA200
                    ind_data = get_indicator_data(symbol, market)
                    if ind_data is not None and 'ma20_latest' in ind_data and 'ma200_latest' in ind_data:
                        if pd.notna(ind_data['ma20_latest']) and pd.notna(ind_data['ma200_latest']):
                            st.metric("MA20 / MA200", f"{ind_data['ma20_latest']:.2f} / {ind_data['ma200_latest']:.2f}")
                
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
                    
                    # 외국인 순매수 5일치 + 합산 (플러스 빨간색, 마이너스 파란색)
                    if market == 'KR':
                        if all(k in row for k in ['외국인순매수_5일전 (주)', '외국인순매수_4일전 (주)', '외국인순매수_3일전 (주)', '외국인순매수_2일전 (주)', '외국인순매수_1일전 (주)', '외국인순매수_합산 (주)']):
                            f5 = int(row['외국인순매수_5일전 (주)'])
                            f4 = int(row['외국인순매수_4일전 (주)'])
                            f3 = int(row['외국인순매수_3일전 (주)'])
                            f2 = int(row['외국인순매수_2일전 (주)'])
                            f1 = int(row['외국인순매수_1일전 (주)'])
                            f_sum = int(row['외국인순매수_합산 (주)'])
                            
                            def format_foreign(val):
                                if val > 0:
                                    return f"<span style='color: #dc2626;'>{val:,}</span>"
                                elif val < 0:
                                    return f"<span style='color: #2563eb;'>{val:,}</span>"
                                else:
                                    return f"{val:,}"
                            
                            st.markdown("**외국인 순매수(5일)**")
                            st.markdown(
                                f"<div style='font-size: 1.1rem; font-weight: 800;'>"
                                f"{format_foreign(f_sum)}({format_foreign(f3)} / {format_foreign(f2)} / {format_foreign(f1)})"
                                f"</div>",
                                unsafe_allow_html=True
                            )
                
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