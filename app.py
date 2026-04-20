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
from datetime import datetime, timedelta
import numpy as np
import warnings

def get_sector_trend_color(trend_text):
    import re
    if not trend_text or pd.isna(trend_text):
        return None
    match = re.search(r'([+-]?\d+\.?\d*)%', str(trend_text))
    if not match:
        return None
    try:
        percent = float(match.group(1))
    except:
        return None
    if percent >= 12:
        return "rgba(220, 38, 38, 0.30)"
    elif percent >= 9:
        return "rgba(220, 38, 38, 0.25)"
    elif percent >= 6:
        return "rgba(239, 68, 68, 0.2)"
    elif percent >= 3:
        return "rgba(248, 113, 113, 0.15)"
    elif percent > 0:
        return "rgba(252, 165, 165, 0.1)"
    elif percent <= -12:
        return "rgba(37, 99, 235, 0.30)"
    elif percent <= -9:
        return "rgba(37, 99, 235, 0.25)"
    elif percent <= -6:
        return "rgba(59, 130, 246, 0.20)"
    elif percent <= -3:
        return "rgba(96, 165, 250, 0.15)"
    elif percent < 0:
        return "rgba(147, 197, 253, 0.1)"
    else:
        return None

def get_sector_check(trend_text):
    import re
    if pd.isna(trend_text) or trend_text == 'N/A':
        return '❌'
    match = re.search(r'([+-]?\d+\.?\d*)%', str(trend_text))
    if match:
        try:
            percent = float(match.group(1))
            return '✅' if percent > 0 else '❌'
        except:
            return '❌'
    return '❌'

def _safe_market_cap_to_억원(val):
    v = float(val) if pd.notna(val) else 0.0
    if v >= 1e6:
        return v / 1e8
    return v

st.set_page_config(page_title="Trading Copilot 🚀", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    /* ── 기본 폰트 (모바일/데스크탑 공통 고정) ── */
    html, body, [class*="css"] { font-size: 13px !important; }
    .main { background: var(--background-color) !important; }

    /* ── 데이터프레임: 화면 너비에 맞게 유동적으로 ── */
    [data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
        font-size: 0.8rem !important;
        width: 100% !important;
        max-width: 100% !important;
    }
    [data-testid="stDataFrame"] > div {
        width: 100% !important;
    }

    /* ── 사이드바 ── */
    [data-testid="stSidebar"] {
        border-right: 1px solid rgba(128,128,128,.2) !important;
        overflow-y: auto !important;
    }
    [data-testid="stSidebar"] * { color: var(--text-color) !important; }
    [data-testid="stSidebar"] label { color: var(--text-color) !important; opacity: 1 !important; }
    [data-testid="stSidebar"] p { color: var(--text-color) !important; }
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] h4 { color: var(--text-color) !important; }
    [data-testid="stSidebar"] .stButton button { color: var(--text-color) !important; }
    [data-testid="stSidebar"] .stCheckbox label,
    [data-testid="stSidebar"] .stRadio label { color: var(--text-color) !important; font-weight: 500 !important; }
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
    [data-testid="stSidebar"] .stSelectbox label { color: var(--text-color) !important; font-weight: 600 !important; }
    [data-testid="stSidebar"] .element-container { margin: 0 !important; padding: 0 !important; }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] { margin: 0 !important; padding: 0 !important; }
    [data-testid="stSidebar"] .stCheckbox {
        margin: 0 !important; padding: 0px 8px !important;
        border: none !important; background: transparent !important;
    }
    [data-testid="stSidebar"] .stSelectbox { margin: 0.05rem 0 !important; }
    [data-testid="stSidebar"] .stRadio { margin: 0.05rem 0 !important; padding-left: 0.3rem !important; }
    [data-testid="stSidebar"] .stRadio > div { padding-left: 0.2rem !important; }
    [data-testid="stSidebar"] .stRadio label { padding-left: 0.3rem !important; }
    [data-testid="stSidebar"] hr { margin: 0.1rem 0 !important; }
    [data-testid="stSidebar"] .stCheckbox label {
        display: flex !important; align-items: center !important;
        padding: 0px 0 !important; margin: 0 !important;
    }
    [data-testid="stSidebar"] .stCheckbox > div { padding: 0 !important; margin: 0 !important; }
    [data-testid="stSidebar"] .stCheckbox + .stCheckbox { margin-top: -10px !important; }
    [data-testid="stSidebar"] .stButton button {
        display: flex !important; align-items: center !important; justify-content: center !important;
    }

    /* ── expander ── */
    .streamlit-expander { margin: 0 !important; padding: 0 !important; }
    .streamlit-expanderHeader { padding: 2px 8px !important; margin: 0 !important; font-size: 0.85rem !important; }
    .streamlit-expanderContent { padding: 2px 8px !important; margin: 0 !important; }
    .streamlit-expanderContent .stCheckbox { margin-top: -5px !important; }

    /* ── 헤딩 (고정 크기) ── */
    h1 { font-weight: 800; letter-spacing: 0.2px; font-size: 1.8rem !important; }
    h2 { font-weight: 800; letter-spacing: 0.2px; font-size: 1.4rem !important; }
    h3 { font-weight: 800; letter-spacing: 0.2px; font-size: 1.1rem !important; }
    h4 { font-weight: 700; letter-spacing: 0.2px; font-size: 0.95rem !important; margin-bottom: 0.4rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.3rem !important; font-weight: 1000; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; }

    /* ── 버튼 (고정 크기) ── */
    .stButton>button {
        border-radius: 12px; font-weight: 900; font-size: 0.85rem !important;
        transition: 0.15s ease; padding: 0.35rem 0.7rem;
        min-height: 36px !important;
    }
    [data-testid="stSidebar"] .stButton button[kind="primary"] {
        background-color: rgba(239, 68, 68, 0.7) !important;
        border-color: rgba(239, 68, 68, 0.5) !important;
    }
    [data-testid="stSidebar"] .stButton button[kind="primary"]:hover {
        background-color: rgba(239, 68, 68, 0.8) !important;
        border-color: rgba(239, 68, 68, 0.7) !important;
    }
    [data-testid="stSidebar"] .stButton button[kind="primary"]:active {
        background-color: rgba(239, 68, 68, 0.9) !important;
        border-color: rgba(239, 68, 68, 0.9) !important;
    }

    /* ── 기타 컴포넌트 ── */
    .stCheckbox { padding: 5px 8px; border-radius: 12px; margin-bottom: 0.2rem; font-size: 0.85rem !important; }
    .stSelectbox>div>div { border-radius: 12px; font-size: 0.85rem !important; }
    .stRadio > div { gap: 0.2rem !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 999px; padding: 6px 10px; font-weight: 1000; font-size: 0.8rem !important;
    }
    .stTextInput>div>div>input { border-radius: 12px; font-size: 0.85rem !important; }
    .stInfo { border-radius: 14px; font-size: 0.85rem !important; }
    .stWarning { border-radius: 14px; font-size: 0.85rem !important; }
    .streamlit-expanderHeader { font-size: 0.85rem !important; }
    hr { margin: 0.5rem 0 !important; }

    /* ── 비활성화 체크박스 ── */
    [data-testid="stSidebar"] .stCheckbox:has(input:disabled) {
        opacity: 0.4 !important; background: rgba(128, 128, 128, 0.1) !important;
        border: 1px dashed rgba(128, 128, 128, 0.3) !important; pointer-events: none !important;
    }
    [data-testid="stSidebar"] .stCheckbox:has(input:disabled) label {
        cursor: not-allowed !important; color: rgba(128, 128, 128, 0.5) !important;
    }
    [data-testid="stSidebar"] .stCheckbox:has(input:disabled):hover {
        background: rgba(128, 128, 128, 0.15) !important;
    }

    /* ── 필터 비활성화 안내 ── */
    .filter-disabled-notice {
        background: rgba(255, 193, 7, 0.1) !important;
        border-left: 3px solid #ffc107 !important;
        padding: 8px 12px !important; border-radius: 8px !important;
        margin: 8px 0 !important; font-size: 0.75rem !important;
        color: var(--text-color) !important;
    }

    /* ── CSV 버튼 고정 ── */
    .csv-btn-fixed > div > button {
        min-width: 80px !important;
        max-width: 80px !important;
        width: 80px !important;
        white-space: nowrap !important;
        overflow: hidden !important;
    }

    /* ── 모든 버튼 줄바꿈 방지 ── */
    .stButton > button {
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
    }
    .stDownloadButton > button {
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        font-size: 0.85rem !important;
    }

    /* ── 토글 크기 고정 + 줄바꿈 방지 ── */
    .stToggle {
        white-space: nowrap !important;
    }
    .stToggle label {
        font-size: 0.85rem !important;
        white-space: nowrap !important;
    }
    .stToggle > label > div {
        white-space: nowrap !important;
    }

    /* ── number_input 크기 줄이기 ── */
    .page-input-small input[type="number"] {
        font-size: 0.85rem !important;
        padding: 4px 6px !important;
        height: 36px !important;
    }
    .page-input-small > div > div > div > input {
        font-size: 0.85rem !important;
        height: 36px !important;
    }

    /* ── selectbox 높이 맞추기 ── */
    .stSelectbox > div > div {
        min-height: 36px !important;
        font-size: 0.85rem !important;
    }

    /* ── 모바일 반응형 (768px 이하) ── */
    @media (max-width: 768px) {
        /* 메인 패딩 줄이기 */
        .main .block-container {
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
            padding-top: 0.5rem !important;
        }
        /* 헤더 박스 패딩 줄이기 */
        .main-header-box {
            padding: 12px 14px !important;
        }
        /* 컬럼 간격 줄이기 */
        [data-testid="column"] {
            padding: 0 2px !important;
        }
        /* 탭 폰트 유지하되 패딩 줄이기 */
        .stTabs [data-baseweb="tab"] {
            padding: 4px 8px !important;
        }
        /* 데이터프레임 가로 스크롤 허용 */
        [data-testid="stDataFrame"] {
            overflow-x: auto !important;
        }
    }
</style>

<script>
(function() {
    function fixSidebarBackground() {
        const sidebar = document.querySelector('[data-testid="stSidebar"]');
        if (!sidebar) { setTimeout(fixSidebarBackground, 100); return; }
        const root = document.documentElement;
        const bgColor = getComputedStyle(root).getPropertyValue('--secondary-background-color').trim();
        const match = bgColor.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
        if (match) {
            sidebar.style.backgroundColor = `rgb(${match[1]}, ${match[2]}, ${match[3]})`;
        } else {
            const isDark = getComputedStyle(root).getPropertyValue('--text-color').includes('250');
            sidebar.style.backgroundColor = isDark ? '#0e1117' : '#ffffff';
        }
    }
    fixSidebarBackground();
    const observer = new MutationObserver(fixSidebarBackground);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme', 'class', 'style'] });
    if (document.readyState === 'loading') { document.addEventListener('DOMContentLoaded', fixSidebarBackground); }
})();
</script>
""", unsafe_allow_html=True)

warnings.filterwarnings("ignore", message=".*keyword arguments.*deprecated.*config.*")
warnings.filterwarnings("ignore", category=FutureWarning, message=".*to_pydatetime.*")
warnings.filterwarnings("ignore", category=UserWarning, module="pykrx")

@st.cache_data(ttl=3600)
def load_data():
    DB_PATH = "data/meta/universe.db"
    if not os.path.exists(DB_PATH):
        st.warning("데이터 없음 – 배치 실행하세요.")
        return pd.DataFrame()
    con = get_db_connection()
    if con is None:
        st.warning("데이터 없음 – 배치 실행하세요.")
        return pd.DataFrame()
    return con.execute("SELECT * FROM indicators").fetchdf()

@st.cache_resource
def get_db_connection():
    DB_PATH = "data/meta/universe.db"
    if not os.path.exists(DB_PATH):
        return None
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
        st.warning(f"이름 로드 에러: {e}")
        df = df.copy()
        df['name'] = 'N/A'
        return df

@st.cache_data
def load_meta():
    META_FILE = "data/meta/tickers_meta.json"
    if os.path.exists(META_FILE):
        with open(META_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'KOSPI': {}, 'KOSDAQ': {}}

KR_MARKETS = {'KR', 'KOSPI', 'KOSDAQ'}

def get_meta_info(meta, symbol, market):
    if market in ('KOSPI', 'KOSDAQ'):
        return meta.get(market, {}).get(symbol, {})
    else:
        info = meta.get('KOSPI', {}).get(symbol, {})
        if not info:
            info = meta.get('KOSDAQ', {}).get(symbol, {})
        if not info:
            info = meta.get('KR', {}).get(symbol, {})
        return info

def get_daily_path(symbol, market):
    base_dir = "data"
    if market == 'KOSPI':
        return os.path.join(base_dir, 'kr_daily', 'kospi', f"{symbol}.csv")
    elif market == 'KOSDAQ':
        return os.path.join(base_dir, 'kr_daily', 'kosdaq', f"{symbol}.csv")
    else:
        root_path = os.path.join(base_dir, 'kr_daily', f"{symbol}.csv")
        if os.path.exists(root_path):
            return root_path
        kospi_path = os.path.join(base_dir, 'kr_daily', 'kospi', f"{symbol}.csv")
        if os.path.exists(kospi_path):
            return kospi_path
        kosdaq_path = os.path.join(base_dir, 'kr_daily', 'kosdaq', f"{symbol}.csv")
        if os.path.exists(kosdaq_path):
            return kosdaq_path
        return root_path

# =============================================
# ✅ 벡터화된 함수들 (apply → list comprehension)
# =============================================

@st.cache_data(ttl=3600)
def add_foreign_net_buy(df):
    if df.empty or 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()

    all_meta = {}
    for mkt in ('KOSPI', 'KOSDAQ'):
        for sym, info in meta.get(mkt, {}).items():
            all_meta[(sym, mkt)] = info.get('foreign_net_buy', [0, 0, 0, 0, 0])

    keys = list(zip(df['symbol'], df['market']))
    fnb_list = [all_meta.get(k, [0, 0, 0, 0, 0]) for k in keys]

    df['foreign_net_buy_1ago'] = [fnb[0] if len(fnb) > 0 else 0 for fnb in fnb_list]
    df['foreign_net_buy_2ago'] = [fnb[1] if len(fnb) > 1 else 0 for fnb in fnb_list]
    df['foreign_net_buy_3ago'] = [fnb[2] if len(fnb) > 2 else 0 for fnb in fnb_list]
    df['foreign_net_buy_4ago'] = [fnb[3] if len(fnb) > 3 else 0 for fnb in fnb_list]
    df['foreign_net_buy_5ago'] = [fnb[4] if len(fnb) > 4 else 0 for fnb in fnb_list]
    df['foreign_net_buy_sum']  = [sum(fnb) for fnb in fnb_list]
    return df

@st.cache_data(ttl=3600)
def add_institutional_net_buy(df):
    if df.empty or 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()

    all_meta = {}
    for mkt in ('KOSPI', 'KOSDAQ'):
        for sym, info in meta.get(mkt, {}).items():
            all_meta[(sym, mkt)] = info.get('institutional_net_buy', [0, 0, 0, 0, 0])

    keys = list(zip(df['symbol'], df['market']))
    inb_list = [all_meta.get(k, [0, 0, 0, 0, 0]) for k in keys]

    df['institutional_net_buy_1ago'] = [inb[0] if len(inb) > 0 else 0 for inb in inb_list]
    df['institutional_net_buy_2ago'] = [inb[1] if len(inb) > 1 else 0 for inb in inb_list]
    df['institutional_net_buy_3ago'] = [inb[2] if len(inb) > 2 else 0 for inb in inb_list]
    df['institutional_net_buy_4ago'] = [inb[3] if len(inb) > 3 else 0 for inb in inb_list]
    df['institutional_net_buy_5ago'] = [inb[4] if len(inb) > 4 else 0 for inb in inb_list]
    df['institutional_net_buy_sum']  = [sum(inb) for inb in inb_list]
    return df

@st.cache_data(ttl=3600)
def add_ownership(df):
    if df.empty or 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()

    all_meta = {}
    for mkt in ('KOSPI', 'KOSDAQ'):
        for sym, info in meta.get(mkt, {}).items():
            all_meta[(sym, mkt)] = info.get('ownership_foreign_institution', 0.0)

    keys = list(zip(df['symbol'], df['market']))
    df['ownership_foreign_institution'] = [all_meta.get(k, 0.0) for k in keys]
    return df

@st.cache_data(ttl=3600)
def add_close_price(df):
    if df.empty or 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()

    all_meta = {}
    for mkt in ('KOSPI', 'KOSDAQ'):
        for sym, info in meta.get(mkt, {}).items():
            all_meta[(sym, mkt)] = info.get('close', 0.0)

    keys = list(zip(df['symbol'], df['market']))
    df['close'] = [all_meta.get(k, 0.0) for k in keys]
    return df

def calculate_buy_signals(df):
    if df.empty:
        return df
    import re
    df = df.copy()
    df['foreign_sum'] = df['foreign_net_buy_sum'] if 'foreign_net_buy_sum' in df.columns else 0
    df['institutional_sum'] = df['institutional_net_buy_sum'] if 'institutional_net_buy_sum' in df.columns else 0
    if 'upper_closes' in df.columns and 'lower_closes' in df.columns:
        df['candle_bullish'] = df['upper_closes'] > df['lower_closes']
        df['candle_bearish'] = df['lower_closes'] >= df['upper_closes']
    else:
        df['candle_bullish'] = False
        df['candle_bearish'] = False

    def check_sector_positive(trend_text):
        if pd.isna(trend_text): return False
        match = re.search(r'([+-]?\d+\.?\d*)%', str(trend_text))
        if match:
            try: return float(match.group(1)) > 0
            except: return False
        return False

    def check_sector_negative(trend_text):
        if pd.isna(trend_text): return False
        match = re.search(r'([+-]?\d+\.?\d*)%', str(trend_text))
        if match:
            try: return float(match.group(1)) < 0
            except: return False
        return False

    if 'sector_trend' in df.columns:
        df['sector_positive'] = df['sector_trend'].apply(check_sector_positive)
        df['sector_negative'] = df['sector_trend'].apply(check_sector_negative)
    else:
        df['sector_positive'] = False
        df['sector_negative'] = False

    df['short_obv_cross'] = df.get('obv_bullish_cross', False)
    df['short_trading'] = df.get('trading_surge_2x', False)
    df['short_break'] = df.get('breakout', False)
    df['short_foreign'] = df['foreign_sum'] > 0
    df['short_institutional'] = df['institutional_sum'] > 0
    df['short_candle'] = df['candle_bullish']
    df['short_sector'] = df['sector_positive']

    df['단기매수신호'] = (
        df['short_obv_cross'].astype(int) + df['short_trading'].astype(int) +
        df['short_break'].astype(int) + df['short_foreign'].astype(int) +
        df['short_institutional'].astype(int) + df['short_candle'].astype(int) +
        df['short_sector'].astype(int)
    )

    df['mid_rsi'] = df.get('rsi_3up', False)
    df['mid_obv'] = df.get('obv_mid_condition', False)
    df['mid_golden'] = df.get('ma50_above_200', False)
    df['mid_trading'] = df.get('trading_above_avg', False)
    df['mid_foreign'] = df['foreign_sum'] > 0
    df['mid_institutional'] = df['institutional_sum'] > 0
    df['mid_candle'] = df['candle_bullish']
    df['mid_sector'] = df['sector_positive']

    df['중기매수신호'] = (
        df['mid_rsi'].astype(int) + df['mid_obv'].astype(int) +
        df['mid_golden'].astype(int) + df['mid_trading'].astype(int) +
        df['mid_foreign'].astype(int) + df['mid_institutional'].astype(int) +
        df['mid_candle'].astype(int) + df['mid_sector'].astype(int)
    )

    df['sell_rsi_overbought'] = df.get('rsi_overbought', False)
    df['sell_rsi_down'] = df.get('rsi_3down', False)
    df['sell_obv_cross'] = df.get('obv_bearish_cross', False)
    df['sell_foreign'] = df['foreign_sum'] < 0
    df['sell_institutional'] = df['institutional_sum'] < 0
    df['sell_candle'] = df['candle_bearish']
    df['sell_sector'] = df['sector_negative']

    df['매도신호'] = (
        df['sell_rsi_overbought'].astype(int) + df['sell_rsi_down'].astype(int) +
        df['sell_obv_cross'].astype(int) + df['sell_foreign'].astype(int) +
        df['sell_institutional'].astype(int) + df['sell_candle'].astype(int) +
        df['sell_sector'].astype(int)
    )
    return df

def format_buy_signal(score, signal_type):
    if pd.isna(score): return ''
    score = int(score)
    if signal_type == 'short':
        if score == 7: return f'🟣 {score}점'
        elif score >= 5: return f'🔵 {score}점'
        elif score >= 3: return f'🟢 {score}점'
        else: return str(score)
    elif signal_type == 'mid':
        if score == 8: return f'🟣 {score}점'
        elif score >= 6: return f'🔵 {score}점'
        elif score >= 4: return f'🟢 {score}점'
        else: return str(score)
    elif signal_type == 'all_short':
        if score == 7: return f'🟣 {score}점'
        elif score >= 5: return f'🔵 {score}점'
        elif score >= 3: return f'🟢 {score}점'
        elif score >= 2: return f'🟡 {score}점'
        else: return f'🔴 {score}점'
    elif signal_type == 'all_mid':
        if score == 8: return f'🟣 {score}점'
        elif score >= 6: return f'🔵 {score}점'
        elif score >= 4: return f'🟢 {score}점'
        elif score >= 2: return f'🟡 {score}점'
        else: return f'🔴 {score}점'
    return str(score)

def run_screener_query(con, filter_condition="all", top_n=None, additional_filters=None):
    try:
        con.execute("SELECT 1").fetchone()
    except:
        con = get_db_connection()
        st.session_state.con = con

    market_filter = "market IN ('KR', 'KOSPI', 'KOSDAQ')"

    if filter_condition == "short_term":
        condition = """(obv_latest > signal_obv_9_latest AND obv_1ago <= signal_obv_9_1ago)
                       AND (today_trading_value >= 2.0 * avg_trading_value_20d)
                       AND (break_20high = 1 OR (close_latest > ma20_latest AND close_1ago <= ma20_1ago))"""
    elif filter_condition == "mid_term":
        condition = """(rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest >= 40 AND rsi_d_latest <= 60)
                       AND (obv_latest > signal_obv_20_latest AND
                            (signal_obv_20_latest > signal_obv_20_3ago OR
                             (obv_2ago <= signal_obv_20_2ago AND obv_latest > signal_obv_20_latest) OR
                             (obv_1ago <= signal_obv_20_1ago AND obv_latest > signal_obv_20_latest)))
                       AND (ma50_latest > ma200_latest)
                       AND (today_trading_value >= avg_trading_value_20d)"""
    elif filter_condition == "sell":
        condition = """(rsi_d_latest >= 70)
                       OR (obv_latest < signal_obv_9_latest AND obv_1ago >= signal_obv_9_1ago)
                       OR (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50)"""
    elif filter_condition == "all":
        condition = "1=1"
    else:
        condition = "1=1"

    additional_condition = ""
    if additional_filters:
        for key, value in additional_filters.items():
            if value and key == "candle":
                additional_condition += " AND upper_closes >= 3"

    query = f"""
    WITH parsed AS (
        SELECT symbol, market,
            rsi_d, macd_d, signal_d, obv_d, signal_obv_9d, signal_obv_20d,
            market_cap, avg_trading_value_20d, today_trading_value, turnover,
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
        rsi_d AS rsi_d_array, macd_d AS macd_array, signal_d AS signal_array,
        obv_d AS obv_array, signal_obv_9d AS signal_obv_9_array, signal_obv_20d AS signal_obv_20_array,
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
    WHERE {market_filter} AND {condition} {additional_condition}
    ORDER BY market_cap DESC
    """
    df = con.execute(query).fetchdf()
    if top_n:
        df = df.head(top_n)
    return df

def format_dataframe(df, market_type):
    if market_type == 'KR':
        df = df.rename(columns={
            '시가총액': '시가총액 (KRW 억원)', '20일평균거래대금': '20일평균거래대금 (KRW 억원)',
            '오늘거래대금': '오늘거래대금 (KRW 억원)', '회전율': '회전율 (%)',
            'PER_TTM': 'PER_TTM (x)', '종가': '종가 (KRW)',
            '외국인순매수_5일전': '외국인순매수_5일전 (주)', '외국인순매수_4일전': '외국인순매수_4일전 (주)',
            '외국인순매수_3일전': '외국인순매수_3일전 (주)', '외국인순매수_2일전': '외국인순매수_2일전 (주)',
            '외국인순매수_1일전': '외국인순매수_1일전 (주)', '외국인순매수_합산': '외국인순매수_합산 (주)',
            'sector': '업종', 'sector_trend': '업종트렌드',
        })

    def safe_float(x): return float(x) if pd.notna(x) else 0.0

    for col_name, divisor in [
        ('시가총액 (KRW 억원)', None),
        ('20일평균거래대금 (KRW 억원)', 1e8),
        ('오늘거래대금 (KRW 억원)', 1e8),
    ]:
        if col_name in df.columns:
            df[col_name] = df[col_name].apply(safe_float)
            if divisor:
                df[col_name] = df[col_name] / divisor

    if '회전율 (%)' in df.columns:
        df['회전율 (%)'] = df['회전율 (%)'].apply(safe_float) * 100
    for col in ['PER_TTM (x)', 'EPS_TTM', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가 (KRW)']:
        if col in df.columns:
            df[col] = df[col].apply(safe_float)

    foreign_cols = [col for col in df.columns if col.startswith('외국인순매수_')]
    for col in foreign_cols:
        df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else 0)

    def bool_fmt(x): return '✅' if x else '❌'
    bool_cols = [col for col in df.columns if col in [
        'OBV 상승 크로스', '거래대금 급증(20일평균2배)', '돌파(20일 고가 or MA20 상향)',
        'RSI 상승', 'OBV 우상향/크로스', '50MA > 200MA', '거래대금(20평균이상)',
        'RSI 과열(70 이상)', 'RSI 하강 지속', 'OBV 하락 크로스'
    ]]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(bool_fmt)

    numeric_cols = df.select_dtypes(include='float').columns.drop('회전율 (%)', errors='ignore')
    df[numeric_cols] = df[numeric_cols].round(2)
    return df

@st.cache_data(ttl=3600)
def load_daily_data(symbol, market):
    daily_path = get_daily_path(symbol, market)
    if not os.path.exists(daily_path):
        return None
    df = pd.read_csv(daily_path, index_col=0, parse_dates=True)
    return df.rename(columns={'시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'})

def show_chart(symbol, market, chart_type):
    df_chart = load_daily_data(symbol, market)
    if df_chart is None:
        st.warning("데이터 없음")
        return

    period = st.session_state.chart_period
    if period != '전체':
        days_map = {'1개월': 30, '3개월': 90, '6개월': 180, '1년': 365}
        days = days_map.get(period, 180)
        cutoff_date = datetime.now() - timedelta(days=days)
        df_chart.index = pd.to_datetime(df_chart.index, utc=True).tz_localize(None)
        df_chart = df_chart[df_chart.index >= cutoff_date]

    close_col, vol_col = 'Close', 'Volume'
    if close_col in df_chart.columns:
        df_chart[close_col] = df_chart[close_col].round(2)

    if chart_type == "종가":
        if df_chart.empty:
            st.warning("데이터가 없습니다.")
            return
        fig = px.line(df_chart, x=df_chart.index, y=close_col, title=f"{symbol} Close")
        fig.update_traces(name='Close', showlegend=True, line=dict(color='#2563eb', width=2))
        fig.update_layout(height=350, template="plotly")
        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False}, theme="streamlit")

    elif chart_type == "MACD":
        if len(df_chart) < 40:
            st.warning(f"MACD 계산에는 최소 40일이 필요합니다 (현재: {len(df_chart)}일).")
            return
        macd_df = ta.macd(df_chart[close_col], fast=12, slow=26)
        if macd_df is None or macd_df.empty:
            st.warning("MACD 계산 실패. 더 긴 기간을 선택하세요.")
            return
        df_macd = pd.DataFrame({
            'Date': df_chart.index,
            'MACD': macd_df['MACD_12_26_9'],
            'Signal': macd_df['MACDs_12_26_9'],
            'Hist': macd_df['MACDh_12_26_9']
        }).dropna()
        if df_macd.empty:
            st.warning("MACD 데이터 부족.")
            return
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_macd['Date'], y=df_macd['MACD'], name='MACD', line=dict(color='#2563eb', width=2)))
        fig.add_trace(go.Scatter(x=df_macd['Date'], y=df_macd['Signal'], name='Signal', line=dict(color='#dc2626', width=2)))
        fig.add_trace(go.Bar(x=df_macd['Date'], y=df_macd['Hist'], name='Histogram', marker_color='#059669'))
        fig.update_layout(height=350, title="MACD", template="plotly")
        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False}, theme="streamlit")

    elif chart_type == "OBV":
        if len(df_chart) < 15:
            st.warning(f"OBV 계산에는 최소 15일이 필요합니다 (현재: {len(df_chart)}일).")
            return
        obv = ta.obv(df_chart[close_col], df_chart[vol_col])
        obv_signal = ta.sma(obv, length=9)
        df_obv = pd.DataFrame({'Date': df_chart.index, 'OBV': obv, 'OBV_SIGNAL': obv_signal}).dropna()
        if df_obv.empty:
            st.warning("OBV 데이터 부족.")
            return
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_obv['Date'], y=df_obv['OBV'], name='OBV', line=dict(color='#059669', width=2)))
        fig.add_trace(go.Scatter(x=df_obv['Date'], y=df_obv['OBV_SIGNAL'], name='OBV Signal', line=dict(color='#f59e0b', width=2)))
        fig.update_layout(height=350, title="OBV", template="plotly")
        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False}, theme="streamlit")

    elif chart_type == "RSI":
        if len(df_chart) < 14:
            st.warning(f"RSI 계산에는 최소 14일이 필요합니다 (현재: {len(df_chart)}일).")
            return
        rsi = ta.rsi(df_chart[close_col], length=14)
        df_rsi = pd.DataFrame({'Date': df_chart.index, 'RSI': rsi}).dropna()
        if df_rsi.empty:
            st.warning("RSI 데이터 부족.")
            return
        fig = px.line(df_rsi, x='Date', y='RSI', title="RSI")
        fig.add_hline(y=30, line_dash="dot", line_color="#dc2626", annotation_text="OverSold (30)", annotation_position="bottom right")
        fig.add_hline(y=70, line_dash="dot", line_color="#dc2626", annotation_text="OverBought (70)", annotation_position="top right")
        fig.update_traces(name='RSI', showlegend=True, line=dict(color='#8b5cf6', width=2))
        fig.update_layout(height=350, template="plotly")
        st.plotly_chart(fig, width='stretch', config={'displayModeBar': False}, theme="streamlit")

@st.cache_data(ttl=3600)
def get_indicator_data(symbol, market):
    con = get_db_connection()
    query = """
    WITH parsed AS (
        SELECT rsi_d, macd_d, signal_d, obv_d, signal_obv_9d, market_cap, avg_trading_value_20d, today_trading_value, turnover,
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
        FROM indicators WHERE symbol = ? AND market = ?
    )
    SELECT rsi_d_2ago, rsi_d_1ago, rsi_d_latest,
        market_cap, avg_trading_value_20d, today_trading_value, turnover,
        per, eps, upper_closes, lower_closes, sector, sector_trend,
        ma20_latest, ma200_latest,
        (obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AS obv_bullish_cross,
        (today_trading_value > 2.0 * avg_trading_value_20d) AS trading_surge_2x,
        (break_20high = 1 OR (close_latest > ma20_latest AND close_1ago <= ma20_1ago)) AS breakout,
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
    if not df.empty:
        series = df.iloc[0]
        return series.rename({'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest'})
    return None

# =============================================
# 세션 상태 초기화
# =============================================
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
if 'chart_period' not in st.session_state:
    st.session_state.chart_period = '6개월'
if 'backtest_short' not in st.session_state:
    st.session_state.backtest_short = pd.DataFrame()
if 'backtest_mid' not in st.session_state:
    st.session_state.backtest_mid = pd.DataFrame()
if 'backtest_completed' not in st.session_state:
    st.session_state.backtest_completed = pd.DataFrame()
if 'backtest_test' not in st.session_state:
    st.session_state.backtest_test = pd.DataFrame()
if 'backtest_tab' not in st.session_state:
    st.session_state.backtest_tab = 0
if 'last_period' not in st.session_state:
    st.session_state.last_period = None
if 'reset_filters' not in st.session_state:
    st.session_state.reset_filters = False

# 페이지네이션 / 정렬 상태
if 'kr_page' not in st.session_state:
    st.session_state.kr_page = 1
if 'kr_sort_col' not in st.session_state:
    st.session_state.kr_sort_col = '시가총액 (KRW 억원)'
if 'kr_sort_asc' not in st.session_state:
    st.session_state.kr_sort_asc = False
if 'back_short_page' not in st.session_state:
    st.session_state.back_short_page = 1
if 'back_mid_page' not in st.session_state:
    st.session_state.back_mid_page = 1
if 'back_completed_page' not in st.session_state:
    st.session_state.back_completed_page = 1

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

filter_defaults = {
    'short_obv': False, 'short_trading': False, 'short_break': False,
    'mid_rsi': False, 'mid_obv': False, 'mid_golden': False, 'mid_trading': False,
    'foreign': False, 'institutional': False, 'candle': False, 'sector': False
}
for key, default_val in filter_defaults.items():
    if key not in st.session_state:
        st.session_state[key] = default_val

df_ind = load_data()
con = get_db_connection()

# =============================================
# 사이드바
# =============================================
with st.sidebar:
    st.markdown("<h2 style='font-size: 1.8rem; margin-bottom: 0;'>🚀 Trading Copilot</h2>", unsafe_allow_html=True)
    st.markdown("---")

    st.markdown("#### 시장 · 기간")
    market = "KR"
    st.markdown("**🇰🇷 국내 (KR)**")

    period = st.radio(
        "기간",
        ["전체", "단기", "중기", "매도", "백데이터"],
        horizontal=False,
        label_visibility="collapsed"
    )

    st.markdown("---")
    filter_disabled = period != "전체"

    if period == "단기":
        with st.expander("필터(단기) - 자동 적용됨", expanded=True):
            st.markdown("""
            ✅ OBV 상승 크로스  
            ✅ 거래대금 급증(20일평균2배)  
            ✅ 돌파(20일 고가 or MA20 상향)  
                        ➕외국인 순매수(5일)  
                        ➕기관 순매수(5일)  
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
                        ➕기관 순매수(5일)  
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
                        ➕기관 순매수(리버스)  
                        ➕캔들(리버스)  
                        ➕섹터 트렌드(리버스)
            """)

    if period != "전체":
        st.markdown("""
        <div class="filter-disabled-notice">
            ⚠️ 필터를 사용하려면 <strong>'전체'</strong>를 선택하세요.
        </div>
        """, unsafe_allow_html=True)

    with st.expander("필터(단기)", expanded=False):
        short_obv = st.checkbox("OBV 상승 크로스", disabled=filter_disabled, key="short_obv")
        short_trading = st.checkbox("거래대금 급증(20일평균2배)", disabled=filter_disabled, key="short_trading")
        short_break = st.checkbox("돌파(20일 고가 or MA20 상향)", disabled=filter_disabled, key="short_break")

    with st.expander("필터(중기)", expanded=False):
        mid_rsi = st.checkbox("RSI 상승", disabled=filter_disabled, key="mid_rsi")
        mid_obv = st.checkbox("OBV 우상향/크로스", disabled=filter_disabled, key="mid_obv")
        mid_golden = st.checkbox("50MA > 200MA", disabled=filter_disabled, key="mid_golden")
        mid_trading = st.checkbox("거래대금(20평균이상)", disabled=filter_disabled, key="mid_trading")

    with st.expander("필터(참고)", expanded=False):
        foreign_apply = st.checkbox("외국인 순매수(5일 합산 > 0)", disabled=filter_disabled, key="foreign")
        institutional_apply = st.checkbox("기관 순매수(5일 합산 > 0)", disabled=filter_disabled, key="institutional")
        candle_apply = st.checkbox("캔들(5일중 상단 > 하단)", disabled=filter_disabled, key="candle")
        sector_apply = st.checkbox("섹터(업종 트렌드 +)", disabled=filter_disabled, key="sector")

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        apply_btn = st.button("🔍 검색 적용", width='stretch', type="primary", disabled=filter_disabled)
    with col2:
        reset_btn = st.button("초기화", width='stretch', disabled=filter_disabled)

    st.markdown("---")

    with st.expander("📋 사용설명서", expanded=False):
        st.markdown("""
## 📍 이 앱의 목적
**차트 신호로 '좋아 보이는 종목'과 '조심해야 할 종목'을 골라주는 도구**입니다.

---

## 🧭 필터 전략

### 🌐 전체 (필터)
1. **11개 필터** → AND조건

2. **단기신호(7점)**  
- 🟣 7점 : 매수 강력 고려  
- 🔵 6점 : 매수 고려  
- 🟢 4~5점 : 관심  
- 🟡 2~3점 : 주의  
- 🔴 0~1점 : 매수 제외

3. **중기신호(8점)**  
- 🟣 8점 : 매수 강력 고려  
- 🔵 7점 : 매수 고려  
- 🟢 4~6점 : 관심  
- 🟡 2~3점 : 주의  
- 🔴 0~1점 : 매수 제외

---

### ⚡ 단기 (3개 AND)
1. **OBV 상승 크로스** → 돈이 갑자기 들어오기 시작
2. **거래대금 급증 (평균 2배)** → 사람들이 몰림
3. **가격 돌파** → 20일 고가 또는 MA20 위로 돌파

---

### 🌳 중기 (4개 AND)
1. **RSI 3일 상승 (40~60)** → 바닥에서 회복 중
2. **OBV 우상향** → 돈이 꾸준히 유입
3. **50MA > 200MA** → 골든크로스
4. **거래대금 평균 이상** → 관심이 계속 유지됨

---

### 🛑 매도 (하나라도 OR)
1. **RSI ≥ 70** → 과열 구간
2. **OBV 하락 크로스** → 돈이 빠져나가기 시작
3. **RSI 3일 하락 (≤50)** → 매수 심리 꺾임

---

### 📊 백데이터
**변동율** → 필터링된 종목의 성능 검증
        """)

    with st.expander("📘 주식 데이터 설명", expanded=False):
        st.markdown("""
1. **RSI (0~100)**: 주가 과열/과매도 지표  
2. **MA (이동평균)**: 최근 N일 평균 가격  
3. **OBV**: 거래량으로 돈의 흐름  
4. **거래대금**: 하루 동안 거래된 총 금액  
5. **회전율**: 주식이 얼마나 바쁘게 사고팔리는지  
6. **외국인 순매수**: 외국인 자금 유입 여부  
7. **기관 순매수**: 기관 자금 유입 여부  
8. **캔들**: 하루 매수·매도 힘의 결과  
9. **업종**: 이 회사가 속한 산업 분위기  
10. **EPS**: 주당순이익  
11. **PER**: 주가수익비율
        """)

# =============================================
# 필터 적용 로직
# =============================================

if period == "전체":
    if apply_btn or reset_btn:
        if reset_btn:
            st.session_state.filter_results = pd.DataFrame()
            st.session_state.selected_symbol = None
            st.session_state.selected_market = None
            st.session_state.last_selected = None
            st.session_state.reset_filters = True
            st.rerun()
        else:
            filter_parts = []
            if st.session_state.short_obv:
                filter_parts.append("(obv_latest > signal_obv_9_latest AND obv_1ago <= signal_obv_9_1ago)")
            if st.session_state.short_trading:
                filter_parts.append("(today_trading_value >= 2.0 * avg_trading_value_20d)")
            if st.session_state.short_break:
                filter_parts.append("(break_20high = 1 OR (close_latest > ma20_latest AND close_1ago <= ma20_1ago))")
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

            combined_condition = " AND ".join(filter_parts) if filter_parts else "1=1"
            additional_condition = ""
            if st.session_state.candle:
                additional_condition += " AND upper_closes >= 3"

            try:
                con.execute("SELECT 1").fetchone()
            except:
                con = get_db_connection()

            market_filter = "market IN ('KR', 'KOSPI', 'KOSDAQ')"

            query = f"""
            WITH parsed AS (
                SELECT symbol, market,
                    rsi_d, macd_d, signal_d, obv_d, signal_obv_9d, signal_obv_20d,
                    market_cap, avg_trading_value_20d, today_trading_value, turnover,
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
                rsi_d AS rsi_d_array, macd_d AS macd_array, signal_d AS signal_array,
                obv_d AS obv_array, signal_obv_9d AS signal_obv_9_array, signal_obv_20d AS signal_obv_20_array,
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
            WHERE {market_filter} AND ({combined_condition}) {additional_condition}
            ORDER BY market_cap DESC
            """
            df_filter = con.execute(query).fetchdf()
            df_filter = add_foreign_net_buy(df_filter)
            df_filter = add_institutional_net_buy(df_filter)
            df_filter = add_ownership(df_filter)
            df_filter = calculate_buy_signals(df_filter)

            if st.session_state.foreign and not df_filter.empty and 'foreign_sum' in df_filter.columns:
                df_filter = df_filter[df_filter['foreign_sum'] > 0]
            if st.session_state.institutional and not df_filter.empty and 'institutional_sum' in df_filter.columns:
                df_filter = df_filter[df_filter['institutional_sum'] > 0]
            if st.session_state.sector and not df_filter.empty and 'sector_positive' in df_filter.columns:
                df_filter = df_filter[df_filter['sector_positive'] == True]

            df_filter = add_names(df_filter)
            df_filter = add_close_price(df_filter)

            if not df_filter.empty:
                df_filter['_외국인_순매수'] = df_filter.get('short_foreign', pd.Series([False]*len(df_filter))).apply(lambda x: '✅' if x else '❌')
                df_filter['_기관_순매수'] = df_filter.get('short_institutional', pd.Series([False]*len(df_filter))).apply(lambda x: '✅' if x else '❌')
                df_filter['_캔들'] = df_filter.get('short_candle', pd.Series([False]*len(df_filter))).apply(lambda x: '✅' if x else '❌')
                df_filter['_섹터'] = df_filter.get('short_sector', pd.Series([False]*len(df_filter))).apply(lambda x: '✅' if x else '❌')
                df_filter['단기매수신호_fmt'] = df_filter['단기매수신호'].apply(lambda x: format_buy_signal(x, 'all_short'))
                df_filter['중기매수신호_fmt'] = df_filter['중기매수신호'].apply(lambda x: format_buy_signal(x, 'all_mid'))

                df_filter = df_filter.rename(columns={
                    'symbol': '종목코드', 'market': '시장', 'name': '회사명',
                    'sector': '업종', 'sector_trend': '업종트렌드', 'close': '종가',
                    'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금',
                    'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                    'per': 'PER_TTM', 'eps': 'EPS_TTM',
                    'foreign_net_buy_5ago': '외국인순매수_5일전', 'foreign_net_buy_4ago': '외국인순매수_4일전',
                    'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전',
                    'foreign_net_buy_1ago': '외국인순매수_1일전', 'foreign_net_buy_sum': '외국인순매수_합산',
                    'institutional_net_buy_5ago': '기관순매수_5일전', 'institutional_net_buy_4ago': '기관순매수_4일전',
                    'institutional_net_buy_3ago': '기관순매수_3일전', 'institutional_net_buy_2ago': '기관순매수_2일전',
                    'institutional_net_buy_1ago': '기관순매수_1일전', 'institutional_net_buy_sum': '기관순매수_합산',
                    'cap_status': '업데이트', '_외국인_순매수': '외국인 순매수', '_기관_순매수': '기관 순매수',
                    '_캔들': '캔들', '_섹터': '섹터',
                    '단기매수신호_fmt': '단기신호', '중기매수신호_fmt': '중기신호',
                    'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest',
                    'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)',
                    'obv_bullish_cross': 'OBV 상승 크로스', 'trading_surge_2x': '거래대금 급증(20일평균2배)',
                    'breakout': '돌파(20일 고가 or MA20 상향)', 'rsi_3up': 'RSI 상승',
                    'obv_mid_condition': 'OBV 우상향/크로스', 'ma50_above_200': '50MA > 200MA',
                    'trading_above_avg': '거래대금(20평균이상)', 'rsi_overbought': 'RSI 과열(70 이상)',
                    'rsi_3down': 'RSI 하강 지속'
                })
                drop_cols = [
                    'short_obv_cross', 'short_trading', 'short_break', 'short_foreign', 'short_institutional', 'short_candle', 'short_sector',
                    'mid_rsi', 'mid_obv', 'mid_golden', 'mid_trading', 'mid_foreign', 'mid_institutional', 'mid_candle', 'mid_sector',
                    '단기매수신호', '중기매수신호'
                ]
                df_filter = df_filter.drop(columns=[col for col in drop_cols if col in df_filter.columns], errors='ignore')
                df_filter = df_filter.sort_values('시가총액', ascending=False)

                df_kr = df_filter[df_filter['시장'].isin(KR_MARKETS)].copy() if '시장' in df_filter.columns else pd.DataFrame()
                if not df_kr.empty:
                    df_kr = format_dataframe(df_kr, 'KR')
                st.session_state.filter_results = df_kr
            else:
                st.session_state.filter_results = pd.DataFrame()

    df_display = st.session_state.filter_results

elif period == "단기":
    df_result = run_screener_query(con, "short_term")
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    df_result = add_institutional_net_buy(df_result)
    df_result = add_ownership(df_result)

    if not df_result.empty:
        df_result = calculate_buy_signals(df_result)
        df_result['_기관_순매수'] = df_result.get('short_institutional', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_외국인_순매수'] = df_result.get('mid_foreign', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_캔들'] = df_result.get('mid_candle', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_섹터'] = df_result.get('mid_sector', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')

        df_result = df_result.rename(columns={
            'symbol': '종목코드', 'market': '시장', 'name': '회사명',
            'sector': '업종', 'sector_trend': '업종트렌드', 'close': '종가',
            'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금',
            'today_trading_value': '오늘거래대금', 'turnover': '회전율',
            'per': 'PER_TTM', 'eps': 'EPS_TTM',
            'obv_bullish_cross': 'OBV 상승 크로스', 'trading_surge_2x': '거래대금 급증(20일평균2배)',
            'breakout': '돌파(20일 고가 or MA20 상향)',
            'foreign_net_buy_5ago': '외국인순매수_5일전', 'foreign_net_buy_4ago': '외국인순매수_4일전',
            'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전',
            'foreign_net_buy_1ago': '외국인순매수_1일전', 'foreign_net_buy_sum': '외국인순매수_합산',
            '_외국인_순매수': '외국인 순매수', '_기관_순매수': '기관 순매수', '_캔들': '캔들', '_섹터': '섹터',
            'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest',
            'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
        })
        if '단기매수신호' in df_result.columns:
            df_result['단기매수신호'] = df_result['단기매수신호'].apply(lambda x: format_buy_signal(x, 'short'))
        drop_cols = ['short_obv_cross', 'short_trading', 'short_break', 'short_foreign', 'short_candle', 'short_sector']
        df_result = df_result.drop(columns=[col for col in drop_cols if col in df_result.columns], errors='ignore')
        df_result = df_result.sort_values('시가총액', ascending=False)

        df_kr = df_result[df_result['시장'].isin(KR_MARKETS)].copy() if '시장' in df_result.columns else pd.DataFrame()
        if not df_kr.empty:
            df_kr = format_dataframe(df_kr, 'KR')
        df_display = df_kr
    else:
        df_display = pd.DataFrame()

elif period == "중기":
    df_result = run_screener_query(con, "mid_term")
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    df_result = add_institutional_net_buy(df_result)
    df_result = add_ownership(df_result)

    if not df_result.empty:
        df_result = calculate_buy_signals(df_result)
        df_result['_기관_순매수'] = df_result.get('mid_institutional', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_외국인_순매수'] = df_result.get('mid_foreign', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_캔들'] = df_result.get('mid_candle', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_섹터'] = df_result.get('mid_sector', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')

        df_result = df_result.rename(columns={
            'symbol': '종목코드', 'market': '시장', 'name': '회사명',
            'sector': '업종', 'sector_trend': '업종트렌드', 'close': '종가',
            'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금',
            'today_trading_value': '오늘거래대금', 'turnover': '회전율',
            'per': 'PER_TTM', 'eps': 'EPS_TTM',
            'rsi_3up': 'RSI 상승', 'obv_mid_condition': 'OBV 우상향/크로스',
            'ma50_above_200': '50MA > 200MA', 'trading_above_avg': '거래대금(20평균이상)',
            'foreign_net_buy_5ago': '외국인순매수_5일전', 'foreign_net_buy_4ago': '외국인순매수_4일전',
            'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전',
            'foreign_net_buy_1ago': '외국인순매수_1일전', 'foreign_net_buy_sum': '외국인순매수_합산',
            '_외국인_순매수': '외국인 순매수', '_기관_순매수': '기관 순매수', '_캔들': '캔들', '_섹터': '섹터',
            'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest',
            'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
        })
        if '중기매수신호' in df_result.columns:
            df_result['중기매수신호'] = df_result['중기매수신호'].apply(lambda x: format_buy_signal(x, 'mid'))
        drop_cols = ['mid_rsi', 'mid_obv', 'mid_golden', 'mid_trading', 'mid_foreign', 'mid_candle', 'mid_sector']
        df_result = df_result.drop(columns=[col for col in drop_cols if col in df_result.columns], errors='ignore')
        df_result = df_result.sort_values('시가총액', ascending=False)

        df_kr = df_result[df_result['시장'].isin(KR_MARKETS)].copy() if '시장' in df_result.columns else pd.DataFrame()
        if not df_kr.empty:
            df_kr = format_dataframe(df_kr, 'KR')
        df_display = df_kr
    else:
        df_display = pd.DataFrame()

elif period == "매도":
    df_result = run_screener_query(con, "sell")
    df_result = add_names(df_result)
    df_result = add_foreign_net_buy(df_result)
    df_result = add_close_price(df_result)
    df_result = add_institutional_net_buy(df_result)
    df_result = add_ownership(df_result)

    if not df_result.empty:
        df_result = calculate_buy_signals(df_result)
        df_result['_기관_순매수_리버스'] = df_result.get('sell_institutional', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_외국인_순매수_리버스'] = df_result.get('sell_foreign', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_캔들_리버스'] = df_result.get('sell_candle', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')
        df_result['_섹터_리버스'] = df_result.get('sell_sector', pd.Series([False]*len(df_result))).apply(lambda x: '✅' if x else '❌')

        if '매도신호' in df_result.columns:
            df_result['매도신호_fmt'] = df_result['매도신호'].apply(
                lambda x: f'🟢 {x}점' if x <= 2 else f'🟡 {x}점' if x <= 4 else f'🔴 {x}점'
            )
            df_result = df_result.drop(columns=['매도신호'])

        df_result = df_result.rename(columns={
            'symbol': '종목코드', 'market': '시장', 'name': '회사명',
            'sector': '업종', 'sector_trend': '업종트렌드', 'close': '종가',
            'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금',
            'today_trading_value': '오늘거래대금', 'turnover': '회전율',
            'per': 'PER_TTM', 'eps': 'EPS_TTM',
            'rsi_overbought': 'RSI 과열(70 이상)', 'rsi_3down': 'RSI 하강 지속',
            'obv_bearish_cross': 'OBV 하락 크로스',
            'foreign_net_buy_5ago': '외국인순매수_5일전', 'foreign_net_buy_4ago': '외국인순매수_4일전',
            'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전',
            'foreign_net_buy_1ago': '외국인순매수_1일전', 'foreign_net_buy_sum': '외국인순매수_합산',
            '_외국인_순매수_리버스': '외국인 순매수(리버스)', '_기관_순매수_리버스': '기관 순매수(리버스)',
            '_캔들_리버스': '캔들(리버스)', '_섹터_리버스': '섹터(리버스)',
            '매도신호_fmt': '매도신호',
            'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest',
            'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
        })
        drop_cols = ['sell_rsi_overbought', 'sell_rsi_down', 'sell_obv_cross', 'sell_foreign', 'sell_candle', 'sell_sector']
        df_result = df_result.drop(columns=[col for col in drop_cols if col in df_result.columns], errors='ignore')
        df_result = df_result.sort_values('시가총액', ascending=False)

        df_kr = df_result[df_result['시장'].isin(KR_MARKETS)].copy() if '시장' in df_result.columns else pd.DataFrame()
        if not df_kr.empty:
            df_kr = format_dataframe(df_kr, 'KR')
        df_display = df_kr
    else:
        df_display = pd.DataFrame()

elif period == "백데이터":
    BACKTEST_DB_PATH = "data/meta/backtest.db"
    BACKTEST_COMPLETED_CSV = "data/backtest_completed.csv"
    BACKTEST_TEST_CSV = "data/backtest_test.csv"
    if not os.path.exists(BACKTEST_DB_PATH):
        st.warning("백테스팅 DB 없음 – 배치 실행하세요.")
        df_display = pd.DataFrame()
    else:
        con_back = duckdb.connect(BACKTEST_DB_PATH, read_only=True)
        df_back = con_back.execute("SELECT * FROM backtest").fetchdf()
        con_back.close()

        if not df_back.empty:
            df_back = df_back[df_back['market'].isin(KR_MARKETS)]
            df_back['symbol'] = df_back['symbol'].astype(str).str.zfill(6)
            if 'type' in df_back.columns:
                type_mapping = {'short': '단기', 'mid': '중기', 'short_mid': '단기+중기', 'short+mid': '단기+중기'}
                df_back['type'] = df_back['type'].map(type_mapping).fillna(df_back['type'])

            df_back = add_foreign_net_buy(df_back)
            df_back = add_institutional_net_buy(df_back)

            if not df_back.empty:
                df_back['foreign_positive'] = df_back['foreign_net_buy_sum'].apply(lambda x: '✅' if x > 0 else '❌')
                df_back['institutional_positive'] = df_back['institutional_net_buy_sum'].apply(lambda x: '✅' if x > 0 else '❌')
                df_back['candle_upper_3'] = df_back['upper_closes'].apply(lambda x: '✅' if x >= 3 else '❌')

                rename_dict = {
                    'symbol': '종목코드', 'name': '회사명', 'sector': '업종', 'sector_trend': '업종트렌드',
                    'market': '시장', 'close': '종가', 'market_cap': '시가총액',
                    'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금',
                    'turnover': '회전율', 'per': 'PER_TTM', 'eps': 'EPS_TTM', 'cap_status': '업데이트',
                    'type': '타입', 'latest_close': '최신종가', 'latest_update': '최신업데이트', 'change_rate': '변동율%',
                    'foreign_net_buy_5ago': '외국인순매수_5일전', 'foreign_net_buy_4ago': '외국인순매수_4일전',
                    'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전',
                    'foreign_net_buy_1ago': '외국인순매수_1일전', 'foreign_net_buy_sum': '외국인순매수_합산',
                    'institutional_net_buy_5ago': '기관순매수_5일전', 'institutional_net_buy_4ago': '기관순매수_4일전',
                    'institutional_net_buy_3ago': '기관순매수_3일전', 'institutional_net_buy_2ago': '기관순매수_2일전',
                    'institutional_net_buy_1ago': '기관순매수_1일전', 'institutional_net_buy_sum': '기관순매수_합산',
                    'foreign_positive': '외국인 순매수', 'institutional_positive': '기관 순매수',
                    'candle_upper_3': '캔들', 'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago',
                    'rsi_d_latest': 'RSI_3일_latest', 'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
                }
                df_back = df_back.rename(columns=rename_dict)
                df_back = df_back.sort_values('업데이트', ascending=False)

                df_short = df_back[df_back['타입'].isin(['단기', '단기+중기'])].copy()
                df_mid = df_back[df_back['타입'].isin(['중기', '단기+중기'])].copy()

                df_short_kr = df_short.copy() if not df_short.empty else pd.DataFrame()
                df_mid_kr = df_mid.copy() if not df_mid.empty else pd.DataFrame()

                if not df_short_kr.empty:
                    if '시가총액' in df_short_kr.columns:
                        df_short_kr['시가총액'] = df_short_kr['시가총액'].apply(_safe_market_cap_to_억원)
                    df_short_kr = format_dataframe(df_short_kr, 'KR')
                if not df_mid_kr.empty:
                    if '시가총액' in df_mid_kr.columns:
                        df_mid_kr['시가총액'] = df_mid_kr['시가총액'].apply(_safe_market_cap_to_억원)
                    df_mid_kr = format_dataframe(df_mid_kr, 'KR')

                st.session_state.backtest_short = df_short_kr
                st.session_state.backtest_mid = df_mid_kr
                df_display = df_back
            else:
                st.session_state.backtest_short = pd.DataFrame()
                st.session_state.backtest_mid = pd.DataFrame()
                df_display = pd.DataFrame()

        if os.path.exists(BACKTEST_COMPLETED_CSV):
            df_completed = pd.read_csv(BACKTEST_COMPLETED_CSV, dtype={'symbol': str})
            if not df_completed.empty:
                df_completed = df_completed[df_completed['market'].isin(KR_MARKETS)]
                df_completed['symbol'] = df_completed['symbol'].astype(str).str.zfill(6)
                if 'type' in df_completed.columns:
                    type_mapping = {'short': '단기', 'mid': '중기', 'short_mid': '단기+중기', 'short+mid': '단기+중기'}
                    df_completed['type'] = df_completed['type'].map(type_mapping).fillna(df_completed['type'])

                df_completed = add_foreign_net_buy(df_completed)
                df_completed = add_institutional_net_buy(df_completed)

                if not df_completed.empty:
                    df_completed['foreign_positive'] = df_completed['foreign_net_buy_sum'].apply(lambda x: '✅' if x > 0 else '❌')
                    df_completed['institutional_positive'] = df_completed['institutional_net_buy_sum'].apply(lambda x: '✅' if x > 0 else '❌')
                    df_completed['candle_upper_3'] = df_completed['upper_closes'].apply(lambda x: '✅' if x >= 3 else '❌')

                    rename_dict = {
                        'symbol': '종목코드', 'name': '회사명', 'sector': '업종', 'sector_trend': '업종트렌드',
                        'market': '시장', 'close': '종가', 'market_cap': '시가총액',
                        'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금',
                        'turnover': '회전율', 'per': 'PER_TTM', 'eps': 'EPS_TTM', 'cap_status': '업데이트',
                        'type': '타입', 'base_date': '기준일', 'target_date': '목표일',
                        'latest_close': '최신종가', 'latest_update': '최신업데이트', 'change_rate': '변동율%',
                        'foreign_net_buy_5ago': '외국인순매수_5일전', 'foreign_net_buy_4ago': '외국인순매수_4일전',
                        'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전',
                        'foreign_net_buy_1ago': '외국인순매수_1일전', 'foreign_net_buy_sum': '외국인순매수_합산',
                        'institutional_net_buy_5ago': '기관순매수_5일전', 'institutional_net_buy_4ago': '기관순매수_4일전',
                        'institutional_net_buy_3ago': '기관순매수_3일전', 'institutional_net_buy_2ago': '기관순매수_2일전',
                        'institutional_net_buy_1ago': '기관순매수_1일전', 'institutional_net_buy_sum': '기관순매수_합산',
                        'foreign_positive': '외국인 순매수', 'institutional_positive': '기관 순매수',
                        'candle_upper_3': '캔들', 'upper_closes': '캔들(상단)', 'lower_closes': '캔들(하단)'
                    }
                    df_completed = df_completed.rename(columns=rename_dict)
                    df_completed = df_completed.sort_values('최신업데이트', ascending=False)
                    df_completed_kr = df_completed.copy()
                    if not df_completed_kr.empty:
                        if '시가총액' in df_completed_kr.columns:
                            df_completed_kr['시가총액'] = df_completed_kr['시가총액'].apply(_safe_market_cap_to_억원)
                        df_completed_kr = format_dataframe(df_completed_kr, 'KR')
                    st.session_state.backtest_completed = df_completed_kr
                else:
                    st.session_state.backtest_completed = pd.DataFrame()
            else:
                st.session_state.backtest_completed = pd.DataFrame()
        else:
            st.session_state.backtest_completed = pd.DataFrame()

    if os.path.exists(BACKTEST_TEST_CSV):
        df_test = pd.read_csv(BACKTEST_TEST_CSV, dtype={'symbol': str})
        if not df_test.empty:
            df_test = df_test[df_test['market'].isin(KR_MARKETS)]
            df_test['symbol'] = df_test['symbol'].astype(str).str.zfill(6)
            if 'type' in df_test.columns:
                type_mapping = {'short': '단기', 'mid': '중기'}
                df_test['type'] = df_test['type'].map(type_mapping).fillna(df_test['type'])
            if 'is_completed' in df_test.columns:
                df_test['is_completed'] = df_test['is_completed'].apply(lambda x: '완료' if pd.notna(x) and int(x) == 1 else '대기')
            rename_test = {
                'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'sector': '업종',
                'type': '타입', 'base_date': '기준일', 'target_date': '목표일', 'base_close': '기준가',
                'date_5pct': '+5% 달성일', 'date_10pct': '+10% 달성일',
                'final_close': '최종종가', 'final_change_rate': '최종수익률%', 'is_completed': '완료여부',
            }
            df_test = df_test.rename(columns=rename_test)
            if '기준일' in df_test.columns:
                df_test = df_test.sort_values('기준일', ascending=False)
            st.session_state.backtest_test = df_test
        else:
            st.session_state.backtest_test = pd.DataFrame()
    else:
        st.session_state.backtest_test = pd.DataFrame()

# =============================================
# 배치 시간 로드
# =============================================
log_time_file = "logs/batch_time.txt"
batch_time = ""
if os.path.exists(log_time_file):
    with open(log_time_file, "r") as f:
        batch_time = f.read().strip()

active_filters = []
if not df_display.empty:
    if period == "전체":
        short_filters = []
        if st.session_state.short_obv: short_filters.append("OBV 상승 크로스")
        if st.session_state.short_trading: short_filters.append("거래대금 급증")
        if st.session_state.short_break: short_filters.append("돌파")
        if short_filters: active_filters.append(f"단기({', '.join(short_filters)})")
        mid_filters = []
        if st.session_state.mid_rsi: mid_filters.append("RSI 상승")
        if st.session_state.mid_obv: mid_filters.append("OBV 우상향")
        if st.session_state.mid_golden: mid_filters.append("골든크로스")
        if st.session_state.mid_trading: mid_filters.append("거래대금")
        if mid_filters: active_filters.append(f"중기({', '.join(mid_filters)})")
        if st.session_state.foreign: active_filters.append("외국인 순매수")
        if st.session_state.candle: active_filters.append("캔들")
    elif period in ["단기", "중기", "매도"]:
        active_filters.append(f"{period} 전략")

# =============================================
# 상단 헤더 박스
# =============================================
st.markdown(f"""
<div style='
    background: var(--secondary-background-color);
    padding: 20px 24px; border-radius: 24px;
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

# =============================================
# _display_backtest_table 함수
# =============================================
def _display_backtest_table(df_filtered, tab_type, apply_btn, foreign_apply, institutional_apply, candle_apply):
    PAGE_SIZE = 50

    display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
    for col in ['종가 (KRW)', '시가총액 (KRW 억원)']:
        if col in df_filtered.columns:
            display_cols.append(col)

    if tab_type == "completed":
        back_cols = ['업데이트', '타입', '기준일', '목표일', '최신종가', '최신업데이트', '변동율%']
    else:
        back_cols = ['업데이트', '타입', '최신종가', '최신업데이트', '변동율%']

    for col in back_cols:
        if col in df_filtered.columns:
            display_cols.append(col)

    if apply_btn:
        if foreign_apply and '외국인 순매수' in df_filtered.columns:
            display_cols.append('외국인 순매수')
        if institutional_apply and '기관 순매수' in df_filtered.columns:
            display_cols.append('기관 순매수')
        if candle_apply and '캔들' in df_filtered.columns:
            display_cols.append('캔들')

    display_cols = [col for col in display_cols if col in df_filtered.columns]
    df_kr_filtered = df_filtered[df_filtered['시장'].isin(KR_MARKETS)].copy() if '시장' in df_filtered.columns else pd.DataFrame()
    df_kr_filtered = df_kr_filtered.reset_index(drop=True)

    def _calc_stats(df_sub):
        cnt = len(df_sub)
        if '변동율%' in df_sub.columns and cnt > 0:
            rates = pd.to_numeric(df_sub['변동율%'], errors='coerce').dropna()
            up = int((rates > 0).sum())
            down = int((rates < 0).sum())
            avg = rates.mean() if len(rates) > 0 else 0.0
            wr = up / len(rates) * 100 if len(rates) > 0 else 0.0
        else:
            up = down = 0
            avg = wr = 0.0
        return cnt, up, down, avg, wr

    kr_cnt, kr_up, kr_down, kr_avg, kr_wr = _calc_stats(df_kr_filtered)

    if not df_kr_filtered.empty:
        kr_avg_color = "#dc2626" if kr_avg >= 0 else "#2563eb"
        if tab_type == "completed":
            kr_short = len(df_kr_filtered[df_kr_filtered['타입'] == '단기']) if '타입' in df_kr_filtered.columns else 0
            kr_mid   = len(df_kr_filtered[df_kr_filtered['타입'] == '중기']) if '타입' in df_kr_filtered.columns else 0
            kr_extra = f"&nbsp;단기 <b>{kr_short}</b> · 중기 <b>{kr_mid}</b>"
        else:
            kr_extra = ""

        st.markdown(
            "<div style='background:var(--secondary-background-color);padding:12px 18px;border-radius:14px;"
            "border:1px solid rgba(128,128,128,.15);margin-bottom:12px;display:flex;gap:0;flex-wrap:wrap;align-items:center;'>"
            f"<span style='font-weight:700;margin-right:16px;'>🇰🇷 국내</span>"
            f"<span style='margin-right:12px;'>📋 <b>{kr_cnt}</b></span>"
            f"<span style='margin-right:12px;'>📈 <b style='color:#dc2626'>{kr_up}</b> · 📉 <b style='color:#2563eb'>{kr_down}</b></span>"
            f"<span style='margin-right:12px;'>평균 <b style='color:{kr_avg_color}'>{kr_avg:+.2f}%</b></span>"
            f"<span>승률 <b>{kr_wr:.1f}%</b>{kr_extra}</span>"
            "</div>",
            unsafe_allow_html=True
        )

        csv_columns_kr = display_cols.copy()
        df_kr_csv = df_kr_filtered[csv_columns_kr]
        csv_kr = df_kr_csv.to_csv(index=False).encode('utf-8-sig')

        # 페이지네이션 계산
        total_pages = max(1, (kr_cnt - 1) // PAGE_SIZE + 1)
        page_key = f"back_{tab_type}_page"
        if page_key not in st.session_state:
            st.session_state[page_key] = 1
        cur_page = min(st.session_state[page_key], total_pages)
        start_idx = (cur_page - 1) * PAGE_SIZE
        end_idx = min(start_idx + PAGE_SIZE, kr_cnt)
        df_page = df_kr_filtered.iloc[start_idx:end_idx].copy()

        # 헤더: 제목 + 페이지이동 + CSV
        col_kr_h1, col_kr_h2, col_kr_h3, col_kr_h4 = st.columns([3, 2, 4, 1])
        with col_kr_h1:
            st.markdown("#### 국내 (KR)")
        with col_kr_h2:
            st.markdown(f"<div style='padding-top:8px; font-size:0.85rem; white-space:nowrap;'><b>총 {kr_cnt}개</b> · {cur_page}/{total_pages}p</div>", unsafe_allow_html=True)
        with col_kr_h3:
            pg_c1, pg_c2, pg_c3 = st.columns([2, 2, 2])
            with pg_c1:
                if st.button("◀ 이전", key=f"back_prev_{tab_type}", disabled=(cur_page <= 1), use_container_width=True):
                    st.session_state[page_key] = cur_page - 1
                    st.rerun()
            with pg_c2:
                input_page = st.number_input(
                    "p", min_value=1, max_value=total_pages,
                    value=cur_page, key=f"back_page_input_{tab_type}",
                    label_visibility="collapsed"
                )
                if int(input_page) != cur_page:
                    st.session_state[page_key] = int(input_page)
                    st.rerun()
            with pg_c3:
                if st.button("다음 ▶", key=f"back_next_{tab_type}", disabled=(cur_page >= total_pages), use_container_width=True):
                    st.session_state[page_key] = cur_page + 1
                    st.rerun()
        with col_kr_h4:
            st.markdown('<div class="csv-btn-fixed">', unsafe_allow_html=True)
            st.download_button(
                label="💾CSV",
                data=csv_kr,
                file_name=f'kr_backtest_{tab_type}.csv',
                mime='text/csv',
                key=f"download_kr_back_{tab_type}",
                use_container_width=True
            )
            st.markdown('</div>', unsafe_allow_html=True)

        kr_display_cols = [col for col in display_cols if '(USD' not in col and '(N/A)' not in col]

        ROW_H = 35
        HEADER_H = 38
        MAX_ROWS = 10
        kr_height = min(len(df_page), MAX_ROWS) * ROW_H + HEADER_H

        df_kr_display_full = df_page[kr_display_cols].copy().reset_index(drop=True)
        kr_sector_trends = df_kr_display_full['업종트렌드'].copy() if '업종트렌드' in df_kr_display_full.columns else None
        df_kr_display = df_kr_display_full.drop(columns=['업종트렌드'], errors='ignore')

        kr_key = f"kr_back_{tab_type}_df"

        if kr_sector_trends is not None:
            bg_colors = kr_sector_trends.apply(
                lambda x: get_sector_trend_color(x) if pd.notna(x) else ''
            ).fillna('').reset_index(drop=True)
            styled_kr = df_kr_display.style.apply(
                lambda col: [f'background-color: {bg}' if bg else '' for bg in bg_colors],
                axis=0
            )
        else:
            styled_kr = df_kr_display.style

        def apply_change_rate_color(val):
            if pd.isna(val): return ''
            try:
                num_val = float(val)
                if num_val > 0: return 'color: #dc2626; font-weight: 700'
                elif num_val < 0: return 'color: #2563eb; font-weight: 700'
            except:
                pass
            return ''

        format_dict = {}
        for col in df_kr_display.columns:
            if df_kr_display[col].dtype in ['int64', 'float64']:
                if col == '종가 (KRW)': format_dict[col] = '{:,.0f}'
                elif '시가총액' in col: format_dict[col] = '{:,.2f}'
                elif col == '변동율%': continue
                else: format_dict[col] = '{:,.2f}'
        if format_dict:
            styled_kr = styled_kr.format(format_dict, na_rep='')
        if '변동율%' in df_kr_display.columns:
            styled_kr = styled_kr.map(apply_change_rate_color, subset=['변동율%'])
            styled_kr = styled_kr.format('{:.2f}', subset=['변동율%'])

        event_kr = st.dataframe(
            styled_kr,
            on_select="rerun",
            selection_mode="single-row",
            hide_index=True,
            width='stretch',
            height=kr_height,
            key=kr_key,
            column_config={
                "종목코드": st.column_config.Column(width=75),
                "시장": st.column_config.Column(width=60),
                "회사명": st.column_config.Column(width=150),
                "업종": st.column_config.Column(width=150),
                "종가 (KRW)": st.column_config.Column(width=90),
                "시가총액 (KRW 억원)": st.column_config.Column(width=120),
                "업데이트": st.column_config.Column(width=90),
                "타입": st.column_config.Column(width=75),
                "최신종가": st.column_config.Column(width=90),
                "최신업데이트": st.column_config.Column(width=90),
                "변동율%": st.column_config.Column(width=60),
                "외국인 순매수": st.column_config.Column(width=60),
                "기관 순매수": st.column_config.Column(width=60),
                "캔들": st.column_config.Column(width=60),
            }
        )

        if event_kr.selection.rows:
            selected_idx = event_kr.selection.rows[0]
            real_idx = start_idx + selected_idx
            new_symbol = df_kr_filtered.iloc[real_idx]['종목코드']
            if new_symbol != st.session_state.selected_symbol or st.session_state.selected_market != 'KR':
                st.session_state.selected_symbol = new_symbol
                st.session_state.selected_market = 'KR'
                st.rerun()

# =============================================
# 결과 리스트 섹션
# =============================================
st.markdown("### 결과 리스트")

# ✅ period가 바뀔 때만 페이지 1로 리셋
if st.session_state.last_period != period:
    st.session_state.last_period = period
    st.session_state.kr_page = 1

if not df_display.empty:
    if period == "백데이터":
        back_tab1, back_tab2, back_tab3, back_tab4 = st.tabs(["단기", "중기", "완료", "테스트"])

        with back_tab1:
            df_to_show = st.session_state.backtest_short
            if not df_to_show.empty:
                search_term_short = st.text_input("🔍 종목 검색", placeholder="코드 또는 회사명 입력", key="back_search_short")
                if search_term_short:
                    mask = (df_to_show['종목코드'].astype(str).str.contains(search_term_short, case=False, na=False)) | \
                           (df_to_show['회사명'].astype(str).str.contains(search_term_short, case=False, na=False))
                    df_filtered = df_to_show[mask]
                else:
                    df_filtered = df_to_show
                _display_backtest_table(df_filtered, "short", apply_btn, foreign_apply, institutional_apply, candle_apply)
            else:
                st.info("단기 조건에 맞는 종목이 없습니다.")

        with back_tab2:
            df_to_show = st.session_state.backtest_mid
            if not df_to_show.empty:
                search_term_mid = st.text_input("🔍 종목 검색", placeholder="코드 또는 회사명 입력", key="back_search_mid")
                if search_term_mid:
                    mask = (df_to_show['종목코드'].astype(str).str.contains(search_term_mid, case=False, na=False)) | \
                           (df_to_show['회사명'].astype(str).str.contains(search_term_mid, case=False, na=False))
                    df_filtered = df_to_show[mask]
                else:
                    df_filtered = df_to_show
                _display_backtest_table(df_filtered, "mid", apply_btn, foreign_apply, institutional_apply, candle_apply)
            else:
                st.info("중기 조건에 맞는 종목이 없습니다.")

        with back_tab3:
            df_to_show = st.session_state.backtest_completed
            if not df_to_show.empty:
                search_term_completed = st.text_input("🔍 종목 검색", placeholder="코드 또는 회사명 입력", key="back_search_completed")
                if search_term_completed:
                    mask = (df_to_show['종목코드'].astype(str).str.contains(search_term_completed, case=False, na=False)) | \
                           (df_to_show['회사명'].astype(str).str.contains(search_term_completed, case=False, na=False))
                    df_filtered = df_to_show[mask]
                else:
                    df_filtered = df_to_show
                _display_backtest_table(df_filtered, "completed", apply_btn, foreign_apply, institutional_apply, candle_apply)
            else:
                st.info("완료된 백테스트 종목이 없습니다.")

        with back_tab4:
            df_to_show = st.session_state.backtest_test
            if not df_to_show.empty:
                search_term_test = st.text_input("🔍 종목 검색", placeholder="코드 또는 회사명 입력", key="back_search_test")
                if search_term_test:
                    mask = (
                        df_to_show['종목코드'].astype(str).str.contains(search_term_test, case=False, na=False) |
                        df_to_show['회사명'].astype(str).str.contains(search_term_test, case=False, na=False)
                    )
                    df_test_filtered = df_to_show[mask].copy()
                else:
                    df_test_filtered = df_to_show.copy()

                if not df_test_filtered.empty:
                    completed_rows = df_test_filtered[df_test_filtered['완료여부'] == '완료'].copy()
                    pending_rows   = df_test_filtered[df_test_filtered['완료여부'] == '대기'].copy()
                    total_cnt = len(df_test_filtered)
                    done_cnt = len(completed_rows)
                    pending_cnt = len(pending_rows)

                    cnt_5pct  = completed_rows['+5% 달성일'].apply(lambda x: x not in ['', None] and pd.notna(x)).sum() if done_cnt > 0 else 0
                    cnt_10pct = completed_rows['+10% 달성일'].apply(lambda x: x not in ['', None] and pd.notna(x)).sum() if done_cnt > 0 else 0

                    if done_cnt > 0 and '최종수익률%' in completed_rows.columns:
                        numeric_rates = pd.to_numeric(completed_rows['최종수익률%'], errors='coerce').dropna()
                        avg_rate = numeric_rates.mean() if len(numeric_rates) > 0 else 0.0
                        win_rate = (numeric_rates > 0).sum() / len(numeric_rates) * 100 if len(numeric_rates) > 0 else 0.0
                    else:
                        avg_rate = 0.0
                        win_rate = 0.0

                    pct5_ratio  = f"{cnt_5pct}/{done_cnt}" if done_cnt > 0 else "-"
                    pct10_ratio = f"{cnt_10pct}/{done_cnt}" if done_cnt > 0 else "-"
                    st.markdown(f"""
<div style='background:var(--secondary-background-color);padding:12px 18px;border-radius:14px;
            border:1px solid rgba(128,128,128,.15);margin-bottom:12px;display:flex;gap:32px;flex-wrap:wrap;'>
    <span>📋 전체 <b>{total_cnt}</b></span>
    <span>✅ 완료 <b>{done_cnt}</b> · ⏳ 대기 <b>{pending_cnt}</b></span>
    <span>+5% 달성 <b style='color:#dc2626'>{pct5_ratio}</b></span>
    <span>+10% 달성 <b style='color:#dc2626'>{pct10_ratio}</b></span>
    <span>평균수익률 <b style='color:{"#dc2626" if avg_rate >= 0 else "#2563eb"}'>{avg_rate:+.2f}%</b></span>
    <span>승률 <b>{win_rate:.1f}%</b></span>
</div>""", unsafe_allow_html=True)

                    test_display_cols = ['종목코드', '시장', '회사명', '업종', '타입',
                                         '기준일', '목표일', '기준가',
                                         '+5% 달성일', '+10% 달성일',
                                         '최종종가', '최종수익률%', '완료여부']
                    test_display_cols = [c for c in test_display_cols if c in df_test_filtered.columns]

                    col_t_h1, col_t_h2 = st.columns([10, 1])
                    with col_t_h1:
                        st.markdown("#### 📊 +5% / +10% 달성일 추적")
                    with col_t_h2:
                        csv_test = df_test_filtered[test_display_cols].to_csv(index=False).encode('utf-8-sig')
                        st.markdown('<div class="csv-btn-fixed">', unsafe_allow_html=True)
                        st.download_button(
                            label="💾CSV",
                            data=csv_test,
                            file_name='backtest_test.csv',
                            mime='text/csv',
                            key="download_backtest_test",
                            width='stretch'
                        )
                        st.markdown('</div>', unsafe_allow_html=True)

                    df_test_display = df_test_filtered[test_display_cols].copy().reset_index(drop=True)

                    def apply_test_row_style(row):
                        val = row.get('완료여부', '')
                        bg = 'rgba(5, 150, 105, 0.08)' if val == '완료' else ''
                        return [f'background-color: {bg}' if bg else '' for _ in row.index]

                    def color_rate(val):
                        if pd.isna(val) or val == '': return ''
                        try:
                            v = float(val)
                            if v > 0: return 'color: #dc2626; font-weight: 700'
                            elif v < 0: return 'color: #2563eb; font-weight: 700'
                        except: pass
                        return ''

                    styled_test = df_test_display.style.apply(apply_test_row_style, axis=1)
                    fmt = {}
                    if '기준가' in df_test_display.columns and df_test_display['기준가'].dtype in ['int64', 'float64']:
                        fmt['기준가'] = '{:,.0f}'
                    if '최종종가' in df_test_display.columns and df_test_display['최종종가'].dtype in ['int64', 'float64']:
                        fmt['최종종가'] = '{:,.0f}'
                    if fmt:
                        styled_test = styled_test.format(fmt, na_rep='')
                    if '최종수익률%' in df_test_display.columns:
                        styled_test = styled_test.map(color_rate, subset=['최종수익률%'])
                        styled_test = styled_test.format(
                            lambda x: f'{float(x):+.2f}' if x not in ['', None] and pd.notna(x) else '',
                            subset=['최종수익률%'], na_rep=''
                        )

                    ROW_H = 35
                    HEADER_H = 38
                    MAX_ROWS = 10
                    test_height = min(len(df_test_display), MAX_ROWS) * ROW_H + HEADER_H
                    st.dataframe(
                        styled_test,
                        hide_index=True,
                        width='stretch',
                        height=test_height,
                        key="test_df",
                        column_config={
                            "종목코드":    st.column_config.Column(width=75),
                            "시장":        st.column_config.Column(width=60),
                            "회사명":      st.column_config.Column(width=150),
                            "업종":        st.column_config.Column(width=150),
                            "타입":        st.column_config.Column(width=60),
                            "기준일":      st.column_config.Column(width=90),
                            "목표일":      st.column_config.Column(width=90),
                            "기준가":      st.column_config.Column(width=90),
                            "+5% 달성일":  st.column_config.Column(width=105),
                            "+10% 달성일": st.column_config.Column(width=105),
                            "최종종가":    st.column_config.Column(width=90),
                            "최종수익률%": st.column_config.Column(width=75),
                            "완료여부":    st.column_config.Column(width=60),
                        }
                    )
                else:
                    st.info("검색 결과가 없습니다.")
            else:
                st.info("테스트 데이터가 없습니다.")

    else:
        # =============================================
        # 단기/중기/매도/전체 결과리스트
        # =============================================
        if period == "단기":
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '시가총액 (KRW 억원)', '단기매수신호',
                        'OBV 상승 크로스', '거래대금 급증(20일평균2배)', '돌파(20일 고가 or MA20 상향)',
                        '외국인 순매수', '기관 순매수', '캔들', '섹터']:
                if col in df_display.columns: display_cols.append(col)

        elif period == "중기":
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '시가총액 (KRW 억원)', '중기매수신호',
                        'RSI 상승', 'OBV 우상향/크로스', '50MA > 200MA', '거래대금(20평균이상)',
                        '외국인 순매수', '기관 순매수', '캔들', '섹터']:
                if col in df_display.columns: display_cols.append(col)

        elif period == "매도":
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '시가총액 (KRW 억원)', '매도신호',
                        'RSI 과열(70 이상)', 'RSI 하강 지속', 'OBV 하락 크로스',
                        '외국인 순매수(리버스)', '기관 순매수(리버스)', '캔들(리버스)', '섹터(리버스)']:
                if col in df_display.columns: display_cols.append(col)

        else:  # 전체
            display_cols = ['종목코드', '시장', '회사명', '업종', '업종트렌드']
            for col in ['종가 (KRW)', '시가총액 (KRW 억원)', '단기신호', '중기신호',
                        'OBV 상승 크로스', '거래대금 급증(20일평균2배)', '돌파(20일 고가 or MA20 상향)',
                        'RSI 상승', 'OBV 우상향/크로스', '50MA > 200MA', '거래대금(20평균이상)',
                        '외국인 순매수', '기관 순매수', '캔들', '섹터']:
                if col in df_display.columns: display_cols.append(col)

        display_cols = [col for col in display_cols if col in df_display.columns]

        PAGE_SIZE = 50

        sortable_cols = ['시가총액 (KRW 억원)', '종가 (KRW)']
        if period == "단기":
            sortable_cols += ['단기매수신호']
        elif period == "중기":
            sortable_cols += ['중기매수신호']
        elif period == "전체":
            sortable_cols += ['단기신호', '중기신호']

        sortable_cols = [c for c in sortable_cols if c in df_display.columns]

        st.markdown("""
        <style>
        div[data-testid="stToggle"] > label {
            font-size: 0.85rem !important;
            white-space: nowrap !important;
        }
        div[data-testid="stNumberInput"] input {
            font-size: 0.85rem !important;
            height: 36px !important;
            padding: 4px 6px !important;
        }
        div[data-testid="stNumberInput"] > div {
            min-height: 36px !important;
        }
        </style>
        """, unsafe_allow_html=True)

        # ✅ 정렬 변경 시에만 페이지 1로 리셋하는 콜백 함수
        def _on_sort_change():
            st.session_state.kr_sort_col = st.session_state[f"sort_col_{period}"]
            st.session_state.kr_sort_asc = st.session_state[f"sort_asc_{period}"]
            st.session_state.kr_page = 1

        ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([5, 3, 2])
        with ctrl_col1:
            search_term = st.text_input("🔍 종목 검색", placeholder="코드 또는 회사명 입력",
                                        key=f"main_search_{period}", label_visibility="collapsed")
        with ctrl_col2:
            sort_col = st.selectbox(
                "정렬 기준", sortable_cols,
                index=sortable_cols.index(st.session_state.kr_sort_col)
                      if st.session_state.kr_sort_col in sortable_cols else 0,
                key=f"sort_col_{period}",
                label_visibility="collapsed",
                on_change=_on_sort_change   # ✅ 변경 시에만 페이지 리셋
            )
        with ctrl_col3:
            sort_asc = st.toggle(
                "오름차순",
                value=st.session_state.kr_sort_asc,
                key=f"sort_asc_{period}",
                on_change=_on_sort_change   # ✅ 변경 시에만 페이지 리셋
            )

        # ✅ 기존 if 블록 완전 삭제 (on_change 콜백으로 대체됨)
        # 현재 세션 상태에서 정렬 값을 읽어서 사용
        sort_col = st.session_state.kr_sort_col
        sort_asc = st.session_state.kr_sort_asc

        # 검색 필터
        if search_term:
            mask = (df_display['종목코드'].astype(str).str.contains(search_term, case=False, na=False)) | \
                   (df_display['회사명'].astype(str).str.contains(search_term, case=False, na=False))
            df_kr_filtered = df_display[mask & df_display['시장'].isin(KR_MARKETS)].copy()
        else:
            df_kr_filtered = df_display[df_display['시장'].isin(KR_MARKETS)].copy() \
                if '시장' in df_display.columns else pd.DataFrame()

        if not df_kr_filtered.empty:
            kr_total = len(df_kr_filtered)

            if sort_col in df_kr_filtered.columns:
                if '신호' in sort_col or '매수신호' in sort_col:
                    df_kr_filtered['_sort_tmp'] = df_kr_filtered[sort_col].astype(str).str.extract(r'(\d+)').astype(float)
                    df_kr_filtered = df_kr_filtered.sort_values('_sort_tmp', ascending=sort_asc).drop(columns=['_sort_tmp'])
                else:
                    df_kr_filtered = df_kr_filtered.sort_values(sort_col, ascending=sort_asc)
            df_kr_filtered = df_kr_filtered.reset_index(drop=True)

            # 페이지네이션 계산
            total_pages = max(1, (kr_total - 1) // PAGE_SIZE + 1)
            cur_page = min(st.session_state.kr_page, total_pages)
            start_idx = (cur_page - 1) * PAGE_SIZE
            end_idx = min(start_idx + PAGE_SIZE, kr_total)
            df_page = df_kr_filtered.iloc[start_idx:end_idx].copy()

            # 헤더: 제목 + 종목수 + 페이지이동 + CSV
            col_h1, col_h2, col_h3, col_h4 = st.columns([3, 2, 4, 1])
            with col_h1:
                st.markdown("#### 국내 (KR)")
            with col_h2:
                st.markdown(f"<div style='padding-top:8px; font-size:0.85rem; white-space:nowrap;'><b>총 {kr_total}개</b> · {cur_page}/{total_pages}p</div>", unsafe_allow_html=True)
            with col_h3:
                pg_c1, pg_c2, pg_c3 = st.columns([2, 2, 2])
                with pg_c1:
                    if st.button("◀ 이전", key=f"prev_{period}", disabled=(cur_page <= 1), use_container_width=True):
                        st.session_state.kr_page = cur_page - 1
                        st.rerun()
                with pg_c2:
                    input_page = st.number_input(
                        "p", min_value=1, max_value=total_pages,
                        value=cur_page, key=f"page_input_{period}",
                        label_visibility="collapsed"
                    )
                    if input_page != cur_page:
                        st.session_state.kr_page = int(input_page)
                        st.rerun()
                with pg_c3:
                    if st.button("다음 ▶", key=f"next_{period}", disabled=(cur_page >= total_pages), use_container_width=True):
                        st.session_state.kr_page = cur_page + 1
                        st.rerun()
            with col_h4:
                csv_kr = df_kr_filtered[display_cols].to_csv(index=False).encode('utf-8-sig')
                st.markdown('<div class="csv-btn-fixed">', unsafe_allow_html=True)
                st.download_button(
                    label="💾CSV",
                    data=csv_kr,
                    file_name=f'kr_stocks_{period}.csv',
                    mime='text/csv',
                    key=f"download_kr_{period}",
                    use_container_width=True
                )
                st.markdown('</div>', unsafe_allow_html=True)

            kr_display_cols = [col for col in display_cols if '(USD' not in col and '(N/A)' not in col]
            df_page_full = df_page[kr_display_cols].copy().reset_index(drop=True)
            kr_sector_trends = df_page_full['업종트렌드'].copy() if '업종트렌드' in df_page_full.columns else None
            df_kr_display = df_page_full.drop(columns=['업종트렌드'], errors='ignore')

            kr_key = f"kr_dataframe_{period}"

            if kr_sector_trends is not None:
                bg_colors = kr_sector_trends.apply(
                    lambda x: get_sector_trend_color(x) if pd.notna(x) else ''
                ).fillna('').reset_index(drop=True)
                styled_kr = df_kr_display.style.apply(
                    lambda col: [f'background-color: {bg}' if bg else '' for bg in bg_colors],
                    axis=0
                )
            else:
                styled_kr = df_kr_display.style

            format_dict = {}
            for col in df_kr_display.columns:
                if df_kr_display[col].dtype in ['int64', 'float64']:
                    if col == '종가 (KRW)': format_dict[col] = '{:,.0f}'
                    elif '시가총액' in col: format_dict[col] = '{:,.2f}'
                    else: format_dict[col] = '{:,.2f}'
            if format_dict:
                styled_kr = styled_kr.format(format_dict, na_rep='')

            ROW_H = 35
            HEADER_H = 38
            MAX_ROWS = 10
            kr_height = min(len(df_kr_display), MAX_ROWS) * ROW_H + HEADER_H

            event_kr = st.dataframe(
                styled_kr,
                on_select="rerun",
                selection_mode="single-row",
                hide_index=True,
                width='stretch',
                height=kr_height,
                key=kr_key,
                column_config={
                    "종목코드": st.column_config.Column(width=75),
                    "시장": st.column_config.Column(width=60),
                    "회사명": st.column_config.Column(width=150),
                    "업종": st.column_config.Column(width=150),
                    "종가 (KRW)": st.column_config.Column(width=90),
                    "시가총액 (KRW 억원)": st.column_config.Column(width=120),
                    "단기매수신호": st.column_config.Column(width=90),
                    "중기매수신호": st.column_config.Column(width=90),
                    "단기신호": st.column_config.Column(width=90),
                    "중기신호": st.column_config.Column(width=90),
                    "OBV 상승 크로스": st.column_config.Column(width=60),
                    "거래대금 급증(20일평균2배)": st.column_config.Column(width=60),
                    "돌파(20일 고가 or MA20 상향)": st.column_config.Column(width=60),
                    "RSI 상승": st.column_config.Column(width=60),
                    "OBV 우상향/크로스": st.column_config.Column(width=60),
                    "50MA > 200MA": st.column_config.Column(width=60),
                    "거래대금(20평균이상)": st.column_config.Column(width=60),
                    "RSI 과열(70 이상)": st.column_config.Column(width=60),
                    "RSI 하강 지속": st.column_config.Column(width=60),
                    "OBV 하락 크로스": st.column_config.Column(width=60),
                    "외국인 순매수": st.column_config.Column(width=60),
                    "기관 순매수": st.column_config.Column(width=60),
                    "캔들": st.column_config.Column(width=60),
                    "섹터": st.column_config.Column(width=60),
                    "외국인 순매수(리버스)": st.column_config.Column(width=60),
                    "기관 순매수(리버스)": st.column_config.Column(width=60),
                    "캔들(리버스)": st.column_config.Column(width=60),
                    "섹터(리버스)": st.column_config.Column(width=60),
                    "매도신호": st.column_config.Column(width=90),
                }
            )

            if event_kr.selection.rows:
                selected_idx = event_kr.selection.rows[0]
                real_idx = start_idx + selected_idx
                new_symbol = df_kr_filtered.iloc[real_idx]['종목코드']
                if new_symbol != st.session_state.selected_symbol or st.session_state.selected_market != 'KR':
                    st.session_state.selected_symbol = new_symbol
                    st.session_state.selected_market = 'KR'
                    st.rerun()

        else:
            st.info("조건에 맞는 종목이 없습니다.")
else:
    st.info("조건에 맞는 종목이 없습니다.")

# =============================================
# 자세히 보기
# =============================================
st.markdown("---")
st.markdown("### 자세히 보기")

if st.session_state.selected_symbol and st.session_state.selected_market:
    symbol = st.session_state.selected_symbol
    market = st.session_state.selected_market

    if not df_display.empty:
        selected_data = df_display[df_display['종목코드'] == symbol]

        if not selected_data.empty:
            row = selected_data.iloc[0]

            if period == "백데이터":
                ind_data = get_indicator_data(symbol, market)
                if ind_data is not None:
                    row = pd.concat([row, ind_data])

            meta = load_meta()
            meta_dict = get_meta_info(meta, symbol, market)

            st.markdown(f"**종목**: {row['회사명']}")
            st.markdown(f"**코드**: {symbol} · **시장**: {market} · **업종**: {row.get('업종', 'N/A')}")

            if market in KR_MARKETS:
                if 'ownership_foreign_institution' in row and pd.notna(row['ownership_foreign_institution']):
                    ownership_val = float(row['ownership_foreign_institution'])
                    if ownership_val > 0:
                        st.markdown(f"**기관+외국인 보유율**: {ownership_val:.2f}%")

            if '업종트렌드' in row:
                trend_text = row['업종트렌드']
                bg_color = get_sector_trend_color(trend_text)
                if bg_color:
                    st.markdown(
                        f"<div style='background-color: {bg_color}; padding: 8px 12px; border-radius: 6px; margin: 4px 0;'>"
                        f"<strong>업종트렌드</strong>: {trend_text}</div>",
                        unsafe_allow_html=True
                    )
                else:
                    st.markdown(f"**업종트렌드**: {trend_text}")

            st.markdown("---")
            st.markdown("#### 주요 지표")

            kpi_col1, kpi_col2 = st.columns(2)

            with kpi_col1:
                if all(k in row for k in ['RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest']):
                    st.metric(
                        "RSI 3일 데이터",
                        f"{row['RSI_3일_2ago']:.2f} / {row['RSI_3일_1ago']:.2f} / {row['RSI_3일_latest']:.2f}"
                    )
                if '종가 (KRW)' in row and pd.notna(row['종가 (KRW)']):
                    st.metric("종가", f"{row['종가 (KRW)']:,.0f} 원")
                elif '종가 (USD)' in row and pd.notna(row['종가 (USD)']):
                    st.metric("종가", f"${row['종가 (USD)']:,.2f}")
                if '시가총액 (KRW 억원)' in row and pd.notna(row['시가총액 (KRW 억원)']):
                    st.metric("시가총액", f"{row['시가총액 (KRW 억원)']:,.0f} 억원")
                if 'PER_TTM (x)' in row and 'EPS_TTM' in row:
                    st.metric("PER / EPS", f"{row['PER_TTM (x)']:.2f} / {row['EPS_TTM']:.2f}")
                ind_data = get_indicator_data(symbol, market)
                if ind_data is not None and 'ma20_latest' in ind_data and 'ma200_latest' in ind_data:
                    if pd.notna(ind_data['ma20_latest']) and pd.notna(ind_data['ma200_latest']):
                        st.metric("MA20 / MA200", f"{ind_data['ma20_latest']:.2f} / {ind_data['ma200_latest']:.2f}")

            with kpi_col2:
                if all(k in row for k in ['20일평균거래대금 (KRW 억원)', '오늘거래대금 (KRW 억원)', '회전율 (%)']):
                    avg_val = f"{row['20일평균거래대금 (KRW 억원)']:,.0f}억원"
                    today_val = f"{row['오늘거래대금 (KRW 억원)']:,.0f}억원"
                    turnover_val = f"{row['회전율 (%)']:.2f}%"
                    st.metric("20일평균 / 오늘 / 회전율", f"{avg_val} / {today_val} / {turnover_val}")

                if '캔들(상단)' in row and '캔들(하단)' in row:
                    upper = int(row['캔들(상단)'])
                    lower = int(row['캔들(하단)'])
                    st.markdown(
                        f"<div style='margin-bottom: 1rem;'>"
                        f"<div style='font-weight: 600; font-size: 0.875rem; margin-bottom: 0.25rem;'>캔들 (상단/하단)</div>"
                        f"<div style='font-size: 1.1rem; font-weight: 800;'>"
                        f"<span style='color: #dc2626;'>{upper}</span> / <span style='color: #2563eb;'>{lower}</span>"
                        f"</div></div>",
                        unsafe_allow_html=True
                    )

                def format_value(val):
                    if val > 0: return f"<span style='color: #dc2626;'>{val:,}</span>"
                    elif val < 0: return f"<span style='color: #2563eb;'>{val:,}</span>"
                    else: return f"{val:,}"

                if market in KR_MARKETS:
                    foreign_cols = ['외국인순매수_5일전 (주)', '외국인순매수_4일전 (주)', '외국인순매수_3일전 (주)',
                                    '외국인순매수_2일전 (주)', '외국인순매수_1일전 (주)', '외국인순매수_합산 (주)']
                    if all(col in row for col in foreign_cols):
                        f3 = int(row['외국인순매수_3일전 (주)'])
                        f2 = int(row['외국인순매수_2일전 (주)'])
                        f1 = int(row['외국인순매수_1일전 (주)'])
                        f_sum = int(row['외국인순매수_합산 (주)'])
                        st.markdown(
                            f"<div style='margin-bottom: 1rem;'>"
                            f"<div style='font-weight: 600; font-size: 0.875rem; margin-bottom: 0.25rem;'>외국인 순매수(5일)</div>"
                            f"<div style='font-size: 1.1rem; font-weight: 800;'>"
                            f"{format_value(f_sum)} ({format_value(f3)} / {format_value(f2)} / {format_value(f1)})"
                            f"</div></div>",
                            unsafe_allow_html=True
                        )

                    meta_dict = get_meta_info(meta, symbol, market)
                    institutional_data = meta_dict.get('institutional_net_buy', [0, 0, 0, 0, 0])
                    if len(institutional_data) >= 5:
                        i3 = institutional_data[2]
                        i2 = institutional_data[1]
                        i1 = institutional_data[0]
                        i_sum = sum(institutional_data)
                        st.markdown(
                            f"<div style='margin-bottom: 1rem;'>"
                            f"<div style='font-weight: 600; font-size: 0.875rem; margin-bottom: 0.25rem;'>기관 순매수(5일)</div>"
                            f"<div style='font-size: 1.1rem; font-weight: 800;'>"
                            f"{format_value(i_sum)} ({format_value(i3)} / {format_value(i2)} / {format_value(i1)})"
                            f"</div></div>",
                            unsafe_allow_html=True
                        )

            st.markdown("---")
            st.markdown("#### 📊 차트 기간 선택")
            chart_period = st.radio(
                "차트 기간",
                ["1개월", "3개월", "6개월", "1년", "전체"],
                index=2,
                horizontal=True,
                label_visibility="collapsed",
                key="chart_period_selector"
            )
            if chart_period != st.session_state.chart_period:
                st.session_state.chart_period = chart_period

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
    st.info("위 테이블에서 종목을 선택하세요.")