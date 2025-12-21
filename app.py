# Modified code with fixes
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

# 캐시 클리어 추가: 오래된 데이터 로드 방지
st.cache_data.clear()
st.cache_resource.clear()

st.set_page_config(page_title="Smart Stock Screener 📈", layout="wide")
st.header("Trading Copilot 🚀")
st.markdown("""
<style>
    /* 전체 앱 기본 폰트 크기 */
    html, body, [class*="css"] {
        font-size: 14px !important;
    }
</style>
""", unsafe_allow_html=True)

warnings.filterwarnings("ignore", message=".*keyword arguments.*deprecated.*config.*")
warnings.filterwarnings("ignore", category=FutureWarning, message=".*to_pydatetime.*")
warnings.filterwarnings("ignore", category=UserWarning, module="pykrx")

@st.cache_data
def load_data():
    DB_PATH = "data/meta/universe.db"
    if not os.path.exists(DB_PATH):
        st.warning("데이터 없음 – 배치 실행하세요.")
        return pd.DataFrame()  # 빈 데이터 반환
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
    # 항상 컬럼 초기화 (빈 df라도 컬럼 생성)
    df['foreign_net_buy_3ago'] = np.nan
    df['foreign_net_buy_2ago'] = np.nan
    df['foreign_net_buy_1ago'] = np.nan
    if df.empty:
        return df  # 빈 df 반환 but 컬럼 있음
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
        condition = "1=1"  # No OBV or RSI condition
    elif filter_condition == "short_term":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AND (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50) AND (today_trading_value > 1.5 * avg_trading_value_20d)"
    elif filter_condition == "mid_term":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AND (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50)"
    elif filter_condition == "long_term":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AND (rsi_d_2ago > rsi_d_1ago AND rsi_d_1ago > rsi_d_latest AND rsi_d_latest <= 50)"
    
    # 거래대금, 회전율 조건 제거 → 시가총액 조건만 유지
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
            per, eps, cap_status, upper_closes, lower_closes,  -- 추가: upper_closes, lower_closes
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
        per, eps, cap_status, upper_closes, lower_closes,  -- 추가
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
    # 컬럼 이름에 단위 추가 (기존과 동일)
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
        })

    # 숫자 컬럼: 단위 변환만 적용 (숫자 타입 유지)
    def safe_float(x):
        return float(x) if pd.notna(x) else 0.0

    if '시가총액 (KRW 억원)' in df.columns or '시가총액 (USD M)' in df.columns:
        col_name = df.columns[df.columns.str.startswith('시가총액 (')][0]
        df[col_name] = df[col_name].apply(safe_float)
        if market_type == 'KR':
            df[col_name] = df[col_name] / 1e8  # 억원 단위 (숫자 유지)
        else:
            df[col_name] = df[col_name] / 1e6  # Million USD (숫자 유지)

    if '20일평균거래대금 (KRW 억원)' in df.columns or '20일평균거래대금 (USD M)' in df.columns:
        col_name = df.columns[df.columns.str.startswith('20일평균거래대금 (')][0]
        df[col_name] = df[col_name].apply(safe_float)
        if market_type == 'KR':
            df[col_name] = df[col_name] / 1e8  # 억원 (숫자)
        else:
            df[col_name] = df[col_name] / 1e6  # Million USD (숫자)

    if '오늘거래대금 (KRW 억원)' in df.columns or '오늘거래대금 (USD M)' in df.columns:
        col_name = df.columns[df.columns.str.startswith('오늘거래대금 (')][0]
        df[col_name] = df[col_name].apply(safe_float)
        if market_type == 'KR':
            df[col_name] = df[col_name] / 1e8  # 억원 (숫자)
        else:
            df[col_name] = df[col_name] / 1e6  # Million USD (숫자)

    if '회전율 (%)' in df.columns:
        df['회전율 (%)'] = df['회전율 (%)'].apply(safe_float) * 100  # % 단위 (숫자)

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
        df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else 0)  # int로 유지 (US는 0)

    # bool 컬럼: 문자열로 변환 (기존과 동일, TextColumn으로 렌더링)
    def bool_fmt(x):
        return '✅' if x else '❌'

    bool_cols = ['OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(bool_fmt)  # 문자열로 변환 (TextColumn)

    # 숫자 컬럼 강제 반올림 (서버 이슈 해결)
    numeric_cols = df.select_dtypes(include='float').columns
    numeric_cols = numeric_cols.drop('회전율 (%)', errors='ignore')  # 이 줄 추가: 회전율 제외
    df[numeric_cols] = df[numeric_cols].round(2)

    return df  # styled_df 대신 기본 df 반환

def show_graphs(symbol, market):
    base_dir = "data"
    daily_path = os.path.join(base_dir, ('us_daily' if market == 'US' else 'kr_daily'), f"{symbol}.csv")
    if os.path.exists(daily_path):
        df_chart = pd.read_csv(daily_path, index_col=0)
        if market == 'KR':
            df_chart = df_chart.rename(columns={'시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'})
        close_col = 'Close'
        vol_col = 'Volume'
        
        if close_col in df_chart.columns:
            df_chart[close_col] = df_chart[close_col].round(2)
        
        # Price Chart (Close 선 범례 추가)
        fig_price = px.line(df_chart, x=df_chart.index, y=close_col, title=f"{symbol} Close")
        fig_price.update_traces(name='Close', showlegend=True)  # 범례 추가
        fig_price.update_layout(height=400)
        fig_price.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightPink', title_text=None)  # x축 레이블 삭제 (필요시)
        fig_price.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightPink', title_text=None)  # y축 레이블 삭제
        st.plotly_chart(fig_price, config={'displayModeBar': False}, key=f"{st.session_state.current_tab}_{symbol}_price_chart")
        
        # MACD Chart
        macd_df = ta.macd(df_chart[close_col], fast=12, slow=26)
        macd = macd_df['MACD_12_26_9']
        signal = macd_df['MACDs_12_26_9']
        hist = macd_df['MACDh_12_26_9']
        df_macd = pd.DataFrame({'Date': df_chart.index, 'MACD': macd, 'Signal': signal, 'Hist': hist}).dropna()
        fig_macd = go.Figure()
        fig_macd.add_trace(go.Scatter(x=df_macd['Date'], y=df_macd['MACD'], name='MACD', line=dict(color='blue')))
        fig_macd.add_trace(go.Scatter(x=df_macd['Date'], y=df_macd['Signal'], name='Signal', line=dict(color='red')))
        fig_macd.add_trace(go.Bar(x=df_macd['Date'], y=df_macd['Hist'], name='Histogram'))
        fig_macd.update_layout(height=400, title="MACD")
        fig_macd.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightPink')
        fig_macd.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightPink')
        st.plotly_chart(fig_macd, config={'displayModeBar': False}, key=f"{st.session_state.current_tab}_{symbol}_macd_chart")
        
        # OBV Chart with Signal
        obv = ta.obv(df_chart[close_col], df_chart[vol_col])
        obv_signal = ta.sma(obv, length=9)
        df_obv = pd.DataFrame({'Date': df_chart.index, 'OBV': obv, 'OBV_SIGNAL': obv_signal}).dropna()
        fig_obv = go.Figure()
        fig_obv.add_trace(go.Scatter(x=df_obv['Date'], y=df_obv['OBV'], name='OBV', line=dict(color='green')))
        fig_obv.add_trace(go.Scatter(x=df_obv['Date'], y=df_obv['OBV_SIGNAL'], name='OBV Signal', line=dict(color='orange')))
        fig_obv.update_layout(height=400, title="OBV")
        fig_obv.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightPink')
        fig_obv.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightPink')
        st.plotly_chart(fig_obv, config={'displayModeBar': False}, key=f"{st.session_state.current_tab}_{symbol}_obv_chart")
        
        # RSI Chart (RSI 선 범례 추가)
        rsi = ta.rsi(df_chart[close_col], length=14)
        df_rsi = pd.DataFrame({'Date': df_chart.index, 'RSI': rsi}).dropna()
        fig_rsi = px.line(df_rsi, x='Date', y='RSI', title="RSI")
        fig_rsi.add_hline(y=30, line_dash="dot", line_color="red", annotation_text="OverSold (30)", annotation_position="bottom right")
        fig_rsi.add_hline(y=70, line_dash="dot", line_color="red", annotation_text="OverBought (70)", annotation_position="top right")
        fig_rsi.update_traces(name='RSI', showlegend=True)  # 범례 추가
        fig_rsi.update_layout(height=400)
        fig_rsi.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightPink', title_text=None)
        fig_rsi.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightPink', title_text=None)  # y축 레이블 삭제
        st.plotly_chart(fig_rsi, config={'displayModeBar': False}, key=f"{st.session_state.current_tab}_{symbol}_rsi_chart")
    else:
        st.warning("데이터 없음")

def prepare_tab_df(df, is_total=False):
    if is_total:
        return df
    else:
        return df

def get_filtered_symbols(df, search_term):
    if search_term:
        df_filtered = df[(df['종목코드'].str.contains(search_term, case=False)) | (df['회사명'].str.contains(search_term, case=False))]
        return df_filtered['종목코드'].tolist()
    return df['종목코드'].tolist() if '종목코드' in df.columns else []

# 세션 상태 초기화 (앱 시작 시)
if 'filter_results' not in st.session_state:
    st.session_state.filter_results = pd.DataFrame()
if 'filter_results_kr' not in st.session_state:
    st.session_state.filter_results_kr = pd.DataFrame()
if 'filter_results_us' not in st.session_state:
    st.session_state.filter_results_us = pd.DataFrame()
if 'selected_symbol' not in st.session_state:
    st.session_state.selected_symbol = None
if 'con' not in st.session_state:
    st.session_state.con = None
if 'current_tab' not in st.session_state:
    st.session_state.current_tab = "장기"

df_ind = load_data()
con = get_db_connection()

main_tabs = st.tabs(["필터", "백테스팅", "KR", "US", "로그"])

column_config_kr = {
    "종목코드": st.column_config.TextColumn(width="small"),
    "회사명": st.column_config.TextColumn(width="small"),
    "시장": st.column_config.TextColumn(width="small"),
    "RSI_3일_2ago": st.column_config.NumberColumn(width=80, format="%.2f"),
    "RSI_3일_1ago": st.column_config.NumberColumn(width=80, format="%.2f"),
    "RSI_3일_latest": st.column_config.NumberColumn(width=80, format="%.2f"),
    "종가 (KRW)": st.column_config.NumberColumn(width=80, format="%.0f"),
    "시가총액 (KRW 억원)": st.column_config.NumberColumn(width=80, format="%.0f"),
    "업데이트": st.column_config.TextColumn(width="small"),
    "20일평균거래대금 (KRW 억원)": st.column_config.NumberColumn(width=80, format="%.0f"),
    "오늘거래대금 (KRW 억원)": st.column_config.NumberColumn(width=80, format="%.0f"),
    "회전율 (%)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "캔들(5일)": st.column_config.TextColumn(width=120),  # 추가
    "외국인순매수_3일전 (주)": st.column_config.NumberColumn(width=80, format="%d"),
    "외국인순매수_2일전 (주)": st.column_config.NumberColumn(width=80, format="%d"),
    "외국인순매수_1일전 (주)": st.column_config.NumberColumn(width=80, format="%d"),
    "PER_TTM (x)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "EPS_TTM": st.column_config.NumberColumn(width=80, format="%.2f"),
    "OBV_상승": st.column_config.TextColumn(width="small"),
    "RSI_3상승": st.column_config.TextColumn(width="small"),
    "RSI_3하강": st.column_config.TextColumn(width="small"),
    "거래대금_상승": st.column_config.TextColumn(width="small"),
    "EPS > 0": st.column_config.TextColumn(width="small"),
    "3<=PER<=30": st.column_config.TextColumn(width="small"),
    "외국인 순매수": st.column_config.TextColumn(width="small"),
    "캔들": st.column_config.TextColumn(width="small"),
}

column_config_us = {
    "종목코드": st.column_config.TextColumn(width="small"),
    "회사명": st.column_config.TextColumn(width="small"),
    "시장": st.column_config.TextColumn(width="small"),
    "RSI_3일_2ago": st.column_config.NumberColumn(width=80, format="%.2f"),
    "RSI_3일_1ago": st.column_config.NumberColumn(width=80, format="%.2f"),
    "RSI_3일_latest": st.column_config.NumberColumn(width=80, format="%.2f"),
    "종가 (USD)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "시가총액 (USD M)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "업데이트": st.column_config.TextColumn(width="small"),
    "20일평균거래대금 (USD M)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "오늘거래대금 (USD M)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "회전율 (%)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "캔들(5일)": st.column_config.TextColumn(width=120),  # 추가
    "외국인순매수_3일전 (N/A)": st.column_config.NumberColumn(width=80, format="%d"),
    "외국인순매수_2일전 (N/A)": st.column_config.NumberColumn(width=80, format="%d"),
    "외국인순매수_1일전 (N/A)": st.column_config.NumberColumn(width=80, format="%d"),
    "PER_TTM (x)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "EPS_TTM": st.column_config.NumberColumn(width=80, format="%.2f"),
    "OBV_상승": st.column_config.TextColumn(width="small"),
    "RSI_3상승": st.column_config.TextColumn(width="small"),
    "RSI_3하강": st.column_config.TextColumn(width="small"),
    "거래대금_상승": st.column_config.TextColumn(width="small"),
    "EPS > 0": st.column_config.TextColumn(width="small"),
    "3<=PER<=30": st.column_config.TextColumn(width="small"),
    "외국인 순매수": st.column_config.TextColumn(width="small"),
    "캔들": st.column_config.TextColumn(width="small"),
}

with main_tabs[0]:  # 필터 탭
    st.header("Search")

    with st.form(key="filter_form"):
        market = st.selectbox("시장", ["모두", "US", "KR"])
        
        col1, col2, col3, col4 = st.columns(4)  
        
        with col1:
            obv_apply = st.checkbox("OBV 상승 크로스")
            rsi_up_apply = st.checkbox("RSI 상승 지속")

        with col2:
            rsi_down_apply = st.checkbox("RSI 하강 지속")
            eps_per_apply = st.checkbox("EPS & PER")

        with col3:
            trading_apply = st.checkbox("거래대금")
            foreign_apply = st.checkbox("외국인 순매수")

        with col4:
            candle_apply = st.checkbox("캔들")
        
        submitted = st.form_submit_button("🔍필터 적용")
        
        if submitted:
            use_us = market in ["모두", "US"]
            use_kr = market in ["모두", "KR"]
            
            condition = "rsi_d_latest == rsi_d_latest"
            if obv_apply:
                condition += " and (obv_latest > signal_obv_latest and obv_1ago <= signal_obv_1ago)"
            if rsi_up_apply:
                condition += " and (rsi_d_2ago < rsi_d_1ago and rsi_d_1ago < rsi_d_latest and rsi_d_latest <= 50)"
            if rsi_down_apply:
                condition += " and (rsi_d_2ago > rsi_d_1ago and rsi_d_1ago > rsi_d_latest and rsi_d_latest <= 50)"
            if eps_per_apply:
                condition += " and eps > 0 and per >= 3 and per <= 30"
            if trading_apply:
                condition += " and today_trading_value > 1.5 * avg_trading_value_20d"
            
            df_filter = run_screener_query(con, filter_condition="eps_per_only", use_us=use_us, use_kr=use_kr)
            df_filter = df_filter.query(condition)
            
            # foreign_net_buy 추가 (기존)
            df_filter = add_foreign_net_buy(df_filter)

            # foreign_apply 안전하게 적용
            if foreign_apply and not df_filter.empty and 'foreign_net_buy_1ago' in df_filter.columns:
                df_filter = df_filter[(df_filter['foreign_net_buy_1ago'] > 0) & (df_filter['foreign_net_buy_2ago'] > 0)]

            # candle_apply도 비슷하게 (안전 추가)
            if candle_apply and not df_filter.empty and 'upper_closes' in df_filter.columns:
                df_filter = df_filter[df_filter['upper_closes'] >= 3]

            df_filter = add_names(df_filter)
            df_filter = add_close_price(df_filter)

            if not df_filter.empty:
                df_filter['foreign_positive'] = ((df_filter['foreign_net_buy_1ago'] > 0) & (df_filter['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
                df_filter['candle_upper_3'] = (df_filter['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
                
                df_filter['eps_positive'] = df_filter['eps'] > 0
                df_filter['per_range'] = (df_filter['per'] >= 3) & (df_filter['per'] <= 30)
                
                df_filter['캔들(5일)'] = df_filter['upper_closes'].astype(str) + ' (상단) / ' + df_filter['lower_closes'].astype(str) + ' (하단)'
            else:
                # 빈 결과 처리: 빈 DF로 세션 상태 업데이트
                st.session_state.filter_results_kr = pd.DataFrame()
                st.session_state.filter_results_us = pd.DataFrame()
                st.session_state.filter_results = pd.DataFrame()
                st.info("필터 결과가 없습니다.")
                # 빈 테이블 표시를 위해 continue 하지 않고, 아래 테이블 표시 부분에서 빈 상태 반영
            
            df_filter = df_filter.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                       'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                       'close': '종가',
                       'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                       'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                       'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                       'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}.items() if k in df_filter.columns})
            
            df_kr_results = df_filter[df_filter['시장'] == 'KR'] if '시장' in df_filter.columns else pd.DataFrame()
            df_us_results = df_filter[df_filter['시장'] == 'US'] if '시장' in df_filter.columns else pd.DataFrame()
            
            if not df_kr_results.empty:
                cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
                df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
                df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
                df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.session_state.filter_results_kr = df_kr_results  # 빈 경우도 저장
            
            if not df_us_results.empty:
                cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
                df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
                df_us_results = df_us_results.sort_values('시가총액', ascending=False)
                df_us_results = format_dataframe(df_us_results, 'US')
            st.session_state.filter_results_us = df_us_results  # 빈 경우도 저장
            
            # 전체 필터 결과 저장 (검색/선택용)
            st.session_state.filter_results = pd.concat([st.session_state.filter_results_kr, st.session_state.filter_results_us], ignore_index=True)
    
    # 테이블 표시 (폼 밖, 세션 상태 기반)
    if not st.session_state.filter_results_kr.empty:
        st.markdown(f"### 국내 (KR) - 후보 수: {len(st.session_state.filter_results_kr)}")  
        st.dataframe(st.session_state.filter_results_kr, column_config=column_config_kr)
    else:
        st.markdown("### 국내 (KR) - 후보 수: 0")  
        st.dataframe(pd.DataFrame(), column_config=column_config_kr)  # 빈 테이블 표시
    
    if not st.session_state.filter_results_us.empty:
        st.markdown(f"### 해외 (US) - 후보 수: {len(st.session_state.filter_results_us)}")  
        st.dataframe(st.session_state.filter_results_us, column_config=column_config_us)
    else:
        st.markdown("### 해외 (US) - 후보 수: 0")  
        st.dataframe(pd.DataFrame(), column_config=column_config_us)  # 빈 테이블 표시
    
    # 검색 및 선택 (세션 상태 기반, 데이터 사라짐 방지)
    search_term = st.text_input("종목 검색 (필터)", placeholder="코드/회사명 입력", key="search_filter")
    
    filtered_symbols = get_filtered_symbols(st.session_state.filter_results, search_term)
    
    if filtered_symbols:
        selected_symbol = st.selectbox("종목 선택 (필터)", filtered_symbols, key="select_filter")
        if selected_symbol != st.session_state.selected_symbol:
            st.session_state.selected_symbol = selected_symbol
        if st.session_state.selected_symbol:
            market = st.session_state.filter_results[st.session_state.filter_results['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if not st.session_state.filter_results.empty else 'US'
            show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("검색 결과 없음")

with main_tabs[1]:  # 백테스팅 탭
    st.header("Data Research")

    BACKTEST_DB_PATH = "data/meta/backtest.db"
    if not os.path.exists(BACKTEST_DB_PATH):
        st.warning("백테스팅 DB 없음 – 배치 실행하세요.")
    else:
        con_back = duckdb.connect(BACKTEST_DB_PATH, read_only=True)
        df_back = con_back.execute("SELECT * FROM backtest").fetchdf()
        con_back.close()
        
        if df_back.empty:
            st.info("백테스팅 데이터 없음")
        else:
            # symbol zfill 적용 (leading zero 보장)
            df_back['symbol'] = df_back.apply(lambda row: str(row['symbol']).zfill(6) if row['market'] == 'KR' else str(row['symbol']), axis=1)
            
            # 탭별 df 분리
            df_long_back = df_back[df_back['type'] == 'long'].copy()
            df_short_back = df_back[df_back['type'] == 'short'].copy()
            df_mid_back = df_back[df_back['type'] == 'mid'].copy()
            
            # 컬럼 이름 한글화 및 선택 (type 제외)
            rename_dict = {
                'symbol': '종목코드',
                'name': '회사명',
                'market': '시장',
                'rsi_d_2ago': 'RSI_3일_2ago',
                'rsi_d_1ago': 'RSI_3일_1ago',
                'rsi_d_latest': 'RSI_3일_latest',
                'close': '종가',
                'market_cap': '시가총액',
                'avg_trading_value_20d': '20일평균거래대금',
                'today_trading_value': '오늘거래대금',
                'turnover': '회전율',
                'per': 'PER_TTM',
                'eps': 'EPS_TTM',
                'cap_status': '업데이트',
                'latest_close': '최신종가',
                'latest_update': '최신업데이트',
                'change_rate': '변동율 (%)'
            }
            
            def apply_rename_format_and_unit(df, market_type):
                if df.empty:
                    return df
                df = df.rename(columns=rename_dict)
                # 캔들(5일) 생성
                if 'upper_closes' in df.columns and 'lower_closes' in df.columns:
                    df['캔들(5일)'] = df['upper_closes'].astype(str) + ' (상단) / ' + df['lower_closes'].astype(str) + ' (하단)'
                
                # ⭐ 회전율 먼저 백업 (반올림 방지)
                turnover_backup = None
                if '회전율' in df.columns:
                    turnover_backup = df['회전율'].copy()
                
                # 숫자 컬럼 반올림 (회전율 제외)
                numeric_cols = df.select_dtypes(include='float').columns
                numeric_cols = numeric_cols.drop('회전율', errors='ignore')  # 회전율 제외
                df[numeric_cols] = df[numeric_cols].round(2)
                
                # 단위 적용
                df = format_dataframe(df, market_type)
                
                # ⭐ 회전율 복원 (반올림 방지)
                if turnover_backup is not None and '회전율 (%)' in df.columns:
                    df['회전율 (%)'] = turnover_backup * 100  # % 단위만 적용, 반올림 없음
                
                # 최신종가 단위 추가
                if market_type == 'KR':
                    if '최신종가' in df.columns:
                        df = df.rename(columns={'최신종가': '최신종가 (KRW)'})
                        df['최신종가 (KRW)'] = df['최신종가 (KRW)'].apply(lambda x: float(x) if pd.notna(x) else 0.0).round(0)
                    # cols 재정의 (단위 반영)
                    cols = [
                        '종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest',
                        '종가 (KRW)', '시가총액 (KRW 억원)', '업데이트', '20일평균거래대금 (KRW 억원)', '오늘거래대금 (KRW 억원)', '회전율 (%)',
                        '캔들(5일)', 'PER_TTM (x)', 'EPS_TTM', '최신종가 (KRW)', '최신업데이트', '변동율 (%)'
                    ]
                elif market_type == 'US':
                    if '최신종가' in df.columns:
                        df = df.rename(columns={'최신종가': '최신종가 (USD)'})
                        df['최신종가 (USD)'] = df['최신종가 (USD)'].apply(lambda x: float(x) if pd.notna(x) else 0.0).round(2)
                    # cols 재정의 (단위 반영)
                    cols = [
                        '종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest',
                        '종가 (USD)', '시가총액 (USD M)', '업데이트', '20일평균거래대금 (USD M)', '오늘거래대금 (USD M)', '회전율 (%)',
                        '캔들(5일)', 'PER_TTM (x)', 'EPS_TTM', '최신종가 (USD)', '최신업데이트', '변동율 (%)'
                    ]
                df = df[[col for col in cols if col in df.columns]]
                return df
            
            # column_config 업데이트 (최신종가 추가)
            column_config_kr['최신종가 (KRW)'] = st.column_config.NumberColumn(format="%.0f")
            column_config_us['최신종가 (USD)'] = st.column_config.NumberColumn(format="%.2f")
            
            back_sub_tabs = st.tabs(["장기", "단기", "중기"])
            
            with back_sub_tabs[0]:  # 장기
                kr_long_back = apply_rename_format_and_unit(df_long_back[df_long_back['market'] == 'KR'], 'KR')
                us_long_back = apply_rename_format_and_unit(df_long_back[df_long_back['market'] == 'US'], 'US')
                if not kr_long_back.empty:
                    kr_long_back = kr_long_back.sort_values(['변동율 (%)', '시가총액 (KRW 억원)'], ascending=[False, False])
                    total_kr = len(kr_long_back)
                    positive_kr = (kr_long_back['변동율 (%)'] > 0).sum()
                    negative_kr = (kr_long_back['변동율 (%)'] < 0).sum()
                    st.markdown(f"### 국내 (KR) (총: {total_kr} / 상승: {positive_kr} / 하락: {negative_kr})")
                    st.dataframe(kr_long_back, column_config=column_config_kr)
                else:
                    st.info("KR 장기 데이터 없음")
                if not us_long_back.empty:
                    us_long_back = us_long_back.sort_values(['변동율 (%)', '시가총액 (USD M)'], ascending=[False, False])
                    total_us = len(us_long_back)
                    positive_us = (us_long_back['변동율 (%)'] > 0).sum()
                    negative_us = (us_long_back['변동율 (%)'] < 0).sum()
                    st.markdown(f"### 해외 (US) (총: {total_us} / 상승: {positive_us} / 하락: {negative_us})")
                    st.dataframe(us_long_back, column_config=column_config_us)
                else:
                    st.info("US 장기 데이터 없음")
            
            with back_sub_tabs[1]:  # 단기
                kr_short_back = apply_rename_format_and_unit(df_short_back[df_short_back['market'] == 'KR'], 'KR')
                us_short_back = apply_rename_format_and_unit(df_short_back[df_short_back['market'] == 'US'], 'US')
                if not kr_short_back.empty:
                    kr_short_back = kr_short_back.sort_values(['변동율 (%)', '시가총액 (KRW 억원)'], ascending=[False, False])
                    total_kr = len(kr_short_back)
                    positive_kr = (kr_short_back['변동율 (%)'] > 0).sum()
                    negative_kr = (kr_short_back['변동율 (%)'] < 0).sum()
                    st.markdown(f"### KR (총 종목: {total_kr} / 상승: {positive_kr} / 하락: {negative_kr})")
                    st.dataframe(kr_short_back, column_config=column_config_kr)
                else:
                    st.info("KR 단기 데이터 없음")
                if not us_short_back.empty:
                    us_short_back = us_short_back.sort_values(['변동율 (%)', '시가총액 (USD M)'], ascending=[False, False])
                    total_us = len(us_short_back)
                    positive_us = (us_short_back['변동율 (%)'] > 0).sum()
                    negative_us = (us_short_back['변동율 (%)'] < 0).sum()
                    st.markdown(f"### 해외 (US) (총 종목: {total_us} / 상승: {positive_us} / 하락: {negative_us})")
                    st.dataframe(us_short_back, column_config=column_config_us)
                else:
                    st.info("US 단기 데이터 없음")
            
            with back_sub_tabs[2]:  # 중기
                kr_mid_back = apply_rename_format_and_unit(df_mid_back[df_mid_back['market'] == 'KR'], 'KR')
                us_mid_back = apply_rename_format_and_unit(df_mid_back[df_mid_back['market'] == 'US'], 'US')
                if not kr_mid_back.empty:
                    kr_mid_back = kr_mid_back.sort_values(['변동율 (%)', '시가총액 (KRW 억원)'], ascending=[False, False])
                    total_kr = len(kr_mid_back)
                    positive_kr = (kr_mid_back['변동율 (%)'] > 0).sum()
                    negative_kr = (kr_mid_back['변동율 (%)'] < 0).sum()
                    st.markdown(f"### 국내 (KR) (총 종목: {total_kr} / 상승: {positive_kr} / 하락: {negative_kr})")
                    st.dataframe(kr_mid_back, column_config=column_config_kr)
                else:
                    st.info("KR 중기 데이터 없음")
                if not us_mid_back.empty:
                    us_mid_back = us_mid_back.sort_values(['변동율 (%)', '시가총액 (USD M)'], ascending=[False, False])
                    total_us = len(us_mid_back)
                    positive_us = (us_mid_back['변동율 (%)'] > 0).sum()
                    negative_us = (us_mid_back['변동율 (%)'] < 0).sum()
                    st.markdown(f"### 해외 (US) (총 종목: {total_us} / 상승: {positive_us} / 하락: {negative_us})")
                    st.dataframe(us_mid_back, column_config=column_config_us)
                else:
                    st.info("US 중기 데이터 없음")

with main_tabs[2]:  # KR 탭
    kr_sub_tabs = st.tabs(["장기", "단기", "중기", "Total"])
    
    with kr_sub_tabs[0]:  # 장기
        st.session_state.current_tab = "KR_장기"
        st.header("장기 (OBV 상승크로스 + RSI 하강 지속 (50이하) + EPS & PER)")
        df_long_full = run_screener_query(con, "long_term", use_us=False, use_kr=True, top_n=None, additional_filter="eps_per")
        df_long = df_long_full
        df_long = add_names(df_long)
        df_long = add_foreign_net_buy(df_long)
        df_long = add_close_price(df_long)
        df_long = prepare_tab_df(df_long)
        
        if not df_long_full.empty:
            total_candidates = len(df_long)
            st.metric("후보 수", total_candidates)
            
            df_long['eps_positive'] = df_long['eps'] > 0
            df_long['per_range'] = (df_long['per'] >= 3) & (df_long['per'] <= 30)
            
            df_long['foreign_positive'] = ((df_long['foreign_net_buy_1ago'] > 0) & (df_long['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
            df_long['candle_upper_3'] = (df_long['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
            
            df_long['캔들(5일)'] = df_long['upper_closes'].astype(str) + ' (상단) / ' + df_long['lower_closes'].astype(str) + ' (하단)'  # 추가
            
            df_long = df_long.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                       'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                       'close': '종가',
                       'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                       'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                       'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                       'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}.items() if k in df_long.columns})
            
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
            df_long = df_long[[col for col in cols_kr if col in df_long.columns]]
            df_long = df_long.sort_values('시가총액', ascending=False)
            df_long = format_dataframe(df_long, 'KR')
            st.dataframe(df_long, column_config=column_config_kr)
            
            search_term = st.text_input("종목 검색 (KR 장기)", placeholder="코드/회사명 입력", key="search_kr_long")
            filtered_symbols = get_filtered_symbols(df_long, search_term)
            if filtered_symbols:
                selected_symbol = st.selectbox("종목 선택 (KR 장기)", filtered_symbols, key="select_kr_long")
                if selected_symbol != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_symbol
                if st.session_state.selected_symbol:
                    market = 'KR'
                    show_graphs(st.session_state.selected_symbol, market)
        else:
            st.info("장기 후보 없음")
    
    with kr_sub_tabs[1]:  # 단기
        st.session_state.current_tab = "KR_단기"
        st.header("단기 (OBV 상승크로스 + RSI 상승 지속 (50이하) + 거래대금)")
        df_short_full = run_screener_query(con, "short_term", use_us=False, use_kr=True, top_n=None)
        df_short = df_short_full
        df_short = add_names(df_short)
        df_short = add_foreign_net_buy(df_short)
        df_short = add_close_price(df_short)
        df_short = prepare_tab_df(df_short)
        
        if not df_short_full.empty:
            total_candidates = len(df_short)
            st.metric("후보 수", total_candidates)
            
            df_short['eps_positive'] = df_short['eps'] > 0
            df_short['per_range'] = (df_short['per'] >= 3) & (df_short['per'] <= 30)
            
            df_short['foreign_positive'] = ((df_short['foreign_net_buy_1ago'] > 0) & (df_short['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
            df_short['candle_upper_3'] = (df_short['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
            
            df_short['캔들(5일)'] = df_short['upper_closes'].astype(str) + ' (상단) / ' + df_short['lower_closes'].astype(str) + ' (하단)'  # 추가
            
            df_short = df_short.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                       'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                       'close': '종가',
                       'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                       'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                       'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                       'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}.items() if k in df_short.columns})
            
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
            df_short = df_short[[col for col in cols_kr if col in df_short.columns]]
            df_short = df_short.sort_values('시가총액', ascending=False)
            df_short = format_dataframe(df_short, 'KR')
            st.dataframe(df_short, column_config=column_config_kr)
            
            search_term = st.text_input("종목 검색 (KR 단기)", placeholder="코드/회사명 입력", key="search_kr_short")
            filtered_symbols = get_filtered_symbols(df_short, search_term)
            if filtered_symbols:
                selected_symbol = st.selectbox("종목 선택 (KR 단기)", filtered_symbols, key="select_kr_short")
                if selected_symbol != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_symbol
                if st.session_state.selected_symbol:
                    market = 'KR'
                    show_graphs(st.session_state.selected_symbol, market)
        else:
            st.info("단기 후보 없음")
    
    with kr_sub_tabs[2]:  # 중기
        st.session_state.current_tab = "KR_중기"
        st.header("중기 (OBV 상승크로스 + RSI 상승 지속 (50이하) + EPS & PER)")
        df_mid_full = run_screener_query(con, "mid_term", use_us=False, use_kr=True, top_n=None, additional_filter="eps_per")
        df_mid = df_mid_full
        df_mid = add_names(df_mid)
        df_mid = add_foreign_net_buy(df_mid)
        df_mid = add_close_price(df_mid)
        df_mid = prepare_tab_df(df_mid)
        
        if not df_mid_full.empty:
            total_candidates = len(df_mid)
            st.metric("후보 수", total_candidates)
            
            df_mid['eps_positive'] = df_mid['eps'] > 0
            df_mid['per_range'] = (df_mid['per'] >= 3) & (df_mid['per'] <= 30)
            
            df_mid['foreign_positive'] = ((df_mid['foreign_net_buy_1ago'] > 0) & (df_mid['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
            df_mid['candle_upper_3'] = (df_mid['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
            
            df_mid['캔들(5일)'] = df_mid['upper_closes'].astype(str) + ' (상단) / ' + df_mid['lower_closes'].astype(str) + ' (하단)'  # 추가
            
            df_mid = df_mid.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                       'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                       'close': '종가',
                       'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                       'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                       'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                       'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}.items() if k in df_mid.columns})
            
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
            df_mid = df_mid[[col for col in cols_kr if col in df_mid.columns]]
            df_mid = df_mid.sort_values('시가총액', ascending=False)
            df_mid = format_dataframe(df_mid, 'KR')
            st.dataframe(df_mid, column_config=column_config_kr)
            
            search_term = st.text_input("종목 검색 (KR 중기)", placeholder="코드/회사명 입력", key="search_kr_mid")
            filtered_symbols = get_filtered_symbols(df_mid, search_term)
            if filtered_symbols:
                selected_symbol = st.selectbox("종목 선택 (KR 중기)", filtered_symbols, key="select_kr_mid")
                if selected_symbol != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_symbol
                if st.session_state.selected_symbol:
                    market = 'KR'
                    show_graphs(st.session_state.selected_symbol, market)
        else:
            st.info("중기 후보 없음")
    
    with kr_sub_tabs[3]:  # Total
        st.session_state.current_tab = "KR_Total"
        st.header("Total (전체 종목 목록)")
        if not df_ind.empty:
            df_kr_ind = df_ind[df_ind['market'] == 'KR']
            df_kr_ind = add_names(df_kr_ind)
            df_kr_ind = add_foreign_net_buy(df_kr_ind)
            df_kr_ind = add_close_price(df_kr_ind)
            # JSON 파싱 추가 (에러 해결)
            df_kr_ind['rsi_d_2ago'] = df_kr_ind['rsi_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
            df_kr_ind['rsi_d_1ago'] = df_kr_ind['rsi_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
            df_kr_ind['rsi_d_latest'] = df_kr_ind['rsi_d'].apply(lambda x: json.loads(x)[2] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
            df_kr_ind['obv_1ago'] = df_kr_ind['obv_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 1 else np.nan)
            df_kr_ind['obv_latest'] = df_kr_ind['obv_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 0 else np.nan)
            df_kr_ind['signal_obv_1ago'] = df_kr_ind['signal_obv_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 1 else np.nan)
            df_kr_ind['signal_obv_latest'] = df_kr_ind['signal_obv_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 0 else np.nan)
            df_kr_ind = prepare_tab_df(df_kr_ind, is_total=True)
            
            df_kr_ind['eps_positive'] = df_kr_ind['eps'] > 0
            df_kr_ind['per_range'] = (df_kr_ind['per'] >= 3) & (df_kr_ind['per'] <= 30)
            df_kr_ind['obv_bullish_cross'] = (df_kr_ind['obv_latest'] > df_kr_ind['signal_obv_latest']) & (df_kr_ind['obv_1ago'] <= df_kr_ind['signal_obv_1ago'])
            df_kr_ind['rsi_3up'] = (df_kr_ind['rsi_d_2ago'] < df_kr_ind['rsi_d_1ago']) & (df_kr_ind['rsi_d_1ago'] < df_kr_ind['rsi_d_latest']) & (df_kr_ind['rsi_d_latest'] <= 50)
            df_kr_ind['rsi_3down'] = (df_kr_ind['rsi_d_2ago'] > df_kr_ind['rsi_d_1ago']) & (df_kr_ind['rsi_d_1ago'] > df_kr_ind['rsi_d_latest']) & (df_kr_ind['rsi_d_latest'] <= 50)
            df_kr_ind['trading_high'] = df_kr_ind['today_trading_value'] > 1.5 * df_kr_ind['avg_trading_value_20d']
            
            df_kr_ind['foreign_positive'] = ((df_kr_ind['foreign_net_buy_1ago'] > 0) & (df_kr_ind['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
            df_kr_ind['candle_upper_3'] = (df_kr_ind['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
            
            df_kr_ind['캔들(5일)'] = df_kr_ind['upper_closes'].astype(str) + ' (상단) / ' + df_kr_ind['lower_closes'].astype(str) + ' (하단)'  # 추가
            
            col_map_total = {'symbol': '종목코드', 'market': '시장', 'name': '회사명',
                             'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                             'close': '종가',
                             'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 
                             'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                             'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                             'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                             'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}
            df_kr_ind = df_kr_ind.rename(columns={k: v for k, v in col_map_total.items() if k in df_kr_ind.columns})
            df_kr_ind = df_kr_ind.sort_values('시가총액', ascending=False).reset_index(drop=True)
            
            cols_kr_total = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
            df_kr_ind = df_kr_ind[[col for col in cols_kr_total if col in df_kr_ind.columns]]
            df_kr_ind = format_dataframe(df_kr_ind, 'KR')
            st.dataframe(df_kr_ind, column_config=column_config_kr)
            
            search_term = st.text_input("종목 검색 (KR Total)", placeholder="코드 입력", key="search_kr_total")
            filtered_symbols = get_filtered_symbols(df_kr_ind, search_term)
            if filtered_symbols:
                selected_symbol = st.selectbox("종목 선택 (KR Total)", filtered_symbols, key="select_kr_total")
                if selected_symbol != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_symbol
                if st.session_state.selected_symbol:
                    market = 'KR'
                    show_graphs(st.session_state.selected_symbol, market)
            else:
                st.info("검색 결과 없음")
        else:
            st.info("데이터 없음 – 배치 실행하세요.")

with main_tabs[3]:  # US 탭
    us_sub_tabs = st.tabs(["장기", "단기", "중기", "Total"])
    
    with us_sub_tabs[0]:  # 장기
        st.session_state.current_tab = "US_장기"
        st.header("장기 (OBV 상승크로스 + RSI 하강 지속 (50이하) + EPS & PER)")
        df_long_full = run_screener_query(con, "long_term", use_us=True, use_kr=False, top_n=None, additional_filter="eps_per")
        df_long = df_long_full
        df_long = add_names(df_long)
        df_long = add_foreign_net_buy(df_long)
        df_long = add_close_price(df_long)
        df_long = prepare_tab_df(df_long)
        
        if not df_long_full.empty:
            total_candidates = len(df_long)
            st.metric("후보 수", total_candidates)
            
            df_long['eps_positive'] = df_long['eps'] > 0
            df_long['per_range'] = (df_long['per'] >= 3) & (df_long['per'] <= 30)
            
            df_long['foreign_positive'] = ((df_long['foreign_net_buy_1ago'] > 0) & (df_long['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
            df_long['candle_upper_3'] = (df_long['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
            
            df_long['캔들(5일)'] = df_long['upper_closes'].astype(str) + ' (상단) / ' + df_long['lower_closes'].astype(str) + ' (하단)'  # 추가
            
            df_long = df_long.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                       'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                       'close': '종가',
                       'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                       'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                       'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                       'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}.items() if k in df_long.columns})
            
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
            df_long = df_long[[col for col in cols_us if col in df_long.columns]]
            df_long = df_long.sort_values('시가총액', ascending=False)
            df_long = format_dataframe(df_long, 'US')
            st.dataframe(df_long, column_config=column_config_us)
            
            search_term = st.text_input("종목 검색 (US 장기)", placeholder="코드/회사명 입력", key="search_us_long")
            filtered_symbols = get_filtered_symbols(df_long, search_term)
            if filtered_symbols:
                selected_symbol = st.selectbox("종목 선택 (US 장기)", filtered_symbols, key="select_us_long")
                if selected_symbol != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_symbol
                if st.session_state.selected_symbol:
                    market = 'US'
                    show_graphs(st.session_state.selected_symbol, market)
        else:
            st.info("장기 후보 없음")
    
    with us_sub_tabs[1]:  # 단기
        st.session_state.current_tab = "US_단기"
        st.header("단기 (OBV 상승크로스 + RSI 상승 지속 (50이하) + 거래대금)")
        df_short_full = run_screener_query(con, "short_term", use_us=True, use_kr=False, top_n=None)
        df_short = df_short_full
        df_short = add_names(df_short)
        df_short = add_foreign_net_buy(df_short)
        df_short = add_close_price(df_short)
        df_short = prepare_tab_df(df_short)
        
        if not df_short_full.empty:
            total_candidates = len(df_short)
            st.metric("후보 수", total_candidates)
            
            df_short['eps_positive'] = df_short['eps'] > 0
            df_short['per_range'] = (df_short['per'] >= 3) & (df_short['per'] <= 30)
            
            df_short['foreign_positive'] = ((df_short['foreign_net_buy_1ago'] > 0) & (df_short['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
            df_short['candle_upper_3'] = (df_short['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
            
            df_short['캔들(5일)'] = df_short['upper_closes'].astype(str) + ' (상단) / ' + df_short['lower_closes'].astype(str) + ' (하단)'  # 추가
            
            df_short = df_short.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                       'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                       'close': '종가',
                       'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                       'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                       'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                       'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}.items() if k in df_short.columns})
            
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
            df_short = df_short[[col for col in cols_us if col in df_short.columns]]
            df_short = df_short.sort_values('시가총액', ascending=False)
            df_short = format_dataframe(df_short, 'US')
            st.dataframe(df_short, column_config=column_config_us)
            
            search_term = st.text_input("종목 검색 (US 단기)", placeholder="코드/회사명 입력", key="search_us_short")
            filtered_symbols = get_filtered_symbols(df_short, search_term)
            if filtered_symbols:
                selected_symbol = st.selectbox("종목 선택 (US 단기)", filtered_symbols, key="select_us_short")
                if selected_symbol != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_symbol
                if st.session_state.selected_symbol:
                    market = 'US'
                    show_graphs(st.session_state.selected_symbol, market)
        else:
            st.info("단기 후보 없음")
    
    with us_sub_tabs[2]:  # 중기
        st.session_state.current_tab = "US_중기"
        st.header("중기 (OBV 상승크로스 + RSI 상승 지속 (50이하) + EPS & PER)")
        df_mid_full = run_screener_query(con, "mid_term", use_us=True, use_kr=False, top_n=None, additional_filter="eps_per")
        df_mid = df_mid_full
        df_mid = add_names(df_mid)
        df_mid = add_foreign_net_buy(df_mid)
        df_mid = add_close_price(df_mid)
        df_mid = prepare_tab_df(df_mid)
        
        if not df_mid_full.empty:
            total_candidates = len(df_mid)
            st.metric("후보 수", total_candidates)
            
            df_mid['eps_positive'] = df_mid['eps'] > 0
            df_mid['per_range'] = (df_mid['per'] >= 3) & (df_mid['per'] <= 30)
            
            df_mid['foreign_positive'] = ((df_mid['foreign_net_buy_1ago'] > 0) & (df_mid['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
            df_mid['candle_upper_3'] = (df_mid['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
            
            df_mid['캔들(5일)'] = df_mid['upper_closes'].astype(str) + ' (상단) / ' + df_mid['lower_closes'].astype(str) + ' (하단)'  # 추가
            
            df_mid = df_mid.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                       'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                       'close': '종가',
                       'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                       'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                       'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                       'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}.items() if k in df_mid.columns})
            
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
            df_mid = df_mid[[col for col in cols_us if col in df_mid.columns]]
            df_mid = df_mid.sort_values('시가총액', ascending=False)
            df_mid = format_dataframe(df_mid, 'US')
            st.dataframe(df_mid, column_config=column_config_us)
            
            search_term = st.text_input("종목 검색 (US 중기)", placeholder="코드/회사명 입력", key="search_us_mid")
            filtered_symbols = get_filtered_symbols(df_mid, search_term)
            if filtered_symbols:
                selected_symbol = st.selectbox("종목 선택 (US 중기)", filtered_symbols, key="select_us_mid")
                if selected_symbol != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_symbol
                if st.session_state.selected_symbol:
                    market = 'US'
                    show_graphs(st.session_state.selected_symbol, market)
        else:
            st.info("중기 후보 없음")
    
    with us_sub_tabs[3]:  # Total
        st.session_state.current_tab = "US_Total"
        st.header("Total (전체 종목 목록)")
        if not df_ind.empty:
            df_us_ind = df_ind[df_ind['market'] == 'US']
            df_us_ind = add_names(df_us_ind)
            df_us_ind = add_foreign_net_buy(df_us_ind)
            df_us_ind = add_close_price(df_us_ind)
            # JSON 파싱 추가 (에러 해결)
            df_us_ind['rsi_d_2ago'] = df_us_ind['rsi_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
            df_us_ind['rsi_d_1ago'] = df_us_ind['rsi_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
            df_us_ind['rsi_d_latest'] = df_us_ind['rsi_d'].apply(lambda x: json.loads(x)[2] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
            df_us_ind['obv_1ago'] = df_us_ind['obv_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 1 else np.nan)
            df_us_ind['obv_latest'] = df_us_ind['obv_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 0 else np.nan)
            df_us_ind['signal_obv_1ago'] = df_us_ind['signal_obv_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 1 else np.nan)
            df_us_ind['signal_obv_latest'] = df_us_ind['signal_obv_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 0 else np.nan)
            df_us_ind = prepare_tab_df(df_us_ind, is_total=True)
            
            df_us_ind['eps_positive'] = df_us_ind['eps'] > 0
            df_us_ind['per_range'] = (df_us_ind['per'] >= 3) & (df_us_ind['per'] <= 30)
            df_us_ind['obv_bullish_cross'] = (df_us_ind['obv_latest'] > df_us_ind['signal_obv_latest']) & (df_us_ind['obv_1ago'] <= df_us_ind['signal_obv_1ago'])
            df_us_ind['rsi_3up'] = (df_us_ind['rsi_d_2ago'] < df_us_ind['rsi_d_1ago']) & (df_us_ind['rsi_d_1ago'] < df_us_ind['rsi_d_latest']) & (df_us_ind['rsi_d_latest'] <= 50)
            df_us_ind['rsi_3down'] = (df_us_ind['rsi_d_2ago'] > df_us_ind['rsi_d_1ago']) & (df_us_ind['rsi_d_1ago'] > df_us_ind['rsi_d_latest']) & (df_us_ind['rsi_d_latest'] <= 50)
            df_us_ind['trading_high'] = df_us_ind['today_trading_value'] > 1.5 * df_us_ind['avg_trading_value_20d']
            
            df_us_ind['foreign_positive'] = ((df_us_ind['foreign_net_buy_1ago'] > 0) & (df_us_ind['foreign_net_buy_2ago'] > 0)).apply(lambda x: '✅' if x else '❌')
            df_us_ind['candle_upper_3'] = (df_us_ind['upper_closes'] >= 3).apply(lambda x: '✅' if x else '❌')
            
            df_us_ind['캔들(5일)'] = df_us_ind['upper_closes'].astype(str) + ' (상단) / ' + df_us_ind['lower_closes'].astype(str) + ' (하단)'  # 추가
            
            col_map_total = {'symbol': '종목코드', 'market': '시장', 'name': '회사명',
                             'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                             'close': '종가',
                             'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 
                             'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                             'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                             'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                             'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '업데이트', 'foreign_positive': '외국인 순매수', 'candle_upper_3': '캔들'}
            df_us_ind = df_us_ind.rename(columns={k: v for k, v in col_map_total.items() if k in df_us_ind.columns})
            df_us_ind = df_us_ind.sort_values('시가총액', ascending=False).reset_index(drop=True)
            
            cols_us_total = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '업데이트', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30', '외국인 순매수', '캔들']
            df_us_ind = df_us_ind[[col for col in cols_us_total if col in df_us_ind.columns]]
            df_us_ind = format_dataframe(df_us_ind, 'US')
            st.dataframe(df_us_ind, column_config=column_config_us)
            
            search_term = st.text_input("종목 검색 (US Total)", placeholder="코드 입력", key="search_us_total")
            filtered_symbols = get_filtered_symbols(df_us_ind, search_term)
            if filtered_symbols:
                selected_symbol = st.selectbox("종목 선택 (US Total)", filtered_symbols, key="select_us_total")
                if selected_symbol != st.session_state.selected_symbol:
                    st.session_state.selected_symbol = selected_symbol
                if st.session_state.selected_symbol:
                    market = 'US'
                    show_graphs(st.session_state.selected_symbol, market)
            else:
                st.info("검색 결과 없음")
        else:
            st.info("데이터 없음 – 배치 실행하세요.")

with main_tabs[4]:  # 로그 탭
    st.header("로그")
    log_time_file = "logs/batch_time.txt"
    if os.path.exists(log_time_file):
        with open(log_time_file, "r") as f:
            last_time = f.read().strip()
        st.info(f"마지막 갱신: {last_time}")
    else:
        st.info("로그 없음 – 로그 실행하세요.")

if hasattr(st.session_state, 'con') and st.session_state.con:
    try:
        st.session_state.con.close()
    except:
        pass