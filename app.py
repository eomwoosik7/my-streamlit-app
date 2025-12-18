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

st.set_page_config(page_title="Trading Copilot", layout="wide")

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

def add_foreign_net_buy(df):
    if df.empty or 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    meta = load_meta()
    df = df.copy()
    df['foreign_net_buy_3ago'] = np.nan
    df['foreign_net_buy_2ago'] = np.nan
    df['foreign_net_buy_1ago'] = np.nan
    for idx, row in df.iterrows():
        symbol = row['symbol']
        market = row['market']
        meta_dict = meta.get(market, {}).get(symbol, {})
        fnb = meta_dict.get('foreign_net_buy', [0, 0, 0])
        # fnb[0] = recent (1ago), fnb[1] = 2ago, fnb[2] = 3ago
        df.at[idx, 'foreign_net_buy_3ago'] = fnb[2] if len(fnb) > 2 else 0
        df.at[idx, 'foreign_net_buy_2ago'] = fnb[1] if len(fnb) > 1 else 0
        df.at[idx, 'foreign_net_buy_1ago'] = fnb[0] if len(fnb) > 0 else 0
    return df

@st.cache_data(ttl=3600)
def add_close_price(df):
    if df.empty or 'symbol' not in df.columns or 'market' not in df.columns:
        return df
    df = df.copy()
    df['close'] = np.nan
    base_dir = "data"  # 기존 DATA_DIR와 맞춤
    for idx, row in df.iterrows():
        symbol = row['symbol']
        market = row['market']
        if market == 'US':
            daily_path = os.path.join(base_dir, 'us_daily', f"{symbol}.parquet")
            close_col = 'Close'
        elif market == 'KR':
            daily_path = os.path.join(base_dir, 'kr_daily', f"{symbol}.parquet")
            close_col = 'Close'  # KR 컬럼명 맞춤
        else:
            continue
        
        if os.path.exists(daily_path):
            try:
                df_daily = pd.read_parquet(daily_path)
                if not df_daily.empty and close_col in df_daily.columns:
                    df.at[idx, 'close'] = df_daily[close_col].iloc[-1]  # 마지막 행 종가
            except Exception as e:
                pass  # 에러 스킵 (로그 추가 가능)
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
            '시가총액': '시가총액 (USD B)',
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

    if '시가총액 (KRW 억원)' in df.columns or '시가총액 (USD B)' in df.columns:
        col_name = df.columns[df.columns.str.startswith('시가총액 (')][0]
        df[col_name] = df[col_name].apply(safe_float)
        if market_type == 'KR':
            df[col_name] = df[col_name] / 1e8  # 억원 단위 (숫자 유지)
        else:
            df[col_name] = df[col_name] / 1e9  # Billion USD (숫자 유지)

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

    styled_df = df.style.set_properties(**{'text-align': 'center'})

    return styled_df

def show_graphs(symbol, market):
    base_dir = "data"
    daily_path = os.path.join(base_dir, ('us_daily' if market == 'US' else 'kr_daily'), f"{symbol}.parquet")
    if os.path.exists(daily_path):
        df_chart = pd.read_parquet(daily_path)
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

if 'selected_symbol' not in st.session_state:
    st.session_state.selected_symbol = None
if 'con' not in st.session_state:
    st.session_state.con = None
if 'current_tab' not in st.session_state:
    st.session_state.current_tab = "장기"

st.sidebar.title("설정")
use_us = st.sidebar.checkbox("US 시장", value=True)
use_kr = st.sidebar.checkbox("KR 시장", value=True)
top_n = st.sidebar.number_input("Top N", min_value=10, max_value=100, value=20)

if st.sidebar.button("배치 실행"):
    subprocess.run(["python", "batch.py", str(use_us), str(use_kr), str(top_n)])

df_ind = load_data()
con = get_db_connection()

tab1, tab8, tab9, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["장기", "단기", "중기", "OBV 상승 크로스", "RSI 상승 지속 (50 이하)", "RSI 하강 지속 (50 이하)", "EPS & PER", "거래대금", "Total"])

column_config_kr = {
    "종목코드": st.column_config.TextColumn(width="small"),
    "회사명": st.column_config.TextColumn(width="small"),
    "시장": st.column_config.TextColumn(width="small"),
    "RSI_3일_2ago": st.column_config.NumberColumn(width=80, format="%.2f"),
    "RSI_3일_1ago": st.column_config.NumberColumn(width=80, format="%.2f"),
    "RSI_3일_latest": st.column_config.NumberColumn(width=80, format="%.2f"),
    "종가 (KRW)": st.column_config.NumberColumn(width=80, format="%.0f"),
    "시가총액 (KRW 억원)": st.column_config.NumberColumn(width=80, format="%.0f"),
    "시가총액_상태": st.column_config.TextColumn(width="small"),
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
}

column_config_us = {
    "종목코드": st.column_config.TextColumn(width="small"),
    "회사명": st.column_config.TextColumn(width="small"),
    "시장": st.column_config.TextColumn(width="small"),
    "RSI_3일_2ago": st.column_config.NumberColumn(width=80, format="%.2f"),
    "RSI_3일_1ago": st.column_config.NumberColumn(width=80, format="%.2f"),
    "RSI_3일_latest": st.column_config.NumberColumn(width=80, format="%.2f"),
    "종가 (USD)": st.column_config.NumberColumn(width=80, format="%.2f"),
    "시가총액 (USD B)": st.column_config.NumberColumn(width=80, format="%.1f"),
    "시가총액_상태": st.column_config.TextColumn(width="small"),
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
}

with tab1:
    st.session_state.current_tab = "장기"
    st.header("장기 (OBV 상승크로스 + RSI 하강 지속 (50이하) + EPS & PER)")
    df_long_full = run_screener_query(con, "long_term", use_us, use_kr, top_n=None, additional_filter="eps_per")
    df_long = df_long_full
    df_long = add_names(df_long)
    df_long = add_foreign_net_buy(df_long)
    df_long = add_close_price(df_long)
    df_long = prepare_tab_df(df_long)
    
    if not df_long_full.empty:
        df_kr_temp = df_long_full[df_long_full['market'] == 'KR']
        df_us_temp = df_long_full[df_long_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_long['eps_positive'] = df_long['eps'] > 0
        df_long['per_range'] = (df_long['per'] >= 3) & (df_long['per'] <= 30)
        
        df_long['캔들(5일)'] = df_long['upper_closes'].astype(str) + ' (상단) / ' + df_long['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        df_long = df_long.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                   'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                   'close': '종가',
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}.items() if k in df_long.columns})
        
        df_kr_results = df_long[df_long['시장'] == 'KR'] if '시장' in df_long.columns else pd.DataFrame()
        df_us_results = df_long[df_long['시장'] == 'US'] if '시장' in df_long.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_results, column_config=column_config_kr)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_results, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (장기)", placeholder="코드/회사명 입력", key="search_long")
        filtered_symbols = get_filtered_symbols(df_long, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (장기)", filtered_symbols, key="select_long")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_long[df_long['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_long.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("장기 후보 없음")

with tab8:
    st.session_state.current_tab = "단기"
    st.header("단기 (OBV 상승크로스 + RSI 상승 지속 (50이하) + 거래대금)")
    df_short_full = run_screener_query(con, "short_term", use_us, use_kr, top_n=None)
    df_short = df_short_full
    df_short = add_names(df_short)
    df_short = add_foreign_net_buy(df_short)
    df_short = add_close_price(df_short)
    df_short = prepare_tab_df(df_short)
    
    if not df_short_full.empty:
        df_kr_temp = df_short_full[df_short_full['market'] == 'KR']
        df_us_temp = df_short_full[df_short_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_short['eps_positive'] = df_short['eps'] > 0
        df_short['per_range'] = (df_short['per'] >= 3) & (df_short['per'] <= 30)
        
        df_short['캔들(5일)'] = df_short['upper_closes'].astype(str) + ' (상단) / ' + df_short['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        df_short = df_short.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                   'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                   'close': '종가',
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}.items() if k in df_short.columns})
        
        df_kr_results = df_short[df_short['시장'] == 'KR'] if '시장' in df_short.columns else pd.DataFrame()
        df_us_results = df_short[df_short['시장'] == 'US'] if '시장' in df_short.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_results, column_config=column_config_kr)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_results, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (단기)", placeholder="코드/회사명 입력", key="search_short")
        filtered_symbols = get_filtered_symbols(df_short, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (단기)", filtered_symbols, key="select_short")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_short[df_short['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_short.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("단기 후보 없음")

with tab9:
    st.session_state.current_tab = "중기"
    st.header("중기 (OBV 상승크로스 + RSI 상승 지속 (50이하) + EPS & PER)")
    df_mid_full = run_screener_query(con, "mid_term", use_us, use_kr, top_n=None, additional_filter="eps_per")
    df_mid = df_mid_full
    df_mid = add_names(df_mid)
    df_mid = add_foreign_net_buy(df_mid)
    df_mid = add_close_price(df_mid)
    df_mid = prepare_tab_df(df_mid)
    
    if not df_mid_full.empty:
        df_kr_temp = df_mid_full[df_mid_full['market'] == 'KR']
        df_us_temp = df_mid_full[df_mid_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_mid['eps_positive'] = df_mid['eps'] > 0
        df_mid['per_range'] = (df_mid['per'] >= 3) & (df_mid['per'] <= 30)
        
        df_mid['캔들(5일)'] = df_mid['upper_closes'].astype(str) + ' (상단) / ' + df_mid['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        df_mid = df_mid.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                   'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                   'close': '종가',
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}.items() if k in df_mid.columns})
        
        df_kr_results = df_mid[df_mid['시장'] == 'KR'] if '시장' in df_mid.columns else pd.DataFrame()
        df_us_results = df_mid[df_mid['시장'] == 'US'] if '시장' in df_mid.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_results, column_config=column_config_kr)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_results, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (중기)", placeholder="코드/회사명 입력", key="search_mid")
        filtered_symbols = get_filtered_symbols(df_mid, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (중기)", filtered_symbols, key="select_mid")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_mid[df_mid['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_mid.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("중기 후보 없음")

with tab2:
    st.session_state.current_tab = "OBV 상승 크로스"
    st.header("OBV 상승 크로스 (조건 1 + 유동성)")
    df_obv_full = run_screener_query(con, "obv", use_us, use_kr, top_n=None)
    df_obv = df_obv_full
    df_obv = add_names(df_obv)
    df_obv = add_foreign_net_buy(df_obv)
    df_obv = add_close_price(df_obv)
    df_obv = prepare_tab_df(df_obv)
    
    if not df_obv_full.empty:
        df_kr_temp = df_obv_full[df_obv_full['market'] == 'KR']
        df_us_temp = df_obv_full[df_obv_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_obv['eps_positive'] = df_obv['eps'] > 0
        df_obv['per_range'] = (df_obv['per'] >= 3) & (df_obv['per'] <= 30)
        
        df_obv['캔들(5일)'] = df_obv['upper_closes'].astype(str) + ' (상단) / ' + df_obv['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        df_obv = df_obv.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                   'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                   'close': '종가',
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}.items() if k in df_obv.columns})
        
        df_kr_results = df_obv[df_obv['시장'] == 'KR'] if '시장' in df_obv.columns else pd.DataFrame()
        df_us_results = df_obv[df_obv['시장'] == 'US'] if '시장' in df_obv.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_results, column_config=column_config_kr)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_results, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (OBV)", placeholder="코드/회사명 입력", key="search_obv")
        filtered_symbols = get_filtered_symbols(df_obv, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (OBV)", filtered_symbols, key="select_obv")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_obv[df_obv['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_obv.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("OBV 후보 없음")

with tab3:
    st.session_state.current_tab = "RSI 상승 지속"
    st.header("RSI 상승 지속 (50 이하, 조건 2 + 유동성)")
    df_rsi_up_full = run_screener_query(con, "rsi_up", use_us, use_kr, top_n=None)
    df_rsi_up = df_rsi_up_full
    df_rsi_up = add_names(df_rsi_up)
    df_rsi_up = add_foreign_net_buy(df_rsi_up)
    df_rsi_up = add_close_price(df_rsi_up)
    df_rsi_up = prepare_tab_df(df_rsi_up)
    
    if not df_rsi_up_full.empty:
        df_kr_temp = df_rsi_up_full[df_rsi_up_full['market'] == 'KR']
        df_us_temp = df_rsi_up_full[df_rsi_up_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_rsi_up['eps_positive'] = df_rsi_up['eps'] > 0
        df_rsi_up['per_range'] = (df_rsi_up['per'] >= 3) & (df_rsi_up['per'] <= 30)
        
        df_rsi_up['캔들(5일)'] = df_rsi_up['upper_closes'].astype(str) + ' (상단) / ' + df_rsi_up['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        df_rsi_up = df_rsi_up.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                   'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                   'close': '종가',
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}.items() if k in df_rsi_up.columns})
        
        df_kr_results = df_rsi_up[df_rsi_up['시장'] == 'KR'] if '시장' in df_rsi_up.columns else pd.DataFrame()
        df_us_results = df_rsi_up[df_rsi_up['시장'] == 'US'] if '시장' in df_rsi_up.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_results, column_config=column_config_kr)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_results, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (RSI 상승)", placeholder="코드/회사명 입력", key="search_rsi_up")
        filtered_symbols = get_filtered_symbols(df_rsi_up, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (RSI 상승)", filtered_symbols, key="select_rsi_up")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_rsi_up[df_rsi_up['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_rsi_up.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("RSI 상승 후보 없음")

with tab4:
    st.session_state.current_tab = "RSI 하강 지속"
    st.header("RSI 하강 지속 (50 이하, 조건 + 유동성)")
    df_rsi_down_full = run_screener_query(con, "rsi_down", use_us, use_kr, top_n=None)
    df_rsi_down = df_rsi_down_full
    df_rsi_down = add_names(df_rsi_down)
    df_rsi_down = add_foreign_net_buy(df_rsi_down)
    df_rsi_down = add_close_price(df_rsi_down)
    df_rsi_down = prepare_tab_df(df_rsi_down)
    
    if not df_rsi_down_full.empty:
        df_kr_temp = df_rsi_down_full[df_rsi_down_full['market'] == 'KR']
        df_us_temp = df_rsi_down_full[df_rsi_down_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_rsi_down['eps_positive'] = df_rsi_down['eps'] > 0
        df_rsi_down['per_range'] = (df_rsi_down['per'] >= 3) & (df_rsi_down['per'] <= 30)
        
        df_rsi_down['캔들(5일)'] = df_rsi_down['upper_closes'].astype(str) + ' (상단) / ' + df_rsi_down['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        df_rsi_down = df_rsi_down.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                   'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                   'close': '종가',
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}.items() if k in df_rsi_down.columns})
        
        df_kr_results = df_rsi_down[df_rsi_down['시장'] == 'KR'] if '시장' in df_rsi_down.columns else pd.DataFrame()
        df_us_results = df_rsi_down[df_rsi_down['시장'] == 'US'] if '시장' in df_rsi_down.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_results, column_config=column_config_kr)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_results, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (RSI 하강)", placeholder="코드/회사명 입력", key="search_rsi_down")
        filtered_symbols = get_filtered_symbols(df_rsi_down, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (RSI 하강)", filtered_symbols, key="select_rsi_down")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_rsi_down[df_rsi_down['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_rsi_down.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("RSI 하강 후보 없음")

with tab5:
    st.session_state.current_tab = "EPS & PER"
    st.header("EPS & PER(EPS>0, 3<=PER<=30 조건3 + 유동성)")
    df_rsi_eps_per_full = run_screener_query(con, "eps_per_only", use_us, use_kr, top_n=None, additional_filter="eps_per")
    df_rsi_eps_per = df_rsi_eps_per_full
    df_rsi_eps_per = add_names(df_rsi_eps_per)
    df_rsi_eps_per = add_foreign_net_buy(df_rsi_eps_per)
    df_rsi_eps_per = add_close_price(df_rsi_eps_per)
    df_rsi_eps_per = prepare_tab_df(df_rsi_eps_per)
    
    if not df_rsi_eps_per_full.empty:
        df_kr_temp = df_rsi_eps_per_full[df_rsi_eps_per_full['market'] == 'KR']
        df_us_temp = df_rsi_eps_per_full[df_rsi_eps_per_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_rsi_eps_per['eps_positive'] = df_rsi_eps_per['eps'] > 0
        df_rsi_eps_per['per_range'] = (df_rsi_eps_per['per'] >= 3) & (df_rsi_eps_per['per'] <= 30)
        
        df_rsi_eps_per['캔들(5일)'] = df_rsi_eps_per['upper_closes'].astype(str) + ' (상단) / ' + df_rsi_eps_per['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        df_rsi_eps_per = df_rsi_eps_per.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                   'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                   'close': '종가',
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}.items() if k in df_rsi_eps_per.columns})
        
        df_kr_results = df_rsi_eps_per[df_rsi_eps_per['시장'] == 'KR'] if '시장' in df_rsi_eps_per.columns else pd.DataFrame()
        df_us_results = df_rsi_eps_per[df_rsi_eps_per['시장'] == 'US'] if '시장' in df_rsi_eps_per.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_results, column_config=column_config_kr)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_results, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (EPS & PER)", placeholder="코드/회사명 입력", key="search_eps_per")
        filtered_symbols = get_filtered_symbols(df_rsi_eps_per, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (EPS & PER)", filtered_symbols, key="select_eps_per")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_rsi_eps_per[df_rsi_eps_per['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_rsi_eps_per.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("EPS & PER 후보 없음")

with tab6:
    st.session_state.current_tab = "거래대금"
    st.header("거래대금 (오늘 거래대금 > 1.5 * 20일 평균 + 유동성)")
    df_trading_full = run_screener_query(con, "trading_volume", use_us, use_kr, top_n=None)
    df_trading = df_trading_full
    df_trading = add_names(df_trading)
    df_trading = add_foreign_net_buy(df_trading)
    df_trading = add_close_price(df_trading)
    df_trading = prepare_tab_df(df_trading)
    
    if not df_trading_full.empty:
        df_kr_temp = df_trading_full[df_trading_full['market'] == 'KR']
        df_us_temp = df_trading_full[df_trading_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_trading['eps_positive'] = df_trading['eps'] > 0
        df_trading['per_range'] = (df_trading['per'] >= 3) & (df_trading['per'] <= 30)
        
        df_trading['캔들(5일)'] = df_trading['upper_closes'].astype(str) + ' (상단) / ' + df_trading['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        df_trading = df_trading.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 
                   'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                   'close': '종가',
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}.items() if k in df_trading.columns})
        
        df_kr_results = df_trading[df_trading['시장'] == 'KR'] if '시장' in df_trading.columns else pd.DataFrame()
        df_us_results = df_trading[df_trading['시장'] == 'US'] if '시장' in df_trading.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_results, column_config=column_config_kr)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_results, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (거래대금)", placeholder="코드/회사명 입력", key="search_trading")
        filtered_symbols = get_filtered_symbols(df_trading, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (거래대금)", filtered_symbols, key="select_trading")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_trading[df_trading['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_trading.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("거래대금 후보 없음")

with tab7:
    st.session_state.current_tab = "Total"
    st.header("Total (전체 종목 목록)")
    if not df_ind.empty:
        df_ind = add_names(df_ind)
        df_ind = add_foreign_net_buy(df_ind)
        df_ind = add_close_price(df_ind)
        # JSON 파싱 추가 (에러 해결)
        df_ind['rsi_d_2ago'] = df_ind['rsi_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
        df_ind['rsi_d_1ago'] = df_ind['rsi_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
        df_ind['rsi_d_latest'] = df_ind['rsi_d'].apply(lambda x: json.loads(x)[2] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 2 else np.nan)
        df_ind['obv_1ago'] = df_ind['obv_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 1 else np.nan)
        df_ind['obv_latest'] = df_ind['obv_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 0 else np.nan)
        df_ind['signal_obv_1ago'] = df_ind['signal_obv_d'].apply(lambda x: json.loads(x)[1] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 1 else np.nan)
        df_ind['signal_obv_latest'] = df_ind['signal_obv_d'].apply(lambda x: json.loads(x)[0] if x and isinstance(json.loads(x), list) and len(json.loads(x)) > 0 else np.nan)
        df_ind = prepare_tab_df(df_ind, is_total=True)
        
        df_ind['eps_positive'] = df_ind['eps'] > 0
        df_ind['per_range'] = (df_ind['per'] >= 3) & (df_ind['per'] <= 30)
        df_ind['obv_bullish_cross'] = (df_ind['obv_latest'] > df_ind['signal_obv_latest']) & (df_ind['obv_1ago'] <= df_ind['signal_obv_1ago'])
        df_ind['rsi_3up'] = (df_ind['rsi_d_2ago'] < df_ind['rsi_d_1ago']) & (df_ind['rsi_d_1ago'] < df_ind['rsi_d_latest']) & (df_ind['rsi_d_latest'] <= 50)
        df_ind['rsi_3down'] = (df_ind['rsi_d_2ago'] > df_ind['rsi_d_1ago']) & (df_ind['rsi_d_1ago'] > df_ind['rsi_d_latest']) & (df_ind['rsi_d_latest'] <= 50)
        df_ind['trading_high'] = df_ind['today_trading_value'] > 1.5 * df_ind['avg_trading_value_20d']
        
        df_ind['캔들(5일)'] = df_ind['upper_closes'].astype(str) + ' (상단) / ' + df_ind['lower_closes'].astype(str) + ' (하단)'  # 추가
        
        col_map_total = {'symbol': '종목코드', 'market': '시장',
                         'rsi_d_2ago': 'RSI_3일_2ago', 'rsi_d_1ago': 'RSI_3일_1ago', 'rsi_d_latest': 'RSI_3일_latest', 
                         'close': '종가',
                         'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 
                         'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                         'foreign_net_buy_3ago': '외국인순매수_3일전', 'foreign_net_buy_2ago': '외국인순매수_2일전', 'foreign_net_buy_1ago': '외국인순매수_1일전',
                         'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승', 'rsi_3down': 'RSI_3하강', 'trading_high': '거래대금_상승',
                         'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30', 'cap_status': '시가총액_상태'}
        df_ind_renamed = df_ind.rename(columns={k: v for k, v in col_map_total.items() if k in df_ind.columns})
        df_ind_renamed = df_ind_renamed.sort_values('시가총액', ascending=False).reset_index(drop=True)
        
        df_kr_ind = df_ind_renamed[df_ind_renamed['시장'] == 'KR'] if '시장' in df_ind_renamed.columns else pd.DataFrame()
        df_us_ind = df_ind_renamed[df_ind_renamed['시장'] == 'US'] if '시장' in df_ind_renamed.columns else pd.DataFrame()
        
        if not df_kr_ind.empty:
            cols_kr_total = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_kr_ind = df_kr_ind[[col for col in cols_kr_total if col in df_kr_ind.columns]]
            df_kr_ind = format_dataframe(df_kr_ind, 'KR')
            st.subheader("국내 (KR)")
            st.dataframe(df_kr_ind, column_config=column_config_kr)
        if not df_us_ind.empty:
            cols_us_total = ['종목코드', '회사명', '시장', 'RSI_3일_2ago', 'RSI_3일_1ago', 'RSI_3일_latest', '종가', '시가총액', '시가총액_상태', '20일평균거래대금', '오늘거래대금', '회전율', '캔들(5일)', '외국인순매수_3일전', '외국인순매수_2일전', '외국인순매수_1일전', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'RSI_3하강', '거래대금_상승', 'EPS > 0', '3<=PER<=30']
            df_us_ind = df_us_ind[[col for col in cols_us_total if col in df_us_ind.columns]]
            df_us_ind = format_dataframe(df_us_ind, 'US')
            st.subheader("해외 (US)")
            st.dataframe(df_us_ind, column_config=column_config_us)
        
        search_term = st.text_input("종목 검색 (Total)", placeholder="코드 입력", key="search_total")
        filtered_symbols = get_filtered_symbols(df_ind_renamed, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (Total)", filtered_symbols, key="select_total")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_ind[df_ind['symbol'] == st.session_state.selected_symbol]['market'].iloc[0] if 'market' in df_ind.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
        else:
            st.info("검색 결과 없음")
    else:
        st.info("데이터 없음 – 배치 실행하세요.")

if hasattr(st.session_state, 'con') and st.session_state.con:
    try:
        st.session_state.con.close()
    except:
        pass

log_time_file = "logs/batch_time.txt"
if os.path.exists(log_time_file):
    with open(log_time_file, "r") as f:
        last_time = f.read().strip()
    st.sidebar.info(f"마지막 갱신: {last_time}")
else:
    st.sidebar.info("로그 없음 – 배치 실행하세요.")