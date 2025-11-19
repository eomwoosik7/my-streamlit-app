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
from datetime import datetime
import numpy as np
import warnings

st.set_page_config(page_title="Trading Copilot", layout="wide")

warnings.filterwarnings("ignore", message=".*keyword arguments.*deprecated.*config.*")
warnings.filterwarnings("ignore", category=FutureWarning, message=".*to_pydatetime.*")
warnings.filterwarnings("ignore", category=UserWarning, module="pykrx")

@st.cache_data
def load_data():
    DB_PATH = "data/meta/universe.db"
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

def run_screener_query(con, filter_condition="all", use_us=True, use_kr=True, top_n=None, additional_filter=None):
    try:
        con.execute("SELECT 1").fetchone()
    except:
        con = get_db_connection()
        st.session_state.con = con
    
    market_filter = "market = 'US'" if use_us and not use_kr else "market = 'KR'" if use_kr and not use_us else "market IN ('US', 'KR')"
    
    if filter_condition == "obv":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago)"
    elif filter_condition == "rsi":
        condition = "(rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest) AND rsi_d_latest <= 50"
    elif filter_condition == "all":
        condition = "(obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AND (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50)"
    elif filter_condition == "eps_per_only":
        condition = "1=1"  # No OBV or RSI condition
    
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
            per, eps,
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
        per, eps,
        rsi_d_2ago, rsi_d_1ago, rsi_d_latest,
        macd_latest, signal_latest,
        obv_latest, signal_obv_latest,
        obv_1ago, signal_obv_1ago,
        (obv_latest > signal_obv_latest AND obv_1ago <= signal_obv_1ago) AS obv_bullish_cross,
        (rsi_d_2ago < rsi_d_1ago AND rsi_d_1ago < rsi_d_latest AND rsi_d_latest <= 50) AS rsi_3up
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
    def market_cap_fmt(x):
        if pd.isna(x) or x == 0: return 'N/A'
        x = float(x)
        prefix = 'KRW ' if market_type == 'KR' else 'USD '
        if market_type == 'KR': return f"{prefix}{x / 1e8:,.0f}억원"
        else: return f"{prefix}{x / 1e9:,.1f}B"
    
    def trading_value_fmt(x):
        if pd.isna(x) or x == 0: return 'N/A'
        x = float(x)
        if market_type == 'KR': return f"{x / 1e8:,.0f}억원"
        else: return f"{x / 1e6:,.0f}M USD"
    
    def turnover_fmt(x):
        if pd.isna(x) or x == 0: return 'N/A'
        x = float(x) * 100
        return f"{x:.2f}%"
    
    def per_fmt(x):
        if pd.isna(x) or x <= 0: return 'N/A'
        return f"{float(x):.2f}x"  # 소숫점 둘째 자리로 변경
    
    def eps_fmt(x):
        if pd.isna(x) or x == 0: return 'N/A'
        return f"{float(x):,.2f}"  # 소숫점 둘째 자리로 변경
    
    def rsi_fmt(x):
        if pd.isna(x): return 'N/A'
        if isinstance(x, str):
            try:
                vals = json.loads(x)
                if isinstance(vals, list):
                    return ', '.join(f"{v:.2f}" for v in vals)
                else:
                    return f"{float(vals):.2f}"
            except:
                return str(x)
        else:
            return f"{float(x):.2f}"
    
    def bool_fmt(x):
        return '✅' if x else '❌'

    format_dict = {}
    if '시가총액' in df.columns:
        format_dict['시가총액'] = market_cap_fmt
    if '20일평균거래대금' in df.columns:
        format_dict['20일평균거래대금'] = trading_value_fmt
    if '오늘거래대금' in df.columns:
        format_dict['오늘거래대금'] = trading_value_fmt
    if '회전율' in df.columns:
        format_dict['회전율'] = turnover_fmt
    if 'PER_TTM' in df.columns:  # TTM 명시
        format_dict['PER_TTM'] = per_fmt
    if 'EPS_TTM' in df.columns:  # TTM 명시
        format_dict['EPS_TTM'] = eps_fmt
    if 'RSI_3일' in df.columns:
        format_dict['RSI_3일'] = rsi_fmt
    if 'OBV_상승' in df.columns:
        format_dict['OBV_상승'] = bool_fmt
    if 'RSI_3상승' in df.columns:
        format_dict['RSI_3상승'] = bool_fmt
    if 'EPS > 0' in df.columns:
        format_dict['EPS > 0'] = bool_fmt
    if '3<=PER<=30' in df.columns:
        format_dict['3<=PER<=30'] = bool_fmt

    styled_df = df.style.format(format_dict).set_properties(**{'text-align': 'center'})

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
    st.session_state.current_tab = "Home"

st.sidebar.title("설정")
use_us = st.sidebar.checkbox("US 시장", value=True)
use_kr = st.sidebar.checkbox("KR 시장", value=True)
top_n = st.sidebar.number_input("Top N", min_value=10, max_value=100, value=20)

if st.sidebar.button("배치 실행"):
    subprocess.run(["python", "batch.py", str(use_us), str(use_kr), str(top_n)])

df_ind = load_data()
con = get_db_connection()

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Home", "OBV 상승 크로스", "RSI 상승 지속 (50 이하)", "EPS & PER", "Total"])

with tab1:
    st.session_state.current_tab = "Home"
    st.header("All (조건 1+2+3 유동성)")
    df_all_full = run_screener_query(con, "all", use_us, use_kr, top_n=None, additional_filter="eps_per")
    df_all = df_all_full
    df_all = add_names(df_all)
    df_all = prepare_tab_df(df_all)
    
    if not df_all_full.empty:
        df_kr_temp = df_all_full[df_all_full['market'] == 'KR']
        df_us_temp = df_all_full[df_all_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_all['eps_positive'] = df_all['eps'] > 0
        df_all['per_range'] = (df_all['per'] >= 3) & (df_all['per'] <= 30)
        
        df_all = df_all.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'rsi_d_array': 'RSI_3일', 
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30'}.items() if k in df_all.columns})
        
        df_kr_results = df_all[df_all['시장'] == 'KR'] if '시장' in df_all.columns else pd.DataFrame()
        df_us_results = df_all[df_all['시장'] == 'US'] if '시장' in df_all.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR) - 시총: KRW 억원")
            st.dataframe(df_kr_results)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US) - 시총: USD B")
            st.dataframe(df_us_results)
        
        search_term = st.text_input("종목 검색 (Home)", placeholder="코드/회사명 입력", key="search_home")
        filtered_symbols = get_filtered_symbols(df_all, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (Home)", filtered_symbols, key="select_home")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_all[df_all['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_all.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("All 후보 없음")

with tab2:
    st.session_state.current_tab = "OBV 상승 크로스"
    st.header("OBV 상승 크로스 (조건 1 + 유동성)")
    df_obv_full = run_screener_query(con, "obv", use_us, use_kr, top_n=None)
    df_obv = df_obv_full
    df_obv = add_names(df_obv)
    df_obv = prepare_tab_df(df_obv)
    
    if not df_obv_full.empty:
        df_kr_temp = df_obv_full[df_obv_full['market'] == 'KR']
        df_us_temp = df_obv_full[df_obv_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_obv['eps_positive'] = df_obv['eps'] > 0
        df_obv['per_range'] = (df_obv['per'] >= 3) & (df_obv['per'] <= 30)
        
        df_obv = df_obv.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'rsi_d_array': 'RSI_3일', 
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30'}.items() if k in df_obv.columns})
        
        df_kr_results = df_obv[df_obv['시장'] == 'KR'] if '시장' in df_obv.columns else pd.DataFrame()
        df_us_results = df_obv[df_obv['시장'] == 'US'] if '시장' in df_obv.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR) - 시총: KRW 억원")
            st.dataframe(df_kr_results)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US) - 시총: USD B")
            st.dataframe(df_us_results)
        
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
    df_rsi_full = run_screener_query(con, "rsi", use_us, use_kr, top_n=None)
    df_rsi = df_rsi_full
    df_rsi = add_names(df_rsi)
    df_rsi = prepare_tab_df(df_rsi)
    
    if not df_rsi_full.empty:
        df_kr_temp = df_rsi_full[df_rsi_full['market'] == 'KR']
        df_us_temp = df_rsi_full[df_rsi_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_rsi['eps_positive'] = df_rsi['eps'] > 0
        df_rsi['per_range'] = (df_rsi['per'] >= 3) & (df_rsi['per'] <= 30)
        
        df_rsi = df_rsi.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'rsi_d_array': 'RSI_3일', 
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30'}.items() if k in df_rsi.columns})
        
        df_kr_results = df_rsi[df_rsi['시장'] == 'KR'] if '시장' in df_rsi.columns else pd.DataFrame()
        df_us_results = df_rsi[df_rsi['시장'] == 'US'] if '시장' in df_rsi.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR) - 시총: KRW 억원")
            st.dataframe(df_kr_results)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US) - 시총: USD B")
            st.dataframe(df_us_results)
        
        search_term = st.text_input("종목 검색 (RSI)", placeholder="코드/회사명 입력", key="search_rsi")
        filtered_symbols = get_filtered_symbols(df_rsi, search_term)
        if filtered_symbols:
            selected_symbol = st.selectbox("종목 선택 (RSI)", filtered_symbols, key="select_rsi")
            if selected_symbol != st.session_state.selected_symbol:
                st.session_state.selected_symbol = selected_symbol
            if st.session_state.selected_symbol:
                market = df_rsi[df_rsi['종목코드'] == st.session_state.selected_symbol]['시장'].iloc[0] if '시장' in df_rsi.columns else 'US'
                show_graphs(st.session_state.selected_symbol, market)
    else:
        st.info("RSI 후보 없음")

with tab4:
    st.session_state.current_tab = "EPS & PER"
    st.header("EPS & PER(EPS>0, 3<=PER<=30 조건3 + 유동성)")
    df_rsi_eps_per_full = run_screener_query(con, "eps_per_only", use_us, use_kr, top_n=None, additional_filter="eps_per")
    df_rsi_eps_per = df_rsi_eps_per_full
    df_rsi_eps_per = add_names(df_rsi_eps_per)
    df_rsi_eps_per = prepare_tab_df(df_rsi_eps_per)
    
    if not df_rsi_eps_per_full.empty:
        df_kr_temp = df_rsi_eps_per_full[df_rsi_eps_per_full['market'] == 'KR']
        df_us_temp = df_rsi_eps_per_full[df_rsi_eps_per_full['market'] == 'US']
        total_candidates = len(df_kr_temp) + len(df_us_temp)
        st.metric("후보 수", total_candidates)
        
        df_rsi_eps_per['eps_positive'] = df_rsi_eps_per['eps'] > 0
        df_rsi_eps_per['per_range'] = (df_rsi_eps_per['per'] >= 3) & (df_rsi_eps_per['per'] <= 30)
        
        df_rsi_eps_per = df_rsi_eps_per.rename(columns={k: v for k, v in {'symbol': '종목코드', 'market': '시장', 'name': '회사명', 'rsi_d_array': 'RSI_3일', 
                   'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                   'per': 'PER_TTM', 'eps': 'EPS_TTM', 'obv_bullish_cross': 'OBV_상승', 'rsi_3up': 'RSI_3상승',
                   'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30'}.items() if k in df_rsi_eps_per.columns})
        
        df_kr_results = df_rsi_eps_per[df_rsi_eps_per['시장'] == 'KR'] if '시장' in df_rsi_eps_per.columns else pd.DataFrame()
        df_us_results = df_rsi_eps_per[df_rsi_eps_per['시장'] == 'US'] if '시장' in df_rsi_eps_per.columns else pd.DataFrame()
        
        if not df_kr_results.empty:
            cols_kr = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'EPS > 0', '3<=PER<=30']
            df_kr_results = df_kr_results[[col for col in cols_kr if col in df_kr_results.columns]]
            df_kr_results = df_kr_results.sort_values('시가총액', ascending=False)
            df_kr_results = format_dataframe(df_kr_results, 'KR')
            st.subheader("국내 (KR) - 시총: KRW 억원")
            st.dataframe(df_kr_results)
        if not df_us_results.empty:
            cols_us = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'OBV_상승', 'RSI_3상승', 'EPS > 0', '3<=PER<=30']
            df_us_results = df_us_results[[col for col in cols_us if col in df_us_results.columns]]
            df_us_results = df_us_results.sort_values('시가총액', ascending=False)
            df_us_results = format_dataframe(df_us_results, 'US')
            st.subheader("해외 (US) - 시총: USD B")
            st.dataframe(df_us_results)
        
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

with tab5:
    st.session_state.current_tab = "Total"
    st.header("Total (전체 종목 목록)")
    if not df_ind.empty:
        df_ind = add_names(df_ind)
        df_ind = prepare_tab_df(df_ind, is_total=True)
        
        df_ind['eps_positive'] = df_ind['eps'] > 0
        df_ind['per_range'] = (df_ind['per'] >= 3) & (df_ind['per'] <= 30)
        
        col_map_total = {'symbol': '종목코드', 'market': '시장',
                         'rsi_d': 'RSI_3일', 
                         'market_cap': '시가총액', 'avg_trading_value_20d': '20일평균거래대금', 
                         'today_trading_value': '오늘거래대금', 'turnover': '회전율',
                         'per': 'PER_TTM', 'eps': 'EPS_TTM',
                         'eps_positive': 'EPS > 0', 'per_range': '3<=PER<=30'}
        df_ind_renamed = df_ind.rename(columns={k: v for k, v in col_map_total.items() if k in df_ind.columns})
        df_ind_renamed = df_ind_renamed.sort_values('시가총액', ascending=False).reset_index(drop=True)
        
        df_kr_ind = df_ind_renamed[df_ind_renamed['시장'] == 'KR'] if '시장' in df_ind_renamed.columns else pd.DataFrame()
        df_us_ind = df_ind_renamed[df_ind_renamed['시장'] == 'US'] if '시장' in df_ind_renamed.columns else pd.DataFrame()
        
        if not df_kr_ind.empty:
            cols_kr_total = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'EPS > 0', '3<=PER<=30']
            df_kr_ind = df_kr_ind[[col for col in cols_kr_total if col in df_kr_ind.columns]]
            df_kr_ind = format_dataframe(df_kr_ind, 'KR')
            st.subheader("국내 (KR) - 시총: KRW 억원")
            st.dataframe(df_kr_ind)
        if not df_us_ind.empty:
            cols_us_total = ['종목코드', '회사명', '시장', 'RSI_3일', '시가총액', '20일평균거래대금', '오늘거래대금', '회전율', 'PER_TTM', 'EPS_TTM', 'EPS > 0', '3<=PER<=30']
            df_us_ind = df_us_ind[[col for col in cols_us_total if col in df_us_ind.columns]]
            df_us_ind = format_dataframe(df_us_ind, 'US')
            st.subheader("해외 (US) - 시총: USD B")
            st.dataframe(df_us_ind)
        
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