import datetime
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import json
import yfinance as yf
from datetime import timedelta

today = datetime.date.today()
if today.weekday() >= 5:
    days_back = today.weekday() - 4
    today -= datetime.timedelta(days=days_back)

data_dir = r"C:\Users\ws\Desktop\Python\Project_Hermes5\data"
os.makedirs(data_dir, exist_ok=True)

def crawl_naver_stock_data(code):
    """
    네이버 증권에서 모든 필요한 데이터 크롤링
    
    Returns:
    - dict: {
        'per': float,
        'eps': float, 
        'pbr': float,
        'sector': str,
        'institutional_ownership': float,  # 기관보유율(%)
        'foreign_net_buy': list (5일치)
      }
    """
    result = {
        'per': None,
        'eps': None,
        'pbr': None,
        'sector': 'N/A',
        'foreign_ownership': None,  # 외국인 보유율 (기관 포함)
        'foreign_net_buy': [0, 0, 0, 0, 0],
        'institutional_net_buy': [0, 0, 0, 0, 0],  # 기관 순매매량 5일치
        'foreign_dates': ['N/A', 'N/A', 'N/A', 'N/A', 'N/A']  # 실제 날짜
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # ============================================
    # 1. 메인 페이지: PER, EPS, PBR, 업종, 기관보유율
    # ============================================
    try:
        main_url = f"https://finance.naver.com/item/main.nhn?code={code}"
        response = requests.get(main_url, headers=headers, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # PER
        per_tags = soup.find_all('em', id='_per')
        if per_tags:
            try:
                result['per'] = float(per_tags[0].text.strip().replace(',', ''))
            except:
                pass
        
        # EPS
        eps_tags = soup.find_all('em', id='_eps')
        if eps_tags:
            try:
                result['eps'] = float(eps_tags[0].text.strip().replace(',', ''))
            except:
                pass
        
        # PBR
        pbr_tags = soup.find_all('em', id='_pbr')
        if pbr_tags:
            try:
                result['pbr'] = float(pbr_tags[0].text.strip().replace(',', ''))
            except:
                pass
        
        # ✅ 업종 추출 개선
        try:
            import re
            text = soup.get_text()
            
            # 패턴 1: "업종명 : 무선통신서비스｜재무정보" (괄호 없음)
            match = re.search(r'업종명\s*[:：]\s*([^\|｜]+)', text)
            if match:
                sector_text = match.group(1).strip()
                # "재무정보" 이후 제거
                sector_text = re.split(r'재무정보|분기|기준', sector_text)[0].strip()
                # 숫자, 공백 제거
                sector_text = re.sub(r'[\d\s.]+', '', sector_text)
                if sector_text and len(sector_text) > 0:
                    result['sector'] = sector_text
        except:
            pass
        
    except Exception as e:
        pass
        # print(f"❌ {code} 메인 페이지 크롤링 실패: {e}")
    
    # ============================================
    # 2. 외국인 페이지: 순매수거래량 (최근 5일) + 보유율
    # ============================================
    try:
        foreign_url = f"https://finance.naver.com/item/frgn.nhn?code={code}"
        response = requests.get(foreign_url, headers=headers, timeout=5)
        response.raise_for_status()
        
        # pandas read_html로 테이블 파싱
        tables = pd.read_html(response.text)
        
        df_foreign = None
        
        # ✅ 멀티레벨 컬럼 처리
        for table in tables:
            # 컬럼이 튜플 형태인 경우 (멀티레벨)
            if isinstance(table.columns, pd.MultiIndex):
                # ('날짜', '날짜'), ('외국인', '순매매량') 형태
                if '날짜' in [col[0] for col in table.columns]:
                    # 외국인 관련 컬럼 확인
                    has_foreign = False
                    for col in table.columns:
                        if '외국인' in str(col):
                            has_foreign = True
                            break
                    
                    if has_foreign:
                        df_foreign = table.copy()
                        
                        # ✅ 컬럼명 단순화 및 정리
                        new_columns = []
                        for col in df_foreign.columns:
                            if col[0] == '날짜':
                                new_columns.append('날짜')
                            elif col[0] == '외국인' and '순매매량' in str(col):
                                new_columns.append('순매수거래량')
                            elif col[0] == '외국인' and '보유율' in str(col):
                                new_columns.append('외국인보유율')
                            elif col[0] == '기관' and '순매매량' in str(col):
                                new_columns.append('기관순매매량')
                            else:
                                new_columns.append(col[0] if col[0] == col[1] else f"{col[0]}_{col[1]}")
                        
                        df_foreign.columns = new_columns
                        break
            else:
                # 일반 컬럼인 경우 (혹시 모를 대비)
                if '날짜' in table.columns:
                    for col in table.columns:
                        if '순매매량' in col or '순매수량' in col:
                            df_foreign = table.copy()
                            break
                if df_foreign is not None:
                    break
        
        if df_foreign is not None and not df_foreign.empty:
            df_foreign = df_foreign.dropna(subset=['날짜'])
            
            # ✅ 날짜 처리
            df_foreign['날짜'] = df_foreign['날짜'].astype(str).str.replace('.', '').str.replace('-', '').str.strip()
            
            # 순매수거래량 처리
            if '순매수거래량' in df_foreign.columns:
                df_foreign['순매수거래량'] = pd.to_numeric(
                    df_foreign['순매수거래량'].astype(str).str.replace(',', ''), 
                    errors='coerce'
                ).fillna(0).astype(int)
                
                # ✅ 최근 5일치 (날짜와 함께)
                recent_rows = df_foreign.head(5)
                result['foreign_net_buy'] = []
                result['foreign_dates'] = []
                
                for _, row in recent_rows.iterrows():
                    result['foreign_net_buy'].append(int(row['순매수거래량']))
                    result['foreign_dates'].append(row['날짜'])
                
                # 5개 미만이면 0으로 패딩
                while len(result['foreign_net_buy']) < 5:
                    result['foreign_net_buy'].append(0)
                    result['foreign_dates'].append('N/A')
            
            # ✅ 기관 순매매량 처리 (5일치)
            if '기관순매매량' in df_foreign.columns:
                df_foreign['기관순매매량'] = pd.to_numeric(
                    df_foreign['기관순매매량'].astype(str).str.replace(',', ''), 
                    errors='coerce'
                ).fillna(0).astype(int)
                
                # 최근 5일치
                recent_inst = df_foreign['기관순매매량'].head(5).tolist()
                result['institutional_net_buy'] = recent_inst
                
                # 5개 미만이면 0으로 패딩
                while len(result['institutional_net_buy']) < 5:
                    result['institutional_net_buy'].append(0)
            
            # ✅ 외국인 보유율 (가장 최근 날짜)
            if '외국인보유율' in df_foreign.columns:
                try:
                    latest_rate = df_foreign['외국인보유율'].iloc[0]
                    # "12.34%" 형태에서 숫자만 추출
                    import re
                    match = re.search(r'([\d,.]+)', str(latest_rate))
                    if match:
                        result['foreign_ownership'] = float(match.group(1).replace(',', ''))
                except:
                    result['foreign_ownership'] = None
        
    except Exception as e:
        pass
        # print(f"❌ {code} 외국인 페이지 크롤링 실패: {e}")
    
    return result


# ============================================
# 메인 실행
# ============================================

print("="*60)
print("📊 네이버 증권 통합 크롤링 시작")
print("수집 항목: PER, EPS, PBR, 업종, 외국인 순매수(5일)")
print("="*60)

# 1. 상위 1000개 종목 조회 (네이버 금융 시가총액 순위)
print("\n📋 KRX 종목 리스트 조회 중 (네이버 금융)...")

_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.naver.com/'
}
_all_stocks = []

for _sosok in [0, 1]:
    _market_name = 'KOSPI' if _sosok == 0 else 'KOSDAQ'
    _page = 1
    while True:
        _url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={_sosok}&page={_page}'
        _res = requests.get(_url, headers=_headers, timeout=10)
        _res.encoding = 'euc-kr'
        _soup = BeautifulSoup(_res.text, 'html.parser')

        _pager = _soup.find('td', class_='pgRR')
        if _pager is None:
            break
        _last_page = int(_pager.find('a')['href'].split('page=')[-1])

        _table = _soup.find('table', class_='type_2')
        if _table is None:
            break

        for _row in _table.find_all('tr'):
            _link = _row.find('a', class_='tltle')
            if _link is None:
                continue
            _href = _link.get('href', '')
            if 'code=' not in _href:
                continue
            _code = _href.split('code=')[-1]
            _name = _link.text.strip()
            _tds = _row.find_all('td')
            _cap = 0
            if len(_tds) >= 7:
                try:
                    _cap_text = _tds[6].text.strip().replace(',', '')
                    _cap = int(_cap_text) if _cap_text.isdigit() else 0
                except:
                    _cap = 0
            if _code and len(_code) == 6 and _code.isdigit():
                # ETF/ETN 제외 필터 (개별 기업만 수집)
                _etf_prefixes = (
                    'KODEX', 'TIGER', 'RISE', 'ACE', 'SOL', 'PLUS',
                    'KIWOOM', 'HANARO', 'TIME', 'KoAct', 'ARIRANG',
                    'FOCUS', 'SMART', 'TREX', 'BNK', 'NEXT', 'KOSEF',
                    'TIMEFOLIO', 'KTOP', '1Q', 'N2 ', 'KB KIS',
                    '삼성 레버리지', '미래에셋 레버리지', '신한 레버리지',
                    '한투 KIS', '키움 CD', '키움 레버리지', '하나 CD',
                    '하나 레버리지',
                )
                if _name.startswith(_etf_prefixes) or 'ETN' in _name or 'ETF' in _name:
                    continue
                _all_stocks.append({'Code': _code, 'Name': _name, 'Marcap': _cap})

        if _page >= _last_page:
            break
        _page += 1
        time.sleep(0.3)

    print(f"  ✅ {_market_name} 수집 완료")

if not _all_stocks:
    print("🚨 KRX 데이터 조회 실패")
    exit()

df_krx = pd.DataFrame(_all_stocks)
df_krx['Marcap'] = pd.to_numeric(df_krx['Marcap'], errors='coerce').fillna(0)
df_krx = df_krx.drop_duplicates('Code')
df_top1000 = df_krx.sort_values('Marcap', ascending=False).head(1000).reset_index(drop=True)
print(f"✅ 상위 1000개 종목 조회 완료")

# ============================================
# 2. 크롤링 실행 (멀티스레딩)
# ============================================
print("\n🕷️ 네이버 증권 크롤링 시작 (멀티스레딩 x5)")
print("⏱️ 예상 시간: 약 2~3분")
print()

per_eps_results = []
foreign_results = []
sector_results = []

# 결과 저장용 딕셔너리 (순서 보장)
results_dict = {}
lock = threading.Lock()

def crawl_one(args):
    idx, row = args
    code = row['Code']
    name = row['Name']
    data = crawl_naver_stock_data(code)
    time.sleep(0.2)  # 서버 부하 방지 (0.5 → 0.2)
    return idx, code, name, data

rows = list(df_top1000.iterrows())

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(crawl_one, (idx, row)): idx for idx, row in rows}
    
    completed_count = 0
    for future in tqdm(as_completed(futures), total=len(futures), desc="크롤링 진행"):
        try:
            idx, code, name, data = future.result()
        except Exception as e:
            print(f"❌ 에러: {e}")
            continue

        # PER/EPS/PBR 결과 저장
        per_eps_results.append({
            '티커': code,
            '종목명': name,
            'PER': data['per'] if data['per'] is not None else '-',
            'EPS': data['eps'] if data['eps'] is not None else '-',
            'PBR': data['pbr'] if data['pbr'] is not None else '-',
            '외국인보유율': data['foreign_ownership'] if data['foreign_ownership'] is not None else '-',
            '날짜': today.strftime('%Y%m%d')
        })

        # 섹터 결과 저장
        sector_results.append({
            '회사명': name,
            '종목코드': code,
            '업종': data['sector']
        })

        # 외국인/기관 순매수 결과 저장
        for day_idx in range(5):
            foreign_net_buy = data['foreign_net_buy'][day_idx] if day_idx < len(data['foreign_net_buy']) else 0
            inst_net_buy = data['institutional_net_buy'][day_idx] if day_idx < len(data['institutional_net_buy']) else 0
            date_str = data['foreign_dates'][day_idx] if day_idx < len(data['foreign_dates']) else 'N/A'

            if date_str != 'N/A':
                foreign_results.append({
                    '티커': code,
                    '종목명': name,
                    '날짜': date_str,
                    '외국인순매수': foreign_net_buy,
                    '기관순매수': inst_net_buy
                })

        completed_count += 1
        if completed_count % 100 == 0:
            per_success = sum(1 for r in per_eps_results if r['PER'] != '-')
            sector_success = sum(1 for r in sector_results if r['업종'] != 'N/A')
            trading_success = len([r for r in foreign_results if r['외국인순매수'] != 0 or r['기관순매수'] != 0])
            print(f"\n📊 진행: {completed_count}/1000 | PER: {per_success}개 | 업종: {sector_success}개 | 매매: {trading_success}건")

# ============================================
# 3. 결과 저장
# ============================================

print("\n" + "="*60)
print("💾 파일 저장 중...")
print("="*60)

# PER/EPS 저장
df_per_eps = pd.DataFrame(per_eps_results)
per_eps_path = os.path.join(data_dir, 'per_eps_top_1000.csv')
df_per_eps.to_csv(per_eps_path, encoding='utf-8-sig', index=False)
per_success = len(df_per_eps[df_per_eps['PER'] != '-'])
print(f"✅ PER/EPS: {per_eps_path}")
print(f"   성공: {per_success}/{len(df_per_eps)} ({per_success/len(df_per_eps)*100:.1f}%)")

# 외국인/기관 순매수 저장 (통합)
df_trading = pd.DataFrame(foreign_results)
# ✅ 시가총액 순으로 정렬하기 위해 df_top1000과 조인
df_trading = df_trading.merge(df_top1000[['Code', 'Marcap']], left_on='티커', right_on='Code', how='left')
df_trading = df_trading.sort_values(by=['날짜', 'Marcap'], ascending=[False, False])
df_trading = df_trading.drop(columns=['Code', 'Marcap'])
trading_path = os.path.join(data_dir, 'foreign_institutional_net_buy_daily_top_1000.csv')
df_trading.to_csv(trading_path, encoding='utf-8-sig', index=False)
trading_dates = sorted(df_trading['날짜'].unique(), reverse=True)
print(f"✅ 외국인/기관 순매수: {trading_path}")
print(f"   수집 날짜: {trading_dates}")

# 섹터 저장 (누적 방식 - 기존 종목 유지, 신규 종목 추가, N/A였던 종목 업데이트)
df_sector_new = pd.DataFrame(sector_results)
sector_path = os.path.join(data_dir, 'kr_stock_sectors.csv')

if os.path.exists(sector_path):
    df_sector_existing = pd.read_csv(sector_path, encoding='utf-8-sig', dtype={'종목코드': str})
    df_sector_existing['종목코드'] = df_sector_existing['종목코드'].str.zfill(6)
    df_sector_new['종목코드'] = df_sector_new['종목코드'].astype(str).str.zfill(6)

    existing_codes = set(df_sector_new['종목코드'].tolist())
    df_sector_keep = df_sector_existing[~df_sector_existing['종목코드'].isin(existing_codes)]

    df_sector = pd.concat([df_sector_keep, df_sector_new], ignore_index=True)
    df_sector = df_sector.drop_duplicates(subset='종목코드', keep='last')
    df_sector = df_sector.sort_values('종목코드').reset_index(drop=True)

    added = len(df_sector) - len(df_sector_existing)
    updated = len(df_sector_new)
    print(f"   기존 종목 수: {len(df_sector_existing)}개")
    print(f"   오늘 수집: {updated}개 (업데이트/신규 포함)")
    print(f"   누적 종목 수: {len(df_sector)}개 (신규 추가: {max(added,0)}개)")
else:
    df_sector_new['종목코드'] = df_sector_new['종목코드'].astype(str).str.zfill(6)
    df_sector = df_sector_new
    print(f"   신규 파일 생성: {len(df_sector)}개")

df_sector.to_csv(sector_path, encoding='utf-8-sig', index=False)
sector_success = len(df_sector[df_sector['업종'] != 'N/A'])
print(f"✅ 섹터: {sector_path}")
print(f"   업종 매핑 성공: {sector_success}/{len(df_sector)} ({sector_success/len(df_sector)*100:.1f}%)")

# ============================================
# 4. 섹터 ETF 트렌드 수집
# ============================================
print("\n📈 섹터 ETF 트렌드 수집 중...")

import re
import yfinance as yf
from datetime import timedelta

# 섹터별 대표 ETF
sector_etfs = {
    'Information Technology': {'US': 'XLK', 'KR': '139260', 'kr_name': 'TIGER 200 IT'},
    'Consumer Discretionary': {'US': 'XLY', 'KR': '139290', 'kr_name': 'TIGER 200 경기소비재'},
    'Communication Services': {'US': 'XLC', 'KR': '228810', 'kr_name': 'TIGER 미디어컨텐츠'},
    'Health Care': {'US': 'XLV', 'KR': '143860', 'kr_name': 'TIGER 헬스케어'},
    'Consumer Staples': {'US': 'XLP', 'KR': '266410', 'kr_name': 'KODEX 필수소비재'},
    'Financials': {'US': 'XLF', 'KR': '139270', 'kr_name': 'TIGER 200 금융'},
    'Energy': {'US': 'XLE', 'KR': '117680', 'kr_name': 'KODEX 에너지화학'},
    'Industrials': {'US': 'XLI', 'KR': '117700', 'kr_name': 'KODEX 산업재'},
    'Materials': {'US': 'XLB', 'KR': '117690', 'kr_name': 'KODEX 소재산업'},
    'Utilities': {'US': 'XLU', 'KR': '404650', 'kr_name': 'TIGER KRX 기후변화솔루션'},
    'Real Estate': {'US': 'XLRE', 'KR': '329200', 'kr_name': 'TIGER 리츠부동산인프라'}
}

def get_kr_etf_trend(code, name):
    """KR ETF 1개월 수익률 크롤링 (네이버)"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # "1개월 수익률" 찾기
        text = soup.get_text()
        match = re.search(r'1개월\s*수익률\s*([+\-]?[\d,.]+)%', text)
        if match:
            rate = float(match.group(1).replace(',', ''))
            trend = '상승' if rate > 0 else '하락'
            return f"{trend}({rate:+.2f}%) {name}"
    except:
        pass
    return None

def get_us_etf_trend(ticker):
    """US ETF 1개월 수익률 계산 (yfinance)"""
    try:
        end_date = datetime.datetime.now()
        start_date = end_date - timedelta(days=35)
        
        etf = yf.Ticker(ticker)
        hist = etf.history(start=start_date, end=end_date)
        
        if hist.empty or len(hist) < 2:
            return None
        
        latest_close = hist['Close'].iloc[-1]
        month_ago_close = hist['Close'].iloc[0]
        change_rate = ((latest_close - month_ago_close) / month_ago_close) * 100
        
        trend = '상승' if change_rate > 0 else '하락'
        return f"{trend}({change_rate:+.2f}%) {ticker}"
    except Exception as e:
        print(f"      US {ticker} 에러: {type(e).__name__} - {str(e)[:50]}")
        return None

sector_trends = []
for sector, etfs in sector_etfs.items():
    print(f"  {sector} 수집 중...")
    
    # US (yfinance)
    us_trend = get_us_etf_trend(etfs['US'])
    if us_trend:
        sector_trends.append({
            'sector': sector,
            'market': 'US',
            'trend_display': us_trend
        })
        print(f"    US: {us_trend}")
    else:
        print(f"    US: ❌ 실패")
    
    # KR (네이버 크롤링)
    kr_trend = get_kr_etf_trend(etfs['KR'], etfs['kr_name'])
    if kr_trend:
        sector_trends.append({
            'sector': sector,
            'market': 'KR',
            'trend_display': kr_trend
        })
        print(f"    KR: {kr_trend}")
    else:
        print(f"    KR: ❌ 실패")
    
    time.sleep(0.3)

df_sector_trends = pd.DataFrame(sector_trends)
sector_trend_path = os.path.join(data_dir, 'sector_etf_trends.csv')
df_sector_trends.to_csv(sector_trend_path, encoding='utf-8-sig', index=False)
print(f"\n✅ 섹터 트렌드: {sector_trend_path}")
print(f"   수집: {len(df_sector_trends)}개 (US: {len(df_sector_trends[df_sector_trends['market']=='US'])}개, KR: {len(df_sector_trends[df_sector_trends['market']=='KR'])}개)")

# ============================================
# 5. 샘플 출력
# ============================================

print("\n" + "="*60)
print("📋 샘플 데이터")
print("="*60)

print("\n[PER/EPS 상위 5개]")
print(df_per_eps.head(5).to_string(index=False))

print("\n[외국인/기관 순매수 최신일 상위 5개]")
latest_date = df_trading['날짜'].max()
top5_trading = df_trading[df_trading['날짜'] == latest_date].head(5)
print(top5_trading[['종목명', '외국인순매수', '기관순매수']].to_string(index=False))

print("\n[섹터 샘플 5개]")
print(df_sector[df_sector['업종'] != 'N/A'].head(5)[['회사명', '업종']].to_string(index=False))

print("\n[섹터 ETF 트렌드 샘플]")
print(df_sector_trends.head(6).to_string(index=False))

print("\n" + "="*60)
print("✅ 모든 크롤링 완료!")
print("="*60)
print("\n⚠️ 주의사항:")
print("1. kr_stock_sectors.csv는 '업종'만 포함 (Sector, sector_trend는 별도 추가 필요)")
print("2. 외국인보유율은 외국인+기관 보유율을 포함합니다")
print("3. 섹터 ETF 트렌드는 네이버 증권 1개월 수익률 기준입니다")