import datetime
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import re
import json

today = datetime.date.today()
if today.weekday() >= 5:
    days_back = today.weekday() - 4
    today -= datetime.timedelta(days=days_back)

DATA_DIR = os.getenv('DATA_DIR', './data')
data_dir = DATA_DIR
os.makedirs(data_dir, exist_ok=True)

SHORT_FOLDER = os.path.join(data_dir, 'short_term_results')
MID_FOLDER = os.path.join(data_dir, 'screener_results')

# ============================================
# ETF 판별 함수
# ============================================

ETF_PREFIXES = (
    'KODEX', 'TIGER', 'RISE', 'ACE', 'SOL', 'PLUS',
    'KIWOOM', 'HANARO', 'TIME', 'KoAct', 'ARIRANG',
    'FOCUS', 'SMART', 'TREX', 'BNK', 'NEXT', 'KOSEF',
    'TIMEFOLIO', 'KTOP', '1Q', 'N2 ', 'KB KIS',
    '삼성 레버리지', '미래에셋 레버리지', '신한 레버리지',
    '한투 KIS', '키움 CD', '키움 레버리지', '하나 CD',
    '하나 레버리지',
)

def is_etf(name):
    """종목명 기준으로 ETF/ETN 여부 판별"""
    if name.startswith(ETF_PREFIXES):
        return True
    if 'ETN' in name or 'ETF' in name:
        return True
    return False


def crawl_naver_stock_data(code):
    """
    네이버 증권에서 모든 필요한 데이터 크롤링

    Returns:
    - dict: {
        'per': float,
        'eps': float,
        'pbr': float,
        'sector': str,
        'foreign_ownership': float,
        'foreign_net_buy': list (5일치),
        'institutional_net_buy': list (5일치),
        'foreign_dates': list (5일치)
      }
    """
    result = {
        'per': None,
        'eps': None,
        'pbr': None,
        'sector': 'N/A',
        'foreign_ownership': None,
        'foreign_net_buy': [0, 0, 0, 0, 0],
        'institutional_net_buy': [0, 0, 0, 0, 0],
        'foreign_dates': ['N/A', 'N/A', 'N/A', 'N/A', 'N/A']
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    # ============================================
    # 1. 메인 페이지: PER, EPS, PBR, 업종
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

        # 업종 추출
        try:
            text = soup.get_text()
            match = re.search(r'업종명\s*[:：]\s*([^\|｜]+)', text)
            if match:
                sector_text = match.group(1).strip()
                sector_text = re.split(r'재무정보|분기|기준', sector_text)[0].strip()
                sector_text = re.sub(r'[\d\s.]+', '', sector_text)
                if sector_text and len(sector_text) > 0:
                    result['sector'] = sector_text
        except:
            pass

    except Exception as e:
        pass

    # ============================================
    # 2. 외국인 페이지: 순매수거래량 (최근 5일) + 보유율
    # ============================================
    try:
        foreign_url = f"https://finance.naver.com/item/frgn.nhn?code={code}"
        response = requests.get(foreign_url, headers=headers, timeout=5)
        response.raise_for_status()

        tables = pd.read_html(response.text)
        df_foreign = None

        for table in tables:
            if isinstance(table.columns, pd.MultiIndex):
                if '날짜' in [col[0] for col in table.columns]:
                    has_foreign = False
                    for col in table.columns:
                        if '외국인' in str(col):
                            has_foreign = True
                            break

                    if has_foreign:
                        df_foreign = table.copy()
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
                if '날짜' in table.columns:
                    for col in table.columns:
                        if '순매매량' in col or '순매수량' in col:
                            df_foreign = table.copy()
                            break
                if df_foreign is not None:
                    break

        if df_foreign is not None and not df_foreign.empty:
            df_foreign = df_foreign.dropna(subset=['날짜'])
            df_foreign['날짜'] = df_foreign['날짜'].astype(str).str.replace('.', '').str.replace('-', '').str.strip()

            if '순매수거래량' in df_foreign.columns:
                df_foreign['순매수거래량'] = pd.to_numeric(
                    df_foreign['순매수거래량'].astype(str).str.replace(',', ''),
                    errors='coerce'
                ).fillna(0).astype(int)

                recent_rows = df_foreign.head(5)
                result['foreign_net_buy'] = []
                result['foreign_dates'] = []

                for _, row in recent_rows.iterrows():
                    result['foreign_net_buy'].append(int(row['순매수거래량']))
                    result['foreign_dates'].append(row['날짜'])

                while len(result['foreign_net_buy']) < 5:
                    result['foreign_net_buy'].append(0)
                    result['foreign_dates'].append('N/A')

            if '기관순매매량' in df_foreign.columns:
                df_foreign['기관순매매량'] = pd.to_numeric(
                    df_foreign['기관순매매량'].astype(str).str.replace(',', ''),
                    errors='coerce'
                ).fillna(0).astype(int)

                recent_inst = df_foreign['기관순매매량'].head(5).tolist()
                result['institutional_net_buy'] = recent_inst

                while len(result['institutional_net_buy']) < 5:
                    result['institutional_net_buy'].append(0)

            if '외국인보유율' in df_foreign.columns:
                try:
                    latest_rate = df_foreign['외국인보유율'].iloc[0]
                    match = re.search(r'([\d,.]+)', str(latest_rate))
                    if match:
                        result['foreign_ownership'] = float(match.group(1).replace(',', ''))
                except:
                    result['foreign_ownership'] = None

    except Exception as e:
        pass

    return result


# ============================================
# 백테스트 누락 종목 추가 크롤링 함수
# ============================================

def get_backtest_missing_codes(all_codes_set):
    """
    short_term_results, screener_results 폴더에서
    백테스트 대상 종목 추출 후 전체 수집 목록에서 누락된 종목 반환
    """
    symbols = set()
    for folder in [SHORT_FOLDER, MID_FOLDER]:
        if not os.path.exists(folder):
            continue
        for file in os.listdir(folder):
            if not file.endswith('.csv'):
                continue
            try:
                df = pd.read_csv(
                    os.path.join(folder, file),
                    dtype={'symbol': str},
                    usecols=['symbol', 'market']
                )
                kr_df = df[df['market'].isin(['KR', 'KOSPI', 'KOSDAQ'])]
                for sym in kr_df['symbol'].tolist():
                    symbols.add(str(sym).zfill(6))
            except Exception as e:
                print(f"⚠️ {file} 읽기 실패: {e}")

    missing = [s for s in symbols if s not in all_codes_set]
    return missing


def crawl_missing_and_append(missing_codes, per_eps_results, foreign_results, sector_results, df_all):
    """
    누락 종목 크롤링 후 기존 결과 리스트에 추가
    종목명은 tickers_meta.json에서 조회
    """
    if not missing_codes:
        print("✅ 누락 종목 없음 (추가 크롤링 스킵)")
        return

    print(f"\n" + "="*60)
    print(f"🔍 백테스트 누락 종목 추가 크롤링: {len(missing_codes)}개")
    print("="*60)

    # tickers_meta.json에서 종목명 로드 (KOSPI + KOSDAQ 합쳐서)
    meta_file = os.path.join(data_dir, 'meta', 'tickers_meta.json')
    meta_all = {}
    if os.path.exists(meta_file):
        try:
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta = json.load(f)
            meta_all.update(meta.get('KOSPI', {}))
            meta_all.update(meta.get('KOSDAQ', {}))
            # 기존 KR 키도 호환
            meta_all.update(meta.get('KR', {}))
            print(f"📂 tickers_meta.json 로드 완료 (전체: {len(meta_all)}개)")
        except Exception as e:
            print(f"⚠️ tickers_meta.json 로드 실패: {e}")
    else:
        print("⚠️ tickers_meta.json 없음 → 종목명 N/A로 처리")

    def crawl_one_missing(code):
        data = crawl_naver_stock_data(code)
        time.sleep(0.2)
        return code, data

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(crawl_one_missing, code): code for code in missing_codes}

        for future in tqdm(as_completed(futures), total=len(futures), desc="누락 종목 크롤링"):
            try:
                code, data = future.result()
            except Exception as e:
                print(f"❌ 에러: {e}")
                continue

            name = meta_all.get(code, {}).get('name', 'N/A')

            # ETF 여부 확인 후 업종 덮어쓰기
            sector_val = data['sector']
            if is_etf(name):
                sector_val = 'ETF'

            per_eps_results.append({
                '티커': code,
                '종목명': name,
                'PER': data['per'] if data['per'] is not None else '-',
                'EPS': data['eps'] if data['eps'] is not None else '-',
                'PBR': data['pbr'] if data['pbr'] is not None else '-',
                '외국인보유율': data['foreign_ownership'] if data['foreign_ownership'] is not None else '-',
                '날짜': today.strftime('%Y%m%d')
            })

            sector_results.append({
                '회사명': name,
                '종목코드': code,
                '업종': sector_val
            })

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

            print(f"   ✅ {code} ({name}) 크롤링 완료 (PER: {data['per']}, 업종: {sector_val})")

    print(f"✅ 누락 종목 추가 크롤링 완료!")


# ============================================
# 메인 실행
# ============================================

print("="*60)
print("📊 네이버 증권 통합 크롤링 시작")
print("수집 항목: PER, EPS, PBR, 업종, 외국인 순매수(5일)")
print("="*60)

# ============================================
# 1. 코스피 + 코스닥 전체 종목 조회
# ============================================
print("\n📋 KRX 전체 종목 리스트 조회 중 (네이버 금융)...")

_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.naver.com/'
}

_kospi_stocks = []
_kosdaq_stocks = []

for _sosok in [0, 1]:
    _market_name = 'KOSPI' if _sosok == 0 else 'KOSDAQ'
    _target_list = _kospi_stocks if _sosok == 0 else _kosdaq_stocks
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
                _target_list.append({
                    'Code': _code,
                    'Name': _name,
                    'Marcap': _cap,
                    'Market': _market_name
                })

        print(f"  {_market_name} {_page}/{_last_page} 페이지 수집 중...")

        if _page >= _last_page:
            break
        _page += 1
        time.sleep(0.3)

    print(f"  ✅ {_market_name} 수집 완료: {len(_target_list)}개")

if not _kospi_stocks and not _kosdaq_stocks:
    print("🚨 KRX 데이터 조회 실패")
    exit()

df_kospi = pd.DataFrame(_kospi_stocks)
df_kospi['Marcap'] = pd.to_numeric(df_kospi['Marcap'], errors='coerce').fillna(0)
df_kospi = df_kospi.drop_duplicates('Code').reset_index(drop=True)

df_kosdaq = pd.DataFrame(_kosdaq_stocks)
df_kosdaq['Marcap'] = pd.to_numeric(df_kosdaq['Marcap'], errors='coerce').fillna(0)
df_kosdaq = df_kosdaq.drop_duplicates('Code').reset_index(drop=True)

# 코스피 + 코스닥 합친 전체 df (정렬/중복 제거용)
df_all = pd.concat([df_kospi, df_kosdaq], ignore_index=True).drop_duplicates('Code').reset_index(drop=True)

# ============================================
# ETF 필터링 (kr_stock_sectors.csv 있으면 적용)
# ============================================
def load_etf_codes():
    """
    kr_stock_sectors.csv 가 있으면 Sector == 'ETF' 인 종목코드 set 반환
    없으면 빈 set 반환 (전체 수집)
    """
    sector_path = os.path.join(data_dir, 'kr_stock_sectors.csv')
    if not os.path.exists(sector_path):
        print("ℹ️ kr_stock_sectors.csv 없음 → ETF 필터 없이 전체 크롤링")
        return set()
    try:
        df = pd.read_csv(sector_path, encoding='utf-8-sig', dtype={'종목코드': str})
        df['종목코드'] = df['종목코드'].str.zfill(6)
        etf_codes = set(df[df['Sector'] == 'ETF']['종목코드'].tolist())
        print(f"ℹ️ kr_stock_sectors.csv 로드 완료 → ETF {len(etf_codes)}개 크롤링 제외 예정")
        return etf_codes
    except Exception as e:
        print(f"⚠️ kr_stock_sectors.csv 로드 실패: {e} → ETF 필터 없이 전체 크롤링")
        return set()

etf_codes = load_etf_codes()

if etf_codes:
    before = len(df_all)
    df_all = df_all[~df_all['Code'].isin(etf_codes)].reset_index(drop=True)
    print(f"ℹ️ ETF 제외: {before}개 → {len(df_all)}개")

# 전체 코드 set (누락 종목 비교용)
all_codes_set = set(df_all['Code'].astype(str).str.zfill(6).tolist())

print(f"\n✅ 전체 종목 조회 완료")
print(f"   KOSPI: {len(df_kospi)}개 / KOSDAQ: {len(df_kosdaq)}개 / 합계(ETF 제외): {len(df_all)}개")

# ============================================
# 2. 크롤링 실행 (멀티스레딩)
# ============================================
print(f"\n🕷️ 네이버 증권 크롤링 시작 (멀티스레딩 x5)")
print(f"⏱️ 전체 {len(df_all)}개 종목 처리 예정")
print()

per_eps_results = []
foreign_results = []
sector_results = []

lock = threading.Lock()

def crawl_one(args):
    idx, row = args
    code = row['Code']
    name = row['Name']
    data = crawl_naver_stock_data(code)
    time.sleep(0.2)
    return idx, code, name, data

rows = list(df_all.iterrows())

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(crawl_one, (idx, row)): idx for idx, row in rows}

    completed_count = 0
    for future in tqdm(as_completed(futures), total=len(futures), desc="크롤링 진행"):
        try:
            idx, code, name, data = future.result()
        except Exception as e:
            print(f"❌ 에러: {e}")
            continue

        # ETF 여부 확인 후 업종 기록
        sector_val = data['sector']
        if is_etf(name):
            sector_val = 'ETF'

        per_eps_results.append({
            '티커': code,
            '종목명': name,
            'PER': data['per'] if data['per'] is not None else '-',
            'EPS': data['eps'] if data['eps'] is not None else '-',
            'PBR': data['pbr'] if data['pbr'] is not None else '-',
            '외국인보유율': data['foreign_ownership'] if data['foreign_ownership'] is not None else '-',
            '날짜': today.strftime('%Y%m%d')
        })

        sector_results.append({
            '회사명': name,
            '종목코드': code,
            '업종': sector_val
        })

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
        if completed_count % 200 == 0:
            per_success = sum(1 for r in per_eps_results if r['PER'] != '-')
            sector_success = sum(1 for r in sector_results if r['업종'] not in ('N/A', 'ETF'))
            etf_count = sum(1 for r in sector_results if r['업종'] == 'ETF')
            print(f"\n📊 진행: {completed_count}/{len(df_all)} | PER: {per_success}개 | 업종: {sector_success}개 | ETF: {etf_count}개")

# ============================================
# ✅ 백테스트 누락 종목 추가 크롤링
# ============================================
missing_codes = get_backtest_missing_codes(all_codes_set)
crawl_missing_and_append(missing_codes, per_eps_results, foreign_results, sector_results, df_all)

# ============================================
# 3. 결과 저장
# ============================================

print("\n" + "="*60)
print("💾 파일 저장 중...")
print("="*60)

# PER/EPS 저장 (파일명 변경)
df_per_eps = pd.DataFrame(per_eps_results)
per_eps_path = os.path.join(data_dir, 'per_eps_all.csv')
df_per_eps.to_csv(per_eps_path, encoding='utf-8-sig', index=False)
per_success = len(df_per_eps[df_per_eps['PER'] != '-'])
print(f"✅ PER/EPS: {per_eps_path}")
print(f"   성공: {per_success}/{len(df_per_eps)} ({per_success/len(df_per_eps)*100:.1f}%)")

# 외국인/기관 순매수 저장 (파일명 변경, df_all 기준 정렬)
df_trading = pd.DataFrame(foreign_results)
df_trading = df_trading.merge(df_all[['Code', 'Marcap']], left_on='티커', right_on='Code', how='left')
df_trading = df_trading.sort_values(by=['날짜', 'Marcap'], ascending=[False, False])
df_trading = df_trading.drop(columns=['Code', 'Marcap'])
trading_path = os.path.join(data_dir, 'foreign_institutional_net_buy_daily_all.csv')
df_trading.to_csv(trading_path, encoding='utf-8-sig', index=False)
trading_dates = sorted(df_trading['날짜'].unique(), reverse=True)
print(f"✅ 외국인/기관 순매수: {trading_path}")
print(f"   수집 날짜: {trading_dates}")

# 섹터 저장 (누적 방식)
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
sector_success = len(df_sector[~df_sector['업종'].isin(['N/A', 'ETF'])])
etf_count = len(df_sector[df_sector['업종'] == 'ETF'])
print(f"✅ 섹터: {sector_path}")
print(f"   업종 매핑 성공: {sector_success}/{len(df_sector)} ({sector_success/len(df_sector)*100:.1f}%)")
print(f"   ETF: {etf_count}개")

# ============================================
# 4. 섹터 ETF 트렌드 수집 (KR만)
# ============================================
print("\n📈 섹터 ETF 트렌드 수집 중 (KR)...")

sector_etfs = {
    'Information Technology': {'KR': '139260', 'kr_name': 'TIGER 200 IT'},
    'Consumer Discretionary': {'KR': '139290', 'kr_name': 'TIGER 200 경기소비재'},
    'Communication Services': {'KR': '228810', 'kr_name': 'TIGER 미디어컨텐츠'},
    'Health Care':            {'KR': '143860', 'kr_name': 'TIGER 헬스케어'},
    'Consumer Staples':       {'KR': '266410', 'kr_name': 'KODEX 필수소비재'},
    'Financials':             {'KR': '139270', 'kr_name': 'TIGER 200 금융'},
    'Energy':                 {'KR': '117680', 'kr_name': 'KODEX 에너지화학'},
    'Industrials':            {'KR': '117700', 'kr_name': 'KODEX 산업재'},
    'Materials':              {'KR': '117690', 'kr_name': 'KODEX 소재산업'},
    'Utilities':              {'KR': '404650', 'kr_name': 'TIGER KRX 기후변화솔루션'},
    'Real Estate':            {'KR': '329200', 'kr_name': 'TIGER 리츠부동산인프라'}
}

def get_kr_etf_trend(code, name):
    """KR ETF 1개월 수익률 크롤링 (네이버)"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')

        text = soup.get_text()
        match = re.search(r'1개월\s*수익률\s*([+\-]?[\d,.]+)%', text)
        if match:
            rate = float(match.group(1).replace(',', ''))
            trend = '상승' if rate > 0 else '하락'
            return f"{trend}({rate:+.2f}%) {name}"
    except:
        pass
    return None

sector_trends = []
for sector, etfs in sector_etfs.items():
    print(f"  {sector} 수집 중...")

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
print(f"   수집: {len(df_sector_trends)}개 (KR)")

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

print("\n[섹터 샘플 5개 (ETF 제외)]")
print(df_sector[~df_sector['업종'].isin(['N/A', 'ETF'])].head(5)[['회사명', '업종']].to_string(index=False))

print("\n[ETF 샘플 3개]")
print(df_sector[df_sector['업종'] == 'ETF'].head(3)[['회사명', '종목코드']].to_string(index=False))

print("\n[섹터 ETF 트렌드 샘플]")
print(df_sector_trends.head(6).to_string(index=False))

print("\n" + "="*60)
print("✅ 모든 크롤링 완료!")
print("="*60)
print(f"\n📊 최종 결과:")
print(f"   KOSPI: {len(df_kospi)}개 / KOSDAQ: {len(df_kosdaq)}개 / 합계: {len(df_all)}개")
print(f"   ETF 종목: {etf_count}개")
print(f"   업종 매핑 성공: {sector_success}개")
print("\n⚠️ 주의사항:")
print("1. kr_stock_sectors.csv는 '업종'만 포함 (Sector, sector_trend는 별도 추가 필요)")
print("2. 외국인보유율은 외국인+기관 보유율을 포함합니다")
print("3. 섹터 ETF 트렌드는 네이버 증권 1개월 수익률 기준입니다 (KR만)")