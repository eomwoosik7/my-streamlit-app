import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
import pandas as pd
import json

# 경로 설정
per_eps_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\per_eps_top_1000.csv'
foreign_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\foreign_net_buy_daily_top_1000.csv'
sector_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\kr_stock_sectors.csv'
sector_trend_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\sector_etf_trends.csv'  # ✅ 추가
json_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\meta\tickers_meta.json'

# 1. CSV 로드
df_per_eps = pd.read_csv(per_eps_path)
df_foreign = pd.read_csv(foreign_path)
df_sectors = pd.read_csv(sector_path)

# ✅ 섹터 트렌드 CSV 로드
try:
    df_sector_trend = pd.read_csv(sector_trend_path)
    # {('Information Technology', 'US'): '상승(+2.5%) XLK', ...}
    sector_trend_dict = {}
    for _, row in df_sector_trend.iterrows():
        key = (row['sector'], row['market'])
        sector_trend_dict[key] = row['trend_display']
    print(f"✅ 섹터 트렌드 {len(sector_trend_dict)}개 로드 완료")
except FileNotFoundError:
    print("⚠️ sector_etf_trends.csv 없음 - 섹터 트렌드 없이 진행")
    sector_trend_dict = {}
except Exception as e:
    print(f"⚠️ 섹터 트렌드 로드 실패: {e}")
    sector_trend_dict = {}

# 외국인 데이터 처리: 종목별 최근 3일 순매수거래량 리스트 (최근 날짜부터, 없으면 0 패딩)
df_foreign['날짜'] = pd.to_datetime(df_foreign['날짜'], format='%Y%m%d')
df_foreign = df_foreign.sort_values(by=['종목명', '날짜'], ascending=[True, False])
grouped = df_foreign.groupby('종목명')['순매수거래량'].apply(list)
foreign_dict = {name: vals[:3] + [0] * (3 - len(vals[:3])) for name, vals in grouped.items()}

# 섹터 데이터 처리: 회사명 -> Sector 딕셔너리
sector_dict = dict(zip(df_sectors['회사명'].str.strip(), df_sectors['Sector']))

# 2. JSON 로드
with open(json_path, 'r', encoding='utf-8') as f:
    meta = json.load(f)

updated_count = 0

# KR 섹션 업데이트
for code, info in meta.get("KR", {}).items():
    meta_name = info.get("name", "").strip()
    
    # PER/EPS 업데이트 (B열 종목명 비교)
    matching = df_per_eps[df_per_eps['종목명'].str.strip() == meta_name]
    if not matching.empty:
        row = matching.iloc[0]
        per_str = str(row['PER']).strip() if pd.notna(row['PER']) else ''
        eps_str = str(row['EPS']).strip() if pd.notna(row['EPS']) else ''
        
        if per_str not in ['-', 'N/A', '']:
            try:
                info['per'] = float(per_str)
            except:
                pass
        
        if eps_str not in ['-', 'N/A', '']:
            try:
                info['eps'] = float(eps_str)
            except:
                pass
        
        updated_count += 1
    
    # 외국인 순매수거래량 추가 (3일치 리스트, 없으면 [0,0,0])
    info['foreign_net_buy'] = foreign_dict.get(meta_name, [0, 0, 0])
    
    # 섹터 업데이트 (회사명 매칭, 없으면 기존 "N/A" 유지)
    if meta_name in sector_dict:
        info['sector'] = sector_dict[meta_name]
    
    # ✅ 섹터 트렌드 추가
    sector_val = info.get('sector', 'N/A')
    if sector_val != 'N/A' and (sector_val, 'KR') in sector_trend_dict:
        info['sector_trend'] = sector_trend_dict[(sector_val, 'KR')]
    else:
        info['sector_trend'] = 'N/A'

# US 섹션: foreign_net_buy = [0,0,0] 설정 + 섹터 트렌드 추가
for code, info in meta.get("US", {}).items():
    info['foreign_net_buy'] = [0, 0, 0]
    
    # ✅ 섹터 트렌드 추가
    sector_val = info.get('sector', 'N/A')
    if sector_val != 'N/A' and (sector_val, 'US') in sector_trend_dict:
        info['sector_trend'] = sector_trend_dict[(sector_val, 'US')]
    else:
        info['sector_trend'] = 'N/A'

# 기존 JSON 파일에 덮어쓰기
with open(json_path, 'w', encoding='utf-8') as f:
    json.dump(meta, f, ensure_ascii=False, indent=4)

print(f"성공! 총 {updated_count}개 KR 종목의 eps/per 최신화 완료")
print("KR: foreign_net_buy 3일치 리스트 추가 (순매수거래량, 최근부터)")
print("KR: sector 업데이트 (매칭되는 경우)")
print("✅ KR: sector_trend 추가 (섹터별 ETF 트렌드)")
print("US: foreign_net_buy = [0,0,0] 추가")
print("✅ US: sector_trend 추가 (섹터별 ETF 트렌드)")
print(f"기존 파일에 저장됨 → {json_path}")