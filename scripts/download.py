import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
import pandas as pd
import json

# 경로 설정
per_eps_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\per_eps_top_1000.csv'
foreign_institutional_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\foreign_institutional_net_buy_daily_top_1000.csv'
sector_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\kr_stock_sectors.csv'
sector_trend_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\sector_etf_trends.csv'
json_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\meta\tickers_meta.json'

# ============================================
# 1. CSV 로드
# ============================================
print("📂 CSV 파일 로딩 중...")

df_per_eps = pd.read_csv(per_eps_path, encoding='utf-8-sig')
df_foreign_inst = pd.read_csv(foreign_institutional_path, encoding='utf-8-sig')
df_sectors = pd.read_csv(sector_path, encoding='utf-8-sig')

print(f"  PER/EPS: {len(df_per_eps)}개")
print(f"  외국인/기관: {len(df_foreign_inst)}개")
print(f"  섹터: {len(df_sectors)}개")

# ============================================
# 2. 섹터 트렌드 CSV 로드
# ============================================
try:
    df_sector_trend = pd.read_csv(sector_trend_path, encoding='utf-8-sig')
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

# ============================================
# 3. 데이터 딕셔너리 생성
# ============================================
print("\n📊 데이터 처리 중...")

# 3-1. PER/EPS/기관외국인보유율 딕셔너리
per_eps_dict = {}
for _, row in df_per_eps.iterrows():
    name = str(row['종목명']).strip()
    per_eps_dict[name] = {
        'per': row['PER'] if pd.notna(row['PER']) and str(row['PER']).strip() not in ['-', 'N/A', ''] else None,
        'eps': row['EPS'] if pd.notna(row['EPS']) and str(row['EPS']).strip() not in ['-', 'N/A', ''] else None,
        'ownership_foreign_institution': row['외국인보유율'] if pd.notna(row['외국인보유율']) and str(row['외국인보유율']).strip() not in ['-', 'N/A', ''] else None
    }

# 3-2. 섹터 딕셔너리 (회사명 -> Sector)
sector_dict = {}
for _, row in df_sectors.iterrows():
    name = str(row['회사명']).strip()
    sector = str(row['Sector']).strip() if pd.notna(row['Sector']) else 'N/A'
    sector_dict[name] = sector

# 3-3. 외국인/기관 순매수 딕셔너리 (종목별 최근 5일)
df_foreign_inst['날짜'] = pd.to_datetime(df_foreign_inst['날짜'], format='%Y%m%d')
df_foreign_inst = df_foreign_inst.sort_values(by=['종목명', '날짜'], ascending=[True, False])

foreign_inst_dict = {}
for name, group in df_foreign_inst.groupby('종목명'):
    name = str(name).strip()
    foreign_list = group['외국인순매수'].tolist()[:5]
    inst_list = group['기관순매수'].tolist()[:5]

    # 5개 미만이면 0으로 패딩
    foreign_list += [0] * (5 - len(foreign_list))
    inst_list += [0] * (5 - len(inst_list))

    foreign_inst_dict[name] = {
        'foreign_net_buy': foreign_list,
        'institutional_net_buy': inst_list
    }

# ============================================
# 4. JSON 로드 및 업데이트
# ============================================
print("\n📝 메타 데이터 업데이트 중...")

with open(json_path, 'r', encoding='utf-8') as f:
    meta = json.load(f)

kr_updated = 0

# ============================================
# 4-1. KR 종목 업데이트
# ============================================
for code, info in meta.get("KR", {}).items():
    meta_name = info.get("name", "").strip()

    # PER/EPS/기관외국인보유율 업데이트
    if meta_name in per_eps_dict:
        data = per_eps_dict[meta_name]
        if data['per'] is not None:
            try:
                info['per'] = float(data['per'])
            except:
                pass
        if data['eps'] is not None:
            try:
                info['eps'] = float(data['eps'])
            except:
                pass
        if data['ownership_foreign_institution'] is not None:
            try:
                info['ownership_foreign_institution'] = float(data['ownership_foreign_institution'])
            except:
                pass
        kr_updated += 1

    # 섹터 업데이트
    if meta_name in sector_dict:
        info['sector'] = sector_dict[meta_name]

    # 외국인/기관 순매수 업데이트
    if meta_name in foreign_inst_dict:
        data = foreign_inst_dict[meta_name]
        info['foreign_net_buy'] = data['foreign_net_buy']
        info['institutional_net_buy'] = data['institutional_net_buy']
    else:
        info['foreign_net_buy'] = [0, 0, 0, 0, 0]
        info['institutional_net_buy'] = [0, 0, 0, 0, 0]

    # 섹터 트렌드 추가
    sector_val = info.get('sector', 'N/A')
    if sector_val != 'N/A' and (sector_val, 'KR') in sector_trend_dict:
        info['sector_trend'] = sector_trend_dict[(sector_val, 'KR')]
    else:
        info['sector_trend'] = 'N/A'

# ============================================
# 5. JSON 저장
# ============================================
with open(json_path, 'w', encoding='utf-8') as f:
    json.dump(meta, f, ensure_ascii=False, indent=4)

# ============================================
# 6. 결과 출력
# ============================================
print("\n" + "="*60)
print("✅ 메타 데이터 업데이트 완료!")
print("="*60)
print(f"\n📌 KR 종목 ({kr_updated}개 업데이트)")
print("  - PER, EPS, 기관+외국인 보유율")
print("  - Sector")
print("  - 외국인 순매수 (5일치)")
print("  - 기관 순매수 (5일치)")
print("  - Sector 트렌드")
print(f"\n💾 저장 위치: {json_path}")