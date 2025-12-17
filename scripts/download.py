import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
import pandas as pd
import json

# 경로 설정
per_eps_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\per_eps_top_1000.csv'
foreign_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\foreign_net_buy_daily_top_1000.csv'
json_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\meta\tickers_meta.json'

# 1. CSV 로드
df_per_eps = pd.read_csv(per_eps_path)
df_foreign = pd.read_csv(foreign_path)

# 외국인 데이터 처리: 종목별 최근 3일 순매수거래량 리스트 (최근 날짜부터, 없으면 0 패딩)
df_foreign['날짜'] = pd.to_datetime(df_foreign['날짜'], format='%Y%m%d')
df_foreign = df_foreign.sort_values(by=['종목명', '날짜'], ascending=[True, False])
grouped = df_foreign.groupby('종목명')['순매수거래량'].apply(list)
foreign_dict = {name: vals[:3] + [0] * (3 - len(vals[:3])) for name, vals in grouped.items()}

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

# US 섹션: foreign_net_buy = [0,0,0] 설정
for code, info in meta.get("US", {}).items():
    info['foreign_net_buy'] = [0, 0, 0]

# 기존 JSON 파일에 덮어쓰기
with open(json_path, 'w', encoding='utf-8') as f:
    json.dump(meta, f, ensure_ascii=False, indent=4)

print(f"성공! 총 {updated_count}개 KR 종목의 eps/per 최신화 완료")
print("KR: foreign_net_buy 3일치 리스트 추가 (순매수거래량, 최근부터)")
print("US: foreign_net_buy = [0,0,0] 추가")
print(f"기존 파일에 저장됨 → {json_path}")