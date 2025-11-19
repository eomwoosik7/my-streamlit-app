import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
import pandas as pd
import json

# 경로 설정 (기존 파일 그대로 사용)
excel_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\data_4742_20251118.xlsx'
json_path = r'C:\Users\ws\Desktop\Python\Project_Hermes5\data\meta\tickers_meta.json'

# 1. 엑셀 & json 로드
df = pd.read_excel(excel_path)

with open(json_path, 'r', encoding='utf-8') as f:
    meta = json.load(f)

updated_count = 0

for _, row in df.iterrows():
    excel_name = str(row['종목명']).strip()

    # 엑셀에서 값 가져오기 (없으면 None)
    eps_new = None
    if pd.notna(row['EPS']):
        eps_str = str(row['EPS']).strip()
        if eps_str not in ['-', 'N/A', '']:
            try:
                eps_new = float(eps_str)
            except:
                pass

    per_new = None
    if pd.notna(row['PER']):
        per_str = str(row['PER']).strip()
        if per_str not in ['-', 'N/A', '']:
            try:
                per_new = float(per_str)
            except:
                pass

    # KR 섹션에서 종목 찾기
    for code, info in meta.get("KR", {}).items():
        meta_name = info.get("name", "").strip()

        # 정확히 일치하는 경우 (대부분 이걸로 잡힘)
        if meta_name == excel_name:
            if eps_new is not None:
                info['eps'] = eps_new
            if per_new is not None:
                info['per'] = per_new
            updated_count += 1
            break

        # 혹시 "삼성전자 우선주" → "삼성전자우" 같은 미세한 차이 대비
        elif excel_name.replace("우선주", "우") == meta_name or meta_name.replace("우", "우선주") == excel_name:
            if eps_new is not None:
                info['eps'] = eps_new
            if per_new is not None:
                info['per'] = per_new
            updated_count += 1
            break

# 기존 파일에 바로 덮어쓰기 (새 파일 생성 X)
with open(json_path, 'w', encoding='utf-8') as f:
    json.dump(meta, f, ensure_ascii=False, indent=4)

print(f"성공! 총 {updated_count}개 종목의 eps/per 최신화 완료")
print(f"기존 파일에 바로 저장됨 → {json_path}")