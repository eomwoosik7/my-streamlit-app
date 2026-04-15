import pandas as pd
import os

DATA_DIR = os.getenv('DATA_DIR', './data')
data_dir = DATA_DIR

# 파일 경로
file_path = os.path.join(data_dir, 'kr_stock_sectors.csv')

# CSV 로드
df = pd.read_csv(file_path, encoding='utf-8-sig')

# NaN을 'N/A' 문자열로 변환
df['업종'] = df['업종'].fillna('N/A')

print(f"총 {len(df)}개 종목")
print(f"업종 N/A: {len(df[df['업종'] == 'N/A'])}개")
print(f"업종 ETF: {len(df[df['업종'] == 'ETF'])}개")

# ============================================
# 1. N/A 종목 중 리츠/인프라 처리
#    (ETF는 건드리지 않음)
# ============================================
print("\n[리츠/인프라 종목 처리]")
rits_infra = df[df['회사명'].str.contains('리츠|인프라', na=False)]
print(f"리츠/인프라 종목: {len(rits_infra)}개")
for idx, row in rits_infra.iterrows():
    print(f"  {row['회사명']:30s} 업종: {row['업종']}")

def classify_na_sector(row):
    """N/A 종목의 업종 분류 (ETF는 그대로 유지)"""
    # ETF는 건드리지 않음
    if row['업종'] == 'ETF':
        return 'ETF'

    if row['업종'] != 'N/A':
        return row['업종']

    name = row['회사명']

    # 리츠와 인프라 둘 다 포함 → 마지막 단어 기준
    if '리츠' in name and '인프라' in name:
        if name.endswith('리츠'):
            return '부동산'
        elif name.endswith('인프라'):
            return '금융'

    # 리츠만 포함
    if '리츠' in name:
        return '부동산'

    # 인프라만 포함
    if '인프라' in name:
        return '금융'

    # 맵스리얼티 (부동산)
    if '리얼티' in name or '맵스리얼티' in name:
        return '부동산'

    return 'N/A'

df['업종'] = df.apply(classify_na_sector, axis=1)

print(f"\n처리 후:")
for idx, row in rits_infra.iterrows():
    current_upjong = df.loc[idx, '업종']
    print(f"  {row['회사명']:30s} 업종: {current_upjong}")

print(f"\n처리 후 업종 N/A: {len(df[df['업종'] == 'N/A'])}개")

# ============================================
# 2. 업종 → Sector 매핑
# ============================================

# 11개 섹터 정의
sectors = {
    'Information Technology': [
        '소프트웨어', '컴퓨터', '반도체', '전자부품', '통신장비', '전자장비',
        '사진장비', '광학', 'IT서비스', '시스템통합', '자료처리', '호스팅',
        '포털', '인터넷', '정보서비스', '마그네틱', '광학매체', '측정', '시험',
        '항해', '제어', '정밀기기', '디스플레이장비', '디스플레이패널', '핸드셋',
        '전자제품', '사무용전자제품'
    ],
    'Consumer Discretionary': [
        '봉제', '의복', '신발', '가죽', '가구', '자동차', '스포츠', '유원지',
        '오락', '여행', '창작', '예술', '영화', '비디오', '방송', '오디오',
        '가정용기기', '액세서리', '운동', '경기용구', '섬유', '악기', '귀금속',
        '장신용품', '편조', '가방', '개인용품', '가정용품', '무점포소매',
        '상품전문소매', '생활용품소매', '음식료품소매', '담배소매', '떡',
        '빵', '과자', '종합소매', '가전제품소매', '정보통신장비소매', '숙박',
        '음식점', '호텔', '레스토랑', '레저', '백화점', '일반상점',
        '화장품', '판매업체'
    ],
    'Communication Services': [
        '전기통신', '텔레비전방송', '광고', '영상', '서적', '잡지', '인쇄물',
        '텔레비전', '기록매체복제', '전문디자인', '시장조사', '여론조사',
        '양방향미디어', '게임엔터테인먼트', '다각화된통신', '무선통신'
    ],
    'Health Care': [
        '의약품', '의료용품', '의료용기기', '기초의약물질', '자연과학',
        '공학연구', '과학기술', '제약', '생물공학', '건강관리장비', '건강관리업체',
        '건강관리기술', '생명과학도구', '생명과학'
    ],
    'Consumer Staples': [
        '식품', '곡물', '전분', '동물성', '식물성', '유지', '낙농', '과실',
        '채소가공', '도축', '육류가공', '수산물가공', '알코올음료', '비알코올음료',
        '비료', '농약', '동물용사료', '도시락', '조리식품', '음료', '작물재배',
        '어로', '어업', '담배', '식품과기본식료품소매'
    ],
    'Financials': [
        '금융', '은행', '저축기관', '보험', '신탁', '집합투자', '경영컨설팅',
        '재보험', '연금', '증권', '창업투자', '카드', '기타금융', '손해보험',
        '생명보험', '부동산', '자산신탁'
    ],
    'Energy': [
        '기초화학물질', '석유정제', '연료용가스', '연료소매', '석유', '가스',
        '에너지장비및서비스'
    ],
    'Industrials': [
        '건물건설', '토목건설', '실내건축', '건축마무리', '전기공사', '통신공사',
        '특수목적용기계', '일반목적용기계', '구조용금속', '전동기', '발전기',
        '전기변환', '항공기', '우주선', '선박', '보트건조', '철도장비',
        '운송장비', '도로화물운송', '해상운송', '운송관련', '기반조성',
        '시설물축조', '건물설비', '육상여객운송', '항공여객운송', '운송장비임대',
        '경비', '경호', '탐정', '사업시설유지', '산업용기계', '증기', '냉온수',
        '공기조절', '폐기물처리', '개인용품수리', '가정용품수리', '건축기술',
        '엔지니어링', '전문서비스', '사업지원', '교육지원', '일반교습', '교육기관',
        '초등교육', '건축자재', '철물', '난방장치', '전문도매', '기계장비',
        '산업용농축산물', '동식물', '이차전지', '재생', '조선', '기계', '건설',
        '항공화물운송', '물류', '우주항공', '국방', '상업서비스', '공급품', '교육서비스',
        '운송인프라', '도로', '철도운송', '항공사', '해운사',
        '복합기업', '무역회사와판매업체'
    ],
    'Materials': [
        '철강', '비철금속', '비금속광물', '유리', '유리제품', '시멘트',
        '석회', '플라스터', '내화', '요업', '화학제품', '합성고무',
        '플라스틱물질', '화학섬유', '플라스틱제품', '고무제품', '금속가공',
        '금속주조', '종이', '판지제품', '골판지', '종이상자', '종이용기',
        '펄프', '나무제품', '제재', '목재가공', '무기', '총포탄',
        '섬유제품염색', '정리', '마무리가공', '방적', '가공사', '편조원단',
        '직물직조', '직물제품', '인쇄', '인쇄관련', '화학', '포장재',
        '건축제품'
    ],
    'Utilities': [
        '전기', '전기장비', '절연선', '케이블', '전구', '조명장치',
        '전기유틸리티', '가스유틸리티', '복합유틸리티', '전기제품'
    ],
    'Real Estate': [
        '부동산'
    ]
}

def map_sector(upjong):
    """업종 → Sector 매핑"""
    if pd.isna(upjong) or upjong == 'N/A' or str(upjong).strip() == '':
        return 'N/A'

    # ETF는 그대로 ETF로 유지
    if upjong == 'ETF':
        return 'ETF'

    upjong_str = str(upjong).lower()

    for sector, keywords in sectors.items():
        for keyword in keywords:
            if keyword.lower() in upjong_str:
                return sector

    return 'Other'

df['Sector'] = df['업종'].apply(map_sector)

# ============================================
# 3. 결과 저장 및 통계
# ============================================

# 원본 백업
backup_path = os.path.join(data_dir, 'kr_stock_sectors_backup.csv')
if not os.path.exists(backup_path):
    df_original = pd.read_csv(file_path, encoding='utf-8-sig')
    df_original.to_csv(backup_path, encoding='utf-8-sig', index=False)
    print(f"\n백업 생성: {backup_path}")

# 저장
df.to_csv(file_path, encoding='utf-8-sig', index=False)
print(f"\n✅ 파일 저장 완료: {file_path}")

# 통계
print("\n" + "="*60)
print("📊 Sector 분포")
print("="*60)
sector_counts = df['Sector'].value_counts()
for sector, count in sector_counts.items():
    percentage = (count / len(df)) * 100
    print(f"{sector:30s} {count:4d}개 ({percentage:5.1f}%)")

print(f"\n총 {len(df)}개 종목")
print(f"ETF: {len(df[df['Sector'] == 'ETF'])}개")
print(f"매핑 성공: {len(df[~df['Sector'].isin(['N/A', 'ETF', 'Other'])])}개")
print(f"매핑 실패 (N/A): {len(df[df['Sector'] == 'N/A'])}개")
print(f"기타 (Other): {len(df[df['Sector'] == 'Other'])}개")

# N/A 종목 출력
if len(df[df['Sector'] == 'N/A']) > 0:
    print("\n⚠️ 매핑되지 않은 종목 (N/A):")
    print(df[df['Sector'] == 'N/A'][['회사명', '종목코드', '업종']].to_string(index=False))

# Other 종목 출력 (매핑 개선 필요)
if len(df[df['Sector'] == 'Other']) > 0:
    print("\n⚠️ 'Other'로 분류된 종목:")
    print(df[df['Sector'] == 'Other'][['회사명', '종목코드', '업종']].head(10).to_string(index=False))