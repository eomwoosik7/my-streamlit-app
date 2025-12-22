import pandas as pd
import os
from pykrx import stock

# 데이터 저장 디렉토리
data_dir = r"C:\Users\ws\Desktop\Python\Project_Hermes5\data"
os.makedirs(data_dir, exist_ok=True)

# pykrx로 전체 종목 티커와 이름 가져오기 (KOSPI + KOSDAQ + KONEX)
tickers_kospi = stock.get_market_ticker_list(market="KOSPI")
tickers_kosdaq = stock.get_market_ticker_list(market="KOSDAQ")
tickers_konex = stock.get_market_ticker_list(market="KONEX")
tickers = tickers_kospi + tickers_kosdaq + tickers_konex

names = [stock.get_market_ticker_name(t) for t in tickers]
df_pykrx = pd.DataFrame({'종목코드': tickers, '회사명': names})

# KRX 상장법인 목록 다운로드 (업종 포함)
url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
stock_list = pd.read_html(url, header=0, encoding='cp949')[0]
stock_list['종목코드'] = stock_list['종목코드'].astype(str).str.zfill(6)
df_krx = stock_list[['회사명', '종목코드', '업종', '상장일']]

# 회사명 정규화 (우선주 suffix 제거)
def normalize_name(name):
    suffixes = ['우', '우B', '우C', '2우B', '3우B', '전환', '2우', '3우']
    for suffix in suffixes:
        if suffix in name:
            name = name.replace(suffix, '').strip()
    return name

df_pykrx['normalized_name'] = df_pykrx['회사명'].apply(normalize_name)
df_krx['normalized_name'] = df_krx['회사명'].apply(normalize_name)

# 매칭 (normalized_name 기준, 업종/상장일 가져옴)
df_merged = pd.merge(df_pykrx, df_krx[['normalized_name', '업종', '상장일']], on='normalized_name', how='left')
df_merged = df_merged[['회사명', '종목코드', '업종', '상장일']]

# 11개 섹터 정의 및 매핑 함수 (원래 코드 그대로)
sectors = {
    'Information Technology': '정보기술',
    'Consumer Discretionary': '임의소비재',
    'Communication Services': '통신서비스',
    'Health Care': '헬스케어',
    'Consumer Staples': '필수소비재',
    'Financials': '금융',
    'Energy': '에너지',
    'Industrials': '산업재',
    'Materials': '소재', 
    'Utilities': '유틸리티',
    'Real Estate': '부동산'
}

def map_to_sector(upjong):
    upjong_lower = upjong.lower() if isinstance(upjong, str) else ''
    if any(word in upjong_lower for word in ['소프트웨어', '컴퓨터', '반도체', '전자부품', '통신 및 방송 장비', '영상 및 음향기기', '사진장비 및 광학기기', '전자', '컴퓨터 프로그래밍', '시스템 통합', '자료처리', '호스팅', '포털', '인터넷', '기타 정보 서비스', '마그네틱', '광학 매체', '측정, 시험, 항해, 제어 및 기타 정밀기기', '컴퓨터 및 주변장치', '통신 및 방송 장비', '측정', '시험', '항해', '제어 및 기타 정밀기기 제조업; 광학기기 제외']):
        return 'Information Technology'
    elif any(word in upjong_lower for word in ['봉제의복', '의복', '신발', '가죽', '가구', '자동차용 엔진 및 자동차', '자동차 신품 부품', '자동차 차체나 트레일러', '자동차 부품 및 내장품', '스포츠 서비스', '유원지', '오락관련 서비스', '여행사', '창작 및 예술관련', '영화, 비디오물, 방송프로그램', '오디오물 출판', '영상·오디오물 제공', '가정용 기기', '의복 액세서리', '자동차 판매', '운동 및 경기용구', '섬유, 의복, 신발 및 가죽제품 소매', '악기', '귀금속 및 장신용품', '편조의복', '자동차 재제조 부품', '자동차 부품 및 내장품 판매', '신발 및 신발 부분품', '가죽, 가방 및 유사제품', '그외 기타 제품', '개인 및 가정용품', '무점포 소매', '기타 상품 전문 소매', '섬유제품', '그외 기타 개인 서비스', '기타 생활용품 소매', '음·식료품 및 담배 소매', '떡, 빵 및 과자류', '종합 소매업', '가전제품 및 정보통신장비 소매', '일반 및 생활 숙박시설 운영업', '음식점업', '기타 상품 전문 소매업', '섬유, 의복, 신발 및 가죽제품 소매업', '종합 소매업', '도매업', '상품']):
        return 'Consumer Discretionary'
    elif any(word in upjong_lower for word in ['전기 통신업', '텔레비전 방송업', '자료처리, 호스팅, 포털', '기타 정보 서비스업', '기타 정보 서비스', '광고업', '영상·오디오물 제공 서비스', '오디오물 출판 및 원판 녹음', '서적, 잡지 및 기타 인쇄물 출판', '텔레비전 방송', '기록매체 복제', '영화, 비디오물, 방송프로그램', '창작 및 예술관련 서비스', '전문디자인', '시장조사 및 여론조사업', '서적', '잡지 및 기타 인쇄물 출판업']):
        return 'Communication Services'
    elif any(word in upjong_lower for word in ['의약품', '의료용품', '의료용 기기', '기초 의약물질', '자연과학 및 공학 연구개발업', '기초 의약물질 제조업', '그외 기타 전문, 과학 및 기술 서비스', '기타 과학기술 서비스', '의료용 기기', '기초 의약물질 제조업']):
        return 'Health Care'
    elif any(word in upjong_lower for word in ['기타 식품', '곡물가공품', '전분', '동·식물성 유지 및 낙농제품', '과실, 채소 가공', '도축, 육류 가공', '수산물 가공', '알코올음료', '비알코올음료', '비료, 농약', '동물용 사료', '떡, 빵 및 과자', '도시락 및 식사용 조리식품', '음식점', '담배', '음·식료품 및 담배 도매', '기타 식품 제조', '동물용 사료 및 조제식품', '수산물 가공 및 저장 처리', '과실, 채소 가공 및 저장 처리', '곡물가공품, 전분 및 전분제품', '도축, 육류 가공 및 저장 처리', '작물 재배', '어로 어업', '기타 식품 제조업', '떡', '빵 및 과자류 제조업']):
        return 'Consumer Staples'
    elif any(word in upjong_lower for word in ['기타 금융업', '금융 지원 서비스업', '은행 및 저축기관', '보험업', '신탁업 및 집합투자업', '회사 본부 및 경영 컨설팅 서비스업', '재 보험', '보험 및 연금관련 서비스', '기타 금융업']):
        return 'Financials'
    elif any(word in upjong_lower for word in ['기초 화학물질', '석유 정제품', '연료용 가스', '연료 소매', '연료용 가스 제조 및 배관공급업']):
        return 'Energy'
    elif any(word in upjong_lower for word in ['건물 건설업', '토목 건설업', '실내건축 및 건축마무리 공사업', '전기 및 통신 공사업', '특수 목적용 기계', '일반 목적용 기계', '구조용 금속제품', '전동기, 발전기 및 전기 변환', '항공기,우주선', '선박 및 보트 건조업', '철도장비', '그외 기타 운송장비', '도로 화물 운송업', '해상 운송업', '기타 운송관련 서비스업', '기반조성 및 시설물 축조관련 전문공사업', '건물설비 설치 공사업', '육상 여객 운송업', '항공 여객 운송업', '운송장비 임대업', '경비, 경호 및 탐정업', '사업시설 유지·관리 서비스업', '산업용 기계 및 장비 임대업', '증기, 냉·온수 및 공기조절 공급업', '폐기물 처리', '개인 및 가정용품 수리업', '전기 및 통신 공사업', '건축기술, 엔지니어링 및 관련 기술 서비스업', '기타 전문 서비스업', '기타 사업지원 서비스업', '교육지원 서비스업', '일반 교습 학원', '기타 교육기관', '초등 교육기관', '건축자재, 철물 및 난방장치 도매업', '기타 전문 도매업', '기계장비 및 관련 물품 도매업', '기타 운송관련 서비스업', '산업용 농·축산물 및 동·식물 도매업', '경비', '경호 및 탐정업', '기타 사업지원 서비스업', '교육지원 서비스업', '일차전지', '이차전지', '재생업']):
        return 'Industrials'
    elif any(word in upjong_lower for word in ['1차 철강', '1차 비철금속', '기타 비금속 광물제품', '유리 및 유리제품', '시멘트, 석회, 플라스터', '내화, 비내화 요업제품', '기타 화학제품', '합성고무 및 플라스틱 물질', '화학섬유', '플라스틱제품', '고무제품', '기타 금속 가공제품', '금속 주조', '기타 종이 및 판지 제품', '골판지, 종이 상자 및 종이용기', '펄프, 종이 및 판지', '나무제품', '제재 및 목재 가공업', '무기 및 총포탄', '섬유제품 염색, 정리 및 마무리 가공업', '방적 및 가공사 제조업', '편조원단 제조업', '직물직조 및 직물제품 제조업', '인쇄 및 인쇄관련 산업', '시멘트', '석회', '플라스터 및 그 제품 제조업', '내화', '비내화 요업제품 제조업', '골판지', '종이 상자 및 종이용기 제조업', '펄프', '종이 및 판지 제조업', '제재 및 목재 가공업', '기타 비금속 광물제품 제조업']):
        return 'Materials'
    elif any(word in upjong_lower for word in ['전기업', '기타 전기장비 제조업', '절연선 및 케이블 제조업', '전구 및 조명장치 제조업', '전동기, 발전기 및 전기 변환 · 공급 · 제어 장치 제조업']):
        return 'Utilities'
    elif any(word in upjong_lower for word in ['부동산 임대 및 공급업', '비거주 복지시설 운영업', '일반 및 생활 숙박시설 운영업', '부동산 관련 서비스업']):
        return 'Real Estate'
    else:
        return 'Other'

# 섹터 추가
df_merged['Sector'] = df_merged['업종'].apply(map_to_sector)

# 정렬 (회사명 기준)
df_merged = df_merged.sort_values(by='회사명')

# CSV 저장
file_path = os.path.join(data_dir, 'kr_stock_sectors.csv')
df_merged.to_csv(file_path, encoding='utf-8-sig', index=False)
print(f"CSV 파일 저장 완료 (섹터 추가): {file_path}")