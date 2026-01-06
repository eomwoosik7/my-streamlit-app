import schedule
import time
import subprocess
from datetime import datetime
import os
import sys

def run_batch(use_kr=True, use_us=True):
    print(f"배치 시작: {datetime.now()}")
    
    SCRIPT_DIR = os.getenv('SCRIPT_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts'))
    
    # 1. 기본 데이터 수집
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "fetch_data.py")])
    
    # 2. 네이버 크롤링 (통합) - 4개 CSV 생성
    print("\n📊 네이버 크롤링 시작...")
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "naver_crawler_integrated.py")])
    
    # 3. 섹터 데이터 처리 (리츠/인프라 업종 매핑 + Sector 분류)
    print("\n🏢 섹터 데이터 처리 시작...")
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "process_kr_sectors.py")])
    
    # 4. CSV → JSON 메타 통합
    print("\n💾 메타 데이터 통합 시작...")
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "download.py")])
    
    # 5. 기술적 지표 계산
    print("\n📈 기술적 지표 계산 시작...")
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "compute_indicators.py")])
    time.sleep(1)
    
    # 6. 스크리너 실행
    print("\n🔍 스크리너 실행 시작...")
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "screener.py"), str(use_us), str(use_kr)])
    time.sleep(1)
    
    # 7. 로그 저장
    LOG_DIR = os.getenv('LOG_DIR', './logs')
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(os.path.join(LOG_DIR, 'batch_time.txt'), "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    print(f"\n✅ 배치 완료! {datetime.now()}")

if __name__ == "__main__":
    use_kr = sys.argv[1] == 'True' if len(sys.argv) > 1 else True
    use_us = sys.argv[2] == 'True' if len(sys.argv) > 2 else True
    run_batch(use_kr, use_us)