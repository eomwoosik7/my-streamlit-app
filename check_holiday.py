import sys
from datetime import date

# 임시 테스트용 - 항상 영업일로 처리
today = date.today()
print(f"테스트 모드: {today} → 배치 실행")
sys.exit(0)