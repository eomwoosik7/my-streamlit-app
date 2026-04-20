import sys
from datetime import date
import holidays

kr_holidays = holidays.KR()
today = date.today()

if today.weekday() >= 5 or today in kr_holidays:
    print(f"Today is a holiday: {today} -> Batch skipped")
    sys.exit(1)
else:
    print(f"Today is a business day: {today} -> Batch running")
    sys.exit(0)