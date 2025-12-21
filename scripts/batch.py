import schedule
import time
import subprocess
from datetime import datetime
import os
import sys

def run_batch(use_kr=True, use_us=True):  # top_n 제거
    print(f"배치 시작: {datetime.now()}")
    
    SCRIPT_DIR = os.getenv('SCRIPT_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts'))
    
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "fetch_data.py")])
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "foreign_net_buy.py")])
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "per_eps.py")])
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "download.py")])
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "compute_indicators.py")])
    time.sleep(1)
    
    subprocess.run(["python", os.path.join(SCRIPT_DIR, "screener.py"), str(use_us), str(use_kr)])  # top_n 제거
    time.sleep(1)
    
    # 텔레그램 알림 삭제됨
    
    LOG_DIR = os.getenv('LOG_DIR', './logs')
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(os.path.join(LOG_DIR, 'batch_time.txt'), "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("배치 완료!")

if __name__ == "__main__":
    use_kr = sys.argv[1] == 'True' if len(sys.argv) > 1 else True
    use_us = sys.argv[2] == 'True' if len(sys.argv) > 2 else True
    run_batch(use_kr, use_us)  # top_n 제거