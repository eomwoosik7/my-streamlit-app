@echo off
title 주식배치 실행중...
cd /d C:\Users\ws\Desktop\Python\Project_Hermes5

python check_holiday.py
if %errorlevel% == 1 goto end

python scripts/batch.py

git add .
git commit -m "%date% 자동배치"
git push origin main

:end