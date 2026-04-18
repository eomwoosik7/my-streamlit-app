@echo off
title Batch Running...
cd /d C:\Users\ws\Desktop\Python\Project_Hermes5

python check_holiday.py
if %errorlevel% == 1 goto end

python scripts/batch.py

git add .
git commit -m "%date% auto batch"
git push origin main

:end