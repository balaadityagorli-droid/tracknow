@echo off
title TrackNow PS08 Real-Time Platform
echo ====================================================
echo Starting TrackNow: Redis + 2 Apps + Worker + 2000 Riders
echo ====================================================

REM Kill any stale node processes on 3001, 3002, 6390
taskkill /F /IM node.exe >nul 2>&1
timeout /t 1 /nobreak >nul

start "" "http://localhost:3001"
node dev/stack.js --sim
pause
