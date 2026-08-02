@echo off
title FinDashboard web - public tunnel (close this window to stop)
set PORT=8599
set FINDASH_DATA=%LOCALAPPDATA%\FinDashboardWeb-data
start "FinDashboard server" /min "%LOCALAPPDATA%\findash-venv\Scripts\python.exe" "%~dp0app.py"
echo Waiting for the server to start...
timeout /t 8 >nul
echo.
echo Your public URL appears below (the https://....trycloudflare.com line).
echo It works from any device while this window stays open. Local: http://localhost:8599
echo NOTE: needs a non-corporate network (hotspot/home) - the office network blocks tunnels.
echo.
"%LOCALAPPDATA%\FinDashboard\cloudflared.exe" tunnel --url http://localhost:8599
pause
