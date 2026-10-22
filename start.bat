@echo off
title TG Automation Bot
color 0A

echo.
echo  ============================================
echo   TG Automation Bot - Desktop Launcher
echo  ============================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH.
    echo Please install Python 3.10+ from https://python.org
    pause
    exit /b 1
)

echo [1/3] Python found.

:: Install dependencies
echo [2/3] Installing/checking requirements...
pip install -r requirements.txt -q --disable-pip-version-check

echo [3/3] Starting server...
echo.
echo  Dashboard: http://localhost:5000
echo  Login    : admin / admin123
echo.
echo  Press Ctrl+C to stop the server.
echo.

:: Open browser after 3 seconds
start "" timeout /t 3 >nul && start http://localhost:5000

:: Start Flask
python main.py
pause
