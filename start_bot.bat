@echo off
title musicdude Discord Bot

cd /d "C:\Users\derek\Personal Projects\discord bot"

:restart
".venv\Scripts\python.exe" "bot.py"

echo.
echo musicdude stopped. Restarting in 10 seconds...
timeout /t 10 /nobreak >nul
goto restart
