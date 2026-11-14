@echo off
set "CLAUDE=%APPDATA%\npm\node_modules\@anthropic-ai\claude-code\bin\claude.exe"
cd /d "%~dp0"
"%CLAUDE%"
