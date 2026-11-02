@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0Test-SRDT.ps1" %*
exit /b %ERRORLEVEL%
