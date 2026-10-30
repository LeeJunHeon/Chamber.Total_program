@echo off
rem 챔버12 로봇 서버(별도 프로그램, 0.0.0.0:50070) — 판단하지 않고 제어 프로그램 내부 API(127.0.0.1)로 넘긴다.
chcp 65001 >nul
title 챔버12 로봇 서버 — 닫지 마세요
cd /d "%~dp0.."
set "PY="
py -3.11 -c "" >nul 2>&1 && set "PY=py -3.11"
if not defined PY (
  python -c "" >nul 2>&1 && set "PY=python"
)
if not defined PY (
  echo [중지] 파이썬을 찾을 수 없음
  pause
  exit /b 1
)
%PY% -X utf8 -m v2.robot_server %*
echo 종료 코드 %ERRORLEVEL%
pause
