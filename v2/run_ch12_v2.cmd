@echo off
rem 챔버12(CH1&2) v2 실행 — 감시 프로그램(v2.supervisor)이 제어 프로그램(v2.ch12)을 켜고 비정상 종료 때 다시 켠다(10분 3회 초과면 멈춤).
rem 작업 폴더는 저장소 루트(이 파일의 상위). 창을 닫거나 Ctrl+C 면 저널에 의도적 종료를 적고 파워 출력만 끈다(결정 4).
rem 파이썬: py -3.11 런처 → 없으면 PATH 의 python. 이 파일에 PC 별 경로를 넣지 않는다. 로봇 서버는 run_ch12_robot_server.cmd 로 따로.
chcp 65001 >nul
title 챔버12 v2 — 닫지 마세요
cd /d "%~dp0.."
set "PY="
py -3.11 -c "" >nul 2>&1 && set "PY=py -3.11"
if not defined PY (
  python -c "" >nul 2>&1 && set "PY=python"
)
if not defined PY (
  echo [중지] 파이썬을 찾을 수 없음 — py -3.11 런처나 PATH 의 python 이 필요합니다
  pause
  exit /b 1
)
echo 파이썬: %PY%
%PY% -X utf8 -m v2.supervisor --log-dir "C:\Ch12_v2\logs" -- --config-dir "C:\Ch12_v2\config" %*
echo 종료 코드 %ERRORLEVEL%
pause
