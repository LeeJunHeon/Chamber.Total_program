@echo off
rem 챔버12 v2 시험용 로봇 서버 — 127.0.0.1:50170 만 듣는다(v1 로봇 서버의 0.0.0.0:50070 과 겹치지 않음, sim 격리: v2/ch12/sim_guard.py).
rem   포트를 바꾸려면 --port N(50070 은 거부). 로그·캐시는 C:\CH12_v2\sim_out 아래. 제어 프로그램 sim 이 켜져 있어야 내부 API(127.0.0.1:50071)로 전달된다.
rem   파이썬·우선순위는 run_ch12_sim.cmd 와 같음(C:\CH12_v2\venv 우선, 프로그램이 BelowNormal 로 낮춤 — D429). 포트를 이미 쓰면 [중지] 줄과 종료 코드 1.
rem   · 창: 이 배치 파일은 파이썬을 고른 뒤 새 창(cmd /k)에 프로그램을 띄우고 끝난다 — Ctrl+C 로 끝낼 때 '일괄 작업을 끝내시겠습니까
rem     (Y/N)?' 가 뜨지 않는다(배치 작업이 아니므로). 프로그램이 끝나면 마지막 줄 '정상 종료. 이 창을 닫아도 됩니다' 와 종료 코드가 보이고
rem     창은 남는다(D431). venv 선택·우선순위(프로그램이 BelowNormal)·UTF-8(chcp 65001·-X utf8)은 그대로.
chcp 65001 >nul
title 챔버12 v2 SIM 로봇 서버 — v1 과 별개
cd /d "%~dp0.."
set "PY="
if exist "C:\CH12_v2\venv\Scripts\python.exe" set PY="C:\CH12_v2\venv\Scripts\python.exe"
if not defined PY (
  py -3.13 -c "" >nul 2>&1 && set "PY=py -3.13"
)
if not defined PY (
  py -3.11 -c "" >nul 2>&1 && set "PY=py -3.11"
)
if not defined PY (
  python -c "" >nul 2>&1 && set "PY=python"
)
if not defined PY (
  echo [중지] 파이썬을 찾을 수 없음 — C:\CH12_v2\venv, py -3.13, py -3.11, PATH 의 python 중 하나가 필요합니다
  pause
  exit /b 1
)
echo 파이썬: %PY%
start "챔버12 v2 SIM 로봇 서버 - v1 과 별개" cmd /v:on /k "chcp 65001 >nul & %PY% -X utf8 -m v2.robot_server --sim %* & echo 종료 코드 !errorlevel!"
