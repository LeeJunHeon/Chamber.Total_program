@echo off
rem 챔버12(CH1&2) v2 sim — v1 이 켜진 장비 PC 에서 함께 띄워도 운영에 영향이 없게(sim 격리: v2/ch12/sim_guard.py).
rem   · 모든 출력은 C:\CH12_v2\sim_out\ 아래, 설정은 C:\CH12_v2\sim_config(RUN_MODE=sim 아니면 시작 거부), 127.0.0.1 만(로컬 화면 포트 LOCAL_HMI_PORT).
rem   · NAS·UNC 경로·v1 기록 폴더(C:\VanaM_Logs)·로봇 서버 50070·OES 뮤텍스 Local\VanaM_OES_USB* 는 쓰지 않는다.
rem   · 감시 프로그램 없이(자동 재시작 없음) 제어 프로그램만. 시험용 로봇 서버는 run_ch12_sim_robot_server.cmd(127.0.0.1:50170).
rem   · 파이썬: C:\CH12_v2\venv 가 있으면 그것(pyserial·openpyxl·lxml·numpy — v2\requirements.txt), 없으면 py -3.13 → py -3.11 → PATH 의 python.
rem   · 우선순위: 프로그램이 스스로 BelowNormal 로 낮춘다(자식 워커도 물려받음 — v1 운영보다 뒤, D429). start /belownormal /b 는 Ctrl+C 를 꺼서 쓰지 않는다.
rem   · 버전: .git 이 없으면(꾸러미 zip 을 푼 폴더) 맨 위 VERSION 의 '브랜치@커밋'(python -m v2.tools.pack 으로 만든 것).
rem   · 창: 이 배치 파일은 파이썬을 고른 뒤 새 창(cmd /k)에 프로그램을 띄우고 끝난다 — Ctrl+C 로 끝낼 때 '일괄 작업을 끝내시겠습니까
rem     (Y/N)?' 가 뜨지 않는다(배치 작업이 아니므로). 프로그램이 끝나면 마지막 줄 '정상 종료. 이 창을 닫아도 됩니다' 와 종료 코드가 보이고
rem     창은 남는다(D431). venv 선택·우선순위(프로그램이 BelowNormal)·UTF-8(chcp 65001·-X utf8)은 그대로.
chcp 65001 >nul
title 챔버12 v2 SIM — v1 과 별개(닫지 마세요)
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
start "챔버12 v2 SIM - v1 과 별개(닫지 마세요)" cmd /v:on /k "chcp 65001 >nul & %PY% -X utf8 -m v2.ch12 --sim %* & echo 종료 코드 !errorlevel!"
