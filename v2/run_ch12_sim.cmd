@echo off
rem 챔버12(CH1&2) v2 sim — v1 이 켜진 장비 PC 에서 함께 띄워도 운영에 영향이 없게(sim 격리: v2/ch12/sim_guard.py).
rem   · 모든 출력은 C:\CH12_v2\sim_out\ 아래, 설정은 C:\CH12_v2\sim_config(RUN_MODE=sim 아니면 시작 거부), 127.0.0.1 만(로컬 화면 포트 LOCAL_HMI_PORT).
rem   · NAS·UNC 경로·v1 기록 폴더(C:\VanaM_Logs)·로봇 서버 50070·OES 뮤텍스 Local\VanaM_OES_USB* 는 쓰지 않는다.
rem   · 감시 프로그램 없이(자동 재시작 없음) 제어 프로그램만. 시험용 로봇 서버는 run_ch12_sim_robot_server.cmd(127.0.0.1:50170).
chcp 65001 >nul
title 챔버12 v2 SIM — v1 과 별개(닫지 마세요)
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
%PY% -X utf8 -m v2.ch12 --sim %*
echo 종료 코드 %ERRORLEVEL%
pause
