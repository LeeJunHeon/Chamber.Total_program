# apps/rf_service/rf_worker.py
# -*- coding: utf-8 -*-
"""
RF 디스플레이 판독 배치 워커 (장비 노트북 로컬 실행).
원본: rf-display-reader/rf_reader_v3/worker.py

흐름
----
1) CAMERA_LOG_ROOT/{CH1,CH2,CLEANING}/(raw/){YYYYMMDD_HHMMSS}/ 세션 폴더 스캔.
2) 완료된(=최근 STALE_MIN분간 변화 없는) 미처리 세션만 처리.
   - 진행 중일 수 있는 최신 세션은 보류 → 다음 실행 때 처리.
3) 세션 사진 배치 판독(ONNX, TTA+양방향 시계열 융합)
   → RF_READER_CSV_DIR/{mode}/{MODE}_{session}.csv 작성.
4) CSV 정상 작성 확인 후: 판독 성공 사진은 삭제, 애매(needs_review/판독실패) 사진만 보존.
5) 처리 매니페스트 기록(크래시 복구/중복 방지) → RF_READER_STATE_DIR.

torch 불필요. onnxruntime + opencv + numpy 만 필요.

설정(settings.json common 섹션 → lib.config_common; 환경변수/CLI 로 override)
  CAMERA_LOG_ROOT      사진 루트                (env RF_CAMERA_LOGS)
  RF_READER_STATE_DIR  manifest.json / run.log  (env RF_STATE_DIR)
  RF_READER_CSV_DIR    판독 결과 CSV(로컬 정본)  (env RF_CSV_DIR)
  RF_READER_STALE_MIN  이 분 내 수정 세션 보류   (env RF_STALE_MIN)
  RF_READER_NAS_DIR    CSV 사본 전송 대상(NAS)  (env RF_NAS_DIR)
  RF_READER_RETENTION_DAYS    판독 완료 세션 폴더 보관 기간(일)
  RF_READER_MIN_FREE_GB       사진 루트 여유 임계(GB) — 미만이면 완료 세션을 오래된 순으로 정리
  RF_READER_UPLOAD_ALERT_DAYS 미전송 CSV 가 이 일수 이상 밀리면 알림
  RF_SETTINGS          settings.json 경로 직접 지정
  rf_config.json       (exe 옆) {"webhook_url": "...", "enabled": true} — 구글챗 알림
  RF_MODEL             onnx 경로(기본 models/cnn.onnx)
  RF_SINCE             "YYYYMMDD_HHMMSS" 이상 세션만
  RF_TTA               1(기본) TTA 사용 / 0 끔(빠름)
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import shutil
import ssl
import sys
import time
import traceback
import urllib.request
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import cv2
import numpy as np
import onnxruntime as ort

# ✅ 개발(py 실행)일 때만 저장소 루트를 sys.path 에 넣어 lib.* 를 찾게 한다
if not getattr(sys, "frozen", False):
    _ROOT = Path(__file__).resolve().parents[2]
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))

from rf_config_defs import LABELS, CSV_COLS, DIGIT_W, DIGIT_H
from geometry import build_homographies, warp_panels
from extract import load_slots, extract_cell
from preprocess import align_com

# ---- 상수(parse.py와 동일) ----
BLANK = 10                # 11클래스 빈칸 라벨
CONF_DIGIT_MIN = 0.90     # 자릿수 신뢰 임계(시계열 가중치용)
WIN = 3                   # 시계열 윈도우 반경(프레임)
MODE_FRAC_MIN = 0.6       # 윈도우 최빈비율 임계
# 이 신뢰도 이상으로 읽힌 프레임은 평탄화하지 않고 '사진 그대로' 기록.
# (모델이 확신하는 프레임은 디스플레이 실제값이므로, 실제 변동을 다수결로 뭉개지 않음.
#  흐릿/저신뢰 프레임만 이웃 다수결로 보정 → CSV가 사진과 일치 + 노이즈는 제거)
TRUST_CONF = 0.90
# 사진 보존(=애매) 판정용 프레임 신뢰 임계. 한 칸이라도 이보다 낮으면 그 사진 보존.
# needs_review(시계열 불확실)가 아니라 '모델이 그 이미지를 확신 못함'을 기준으로 함:
# 정상적으로 값이 깜빡이는 칸(예 FWD 007↔009)은 각 프레임 conf~1.0이라 삭제 대상.
# 0.50: 일부 셀(RF3_TUNE 등 0 근처)이 만성적으로 conf 0.7대라, 0.90이면 거의 다 보존됨.
# 0.50이면 '정말 애매한(<0.5)' 프레임만 보존(실측 ~13%), 나머지 삭제. 더 줄이려면 0.4, 늘리려면 0.6.
KEEP_CONF_MIN = float(os.environ.get("RF_KEEP_CONF", "0.50"))
# 셀별 임계 override. 만성적으로 흐릿하지만 값이 안정적인 '끝자리 클리핑' 셀은 매 프레임
# 저신뢰로 읽혀 보존을 거의 다 유발하는데(실측 RF3_TUNE이 전체 보존의 ~95% 차지),
# 그 사진들은 전부 비슷해 검토가치가 없음 → 별도 낮은 임계로 '정말 깨진 것'만 보존.
# RF3_TUNE: 카메라 구도상 끝자리 클리핑, 보통 0 근처 안정값, mean conf~0.68.
# 환경변수 RF_KEEP_CONF_<LABEL> (예: RF_KEEP_CONF_RF3_TUNE)로 셀별 조정 가능.
KEEP_CONF_CELL = {
    lbl: float(os.environ[f"RF_KEEP_CONF_{lbl}"])
    for lbl in ("RF3_TUNE", "CH2_LOAD", "CH1_LOAD")
    if f"RF_KEEP_CONF_{lbl}" in os.environ
}
KEEP_CONF_CELL.setdefault("RF3_TUNE", 0.30)
MODES = ["CH1", "CH2", "CLEANING"]
SESSION_RE = re.compile(r"^\d{8}_\d{6}$")


# ===================== 에셋 / 설정 경로 해석 =====================
def _asset_path(rel: str) -> Path:
    """모델·보정 파일 경로.
    frozen: sys._MEIPASS / rel (없으면 exe 폴더 / rel 로 재시도)
    dev   : 이 파일 폴더 / rel"""
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            cand = Path(meipass) / rel
            if cand.exists():
                return cand
        return Path(sys.executable).parent / rel
    return Path(__file__).resolve().parent / rel


def _worker_base_dir() -> Path:
    """exe 옆(번들되지 않는 파일: rf_config.json) 기준 폴더.
    frozen: exe 폴더 / dev: 이 파일 폴더.  (_asset_path 는 번들 대상 전용)"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


# ===================== 구글챗 알림 (oes_api.py 패턴) =====================
_NOTIFY_URL = ""
_NOTIFY_ENABLED = False


def _load_notify_config() -> None:
    """exe 옆 rf_config.json → webhook_url / enabled. 없거나 비어 있으면 알림 생략(에러 아님)."""
    global _NOTIFY_URL, _NOTIFY_ENABLED
    cfg_path = _worker_base_dir() / "rf_config.json"
    try:
        if not cfg_path.exists():
            return
        with open(cfg_path, "r", encoding="utf-8") as fp:
            cfg = json.loads(fp.read())
        _NOTIFY_ENABLED = bool(cfg.get("enabled", True))
        _NOTIFY_URL = str(cfg.get("webhook_url", "") or "").strip()
    except Exception:
        pass


def notify(msg: str) -> None:
    """Google Chat webhook 으로 문제 알림 (blocking, 실패 전부 무시). 정상 완료 알림은 보내지 않는다."""
    if not _NOTIFY_ENABLED or not _NOTIFY_URL:
        return
    try:
        payload = json.dumps({"text": f"[RF-Reader] {msg}"}).encode("utf-8")
        req = urllib.request.Request(_NOTIFY_URL, data=payload,
                                     headers={"Content-Type": "application/json"})
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=5, context=ctx) as resp:
            resp.read()
    except Exception:
        pass


def _free_gb(path) -> float:
    """경로가 속한 디스크의 여유(GB). 조회 실패 시 inf (가드로 워커가 막히지 않도록)."""
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except Exception:
        return float("inf")


def _find_settings_json() -> Path | None:
    """settings.json 탐색: 1) env RF_SETTINGS  2) 시작 폴더에서 위로 최대 4단계 config/settings.json."""
    env = os.environ.get("RF_SETTINGS", "").strip()
    if env:
        return Path(env)
    if getattr(sys, "frozen", False):
        start = Path(sys.executable).parent
    else:
        start = Path(__file__).resolve().parent
    cur = start
    for _ in range(5):          # 시작 폴더 + 위로 4단계
        cand = cur / "config" / "settings.json"
        if cand.is_file():
            return cand
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


def _load_config(log):
    """settings.json 을 config_common 에 적용하고, 경로 3종 + STALE_MIN 을 getattr 로 읽는다.
    반환: (camera_root, state_dir, csv_dir, stale_min, settings_path)"""
    settings = _find_settings_json()
    try:
        from lib import _config_loader
        if settings is not None and settings.is_file():
            _config_loader.load_settings(path=settings)
        else:
            log(f"  ! settings.json 없음 → config_common 기본값 사용 (탐색 시작: {settings})")
            settings = None
    except Exception as e:
        log(f"  ! settings.json 적용 실패({e}) → config_common 기본값 사용")
        settings = None
    from lib import config_common as cfgc
    camera_root = getattr(cfgc, "CAMERA_LOG_ROOT",     r"C:\VanaM_Logs\Camera_Logs")
    state_dir   = getattr(cfgc, "RF_READER_STATE_DIR", r"C:\VanaM_Logs\Camera_Logs\_state")
    csv_dir     = getattr(cfgc, "RF_READER_CSV_DIR",   r"C:\VanaM_Logs\Camera_Logs\_csv")
    stale_min   = float(getattr(cfgc, "RF_READER_STALE_MIN", 10.0))
    extra = {
        "nas_dir":           getattr(cfgc, "RF_READER_NAS_DIR",
                                     r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2\Camera_Logs"),
        "retention_days":    int(getattr(cfgc, "RF_READER_RETENTION_DAYS", 14)),
        "min_free_gb":       float(getattr(cfgc, "RF_READER_MIN_FREE_GB", 20.0)),
        "upload_alert_days": int(getattr(cfgc, "RF_READER_UPLOAD_ALERT_DAYS", 2)),
    }
    return camera_root, state_dir, csv_dir, stale_min, settings, extra


# ===================== ONNX 추론 (model.py predict_digits 대체) =====================
def _tta_variants(crop):
    out = [crop]
    for dx, dy, sc in [(-1, 0, 1.0), (1, 0, 1.0), (0, -1, 1.0), (0, 1, 1.0),
                       (0, 0, 0.92), (0, 0, 1.08)]:
        M = cv2.getRotationMatrix2D((DIGIT_W / 2, DIGIT_H / 2), 0, sc)
        M[0, 2] += dx; M[1, 2] += dy
        out.append(cv2.warpAffine(crop, M, (DIGIT_W, DIGIT_H), borderValue=float(crop.min())))
    return out


def _softmax(z):
    e = np.exp(z - z.max(axis=1, keepdims=True))
    return e / e.sum(axis=1, keepdims=True)


class OnnxDigit:
    def __init__(self, path):
        so = ort.SessionOptions()
        so.intra_op_num_threads = int(os.environ.get("RF_THREADS", "2"))
        self.sess = ort.InferenceSession(path, sess_options=so,
                                         providers=["CPUExecutionProvider"])
        self.iname = self.sess.get_inputs()[0].name
        self.oname = self.sess.get_outputs()[0].name

    def predict(self, crops, tta=True):
        """crops: list of (DIGIT_H,DIGIT_W) uint8 → (preds[int], confs[float])."""
        if len(crops) == 0:
            return np.array([], int), np.array([], float)
        batch = []; counts = []
        for c in crops:
            vs = _tta_variants(c) if tta else [c]
            counts.append(len(vs)); batch.extend(vs)
        arr = np.stack(batch).astype(np.float32) / 255.0
        inp = arr[:, None, :, :]
        logits = self.sess.run([self.oname], {self.iname: inp})[0]
        prob = _softmax(logits)
        preds = np.zeros(len(crops), int); confs = np.zeros(len(crops), float)
        i = 0
        for j, n in enumerate(counts):
            p = prob[i:i + n].mean(axis=0); i += n
            preds[j] = int(p.argmax()); confs[j] = float(p.max())
        return preds, confs


# ===================== 프레임/시계열 판독 (parse.py 로직 이식) =====================
def read_frame(img, net, mats, slots, tta=True):
    panels = warp_panels(img, mats)
    out = {}
    for lbl in LABELS:
        cc = slots[lbl]
        crops, is_neg, _ = extract_cell(panels, cc)
        aligned = [align_com(c) for c in crops if c is not None]
        if len(aligned) != 3:
            out[lbl] = (None, 0.0); continue
        preds, confs = net.predict(aligned, tta=tta)
        if any(p == BLANK for p in preds):
            out[lbl] = (None, 0.0); continue
        val = "".join(str(int(d)) for d in preds)
        if is_neg:
            val = "-" + val
        out[lbl] = (val, float(np.min(confs)))
    return out


def temporal_fuse(per_frame, label):
    n = len(per_frame)
    raw = [per_frame[i][label][0] for i in range(n)]
    conf = [per_frame[i][label][1] for i in range(n)]
    out_v = [None] * n; out_r = [1] * n
    for i in range(n):
        # 1) 모델이 충분히 확신한 프레임 → 사진 그대로 기록(평탄화 안 함)
        if raw[i] is not None and conf[i] >= TRUST_CONF:
            out_v[i] = raw[i]; out_r[i] = 0
            continue
        # 2) 저신뢰/판독실패 프레임만 이웃 다수결로 보정(노이즈 제거)
        lo = max(0, i - WIN); hi = min(n, i + WIN + 1)
        votes = Counter()
        for j in range(lo, hi):
            v = raw[j]
            if v is None:
                continue
            w = 2 if conf[j] >= CONF_DIGIT_MIN else 1
            votes[v] += w
        if not votes:
            out_v[i] = raw[i]; out_r[i] = 1; continue
        best, bc = votes.most_common(1)[0]
        total = sum(votes.values()); frac = bc / total
        out_v[i] = best
        out_r[i] = 0 if (frac >= MODE_FRAC_MIN) else 1
    return out_v, out_r


# ===================== 세션 처리 =====================
def _session_timestamp(session_name, fname):
    """세션명(YYYYMMDD_HHMMSS) + 파일명(HHMMSS_NNNN) → 'YYYY-MM-DD HH:MM:SS'.
    프레임 시각이 세션 시작 시각보다 작으면 자정 넘긴 것으로 +1일."""
    base = datetime.strptime(session_name, "%Y%m%d_%H%M%S")
    m = re.match(r"^(\d{2})(\d{2})(\d{2})_", os.path.basename(fname))
    if not m:
        return os.path.basename(fname)
    h, mi, s = int(m.group(1)), int(m.group(2)), int(m.group(3))
    dt = base.replace(hour=h, minute=mi, second=s)
    if (h, mi, s) < (base.hour, base.minute, base.second):
        dt += timedelta(days=1)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def process_session(mode, session_dir, session_name, net, mats, slots,
                    csv_dir, tta=True, dry_run=False, log=print):
    files = sorted(glob.glob(os.path.join(session_dir, "*.jpg")))
    if not files:
        return {"status": "empty", "frames": 0}

    per_frame = []; used_files = []
    for f in files:
        im = cv2.imread(f)
        if im is None or im.shape[:2] != (1920, 1080):
            continue
        per_frame.append(read_frame(im, net, mats, slots, tta=tta))
        used_files.append(f)
    n = len(per_frame)
    if n == 0:
        return {"status": "no_valid_frames", "frames": 0}

    fused = {lbl: temporal_fuse(per_frame, lbl) for lbl in LABELS}

    # 프레임별 '애매' 여부: 숫자로 읽힌 칸 중 하나라도 그 프레임 신뢰도가 낮으면 보존.
    # (시계열 needs_review가 아니라 프레임 자체 신뢰도 → 정상 깜빡임은 삭제 대상)
    keep_flag = [False] * n
    for i in range(n):
        for lbl in LABELS:
            v, c = per_frame[i][lbl]
            thr = KEEP_CONF_CELL.get(lbl, KEEP_CONF_MIN)
            if v is not None and c < thr:
                keep_flag[i] = True
                break

    # CSV 작성 (RF_READER_CSV_DIR/{mode}/{MODE}_{session}.csv) — 사진 루트와 분리
    csv_path = os.path.join(csv_dir, mode, f"{mode}_{session_name}.csv")
    tmp_path = csv_path + ".tmp"
    rows = []
    for i in range(n):
        row = [_session_timestamp(session_name, used_files[i])]
        for lbl in LABELS:
            v = fused[lbl][0][i]
            row.append(v if v is not None else "")
        rows.append(row)

    if dry_run:
        kept = sum(keep_flag)
        log(f"    [dry-run] {mode}/{session_name}: frames={n} CSV={csv_path} "
            f"보존={kept} 삭제예정={n - kept}")
        return {"status": "dry_run", "frames": n, "kept": kept, "deleted": 0}

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f); w.writerow(CSV_COLS); w.writerows(rows)
    # 검증: 줄 수 = 헤더+n
    with open(tmp_path, encoding="utf-8-sig") as f:
        nlines = sum(1 for _ in f)
    if nlines != n + 1:
        os.remove(tmp_path)
        raise RuntimeError(f"CSV 줄 수 불일치({nlines} != {n+1}) → 삭제 중단")
    os.replace(tmp_path, csv_path)   # 원자적 교체

    # CSV 확정 후에만 사진 삭제
    deleted = kept = 0
    for i in range(n):
        if keep_flag[i]:
            kept += 1
        else:
            try:
                os.remove(used_files[i]); deleted += 1
            except OSError as e:
                log(f"    삭제 실패 {used_files[i]}: {e}")
    log(f"    {mode}/{session_name}: frames={n} CSV작성 보존={kept} 삭제={deleted}")
    return {"status": "done", "frames": n, "kept": kept, "deleted": deleted,
            "csv": csv_path}


# ===================== 세션 탐색/매니페스트 =====================
def discover_sessions(root):
    """[(mode, session_dir, session_name)] — raw/ 와 모드 직속 둘 다 스캔."""
    found = []
    for mode in MODES:
        for pat in (os.path.join(root, mode, "raw", "*"),
                    os.path.join(root, mode, "*")):
            for d in glob.glob(pat):
                name = os.path.basename(d.rstrip("/\\"))
                if os.path.isdir(d) and SESSION_RE.match(name):
                    found.append((mode, d, name))
    # 중복 제거(같은 mode/name)
    seen = {}
    for mode, d, name in found:
        seen[(mode, name)] = (mode, d, name)
    return sorted(seen.values(), key=lambda t: (t[0], t[2]))


def is_in_progress(session_dir, stale_min):
    """최근 stale_min분 내 수정 → 진행 중일 수 있으니 보류."""
    try:
        mtime = os.path.getmtime(session_dir)
    except OSError:
        return True
    return (time.time() - mtime) < stale_min * 60


def load_manifest(path):
    """매니페스트 dict 반환(없거나 깨졌으면 빈 dict). 없는 세션은 항상 처리 대상."""
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_manifest(path, man):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(man, f, ensure_ascii=False, indent=0)
    os.replace(tmp, path)


# ===================== NAS 전송 (로컬 CSV 정본 → NAS 사본, 재읽기 검증) =====================
def _count_lines(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f)


def upload_csvs(csv_dir, nas_dir, alert_days, log):
    """마커(.uploaded) 없는 CSV 전부 NAS 로 복사 → 재읽기 검증 → 마커 생성.
    NAS 상태는 사진 삭제와 무관. 반환: (업로드성공, 미전송)."""
    pending = []
    for mode in MODES:
        for f in sorted(glob.glob(os.path.join(csv_dir, mode, "*.csv"))):
            if not os.path.exists(f + ".uploaded"):
                pending.append((mode, f))
    if not pending:
        return 0, 0

    # NAS 접근 자체가 안 되면 전송 단계 전체 건너뜀
    try:
        os.makedirs(nas_dir, exist_ok=True)
        if not os.path.isdir(nas_dir):
            raise RuntimeError("폴더 아님")
    except Exception as e:
        log(f"  [upload] NAS 접근 불가({nas_dir}): {e} → 전송 건너뜀(미전송 {len(pending)}개)")
        _check_upload_backlog(pending, alert_days, log)
        return 0, len(pending)

    n_ok = 0
    for mode, src in pending:
        name = os.path.basename(src)
        dst_dir = os.path.join(nas_dir, mode)
        dst = os.path.join(dst_dir, name)
        tmp = dst + ".tmp"
        try:
            os.makedirs(dst_dir, exist_ok=True)
            shutil.copyfile(src, tmp)                       # 로컬 읽기 → NAS 쓰기
            os.replace(tmp, dst)                            # NAS 안에서 원자적 교체
            # 반드시 NAS 파일을 다시 읽어 검증
            sz_dst, sz_src = os.path.getsize(dst), os.path.getsize(src)
            if sz_dst != sz_src:
                raise RuntimeError(f"바이트 크기 불일치 {sz_dst} != {sz_src}")
            ln_dst, ln_src = _count_lines(dst), _count_lines(src)
            if ln_dst != ln_src:
                raise RuntimeError(f"줄 수 불일치 {ln_dst} != {ln_src}")
            with open(src + ".uploaded", "w"):
                pass
            n_ok += 1
        except Exception as e:
            log(f"  [upload] 실패 {mode}/{name}: {e}")
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
    n_pending = len(pending) - n_ok
    log(f"  [upload] 성공={n_ok} 미전송={n_pending}")
    _check_upload_backlog([p for p in pending if not os.path.exists(p[1] + ".uploaded")],
                          alert_days, log)
    return n_ok, n_pending


def _check_upload_backlog(pending, alert_days, log):
    """마커 없는 CSV 중 mtime 이 alert_days 보다 오래된 것이 있으면 알림."""
    if not pending:
        return
    now = time.time()
    ages = [(now - os.path.getmtime(f)) / 86400.0 for _, f in pending if os.path.exists(f)]
    if not ages:
        return
    oldest = max(ages)
    if oldest >= alert_days:
        msg = f"CSV {len(pending)}개가 {int(oldest)}일째 NAS 미전송"
        log(f"  [upload] ! {msg}")
        notify(msg)


# ===================== 보관 기간 정리 / 디스크 가드 =====================
def _session_date(name):
    """세션 폴더명(YYYYMMDD_HHMMSS) → datetime. mtime 은 쓰지 않는다(삭제로 갱신됨)."""
    try:
        return datetime.strptime(name, "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def _done_sessions(root, man):
    """매니페스트 status=='done' 인 세션 폴더만 [(date, mode, dir, name)] 오래된 순.
    미처리 세션은 여기 포함되지 않으므로 어떤 정리 단계에서도 삭제되지 않는다.
    CSV_DIR / STATE_DIR(_csv/_state) 은 MODES 하위 세션 패턴이 아니므로 대상이 아니다."""
    out = []
    for mode, sdir, name in discover_sessions(root):
        ent = man.get(f"{mode}/{name}")
        if not isinstance(ent, dict) or ent.get("status") != "done":
            continue
        d = _session_date(name)
        if d is None:
            continue
        out.append((d, mode, sdir, name))
    return sorted(out, key=lambda t: t[0])


def _rmtree_session(sdir, log):
    try:
        shutil.rmtree(sdir)
        return True
    except Exception as e:
        log(f"  [cleanup] 삭제 실패 {sdir}: {e}")
        return False


def cleanup_retention(root, man, retention_days, log):
    """(a) 매니페스트 done + (b) 폴더명 날짜가 retention_days 보다 오래된 세션 폴더 삭제.
    매니페스트 항목은 지우지 않는다(재판독 방지)."""
    cutoff = datetime.now() - timedelta(days=retention_days)
    n = 0
    for d, mode, sdir, name in _done_sessions(root, man):
        if d >= cutoff:
            continue
        if _rmtree_session(sdir, log):
            log(f"  [cleanup] 삭제 {mode}/{name}")
            n += 1
    log(f"  [cleanup] 보관기간({retention_days}일) 초과 완료세션 삭제={n}")
    return n


def disk_guard(root, man, min_free_gb, log):
    """여유 < 임계면 done 세션을 오래된 순으로 삭제하며 임계를 넘을 때까지 반복(보관기간 무시).
    다 지워도 부족하면 알림. 반환: 정리한 세션 수."""
    free = _free_gb(root)
    if free >= min_free_gb:
        return 0
    log(f"  [disk] 여유 부족 {free:.1f}GB < {min_free_gb:.1f}GB → 완료 세션 오래된 순 정리 시작")
    n = 0
    for d, mode, sdir, name in _done_sessions(root, man):
        if _rmtree_session(sdir, log):
            log(f"  [disk] 삭제 {mode}/{name}")
            n += 1
        free = _free_gb(root)
        if free >= min_free_gb:
            break
    log(f"  [disk] 정리한 세션={n} 여유={free:.1f}GB")
    if free < min_free_gb:
        msg = (f"디스크 여유 부족 {free:.1f}GB < {min_free_gb:.1f}GB "
               f"(정리 가능한 완료 세션 {n}개 삭제 후에도 부족)")
        log(f"  [disk] ! {msg}")
        notify(msg)
    return n


def main():
    early = []                        # 설정 로드 전 로그 버퍼(run.log 위치 확정 후 기록)
    def _pre_log(*x):
        line = " ".join(str(v) for v in x)
        print(line, flush=True); early.append(line)

    cfg_root, cfg_state, cfg_csv, cfg_stale, settings_path, cfg_x = _load_config(_pre_log)
    _load_notify_config()

    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.environ.get("RF_CAMERA_LOGS", cfg_root))
    ap.add_argument("--model", default=os.environ.get("RF_MODEL", str(_asset_path("models/cnn.onnx"))))
    ap.add_argument("--state-dir", default=os.environ.get("RF_STATE_DIR", cfg_state))
    ap.add_argument("--csv-dir", default=os.environ.get("RF_CSV_DIR", cfg_csv))
    ap.add_argument("--nas-dir", default=os.environ.get("RF_NAS_DIR", cfg_x["nas_dir"]))
    ap.add_argument("--stale-min", type=float, default=float(os.environ.get("RF_STALE_MIN", cfg_stale)))
    ap.add_argument("--since", default=os.environ.get("RF_SINCE", ""))
    ap.add_argument("--no-tta", action="store_true", default=os.environ.get("RF_TTA", "1") == "0")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    tta = not a.no_tta

    t0 = time.time()
    man_path = os.path.join(a.state_dir, "manifest.json")
    man = load_manifest(man_path)

    # run.log: state_dir 에 append (stdout 과 병행)
    os.makedirs(a.state_dir, exist_ok=True)
    run_log = open(os.path.join(a.state_dir, "run.log"), "a", encoding="utf-8")
    for line in early:
        run_log.write(f"{_now()} {line}\n")

    def log(*x):
        line = " ".join(str(v) for v in x)
        print(line, flush=True)
        try:
            run_log.write(f"{_now()} {line}\n"); run_log.flush()
        except Exception:
            pass

    slots_path = _asset_path("calibration/slots.json")
    log(f"[rf-worker] settings={settings_path or '-'}")
    log(f"  root={a.root}")
    log(f"  state_dir={a.state_dir}")
    log(f"  csv_dir={a.csv_dir}")
    log(f"  nas_dir={a.nas_dir} retention={cfg_x['retention_days']}d min_free={cfg_x['min_free_gb']}GB "
        f"upload_alert={cfg_x['upload_alert_days']}d notify={'on' if (_NOTIFY_ENABLED and _NOTIFY_URL) else 'off'}")
    log(f"  model={a.model} slots={slots_path} tta={tta} stale_min={a.stale_min} "
        f"since={a.since or '-'} dry_run={a.dry_run}")
    if not os.path.exists(a.model):
        log(f"  ! 모델 없음: {a.model}"); sys.exit(2)
    if not slots_path.is_file():
        log(f"  ! 보정 파일 없음: {slots_path}"); sys.exit(2)
    if not os.path.isdir(a.root):
        log(f"  ! 루트 없음: {a.root}"); sys.exit(2)

    # ── 디스크 가드 (판독 전) ──
    free_start = _free_gb(a.root)
    log(f"  디스크 여유(시작)={free_start:.1f}GB")
    n_guard = 0
    if not a.dry_run:
        n_guard = disk_guard(a.root, man, cfg_x["min_free_gb"], log)

    net = OnnxDigit(a.model)
    mats = build_homographies(); slots = load_slots(str(slots_path))

    sessions = discover_sessions(a.root)
    log(f"  발견 세션 {len(sessions)}개")

    n_done = n_skip = n_hold = 0
    n_del_img = n_keep_img = 0
    for mode, sdir, name in sessions:
        key = f"{mode}/{name}"
        if key in man:
            n_skip += 1; continue
        if a.since and name < a.since:
            n_skip += 1; continue
        if is_in_progress(sdir, a.stale_min):
            log(f"  보류(진행중?) {key}"); n_hold += 1; continue
        try:
            res = process_session(mode, sdir, name, net, mats, slots, a.csv_dir,
                                  tta=tta, dry_run=a.dry_run, log=log)
            if not a.dry_run and res.get("status") == "done":
                res["at"] = _now(); man[key] = res; n_done += 1
                n_del_img += int(res.get("deleted", 0)); n_keep_img += int(res.get("kept", 0))
            elif res.get("status") in ("empty", "no_valid_frames"):
                # 빈 세션은 매니페스트에 남겨 재시도 방지(원하면 제외 가능)
                if not a.dry_run:
                    res["at"] = _now(); man[key] = res
                n_skip += 1
        except Exception as e:
            log(f"  ! 처리 실패 {key}: {e}")

    if not a.dry_run:
        save_manifest(man_path, man)

    # ── NAS 전송 (판독 루프 뒤; 사진 삭제 판정과 무관) → 보관 기간 정리 ──
    n_up = n_pend = n_clean = 0
    if not a.dry_run:
        n_up, n_pend = upload_csvs(a.csv_dir, a.nas_dir, cfg_x["upload_alert_days"], log)
        n_clean = cleanup_retention(a.root, man, cfg_x["retention_days"], log)
    else:
        log("  [dry-run] 디스크 가드 / NAS 전송 / 보관 정리 생략")

    free_end = _free_gb(a.root)
    log(f"[rf-worker] 완료 {time.time()-t0:.1f}s  처리={n_done} 건너뜀={n_skip} 보류={n_hold} "
        f"삭제사진={n_del_img} 보존사진={n_keep_img} 업로드성공={n_up} 미전송={n_pend} "
        f"정리한세션={n_clean + n_guard} 디스크여유={free_start:.1f}→{free_end:.1f}GB")
    run_log.close()


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        print(tb, flush=True)
        # traceback → run.log (state_dir 해석 실패 시 exe 옆 run_error.log)
        try:
            try:
                st = os.environ.get("RF_STATE_DIR") or _load_config(lambda *x: None)[1]
                os.makedirs(st, exist_ok=True)
                lp = os.path.join(st, "run.log")
            except Exception:
                lp = str(_worker_base_dir() / "run_error.log")
            with open(lp, "a", encoding="utf-8") as f:
                f.write(f"{_now()} [rf-worker] ! 예상 못 한 예외\n{tb}\n")
        except Exception:
            pass
        _load_notify_config()
        notify(f"워커 비정상 종료: {tb.strip().splitlines()[-1]}")
        sys.exit(1)
