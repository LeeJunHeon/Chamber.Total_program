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
  RF_SETTINGS          settings.json 경로 직접 지정
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
import sys
import time
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
    return camera_root, state_dir, csv_dir, stale_min, settings


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


def main():
    early = []                        # 설정 로드 전 로그 버퍼(run.log 위치 확정 후 기록)
    def _pre_log(*x):
        line = " ".join(str(v) for v in x)
        print(line, flush=True); early.append(line)

    cfg_root, cfg_state, cfg_csv, cfg_stale, settings_path = _load_config(_pre_log)

    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.environ.get("RF_CAMERA_LOGS", cfg_root))
    ap.add_argument("--model", default=os.environ.get("RF_MODEL", str(_asset_path("models/cnn.onnx"))))
    ap.add_argument("--state-dir", default=os.environ.get("RF_STATE_DIR", cfg_state))
    ap.add_argument("--csv-dir", default=os.environ.get("RF_CSV_DIR", cfg_csv))
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
    log(f"  model={a.model} slots={slots_path} tta={tta} stale_min={a.stale_min} "
        f"since={a.since or '-'} dry_run={a.dry_run}")
    if not os.path.exists(a.model):
        log(f"  ! 모델 없음: {a.model}"); sys.exit(2)
    if not slots_path.is_file():
        log(f"  ! 보정 파일 없음: {slots_path}"); sys.exit(2)
    if not os.path.isdir(a.root):
        log(f"  ! 루트 없음: {a.root}"); sys.exit(2)

    net = OnnxDigit(a.model)
    mats = build_homographies(); slots = load_slots(str(slots_path))

    sessions = discover_sessions(a.root)
    log(f"  발견 세션 {len(sessions)}개")

    n_done = n_skip = n_hold = 0
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
            elif res.get("status") in ("empty", "no_valid_frames"):
                # 빈 세션은 매니페스트에 남겨 재시도 방지(원하면 제외 가능)
                if not a.dry_run:
                    res["at"] = _now(); man[key] = res
                n_skip += 1
        except Exception as e:
            log(f"  ! 처리 실패 {key}: {e}")

    if not a.dry_run:
        save_manifest(man_path, man)
    log(f"[rf-worker] 완료 {time.time()-t0:.1f}s  처리={n_done} "
        f"건너뜀={n_skip} 보류={n_hold}")
    run_log.close()


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    main()
