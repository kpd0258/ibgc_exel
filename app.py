import os
import uuid
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string


app = Flask(__name__)
CORS(app)

# -------------------------
# Storage (in-memory)
# -------------------------
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(BASE_DIR, "IBGC_Application_Template.xlsx")

# Render 환경에서 쓰기 가능한 경로
GENERATED_DIR = os.path.join("/tmp", "generated")
os.makedirs(GENERATED_DIR, exist_ok=True)


# -------------------------
# Helpers
# -------------------------
def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def safe_filename(name: str) -> str:
    # 아주 단순한 sanitize (윈도우/리눅스 공통 문제 문자 제거)
    bad = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
    for ch in bad:
        name = name.replace(ch, "_")
    name = name.strip()
    if not name:
        name = "export"
    if not name.lower().endswith(".xlsx"):
        name += ".xlsx"
    return name


def parse_rows_text(rows_text: str) -> List[List[str]]:
    """
    rows_text:
      - 여러 줄: 각 줄 = 1행
      - 각 줄: | 로 컬럼 분리
    예)
      JOB001|회사A|접수
      JOB002|회사B|접수
    """
    if not rows_text:
        return []
    lines = [ln.strip() for ln in rows_text.splitlines() if ln.strip()]
    rows: List[List[str]] = []
    for ln in lines:
        cols = [c.strip() for c in ln.split("|")]
        rows.append(cols)
    return rows


def set_job(job_id: str, patch: Dict[str, Any]) -> None:
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(patch)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with JOBS_LOCK:
        j = JOBS.get(job_id)
        return dict(j) if j else None


def compute_total_units(sheets_payload: List[Dict[str, Any]]) -> int:
    """
    진행률 단위는 '행(row) 단위'로 계산 (너가 말한 기준대로)
    """
    total = 0
    for s in sheets_payload:
        rows_text = s.get("rows_text", "") or ""
        rows = parse_rows_text(rows_text)
        total += len(rows)
    return max(total, 1)  # 0 division 방지


def col_to_index(col: Any) -> int:
    """
    start_col 입력이 1-based 숫자거나, 'A' 같은 문자인 경우 처리
    """
    if col is None:
        return 1
    if isinstance(col, int):
        return max(col, 1)
    if isinstance(col, str):
        c = col.strip()
        if c.isdigit():
            return max(int(c), 1)
        # 'A', 'B', ...
        return column_index_from_string(c.upper())
    return 1


# -------------------------
# Core worker
# -------------------------
def run_excel_job(job_id: str, sheets_payload: List[Dict[str, Any]], filename: str) -> None:
    try:
        set_job(job_id, {"status": "running", "progress_percent": 0, "updated_at": now_iso()})

        if not os.path.exists(TEMPLATE_PATH):
            raise FileNotFoundError(f"Template not found: {TEMPLATE_PATH}")

        wb = load_workbook(TEMPLATE_PATH)

        total_units = compute_total_units(sheets_payload)
        done_units = 0

        # 1번 시트 No 요구사항:
        # - 1번 시트(첫번째 payload 시트 또는 워크북 첫 시트)에만
        # - A9부터 No 자동
        NO_SHEET_INDEX = 0
        NO_START_ROW = 9
        NO_COL_INDEX = 1  # A열

        for sheet_idx, sheet_conf in enumerate(sheets_payload):
            sheet_name = sheet_conf.get("name")
            if not sheet_name:
                # 이름이 없으면 워크북의 순서대로 잡기
                ws = wb.worksheets[sheet_idx] if sheet_idx < len(wb.worksheets) else wb.active
            else:
                ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active

            rows_text = sheet_conf.get("rows_text", "") or ""
            rows = parse_rows_text(rows_text)

            # 기본 시작 좌표 (사용자가 body에서 넣을 수 있음)
            start_row = int(sheet_conf.get("start_row", 1) or 1)
            start_col = col_to_index(sheet_conf.get("start_col", 1))

            # ✅ 요구사항: 1번 시트만 A9부터 No 자동
            if sheet_idx == NO_SHEET_INDEX:
                # A9 고정
                start_row = NO_START_ROW

                # No가 A열을 쓰므로 데이터는 B열부터(최소 2)
                # 사용자가 start_col을 1로 줬든 2로 줬든, 안전하게 2 이상으로 강제
                data_start_col = max(2, start_col if start_col else 2)
            else:
                data_start_col = start_col

            for r_i, cols in enumerate(rows):
                excel_row = start_row + r_i

                # 1번 시트면 A열에 No 자동
                if sheet_idx == NO_SHEET_INDEX:
                    no_value = r_i + 1
                    ws.cell(row=excel_row, column=NO_COL_INDEX, value=no_value)

                    # 안전장치:
                    # Bubble에서 혹시 예전처럼 "1|JOB001|..." 형태로 보내면,
                    # 첫 토큰이 숫자면 제거하고 나머지 값만 채움
                    if cols and cols[0].isdigit():
                        cols = cols[1:]

                # 데이터 채우기
                for c_i, v in enumerate(cols):
                    ws.cell(row=excel_row, column=data_start_col + c_i, value=v)

                # progress update (행 단위)
                done_units += 1
                progress = int((done_units / total_units) * 100)
                # 너무 자주 업데이트해도 무방하지만, 깔끔하게 0~100 보장
                progress = max(0, min(progress, 100))
                set_job(job_id, {"progress_percent": progress, "updated_at": now_iso()})

                # (선택) CPU 여유를 위해 아주 미세한 sleep (필요 없으면 제거 가능)
                # time.sleep(0.001)

        # 저장
        out_filename = safe_filename(filename)
        # 같은 파일명 충돌 방지: job_id prefix
        saved_name = f"{job_id}__{out_filename}"
        out_path = os.path.join(GENERATED_DIR, saved_name)
        wb.save(out_path)

        file_url = f"/generated/{saved_name}"

        set_job(job_id, {
            "status": "done",
            "progress_percent": 100,
            "file_url": file_url,
            "file_name": out_filename,
            "updated_at": now_iso()
        })

    except Exception as e:
        set_job(job_id, {
            "status": "failed",
            "error": str(e),
            "progress_percent": 0,
            "updated_at": now_iso()
        })


# -------------------------
# Routes
# -------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/excel/start", methods=["POST"])
def excel_start():
    payload = request.get_json(force=True, silent=True) or {}

    filename = payload.get("filename", "IBGC_Application.xlsx")
    filename = safe_filename(str(filename))

    sheets_payload = payload.get("sheets", [])
    if not isinstance(sheets_payload, list) or len(sheets_payload) == 0:
        return jsonify({"error": "sheets must be a non-empty list"}), 400

    job_id = str(uuid.uuid4())

    with JOBS_LOCK:
        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "progress_percent": 0,
            "file_url": "",
            "file_name": filename,
            "error": "",
            "created_at": now_iso(),
            "updated_at": now_iso()
        }

    t = threading.Thread(target=run_excel_job, args=(job_id, sheets_payload, filename), daemon=True)
    t.start()

    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "progress_percent": 0
    })


@app.route("/excel/status/<job_id>", methods=["GET"])
def excel_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "job not found", "job_id": job_id, "status": "failed", "progress_percent": 0}), 404

    # 절대경로가 아닌 상대 file_url을 Bubble이 열 수 있게 전체 URL 조합이 필요하면
    # Bubble에서 "https://ibgc-exel.onrender.com" + file_url 로 붙여서 열면 됨
    return jsonify({
        "job_id": job["job_id"],
        "status": job["status"],
        "progress_percent": job.get("progress_percent", 0),
        "file_url": job.get("file_url", ""),
        "file_name": job.get("file_name", ""),
        "error": job.get("error", ""),
        "created_at": job.get("created_at", ""),
        "updated_at": job.get("updated_at", "")
    })


@app.route("/generated/<path:filename>", methods=["GET"])
def download_generated(filename: str):
    # 브라우저 다운로드 파일명(Content-Disposition)을 사람이 읽는 이름으로 주고 싶으면:
    # -> 저장된 파일명은 job_id__원하는이름.xlsx 이지만
    # -> 실제 다운로드 이름은 원래 filename에서 job_id__ 떼고 제공
    download_name = filename
    if "__" in filename:
        download_name = filename.split("__", 1)[1]

    return send_from_directory(
        GENERATED_DIR,
        filename,
        as_attachment=True,
        download_name=download_name
    )


if __name__ == "__main__":
    # 로컬 테스트용
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
