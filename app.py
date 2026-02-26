import os
import uuid
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from openpyxl import load_workbook

app = Flask(__name__)
CORS(app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(BASE_DIR, "IBGC_Application_Template.xlsx")

GENERATED_DIR = os.path.join(BASE_DIR, "generated")
os.makedirs(GENERATED_DIR, exist_ok=True)

# -----------------------------
# In-memory job store
# -----------------------------
# job_id -> {
#   status: "queued" | "running" | "done" | "failed",
#   progress_percent: int (0~100),
#   message: str,
#   file_name: str,
#   file_url: str,
#   error: str
# }
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_filename(name: str) -> str:
    """
    - 확장자 제거/정리
    - 위험 문자 제거
    - 비어있으면 default
    """
    if not name:
        return "IBGC_Export"
    name = name.strip()
    if name.lower().endswith(".xlsx"):
        name = name[:-5]
    # 파일명에 위험한 문자 제거
    bad = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
    for ch in bad:
        name = name.replace(ch, "_")
    name = name.strip()
    return name if name else "IBGC_Export"

def set_job(job_id: str, **kwargs):
    with JOBS_LOCK:
        if job_id not in JOBS:
            JOBS[job_id] = {}
        JOBS[job_id].update(kwargs)

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with JOBS_LOCK:
        return JOBS.get(job_id)

# -----------------------------
# Data helpers
# -----------------------------
def parse_rows_text(rows_text: str) -> List[List[str]]:
    """
    rows_text 예:
      1|JOB001|회사A|접수
      2|JOB002|회사B|접수

    => [ ["1","JOB001","회사A","접수"], ["2","JOB002","회사B","접수"] ]
    """
    if not rows_text:
        return []
    lines = [ln.strip() for ln in rows_text.splitlines() if ln.strip()]
    rows = []
    for ln in lines:
        rows.append(ln.split("|"))
    return rows

def parse_rows_any(rows_any: Any) -> List[List[str]]:
    """
    rows는 다음 두 형태 모두 지원:
    1) rows_text (문자열)
    2) rows (2차원 배열)  예: [["1","JOB001","회사A","접수"], ["2","JOB002","회사B","접수"]]
    """
    if rows_any is None:
        return []
    if isinstance(rows_any, str):
        return parse_rows_text(rows_any)
    if isinstance(rows_any, list):
        out = []
        for r in rows_any:
            if isinstance(r, list):
                out.append([("" if v is None else str(v)) for v in r])
            else:
                # 단일값이면 1열로 취급
                out.append([("" if r is None else str(r))])
        return out
    return []

def write_rows_to_sheet(ws, start_row: int, rows: List[List[str]], job_id: str, done_cells: int, total_cells: int) -> int:
    """
    시트에 rows를 start_row부터 기록.
    진행률을 "셀 단위"로 정확히 업데이트.
    반환: 업데이트된 done_cells
    """
    r = start_row
    for row in rows:
        c = 1
        for value in row:
            ws.cell(row=r, column=c, value=value)
            done_cells += 1
            c += 1

            # 너무 잦은 업데이트는 느려질 수 있어, 셀 50개마다 한 번 갱신
            if total_cells > 0 and (done_cells % 50 == 0):
                pct = int(done_cells * 100 / total_cells)
                # 99에서 멈추지 않게 상한 처리
                if pct >= 100:
                    pct = 99
                set_job(job_id, progress_percent=pct, message=f"Writing cells... {pct}%")
        r += 1
    return done_cells

# -----------------------------
# Worker
# -----------------------------
def excel_worker(job_id: str, payload: Dict[str, Any]):
    """
    payload:
    {
      "filename": "IBGC_Application",
      "sheets": [
        {"name":"1. 인증신청서 관리","start_row":10,"rows_text":"..."}  # or rows: [[...],[...]]
        ...
      ]
    }
    """
    try:
        if not os.path.exists(TEMPLATE_PATH):
            raise FileNotFoundError(f"Template not found: {TEMPLATE_PATH}")

        set_job(job_id, status="running", progress_percent=1, message="Loading template...")

        wb = load_workbook(TEMPLATE_PATH)
        sheets_cfg = payload.get("sheets", [])
        filename_req = safe_filename(payload.get("filename", "IBGC_Export"))

        # 1) 전체 셀 개수 계산(정확한 %를 위해)
        #   - rows를 미리 파싱해서 total_cells 산출
        parsed_sheets = []
        total_cells = 0

        for s in sheets_cfg:
            sheet_name = s.get("name", "")
            start_row = int(s.get("start_row", 1))
            rows = parse_rows_any(s.get("rows", None))
            if not rows:
                rows = parse_rows_text(s.get("rows_text", ""))

            # 열 수는 row별로 다를 수 있으니 실제 데이터 기준
            cell_count = sum(len(r) for r in rows)
            total_cells += cell_count

            parsed_sheets.append({
                "name": sheet_name,
                "start_row": start_row,
                "rows": rows
            })

        # 데이터가 없으면 실패 처리
        if total_cells == 0:
            set_job(job_id, status="failed", progress_percent=0, error="No rows to write", message="No data")
            return

        set_job(job_id, progress_percent=3, message="Preparing sheets...")

        # 2) 실제 쓰기
        done_cells = 0
        for idx, s in enumerate(parsed_sheets, start=1):
            sheet_name = s["name"]
            start_row = s["start_row"]
            rows = s["rows"]

            if sheet_name not in wb.sheetnames:
                # 템플릿에 시트명이 정확히 없으면 실패시키는 게 안전
                raise ValueError(f"Sheet not found in template: {sheet_name}")

            ws = wb[sheet_name]
            set_job(job_id, message=f"Writing: {sheet_name} ({idx}/{len(parsed_sheets)})")

            done_cells = write_rows_to_sheet(ws, start_row, rows, job_id, done_cells, total_cells)

        set_job(job_id, progress_percent=99, message="Saving workbook...")

        # 3) 저장
        # 파일명: 요청파일명 + 날짜시간 + uuid짧게
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        short = job_id[:8]
        out_name = f"{filename_req}_{stamp}_{short}.xlsx"
        out_path = os.path.join(GENERATED_DIR, out_name)

        wb.save(out_path)

        file_url = f"{request.host_url.rstrip('/')}/generated/{out_name}"

        set_job(
            job_id,
            status="done",
            progress_percent=100,
            message="Done",
            file_name=out_name,
            file_url=file_url,
            finished_at=_now_str()
        )

    except Exception as e:
        set_job(job_id, status="failed", progress_percent=0, error=str(e), message="Failed")

# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/excel/start")
def excel_start():
    """
    Start job and return job_id immediately.
    """
    payload = request.get_json(silent=True) or {}

    job_id = str(uuid.uuid4())
    set_job(
        job_id,
        status="queued",
        progress_percent=0,
        message="Queued",
        created_at=_now_str(),
        file_url="",
        error=""
    )

    t = threading.Thread(target=excel_worker, args=(job_id, payload), daemon=True)
    t.start()

    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "progress_percent": 0
    })

@app.get("/excel/status/<job_id>")
def excel_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return jsonify({
            "job_id": job_id,
            "status": "not_found",
            "progress_percent": 0,
            "file_url": "",
            "error": "job_id not found"
        }), 404

    # 안정적으로 키가 없어도 반환되게
    return jsonify({
        "job_id": job_id,
        "status": job.get("status", "unknown"),
        "progress_percent": int(job.get("progress_percent", 0)),
        "message": job.get("message", ""),
        "file_url": job.get("file_url", ""),
        "file_name": job.get("file_name", ""),
        "error": job.get("error", "")
    })

@app.get("/generated/<path:filename>")
def download_generated(filename: str):
    # 파일 다운로드 엔드포인트
    return send_from_directory(GENERATED_DIR, filename, as_attachment=True)

if __name__ == "__main__":
    # 로컬 테스트용
    app.run(host="0.0.0.0", port=5000, debug=True)
