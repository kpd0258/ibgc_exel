import os
import uuid
import time
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional

from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS
from openpyxl import load_workbook


# -----------------------------
# Config
# -----------------------------
TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", "IBGC_Application_Template.xlsx")

# Render 디스크 경로(런타임)
GENERATED_DIR = os.environ.get("GENERATED_DIR", "/tmp/generated")
os.makedirs(GENERATED_DIR, exist_ok=True)

# 오래된 파일/잡 정리
JOB_TTL_HOURS = int(os.environ.get("JOB_TTL_HOURS", "6"))

app = Flask(__name__)
CORS(app)


# -----------------------------
# In-memory job store
# -----------------------------
# JOBS[job_id] = {
#   "job_id": str,
#   "status": "queued"|"processing"|"done"|"failed",
#   "progress_percent": int,
#   "error": str,
#   "created_at": float,
#   "updated_at": float,
#   "base_url": str,
#   "output_filename": str,      # download name (예: IBGC_Application.xlsx)
#   "stored_filename": str,      # 실제 서버 저장 파일명(충돌 방지)
#   "file_url": str,
# }
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


# -----------------------------
# Helpers
# -----------------------------
def now_ts() -> float:
    return time.time()

def safe_filename(name: str) -> str:
    # 아주 단순하게 위험문자 제거
    keep = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_", ".", " ", "(", ")", "[", "]"):
            keep.append(ch)
        else:
            keep.append("_")
    out = "".join(keep).strip()
    if not out:
        out = "export"
    return out

def parse_rows_text(rows_text: str) -> List[List[str]]:
    """
    rows_text 규칙:
    - 한 줄 = 엑셀의 한 행
    - 열 구분자 = | (파이프)
    예)
    1|JOB001|회사A|접수
    2|JOB002|회사B|접수
    """
    if not rows_text:
        return []
    lines = [ln.rstrip("\n") for ln in rows_text.splitlines()]
    # 빈 줄 제거(완전 공백은 제외)
    lines = [ln for ln in lines if ln.strip() != ""]
    rows: List[List[str]] = []
    for ln in lines:
        cols = ln.split("|")
        rows.append(cols)
    return rows

def col_to_index(start_col: Any) -> int:
    """
    start_col:
      - 숫자(1=A, 2=B ...) 또는
      - 엑셀 컬럼 문자(A, B, AA ...)
    """
    if start_col is None:
        return 1
    if isinstance(start_col, int):
        return max(1, start_col)
    s = str(start_col).strip()
    if s.isdigit():
        return max(1, int(s))
    # letters -> index
    s = s.upper()
    n = 0
    for ch in s:
        if not ("A" <= ch <= "Z"):
            continue
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return max(1, n) if n > 0 else 1

def cleanup_old_jobs_and_files() -> None:
    cutoff = now_ts() - JOB_TTL_HOURS * 3600
    to_delete: List[str] = []
    with JOBS_LOCK:
        for job_id, job in JOBS.items():
            if job.get("created_at", now_ts()) < cutoff:
                to_delete.append(job_id)

        for job_id in to_delete:
            stored = JOBS[job_id].get("stored_filename")
            # job 제거
            del JOBS[job_id]
            # 파일 제거
            if stored:
                path = os.path.join(GENERATED_DIR, stored)
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except Exception:
                    pass


def update_job(job_id: str, **kwargs: Any) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job.update(kwargs)
        job["updated_at"] = now_ts()

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with JOBS_LOCK:
        j = JOBS.get(job_id)
        return dict(j) if j else None


def generate_excel_job(
    job_id: str,
    base_url: str,
    output_filename: str,
    stored_filename: str,
    sheets: List[Dict[str, Any]],
) -> None:
    """
    백그라운드 스레드에서 실행
    - request/response 절대 사용 금지 (request context 에러 방지)
    """
    try:
        update_job(job_id, status="processing", progress_percent=1, error="")

        # 템플릿 로드
        if not os.path.exists(TEMPLATE_PATH):
            raise FileNotFoundError(f"Template not found: {TEMPLATE_PATH}")

        wb = load_workbook(TEMPLATE_PATH)

        # 총 작업량(진행률 계산용)
        total_rows = 0
        parsed_sheet_rows: List[Tuple[str, int, int, List[List[str]]]] = []
        for s in sheets:
            sheet_name = str(s.get("name", "")).strip()
            start_row = int(s.get("start_row", 1))
            start_col = col_to_index(s.get("start_col", 1))
            rows_text = s.get("rows_text", "") or ""
            rows = parse_rows_text(rows_text)
            parsed_sheet_rows.append((sheet_name, start_row, start_col, rows))
            total_rows += len(rows)

        if total_rows == 0:
            # 데이터가 없으면 그냥 템플릿 그대로 저장
            out_path = os.path.join(GENERATED_DIR, stored_filename)
            wb.save(out_path)
            file_url = f"{base_url}generated/{stored_filename}"
            update_job(job_id, status="done", progress_percent=100, file_url=file_url)
            return

        done_rows = 0

        for (sheet_name, start_row, start_col, rows) in parsed_sheet_rows:
            if sheet_name not in wb.sheetnames:
                # 시트명이 다르면 생성(혹은 실패 처리 선택 가능)
                wb.create_sheet(sheet_name)
            ws = wb[sheet_name]

            r = start_row
            for row_vals in rows:
                c = start_col
                for v in row_vals:
                    ws.cell(row=r, column=c, value=v)
                    c += 1
                r += 1

                done_rows += 1
                # 1~99 사이 진행률
                progress = int((done_rows / total_rows) * 98) + 1
                if progress > 99:
                    progress = 99
                update_job(job_id, progress_percent=progress)

        out_path = os.path.join(GENERATED_DIR, stored_filename)
        wb.save(out_path)

        file_url = f"{base_url}generated/{stored_filename}"
        update_job(job_id, status="done", progress_percent=100, file_url=file_url)

    except Exception as e:
        update_job(job_id, status="failed", progress_percent=0, error=str(e))


# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    cleanup_old_jobs_and_files()
    return jsonify({"ok": True})


@app.post("/excel/start")
def excel_start():
    """
    요청 예:
    {
      "filename": "IBGC_Application",
      "sheets": [
        {"name":"1. 인증신청서 관리","start_row":10,"start_col":1,"rows_text":"1|JOB001|회사A|접수\n2|JOB002|회사B|접수"},
        ...
      ]
    }

    응답:
    {
      "job_id":"...",
      "status":"queued",
      "progress_percent":0
    }
    """
    cleanup_old_jobs_and_files()

    payload = request.get_json(silent=True) or {}
    filename = payload.get("filename", "export")
    filename = safe_filename(str(filename))
    if not filename.lower().endswith(".xlsx"):
        output_filename = f"{filename}.xlsx"
    else:
        output_filename = filename

    sheets = payload.get("sheets", [])
    if not isinstance(sheets, list):
        return jsonify({"error": "sheets must be a list"}), 400

    job_id = str(uuid.uuid4())

    # 서버 저장 파일명은 충돌 방지로 job_id 포함
    base_no_ext = output_filename[:-5] if output_filename.lower().endswith(".xlsx") else output_filename
    stored_filename = safe_filename(f"{base_no_ext}_{job_id}.xlsx")

    base_url = request.host_url  # 여기서만 request 사용 (문자열로 저장)

    with JOBS_LOCK:
        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "progress_percent": 0,
            "error": "",
            "created_at": now_ts(),
            "updated_at": now_ts(),
            "base_url": base_url,
            "output_filename": output_filename,  # 사용자에게 보여줄 이름
            "stored_filename": stored_filename,  # 서버 저장 이름
            "file_url": "",
        }

    # 백그라운드 실행
    th = threading.Thread(
        target=generate_excel_job,
        args=(job_id, base_url, output_filename, stored_filename, sheets),
        daemon=True,
    )
    th.start()

    return jsonify({"job_id": job_id, "status": "queued", "progress_percent": 0})


@app.get("/excel/status/<job_id>")
def excel_status(job_id: str):
    cleanup_old_jobs_and_files()

    job = get_job(job_id)
    if not job:
        return jsonify({"error": "job not found", "status": "failed", "progress_percent": 0}), 404

    # file_url은 done일 때만 의미 있음
    return jsonify(
        {
            "job_id": job["job_id"],
            "status": job["status"],
            "progress_percent": int(job.get("progress_percent", 0)),
            "file_url": job.get("file_url", "") if job["status"] == "done" else "",
            "error": job.get("error", "") if job["status"] == "failed" else "",
            "output_filename": job.get("output_filename", ""),
        }
    )


@app.get("/generated/<path:stored_filename>")
def download_generated(stored_filename: str):
    """
    실제 파일 다운로드 엔드포인트.
    - Bubble에서 Open external website 로 여길 열면 다운로드됨
    """
    # 보안상 디렉토리 트래버설 방지: send_from_directory가 기본 방어를 어느 정도 하지만
    # 그래도 너무 이상한 값은 컷
    if ".." in stored_filename or stored_filename.startswith("/"):
        abort(400)

    file_path = os.path.join(GENERATED_DIR, stored_filename)
    if not os.path.exists(file_path):
        abort(404)

    # 사용자에게 보여줄 파일명(output_filename)을 job에서 찾아 download_name으로 제공
    # stored_filename에 job_id가 들어가 있으니 역으로 찾아도 됨
    download_name = None
    with JOBS_LOCK:
        for j in JOBS.values():
            if j.get("stored_filename") == stored_filename:
                download_name = j.get("output_filename")
                break

    # Flask 2.0+: download_name 사용
    return send_from_directory(
        GENERATED_DIR,
        stored_filename,
        as_attachment=True,
        download_name=download_name or stored_filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    # 로컬 테스트용
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
