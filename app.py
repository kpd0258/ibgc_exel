import os
import re
import uuid
import time
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional

from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS
from openpyxl import load_workbook


APP_ROOT = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(APP_ROOT, "IBGC_Application_Template.xlsx")

GENERATED_DIR = os.path.join(APP_ROOT, "generated")
os.makedirs(GENERATED_DIR, exist_ok=True)

# 간단 job store (Render 단일 인스턴스 기준)
# (추후 대량처리/다중인스턴스면 Redis/DB로 바꾸는 게 정석)
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

app = Flask(__name__)
CORS(app)


# -------------------------
# Utilities
# -------------------------
def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def safe_filename(name: str) -> str:
    """
    파일명 안전화:
    - 한글/영문/숫자/공백/._- 허용
    - 나머지는 _
    - 공백은 _로
    - 끝에 .xlsx 없으면 붙임
    """
    name = (name or "").strip()
    if not name:
        name = "export"
    name = name.replace(" ", "_")
    name = re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", name)
    if not name.lower().endswith(".xlsx"):
        name += ".xlsx"
    return name


def parse_rows_text(rows_text: Optional[str]) -> List[List[str]]:
    """
    rows_text:
    - 각 줄 = 한 행
    - 열 구분 = |
    """
    if not rows_text:
        return []
    lines = [ln for ln in rows_text.splitlines() if ln.strip() != ""]
    rows = []
    for ln in lines:
        rows.append([cell.strip() for cell in ln.split("|")])
    return rows


def set_job(job_id: str, patch: Dict[str, Any]) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job.update(patch)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        return dict(job) if job else None


def init_job(job_id: str, filename: str) -> None:
    with JOBS_LOCK:
        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",          # queued | running | done | failed
            "progress_percent": 0,
            "error": "",
            "file_path": "",
            "file_url": "",
            "filename": filename,
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }


# -------------------------
# Excel generation worker
# -------------------------
def generate_excel_worker(job_id: str, payload: Dict[str, Any], base_url: str) -> None:
    """
    정확한 진행률을 위해:
    - 전체 "행" 단위 작업량(total_rows)을 계산
    - 각 행을 쓰면 done_rows 증가
    """
    try:
        if not os.path.exists(TEMPLATE_PATH):
            raise FileNotFoundError(f"Template not found: {TEMPLATE_PATH}")

        set_job(job_id, {"status": "running", "progress_percent": 1, "updated_at": now_iso()})

        sheets = payload.get("sheets") or []
        if not isinstance(sheets, list) or len(sheets) == 0:
            raise ValueError("payload.sheets must be a non-empty list")

        # 사전 파싱 (total 계산)
        parsed: List[Dict[str, Any]] = []
        total_rows = 0
        for sh in sheets:
            name = sh.get("name")
            start_row = int(sh.get("start_row", 1))
            start_col = int(sh.get("start_col", 1))
            rows_text = sh.get("rows_text", "")
            rows = parse_rows_text(rows_text)
            total_rows += len(rows)
            parsed.append({
                "name": name,
                "start_row": start_row,
                "start_col": start_col,
                "rows": rows
            })

        # total_rows가 0이면 진행률을 "즉시 done"으로 만들기보단 실패로 처리(원하면 정책 변경 가능)
        if total_rows == 0:
            raise ValueError("No rows to write (all rows_text are empty).")

        # 템플릿 로딩 (여기서도 진행률 조금 올림)
        wb = load_workbook(TEMPLATE_PATH)
        set_job(job_id, {"progress_percent": 3, "updated_at": now_iso()})

        done_rows = 0

        # 시트별 기록
        for sh in parsed:
            sheet_name = sh["name"]
            if sheet_name not in wb.sheetnames:
                raise ValueError(f"Sheet not found in template: {sheet_name}")

            ws = wb[sheet_name]
            r0 = sh["start_row"]
            c0 = sh["start_col"]
            rows = sh["rows"]

            for i, row_vals in enumerate(rows):
                r = r0 + i
                for j, val in enumerate(row_vals):
                    c = c0 + j
                    ws.cell(row=r, column=c, value=val)

                done_rows += 1
                # 정확한 %: 3%~98% 사이로 매핑 (마지막 저장/정리 구간 남겨둠)
                pct = 3 + int((done_rows / total_rows) * 95)
                if pct > 98:
                    pct = 98
                set_job(job_id, {"progress_percent": pct, "updated_at": now_iso()})

                # 너무 잦은 업데이트가 부담되면 (예: 1000행 이상) 아래처럼 간격을 둘 수도 있음
                # if done_rows % 10 == 0: set_job(...)

        # 저장 (여기서 99→100)
        filename = safe_filename(payload.get("filename", "export"))
        job_dir = os.path.join(GENERATED_DIR, job_id)
        os.makedirs(job_dir, exist_ok=True)
        out_path = os.path.join(job_dir, filename)

        set_job(job_id, {"progress_percent": 99, "updated_at": now_iso()})
        wb.save(out_path)

        file_url = f"{base_url}/generated/{job_id}/{filename}"

        set_job(job_id, {
            "status": "done",
            "progress_percent": 100,
            "file_path": out_path,
            "file_url": file_url,
            "updated_at": now_iso()
        })

    except Exception as e:
        set_job(job_id, {
            "status": "failed",
            "error": str(e),
            "updated_at": now_iso()
        })


# -------------------------
# Routes
# -------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.post("/excel/start")
def excel_start():
    """
    Request JSON:
    {
      "filename": "IBGC_Application_2026-02-27",
      "sheets": [
        {"name":"1. 인증신청서 관리","start_row":8,"start_col":1,"rows_text":"..."},
        ...
      ]
    }

    Response:
    { "job_id": "...", "status_url": "...", "status": "queued" }
    """
    payload = request.get_json(force=True, silent=False)
    filename = safe_filename((payload or {}).get("filename", "export"))

    job_id = uuid.uuid4().hex
    init_job(job_id, filename)

    # Render 환경에서 base_url 만들기
    base_url = request.host_url.rstrip("/")

    th = threading.Thread(
        target=generate_excel_worker,
        args=(job_id, payload, base_url),
        daemon=True
    )
    th.start()

    status_url = f"{base_url}/excel/status/{job_id}"
    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "status_url": status_url
    })


@app.get("/excel/status/<job_id>")
def excel_status(job_id: str):
    """
    Response:
    {
      "job_id": "...",
      "status": "running|done|failed",
      "progress_percent": 0-100,
      "file_url": "...(done일 때)",
      "error": "...(failed일 때)"
    }
    """
    job = get_job(job_id)
    if not job:
        return jsonify({
            "job_id": job_id,
            "status": "failed",
            "progress_percent": 0,
            "file_url": "",
            "error": "job not found"
        }), 404

    return jsonify({
        "job_id": job_id,
        "status": job.get("status", "failed"),
        "progress_percent": int(job.get("progress_percent", 0)),
        "file_url": job.get("file_url", ""),
        "error": job.get("error", "")
    })


@app.get("/generated/<job_id>/<path:filename>")
def download_generated(job_id: str, filename: str):
    """
    실제 다운로드 엔드포인트.
    filename을 URL에 포함시켜서 "저장되는 파일명"이 난수가 아니라 지정된 이름으로 떨어지게 함.
    """
    job_dir = os.path.join(GENERATED_DIR, job_id)
    file_path = os.path.join(job_dir, filename)

    if not os.path.exists(file_path):
        abort(404)

    # as_attachment=True 로 다운로드 강제 + download_name으로 파일명 고정(Flask 2+)
    return send_from_directory(
        directory=job_dir,
        path=filename,
        as_attachment=True,
        download_name=filename
    )


if __name__ == "__main__":
    # 로컬 테스트
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
