import os
import uuid
import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import requests
from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS
from openpyxl import load_workbook

app = Flask(__name__)
CORS(app)

# =========================
# ENV
# =========================
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")  # e.g. https://ibgc-exel.onrender.com
EXCEL_API_KEY = os.getenv("EXCEL_API_KEY", "")  # simple shared secret for Bubble -> Render

BUBBLE_BASE_URL = os.getenv("BUBBLE_BASE_URL", "").rstrip("/")  # e.g. https://ibgc.co.kr/version-test
BUBBLE_API_TOKEN = os.getenv("BUBBLE_API_TOKEN", "")  # Bubble Admin API Token

# Bubble Data API endpoint base:
# Bubble typically: {base}/api/1.1/obj/{Thing}
BUBBLE_DATA_API_BASE = f"{BUBBLE_BASE_URL}/api/1.1/obj"

# Template & output
TEMPLATE_PATH = os.getenv("TEMPLATE_PATH", "template.xlsx")
GENERATED_DIR = os.getenv("GENERATED_DIR", "generated")
os.makedirs(GENERATED_DIR, exist_ok=True)

# =========================
# In-memory job store
# =========================
_jobs_lock = threading.Lock()
_jobs: Dict[str, Dict[str, Any]] = {}
_latest: Dict[str, Any] = {
    "status": "none",
    "file_url": "",
    "filename": "",
    "created_at": "",
    "row_count": 0,
}

def _auth_ok(req) -> bool:
    if not EXCEL_API_KEY:
        return True  # if you didn't set it, allow
    return req.headers.get("X-API-Key", "") == EXCEL_API_KEY

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _public_file_url(filename: str) -> str:
    # MUST be absolute and valid
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL}/generated/{filename}"
    # fallback to request host if PUBLIC_BASE_URL not set
    base = request.host_url.rstrip("/")
    return f"{base}/generated/{filename}"

# =========================
# Bubble Data API fetch
# =========================
def _bubble_headers() -> Dict[str, str]:
    # Bubble Admin token header is typically "Authorization: Bearer <token>"
    # If your Bubble workspace requires a different header, change here.
    return {
        "Authorization": f"Bearer {BUBBLE_API_TOKEN}",
        "Content-Type": "application/json",
    }

def fetch_all_applications(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch ALL Application objects from Bubble Data API with pagination.
    """
    if not BUBBLE_BASE_URL or not BUBBLE_API_TOKEN:
        raise RuntimeError("BUBBLE_BASE_URL / BUBBLE_API_TOKEN is not set in Render environment variables.")

    results: List[Dict[str, Any]] = []
    cursor = 0

    while True:
        url = f"{BUBBLE_DATA_API_BASE}/Application"
        params = {"cursor": cursor, "limit": limit}
        r = requests.get(url, headers=_bubble_headers(), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        chunk = data.get("response", {}).get("results", [])
        results.extend(chunk)

        remaining = data.get("response", {}).get("remaining", 0)
        if remaining is None:
            # Some Bubble setups may omit 'remaining'. If so, break when chunk smaller than limit.
            if len(chunk) < limit:
                break
        else:
            if remaining <= 0:
                break

        cursor += limit

    return results

# =========================
# Excel write logic
# =========================
def write_excel_from_applications(
    template_path: str,
    out_path: str,
    applications: List[Dict[str, Any]],
    job_id: str,
) -> Tuple[int, str]:
    """
    - Fill FIRST sheet only (per your last instruction)
    - A9부터: A열=No(1..N)
    - 나머지 컬럼은 예시로 B열에 Job No를 넣는 형태로 작성
      (원하는 매핑이 있으면 컬럼별로 확장하면 됨)
    """
    wb = load_workbook(template_path)
    ws = wb.worksheets[0]  # first sheet

    start_row = 9  # A9
    total = max(len(applications), 1)
    done = 0

    for idx, app_obj in enumerate(applications, start=1):
        r = start_row + (idx - 1)

        # ---- A열: No 자동
        ws.cell(row=r, column=1, value=idx)  # A

        # ---- 예시: B열에 Job No
        # Bubble field name이 뭔지에 따라 바꿔야 함.
        # 보통은 app_obj["job_no"] 같은 형태일 수 있음.
        # 안전하게 여러 후보를 시도:
        job_no = (
            app_obj.get("Job No")
            or app_obj.get("job_no")
            or app_obj.get("JobNo")
            or app_obj.get("jobNo")
            or ""
        )
        ws.cell(row=r, column=2, value=job_no)  # B

        done += 1
        progress = int(done * 100 / total)

        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "processing"
                _jobs[job_id]["progress_percent"] = progress
                _jobs[job_id]["done_rows"] = done
                _jobs[job_id]["total_rows"] = total

    wb.save(out_path)
    return len(applications), out_path

# =========================
# Background worker
# =========================
def _run_refresh_all(job_id: str, filename_base: str):
    try:
        with _jobs_lock:
            _jobs[job_id]["status"] = "fetching"
            _jobs[job_id]["progress_percent"] = 1

        apps = fetch_all_applications(limit=100)

        safe_base = "".join(c for c in filename_base if c.isalnum() or c in ("-", "_"))
        if not safe_base:
            safe_base = "IBGC_Application"

        out_filename = f"{safe_base}.xlsx"
        out_path = os.path.join(GENERATED_DIR, out_filename)

        with _jobs_lock:
            _jobs[job_id]["status"] = "writing"
            _jobs[job_id]["progress_percent"] = 5

        row_count, _ = write_excel_from_applications(
            template_path=TEMPLATE_PATH,
            out_path=out_path,
            applications=apps,
            job_id=job_id,
        )

        file_url = f"{PUBLIC_BASE_URL}/generated/{out_filename}" if PUBLIC_BASE_URL else f"/generated/{out_filename}"

        with _jobs_lock:
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["progress_percent"] = 100
            _jobs[job_id]["file_url"] = file_url
            _jobs[job_id]["filename"] = out_filename
            _jobs[job_id]["row_count"] = row_count
            _jobs[job_id]["finished_at"] = _now_iso()

            _latest.update({
                "status": "done",
                "file_url": file_url if file_url.startswith("http") else (f"{PUBLIC_BASE_URL}{file_url}" if PUBLIC_BASE_URL else file_url),
                "filename": out_filename,
                "created_at": _jobs[job_id]["finished_at"],
                "row_count": row_count,
            })

    except Exception as e:
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["progress_percent"] = 0
            _jobs[job_id]["error"] = str(e)

# =========================
# Routes
# =========================
@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/excel/refresh_all")
def excel_refresh_all():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    filename = body.get("filename") or f"IBGC_Application_{datetime.now().strftime('%y%m%d')}"

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "progress_percent": 0,
            "file_url": "",
            "filename": "",
            "row_count": 0,
            "total_rows": 0,
            "done_rows": 0,
            "error": "",
            "created_at": _now_iso(),
            "finished_at": "",
        }

    t = threading.Thread(target=_run_refresh_all, args=(job_id, filename), daemon=True)
    t.start()

    return jsonify({"job_id": job_id, "status": "queued"})

@app.get("/excel/status/<job_id>")
def excel_status(job_id: str):
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401

    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job:
        return jsonify({"error": "job not found", "status": "failed", "progress_percent": 0}), 404

    # Ensure absolute file_url
    file_url = job.get("file_url", "")
    if file_url and not file_url.startswith("http"):
        if PUBLIC_BASE_URL:
            file_url = f"{PUBLIC_BASE_URL}{file_url}"
        else:
            file_url = _public_file_url(job.get("filename", ""))

    return jsonify({
        "job_id": job_id,
        "status": job.get("status", ""),
        "progress_percent": job.get("progress_percent", 0),
        "file_url": file_url,
        "filename": job.get("filename", ""),
        "row_count": job.get("row_count", 0),
        "total_rows": job.get("total_rows", 0),
        "done_rows": job.get("done_rows", 0),
        "error": job.get("error", ""),
    })

@app.get("/excel/latest")
def excel_latest():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401

    # If still empty (server restarted), try to find newest file from directory
    if not _latest.get("file_url"):
        try:
            files = [f for f in os.listdir(GENERATED_DIR) if f.lower().endswith(".xlsx")]
            if files:
                files.sort(key=lambda f: os.path.getmtime(os.path.join(GENERATED_DIR, f)), reverse=True)
                f0 = files[0]
                url = f"{PUBLIC_BASE_URL}/generated/{f0}" if PUBLIC_BASE_URL else _public_file_url(f0)
                _latest.update({
                    "status": "done",
                    "file_url": url,
                    "filename": f0,
                    "created_at": _now_iso(),
                    "row_count": 0,
                })
        except Exception:
            pass

    return jsonify(_latest)

@app.get("/generated/<path:filename>")
def download_generated(filename: str):
    # Serve with correct headers
    file_path = os.path.join(GENERATED_DIR, filename)
    if not os.path.isfile(file_path):
        abort(404)

    # as_attachment=True forces download
    return send_from_directory(GENERATED_DIR, filename, as_attachment=True, download_name=filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
