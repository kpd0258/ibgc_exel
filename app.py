import os
import re
import uuid
import threading
import time
from copy import copy
from typing import Dict, Any, List

from flask import Flask, request, jsonify, send_from_directory
from openpyxl import load_workbook

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 템플릿 파일명 (repo에 있는 파일명과 동일해야 함)
TEMPLATE_FILENAME = "IBGC_Application_Template.xlsx"
TEMPLATE_PATH = os.path.join(BASE_DIR, TEMPLATE_FILENAME)

# Render free 환경: 쓰기 가능한 위치는 /tmp
OUT_DIR = "/tmp/generated"
os.makedirs(OUT_DIR, exist_ok=True)

# ----------------------------
# Job Store (in-memory)
# Render free는 재시작/슬립 시 메모리 초기화 가능
# -> "정확한 진행률"은 실행 중에만 보장됨
# ----------------------------
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


@app.get("/health")
def health():
    return "ok", 200


def safe_filename(name: str) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "", name or "")
    return name.strip()


def _remove_external_links_if_possible(wb):
    """
    템플릿에 externalLinks가 있으면 Excel이 '복구' 메시지를 띄울 수 있음.
    완전 해결은 템플릿에서 '링크 끊기'가 정답이지만,
    서버에서도 가능한 범위에서 제거 시도(경고 감소용)
    """
    try:
        if hasattr(wb, "_external_links"):
            wb._external_links = []
    except Exception:
        pass

    try:
        if hasattr(wb, "_rels") and wb._rels:
            to_remove = []
            for r in wb._rels:
                rtype = getattr(r, "type", "") or ""
                if "externalLink" in rtype:
                    to_remove.append(r)
            for r in to_remove:
                try:
                    wb._rels.remove(r)
                except Exception:
                    pass
    except Exception:
        pass


def _sanitize_cell_value(v: str) -> str:
    """
    구분자(|) / 줄바꿈이 데이터 안에 있으면 컬럼/행 깨짐
    방어적으로 제거
    """
    if v is None:
        return ""
    s = str(v)
    s = s.replace("|", " ")
    s = s.replace("\r\n", " ")
    s = s.replace("\n", " ")
    s = s.replace("\r", " ")
    return s


def _count_rows_from_rows_text(rows_text: str) -> int:
    rows_text = (rows_text or "").strip()
    if not rows_text:
        return 0
    return len([line for line in rows_text.split("\n") if line.strip() != ""])


def _write_one_row(ws, excel_row: int, row_text: str, clear_strike_and_underline: bool = True):
    cols = row_text.split("|")
    for j, value in enumerate(cols):
        c = ws.cell(row=excel_row, column=j + 1)
        c.value = _sanitize_cell_value(value)

        # 템플릿의 취소선/밑줄이 값에 따라오는 문제 방지
        if clear_strike_and_underline:
            try:
                f = copy(c.font)
                c.font = f.copy(strike=False, underline=None)
            except Exception:
                pass


def _job_update(job_id: str, **kwargs):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(kwargs)


def _job_get(job_id: str) -> Dict[str, Any]:
    with JOBS_LOCK:
        return dict(JOBS.get(job_id, {}))


def _build_excel_background(job_id: str, payload: Dict[str, Any]):
    """
    백그라운드에서 엑셀 생성하면서 progress를 업데이트
    """
    try:
        if not os.path.exists(TEMPLATE_PATH):
            _job_update(job_id, status="failed", error=f"Template not found: {TEMPLATE_FILENAME}")
            return

        wb = load_workbook(TEMPLATE_PATH)
        _remove_external_links_if_possible(wb)

        # 파일명 결정
        custom_filename = payload.get("filename")
        if custom_filename:
            custom_filename = safe_filename(custom_filename)
            if not custom_filename.lower().endswith(".xlsx"):
                custom_filename += ".xlsx"
            filename = custom_filename
        else:
            filename = f"{uuid.uuid4()}.xlsx"

        sheets: List[Dict[str, Any]] = payload.get("sheets") or []
        if not isinstance(sheets, list) or len(sheets) == 0:
            _job_update(job_id, status="failed", error="No sheets provided")
            return

        # 총 작업량(정확 진행률) = 모든 시트의 총 행 수 합
        total_rows = 0
        per_sheet_counts = []
        for s in sheets:
            rows_text = s.get("rows_text", "")
            c = _count_rows_from_rows_text(rows_text)
            per_sheet_counts.append(c)
            total_rows += c

        if total_rows == 0:
            _job_update(job_id, status="failed", error="No rows to write (rows_text all empty)")
            return

        _job_update(job_id, status="running", total_rows=total_rows, processed_rows=0, progress_percent=0)

        processed = 0

        # 시트별로 작성
        for si, s in enumerate(sheets):
            name = (s.get("name") or "").strip()
            start_row = int(s.get("start_row", 9))
            rows_text = (s.get("rows_text") or "").strip()

            if not name:
                continue
            if name not in wb.sheetnames:
                # 시트명 불일치면 실패 처리(진짜 진행률이 필요하므로 조용히 스킵하지 않음)
                _job_update(job_id, status="failed", error=f"Sheet not found: {name}")
                return

            ws = wb[name]

            if not rows_text:
                continue

            lines = [line for line in rows_text.split("\n") if line.strip() != ""]
            for i, line in enumerate(lines):
                excel_row = start_row + i
                _write_one_row(ws, excel_row, line, clear_strike_and_underline=True)

                processed += 1
                percent = int((processed / total_rows) * 100)
                if percent > 100:
                    percent = 100

                _job_update(
                    job_id,
                    processed_rows=processed,
                    progress_percent=percent,
                    current_sheet=name,
                    current_sheet_index=si + 1,
                    current_sheet_total=len(sheets),
                )

                # 너무 빠르면 UI가 못 따라가서, 아주 약간의 여유(원하면 제거 가능)
                # time.sleep(0.001)

        output_path = os.path.join(OUT_DIR, filename)
        wb.save(output_path)

        file_url = f"{payload.get('_host_url', '').rstrip('/')}/generated/{filename}"
        _job_update(job_id, status="done", progress_percent=100, file_url=file_url, filename=filename)

    except Exception as e:
        _job_update(job_id, status="failed", error=str(e))


@app.post("/excel/start")
def excel_start():
    payload = request.get_json(force=True) or {}

    # host_url은 백그라운드에서 file_url 생성용
    payload["_host_url"] = request.host_url.rstrip("/")

    job_id = str(uuid.uuid4())
    with JOBS_LOCK:
        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "progress_percent": 0,
            "processed_rows": 0,
            "total_rows": 0,
            "file_url": None,
            "error": None,
            "filename": None,
            "created_at": int(time.time()),
        }

    t = threading.Thread(target=_build_excel_background, args=(job_id, payload), daemon=True)
    t.start()

    return jsonify({"job_id": job_id})


@app.get("/excel/status/<job_id>")
def excel_status(job_id: str):
    job = _job_get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404

    # 필요한 필드만 깔끔하게 반환
    return jsonify({
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "progress_percent": job.get("progress_percent", 0),
        "processed_rows": job.get("processed_rows", 0),
        "total_rows": job.get("total_rows", 0),
        "current_sheet": job.get("current_sheet"),
        "current_sheet_index": job.get("current_sheet_index"),
        "current_sheet_total": job.get("current_sheet_total"),
        "file_url": job.get("file_url"),
        "filename": job.get("filename"),
        "error": job.get("error"),
    })


@app.get("/generated/<path:filename>")
def download_file(filename):
    return send_from_directory(
        OUT_DIR,
        filename,
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
