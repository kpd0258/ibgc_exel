import os
import uuid
import re
from copy import copy

from flask import Flask, request, jsonify, send_from_directory
from openpyxl import load_workbook

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 템플릿 파일명 (repo에 있는 파일명과 동일해야 함)
TEMPLATE_FILENAME = "IBGC_Application_Template.xlsx"
TEMPLATE_PATH = os.path.join(BASE_DIR, TEMPLATE_FILENAME)

OUT_DIR = "/tmp/generated"
os.makedirs(OUT_DIR, exist_ok=True)


@app.get("/health")
def health():
    return "ok", 200


def safe_filename(name: str) -> str:
    # 윈도우/엑셀에서 문제되는 문자 제거
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


def _write_rows(ws, start_row: int, rows_text: str, clear_strike_and_underline: bool = True):
    rows_text = (rows_text or "").strip()
    if not rows_text:
        return

    for i, row in enumerate(rows_text.split("\n")):
        cols = row.split("|")
        for j, value in enumerate(cols):
            c = ws.cell(row=start_row + i, column=j + 1)
            c.value = value

            # 템플릿의 취소선/밑줄이 값에 따라오는 문제 방지
            if clear_strike_and_underline:
                try:
                    f = copy(c.font)
                    c.font = f.copy(strike=False, underline=None)
                except Exception:
                    pass


@app.post("/excel/build")
def build_excel():
    data = request.get_json(force=True) or {}

    if not os.path.exists(TEMPLATE_PATH):
        return jsonify({"error": f"Template not found: {TEMPLATE_FILENAME}"}), 500

    wb = load_workbook(TEMPLATE_PATH)
    _remove_external_links_if_possible(wb)

    # ✅ 파일명 지정
    custom_filename = data.get("filename")
    if custom_filename:
        custom_filename = safe_filename(custom_filename)
        if not custom_filename.lower().endswith(".xlsx"):
            custom_filename += ".xlsx"
        filename = custom_filename
    else:
        filename = f"{uuid.uuid4()}.xlsx"

    # ✅ 멀티 시트 모드 우선 처리
    sheets = data.get("sheets")
    if isinstance(sheets, list) and len(sheets) > 0:
        for s in sheets:
            name = s.get("name")
            rows_text = s.get("rows_text", "")
            start_row = int(s.get("start_row", 9))

            if not name:
                continue

            # 시트명 매칭
            if name in wb.sheetnames:
                ws = wb[name]
            else:
                # 혹시 공백/오타 등 대비: 양끝 공백 제거 후 재시도
                name2 = str(name).strip()
                if name2 in wb.sheetnames:
                    ws = wb[name2]
                else:
                    # 시트명이 틀리면 스킵 (조용히 넘어감)
                    continue

            _write_rows(ws, start_row=start_row, rows_text=rows_text, clear_strike_and_underline=True)

    else:
        # ✅ 단일 시트 모드 (예전 호환)
        rows_text = data.get("rows_text", "")
        start_row = int(data.get("start_row", 9))
        sheet_name = data.get("sheet_name")
        sheet_index = data.get("sheet_index")

        if sheet_name and sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
        elif isinstance(sheet_index, int) and 0 <= sheet_index < len(wb.worksheets):
            ws = wb.worksheets[sheet_index]
        else:
            ws = wb.active

        _write_rows(ws, start_row=start_row, rows_text=rows_text, clear_strike_and_underline=True)

    output_path = os.path.join(OUT_DIR, filename)
    wb.save(output_path)

    file_url = f"{request.host_url}generated/{filename}"
    return jsonify({"file_url": file_url})


@app.get("/generated/<path:filename>")
def download_file(filename):
    return send_from_directory(
        OUT_DIR,
        filename,
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
