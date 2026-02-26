import os
import uuid
import re
from copy import copy

from flask import Flask, request, jsonify, send_from_directory
from openpyxl import load_workbook

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_FILENAME = "IBGC_Application_Template.xlsx"
TEMPLATE_PATH = os.path.join(BASE_DIR, TEMPLATE_FILENAME)

OUT_DIR = "/tmp/generated"
os.makedirs(OUT_DIR, exist_ok=True)


@app.get("/health")
def health():
    return "ok", 200


def safe_filename(name: str) -> str:
    """
    파일명에 사용할 수 없는 문자 제거
    """
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    return name.strip()


def _remove_external_links_if_possible(wb):
    try:
        if hasattr(wb, "_external_links"):
            wb._external_links = []
    except:
        pass


def _write_rows(ws, start_row: int, rows_text: str):
    rows_text = (rows_text or "").strip()
    if not rows_text:
        return

    rows = rows_text.split("\n")
    for i, row in enumerate(rows):
        cols = row.split("|")
        for j, value in enumerate(cols):
            c = ws.cell(row=start_row + i, column=j + 1)
            c.value = value

            # 취소선/밑줄 제거
            try:
                f = copy(c.font)
                c.font = f.copy(strike=False, underline=None)
            except:
                pass


@app.post("/excel/build")
def build_excel():
    data = request.get_json(force=True) or {}

    if not os.path.exists(TEMPLATE_PATH):
        return jsonify({"error": "Template not found"}), 500

    wb = load_workbook(TEMPLATE_PATH)
    _remove_external_links_if_possible(wb)

    # -------------------------
    # 시트 선택
    # -------------------------
    sheet_name = data.get("sheet_name")
    sheet_index = data.get("sheet_index")

    if sheet_name and sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    elif isinstance(sheet_index, int) and 0 <= sheet_index < len(wb.worksheets):
        ws = wb.worksheets[sheet_index]
    else:
        ws = wb.active

    # -------------------------
    # 데이터 입력
    # -------------------------
    rows_text = data.get("rows_text", "")
    start_row = int(data.get("start_row", 9))

    _write_rows(ws, start_row=start_row, rows_text=rows_text)

    # -------------------------
    # ✅ 파일명 지정 기능
    # -------------------------
    custom_filename = data.get("filename")

    if custom_filename:
        custom_filename = safe_filename(custom_filename)
        if not custom_filename.lower().endswith(".xlsx"):
            custom_filename += ".xlsx"
        filename = custom_filename
    else:
        filename = f"{uuid.uuid4()}.xlsx"

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
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
