import os
import uuid
from copy import copy

from flask import Flask, request, jsonify, send_from_directory
from openpyxl import load_workbook

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ✅ 템플릿 파일명 (repo에 올린 파일명에 맞추세요)
TEMPLATE_FILENAME = "IBGC_Application_Template.xlsx"
TEMPLATE_PATH = os.path.join(BASE_DIR, TEMPLATE_FILENAME)

# Render free 환경: 쓰기 가능한 위치는 /tmp
OUT_DIR = "/tmp/generated"
os.makedirs(OUT_DIR, exist_ok=True)


@app.get("/health")
def health():
    return "ok", 200


def _remove_external_links_if_possible(wb):
    """
    Excel 복구 메시지의 원인: /xl/externalLinks/externalLink*.xml
    openpyxl 저장 과정에서 외부 링크 파트가 깨질 수 있어,
    가능한 선에서 외부 링크/관계(rels)를 제거해 경고를 줄입니다.
    (완전한 해결은 템플릿에서 링크 끊기가 가장 확실)
    """
    try:
        # openpyxl 내부 external links 제거
        if hasattr(wb, "_external_links"):
            wb._external_links = []
    except Exception:
        pass

    try:
        # workbook relationship에서 externalLink 관련 rel 제거
        if hasattr(wb, "_rels") and wb._rels:
            to_remove = []
            for r in wb._rels:
                # r.type 예: ".../relationships/externalLink"
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
    """
    rows_text:
      - 줄바꿈(\n) = 행
      - | = 열 구분자
    """
    rows_text = (rows_text or "").strip()
    if not rows_text:
        return

    rows = rows_text.split("\n")
    for i, row in enumerate(rows):
        cols = row.split("|")
        for j, value in enumerate(cols):
            c = ws.cell(row=start_row + i, column=j + 1)
            c.value = value

            # ✅ 템플릿 서식에 취소선/밑줄이 있으면 값 넣어도 그대로 따라오므로 강제 제거
            if clear_strike_and_underline:
                try:
                    f = copy(c.font)  # 폰트 복사
                    # strike False, underline None 로 정리 (다른 폰트 속성은 유지)
                    c.font = f.copy(strike=False, underline=None)
                except Exception:
                    pass


@app.post("/excel/build")
def build_excel():
    data = request.get_json(force=True) or {}

    if not os.path.exists(TEMPLATE_PATH):
        return jsonify({"error": f"Template not found: {TEMPLATE_FILENAME}"}), 500

    # keep_links=False가 기본이지만, external link로 인한 경고가 생기는 경우가 있어
    # 아래에서 외부 링크 제거 시도를 추가로 합니다.
    wb = load_workbook(TEMPLATE_PATH)

    # ✅ 외부 링크 제거 시도 (복구 메시지 줄이기)
    _remove_external_links_if_possible(wb)

    # ---- 어떤 시트에 쓸지 선택 ----
    # 1) sheet_name 우선
    sheet_name = data.get("sheet_name")
    # 2) sheet_index (0부터 시작)
    sheet_index = data.get("sheet_index")

    if sheet_name and sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    elif isinstance(sheet_index, int) and 0 <= sheet_index < len(wb.worksheets):
        ws = wb.worksheets[sheet_index]
    else:
        ws = wb.active  # 기본: active 시트

    # ---- 입력값 ----
    rows_text = data.get("rows_text", "")
    start_row = int(data.get("start_row", 25))

    _write_rows(ws, start_row=start_row, rows_text=rows_text, clear_strike_and_underline=True)

    # 저장
    file_id = str(uuid.uuid4())
    filename = f"{file_id}.xlsx"
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
