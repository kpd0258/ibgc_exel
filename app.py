import os, uuid
from flask import Flask, request, jsonify, send_from_directory
from openpyxl import load_workbook

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(BASE_DIR, "template.xlsx")

# Render는 /tmp만 쓰기 가능(안전하게 /tmp 사용)
OUT_DIR = "/tmp/generated"
os.makedirs(OUT_DIR, exist_ok=True)

@app.get("/health")
def health():
    return "ok", 200

@app.post("/excel/build")
def build_excel():
    data = request.get_json(force=True) or {}
    rows_text = data.get("rows_text", "").strip()

    if not os.path.exists(TEMPLATE_PATH):
        return jsonify({"error": "template.xlsx not found in repo root"}), 500

    wb = load_workbook(TEMPLATE_PATH)
    ws = wb.active

    start_row = int(data.get("start_row", 25))  # 템플릿 표 시작행(필요시 조정)

    if rows_text:
        rows = rows_text.split("\n")
        for i, row in enumerate(rows):
            cols = row.split("|")
            for j, value in enumerate(cols):
                ws.cell(row=start_row + i, column=j + 1).value = value

    file_id = str(uuid.uuid4())
    filename = f"{file_id}.xlsx"
    output_path = os.path.join(OUT_DIR, filename)
    wb.save(output_path)

    # 다운로드 URL 제공
    file_url = f"{request.host_url}generated/{filename}"
    return jsonify({"file_url": file_url})

@app.get("/generated/<path:filename>")
def download_file(filename):
    return send_from_directory(OUT_DIR, filename, as_attachment=True)
