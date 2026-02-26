@app.post("/excel/build")
def build_excel():
    data = request.get_json(force=True) or {}

    if not os.path.exists(TEMPLATE_PATH):
        return jsonify({"error": "template.xlsx not found"}), 500

    wb = load_workbook(TEMPLATE_PATH)

    sheets = data.get("sheets")

    # ✅ (A) 단일 시트 모드: rows_text + sheet_name(optional)
    if not sheets:
        rows_text = (data.get("rows_text") or "").strip()
        start_row = int(data.get("start_row", 25))
        sheet_name = data.get("sheet_name")  # optional

        ws = wb[sheet_name] if sheet_name else wb.active

        if rows_text:
            for i, row in enumerate(rows_text.split("\n")):
                cols = row.split("|")
                for j, value in enumerate(cols):
                    ws.cell(row=start_row + i, column=j + 1).value = value

    # ✅ (B) 멀티 시트 모드: sheets[]
    else:
        for s in sheets:
            name = s.get("name")
            rows_text = (s.get("rows_text") or "").strip()
            start_row = int(s.get("start_row", 25))

            if not name or name not in wb.sheetnames:
                continue  # 시트명 없거나 틀리면 스킵

            ws = wb[name]

            if rows_text:
                for i, row in enumerate(rows_text.split("\n")):
                    cols = row.split("|")
                    for j, value in enumerate(cols):
                        ws.cell(row=start_row + i, column=j + 1).value = value

    # 저장/다운로드는 기존 그대로
    file_id = str(uuid.uuid4())
    filename = f"{file_id}.xlsx"
    output_path = os.path.join(OUT_DIR, filename)
    wb.save(output_path)

    file_url = f"{request.host_url}generated/{filename}"
    return jsonify({"file_url": file_url})
