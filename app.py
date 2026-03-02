import os
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from openpyxl import load_workbook

app = Flask(__name__)
CORS(app)

# =========================
# ENV VARIABLES
# =========================
EXCEL_API_KEY = os.environ.get("EXCEL_API_KEY", "")

BUBBLE_BASE_URL = os.environ.get("BUBBLE_BASE_URL", "").rstrip("/")
BUBBLE_DATA_API_TOKEN = os.environ.get("BUBBLE_DATA_API_TOKEN", "")
BUBBLE_APP_TYPE = os.environ.get("BUBBLE_APP_TYPE", "00. Application")

TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", "IBGC_Application_Template.xlsx")

BUBBLE_DATA_API_BASE = f"{BUBBLE_BASE_URL}/api/1.1/obj"

KST = timezone(timedelta(hours=9))


# =========================
# UTIL
# =========================
def require_api_key(req):
    if not EXCEL_API_KEY:
        return True
    return req.headers.get("X-API-Key") == EXCEL_API_KEY


def now_kst():
    return datetime.now(KST)


def today_label():
    return now_kst().strftime("IBGC_Application_%Y%m%d.xlsx")


# =========================
# BUBBLE DATA API HELPERS
# =========================
def bubble_headers():
    return {
        "Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}",
        "Content-Type": "application/json",
    }


def _ensure_success(res, context: str):
    # Bubble Data API: create는 201, read는 200이 흔함
    if res.status_code not in (200, 201):
        raise Exception(f"{context}: {res.status_code} {res.text}")


def get_all_applications():
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}"
    res = requests.get(url, headers=bubble_headers())
    _ensure_success(res, "Bubble fetch error")
    return res.json().get("response", {}).get("results", [])


def create_daily_excel_record(file_url, label, source_count):
    # DailyExcel 타입에 저장
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"

    # ⚠️ dev/live 스키마 불일치 이슈가 있어서 status는 일단 빼고 안정화
    payload = {
        "file": file_url,       # Bubble file 필드(Url string 저장 가능)
        "file_url": file_url,   # text 필드
        "label": label,         # text
        "source_count": source_count,  # number
    }

    res = requests.post(url, headers=bubble_headers(), json=payload)
    _ensure_success(res, "Bubble create error")

    return res.json()


def get_latest_daily_excel():
    # Bubble Data API의 정렬 키는 보통 created_date / modified_date 를 사용
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel?sort_field=Created%20Date&descending=true&limit=1"
    res = requests.get(url, headers=bubble_headers())
    _ensure_success(res, "Bubble latest error")
    results = res.json().get("response", {}).get("results", [])
    return results[0] if results else None


# =========================
# EXCEL GENERATION
# =========================
def generate_excel_file():
    applications = get_all_applications()

    if not os.path.exists(TEMPLATE_PATH):
        raise Exception(f"Template file not found: {TEMPLATE_PATH}")

    wb = load_workbook(TEMPLATE_PATH)
    ws = wb.active

    start_row = 9
    row = start_row

    for idx, app_data in enumerate(applications, start=1):
        ws[f"A{row}"] = idx  # No
        ws[f"B{row}"] = app_data.get("company", "")
        ws[f"C{row}"] = app_data.get("iso", "")
        row += 1

    filename = today_label()
    generated_dir = "generated"
    os.makedirs(generated_dir, exist_ok=True)

    file_path = os.path.join(generated_dir, filename)
    wb.save(file_path)

    public_url = f"{request.host_url.rstrip('/')}/download/{filename}"
    return public_url, filename, len(applications)


# =========================
# ROUTES
# =========================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/excel/generate_daily", methods=["POST"])
def excel_generate_daily():
    if not require_api_key(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    try:
        file_url, label, source_count = generate_excel_file()
        bubble_result = create_daily_excel_record(file_url, label, source_count)

        return jsonify({
            "ok": True,
            "file_url": file_url,
            "label": label,
            "source_count": source_count,
            "bubble": bubble_result,  # Bubble이 준 id 등을 같이 반환
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/excel/refresh_now", methods=["POST"])
def excel_refresh_now():
    # 같은 로직 재사용
    return excel_generate_daily()


@app.route("/excel/latest", methods=["GET"])
def excel_latest():
    if not require_api_key(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    try:
        latest = get_latest_daily_excel()
        if not latest:
            return jsonify({"ok": False, "error": "No file found"}), 404

        return jsonify({
            "ok": True,
            "file_url": latest.get("file") or latest.get("file_url"),
            "label": latest.get("label"),
            "source_count": latest.get("source_count"),
            "created_at": latest.get("Created Date") or latest.get("created_date"),
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    generated_dir = os.path.join(os.getcwd(), "generated")
    return send_from_directory(generated_dir, filename, as_attachment=True)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
