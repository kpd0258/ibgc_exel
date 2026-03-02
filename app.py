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
GENERATED_DIR = os.environ.get("GENERATED_DIR", "generated")


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
        "Content-Type": "application/json"
    }


def get_all_applications():
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}"
    res = requests.get(url, headers=bubble_headers())
    if res.status_code != 200:
        raise Exception(f"Bubble fetch error: {res.text}")
    return res.json().get("response", {}).get("results", [])


def create_daily_excel_record(file_url, label):
    """
    DailyExcel 타입에 file, label만 저장.
    (status는 환경/필드명 이슈 분리를 위해 일단 제거)
    """
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"
    payload = {
        "file": file_url,
        "label": label
    }
    res = requests.post(url, headers=bubble_headers(), json=payload)
    if res.status_code != 200:
        raise Exception(f"Bubble create error: {res.text}")
    return res.json()


def get_latest_daily_excel():
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel?sort_field=Created Date&descending=true&limit=1"
    res = requests.get(url, headers=bubble_headers())
    if res.status_code != 200:
        raise Exception(f"Bubble latest error: {res.text}")
    results = res.json().get("response", {}).get("results", [])
    return results[0] if results else None


# =========================
# EXCEL GENERATION
# =========================

def generate_excel_file():
    applications = get_all_applications()

    if not os.path.exists(TEMPLATE_PATH):
        raise Exception("Template file not found")

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
    os.makedirs(GENERATED_DIR, exist_ok=True)

    file_path = os.path.join(GENERATED_DIR, filename)
    wb.save(file_path)

    # Render에서 접근 가능한 다운로드 URL 생성
    public_url = f"{request.host_url.rstrip('/')}/download/{filename}"
    return public_url, filename


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
        file_url, label = generate_excel_file()
        create_daily_excel_record(file_url, label)

        return jsonify({
            "ok": True,
            "file_url": file_url,
            "label": label
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/excel/refresh_now", methods=["POST"])
def excel_refresh_now():
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
            "file_url": latest.get("file"),
            "label": latest.get("label"),
            "created_at": latest.get("Created Date")
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    # generated 폴더에서 직접 서빙
    return send_from_directory(GENERATED_DIR, filename, as_attachment=True)


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
