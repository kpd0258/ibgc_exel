import os
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request
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
GENERATED_DIR = os.environ.get("GENERATED_DIR", "generated")

BUBBLE_DATA_API_BASE = f"{BUBBLE_BASE_URL}/api/1.1/obj"
BUBBLE_FILEUPLOAD_URL = f"{BUBBLE_BASE_URL}/fileupload"

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


def bubble_headers_json():
    return {
        "Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}",
        "Content-Type": "application/json"
    }


def bubble_headers_auth_only():
    # fileupload는 multipart라 Content-Type을 requests가 잡게 두는게 안전
    return {
        "Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}"
    }


# =========================
# BUBBLE DATA API HELPERS
# =========================
def get_all_applications():
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}"
    res = requests.get(url, headers=bubble_headers_json())
    if res.status_code != 200:
        raise Exception(f"Bubble fetch error: {res.status_code} {res.text}")
    return res.json().get("response", {}).get("results", [])


def create_daily_excel_record(file_url, label, source_count, status="ready"):
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"
    payload = {
        "file": file_url,       # Bubble file field
        "file_url": file_url,   # text backup
        "label": label,
        "status": status,
        "source_count": source_count
    }
    res = requests.post(url, headers=bubble_headers_json(), json=payload)
    if res.status_code != 200:
        raise Exception(f"Bubble create error: {res.status_code} {res.text}")
    return res.json()


def get_latest_daily_excel():
    # Bubble Data API는 정렬 필드명이 "Created Date"인 경우가 많음 (UI의 Create Date)
    # 안전하게: sort_field=Created Date 로 시도하고, 안되면 그냥 latest 1개만 가져옴
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel?sort_field=Created%20Date&descending=true&limit=1"
    res = requests.get(url, headers=bubble_headers_json())
    if res.status_code != 200:
        # fallback
        url2 = f"{BUBBLE_DATA_API_BASE}/DailyExcel?descending=true&limit=1"
        res2 = requests.get(url2, headers=bubble_headers_json())
        if res2.status_code != 200:
            raise Exception(f"Bubble latest error: {res.status_code} {res.text} / fallback: {res2.status_code} {res2.text}")
        results = res2.json().get("response", {}).get("results", [])
        return results[0] if results else None

    results = res.json().get("response", {}).get("results", [])
    return results[0] if results else None


# =========================
# BUBBLE FILE UPLOAD
# =========================
def upload_file_to_bubble(file_path, filename):
    """
    Bubble에 파일 자체를 업로드하고, Bubble이 반환하는 파일 URL을 받는다.
    """
    if not BUBBLE_BASE_URL:
        raise Exception("BUBBLE_BASE_URL is not set")
    if not BUBBLE_DATA_API_TOKEN:
        raise Exception("BUBBLE_DATA_API_TOKEN is not set")

    with open(file_path, "rb") as f:
        files = {
            "file": (filename, f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        }
        res = requests.post(
            BUBBLE_FILEUPLOAD_URL,
            headers=bubble_headers_auth_only(),
            files=files,
            timeout=120
        )

    # Bubble /fileupload는 보통 plain text로 파일 URL을 돌려준다.
    if res.status_code != 200:
        raise Exception(f"Bubble fileupload error: {res.status_code} {res.text}")

    bubble_file_url = res.text.strip().strip('"')
    if not bubble_file_url.startswith("http"):
        raise Exception(f"Unexpected fileupload response: {res.text}")

    return bubble_file_url


# =========================
# EXCEL GENERATION
# =========================
def generate_excel_file_to_disk():
    applications = get_all_applications()
    source_count = len(applications)

    if not os.path.exists(TEMPLATE_PATH):
        raise Exception("Template file not found")

    wb = load_workbook(TEMPLATE_PATH)
    ws = wb.active

    start_row = 9
    row = start_row

    for idx, app_data in enumerate(applications, start=1):
        ws[f"A{row}"] = idx
        ws[f"B{row}"] = app_data.get("company", "")
        ws[f"C{row}"] = app_data.get("iso", "")
        row += 1

    filename = today_label()
    os.makedirs(GENERATED_DIR, exist_ok=True)
    file_path = os.path.join(GENERATED_DIR, filename)
    wb.save(file_path)

    return file_path, filename, source_count


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
        # 1) disk에 생성
        file_path, filename, source_count = generate_excel_file_to_disk()

        # 2) Bubble로 업로드 (여기서 Bubble 저장소에 실제 파일 들어감)
        bubble_file_url = upload_file_to_bubble(file_path, filename)

        # 3) Bubble DB에 레코드 생성 (file 필드에 bubble url)
        created = create_daily_excel_record(
            file_url=bubble_file_url,
            label=filename,
            source_count=source_count,
            status="ready"
        )

        return jsonify({
            "ok": True,
            "bubble_id": created.get("id"),
            "bubble_status": created.get("status", "success"),
            "file_url": bubble_file_url,
            "label": filename,
            "source_count": source_count
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
            "file_url": latest.get("file") or latest.get("file_url"),
            "label": latest.get("label"),
            "status": latest.get("status"),
            "created_at": latest.get("Created Date") or latest.get("created_date")
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
