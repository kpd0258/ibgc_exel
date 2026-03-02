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

GENERATED_DIR = "generated"
os.makedirs(GENERATED_DIR, exist_ok=True)


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


def parse_bubble_dt(value):
    """
    Bubble date format can vary. We'll attempt ISO parse; if fails, return None.
    """
    if not value:
        return None
    try:
        # common: "2026-03-02T14:56:00.000Z" or similar
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


# =========================
# BUBBLE DATA API HELPERS
# =========================

def bubble_headers():
    return {
        "Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}",
        "Content-Type": "application/json",
    }


def bubble_get(url):
    res = requests.get(url, headers=bubble_headers(), timeout=60)
    return res


def bubble_post(url, payload):
    res = requests.post(url, headers=bubble_headers(), json=payload, timeout=60)
    return res


def get_all_applications():
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}"
    res = bubble_get(url)
    if res.status_code != 200:
        raise Exception(f"Bubble fetch error: {res.status_code} {res.text}")

    return res.json().get("response", {}).get("results", [])


def create_daily_excel_record(file_url, label, source_count, status="ready"):
    """
    DailyExcel fields (as per your Bubble screenshot):
    - file (file)          -> we won't set via URL string
    - file_url (text)      -> set this
    - label (text)
    - source_count (number)
    - status (text)
    """
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"

    payload = {
        "file_url": file_url,
        "label": label,
        "source_count": source_count,
        "status": status,
    }

    res = bubble_post(url, payload)

    # Bubble can return 200 or 201 on create
    if res.status_code not in (200, 201):
        raise Exception(f"Bubble create error: {res.status_code} {res.text}")

    # Some Bubble responses are simple {"status":"success","id":"..."}
    try:
        return res.json()
    except Exception:
        return {"raw": res.text}


def get_latest_daily_excel():
    """
    Avoid relying on Bubble 'sort_field' (can break with Created Date naming).
    We'll fetch a chunk and pick latest on server side.
    """
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel?limit=100"
    res = bubble_get(url)
    if res.status_code != 200:
        raise Exception(f"Bubble latest error: {res.status_code} {res.text}")

    results = res.json().get("response", {}).get("results", [])
    if not results:
        return None

    def get_created_dt(item):
        # Bubble sometimes returns "Created Date" or "created_date"
        v = item.get("Created Date") or item.get("created_date") or item.get("created_at")
        dt = parse_bubble_dt(v)
        return dt or datetime.min.replace(tzinfo=timezone.utc)

    results.sort(key=get_created_dt, reverse=True)
    return results[0]


# =========================
# EXCEL GENERATION
# =========================

def generate_excel_file():
    applications = get_all_applications()

    if not os.path.exists(TEMPLATE_PATH):
        raise Exception(f"Template file not found: {TEMPLATE_PATH}")

    wb = load_workbook(TEMPLATE_PATH)
    ws = wb.active

    # example: write from row 9
    start_row = 9
    row = start_row

    for idx, app_data in enumerate(applications, start=1):
        ws[f"A{row}"] = idx
        ws[f"B{row}"] = app_data.get("company", "")
        ws[f"C{row}"] = app_data.get("iso", "")
        row += 1

    filename = today_label()
    file_path = os.path.join(GENERATED_DIR, filename)
    wb.save(file_path)

    # Public download URL from this service
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

        # status까지 확실히 저장
        bubble_res = create_daily_excel_record(
            file_url=file_url,
            label=label,
            source_count=source_count,
            status="ready",
        )

        return jsonify({
            "ok": True,
            "file_url": file_url,
            "label": label,
            "source_count": source_count,
            "bubble_status": bubble_res.get("status", "success"),
            "bubble_id": bubble_res.get("id"),
        })

    except Exception as e:
        # 실패 케이스도 status 기록하고 싶으면 여기서 Bubble에 error 레코드 남기게 확장 가능
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/excel/refresh_now", methods=["POST"])
def excel_refresh_now():
    # same behavior
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
            "file_url": latest.get("file_url") or latest.get("file"),
            "label": latest.get("label"),
            "status": latest.get("status"),
            "source_count": latest.get("source_count"),
            "created_at": latest.get("Created Date") or latest.get("created_date"),
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    # Serve from generated folder
    return send_from_directory(GENERATED_DIR, filename, as_attachment=True)


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
