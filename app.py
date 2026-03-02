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

BUBBLE_BASE_URL = os.environ.get("BUBBLE_BASE_URL", "").rstrip("/")  # e.g. https://ibgc.co.kr or https://ibgc.co.kr/version-test
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
def require_api_key(req) -> bool:
    # if EXCEL_API_KEY not set, allow all
    if not EXCEL_API_KEY:
        return True
    return req.headers.get("X-API-Key") == EXCEL_API_KEY


def now_kst():
    return datetime.now(KST)


def today_label():
    return now_kst().strftime("IBGC_Application_%Y%m%d.xlsx")


def bubble_headers():
    return {
        "Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}",
        "Content-Type": "application/json",
    }


def bubble_ok(res: requests.Response) -> bool:
    # Bubble sometimes returns 200/201
    return res.status_code in (200, 201)


def normalize_bubble_file_url(u: str) -> str:
    # Bubble fileupload often returns //cdn.bubble.io/...
    if not u:
        return u
    if u.startswith("//"):
        return "https:" + u
    return u


# =========================
# BUBBLE DATA API HELPERS
# =========================
def get_all_applications():
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}"
    res = requests.get(url, headers=bubble_headers(), timeout=60)
    if not bubble_ok(res):
        raise Exception(f"Bubble fetch error: {res.status_code} {res.text}")
    return res.json().get("response", {}).get("results", [])


def bubble_create_daily_excel_minimal(label: str, source_count: int):
    """
    Create DailyExcel with status=generating first (no file yet)
    Returns Bubble thing id.
    """
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"
    payload = {
        "label": label,
        "source_count": source_count,
        "status": "generating",
    }
    res = requests.post(url, headers=bubble_headers(), json=payload, timeout=60)
    if not bubble_ok(res):
        raise Exception(f"Bubble create error: {res.status_code} {res.text}")

    data = res.json()
    # Data API returns {"status":"success","id":"..."}
    thing_id = data.get("id") or data.get("response", {}).get("id")
    if not thing_id:
        raise Exception(f"Bubble create unexpected response: {data}")
    return thing_id


def bubble_update_daily_excel(thing_id: str, fields: dict):
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel/{thing_id}"
    res = requests.patch(url, headers=bubble_headers(), json=fields, timeout=60)
    if not bubble_ok(res):
        raise Exception(f"Bubble update error: {res.status_code} {res.text}")
    return res.json()


def bubble_get_latest_daily_excel():
    # IMPORTANT: Bubble built-in field name is "Created Date"
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel?sort_field=Created%20Date&descending=true&limit=1"
    res = requests.get(url, headers=bubble_headers(), timeout=60)
    if not bubble_ok(res):
        raise Exception(f"Bubble latest error: {res.status_code} {res.text}")
    results = res.json().get("response", {}).get("results", [])
    return results[0] if results else None


def bubble_file_upload(file_path: str) -> str:
    """
    Upload local file to Bubble storage via /fileupload.
    Returns a URL (normalized to https:// if needed).
    """
    if not BUBBLE_BASE_URL:
        raise Exception("BUBBLE_BASE_URL not set")
    if not BUBBLE_DATA_API_TOKEN:
        raise Exception("BUBBLE_DATA_API_TOKEN not set")

    # Bubble's /fileupload needs Authorization Bearer too.
    headers = {"Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}"}
    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        res = requests.post(BUBBLE_FILEUPLOAD_URL, headers=headers, files=files, timeout=120)

    if not bubble_ok(res):
        raise Exception(f"Bubble fileupload error: {res.status_code} {res.text}")

    # Response is usually plain text: //cdn.bubble.io/...
    uploaded_url = res.text.strip().strip('"')
    uploaded_url = normalize_bubble_file_url(uploaded_url)

    if not uploaded_url.startswith("http"):
        raise Exception(f"Unexpected fileupload response: {res.text}")

    return uploaded_url


# =========================
# EXCEL GENERATION
# =========================
def generate_excel_file(applications):
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
    return file_path, filename


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

    thing_id = None
    try:
        applications = get_all_applications()
        file_path, label = generate_excel_file(applications)

        # 1) create DailyExcel first -> status generating
        thing_id = bubble_create_daily_excel_minimal(label=label, source_count=len(applications))

        # 2) upload file to Bubble storage
        bubble_file_url = bubble_file_upload(file_path)

        # 3) update DailyExcel -> status ready + file fields
        bubble_update_daily_excel(
            thing_id,
            {
                "status": "ready",
                "file": bubble_file_url,      # file field (type: file)
                "file_url": bubble_file_url,  # text field
            },
        )

        return jsonify(
            {
                "ok": True,
                "bubble_id": thing_id,
                "bubble_status": "success",
                "file_url": bubble_file_url,
                "label": label,
                "source_count": len(applications),
            }
        )

    except Exception as e:
        # if we already created a record, mark failed
        try:
            if thing_id:
                bubble_update_daily_excel(thing_id, {"status": "failed", "note": str(e)})
        except Exception:
            pass

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
        latest = bubble_get_latest_daily_excel()
        if not latest:
            return jsonify({"ok": False, "error": "No file found"}), 404

        return jsonify(
            {
                "ok": True,
                "bubble_id": latest.get("_id"),
                "status": latest.get("status"),
                "file_url": latest.get("file") or latest.get("file_url"),
                "label": latest.get("label"),
                "created_at": latest.get("Created Date"),
                "source_count": latest.get("source_count"),
            }
        )

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    # Optional: still allow downloading from Render disk (not required if Bubble storage is used)
    return send_from_directory(GENERATED_DIR, filename, as_attachment=True)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
