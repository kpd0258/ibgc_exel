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
        "Content-Type": "application/json"
    }


def get_all_applications():
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}"
    res = requests.get(url, headers=bubble_headers())
    if res.status_code != 200:
        raise Exception(f"Bubble fetch error: {res.text}")
    return res.json().get("response", {}).get("results", [])


def create_daily_excel_record(file_url, label, source_count):
    """
    DailyExcel 타입에 저장
    - file: (Bubble file URL)
    - file_url: text (원하면 같이 저장)
    - label: 파일명
    - source_count: 이번에 넣은 row 수
    - status: text (ready)
    """
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"
    payload = {
        "file": file_url,
        "file_url": file_url,
        "label": label,
        "source_count": source_count,
        "status": "ready"
    }
    res = requests.post(url, headers=bubble_headers(), json=payload)

    # Bubble은 생성 성공 시 200 + JSON을 주는 경우가 많지만,
    # 환경/설정에 따라 201/204 등도 가능하므로 넓게 허용
    if res.status_code not in (200, 201, 204):
        raise Exception(f"Bubble create error: {res.text}")

    # 204면 body가 비어있을 수 있음
    if res.status_code == 204:
        return {"status": "success", "id": None}

    return res.json()


def get_latest_daily_excel():
    # Bubble built-in 필드명은 "Created Date"를 쓰는 게 안전함
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel?sort_field=Created Date&descending=true&limit=1"
    res = requests.get(url, headers=bubble_headers())
    if res.status_code != 200:
        raise Exception(f"Bubble latest error: {res.status_code} {res.text}")
    results = res.json().get("response", {}).get("results", [])
    return results[0] if results else None


# =========================
# EXCEL GENERATION
# =========================

def _get_field(app_data: dict, key: str, default=""):
    """
    Bubble Data API가 필드 키를 그대로 내려준다는 가정 하에,
    안전하게 .get() 처리
    """
    val = app_data.get(key, default)
    if val is None:
        return default
    return val


def generate_excel_file():
    applications = get_all_applications()

    if not os.path.exists(TEMPLATE_PATH):
        raise Exception(f"Template file not found: {TEMPLATE_PATH}")

    wb = load_workbook(TEMPLATE_PATH)

    # 템플릿의 "첫 번째 시트"를 1번 시트로 간주
    # (시트 이름을 하드코딩하지 않음)
    if len(wb.worksheets) < 1:
        raise Exception("Template has no worksheets")

    ws1 = wb.worksheets[0]

    # ===== 1번 시트 채우기 규칙 =====
    # 시작행: 9
    # 1열(A): index (1부터)
    # 2열(B): 00. Application의 In_charge_of
    # 3열(C): 00. Application의 recommend (비면 "IBGC")
    # 4열(D): 00. Application의 JOB_NO
    start_row = 9
    row = start_row

    for idx, app_data in enumerate(applications, start=1):
        in_charge_of = _get_field(app_data, "In_charge_of", "")
        recommend = _get_field(app_data, "recommend", "")
        job_no = _get_field(app_data, "JOB_NO", "")

        if not str(recommend).strip():
            recommend = "IBGC"

        ws1[f"A{row}"] = idx
        ws1[f"B{row}"] = in_charge_of
        ws1[f"C{row}"] = recommend
        ws1[f"D{row}"] = job_no

        row += 1

    source_count = len(applications)

    filename = today_label()
    generated_dir = "generated"
    os.makedirs(generated_dir, exist_ok=True)

    file_path = os.path.join(generated_dir, filename)
    wb.save(file_path)

    # 다운로드 URL
    public_url = f"{request.host_url.rstrip('/')}/download/{filename}"
    return public_url, filename, source_count


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
        bubble_res = create_daily_excel_record(file_url, label, source_count)

        return jsonify({
            "ok": True,
            "file_url": file_url,
            "label": label,
            "source_count": source_count,
            "bubble_status": bubble_res.get("status", "success"),
            "bubble_id": bubble_res.get("id")
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
            "created_at": latest.get("Created Date"),
            "status": latest.get("status"),
            "source_count": latest.get("source_count")
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
