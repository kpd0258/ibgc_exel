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

BUBBLE_BASE_URL = os.environ.get("BUBBLE_BASE_URL", "").rstrip("/")  # ex) https://ibgc.co.kr/version-test
BUBBLE_DATA_API_TOKEN = os.environ.get("BUBBLE_DATA_API_TOKEN", "")
BUBBLE_APP_TYPE = os.environ.get("BUBBLE_APP_TYPE", "00. Application")
TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", "IBGC_Application_Template.xlsx")

# Bubble Data API base
BUBBLE_DATA_API_BASE = f"{BUBBLE_BASE_URL}/api/1.1/obj"

# Bubble fileupload endpoint (필요하면 직접 ENV로 오버라이드 가능)
# 보통: https://yourdomain.com/version-test/fileupload
BUBBLE_FILEUPLOAD_URL = os.environ.get("BUBBLE_FILEUPLOAD_URL", f"{BUBBLE_BASE_URL}/fileupload")

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
    # 파일명은 날짜+시간까지 포함시키는 걸 추천 (중복 방지)
    return now_kst().strftime("IBGC_Application_%Y%m%d_%H%M%S.xlsx")


def bubble_headers():
    return {
        "Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}",
        "Content-Type": "application/json",
    }


def ok_status(code: int) -> bool:
    # Bubble은 create=201, update=204가 흔함
    return code in (200, 201, 204)


# =========================
# BUBBLE DATA API HELPERS
# =========================

def get_all_applications():
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}"
    res = requests.get(url, headers=bubble_headers(), timeout=60)
    if not ok_status(res.status_code):
        raise Exception(f"Bubble fetch error ({res.status_code}): {res.text}")
    return res.json().get("response", {}).get("results", [])


def create_daily_excel_record(file_bubble_url, file_url, label, status="ready", source_count=0, note=""):
    """
    DailyExcel data type fields:
      - file (file type)
      - file_url (text)
      - label (text)
      - status (text)
      - source_count (number)
      - note (text)
    """
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"
    payload = {
        "file": file_bubble_url,     # Bubble file field는 //cdn... 형태도 OK
        "file_url": file_url,        # 텍스트용(편의)
        "label": label,
        "status": status,
        "source_count": source_count,
        "note": note
    }
    res = requests.post(url, headers=bubble_headers(), json=payload, timeout=60)
    if not ok_status(res.status_code):
        raise Exception(f"Bubble create error ({res.status_code}): {res.text}")
    return res.json() if res.text else {"status": "success"}


def upload_file_to_bubble_storage(local_path: str, upload_filename: str) -> str:
    """
    Bubble의 /fileupload 는 보통 multipart로 업로드하며,
    응답이 JSON이 아니라 '문자열(URL)' 로 오는 경우가 많음.
    예: //xxxxx.cdn.bubble.io/f123/filename.xlsx
    """
    if not os.path.exists(local_path):
        raise Exception(f"File not found: {local_path}")

    with open(local_path, "rb") as f:
        files = {
            "file": (upload_filename, f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        }
        # Bubble fileupload는 Authorization이 필요없는 경우가 많지만, 환경 따라 막혀있으면 헤더 추가 가능
        # 여기서는 토큰 없이 시도(가장 일반적). 필요하면 아래 headers에 Authorization 추가.
        res = requests.post(BUBBLE_FILEUPLOAD_URL, files=files, timeout=120)

    if not ok_status(res.status_code):
        raise Exception(f"Bubble fileupload error ({res.status_code}): {res.text}")

    # 응답이 JSON이 아니라 텍스트 URL인 케이스가 일반적
    text = (res.text or "").strip()

    # 혹시 JSON으로 오는 경우도 대비
    if text.startswith("{"):
        try:
            j = res.json()
            # Bubble이 json을 주는 케이스가 있으면 여기에 맞춰 파싱
            # (일반적으로는 거의 없음)
            if "url" in j:
                return j["url"]
        except Exception:
            pass

    # 정상 케이스: //cdn.bubble.io/... 또는 https://cdn... 또는 /fileupload/.. 등
    if text.startswith("//") or text.startswith("http"):
        return text

    # Bubble이 따옴표 포함해서 주는 경우
    if text.startswith('"//') and text.endswith('"'):
        return text.strip('"')

    raise Exception(f"Unexpected fileupload response: {res.text}")


# =========================
# EXCEL GENERATION
# =========================

def generate_excel_file():
    applications = get_all_applications()

    if not os.path.exists(TEMPLATE_PATH):
        raise Exception(f"Template file not found: {TEMPLATE_PATH}")

    wb = load_workbook(TEMPLATE_PATH)

    # ✅ 첫번째 시트 사용 (시트명이 정확히 뭔지 몰라도 안전)
    ws = wb.worksheets[0]

    start_row = 9
    row = start_row

    for idx, app_data in enumerate(applications, start=1):
        in_charge = app_data.get("in_charge_of", "") or ""
        recommend = app_data.get("recommend", "") or "IBGC"
        job_no = app_data.get("JOB_NO", "") or ""

        ws[f"A{row}"] = idx
        ws[f"B{row}"] = in_charge
        ws[f"C{row}"] = recommend
        ws[f"D{row}"] = job_no

        row += 1

    filename = today_label()
    generated_dir = "generated"
    os.makedirs(generated_dir, exist_ok=True)

    file_path = os.path.join(generated_dir, filename)
    wb.save(file_path)

    # ✅ Bubble storage 업로드
    bubble_file_url = upload_file_to_bubble_storage(file_path, filename)

    # Bubble file url이 // 로 시작하면, 브라우저 다운로드를 위해 https: 붙인 버전도 같이 저장해두면 편함
    if bubble_file_url.startswith("//"):
        downloadable_url = "https:" + bubble_file_url
    else:
        downloadable_url = bubble_file_url

    return {
        "local_path": file_path,
        "filename": filename,
        "bubble_file_url": bubble_file_url,
        "download_url": downloadable_url,
        "source_count": len(applications),
    }


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
        result = generate_excel_file()

        create_daily_excel_record(
            file_bubble_url=result["bubble_file_url"],
            file_url=result["download_url"],
            label=result["filename"],
            status="ready",
            source_count=result["source_count"],
            note=""
        )

        return jsonify({
            "ok": True,
            "file": result["bubble_file_url"],
            "file_url": result["download_url"],
            "label": result["filename"],
            "status": "ready",
            "source_count": result["source_count"],
            "created_at": now_kst().isoformat()
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/excel/refresh_now", methods=["POST"])
def excel_refresh_now():
    return excel_generate_daily()


# (옵션) 로컬 다운로드가 필요하면 사용
@app.route("/download/<filename>", methods=["GET"])
def download_local_file(filename):
    generated_dir = "generated"
    return send_from_directory(generated_dir, filename, as_attachment=True)


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
