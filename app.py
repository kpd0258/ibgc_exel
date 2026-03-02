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

# dev: https://ibgc.co.kr/version-test
# live: https://ibgc.co.kr
BUBBLE_BASE_URL = os.environ.get("BUBBLE_BASE_URL", "").rstrip("/")

# Bubble Settings > API > Admin API Tokens 의 Private key
BUBBLE_DATA_API_TOKEN = os.environ.get("BUBBLE_DATA_API_TOKEN", "")

# Bubble Data type 이름(정확히!)
BUBBLE_APP_TYPE = os.environ.get("BUBBLE_APP_TYPE", "00. Application")

# 템플릿 파일 경로
TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", "IBGC_Application_Template.xlsx")

BUBBLE_DATA_API_BASE = f"{BUBBLE_BASE_URL}/api/1.1/obj"

KST = timezone(timedelta(hours=9))


# =========================
# UTIL
# =========================

def require_api_key(req):
    # 키 미설정이면 전체 오픈(개발 편의)
    if not EXCEL_API_KEY:
        return True
    return req.headers.get("X-API-Key") == EXCEL_API_KEY


def now_kst():
    return datetime.now(KST)


def today_label():
    return now_kst().strftime("IBGC_Application_%Y%m%d.xlsx")


def normalize_bubble_url(u: str) -> str:
    """
    Bubble fileupload가 아래 형태로 올 수 있음:
    - "https://...."
    - "//86c1...cdn.bubble.io/...."
    - "/fileupload/...." (드물게)
    그래서 URL을 무조건 https 절대경로로 정규화.
    """
    if not u:
        return u
    u = u.strip().strip('"').strip("'").strip()

    if u.startswith("//"):
        return "https:" + u

    if u.startswith("/"):
        # base url의 도메인 부분만 붙여줌
        # 예: https://ibgc.co.kr/version-test + /something  => https://ibgc.co.kr/something
        # (Bubble은 루트 도메인 기준으로 주는 경우가 많아서 이렇게 처리)
        # 안전하게 scheme+host만 추출
        # BUBBLE_BASE_URL = https://ibgc.co.kr/version-test
        parts = BUBBLE_BASE_URL.split("/")
        host = parts[0] + "//" + parts[2]  # https://ibgc.co.kr
        return host + u

    return u


# =========================
# BUBBLE API HELPERS
# =========================

def bubble_headers_json():
    return {
        "Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}",
        "Content-Type": "application/json",
    }


def bubble_headers_fileupload():
    # multipart 업로드는 Content-Type을 requests가 자동으로 잡게 둠
    return {"Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}"}


def bubble_fileupload_url():
    return f"{BUBBLE_BASE_URL}/fileupload"


def upload_file_to_bubble(file_path: str, filename: str) -> str:
    """
    Excel 파일을 Bubble File Storage로 업로드하고 bubblecdn URL 반환
    """
    url = bubble_fileupload_url()

    with open(file_path, "rb") as f:
        files = {
            "file": (
                filename,
                f,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        }
        data = {"name": filename}

        res = requests.post(
            url,
            headers=bubble_headers_fileupload(),
            files=files,
            data=data,
            timeout=180,
        )

    if res.status_code not in (200, 201):
        raise Exception(f"Bubble file upload error: {res.status_code} {res.text}")

    # 1) JSON으로 오는 케이스
    try:
        j = res.json()
        if isinstance(j, dict):
            for k in ("url", "file", "public_url"):
                if j.get(k):
                    return normalize_bubble_url(str(j[k]))
    except Exception:
        pass

    # 2) 문자열로 오는 케이스 (여기서 "//..."가 많이 나옴)
    text = normalize_bubble_url(res.text)

    if not text.startswith("http"):
        # 그래도 http가 아니면 진짜 이상 응답
        raise Exception(f"Unexpected fileupload response: {res.text}")

    return text


def get_all_applications(limit: int = 200):
    """
    Bubble Data API로 00. Application 목록 조회
    """
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}?limit={limit}"
    res = requests.get(url, headers=bubble_headers_json(), timeout=60)
    if res.status_code != 200:
        raise Exception(f"Bubble fetch error: {res.status_code} {res.text}")
    return res.json().get("response", {}).get("results", [])


def create_daily_excel_record(file_url: str, label: str, source_count: int, status: str = "ready"):
    """
    DailyExcel 타입에 레코드 생성
    - file: file
    - file_url: text
    - label: text
    - source_count: number
    - status: text
    """
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"
    payload = {
        "file": file_url,
        "file_url": file_url,
        "label": label,
        "source_count": source_count,
        "status": status,
    }
    res = requests.post(url, headers=bubble_headers_json(), json=payload, timeout=60)
    if res.status_code != 200:
        raise Exception(f"Bubble create error: {res.status_code} {res.text}")
    return res.json()


# =========================
# EXCEL GENERATION
# =========================

def generate_excel_file_and_upload_to_bubble():
    applications = get_all_applications()
    source_count = len(applications)

    if not os.path.exists(TEMPLATE_PATH):
        raise Exception(f"Template file not found: {TEMPLATE_PATH}")

    wb = load_workbook(TEMPLATE_PATH)

    sheets = wb.worksheets
    if len(sheets) < 4:
        raise Exception(f"Template must contain at least 4 sheets. Current: {len(sheets)}")

    ws1 = sheets[0]  # 첫번째 시트

    # 9행부터 입력
    start_row = 9
    row = start_row

    for idx, app_data in enumerate(applications, start=1):
        # 1열 index
        ws1[f"A{row}"] = idx

        # 2열 In_charge_of
        ws1[f"B{row}"] = app_data.get("In_charge_of", "") or ""

        # 3열 recommend (빈값이면 IBGC)
        recommend_val = app_data.get("recommend", "")
        ws1[f"C{row}"] = recommend_val if (recommend_val is not None and str(recommend_val).strip() != "") else "IBGC"

        # 4열 JOB_NO
        ws1[f"D{row}"] = app_data.get("JOB_NO", "") or ""

        row += 1

    filename = today_label()
    generated_dir = "generated"
    os.makedirs(generated_dir, exist_ok=True)

    file_path = os.path.join(generated_dir, filename)
    wb.save(file_path)

    bubble_file_url = upload_file_to_bubble(file_path, filename)

    return bubble_file_url, filename, source_count


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
        bubble_file_url, label, source_count = generate_excel_file_and_upload_to_bubble()

        created = create_daily_excel_record(
            file_url=bubble_file_url,
            label=label,
            source_count=source_count,
            status="ready",
        )

        return jsonify({
            "ok": True,
            "file_url": bubble_file_url,
            "label": label,
            "source_count": source_count,
            "bubble_status": created.get("status"),
            "bubble_id": created.get("id"),
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/excel/refresh_now", methods=["POST"])
def excel_refresh_now():
    return excel_generate_daily()


# (선택) 디버깅용
@app.route("/excel/latest", methods=["GET"])
def excel_latest():
    if not require_api_key(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    try:
        url = f"{BUBBLE_DATA_API_BASE}/DailyExcel?sort_field=Created Date&descending=true&limit=1"
        res = requests.get(url, headers=bubble_headers_json(), timeout=60)
        if res.status_code != 200:
            raise Exception(f"Bubble latest error: {res.status_code} {res.text}")

        results = res.json().get("response", {}).get("results", [])
        if not results:
            return jsonify({"ok": False, "error": "No file found"}), 404

        latest = results[0]
        return jsonify({
            "ok": True,
            "file_url": latest.get("file") or latest.get("file_url"),
            "label": latest.get("label"),
            "status": latest.get("status"),
            "source_count": latest.get("source_count"),
            "created_at": latest.get("Created Date"),
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    return jsonify({"ok": False, "error": "Use Bubble CDN link from DailyExcel.file instead."}), 404


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
