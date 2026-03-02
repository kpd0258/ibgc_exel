import os
import json
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

# 예) https://ibgc.co.kr/version-test  (dev)
# 예) https://ibgc.co.kr              (live)
BUBBLE_BASE_URL = os.environ.get("BUBBLE_BASE_URL", "").rstrip("/")

# Bubble Settings > API > Admin API Tokens 의 Private key
BUBBLE_DATA_API_TOKEN = os.environ.get("BUBBLE_DATA_API_TOKEN", "")

# Bubble Data type 이름(정확히!)
BUBBLE_APP_TYPE = os.environ.get("BUBBLE_APP_TYPE", "00. Application")

# 템플릿 파일 경로(레포에 있는 파일명)
TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", "IBGC_Application_Template.xlsx")

# Bubble Data API base
BUBBLE_DATA_API_BASE = f"{BUBBLE_BASE_URL}/api/1.1/obj"

# KST
KST = timezone(timedelta(hours=9))


# =========================
# UTIL
# =========================

def require_api_key(req):
    # 키를 안 넣으면 전체 오픈(개발 편의)
    if not EXCEL_API_KEY:
        return True
    return req.headers.get("X-API-Key") == EXCEL_API_KEY


def now_kst():
    return datetime.now(KST)


def today_label():
    # 파일명: IBGC_Application_YYYYMMDD.xlsx
    return now_kst().strftime("IBGC_Application_%Y%m%d.xlsx")


# =========================
# BUBBLE API HELPERS
# =========================

def bubble_headers_json():
    return {
        "Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}",
        "Content-Type": "application/json",
    }


def bubble_headers_fileupload():
    # multipart 업로드는 Content-Type을 requests가 자동으로 잡게 두는게 안전
    return {"Authorization": f"Bearer {BUBBLE_DATA_API_TOKEN}"}


def bubble_fileupload_url():
    # Bubble file storage 업로드 엔드포인트
    return f"{BUBBLE_BASE_URL}/fileupload"


def upload_file_to_bubble(file_path: str, filename: str) -> str:
    """
    Excel 파일을 Bubble File Storage로 업로드하고, bubblecdn URL을 반환
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

        # Bubble 쪽에서 name을 받는 케이스가 있어 같이 전송(없어도 무해)
        data = {"name": filename}

        res = requests.post(
            url,
            headers=bubble_headers_fileupload(),
            files=files,
            data=data,
            timeout=120,
        )

    if res.status_code not in (200, 201):
        raise Exception(f"Bubble file upload error: {res.status_code} {res.text}")

    # 케이스 1) JSON으로 오거나
    try:
        j = res.json()
        if isinstance(j, dict):
            for k in ("url", "file", "public_url"):
                if j.get(k):
                    return str(j[k])
    except Exception:
        pass

    # 케이스 2) 그냥 문자열("https://...")로 오거나
    text = res.text.strip().strip('"').strip("'")
    if not text.startswith("http"):
        raise Exception(f"Unexpected fileupload response: {res.text}")

    return text


def get_all_applications(limit: int = 200):
    """
    Bubble Data API로 00. Application 전체를 가져옴.
    (필요하면 limit/페이지네이션 확장 가능)
    """
    url = f"{BUBBLE_DATA_API_BASE}/{BUBBLE_APP_TYPE}?limit={limit}"
    res = requests.get(url, headers=bubble_headers_json(), timeout=60)
    if res.status_code != 200:
        raise Exception(f"Bubble fetch error: {res.status_code} {res.text}")
    return res.json().get("response", {}).get("results", [])


def create_daily_excel_record(file_url: str, label: str, source_count: int, status: str = "ready"):
    """
    DailyExcel 타입에 레코드 생성
    - file: Bubble file (file 타입)
    - file_url: text (선택)
    - label: text
    - source_count: number
    - status: text
    """
    url = f"{BUBBLE_DATA_API_BASE}/DailyExcel"
    payload = {
        "file": file_url,        # ✅ bubblecdn url 저장(=Bubble file storage)
        "file_url": file_url,    # ✅ text 필드에도 동일하게 넣어둠(편의)
        "label": label,
        "source_count": source_count,
        "status": status,
    }
    res = requests.post(url, headers=bubble_headers_json(), json=payload, timeout=60)

    # Bubble Data API create는 보통 200 + {"status":"success","id":"..."} 형태
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

    # 시트가 4개라고 했으니: 템플릿의 첫 4개 시트를 그대로 사용
    sheets = wb.worksheets
    if len(sheets) < 4:
        raise Exception(f"Template must contain at least 4 sheets. Current: {len(sheets)}")

    ws1 = sheets[0]  # 첫번째 시트
    # ws2 = sheets[1]
    # ws3 = sheets[2]
    # ws4 = sheets[3]

    # ---- 1번 시트 채우기 ----
    # 9행부터
    start_row = 9
    row = start_row

    for idx, app_data in enumerate(applications, start=1):
        # 1열: index 자동
        ws1[f"A{row}"] = idx

        # 2열: In_charge_of
        ws1[f"B{row}"] = app_data.get("In_charge_of", "") or ""

        # 3열: recommend (비어있으면 "IBGC")
        recommend_val = app_data.get("recommend", "")
        ws1[f"C{row}"] = recommend_val if (recommend_val is not None and str(recommend_val).strip() != "") else "IBGC"

        # 4열: JOB_NO
        ws1[f"D{row}"] = app_data.get("JOB_NO", "") or ""

        row += 1

    # ---- 저장 ----
    filename = today_label()
    generated_dir = "generated"
    os.makedirs(generated_dir, exist_ok=True)
    file_path = os.path.join(generated_dir, filename)
    wb.save(file_path)

    # ✅ Bubble File Storage로 업로드
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
        # 1) 엑셀 생성 + Bubble 업로드(bubblecdn URL)
        bubble_file_url, label, source_count = generate_excel_file_and_upload_to_bubble()

        # 2) DailyExcel 레코드 생성(=Bubble DB에 저장)
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
    # generate_daily와 동일 동작
    return excel_generate_daily()


# 디버깅/확인용(원하면 유지, 프론트 워크플로우는 get 없이도 가능)
@app.route("/excel/latest", methods=["GET"])
def excel_latest():
    if not require_api_key(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    try:
        # Bubble Data API에서 최신 1개 가져오기
        # (필드명은 Bubble이 제공하는 sort_field 문법을 따라야 함)
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


# (옵션) 우리 서버에서 파일 내려받는 라우트는 이제 거의 안 씀.
# Bubblecdn로 저장하니까, 프론트에선 bubblecdn 링크로 바로 다운로드하면 됨.
@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    # Render 디스크에 남아있는 파일 디버깅용
    # 주의: Flask의 send_static_file은 static 폴더만 지원. 여기서는 안전하게 404 처리.
    return jsonify({"ok": False, "error": "Use Bubble CDN link from DailyExcel.file instead."}), 404


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
