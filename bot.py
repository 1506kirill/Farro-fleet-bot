import os
import re
import json
import logging
from datetime import datetime, date, time, timedelta
from statistics import median
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import anthropic
import gspread
from openai import OpenAI
from google.oauth2.service_account import Credentials
from telegram import Update
from telegram.ext import (
    Application,
    MessageHandler,
    CommandHandler,
    filters,
    ContextTypes,
)
from gspread_formatting import format_cell_range, CellFormat, Color, TextFormat

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KYIV_TZ = ZoneInfo("Europe/Kyiv")
MINFIN_URL = "https://minfin.com.ua/currency/auction/usd/buy/dnepropetrovsk/"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
GOOGLE_CREDS = os.environ.get("GOOGLE_CREDS", "")

ALLOWED_USERS_STR = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in ALLOWED_USERS_STR.split(",") if x.strip()]

FULL_PLATES = [
    "AI1457MM", "ÐÐ0418ÐÐ ", "ÐÐ2993Ð I", "AE7935PI", "ÐÐ3021ÐÐ", "ÐÐ9489ÐÐ ",
    "ÐÐ7121Ð¢Ð", "ÐÐ8204Ð¢Ð", "AE2548TB", "ÐÐ9245Ð¢Ð", "AE0736PK", "AE4715TH",
    "ÐÐ6514Ð¢Ð¡", "KA4895HE", "KA6843HB", "ÐÐ5308Ð¢Ð", "BI1875HO", "KA0665IH",
    "KA0349HO", "BC9854PM", "ÐÐ8391Ð¢Ð", "AE4553XB", "KA8730IX", "AE5725OO",
    "Ð¡Ð6584ÐÐ", "AI3531PH",
]

TO_BUNDLE = [
    {"description": "ÐÐ°ÑÐ»Ð¾ Ð² Ð´Ð²Ð¸Ð³Ð°ÑÐµÐ»Ñ", "amount": 780},
    {"description": "ÐÐ¾Ð·Ð´ÑÑÐ½ÑÐ¹ ÑÐ¸Ð»ÑÑÑ WX WA9545", "amount": 270},
    {"description": "ÐÐ°Ð·Ð¾Ð²ÑÐµ ÑÐ¸Ð»ÑÑÑÐ°", "amount": 100},
    {"description": "ÐÐ°ÑÐ»ÑÐ½ÑÐ¹ ÑÐ¸Ð»ÑÑÑ BO 0451103318", "amount": 160},
    {"description": "Ð Ð°Ð±Ð¾ÑÑ Ð·Ð° Ð¢Ð", "amount": 300},
]

SKIP_GRM = {"9245", "5308", "4715", "8204", "0736"}

INSURANCE_DATE_COL = 18  # R (1-based)
INSURANCE_COMPANY_COL = 19  # S (1-based)

REPORT_CACHE: Dict[str, Any] = {"snapshot": None, "time": None}
REPORT_CACHE_TTL = 180


def extract_digits(value: str) -> str:
    return "".join(re.findall(r"\d+", str(value or "")))


VEHICLE_MAP = {extract_digits(p): p for p in FULL_PLATES if extract_digits(p)}
KNOWN_CAR_IDS = sorted(VEHICLE_MAP.keys())

claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY) if CLAUDE_API_KEY else None
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# ===== Formatting helpers =====

def blue_text_format() -> CellFormat:
    return CellFormat(textFormat=TextFormat(foregroundColor=Color(0, 0, 1)))


def yellow_fill_format() -> CellFormat:
    return CellFormat(backgroundColor=Color(1, 0.96, 0.75))


def apply_blue_text(ws, cell_range: str) -> None:
    try:
        format_cell_range(ws, cell_range, blue_text_format())
    except Exception as e:
        logger.error(f"Blue text format error: {e}")


def mark_cell_yellow(ws, cell_range: str) -> None:
    try:
        format_cell_range(ws, cell_range, yellow_fill_format())
    except Exception as e:
        logger.error(f"Yellow fill format error: {e}")


# ===== Google Sheets =====

def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDS)
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def get_matching_worksheet(spreadsheet, car_id: str):
    full_plate = VEHICLE_MAP.get(car_id, "")
    for ws in spreadsheet.worksheets():
        title = str(ws.title)
        if car_id in title or (full_plate and full_plate in title):
            return ws
    return None


def get_data_snapshot(force_refresh: bool = False) -> Dict[str, List[List[str]]]:
    global REPORT_CACHE
    now = datetime.now(KYIV_TZ)
    if not force_refresh and REPORT_CACHE["snapshot"] and REPORT_CACHE["time"]:
        if (now - REPORT_CACHE["time"]).total_seconds() < REPORT_CACHE_TTL:
            return REPORT_CACHE["snapshot"]

    spreadsheet = get_sheet()
    snapshot: Dict[str, List[List[str]]] = {}
    for ws in spreadsheet.worksheets():
        snapshot[ws.title] = ws.get_all_values()

    REPORT_CACHE = {"snapshot": snapshot, "time": now}
    return snapshot


# ===== Basic parsers =====

def parse_num(v) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    digits = re.sub(r"[^\d\-]", "", s)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def normalize_date_short(date_str: Optional[str]) -> str:
    if not date_str:
        return datetime.now(KYIV_TZ).strftime("%d.%m.%y")
    s = str(date_str).strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d.%m.%y")
        except ValueError:
            pass
    return datetime.now(KYIV_TZ).strftime("%d.%m.%y")


def parse_short_date(date_str: Optional[str]) -> Optional[date]:
    if not date_str:
        return None
    s = str(date_str).strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def format_km(v: Optional[int]) -> str:
    if v is None:
        return ""
    sign = "-" if v < 0 else ""
    return f"{sign}{abs(v):,}".replace(",", ".")


def resolve_car_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    raw = str(value).strip().upper()
    digits = extract_digits(raw)
    if digits in VEHICLE_MAP:
        return digits
    for short_id, full_plate in VEHICLE_MAP.items():
        if raw == full_plate.upper():
            return short_id
    return None


def full_plate_from_short(car_id: Optional[str]) -> str:
    if not car_id:
        return "ÐÐµÐ²ÑÐ´Ð¾Ð¼Ð¾"
    return VEHICLE_MAP.get(car_id, car_id)


def clean_json_text(text: str) -> str:
    if not text:
        return ""
    s = text.strip().replace("```json", "").replace("```", "").strip()
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        return s[start:end + 1]
    return s


# ===== AI parsing =====

def build_known_cars_block() -> str:
    return "\n".join(f"{k} -> {VEHICLE_MAP[k]}" for k in KNOWN_CAR_IDS)


def build_prompt(message: str, existing_data: Optional[dict] = None) -> str:
    today = datetime.now(KYIV_TZ).strftime("%d.%m.%y")
    existing_block = ""
    if existing_data:
        existing_block = f'\nAlready known data:\n{json.dumps(existing_data, ensure_ascii=False)}\n'
    cars_block = build_known_cars_block()
    return f"""Ð¢Ñ Ð¿Ð¾Ð¼Ð¾ÑÐ½Ð¸Ðº Ð´Ð»Ñ ÑÑÐµÑÐ° Ð°Ð²ÑÐ¾Ð¿Ð°ÑÐºÐ°. Ð¡ÐµÐ³Ð¾Ð´Ð½Ñ {today}.

Ð¢Ð²Ð¾Ñ Ð·Ð°Ð´Ð°ÑÐ°: ÑÐ°Ð·Ð¾Ð±ÑÐ°ÑÑ ÑÐ¾Ð¾Ð±ÑÐµÐ½Ð¸Ðµ Ð¿Ð¾Ð»ÑÐ·Ð¾Ð²Ð°ÑÐµÐ»Ñ Ð² Ð¡Ð¢Ð ÐÐÐÐ JSON Ð´Ð»Ñ Ð·Ð°Ð¿Ð¸ÑÐ¸ Ð² Google Sheets.
{existing_block}
ÐÐ·Ð²ÐµÑÑÐ½ÑÐµ Ð¼Ð°ÑÐ¸Ð½Ñ Ð°Ð²ÑÐ¾Ð¿Ð°ÑÐºÐ°:
{cars_block}

ÐÑÐ°Ð²Ð¸Ð»Ð°:
1. ÐÐ¾Ð»ÑÐ·Ð¾Ð²Ð°ÑÐµÐ»Ñ Ð¼Ð¾Ð¶ÐµÑ Ð¿Ð¸ÑÐ°ÑÑ Ð´Ð°Ð½Ð½ÑÐµ Ð² Ð»ÑÐ±Ð¾Ð¼ Ð¿Ð¾ÑÑÐ´ÐºÐµ.
2. ÐÐ¾Ð»ÑÐ·Ð¾Ð²Ð°ÑÐµÐ»Ñ ÑÐ°ÑÑÐ¾ Ð¿Ð¸ÑÐµÑ ÑÐ¾Ð»ÑÐºÐ¾ ÑÐ¸ÑÑÑ Ð¼Ð°ÑÐ¸Ð½Ñ, Ð½Ð°Ð¿ÑÐ¸Ð¼ÐµÑ 4553 Ð¸Ð»Ð¸ 8730.
3. car_id Ð² JSON Ð´Ð¾Ð»Ð¶ÐµÐ½ Ð±ÑÑÑ ÑÐ¾Ð»ÑÐºÐ¾ Ð¸Ð· ÑÐ¿Ð¸ÑÐºÐ° Ð¸Ð·Ð²ÐµÑÑÐ½ÑÑ Ð¼Ð°ÑÐ¸Ð½.
4. ÐÑÐ»Ð¸ Ð´Ð°ÑÐ° Ð½Ðµ ÑÐºÐ°Ð·Ð°Ð½Ð° - Ð¸ÑÐ¿Ð¾Ð»ÑÐ·ÑÐ¹ ÑÐµÐ³Ð¾Ð´Ð½ÑÑÐ½ÑÑ Ð´Ð°ÑÑ Ð² ÑÐ¾ÑÐ¼Ð°ÑÐµ DD.MM.YY.
5. ÐÐÐÐÐ«Ð ÐÐÐ¯ Ð¢ÐÐÐÐÐ¦Ð« ÐÐÐ¨Ð ÐÐ Ð Ð£Ð¡Ð¡ÐÐÐ Ð¯ÐÐ«ÐÐ.
6. ÐÑÐ²ÐµÑ Ð´Ð¾Ð»Ð¶ÐµÐ½ Ð±ÑÑÑ Ð¢ÐÐÐ¬ÐÐ JSON.
7. ÐÑÐ»Ð¸ Ð½Ðµ ÑÐ²Ð°ÑÐ°ÐµÑ Ð²Ð°Ð¶Ð½ÑÑ Ð´Ð°Ð½Ð½ÑÑ - Ð²ÐµÑÐ½Ð¸ missing_fields.
8. ÐÑÐ»Ð¸ Ð¿Ð¾Ð»ÑÐ·Ð¾Ð²Ð°ÑÐµÐ»Ñ Ð¿Ð¸ÑÐµÑ "Ð¢Ð" Ð¸Ð»Ð¸ "Ð¿Ð»Ð°Ð½Ð¾Ð²Ð¾Ðµ Ð¢Ð", description Ð²ÐµÑÐ½Ð¸ ÐºÐ°Ðº "Ð¢Ð".
9. ÐÑÐ»Ð¸ Ð¿Ð¾Ð»ÑÐ·Ð¾Ð²Ð°ÑÐµÐ»Ñ Ð¿Ð¸ÑÐµÑ Ð¿ÑÐ¾ ÑÑÑÐ°Ñ, Ð´Ð¾Ð»Ð³, Ð´Ð¾Ð»Ð¶ÐµÐ½, Ð´Ð¾Ð¶ÐµÐ½ - type Ð²ÐµÑÐ½Ð¸ ÐºÐ°Ðº "liability_minus".
10. ÐÑÐ»Ð¸ Ð¿Ð¾Ð»ÑÐ·Ð¾Ð²Ð°ÑÐµÐ»Ñ Ð¿Ð¸ÑÐµÑ "Ð²Ð·ÑÐ»", "Ð¿ÑÐ¸Ð½ÑÐ»", "Ð¿Ð¾Ð³Ð°ÑÐ¸Ð»", "Ð´Ð°Ð»" Ð² ÐºÐ¾Ð½ÑÐµÐºÑÑÐµ Ð´Ð¾Ð»Ð³Ð° - type Ð²ÐµÑÐ½Ð¸ ÐºÐ°Ðº "liability_plus".
11. ÐÐ»Ñ liability_minus Ð¸ liability_plus odometer Ð½Ðµ Ð½ÑÐ¶ÐµÐ½.
12. ÐÐ»Ñ liability_minus Ð¸ liability_plus description Ð´Ð¾Ð»Ð¶Ð½Ð° Ð±ÑÑÑ ÑÐ¾Ð»ÑÐºÐ¾ ÑÐµÐºÑÑÐ¾Ð¼ Ð¿ÑÐ¸ÑÐ¸Ð½Ñ ÐÐÐ Ð½Ð¾Ð¼ÐµÑÐ° Ð¼Ð°ÑÐ¸Ð½Ñ Ð¸ ÐÐÐ ÑÑÐ¼Ð¼Ñ.
13. ÐÐ»Ñ income description Ð¼Ð¾Ð¶ÐµÑ Ð±ÑÑÑ Ð¿ÑÑÑÑÐ¼.

Ð¡Ð¾Ð¾Ð±ÑÐµÐ½Ð¸Ðµ Ð¿Ð¾Ð»ÑÐ·Ð¾Ð²Ð°ÑÐµÐ»Ñ:
"{message}"

ÐÐµÑÐ½Ð¸ JSON ÑÑÑÐ¾Ð³Ð¾ ÑÐ°ÐºÐ¾Ð³Ð¾ Ð²Ð¸Ð´Ð°:
{{
  "type": "expense" Ð¸Ð»Ð¸ "income" Ð¸Ð»Ð¸ "liability_minus" Ð¸Ð»Ð¸ "liability_plus" Ð¸Ð»Ð¸ null,
  "car_id": "8730" Ð¸Ð»Ð¸ null,
  "date": "DD.MM.YY",
  "amount": 370,
  "description": "ÐÐ¾Ð»Ð¾Ð´ÐºÐ¸ ÐÐ¾Ñ",
  "odometer": 470420,
  "notes": null,
  "missing_fields": []
}}"""


def ask_claude(prompt: str) -> dict:
    if not claude_client:
        raise Exception("CLAUDE_API_KEY not set")
    response = claude_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=700,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    return json.loads(clean_json_text(text))


def ask_openai(prompt: str) -> dict:
    if not openai_client:
        raise Exception("OPENAI_API_KEY not set")
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": "ÐÐ¾Ð·Ð²ÑÐ°ÑÐ°Ð¹ ÑÐ¾Ð»ÑÐºÐ¾ Ð²Ð°Ð»Ð¸Ð´Ð½ÑÐ¹ JSON. ÐÐµÐ· Ð¿Ð¾ÑÑÐ½ÐµÐ½Ð¸Ð¹. ÐÐµÐ· markdown."},
            {"role": "user", "content": prompt},
        ],
    )
    text = response.choices[0].message.content.strip()
    return json.loads(clean_json_text(text))


def ask_ai(message: str, existing_data: Optional[dict] = None) -> dict:
    prompt = build_prompt(message, existing_data)
    if claude_client:
        try:
            return ask_claude(prompt)
        except Exception as e:
            logger.error(f"Claude error: {e}")
    if openai_client:
        try:
            return ask_openai(prompt)
        except Exception as e:
            logger.error(f"OpenAI error: {e}")
            return {"error": f"AI Ð½ÐµÐ´Ð¾ÑÑÑÐ¿Ð½Ð¸Ð¹: {e}"}
    return {"error": "ÐÐµ Ð·Ð°Ð´Ð°Ð½Ñ CLAUDE_API_KEY Ñ OPENAI_API_KEY"}


# ===== Special parsing =====

def is_to_phrase(text: str) -> bool:
    t = str(text or "").lower().strip()
    return t == "ÑÐ¾" or " ÑÐ¾ " in f" {t} " or "Ð¿Ð»Ð°Ð½Ð¾Ð²Ð¾Ðµ ÑÐ¾" in t or "Ð¿Ð»Ð°Ð½Ð¾Ð²Ðµ ÑÐ¾" in t


def detect_liability_type(text: str) -> Optional[str]:
    t = str(text or "").lower().strip()
    plus_markers = ["Ð²Ð·ÑÐ»", "Ð¿ÑÐ¸Ð½ÑÐ»", "Ð¿Ð¾Ð³Ð°ÑÐ¸Ð»", "Ð´Ð°Ð» "]
    minus_markers = ["ÑÑÑÐ°Ñ", "Ð´Ð¾Ð»Ð³", "Ð´Ð¾Ð»Ð¶ÐµÐ½", "Ð´Ð¾Ð»Ð¶Ð½Ð°", "Ð´Ð¾Ð¶ÐµÐ½"]
    if any(marker in t for marker in plus_markers):
        return "liability_plus"
    if any(marker in t for marker in minus_markers):
        return "liability_minus"
    return None


def apply_special_cases(data: dict, raw_text: str) -> dict:
    liability_type = detect_liability_type(raw_text)
    if liability_type and not data.get("type"):
        data["type"] = liability_type
    if is_to_phrase(raw_text):
        data.setdefault("type", "expense")
        data.setdefault("description", "Ð¢Ð")
        if data.get("amount") in ("", None):
            data["amount"] = 0
    return data


def compute_missing_fields(data: dict, raw_text: str = "") -> list[str]:
    missing = []
    op_type = data.get("type")
    to_case = is_to_phrase(raw_text) or str(data.get("description", "")).lower().strip() in {"ÑÐ¾", "Ð¿Ð»Ð°Ð½Ð¾Ð²Ð¾Ðµ ÑÐ¾", "Ð¿Ð»Ð°Ð½Ð¾Ð²Ðµ ÑÐ¾"}

    if not op_type:
        missing.append("type")
    if not data.get("car_id"):
        missing.append("car_id")
    if data.get("amount") in (None, "") and not to_case:
        missing.append("amount")
    if op_type in ["expense", "liability_minus", "liability_plus"] and not data.get("description"):
        missing.append("description")
    if op_type in ["expense", "income"] and data.get("odometer") in (None, ""):
        missing.append("odometer")
    return missing


def ask_for_next_missing_field(missing_fields: list[str]) -> str:
    if not missing_fields:
        return "Ð£ÑÐ¾ÑÐ½Ð¸, Ð±ÑÐ´Ñ Ð»Ð°ÑÐºÐ°, Ð²ÑÐ´ÑÑÑÐ½Ñ Ð´Ð°Ð½Ñ."
    field = missing_fields[0]
    mapping = {
        "type": "ÐÐºÐ°Ð¶Ð¸, Ð±ÑÐ´Ñ Ð»Ð°ÑÐºÐ°, ÑÐµ Ð¿ÑÐ¸ÑÑÐ´, Ð²Ð¸ÑÑÐ°ÑÐ°, ÑÑÑÐ°Ñ ÑÐ¸ Ð±Ð¾ÑÐ³.",
        "car_id": f"ÐÐºÐ°Ð¶Ð¸ Ð½Ð¾Ð¼ÐµÑ Ð¼Ð°ÑÐ¸Ð½Ð¸. ÐÐ¾ÑÑÑÐ¿Ð½Ñ: {', '.join(KNOWN_CAR_IDS)}",
        "amount": "ÐÐºÐ°Ð¶Ð¸ ÑÑÐ¼Ñ Ð² Ð³ÑÐ¸Ð²Ð½ÑÑ.",
        "description": "ÐÐºÐ°Ð¶Ð¸ Ð¾Ð¿Ð¸Ñ Ð°Ð±Ð¾ Ð¿ÑÐ¸ÑÐ¸Ð½Ñ.",
        "odometer": "ÐÐµÐ½Ñ Ð´Ð¾Ð´Ð°ÑÐ¸ ÑÐµÑÐµÐ´Ð½ÑÐ¾ÑÑÐ°ÑÐ¸ÑÑÐ¸ÑÐ½Ð¸Ð¹ Ð¿ÑÐ¾Ð±ÑÐ³? ÐÐ°Ð¿Ð¸ÑÐ¸ Â«ÑÐ°ÐºÂ» Ð°Ð±Ð¾ Ð¿ÑÐ¾ÑÑÐ¾ Ð½Ð°Ð´ÑÑÐ»Ð¸ ÑÐ¸ÑÑÐ¸ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.",
    }
    return mapping.get(field, "Ð£ÑÐ¾ÑÐ½Ð¸, Ð±ÑÐ´Ñ Ð»Ð°ÑÐºÐ°, Ð²ÑÐ´ÑÑÑÐ½Ñ Ð´Ð°Ð½Ñ.")


def build_liability_description(op_type: str, raw_text: str, ai_description: Optional[str]) -> str:
    t = str(raw_text or "").lower()
    desc = str(ai_description or "").strip()
    if "Ð´ÑÐ¿" in t:
        base = "Ð·Ð° ÐÐ¢Ð"
    elif "ÑÐµÐ»ÐµÐ²Ð¸Ð·" in t:
        base = "Ð·Ð° ÑÐµÐ»ÐµÐ²Ð¸Ð·Ð¾Ñ"
    elif "Ð¿Ð°ÑÐºÐ¾Ð²" in t:
        base = "Ð·Ð° Ð¿Ð°ÑÐºÐ¾Ð²ÐºÑ"
    elif "Ð¿ÑÐµÐ²ÑÑ" in t:
        base = "Ð·Ð° Ð¿ÑÐµÐ²ÑÑÐµÐ½Ð¸Ðµ"
    elif "ÑÑÑÐ°Ñ" in t and op_type == "liability_plus":
        base = "Ð·Ð° ÑÑÑÐ°Ñ"
    elif desc:
        base = desc if desc.lower().startswith("Ð·Ð° ") else f"Ð·Ð° {desc}"
    else:
        base = ""
    if op_type == "liability_minus":
        return f"{'ÑÑÑÐ°Ñ' if 'ÑÑÑÐ°Ñ' in t else 'Ð´Ð¾Ð»Ð³'} {base}".strip()
    return f"Ð¿Ð¾Ð³Ð°ÑÐµÐ½Ð¸Ðµ Ð´Ð¾Ð»Ð³Ð° {base}".strip()


def heuristic_multi_parse(text: str) -> Optional[List[dict]]:
    t = str(text or "").strip()
    if "," in t:
        actions: List[dict] = []
        shared_car_id = None
        for car_id in KNOWN_CAR_IDS:
            if re.search(rf"(?<!\d){re.escape(car_id)}(?!\d)", t):
                shared_car_id = car_id
                break
        if not shared_car_id:
            return None
        parts = [p.strip() for p in t.split(",") if p.strip()]
        for part in parts:
            low = part.lower()
            nums = [int(x) for x in re.findall(r"\d+", part)]
            amounts = [n for n in nums if str(n) != shared_car_id and str(n) not in KNOWN_CAR_IDS]
            if "Ð¿ÑÐ¸ÑÐ¾Ð´" in low and amounts:
                actions.append({
                    "type": "income", "car_id": shared_car_id, "date": normalize_date_short(None),
                    "amount": max(amounts), "description": "", "odometer": None, "notes": None, "missing_fields": []
                })
            elif detect_liability_type(low) == "liability_minus" and amounts:
                actions.append({
                    "type": "liability_minus", "car_id": shared_car_id, "date": normalize_date_short(None),
                    "amount": amounts[0], "description": build_liability_description("liability_minus", part, None),
                    "odometer": None, "notes": None, "missing_fields": []
                })
            elif detect_liability_type(low) == "liability_plus" and amounts:
                actions.append({
                    "type": "liability_plus", "car_id": shared_car_id, "date": normalize_date_short(None),
                    "amount": amounts[0], "description": build_liability_description("liability_plus", part, None),
                    "odometer": None, "notes": None, "missing_fields": []
                })
        return actions or None

    shared_car_id = None
    for car_id in KNOWN_CAR_IDS:
        if re.search(rf"(?<!\d){re.escape(car_id)}(?!\d)", t):
            shared_car_id = car_id
            break
    if not shared_car_id:
        return None

    nums = [int(x) for x in re.findall(r"\d+", t)]
    amounts = [n for n in nums if str(n) != shared_car_id and str(n) not in KNOWN_CAR_IDS]
    liability_type = detect_liability_type(t)

    if is_to_phrase(t):
        return [{
            "type": "expense", "car_id": shared_car_id, "date": normalize_date_short(None),
            "amount": 0, "description": "Ð¢Ð", "odometer": None, "notes": None, "missing_fields": []
        }]
    if liability_type == "liability_minus" and amounts:
        return [{
            "type": "liability_minus", "car_id": shared_car_id, "date": normalize_date_short(None),
            "amount": amounts[0], "description": build_liability_description("liability_minus", t, None),
            "odometer": None, "notes": None, "missing_fields": []
        }]
    if liability_type == "liability_plus" and len(amounts) == 1:
        return [{
            "type": "income", "car_id": shared_car_id, "date": normalize_date_short(None),
            "amount": amounts[0], "description": "", "odometer": None, "notes": None, "missing_fields": []
        }]
    if liability_type == "liability_plus" and len(amounts) >= 2:
        sorted_amounts = sorted(amounts, reverse=True)
        actions = [{
            "type": "income", "car_id": shared_car_id, "date": normalize_date_short(None),
            "amount": sorted_amounts[0], "description": "", "odometer": None, "notes": None, "missing_fields": []
        }]
        for extra in sorted_amounts[1:]:
            actions.append({
                "type": "liability_plus", "car_id": shared_car_id, "date": normalize_date_short(None),
                "amount": extra, "description": build_liability_description("liability_plus", t, None),
                "odometer": None, "notes": None, "missing_fields": []
            })
        return actions
    return None


# ===== Reports: current odometer and service blocks =====

def get_current_odometer_from_rows(rows: List[List[str]]) -> Optional[int]:
    f_last = None
    l_last = None
    for r in rows[7:]:
        if len(r) > 5:
            val = parse_num(r[5])
            if val is not None:
                f_last = val
        if len(r) > 11:
            val = parse_num(r[11])
            if val is not None:
                l_last = val
    if f_last is not None and l_last is not None:
        return max(f_last, l_last)
    return f_last if f_last is not None else l_last


def split_expense_blocks(rows: List[List[str]]) -> List[List[Dict[str, Any]]]:
    blocks: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    current_date = ""
    current_odo = None

    for row in rows[7:]:
        e = row[4] if len(row) > 4 else ""
        f = parse_num(row[5] if len(row) > 5 else None)
        g = str(row[6]).strip() if len(row) > 6 else ""
        h = parse_num(row[7] if len(row) > 7 else None)
        i = row[8] if len(row) > 8 else ""

        new_block = False
        if e and f is not None:
            if current:
                new_block = True
            current_date = e
            current_odo = f

        if new_block:
            blocks.append(current)
            current = []

        if current_date and current_odo is not None and any([e, f is not None, g, h is not None, i]):
            current.append({
                "date": current_date,
                "odo": current_odo,
                "desc": g.lower(),
                "amount": h,
            })

    if current:
        blocks.append(current)
    return blocks


def score_oil_block(block: List[Dict[str, Any]]) -> int:
    text = " | ".join(x["desc"] for x in block)
    score = 0
    if "Ð¼Ð°ÑÐ»Ð¾ Ð² Ð´Ð²Ð¸Ð³Ð°ÑÐµÐ»Ñ" in text:
        score += 10
    if "Ð¼Ð¾ÑÐ¾ÑÐ½Ð¾Ðµ Ð¼Ð°ÑÐ»Ð¾" in text:
        score += 8
    if "Ð·Ð°Ð¼ÐµÐ½Ð° Ð¼Ð°ÑÐ»Ð°" in text:
        score += 8
    if "Ð¼Ð°ÑÐ»ÑÐ½ÑÐ¹ ÑÐ¸Ð»ÑÑÑ" in text:
        score += 4
    if "Ð¼Ð°ÑÐ»Ð¾" in text:
        score += 2
    return score


def score_grm_block(block: List[Dict[str, Any]]) -> int:
    text = " | ".join(x["desc"] for x in block)
    score = 0
    if "ÐºÐ¾Ð¼Ð¿Ð»ÐµÐºÑ Ð³ÑÐ¼" in text:
        score += 10
    if "Ð·Ð°Ð¼ÐµÐ½Ð° Ð³ÑÐ¼" in text or "Ð·Ð°Ð¼Ð°Ð½Ð° Ð³ÑÐ¼" in text:
        score += 10
    if "ÑÐµÐ¼ÐµÐ½Ñ Ð³ÑÐ¼" in text:
        score += 7
    if "ÑÐ¾Ð»Ð¸Ðº Ð³ÑÐ¼" in text:
        score += 6
    if "Ð³ÑÐ¼" in text:
        score += 4
    if "Ð¿Ð¾Ð¼Ð¿Ð°" in text:
        score += 2
    return score


def find_last_service(rows: List[List[str]], mode: str) -> Tuple[Optional[str], Optional[int]]:
    blocks = split_expense_blocks(rows)
    if not blocks:
        return None, None

    scorer = score_oil_block if mode == "oil" else score_grm_block
    for block in reversed(blocks):
        if scorer(block) >= (10 if mode == "oil" else 8):
            return block[0]["date"], block[0]["odo"]
    return None, None


def get_color_icon(remaining: Optional[int], total: int) -> str:
    if remaining is None:
        return "âª"
    if remaining <= 1000:
        return "ð´"
    ratio = remaining / total
    if ratio > 0.66:
        return "ð¢"
    if ratio > 0.33:
        return "ð¡"
    return "ð "


def build_oil_report() -> str:
    snapshot = get_data_snapshot()
    lines = []
    for car_id in KNOWN_CAR_IDS:
        rows = None
        for title, data in snapshot.items():
            if car_id in title or VEHICLE_MAP.get(car_id, "") in title:
                rows = data
                break
        if not rows:
            continue
        last_date, last_odo = find_last_service(rows, "oil")
        current_odo = get_current_odometer_from_rows(rows)
        if last_odo is None or current_odo is None:
            continue
        if current_odo < last_odo:
            current_odo = last_odo
        remaining = 10000 - (current_odo - last_odo)
        icon = get_color_icon(remaining, 10000)
        lines.append(f"{icon} {car_id} | {last_date} | {last_odo} | {format_km(remaining)} ÐºÐ¼")
    return "\n".join(lines)


def build_grm_report() -> str:
    snapshot = get_data_snapshot()
    lines = []
    for car_id in KNOWN_CAR_IDS:
        if car_id in SKIP_GRM:
            continue
        rows = None
        for title, data in snapshot.items():
            if car_id in title or VEHICLE_MAP.get(car_id, "") in title:
                rows = data
                break
        if not rows:
            continue
        last_date, last_odo = find_last_service(rows, "grm")
        current_odo = get_current_odometer_from_rows(rows)
        if last_odo is None or current_odo is None:
            continue
        if current_odo < last_odo:
            current_odo = last_odo
        remaining = 50000 - (current_odo - last_odo)
        icon = get_color_icon(remaining, 50000)
        lines.append(f"{icon} {car_id} | {last_date} | {last_odo} | {format_km(remaining)} ÐºÐ¼")
    return "\n".join(lines)


# ===== Insurance =====

def insurance_days_icon(days_left: int) -> str:
    if days_left <= 14:
        return "ð´"
    if days_left <= 30:
        return "ð "
    if days_left <= 90:
        return "ð¡"
    return "ð¢"


def build_insurance_report() -> str:
    snapshot = get_data_snapshot()
    today = datetime.now(KYIV_TZ).date()
    lines = []
    for car_id in KNOWN_CAR_IDS:
        rows = None
        for title, data in snapshot.items():
            if car_id in title or VEHICLE_MAP.get(car_id, "") in title:
                rows = data
                break
        if not rows:
            continue

        best: Optional[Tuple[date, str]] = None
        for row in rows[7:]:
            if len(row) >= INSURANCE_COMPANY_COL:
                d = parse_short_date(row[INSURANCE_DATE_COL - 1])
                company = str(row[INSURANCE_COMPANY_COL - 1]).strip()
                if d and company:
                    if best is None or d > best[0]:
                        best = (d, company)
        if not best:
            continue
        end_date, company = best
        days_left = (end_date - today).days
        icon = insurance_days_icon(days_left)
        lines.append(f"{icon} {car_id} | {end_date.strftime('%d.%m.%y')} | {company}")
    return "\n".join(lines)


async def check_service_and_insurance_notifications(context: ContextTypes.DEFAULT_TYPE):
    snapshot = get_data_snapshot(force_refresh=True)
    today = datetime.now(KYIV_TZ).date()
    messages: List[str] = []

    for car_id in KNOWN_CAR_IDS:
        rows = None
        for title, data in snapshot.items():
            if car_id in title or VEHICLE_MAP.get(car_id, "") in title:
                rows = data
                break
        if not rows:
            continue

        current_odo = get_current_odometer_from_rows(rows)

        oil_date, oil_odo = find_last_service(rows, "oil")
        if oil_odo is not None and current_odo is not None:
            remaining = 10000 - (max(current_odo, oil_odo) - oil_odo)
            if remaining <= 1000:
                messages.append(f"ð {car_id} â Ð¼Ð°ÑÐ»Ð¾ ÑÐµÑÐµÐ· {format_km(remaining)} ÐºÐ¼")

        if car_id not in SKIP_GRM:
            grm_date, grm_odo = find_last_service(rows, "grm")
            if grm_odo is not None and current_odo is not None:
                remaining = 50000 - (max(current_odo, grm_odo) - grm_odo)
                if remaining <= 1000:
                    messages.append(f"ð {car_id} â ÐÐ Ð ÑÐµÑÐµÐ· {format_km(remaining)} ÐºÐ¼")

        best: Optional[Tuple[date, str]] = None
        for row in rows[7:]:
            if len(row) >= INSURANCE_COMPANY_COL:
                d = parse_short_date(row[INSURANCE_DATE_COL - 1])
                company = str(row[INSURANCE_COMPANY_COL - 1]).strip()
                if d and company:
                    if best is None or d > best[0]:
                        best = (d, company)
        if best:
            end_date, company = best
            days_left = (end_date - today).days
            if days_left <= 14:
                messages.append(f"ð {car_id} â ÑÑÑÐ°ÑÐ¾Ð²ÐºÐ° ÑÐµÑÐµÐ· {days_left} Ð´Ð½. ({company})")

    if messages:
        text = "â ï¸ ÐÐ°Ð³Ð°Ð´ÑÐ²Ð°Ð½Ð½Ñ:\n\n" + "\n".join(messages)
        for user_id in ALLOWED_USERS:
            try:
                await context.bot.send_message(chat_id=user_id, text=text)
            except Exception as e:
                logger.error(f"Notification send error: {e}")


# ===== USD rate =====

def get_usd_black_rate_dnipro() -> Optional[float]:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(MINFIN_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    patterns = [
        r"Ð¡ÑÐµÐ´Ð½ÑÑ Ð¿Ð¾ÐºÑÐ¿ÐºÐ°\s*([0-9]+[.,][0-9]+)",
        r"Ð¡ÐµÑÐµÐ´Ð½Ñ ÐºÑÐ¿ÑÐ²Ð»Ñ\s*([0-9]+[.,][0-9]+)",
        r"ÐÐ¾ÐºÑÐ¿ÐºÐ°\s*([0-9]+[.,][0-9]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", "."))
    matches = re.findall(r"\b([0-9]{2}[.,][0-9]{2})\b", text)
    for val in matches:
        num = float(val.replace(",", "."))
        if 35 <= num <= 50:
            return num
    return None


# ===== Duplicate / anomaly =====

def get_last_8_weekly_points(ws) -> List[Tuple[date, int]]:
    all_vals = ws.get_all_values()
    points = []
    for row in all_vals[7:]:
        d = parse_short_date(row[10] if len(row) > 10 else None)
        odo = parse_num(row[11] if len(row) > 11 else None)
        if d and odo is not None:
            points.append((d, odo))
    return points[-8:]


def estimate_odometer_for_car(car_id: str, target_date_str: Optional[str] = None) -> Optional[int]:
    spreadsheet = get_sheet()
    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return None
    points = get_last_8_weekly_points(ws)
    if not points:
        return None
    target_date = parse_short_date(target_date_str) or datetime.now(KYIV_TZ).date()
    last_date, last_odo = points[-1]
    if target_date <= last_date:
        return last_odo

    daily_rates = []
    for i in range(1, len(points)):
        prev_date, prev_odo = points[i - 1]
        curr_date, curr_odo = points[i]
        delta_days = (curr_date - prev_date).days
        delta_km = curr_odo - prev_odo
        if delta_days > 0 and 0 <= delta_km <= 7000:
            rate = delta_km / delta_days
            if 0 <= rate <= 300:
                daily_rates.append(rate)
    if daily_rates:
        median_daily_rate = median(daily_rates)
        future_days = (target_date - last_date).days
        return int(round(last_odo + median_daily_rate * future_days))
    return last_odo


def odometer_is_anomalous(ws, new_odometer: int, operation_date_str: Optional[str]) -> bool:
    points = get_last_8_weekly_points(ws)
    if not points:
        return False
    last_date, last_odo = points[-1]
    target_date = parse_short_date(operation_date_str) or datetime.now(KYIV_TZ).date()
    if new_odometer <= last_odo:
        return False
    delta_km = new_odometer - last_odo
    delta_days = max((target_date - last_date).days, 1)
    weekly_equivalent = delta_km * 7 / delta_days
    return weekly_equivalent > 2500


def detect_duplicate(ws, action: dict, raw_text: str = "") -> bool:
    all_vals = ws.get_all_values()
    op_type = action.get("type")
    if op_type == "expense":
        for row in reversed(all_vals[7:]):
            if len(row) >= 9 and any(str(x).strip() for x in row[4:9]):
                last_date = str(row[4]).strip() if len(row) > 4 else ""
                last_odo = parse_num(row[5] if len(row) > 5 else None)
                last_desc = str(row[6]).strip().lower() if len(row) > 6 else ""
                last_amount = parse_num(row[7] if len(row) > 7 else None)
                return (
                    last_date == normalize_date_short(action.get("date"))
                    and last_odo == parse_num(action.get("odometer"))
                    and last_amount == parse_num(action.get("amount"))
                    and last_desc == str(action.get("description", "")).strip().lower()
                )
        return False
    if op_type == "income":
        for row in reversed(all_vals[7:]):
            if len(row) >= 15 and any(str(x).strip() for x in row[10:15]):
                last_date = str(row[10]).strip()
                last_odo = parse_num(row[11] if len(row) > 11 else None)
                last_amount = parse_num(row[12] if len(row) > 12 else None)
                return (
                    last_date == normalize_date_short(action.get("date"))
                    and last_odo == parse_num(action.get("odometer"))
                    and last_amount == parse_num(action.get("amount"))
                )
        return False
    if op_type in ["liability_minus", "liability_plus"]:
        current_desc = build_liability_description(op_type, raw_text, action.get("description")).lower()
        current_amount = -abs(float(action.get("amount", 0))) if op_type == "liability_minus" else abs(float(action.get("amount", 0)))
        for row in reversed(all_vals[7:]):
            if len(row) >= 17 and any(str(x).strip() for x in row[15:17]):
                last_date = str(row[10]).strip() if len(row) > 10 else ""
                last_amount = str(row[15]).strip() if len(row) > 15 else ""
                last_desc = str(row[16]).strip().lower() if len(row) > 16 else ""
                amount_str = str(int(current_amount)) if float(current_amount).is_integer() else str(current_amount)
                return (
                    last_date == normalize_date_short(action.get("date"))
                    and last_amount == amount_str
                    and last_desc == current_desc
                )
        return False
    return False


# ===== Write to sheet =====

def get_last_used_row_for_block(ws, start_col: int, end_col: int, start_row: int = 8) -> int:
    all_vals = ws.get_all_values()
    last_used = start_row - 1
    for row_idx in range(start_row, len(all_vals) + 1):
        row = all_vals[row_idx - 1]
        block = row[start_col - 1:end_col]
        if any(str(cell).strip() for cell in block):
            last_used = row_idx
    return last_used


def get_next_expense_row(ws) -> int:
    return get_last_used_row_for_block(ws, 5, 9, 8) + 1


def get_next_right_block_row(ws) -> int:
    return max(get_last_used_row_for_block(ws, 11, 15, 8), get_last_used_row_for_block(ws, 16, 17, 8)) + 1


def get_previous_income_odometer(ws) -> Optional[int]:
    all_vals = ws.get_all_values()
    odometers = []
    for row in all_vals[7:]:
        if len(row) > 11:
            value = parse_num(row[11])
            if value is not None:
                odometers.append(value)
    return odometers[-1] if odometers else None


def write_expense_rows(ws, date_value: str, odometer: int, items: List[Dict[str, Any]], usd_rate: Optional[float], odometer_estimated: bool):
    start_row = get_next_expense_row(ws)
    rows = []
    for item in items:
        amount = float(item["amount"])
        usd_amount = round(amount / usd_rate, 2) if usd_rate else ""
        rows.append([date_value, odometer, item["description"], amount, usd_amount])
    end_row = start_row + len(rows) - 1
    rng = f"E{start_row}:I{end_row}"
    ws.update(rng, rows)
    apply_blue_text(ws, rng)
    if odometer_estimated:
        for row_idx in range(start_row, end_row + 1):
            mark_cell_yellow(ws, f"F{row_idx}")
    return start_row, end_row, sum(float(x["amount"]) for x in items)


def write_single_action_to_sheet(data: dict, raw_text: str = "") -> str:
    spreadsheet = get_sheet()
    car_id = str(data.get("car_id", "")).strip()
    full_plate = full_plate_from_short(car_id)

    date_value = normalize_date_short(data.get("date"))
    amount = float(data.get("amount", 0) or 0)
    odometer = data.get("odometer", "")
    description = data.get("description", "")
    odometer_estimated = bool(data.get("odometer_estimated", False))
    op_type = data.get("type")

    usd_rate = None
    usd_note = ""
    try:
        usd_rate = get_usd_black_rate_dnipro()
        if usd_rate:
            usd_note = f"\nð± ÐÑÑÑ USD: {usd_rate}"
    except Exception as e:
        logger.error(f"USD rate error: {e}")
        usd_note = "\nâ ï¸ ÐÑÑÑ USD Ð½Ðµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¾ÑÑÐ¸Ð¼Ð°ÑÐ¸"

    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return f"â ÐÐ°ÑÐ¸Ð½Ñ {full_plate} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾ Ð² ÑÐ°Ð±Ð»Ð¸ÑÑ"
    sheet_name = ws.title

    if op_type == "expense":
        desc_lower = str(description).lower().strip()
        if desc_lower in {"ÑÐ¾", "Ð¿Ð»Ð°Ð½Ð¾Ð²Ð¾Ðµ ÑÐ¾", "Ð¿Ð»Ð°Ð½Ð¾Ð²Ðµ ÑÐ¾"} or is_to_phrase(description):
            start_row, end_row, total_amount = write_expense_rows(ws, date_value, odometer, TO_BUNDLE, usd_rate, odometer_estimated)
            return (
                f"â Ð¢Ð Ð²Ð½ÐµÑÐµÐ½Ð¾!\nð ÐÐ°ÑÐ¸Ð½Ð°: {full_plate}\nð§¾ ÐÐ¾Ð´Ð°Ð½Ð¾ 5 ÑÑÐ´ÐºÑÐ²\n"
                f"ð¸ ÐÐ°Ð³Ð°Ð»ÑÐ½Ð° ÑÑÐ¼Ð°: {total_amount} Ð³ÑÐ½\nð {date_value}\n"
                f"ð ÐÐ½ÐµÑÐµÐ½Ð¾: Ð»Ð¸ÑÑ '{sheet_name}', ÑÑÐ´ÐºÐ¸ {start_row}-{end_row}, ÑÑÐ¾Ð²Ð¿ÑÑ E:I{usd_note}"
            )

        next_row = get_next_expense_row(ws)
        usd_amount = round(amount / usd_rate, 2) if usd_rate else ""
        rng = f"E{next_row}:I{next_row}"
        ws.update(rng, [[date_value, odometer, description, amount, usd_amount]])
        apply_blue_text(ws, rng)
        if odometer_estimated:
            mark_cell_yellow(ws, f"F{next_row}")
        return (
            f"â ÐÐ¸ÑÑÐ°ÑÐ° Ð²Ð½ÐµÑÐµÐ½Ð°!\nð ÐÐ°ÑÐ¸Ð½Ð°: {full_plate}\nð {description}\nð¸ {amount} Ð³ÑÐ½\n"
            f"ð {date_value}\nð ÐÐ½ÐµÑÐµÐ½Ð¾: Ð»Ð¸ÑÑ '{sheet_name}', ÑÑÐ´Ð¾Ðº {next_row}, ÑÑÐ¾Ð²Ð¿ÑÑ E:I{usd_note}"
        )

    if op_type == "income":
        next_row = get_next_right_block_row(ws)
        usd_amount = round(amount / usd_rate, 2) if usd_rate else ""
        prev_odo = get_previous_income_odometer(ws)
        mileage_delta = ""
        if prev_odo is not None and odometer not in ("", None):
            try:
                mileage_delta = int(odometer) - int(prev_odo)
            except Exception:
                mileage_delta = ""
        rng = f"K{next_row}:O{next_row}"
        ws.update(rng, [[date_value, odometer, amount, usd_amount, mileage_delta]])
        apply_blue_text(ws, rng)
        if odometer_estimated:
            mark_cell_yellow(ws, f"L{next_row}")
        delta_text = f"\nð Ð ÑÐ·Ð½Ð¸ÑÑ Ð¿ÑÐ¾Ð±ÑÐ³Ñ: {mileage_delta}" if mileage_delta != "" else ""
        return (
            f"â ÐÐ¾ÑÑÐ´ Ð²Ð½ÐµÑÐµÐ½Ð¾!\nð ÐÐ°ÑÐ¸Ð½Ð°: {full_plate}\nð° {amount} Ð³ÑÐ½\nð {date_value}\nð ÐÐ´Ð¾Ð¼ÐµÑÑ: {odometer}\n"
            f"ð ÐÐ½ÐµÑÐµÐ½Ð¾: Ð»Ð¸ÑÑ '{sheet_name}', ÑÑÐ´Ð¾Ðº {next_row}, ÑÑÐ¾Ð²Ð¿ÑÑ K:O{delta_text}{usd_note}"
        )

    if op_type in ["liability_minus", "liability_plus"]:
        next_row = get_next_right_block_row(ws)
        sign_amount = -abs(amount) if op_type == "liability_minus" else abs(amount)
        liability_desc = build_liability_description(op_type, raw_text, description)
        rng = f"K{next_row}:Q{next_row}"
        ws.update(rng, [[date_value, "", "", "", "", sign_amount, liability_desc]])
        apply_blue_text(ws, rng)
        label = "Ð¨ÑÑÐ°Ñ/Ð±Ð¾ÑÐ³" if op_type == "liability_minus" else "ÐÐ¾Ð³Ð°ÑÐµÐ½Ð½Ñ/Ð½Ð°Ð´ÑÐ¾Ð´Ð¶ÐµÐ½Ð½Ñ"
        return (
            f"â {label} Ð²Ð½ÐµÑÐµÐ½Ð¾!\nð ÐÐ°ÑÐ¸Ð½Ð°: {full_plate}\nðµ {sign_amount} Ð³ÑÐ½\nð {liability_desc}\n"
            f"ð ÐÐ½ÐµÑÐµÐ½Ð¾: Ð»Ð¸ÑÑ '{sheet_name}', ÑÑÐ´Ð¾Ðº {next_row}, ÑÑÐ¾Ð²Ð¿ÑÑ P:Q"
        )

    return "â ÐÐµÐ²ÑÐ´Ð¾Ð¼Ð¸Ð¹ ÑÐ¸Ð¿ Ð¾Ð¿ÐµÑÐ°ÑÑÑ"


def write_actions_to_sheet(actions: List[dict], raw_text: str = "") -> str:
    return "\n\n".join(write_single_action_to_sheet(action, raw_text=raw_text) for action in actions)


def is_yes_statistical(text: str) -> bool:
    return str(text).lower().strip() in {"ÑÐ°Ðº", "Ð´Ð°", "yes", "Ð¾Ðº", "Ð¾ÐºÐµÐ¹", "Ð°Ð³Ð°"}


def is_yes_confirm(text: str) -> bool:
    return str(text).lower().strip() in {"ÑÐ°Ðº", "Ð´Ð°", "yes", "Ð½Ð¾Ð²Ð¸Ð¹", "Ð½Ð¾Ð²Ð°Ñ", "Ð½Ð¾Ð²Ð¾Ðµ"}


def is_no_confirm(text: str) -> bool:
    return str(text).lower().strip() in {"Ð½Ñ", "Ð½ÐµÑ", "Ð´ÑÐ±Ð»Ñ", "ÑÐºÐ°ÑÑÐ²Ð°ÑÐ¸", "Ð¾ÑÐ¼ÐµÐ½Ð°", "cancel"}


def actions_need_odometer(actions: List[dict]) -> bool:
    return any(action.get("type") in ["expense", "income"] and action.get("odometer") in (None, "") for action in actions)


def fill_odometer_for_actions(actions: List[dict], odometer_value: int, estimated: bool) -> None:
    for action in actions:
        if action.get("type") in ["expense", "income"] and action.get("odometer") in (None, ""):
            action["odometer"] = odometer_value
            action["odometer_estimated"] = estimated


def detect_month_summary_request(text: str) -> Optional[str]:
    t = str(text or "").lower()
    if any(x in t for x in ["Ð¼ÑÑÑÑÑ", "Ð¼ÐµÑÑÑ", "Ð¿Ð¾ÑÐ¾ÑÐ½Ð¸Ð¹ Ð¼ÑÑÑÑÑ", "ÑÐµÐºÑÑÐ¸Ð¹ Ð¼ÐµÑÑÑ"]):
        for car_id in KNOWN_CAR_IDS:
            if re.search(rf"(?<!\d){re.escape(car_id)}(?!\d)", text):
                return car_id
    return None


def monthly_summary(car_id: str) -> str:
    spreadsheet = get_sheet()
    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return f"â ÐÐ°ÑÐ¸Ð½Ñ {car_id} Ð½Ðµ Ð·Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾ Ð² ÑÐ°Ð±Ð»Ð¸ÑÑ"

    today = datetime.now(KYIV_TZ)
    month = today.month
    year = today.year

    all_vals = ws.get_all_values()
    income_sum = 0.0
    expense_sum = 0.0
    liability_sum = 0.0

    for row in all_vals[7:]:
        if len(row) > 7:
            d = parse_short_date(row[4] if len(row) > 4 else None)
            num = parse_num(row[7] if len(row) > 7 else None)
            if d and d.month == month and d.year == year and num is not None:
                expense_sum += num
        if len(row) > 12:
            d = parse_short_date(row[10] if len(row) > 10 else None)
            num = parse_num(row[12] if len(row) > 12 else None)
            if d and d.month == month and d.year == year and num is not None:
                income_sum += num
        if len(row) > 15:
            d = parse_short_date(row[10] if len(row) > 10 else None)
            raw_p = row[15] if len(row) > 15 else None
            if d and d.month == month and d.year == year and str(raw_p).strip():
                try:
                    liability_sum += float(str(raw_p).replace(",", "."))
                except ValueError:
                    pass

    def fmt(x: float) -> str:
        return str(int(x)) if x.is_integer() else str(round(x, 2))
    return (
        f"ð ÐÐ° Ð¿Ð¾ÑÐ¾ÑÐ½Ð¸Ð¹ Ð¼ÑÑÑÑÑ Ð¿Ð¾ {car_id}:\n"
        f"ð° ÐÐ¾ÑÑÐ´: {fmt(income_sum)} Ð³ÑÐ½\n"
        f"ð¸ ÐÐ¸ÑÑÐ°ÑÐ¸: {fmt(expense_sum)} Ð³ÑÐ½\n"
        f"ð ÐÐ°Ð»Ð¸ÑÐ¾Ðº Ð±Ð¾ÑÐ³Ñ: {fmt(liability_sum)} Ð³ÑÐ½"
    )


# ===== Command detectors =====

def is_oil_report_request(text: str) -> bool:
    t = str(text or "").lower().strip()
    return t in {"Ð¼Ð°ÑÐ»Ð¾", "Ð·Ð°Ð¼ÐµÐ½Ð° Ð¼Ð°ÑÐ»Ð°", "ÑÐ¾", "Ð¿Ð»Ð°Ð½Ð¾Ð²Ð¾Ðµ ÑÐ¾", "Ð¿Ð»Ð°Ð½Ð¾Ð²Ðµ ÑÐ¾"}


def is_grm_report_request(text: str) -> bool:
    t = str(text or "").lower().strip()
    return t in {"Ð³ÑÐ¼", "Ð·Ð°Ð¼ÐµÐ½Ð° Ð³ÑÐ¼", "ÐºÐ¾Ð¼Ð¿Ð»ÐµÐºÑ Ð³ÑÐ¼"}


def is_insurance_report_request(text: str) -> bool:
    t = str(text or "").lower().strip()
    return t in {"ÑÑÑÐ°ÑÐ¾Ð²ÐºÐ°", "ÑÑÑÐ°ÑÑÐ²Ð°Ð½Ð½Ñ", "ÑÑÑÐ°ÑÐ¾Ð²ÐºÐ°?"}


# ===== Telegram handlers =====

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("â ÐÐ¾ÑÑÑÐ¿ Ð·Ð°Ð±Ð¾ÑÐ¾Ð½ÐµÐ½Ð¾")
        return

    text = (update.message.text or "").strip()
    logger.info(f"Incoming message from {user_id}: {text}")

    try:
        if context.user_data.get("waiting_duplicate_confirm"):
            pending_actions = context.user_data.get("pending_actions_after_duplicate", [])
            if is_yes_confirm(text):
                context.user_data.pop("waiting_duplicate_confirm", None)
                context.user_data.pop("pending_actions_after_duplicate", None)
                result = write_actions_to_sheet(pending_actions, raw_text=text)
                await update.message.reply_text(result)
                return
            if is_no_confirm(text):
                context.user_data.pop("waiting_duplicate_confirm", None)
                context.user_data.pop("pending_actions_after_duplicate", None)
                await update.message.reply_text("â ÐÐ°Ð¿Ð¸Ñ ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾ ÑÐº Ð´ÑÐ±Ð»Ñ.")
                return
            await update.message.reply_text("ÐÐ°Ð¿Ð¸ÑÐ¸ Â«Ð½Ð¾Ð²Ð¸Ð¹Â» Ð°Ð±Ð¾ Â«Ð´ÑÐ±Ð»ÑÂ».")
            return

        if context.user_data.get("waiting_odometer_anomaly_confirm"):
            pending_actions = context.user_data.get("pending_actions_after_anomaly", [])
            if is_yes_confirm(text):
                context.user_data.pop("waiting_odometer_anomaly_confirm", None)
                context.user_data.pop("pending_actions_after_anomaly", None)
                result = write_actions_to_sheet(pending_actions, raw_text=text)
                await update.message.reply_text(result)
                return
            if is_no_confirm(text):
                context.user_data.pop("waiting_odometer_anomaly_confirm", None)
                context.user_data["waiting_odometer_choice_actions"] = True
                context.user_data["pending_actions"] = pending_actions
                context.user_data.pop("pending_actions_after_anomaly", None)
                await update.message.reply_text("ÐÐ¾Ð±ÑÐµ. ÐÐ°Ð´ÑÑÐ»Ð¸ Ð¿ÑÐ°Ð²Ð¸Ð»ÑÐ½Ð¸Ð¹ Ð¾Ð´Ð¾Ð¼ÐµÑÑ Ð°Ð±Ð¾ Ð½Ð°Ð¿Ð¸ÑÐ¸ Â«ÑÐ°ÐºÂ», ÑÐ¾Ð± Ñ Ð¿ÑÐ´ÑÑÐ°Ð²Ð¸Ð² ÑÐµÑÐµÐ´Ð½ÑÐ¾ÑÑÐ°ÑÐ¸ÑÑÐ¸ÑÐ½Ð¸Ð¹.")
                return
            await update.message.reply_text("ÐÐ°Ð¿Ð¸ÑÐ¸ Â«ÑÐ°ÐºÂ» Ð´Ð»Ñ Ð¿ÑÐ´ÑÐ²ÐµÑÐ´Ð¶ÐµÐ½Ð½Ñ Ð°Ð±Ð¾ Â«Ð½ÑÂ» Ð´Ð»Ñ ÑÐºÐ°ÑÑÐ²Ð°Ð½Ð½Ñ.")
            return

        if context.user_data.get("waiting_odometer_choice_actions"):
            pending_actions = context.user_data.get("pending_actions", [])
            numeric_odo = parse_num(text)
            if numeric_odo is not None:
                fill_odometer_for_actions(pending_actions, numeric_odo, estimated=False)
                context.user_data.pop("waiting_odometer_choice_actions", None)
                context.user_data.pop("pending_actions", None)

                first_action = next((a for a in pending_actions if a.get("type") in ["expense", "income"]), None)
                if first_action:
                    spreadsheet = get_sheet()
                    ws = get_matching_worksheet(spreadsheet, first_action["car_id"])
                    if ws and odometer_is_anomalous(ws, numeric_odo, first_action.get("date")):
                        context.user_data["waiting_odometer_anomaly_confirm"] = True
                        context.user_data["pending_actions_after_anomaly"] = pending_actions
                        await update.message.reply_text("â ï¸ ÐÑÐ¾Ð±ÑÐ³ Ð²Ð¸Ð³Ð»ÑÐ´Ð°Ñ Ð½ÐµÑÐ¸Ð¿Ð¾Ð²Ð¾ Ð²ÐµÐ»Ð¸ÐºÐ¸Ð¼. ÐÑÐ´ÑÐ²ÐµÑÐ´Ð¸ÑÐ¸?")
                        return

                spreadsheet = get_sheet()
                for action in pending_actions:
                    ws = get_matching_worksheet(spreadsheet, action["car_id"])
                    if ws and detect_duplicate(ws, action, raw_text=text):
                        context.user_data["waiting_duplicate_confirm"] = True
                        context.user_data["pending_actions_after_duplicate"] = pending_actions
                        await update.message.reply_text("â Ð¦Ðµ Ð½Ð¾Ð²Ð¸Ð¹ Ð·Ð°Ð¿Ð¸Ñ ÑÐ¸ Ð´ÑÐ±Ð»Ñ Ð¿Ð¾Ð¿ÐµÑÐµÐ´Ð½ÑÐ¾Ð³Ð¾?")
                        return

                result = write_actions_to_sheet(pending_actions, raw_text=text)
                await update.message.reply_text(result)
                return

            if is_yes_statistical(text):
                if not pending_actions:
                    await update.message.reply_text("ÐÐµÐ¼Ð°Ñ Ð´Ð°Ð½Ð¸Ñ Ð´Ð»Ñ Ð¾Ð±ÑÐ¾Ð±ÐºÐ¸.")
                    return
                first_action = next((a for a in pending_actions if a.get("type") in ["expense", "income"]), None)
                if not first_action:
                    result = write_actions_to_sheet(pending_actions, raw_text=text)
                    await update.message.reply_text(result)
                    return
                estimated = estimate_odometer_for_car(first_action["car_id"], first_action.get("date"))
                if not estimated:
                    context.user_data.pop("waiting_odometer_choice_actions", None)
                    context.user_data.pop("pending_actions", None)
                    await update.message.reply_text("ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¾Ð±ÑÐ¸ÑÐ»Ð¸ÑÐ¸ ÑÐµÑÐµÐ´Ð½ÑÐ¾ÑÑÐ°ÑÐ¸ÑÑÐ¸ÑÐ½Ð¸Ð¹ Ð¿ÑÐ¾Ð±ÑÐ³. ÐÐ°Ð´ÑÑÐ»Ð¸, Ð±ÑÐ´Ñ Ð»Ð°ÑÐºÐ°, ÑÐ¸ÑÑÐ¸ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.")
                    return
                fill_odometer_for_actions(pending_actions, estimated, estimated=True)
                context.user_data.pop("waiting_odometer_choice_actions", None)
                context.user_data.pop("pending_actions", None)

                spreadsheet = get_sheet()
                for action in pending_actions:
                    ws = get_matching_worksheet(spreadsheet, action["car_id"])
                    if ws and detect_duplicate(ws, action, raw_text=text):
                        context.user_data["waiting_duplicate_confirm"] = True
                        context.user_data["pending_actions_after_duplicate"] = pending_actions
                        await update.message.reply_text("â Ð¦Ðµ Ð½Ð¾Ð²Ð¸Ð¹ Ð·Ð°Ð¿Ð¸Ñ ÑÐ¸ Ð´ÑÐ±Ð»Ñ Ð¿Ð¾Ð¿ÐµÑÐµÐ´Ð½ÑÐ¾Ð³Ð¾?")
                        return

                result = write_actions_to_sheet(pending_actions, raw_text=text)
                await update.message.reply_text(result)
                return

            await update.message.reply_text("ÐÐ°Ð¿Ð¸ÑÐ¸ Â«ÑÐ°ÐºÂ», ÑÐºÑÐ¾ Ð¼ÐµÐ½Ñ Ð´Ð¾Ð´Ð°ÑÐ¸ ÑÐµÑÐµÐ´Ð½ÑÐ¾ÑÑÐ°ÑÐ¸ÑÑÐ¸ÑÐ½Ð¸Ð¹ Ð¿ÑÐ¾Ð±ÑÐ³, Ð°Ð±Ð¾ Ð¿ÑÐ¾ÑÑÐ¾ Ð½Ð°Ð´ÑÑÐ»Ð¸ ÑÐ¸ÑÑÐ¸ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.")
            return

        if context.user_data.get("waiting_odometer_choice"):
            pending_data = context.user_data.get("pending_data", {})
            numeric_odo = parse_num(text)
            if numeric_odo is not None:
                pending_data["odometer"] = numeric_odo
                pending_data["odometer_estimated"] = False
                pending_data["missing_fields"] = []
                context.user_data["pending_data"] = pending_data
                context.user_data.pop("waiting_odometer_choice", None)

                spreadsheet = get_sheet()
                ws = get_matching_worksheet(spreadsheet, pending_data["car_id"])
                if ws and odometer_is_anomalous(ws, numeric_odo, pending_data.get("date")):
                    context.user_data["waiting_odometer_anomaly_confirm"] = True
                    context.user_data["pending_actions_after_anomaly"] = [pending_data]
                    context.user_data.pop("pending_data", None)
                    await update.message.reply_text("â ï¸ ÐÑÐ¾Ð±ÑÐ³ Ð²Ð¸Ð³Ð»ÑÐ´Ð°Ñ Ð½ÐµÑÐ¸Ð¿Ð¾Ð²Ð¾ Ð²ÐµÐ»Ð¸ÐºÐ¸Ð¼. ÐÑÐ´ÑÐ²ÐµÑÐ´Ð¸ÑÐ¸?")
                    return
                if ws and detect_duplicate(ws, pending_data, raw_text=text):
                    context.user_data["waiting_duplicate_confirm"] = True
                    context.user_data["pending_actions_after_duplicate"] = [pending_data]
                    context.user_data.pop("pending_data", None)
                    await update.message.reply_text("â Ð¦Ðµ Ð½Ð¾Ð²Ð¸Ð¹ Ð·Ð°Ð¿Ð¸Ñ ÑÐ¸ Ð´ÑÐ±Ð»Ñ Ð¿Ð¾Ð¿ÐµÑÐµÐ´Ð½ÑÐ¾Ð³Ð¾?")
                    return

                result = write_single_action_to_sheet(pending_data, raw_text=text)
                context.user_data.pop("pending_data", None)
                await update.message.reply_text(result)
                return

            if is_yes_statistical(text):
                car_id = pending_data.get("car_id")
                operation_date = pending_data.get("date")
                if not car_id:
                    context.user_data.pop("waiting_odometer_choice", None)
                    await update.message.reply_text("Ð¡Ð¿Ð¾ÑÐ°ÑÐºÑ Ð²ÐºÐ°Ð¶Ð¸ Ð½Ð¾Ð¼ÐµÑ Ð¼Ð°ÑÐ¸Ð½Ð¸.")
                    return
                estimated = estimate_odometer_for_car(car_id, operation_date)
                if not estimated:
                    context.user_data.pop("waiting_odometer_choice", None)
                    await update.message.reply_text("ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð¾Ð±ÑÐ¸ÑÐ»Ð¸ÑÐ¸ ÑÐµÑÐµÐ´Ð½ÑÐ¾ÑÑÐ°ÑÐ¸ÑÑÐ¸ÑÐ½Ð¸Ð¹ Ð¿ÑÐ¾Ð±ÑÐ³. ÐÐ°Ð´ÑÑÐ»Ð¸, Ð±ÑÐ´Ñ Ð»Ð°ÑÐºÐ°, ÑÐ¸ÑÑÐ¸ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.")
                    return

                pending_data["odometer"] = estimated
                pending_data["odometer_estimated"] = True
                pending_data["missing_fields"] = []
                context.user_data["pending_data"] = pending_data
                context.user_data.pop("waiting_odometer_choice", None)

                spreadsheet = get_sheet()
                ws = get_matching_worksheet(spreadsheet, pending_data["car_id"])
                if ws and detect_duplicate(ws, pending_data, raw_text=text):
                    context.user_data["waiting_duplicate_confirm"] = True
                    context.user_data["pending_actions_after_duplicate"] = [pending_data]
                    context.user_data.pop("pending_data", None)
                    await update.message.reply_text("â Ð¦Ðµ Ð½Ð¾Ð²Ð¸Ð¹ Ð·Ð°Ð¿Ð¸Ñ ÑÐ¸ Ð´ÑÐ±Ð»Ñ Ð¿Ð¾Ð¿ÐµÑÐµÐ´Ð½ÑÐ¾Ð³Ð¾?")
                    return

                result = write_single_action_to_sheet(pending_data, raw_text=text)
                context.user_data.pop("pending_data", None)
                await update.message.reply_text(result)
                return

            await update.message.reply_text("ÐÐ°Ð¿Ð¸ÑÐ¸ Â«ÑÐ°ÐºÂ», ÑÐºÑÐ¾ Ð¼ÐµÐ½Ñ Ð´Ð¾Ð´Ð°ÑÐ¸ ÑÐµÑÐµÐ´Ð½ÑÐ¾ÑÑÐ°ÑÐ¸ÑÑÐ¸ÑÐ½Ð¸Ð¹ Ð¿ÑÐ¾Ð±ÑÐ³, Ð°Ð±Ð¾ Ð¿ÑÐ¾ÑÑÐ¾ Ð½Ð°Ð´ÑÑÐ»Ð¸ ÑÐ¸ÑÑÐ¸ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.")
            return

        if is_oil_report_request(text):
            report = build_oil_report()
            await update.message.reply_text("ð¢ Ð¡ÑÐ°Ð½ Ð¼Ð°ÑÐ»Ð°:\n\n" + (report or "ÐÐµÐ¼Ð°Ñ Ð´Ð°Ð½Ð¸Ñ"))
            return

        if is_grm_report_request(text):
            report = build_grm_report()
            await update.message.reply_text("âï¸ Ð¡ÑÐ°Ð½ ÐÐ Ð:\n\n" + (report or "ÐÐµÐ¼Ð°Ñ Ð´Ð°Ð½Ð¸Ñ"))
            return

        if is_insurance_report_request(text):
            report = build_insurance_report()
            await update.message.reply_text("ð¡ Ð¡ÑÑÐ°ÑÐ¾Ð²ÐºÐ°:\n\n" + (report or "ÐÐµÐ¼Ð°Ñ Ð´Ð°Ð½Ð¸Ñ"))
            return

        car_id_for_summary = detect_month_summary_request(text)
        if car_id_for_summary:
            await update.message.reply_text(monthly_summary(car_id_for_summary))
            return

        await update.message.reply_text("â³ ÐÐ±ÑÐ¾Ð±Ð»ÑÑ...")

        heuristic_actions = heuristic_multi_parse(text)
        if heuristic_actions:
            if actions_need_odometer(heuristic_actions):
                context.user_data["pending_actions"] = heuristic_actions
                context.user_data["waiting_odometer_choice_actions"] = True
                await update.message.reply_text("â ÐÐµÐ¼Ð°Ñ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.\nÐÐµÐ½Ñ Ð´Ð¾Ð´Ð°ÑÐ¸ ÑÐµÑÐµÐ´Ð½ÑÐ¾ÑÑÐ°ÑÐ¸ÑÑÐ¸ÑÐ½Ð¸Ð¹ Ð¿ÑÐ¾Ð±ÑÐ³?\nÐÐ°Ð¿Ð¸ÑÐ¸ Â«ÑÐ°ÐºÂ» Ð°Ð±Ð¾ Ð¿ÑÐ¾ÑÑÐ¾ Ð½Ð°Ð´ÑÑÐ»Ð¸ ÑÐ¸ÑÑÐ¸ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.")
                return

            spreadsheet = get_sheet()
            for action in heuristic_actions:
                ws = get_matching_worksheet(spreadsheet, action["car_id"])
                if ws and detect_duplicate(ws, action, raw_text=text):
                    context.user_data["waiting_duplicate_confirm"] = True
                    context.user_data["pending_actions_after_duplicate"] = heuristic_actions
                    await update.message.reply_text("â Ð¦Ðµ Ð½Ð¾Ð²Ð¸Ð¹ Ð·Ð°Ð¿Ð¸Ñ ÑÐ¸ Ð´ÑÐ±Ð»Ñ Ð¿Ð¾Ð¿ÐµÑÐµÐ´Ð½ÑÐ¾Ð³Ð¾?")
                    return

            result = write_actions_to_sheet(heuristic_actions, raw_text=text)
            await update.message.reply_text(result)
            return

        pending_data = context.user_data.get("pending_data")
        parsed = ask_ai(text, existing_data=pending_data)
        if "error" in parsed:
            await update.message.reply_text(f"â AI ÑÐ¸Ð¼ÑÐ°ÑÐ¾Ð²Ð¾ Ð½ÐµÐ´Ð¾ÑÑÑÐ¿Ð½Ð¸Ð¹.\n\nÐÐµÑÐ°Ð»Ñ: {parsed['error']}")
            return

        parsed["car_id"] = resolve_car_id(parsed.get("car_id"))
        parsed["date"] = normalize_date_short(parsed.get("date"))
        parsed = apply_special_cases(parsed, text)
        parsed["missing_fields"] = compute_missing_fields(parsed, text)

        if "car_id" in parsed["missing_fields"]:
            context.user_data["pending_data"] = parsed
            await update.message.reply_text(f"â ÐÐµ Ð²Ð´Ð°Ð»Ð¾ÑÑ Ð²Ð¸Ð·Ð½Ð°ÑÐ¸ÑÐ¸ Ð¼Ð°ÑÐ¸Ð½Ñ.\nÐÐºÐ°Ð¶Ð¸ Ð½Ð¾Ð¼ÐµÑ Ð¼Ð°ÑÐ¸Ð½Ð¸ Ð· ÑÑÐ¾Ð³Ð¾ ÑÐ¿Ð¸ÑÐºÑ:\n{', '.join(KNOWN_CAR_IDS)}")
            return

        if parsed["missing_fields"]:
            context.user_data["pending_data"] = parsed
            if "odometer" in parsed["missing_fields"]:
                context.user_data["waiting_odometer_choice"] = True
                await update.message.reply_text("â ÐÐµÐ¼Ð°Ñ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.\nÐÐµÐ½Ñ Ð´Ð¾Ð´Ð°ÑÐ¸ ÑÐµÑÐµÐ´Ð½ÑÐ¾ÑÑÐ°ÑÐ¸ÑÑÐ¸ÑÐ½Ð¸Ð¹ Ð¿ÑÐ¾Ð±ÑÐ³?\nÐÐ°Ð¿Ð¸ÑÐ¸ Â«ÑÐ°ÐºÂ» Ð°Ð±Ð¾ Ð¿ÑÐ¾ÑÑÐ¾ Ð½Ð°Ð´ÑÑÐ»Ð¸ ÑÐ¸ÑÑÐ¸ Ð¾Ð´Ð¾Ð¼ÐµÑÑÐ°.")
                return

            await update.message.reply_text(f"â ÐÐµ Ð²Ð¸ÑÑÐ°ÑÐ°Ñ Ð´Ð°Ð½Ð¸Ñ.\n{ask_for_next_missing_field(parsed['missing_fields'])}")
            return

        spreadsheet = get_sheet()
        ws = get_matching_worksheet(spreadsheet, parsed["car_id"])
        if ws and parsed.get("type") in ["expense", "income"] and parsed.get("odometer") not in (None, ""):
            if odometer_is_anomalous(ws, int(parsed["odometer"]), parsed.get("date")):
                context.user_data["waiting_odometer_anomaly_confirm"] = True
                context.user_data["pending_actions_after_anomaly"] = [parsed]
                await update.message.reply_text("â ï¸ ÐÑÐ¾Ð±ÑÐ³ Ð²Ð¸Ð³Ð»ÑÐ´Ð°Ñ Ð½ÐµÑÐ¸Ð¿Ð¾Ð²Ð¾ Ð²ÐµÐ»Ð¸ÐºÐ¸Ð¼. ÐÑÐ´ÑÐ²ÐµÑÐ´Ð¸ÑÐ¸?")
                return

        if ws and detect_duplicate(ws, parsed, raw_text=text):
            context.user_data["waiting_duplicate_confirm"] = True
            context.user_data["pending_actions_after_duplicate"] = [parsed]
            await update.message.reply_text("â Ð¦Ðµ Ð½Ð¾Ð²Ð¸Ð¹ Ð·Ð°Ð¿Ð¸Ñ ÑÐ¸ Ð´ÑÐ±Ð»Ñ Ð¿Ð¾Ð¿ÐµÑÐµÐ´Ð½ÑÐ¾Ð³Ð¾?")
            return

        result = write_single_action_to_sheet(parsed, raw_text=text)
        context.user_data.pop("pending_data", None)
        await update.message.reply_text(result)

    except Exception as e:
        logger.exception("Error")
        await update.message.reply_text(f"â ÐÐ¾Ð¼Ð¸Ð»ÐºÐ°: {str(e)}")


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        f"ð ÐÑÐ¸Ð²ÑÑ! Ð¯ Ð±Ð¾Ñ Ð°Ð²ÑÐ¾Ð¿Ð°ÑÐºÑ.\n\n"
        f"Ð¢Ð²ÑÐ¹ Telegram ID: `{user_id}`\n\n"
        f"Ð¯ Ð·Ð½Ð°Ñ ÑÐ°ÐºÑ Ð¼Ð°ÑÐ¸Ð½Ð¸:\n{', '.join(KNOWN_CAR_IDS)}\n\n"
        f"ÐÐ¾Ð¼Ð°Ð½Ð´Ð¸:\n"
        f"â¢ Ð¼Ð°ÑÐ»Ð¾\n"
        f"â¢ Ð³ÑÐ¼\n"
        f"â¢ ÑÑÑÐ°ÑÐ¾Ð²ÐºÐ°\n"
        f"â¢ 8730 Ð¼ÑÑÑÑÑ\n"
        f"â¢ Ð¢Ð 4553\n"
        f"â¢ 8730 Ð¿ÑÐ¸ÑÐ¾Ð´ 3800, Ð´Ð¾Ð»Ð³ 200 Ð·Ð° Ð´ÑÐ¿, ÑÑÑÐ°Ñ 300 Ð·Ð° Ð¿Ð°ÑÐºÐ¾Ð²ÐºÑ\n",
        parse_mode="Markdown",
    )


async def handle_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for key in [
        "pending_data",
        "pending_actions",
        "waiting_odometer_choice",
        "waiting_odometer_choice_actions",
        "waiting_duplicate_confirm",
        "pending_actions_after_duplicate",
        "waiting_odometer_anomaly_confirm",
        "pending_actions_after_anomaly",
    ]:
        context.user_data.pop(key, None)
    await update.message.reply_text("â ÐÐ¾ÑÐ¾ÑÐ½Ðµ Ð²Ð²ÐµÐ´ÐµÐ½Ð½Ñ ÑÐºÐ°ÑÐ¾Ð²Ð°Ð½Ð¾.")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("cancel", handle_cancel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.job_queue.run_daily(check_service_and_insurance_notifications, time=time(9, 15, tzinfo=KYIV_TZ))
    app.job_queue.run_daily(check_service_and_insurance_notifications, time=time(16, 0, tzinfo=KYIV_TZ))
    logger.info("Bot started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
