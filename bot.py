
import os
import asyncio
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
MINFIN_ARCHIVE_URL = "https://minfin.com.ua/ua/currency/auction/archive/usd/dnepropetrovsk/"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
GOOGLE_CREDS = os.environ.get("GOOGLE_CREDS", "")

ALLOWED_USERS_STR = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in ALLOWED_USERS_STR.split(",") if x.strip()]

FULL_PLATES = [
    "AI1457MM", "АЕ0418ОР", "АЕ2993РI", "AE7935PI", "КА3021ЕО", "КА9489ЕР",
    "АЕ7121ТА", "АЕ8204ТВ", "AE2548TB", "АЕ9245ТО", "AE0736PK", "AE4715TH",
    "АЕ6514ТС", "KA4895HE", "KA6843HB", "АЕ5308ТЕ", "BI1875HO", "KA0665IH",
    "KA0349HO", "BC9854PM", "АЕ8391ТМ", "AE4553XB", "KA8730IX", "AE5725OO",
    "СА6584КА", "AI3531PH",
]

TO_BUNDLE = [
    {"description": "Масло в двигатель", "amount": 780},
    {"description": "Воздушный фильтр WX WA9545", "amount": 270},
    {"description": "Газовые фильтра", "amount": 100},
    {"description": "Масляный фильтр BO 0451103318", "amount": 160},
    {"description": "Работы за ТО", "amount": 300},
]

SKIP_GRM = {"9245", "5308", "4715", "8204", "0736"}

INSURANCE_DATE_COL = 18  # R (1-based)
INSURANCE_COMPANY_COL = 19  # S (1-based)

REPORT_CACHE: Dict[str, Any] = {"snapshot": None, "time": None}
REPORT_CACHE_TTL = 600



DRIVERS_SPREADSHEET_ID = os.environ.get("DRIVERS_SPREADSHEET_ID", "1WzJyXkrI6kUwg7vIRbssNwP5LM9-1-jK3b4SWSOHUYU")
DRIVERS_SHEET_NAME = "ТО і ГРМ"

_gspread_client = None
_gspread_client_ts = None

_DRIVERS_CACHE: Dict[str, Dict] = {}
_DRIVERS_CACHE_TS: Optional[datetime] = None


def parse_insurance_a4(text) -> tuple:
    if not text:
        return None, None
    s = str(text).strip()
    m = re.search(r"(\d{2}\.\d{2}\.\d{2,4})", s)
    if not m:
        return None, None
    date_str = m.group(1)
    try:
        fmt = "%d.%m.%y" if len(date_str) == 8 else "%d.%m.%Y"
        d = datetime.strptime(date_str, fmt).date()
    except Exception:
        return None, None
    company = s[m.end():].strip() or "Страховка"
    return d, company


def _load_drivers_cache() -> None:
    global _DRIVERS_CACHE, _DRIVERS_CACHE_TS
    now = datetime.now(KYIV_TZ)
    if _DRIVERS_CACHE_TS and (now - _DRIVERS_CACHE_TS).total_seconds() < 300:
        return
    try:
        import time as _time
        creds_dict = json.loads(GOOGLE_CREDS)
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        for _attempt in range(3):
            try:
                sp = client.open_by_key(DRIVERS_SPREADSHEET_ID)
                break
            except gspread.exceptions.APIError as _e:
                if "429" in str(_e) and _attempt < 2:
                    _time.sleep(10 * (_attempt + 1))
                else:
                    raise
        ws = None
        for sheet in sp.worksheets():
            if "ТО" in sheet.title or "грм" in sheet.title.lower():
                ws = sheet
                break
        if not ws:
            ws = sp.sheet1
        cache = {}
        all_rows = ws.get_all_values()
        logger.info("Drivers sheet: %d rows", len(all_rows))
        for row in all_rows[2:]:
            if not row or not str(row[0]).strip():
                continue
            key = re.sub(r"[^0-9]", "", str(row[0]).strip())
            name = str(row[11]).strip() if len(row) > 11 else ""
            phone1 = str(row[12]).strip() if len(row) > 12 else ""
            phone2 = str(row[13]).strip() if len(row) > 13 else ""
            if key:
                cache[key] = {"name": name, "phone1": phone1, "phone2": phone2}
        _DRIVERS_CACHE = cache
        _DRIVERS_CACHE_TS = now
        logger.info("Drivers cache: %d entries", len(cache))
    except Exception as e:
        logger.error("_load_drivers_cache: %s", e)


def fmt_driver(car_id: str) -> str:
    _load_drivers_cache()
    info = _DRIVERS_CACHE.get(car_id, {})
    name = info.get("name", "").strip()
    phone1 = info.get("phone1", "").strip()
    phone2 = info.get("phone2", "").strip()
    if not name and not phone1:
        return "Немає водiя"
    phones = " / ".join(p for p in [phone1, phone2] if p)
    parts = []
    if name: parts.append(name)
    if phones: parts.append(phones)
    return " | ".join(parts)


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
    global _gspread_client, _gspread_client_ts
    import time as _time
    now = datetime.now(KYIV_TZ)
    if _gspread_client is None or _gspread_client_ts is None or             (now - _gspread_client_ts).total_seconds() > 1800:
        creds_dict = json.loads(GOOGLE_CREDS)
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        _gspread_client = gspread.authorize(creds)
        _gspread_client_ts = now
    for attempt in range(3):
        try:
            return _gspread_client.open_by_key(SPREADSHEET_ID)
        except gspread.exceptions.APIError as e:
            if "429" in str(e) and attempt < 2:
                _time.sleep(10 * (attempt + 1))
            else:
                raise


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


def parse_short_date(date_str) -> Optional[date]:
    if not date_str:
        return None
    if hasattr(date_str, 'date'):
        return date_str.date()
    if isinstance(date_str, date):
        return date_str
    s = str(date_str).strip()
    if not s or s in ('None', ''):
        return None
    if 'T' in s or (len(s) > 8 and '-' in s[:8]):
        try:
            return datetime.fromisoformat(s.split(' ')[0].split('T')[0]).date()
        except Exception:
            pass
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%Y.%m.%d"):
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
        return "Невідомо"
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
    return f"""Ты помощник для учета автопарка. Сегодня {today}.

Твоя задача: разобрать сообщение пользователя в СТРОГИЙ JSON для записи в Google Sheets.
{existing_block}
Известные машины автопарка:
{cars_block}

Правила:
1. Пользователь может писать данные в любом порядке.
2. Пользователь часто пишет только цифры машины, например 4553 или 8730.
3. car_id в JSON должен быть только из списка известных машин.
4. Если дата не указана - используй сегодняшнюю дату в формате DD.MM.YY.
5. ДАННЫЕ ДЛЯ ТАБЛИЦЫ ПИШИ НА РУССКОМ ЯЗЫКЕ.
6. Ответ должен быть ТОЛЬКО JSON.
7. Если не хватает важных данных - верни missing_fields.
8. Если пользователь пишет "ТО" или "плановое ТО", description верни как "ТО".
9. Если пользователь пишет про штраф, долг, должен, дожен - type верни как "liability_minus".
10. Если пользователь пишет "взял", "принял", "погасил", "дал" в контексте долга - type верни как "liability_plus".
11. Для liability_minus и liability_plus odometer не нужен.
12. Для liability_minus и liability_plus description должна быть только текстом причины БЕЗ номера машины и БЕЗ суммы.
13. Для income description может быть пустым.

Сообщение пользователя:
"{message}"

Верни JSON строго такого вида:
{{
  "type": "expense" или "income" или "liability_minus" или "liability_plus" или null,
  "car_id": "8730" или null,
  "date": "DD.MM.YY",
  "amount": 370,
  "description": "Колодки Бош",
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
            {"role": "system", "content": "Возвращай только валидный JSON. Без пояснений. Без markdown."},
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
            return {"error": f"AI недоступний: {e}"}
    return {"error": "Не задані CLAUDE_API_KEY і OPENAI_API_KEY"}


# ===== Special parsing =====

def is_to_phrase(text: str) -> bool:
    t = str(text or "").lower().strip()
    return t == "то" or " то " in f" {t} " or "плановое то" in t or "планове то" in t


def detect_liability_type(text: str) -> Optional[str]:
    t = str(text or "").lower().strip()
    plus_markers = ["взял", "принял", "погасил", "дал "]
    minus_markers = ["штраф", "долг", "должен", "должна", "дожен"]
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
        data.setdefault("description", "ТО")
        if data.get("amount") in ("", None):
            data["amount"] = 0
    return data


def compute_missing_fields(data: dict, raw_text: str = "") -> list[str]:
    missing = []
    op_type = data.get("type")
    to_case = is_to_phrase(raw_text) or str(data.get("description", "")).lower().strip() in {"то", "плановое то", "планове то"}

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
        return "Уточни, будь ласка, відсутні дані."
    field = missing_fields[0]
    mapping = {
        "type": "Вкажи, будь ласка, це прихід, витрата, штраф чи борг.",
        "car_id": f"Вкажи номер машини. Доступні: {', '.join(KNOWN_CAR_IDS)}",
        "amount": "Вкажи суму в гривнях.",
        "description": "Вкажи опис або причину.",
        "odometer": "Мені додати середньостатистичний пробіг? Напиши «так» або просто надішли цифри одометра.",
    }
    return mapping.get(field, "Уточни, будь ласка, відсутні дані.")


def build_liability_description(op_type: str, raw_text: str, ai_description: Optional[str]) -> str:
    t = str(raw_text or "").lower()
    desc = str(ai_description or "").strip()
    if "дтп" in t:
        base = "за ДТП"
    elif "телевиз" in t:
        base = "за телевизор"
    elif "парков" in t:
        base = "за парковку"
    elif "превыш" in t:
        base = "за превышение"
    elif "штраф" in t and op_type == "liability_plus":
        base = "за штраф"
    elif desc:
        base = desc if desc.lower().startswith("за ") else f"за {desc}"
    else:
        base = ""
    if op_type == "liability_minus":
        return f"{'штраф' if 'штраф' in t else 'долг'} {base}".strip()
    return f"погашение долга {base}".strip()


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
            if "приход" in low and amounts:
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
            "amount": 0, "description": "ТО", "odometer": None, "notes": None, "missing_fields": []
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
    latest_f: Optional[Tuple[date, int]] = None
    latest_l: Optional[Tuple[date, int]] = None

    for r in rows[7:]:
        if len(r) > 5:
            d = parse_short_date(r[4] if len(r) > 4 else None)
            odo = parse_num(r[5] if len(r) > 5 else None)
            if d and odo is not None and odo > 1000:
                if latest_f is None or d > latest_f[0] or (d == latest_f[0] and odo > latest_f[1]):
                    latest_f = (d, odo)
        if len(r) > 11:
            d = parse_short_date(r[10] if len(r) > 10 else None)
            odo = parse_num(r[11] if len(r) > 11 else None)
            if d and odo is not None and odo > 1000:
                if latest_l is None or d > latest_l[0] or (d == latest_l[0] and odo > latest_l[1]):
                    latest_l = (d, odo)

    if latest_f and latest_l:
        if latest_f[0] > latest_l[0]: return latest_f[1]
        if latest_l[0] > latest_f[0]: return latest_l[1]
        return max(latest_f[1], latest_l[1])
    if latest_f: return latest_f[1]
    if latest_l: return latest_l[1]
    return None


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
    if "масло в двигатель" in text: score += 10
    if "моторное масло" in text:    score += 8
    if "замена масла" in text:      score += 8
    if "замiна масла" in text:      score += 8
    if "масло в двигун" in text:    score += 8
    if "моторне масло" in text:     score += 8
    if "масляный фильтр" in text:   score += 4
    if "масляний фiльтр" in text:   score += 4
    if "масло" in text:             score += 2
    return score


def score_grm_block(block: List[Dict[str, Any]]) -> int:
    text = " | ".join(x["desc"] for x in block)
    score = 0
    if "комплект грм" in text:
        score += 10
    if "замена грм" in text or "замана грм" in text:
        score += 10
    if "ремень грм" in text:
        score += 7
    if "ролик грм" in text:
        score += 6
    if "грм" in text:
        score += 4
    if "помпа" in text:
        score += 2
    return score


def find_last_service(rows: List[List[str]], mode: str) -> Tuple[Optional[str], Optional[int]]:
    blocks = split_expense_blocks(rows)
    if not blocks:
        return None, None

    scorer = score_oil_block if mode == "oil" else score_grm_block
    for block in reversed(blocks):
        if scorer(block) >= 8:
            return block[0]["date"], block[0]["odo"]
    return None, None


def get_color_icon(remaining: Optional[int], total: int) -> str:
    if remaining is None:
        return "⚪"
    if remaining <= 1000:
        return "🔴"
    ratio = remaining / total
    if ratio > 0.66:
        return "🟢"
    if ratio > 0.33:
        return "🟡"
    return "🟠"


def build_oil_report() -> str:
    snapshot = get_data_snapshot()
    items = []
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
        line = f"{icon} {car_id} | {last_date} | {last_odo} | {format_km(remaining)} км"
        if remaining <= 1000:
            drv = fmt_driver(car_id)
            line += f"\n   👤 {drv}"
        items.append((remaining, line))
    items.sort(key=lambda x: x[0])
    return "\n".join(line for _, line in items)


def build_grm_report() -> str:
    snapshot = get_data_snapshot()
    items = []
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
        line = f"{icon} {car_id} | {last_date} | {last_odo} | {format_km(remaining)} км"
        if remaining <= 1000:
            drv = fmt_driver(car_id)
            line += f"\n   👤 {drv}"
        items.append((remaining, line))
    items.sort(key=lambda x: x[0])
    return "\n".join(line for _, line in items)


# ===== Insurance =====

def insurance_days_icon(days_left: int) -> str:
    if days_left <= 14:
        return "🔴"
    if days_left <= 30:
        return "🟠"
    if days_left <= 90:
        return "🟡"
    return "🟢"


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
        if len(rows) > 3 and rows[3] and rows[3][0]:
            d, company = parse_insurance_a4(rows[3][0])
            if d:
                best = (d, company)
        if not best:
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
        lines.append((days_left, f"{icon} {car_id} | {end_date.strftime('%d.%m.%y')} | {company}"))
    lines.sort(key=lambda x: x[0])
    return "\n".join(line for _, line in lines)


async def check_service_and_insurance_notifications(context: ContextTypes.DEFAULT_TYPE):
    now_kyiv = datetime.now(KYIV_TZ)
    if now_kyiv.weekday() >= 5:
        logger.info("Notify skipped: weekend")
        return
    logger.info("Running daily notification check...")
    try:
        await _run_notify(context)
    except Exception as e:
        logger.error("Notify error: %s", e, exc_info=True)
        for uid in ALLOWED_USERS:
            try:
                await context.bot.send_message(chat_id=uid, text=f"⚠️ Помилка регламентiв: {e}")
            except Exception:
                pass


async def _run_notify(context: ContextTypes.DEFAULT_TYPE):
    now_kyiv = datetime.now(KYIV_TZ)

    snapshot = get_data_snapshot(force_refresh=True)
    today = now_kyiv.date()
    alert_items: List[Tuple[int, str]] = []

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
                if remaining < 0:
                    alert_items.append((remaining, f"🚗 {car_id} — масло прострочено на {format_km(abs(remaining))} км"))
                else:
                    alert_items.append((remaining, f"🚗 {car_id} — масло через {format_km(remaining)} км"))

        if car_id not in SKIP_GRM:
            grm_date, grm_odo = find_last_service(rows, "grm")
            if grm_odo is not None and current_odo is not None:
                remaining = 50000 - (max(current_odo, grm_odo) - grm_odo)
                if remaining <= 1000:
                    icon = "🔴" if remaining <= 0 else "🟠"
                    drv = fmt_driver(car_id)
                    drv_line = f"\n    👤 {drv}"
                    if remaining < 0:
                        alert_items.append((remaining, f"{icon} {car_id} — ГРМ прострочено на {format_km(abs(remaining))} км{drv_line}"))
                    else:
                        alert_items.append((remaining, f"{icon} {car_id} — ГРМ через {format_km(remaining)} км{drv_line}"))

        best: Optional[Tuple[date, str]] = None
        if len(rows) > 3 and rows[3] and rows[3][0]:
            d, company = parse_insurance_a4(rows[3][0])
            if d:
                best = (d, company)
        if not best:
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
                ins_icon = "🔴" if days_left < 0 else "🟠"
                if days_left < 0:
                    alert_items.append((days_left, f"{ins_icon} {car_id} — страховка прострочена на {abs(days_left)} дн. ({company})"))
                else:
                    alert_items.append((days_left, f"{ins_icon} {car_id} — страховка через {days_left} дн. ({company})"))

    logger.info("Notify: %d alerts", len(alert_items))
    if alert_items:
        alert_items.sort(key=lambda x: x[0])
        text = "⚠️ Стан регламентiв на сьогоднi:\n\n" + "\n".join(msg for _, msg in alert_items)
        for user_id in ALLOWED_USERS:
            try:
                await context.bot.send_message(chat_id=user_id, text=text)
                logger.info("Notification sent to %s", user_id)
            except Exception as e:
                logger.error("Notification send error: %s", e)
    else:
        logger.info("Notify: no alerts today")


# ===== USD rate =====

def get_usd_black_rate_dnipro() -> Optional[float]:
    """Return Minfin Dnipro USD cash SELL rate (Наличный курс -> USD -> Продажа)."""
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "ru-RU,ru;q=0.9,uk;q=0.8,en;q=0.7",
    }
    resp = requests.get(MINFIN_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Берем только USD из секции "Наличный курс" и именно колонку "Продажа".
    # Банковский средний курс выше на странице сознательно игнорируем.
    cash_heading = soup.find(
        lambda tag: tag.name in {"h2", "h3", "div", "span", "p"}
        and tag.get_text(" ", strip=True).lower() in {"наличный курс", "готівковий курс"}
    )
    if cash_heading:
        table = cash_heading.find_next("table")
        if table:
            for tr in table.find_all("tr"):
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                if cells and cells[0].strip().upper() == "USD" and len(cells) >= 3:
                    m = re.search(r"([0-9]{2}[.,][0-9]{1,4})", cells[2])
                    if m:
                        rate = float(m.group(1).replace(",", "."))
                        if 20 <= rate <= 100:
                            return rate

    # Fallback для плоской верстки: ищем две цифры в строке USD
    # после секции "Наличный курс": первая = покупка, вторая = продажа.
    flat = soup.get_text(" ", strip=True)
    m = re.search(
        r"(?:Наличный курс|Готівковий курс).*?USD\s+"
        r"([0-9]{2}[.,][0-9]{1,4}).*?"
        r"([0-9]{2}[.,][0-9]{1,4})",
        flat,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        rate = float(m.group(2).replace(",", "."))
        if 20 <= rate <= 100:
            return rate
    return None

def _parse_minfin_archive_sell_rates(html: str) -> Dict[date, float]:
    """Parse Minfin Dnipro archive table: Date | Spread | Buy | Sell | NBU; return SELL."""
    rates: Dict[date, float] = {}
    soup = BeautifulSoup(html, "html.parser")

    # Primary parser: HTML table rows.
    for tr in soup.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) < 4:
            continue
        d = parse_short_date(cells[0])
        if not d:
            continue
        sell_raw = cells[3].replace(",", ".").strip()
        m = re.search(r"(\d{2}(?:\.\d{1,4})?)", sell_raw)
        if not m:
            continue
        try:
            rate = float(m.group(1))
        except ValueError:
            continue
        if 20 <= rate <= 100:
            rates[d] = rate

    # Fallback parser for pages where table markup is flattened.
    if not rates:
        flat = soup.get_text(" ", strip=True)
        pattern = re.compile(
            r"(\d{2}\.\d{2}\.\d{4})\s+"
            r"(?:\d+(?:[.,]\d+)?)\s+"
            r"(\d{2}(?:[.,]\d{1,4})?|-\.\--)",
            re.IGNORECASE,
        )
        for ds, sell_raw in pattern.findall(flat):
            d = parse_short_date(ds)
            if not d or sell_raw == "-.--":
                continue
            try:
                rate = float(sell_raw.replace(",", "."))
            except ValueError:
                continue
            if 20 <= rate <= 100:
                rates[d] = rate
    return rates


def load_minfin_dnipro_sell_archive() -> Dict[date, float]:
    """
    Load Minfin USD SELL archive for Dnipro.
    Several URL variants are tried because Minfin can change the period selector.
    No table cells are modified here.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; FarroAutoparkBot/1.0)",
        "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
    }
    urls = [
        MINFIN_ARCHIVE_URL + "?period=max",
        MINFIN_ARCHIVE_URL + "?period=all",
        MINFIN_ARCHIVE_URL,
    ]

    merged: Dict[date, float] = {}
    errors = []
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            parsed = _parse_minfin_archive_sell_rates(resp.text)
            if parsed:
                merged.update(parsed)
        except Exception as e:
            errors.append(f"{url}: {e}")

    if not merged:
        raise RuntimeError(
            "Не вдалося отримати архів курсу продажу USD Minfin для Дніпра. "
            + ("; ".join(errors[:2]) if errors else "")
        )
    return merged


def historical_sell_rate_for_date(target: date, rates: Dict[date, float]) -> Optional[float]:
    """
    Exact date first. If Minfin has no SELL quote that day (weekend/empty row),
    use the nearest earlier available quote, but no more than 7 days back.
    """
    if target in rates:
        return rates[target]
    for back in range(1, 8):
        d = target - timedelta(days=back)
        if d in rates:
            return rates[d]
    return None


def backfill_historical_usd_buy_rates() -> str:
    """
    One-time mass update of historical USD amounts:
      E date + H UAH amount -> I USD amount
      K date + M UAH amount -> N USD amount
    Only I and N are changed.
    """
    rates = load_minfin_dnipro_sell_archive()
    spreadsheet = get_sheet()

    total_i = 0
    total_o = 0
    missing_dates = set()
    changed_sheets = 0

    for car_id in KNOWN_CAR_IDS:
        ws = get_matching_worksheet(spreadsheet, car_id)
        if not ws:
            continue

        rows = ws.get_all_values()
        updates = []
        blue_i_rows = []
        blue_n_rows = []

        for row_idx, row in enumerate(rows[7:], start=8):
            # Expense block: E date + H UAH amount -> I USD amount.
            expense_date = parse_short_date(row[4] if len(row) > 4 else None)
            expense_amount = parse_num(row[7] if len(row) > 7 else None)
            if expense_date and expense_amount is not None:
                rate = historical_sell_rate_for_date(expense_date, rates)
                if rate is not None and rate > 0:
                    usd_amount = round(expense_amount / rate, 2)
                    updates.append({
                        "range": f"I{row_idx}",
                        "values": [[usd_amount]],
                    })
                    blue_i_rows.append(row_idx)
                    total_i += 1
                else:
                    missing_dates.add(expense_date)

            # Income block: K date + M UAH amount -> N USD amount.
            income_date = parse_short_date(row[10] if len(row) > 10 else None)
            income_amount = parse_num(row[12] if len(row) > 12 else None)
            if income_date and income_amount is not None:
                rate = historical_sell_rate_for_date(income_date, rates)
                if rate is not None and rate > 0:
                    usd_amount = round(income_amount / rate, 2)
                    updates.append({
                        "range": f"N{row_idx}",
                        "values": [[usd_amount]],
                    })
                    blue_n_rows.append(row_idx)
                    total_o += 1
                else:
                    missing_dates.add(income_date)

        if updates:
            # One batch request per vehicle sheet, so we do not hammer Sheets API.
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            _apply_blue_to_written_cells(
                ws,
                {"I": blue_i_rows, "N": blue_n_rows},
            )
            changed_sheets += 1

    # Invalidate report snapshot only; no other state is touched.
    global REPORT_CACHE
    REPORT_CACHE = {"snapshot": None, "time": None}

    result = (
        "✅ Історичні суми USD перераховано за курсом продажу.\n\n"
        f"🚗 Листів змінено: {changed_sheets}\n"
        f"💸 Колонка I (витрати): {total_i} значень\n"
        f"💰 Колонка N (приходи): {total_o} значень"
    )
    if missing_dates:
        preview = ", ".join(sorted(d.strftime("%d.%m.%Y") for d in missing_dates)[:15])
        result += (
            f"\n\n⚠️ Не знайдено курсу для {len(missing_dates)} дат. "
            f"Їх не змінено: {preview}"
        )
    return result


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
        usd_value = round(amount / usd_rate, 2) if usd_rate and usd_rate > 0 else ""
        rows.append([date_value, odometer, item["description"], amount, usd_value])
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
            usd_note = f"\n💱 Готівковий курс продажу USD: {usd_rate}; USD-сумма записана в I/N"
    except Exception as e:
        logger.error(f"USD rate error: {e}")
        usd_note = "\n⚠️ Курс USD не вдалося отримати"

    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return f"❌ Машину {full_plate} не знайдено в таблиці"
    sheet_name = ws.title

    if op_type == "expense":
        desc_lower = str(description).lower().strip()
        if desc_lower in {"то", "плановое то", "планове то"} or is_to_phrase(description):
            start_row, end_row, total_amount = write_expense_rows(ws, date_value, odometer, TO_BUNDLE, usd_rate, odometer_estimated)
            return (
                f"✅ ТО внесено!\n🚘 Машина: {full_plate}\n🧾 Додано 5 рядків\n"
                f"💸 Загальна сума: {total_amount} грн\n📅 {date_value}\n"
                f"📍 Внесено: лист '{sheet_name}', рядки {start_row}-{end_row}, стовпці E:I{usd_note}"
            )

        next_row = get_next_expense_row(ws)
        usd_value = round(amount / usd_rate, 2) if usd_rate and usd_rate > 0 else ""
        rng = f"E{next_row}:I{next_row}"
        ws.update(rng, [[date_value, odometer, description, amount, usd_value]])
        apply_blue_text(ws, rng)
        if odometer_estimated:
            mark_cell_yellow(ws, f"F{next_row}")
        return (
            f"✅ Витрата внесена!\n🚘 Машина: {full_plate}\n📋 {description}\n💸 {amount} грн\n"
            f"📅 {date_value}\n📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці E:I{usd_note}"
        )

    if op_type == "income":
        next_row = get_next_right_block_row(ws)
        usd_value = round(amount / usd_rate, 2) if usd_rate and usd_rate > 0 else ""
        prev_odo = get_previous_income_odometer(ws)
        mileage_delta = ""
        if prev_odo is not None and odometer not in ("", None):
            try:
                mileage_delta = int(odometer) - int(prev_odo)
            except Exception:
                mileage_delta = ""
        rng = f"K{next_row}:O{next_row}"
        ws.update(rng, [[date_value, odometer, amount, usd_value, mileage_delta]])
        apply_blue_text(ws, rng)
        if odometer_estimated:
            mark_cell_yellow(ws, f"L{next_row}")
        delta_text = f"\n📈 Різниця пробігу: {mileage_delta}" if mileage_delta != "" else ""
        return (
            f"✅ Дохід внесено!\n🚘 Машина: {full_plate}\n💰 {amount} грн\n📅 {date_value}\n📍 Одометр: {odometer}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці K:O{delta_text}{usd_note}"
        )

    if op_type in ["liability_minus", "liability_plus"]:
        next_row = get_next_right_block_row(ws)
        sign_amount = -abs(amount) if op_type == "liability_minus" else abs(amount)
        liability_desc = build_liability_description(op_type, raw_text, description)
        rng = f"K{next_row}:Q{next_row}"
        ws.update(rng, [[date_value, "", "", "", "", sign_amount, liability_desc]])
        apply_blue_text(ws, rng)
        label = "Штраф/борг" if op_type == "liability_minus" else "Погашення/надходження"
        return (
            f"✅ {label} внесено!\n🚘 Машина: {full_plate}\n💵 {sign_amount} грн\n📝 {liability_desc}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці P:Q"
        )

    return "❌ Невідомий тип операції"


def write_actions_to_sheet(actions: List[dict], raw_text: str = "") -> str:
    return "\n\n".join(write_single_action_to_sheet(action, raw_text=raw_text) for action in actions)


def is_yes_statistical(text: str) -> bool:
    return str(text).lower().strip() in {"так", "да", "yes", "ок", "окей", "ага"}


def is_yes_confirm(text: str) -> bool:
    return str(text).lower().strip() in {"так", "да", "yes", "новий", "новая", "новое"}


def is_no_confirm(text: str) -> bool:
    return str(text).lower().strip() in {"ні", "нет", "дубль", "скасувати", "отмена", "cancel"}


def actions_need_odometer(actions: List[dict]) -> bool:
    return any(action.get("type") in ["expense", "income"] and action.get("odometer") in (None, "") for action in actions)


def fill_odometer_for_actions(actions: List[dict], odometer_value: int, estimated: bool) -> None:
    for action in actions:
        if action.get("type") in ["expense", "income"] and action.get("odometer") in (None, ""):
            action["odometer"] = odometer_value
            action["odometer_estimated"] = estimated


def detect_month_summary_request(text: str) -> Optional[str]:
    t = str(text or "").lower()
    if any(x in t for x in ["місяць", "месяц", "поточний місяць", "текущий месяц"]):
        for car_id in KNOWN_CAR_IDS:
            if re.search(rf"(?<!\d){re.escape(car_id)}(?!\d)", text):
                return car_id
    return None


def monthly_summary(car_id: str) -> str:
    spreadsheet = get_sheet()
    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return f"❌ Машину {car_id} не знайдено в таблиці"

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
        f"📊 За поточний місяць по {car_id}:\n"
        f"💰 Дохід: {fmt(income_sum)} грн\n"
        f"💸 Витрати: {fmt(expense_sum)} грн\n"
        f"📌 Залишок боргу: {fmt(liability_sum)} грн"
    )


# ===== Command detectors =====

def is_oil_report_request(text: str) -> bool:
    t = str(text or "").lower().strip()
    if t in {"масло", "замена масла", "то", "плановое то", "планове то"}:
        return True
    parts = t.split()
    if parts and parts[0] in {"масло", "то"}:
        return True
    return False


def is_grm_report_request(text: str) -> bool:
    t = str(text or "").lower().strip()
    if t in {"грм", "замена грм", "комплект грм"}:
        return True
    parts = t.split()
    if parts and parts[0] in {"грм"}:
        return True
    return False


def is_insurance_report_request(text: str) -> bool:
    t = str(text or "").lower().strip()
    # Точна команда
    if t in {"страховка", "страхування", "страховка?"}:
        return True
    # "страховка 6514", "страховка по 7121" тощо
    parts = t.split()
    if parts and parts[0] in {"страховка", "страхування"}:
        return True
    return False





def _compact_cell_ranges(column: str, row_numbers: List[int]) -> List[str]:
    """Compact row numbers into A1 ranges to reduce formatting API calls."""
    rows = sorted(set(int(r) for r in row_numbers if r))
    if not rows:
        return []
    ranges = []
    start = prev = rows[0]
    for r in rows[1:]:
        if r == prev + 1:
            prev = r
            continue
        ranges.append(
            f"{column}{start}:{column}{prev}" if start != prev
            else f"{column}{start}"
        )
        start = prev = r
    ranges.append(
        f"{column}{start}:{column}{prev}" if start != prev
        else f"{column}{start}"
    )
    return ranges


def _apply_blue_to_written_cells(ws, cells_by_column: Dict[str, List[int]]) -> None:
    """Blue-format only cells written/calculated by the bot."""
    for column, rows in cells_by_column.items():
        for cell_range in _compact_cell_ranges(column, rows):
            apply_blue_text(ws, cell_range)


def _income_mileage_delta_updates(
    rows: List[List[str]],
) -> Tuple[List[Dict[str, Any]], List[int]]:
    """
    Column O = mileage difference between the current driver's PAYMENT
    and the previous driver's PAYMENT.

    Payment means M > 0.
    K = payment date, L = odometer, M = UAH payment, N = USD payment, O = delta km.
    Non-payment rows get blank O.
    """
    payment_rows = []

    for row_idx, row in enumerate(rows[7:], start=8):
        d = parse_short_date(row[10] if len(row) > 10 else None)
        odo = parse_num(row[11] if len(row) > 11 else None)
        amount = parse_money_float(row[12] if len(row) > 12 else None)
        current_o_raw = str(row[14]).strip() if len(row) > 14 else ""

        if d and amount is not None and amount > 0:
            payment_rows.append((d, row_idx, odo, current_o_raw))
        elif current_o_raw:
            # O must not contain exchange rates or other values on non-payment rows.
            payment_rows.append((d or date.min, row_idx, None, current_o_raw, "clear"))

    # Calculate actual payment deltas chronologically.
    real_payments = [
        item for item in payment_rows
        if len(item) == 4
    ]
    real_payments.sort(key=lambda x: (x[0], x[1]))

    expected_by_row: Dict[int, Optional[int]] = {}
    previous_payment_odo: Optional[int] = None

    for _d, row_idx, odo, _current_o_raw in real_payments:
        if odo is None:
            expected_by_row[row_idx] = None
            previous_payment_odo = None
            continue

        if previous_payment_odo is None:
            expected_by_row[row_idx] = 0
        else:
            delta = odo - previous_payment_odo
            # Negative mileage means source odometer data needs manual review.
            expected_by_row[row_idx] = delta if delta >= 0 else None

        previous_payment_odo = odo

    updates: List[Dict[str, Any]] = []
    blue_rows: List[int] = []

    # Clear O on non-payment rows if something is there.
    for item in payment_rows:
        if len(item) == 5 and item[4] == "clear":
            row_idx = item[1]
            updates.append({
                "range": f"O{row_idx}",
                "values": [[""]],
            })

    # Write only when O is missing/wrong.
    for _d, row_idx, _odo, current_o_raw in real_payments:
        expected = expected_by_row.get(row_idx)
        if expected is None:
            continue

        current_o = parse_num(current_o_raw) if current_o_raw else None
        if current_o != expected:
            updates.append({
                "range": f"O{row_idx}",
                "values": [[expected]],
            })
            blue_rows.append(row_idx)

    return updates, blue_rows



def _is_google_sheets_quota_error(exc: Exception) -> bool:
    s = str(exc).lower()
    return (
        "429" in s or
        "quota exceeded" in s or
        "write requests per minute" in s or
        "rate limit" in s
    )


def _run_with_sheets_backoff(func, *, operation_name: str = "Google Sheets"):
    """
    Retry only quota/rate-limit errors.
    Other errors are raised immediately so real problems are not hidden.
    """
    import time as _time

    waits = (30, 60, 90)
    for attempt in range(len(waits) + 1):
        try:
            return func()
        except Exception as e:
            if not _is_google_sheets_quota_error(e) or attempt >= len(waits):
                raise
            wait_seconds = waits[attempt]
            logger.warning(
                "%s quota limit. Retry in %s sec: %s",
                operation_name,
                wait_seconds,
                e,
            )
            _time.sleep(wait_seconds)


def _repair_one_vehicle_history(
    spreadsheet,
    car_id: str,
    historical_rates: Dict[date, float],
) -> Dict[str, Any]:
    """
    Repair only one vehicle sheet.
    One data batch + compact blue formatting, with quota backoff.
    """
    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return {
            "car_id": car_id,
            "found": False,
            "changed": False,
            "expense_odos": 0,
            "usd_expenses": 0,
            "usd_incomes": 0,
            "mileage_deltas": 0,
        }

    rows = _run_with_sheets_backoff(
        ws.get_all_values,
        operation_name=f"{car_id}: read",
    )

    updates: List[Dict[str, Any]] = []
    blue: Dict[str, List[int]] = {
        "F": [], "I": [], "N": [], "O": []
    }

    expense_odos = 0
    usd_expenses = 0
    usd_incomes = 0

    for row_idx, row in enumerate(rows[7:], start=8):
        expense_date = parse_short_date(
            row[4] if len(row) > 4 else None
        )
        expense_odo_raw = (
            str(row[5]).strip() if len(row) > 5 else ""
        )
        expense_desc = (
            str(row[6]).strip() if len(row) > 6 else ""
        )
        expense_uah = parse_money_float(
            row[7] if len(row) > 7 else None
        )
        expense_usd_raw = (
            str(row[8]).strip() if len(row) > 8 else ""
        )

        expense_has_operation = bool(
            expense_date and (
                expense_desc or
                expense_uah is not None or
                expense_usd_raw
            )
        )

        if expense_has_operation and not expense_odo_raw:
            estimated = estimate_missing_odometer_from_rows(
                rows, expense_date
            )
            if estimated is not None:
                updates.append({
                    "range": f"F{row_idx}",
                    "values": [[estimated]],
                })
                blue["F"].append(row_idx)
                expense_odos += 1

        if (
            expense_date and
            expense_uah is not None and
            not expense_usd_raw and
            historical_rates
        ):
            rate = historical_sell_rate_for_date(
                expense_date, historical_rates
            )
            if rate is not None and rate > 0:
                updates.append({
                    "range": f"I{row_idx}",
                    "values": [[round(expense_uah / rate, 2)]],
                })
                blue["I"].append(row_idx)
                usd_expenses += 1

        income_date = parse_short_date(
            row[10] if len(row) > 10 else None
        )
        income_uah = parse_money_float(
            row[12] if len(row) > 12 else None
        )
        income_usd_raw = (
            str(row[13]).strip() if len(row) > 13 else ""
        )

        if (
            income_date and
            income_uah is not None and
            income_uah > 0 and
            not income_usd_raw and
            historical_rates
        ):
            rate = historical_sell_rate_for_date(
                income_date, historical_rates
            )
            if rate is not None and rate > 0:
                updates.append({
                    "range": f"N{row_idx}",
                    "values": [[round(income_uah / rate, 2)]],
                })
                blue["N"].append(row_idx)
                usd_incomes += 1

    o_updates, o_blue_rows = _income_mileage_delta_updates(rows)
    updates.extend(o_updates)
    blue["O"].extend(o_blue_rows)

    if updates:
        _run_with_sheets_backoff(
            lambda: ws.batch_update(
                updates,
                value_input_option="USER_ENTERED",
            ),
            operation_name=f"{car_id}: batch write",
        )
        _run_with_sheets_backoff(
            lambda: _apply_blue_to_written_cells(ws, blue),
            operation_name=f"{car_id}: blue formatting",
        )

    return {
        "car_id": car_id,
        "found": True,
        "changed": bool(updates),
        "expense_odos": expense_odos,
        "usd_expenses": usd_expenses,
        "usd_incomes": usd_incomes,
        "mileage_deltas": len(o_blue_rows),
    }


def repair_one_vehicle_history(car_id: str) -> str:
    """
    Manual one-car repair, e.g. "исправить 1457".
    Useful for checking one sheet without touching the whole fleet.
    """
    car_id = resolve_car_id(car_id) or str(car_id).strip()
    if car_id not in KNOWN_CAR_IDS:
        return (
            "❌ Машина не найдена. Доступные номера: "
            + ", ".join(KNOWN_CAR_IDS)
        )

    historical_rates: Dict[date, float] = {}
    rate_error = None
    try:
        historical_rates = load_minfin_dnipro_sell_archive()
    except Exception as e:
        rate_error = e
        logger.error("One-car historical USD archive error: %s", e)

    spreadsheet = get_sheet()
    result = _repair_one_vehicle_history(
        spreadsheet,
        car_id,
        historical_rates,
    )

    global REPORT_CACHE
    REPORT_CACHE = {"snapshot": None, "time": None}

    if not result["found"]:
        return f"❌ Лист машины {car_id} не найден."

    msg = (
        f"✅ Машина {full_plate_from_short(car_id)} обработана.\n"
        f"🔵 Одометры расходов F: {result['expense_odos']}\n"
        f"🔵 USD расходов I: {result['usd_expenses']}\n"
        f"🔵 USD приходов N: {result['usd_incomes']}\n"
        f"🔵 Пробеги O: {result['mileage_deltas']}"
    )
    if not result["changed"]:
        msg += "\nИзменений не требовалось."
    if rate_error:
        msg += "\n⚠️ Архив USD был недоступен; пустые I/N не заполнялись."
    return msg


async def repair_all_vehicle_history_slow(
    progress_callback=None,
    pause_seconds: int = 4,
) -> str:
    """
    Quota-safe fleet repair:
      one vehicle -> one batch -> formatting -> pause.
    429 errors are retried with 30/60/90-second backoff.
    """
    historical_rates: Dict[date, float] = {}
    rate_error = None

    try:
        historical_rates = await asyncio.to_thread(
            load_minfin_dnipro_sell_archive
        )
    except Exception as e:
        rate_error = e
        logger.error("Slow historical repair USD archive error: %s", e)

    spreadsheet = await asyncio.to_thread(get_sheet)

    totals = {
        "changed_sheets": 0,
        "expense_odos": 0,
        "usd_expenses": 0,
        "usd_incomes": 0,
        "mileage_deltas": 0,
    }
    processed = 0

    for car_id in KNOWN_CAR_IDS:
        result = await asyncio.to_thread(
            _repair_one_vehicle_history,
            spreadsheet,
            car_id,
            historical_rates,
        )
        processed += 1

        if result["changed"]:
            totals["changed_sheets"] += 1
        totals["expense_odos"] += result["expense_odos"]
        totals["usd_expenses"] += result["usd_expenses"]
        totals["usd_incomes"] += result["usd_incomes"]
        totals["mileage_deltas"] += result["mileage_deltas"]

        logger.info(
            "Slow repair %s/%s: %s changed=%s",
            processed,
            len(KNOWN_CAR_IDS),
            car_id,
            result["changed"],
        )

        if progress_callback and (
            processed == 1 or
            processed % 5 == 0 or
            processed == len(KNOWN_CAR_IDS)
        ):
            try:
                await progress_callback(
                    processed,
                    len(KNOWN_CAR_IDS),
                    car_id,
                )
            except Exception as e:
                logger.warning("Repair progress message failed: %s", e)

        # Deliberately spread requests across time.
        if processed < len(KNOWN_CAR_IDS):
            await asyncio.sleep(pause_seconds)

    global REPORT_CACHE
    REPORT_CACHE = {"snapshot": None, "time": None}

    msg = (
        "✅ Плавное исправление автопарка завершено.\n"
        f"🚗 Обработано машин: {processed}\n"
        f"🚗 Листов с изменениями: {totals['changed_sheets']}\n"
        f"🔵 Одометры расходов F: {totals['expense_odos']}\n"
        f"🔵 USD расходов I: {totals['usd_expenses']}\n"
        f"🔵 USD приходов N: {totals['usd_incomes']}\n"
        f"🔵 Пробеги O: {totals['mileage_deltas']}"
    )
    if rate_error:
        msg += (
            "\n⚠️ Архив USD был недоступен; "
            "пустые исторические I/N могли остаться незаполненными."
        )
    return msg


def repair_all_vehicle_history() -> str:
    """
    Compatibility wrapper kept for code safety.
    Manual Telegram command uses repair_all_vehicle_history_slow().
    """
    return (
        "Используй асинхронное плавное исправление "
        "repair_all_vehicle_history_slow()."
    )


# ===== Weekday automatic completion of manually entered rows =====

def _known_odometer_points_from_rows(rows: List[List[str]]) -> List[Tuple[date, int]]:
    """
    Collect trustworthy odometer points from both sides of a vehicle sheet:
      E/F = expense date / odometer
      K/L = income date / odometer
    """
    by_date: Dict[date, int] = {}

    for row in rows[7:]:
        pairs = [
            (
                parse_short_date(row[4] if len(row) > 4 else None),
                parse_num(row[5] if len(row) > 5 else None),
            ),
            (
                parse_short_date(row[10] if len(row) > 10 else None),
                parse_num(row[11] if len(row) > 11 else None),
            ),
        ]
        for d, odo in pairs:
            if d and odo is not None and odo > 1000:
                # If the same date has several readings, keep the largest.
                if d not in by_date or odo > by_date[d]:
                    by_date[d] = odo

    return sorted(by_date.items(), key=lambda x: x[0])


def estimate_missing_odometer_from_rows(
    rows: List[List[str]], target_date: date
) -> Optional[int]:
    """
    Estimate a missing odometer for one vehicle.

    Priority:
      1) interpolate between the nearest valid readings before/after the date;
      2) otherwise extrapolate from the nearest reading using that vehicle's
         median valid daily mileage.

    Existing odometers are never overwritten.
    """
    points = _known_odometer_points_from_rows(rows)
    if len(points) < 2:
        return None

    before = None
    after = None
    for d, odo in points:
        if d <= target_date:
            before = (d, odo)
        if d >= target_date and after is None:
            after = (d, odo)

    # Best case: interpolate inside a known interval.
    if before and after and before[0] != after[0]:
        days = (after[0] - before[0]).days
        delta = after[1] - before[1]
        if days > 0 and delta >= 0:
            daily = delta / days
            if 0 <= daily <= 300:
                elapsed = (target_date - before[0]).days
                return int(round(before[1] + daily * elapsed))

    # Vehicle-specific median daily mileage from valid historical segments.
    daily_rates = []
    for i in range(1, len(points)):
        d1, o1 = points[i - 1]
        d2, o2 = points[i]
        days = (d2 - d1).days
        delta = o2 - o1
        if days <= 0 or delta < 0:
            continue
        daily = delta / days
        if 0 <= daily <= 300:
            daily_rates.append(daily)

    if not daily_rates:
        return None

    avg_daily = median(daily_rates)

    if before:
        days = (target_date - before[0]).days
        return max(0, int(round(before[1] + avg_daily * days)))

    if after:
        days = (after[0] - target_date).days
        return max(0, int(round(after[1] - avg_daily * days)))

    return None


def complete_manual_rows_all_vehicles() -> str:
    """
    Every weekday at 18:30:
      - fill missing USD I/N using today's Dnipro cash USD SELL rate;
      - fill missing EXPENSE odometers F from each vehicle's own history;
      - keep O as payment-to-payment mileage delta;
      - color every cell written by the bot blue;
      - never overwrite manually filled F/I/N values.
    """
    spreadsheet = get_sheet()

    usd_rate = None
    try:
        usd_rate = get_usd_black_rate_dnipro()
    except Exception as e:
        logger.error("Weekday completion USD rate error: %s", e)

    sheets_changed = 0
    usd_cells = 0
    expense_odo_cells = 0
    mileage_cells = 0
    skipped_odo = 0

    for car_id in KNOWN_CAR_IDS:
        ws = get_matching_worksheet(spreadsheet, car_id)
        if not ws:
            continue

        rows = ws.get_all_values()
        updates: List[Dict[str, Any]] = []
        blue: Dict[str, List[int]] = {
            "F": [], "I": [], "N": [], "O": []
        }

        for row_idx, row in enumerate(rows[7:], start=8):
            # EXPENSE: E date, F odometer, G article, H UAH, I USD.
            expense_date = parse_short_date(
                row[4] if len(row) > 4 else None
            )
            expense_odo_raw = (
                str(row[5]).strip() if len(row) > 5 else ""
            )
            expense_desc = (
                str(row[6]).strip() if len(row) > 6 else ""
            )
            expense_uah = parse_money_float(
                row[7] if len(row) > 7 else None
            )
            expense_usd_raw = (
                str(row[8]).strip() if len(row) > 8 else ""
            )

            expense_has_operation = bool(
                expense_date and (
                    expense_desc or
                    expense_uah is not None or
                    expense_usd_raw
                )
            )

            if expense_has_operation:
                if (
                    not expense_usd_raw and
                    expense_uah is not None and
                    usd_rate is not None and
                    usd_rate > 0
                ):
                    updates.append({
                        "range": f"I{row_idx}",
                        "values": [[round(expense_uah / usd_rate, 2)]],
                    })
                    blue["I"].append(row_idx)
                    usd_cells += 1

                if not expense_odo_raw:
                    estimated = estimate_missing_odometer_from_rows(
                        rows, expense_date
                    )
                    if estimated is not None:
                        updates.append({
                            "range": f"F{row_idx}",
                            "values": [[estimated]],
                        })
                        blue["F"].append(row_idx)
                        expense_odo_cells += 1
                    else:
                        skipped_odo += 1

            # INCOME: K date, L odometer, M UAH, N USD, O payment delta km.
            income_date = parse_short_date(
                row[10] if len(row) > 10 else None
            )
            income_uah = parse_money_float(
                row[12] if len(row) > 12 else None
            )
            income_usd_raw = (
                str(row[13]).strip() if len(row) > 13 else ""
            )

            if (
                income_date and
                income_uah is not None and
                income_uah > 0 and
                not income_usd_raw and
                usd_rate is not None and
                usd_rate > 0
            ):
                updates.append({
                    "range": f"N{row_idx}",
                    "values": [[round(income_uah / usd_rate, 2)]],
                })
                blue["N"].append(row_idx)
                usd_cells += 1

        # Keep O correct after any manually added income/payment rows.
        o_updates, o_blue_rows = _income_mileage_delta_updates(rows)
        updates.extend(o_updates)
        blue["O"].extend(o_blue_rows)
        mileage_cells += len(o_blue_rows)

        if updates:
            ws.batch_update(
                updates,
                value_input_option="USER_ENTERED",
            )
            _apply_blue_to_written_cells(ws, blue)
            sheets_changed += 1

    global REPORT_CACHE
    REPORT_CACHE = {"snapshot": None, "time": None}

    rate_text = f"{usd_rate:.2f}" if usd_rate else "не получен"
    return (
        "✅ Автозаполнение таблиц завершено. "
        f"Листов изменено: {sheets_changed}; "
        f"USD-ячеек: {usd_cells}; "
        f"одометров расходов F: {expense_odo_cells}; "
        f"пробегов O: {mileage_cells}; "
        f"курс продажи USD: {rate_text}; "
        f"одометров без достаточных данных: {skipped_odo}."
    )



def _complete_one_vehicle_manual_rows(
    spreadsheet,
    car_id: str,
    usd_rate: Optional[float],
) -> Dict[str, Any]:
    """Quota-safe weekday completion for one vehicle only."""
    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return {
            "found": False,
            "changed": False,
            "usd_cells": 0,
            "expense_odo_cells": 0,
            "mileage_cells": 0,
            "skipped_odo": 0,
        }

    rows = _run_with_sheets_backoff(
        ws.get_all_values,
        operation_name=f"{car_id}: weekday read",
    )

    updates: List[Dict[str, Any]] = []
    blue: Dict[str, List[int]] = {
        "F": [], "I": [], "N": [], "O": []
    }
    usd_cells = 0
    expense_odo_cells = 0
    skipped_odo = 0

    for row_idx, row in enumerate(rows[7:], start=8):
        expense_date = parse_short_date(
            row[4] if len(row) > 4 else None
        )
        expense_odo_raw = (
            str(row[5]).strip() if len(row) > 5 else ""
        )
        expense_desc = (
            str(row[6]).strip() if len(row) > 6 else ""
        )
        expense_uah = parse_money_float(
            row[7] if len(row) > 7 else None
        )
        expense_usd_raw = (
            str(row[8]).strip() if len(row) > 8 else ""
        )

        expense_has_operation = bool(
            expense_date and (
                expense_desc or
                expense_uah is not None or
                expense_usd_raw
            )
        )

        if expense_has_operation:
            if (
                not expense_usd_raw and
                expense_uah is not None and
                usd_rate is not None and
                usd_rate > 0
            ):
                updates.append({
                    "range": f"I{row_idx}",
                    "values": [[round(expense_uah / usd_rate, 2)]],
                })
                blue["I"].append(row_idx)
                usd_cells += 1

            if not expense_odo_raw:
                estimated = estimate_missing_odometer_from_rows(
                    rows, expense_date
                )
                if estimated is not None:
                    updates.append({
                        "range": f"F{row_idx}",
                        "values": [[estimated]],
                    })
                    blue["F"].append(row_idx)
                    expense_odo_cells += 1
                else:
                    skipped_odo += 1

        income_date = parse_short_date(
            row[10] if len(row) > 10 else None
        )
        income_uah = parse_money_float(
            row[12] if len(row) > 12 else None
        )
        income_usd_raw = (
            str(row[13]).strip() if len(row) > 13 else ""
        )

        if (
            income_date and
            income_uah is not None and
            income_uah > 0 and
            not income_usd_raw and
            usd_rate is not None and
            usd_rate > 0
        ):
            updates.append({
                "range": f"N{row_idx}",
                "values": [[round(income_uah / usd_rate, 2)]],
            })
            blue["N"].append(row_idx)
            usd_cells += 1

    o_updates, o_blue_rows = _income_mileage_delta_updates(rows)
    updates.extend(o_updates)
    blue["O"].extend(o_blue_rows)

    if updates:
        _run_with_sheets_backoff(
            lambda: ws.batch_update(
                updates,
                value_input_option="USER_ENTERED",
            ),
            operation_name=f"{car_id}: weekday batch write",
        )
        _run_with_sheets_backoff(
            lambda: _apply_blue_to_written_cells(ws, blue),
            operation_name=f"{car_id}: weekday formatting",
        )

    return {
        "found": True,
        "changed": bool(updates),
        "usd_cells": usd_cells,
        "expense_odo_cells": expense_odo_cells,
        "mileage_cells": len(o_blue_rows),
        "skipped_odo": skipped_odo,
    }


async def complete_manual_rows_all_vehicles_slow(
    pause_seconds: int = 4,
) -> str:
    """Weekday quota-safe completion, one vehicle at a time."""
    spreadsheet = await asyncio.to_thread(get_sheet)

    usd_rate = None
    try:
        usd_rate = await asyncio.to_thread(
            get_usd_black_rate_dnipro
        )
    except Exception as e:
        logger.error("Weekday slow USD rate error: %s", e)

    totals = {
        "changed_sheets": 0,
        "usd_cells": 0,
        "expense_odo_cells": 0,
        "mileage_cells": 0,
        "skipped_odo": 0,
    }

    for pos, car_id in enumerate(KNOWN_CAR_IDS, start=1):
        result = await asyncio.to_thread(
            _complete_one_vehicle_manual_rows,
            spreadsheet,
            car_id,
            usd_rate,
        )

        if result["changed"]:
            totals["changed_sheets"] += 1
        totals["usd_cells"] += result["usd_cells"]
        totals["expense_odo_cells"] += result["expense_odo_cells"]
        totals["mileage_cells"] += result["mileage_cells"]
        totals["skipped_odo"] += result["skipped_odo"]

        logger.info(
            "Weekday slow completion %s/%s: %s",
            pos,
            len(KNOWN_CAR_IDS),
            car_id,
        )

        if pos < len(KNOWN_CAR_IDS):
            await asyncio.sleep(pause_seconds)

    global REPORT_CACHE
    REPORT_CACHE = {"snapshot": None, "time": None}

    rate_text = f"{usd_rate:.2f}" if usd_rate else "не получен"
    return (
        "✅ Плавное автозаполнение завершено. "
        f"Листов изменено: {totals['changed_sheets']}; "
        f"USD-ячеек: {totals['usd_cells']}; "
        f"одометров расходов F: {totals['expense_odo_cells']}; "
        f"пробегов O: {totals['mileage_cells']}; "
        f"курс продажи USD: {rate_text}; "
        f"одометров без данных: {totals['skipped_odo']}."
    )


async def weekday_vehicle_completion_job(context: ContextTypes.DEFAULT_TYPE):
    """Monday-Thursday 18:30 Kyiv. Process vehicles slowly to respect Sheets quotas."""
    try:
        result = await complete_manual_rows_all_vehicles_slow()
        logger.info("Weekday vehicle completion: %s", result)
    except Exception as e:
        logger.error("Weekday vehicle completion failed: %s", e, exc_info=True)
        for user_id in ALLOWED_USERS:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"⚠️ Ошибка автозаполнения таблиц: {e}",
                )
            except Exception:
                pass


# ===== Fleet monthly reports =====

REPORT_SHEET_TITLES = {"отчет", "отчеты", "отчёт", "отчёты"}

MONTH_NAMES_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def parse_money_float(value) -> Optional[float]:
    """Parse a money value from Sheets and preserve cents."""
    if value is None:
        return None
    s = str(value).strip().replace("\xa0", "").replace(" ", "")
    if not s:
        return None
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if not s or s in {"-", ".", "-."}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def get_reports_worksheet(spreadsheet):
    """Find the user's existing report sheet without creating or renaming anything."""
    for ws in spreadsheet.worksheets():
        if str(ws.title).strip().lower() in REPORT_SHEET_TITLES:
            return ws
    return None


def month_label(year: int, month: int) -> str:
    return f"{MONTH_NAMES_RU[month]} {year}"


def iter_months_desc(start_year: int, start_month: int, end_year: int, end_month: int):
    """Current month -> oldest month, including empty calendar months."""
    y, m = start_year, start_month
    while (y, m) >= (end_year, end_month):
        yield y, m
        m -= 1
        if m == 0:
            m = 12
            y -= 1


def detect_report_layout(ws):
    """
    Read the layout already created in the report sheet.

    Expected structure (as on the user's sheet):
      row above subheaders: vehicle IDs, one vehicle per 3 columns
      subheader row: Приход | Расход | Чистые
      next row and below: months/data

    Returns: (data_start_row, {car_id: start_column})
    """
    probe = ws.get(f"A1:ZZ10")
    subheader_row = None

    for r_idx, row in enumerate(probe, start=1):
        normalized = [str(x).strip().lower() for x in row]
        hits = sum(
            1 for x in normalized
            if x in {"приход", "расход", "чистые", "чистая", "прибыль"}
        )
        if hits >= 2:
            subheader_row = r_idx
            break

    if subheader_row is None:
        raise RuntimeError(
            "На листе отчета не найдена строка заголовков "
            "'Приход / Расход / Чистые'."
        )

    car_columns: Dict[str, int] = {}

    # Vehicle number can be on the same row or several rows above it.
    first_scan_row = max(1, subheader_row - 3)
    for r_idx in range(first_scan_row, subheader_row + 1):
        row = probe[r_idx - 1] if r_idx - 1 < len(probe) else []
        for c_idx, cell in enumerate(row, start=1):
            digits = extract_digits(str(cell))
            if digits in KNOWN_CAR_IDS and digits not in car_columns:
                car_columns[digits] = c_idx

    if not car_columns:
        raise RuntimeError("На листе отчета не найдены номера машин в шапке.")

    return subheader_row + 1, car_columns


def collect_vehicle_monthly_usd():
    """
    Aggregate all vehicle sheets by month:
      expense USD = column I, expense date = E
      income  USD = column N, income  date = K
      clean profit = income - expense

    Returns (monthly_data, oldest_date).
    """
    spreadsheet = get_sheet()
    monthly: Dict[Tuple[int, int], Dict[str, Dict[str, float]]] = {}
    oldest_date: Optional[date] = None

    for car_id in KNOWN_CAR_IDS:
        ws = get_matching_worksheet(spreadsheet, car_id)
        if not ws:
            continue

        rows = ws.get_all_values()
        for row in rows[7:]:
            expense_date = parse_short_date(row[4] if len(row) > 4 else None)
            expense_usd = parse_money_float(row[8] if len(row) > 8 else None)
            if expense_date:
                if oldest_date is None or expense_date < oldest_date:
                    oldest_date = expense_date
                if expense_usd is not None:
                    key = (expense_date.year, expense_date.month)
                    totals = monthly.setdefault(key, {}).setdefault(
                        car_id, {"income": 0.0, "expense": 0.0}
                    )
                    totals["expense"] += expense_usd

            income_date = parse_short_date(row[10] if len(row) > 10 else None)
            income_usd = parse_money_float(row[13] if len(row) > 13 else None)
            if income_date:
                if oldest_date is None or income_date < oldest_date:
                    oldest_date = income_date
                if income_usd is not None:
                    key = (income_date.year, income_date.month)
                    totals = monthly.setdefault(key, {}).setdefault(
                        car_id, {"income": 0.0, "expense": 0.0}
                    )
                    totals["income"] += income_usd

    return monthly, oldest_date


def report_values_for_car(monthly_data, year: int, month: int, car_id: str):
    totals = monthly_data.get((year, month), {}).get(car_id, {})
    income = round(float(totals.get("income", 0.0)), 2)
    expense = round(float(totals.get("expense", 0.0)), 2)
    profit = round(income - expense, 2)
    return [income, expense, profit]



def build_report_period_rows(today: date, oldest_date: date):
    """
    Build report rows newest -> oldest.
    After the last month of every year add a yearly total row.
    Returns list of tuples: ("month"|"year", year, month_or_none).
    """
    months = list(iter_months_desc(
        today.year, today.month, oldest_date.year, oldest_date.month
    ))
    result = []
    for pos, (year, month) in enumerate(months):
        result.append(("month", year, month))
        next_year = months[pos + 1][0] if pos + 1 < len(months) else None
        if next_year != year:
            result.append(("year", year, None))
    return result


def yearly_values_for_car(monthly_data, year: int, car_id: str):
    income = 0.0
    expense = 0.0
    for (y, _m), cars in monthly_data.items():
        if y != year:
            continue
        totals = cars.get(car_id, {})
        income += float(totals.get("income", 0.0))
        expense += float(totals.get("expense", 0.0))
    income = round(income, 2)
    expense = round(expense, 2)
    return [income, expense, round(income - expense, 2)]


def format_report_year_rows(reports_ws, data_start_row: int, period_rows, car_columns):
    """Visually separate years without changing the user's existing header."""
    if not period_rows:
        return

    last_col = max((c + 2 for c in car_columns.values()), default=1)
    last_letter = gspread.utils.rowcol_to_a1(1, last_col).rstrip("1")

    normal_fmt = CellFormat(
        textFormat=TextFormat(bold=False),
        borders={
            "top": {"style": "NONE"},
            "bottom": {"style": "NONE"},
        },
    )
    year_fmt = CellFormat(
        textFormat=TextFormat(bold=True),
        borders={
            "top": {"style": "SOLID_MEDIUM"},
            "bottom": {"style": "SOLID_MEDIUM"},
        },
    )

    # Only touch the report body; the user's header/design remains intact.
    format_cell_range(
        reports_ws,
        f"A{data_start_row}:{last_letter}{data_start_row + len(period_rows) - 1}",
        normal_fmt,
    )
    for offset, (kind, _year, _month) in enumerate(period_rows):
        if kind == "year":
            row_no = data_start_row + offset
            format_cell_range(
                reports_ws,
                f"A{row_no}:{last_letter}{row_no}",
                year_fmt,
            )


def get_graphs_worksheet(spreadsheet):
    for ws in spreadsheet.worksheets():
        if str(ws.title).strip().lower() in {"графики", "графік", "графіки"}:
            return ws
    return None


def _delete_existing_sheet_charts(spreadsheet, sheet_id: int):
    """Delete charts only from the 'Графики' sheet, never from other sheets."""
    try:
        metadata = spreadsheet.fetch_sheet_metadata()
        requests_to_delete = []
        for sheet in metadata.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("sheetId") != sheet_id:
                continue
            for chart in sheet.get("charts", []) or []:
                chart_id = chart.get("chartId")
                if chart_id is not None:
                    requests_to_delete.append({
                        "deleteEmbeddedObject": {"objectId": chart_id}
                    })
        if requests_to_delete:
            spreadsheet.batch_update({"requests": requests_to_delete})
    except Exception as e:
        logger.warning("Не удалось удалить старые графики: %s", e)


def rebuild_fleet_graphs(monthly_data=None) -> str:
    """
    Update only the two requested charts on the existing 'Графики' sheet:
      1) fleet income vs expense for the last 12 months;
      2) current-month expense share by vehicle.

    Charts use hidden helper columns on the same sheet because Google Sheets
    embedded charts require a cell range as their data source.
    No visible source tables are left on the sheet.
    """
    spreadsheet = get_sheet()
    graphs_ws = get_graphs_worksheet(spreadsheet)
    if not graphs_ws:
        return "⚠️ Лист 'Графики' не найден."

    if monthly_data is None:
        monthly_data, oldest_date = collect_vehicle_monthly_usd()
    else:
        all_dates = [date(y, m, 1) for (y, m) in monthly_data.keys()]
        oldest_date = min(all_dates) if all_dates else datetime.now(KYIV_TZ).date()

    today = datetime.now(KYIV_TZ).date()
    if oldest_date is None:
        oldest_date = today

    # Make sure helper columns X:AE exist.
    required_cols = 31  # AE
    if graphs_ws.col_count < required_cols:
        graphs_ws.add_cols(required_cols - graphs_ws.col_count)

    # Last 12 months, oldest -> newest for a natural timeline.
    months_all = list(iter_months_desc(
        today.year, today.month, oldest_date.year, oldest_date.month
    ))
    months_12 = list(reversed(months_all[:12]))

    fleet_rows = [["Месяц", "Приход $", "Расход $"]]
    for y, m in months_12:
        income = 0.0
        expense = 0.0
        for car_id in KNOWN_CAR_IDS:
            car_income, car_expense, _profit = report_values_for_car(
                monthly_data, y, m, car_id
            )
            income += car_income
            expense += car_expense
        fleet_rows.append([
            month_label(y, m),
            round(income, 2),
            round(expense, 2),
        ])

    # Pie chart: only vehicles that actually have expenses this month.
    expense_rows = [["Авто", "Расход $"]]
    for car_id in KNOWN_CAR_IDS:
        _income, expense, _profit = report_values_for_car(
            monthly_data, today.year, today.month, car_id
        )
        if expense > 0:
            expense_rows.append([
                VEHICLE_MAP.get(car_id, car_id),
                round(expense, 2),
            ])

    # Remove old visible helper tables left by the previous version.
    graphs_ws.batch_clear([
        "A1:I100",
        "X1:AE100",
    ])

    # Hidden data sources:
    # X:Z = monthly fleet dynamics
    # AC:AD = current-month expense by vehicle
    graphs_ws.update(
        range_name="X1",
        values=fleet_rows,
        value_input_option="USER_ENTERED",
    )
    graphs_ws.update(
        range_name="AC1",
        values=expense_rows,
        value_input_option="USER_ENTERED",
    )

    # Hide helper columns X:AE.
    spreadsheet.batch_update({
        "requests": [{
            "updateDimensionProperties": {
                "range": {
                    "sheetId": graphs_ws.id,
                    "dimension": "COLUMNS",
                    "startIndex": 23,  # X
                    "endIndex": 31,    # through AE
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        }]
    })

    # Delete only charts on the 'Графики' sheet and recreate the two requested ones.
    _delete_existing_sheet_charts(spreadsheet, graphs_ws.id)

    sheet_id = graphs_ws.id
    fleet_end = len(fleet_rows)
    expense_end = len(expense_rows)

    def chart_range(start_col_index: int, end_col_index: int, end_row: int):
        return {
            "sources": [{
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": end_row,
                "startColumnIndex": start_col_index,
                "endColumnIndex": end_col_index,
            }]
        }

    chart_requests = [
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Автопарк: приходы и расходы за последние 12 месяцев",
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "Месяц"},
                                {"position": "LEFT_AXIS", "title": "USD"},
                            ],
                            "domains": [{
                                "domain": {
                                    "sourceRange": {
                                        "sources": [{
                                            "sheetId": sheet_id,
                                            "startRowIndex": 0,
                                            "endRowIndex": fleet_end,
                                            "startColumnIndex": 23,  # X
                                            "endColumnIndex": 24,
                                        }]
                                    }
                                }
                            }],
                            "series": [
                                {
                                    "series": {
                                        "sourceRange": {
                                            "sources": [{
                                                "sheetId": sheet_id,
                                                "startRowIndex": 0,
                                                "endRowIndex": fleet_end,
                                                "startColumnIndex": 24,  # Y
                                                "endColumnIndex": 25,
                                            }]
                                        }
                                    },
                                    "targetAxis": "LEFT_AXIS",
                                },
                                {
                                    "series": {
                                        "sourceRange": {
                                            "sources": [{
                                                "sheetId": sheet_id,
                                                "startRowIndex": 0,
                                                "endRowIndex": fleet_end,
                                                "startColumnIndex": 25,  # Z
                                                "endColumnIndex": 26,
                                            }]
                                        }
                                    },
                                    "targetAxis": "LEFT_AXIS",
                                },
                            ],
                            "headerCount": 1,
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": sheet_id,
                                "rowIndex": 0,
                                "columnIndex": 0,  # A1
                            },
                            "widthPixels": 1050,
                            "heightPixels": 500,
                        }
                    },
                }
            }
        },
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": f"Расходы по машинам — {month_label(today.year, today.month)}",
                        "pieChart": {
                            "legendPosition": "RIGHT_LEGEND",
                            "domain": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": sheet_id,
                                        "startRowIndex": 0,
                                        "endRowIndex": expense_end,
                                        "startColumnIndex": 28,  # AC
                                        "endColumnIndex": 29,
                                    }]
                                }
                            },
                            "series": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": sheet_id,
                                        "startRowIndex": 0,
                                        "endRowIndex": expense_end,
                                        "startColumnIndex": 29,  # AD
                                        "endColumnIndex": 30,
                                    }]
                                }
                            },
                            "threeDimensional": False,
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": sheet_id,
                                "rowIndex": 25,
                                "columnIndex": 0,  # A26
                            },
                            "widthPixels": 1050,
                            "heightPixels": 560,
                        }
                    },
                }
            }
        },
    ]

    spreadsheet.batch_update({"requests": chart_requests})
    return "✅ Графики обновлены."


def rebuild_reports_sheet_history() -> str:
    """
    Full rebuild of the report body.
    Keeps the user's header/layout, adds yearly total rows and separators.
    """
    spreadsheet = get_sheet()
    reports_ws = get_reports_worksheet(spreadsheet)
    if not reports_ws:
        return "❌ Лист 'Отчет/Отчеты' не найден."

    data_start_row, car_columns = detect_report_layout(reports_ws)
    monthly_data, oldest_date = collect_vehicle_monthly_usd()

    today = datetime.now(KYIV_TZ).date()
    if oldest_date is None:
        oldest_date = today

    period_rows = build_report_period_rows(today, oldest_date)
    end_row = data_start_row + len(period_rows) - 1

    # Clear only old report body values, not headers/formatting.
    old_last = max(reports_ws.row_count, end_row)
    last_col = max((c + 2 for c in car_columns.values()), default=1)
    last_letter = gspread.utils.rowcol_to_a1(1, last_col).rstrip("1")
    reports_ws.batch_clear([f"A{data_start_row}:{last_letter}{old_last}"])

    updates = [{
        "range": f"A{data_start_row}:A{end_row}",
        "values": [[
            month_label(year, month) if kind == "month" else f"ИТОГО {year}"
        ] for kind, year, month in period_rows],
    }]

    for car_id, start_col in car_columns.items():
        end_col = start_col + 2
        start_letter = gspread.utils.rowcol_to_a1(1, start_col).rstrip("1")
        end_letter = gspread.utils.rowcol_to_a1(1, end_col).rstrip("1")

        values = []
        for kind, year, month in period_rows:
            if kind == "month":
                values.append(report_values_for_car(
                    monthly_data, year, month, car_id
                ))
            else:
                values.append(yearly_values_for_car(
                    monthly_data, year, car_id
                ))

        updates.append({
            "range": f"{start_letter}{data_start_row}:{end_letter}{end_row}",
            "values": values,
        })

    reports_ws.batch_update(updates, value_input_option="USER_ENTERED")
    format_report_year_rows(
        reports_ws, data_start_row, period_rows, car_columns
    )
    update_report_vehicle_headers_and_borders(
        reports_ws, data_start_row, car_columns, end_row
    )

    missing_layout = [c for c in KNOWN_CAR_IDS if c not in car_columns]
    result = (
        "✅ Отчеты по автопарку заполнены.\n"
        f"📅 Строк периодов: {len(period_rows)}\n"
        f"🚗 Машин в шапке отчета: {len(car_columns)}\n"
        "📊 После каждого года добавлена строка ИТОГО.\n"
        "💰 Приход = сумма колонки N\n"
        "💸 Расход = сумма колонки I\n"
        "📈 Чистые = Приход - Расход"
    )
    if missing_layout:
        result += (
            "\n\n⚠️ В шапке листа отчета не найдены машины: "
            + ", ".join(missing_layout)
        )
    return result

def update_current_month_report() -> str:
    """
    Friday update: recalculate current month, current-year total and graphs.
    If a new month/year layout row is absent, safely rebuild the report.
    """
    spreadsheet = get_sheet()
    reports_ws = get_reports_worksheet(spreadsheet)
    if not reports_ws:
        return "❌ Лист 'Отчет/Отчеты' не найден."

    data_start_row, car_columns = detect_report_layout(reports_ws)
    current = datetime.now(KYIV_TZ)
    current_label = month_label(current.year, current.month)
    year_label = f"ИТОГО {current.year}"

    col_a = reports_ws.col_values(1)
    target_row = None
    year_row = None
    for row_idx in range(data_start_row, len(col_a) + 1):
        label = str(col_a[row_idx - 1]).strip()
        if label == current_label:
            target_row = row_idx
        elif label == year_label:
            year_row = row_idx

    # New month or old report layout without yearly rows.
    if target_row is None or year_row is None:
        return rebuild_reports_sheet_history()

    monthly_data, _ = collect_vehicle_monthly_usd()
    updates = []

    for car_id, start_col in car_columns.items():
        end_col = start_col + 2
        start_letter = gspread.utils.rowcol_to_a1(1, start_col).rstrip("1")
        end_letter = gspread.utils.rowcol_to_a1(1, end_col).rstrip("1")

        updates.append({
            "range": f"{start_letter}{target_row}:{end_letter}{target_row}",
            "values": [report_values_for_car(
                monthly_data, current.year, current.month, car_id
            )],
        })
        updates.append({
            "range": f"{start_letter}{year_row}:{end_letter}{year_row}",
            "values": [yearly_values_for_car(
                monthly_data, current.year, car_id
            )],
        })

    if updates:
        reports_ws.batch_update(updates, value_input_option="USER_ENTERED")

    # Keep report headers and vehicle separators correct on every Friday refresh.
    last_report_row = max(len(col_a), year_row, target_row)
    update_report_vehicle_headers_and_borders(
        reports_ws, data_start_row, car_columns, last_report_row
    )

    return (
        f"✅ Текущий месяц '{current_label}' обновлен. "
        f"Итог {current.year} пересчитан."
    )


async def friday_reports_update_job(context: ContextTypes.DEFAULT_TYPE):
    """Friday 18:30 Kyiv: complete vehicle sheets, then update report and graphs."""
    try:
        completion_result = await complete_manual_rows_all_vehicles_slow()
        report_result = await asyncio.to_thread(update_current_month_report)
        graphs_result = await asyncio.to_thread(rebuild_fleet_graphs)
        logger.info(
            "Friday maintenance: %s | %s | %s",
            completion_result,
            report_result,
            graphs_result,
        )
    except Exception as e:
        logger.error(
            "Friday vehicle/report/graphs update failed: %s",
            e,
            exc_info=True,
        )
        for user_id in ALLOWED_USERS:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"⚠️ Ошибка пятничного обновления таблиц/отчета/графиков: {e}",
                )
            except Exception:
                pass


# ===== Telegram handlers =====

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено")
        return

    text = (update.message.text or "").strip()
    logger.info(f"Incoming message from {user_id}: {text}")

    try:
        repair_text = text.lower().strip()

        # One-car command, for example: "исправить 1457".
        m_repair_car = re.fullmatch(
            r"(?:исправить|восстановить)\s+([a-zа-я0-9]+)",
            repair_text,
            re.IGNORECASE,
        )
        if m_repair_car:
            requested_car = resolve_car_id(m_repair_car.group(1))
            if requested_car:
                await update.message.reply_text(
                    f"⏳ Исправляю только машину "
                    f"{full_plate_from_short(requested_car)}..."
                )
                result = await asyncio.to_thread(
                    repair_one_vehicle_history,
                    requested_car,
                )
                await update.message.reply_text(result)
                return

        if repair_text in {
            "исправить таблицы",
            "исправить автопарк",
            "восстановить таблицы",
        }:
            await update.message.reply_text(
                "⏳ Начинаю плавное исправление всех машин. "
                "Обрабатываю по одной машине с паузами, чтобы "
                "не превышать лимиты Google Sheets."
            )

            async def _repair_progress(done, total, car_id):
                await update.message.reply_text(
                    f"🔄 Исправлено {done}/{total}. "
                    f"Последняя машина: {full_plate_from_short(car_id)}"
                )

            result = await repair_all_vehicle_history_slow(
                progress_callback=_repair_progress,
                pause_seconds=4,
            )
            await update.message.reply_text(result)
            return

        if text.lower().strip() in {
            "обновить отчеты",
            "обновить отчёт",
            "обновить отчет",
            "оновити звіти",
            "оновити звіт",
        }:
            await update.message.reply_text(
                "⏳ Собираю отчеты по всем машинам и всем месяцам..."
            )
            result = await asyncio.to_thread(rebuild_reports_sheet_history)
            await update.message.reply_text(result)
            return

        if text.lower().strip() in {
            "обновить курсы",
            "оновити курси",
            "обновить курс",
            "оновити курс",
        }:
            await update.message.reply_text(
                "⏳ Оновлюю історичні USD-суми за готівковим курсом продажу USD у Дніпрі. "
                "Перерахую лише колонки I та N у доларові суми. Це може зайняти кілька хвилин."
            )
            result = await asyncio.to_thread(backfill_historical_usd_buy_rates)
            await update.message.reply_text(result)
            return

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
                await update.message.reply_text("✅ Запис скасовано як дубль.")
                return
            await update.message.reply_text("Напиши «новий» або «дубль».")
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
                await update.message.reply_text("Добре. Надішли правильний одометр або напиши «так», щоб я підставив середньостатистичний.")
                return
            await update.message.reply_text("Напиши «так» для підтвердження або «ні» для скасування.")
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
                        await update.message.reply_text("⚠️ Пробіг виглядає нетипово великим. Підтвердити?")
                        return

                spreadsheet = get_sheet()
                for action in pending_actions:
                    ws = get_matching_worksheet(spreadsheet, action["car_id"])
                    if ws and detect_duplicate(ws, action, raw_text=text):
                        context.user_data["waiting_duplicate_confirm"] = True
                        context.user_data["pending_actions_after_duplicate"] = pending_actions
                        await update.message.reply_text("❓ Це новий запис чи дубль попереднього?")
                        return

                result = write_actions_to_sheet(pending_actions, raw_text=text)
                await update.message.reply_text(result)
                return

            if is_yes_statistical(text):
                if not pending_actions:
                    await update.message.reply_text("Немає даних для обробки.")
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
                    await update.message.reply_text("Не вдалося обчислити середньостатистичний пробіг. Надішли, будь ласка, цифри одометра.")
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
                        await update.message.reply_text("❓ Це новий запис чи дубль попереднього?")
                        return

                result = write_actions_to_sheet(pending_actions, raw_text=text)
                await update.message.reply_text(result)
                return

            await update.message.reply_text("Напиши «так», якщо мені додати середньостатистичний пробіг, або просто надішли цифри одометра.")
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
                    await update.message.reply_text("⚠️ Пробіг виглядає нетипово великим. Підтвердити?")
                    return
                if ws and detect_duplicate(ws, pending_data, raw_text=text):
                    context.user_data["waiting_duplicate_confirm"] = True
                    context.user_data["pending_actions_after_duplicate"] = [pending_data]
                    context.user_data.pop("pending_data", None)
                    await update.message.reply_text("❓ Це новий запис чи дубль попереднього?")
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
                    await update.message.reply_text("Спочатку вкажи номер машини.")
                    return
                estimated = estimate_odometer_for_car(car_id, operation_date)
                if not estimated:
                    context.user_data.pop("waiting_odometer_choice", None)
                    await update.message.reply_text("Не вдалося обчислити середньостатистичний пробіг. Надішли, будь ласка, цифри одометра.")
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
                    await update.message.reply_text("❓ Це новий запис чи дубль попереднього?")
                    return

                result = write_single_action_to_sheet(pending_data, raw_text=text)
                context.user_data.pop("pending_data", None)
                await update.message.reply_text(result)
                return

            await update.message.reply_text("Напиши «так», якщо мені додати середньостатистичний пробіг, або просто надішли цифри одометра.")
            return

        if is_oil_report_request(text):
            report = build_oil_report()
            await update.message.reply_text("🛢 Стан масла:\n\n" + (report or "Немає даних"))
            return

        if is_grm_report_request(text):
            report = build_grm_report()
            await update.message.reply_text("⚙️ Стан ГРМ:\n\n" + (report or "Немає даних"))
            return

        if is_insurance_report_request(text):
            report = build_insurance_report()
            await update.message.reply_text("🛡 Страховка:\n\n" + (report or "Немає даних"))
            return

        car_id_for_summary = detect_month_summary_request(text)
        if car_id_for_summary:
            await update.message.reply_text(monthly_summary(car_id_for_summary))
            return

        # Якщо повiдомлення — тiльки номер машини (4 цифри) — показуємо картку авто
        text_stripped = text.strip()
        if re.match(r"^\d{4}$", text_stripped) and text_stripped in KNOWN_CAR_IDS:
            car_id = text_stripped
            snapshot = get_data_snapshot()
            rows = next((v for t, v in snapshot.items()
                         if car_id in t or VEHICLE_MAP.get(car_id, "") in t), None)
            lines = [f"🚗 Машина {car_id} ({VEHICLE_MAP.get(car_id, '')})\n"]
            if rows:
                cur_odo = get_current_odometer_from_rows(rows)
                if cur_odo:
                    lines.append(f"📍 Поточний одометр: {format_km(cur_odo)} км")
                _, oil_odo = find_last_service(rows, "oil")
                if oil_odo and cur_odo:
                    oil_rem = 10000 - (max(cur_odo, oil_odo) - oil_odo)
                    lines.append(f"{get_color_icon(oil_rem, 10000)} Масло: {format_km(oil_rem)} км до регламенту")
                if car_id not in SKIP_GRM:
                    _, grm_odo = find_last_service(rows, "grm")
                    if grm_odo and cur_odo:
                        grm_rem = 50000 - (max(cur_odo, grm_odo) - grm_odo)
                        lines.append(f"{get_color_icon(grm_rem, 50000)} ГРМ: {format_km(grm_rem)} км до регламенту")
                today_d = datetime.now(KYIV_TZ).date()
                best = None
                if len(rows) > 3 and rows[3] and rows[3][0]:
                    d, company = parse_insurance_a4(rows[3][0])
                    if d: best = (d, company)
                if not best:
                    for row in rows[7:]:
                        if len(row) >= INSURANCE_COMPANY_COL:
                            d = parse_short_date(row[INSURANCE_DATE_COL - 1])
                            company = str(row[INSURANCE_COMPANY_COL - 1]).strip()
                            if d and company:
                                if best is None or d > best[0]: best = (d, company)
                if best:
                    days_left = (best[0] - today_d).days
                    lines.append(f"{insurance_days_icon(days_left)} Страховка: {best[0].strftime('%d.%m.%y')} ({best[1]})")
            lines.append("")
            lines.append(monthly_summary(car_id))
            await update.message.reply_text("\n".join(lines))
            return

        await update.message.reply_text("⏳ Обробляю...")

        heuristic_actions = heuristic_multi_parse(text)
        if heuristic_actions:
            if actions_need_odometer(heuristic_actions):
                context.user_data["pending_actions"] = heuristic_actions
                context.user_data["waiting_odometer_choice_actions"] = True
                await update.message.reply_text("❓ Немає одометра.\nМені додати середньостатистичний пробіг?\nНапиши «так» або просто надішли цифри одометра.")
                return

            spreadsheet = get_sheet()
            for action in heuristic_actions:
                ws = get_matching_worksheet(spreadsheet, action["car_id"])
                if ws and detect_duplicate(ws, action, raw_text=text):
                    context.user_data["waiting_duplicate_confirm"] = True
                    context.user_data["pending_actions_after_duplicate"] = heuristic_actions
                    await update.message.reply_text("❓ Це новий запис чи дубль попереднього?")
                    return

            result = write_actions_to_sheet(heuristic_actions, raw_text=text)
            await update.message.reply_text(result)
            return

        pending_data = context.user_data.get("pending_data")
        parsed = ask_ai(text, existing_data=pending_data)
        if "error" in parsed:
            await update.message.reply_text(f"❌ AI тимчасово недоступний.\n\nДеталь: {parsed['error']}")
            return

        parsed["car_id"] = resolve_car_id(parsed.get("car_id"))
        parsed["date"] = normalize_date_short(parsed.get("date"))
        parsed = apply_special_cases(parsed, text)
        parsed["missing_fields"] = compute_missing_fields(parsed, text)

        if "car_id" in parsed["missing_fields"]:
            context.user_data["pending_data"] = parsed
            await update.message.reply_text(f"❓ Не вдалося визначити машину.\nВкажи номер машини з цього списку:\n{', '.join(KNOWN_CAR_IDS)}")
            return

        if parsed["missing_fields"]:
            context.user_data["pending_data"] = parsed
            if "odometer" in parsed["missing_fields"]:
                context.user_data["waiting_odometer_choice"] = True
                await update.message.reply_text("❓ Немає одометра.\nМені додати середньостатистичний пробіг?\nНапиши «так» або просто надішли цифри одометра.")
                return

            await update.message.reply_text(f"❓ Не вистачає даних.\n{ask_for_next_missing_field(parsed['missing_fields'])}")
            return

        spreadsheet = get_sheet()
        ws = get_matching_worksheet(spreadsheet, parsed["car_id"])
        if ws and parsed.get("type") in ["expense", "income"] and parsed.get("odometer") not in (None, ""):
            if odometer_is_anomalous(ws, int(parsed["odometer"]), parsed.get("date")):
                context.user_data["waiting_odometer_anomaly_confirm"] = True
                context.user_data["pending_actions_after_anomaly"] = [parsed]
                await update.message.reply_text("⚠️ Пробіг виглядає нетипово великим. Підтвердити?")
                return

        if ws and detect_duplicate(ws, parsed, raw_text=text):
            context.user_data["waiting_duplicate_confirm"] = True
            context.user_data["pending_actions_after_duplicate"] = [parsed]
            await update.message.reply_text("❓ Це новий запис чи дубль попереднього?")
            return

        result = write_single_action_to_sheet(parsed, raw_text=text)
        context.user_data.pop("pending_data", None)
        await update.message.reply_text(result)

    except Exception as e:
        logger.exception("Error")
        await update.message.reply_text(f"❌ Помилка: {str(e)}")


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        f"👋 Привіт! Я бот автопарку.\n\n"
        f"Твій Telegram ID: `{user_id}`\n\n"
        f"Я знаю такі машини:\n{', '.join(KNOWN_CAR_IDS)}\n\n"
        f"Команди:\n"
        f"• масло\n"
        f"• грм\n"
        f"• страховка\n"
        f"• обновить курсы (одноразово: I та O)\n"
        f"• 8730 місяць\n"
        f"• ТО 4553\n"
        f"• 8730 приход 3800, долг 200 за дтп, штраф 300 за парковку\n",
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
    await update.message.reply_text("✅ Поточне введення скасовано.")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("cancel", handle_cancel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.job_queue.run_daily(
        check_service_and_insurance_notifications,
        time=time(9, 15, tzinfo=KYIV_TZ),
        days=(0, 1, 2, 3, 4),
        name="weekday_morning_regulations"
    )
    app.job_queue.run_daily(
        weekday_vehicle_completion_job,
        time=time(18, 30, tzinfo=KYIV_TZ),
        days=(1, 2, 3, 4),
        name="weekday_vehicle_completion_1830"
    )
    app.job_queue.run_daily(
        friday_reports_update_job,
        time=time(18, 30, tzinfo=KYIV_TZ),
        days=(5,),
        name="friday_fleet_reports_1830"
    )
    logger.info("Bot started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
