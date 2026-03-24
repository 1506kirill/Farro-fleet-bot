import os
import re
import json
import logging
from datetime import datetime, date, time as dt_time
from statistics import median
from zoneinfo import ZoneInfo
import time

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

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS = os.environ.get("GOOGLE_CREDS")

ALLOWED_USERS_STR = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in ALLOWED_USERS_STR.split(",") if x.strip()]

MINFIN_URL = "https://minfin.com.ua/currency/auction/usd/buy/dnepropetrovsk/"

FULL_PLATES = [
    "AI1457MM",
    "АЕ0418ОР",
    "АЕ2993РI",
    "AE7935PI",
    "КА3021ЕО",
    "КА9489ЕР",
    "АЕ7121ТА",
    "АЕ8204ТВ",
    "AE2548TB",
    "АЕ9245ТО",
    "AE0736PK",
    "AE4715TH",
    "АЕ6514ТС",
    "KA4895HE",
    "KA6843HB",
    "АЕ5308ТЕ",
    "BI1875HO",
    "KA0665IH",
    "KA0349HO",
    "BC9854PM",
    "АЕ8391ТМ",
    "AE4553XB",
    "KA8730IX",
    "AE5725OO",
    "СА6584КА",
    "AI3531PH",
]

TO_BUNDLE = [
    {"description": "Масло в двигатель", "amount": 780},
    {"description": "Воздушный фильтр WX WA9545", "amount": 270},
    {"description": "Газовые фильтра", "amount": 100},
    {"description": "Масляный фильтр BO 0451103318", "amount": 160},
    {"description": "Работы за ТО", "amount": 300},
]

SKIP_GRM = {"9245", "5308", "4715", "8204", "0736"}


def extract_digits(value: str) -> str:
    return "".join(re.findall(r"\d+", str(value or "")))


VEHICLE_MAP = {}
for plate in FULL_PLATES:
    digits = extract_digits(plate)
    if digits:
        VEHICLE_MAP[digits] = plate

KNOWN_CAR_IDS = sorted(VEHICLE_MAP.keys())

claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY) if CLAUDE_API_KEY else None
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# ================= GOOGLE =================

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
    full_plate = full_plate_from_short(car_id)
    for ws in spreadsheet.worksheets():
        title = str(ws.title)
        if car_id in title or full_plate in title:
            return ws
    return None


# ================= UTIL =================

def normalize_date_short(date_str: str | None) -> str:
    if not date_str:
        return datetime.now().strftime("%d.%m.%y")

    date_str = str(date_str).strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d-%m-%Y", "%d-%m-%y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%d.%m.%y")
        except ValueError:
            pass
    return datetime.now().strftime("%d.%m.%y")


def parse_short_date(date_str: str | None) -> date | None:
    if not date_str:
        return None
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            pass
    return None


def clean_json_text(text: str) -> str:
    if not text:
        return ""
    text = text.strip().replace("```json", "").replace("```", "").strip()
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start:end + 1]
    return text


def resolve_car_id(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip().upper()
    digits = extract_digits(raw)
    if digits in VEHICLE_MAP:
        return digits
    for short_id, full_plate in VEHICLE_MAP.items():
        if raw == str(full_plate).upper():
            return short_id
    return digits if digits in VEHICLE_MAP else None


def full_plate_from_short(car_id: str | None) -> str:
    if not car_id:
        return "Невідомо"
    return VEHICLE_MAP.get(str(car_id), str(car_id))


def is_to_phrase(text: str) -> bool:
    t = str(text or "").lower().strip()
    return (
        t == "то"
        or " то " in f" {t} "
        or "плановое то" in t
        or "планове то" in t
        or t.startswith("то ")
        or t.endswith(" то")
    )


def detect_liability_type(text: str) -> str | None:
    t = str(text or "").lower().strip()
    plus_markers = ["взял", "принял", "погасил", "дал "]
    minus_markers = ["штраф", "долг", "должен", "должна", "дожен"]
    if any(marker in t for marker in plus_markers):
        return "liability_plus"
    if any(marker in t for marker in minus_markers):
        return "liability_minus"
    return None


def blue_text_format():
    return CellFormat(textFormat=TextFormat(foregroundColor=Color(0, 0, 1)))


def yellow_fill_format():
    return CellFormat(backgroundColor=Color(1, 0.96, 0.75))


def apply_blue_text(ws, cell_range: str):
    format_cell_range(ws, cell_range, blue_text_format())


def mark_cell_yellow(ws, cell_range: str):
    format_cell_range(ws, cell_range, yellow_fill_format())


def parse_numeric_text(value) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    digits = re.sub(r"[^\d]", "", s)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def find_all_numbers(text: str) -> list[int]:
    return [int(x) for x in re.findall(r"\d+", str(text or ""))]


# ================= TABLE HELPERS =================

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
    last_income_row = get_last_used_row_for_block(ws, 11, 15, 8)
    last_liability_row = get_last_used_row_for_block(ws, 16, 17, 8)
    return max(last_income_row, last_liability_row) + 1


def get_previous_income_odometer(ws) -> int | None:
    all_vals = ws.get_all_values()
    odometers = []
    for row in all_vals[7:]:
        if len(row) > 11:
            value = parse_numeric_text(row[11])
            if value:
                odometers.append(value)
    return odometers[-1] if odometers else None


def get_current_odometer(ws):
    rows = ws.get_all_values()
    last_f = 0
    last_l = 0
    for r in rows[7:]:
        if len(r) > 5:
            v = parse_num(r[5])
            if v:
                last_f = v
        if len(r) > 11:
            v = parse_num(r[11])
            if v:
                last_l = v
    return max(last_f, last_l)


def parse_num(v):
    if not v:
        return None
    v = re.sub(r"[^\d]", "", str(v))
    return int(v) if v else None


def find_last(ws, keywords):
    rows = ws.get_all_values()
    for r in reversed(rows[7:]):
        if len(r) > 6:
            text = str(r[6]).lower()
            if any(k in text for k in keywords):
                return r[4], parse_num(r[5])
    return None, None


# ================= TO/GRM REPORTS =================



KYIV_TZ = ZoneInfo("Europe/Kyiv")
SERVICE_CACHE_TTL = 180
_service_snapshot_cache = {"ts": 0.0, "data": None}


def format_km_value(value: int | float) -> str:
    try:
        n = int(round(float(value)))
    except Exception:
        return str(value)
    s = f"{abs(n):,}".replace(",", ".")
    return f"-{s}" if n < 0 else s


def oil_status_icon(remaining: int | float) -> str:
    r = float(remaining)
    if r <= 1000:
        return "🔴"
    if r <= 3333:
        return "🟠"
    if r <= 6666:
        return "🟡"
    return "🟢"

def grm_status_icon(remaining: int | float) -> str:
    r = float(remaining)
    if r <= 1000:
        return "🔴"
    if r <= 16666:
        return "🟠"
    if r <= 33333:
        return "🟡"
    return "🟢"

def is_oil_report_request(text: str) -> bool:
    t = re.sub(r"\s+", " ", str(text or "").strip().lower())
    return t in {"масло", "замена масла", "заміна масла", "то", "плановое то", "планове то"}


def is_grm_report_request(text: str) -> bool:
    t = re.sub(r"\s+", " ", str(text or "").strip().lower())
    return t in {"грм", "замена грм", "заміна грм", "комплект грм"}



def normalize_service_text(s: str) -> str:
    s = str(s or "").lower().strip()
    s = s.replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    return s


def build_expense_blocks(rows):
    """
    Собираем блоки расходов по одинаковым дате (E) и одометру (F).
    Это покрывает пакетные записи ТО/ГРМ на несколько строк подряд.
    """
    blocks = []
    current = None

    for row in rows[7:]:
        if len(row) <= 6:
            continue

        row_date = str(row[4]).strip() if len(row) > 4 else ""
        row_odo = parse_num(row[5] if len(row) > 5 else None)
        desc = normalize_service_text(row[6] if len(row) > 6 else "")

        if not row_date or row_odo is None or not desc:
            continue

        key = (row_date, row_odo)
        if current and current["key"] == key:
            current["descs"].append(desc)
        else:
            if current:
                blocks.append(current)
            current = {
                "key": key,
                "date": row_date,
                "odo": row_odo,
                "descs": [desc],
            }

    if current:
        blocks.append(current)

    return blocks


def service_block_score(descs, service_type: str) -> int:
    text = " | ".join(descs)
    score = 0

    if service_type == "oil":
        # Сильные признаки замены масла
        if "масло в двигатель" in text:
            score += 10
        if "моторное масло" in text:
            score += 9
        if "замена масла" in text:
            score += 8
        if "масляный фильтр" in text:
            score += 6

        # Усиливаем типичный пакет ТО, но не используем само "ТО" как единственный ориентир
        if "масло в двигатель" in text and "масляный фильтр" in text:
            score += 5
        if "масло в двигатель" in text and ("работы за то" in text or "газовые фильтра" in text or "воздушный фильтр" in text):
            score += 2

        # Ложные срабатывания ослабляем
        if "масло кпп" in text or "масло в коробку" in text or "масло гур" in text or "масло редуктора" in text:
            score -= 6

    elif service_type == "grm":
        if "комплект грм" in text:
            score += 12
        if "замена грм" in text or "замана грм" in text:
            score += 10
        # Просто "грм" — более слабый сигнал
        if re.search(r"(^|[^а-я])грм([^а-я]|$)", text):
            score += 6
        if "ролик грм" in text or "ремень грм" in text or "помпа" in text:
            score += 3

    return score


def find_last_service_in_rows(rows, service_type: str):
    """
    Более гибкий поиск последней замены масла/ГРМ.
    Ищет по блокам расходов с одинаковыми датой и одометром,
    затем выбирает ПОСЛЕДНИЙ валидный блок с достаточным score.
    """
    blocks = build_expense_blocks(rows)

    min_score = 8 if service_type == "oil" else 9

    for block in reversed(blocks):
        score = service_block_score(block["descs"], service_type)
        if score >= min_score:
            return block["date"], block["odo"]

    # Фолбэк: если строгих блоков не нашли, берем последний блок со слабым, но явным сигналом
    fallback_min = 5 if service_type == "oil" else 6
    for block in reversed(blocks):
        score = service_block_score(block["descs"], service_type)
        if score >= fallback_min:
            return block["date"], block["odo"]

    return None, None


def get_current_odometer_from_rows(rows):
    # Берем последнее заполненное значение одометра в расходах (F)
    # и последнее заполненное значение одометра в приходах (L),
    # затем выбираем большее из этих двух, как и обсуждали.
    last_f = 0
    last_l = 0

    for r in reversed(rows[7:]):
        if not last_f and len(r) > 5:
            v = parse_num(r[5])
            if v is not None:
                last_f = v
        if not last_l and len(r) > 11:
            v = parse_num(r[11])
            if v is not None:
                last_l = v
        if last_f and last_l:
            break

    return max(last_f, last_l)

def get_service_snapshot(force: bool = False):
    now = time.time()
    if (
        not force
        and _service_snapshot_cache["data"] is not None
        and now - _service_snapshot_cache["ts"] < SERVICE_CACHE_TTL
    ):
        return _service_snapshot_cache["data"]

    spreadsheet = get_sheet()
    snapshot = {}
    for car in KNOWN_CAR_IDS:
        ws = get_matching_worksheet(spreadsheet, car)
        if not ws:
            continue
        snapshot[car] = {
            "title": ws.title,
            "rows": ws.get_all_values(),
        }

    _service_snapshot_cache["ts"] = now
    _service_snapshot_cache["data"] = snapshot
    return snapshot


def get_service_snapshot_resilient(force: bool = False):
    try:
        return get_service_snapshot(force=force)
    except Exception:
        if _service_snapshot_cache["data"] is not None:
            logger.warning("Using stale cached snapshot after Sheets read failure")
            return _service_snapshot_cache["data"]
        raise


def find_last_service(ws, service_type: str):
    return find_last_service_in_rows(ws.get_all_values(), service_type)


def build_oil_report():
    snapshot = get_service_snapshot_resilient(force=False)
    out = []
    for car in KNOWN_CAR_IDS:
        data = snapshot.get(car)
        if not data:
            continue
        rows = data["rows"]
        service_date, odo = find_last_service_in_rows(rows, "oil")
        if not odo:
            continue
        cur = get_current_odometer_from_rows(rows)
        remaining = 10000 - (cur - odo)
        icon = oil_status_icon(remaining)
        out.append(f"{icon} {car} | {service_date} | {odo} | {format_km_value(remaining)} км")
    return "\n".join(out) if out else "Немає даних по заміні масла."


def build_grm_report():
    snapshot = get_service_snapshot_resilient(force=False)
    out = []
    for car in KNOWN_CAR_IDS:
        if car in SKIP_GRM:
            continue
        data = snapshot.get(car)
        if not data:
            continue
        rows = data["rows"]
        service_date, odo = find_last_service_in_rows(rows, "grm")
        if not odo:
            continue
        cur = get_current_odometer_from_rows(rows)
        remaining = 50000 - (cur - odo)
        icon = grm_status_icon(remaining)
        out.append(f"{icon} {car} | {service_date} | {odo} | {format_km_value(remaining)} км")
    return "\n".join(out) if out else "Немає даних по заміні ГРМ."


async def check_notifications(context: ContextTypes.DEFAULT_TYPE):
    snapshot = get_service_snapshot_resilient(force=True)
    msgs = []
    for car in KNOWN_CAR_IDS:
        data = snapshot.get(car)
        if not data:
            continue
        rows = data["rows"]
        cur = get_current_odometer_from_rows(rows)

        _, odo = find_last_service_in_rows(rows, "oil")
        if odo:
            remaining = 10000 - (cur - odo)
            if remaining <= 1000:
                msgs.append(f"🚗 {car} — масло через {format_km_value(remaining)} км")

        if car not in SKIP_GRM:
            _, odo = find_last_service_in_rows(rows, "grm")
            if odo:
                remaining = 50000 - (cur - odo)
                if remaining <= 1000:
                    msgs.append(f"🚗 {car} — ГРМ через {format_km_value(remaining)} км")

    if msgs:
        text = "⚠️ Нагадування:\n\n" + "\n".join(msgs)
        for uid in ALLOWED_USERS:
            await context.bot.send_message(chat_id=uid, text=text)


def get_last_8_weekly_points(ws):
    all_vals = ws.get_all_values()
    points = []
    for row in all_vals[7:]:
        if len(row) > 11:
            d = parse_short_date(row[10] if len(row) > 10 else None)
            odo = parse_numeric_text(row[11] if len(row) > 11 else None)
            if d and odo is not None:
                points.append((d, odo))
    return points[-8:]


def estimate_odometer_for_car(car_id: str, target_date_str: str | None = None) -> int | None:
    spreadsheet = get_sheet()
    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return None
    points = get_last_8_weekly_points(ws)
    if not points:
        return None

    target_date = parse_short_date(target_date_str) or datetime.now().date()
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

    if len(points) >= 2:
        prev_date, prev_odo = points[-2]
        delta_days = max((last_date - prev_date).days, 1)
        delta_km = max(last_odo - prev_odo, 0)
        fallback_rate = delta_km / delta_days
        future_days = (target_date - last_date).days
        return int(round(last_odo + fallback_rate * future_days))

    return last_odo


def odometer_is_anomalous(ws, new_odometer: int, operation_date_str: str | None) -> bool:
    points = get_last_8_weekly_points(ws)
    if not points:
        return False
    last_date, last_odo = points[-1]
    target_date = parse_short_date(operation_date_str) or datetime.now().date()
    if new_odometer <= last_odo:
        return False
    delta_km = new_odometer - last_odo
    delta_days = max((target_date - last_date).days, 1)
    weekly_equivalent = delta_km * 7 / delta_days
    return weekly_equivalent > 2500


# ================= AI PARSING =================

def build_prompt(message: str, existing_data: dict | None = None) -> str:
    today = datetime.now().strftime("%d.%m.%y")
    existing_block = ""
    if existing_data:
        existing_block = f"\nУже известные данные из предыдущих сообщений:\n{json.dumps(existing_data, ensure_ascii=False)}\n"

    cars_block = "\n".join(f"{k} -> {VEHICLE_MAP[k]}" for k in KNOWN_CAR_IDS)

    return f"""Ты помощник для учета автопарка. Сегодня {today}.

Твоя задача: разобрать сообщение пользователя в СТРОГИЙ JSON для записи в Google Sheets.
{existing_block}
Известные машины автопарка:
{cars_block}

Правила:
1. Пользователь может писать данные в любом порядке: машина, сумма, одометр, описание, дата, тип операции.
2. Пользователь обычно пишет только цифры машины.
3. car_id в JSON должен быть только из списка известных машин.
4. Если дата не указана — используй сегодняшнюю дату в формате DD.MM.YY.
5. ДАННЫЕ ДЛЯ ТАБЛИЦЫ ПИШИ НА РУССКОМ ЯЗЫКЕ.
6. Ответ должен быть ТОЛЬКО JSON, без markdown, без пояснений.
7. Если не хватает важных данных — верни missing_fields.
8. Если пользователь пишет про штраф, долг, должен, дожен — type = liability_minus.
9. Если пользователь пишет взял, принял, погасил, дал — type = liability_plus.
10. Для liability_minus/liability_plus odometer не нужен.
11. Если пользователь пишет ТО или плановое ТО, description = ТО и amount может быть null.
12. Если в одном сообщении одна машина и две суммы при словах взял/принял/погасил/дал, большая сумма — income, меньшая — liability_plus.

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
}}

Сообщение пользователя:
"{message}"
"""


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
            {"role": "system", "content": "Возвращай только валидный JSON. Без пояснений. Без markdown. Без текста до и после JSON."},
            {"role": "user", "content": prompt},
        ],
    )
    text = response.choices[0].message.content.strip()
    return json.loads(clean_json_text(text))


def ask_ai(message: str, existing_data: dict | None = None) -> dict:
    prompt = build_prompt(message, existing_data=existing_data)
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
            return {"error": f"AI недоступний: {str(e)}"}
    return {"error": "Не задані CLAUDE_API_KEY і OPENAI_API_KEY"}


def apply_special_cases(data: dict, raw_text: str) -> dict:
    liability_type = detect_liability_type(raw_text)
    if liability_type and not data.get("type"):
        data["type"] = liability_type
    if is_to_phrase(raw_text):
        if not data.get("type"):
            data["type"] = "expense"
        if not data.get("description"):
            data["description"] = "ТО"
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


# ================= BUSINESS LOGIC =================

def build_liability_description(op_type: str, raw_text: str, ai_description: str | None) -> str:
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
        return (f"штраф {base}" if "штраф" in t else f"долг {base}").strip()
    return f"погашение долга {base}".strip()


def detect_month_summary_request(text: str) -> str | None:
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

    today = datetime.now()
    month = today.month
    year = today.year
    all_vals = ws.get_all_values()

    income_sum = 0.0
    expense_sum = 0.0
    liability_sum = 0.0

    for row in all_vals[7:]:
        if len(row) > 7:
            d = parse_short_date(row[4] if len(row) > 4 else None)
            num = parse_numeric_text(row[7] if len(row) > 7 else None)
            if d and d.month == month and d.year == year and num is not None:
                expense_sum += num

        if len(row) > 12:
            d = parse_short_date(row[10] if len(row) > 10 else None)
            num = parse_numeric_text(row[12] if len(row) > 12 else None)
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

    return (
        f"📊 За поточний місяць по {car_id}:\n"
        f"💰 Дохід: {int(income_sum) if income_sum.is_integer() else round(income_sum, 2)} грн\n"
        f"💸 Витрати: {int(expense_sum) if expense_sum.is_integer() else round(expense_sum, 2)} грн\n"
        f"📌 Залишок боргу: {int(liability_sum) if liability_sum.is_integer() else round(liability_sum, 2)} грн"
    )


def detect_duplicate(ws, action: dict, raw_text: str = "") -> bool:
    op_type = action.get("type")
    all_vals = ws.get_all_values()

    if op_type == "expense":
        for row in reversed(all_vals[7:]):
            if len(row) >= 9 and any(str(x).strip() for x in row[4:9]):
                last_date = str(row[4]).strip() if len(row) > 4 else ""
                last_odo = parse_numeric_text(row[5] if len(row) > 5 else None)
                last_desc = str(row[6]).strip().lower() if len(row) > 6 else ""
                last_amount = parse_numeric_text(row[7] if len(row) > 7 else None)
                return (
                    last_date == normalize_date_short(action.get("date"))
                    and last_odo == parse_numeric_text(action.get("odometer"))
                    and last_amount == parse_numeric_text(action.get("amount"))
                    and last_desc == str(action.get("description", "")).strip().lower()
                )
        return False

    if op_type == "income":
        for row in reversed(all_vals[7:]):
            if len(row) >= 15 and any(str(x).strip() for x in row[10:15]):
                last_date = str(row[10]).strip()
                last_odo = parse_numeric_text(row[11] if len(row) > 11 else None)
                last_amount = parse_numeric_text(row[12] if len(row) > 12 else None)
                return (
                    last_date == normalize_date_short(action.get("date"))
                    and last_odo == parse_numeric_text(action.get("odometer"))
                    and last_amount == parse_numeric_text(action.get("amount"))
                )
        return False

    if op_type in ["liability_minus", "liability_plus"]:
        for row in reversed(all_vals[7:]):
            if len(row) >= 17 and any(str(x).strip() for x in row[15:17]):
                last_date = str(row[10]).strip() if len(row) > 10 else ""
                last_amount = str(row[15]).strip() if len(row) > 15 else ""
                last_desc = str(row[16]).strip().lower() if len(row) > 16 else ""
                current_desc = build_liability_description(op_type, raw_text, action.get("description")).lower()
                current_amount = -abs(float(action.get("amount", 0))) if op_type == "liability_minus" else abs(float(action.get("amount", 0)))
                return (
                    last_date == normalize_date_short(action.get("date"))
                    and str(last_amount) == str(int(current_amount) if float(current_amount).is_integer() else current_amount)
                    and last_desc == current_desc
                )
        return False

    return False


def write_expense_rows(ws, date_value, odometer, items, usd_rate, odometer_estimated):
    start_row = get_next_expense_row(ws)
    rows = []
    for item in items:
        amount = float(item["amount"])
        usd_amount = round(amount / usd_rate, 2) if usd_rate else ""
        rows.append([date_value, odometer, item["description"], amount, usd_amount])

    end_row = start_row + len(rows) - 1
    update_range = f"E{start_row}:I{end_row}"
    ws.update(update_range, rows)
    apply_blue_text(ws, update_range)

    if odometer_estimated:
        for row_idx in range(start_row, end_row + 1):
            mark_cell_yellow(ws, f"F{row_idx}")

    total_amount = sum(float(x["amount"]) for x in items)
    return start_row, end_row, total_amount


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
            usd_note = f"\n💱 Курс USD: {usd_rate}"
    except Exception as e:
        logger.error(f"USD rate error: {e}")
        usd_note = "\n⚠️ Курс USD не вдалося отримати"

    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return f"❌ Машину {full_plate} не знайдено в таблиці"

    sheet_name = ws.title

    if op_type == "expense":
        desc_lower = str(description).lower().strip()
        is_to_bundle_case = desc_lower in ["то", "плановое то", "планове то"] or is_to_phrase(description)

        if is_to_bundle_case:
            start_row, end_row, total_amount = write_expense_rows(ws, date_value, odometer, TO_BUNDLE, usd_rate, odometer_estimated)
            return (
                f"✅ ТО внесено!\n"
                f"🚘 Машина: {full_plate}\n"
                f"🧾 Додано 5 рядків\n"
                f"💸 Загальна сума: {total_amount} грн\n"
                f"📅 {date_value}\n"
                f"📍 Внесено: лист '{sheet_name}', рядки {start_row}-{end_row}, стовпці E:I{usd_note}"
            )

        next_row = get_next_expense_row(ws)
        usd_amount = round(amount / usd_rate, 2) if usd_rate else ""
        update_range = f"E{next_row}:I{next_row}"
        ws.update(update_range, [[date_value, odometer, description, amount, usd_amount]])
        apply_blue_text(ws, update_range)
        if odometer_estimated:
            mark_cell_yellow(ws, f"F{next_row}")
        return (
            f"✅ Витрата внесена!\n"
            f"🚘 Машина: {full_plate}\n"
            f"📋 {description}\n"
            f"💸 {amount} грн\n"
            f"📅 {date_value}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці E:I{usd_note}"
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

        update_range = f"K{next_row}:O{next_row}"
        ws.update(update_range, [[date_value, odometer, amount, usd_amount, mileage_delta]])
        apply_blue_text(ws, update_range)
        if odometer_estimated:
            mark_cell_yellow(ws, f"L{next_row}")
        delta_text = f"\n📈 Різниця пробігу: {mileage_delta}" if mileage_delta != "" else ""
        return (
            f"✅ Дохід внесено!\n"
            f"🚘 Машина: {full_plate}\n"
            f"💰 {amount} грн\n"
            f"📅 {date_value}\n"
            f"📍 Одометр: {odometer}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці K:O{delta_text}{usd_note}"
        )

    if op_type in ["liability_minus", "liability_plus"]:
        next_row = get_next_right_block_row(ws)
        sign_amount = -abs(amount) if op_type == "liability_minus" else abs(amount)
        liability_desc = build_liability_description(op_type, raw_text, description)
        update_range = f"K{next_row}:Q{next_row}"
        ws.update(update_range, [[date_value, "", "", "", "", sign_amount, liability_desc]])
        apply_blue_text(ws, update_range)
        label = "Штраф/борг" if op_type == "liability_minus" else "Погашення/надходження"
        return (
            f"✅ {label} внесено!\n"
            f"🚘 Машина: {full_plate}\n"
            f"💵 {sign_amount} грн\n"
            f"📝 {liability_desc}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці P:Q"
        )

    return "❌ Невідомий тип операції"


def write_actions_to_sheet(actions: list[dict], raw_text: str = "") -> str:
    return "\n\n".join(write_single_action_to_sheet(action, raw_text=raw_text) for action in actions)


def is_yes_statistical(text: str) -> bool:
    return text.lower().strip() in ["так", "да", "yes", "ок", "окей", "ага"]


def is_yes_confirm(text: str) -> bool:
    return str(text).lower().strip() in ["так", "да", "yes", "новий", "новая", "новое"]


def is_no_confirm(text: str) -> bool:
    return str(text).lower().strip() in ["ні", "нет", "дубль", "скасувати", "отмена", "cancel"]


def actions_need_odometer(actions: list[dict]) -> bool:
    return any(action.get("type") in ["expense", "income"] and action.get("odometer") in (None, "") for action in actions)


def fill_odometer_for_actions(actions: list[dict], odometer_value: int, estimated: bool):
    for action in actions:
        if action.get("type") in ["expense", "income"] and action.get("odometer") in (None, ""):
            action["odometer"] = odometer_value
            action["odometer_estimated"] = estimated


def heuristic_multi_parse(text: str):
    t = str(text or "").strip()
    car_ids_in_text = [car_id for car_id in KNOWN_CAR_IDS if re.search(rf"(?<!\d){re.escape(car_id)}(?!\d)", t)]
    shared_car_id = car_ids_in_text[0] if car_ids_in_text else None
    if not shared_car_id:
        return None

    if "," in t:
        parts = [p.strip() for p in t.split(",") if p.strip()]
        actions = []
        for part in parts:
            part_actions = heuristic_multi_parse(part)
            if part_actions:
                for a in part_actions:
                    if not a.get("car_id"):
                        a["car_id"] = shared_car_id
                actions.extend(part_actions)
            else:
                numbers = find_all_numbers(part)
                amounts = [n for n in numbers if str(n) != shared_car_id and str(n) not in KNOWN_CAR_IDS]
                if "приход" in part.lower() and amounts:
                    actions.append({
                        "type": "income",
                        "car_id": shared_car_id,
                        "date": normalize_date_short(None),
                        "amount": max(amounts),
                        "description": "",
                        "odometer": None,
                        "notes": None,
                        "missing_fields": [],
                    })
                elif detect_liability_type(part) == "liability_minus" and amounts:
                    actions.append({
                        "type": "liability_minus",
                        "car_id": shared_car_id,
                        "date": normalize_date_short(None),
                        "amount": amounts[0],
                        "description": build_liability_description("liability_minus", part, None),
                        "odometer": None,
                        "notes": None,
                        "missing_fields": [],
                    })
                elif detect_liability_type(part) == "liability_plus" and amounts:
                    actions.append({
                        "type": "liability_plus",
                        "car_id": shared_car_id,
                        "date": normalize_date_short(None),
                        "amount": amounts[0],
                        "description": build_liability_description("liability_plus", part, None),
                        "odometer": None,
                        "notes": None,
                        "missing_fields": [],
                    })
        return actions if actions else None

    if is_to_phrase(t):
        return [{
            "type": "expense",
            "car_id": shared_car_id,
            "date": normalize_date_short(None),
            "amount": 0,
            "description": "ТО",
            "odometer": None,
            "notes": None,
            "missing_fields": [],
        }]

    liability_type = detect_liability_type(t)
    numbers = find_all_numbers(t)
    amounts = [n for n in numbers if str(n) != shared_car_id and str(n) not in KNOWN_CAR_IDS]

    if liability_type == "liability_minus" and amounts:
        return [{
            "type": "liability_minus",
            "car_id": shared_car_id,
            "date": normalize_date_short(None),
            "amount": amounts[0],
            "description": build_liability_description("liability_minus", t, None),
            "odometer": None,
            "notes": None,
            "missing_fields": [],
        }]

    if liability_type == "liability_plus" and len(amounts) == 1:
        return [{
            "type": "income",
            "car_id": shared_car_id,
            "date": normalize_date_short(None),
            "amount": amounts[0],
            "description": "",
            "odometer": None,
            "notes": None,
            "missing_fields": [],
        }]

    if liability_type == "liability_plus" and len(amounts) >= 2:
        sorted_amounts = sorted(amounts, reverse=True)
        main_amount = sorted_amounts[0]
        extra_amounts = sorted_amounts[1:]
        actions = [{
            "type": "income",
            "car_id": shared_car_id,
            "date": normalize_date_short(None),
            "amount": main_amount,
            "description": "",
            "odometer": None,
            "notes": None,
            "missing_fields": [],
        }]
        for extra in extra_amounts:
            actions.append({
                "type": "liability_plus",
                "car_id": shared_car_id,
                "date": normalize_date_short(None),
                "amount": extra,
                "description": build_liability_description("liability_plus", t, None),
                "odometer": None,
                "notes": None,
                "missing_fields": [],
            })
        return actions

    return None


# ================= TELEGRAM HANDLERS =================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено")
        return

    text = (update.message.text or "").strip()
    text_lower = text.lower()
    logger.info(f"Incoming message from {user_id}: {text}")

    try:
        # ТО / ГРМ reports without car
        if is_oil_report_request(text):
            await update.message.reply_text("🛢 Стан масла:\n\n" + build_oil_report())
            return

        if is_grm_report_request(text):
            await update.message.reply_text("⚙️ Стан ГРМ:\n\n" + build_grm_report())
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
            numeric_odo = parse_numeric_text(text)

            if numeric_odo:
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
            numeric_odo = parse_numeric_text(text)

            if numeric_odo:
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

        car_id_for_summary = detect_month_summary_request(text)
        if car_id_for_summary:
            await update.message.reply_text(monthly_summary(car_id_for_summary))
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
        if pending_data:
            parsed = ask_ai(text, existing_data=pending_data)
            if "error" in parsed:
                await update.message.reply_text(f"❌ AI тимчасово недоступний.\n\nДеталь: {parsed['error']}")
                return
        else:
            parsed = ask_ai(text)
            if "error" in parsed:
                await update.message.reply_text(f"❌ AI тимчасово недоступний.\n\nДеталь: {parsed['error']}")
                return

        parsed["car_id"] = resolve_car_id(parsed.get("car_id"))
        parsed["date"] = normalize_date_short(parsed.get("date"))
        parsed = apply_special_cases(parsed, text)
        parsed["missing_fields"] = compute_missing_fields(parsed, text)

        missing_fields = parsed.get("missing_fields", [])
        if "car_id" in missing_fields:
            context.user_data["pending_data"] = parsed
            await update.message.reply_text(f"❓ Не вдалося визначити машину.\nВкажи номер машини з цього списку:\n{', '.join(KNOWN_CAR_IDS)}")
            return

        if missing_fields:
            context.user_data["pending_data"] = parsed
            if "odometer" in missing_fields:
                context.user_data["waiting_odometer_choice"] = True
                await update.message.reply_text("❓ Немає одометра.\nМені додати середньостатистичний пробіг?\nНапиши «так» або просто надішли цифри одометра.")
                return
            await update.message.reply_text(f"❓ Не вистачає даних.\n{ask_for_next_missing_field(missing_fields)}")
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

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        await update.message.reply_text("❌ Помилка розбору відповіді від AI. Спробуй ще раз іншими словами.")
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text(f"❌ Помилка: {str(e)}")


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        f"👋 Привіт! Я бот автопарку.\n\n"
        f"Твій Telegram ID: `{user_id}`\n\n"
        f"Я знаю такі машини:\n"
        f"{', '.join(KNOWN_CAR_IDS)}\n\n"
        f"Приклади:\n"
        f"• 8730 місяць\n"
        f"• масло\n"
        f"• грм\n"
        f"• 8730 приход 3800, долг 200 за дтп, штраф 300 за парковку\n"
        f"• Штраф 200 за 8730\n"
        f"• ТО 4553\n\n"
        f"Якщо не вистачить одометра — я або перепитаю, або підставлю середньостатистичний.",
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
    logger.info("Bot started!")

    # Требует job-queue extras/apscheduler в окружении.
    if getattr(app, "job_queue", None) is not None:
        app.job_queue.run_daily(check_notifications, time=dt_time(hour=9, minute=15, tzinfo=KYIV_TZ), name="check_notifications_morning")
        app.job_queue.run_daily(check_notifications, time=dt_time(hour=16, minute=0, tzinfo=KYIV_TZ), name="check_notifications_evening")

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
