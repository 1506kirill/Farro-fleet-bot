import os
import re
import json
import logging
from datetime import datetime
from statistics import median

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
from gspread_formatting import format_cell_range, CellFormat, Color

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


def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDS)
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


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


def clean_json_text(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    text = text.replace("```json", "").replace("```", "").strip()

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


def build_known_cars_block() -> str:
    lines = []
    for short_id in KNOWN_CAR_IDS:
        lines.append(f"{short_id} -> {VEHICLE_MAP[short_id]}")
    return "\n".join(lines)


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


def build_prompt(message: str, existing_data: dict | None = None) -> str:
    today = datetime.now().strftime("%d.%m.%y")
    existing_block = ""

    if existing_data:
        existing_block = f"""
Уже известные данные из предыдущих сообщений:
{json.dumps(existing_data, ensure_ascii=False)}
"""

    cars_block = build_known_cars_block()

    return f"""Ты помощник для учета автопарка. Сегодня {today}.

Твоя задача: разобрать сообщение пользователя в СТРОГИЙ JSON для записи в Google Sheets.

{existing_block}

Известные машины автопарка:
{cars_block}

Правила:
1. Пользователь может писать данные в любом порядке: машина, сумма, одометр, описание, дата, тип операции.
2. Пользователь обычно пишет только цифры машины, например 4553 или 8730.
3. car_id в JSON должен быть только из списка известных машин.
4. Если распознал машину по цифрам, верни car_id только в виде цифр: "4553", "8730" и т.д.
5. Если дата не указана — используй сегодняшнюю дату в формате DD.MM.YY.
6. ДАННЫЕ ДЛЯ ТАБЛИЦЫ ПИШИ НА РУССКОМ ЯЗЫКЕ.
7. Ответ должен быть ТОЛЬКО JSON, без markdown, без пояснений, без текста до и после JSON.
8. Если не хватает важных данных — верни missing_fields.
9. Не выдумывай данные.
10. Для расхода и прихода description обязательно на русском языке.
11. amount всегда в гривне, только число.
12. odometer только число или null.
13. Если пользователь пишет "ТО" или "плановое ТО", description верни как "ТО".
14. Если пользователь пишет "ТО" или "плановое ТО", amount может быть null, потому что это пакет фиксированных позиций.
15. Приход может иметь описание, но оно не обязательно.

Распознавай тип операции по словам:
- income: приход, доход, пришло, заработок, оплата, выручка
- expense: расход, витрата, купил, ремонт, заправка, масло, колодки, запчасти, страховка, шины, ТО

Сообщение пользователя:
"{message}"

Верни JSON строго такого вида:
{{
  "type": "expense" или "income" или null,
  "car_id": "8730" или null,
  "date": "DD.MM.YY",
  "amount": 370,
  "description": "Колодки Бош",
  "odometer": 470420,
  "notes": null,
  "missing_fields": []
}}

Если данных не хватает, верни:
{{
  "type": "expense" или "income" или null,
  "car_id": "8730" или null,
  "date": "DD.MM.YY",
  "amount": null,
  "description": null,
  "odometer": null,
  "notes": null,
  "missing_fields": ["amount", "odometer", "description"]
}}
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
    text = clean_json_text(text)
    return json.loads(text)


def ask_openai(prompt: str) -> dict:
    if not openai_client:
        raise Exception("OPENAI_API_KEY not set")

    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    "Возвращай только валидный JSON. "
                    "Без пояснений. Без markdown. Без текста до и после JSON."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    )
    text = response.choices[0].message.content.strip()
    text = clean_json_text(text)
    return json.loads(text)


def ask_ai(message: str, existing_data: dict | None = None) -> dict:
    prompt = build_prompt(message, existing_data=existing_data)
    logger.info("ask_ai started")

    if claude_client:
        try:
            logger.info("Trying Claude first")
            return ask_claude(prompt)
        except Exception as e:
            logger.error(f"Claude error: {e}")

    if openai_client:
        try:
            logger.info("Switching to OpenAI fallback")
            return ask_openai(prompt)
        except Exception as e:
            logger.error(f"OpenAI error: {e}")
            return {"error": f"AI недоступний: {str(e)}"}

    return {"error": "Не задані CLAUDE_API_KEY і OPENAI_API_KEY"}


def apply_to_special_case(data: dict, raw_text: str) -> dict:
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

    if not data.get("type"):
        missing.append("type")
    if not data.get("car_id"):
        missing.append("car_id")

    to_case = is_to_phrase(raw_text) or str(data.get("description", "")).lower().strip() in [
        "то",
        "плановое то",
        "планове то",
    ]

    if not to_case and data.get("amount") in (None, ""):
        missing.append("amount")

    if data.get("type") == "expense" and not data.get("description"):
        missing.append("description")

    if data.get("odometer") in (None, ""):
        missing.append("odometer")

    return missing


def merge_data(old_data: dict, new_data: dict, raw_text: str = "") -> dict:
    merged = dict(old_data)

    for key, value in new_data.items():
        if key == "missing_fields":
            continue
        if value not in (None, "", []):
            merged[key] = value

    merged["car_id"] = resolve_car_id(merged.get("car_id"))
    merged["date"] = normalize_date_short(merged.get("date"))
    merged = apply_to_special_case(merged, raw_text)
    merged["missing_fields"] = compute_missing_fields(merged, raw_text)
    return merged


def ask_for_next_missing_field(missing_fields: list[str]) -> str:
    if not missing_fields:
        return "Уточни, будь ласка, відсутні дані."

    field = missing_fields[0]

    mapping = {
        "type": "Вкажи, будь ласка, це прихід чи витрата.",
        "car_id": f"Вкажи номер машини. Доступні: {', '.join(KNOWN_CAR_IDS)}",
        "amount": "Вкажи суму в гривнях.",
        "description": "Вкажи назву витрати.",
        "odometer": "Мені додати середньостатистичний пробіг? Напиши «так» або просто надішли цифри одометра.",
    }

    return mapping.get(field, "Уточни, будь ласка, відсутні дані.")


def get_usd_black_rate_dnipro() -> float | None:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(MINFIN_URL, headers=headers, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    patterns = [
        r"Средняя покупка\s*([0-9]+[.,][0-9]+)",
        r"Середня купівля\s*([0-9]+[.,][0-9]+)",
        r"Покупка\s*([0-9]+[.,][0-9]+)",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", "."))

    matches = re.findall(r"\b([0-9]{2}[.,][0-9]{2})\b", text)
    candidates = []
    for val in matches:
        num = float(val.replace(",", "."))
        if 35 <= num <= 50:
            candidates.append(num)

    if candidates:
        return candidates[0]

    return None


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


def get_matching_worksheet(spreadsheet, car_id: str):
    full_plate = full_plate_from_short(car_id)
    for ws in spreadsheet.worksheets():
        title = str(ws.title)
        if car_id in title or full_plate in title:
            return ws
    return None


def get_last_odometer_stats(ws) -> tuple[int | None, int | None]:
    all_vals = ws.get_all_values()
    odometers = []

    for row in all_vals[7:]:
        expense_odo = parse_numeric_text(row[5]) if len(row) > 5 else None
        income_odo = parse_numeric_text(row[11]) if len(row) > 11 else None

        if expense_odo:
            odometers.append(expense_odo)
        if income_odo:
            odometers.append(income_odo)

    if not odometers:
        return None, None

    last_odo = odometers[-1]

    deltas = []
    for i in range(1, len(odometers)):
        delta = odometers[i] - odometers[i - 1]
        if 0 < delta <= 5000:
            deltas.append(delta)

    if deltas:
        recent = deltas[-5:]
        typical_delta = int(round(median(recent)))
        estimated = last_odo + typical_delta
        return last_odo, estimated

    return last_odo, last_odo


def estimate_odometer_for_car(car_id: str) -> int | None:
    spreadsheet = get_sheet()
    ws = get_matching_worksheet(spreadsheet, car_id)
    if not ws:
        return None

    _, estimated = get_last_odometer_stats(ws)
    return estimated


def mark_cell_yellow(ws, cell_range: str):
    fmt = CellFormat(backgroundColor=Color(1, 0.96, 0.75))
    format_cell_range(ws, cell_range, fmt)


def get_next_expense_row(ws) -> int:
    all_vals = ws.get_all_values()
    exp_start_row = 8

    for i, row in enumerate(all_vals):
        for j, cell in enumerate(row):
            if "Дата" in str(cell) and j == 4:
                exp_start_row = i + 2
                break

    e_col_vals = ws.col_values(5)
    next_row = len(e_col_vals) + 1

    for i in range(exp_start_row - 1, len(e_col_vals)):
        if not e_col_vals[i]:
            next_row = i + 1
            break

    return next_row


def get_next_income_row(ws) -> int:
    k_col_vals = ws.col_values(11)
    next_row = len(k_col_vals) + 1

    for i in range(7, len(k_col_vals)):
        if not k_col_vals[i]:
            next_row = i + 1
            break

    return next_row


def get_previous_income_odometer(ws) -> int | None:
    all_vals = ws.get_all_values()
    odometers = []

    for row in all_vals[7:]:
        if len(row) > 11:
            value = parse_numeric_text(row[11])
            if value:
                odometers.append(value)

    if not odometers:
        return None

    return odometers[-1]


def write_expense_rows(ws, date_value, odometer, items, usd_rate, notes, odometer_estimated):
    start_row = get_next_expense_row(ws)
    rows = []
    current_row = start_row

    for item in items:
        amount = float(item["amount"])
        usd_amount = round(amount / usd_rate, 2) if usd_rate else ""
        note_value = notes if current_row == start_row else ""
        rows.append([
            date_value,               # E
            odometer,                 # F
            item["description"],      # G
            amount,                   # H
            usd_amount,               # I
            note_value,               # J
        ])
        current_row += 1

    end_row = start_row + len(rows) - 1
    ws.update(f"E{start_row}:J{end_row}", rows)

    if odometer_estimated:
        for row_idx in range(start_row, end_row + 1):
            try:
                mark_cell_yellow(ws, f"F{row_idx}")
            except Exception as e:
                logger.error(f"Yellow mark error: {e}")

    total_amount = sum(float(x["amount"]) for x in items)
    return start_row, end_row, total_amount


def write_to_sheet(data: dict) -> str:
    spreadsheet = get_sheet()
    car_id = str(data.get("car_id", "")).strip()
    full_plate = full_plate_from_short(car_id)

    date_value = normalize_date_short(data.get("date"))
    amount = float(data.get("amount", 0) or 0)
    odometer = data.get("odometer", "")
    description = data.get("description", "")
    notes = data.get("notes", None)
    odometer_estimated = bool(data.get("odometer_estimated", False))

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

    if data["type"] == "expense":
        desc_lower = str(description).lower().strip()
        is_to_bundle_case = desc_lower in ["то", "плановое то", "планове то"] or is_to_phrase(description)

        if is_to_bundle_case:
            start_row, end_row, total_amount = write_expense_rows(
                ws=ws,
                date_value=date_value,
                odometer=odometer,
                items=TO_BUNDLE,
                usd_rate=usd_rate,
                notes=notes,
                odometer_estimated=odometer_estimated,
            )
            return (
                f"✅ ТО внесено!\n"
                f"🚘 Машина: {full_plate}\n"
                f"🧾 Додано 5 рядків\n"
                f"💸 Загальна сума: {total_amount} грн\n"
                f"📅 {date_value}\n"
                f"📍 Внесено: лист '{sheet_name}', рядки {start_row}-{end_row}, стовпці E:J"
                f"{usd_note}"
            )

        next_row = get_next_expense_row(ws)
        usd_amount = round(amount / usd_rate, 2) if usd_rate else ""

        ws.update(
            f"E{next_row}:J{next_row}",
            [[
                date_value,
                odometer,
                description,
                amount,
                usd_amount,
                notes or "",
            ]]
        )

        if odometer_estimated:
            try:
                mark_cell_yellow(ws, f"F{next_row}")
            except Exception as e:
                logger.error(f"Yellow mark error: {e}")

        return (
            f"✅ Витрата внесена!\n"
            f"🚘 Машина: {full_plate}\n"
            f"📋 {description}\n"
            f"💸 {amount} грн\n"
            f"📅 {date_value}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці E:J"
            f"{usd_note}"
        )

    elif data["type"] == "income":
        next_row = get_next_income_row(ws)
        usd_amount = round(amount / usd_rate, 2) if usd_rate else ""
        prev_odo = get_previous_income_odometer(ws)
        mileage_delta = ""

        if prev_odo is not None and odometer not in ("", None):
            try:
                mileage_delta = int(odometer) - int(prev_odo)
            except Exception:
                mileage_delta = ""

        ws.update(
            f"K{next_row}:P{next_row}",
            [[
                date_value,
                odometer,
                amount,
                usd_amount,
                mileage_delta,
                notes or "",
            ]]
        )

        if odometer_estimated:
            try:
                mark_cell_yellow(ws, f"L{next_row}")
            except Exception as e:
                logger.error(f"Yellow mark error: {e}")

        delta_text = f"\n📈 Різниця пробігу: {mileage_delta}" if mileage_delta != "" else ""

        return (
            f"✅ Дохід внесено!\n"
            f"🚘 Машина: {full_plate}\n"
            f"💰 {amount} грн\n"
            f"📅 {date_value}\n"
            f"📍 Одометр: {odometer}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці K:P"
            f"{delta_text}"
            f"{usd_note}"
        )

    return "❌ Невідомий тип операції"


def is_yes_statistical(text: str) -> bool:
    t = text.lower().strip()
    return t in ["так", "да", "yes", "ок", "окей", "ага"]


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено")
        return

    text = (update.message.text or "").strip()
    logger.info(f"Incoming message from {user_id}: {text}")

    try:
        if context.user_data.get("waiting_odometer_choice"):
            pending_data = context.user_data.get("pending_data", {})

            numeric_odo = parse_numeric_text(text)
            if numeric_odo:
                pending_data["odometer"] = numeric_odo
                pending_data["odometer_estimated"] = False
                pending_data["missing_fields"] = compute_missing_fields(pending_data, text)

                context.user_data["pending_data"] = pending_data
                context.user_data.pop("waiting_odometer_choice", None)

                if pending_data["missing_fields"]:
                    await update.message.reply_text(
                        ask_for_next_missing_field(pending_data["missing_fields"])
                    )
                    return

                result = write_to_sheet(pending_data)
                context.user_data.pop("pending_data", None)
                await update.message.reply_text(result)
                return

            if is_yes_statistical(text):
                car_id = pending_data.get("car_id")
                if not car_id:
                    context.user_data.pop("waiting_odometer_choice", None)
                    await update.message.reply_text("Спочатку вкажи номер машини.")
                    return

                estimated = estimate_odometer_for_car(car_id)
                if not estimated:
                    context.user_data.pop("waiting_odometer_choice", None)
                    await update.message.reply_text(
                        "Не вдалося обчислити середньостатистичний пробіг. Надішли, будь ласка, цифри одометра."
                    )
                    return

                pending_data["odometer"] = estimated
                pending_data["odometer_estimated"] = True
                pending_data["notes"] = "Пробег проставлен автоматически"
                pending_data["missing_fields"] = compute_missing_fields(pending_data, text)

                context.user_data["pending_data"] = pending_data
                context.user_data.pop("waiting_odometer_choice", None)

                if pending_data["missing_fields"]:
                    await update.message.reply_text(
                        ask_for_next_missing_field(pending_data["missing_fields"])
                    )
                    return

                result = write_to_sheet(pending_data)
                context.user_data.pop("pending_data", None)
                await update.message.reply_text(result)
                return

            await update.message.reply_text(
                "Напиши «так», якщо мені додати середньостатистичний пробіг, або просто надішли цифри одометра."
            )
            return

        await update.message.reply_text("⏳ Обробляю...")

        pending_data = context.user_data.get("pending_data")

        if pending_data:
            parsed = ask_ai(text, existing_data=pending_data)
            if "error" in parsed:
                await update.message.reply_text(
                    f"❌ AI тимчасово недоступний.\n\nДеталь: {parsed['error']}"
                )
                return

            parsed = merge_data(pending_data, parsed, text)
        else:
            parsed = ask_ai(text)
            if "error" in parsed:
                await update.message.reply_text(
                    f"❌ AI тимчасово недоступний.\n\nДеталь: {parsed['error']}"
                )
                return

            parsed["car_id"] = resolve_car_id(parsed.get("car_id"))
            parsed["date"] = normalize_date_short(parsed.get("date"))
            parsed = apply_to_special_case(parsed, text)
            parsed["missing_fields"] = compute_missing_fields(parsed, text)

        logger.info(f"Parsed result: {parsed}")

        missing_fields = parsed.get("missing_fields", [])

        if "car_id" in missing_fields:
            context.user_data["pending_data"] = parsed
            await update.message.reply_text(
                f"❓ Не вдалося визначити машину.\nВкажи номер машини з цього списку:\n{', '.join(KNOWN_CAR_IDS)}"
            )
            return

        if missing_fields:
            context.user_data["pending_data"] = parsed

            if missing_fields[0] == "odometer":
                context.user_data["waiting_odometer_choice"] = True
                await update.message.reply_text(
                    "❓ Немає одометра.\nМені додати середньостатистичний пробіг?\nНапиши «так» або просто надішли цифри одометра."
                )
                return

            question = ask_for_next_missing_field(missing_fields)
            await update.message.reply_text(f"❓ Не вистачає даних.\n{question}")
            return

        result = write_to_sheet(parsed)
        context.user_data.pop("pending_data", None)
        context.user_data.pop("waiting_odometer_choice", None)
        await update.message.reply_text(result)

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        await update.message.reply_text(
            "❌ Помилка розбору відповіді від AI. Спробуй ще раз іншими словами."
        )
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
        f"Можеш писати у довільному порядку, наприклад:\n"
        f"• 8730 колодки Бош 370 грн одометр 470420\n"
        f"• 4553 приход 3800\n"
        f"• ТО 4553\n"
        f"• плановое ТО 8730\n\n"
        f"Якщо не вистачить одометра — я або перепитаю, або підставлю середньостатистичний.",
        parse_mode="Markdown",
    )


async def handle_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("pending_data", None)
    context.user_data.pop("waiting_odometer_choice", None)
    await update.message.reply_text("✅ Поточне введення скасовано.")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("cancel", handle_cancel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
