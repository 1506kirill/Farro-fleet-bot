import os
import re
import json
import logging
from datetime import datetime

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

    date_str = date_str.strip()

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


def build_prompt(message: str, existing_data: dict | None = None) -> str:
    today = datetime.now().strftime("%d.%m.%y")
    existing_block = ""

    if existing_data:
        existing_block = f"""
Уже известные данные из предыдущих сообщений:
{json.dumps(existing_data, ensure_ascii=False)}
"""

    return f"""Ты помощник для учета автопарка. Сегодня {today}.

Твоя задача: разобрать сообщение пользователя в СТРОГИЙ JSON для записи в Google Sheets.

{existing_block}

Правила:
1. Пользователь может писать данные в любом порядке: машина, сумма, одометр, описание, дата, тип операции.
2. Нужно понимать свободные формулировки.
3. Если дата не указана — используй сегодняшнюю дату в формате DD.MM.YY.
4. ДАННЫЕ ДЛЯ ТАБЛИЦЫ ПИШИ НА РУССКОМ ЯЗЫКЕ.
5. Ответ должен быть ТОЛЬКО JSON, без markdown, без пояснений, без текста до и после JSON.
6. Если не хватает важных данных — верни missing_fields.
7. Не выдумывай данные.
8. Для расхода и прихода description обязательно на русском языке.
9. category тоже должна быть на русском языке из списка ниже.
10. amount всегда в гривне, только число.
11. odometer только число или null.
12. car_id — номер машины, например 8730.

Распознавай тип операции по словам:
- income: приход, доход, пришло, заработок, оплата, выручка
- expense: расход, витрата, купил, ремонт, заправка, масло, колодки, запчасти, страховка, шины и т.д.

Сообщение пользователя:
"{message}"

Верни JSON строго такого вида:
{{
  "type": "expense" или "income" или null,
  "car_id": "8730" или null,
  "date": "DD.MM.YY",
  "amount": 370,
  "description": "Колодки Бош",
  "category": "Тормоза",
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
  "category": "Прочее",
  "odometer": null,
  "notes": null,
  "missing_fields": ["amount", "odometer", "description"]
}}

Список category:
"ТО|Тормоза|Подвеска/КПП|Двигатель|ГРМ/Охлаждение|Электрика|Шины|Кузов|Салон|GPS|ЗП|Страховка|Страховая выплата|Прочее"
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


def merge_data(old_data: dict, new_data: dict) -> dict:
    merged = dict(old_data)

    for key, value in new_data.items():
        if key == "missing_fields":
            continue
        if value not in (None, "", []):
            merged[key] = value

    merged["date"] = normalize_date_short(merged.get("date"))
    merged["missing_fields"] = compute_missing_fields(merged)
    return merged


def compute_missing_fields(data: dict) -> list[str]:
    missing = []

    if not data.get("type"):
        missing.append("type")
    if not data.get("car_id"):
        missing.append("car_id")
    if data.get("amount") in (None, ""):
        missing.append("amount")
    if not data.get("description"):
        missing.append("description")
    if data.get("odometer") in (None, ""):
        missing.append("odometer")

    return missing


def ask_for_next_missing_field(missing_fields: list[str]) -> str:
    if not missing_fields:
        return "Уточни, будь ласка, відсутні дані."

    field = missing_fields[0]

    mapping = {
        "type": "Вкажи, будь ласка, це прихід чи витрата.",
        "car_id": "Вкажи номер машини.",
        "amount": "Вкажи суму в гривнях.",
        "description": "Вкажи назву приходу або витрати.",
        "odometer": "Вкажи одометр.",
    }

    return mapping.get(field, "Уточни, будь ласка, відсутні дані.")


def get_usd_black_rate_dnipro() -> float | None:
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    resp = requests.get(MINFIN_URL, headers=headers, timeout=15)
    resp.raise_for_status()

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
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


def write_to_sheet(data: dict) -> str:
    spreadsheet = get_sheet()
    car_id = str(data.get("car_id", "")).strip()
    date_value = normalize_date_short(data.get("date"))
    amount = float(data.get("amount", 0) or 0)
    odometer = data.get("odometer", "")
    description = data.get("description", "")
    category = data.get("category", "Прочее")
    notes = data.get("notes", None)

    usd_rate = None
    usd_amount = ""
    usd_note = ""

    try:
        usd_rate = get_usd_black_rate_dnipro()
        if usd_rate:
            usd_amount = round(amount / usd_rate, 2)
            usd_note = f"\n💱 Курс USD: {usd_rate}"
    except Exception as e:
        logger.error(f"USD rate error: {e}")
        usd_note = "\n⚠️ Курс USD не вдалося отримати"

    sheet_name = None
    for ws in spreadsheet.worksheets():
        if car_id and car_id in ws.title:
            sheet_name = ws.title
            break

    if not sheet_name:
        return f"❌ Машину {car_id} не знайдено в таблиці"

    ws = spreadsheet.worksheet(sheet_name)

    if data["type"] == "expense":
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

        ws.update(
            f"E{next_row}:J{next_row}",
            [[
                date_value,      # E
                odometer,        # F
                description,     # G
                amount,          # H
                usd_amount,      # I
                category,        # J
            ]]
        )

        if notes:
            try:
                ws.update(f"K{next_row}", [[str(notes)]])
            except Exception:
                pass

        return (
            f"✅ Витрата внесена!\n"
            f"🚗 Машина: {car_id}\n"
            f"📋 {description}\n"
            f"💸 {amount} грн\n"
            f"📅 {date_value}\n"
            f"📊 Категорія: {category}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці E:J"
            f"{usd_note}"
        )

    elif data["type"] == "income":
        k_col_vals = ws.col_values(11)
        next_row = len(k_col_vals) + 1

        for i in range(7, len(k_col_vals)):
            if not k_col_vals[i]:
                next_row = i + 1
                break

        ws.update(
            f"K{next_row}:P{next_row}",
            [[
                date_value,      # K
                odometer,        # L
                amount,          # M
                usd_amount,      # N
                description,     # O
                category,        # P
            ]]
        )

        if notes:
            try:
                ws.update(f"Q{next_row}", [[str(notes)]])
            except Exception:
                pass

        return (
            f"✅ Дохід внесено!\n"
            f"🚗 Машина: {car_id}\n"
            f"📋 {description}\n"
            f"💰 {amount} грн\n"
            f"📅 {date_value}\n"
            f"📍 Одометр: {odometer}\n"
            f"📍 Внесено: лист '{sheet_name}', рядок {next_row}, стовпці K:P"
            f"{usd_note}"
        )

    return "❌ Невідомий тип операції"


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено")
        return

    text = (update.message.text or "").strip()
    logger.info(f"Incoming message from {user_id}: {text}")

    await update.message.reply_text("⏳ Обробляю...")

    try:
        pending_data = context.user_data.get("pending_data")

        if pending_data:
            parsed = ask_ai(text, existing_data=pending_data)
            if "error" in parsed:
                await update.message.reply_text(
                    f"❌ AI тимчасово недоступний.\n\nДеталь: {parsed['error']}"
                )
                return

            merged = merge_data(pending_data, parsed)
            parsed = merged
        else:
            parsed = ask_ai(text)

            if "error" in parsed:
                await update.message.reply_text(
                    f"❌ AI тимчасово недоступний.\n\nДеталь: {parsed['error']}"
                )
                return

            parsed["date"] = normalize_date_short(parsed.get("date"))
            parsed["missing_fields"] = compute_missing_fields(parsed)

        logger.info(f"Parsed result: {parsed}")

        missing_fields = parsed.get("missing_fields", [])
        if missing_fields:
            context.user_data["pending_data"] = parsed
            question = ask_for_next_missing_field(missing_fields)
            await update.message.reply_text(
                f"❓ Не вистачає даних.\n{question}"
            )
            return

        result = write_to_sheet(parsed)
        context.user_data.pop("pending_data", None)
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
        f"Можеш писати в довільному порядку, наприклад:\n"
        f"• 8730 колодки Бош 370 грн одометр 470420\n"
        f"• одометр 470420 машина 8730 витрата колодки 370 грн\n"
        f"• приход по машине 8730 1500 грн одометр 470420\n\n"
        f"Якщо не вистачить даних — я перепитаю.",
        parse_mode="Markdown",
    )


async def handle_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("pending_data", None)
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
