import os
import logging
import json
from datetime import datetime

import anthropic
import gspread
from openai import OpenAI
from google.oauth2.service_account import Credentials
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS = os.environ.get("GOOGLE_CREDS")

ALLOWED_USERS_STR = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in ALLOWED_USERS_STR.split(",") if x.strip()]

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


def build_prompt(message: str) -> str:
    today = datetime.now().strftime("%d.%m.%Y")
    return f"""Ти помічник для обліку автопарку. Сьогодні {today}.

Розбери це повідомлення і поверни JSON (тільки JSON, без пояснень):

Повідомлення: "{message}"

Поверни такий JSON:
{{
  "type": "expense" або "income",
  "car_id": "номер машини (наприклад 8730)",
  "date": "дата у форматі DD.MM.YYYY (якщо не вказана — сьогодні)",
  "amount": число (сума в гривнях, завжди позитивне),
  "description": "опис що сталось",
  "category": одна з: "ТО|Тормоза|Підвіска/КПП|Двигун|ГРМ/Охлаждення|Електрика|Шини|Кузов|Салон|GPS|ЗП|Страховка|Страхова виплата|Прочее",
  "odometer": число або null,
  "notes": "додаткові примітки або null"
}}

Якщо не можеш розібрати — поверни {{"error": "опис проблеми"}}"""


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


def ask_claude(prompt: str) -> dict:
    if not claude_client:
        raise Exception("CLAUDE_API_KEY not set")

    response = claude_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
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
                "content": "Ты возвращаешь только валидный JSON. Без пояснений, без markdown, без текста до и после JSON.",
            },
            {"role": "user", "content": prompt},
        ],
    )

    text = response.choices[0].message.content.strip()
    text = clean_json_text(text)
    return json.loads(text)


def ask_ai(message: str) -> dict:
    prompt = build_prompt(message)
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


def write_to_sheet(data: dict) -> str:
    spreadsheet = get_sheet()
    car_id = str(data.get("car_id", "")).strip()

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
        exp_col = None
        exp_start_row = None

        for i, row in enumerate(all_vals):
            for j, cell in enumerate(row):
                if "Дата" in str(cell) and j == 4:
                    exp_col = j
                    exp_start_row = i + 2
                    break
            if exp_col is not None:
                break

        if exp_col is None:
            exp_start_row = 8

        e_col_vals = ws.col_values(5)
        next_row = len(e_col_vals) + 1

        for i in range(exp_start_row - 1, len(e_col_vals)):
            if not e_col_vals[i]:
                next_row = i + 1
                break

        amount = float(data.get("amount", 0) or 0)

        ws.update(
            f"E{next_row}:I{next_row}",
            [[
                data.get("date", ""),
                data.get("odometer", ""),
                data.get("description", ""),
                amount,
                round(amount / 41.75, 2),
            ]],
        )

        return (
            f"✅ Витрата внесена!\n"
            f"🚗 Машина: {car_id}\n"
            f"📋 {data.get('description', '')}\n"
            f"💸 {amount} грн\n"
            f"📅 {data.get('date', '')}\n"
            f"📊 Категорія: {data.get('category', '')}"
        )

    elif data["type"] == "income":
        k_col_vals = ws.col_values(11)
        next_row = len(k_col_vals) + 1

        for i in range(7, len(k_col_vals)):
            if not k_col_vals[i]:
                next_row = i + 1
                break

        amount = float(data.get("amount", 0) or 0)

        ws.update(
            f"K{next_row}:O{next_row}",
            [[
                data.get("date", ""),
                data.get("odometer", ""),
                amount,
                round(amount / 41.75, 2),
                "",
            ]],
        )

        return (
            f"✅ Дохід внесено!\n"
            f"🚗 Машина: {car_id}\n"
            f"💰 {amount} грн\n"
            f"📅 {data.get('date', '')}\n"
            f"📍 Одометр: {data.get('odometer', '')}"
        )

    return "❌ Невідомий тип операції"


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено")
        return

    text = update.message.text
    logger.info(f"Incoming message from {user_id}: {text}")
    await update.message.reply_text("⏳ Обробляю...")

    try:
        parsed = ask_ai(text)
        logger.info(f"Parsed result: {parsed}")

        if "error" in parsed:
            await update.message.reply_text(
                f"❌ Не зміг розібрати повідомлення\n\n"
                f"Деталь: {parsed['error']}\n\n"
                f"Спробуй так:\n"
                f"• 8730 колодки Бош 370 грн одометр 470420\n"
                f"• приход 4553 3800 одометр 269518\n"
                f"• 5725 масло Mobil 680 грн ТО"
            )
            return

        result = write_to_sheet(parsed)
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
        f"Пиши мені записи ось так:\n"
        f"• 8730 колодки Бош 370 грн одометр 470420\n"
        f"• приход 4553 3800 одометр 269518\n"
        f"• 5725 масло Mobil 680 грн ТО\n\n"
        f"Я сам розберу і внесу в таблицю.",
        parse_mode="Markdown",
    )


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
