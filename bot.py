import os
import logging
import anthropic
import gspread
from google.oauth2.service_account import Credentials
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
import json
from datetime import datetime
from openai import OpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS   = os.environ.get("GOOGLE_CREDS")

ALLOWED_USERS_STR = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in ALLOWED_USERS_STR.split(",") if x.strip()]

# === AI clients ===
claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)

def build_prompt(message: str):
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

# === NEW: AI fallback ===
def ask_ai(message: str) -> dict:
    prompt = build_prompt(message)

    # --- Claude first ---
    try:
        response = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        text = text.replace("```json","").replace("```","").strip()
        return json.loads(text)

    except Exception as e:
        logger.error(f"Claude error: {e}")

        # --- fallback to OpenAI ---
        try:
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.choices[0].message.content.strip()
            text = text.replace("```json","").replace("```","").strip()
            return json.loads(text)

        except Exception as e2:
            logger.error(f"OpenAI error: {e2}")
            return {"error": "AI недоступний"}

def write_to_sheet(data: dict) -> str:
    spreadsheet = get_sheet()
    car_id = data.get("car_id","")

    sheet_name = None
    for ws in spreadsheet.worksheets():
        if car_id in ws.title:
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

        ws.update(f"E{next_row}:I{next_row}", [[
            data.get("date",""),
            data.get("odometer",""),
            data.get("description",""),
            data.get("amount",""),
            round(data.get("amount",0) / 41.75, 2)
        ]])

        return f"✅ Витрата внесена!\n🚗 {car_id}\n💸 {data.get('amount')} грн"

    elif data["type"] == "income":
        k_col_vals = ws.col_values(11)
        next_row = len(k_col_vals) + 1
        for i in range(7, len(k_col_vals)):
            if not k_col_vals[i]:
                next_row = i + 1
                break

        ws.update(f"K{next_row}:O{next_row}", [[
            data.get("date",""),
            data.get("odometer",""),
            data.get("amount",""),
            round(data.get("amount",0) / 41.75, 2),
            ""
        ]])

        return f"✅ Дохід внесено!\n🚗 {car_id}\n💰 {data.get('amount')} грн"

    return "❌ Невідомий тип"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено")
        return

    text = update.message.text
    await update.message.reply_text("⏳ Обробляю...")

    try:
        parsed = ask_ai(text)

        if "error" in parsed:
            await update.message.reply_text("❌ Не зміг розібрати повідомлення")
            return

        result = write_to_sheet(parsed)
        await update.message.reply_text(result)

    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text(f"❌ Помилка: {str(e)}")

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(f"👋 Твій ID: {user_id}")

def main():
    from telegram.ext import CommandHandler
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
