import os
import logging
import anthropic
import gspread
from google.oauth2.service_account import Credentials
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS   = os.environ.get("GOOGLE_CREDS")

ALLOWED_USERS_STR = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in ALLOWED_USERS_STR.split(",") if x.strip()]

def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)

def ask_claude(message: str) -> dict:
    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    today = datetime.now().strftime("%d.%m.%Y")
    prompt = f"""Ти помічник для обліку автопарку. Сьогодні {today}.

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

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    text = response.content[0].text.strip()
    # clean possible markdown
    text = text.replace("```json","").replace("```","").strip()
    return json.loads(text)

def write_to_sheet(data: dict) -> str:
    spreadsheet = get_sheet()
    car_id = data.get("car_id","")

    # Find sheet for this car
    sheet_name = None
    for ws in spreadsheet.worksheets():
        if car_id in ws.title:
            sheet_name = ws.title
            break

    if not sheet_name:
        return f"❌ Машину {car_id} не знайдено в таблиці"

    ws = spreadsheet.worksheet(sheet_name)

    if data["type"] == "expense":
        # Find expenses section — look for column headers
        all_vals = ws.get_all_values()
        exp_col = None
        exp_start_row = None
        for i, row in enumerate(all_vals):
            for j, cell in enumerate(row):
                if "Дата" in str(cell) and j == 4:  # column E
                    exp_col = j
                    exp_start_row = i + 2
                    break
            if exp_col is not None:
                break

        if exp_col is None:
            exp_start_row = 8

        # Find first empty row in expenses (col E)
        e_col_vals = ws.col_values(5)  # column E
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
        return (f"✅ Витрата внесена!\n"
                f"🚗 Машина: {car_id}\n"
                f"📋 {data.get('description','')}\n"
                f"💸 {data.get('amount','')} грн\n"
                f"📅 {data.get('date','')}\n"
                f"📊 Категорія: {data.get('category','')}")

    elif data["type"] == "income":
        k_col_vals = ws.col_values(11)  # column K
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
        return (f"✅ Дохід внесено!\n"
                f"🚗 Машина: {car_id}\n"
                f"💰 {data.get('amount','')} грн\n"
                f"📅 {data.get('date','')}\n"
                f"📍 Одометр: {data.get('odometer','')}")

    return "❌ Невідомий тип операції"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено")
        return

    text = update.message.text
    await update.message.reply_text("⏳ Обробляю...")

    try:
        parsed = ask_claude(text)

        if "error" in parsed:
            await update.message.reply_text(
                f"❓ Не зміг розібрати: {parsed['error']}\n\n"
                f"Спробуй так:\n"
                f"• «8730 колодки Бош 370 грн одометр 470420»\n"
                f"• «приход 4553 3800 одометр 269518»"
            )
            return

        result = write_to_sheet(parsed)
        await update.message.reply_text(result)

    except json.JSONDecodeError:
        await update.message.reply_text("❌ Помилка розбору відповіді. Спробуй ще раз.")
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text(f"❌ Помилка: {str(e)}")

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        f"👋 Привіт! Я бот автопарку.\n\n"
        f"Твій Telegram ID: `{user_id}`\n\n"
        f"Пиши мені записи ось так:\n"
        f"• «8730 колодки Бош 370 грн одометр 470420»\n"
        f"• «приход 4553 3800 одометр 269518»\n"
        f"• «5725 масло Mobil 680 грн ТО»\n\n"
        f"Я сам розберу і внесу в таблицю! 🚗",
        parse_mode="Markdown"
    )

def main():
    from telegram.ext import CommandHandler
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
