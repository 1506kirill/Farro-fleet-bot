import os
import re
import json
import logging
from datetime import datetime, date
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS = os.environ.get("GOOGLE_CREDS")

ALLOWED_USERS_STR = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in ALLOWED_USERS_STR.split(",") if x.strip()]

KNOWN_CAR_IDS = [
    "1457","0418","2993","7935","3021","9489","7121","8204",
    "2548","9245","0736","4715","6514","4895","6843","5308",
    "1875","0665","0349","9854","8391","4553","8730","5725",
    "6584","3531"
]

SKIP_GRM = ["9245","5308","4715","8204","0736"]

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

def get_ws(spreadsheet, car_id):
    for ws in spreadsheet.worksheets():
        if car_id in ws.title:
            return ws
    return None

def parse_num(v):
    if not v:
        return None
    v = re.sub(r"[^\d]", "", str(v))
    return int(v) if v else None

# ================= ОДОМЕТР =================

def get_current_odometer(ws):
    rows = ws.get_all_values()
    f = 0
    l = 0

    for r in rows[7:]:
        if len(r) > 5:
            v = parse_num(r[5])
            if v: f = v
        if len(r) > 11:
            v = parse_num(r[11])
            if v: l = v

    return max(f, l)

# ================= ПОИСК =================

def find_last(ws, keywords):
    rows = ws.get_all_values()

    for r in reversed(rows[7:]):
        if len(r) > 6:
            text = str(r[6]).lower()
            if any(k in text for k in keywords):
                return r[4], parse_num(r[5])

    return None, None

# ================= ОТЧЕТЫ =================

def build_oil_report():
    s = get_sheet()
    out = []

    for car in KNOWN_CAR_IDS:
        ws = get_ws(s, car)
        if not ws: continue

        date, odo = find_last(ws, ["масло","то"])
        if not odo: continue

        cur = get_current_odometer(ws)
        left = 10000 - (cur - odo)
        if left < 0: left = 0

        icon = "🟢" if left <= 2000 else "🟡" if left <= 5000 else "🔴"

        out.append(f"{icon} {car} | {date} | {odo} | {left} км")

    return "\n".join(out)


def build_grm_report():
    s = get_sheet()
    out = []

    for car in KNOWN_CAR_IDS:
        if car in SKIP_GRM:
            continue

        ws = get_ws(s, car)
        if not ws: continue

        date, odo = find_last(ws, ["грм"])
        if not odo: continue

        cur = get_current_odometer(ws)
        left = 50000 - (cur - odo)
        if left < 0: left = 0

        icon = "🟢" if left <= 5000 else "🟡" if left <= 15000 else "🔴"

        out.append(f"{icon} {car} | {date} | {odo} | {left} км")

    return "\n".join(out)

# ================= УВЕДОМЛЕНИЯ =================

async def check_notifications(context: ContextTypes.DEFAULT_TYPE):
    s = get_sheet()
    msgs = []

    for car in KNOWN_CAR_IDS:
        ws = get_ws(s, car)
        if not ws: continue

        cur = get_current_odometer(ws)

        _, odo = find_last(ws, ["масло","то"])
        if odo:
            left = 10000 - (cur - odo)
            if 0 < left <= 1000:
                msgs.append(f"🚗 {car} — масло через {left} км")

        if car not in SKIP_GRM:
            _, odo = find_last(ws, ["грм"])
            if odo:
                left = 50000 - (cur - odo)
                if 0 < left <= 1000:
                    msgs.append(f"🚗 {car} — ГРМ через {left} км")

    if msgs:
        text = "⚠️ Нагадування:\n\n" + "\n".join(msgs)

        for uid in ALLOWED_USERS:
            await context.bot.send_message(chat_id=uid, text=text)

# ================= TELEGRAM =================

async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено")
        return

    text = update.message.text.lower()

    # ---- НОВОЕ ----
    if text in ["масло","замена масла","то"]:
        await update.message.reply_text("🛢 Стан масла:\n\n" + build_oil_report())
        return

    if text in ["грм","замена грм","комплект грм"]:
        await update.message.reply_text("⚙️ Стан ГРМ:\n\n" + build_grm_report())
        return

    # ---- СТАРАЯ ЛОГИКА ----
    await update.message.reply_text("Запис прийнято (тут залишається твоя логіка з ІІ)")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Бот автопарку працює")

# ================= MAIN =================

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT, handle))

    # авто-проверка раз в день
    app.job_queue.run_repeating(check_notifications, interval=86400, first=10)

    app.run_polling()

if __name__ == "__main__":
    main()
