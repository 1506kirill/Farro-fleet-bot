"""
Бот автопарку v3
Виправлено: алгоритм пошуку поточного одометра, масла, ГРМ, страховки
Додано: команда по страховці конкретної машини
"""

import os, re, json, logging
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
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
from gspread_formatting import format_cell_range, CellFormat, Color, TextFormat

try:
    from googleapiclient.discovery import build as gdrive_build
except ImportError:
    gdrive_build = None
    logger.warning("google-api-python-client not installed — Drive file search disabled")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KYIV_TZ    = ZoneInfo("Europe/Kyiv")
MINFIN_URL = "https://minfin.com.ua/currency/auction/usd/buy/dnepropetrovsk/"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
GOOGLE_CREDS   = os.environ.get("GOOGLE_CREDS", "")
ALLOWED_USERS  = [int(x.strip()) for x in os.environ.get("ALLOWED_USERS","").split(",") if x.strip()]

# Папка Google Drive зі страховками
INSURANCE_DRIVE_FOLDER_ID = "1RPTf7BsuMU8Gfkhviajy-9_ug28hYGVy"
INSURANCE_DRIVE_FOLDER_URL = f"https://drive.google.com/drive/folders/{INSURANCE_DRIVE_FOLDER_ID}"

# ── 26 машин ──────────────────────────────────────────────────────────────────
FULL_PLATES = [
    "AI1457MM","АЕ0418ОР","АЕ2993РI","AE7935PI","КА3021ЕО","КА9489ЕР",
    "АЕ7121ТА","АЕ8204ТВ","AE2548TB","АЕ9245ТО","AE0736PK","AE4715TH",
    "АЕ6514ТС","KA4895HE","KA6843HB","АЕ5308ТЕ","BI1875HO","KA0665IH",
    "KA0349HO","BC9854PM","АЕ8391ТМ","AE4553XB","KA8730IX","AE5725OO",
    "СА6584КА","AI3531PH",
]
SKIP_GRM = {"9245","5308","4715","8204","0736"}

TO_BUNDLE = [
    {"description":"Масло в двигатель",             "amount":780},
    {"description":"Воздушный фильтр WX WA9545",    "amount":270},
    {"description":"Газовые фильтра",               "amount":100},
    {"description":"Масляный фильтр BO 0451103318", "amount":160},
    {"description":"Работы за ТО",                  "amount":300},
]

# Статичні контакти страхових в Дніпрі (оновлено з офіційних сайтів)
INSURANCE_OFFICES_DNIPRO = {
    "євроінс":  {
        "name":    "Євроінс Україна",
        "hotline": "0 800 501 513",
        "address": ["пл. Червона, 3, оф. 10-А", "вул. Миронова, 5"],
        "web":     "https://euroins.com.ua",
    },
    "уніка":    {
        "name":    "Уніка",
        "hotline": "0 800 500 225",
        "address": ["пр. Яворницького, 11"],
        "web":     "https://uniqa.ua",
    },
    "арсенал":  {
        "name":    "Арсенал Страхування",
        "hotline": "0 800 501 010",
        "address": ["пр. Яворницького, 8"],
        "web":     "https://arsenal-insurance.ua",
    },
    "оранта":   {
        "name":    "Оранта",
        "hotline": "0 800 500 090",
        "address": ["вул. Короленка, 2"],
        "web":     "https://oranta.ua",
    },
    "вусо":     {
        "name":    "ВУСО",
        "hotline": "0 800 330 005",
        "address": ["вул. Сичова, 6"],
        "web":     "https://vuso.ua",
    },
    "pzu":      {
        "name":    "PZU Україна",
        "hotline": "0 800 505 798",
        "address": ["пр. Слобожанський, 67"],
        "web":     "https://pzu.ua",
    },
    "уаск":     {
        "name":    "УАСК Аска",
        "hotline": "0 800 501 111",
        "address": ["вул. Шевченка, 4"],
        "web":     "https://aska.ua",
    },
    "альянс":   {
        "name":    "Аллянс",
        "hotline": "0 800 500 700",
        "address": ["пр. Яворницького, 41"],
        "web":     "https://allianz.ua",
    },
    "тас":      {
        "name":    "ТАС",
        "hotline": "0 800 503 580",
        "address": ["вул. Набережна Перемоги, 30"],
        "web":     "https://tas.ua",
    },
}

REPORT_CACHE: Dict[str,Any] = {"snap":None,"ts":None}
CACHE_TTL = 180
_USD_CACHE: Dict[str,Any]   = {"rate":None,"day":None}


# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════

def digs(v:str) -> str:
    return "".join(re.findall(r"\d+", str(v or "")))

VEHICLE_MAP   = {digs(p): p for p in FULL_PLATES if digs(p)}
KNOWN_CAR_IDS = sorted(VEHICLE_MAP.keys())

claude_client = anthropic.Anthropic(api_key=CLAUDE_API_KEY) if CLAUDE_API_KEY else None
openai_client = OpenAI(api_key=OPENAI_API_KEY)              if OPENAI_API_KEY else None

def _blue():   return CellFormat(textFormat=TextFormat(foregroundColor=Color(0.067,0.392,0.784)))
def _yellow(): return CellFormat(backgroundColor=Color(1,0.96,0.75))

def apply_blue(ws, r:str):
    try: format_cell_range(ws, r, _blue())
    except Exception as e: logger.error(f"blue: {e}")

def mark_yellow(ws, r:str):
    try: format_cell_range(ws, r, _yellow())
    except Exception as e: logger.error(f"yellow: {e}")

def parse_num(v) -> Optional[int]:
    s = re.sub(r"[^\d\-]","", str(v or "").strip().replace(" ",""))
    if not s or s=="-": return None
    try: return int(s)
    except: return None

def norm_date(s:Optional[str]) -> str:
    if not s: return datetime.now(KYIV_TZ).strftime("%d.%m.%y")
    for fmt in ("%d.%m.%Y","%d.%m.%y","%d-%m-%Y","%d-%m-%y"):
        try: return datetime.strptime(str(s).strip(),fmt).strftime("%d.%m.%y")
        except: pass
    return datetime.now(KYIV_TZ).strftime("%d.%m.%y")

def parse_date(s:Optional[str]) -> Optional[date]:
    if not s: return None
    for fmt in ("%d.%m.%Y","%d.%m.%y","%d-%m-%Y","%d-%m-%y"):
        try: return datetime.strptime(str(s).strip(),fmt).date()
        except: pass
    return None

def fmt_km(v:Optional[int]) -> str:
    if v is None: return "—"
    sign="-" if v<0 else ""
    return f"{sign}{abs(v):,}".replace(","," ")

def resolve_car(v:Optional[str]) -> Optional[str]:
    if not v: return None
    raw=str(v).strip().upper(); d=digs(raw)
    if d in VEHICLE_MAP: return d
    for k,pl in VEHICLE_MAP.items():
        if raw==pl.upper(): return k
    return None

def fp(car_id:Optional[str]) -> str:
    return VEHICLE_MAP.get(car_id or "", car_id or "Невідомо")

def clean_json(t:str) -> str:
    s=t.strip().replace("```json","").replace("```","").strip()
    a,b=s.find("{"),s.rfind("}")
    return s[a:b+1] if a!=-1 and b>a else s

def is_odo_value(n:int) -> bool:
    """Одометр — 6-значне число від 100000 до 999999"""
    return 100000 <= n <= 999999


# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ════════════════════════════════════════════════════════════

def open_sheet():
    d=json.loads(GOOGLE_CREDS)
    scopes=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds=Credentials.from_service_account_info(d,scopes=scopes)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

def find_ws(sp, car_id:str):
    p=VEHICLE_MAP.get(car_id,"")
    for ws in sp.worksheets():
        if car_id in ws.title or (p and p in ws.title): return ws
    return None

def get_snap(force=False) -> Dict[str,List[List[str]]]:
    global REPORT_CACHE
    now=datetime.now(KYIV_TZ)
    if not force and REPORT_CACHE["snap"] and REPORT_CACHE["ts"]:
        if (now-REPORT_CACHE["ts"]).total_seconds()<CACHE_TTL:
            return REPORT_CACHE["snap"]
    sp=open_sheet()
    snap={ws.title:ws.get_all_values() for ws in sp.worksheets()}
    REPORT_CACHE={"snap":snap,"ts":now}
    return snap

def last_filled_row(ws,c1:int,c2:int,start:int=8) -> int:
    all_v=ws.get_all_values(); last=start-1
    for ri in range(start,len(all_v)+1):
        row=all_v[ri-1]
        if any(str(c).strip() for c in row[c1-1:c2]): last=ri
    return last

def next_exp_row(ws)   -> int: return last_filled_row(ws,5,9,8)+1
def next_right_row(ws) -> int: return max(last_filled_row(ws,11,15,8),last_filled_row(ws,16,17,8))+1

def prev_inc_odo(ws) -> Optional[int]:
    all_v=ws.get_all_values(); odos=[]
    for row in all_v[7:]:
        v=parse_num(row[11] if len(row)>11 else None)
        if v and is_odo_value(v): odos.append(v)
    return odos[-1] if odos else None


# ════════════════════════════════════════════════════════════
# ПОТОЧНИЙ ОДОМЕТР — ВИПРАВЛЕНИЙ АЛГОРИТМ
# ════════════════════════════════════════════════════════════

def get_current_odo(rows: List[List[str]]) -> Optional[int]:
    """
    Повертає поточний одометр як MAX(останній_F, останній_L).
    ВАЖЛИВО: беремо ОСТАННІЙ непорожній запис в кожній колонці,
    а НЕ максимум по всій колонці (щоб не взяти старе більше значення).
    """
    last_f = None  # останній одометр в колонці F (витрати)
    last_l = None  # останній одометр в колонці L (прибутки)

    for row in rows[7:]:
        # Колонка F (індекс 5) — одометр при витраті
        # Перевіряємо що і дата (E, індекс 4) і одометр (F) є в рядку
        e = str(row[4]).strip() if len(row)>4 else ""
        f = parse_num(row[5] if len(row)>5 else None)
        if e and f and is_odo_value(f):
            last_f = f

        # Колонка L (індекс 11) — одометр при прибутку
        # Перевіряємо що і дата (K, індекс 10) є
        k = str(row[10]).strip() if len(row)>10 else ""
        l = parse_num(row[11] if len(row)>11 else None)
        if k and l and is_odo_value(l):
            last_l = l

    # Поточний = максимум з останнього F і останнього L
    candidates = [x for x in [last_f, last_l] if x is not None]
    if not candidates: return None
    return max(candidates)


# ════════════════════════════════════════════════════════════
# ПОШУК ОСТАННЬОЇ ЗАМІНИ МАСЛА — ВИПРАВЛЕНИЙ
# ════════════════════════════════════════════════════════════

def _is_oil_block(descs: List[str]) -> bool:
    """
    Блок = заміна масла якщо є ГОЛОВНИЙ тригер.
    Головні тригери: 'масло в двигатель', 'замена масла', 'моторное масло'
    ТО-блок: 'работа за то' + 'масляный фильтр' разом
    """
    joined = " ".join(d.lower() for d in descs)
    # Головні тригери
    for t in ["масло в двигатель","моторное масло","замена масла","масло в мотор"]:
        if t in joined: return True
    # ТО-блок
    if ("работа за то" in joined or "работы за то" in joined):
        if "масляный фильтр" in joined: return True
    return False


def _is_grm_block(descs: List[str]) -> bool:
    """Блок = заміна ГРМ якщо є явний ГРМ-тригер"""
    joined = " ".join(d.lower() for d in descs)
    for t in ["замена грм","комплект грм","ремень грм","набор грм",
              "грм с помпой","замена ремня грм","ролик грм"]:
        if t in joined: return True
    # Просто "грм" без контексту теж підходить
    if re.search(r"\bгрм\b", joined): return True
    return False


def _build_blocks(rows: List[List[str]]) -> List[Dict]:
    """
    Будує блоки ремонтів з рядків таблиці.
    Блок = всі рядки з однаковим одометром в колонці F.
    Ключ блоку — одометр при витраті (колонка F).
    """
    blocks: List[Dict] = []
    cur_date = cur_odo = None
    cur_descs: List[str] = []

    for row in rows[7:]:
        e = str(row[4]).strip() if len(row)>4 else ""
        f = parse_num(row[5] if len(row)>5 else None)
        g = str(row[6]).strip() if len(row)>6 else ""

        # Новий блок: є дата ТА одометр ТА одометр схожий на реальний
        if e and f and is_odo_value(f):
            # Якщо одометр змінився або це перший рядок — новий блок
            if cur_odo != f:
                if cur_date and cur_descs:
                    blocks.append({"date":cur_date,"odo":cur_odo,"descs":cur_descs[:]})
                cur_date = e; cur_odo = f; cur_descs = []
            # Якщо одометр той самий — просто оновлюємо дату (беремо останню)
            cur_date = e

        # Додаємо опис до поточного блоку
        if cur_date and cur_odo and g and len(g)>2:
            cur_descs.append(g)

    # Останній блок
    if cur_date and cur_descs:
        blocks.append({"date":cur_date,"odo":cur_odo,"descs":cur_descs[:]})

    return blocks


def find_last_oil(rows: List[List[str]]) -> Tuple[Optional[str], Optional[int]]:
    """
    Шукає ОСТАННІЙ блок з заміною масла.
    Повертає (дата, одометр_заміни).
    """
    blocks = _build_blocks(rows)
    # Шукаємо з кінця
    for blk in reversed(blocks):
        if _is_oil_block(blk["descs"]):
            return blk["date"], blk["odo"]
    return None, None


def find_last_grm(rows: List[List[str]]) -> Tuple[Optional[str], Optional[int]]:
    """Шукає ОСТАННІЙ блок з заміною ГРМ"""
    blocks = _build_blocks(rows)
    for blk in reversed(blocks):
        if _is_grm_block(blk["descs"]):
            return blk["date"], blk["odo"]
    return None, None


# ════════════════════════════════════════════════════════════
# ПОШУК СТРАХОВКИ — ВИПРАВЛЕНИЙ
# ════════════════════════════════════════════════════════════

def find_insurance(rows: List[List[str]]) -> Tuple[Optional[date], Optional[str]]:
    """
    Шукає страховку:
    1. Рядок A4 (rows[3][0]) — зазвичай "Страховка до DD.MM.YY Компанія"
    2. Остання знайдена строка в колонці G з ключовими словами страховки
    Повертає (дата_закінчення, назва_компанії).
    """
    results: List[Tuple[date,str]] = []

    # 1. A4 — рядок 3, колонка 0
    if len(rows)>3 and rows[3]:
        a4 = str(rows[3][0]).strip()
        if a4:
            r = _parse_ins_text(a4)
            if r: results.append(r)

    # 2. Колонка G (індекс 6) — шукаємо всі рядки зі страховкою, беремо останній
    ins_keywords = ["страховк","осаго","каско","евроинс","євроінс","уніка","уника","арсенал","вусо"]
    last_g_result = None
    for row in rows[7:]:
        g = str(row[6]).strip() if len(row)>6 else ""
        if not g: continue
        g_lo = g.lower()
        if any(k in g_lo for k in ins_keywords):
            r = _parse_ins_text(g)
            if r: last_g_result = r  # перезаписуємо — беремо ОСТАННІЙ

    if last_g_result:
        results.append(last_g_result)

    if not results: return None, None

    # Беремо запис з найпізнішою датою
    results.sort(key=lambda x: x[0], reverse=True)
    return results[0]


def _parse_ins_text(text: str) -> Optional[Tuple[date, str]]:
    """
    Розбирає текст типу:
    "Страховка до 29.04.26 Евроинс Каско Лайт и Осаго"
    "Страховка ЄвроІнс ОСАГО і Каско до 02.04.2026"
    "Страховка до 31.12.2026 Евроинс"
    Повертає (date_end, company_name) або None.
    """
    if not text or len(text)<5: return None

    # Шукаємо всі дати в тексті
    date_pattern = r"\b(\d{1,2}[.\-]\d{1,2}[.\-](?:\d{2}|\d{4}))\b"
    dates_found  = re.findall(date_pattern, text)

    best_date = None
    for ds in dates_found:
        d = parse_date(ds)
        # Страховка: дата закінчення > 2024 і не в минулому більш ніж на 2 роки
        if d and d.year >= 2024:
            if best_date is None or d > best_date:
                best_date = d

    if not best_date: return None

    # Назва компанії
    text_lo = text.lower()
    company = "—"

    company_list = [
        ("євроінс","Євроінс"),("евроинс","Євроінс"),("euroins","Євроінс"),
        ("уніка","Уніка"),("уника","Уніка"),("unica","Уніка"),
        ("арсенал","Арсенал"),("arsenal","Арсенал"),
        ("оранта","Оранта"),("вусо","ВУСО"),
        ("pzu","PZU"),("пзу","PZU"),
        ("allianz","Аллянс"),("альянс","Аллянс"),
        ("уаск","УАСК"),("аска","УАСК"),
        ("tas","ТАС"),("тас","ТАС"),
        ("гарант","Гарант"),
    ]
    for key, name in company_list:
        if key in text_lo:
            company = name; break

    return (best_date, company)


def get_insurance_office(company_name: str) -> Optional[Dict]:
    """Повертає контактні дані страхової компанії з Дніпра"""
    c_lo = company_name.lower()
    for key, info in INSURANCE_OFFICES_DNIPRO.items():
        if key in c_lo or c_lo in key:
            return info
    return None


def find_insurance_file_in_drive(car_id: str) -> Optional[str]:
    """
    Шукає файл страховки для машини в Google Drive папці.
    Повертає пряме посилання на файл або None.
    Файли названі за номером авто або держномером.
    """
    try:
        creds_dict = json.loads(GOOGLE_CREDS)
        scopes = [
            "https://www.googleapis.com/auth/drive.readonly",
            "https://spreadsheets.google.com/feeds",
        ]
        creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        from googleapiclient.discovery import build
        service = build("drive", "v3", credentials=creds)

        full_plate = VEHICLE_MAP.get(car_id, "")
        # Шукаємо файли у папці що містять номер авто
        search_terms = [car_id]
        if full_plate:
            # Різні варіанти: "8730", "KA8730IX", "КА8730ІХ"
            search_terms.append(full_plate)
            search_terms.append(full_plate.replace("І","I").replace("Х","X"))

        for term in search_terms:
            query = (f"'{INSURANCE_DRIVE_FOLDER_ID}' in parents "
                     f"and name contains '{term}' "
                     f"and trashed=false")
            results = service.files().list(
                q=query,
                fields="files(id,name,webViewLink,mimeType)",
                pageSize=5,
            ).execute()
            files = results.get("files", [])
            if files:
                # Повертаємо перший знайдений файл
                f = files[0]
                return f["webViewLink"], f["name"]

        return None, None
    except Exception as e:
        logger.error(f"Drive search error: {e}")
        return None, None


# ════════════════════════════════════════════════════════════
# USD КУРС
# ════════════════════════════════════════════════════════════

def get_usd() -> Optional[float]:
    today=datetime.now(KYIV_TZ).date()
    if _USD_CACHE["rate"] and _USD_CACHE["day"]==today: return _USD_CACHE["rate"]
    try:
        r=requests.get(MINFIN_URL,headers={"User-Agent":"Mozilla/5.0"},timeout=15)
        r.raise_for_status()
        tx=BeautifulSoup(r.text,"html.parser").get_text(" ",strip=True)
        for pat in [r"Средняя покупка\s*([0-9]+[.,][0-9]+)",
                    r"Середня купівля\s*([0-9]+[.,][0-9]+)",
                    r"Покупка\s*([0-9]+[.,][0-9]+)"]:
            m=re.search(pat,tx,re.IGNORECASE)
            if m:
                rate=float(m.group(1).replace(",",".")); _USD_CACHE.update({"rate":rate,"day":today}); return rate
        for val in re.findall(r"\b([0-9]{2}[.,][0-9]{2})\b",tx):
            n=float(val.replace(",","."))
            if 35<=n<=50: _USD_CACHE.update({"rate":n,"day":today}); return n
    except Exception as e: logger.error(f"USD: {e}")
    return None


# ════════════════════════════════════════════════════════════
# ОДОМЕТР — СТАТИСТИКА
# ════════════════════════════════════════════════════════════

def weekly_pts(ws) -> List[Tuple[date,int]]:
    pts=[]
    for row in ws.get_all_values()[7:]:
        d  =parse_date(row[10] if len(row)>10 else None)
        odo=parse_num(row[11]  if len(row)>11 else None)
        if d and odo and is_odo_value(odo): pts.append((d,odo))
    return pts[-8:]

def estimate_odo(car_id:str, date_str:Optional[str]=None) -> Optional[int]:
    try:
        ws=find_ws(open_sheet(),car_id)
        if not ws: return None
        pts=weekly_pts(ws)
        if not pts: return None
        target=parse_date(date_str) or datetime.now(KYIV_TZ).date()
        ld,lo=pts[-1]
        if target<=ld: return lo
        rates=[]
        for i in range(1,len(pts)):
            pd_,po=pts[i-1]; cd_,co_=pts[i]
            dd=(cd_-pd_).days; dk=co_-po
            if dd>0 and 0<=dk<=7000:
                r=dk/dd
                if 0<=r<=300: rates.append(r)
        if rates: return int(round(lo+median(rates)*(target-ld).days))
        return lo
    except Exception as e: logger.error(f"estimate_odo: {e}"); return None

def odo_anomaly(ws, new_odo:int, date_str:Optional[str]) -> bool:
    pts=weekly_pts(ws)
    if not pts: return False
    ld,lo=pts[-1]
    td=parse_date(date_str) or datetime.now(KYIV_TZ).date()
    if new_odo<=lo: return False
    days=max((td-ld).days,1)
    return (new_odo-lo)*7/days>2500


# ════════════════════════════════════════════════════════════
# РОЗПІЗНАВАННЯ ОДОМЕТРА В ТЕКСТІ
# ════════════════════════════════════════════════════════════

def extract_odo_from_text(text:str, car_id:Optional[str]=None) -> Optional[int]:
    """6-значне число 100000-999999, яке не є номером авто"""
    nums=re.findall(r"\b(\d{5,7})\b",text)
    for n_str in nums:
        n=int(n_str)
        if n_str in VEHICLE_MAP: continue
        if n_str.lstrip("0") in VEHICLE_MAP: continue
        if is_odo_value(n): return n
    return None


# ════════════════════════════════════════════════════════════
# ВИЗНАЧЕННЯ ТИПУ ОПЕРАЦІЇ
# ════════════════════════════════════════════════════════════

def is_to(t:str) -> bool:
    lo=str(t or "").lower().strip()
    return lo in {"то","плановое то","планове то"} or bool(re.search(r"\bто\b",lo))

def liab_type(t:str) -> Optional[str]:
    lo=str(t or "").lower()
    if any(k in lo for k in ["взяв","взял","принял","погасил","погасив","дав ","дал "]): return "liability_plus"
    if any(k in lo for k in ["штраф","долг","борг","должен","должна","дожен","боргує"]): return "liability_minus"
    return None

def is_income_phrase(t:str) -> bool:
    lo=str(t or "").lower()
    return any(k in lo for k in ["приход","прибуток","прийом","оплата","оренда","аренда","взяв","взял","принял"])

def liab_desc(op:str,raw:str,ai:Optional[str]) -> str:
    lo=str(raw or "").lower(); d=str(ai or "").strip()
    if   "дтп"      in lo: base="за ДТП"
    elif "телевиз"  in lo: base="за телевизор"
    elif "парков"   in lo: base="за парковку"
    elif "превыш"   in lo: base="за превышение"
    elif d: base=d if d.lower().startswith("за ") else f"за {d}"
    else:   base=""
    today=datetime.now(KYIV_TZ).strftime("%d.%m.%y")
    if op=="liability_minus":
        pref="штраф" if "штраф" in lo else "долг"
        return f"{today} {pref} {base}".strip()
    return f"{today} погашение долга {base}".strip()


# ════════════════════════════════════════════════════════════
# AI
# ════════════════════════════════════════════════════════════

def build_prompt(msg:str,ex:Optional[dict]=None)->str:
    today=datetime.now(KYIV_TZ).strftime("%d.%m.%y")
    ex_blk=f"\nВідомі дані:\n{json.dumps(ex,ensure_ascii=False)}\n" if ex else ""
    cars="\n".join(f"{k} -> {VEHICLE_MAP[k]}" for k in KNOWN_CAR_IDS)
    return f"""Ти помічник обліку автопарку. Сьогодні {today}.
{ex_blk}
Відомі машини:
{cars}

ПРАВИЛА РОЗПІЗНАВАННЯ ЧИСЕЛ:
1. 6-значне число (100000-999999) = ОДОМЕТР, не сума.
2. 4-значне з переліку машин = НОМЕР АВТО.
3. 3800-4200 без одометра = оплата оренди (income).
4. "Взяв/принял X за CAR" = завжди income, X=сума, CAR=авто.
5. НЕ створюй liability_plus при "взяв/принял" + сума + машина.
6. Якщо одометр вже є в тексті — НЕ питай повторно.
7. Дата DD.MM.YY, якщо немає — сьогодні.
8. ДАНІ ДЛЯ ТАБЛИЦІ — РОСІЙСЬКОЮ.
9. "ТО" → description="ТО".
10. штраф/долг/должен → liability_minus.
11. Для liability_* одометр не потрібен.
12. Тільки JSON.

Повідомлення: "{msg}"

JSON:
{{
  "type": "expense"|"income"|"liability_minus"|"liability_plus"|null,
  "car_id": "4553"|null,
  "date": "DD.MM.YY",
  "amount": 3800,
  "description": "",
  "odometer": 354746,
  "notes": null,
  "missing_fields": []
}}"""

def call_claude(p:str)->dict:
    if not claude_client: raise RuntimeError("CLAUDE_API_KEY не встановлено")
    r=claude_client.messages.create(model="claude-sonnet-4-20250514",max_tokens=700,
                                     messages=[{"role":"user","content":p}])
    return json.loads(clean_json(r.content[0].text))

def call_openai(p:str)->dict:
    if not openai_client: raise RuntimeError("OPENAI_API_KEY не встановлено")
    r=openai_client.chat.completions.create(model="gpt-4o-mini",temperature=0,
        messages=[{"role":"system","content":"Тільки валідний JSON."},{"role":"user","content":p}])
    return json.loads(clean_json(r.choices[0].message.content))

def ask_ai(msg:str,ex:Optional[dict]=None)->dict:
    p=build_prompt(msg,ex)
    if claude_client:
        try: return call_claude(p)
        except Exception as e: logger.error(f"Claude: {e}")
    if openai_client:
        try: return call_openai(p)
        except Exception as e: logger.error(f"OpenAI: {e}"); return {"error":str(e)}
    return {"error":"AI недоступний"}


# ════════════════════════════════════════════════════════════
# ДУБЛІ
# ════════════════════════════════════════════════════════════

def is_dup(ws,action:dict,raw:str="")->bool:
    all_v=ws.get_all_values(); t=action.get("type")
    if t=="expense":
        for row in reversed(all_v[7:]):
            if len(row)>=9 and any(str(x).strip() for x in row[4:9]):
                return (str(row[4]).strip()==norm_date(action.get("date")) and
                        parse_num(row[7] if len(row)>7 else None)==parse_num(action.get("amount")) and
                        str(row[6]).strip().lower()==str(action.get("description","")).lower())
    if t=="income":
        for row in reversed(all_v[7:]):
            if len(row)>=15 and any(str(x).strip() for x in row[10:15]):
                return (str(row[10]).strip()==norm_date(action.get("date")) and
                        parse_num(row[12] if len(row)>12 else None)==parse_num(action.get("amount")))
    return False


# ════════════════════════════════════════════════════════════
# ЗАПИС У ТАБЛИЦЮ
# ════════════════════════════════════════════════════════════

def write_one(data:dict,raw:str="")->str:
    sp=open_sheet()
    car_id=str(data.get("car_id","")).strip(); plate=fp(car_id)
    date_val=norm_date(data.get("date"))
    amount=float(data.get("amount",0) or 0)
    odo=data.get("odometer",""); desc=data.get("description","")
    odo_est=bool(data.get("odometer_estimated",False))
    op_type=data.get("type")

    usd_rate=get_usd()
    usd_note=f"\n💱 Курс: {usd_rate} грн/$" if usd_rate else "\n⚠️ Курс USD недоступний"

    ws=find_ws(sp,car_id)
    if not ws: return f"❌ Машину {plate} не знайдено в таблиці"
    sh=ws.title

    if op_type=="expense":
        if is_to(str(desc)) or str(desc).lower().strip() in {"то","плановое то","планове то"}:
            rs=next_exp_row(ws); rows_=[]
            for item in TO_BUNDLE:
                u=round(item["amount"]/usd_rate,2) if usd_rate else ""
                rows_.append([date_val,odo,item["description"],item["amount"],u])
            re_=rs+len(rows_)-1; rng=f"E{rs}:I{re_}"
            ws.update(rng,rows_); apply_blue(ws,rng)
            if odo_est:
                for r in range(rs,re_+1): mark_yellow(ws,f"F{r}")
            total=sum(i["amount"] for i in TO_BUNDLE)
            return (f"✅ ТО внесено!\n🚘 {plate}\n🧾 5 рядків\n💸 Разом: {total} грн\n"
                    f"📅 {date_val}\n📍 «{sh}», рядки {rs}–{re_}, E:I{usd_note}")
        r=next_exp_row(ws)
        usd=round(amount/usd_rate,2) if usd_rate else ""
        rng=f"E{r}:I{r}"
        ws.update(rng,[[date_val,odo,desc,amount,usd]]); apply_blue(ws,rng)
        if odo_est: mark_yellow(ws,f"F{r}")
        return (f"✅ Витрата внесена!\n🚘 {plate}\n📋 {desc}\n💸 {amount} грн\n"
                f"📅 {date_val}\n📍 «{sh}», рядок {r}, E:I{usd_note}")

    if op_type=="income":
        r=next_right_row(ws)
        usd=round(amount/usd_rate,2) if usd_rate else ""
        prev=prev_inc_odo(ws)
        delta=""
        if prev is not None and odo not in ("",None):
            try: delta=int(odo)-int(prev)
            except: pass
        rng=f"K{r}:O{r}"
        ws.update(rng,[[date_val,odo,amount,usd,delta]]); apply_blue(ws,rng)
        if odo_est: mark_yellow(ws,f"L{r}")
        dt=f"\n📈 Різниця пробігу: {delta} км" if delta!="" else ""
        return (f"✅ Прибуток внесено!\n🚘 {plate}\n💰 {amount} грн (${usd})\n"
                f"📅 {date_val}\n📍 Одометр: {odo}\n📍 «{sh}», рядок {r}, K:O{dt}{usd_note}")

    if op_type in ("liability_minus","liability_plus"):
        r=next_right_row(ws)
        sign=-abs(amount) if op_type=="liability_minus" else abs(amount)
        ld=liab_desc(op_type,raw,desc)
        ws.update(f"K{r}",[[date_val]]); apply_blue(ws,f"K{r}")
        ws.update(f"P{r}:Q{r}",[[sign,ld]]); apply_blue(ws,f"P{r}:Q{r}")
        label="Штраф/борг" if op_type=="liability_minus" else "Погашення"
        return (f"✅ {label} внесено!\n🚘 {plate}\n💵 {sign} грн\n📝 {ld}\n"
                f"📍 «{sh}», рядок {r}, P:Q")

    return "❌ Невідомий тип операції"


def write_all(actions:List[dict],raw:str="")->str:
    return "\n\n".join(write_one(a,raw) for a in actions)


# ════════════════════════════════════════════════════════════
# ЕВРИСТИКА
# ════════════════════════════════════════════════════════════

def find_car_in_text(text:str)->Optional[str]:
    for cid in sorted(KNOWN_CAR_IDS,key=len,reverse=True):
        if re.search(rf"(?<!\d){re.escape(cid)}(?!\d)",text): return cid
    return None

def heur(text:str)->Optional[List[dict]]:
    t=str(text or "").strip()
    car_id=find_car_in_text(t)
    if not car_id: return None
    today=norm_date(None)
    odo=extract_odo_from_text(t,car_id)
    all_nums=[int(x) for x in re.findall(r"\b\d+\b",t)]
    amounts=[n for n in all_nums if str(n)!=car_id and str(n) not in VEHICLE_MAP
             and (odo is None or n!=odo) and 100<=n<=99999]
    lt=liab_type(t); is_inc=is_income_phrase(t)

    if is_to(t):
        return [{"type":"expense","car_id":car_id,"date":today,"amount":0,
                 "description":"ТО","odometer":odo,"notes":None,"missing_fields":[]}]

    m=re.search(r"(?:взяв|взял|принял|прийняв)\s+(\d+)\s+(?:грн\s+)?(?:за\s+)?"
                rf"(?:{re.escape(car_id)})",t,re.IGNORECASE)
    if m or (is_inc and amounts and not (lt=="liability_minus")):
        amount=int(m.group(1)) if m else (max(amounts) if amounts else 0)
        return [{"type":"income","car_id":car_id,"date":today,"amount":amount,
                 "description":"","odometer":odo,"notes":None,"missing_fields":[]}]

    if lt=="liability_minus" and amounts:
        return [{"type":"liability_minus","car_id":car_id,"date":today,
                 "amount":amounts[0],"description":liab_desc("liability_minus",t,None),
                 "odometer":None,"notes":None,"missing_fields":[]}]

    if lt=="liability_plus" and len(amounts)==1:
        if odo and amounts[0]<10000:
            return [{"type":"income","car_id":car_id,"date":today,"amount":amounts[0],
                     "description":"","odometer":odo,"notes":None,"missing_fields":[]}]
        return [{"type":"liability_plus","car_id":car_id,"date":today,
                 "amount":amounts[0],"description":liab_desc("liability_plus",t,None),
                 "odometer":None,"notes":None,"missing_fields":[]}]

    if lt=="liability_plus" and len(amounts)>=2:
        sa=sorted(amounts,reverse=True)
        acts=[{"type":"income","car_id":car_id,"date":today,"amount":sa[0],
               "description":"","odometer":odo,"notes":None,"missing_fields":[]}]
        for x in sa[1:]:
            acts.append({"type":"liability_plus","car_id":car_id,"date":today,"amount":x,
                         "description":liab_desc("liability_plus",t,None),
                         "odometer":None,"notes":None,"missing_fields":[]})
        return acts

    if "," in t:
        parts=[p.strip() for p in t.split(",") if p.strip()]; acts=[]
        for part in parts:
            lo=part.lower()
            pn=[int(x) for x in re.findall(r"\b\d+\b",part)]
            pa=[n for n in pn if str(n)!=car_id and str(n) not in VEHICLE_MAP
                and (odo is None or n!=odo) and 100<=n<=99999]
            pl=liab_type(lo)
            if ("приход" in lo or "прибуток" in lo or "прийом" in lo) and pa:
                acts.append({"type":"income","car_id":car_id,"date":today,"amount":max(pa),
                             "description":"","odometer":odo,"notes":None,"missing_fields":[]})
            elif pl=="liability_minus" and pa:
                acts.append({"type":"liability_minus","car_id":car_id,"date":today,"amount":pa[0],
                             "description":liab_desc("liability_minus",part,None),
                             "odometer":None,"notes":None,"missing_fields":[]})
            elif pl=="liability_plus" and pa:
                acts.append({"type":"liability_plus","car_id":car_id,"date":today,"amount":pa[0],
                             "description":liab_desc("liability_plus",part,None),
                             "odometer":None,"notes":None,"missing_fields":[]})
        if acts: return acts
    return None


def needs_odo(acts:List[dict])->bool:
    return any(a.get("type") in ("expense","income") and a.get("odometer") in (None,"") for a in acts)

def fill_odo_all(acts:List[dict],odo:int,est:bool):
    for a in acts:
        if a.get("type") in ("expense","income") and a.get("odometer") in (None,""):
            a["odometer"]=odo; a["odometer_estimated"]=est

def miss_fields(data:dict,raw:str="")->List[str]:
    m=[]; t=data.get("type")
    to_case=is_to(raw) or str(data.get("description","")).lower().strip() in {"то","плановое то","планове то"}
    if not t: m.append("type")
    if not data.get("car_id"): m.append("car_id")
    if data.get("amount") in (None,"") and not to_case: m.append("amount")
    if t in ("expense","liability_minus","liability_plus") and not data.get("description"): m.append("description")
    if t in ("expense","income") and data.get("odometer") in (None,""): m.append("odometer")
    return m

def ask_miss(fields:List[str])->str:
    m={"type":"Вкажи тип: прихід, витрата, штраф чи борг.",
       "car_id":f"Вкажи номер машини:\n{', '.join(KNOWN_CAR_IDS)}",
       "amount":"Вкажи суму в гривнях.",
       "description":"Вкажи опис або причину.",
       "odometer":"Мені додати середньостатистичний пробіг?\nНапиши «так» або цифри одометра."}
    return m.get(fields[0],"Уточни відсутні дані.")


# ════════════════════════════════════════════════════════════
# ЗВІТИ
# ════════════════════════════════════════════════════════════

def km_icon(rem:Optional[int],total:int)->str:
    if rem is None: return "⚪"
    if rem<=0: return "🔴"
    r=rem/total
    if r>0.6:  return "🟢"
    if r>0.3:  return "🟡"
    if r>0.1:  return "🟠"
    return "🔴"

def ins_icon(days:int)->str:
    if days<=14: return "🔴"
    if days<=30: return "🟠"
    if days<=90: return "🟡"
    return "🟢"


def oil_report()->str:
    snap=get_snap(); lines=[]
    for car_id in KNOWN_CAR_IDS:
        rows=next((v for t,v in snap.items() if car_id in t or VEHICLE_MAP.get(car_id,"") in t),None)
        if not rows: continue
        ld,lo=find_last_oil(rows)
        co=get_current_odo(rows)
        if lo is None or co is None:
            lines.append((999999,f"⚪ {car_id} | даних немає")); continue
        co=max(co,lo)
        rem=10000-(co-lo)
        lines.append((rem,f"{km_icon(rem,10000)} {car_id} | {ld} | {lo:,} | {fmt_km(rem)} км"))
    lines.sort(key=lambda x:x[0])
    return "\n".join(x[1] for x in lines) or "Даних немає"


def grm_report()->str:
    snap=get_snap(); lines=[]
    for car_id in KNOWN_CAR_IDS:
        if car_id in SKIP_GRM: continue
        rows=next((v for t,v in snap.items() if car_id in t or VEHICLE_MAP.get(car_id,"") in t),None)
        if not rows: continue
        ld,lo=find_last_grm(rows)
        co=get_current_odo(rows)
        if lo is None or co is None:
            lines.append((999999,f"⚪ {car_id} | даних немає")); continue
        co=max(co,lo)
        rem=50000-(co-lo)
        lines.append((rem,f"{km_icon(rem,50000)} {car_id} | {ld} | {lo:,} | {fmt_km(rem)} км"))
    lines.sort(key=lambda x:x[0])
    return "\n".join(x[1] for x in lines) or "Даних немає"


def ins_report()->str:
    snap=get_snap(); today=datetime.now(KYIV_TZ).date(); lines=[]
    for car_id in KNOWN_CAR_IDS:
        rows=next((v for t,v in snap.items() if car_id in t or VEHICLE_MAP.get(car_id,"") in t),None)
        if not rows: continue
        end_d,company=find_insurance(rows)
        if not end_d: lines.append((99999,f"⚪ {car_id} | страховки немає")); continue
        dl=(end_d-today).days
        lines.append((dl,f"{ins_icon(dl)} {car_id} | {end_d.strftime('%d.%m.%y')} | {company}"))
    lines.sort(key=lambda x:x[0])
    return "\n".join(x[1] for x in lines) or "Даних немає"


def ins_single(car_id:str) -> str:
    """Детальна інформація про страховку конкретної машини"""
    snap = get_snap()
    rows = next((v for t,v in snap.items() if car_id in t or VEHICLE_MAP.get(car_id,"") in t), None)
    if not rows:
        return f"❌ Машину {fp(car_id)} не знайдено"

    end_d, company = find_insurance(rows)
    if not end_d:
        return f"⚪ {fp(car_id)} — страховки не знайдено в таблиці"

    today  = datetime.now(KYIV_TZ).date()
    dl     = (end_d - today).days
    icon   = ins_icon(dl)
    status = ("⚠️ ЗАКІНЧУЄТЬСЯ!" if dl <= 14
              else (f"Залишилось {dl} дн." if dl > 0 else "❌ ПРОСТРОЧЕНО!"))

    # Контакти страхової
    office = get_insurance_office(company)
    if office:
        addr_str  = "\n           ".join(office["address"])
        office_str = (f"🏢 {office['name']}\n"
                      f"📞 Гаряча лінія: {office['hotline']}\n"
                      f"📍 Дніпро: {addr_str}\n"
                      f"🌐 {office['web']}")
    else:
        office_str = f"🏢 {company}\n📞 Уточніть контакти"

    # Шукаємо файл страховки в Google Drive
    file_url, file_name = find_insurance_file_in_drive(car_id)
    if file_url:
        drive_str = f"📄 Файл страховки ({file_name}):\n{file_url}"
    else:
        drive_str = f"📁 Всі страховки:\n{INSURANCE_DRIVE_FOLDER_URL}"

    return (
        f"{icon} Страховка — {fp(car_id)}\n\n"
        f"📅 До: {end_d.strftime('%d.%m.%Y')}\n"
        f"📊 {status}\n\n"
        f"{office_str}\n\n"
        f"{drive_str}"
    )


def monthly_sum(car_id:str)->str:
    ws=find_ws(open_sheet(),car_id)
    if not ws: return f"❌ Машину {car_id} не знайдено"
    now=datetime.now(KYIV_TZ); m,y=now.month,now.year
    inc=exp=lib=0.0
    for row in ws.get_all_values()[7:]:
        if len(row)>7:
            d=parse_date(row[4] if len(row)>4 else None); n=parse_num(row[7] if len(row)>7 else None)
            if d and d.month==m and d.year==y and n: exp+=n
        if len(row)>12:
            d=parse_date(row[10] if len(row)>10 else None); n=parse_num(row[12] if len(row)>12 else None)
            if d and d.month==m and d.year==y and n: inc+=n
        if len(row)>15:
            d=parse_date(row[10] if len(row)>10 else None); rp=row[15] if len(row)>15 else None
            if d and d.month==m and d.year==y and str(rp).strip():
                try: lib+=float(str(rp).replace(",",".").replace(" ",""))
                except: pass
    return (f"📊 Поточний місяць — {fp(car_id)}:\n"
            f"💰 Дохід: {inc:,.0f} грн\n💸 Витрати: {exp:,.0f} грн\n📌 Борги: {lib:,.0f} грн")


# ════════════════════════════════════════════════════════════
# ФОНОВІ НАГАДУВАННЯ
# ════════════════════════════════════════════════════════════

async def notify(ctx:ContextTypes.DEFAULT_TYPE):
    snap=get_snap(force=True); today=datetime.now(KYIV_TZ).date()
    oil=[]; grm_=[]; ins_=[]
    for car_id in KNOWN_CAR_IDS:
        rows=next((v for t,v in snap.items() if car_id in t or VEHICLE_MAP.get(car_id,"") in t),None)
        if not rows: continue
        co=get_current_odo(rows)

        ld_oil,lo=find_last_oil(rows)
        if lo and co:
            rem=10000-(max(co,lo)-lo)
            if rem<=1000: oil.append((rem,f"  {km_icon(rem,10000)} {car_id} — {fmt_km(rem)} км до масла"))

        if car_id not in SKIP_GRM:
            ld_grm,go=find_last_grm(rows)
            if go and co:
                rem=50000-(max(co,go)-go)
                if rem<=1000: grm_.append((rem,f"  {km_icon(rem,50000)} {car_id} — {fmt_km(rem)} км до ГРМ"))

        end_d,company=find_insurance(rows)
        if end_d:
            dl=(end_d-today).days
            if dl<=14: ins_.append((dl,f"  {ins_icon(dl)} {car_id} — {dl} дн. ({end_d.strftime('%d.%m.%y')}) {company}"))

    oil.sort(key=lambda x:x[0]); grm_.sort(key=lambda x:x[0]); ins_.sort(key=lambda x:x[0])
    msgs=[]
    if oil:  msgs.append("🛢 Масло ≤1000 км:\n"+"\n".join(x[1] for x in oil))
    if grm_: msgs.append("⚙️ ГРМ ≤1000 км:\n"+"\n".join(x[1] for x in grm_))
    if ins_: msgs.append("🛡 Страховка ≤14 дн.:\n"+"\n".join(x[1] for x in ins_))
    if not msgs: return
    text="⚠️ Нагадування:\n\n"+"\n\n".join(msgs)
    for uid in ALLOWED_USERS:
        try: await ctx.bot.send_message(chat_id=uid,text=text)
        except Exception as e: logger.error(f"notify {uid}: {e}")


# ════════════════════════════════════════════════════════════
# ДЕТЕКТОРИ КОМАНД
# ════════════════════════════════════════════════════════════

def is_oil_cmd(t:str)->bool:
    return t.lower().strip() in {"масло","замена масла","заміна масла","масло?","масла"}

def is_grm_cmd(t:str)->bool:
    return t.lower().strip() in {"грм","замена грм","комплект грм","заміна грм","грм?"}

def is_ins_cmd(t:str)->bool:
    return t.lower().strip() in {"страховка","страхування","страховки","страховка?"}

def detect_ins_single(text:str)->Optional[str]:
    """Визначає запит по страховці конкретної машини: 'страховка 8730', '4553 страховая'"""
    lo=text.lower()
    is_ins_word=any(k in lo for k in ["страховк","страховая","страхова","страховой"])
    if not is_ins_word: return None
    return find_car_in_text(text)

def det_month(t:str)->Optional[str]:
    if any(k in t.lower() for k in ["місяць","месяц","місяц"]): return find_car_in_text(t)
    return None

def is_yes(t:str)->bool: return t.lower().strip() in {"так","да","yes","ок","окей","ага","добре"}
def is_yes_c(t:str)->bool: return t.lower().strip() in {"так","да","yes","новий","новая","підтвердити"}
def is_no_c(t:str)->bool: return t.lower().strip() in {"ні","нет","дубль","скасувати","отмена","cancel"}


# ════════════════════════════════════════════════════════════
# TELEGRAM
# ════════════════════════════════════════════════════════════

async def handle_msg(update:Update,ctx:ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    if ALLOWED_USERS and uid not in ALLOWED_USERS:
        await update.message.reply_text("⛔ Доступ заборонено"); return
    text=(update.message.text or "").strip()
    logger.info(f"[{uid}] {text}")
    ud=ctx.user_data

    try:
        # ── підтвердження дубля ──────────────────────────────
        if ud.get("w_dup"):
            acts=ud.get("acts_dup",[])
            if   is_yes_c(text): ud.pop("w_dup",None);ud.pop("acts_dup",None); await update.message.reply_text(write_all(acts,text))
            elif is_no_c(text):  ud.pop("w_dup",None);ud.pop("acts_dup",None); await update.message.reply_text("✅ Скасовано як дубль.")
            else:                await update.message.reply_text("Напиши «новий» або «дубль».")
            return

        # ── аномалія одометра ────────────────────────────────
        if ud.get("w_anom"):
            acts=ud.get("acts_anom",[])
            if is_yes_c(text):
                ud.pop("w_anom",None);ud.pop("acts_anom",None)
                await update.message.reply_text(write_all(acts,text))
            elif is_no_c(text):
                ud.pop("w_anom",None); ud["w_odo"]=True; ud["acts_odo"]=acts; ud.pop("acts_anom",None)
                await update.message.reply_text("Надішли правильний одометр або «так».")
            else: await update.message.reply_text("«так» або «ні».")
            return

        # ── очікування одометра ──────────────────────────────
        if ud.get("w_odo"):
            acts=ud.get("acts_odo",[])
            odo_in=extract_odo_from_text(text)
            num=odo_in or (parse_num(text) if parse_num(text) and is_odo_value(parse_num(text)) else None)
            if num:
                fill_odo_all(acts,num,False); ud.pop("w_odo",None);ud.pop("acts_odo",None)
                first=next((a for a in acts if a.get("type") in ("expense","income")),None)
                if first:
                    ws=find_ws(open_sheet(),first["car_id"])
                    if ws and odo_anomaly(ws,num,first.get("date")):
                        ud["w_anom"]=True;ud["acts_anom"]=acts
                        await update.message.reply_text("⚠️ Пробіг нетипово великий. Підтвердити?"); return
                sp=open_sheet()
                for a in acts:
                    ws=find_ws(sp,a["car_id"])
                    if ws and is_dup(ws,a,text):
                        ud["w_dup"]=True;ud["acts_dup"]=acts
                        await update.message.reply_text("❓ Новий запис чи дубль?"); return
                await update.message.reply_text(write_all(acts,text)); return
            if is_yes(text):
                first=next((a for a in acts if a.get("type") in ("expense","income")),None)
                if not first: await update.message.reply_text(write_all(acts,text)); return
                est=estimate_odo(first["car_id"],first.get("date"))
                if not est: await update.message.reply_text("Не вдалося. Надішли цифри."); return
                fill_odo_all(acts,est,True); ud.pop("w_odo",None);ud.pop("acts_odo",None)
                sp=open_sheet()
                for a in acts:
                    ws=find_ws(sp,a["car_id"])
                    if ws and is_dup(ws,a,text):
                        ud["w_dup"]=True;ud["acts_dup"]=acts
                        await update.message.reply_text("❓ Новий запис чи дубль?"); return
                await update.message.reply_text(write_all(acts,text)); return
            await update.message.reply_text("Надішли 6-значний одометр або «так»."); return

        # ── очікування поля ──────────────────────────────────
        if ud.get("w_field"):
            pending=ud.get("pending",{}); miss=pending.get("missing_fields",[]); f=miss[0] if miss else None
            if f=="odometer":
                odo_in=extract_odo_from_text(text)
                if odo_in:
                    pending["odometer"]=odo_in;pending["odometer_estimated"]=False;pending["missing_fields"]=miss_fields(pending,text)
                elif is_yes(text):
                    est=estimate_odo(pending.get("car_id"),pending.get("date"))
                    if est: pending["odometer"]=est;pending["odometer_estimated"]=True;pending["missing_fields"]=miss_fields(pending,text)
                    else: await update.message.reply_text("Не вдалося. Надішли цифри."); return
                else: await update.message.reply_text("«так» або 6-значний одометр."); return
            elif f=="car_id":  pending["car_id"]=resolve_car(text);pending["missing_fields"]=miss_fields(pending,text)
            elif f=="amount":  pending["amount"]=parse_num(text);pending["missing_fields"]=miss_fields(pending,text)
            elif f=="description": pending["description"]=text;pending["missing_fields"]=miss_fields(pending,text)
            ud["pending"]=pending
            if pending.get("missing_fields"):
                await update.message.reply_text(f"❓ {ask_miss(pending['missing_fields'])}"); return
            ud.pop("w_field",None);ud.pop("pending",None)
            await update.message.reply_text(write_one(pending,text)); return

        # ── команди звітів ───────────────────────────────────
        if is_oil_cmd(text):
            await update.message.reply_text("⏳ Будую звіт по маслу...")
            await update.message.reply_text("🛢 Масло | дата заміни | одометр заміни | залишок:\n\n"+oil_report()); return

        if is_grm_cmd(text):
            await update.message.reply_text("⏳ Будую звіт по ГРМ...")
            await update.message.reply_text("⚙️ ГРМ | дата заміни | одометр заміни | залишок:\n\n"+grm_report()); return

        if is_ins_cmd(text):
            await update.message.reply_text("⏳ Будую звіт по страховках...")
            await update.message.reply_text("🛡 Страховки | машина | дата | компанія:\n\n"+ins_report()); return

        # Страховка конкретної машини: "страховка 8730", "4553 страховая"
        single_car=detect_ins_single(text)
        if single_car:
            await update.message.reply_text(ins_single(single_car)); return

        cm=det_month(text)
        if cm: await update.message.reply_text(monthly_sum(cm)); return

        # ── основна обробка ──────────────────────────────────
        await update.message.reply_text("⏳ Обробляю...")

        h_acts=heur(text)
        if h_acts:
            if needs_odo(h_acts):
                ud["w_odo"]=True;ud["acts_odo"]=h_acts
                await update.message.reply_text("❓ Немає одометра.\nДодати середньостатистичний? «так» або 6-значні цифри."); return
            sp=open_sheet()
            for a in h_acts:
                ws=find_ws(sp,a["car_id"])
                if ws and is_dup(ws,a,text):
                    ud["w_dup"]=True;ud["acts_dup"]=h_acts
                    await update.message.reply_text("❓ Новий запис чи дубль?"); return
            await update.message.reply_text(write_all(h_acts,text)); return

        parsed=ask_ai(text,ud.get("pending"))
        if "error" in parsed: await update.message.reply_text(f"❌ AI: {parsed['error']}"); return
        parsed["car_id"]=resolve_car(parsed.get("car_id"))
        parsed["date"]=norm_date(parsed.get("date"))
        if parsed.get("odometer") in (None,""):
            odo_in=extract_odo_from_text(text)
            if odo_in: parsed["odometer"]=odo_in
        parsed["missing_fields"]=miss_fields(parsed,text)

        if parsed.get("missing_fields"):
            ud["pending"]=parsed; ud["w_field"]=True
            await update.message.reply_text(f"❓ {ask_miss(parsed['missing_fields'])}"); return

        sp=open_sheet(); ws=find_ws(sp,parsed["car_id"])
        if ws and parsed.get("type") in ("expense","income") and parsed.get("odometer") not in (None,""):
            if odo_anomaly(ws,int(parsed["odometer"]),parsed.get("date")):
                ud["w_anom"]=True;ud["acts_anom"]=[parsed]
                await update.message.reply_text("⚠️ Пробіг нетипово великий. Підтвердити?"); return
        if ws and is_dup(ws,parsed,text):
            ud["w_dup"]=True;ud["acts_dup"]=[parsed]
            await update.message.reply_text("❓ Новий запис чи дубль?"); return
        ud.pop("pending",None)
        await update.message.reply_text(write_one(parsed,text))

    except Exception as e:
        logger.exception("handle_msg")
        await update.message.reply_text(f"❌ Помилка: {e}")


async def cmd_start(update:Update,ctx:ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"👋 Бот автопарку v3.\nID: `{update.effective_user.id}`\n\n"
        f"Машини: {', '.join(KNOWN_CAR_IDS)}\n\n"
        f"Команди:\n"
        f"• Взяв 3800 за 9245 354746\n"
        f"• 8730 колодки 370 грн 470420\n"
        f"• ТО 5725\n• Штраф 200 за 8730\n"
        f"• 8730 приход 3800, долг 200 за дтп\n"
        f"• масло | грм | страховка\n"
        f"• страховка 8730\n• 8730 місяць",
        parse_mode="Markdown")

async def cmd_cancel(update:Update,ctx:ContextTypes.DEFAULT_TYPE):
    for k in ["pending","acts_dup","acts_odo","acts_anom","w_field","w_odo","w_dup","w_anom"]:
        ctx.user_data.pop(k,None)
    await update.message.reply_text("✅ Введення скасовано.")


def main():
    app=Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CommandHandler("cancel",cmd_cancel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,handle_msg))
    app.job_queue.run_daily(notify,time=time(9,15,tzinfo=KYIV_TZ))
    app.job_queue.run_daily(notify,time=time(16,0,tzinfo=KYIV_TZ))
    logger.info("Bot v3 started!")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
