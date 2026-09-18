# ==================== RAQAMMARKET BOT ====================
# Flask + Webhook + PostgreSQL (Supabase) | Render tayyor
# O'rnatish: pip install -r requirements.txt
# Ishga tushirish (lokal): python app.py
# Render: gunicorn app:app

import os
import re
import time
import uuid
import json
import asyncio
from datetime import datetime, timedelta
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, request
import telebot
from telebot import types
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

# ==================== SOZLAMALAR ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]
DOGESMS_API_KEY = os.getenv("DOGESMS_API_KEY", "")
DOGESMS_BASE = os.getenv("DOGESMS_BASE", "https://api.dogesms.com")
DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
PORT = int(os.getenv("PORT", "10000"))

API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "")
TELETHON_PHONE = os.getenv("TELETHON_PHONE", "")
HUMO_BOT_USERNAME = os.getenv("HUMO_BOT_USERNAME", "HUMOcardbot")

DEFAULT_SETTINGS = {
    "payment_time_minutes": 15,
    "start_amount": 5000,
    "referral_percent": 10,
    "referral_bonus": 500,
    "free_number_referrals": 20,
    "bot_active": True,
    "maintenance": False,
    "default_service": "telegram",
    "card_number": os.getenv("DEFAULT_CARD", "8600123456789012"),
    "card_owner": os.getenv("DEFAULT_CARD_OWNER", "HUMO CARD"),
    "usd_to_uzs": int(os.getenv("USD_TO_UZS", "12500")),
}

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=True)
app = Flask(__name__)

# ==================== DATABASE POOL + KESH ====================
_db_pool = None
_settings_cache = {}
_settings_cache_time = 0
_SETTINGS_TTL = 30  # soniya
_admin_cache = set(ADMIN_IDS)

def init_pool():
    global _db_pool
    if _db_pool is None and DATABASE_URL:
        _db_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=8,
            dsn=DATABASE_URL,
            cursor_factory=RealDictCursor,
        )
        print("✅ DB pool tayyor")

def get_db():
    if _db_pool is None:
        init_pool()
    conn = _db_pool.getconn()
    conn.autocommit = False
    return conn

def put_db(conn):
    if not conn:
        return
    if _db_pool:
        try:
            _db_pool.putconn(conn)
            return
        except Exception:
            pass
    try:
        conn.close()
    except Exception:
        pass

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            balance REAL DEFAULT 0,
            referral_balance REAL DEFAULT 0,
            referred_by BIGINT DEFAULT 0,
            referrals_count INTEGER DEFAULT 0,
            free_numbers INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0,
            joined_date TEXT,
            last_active TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount REAL,
            unique_amount REAL,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            paid_at TEXT,
            expires_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS numbers (
            id SERIAL PRIMARY KEY,
            country TEXT,
            country_code TEXT,
            number TEXT,
            price REAL,
            status TEXT DEFAULT 'available',
            sold_to BIGINT DEFAULT 0,
            sold_at TEXT,
            dogesms_order_id TEXT,
            added_by BIGINT,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS number_orders (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            number_id INTEGER,
            number TEXT,
            country TEXT,
            price REAL,
            is_free INTEGER DEFAULT 0,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS country_prices (
            country_code TEXT PRIMARY KEY,
            country_name TEXT,
            price REAL,
            dogesms_price_cents INTEGER DEFAULT 0,
            flag TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            source TEXT DEFAULT 'manual'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS free_countries (
            id SERIAL PRIMARY KEY,
            country_code TEXT UNIQUE,
            country_name TEXT,
            flag TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            added_by BIGINT,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            full_name TEXT,
            text TEXT,
            created_at TEXT,
            is_approved INTEGER DEFAULT 1
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            channel_id TEXT,
            channel_username TEXT,
            channel_title TEXT,
            is_private INTEGER DEFAULT 0,
            invite_link TEXT DEFAULT ''
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id BIGINT PRIMARY KEY
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS referral_history (
            id SERIAL PRIMARY KEY,
            from_user BIGINT,
            to_user BIGINT,
            amount REAL,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS saved_numbers (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            number TEXT,
            country TEXT,
            country_code TEXT,
            note TEXT DEFAULT '',
            created_at TEXT,
            UNIQUE(user_id, number)
        )
    """)
    for key, value in DEFAULT_SETTINGS.items():
        c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", (key, str(value)))
    for admin_id in ADMIN_IDS:
        c.execute("INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (admin_id,))
    # Boshlang'ich davlatlar (bo'sh bo'lmasin)
    seeds = [
        ("UZ", "Oʻzbekiston", 15000, "🇺🇿"),
        ("RU", "Rossiya", 12000, "🇷🇺"),
        ("KZ", "Qozogʻiston", 14000, "🇰🇿"),
        ("IN", "Hindiston", 8000, "🇮🇳"),
        ("US", "AQSH", 25000, "🇺🇸"),
        ("GB", "Buyuk Britaniya", 22000, "🇬🇧"),
        ("TR", "Turkiya", 11000, "🇹🇷"),
        ("PH", "Filippin", 9000, "🇵🇭"),
        ("ID", "Indoneziya", 8500, "🇮🇩"),
        ("UA", "Ukraina", 10000, "🇺🇦"),
    ]
    for code, name, price, flag in seeds:
        c.execute("""INSERT INTO country_prices (country_code, country_name, price, flag, is_active, source)
                     VALUES (%s, %s, %s, %s, 1, 'seed')
                     ON CONFLICT (country_code) DO NOTHING""", (code, name, price, flag))
    conn.commit()
    put_db(conn)
    print("✅ PostgreSQL database tayyor")

def _parse_setting_val(val):
    if val is None:
        return None
    if str(val).lower() in ("true", "false"):
        return str(val).lower() == "true"
    try:
        if "." in str(val):
            return float(val)
        return int(val)
    except Exception:
        return val

def get_setting(key, default=None):
    """Settings xotirada keshlanadi — tezroq javob"""
    global _settings_cache, _settings_cache_time
    now = time.time()
    if now - _settings_cache_time > _SETTINGS_TTL or not _settings_cache:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("SELECT key, value FROM settings")
            rows = c.fetchall()
            _settings_cache = {r["key"]: r["value"] for r in rows}
            _settings_cache_time = now
        finally:
            put_db(conn)
    val = _settings_cache.get(key)
    if val is None:
        return default
    return _parse_setting_val(val)

def set_setting(key, value):
    global _settings_cache, _settings_cache_time
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, str(value)))
        conn.commit()
        _settings_cache[key] = str(value)
        _settings_cache_time = time.time()
    finally:
        put_db(conn)

def is_admin(user_id):
    if user_id in _admin_cache:
        return True
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT user_id FROM admins WHERE user_id = %s", (user_id,))
        result = c.fetchone()
        if result:
            _admin_cache.add(user_id)
            return True
        return False
    finally:
        put_db(conn)

def get_user(user_id):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        return c.fetchone()
    finally:
        put_db(conn)

def register_user(user_id, username, full_name, referred_by=0):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT INTO users (user_id, username, full_name, referred_by, joined_date, last_active)
                 VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING""",
              (user_id, username, full_name, referred_by, now, now))
    if referred_by and referred_by != user_id and username and str(username).strip():
        c.execute("SELECT user_id FROM users WHERE user_id = %s", (referred_by,))
        if c.fetchone():
            c.execute("SELECT id FROM referral_history WHERE to_user = %s", (user_id,))
            if not c.fetchone():
                c.execute("UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id = %s", (referred_by,))
                bonus = get_setting("referral_bonus", 500)
                c.execute("UPDATE users SET referral_balance = referral_balance + %s WHERE user_id = %s", (bonus, referred_by))
                c.execute("INSERT INTO referral_history (from_user, to_user, amount, created_at) VALUES (%s, %s, %s, %s)",
                          (referred_by, user_id, bonus, now))
                c.execute("SELECT referrals_count FROM users WHERE user_id = %s", (referred_by,))
                count = c.fetchone()["referrals_count"]
                needed = get_setting("free_number_referrals", 20)
                if count >= needed and needed > 0:
                    c.execute("UPDATE users SET free_numbers = free_numbers + 1, referrals_count = referrals_count - %s WHERE user_id = %s",
                              (needed, referred_by))
    conn.commit()
    put_db(conn)

def update_balance(user_id, amount, is_referral=False):
    conn = get_db()
    c = conn.cursor()
    if is_referral:
        c.execute("UPDATE users SET referral_balance = referral_balance + %s WHERE user_id = %s", (amount, user_id))
    else:
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (amount, user_id))
    conn.commit()
    put_db(conn)

# ==================== DOGESMS ====================
def dogesms_headers():
    return {"Authorization": f"Bearer {DOGESMS_API_KEY}", "Content-Type": "application/json"}

def dogesms_balance():
    try:
        r = requests.get(f"{DOGESMS_BASE}/v1/balance", headers=dogesms_headers(), timeout=15)
        if r.status_code == 200:
            return r.json().get("data", {}).get("balance_cents", 0) / 100
        return None
    except Exception as e:
        print("dogesms balance error:", e)
        return None

def dogesms_create_order(service_code, country_code):
    try:
        payload = {"service_code": service_code, "country_code": country_code}
        headers = dogesms_headers()
        headers["X-Idempotency-Key"] = str(uuid.uuid4())
        r = requests.post(f"{DOGESMS_BASE}/v1/orders", headers=headers, json=payload, timeout=20)
        if r.status_code in (200, 201):
            return r.json().get("data", {})
        print("dogesms order error:", r.status_code, r.text)
        return None
    except Exception as e:
        print("dogesms create order error:", e)
        return None

def dogesms_get_order(order_id):
    try:
        r = requests.get(f"{DOGESMS_BASE}/v1/orders/{order_id}", headers=dogesms_headers(), timeout=15)
        if r.status_code == 200:
            return r.json().get("data", {})
        return None
    except Exception as e:
        print("dogesms get order error:", e)
        return None

def _fetch_one_telegram_price(code, name):
    try:
        pr = requests.get(f"{DOGESMS_BASE}/v1/catalog/prices", headers=dogesms_headers(),
                          params={"country_code": code}, timeout=8)
        if pr.status_code != 200:
            return None
        prices = pr.json().get("data", [])
        best = None
        for p in prices:
            svc = (p.get("service_code") or p.get("code") or "").lower()
            if svc != "telegram":
                continue
            cents = int(p.get("price_cents") or p.get("price") or 0)
            if cents <= 0:
                continue
            if best is None or cents < best["price_cents"]:
                best = {"code": code.upper(), "name": name, "price_cents": cents,
                        "available": p.get("available_count", 0)}
        return best
    except Exception as e:
        print(f"price {code}:", e)
        return None

def dogesms_telegram_prices(progress_callback=None):
    try:
        r = requests.get(f"{DOGESMS_BASE}/v1/catalog/countries", headers=dogesms_headers(), timeout=20)
        if r.status_code != 200:
            return []
        data = r.json().get("data", [])
        if not isinstance(data, list):
            data = r.json() if isinstance(r.json(), list) else []
        pairs = []
        for c in data:
            if not isinstance(c, dict):
                continue
            code = c.get("code") or c.get("country_code")
            name = c.get("name") or code
            if code:
                pairs.append((str(code).upper(), name))
        if not pairs:
            return []
        result = []
        done = 0
        total = len(pairs)
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(_fetch_one_telegram_price, code, name): code for code, name in pairs}
            for fut in as_completed(futs):
                done += 1
                item = fut.result()
                if item:
                    result.append(item)
                if progress_callback and done % 10 == 0:
                    try:
                        progress_callback(done, total, len(result))
                    except:
                        pass
        return result
    except Exception as e:
        print("dogesms telegram prices error:", e)
        return []

COUNTRY_FLAGS = {
    "UZ": "🇺🇿", "RU": "🇷🇺", "US": "🇺🇸", "GB": "🇬🇧", "DE": "🇩🇪", "IN": "🇮🇳",
    "BR": "🇧🇷", "HK": "🇭🇰", "SG": "🇸🇬", "FR": "🇫🇷", "TR": "🇹🇷", "KZ": "🇰🇿",
    "KG": "🇰🇬", "TJ": "🇹🇯", "AZ": "🇦🇿", "UA": "🇺🇦", "PL": "🇵🇱", "NL": "🇳🇱",
    "ID": "🇮🇩", "PH": "🇵🇭", "VN": "🇻🇳", "TH": "🇹🇭", "MY": "🇲🇾", "CN": "🇨🇳",
    "JP": "🇯🇵", "KR": "🇰🇷", "CA": "🇨🇦", "AU": "🇦🇺", "IT": "🇮🇹", "ES": "🇪🇸",
    "PT": "🇵🇹", "RO": "🇷🇴", "BG": "🇧🇬", "CZ": "🇨🇿", "SK": "🇸🇰", "HU": "🇭🇺",
    "SE": "🇸🇪", "NO": "🇳🇴", "FI": "🇫🇮", "DK": "🇩🇰", "IE": "🇮🇪", "BE": "🇧🇪",
    "AT": "🇦🇹", "CH": "🇨🇭", "GR": "🇬🇷", "EG": "🇪🇬", "SA": "🇸🇦", "AE": "🇦🇪",
    "IL": "🇮🇱", "MX": "🇲🇽", "AR": "🇦🇷", "CL": "🇨🇱", "CO": "🇨🇴", "PE": "🇵🇪",
    "NG": "🇳🇬", "ZA": "🇿🇦", "KE": "🇰🇪", "GH": "🇬🇭", "PK": "🇵🇰", "BD": "🇧🇩",
    "LK": "🇱🇰", "NP": "🇳🇵", "MM": "🇲🇲", "KH": "🇰🇭", "LA": "🇱🇦", "TW": "🇹🇼",
    "MO": "🇲🇴", "NZ": "🇳🇿", "BY": "🇧🇾", "MD": "🇲🇩", "GE": "🇬🇪", "AM": "🇦🇲",
    "LT": "🇱🇹", "LV": "🇱🇻", "EE": "🇪🇪", "RS": "🇷🇸", "HR": "🇭🇷", "BA": "🇧🇦",
    "AL": "🇦🇱", "MK": "🇲🇰", "SI": "🇸🇮", "ME": "🇲🇪", "XK": "🇽🇰", "IQ": "🇮🇶",
    "IR": "🇮🇷", "AF": "🇦🇫", "SY": "🇸🇾", "JO": "🇯🇴", "LB": "🇱🇧", "MA": "🇲🇦",
    "TN": "🇹🇳", "DZ": "🇩🇿", "LY": "🇱🇾", "SD": "🇸🇩", "ET": "🇪🇹", "UG": "🇺🇬",
    "TZ": "🇹🇿", "ZW": "🇿🇼", "ZM": "🇿🇲", "MW": "🇲🇼", "AO": "🇦🇴", "MZ": "🇲🇿",
    "QA": "🇶🇦", "BH": "🇧🇭", "KW": "🇰🇼", "OM": "🇴🇲", "YE": "🇾🇪", "PS": "🇵🇸",
    "CY": "🇨🇾", "MT": "🇲🇹", "IS": "🇮🇸", "LU": "🇱🇺", "UY": "🇺🇾", "PY": "🇵🇾",
    "BO": "🇧🇴", "EC": "🇪🇨", "VE": "🇻🇪", "CR": "🇨🇷", "PA": "🇵🇦", "CU": "🇨🇺",
    "DO": "🇩🇴", "JM": "🇯🇲", "MN": "🇲🇳", "BT": "🇧🇹", "MV": "🇲🇻", "SN": "🇸🇳",
    "CI": "🇨🇮", "CM": "🇨🇲", "RW": "🇷🇼", "MG": "🇲🇬", "MU": "🇲🇺", "FJ": "🇫🇯",
}

COUNTRY_UZ = {
    "UZ": "Oʻzbekiston", "RU": "Rossiya", "US": "AQSH", "GB": "Buyuk Britaniya",
    "DE": "Germaniya", "IN": "Hindiston", "BR": "Braziliya", "HK": "Gonkong",
    "SG": "Singapur", "FR": "Fransiya", "TR": "Turkiya", "KZ": "Qozogʻiston",
    "KG": "Qirgʻiziston", "TJ": "Tojikiston", "AZ": "Ozarbayjon", "UA": "Ukraina",
    "PL": "Polsha", "NL": "Niderlandiya", "ID": "Indoneziya", "PH": "Filippin",
    "VN": "Vyetnam", "TH": "Tailand", "MY": "Malayziya", "CN": "Xitoy",
    "JP": "Yaponiya", "KR": "Koreya", "CA": "Kanada", "AU": "Avstraliya",
    "IT": "Italiya", "ES": "Ispaniya", "PT": "Portugaliya", "RO": "Ruminiya",
    "BG": "Bolgariya", "CZ": "Chexiya", "SK": "Slovakiya", "HU": "Vengriya",
    "SE": "Shvetsiya", "NO": "Norvegiya", "FI": "Finlyandiya", "DK": "Daniya",
    "IE": "Irlandiya", "BE": "Belgiya", "AT": "Avstriya", "CH": "Shveytsariya",
    "GR": "Gretsiya", "EG": "Misr", "SA": "Saudiya", "AE": "BAA", "IL": "Isroil",
    "MX": "Meksika", "AR": "Argentina", "CL": "Chili", "CO": "Kolumbiya",
    "PE": "Peru", "NG": "Nigeriya", "ZA": "Janubiy Afrika", "PK": "Pokiston",
    "BD": "Bangladesh", "TW": "Tayvan", "BY": "Belarus", "GE": "Gruziya",
    "AM": "Armaniston", "LT": "Litva", "LV": "Latviya", "EE": "Estoniya",
}

def get_flag(code):
    return COUNTRY_FLAGS.get((code or "").upper(), "🌍")

def get_uz_name(code, fallback=""):
    return COUNTRY_UZ.get((code or "").upper(), fallback or code)

def sync_dogesms_countries(progress_callback=None):
    items = dogesms_telegram_prices(progress_callback=progress_callback)
    if not items:
        return 0
    conn = get_db()
    c = conn.cursor()
    count = 0
    rate = get_setting("usd_to_uzs", 12500)
    for item in items:
        code = item["code"]
        name = get_uz_name(code, item["name"])
        flag = get_flag(code)
        cents = item["price_cents"]
        c.execute("SELECT price, source FROM country_prices WHERE country_code = %s", (code,))
        row = c.fetchone()
        if row:
            c.execute("UPDATE country_prices SET dogesms_price_cents = %s, flag = %s, country_name = %s WHERE country_code = %s",
                      (cents, flag, name, code))
        else:
            price_uzs = max(5000, int(cents / 100 * rate))
            c.execute("""INSERT INTO country_prices (country_code, country_name, price, dogesms_price_cents, flag, is_active, source)
                         VALUES (%s, %s, %s, %s, %s, 1, 'dogesms')""",
                      (code, name, price_uzs, cents, flag))
        count += 1
    conn.commit()
    put_db(conn)
    return count

# ==================== KLAVIATURALAR ====================
def main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("📱 Nomer sotib olish"), types.KeyboardButton("🎁 Tekin nomer"))
    markup.add(types.KeyboardButton("📋 Buyurtmalarim"), types.KeyboardButton("⭐ Saqlangan nomerlar"))
    markup.add(types.KeyboardButton("💰 Hisobim"), types.KeyboardButton("👥 Referal tizimi"))
    markup.add(types.KeyboardButton("💬 Sharhlar"), types.KeyboardButton("📖 Qo'llanma"))
    markup.add(types.KeyboardButton("📞 Adminga yozish"), types.KeyboardButton("⚙️ Sozlamalar"))
    if is_admin(user_id):
        markup.add(types.KeyboardButton("🔐 Admin Panel"))
    return markup

def account_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("💵 Balansim"), types.KeyboardButton("➕ Hisob to'ldirish"))
    markup.add(types.KeyboardButton("📜 To'lov tarixi"), types.KeyboardButton("🔙 Orqaga"))
    return markup

def referral_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("🔗 Mening referal havolam"), types.KeyboardButton("👤 Taklif qilganlarim"))
    markup.add(types.KeyboardButton("💸 Referal balansi"), types.KeyboardButton("📊 Referal statistikasi"))
    markup.add(types.KeyboardButton("🔙 Orqaga"))
    return markup

def admin_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("📊 Statistika"), types.KeyboardButton("💰 To'lovlar"))
    markup.add(types.KeyboardButton("📱 Nomerlar boshqaruvi"), types.KeyboardButton("👥 Foydalanuvchilar"))
    markup.add(types.KeyboardButton("📢 Xabar yuborish"), types.KeyboardButton("🎁 Referal sozlamalari"))
    markup.add(types.KeyboardButton("⏱ To'lov sozlamalari"), types.KeyboardButton("📢 Majburiy obuna"))
    markup.add(types.KeyboardButton("👨‍💼 Adminlar"), types.KeyboardButton("⚙️ Boshqa sozlamalar"))
    markup.add(types.KeyboardButton("🔙 Asosiy menyu"))
    return markup

def numbers_admin_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("🌍 Davlatlar ro'yxati"), types.KeyboardButton("🌐 Barcha davlat kodlari"))
    markup.add(types.KeyboardButton("💵 Nomer narxini belgilash"), types.KeyboardButton("🔄 dogesms dan yangilash"))
    markup.add(types.KeyboardButton("🔄 dogesms balansi"), types.KeyboardButton("🎁 Tekin nomer qo'shish"))
    markup.add(types.KeyboardButton("📦 Tekin ombor"), types.KeyboardButton("📋 Berilgan nomerlar"))
    markup.add(types.KeyboardButton("🔙 Admin Panel"))
    return markup

def payment_settings_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("⏱ To'lov vaqtini o'zgartirish"), types.KeyboardButton("💵 Boshlang'ich summa"))
    markup.add(types.KeyboardButton("💳 Karta raqami"), types.KeyboardButton("👤 Karta egasi"))
    markup.add(types.KeyboardButton("🔙 Admin Panel"))
    return markup

def referral_settings_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("📊 Referal foizi"), types.KeyboardButton("💰 1 referal uchun pul"))
    markup.add(types.KeyboardButton("🎁 Bepul nomer uchun soni"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def channels_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("➕ Kanal qo'shish"), types.KeyboardButton("❌ Kanal o'chirish"))
    markup.add(types.KeyboardButton("📋 Kanallar ro'yxati"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def admins_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("➕ Admin qo'shish"), types.KeyboardButton("❌ Admin o'chirish"))
    markup.add(types.KeyboardButton("📋 Adminlar ro'yxati"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def other_settings_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("🟢 Botni yoqish"), types.KeyboardButton("🔴 Botni o'chirish"))
    markup.add(types.KeyboardButton("🛠 Texnik ishlar"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def users_admin_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("📋 Userlar roʻyxati"), types.KeyboardButton("🔍 Foydalanuvchi qidirish"))
    markup.add(types.KeyboardButton("💵 Balans oʻzgartirish"), types.KeyboardButton("🚫 Bloklash / Ochish"))
    markup.add(types.KeyboardButton("🔙 Admin Panel"))
    return markup

def back_only():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("🔙 Orqaga"))
    return markup

# ==================== YORDAMCHI ====================
def format_money(amount):
    try:
        return f"{float(amount):,.0f}".replace(",", " ") + " soʻm"
    except:
        return f"{amount} soʻm"

def is_amount_busy(amount):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM payments WHERE status = 'pending' AND unique_amount = %s", (amount,))
    row = c.fetchone()
    put_db(conn)
    return row is not None

def get_unique_amount(desired=None):
    start = get_setting("start_amount", 5000)
    amount = int(desired) if desired is not None else int(start)
    if amount < 1000:
        amount = int(start)
    for _ in range(50):
        if not is_amount_busy(amount):
            return amount
        amount += 1
    return amount

def expire_pending_payments():
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""SELECT id, user_id, unique_amount FROM payments
                 WHERE status = 'pending' AND expires_at IS NOT NULL AND expires_at < %s""", (now,))
    rows = c.fetchall()
    for row in rows:
        c.execute("UPDATE payments SET status = 'expired' WHERE id = %s", (row["id"],))
        try:
            bot.send_message(row["user_id"],
                f"⏰ <b>Toʻlov vaqti tugadi!</b>\n\n"
                f"💰 Summa: <b>{format_money(row['unique_amount'])}</b>\n"
                f"Vaqt ichida toʻlov qilinmadi.\n"
                f"Yangi toʻlov uchun «➕ Hisob toʻldirish» ni bosing.")
        except:
            pass
    conn.commit()
    put_db(conn)
    return len(rows)

def check_subscription(user_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT channel_id, channel_username, channel_title, is_private, COALESCE(invite_link,'') as invite_link FROM channels")
    channels = c.fetchall()
    put_db(conn)
    if not channels:
        return True, []
    not_subscribed = []
    for ch in channels:
        try:
            member = bot.get_chat_member(ch["channel_id"], user_id)
            if member.status in ["left", "kicked"]:
                display = ch["channel_title"] or ch["channel_username"] or ch["channel_id"]
                url = ch["invite_link"] or (f"https://t.me/{ch['channel_username'].lstrip('@')}" if ch["channel_username"] else None)
                not_subscribed.append((display, url))
        except:
            display = ch["channel_title"] or ch["channel_username"] or ch["channel_id"]
            url = ch["invite_link"] or (f"https://t.me/{ch['channel_username'].lstrip('@')}" if ch["channel_username"] else None)
            not_subscribed.append((display, url))
    return len(not_subscribed) == 0, not_subscribed

def get_active_countries():
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT country_code, country_name, price,
                            COALESCE(flag,'') AS flag,
                            COALESCE(dogesms_price_cents,0) AS dogesms_price_cents
                     FROM country_prices WHERE is_active = 1 ORDER BY price ASC""")
        return c.fetchall()
    except Exception as e:
        print("get_active_countries error:", e)
        return []
    finally:
        put_db(conn)

def build_countries_keyboard(page=0, per_page=10):
    countries = get_active_countries()
    total = len(countries)
    start = page * per_page
    end = start + per_page
    chunk = countries[start:end]
    markup = types.InlineKeyboardMarkup(row_width=1)
    for row in chunk:
        code = row["country_code"]
        name = row["country_name"] or get_uz_name(code)
        price = row["price"]
        flag = row.get("flag") or get_flag(code)
        btn_text = f"{code} {flag} {name} — {format_money(price)}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"buy_c_{code}"))
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"cpage_{page-1}"))
    if end < total:
        nav.append(types.InlineKeyboardButton("Keyingi ➡️", callback_data=f"cpage_{page+1}"))
    if nav:
        markup.row(*nav)
    markup.add(types.InlineKeyboardButton("🔙 Yopish", callback_data="close_countries"))
    return markup, total, page

# ==================== HUMO TO'LOV ====================
def parse_humo_amount(text):
    patterns = [
        r'[\+➕]?\s*([\d\s]+[.,]\d{2})\s*UZS',
        r'💰\s*([\d\s]+[.,]\d{2})\s*UZS',
        r'([\d\s]+[.,]\d{2})\s*UZS',
        r'[\+➕]\s*([\d\s.,]+)',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(1).replace(" ", "").replace(",", ".")
            try:
                if raw.count(".") > 1:
                    raw = raw.replace(".", "", raw.count(".") - 1)
                return int(round(float(raw)))
            except:
                continue
    m = re.search(r'(\d{4,6})', text.replace(" ", "").replace(",", "").replace(".", ""))
    if m:
        try:
            return int(m.group(1))
        except:
            pass
    return None

def process_humo_payment(amount):
    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT id, user_id, unique_amount FROM payments
                 WHERE status = 'pending' AND unique_amount = %s ORDER BY id DESC LIMIT 1""", (amount,))
    row = c.fetchone()
    if not row:
        c.execute("""SELECT id, user_id, unique_amount FROM payments
                     WHERE status = 'pending' AND unique_amount BETWEEN %s AND %s
                     ORDER BY id DESC LIMIT 1""", (amount - 2, amount + 2))
        row = c.fetchone()
    if row:
        pay_id, user_id, unique_amount = row["id"], row["user_id"], row["unique_amount"]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE payments SET status = 'paid', paid_at = %s WHERE id = %s", (now, pay_id))
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (unique_amount, user_id))
        c.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
        new_bal = c.fetchone()["balance"]
        conn.commit()
        put_db(conn)
        try:
            bot.send_message(user_id,
                f"✅ <b>Toʻlov muvaffaqiyatli amalga oshdi!</b>\n\n"
                f"💰 Hisobingiz <b>{format_money(unique_amount)}</b> ga toʻldirildi.\n"
                f"💵 Joriy balans: <b>{format_money(new_bal)}</b>\n\n"
                f"Endi nomer sotib olishingiz mumkin.")
        except Exception as e:
            print("Notify user error:", e)
        for admin_id in ADMIN_IDS:
            try:
                bot.send_message(admin_id, f"💳 <b>Yangi toʻlov</b>\nUser: <code>{user_id}</code>\nSumma: {format_money(unique_amount)}")
            except:
                pass
        print(f"✅ To'lov: user={user_id}, amount={unique_amount}")
        return True
    put_db(conn)
    print(f"⚠️ Mos to'lov topilmadi: {amount}")
    return False

def start_telethon_listener():
    if not API_ID or not API_HASH or not TELETHON_PHONE:
        print("⚠️ Telethon sozlanmagan — avto-to'lov o'chirilgan")
        return
    try:
        from telethon import TelegramClient, events
    except ImportError:
        print("⚠️ telethon o'rnatilmagan")
        return

    async def run_listener():
        client = TelegramClient("humo_session", API_ID, API_HASH)
        await client.start(phone=TELETHON_PHONE)
        print("✅ Telethon ulandi")

        @client.on(events.NewMessage(from_users=HUMO_BOT_USERNAME))
        async def handler(event):
            text = event.message.message or ""
            amount = parse_humo_amount(text)
            if amount:
                process_humo_payment(amount)

        await client.run_until_disconnected()

    def run_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_listener())
        except Exception as e:
            print("Telethon xatosi:", e)

    Thread(target=run_in_thread, daemon=True).start()

def start_expire_checker():
    def worker():
        while True:
            try:
                n = expire_pending_payments()
                if n:
                    print(f"⏰ {n} ta to'lov muddati o'tdi")
            except Exception as e:
                print("Expire error:", e)
            time.sleep(30)
    Thread(target=worker, daemon=True).start()
    print("🔄 Expire checker ishga tushdi")

# ==================== HANDLERS (asosiy) ====================
@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.from_user.id
    username = message.from_user.username or ""
    full_name = message.from_user.full_name or ""
    referred_by = 0
    if len(message.text.split()) > 1:
        try:
            ref_code = message.text.split()[1]
            if ref_code.startswith("ref"):
                referred_by = int(ref_code[3:])
        except:
            pass
    register_user(user_id, username, full_name, referred_by)
    user = get_user(user_id)
    if user and user["is_blocked"] == 1:
        bot.send_message(user_id, "🚫 Siz bloklangansiz.")
        return
    is_sub, missing = check_subscription(user_id)
    if not is_sub:
        text = "⚠️ <b>Kanallarga obuna boʻling:</b>\n\nKeyin «✅ Tekshirish» ni bosing."
        markup = types.InlineKeyboardMarkup()
        for display, url in missing:
            if url:
                markup.add(types.InlineKeyboardButton(f"📢 {display}", url=url))
            else:
                markup.add(types.InlineKeyboardButton(f"📢 {display}", callback_data="no_link"))
        markup.add(types.InlineKeyboardButton("✅ Tekshirish", callback_data="check_sub"))
        bot.send_message(user_id, text, reply_markup=markup)
        return
    if not get_setting("bot_active", True):
        bot.send_message(user_id, "🔴 Bot vaqtincha o'chirilgan.")
        return
    if get_setting("maintenance", False) and not is_admin(user_id):
        bot.send_message(user_id, "🛠 Texnik ishlar olib borilmoqda.")
        return
    bot.send_message(user_id,
        f"🎉 <b>Assalomu alaykum, {full_name}!</b>\n\n📱 Telegram nomerlarini xavfsiz sotib oling.\nMenyudan tanlang:",
        reply_markup=main_menu(user_id))

@bot.callback_query_handler(func=lambda call: call.data == "check_sub")
def check_sub_callback(call):
    is_sub, missing = check_subscription(call.from_user.id)
    if is_sub:
        bot.answer_callback_query(call.id, "✅ Obuna tasdiqlandi!")
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        class FakeMsg:
            def __init__(self, user):
                self.from_user = user
                self.text = "/start"
                self.chat = type('obj', (object,), {'id': user.id})()
        start(FakeMsg(call.from_user))
    else:
        bot.answer_callback_query(call.id, "❌ Hali obuna bo'lmadingiz!", show_alert=True)

@bot.message_handler(func=lambda m: m.text in ["🔙 Orqaga", "🔙 Asosiy menyu"])
def back_to_main(message):
    bot.send_message(message.chat.id, "🏠 Asosiy menyu", reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "🔙 Admin Panel")
def back_to_admin(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🏠 Asosiy menyu", reply_markup=main_menu(message.from_user.id))
        return
    bot.send_message(message.chat.id, "🔐 <b>Admin Panel</b>", reply_markup=admin_menu())

@bot.message_handler(func=lambda m: m.text == "💰 Hisobim")
def my_account(message):
    bot.send_message(message.chat.id, "💰 <b>Hisobim</b>", reply_markup=account_menu())

@bot.message_handler(func=lambda m: m.text == "👥 Referal tizimi")
def referral_system(message):
    bot.send_message(message.chat.id, "👥 <b>Referal tizimi</b>", reply_markup=referral_menu())

@bot.message_handler(func=lambda m: m.text == "📖 Qo'llanma")
def guide(message):
    text = """📖 <b>Bot haqida toʻliq qoʻllanma</b>

🤖 Bu bot Telegram akkaunt ochish uchun virtual telefon raqamlarini xavfsiz sotib olish imkonini beradi.

💰 <b>1. Hisob toʻldirish</b>
• «💰 Hisobim» → «➕ Hisob toʻldirish»
• Unique summa oling → HUMO kartaga tashlang
• Toʻlov avtomatik tasdiqlanadi

📱 <b>2. Nomer sotib olish</b>
• Davlatni tanlang (UZ 🇺🇿 Oʻzbekiston)
• Balans yetarli boʻlsa tasdiqlang
• Raqam + SMS kod keladi

🎁 <b>3. Tekin nomer</b> — referal orqali doʻstlaringizni taklif qiling

⚠️ <b>XAVFSIZLIK:</b>
1. Faqat norasmiy Telegram
2. Darhol 2FA qoʻying
3. Emailni oʻzgartiring
4. Raqamni hech kimga bermang"""
    bot.send_message(message.chat.id, text, reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "⚙️ Sozlamalar")
def settings_user(message):
    bot.send_message(message.chat.id,
        f"⚙️ <b>Sozlamalar</b>\n\n🆔 <code>{message.from_user.id}</code>\n👤 @{message.from_user.username or 'yoq'}",
        reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "💵 Balansim")
def my_balance(message):
    user = get_user(message.from_user.id)
    if not user:
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as n FROM number_orders WHERE user_id = %s", (message.from_user.id,))
    orders = c.fetchone()["n"]
    c.execute("SELECT COUNT(*) as n FROM payments WHERE user_id = %s AND status = 'paid'", (message.from_user.id,))
    pays = c.fetchone()["n"]
    put_db(conn)
    text = (f"💵 <b>Balans va statistika</b>\n\n"
            f"💰 Asosiy: <b>{format_money(user['balance'])}</b>\n"
            f"🎁 Referal: <b>{format_money(user['referral_balance'])}</b>\n"
            f"📱 Bepul: <b>{user['free_numbers']} ta</b>\n"
            f"👥 Referallar: <b>{user['referrals_count']}</b>\n"
            f"📋 Buyurtmalar: <b>{orders}</b>\n"
            f"✅ Toʻlovlar: <b>{pays}</b>")
    bot.send_message(message.chat.id, text, reply_markup=account_menu())

@bot.message_handler(func=lambda m: m.text == "➕ Hisob to'ldirish")
def top_up(message):
    start = get_setting("start_amount", 5000)
    msg = bot.send_message(message.chat.id,
        f"➕ <b>Hisob toʻldirish</b>\n\nQancha summa?\nMasalan: <code>{start}</code>\n\n/cancel — bekor",
        reply_markup=back_only())
    bot.register_next_step_handler(msg, process_top_up_amount)

def _payment_text(amount, card, owner, note, remain_sec):
    """To'lov xabari — jonli taymer bilan"""
    if remain_sec < 0:
        remain_sec = 0
    mm = remain_sec // 60
    ss = remain_sec % 60
    bar_len = 10
    total = max(1, remain_sec)  # faqat ko'rinish
    # progress bar (qolgan vaqtga nisbatan taxminiy)
    filled = max(0, min(bar_len, int((remain_sec / max(1, mm * 60 + ss or 1)) * bar_len))) if False else None
    # Oddiy visual: daqiqa:soniya
    timer = f"{mm:02d}:{ss:02d}"
    if remain_sec <= 0:
        timer_line = "⏰ <b>Vaqt tugadi!</b>"
    elif remain_sec <= 60:
        timer_line = f"⏱ Qoldi: <b>{timer}</b> ⚠️ shoshiling!"
    else:
        timer_line = f"⏱ Qoldi: <b>{timer}</b>"
    return f"""➕ <b>Hisob toʻldirish</b>
{note}
💳 <b>Summa:</b> <code>{int(amount)}</code> soʻm
🏦 <b>Karta:</b> <code>{card}</code>
👤 <b>Egasi:</b> {owner}
{timer_line}

⚠️ Aynan shu summani tashlang!
Vaqt tugasa toʻlov avtomatik bekor boʻladi."""


def start_payment_timer(chat_id, message_id, user_id, amount, card, owner, note, expires_dt, markup):
    """Har 15 soniyada xabarni yangilab, vaqt tugaganda xabar beradi"""
    def worker():
        while True:
            remain = int((expires_dt - datetime.now()).total_seconds())
            # To'lov allaqachon paid bo'lsa to'xtatish
            conn = get_db()
            try:
                c = conn.cursor()
                c.execute("""SELECT status FROM payments
                             WHERE user_id = %s AND unique_amount = %s AND status = 'pending'
                             ORDER BY id DESC LIMIT 1""", (user_id, amount))
                row = c.fetchone()
            finally:
                put_db(conn)
            if not row:
                # pending yo'q — to'langan yoki expired
                return
            if remain <= 0:
                try:
                    bot.edit_message_text(
                        f"⏰ <b>Toʻlov vaqti tugadi!</b>\n\n"
                        f"💰 Summa: <b>{format_money(amount)}</b>\n"
                        f"Yangi toʻlov uchun «➕ Hisob toʻldirish» ni bosing.",
                        chat_id, message_id
                    )
                except Exception:
                    pass
                expire_pending_payments()
                return
            text = _payment_text(amount, card, owner, note, remain)
            try:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
            except Exception:
                pass
            # Oxirgi daqiqada tezroq yangilash
            time.sleep(10 if remain <= 60 else 15)

    Thread(target=worker, daemon=True).start()


def process_top_up_amount(message):
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "Bekor.", reply_markup=account_menu())
        return
    try:
        desired = int(str(message.text).replace(" ", "").replace(",", "").replace(".", ""))
        if desired < 1000:
            bot.send_message(message.chat.id, "Minimal 1000 soʻm.", reply_markup=back_only())
            bot.register_next_step_handler(message, process_top_up_amount)
            return
    except Exception:
        bot.send_message(message.chat.id, "Faqat raqam (masalan 5000):", reply_markup=back_only())
        bot.register_next_step_handler(message, process_top_up_amount)
        return

    expire_pending_payments()
    user_id = message.from_user.id
    amount = get_unique_amount(desired)
    minutes = int(get_setting("payment_time_minutes", 15))
    card = get_setting("card_number", "8600123456789012")
    owner = get_setting("card_owner", "HUMO CARD")
    now_dt = datetime.now()
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    expires_dt = now_dt + timedelta(minutes=minutes)
    expires = expires_dt.strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO payments (user_id, amount, unique_amount, status, created_at, expires_at)
                     VALUES (%s, %s, %s, 'pending', %s, %s)""", (user_id, amount, amount, now, expires))
        conn.commit()
    finally:
        put_db(conn)

    note = ""
    if amount != desired:
        note = f"\n⚠️ {format_money(desired)} band edi. Sizga: <b>{format_money(amount)}</b>\n"

    remain = minutes * 60
    text = _payment_text(amount, card, owner, note, remain)

    markup = types.InlineKeyboardMarkup(row_width=1)
    try:
        markup.add(types.InlineKeyboardButton(f"📋 Summa ({int(amount)})", copy_text=types.CopyTextButton(text=str(int(amount)))))
        markup.add(types.InlineKeyboardButton("📋 Karta", copy_text=types.CopyTextButton(text=str(card))))
    except Exception:
        markup.add(types.InlineKeyboardButton("📋 Summa", callback_data=f"copy_amt_{int(amount)}"))
        markup.add(types.InlineKeyboardButton("📋 Karta", callback_data="copy_card"))

    sent = bot.send_message(message.chat.id, text, reply_markup=markup)
    bot.send_message(message.chat.id, "Toʻlov qilgach balans avtomatik toʻldiriladi.", reply_markup=account_menu())
    # Jonli taymer
    start_payment_timer(message.chat.id, sent.message_id, user_id, amount, card, owner, note, expires_dt, markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_amt_") or call.data == "copy_card")
def copy_fallback(call):
    if call.data.startswith("copy_amt_"):
        amt = call.data.replace("copy_amt_", "")
        bot.answer_callback_query(call.id, f"Summa: {amt}", show_alert=True)
        bot.send_message(call.from_user.id, f"<code>{amt}</code>")
    else:
        card = get_setting("card_number", "")
        bot.send_message(call.from_user.id, f"<code>{card}</code>")
        bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda m: m.text == "📜 To'lov tarixi")
def payment_history(message):
    expire_pending_payments()
    conn = get_db()
    c = conn.cursor()
    uid = message.from_user.id
    c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE user_id = %s AND status = 'paid'", (uid,))
    paid = c.fetchone()
    c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE user_id = %s AND status = 'pending'", (uid,))
    pend = c.fetchone()
    c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE user_id = %s AND status = 'expired'", (uid,))
    exp = c.fetchone()
    c.execute("SELECT unique_amount, status, created_at FROM payments WHERE user_id = %s ORDER BY id DESC LIMIT 12", (uid,))
    rows = c.fetchall()
    put_db(conn)
    text = (f"📜 <b>Toʻlov tarixi</b>\n\n"
            f"✅ Toʻlangan: <b>{paid['n']}</b> — {format_money(paid['s'])}\n"
            f"⏳ Kutilmoqda: <b>{pend['n']}</b> — {format_money(pend['s'])}\n"
            f"⏰ Tugagan: <b>{exp['n']}</b> — {format_money(exp['s'])}\n\n📋 Oxirgilar:\n")
    if not rows:
        text += "Hali yoʻq."
    else:
        for r in rows:
            st = {"paid": "✅", "expired": "⏰", "pending": "⏳"}.get(r["status"], "❓")
            text += f"{st} {format_money(r['unique_amount'])} | {r['created_at']}\n"
    bot.send_message(message.chat.id, text[:4000], reply_markup=account_menu())

# Referal
@bot.message_handler(func=lambda m: m.text == "🔗 Mening referal havolam")
def my_ref_link(message):
    user_id = message.from_user.id
    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref{user_id}"
    user = get_user(user_id)
    bonus = get_setting("referral_bonus", 500)
    percent = get_setting("referral_percent", 10)
    needed = get_setting("free_number_referrals", 20)
    text = f"""🔗 <b>Referal havola:</b>

<code>{link}</code>

• Har bir haqiqiy doʻst → <b>{format_money(bonus)}</b>
• Doʻst nomer olsa → <b>{percent}%</b>
• {needed} ta doʻst → 1 ta tekin nomer

👥 Hozir: <b>{user['referrals_count'] if user else 0}</b> ta"""
    bot.send_message(message.chat.id, text, reply_markup=referral_menu())

@bot.message_handler(func=lambda m: m.text == "👤 Taklif qilganlarim")
def my_referrals(message):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT full_name, username, joined_date FROM users WHERE referred_by = %s ORDER BY joined_date DESC LIMIT 20",
              (message.from_user.id,))
    rows = c.fetchall()
    put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "👤 Hali yoʻq.", reply_markup=referral_menu())
        return
    text = "👤 <b>Takliflar:</b>\n\n"
    for r in rows:
        text += f"• {r['full_name']} (@{r['username'] or 'yoq'}) — {r['joined_date']}\n"
    bot.send_message(message.chat.id, text, reply_markup=referral_menu())

@bot.message_handler(func=lambda m: m.text == "💸 Referal balansi")
def ref_balance(message):
    user = get_user(message.from_user.id)
    bot.send_message(message.chat.id, f"💸 Referal: <b>{format_money(user['referral_balance'] if user else 0)}</b>",
                     reply_markup=referral_menu())

@bot.message_handler(func=lambda m: m.text == "📊 Referal statistikasi")
def ref_stats(message):
    user = get_user(message.from_user.id)
    needed = get_setting("free_number_referrals", 20)
    text = f"📊 Referallar: <b>{user['referrals_count'] if user else 0}</b>\n📱 Bepul: <b>{user['free_numbers'] if user else 0}</b>\n🎁 Kerakli: <b>{needed}</b>"
    bot.send_message(message.chat.id, text, reply_markup=referral_menu())

# Nomer sotib olish
@bot.message_handler(func=lambda m: m.text and ("Nomer sotib olish" in m.text or m.text == "📱 Nomer sotib olish"))
def select_country(message):
    """User uchun davlatlar ro'yxati — ishonchli"""
    chat_id = message.chat.id
    try:
        wait = bot.send_message(chat_id, "⏳ Davlatlar yuklanmoqda...")
        countries = get_active_countries()
        if not countries:
            text = (
                "❌ <b>Hozircha davlatlar yoʻq.</b>\n\n"
                "Admin «🔄 dogesms dan yangilash» ni bossin.\n"
                "Yoki «💵 Nomer narxini belgilash» orqali qoʻlda qoʻshsin."
            )
            if is_admin(message.from_user.id):
                text += "\n\n👉 Admin: Nomerlar boshqaruvi → dogesms dan yangilash"
            try:
                bot.edit_message_text(text, chat_id, wait.message_id)
            except Exception:
                bot.send_message(chat_id, text, reply_markup=main_menu(message.from_user.id))
            return
        markup, total, page = build_countries_keyboard(0)
        text = f"🌍 <b>Telegram nomerlari</b>\nJami: <b>{total}</b> ta davlat\n\nDavlatni tanlang:"
        try:
            bot.edit_message_text(text, chat_id, wait.message_id, reply_markup=markup)
        except Exception:
            bot.send_message(chat_id, text, reply_markup=markup)
    except Exception as e:
        print("select_country error:", e)
        bot.send_message(chat_id, f"❌ Xatolik: {e}\nQayta urinib koʻring.",
                         reply_markup=main_menu(message.from_user.id))

@bot.callback_query_handler(func=lambda call: call.data.startswith("cpage_"))
def countries_page(call):
    page = int(call.data.replace("cpage_", ""))
    markup, total, page = build_countries_keyboard(page)
    try:
        bot.edit_message_text(f"🌍 <b>Telegram nomerlari</b>\nJami: <b>{total}</b>\nSahifa: {page+1}",
                              call.message.chat.id, call.message.message_id, reply_markup=markup)
    except:
        pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "close_countries")
def close_countries(call):
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_c_"))
def process_buy_country(call):
    country_code = call.data.replace("buy_c_", "")
    user_id = call.from_user.id
    user = get_user(user_id)
    if not user or user["is_blocked"] == 1:
        bot.answer_callback_query(call.id, "Bloklangansiz.", show_alert=True)
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT country_name, price, COALESCE(flag,'') as flag FROM country_prices WHERE country_code = %s AND is_active = 1",
              (country_code,))
    row = c.fetchone()
    put_db(conn)
    if not row:
        bot.answer_callback_query(call.id, "Topilmadi.", show_alert=True)
        return
    if user["balance"] < row["price"]:
        bot.answer_callback_query(call.id, f"Balans yetarli emas! Kerak: {format_money(row['price'])}", show_alert=True)
        return
    fl = row["flag"] or get_flag(country_code)
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"confirm_buy_{country_code}"),
        types.InlineKeyboardButton("❌ Bekor", callback_data="cancel_buy")
    )
    bot.edit_message_text(
        f"{fl} <b>{row['country_name']}</b>\n💵 Narxi: <b>{format_money(row['price'])}</b>\n\nSotib olasizmi?",
        call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "cancel_buy")
def cancel_buy(call):
    bot.edit_message_text("❌ Bekor qilindi.", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_buy_"))
def confirm_buy(call):
    country_code = call.data.replace("confirm_buy_", "")
    user_id = call.from_user.id
    user = get_user(user_id)
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT country_name, price, COALESCE(flag,'') as flag FROM country_prices WHERE country_code = %s", (country_code,))
    row = c.fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Xatolik.", show_alert=True)
        put_db(conn)
        return
    country_name, price, flag = row["country_name"], row["price"], row["flag"]
    fl = flag or get_flag(country_code)
    if user["balance"] < price:
        bot.answer_callback_query(call.id, "Balans yetarli emas!", show_alert=True)
        put_db(conn)
        return
    c.execute("UPDATE users SET balance = balance - %s WHERE user_id = %s", (price, user_id))
    conn.commit()
    bot.edit_message_text(f"⏳ {fl} Nomer olinmoqda...", call.message.chat.id, call.message.message_id)

    service = get_setting("default_service", "telegram")
    order = dogesms_create_order(service, country_code)
    if not order:
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (price, user_id))
        conn.commit()
        put_db(conn)
        bot.send_message(user_id, "❌ Nomer olinmadi. Pul qaytarildi.", reply_markup=main_menu(user_id))
        return

    order_id = order.get("id")
    phone_number = None
    sms_code = None
    for _ in range(18):
        time.sleep(5)
        order_data = dogesms_get_order(order_id)
        if order_data:
            phone_number = order_data.get("phone_number") or phone_number
            sms_code = order_data.get("sms_code") or order_data.get("code") or sms_code
            status = order_data.get("status")
            if phone_number and (sms_code or status == "completed"):
                break
            if status in ("failed", "expired", "cancelled"):
                break

    if not phone_number:
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (price, user_id))
        conn.commit()
        put_db(conn)
        bot.send_message(user_id, "❌ Nomer olinmadi. Pulingiz qaytarildi.", reply_markup=main_menu(user_id))
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT INTO numbers (country, country_code, number, price, status, sold_to, sold_at, dogesms_order_id, created_at)
                 VALUES (%s, %s, %s, %s, 'sold', %s, %s, %s, %s) RETURNING id""",
              (country_name, country_code, phone_number, price, user_id, now, order_id, now))
    number_id = c.fetchone()["id"]
    c.execute("""INSERT INTO number_orders (user_id, number_id, number, country, price, is_free, created_at)
                 VALUES (%s, %s, %s, %s, %s, 0, %s)""", (user_id, number_id, phone_number, country_name, price, now))
    conn.commit()
    put_db(conn)

    if user["referred_by"]:
        percent = get_setting("referral_percent", 10)
        bonus = price * percent / 100
        update_balance(user["referred_by"], bonus, is_referral=True)
        try:
            bot.send_message(user["referred_by"], f"💰 Referal toʻlov: <b>{format_money(bonus)}</b> ({percent}%)")
        except:
            pass

    code_line = f"\n🔐 SMS kod: <code>{sms_code}</code>" if sms_code else "\n🔐 SMS kod hali kelmadi — «Kodni yangilash»."
    text = f"""✅ <b>Nomer berildi!</b>

📱 <code>{phone_number}</code>
{fl} {country_code} {country_name}
💵 {format_money(price)}{code_line}

⚠️ <b>MUHIM:</b>
1. Faqat norasmiy Telegram
2. Darhol 2FA qoʻying
3. Emailni oʻzgartiring"""
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("🔄 Kodni yangilash", callback_data=f"refresh_code_{order_id}"))
    markup.add(types.InlineKeyboardButton("⭐ Saqlash", callback_data=f"save_num_{phone_number}|{country_code}|{country_name}"))
    bot.send_message(user_id, text, reply_markup=markup)
    bot.send_message(user_id, "Asosiy menyu", reply_markup=main_menu(user_id))

@bot.callback_query_handler(func=lambda call: call.data.startswith("refresh_code_"))
def refresh_sms_code(call):
    order_id = call.data.replace("refresh_code_", "")
    order_data = dogesms_get_order(order_id)
    if not order_data:
        bot.answer_callback_query(call.id, "Order topilmadi.", show_alert=True)
        return
    code = order_data.get("sms_code") or order_data.get("code")
    phone = order_data.get("phone_number") or ""
    if code:
        bot.answer_callback_query(call.id, f"Kod: {code}")
        bot.send_message(call.from_user.id, f"🔄 <b>Kod</b>\n📱 <code>{phone}</code>\n🔐 <code>{code}</code>")
    else:
        bot.answer_callback_query(call.id, "Kod hali kelmagan.", show_alert=True)

# Tekin nomer, buyurtmalar, saqlangan — qisqartirilgan lekin to'liq ishlaydigan
@bot.message_handler(func=lambda m: m.text == "🎁 Tekin nomer")
def free_number(message):
    user = get_user(message.from_user.id)
    if not user:
        return
    needed = get_setting("free_number_referrals", 20)
    rights = user["free_numbers"]
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT country_code, country_name, COALESCE(flag,'') as flag FROM free_countries WHERE is_active = 1 ORDER BY country_name")
    rows = c.fetchall()
    put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "❌ Tekin davlatlar yoʻq.", reply_markup=main_menu(message.from_user.id))
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        fl = r["flag"] or get_flag(r["country_code"])
        markup.add(types.InlineKeyboardButton(f"{fl} {r['country_name']}", callback_data=f"free_c_{r['country_code']}"))
    status = f"✅ Huquq: <b>{rights}</b> ta" if rights >= 1 else f"❌ Huquq yoʻq. {needed} ta doʻst kerak (hozir: {user['referrals_count']})"
    bot.send_message(message.chat.id, f"🎁 <b>Tekin nomer</b>\n\n{status}", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("free_c_"))
def take_free_by_country(call):
    country_code = call.data.replace("free_c_", "")
    user_id = call.from_user.id
    user = get_user(user_id)
    if not user or user["free_numbers"] < 1:
        bot.answer_callback_query(call.id, "Huquq yetarli emas!", show_alert=True)
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT country_name, COALESCE(flag,'') as flag FROM free_countries WHERE country_code = %s AND is_active = 1", (country_code,))
    row = c.fetchone()
    if not row:
        put_db(conn)
        bot.answer_callback_query(call.id, "Topilmadi.", show_alert=True)
        return
    c.execute("UPDATE users SET free_numbers = free_numbers - 1 WHERE user_id = %s", (user_id,))
    conn.commit()
    bot.edit_message_text("⏳ Tekin nomer olinmoqda...", call.message.chat.id, call.message.message_id)
    order = dogesms_create_order(get_setting("default_service", "telegram"), country_code)
    if not order:
        c.execute("UPDATE users SET free_numbers = free_numbers + 1 WHERE user_id = %s", (user_id,))
        conn.commit()
        put_db(conn)
        bot.send_message(user_id, "❌ Olinmadi. Huquq qaytarildi.", reply_markup=main_menu(user_id))
        return
    order_id = order.get("id")
    phone_number = None
    for _ in range(12):
        time.sleep(5)
        od = dogesms_get_order(order_id)
        if od:
            phone_number = od.get("phone_number")
            if phone_number or od.get("status") in ("failed", "expired", "cancelled"):
                break
    if not phone_number:
        c.execute("UPDATE users SET free_numbers = free_numbers + 1 WHERE user_id = %s", (user_id,))
        conn.commit()
        put_db(conn)
        bot.send_message(user_id, "❌ Olinmadi. Huquq qaytarildi.", reply_markup=main_menu(user_id))
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fl = row["flag"] or get_flag(country_code)
    c.execute("""INSERT INTO numbers (country, country_code, number, price, status, sold_to, sold_at, dogesms_order_id, created_at)
                 VALUES (%s, %s, %s, 0, 'sold', %s, %s, %s, %s) RETURNING id""",
              (row["country_name"], country_code, phone_number, user_id, now, order_id, now))
    nid = c.fetchone()["id"]
    c.execute("""INSERT INTO number_orders (user_id, number_id, number, country, price, is_free, created_at)
                 VALUES (%s, %s, %s, %s, 0, 1, %s)""", (user_id, nid, phone_number, row["country_name"], now))
    conn.commit()
    put_db(conn)
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⭐ Saqlash", callback_data=f"save_num_{phone_number}|{country_code}|{row['country_name']}"))
    bot.send_message(user_id,
        f"🎁 <b>Tekin nomer!</b>\n\n📱 <code>{phone_number}</code>\n{fl} {country_code} {row['country_name']}\n\n⚠️ Norasmiy TG + 2FA + email!",
        reply_markup=markup)
    bot.send_message(user_id, "Asosiy menyu", reply_markup=main_menu(user_id))

@bot.message_handler(func=lambda m: m.text == "📋 Buyurtmalarim")
def my_orders(message):
    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT number, country, price, is_free, created_at FROM number_orders
                 WHERE user_id = %s ORDER BY id DESC LIMIT 20""", (message.from_user.id,))
    rows = c.fetchall()
    put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📋 Buyurtmalar yoʻq.", reply_markup=main_menu(message.from_user.id))
        return
    text = "📋 <b>Buyurtmalar:</b>\n\n"
    for r in rows:
        tip = "🎁 Tekin" if r["is_free"] else f"💵 {format_money(r['price'])}"
        text += f"📱 <code>{r['number']}</code>\n🌍 {r['country']} | {tip}\n📅 {r['created_at']}\n\n"
    bot.send_message(message.chat.id, text[:4000], reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "⭐ Saqlangan nomerlar")
def saved_numbers(message):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id, number, country, country_code, created_at FROM saved_numbers WHERE user_id = %s ORDER BY id DESC LIMIT 30",
              (message.from_user.id,))
    rows = c.fetchall()
    put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "⭐ Saqlangan yoʻq. Nomer olgach «⭐ Saqlash» bosing.",
                         reply_markup=main_menu(message.from_user.id))
        return
    text = "⭐ <b>Saqlangan:</b>\n\n"
    markup = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        fl = get_flag(r["country_code"])
        text += f"#{r['id']} 📱 <code>{r['number']}</code>\n{fl} {r['country_code']} {r['country']}\n\n"
        markup.add(types.InlineKeyboardButton(f"🗑 #{r['id']}", callback_data=f"del_saved_{r['id']}"))
    bot.send_message(message.chat.id, text[:4000], reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("save_num_"))
def save_number_cb(call):
    try:
        parts = call.data.replace("save_num_", "").split("|")
        number, code, country = parts[0], parts[1] if len(parts) > 1 else "", parts[2] if len(parts) > 2 else ""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = get_db()
        c = conn.cursor()
        c.execute("""INSERT INTO saved_numbers (user_id, number, country, country_code, created_at)
                     VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id, number) DO NOTHING""",
                  (call.from_user.id, number, country, code, now))
        n = c.rowcount
        conn.commit()
        put_db(conn)
        bot.answer_callback_query(call.id, "✅ Saqlandi!" if n else "Allaqachon saqlangan.", show_alert=True)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Xato: {e}", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_saved_"))
def del_saved_cb(call):
    sid = int(call.data.replace("del_saved_", ""))
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM saved_numbers WHERE id = %s AND user_id = %s", (sid, call.from_user.id))
    n = c.rowcount
    conn.commit()
    put_db(conn)
    bot.answer_callback_query(call.id, "✅ Oʻchirildi" if n else "Topilmadi")

# Admin xabar + javob
@bot.message_handler(func=lambda m: m.text == "📞 Adminga yozish")
def write_admin(message):
    msg = bot.send_message(message.chat.id, "✉️ Xabaringizni yuboring (matn/rasm/video):\n/cancel — bekor")
    bot.register_next_step_handler(msg, process_write_admin)

def process_write_admin(message):
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=main_menu(message.from_user.id))
        return
    user = message.from_user
    header = f"📩 <b>User xabari</b>\nFrom: {user.full_name} (@{user.username or 'yoq'})\nID: <code>{user.id}</code>\n"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💬 Javob berish", callback_data=f"reply_user_{user.id}"))
    for aid in ADMIN_IDS:
        try:
            if message.content_type == "text":
                bot.send_message(aid, header + f"\n{message.text}", reply_markup=markup)
            else:
                bot.send_message(aid, header, reply_markup=markup)
                bot.copy_message(aid, message.chat.id, message.message_id)
        except:
            pass
    bot.send_message(message.chat.id, "✅ Yuborildi.", reply_markup=main_menu(message.from_user.id))

@bot.callback_query_handler(func=lambda call: call.data.startswith("reply_user_"))
def admin_reply_start(call):
    if not is_admin(call.from_user.id):
        return
    target_id = int(call.data.replace("reply_user_", ""))
    msg = bot.send_message(call.from_user.id, f"💬 <code>{target_id}</code> ga javob:\n/cancel — bekor")
    bot.register_next_step_handler(msg, lambda m: process_admin_reply(m, target_id))
    bot.answer_callback_query(call.id)

def process_admin_reply(message, target_id):
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=admin_menu())
        return
    try:
        if message.content_type == "text":
            bot.send_message(target_id, f"💬 <b>Admin javobi:</b>\n\n{message.text}")
        else:
            bot.send_message(target_id, "💬 <b>Admin javobi:</b>")
            bot.copy_message(target_id, message.chat.id, message.message_id)
        bot.send_message(message.chat.id, f"✅ Yuborildi → <code>{target_id}</code>", reply_markup=admin_menu())
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ {e}", reply_markup=admin_menu())

# Admin panel
@bot.message_handler(func=lambda m: m.text == "🔐 Admin Panel")
def admin_panel(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, "🔐 <b>Admin Panel</b>", reply_markup=admin_menu())

@bot.message_handler(func=lambda m: m.text == "📊 Statistika")
def admin_stats(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as n FROM users")
    total = c.fetchone()["n"]
    c.execute("SELECT COUNT(*) as n FROM users WHERE username IS NOT NULL AND username != ''")
    real = c.fetchone()["n"]
    c.execute("SELECT COUNT(*) as n FROM payments WHERE status = 'paid'")
    pays = c.fetchone()["n"]
    c.execute("SELECT COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status = 'paid'")
    psum = c.fetchone()["s"]
    c.execute("SELECT COUNT(*) as n FROM numbers WHERE status = 'sold'")
    sold = c.fetchone()["n"]
    put_db(conn)
    doge = dogesms_balance()
    doge_t = f"{doge:.2f} USD" if doge is not None else "API yoʻq"
    text = f"""📊 <b>Statistika</b>

👥 Userlar: <b>{total}</b> (haqiqiy: {real})
💰 Toʻlovlar: <b>{pays}</b> — {format_money(psum)}
📱 Sotilgan: <b>{sold}</b>
🐕 dogesms: <b>{doge_t}</b>"""
    bot.send_message(message.chat.id, text, reply_markup=admin_menu())

@bot.message_handler(func=lambda m: m.text == "📱 Nomerlar boshqaruvi")
def numbers_admin(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, "📱 <b>Nomerlar</b>", reply_markup=numbers_admin_menu())

@bot.message_handler(func=lambda m: m.text == "🌐 Barcha davlat kodlari")
def all_country_codes(message):
    if not is_admin(message.from_user.id):
        return
    items = sorted(COUNTRY_FLAGS.items(), key=lambda x: x[0])
    for i in range(0, len(items), 40):
        chunk = items[i:i+40]
        text = f"🌐 <b>Kodlar</b> ({i+1}–{i+len(chunk)}/{len(items)})\n\n"
        for code, flag in chunk:
            text += f"<code>{code}</code> {flag} {get_uz_name(code)}\n"
        bot.send_message(message.chat.id, text)
    bot.send_message(message.chat.id, "💵 Narx: <code>IN 15000</code>", reply_markup=numbers_admin_menu())

@bot.message_handler(func=lambda m: m.text == "🔄 dogesms dan yangilash")
def sync_countries_admin(message):
    if not is_admin(message.from_user.id):
        return
    chat_id = message.chat.id
    bot.send_message(chat_id, "⏳ Yuklanmoqda...")

    def worker():
        try:
            n = sync_dogesms_countries()
            bot.send_message(chat_id, f"✅ <b>{n}</b> ta davlat yuklandi!" if n else "❌ Hech narsa kelmadi.",
                             reply_markup=numbers_admin_menu())
        except Exception as e:
            bot.send_message(chat_id, f"❌ {e}", reply_markup=numbers_admin_menu())
    Thread(target=worker, daemon=True).start()

@bot.message_handler(func=lambda m: m.text == "🔄 dogesms balansi")
def check_dogesms(message):
    if not is_admin(message.from_user.id):
        return
    bal = dogesms_balance()
    bot.send_message(message.chat.id,
                     f"🐕 dogesms: <b>{bal:.2f} USD</b>" if bal is not None else "❌ API xato",
                     reply_markup=numbers_admin_menu())

@bot.message_handler(func=lambda m: m.text == "💵 Nomer narxini belgilash")
def set_price(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Format: <code>IN 15000</code> yoki <code>UZ Oʻzbekiston 12000</code>", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_set_price)

def process_set_price(message):
    if not is_admin(message.from_user.id):
        return
    if message.text == "🔙 Orqaga":
        bot.send_message(message.chat.id, "📱", reply_markup=numbers_admin_menu())
        return
    parts = message.text.strip().split()
    try:
        if len(parts) == 2:
            code, price = parts[0].upper(), float(parts[1])
            name = get_uz_name(code, code)
            flag = get_flag(code)
            conn = get_db()
            c = conn.cursor()
            c.execute("""INSERT INTO country_prices (country_code, country_name, price, flag, is_active, source)
                         VALUES (%s, %s, %s, %s, 1, 'manual')
                         ON CONFLICT (country_code) DO UPDATE SET price = EXCLUDED.price, flag = EXCLUDED.flag""",
                      (code, name, price, flag))
            conn.commit()
            put_db(conn)
            bot.send_message(message.chat.id, f"✅ {code} {flag} {name} — {format_money(price)}", reply_markup=numbers_admin_menu())
        elif len(parts) >= 3:
            code = parts[0].upper()
            price = float(parts[-1])
            name = " ".join(parts[1:-1])
            flag = get_flag(code)
            conn = get_db()
            c = conn.cursor()
            c.execute("""INSERT INTO country_prices (country_code, country_name, price, flag, is_active, source)
                         VALUES (%s, %s, %s, %s, 1, 'manual')
                         ON CONFLICT (country_code) DO UPDATE SET price = EXCLUDED.price, country_name = EXCLUDED.country_name, flag = EXCLUDED.flag""",
                      (code, name, price, flag))
            conn.commit()
            put_db(conn)
            bot.send_message(message.chat.id, f"✅ {code} {flag} {name} — {format_money(price)}", reply_markup=numbers_admin_menu())
        else:
            bot.send_message(message.chat.id, "Notoʻgʻri format.", reply_markup=numbers_admin_menu())
    except Exception as e:
        bot.send_message(message.chat.id, f"Xato: {e}", reply_markup=numbers_admin_menu())

@bot.message_handler(func=lambda m: m.text == "📢 Xabar yuborish")
def broadcast(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "📢 Xabar yuboring (matn/rasm/video):\n/cancel", reply_markup=types.ReplyKeyboardRemove())
    bot.register_next_step_handler(msg, process_broadcast)

def process_broadcast(message):
    if not is_admin(message.from_user.id):
        return
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=admin_menu())
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE is_blocked = 0")
    users = c.fetchall()
    put_db(conn)
    ok = fail = 0
    for u in users:
        try:
            if message.content_type == "text":
                bot.send_message(u["user_id"], message.text)
            else:
                bot.copy_message(u["user_id"], message.chat.id, message.message_id)
            ok += 1
            time.sleep(0.04)
        except:
            fail += 1
    bot.send_message(message.chat.id, f"✅ {ok} | ❌ {fail}", reply_markup=admin_menu())

# Qolgan admin sozlamalari (qisqa)
@bot.message_handler(func=lambda m: m.text == "⏱ To'lov sozlamalari")
def payment_settings(message):
    if not is_admin(message.from_user.id):
        return
    text = (f"⏱ Vaqt: {get_setting('payment_time_minutes', 15)} daq\n"
            f"💵 Start: {format_money(get_setting('start_amount', 5000))}\n"
            f"💳 {get_setting('card_number')}\n👤 {get_setting('card_owner')}")
    bot.send_message(message.chat.id, text, reply_markup=payment_settings_menu())

@bot.message_handler(func=lambda m: m.text == "⏱ To'lov vaqtini o'zgartirish")
def change_payment_time(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Daqiqa (1-120):", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "payment_time_minutes", payment_settings_menu))

@bot.message_handler(func=lambda m: m.text == "💵 Boshlang'ich summa")
def change_start_amount(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Yangi summa:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "start_amount", payment_settings_menu))

@bot.message_handler(func=lambda m: m.text == "💳 Karta raqami")
def change_card(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Yangi karta:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_str_setting(m, "card_number", payment_settings_menu))

@bot.message_handler(func=lambda m: m.text == "👤 Karta egasi")
def change_owner(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Yangi ism:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_str_setting(m, "card_owner", payment_settings_menu))

def _set_int_setting(message, key, menu_func):
    if not is_admin(message.from_user.id):
        return
    if message.text == "🔙 Orqaga":
        bot.send_message(message.chat.id, "Orqaga", reply_markup=menu_func())
        return
    try:
        set_setting(key, int(message.text))
        bot.send_message(message.chat.id, f"✅ {message.text}", reply_markup=menu_func())
    except:
        bot.send_message(message.chat.id, "Faqat son.", reply_markup=menu_func())

def _set_str_setting(message, key, menu_func):
    if not is_admin(message.from_user.id):
        return
    if message.text == "🔙 Orqaga":
        bot.send_message(message.chat.id, "Orqaga", reply_markup=menu_func())
        return
    set_setting(key, message.text.strip())
    bot.send_message(message.chat.id, f"✅ {message.text.strip()}", reply_markup=menu_func())

@bot.message_handler(func=lambda m: m.text == "🎁 Referal sozlamalari")
def ref_settings(message):
    if not is_admin(message.from_user.id):
        return
    text = f"📊 Foiz: {get_setting('referral_percent', 10)}%\n💰 Bonus: {format_money(get_setting('referral_bonus', 500))}\n🎁 Kerakli: {get_setting('free_number_referrals', 20)}"
    bot.send_message(message.chat.id, text, reply_markup=referral_settings_menu())

@bot.message_handler(func=lambda m: m.text == "📊 Referal foizi")
def change_ref_p(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Foiz:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "referral_percent", referral_settings_menu))

@bot.message_handler(func=lambda m: m.text == "💰 1 referal uchun pul")
def change_ref_b(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Summa:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "referral_bonus", referral_settings_menu))

@bot.message_handler(func=lambda m: m.text == "🎁 Bepul nomer uchun soni")
def change_free_n(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Soni:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "free_number_referrals", referral_settings_menu))

@bot.message_handler(func=lambda m: m.text == "🟢 Botni yoqish")
def enable_bot(message):
    if is_admin(message.from_user.id):
        set_setting("bot_active", True)
        bot.send_message(message.chat.id, "✅ Yoqildi.", reply_markup=other_settings_menu())

@bot.message_handler(func=lambda m: m.text == "🔴 Botni o'chirish")
def disable_bot(message):
    if is_admin(message.from_user.id):
        set_setting("bot_active", False)
        bot.send_message(message.chat.id, "🔴 Oʻchirildi.", reply_markup=other_settings_menu())

@bot.message_handler(func=lambda m: m.text == "⚙️ Boshqa sozlamalar")
def other_settings(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, "⚙️", reply_markup=other_settings_menu())

@bot.message_handler(func=lambda m: m.text == "👥 Foydalanuvchilar")
def users_admin(message):
    if is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "👥 <b>Foydalanuvchilar</b>", reply_markup=users_admin_menu())

@bot.message_handler(func=lambda m: m.text and ("Userlar ro" in m.text or "Userlar roʻyxati" in m.text or "Userlar ro'yxati" in m.text))
def list_users_admin(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT user_id, full_name, username, balance, referrals_count, is_blocked, joined_date
                     FROM users ORDER BY joined_date DESC LIMIT 15""")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "Userlar yoʻq.", reply_markup=users_admin_menu())
        return
    text = "📋 <b>Oxirgi 15 ta user</b>\n\n"
    markup = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        st = "🚫" if r["is_blocked"] else "✅"
        un = f"@{r['username']}" if r["username"] else "—"
        text += f"{st} <code>{r['user_id']}</code> | {r['full_name']} ({un})\n💰 {format_money(r['balance'])}\n"
        markup.add(types.InlineKeyboardButton(f"{st} {r['user_id']} — {(r['full_name'] or '')[:18]}",
                                              callback_data=f"admin_user_{r['user_id']}"))
    bot.send_message(message.chat.id, text[:3500], reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_user_"))
def admin_user_actions(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("admin_user_", ""))
    user = get_user(uid)
    if not user:
        bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
        return
    st = "🚫 Blok" if user["is_blocked"] else "✅ Faol"
    text = (f"👤 <b>User</b>\nID: <code>{user['user_id']}</code>\n"
            f"{user['full_name']} (@{user['username'] or 'yoq'})\n"
            f"💰 {format_money(user['balance'])}\n🎁 Ref: {format_money(user['referral_balance'])}\n"
            f"👥 {user['referrals_count']} | 📱 {user['free_numbers']}\n{st}")
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("💬 Xabar", callback_data=f"adm_msg_{uid}"),
        types.InlineKeyboardButton("💵 +Balans", callback_data=f"adm_bal_{uid}"),
    )
    markup.add(
        types.InlineKeyboardButton("🚫 Blok/Ochish", callback_data=f"adm_block_{uid}"),
        types.InlineKeyboardButton("📋 Buyurtma", callback_data=f"adm_orders_{uid}"),
    )
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception:
        bot.send_message(call.from_user.id, text, reply_markup=markup)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_msg_"))
def adm_msg_start(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("adm_msg_", ""))
    msg = bot.send_message(call.from_user.id, f"💬 <code>{uid}</code> ga xabar:\n/cancel")
    bot.register_next_step_handler(msg, lambda m: process_admin_reply(m, uid))
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_bal_"))
def adm_bal_start(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("adm_bal_", ""))
    msg = bot.send_message(call.from_user.id, f"💵 <code>{uid}</code> uchun summa (+5000 yoki -2000):")
    bot.register_next_step_handler(msg, lambda m: process_adm_bal(m, uid))
    bot.answer_callback_query(call.id)

def process_adm_bal(message, uid):
    if not is_admin(message.from_user.id):
        return
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=users_admin_menu())
        return
    try:
        amount = float(message.text.strip().replace(" ", ""))
        update_balance(uid, amount)
        try:
            bot.send_message(uid, f"💵 Admin balans oʻzgartirdi: <b>{amount:+.0f}</b> soʻm")
        except Exception:
            pass
        bot.send_message(message.chat.id, f"✅ {uid} → {amount:+.0f}", reply_markup=users_admin_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat raqam.", reply_markup=users_admin_menu())

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_block_"))
def adm_block_toggle(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("adm_block_", ""))
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT is_blocked FROM users WHERE user_id = %s", (uid,))
        row = c.fetchone()
        if not row:
            bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
            return
        new_st = 0 if row["is_blocked"] else 1
        c.execute("UPDATE users SET is_blocked = %s WHERE user_id = %s", (new_st, uid))
        conn.commit()
        bot.answer_callback_query(call.id, "Bloklandi ✅" if new_st else "Ochildi ✅", show_alert=True)
    finally:
        put_db(conn)

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_orders_"))
def adm_user_orders(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("adm_orders_", ""))
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT number, country, price, is_free, created_at FROM number_orders
                     WHERE user_id = %s ORDER BY id DESC LIMIT 10""", (uid,))
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.answer_callback_query(call.id, "Buyurtma yoʻq", show_alert=True)
        return
    text = f"📋 <code>{uid}</code> buyurtmalari:\n\n"
    for r in rows:
        tip = "🎁" if r["is_free"] else format_money(r["price"])
        text += f"📱 <code>{r['number']}</code> | {r['country']} | {tip}\n"
    bot.send_message(call.from_user.id, text[:3000])
    bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda m: m.text and ("Balans o" in m.text and "zgartirish" in m.text))
def change_balance(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id,
                           "Format: <code>USER_ID +5000</code> yoki <code>USER_ID -2000</code>\n/cancel",
                           reply_markup=back_only())
    bot.register_next_step_handler(msg, process_change_balance)

def process_change_balance(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👥", reply_markup=users_admin_menu())
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Notoʻgʻri format.", reply_markup=users_admin_menu())
        return
    try:
        uid = int(parts[0])
        amount = float(parts[1])
        update_balance(uid, amount)
        try:
            bot.send_message(uid, f"💵 Balans oʻzgardi: <b>{amount:+.0f}</b> soʻm")
        except Exception:
            pass
        bot.send_message(message.chat.id, f"✅ {uid} → {amount:+.0f}", reply_markup=users_admin_menu())
    except Exception as e:
        bot.send_message(message.chat.id, f"Xato: {e}", reply_markup=users_admin_menu())

@bot.message_handler(func=lambda m: m.text and ("Bloklash" in m.text or "Ochish" in m.text))
def block_user(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "User ID yuboring:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_block_user)

def process_block_user(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👥", reply_markup=users_admin_menu())
        return
    try:
        uid = int(message.text.strip())
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("SELECT is_blocked FROM users WHERE user_id = %s", (uid,))
            row = c.fetchone()
            if not row:
                bot.send_message(message.chat.id, "Topilmadi.", reply_markup=users_admin_menu())
                return
            new_st = 0 if row["is_blocked"] else 1
            c.execute("UPDATE users SET is_blocked = %s WHERE user_id = %s", (new_st, uid))
            conn.commit()
            bot.send_message(message.chat.id, f"✅ {uid} {'bloklandi' if new_st else 'ochildi'}.",
                             reply_markup=users_admin_menu())
        finally:
            put_db(conn)
    except Exception:
        bot.send_message(message.chat.id, "Faqat ID.", reply_markup=users_admin_menu())

@bot.message_handler(func=lambda m: m.text and ("Foydalanuvchi qidirish" in m.text or "qidirish" in m.text.lower() and is_admin(m.from_user.id)))
def search_user(message):
    if not is_admin(message.from_user.id):
        return
    if "qidirish" not in (message.text or "").lower() and "Foydalanuvchi" not in (message.text or ""):
        return
    msg = bot.send_message(message.chat.id, "User ID yoki @username:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_search_user)

def process_search_user(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👥", reply_markup=users_admin_menu())
        return
    q = message.text.strip().replace("@", "")
    conn = get_db()
    try:
        c = conn.cursor()
        try:
            uid = int(q)
            c.execute("SELECT * FROM users WHERE user_id = %s", (uid,))
        except ValueError:
            c.execute("SELECT * FROM users WHERE username = %s", (q,))
        row = c.fetchone()
    finally:
        put_db(conn)
    if not row:
        bot.send_message(message.chat.id, "Topilmadi.", reply_markup=users_admin_menu())
        return
    text = (f"👤 <b>User</b>\nID: <code>{row['user_id']}</code>\n"
            f"{row['full_name']} (@{row['username'] or 'yoq'})\n"
            f"💰 {format_money(row['balance'])}\n🎁 {format_money(row['referral_balance'])}\n"
            f"👥 {row['referrals_count']} | 📱 {row['free_numbers']}\n"
            f"Blok: {'Ha' if row['is_blocked'] else 'Yoʻq'}")
    bot.send_message(message.chat.id, text, reply_markup=users_admin_menu())

@bot.message_handler(func=lambda m: m.text == "💰 To'lovlar" or m.text == "💰 Toʻlovlar")
def admin_payments(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT id, user_id, unique_amount, status, created_at FROM payments
                     ORDER BY id DESC LIMIT 20""")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "Toʻlovlar yoʻq.", reply_markup=admin_menu())
        return
    text = "💰 <b>Oxirgi toʻlovlar</b>\n\n"
    for r in rows:
        st = {"paid": "✅", "pending": "⏳", "expired": "⏰"}.get(r["status"], "❓")
        text += f"{st} #{r['id']} | <code>{r['user_id']}</code> | {format_money(r['unique_amount'])}\n{r['created_at']}\n\n"
    text += "\nTasdiqlash: /confirm ID"
    bot.send_message(message.chat.id, text[:4000], reply_markup=admin_menu())

@bot.message_handler(func=lambda m: m.text == "💬 Sharhlar")
def reviews_menu(message):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT full_name, username, text, created_at FROM reviews WHERE is_approved = 1 ORDER BY id DESC LIMIT 10")
        rows = c.fetchall()
    finally:
        put_db(conn)
    text = "💬 <b>Sharhlar</b>\n\n"
    if not rows:
        text += "Hali sharh yoʻq."
    else:
        for r in rows:
            text += f"👤 {r['full_name']} (@{r['username'] or 'yoq'})\n{r['text']}\n\n"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✍️ Sharh qoldirish", callback_data="add_review"))
    bot.send_message(message.chat.id, text[:3500], reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "add_review")
def add_review_start(call):
    msg = bot.send_message(call.from_user.id, "✍️ Sharhingizni yozing:\n/cancel")
    bot.register_next_step_handler(msg, process_add_review)
    bot.answer_callback_query(call.id)

def process_add_review(message):
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=main_menu(message.from_user.id))
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO reviews (user_id, username, full_name, text, created_at, is_approved)
                     VALUES (%s, %s, %s, %s, %s, 1)""",
                  (message.from_user.id, message.from_user.username or "",
                   message.from_user.full_name or "", message.text[:500], now))
        conn.commit()
    finally:
        put_db(conn)
    bot.send_message(message.chat.id, "✅ Sharh qoʻshildi!", reply_markup=main_menu(message.from_user.id))

# Kanallar
@bot.message_handler(func=lambda m: m.text == "📢 Majburiy obuna")
def channels_admin(message):
    if is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "📢 <b>Kanallar</b>", reply_markup=channels_menu())

@bot.message_handler(func=lambda m: m.text and ("Kanal qo'shish" in m.text or "Kanal qoʻshish" in m.text))
def add_channel(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id,
                           "Kanal @username yoki ID (bot admin boʻlsin):\nForward ham mumkin.\n/cancel",
                           reply_markup=back_only())
    bot.register_next_step_handler(msg, process_add_channel)

def process_add_channel(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "📢", reply_markup=channels_menu())
        return
    try:
        if message.forward_from_chat:
            chat = message.forward_from_chat
        else:
            chat = bot.get_chat(message.text.strip())
        is_private = 0 if getattr(chat, "username", None) else 1
        invite_link = ""
        if is_private:
            try:
                inv = bot.create_chat_invite_link(chat.id)
                invite_link = inv.invite_link
            except Exception:
                try:
                    invite_link = bot.export_chat_invite_link(chat.id)
                except Exception:
                    pass
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("""INSERT INTO channels (channel_id, channel_username, channel_title, is_private, invite_link)
                         VALUES (%s, %s, %s, %s, %s)""",
                      (str(chat.id), f"@{chat.username}" if getattr(chat, "username", None) else "",
                       chat.title or str(chat.id), is_private, invite_link or ""))
            conn.commit()
        finally:
            put_db(conn)
        bot.send_message(message.chat.id, f"✅ {chat.title or chat.id}", reply_markup=channels_menu())
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ {e}", reply_markup=channels_menu())

@bot.message_handler(func=lambda m: m.text and "Kanallar ro" in m.text)
def list_channels(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT channel_id, channel_username, channel_title, is_private, COALESCE(invite_link,'') as invite_link FROM channels")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "Kanallar yoʻq.", reply_markup=channels_menu())
        return
    text = "📋 <b>Kanallar:</b>\n\n"
    for r in rows:
        tip = "🔒" if r["is_private"] else "🌐"
        text += f"{tip} {r['channel_title']} ({r['channel_username'] or r['channel_id']})\n"
    bot.send_message(message.chat.id, text, reply_markup=channels_menu())

@bot.message_handler(func=lambda m: m.text and ("Kanal o'chirish" in m.text or "Kanal oʻchirish" in m.text))
def remove_channel(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Kanal @username yoki ID:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_remove_channel)

def process_remove_channel(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "📢", reply_markup=channels_menu())
        return
    channel = message.text.strip()
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("DELETE FROM channels WHERE channel_id = %s OR channel_username = %s", (channel, channel))
        n = c.rowcount
        conn.commit()
    finally:
        put_db(conn)
    bot.send_message(message.chat.id, "✅ Oʻchirildi." if n else "Topilmadi.", reply_markup=channels_menu())

@bot.message_handler(func=lambda m: m.text == "👨‍💼 Adminlar")
def admins_admin(message):
    if is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "👨‍💼 <b>Adminlar</b>", reply_markup=admins_menu())

@bot.message_handler(func=lambda m: m.text and ("Admin qo'shish" in m.text or "Admin qoʻshish" in m.text))
def add_admin_h(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Yangi admin ID:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_add_admin)

def process_add_admin(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👨‍💼", reply_markup=admins_menu())
        return
    try:
        new_admin = int(message.text)
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (new_admin,))
            conn.commit()
            _admin_cache.add(new_admin)
        finally:
            put_db(conn)
        bot.send_message(message.chat.id, f"✅ {new_admin}", reply_markup=admins_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat raqam.", reply_markup=admins_menu())

@bot.message_handler(func=lambda m: m.text and ("Admin o'chirish" in m.text or "Admin oʻchirish" in m.text))
def remove_admin_h(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Admin ID:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_remove_admin)

def process_remove_admin(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👨‍💼", reply_markup=admins_menu())
        return
    try:
        admin_id = int(message.text)
        if admin_id in ADMIN_IDS:
            bot.send_message(message.chat.id, "Asosiy adminni oʻchirib boʻlmaydi.", reply_markup=admins_menu())
            return
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("DELETE FROM admins WHERE user_id = %s", (admin_id,))
            conn.commit()
            _admin_cache.discard(admin_id)
        finally:
            put_db(conn)
        bot.send_message(message.chat.id, f"✅ {admin_id}", reply_markup=admins_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat raqam.", reply_markup=admins_menu())

@bot.message_handler(func=lambda m: m.text and "Adminlar ro" in m.text)
def list_admins(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT user_id FROM admins")
        rows = c.fetchall()
    finally:
        put_db(conn)
    text = "📋 <b>Adminlar:</b>\n\n" + "\n".join(f"• <code>{r['user_id']}</code>" for r in rows)
    bot.send_message(message.chat.id, text, reply_markup=admins_menu())

@bot.message_handler(func=lambda m: m.text == "🛠 Texnik ishlar")
def toggle_maintenance(message):
    if not is_admin(message.from_user.id):
        return
    current = get_setting("maintenance", False)
    set_setting("maintenance", not current)
    bot.send_message(message.chat.id, f"🛠 {'Yoqildi' if not current else 'Oʻchirildi'}.",
                     reply_markup=other_settings_menu())

@bot.message_handler(commands=['confirm'])
def confirm_payment_cmd(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Format: /confirm PAYMENT_ID")
        return
    try:
        pay_id = int(parts[1])
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_id, unique_amount, status FROM payments WHERE id = %s", (pay_id,))
        row = c.fetchone()
        if not row or row["status"] == "paid":
            bot.send_message(message.chat.id, "Topilmadi yoki allaqachon toʻlangan.")
            put_db(conn)
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE payments SET status = 'paid', paid_at = %s WHERE id = %s", (now, pay_id))
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (row["unique_amount"], row["user_id"]))
        conn.commit()
        put_db(conn)
        try:
            bot.send_message(row["user_id"], f"✅ Toʻlov tasdiqlandi!\n💰 Hisobingiz <b>{format_money(row['unique_amount'])}</b> ga toʻldirildi.")
        except:
            pass
        bot.send_message(message.chat.id, f"✅ #{pay_id} tasdiqlandi.")
    except Exception as e:
        bot.send_message(message.chat.id, f"Xato: {e}")

# ==================== FLASK WEBHOOK ====================
@app.route("/", methods=["GET"])
def health():
    return "RaqamMarket Bot ishlayapti ✅", 200

@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    if request.headers.get("content-type") == "application/json":
        json_string = request.get_data().decode("utf-8")
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "", 200
    return "Bad request", 403

def setup_webhook():
    if WEBHOOK_URL:
        url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        bot.remove_webhook()
        time.sleep(0.5)
        bot.set_webhook(url=url)
        print(f"✅ Webhook: {url}")
    else:
        print("⚠️ WEBHOOK_URL yoʻq — polling rejimida ishlaydi")

# ==================== START ====================
if __name__ == "__main__":
    print("=" * 50)
    print("🤖 RaqamMarket Bot (Flask + PostgreSQL)")
    print("=" * 50)
    if not BOT_TOKEN or not DATABASE_URL:
        print("❌ BOT_TOKEN va DATABASE_URL majburiy!")
        exit(1)
    init_pool()
    init_db()
    start_expire_checker()
    if os.getenv("ENABLE_TELETHON", "0") == "1":
        start_telethon_listener()
    if WEBHOOK_URL:
        setup_webhook()
        app.run(host="0.0.0.0", port=PORT)
    else:
        print("📡 Polling rejimi...")
        bot.remove_webhook()
        bot.infinity_polling(none_stop=True, interval=1)
else:
    # Gunicorn uchun
    if BOT_TOKEN and DATABASE_URL:
        try:
            init_pool()
            init_db()
            start_expire_checker()
            if WEBHOOK_URL:
                setup_webhook()
        except Exception as e:
            print("Startup error:", e)
