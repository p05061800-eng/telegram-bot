# .venv + dotenv (Mac):
#   source .venv/bin/activate
#   pip install python-dotenv
#   pip install -r requirements.txt
#   pip list | grep dotenv
#   python bot.py
# Если pip не найден: python3 -m pip install python-dotenv
import asyncio
import logging
import os
import random
import re
import secrets
import sys
import time
import urllib.parse
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

import aiohttp
from aiohttp import web
from telegram import (
    CallbackQuery,
    InputMediaPhoto,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

load_dotenv()
token = os.getenv("TELEGRAM_BOT_TOKEN")
# Куда бот пишет о новых заказах: по умолчанию @Daniel_official; переопределение — TELEGRAM_ORDER_NOTIFY_ID (int или @username).
def _read_order_notify_target():
    """Куда слать заказы: по умолчанию @Daniel_official; из .env — int id или @username."""
    s = (os.getenv("TELEGRAM_ORDER_NOTIFY_ID") or os.getenv("ORDER_NOTIFY_CHAT_ID") or "").strip()
    if not s:
        return "@Daniel_official"
    if s.startswith("@"):
        return s
    try:
        return int(s)
    except ValueError:
        return s


ORDER_NOTIFY_TARGET = _read_order_notify_target()
ORDER_MENTION = (os.getenv("ORDER_MENTION", "@Daniel_official") or "@Daniel_official").strip()

# Админ-панель (/admin) и /say — только этот Telegram user id
ADMIN_ID = 711309799
ADMIN_ACCESS_DENIED = "⛔ Нет доступа"


def is_admin(user_id):
    return user_id == ADMIN_ID


# Заказы и админка:
# 1) Клиенты не получают сообщений с admin inline-кнопками (accept_/sent_/cancel_/adm:/oam: и т.д.).
# 2) Все новые заказы уходят в ORDER_NOTIFY_TARGET (_notify_admin_new_order).
# 3) Клиенту — только текстовые уведомления о статусе и ответах.
# 4) Админ → клиент: только bot.send_message(..., reply_markup=None).


async def _send_customer_plain(bot, user_id: int, text: str) -> bool:
    """Уведомление клиенту: send_message, только текст, без inline-клавиатуры."""
    if not user_id:
        return False
    log = logging.getLogger(__name__)
    try:
        await bot.send_message(
            chat_id=int(user_id),
            text=text,
            disable_web_page_preview=True,
            reply_markup=None,
        )
        return True
    except Exception:
        log.exception("send_message → клиент user_id=%s", user_id)
        return False


# Одноразовые коды входа на сайт (4 цифры): код -> { user_id, username, expires }
LOGIN_CODES: dict = {}
LOGIN_CODE_TTL_SEC = 5 * 60
# Нормализованный username (без @, lower) -> telegram user_id (кто писал боту и указал @ в профиле)
USERNAME_TO_USER_ID: Dict[str, int] = {}

# Корзина и оформленные заказы в памяти процесса (ключ — Telegram user_id)
USER_CART: dict = {}
USER_ORDERS: dict = {}
# Глобальный реестр заказов по порядковому id (память процесса)
# ORDERS[id] = { user_id, username, items, total, delivery, status, created_at, admin_* }
ORDERS: dict = {}
ORDER_COUNTER = 1
# /start order_<id> — черновики заказов по ссылке (до оформления). Пополняется API/сайтом или register_shared_deep_link_order.
SHARED_DEEP_LINK_ORDERS: dict = {}
# Режим «написать админу»: user_id -> True, пока ждём следующее текстовое сообщение
user_support_state: dict = {}
# Состояния оплаты по user_id (вне user_data — не теряются при смене контекста чата)
# user_states[user_id] = { "awaiting_proof": order_id, "crypto_check": order_id }
user_states: Dict[int, Dict[str, int]] = {}
# Отслеживание пользователей (память процесса): активность, снимок корзины, флаг заказов
USERS: Dict[int, dict] = {}


def _users_snapshot_cart(uid: int) -> List[dict]:
    raw = USER_CART.get(int(uid))
    if not isinstance(raw, dict):
        return []
    return deepcopy(list(raw.get("items") or []))


def _users_has_order(uid: int) -> bool:
    uid = int(uid or 0)
    if not uid:
        return False
    if USER_ORDERS.get(uid):
        return True
    for o in ORDERS.values():
        if int(o.get("user_id") or 0) == uid:
            return True
    return False


def users_ensure(uid: int) -> dict:
    """Создать запись USERS[user_id] с полями по умолчанию."""
    uid = int(uid)
    if uid not in USERS:
        USERS[uid] = {
            "last_action": "start",
            "cart": [],
            "last_activity": 0.0,
            "has_order": False,
        }
    return USERS[uid]


def users_touch(
    uid: int,
    action: Optional[str] = None,
    *,
    activity_only: bool = False,
) -> None:
    """Обновить last_activity, снимок корзины, has_order.

    Если activity_only=False и передан непустой action — обновить last_action.
    activity_only=True — только активность и снимки (любой update).
    """
    uid = int(uid or 0)
    if not uid:
        return
    row = users_ensure(uid)
    row["last_activity"] = time.time()
    row["cart"] = _users_snapshot_cart(uid)
    row["has_order"] = _users_has_order(uid)
    if (
        not activity_only
        and action is not None
        and str(action).strip() != ""
    ):
        row["last_action"] = str(action).strip()


async def track_user_activity(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """При любом update — last_activity и снимки; last_action задаётся в хендлерах."""
    u = update.effective_user
    if not u:
        return
    try:
        users_touch(int(u.id), activity_only=True)
        _register_login_username(int(u.id), getattr(u, "username", None))
    except Exception:
        logging.getLogger(__name__).exception("USERS touch")


def _user_state_bucket(uid: int) -> Dict[str, int]:
    if uid not in user_states:
        user_states[uid] = {}
    return user_states[uid]


def _user_state_get(uid: int, key: str) -> Optional[int]:
    if not uid:
        return None
    v = user_states.get(uid, {}).get(key)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _user_state_set(uid: int, key: str, order_id: int) -> None:
    if not uid:
        return
    b = _user_state_bucket(uid)
    b[key] = int(order_id)


def _user_state_pop(uid: int, key: str) -> Optional[int]:
    if not uid:
        return None
    b = user_states.get(uid)
    if not b:
        return None
    v = b.pop(key, None)
    if not b:
        user_states.pop(uid, None)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _user_state_clear_payment_states(uid: int) -> None:
    """После оплаты / полного сброса: убрать awaiting_proof и crypto_check."""
    if not uid:
        return
    b = user_states.get(uid)
    if not b:
        return
    b.pop("awaiting_proof", None)
    b.pop("crypto_check", None)
    if not b:
        user_states.pop(uid, None)


FALLBACK_USER_TEXT = "❌"

# Reply-клавиатура: короткие подписи + эмодзи
BTN_CATALOG = "📦 Каталог"
BTN_CART = "🛒 Корзина"
BTN_POPULAR = "🔥 Акции"
BTN_CHAT = "💬 Связь"
BTN_MY_ORDERS = "📋 Мои заказы"
BTN_DELIVERY = "🚚 Доставка"
BTN_RANDOM_CARD = "🎁 Случайная карточка"

# Уведомление клиенту при входе админа в режим ответа
ADMIN_TYPING_NOTICE = "⏳ Администратор печатает..."
# Автоответ после успешной отправки заказа админу
ORDER_AUTO_ACK = "📦 Заказ принят"

# Шаги воронки оплаты (подсказка в сообщениях)
PAY_FLOW_STEPS = "💳 Оплата → 📸 Скрин → ⏳ Проверка → ✅ Готово"

# Оплата (реквизиты + callback)
PAY_CARD_BODY = (
    "💳 Оплата картой\n\n"
    "Номер карты:\n"
    "9112 3810 0954 6243\n\n"
    "Имя на карте:\n"
    "DANIL PARFIONAU\n\n"
    "После оплаты нажмите:\n"
    "✅ Я оплатил"
)
PAY_TRANSFER_BODY = (
    "📱 Перевод на номер\n\n"
    "Телефон:\n"
    "+375298124337\n\n"
    "Получатель:\n"
    "DANIL PARFIONAU\n\n"
    "После оплаты нажмите:\n"
    "✅ Я оплатил"
)
PAY_CRYPTO_BODY = (
    "₿ Крипто (USDT TRC20)\n\n"
    "TBRKDLTC6QXED4pEVVm1RpZNKeB4ScJChf\n\n"
    "После оплаты нажмите:\n"
    "✅ Я оплатил"
)
PAY_PROOF_REQUEST = (
    "📸 Отправьте скрин оплаты\n\n"
    "⏳ Мы проверим платёж в течение нескольких минут"
)
PAY_PROOF_WAIT = "⏳ Проверяем оплату..."
PAY_ADMIN_CONFIRMED_CLIENT = "✅"
PAY_ADMIN_REJECTED_CLIENT = "📸 Ещё раз"
CRYPTO_AUTO_OK_CLIENT = "✅ Крипто-платеж получен!"
CRYPTO_AUTO_OK_ADMIN = "💰 КРИПТА ОПЛАЧЕНА"

_RE_START_ORDER_ARG = re.compile(r"^order_(.+)$", re.IGNORECASE)


async def _notify_callback_issue(
    q: Optional[CallbackQuery], context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not q:
        return
    try:
        await q.answer()
    except Exception:
        pass
    if q.message:
        try:
            await q.message.reply_text(FALLBACK_USER_TEXT)
        except Exception:
            try:
                await context.bot.send_message(
                    q.message.chat_id,
                    FALLBACK_USER_TEXT,
                    reply_markup=None,
                )
            except Exception:
                pass


def _normalize_login_username(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("@"):
        s = s[1:]
    return s.lower()


def _register_login_username(user_id: int, username: Optional[str]) -> None:
    """Запомнить @username → user_id для POST /api/send-code."""
    uid = int(user_id or 0)
    if not uid:
        return
    un = (username or "").strip()
    if not un:
        return
    key = _normalize_login_username(un)
    if key:
        USERNAME_TO_USER_ID[key] = uid


def _cleanup_expired_login_codes() -> None:
    now = time.time()
    for k in list(LOGIN_CODES.keys()):
        v = LOGIN_CODES.get(k) or {}
        if v.get("expires", 0) < now:
            LOGIN_CODES.pop(k, None)


def _invalidate_user_login_codes(telegram_id: int) -> None:
    for k, v in list(LOGIN_CODES.items()):
        if int(v.get("user_id") or 0) == int(telegram_id):
            del LOGIN_CODES[k]


def _telegram_login_code_message(code: str) -> str:
    return (
        "🔐 Ваш код для входа:\n\n"
        f"{code}\n\n"
        "⏳ Действует 5 минут"
    )


def _issue_login_code(telegram_id: int, username: str = "") -> str:
    """4-значный код; в LOGIN_CODES сохраняются user_id, нормализованный username, expires."""
    _cleanup_expired_login_codes()
    _invalidate_user_login_codes(telegram_id)
    norm = _normalize_login_username(username) if username else ""
    for _ in range(50):
        c = f"{secrets.randbelow(9000) + 1000:04d}"
        if c not in LOGIN_CODES:
            LOGIN_CODES[c] = {
                "user_id": int(telegram_id),
                "username": norm,
                "expires": time.time() + LOGIN_CODE_TTL_SEC,
            }
            return c
    c = f"{int(time.time() * 1000) % 10000:04d}"
    LOGIN_CODES[c] = {
        "user_id": int(telegram_id),
        "username": norm,
        "expires": time.time() + LOGIN_CODE_TTL_SEC,
    }
    return c


def _login_cors_headers() -> dict:
    origin = (os.getenv("LOGIN_CORS_ORIGIN") or "*").strip()
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }


def _login_json_response(data: dict, status: int = 200) -> web.Response:
    return web.json_response(data, status=status, headers=_login_cors_headers())


async def _http_login_options(_request: web.Request) -> web.Response:
    return web.Response(status=204, headers=_login_cors_headers())


async def _http_send_code(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return _login_json_response(
            {"success": False, "error": "Некорректный JSON"},
            status=400,
        )
    raw_u = data.get("username")
    if raw_u is None or str(raw_u).strip() == "":
        return _login_json_response(
            {"success": False, "error": "Укажите username"},
            status=400,
        )
    key = _normalize_login_username(str(raw_u))
    if not key:
        return _login_json_response(
            {"success": False, "error": "Укажите username"},
            status=400,
        )
    uid = USERNAME_TO_USER_ID.get(key)
    if not uid:
        return _login_json_response(
            {"success": False, "error": "Пользователь не писал боту"},
            status=404,
        )
    bot = request.app.get("bot")
    if bot is None:
        return _login_json_response(
            {"success": False, "error": "Бот недоступен"},
            status=503,
        )
    code = _issue_login_code(int(uid), key)
    try:
        await bot.send_message(
            chat_id=int(uid),
            text=_telegram_login_code_message(code),
            disable_web_page_preview=True,
        )
    except Exception:
        logging.getLogger(__name__).exception("send-code → Telegram user_id=%s", uid)
        return _login_json_response(
            {"success": False, "error": "Не удалось отправить код в Telegram"},
            status=502,
        )
    return _login_json_response({"success": True})


async def _http_verify_code(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return _login_json_response(
            {"success": False, "error": "Некорректный JSON"},
            status=400,
        )
    raw_u = data.get("username")
    code = str(data.get("code") or "").strip()
    if not code or raw_u is None or str(raw_u).strip() == "":
        return _login_json_response(
            {"success": False, "error": "Укажите username и код"},
            status=400,
        )
    key = _normalize_login_username(str(raw_u))
    entry = LOGIN_CODES.get(code)
    if not entry:
        return _login_json_response(
            {"success": False, "error": "Неверный код или срок действия истёк"},
            status=401,
        )
    try:
        exp = float(entry.get("expires") or 0)
    except (TypeError, ValueError):
        exp = 0.0
    if exp < time.time():
        LOGIN_CODES.pop(code, None)
        return _login_json_response(
            {"success": False, "error": "Неверный код или срок действия истёк"},
            status=401,
        )
    if str(entry.get("username") or "").lower() != key:
        return _login_json_response(
            {"success": False, "error": "Неверный код или username"},
            status=401,
        )
    uid = int(entry.get("user_id") or 0)
    LOGIN_CODES.pop(code, None)
    return _login_json_response(
        {
            "success": True,
            "user_id": uid,
            "username": f"@{key}" if key else "",
        },
    )


async def _http_login_page(_request: web.Request) -> web.Response:
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "web", "login.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
    except OSError:
        return web.Response(
            text="login.html not found",
            status=404,
            content_type="text/plain",
            charset="utf-8",
        )
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def _run_login_http_api(bot) -> None:
    """Фоновый HTTP: POST /api/send-code, /api/verify-code и страница входа."""
    log = logging.getLogger(__name__)
    app = web.Application()
    app["bot"] = bot
    app.router.add_get("/", _http_login_page)
    app.router.add_get("/login", _http_login_page)
    app.router.add_options("/api/send-code", _http_login_options)
    app.router.add_options("/api/verify-code", _http_login_options)
    app.router.add_post("/api/send-code", _http_send_code)
    app.router.add_post("/api/verify-code", _http_verify_code)
    runner = web.AppRunner(app)
    await runner.setup()
    host = (os.getenv("LOGIN_API_HOST") or "127.0.0.1").strip()
    try:
        port = int(os.getenv("LOGIN_API_PORT", "8765"))
    except ValueError:
        port = 8765
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    log.info("Вход через Telegram: HTTP %s:%s (/, /login, /api/*)", host, port)
    stop = asyncio.Event()
    try:
        await stop.wait()
    finally:
        await runner.cleanup()


CARDS_JSON_URL = os.getenv("CARDS_JSON_URL", "https://www.illucards.by/cards.json")

PROMO_PHOTO = "https://picsum.photos/seed/promo/400/300"
ILLUCARDS_BASE = "https://www.illucards.by"
SYNC_EVERY_SEC = int(os.getenv("ILLUCARDS_SYNC_EVERY_SEC", "900"))
# Tinder-режим каталога: одна карта на экран, смена через editMessageMedia
TINDER_NO_IMAGE = "https://picsum.photos/seed/illu-noimg/400/550"

# Доставка при оформлении из корзины: callback dl:{by|ru|ua|ot}
DELIVERY_OPTIONS: dict = {
    "by": ("🇧🇾 Беларусь", 6, "BYN"),
    "ru": ("🇷🇺 Россия", 600, "RUB"),
    "ua": ("🇺🇦 Украина", 3000, "RUB"),
    "ot": ("🌍 Другие страны", 800, "RUB"),
}


def _delivery_info_text() -> str:
    parts: List[str] = [
        "🚚 Доставка IlluCards",
        "",
        "Отправляем заказы по всему миру ✨",
        "",
    ]
    for _code, (label, amt, cur) in DELIVERY_OPTIONS.items():
        parts.append(f"📍 {label}")
        parts.append(f"   💰 от {amt} {cur}")
        parts.append("")
    parts.append("Точную сумму увидите при оформлении заказа 👇")
    return "\n".join(parts).rstrip()

# Редкости как на illucards.by (фильтры каталога — индексы callback j:cat:0..4)
CATALOG_RARITY_FILTERS: Tuple[str, ...] = (
    "common",
    "limited",
    "novelty",
    "replica",
    "adult",
)
# Подписи для кнопок и карточек (epic / legendary / rare и пр. не переводим — покажем как в API)
RARITY_RU: dict = {
    "—": "б/р",
    "common": "Обычная",
    "limited": "Лимитированная",
    "novelty": "Новинка",
    "replica": "Реплика",
    "adult": "18+",
}


def _rarity_label_ru(s: str) -> str:
    t = (s or "").strip()
    if t == "—" or t == "":
        return "б/р"
    if any("\u0400" <= c <= "\u04ff" for c in t):
        return t
    k = t.lower()
    if k in RARITY_RU:
        return RARITY_RU[k]
    return t

REPLY_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(BTN_CATALOG), KeyboardButton(BTN_CART)],
        [KeyboardButton(BTN_POPULAR), KeyboardButton(BTN_CHAT)],
        [KeyboardButton(BTN_MY_ORDERS), KeyboardButton(BTN_DELIVERY)],
        [KeyboardButton(BTN_RANDOM_CARD)],
    ],
    resize_keyboard=True,
)

# Тексты reply-клавиатуры (сброс режима «чат с админом» при переходе в меню)
REPLY_MENU_TEXTS = frozenset(
    {
        BTN_CATALOG,
        BTN_CART,
        BTN_POPULAR,
        BTN_CHAT,
        BTN_MY_ORDERS,
        BTN_DELIVERY,
        BTN_RANDOM_CARD,
    },
)

START_WELCOME_TEXT = (
    "Ты вошёл в IlluCards ✅\n\n"
    "Нажми кнопку ниже, чтобы открыть сайт:"
)

START_WELCOME_MENU_TEXT = "Выбери действие в меню ниже 👇"


def _illucards_site_open_markup(telegram_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Открыть сайт",
                    url=f"https://illucards.by/?user={int(telegram_id)}",
                ),
            ],
        ],
    )


async def _send_start_welcome_with_site_button(msg: Message, uid: int) -> None:
    await msg.reply_text(
        START_WELCOME_TEXT,
        reply_markup=_illucards_site_open_markup(uid),
    )
    await msg.reply_text(START_WELCOME_MENU_TEXT, reply_markup=REPLY_KB)


CATALOG_INTRO_TEXT = (
    "📦 Вся коллекция\n\n"
    "Выберите категорию карточек 👇"
)

SUPPORT_INTRO_TEXT = (
    "💬 Связь с нами\n\n"
    "Напишите сюда, если:\n\n"
    "— есть вопросы по карточкам\n"
    "— нужна помощь с заказом\n"
    "— хотите уточнить наличие\n\n"
    "Администратор ответит вам прямо здесь 👇"
)


def _format_caption(p: dict) -> str:
    name = p.get("name") or "Без названия"
    category = p.get("category") or ""
    r_raw = str(p.get("rarity", "") or "").strip() or "—"
    price = p.get("price")
    price_str = str(price) if price is not None and price != "" else "—"
    lines = [
        f"{name}",
        f"🔥 Категория: {category}",
        f"⭐ Редкость: {_rarity_label_ru(r_raw)}",
        f"💰 Цена: {price_str}",
    ]
    return "\n".join(lines)


async def load_products() -> List[dict]:
    try:
        to = aiohttp.ClientTimeout(total=40)
        async with aiohttp.ClientSession(timeout=to) as session:
            async with session.get(CARDS_JSON_URL, headers={"Accept": "application/json"}) as resp:
                if resp.status != 200:
                    print(f"Ошибка: cards.json — HTTP {resp.status}")
                    return []
                data = await resp.json()
    except (aiohttp.ClientError, ValueError, TypeError) as e:
        print(f"Ошибка: не удалось загрузить cards.json — {e}")
        return []
    if not isinstance(data, list):
        print("Ошибка: в cards.json ожидается массив")
        return []
    cards = []
    for item in data:
        if not isinstance(item, dict):
            continue
        front = item.get("frontImage", "") or ""
        if isinstance(front, str) and front.startswith("/"):
            image = ILLUCARDS_BASE + front
        else:
            image = front
        rar = item.get("rarity", "")
        sale_raw = item.get("isSale", False)
        is_sale = sale_raw is True or str(sale_raw).lower() in ("1", "true", "yes")
        cards.append(
            {
                "id": item.get("id"),
                "name": item.get("title", "Без названия"),
                "price": item.get("priceRub", 0),
                "category": item.get("category", "Без категории"),
                "rarity": (str(rar).strip() or "—"),
                "image": image,
                "isSale": is_sale,
            }
        )
    print(f"Загружено карточек: {len(cards)}")
    return cards


def _product_from_callback(ref: str, products: List[dict]) -> Optional[dict]:
    if not ref or not products:
        return None
    s = str(ref).strip()
    for p in products:
        if str(p.get("id", "")) == s:
            return p
    if s.isdecimal():
        i = int(s)
        if 0 <= i < len(products):
            return products[i]
    return None


def _product_ref_for_callback(p: dict, index: int) -> str:
    pid = p.get("id")
    if pid is not None and str(pid).strip() != "":
        return str(pid).strip()
    return str(index)


def _product_price(p: dict) -> int:
    v = p.get("price", 0)
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _ensure_user_cart(user_id: int, user_data: Optional[dict] = None) -> dict:
    if not user_id:
        return {"items": [], "total": 0}
    if user_id not in USER_CART or not isinstance(USER_CART.get(user_id), dict):
        legacy: List[dict] = []
        if user_data and isinstance(user_data.get("cart"), list):
            legacy = list(user_data["cart"])
            user_data.pop("cart", None)
        USER_CART[user_id] = {"items": legacy, "total": 0}
    if "items" not in USER_CART[user_id]:
        USER_CART[user_id]["items"] = []
    _cart_sync_total_uid(user_id)
    return USER_CART[user_id]


def _cart_sync_total_uid(user_id: int) -> int:
    b = USER_CART.get(user_id)
    if not b:
        return 0
    items = list(b.get("items") or [])
    t, _ = _cart_totals(items)
    b["total"] = int(t)
    return int(t)


def _cart_get_lines_uid(user_id: int, user_data: Optional[dict] = None) -> List[dict]:
    return list(_ensure_user_cart(user_id, user_data).get("items") or [])


def _cart_set_items_uid(user_id: int, lines: List[dict]) -> None:
    _ensure_user_cart(user_id)
    USER_CART[user_id]["items"] = list(lines)
    _cart_sync_total_uid(user_id)


def _cart_add_line_uid(
    user_id: int,
    user_data: Optional[dict],
    ref: str,
    product: dict,
    name: str,
    price: int,
) -> None:
    lines = _cart_get_lines_uid(user_id, user_data)
    for line in lines:
        if str(line.get("ref")) == ref:
            line["qty"] = int(line.get("qty") or 1) + 1
            _cart_set_items_uid(user_id, lines)
            return
    lines.append({"ref": ref, "name": name, "price": price, "qty": 1})
    _cart_set_items_uid(user_id, lines)


def _cart_remove_line_uid(user_id: int, user_data: Optional[dict], index: int) -> bool:
    lines = _cart_get_lines_uid(user_id, user_data)
    if 0 <= index < len(lines):
        lines.pop(index)
        _cart_set_items_uid(user_id, lines)
        return True
    return False


def _cart_dec_line_uid(user_id: int, user_data: Optional[dict], index: int) -> bool:
    lines = _cart_get_lines_uid(user_id, user_data)
    if not (0 <= index < len(lines)):
        return False
    line = lines[index]
    q = int(line.get("qty") or 1)
    if q > 1:
        line["qty"] = q - 1
    else:
        lines.pop(index)
    _cart_set_items_uid(user_id, lines)
    return True


def _cart_clear_uid(user_id: int) -> None:
    USER_CART[user_id] = {"items": [], "total": 0}


ORDER_STATUS_RU: dict = {
    "new": "Новый",
    "accepted": "Принят",
    "shipped": "Отправлен",
    "done": "Завершён",
    "canceled": "Отменён",
    "cancelled": "Отменён",
}

# Уведомление клиенту о смене статуса (коротко)
CUSTOMER_STATUS_BODY: dict = {
    "accepted": "🚚 Подготавливаем к отправке",
    "shipped": "🚚 Отправлен",
    "done": "✅",
    "canceled": "❌",
    "cancelled": "❌",
}


def _order_status_label_ru(status: str) -> str:
    s = str(status or "new").strip()
    return ORDER_STATUS_RU.get(s, s)


def _format_customer_order_status_notice(order_id: int, status_key: str) -> str:
    sk = str(status_key or "new").strip()
    if sk == "cancelled":
        sk = "canceled"
    body = CUSTOMER_STATUS_BODY.get(sk, "")
    if body:
        return f"{body} (#{order_id})"
    return f"📦 #{order_id}"


def _payment_intro_text(total: int) -> str:
    """После оформления: сумма и выбор способа оплаты (кнопки — карта / перевод / крипта)."""
    return (
        f"💰 Итого: {int(total)} BYN\n\n"
        "Выберите способ оплаты:\n\n"
        "💳 Карта · 📱 Перевод · ₿ Крипта\n\n"
        f"{PAY_FLOW_STEPS}\n\n"
        "👇 Нажмите кнопку ниже"
    )


def _format_delivery_block(d: Optional[dict]) -> str:
    if not d or not isinstance(d, dict):
        return ""
    lab = str(d.get("label") or "").strip()
    if not lab:
        return ""
    try:
        amt = int(d.get("amount") if d.get("amount") is not None else 0)
    except (TypeError, ValueError):
        amt = 0
    cur = str(d.get("currency") or "").strip() or "—"
    return f"🚚 {lab} — {amt} {cur}"


def _format_order_items_for_admin(lines: List[dict]) -> str:
    rows: List[str] = []
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        rows.append(f"• {name} — {sub} BYN")
    return "\n".join(rows) if rows else "—"


def _format_admin_order_detail_text(order_id: int, o: dict) -> str:
    """Карточка заказа для админа (уведомление, open_order, правка сообщения)."""
    uid = int(o.get("user_id") or 0)
    un = o.get("username")
    un_s = str(un).strip().lstrip("@") if un else ""
    items_block = _format_order_items_for_admin(list(o.get("items") or []))
    tot = int(o.get("total") or 0)
    st = str(o.get("status") or "new")
    st_ru = _order_status_label_ru(st)
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else None
    dline = _format_delivery_block(d)
    d_country = "—"
    if d:
        cc = str(d.get("country") or "").strip().lower()
        opt = DELIVERY_OPTIONS.get(cc)
        if opt:
            d_country = str(opt[0])
        elif d.get("label"):
            d_country = str(d.get("label"))
    try:
        ts = float(o.get("created_at") or 0)
    except (TypeError, ValueError):
        ts = 0.0
    tss = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts > 0 else "—"
    u_show = f"@{un_s}" if un_s else "—"
    parts: List[str] = [
        f"📦 Заказ #{order_id}",
        "",
        f"👤 Пользователь: {u_show}",
        f"🆔 id: {uid}",
        "",
        "📦 Состав:",
        items_block,
        "",
        f"🚚 Доставка: {d_country}",
    ]
    if dline:
        parts.extend(["", dline])
    parts.extend(
        [
            "",
            f"💰 Сумма: {tot} BYN",
            "",
            f"📊 Статус: {st_ru}",
            "",
            f"🕐 Создан: {tss}",
        ]
    )
    body = "\n".join(parts)
    if len(body) > 4090:
        body = body[:4086] + "…"
    return body


def _kb_order_admin_actions(order_id: int, status: str) -> Optional[InlineKeyboardMarkup]:
    rep = InlineKeyboardButton("💬 Ответить", callback_data=f"oam:rep:{order_id}")
    acc = InlineKeyboardButton("✅ Принять", callback_data=f"accept_{order_id}")
    shp = InlineKeyboardButton("🚚 Отправлен", callback_data=f"sent_{order_id}")
    can = InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{order_id}")
    done_btn = InlineKeyboardButton("🏁 Завершён", callback_data=f"done_{order_id}")
    s = str(status or "new")
    if s == "new":
        return InlineKeyboardMarkup([[rep, acc], [shp, can]])
    if s == "accepted":
        return InlineKeyboardMarkup([[shp, can], [rep]])
    if s == "shipped":
        return InlineKeyboardMarkup([[done_btn, can], [rep]])
    if s in ("done", "canceled", "cancelled"):
        return InlineKeyboardMarkup([[rep]])
    return None


async def _notify_admin_new_order(
    context: ContextTypes.DEFAULT_TYPE,
    user,
    lines: List[dict],
    total: int,
    delivery: Optional[dict] = None,
) -> Optional[int]:
    """Новый заказ: только в ORDER_NOTIFY_TARGET, с admin-кнопками; ORDERS; при ошибке — None."""
    global ORDER_COUNTER
    log = logging.getLogger(__name__)
    order_id = int(ORDER_COUNTER)
    uid = int(user.id) if user else 0
    uname = (getattr(user, "username", None) or "").strip()
    drec = deepcopy(delivery) if delivery else {}
    now = time.time()
    o_preview = {
        "user_id": uid,
        "username": uname,
        "items": list(lines),
        "total": int(total),
        "delivery": drec,
        "status": "new",
        "created_at": now,
    }
    text = _format_admin_order_detail_text(order_id, o_preview)
    kb = _kb_order_admin_actions(order_id, "new")
    try:
        m = await context.bot.send_message(
            chat_id=ORDER_NOTIFY_TARGET,
            text=text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        log.exception("Ошибка отправки заказа админу (order_id=%s)", order_id)
        return None
    ORDER_COUNTER = order_id + 1
    ORDERS[order_id] = {
        "user_id": uid,
        "username": uname,
        "items": deepcopy(list(lines)),
        "total": int(total),
        "delivery": drec,
        "status": "new",
        "created_at": now,
        "admin_chat_id": int(m.chat_id),
        "admin_message_id": int(m.message_id),
        "paid": False,
        "payment_proof_submitted": False,
        "clear_cart_on_paid": False,
    }
    if uid:
        users_touch(uid, activity_only=True)
    return order_id


def _kb_payment_methods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💳 Карта", callback_data="pay_card"),
                InlineKeyboardButton("📱 Перевод", callback_data="pay_transfer"),
            ],
            [InlineKeyboardButton("₿ Крипта", callback_data="pay_crypto")],
        ]
    )


def _kb_paid_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Я оплатил", callback_data="paid")]]
    )


def _format_payment_receipt_text(order_id: int, o: dict) -> str:
    """Текст чека клиенту после оплаты."""
    items_block = _format_order_items_for_admin(list(o.get("items") or []))
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else None
    dline = _format_delivery_block(d)
    if not dline and d:
        cc = str(d.get("country") or "").strip().lower()
        opt = DELIVERY_OPTIONS.get(cc)
        dline = f"🚚 {opt[0]}" if opt else ""
    tot = int(o.get("total") or 0)
    lines: List[str] = [
        "📄 Чек:",
        "",
        f"🧾 Заказ #{order_id}",
        "",
        "📦 Состав:",
        items_block,
        "",
    ]
    if dline:
        lines.append(dline)
    else:
        lines.append("🚚 Доставка: —")
    lines.append("")
    lines.append(f"💰 Сумма: {tot} BYN")
    lines.extend(
        [
            "",
            "📊 Статус: Оплачен",
            "",
            "🙏 Спасибо за покупку!",
        ]
    )
    body = "\n".join(lines)
    if len(body) > 4090:
        body = body[:4086] + "…"
    return body


def _kb_payment_receipt() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📦 Мои заказы", callback_data="rcpt_orders"),
                InlineKeyboardButton("💬 Связь", callback_data="rcpt_support"),
            ],
        ]
    )


async def _send_payment_receipt(
    bot, chat_id: int, order_id: int, o: dict
) -> None:
    log = logging.getLogger(__name__)
    if not int(chat_id or 0):
        return
    text = _format_payment_receipt_text(order_id, o)
    try:
        await bot.send_message(
            chat_id=int(chat_id),
            text=text,
            reply_markup=_kb_payment_receipt(),
            disable_web_page_preview=True,
        )
    except Exception:
        log.exception("чек order_id=%s chat_id=%s", order_id, chat_id)


def _kb_payment_admin_review(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Оплата получена",
                    callback_data=f"confirm_payment_{order_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    "❌ Скрин не подходит",
                    callback_data=f"reject_payment_{order_id}",
                ),
            ],
        ]
    )


def _user_data_for(application: Application, user_id: int) -> dict:
    """user_data другого пользователя (для сброса awaiting_* после действия админа)."""
    raw = application.user_data
    if user_id not in raw:
        raw[user_id] = {}
    return raw[user_id]


def _clear_crypto_auto_watch(o: dict, uid: int) -> None:
    """Снять mock-наблюдение за крипто-оплатой (карта/перевод/ручной скрин)."""
    o.pop("crypto_auto_active", None)
    o.pop("crypto_auto_deadline", None)
    _user_state_pop(uid, "crypto_check")


async def _notify_order_customer(
    context: ContextTypes.DEFAULT_TYPE, order: dict, text: str
) -> bool:
    uid = int(order.get("user_id") or 0)
    return await _send_customer_plain(context.bot, uid, text)


async def _refresh_admin_order_message(
    context: ContextTypes.DEFAULT_TYPE, order_id: int
) -> None:
    o = ORDERS.get(order_id)
    if not o:
        return
    cid = o.get("admin_chat_id")
    mid = o.get("admin_message_id")
    if cid is None or mid is None:
        return
    log = logging.getLogger(__name__)
    text = _format_admin_order_detail_text(order_id, o)
    kb = _kb_order_admin_actions(order_id, str(o.get("status") or "new"))
    try:
        await context.bot.edit_message_text(
            chat_id=int(cid),
            message_id=int(mid),
            text=text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        log.warning("Не удалось обновить сообщение заказа #%s", order_id)


def _cart_totals(lines: List[dict]) -> Tuple[int, int]:
    t = 0
    n = 0
    for x in lines:
        q = int(x.get("qty") or 1)
        p = int(x.get("price") or 0)
        t += p * q
        n += q
    return t, n


def _format_cart_message(lines: List[dict]) -> str:
    if not lines:
        return (
            "🛒 Ваша корзина пуста\n\n"
            "Вы ещё не добавили ни одной карточки\n"
            "Перейдите в каталог 👇"
        )
    total, _ = _cart_totals(lines)
    out: List[str] = [
        "🛒 Ваша корзина:",
        "",
    ]
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        if q <= 1:
            out.append(f"• {name} — {p} BYN")
        else:
            out.append(f"• {name} — {p * q} BYN (×{q})")
    out += ["", f"💰 Итого: {total} BYN"]
    s = "\n".join(out)
    if len(s) > 3900:
        s = s[:3890] + "…"
    return s


def _format_user_orders_message(user_id: int) -> str:
    orders = list(USER_ORDERS.get(user_id) or [])
    if not orders:
        return "📦 ∅"
    lines: List[str] = ["📦", ""]
    for o in reversed(orders[-20:]):
        oid = str(o.get("id") or "")[:12]
        st = o.get("status") or "—"
        tg = o.get("total_goods", 0)
        d = o.get("delivery") or {}
        dtxt = d.get("label") or "—"
        lines.append(f"• #{oid} — {st}")
        lines.append(f"  Товары: {tg} BYN · {dtxt}")
        lines.append("")
    s = "\n".join(lines).rstrip()
    if len(s) > 3900:
        s = s[:3890] + "…"
    return s


USER_ORDER_STATUS_ICONS: dict = {
    "new": "🆕",
    "accepted": "✅",
    "shipped": "🚚",
    "done": "📦",
    "canceled": "❌",
    "cancelled": "❌",
}


def _user_order_status_badge(status: str) -> str:
    sk = str(status or "new").strip()
    icon = USER_ORDER_STATUS_ICONS.get(sk, "📋")
    if sk == "cancelled":
        sk = "canceled"
    ru = _order_status_label_ru(sk)
    return f"{icon} {ru}"


def _user_orders_registry_for_user(user_id: int) -> List[Tuple[int, dict]]:
    out: List[Tuple[int, dict]] = []
    for oid, rec in ORDERS.items():
        if int(rec.get("user_id") or 0) != int(user_id):
            continue
        try:
            oi = int(oid)
        except (TypeError, ValueError):
            continue
        out.append((oi, rec))
    out.sort(key=lambda x: x[0], reverse=True)
    return out


def _format_mine_orders_text_and_kb(
    user_id: int,
) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    reg = _user_orders_registry_for_user(user_id)
    if not reg:
        return (
            "📋 Пока нет заказов\n\n"
            "Оформите первый заказ из корзины — и он появится здесь ✨",
            None,
        )
    lines: List[str] = [
        "📋 Мои заказы",
        "",
        "Нажмите на заказ, чтобы открыть подробности 👇",
        "",
    ]
    rows: List[List[InlineKeyboardButton]] = []
    for oid, o in reg[:30]:
        tot = int(o.get("total") or 0)
        st = str(o.get("status") or "new")
        badge = _user_order_status_badge(st)
        lines.append(f"#{oid} — {tot} BYN — {badge}")
        rows.append(
            [
                InlineKeyboardButton("📦", callback_data=f"user_order_{oid}"),
            ],
        )
    text = "\n".join(lines)
    if len(text) > 3500:
        text = text[:3490] + "…"
        rows = rows[:25]
    return (text, InlineKeyboardMarkup(rows))


def _format_user_order_detail(order_id: int, o: dict) -> str:
    items_block = _format_order_items_for_admin(list(o.get("items") or []))
    tot = int(o.get("total") or 0)
    st = str(o.get("status") or "new")
    sk = "canceled" if st == "cancelled" else st
    st_ru = _order_status_label_ru(sk)
    dline = _format_delivery_block(
        o.get("delivery") if isinstance(o.get("delivery"), dict) else None
    )
    parts: List[str] = [
        f"📦 Заказ #{order_id}",
        "",
        f"📊 Статус: {st_ru}",
        "",
        "📦 Состав:",
        items_block,
        "",
        f"💰 Итого: {tot} BYN",
    ]
    if dline:
        parts.extend(["", dline])
    body = "\n".join(parts)
    if len(body) > 4090:
        body = body[:4086] + "…"
    return body


def _kb_cart(lines: List[dict]) -> InlineKeyboardMarkup:
    if not lines:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📦 Каталог", callback_data="m:0"),
                    InlineKeyboardButton("🔥 Акции", callback_data="pop:0"),
                ],
            ],
        )
    rows: List[List[InlineKeyboardButton]] = []
    for i in range(len(lines)):
        rows.append(
            [
                InlineKeyboardButton("➖", callback_data=f"dc:{i}"),
                InlineKeyboardButton("➕", callback_data=f"ic:{i}"),
                InlineKeyboardButton("❌", callback_data=f"rm:{i}"),
            ],
        )
    rows.append(
        [InlineKeyboardButton("✅ Оформить заказ", callback_data="co:0")],
    )
    rows.append(
        [InlineKeyboardButton("📦 Продолжить покупки", callback_data="m:0")],
    )
    return InlineKeyboardMarkup(rows)


def _format_checkout_preview_for_user(lines: List[dict]) -> str:
    total, _ = _cart_totals(lines)
    out: List[str] = ["🛒", ""]
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        out.append(f"• {name} — {p} BYN × {q}")
    out += ["", f"💰 {total} BYN"]
    return "\n".join(out)


def _kb_delivery_country() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇧🇾", callback_data="dl:by"),
                InlineKeyboardButton("🇷🇺", callback_data="dl:ru"),
            ],
            [
                InlineKeyboardButton("🇺🇦", callback_data="dl:ua"),
                InlineKeyboardButton("🌍", callback_data="dl:ot"),
            ],
        ]
    )


def _format_order_preview_with_delivery(user_data: dict) -> str:
    lines: List[dict] = list(user_data.get("order_checkout") or [])
    if not lines:
        return ""
    code = str(user_data.get("delivery_country") or "")
    opt = DELIVERY_OPTIONS.get(code)
    if not opt:
        return ""
    dlabel, damount, dcur = opt[0], opt[1], opt[2]
    goods_total, _ = _cart_totals(lines)
    out: List[str] = [
        "📦 Ваш заказ:",
        "",
    ]
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        out.append(f"• {name} — {sub} BYN")
    out.append("")
    out.append(f"🚚 Доставка: {dlabel}")
    out.append("")
    if code == "by" and dcur == "BYN":
        out.append(f"💰 Итого: {goods_total + damount} BYN")
    else:
        out.append(f"💰 Итого: {goods_total} BYN (+{damount} {dcur} доставка)")
    s = "\n".join(out)
    if len(s) > 4000:
        s = s[:3990] + "…"
    return s


def _clear_checkout_delivery(user_data: dict) -> None:
    for k in (
        "order_checkout",
        "delivery_country",
        "delivery_label",
        "delivery_amount",
        "delivery_currency",
    ):
        user_data.pop(k, None)


def _category_names(products: List[dict]) -> List[str]:
    s = {str(p.get("category", "Без категории") or "Без категории") for p in products}
    return sorted(s, key=str.lower)


def _btn_label(s: str, max_len: int = 22) -> str:
    t = s.strip() or "—"
    return t if len(t) <= max_len else t[: max_len - 1] + "…"


def _kb_categories(categories: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🌐 Вся коллекция", callback_data="c:all")],
    ]
    row: List[InlineKeyboardButton] = []
    for i, name in enumerate(categories):
        row.append(InlineKeyboardButton(_btn_label(name, 28), callback_data=f"c:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def _kb_rarities(cat_tok: str) -> InlineKeyboardMarkup:
    """
    j:{cat_tok}:all — все
    j:{cat_tok}:sale — isSale
    j:{cat_tok}:0..4 — common, limited, novelty, replica, adult
    """
    c = str(cat_tok)[:12]
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⭐ Обычная", callback_data=f"j:{c}:0"),
                InlineKeyboardButton("🔥 Горячая цена", callback_data=f"j:{c}:sale"),
            ],
            [
                InlineKeyboardButton("💎 Лимитированная", callback_data=f"j:{c}:1"),
                InlineKeyboardButton("♻️ Реплика", callback_data=f"j:{c}:3"),
            ],
            [
                InlineKeyboardButton("💫 Новинка", callback_data=f"j:{c}:2"),
                InlineKeyboardButton("🔞 18+", callback_data=f"j:{c}:4"),
            ],
            [InlineKeyboardButton("🌟 Все карточки", callback_data=f"j:{c}:all")],
            [InlineKeyboardButton("⬅️ К категориям", callback_data="m:0")],
        ]
    )


def _global_product_index(all_products: List[dict], p: dict) -> int:
    pid = p.get("id")
    if pid is not None and str(pid).strip():
        s = str(pid)
        for i, x in enumerate(all_products):
            if str(x.get("id", "")) == s:
                return i
    try:
        return all_products.index(p)
    except ValueError:
        return 0


def _filter_wizard(
    products: List[dict], cats: List[str], cat_tok: str, rar_tok: str
) -> tuple[List[dict], str, str]:
    """cat_tok: all | индекс категории. rar_tok: all | sale | 0..4 по CATALOG_RARITY_FILTERS."""
    if cat_tok == "all":
        base = list(products)
        cat_label = "Все категории"
    else:
        cix = int(cat_tok)
        if cix < 0 or cix >= len(cats):
            return [], "", ""
        cn = cats[cix]
        cat_label = cn
        base = [p for p in products if str(p.get("category", "Без категории")) == cn]
    if rar_tok == "all":
        return base, cat_label, "все редкости"
    if rar_tok == "sale":
        out = [p for p in base if p.get("isSale") is True]
        return out, cat_label, "🔥 Горячая цена"
    try:
        rix = int(rar_tok)
    except ValueError:
        return [], cat_label, ""
    if rix < 0 or rix >= len(CATALOG_RARITY_FILTERS):
        return [], cat_label, ""
    rkey = CATALOG_RARITY_FILTERS[rix]
    out = [
        p
        for p in base
        if str(p.get("rarity", "") or "").strip().lower() == rkey
    ]
    rlab = RARITY_RU.get(rkey, rkey)
    return out, cat_label, rlab


def _needs_rarity_step(base: List[dict]) -> bool:
    """Несколько разных редкостей в выборке — показываем экран «⭐ редкость»."""
    if len(base) < 2:
        return False
    keys: set = set()
    for p in base:
        r = str(p.get("rarity", "") or "").strip().lower()
        keys.add(r if r else "_")
    return len(keys) > 1


def _tinder_photo_url(p: dict) -> str:
    u = str(p.get("image") or "").strip()
    if u and u.startswith("http"):
        return u
    if u and u.startswith("/"):
        return ILLUCARDS_BASE + u
    return TINDER_NO_IMAGE


def _tinder_caption(p: dict, cur_1: int, n_total: int) -> str:
    name = (p.get("name") or "—")
    r_raw = str(p.get("rarity", "") or "").strip() or "—"
    r = _rarity_label_ru(r_raw)
    v = p.get("price", 0)
    try:
        pstr = f"{int(float(v))} BYN"
    except (TypeError, ValueError):
        pstr = "—"
    if len(name) > 200:
        name = name[:197] + "…"
    c = f"{name}\n\n💰 {pstr}\n⭐ {r}\n\n🔎 {cur_1} из {n_total}"
    if len(c) > 1024:
        c = c[:1020] + "…"
    return c


def _tinder_keyboard(cat_tok: str, user_data: dict) -> InlineKeyboardMarkup:
    c = str(cat_tok)[:12]
    if user_data.get("tinder_autoplay_paused", False):
        auto_btn = InlineKeyboardButton("▶️", callback_data="t:f")
    else:
        auto_btn = InlineKeyboardButton("⏸", callback_data="t:f")
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬅️", callback_data="t:p"),
                InlineKeyboardButton("🛒", callback_data="t:c"),
                InlineKeyboardButton("➡️", callback_data="t:n"),
            ],
            [auto_btn, InlineKeyboardButton("⬆️", callback_data=f"h:{c}")],
        ]
    )


def _tinder_cancel_autoplay(user_data: dict) -> None:
    t = user_data.get("tinder_autoplay_task")
    if isinstance(t, asyncio.Task) and not t.done():
        t.cancel()
    user_data.pop("tinder_autoplay_task", None)


async def _tinder_message_edit(
    context: ContextTypes.DEFAULT_TYPE,
    message: Optional[Message],
    chat_id: int,
    message_id: int,
    ud: dict,
) -> bool:
    log = logging.getLogger(__name__)
    gixs: List[int] = list(ud.get("tinder_gidxs") or [])
    products: List[dict] = list(context.application.bot_data.get("products") or [])
    if not gixs or not products:
        return False
    n = len(gixs)
    if n == 0:
        return False
    i = int(ud.get("tinder_i", 0)) % n
    gidx = gixs[i]
    if gidx < 0 or gidx >= len(products):
        return False
    p = products[gidx]
    cap = _tinder_caption(p, i + 1, n)
    media = InputMediaPhoto(
        media=_tinder_photo_url(p),
        caption=cap,
    )
    ctk = str(ud.get("tinder_cat_tok", "all"))[:12]
    kb = _tinder_keyboard(ctk, ud)
    try:
        if message and message.message_id == message_id and message.chat_id == chat_id:
            try:
                await message.edit_media(media=media, reply_markup=kb)
            except Exception as e1:
                log.info("Tinder: edit_media fallback: %s", e1)
                try:
                    await message.edit_caption(caption=cap, reply_markup=kb)
                except Exception as e2:
                    log.exception("Tinder: %s", e2)
                    return False
        else:
            try:
                await context.bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=message_id,
                    media=media,
                    reply_markup=kb,
                )
            except Exception as e1:
                log.info("Tinder: edit_message_media fallback: %s", e1)
                try:
                    await context.bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=message_id,
                        caption=cap,
                        reply_markup=kb,
                    )
                except Exception as e2:
                    log.exception("Tinder: %s", e2)
                    return False
    except Exception as e0:
        log.exception("Tinder: %s", e0)
        return False
    return True


def _tinder_start_autoplay(context: ContextTypes.DEFAULT_TYPE, user_data: dict) -> None:
    _tinder_cancel_autoplay(user_data)
    if user_data.get("tinder_autoplay_paused", False):
        return
    if not user_data.get("tinder_gidxs") or not user_data.get("tinder_message_id"):
        return
    t = asyncio.create_task(
        _tinder_autoplay_loop(context, user_data),
        name="tinder_autoplay",
    )
    user_data["tinder_autoplay_task"] = t


async def _tinder_autoplay_loop(
    context: ContextTypes.DEFAULT_TYPE, user_data: dict) -> None:
    log = logging.getLogger(__name__)
    try:
        while True:
            if not user_data.get("tinder_gidxs"):
                return
            if user_data.get("tinder_autoplay_paused", False):
                return
            try:
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                return
            if not user_data.get("tinder_gidxs"):
                return
            if user_data.get("tinder_autoplay_paused", False):
                return
            gixs: List[int] = list(user_data.get("tinder_gidxs") or [])
            n = len(gixs)
            if n == 0:
                return
            new_i = (int(user_data.get("tinder_i", 0)) + 1) % n
            user_data["tinder_i"] = new_i
            cid = int(user_data.get("tinder_chat_id", 0))
            mid = int(user_data.get("tinder_message_id", 0))
            if not cid or not mid:
                return
            if not await _tinder_message_edit(
                context, None, cid, mid, user_data
            ):
                return
    except asyncio.CancelledError:
        return
    except Exception as e:
        log.exception("Tinder: autoplay %s", e)


async def _tinder_start_deck(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    in_scope: List[dict],
    products: List[dict],
    cat_tok: str,
) -> bool:
    gixs: List[int] = []
    for p in in_scope:
        gixs.append(_global_product_index(products, p))
    if not gixs:
        return False
    ud = context.user_data
    ud["tinder_gidxs"] = gixs
    ud["tinder_i"] = 0
    ud["tinder_cat_tok"] = str(cat_tok)[:12]
    n = len(gixs)
    g0 = gixs[0]
    if g0 < 0 or g0 >= len(products):
        return False
    p0 = products[g0]
    cap = _tinder_caption(p0, 1, n)
    photo = _tinder_photo_url(p0)
    kw = str(cat_tok)[:12]
    ud["tinder_autoplay_paused"] = False
    kb = _tinder_keyboard(kw, ud)
    _tinder_cancel_autoplay(ud)
    sent: Optional[Message] = None
    try:
        sent = await context.bot.send_photo(
            chat_id=chat_id,
            photo=photo,
            caption=cap,
            reply_markup=kb,
        )
    except Exception:
        try:
            sent = await context.bot.send_photo(
                chat_id=chat_id,
                photo=TINDER_NO_IMAGE,
                caption=cap,
                reply_markup=kb,
            )
        except Exception:
            return False
    if not sent:
        return False
    ud["tinder_chat_id"] = int(sent.chat_id)
    ud["tinder_message_id"] = int(sent.message_id)
    _tinder_start_autoplay(context, ud)
    return True


async def _tinder_start(
    q: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    in_scope: List[dict],
    products: List[dict],
    cat_tok: str,
) -> None:
    m = q.message
    if not m:
        return
    try:
        await m.delete()
    except Exception:
        pass
    await _tinder_start_deck(context, m.chat_id, in_scope, products, cat_tok)


async def on_tinder_swipe(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """t:p t:n t:c t:f — листалка Tinder; t:f = пауза/продолжить авто-показ."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    s = (q.data or "").strip()
    m = re.match(r"^t:([pncf])$", s)
    if not m:
        return
    op = m.group(1)
    if not q.message.photo and op != "f":
        await _notify_callback_issue(q, context)
        return
    ud = context.user_data
    _tinder_cancel_autoplay(ud)
    gixs: List[int] = list(ud.get("tinder_gidxs") or [])
    products: List[dict] = list(context.application.bot_data.get("products") or [])
    if not gixs or not products:
        await _notify_callback_issue(q, context)
        return
    n = len(gixs)
    if n == 0:
        return
    if op == "f":
        if not q.message.photo:
            await _notify_callback_issue(q, context)
            return
        new_paused = not bool(ud.get("tinder_autoplay_paused", False))
        ud["tinder_autoplay_paused"] = new_paused
        ctk = str(ud.get("tinder_cat_tok", "all"))[:12]
        try:
            await q.message.edit_reply_markup(
                reply_markup=_tinder_keyboard(ctk, ud)
            )
        except Exception:
            pass
        await q.answer("⏸ Пауза" if new_paused else "▶ Автопоказ, следующая — через 3 с")
        if not new_paused:
            _tinder_start_autoplay(context, ud)
        return
    i = int(ud.get("tinder_i", 0)) % n
    if op == "n":
        i = (i + 1) % n
    elif op == "p":
        i = (i - 1) % n
    else:
        gix_cur = gixs[i]
        p_cur = products[gix_cur] if 0 <= gix_cur < len(products) else None
        if p_cur is None:
            await _notify_callback_issue(q, context)
            return
        ref = _product_ref_for_callback(p_cur, gix_cur)
        uid_t = q.from_user.id if q.from_user else 0
        _cart_add_line_uid(
            uid_t,
            ud,
            ref,
            p_cur,
            p_cur.get("name") or "—",
            _product_price(p_cur),
        )
        if uid_t:
            users_touch(uid_t, "cart")
        i = (i + 1) % n
    ud["tinder_i"] = i
    cid = int(q.message.chat_id)
    mid = int(q.message.message_id)
    ok = await _tinder_message_edit(context, q.message, cid, mid, ud)
    if not ok:
        await _notify_callback_issue(q, context)
        return
    if op == "c":
        uid_t = q.from_user.id if q.from_user else 0
        lines = _cart_get_lines_uid(uid_t, ud)
        tot, _ = _cart_totals(lines)
        short = f"{len(lines)} поз. · {tot} BYN"
        await q.answer(f"🛒 В корзине: {short}", show_alert=False)
    else:
        await q.answer()
    if not ud.get("tinder_autoplay_paused", False):
        _tinder_start_autoplay(context, ud)


async def illucards_sync_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    log = logging.getLogger(__name__)
    _cleanup_expired_login_codes()
    app = context.application
    try:
        cards = await load_products()
        if cards:
            app.bot_data["products"] = cards
            app.bot_data["illucards_synced_at"] = time.time()
            log.info("Illucards: синхронизация, карточек: %d", len(cards))
        else:
            log.warning("Illucards: пустой ответ, кэш не сбрасываю")
    except Exception:
        log.exception("Illucards: ошибка фоновой синхронизации")


async def crypto_auto_check_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mock: крипто-оплата через 2–5 мин после выбора ₿ (проверка каждые 30 с)."""
    app = context.application
    bot = app.bot
    now = time.time()
    log = logging.getLogger(__name__)
    for oid, o in list(ORDERS.items()):
        if not o.get("crypto_auto_active"):
            continue
        if o.get("paid"):
            o["crypto_auto_active"] = False
            o.pop("crypto_auto_deadline", None)
            continue
        if o.get("payment_proof_submitted"):
            continue
        dl = float(o.get("crypto_auto_deadline") or 0)
        if dl <= 0 or now < dl:
            continue
        cust = int(o.get("user_id") or 0)
        o["crypto_auto_active"] = False
        o.pop("crypto_auto_deadline", None)
        o["paid"] = True
        o["paid_at"] = now
        clear_cart = bool(o.pop("clear_cart_on_paid", False))
        if clear_cart and cust:
            _cart_clear_uid(cust)
        try:
            _user_state_clear_payment_states(cust)
            cud = _user_data_for(app, cust)
            cud.pop("awaiting_payment_order_id", None)
        except Exception:
            log.exception("crypto auto user_data")
        await _send_payment_receipt(bot, cust, oid, o)
        admin_txt = f"{CRYPTO_AUTO_OK_ADMIN}\n\n📦 Заказ #{oid}\n👤 {cust}"
        try:
            await bot.send_message(
                ORDER_NOTIFY_TARGET,
                admin_txt,
                disable_web_page_preview=True,
            )
        except Exception:
            log.exception("crypto auto → админ")


def _extract_total_from_order_text(text: str) -> Optional[str]:
    if not (text or "").strip():
        return None
    for pat in (
        r"💰\s*Итого\s*:\s*([0-9]+(?:[.,][0-9]+)?)\s*BYN",
        r"Итого\s*:\s*([0-9]+(?:[.,][0-9]+)?)\s*BYN",
        r"Итого\s+([0-9]+(?:[.,][0-9]+)?)\s*BYN",
    ):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            num = m.group(1).replace(",", ".").replace(" ", "")
            return f"{num} BYN"
    return None


def _format_deep_link_order_preview(order_text: str) -> str:
    ot = (order_text or "").strip()
    if not ot:
        return "📦 —"
    lines = ["📦", "", ot]
    tot = _extract_total_from_order_text(ot)
    if tot:
        lines.extend(["", f"💰 {tot}"])
    s = "\n".join(lines)
    if len(s) > 4000:
        s = s[:3990] + "…"
    return s


def _parse_order_id_from_start_arg(first_arg: str) -> Optional[str]:
    m = _RE_START_ORDER_ARG.match((first_arg or "").strip())
    return m.group(1).strip() if m else None


def _normalize_deep_link_order(raw: dict, external_id: str) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    raw_items = raw.get("items")
    if not isinstance(raw_items, list):
        raw_items = raw.get("lines")
    if not isinstance(raw_items, list):
        return None
    items_out: List[dict] = []
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or it.get("title") or "—")[:200]
        try:
            price = int(
                it.get("price")
                if it.get("price") is not None
                else it.get("unit_price")
                or 0
            )
        except (TypeError, ValueError):
            price = 0
        try:
            qty = int(
                it.get("qty")
                if it.get("qty") is not None
                else it.get("quantity")
                or 1
            )
        except (TypeError, ValueError):
            qty = 1
        qty = max(1, qty)
        ref = str(it.get("ref") or it.get("id") or name)[:120]
        items_out.append({"name": name, "price": price, "qty": qty, "ref": ref})
    if not items_out:
        return None
    d = raw.get("delivery")
    if isinstance(d, dict) and (d.get("label") or d.get("name")):
        label = str(d.get("label") or d.get("name") or "").strip()
        try:
            amount = int(
                d.get("amount")
                if d.get("amount") is not None
                else d.get("price")
                or 0
            )
        except (TypeError, ValueError):
            amount = 0
        currency = str(d.get("currency") or "BYN").strip() or "BYN"
        country = str(d.get("country") or "").strip()
    else:
        label = str(
            raw.get("delivery_label")
            or raw.get("delivery_name")
            or raw.get("shipping_label")
            or ""
        ).strip()
        try:
            amount = int(
                raw.get("delivery_amount")
                if raw.get("delivery_amount") is not None
                else raw.get("shipping")
                or 0
            )
        except (TypeError, ValueError):
            amount = 0
        currency = str(raw.get("delivery_currency") or "BYN").strip() or "BYN"
        country = str(raw.get("delivery_country") or "").strip()
    if not label:
        opt = DELIVERY_OPTIONS.get("by", ("🇧🇾 Беларусь", 6, "BYN"))
        label, amount, currency = opt[0], int(opt[1]), str(opt[2])
        country = country or "by"
    return {
        "items": items_out,
        "delivery": {
            "country": country,
            "label": label,
            "amount": int(amount),
            "currency": currency,
        },
        "external_id": str(raw.get("id") or external_id),
    }


def _order_record_to_deep_link_shape(rec: dict, fallback_id: str) -> Optional[dict]:
    items = list(rec.get("items") or [])
    if not items:
        return None
    composed = {"items": items, "delivery": rec.get("delivery") or {}}
    return _normalize_deep_link_order(composed, str(rec.get("id") or fallback_id))


def _find_user_order_snapshot_normalized(order_id: str) -> Optional[dict]:
    for lst in USER_ORDERS.values():
        for rec in lst:
            if str(rec.get("id")) == str(order_id):
                return _order_record_to_deep_link_shape(rec, order_id)
    return None


def _fetch_order_from_shared_memory(order_id: str) -> Optional[dict]:
    raw = SHARED_DEEP_LINK_ORDERS.get(str(order_id))
    if raw is None:
        return None
    if isinstance(raw, dict):
        return _normalize_deep_link_order(deepcopy(raw), str(order_id))
    return None


async def _fetch_order_from_deep_link_api(order_id: str) -> Optional[dict]:
    template = (os.getenv("ORDER_DEEP_LINK_API_URL") or "").strip()
    if not template or "{id}" not in template:
        return None
    safe_id = urllib.parse.quote(str(order_id), safe="")
    url = template.replace("{id}", safe_id)
    log = logging.getLogger(__name__)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=12)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
    except Exception:
        log.exception("ORDER_DEEP_LINK_API_URL: не удалось загрузить заказ %s", order_id)
        return None
    if not isinstance(data, dict):
        return None
    return _normalize_deep_link_order(data, str(order_id))


async def _fetch_order_for_deep_link(order_id: str) -> Optional[dict]:
    o = _fetch_order_from_shared_memory(order_id)
    if o:
        return o
    o = await _fetch_order_from_deep_link_api(order_id)
    if o:
        return o
    return _find_user_order_snapshot_normalized(order_id)


def _format_user_deep_link_order_message(order: dict) -> str:
    lines = list(order.get("items") or [])
    d = order.get("delivery") or {}
    label = str(d.get("label") or "—")
    amount = int(d.get("amount") or 0)
    cur = str(d.get("currency") or "BYN")
    country = str(d.get("country") or "")
    goods_total, _ = _cart_totals(lines)
    out: List[str] = [
        "📦 Ваш заказ",
        "",
    ]
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        out.append(f"• {name} — {sub} BYN")
    out.append("")
    out.append(f"🚚 Доставка: {label}")
    out.append("")
    if country == "by" and cur == "BYN":
        out.append(f"💰 Итого: {goods_total + amount} BYN")
    else:
        out.append(f"💰 Итого: {goods_total} BYN (+{amount} {cur})")
    s = "\n".join(out)
    if len(s) > 4000:
        s = s[:3990] + "…"
    return s


def register_shared_deep_link_order(order_id: str, payload: dict) -> None:
    """Сохранить черновик заказа для ссылки /start order_<order_id> (JSON как из API: items, delivery, …)."""
    SHARED_DEEP_LINK_ORDERS[str(order_id)] = deepcopy(payload)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.from_user:
        return
    uid = msg.from_user.id
    users_touch(uid, "start")
    _register_login_username(uid, msg.from_user.username)
    args = context.args or []
    if args:
        first = (args[0] or "").strip()
        if first.lower() == "login":
            un = (msg.from_user.username or "").strip()
            if not un:
                await msg.reply_text(
                    "⚠️ В настройках Telegram не указан username.\n\n"
                    "Задайте @username в профиле, затем снова: /start login",
                    reply_markup=REPLY_KB,
                )
                return
            code = _issue_login_code(uid, un)
            await msg.reply_text(
                _telegram_login_code_message(code),
                reply_markup=REPLY_KB,
            )
            await _send_start_welcome_with_site_button(msg, uid)
            return
        oid = _parse_order_id_from_start_arg(first)
        if oid:
            order = await _fetch_order_for_deep_link(oid)
            if not order:
                await msg.reply_text(
                    "😔 Заказ по ссылке не найден\n\n"
                    "Проверьте ссылку или напишите нам в 💬 Связь",
                    reply_markup=REPLY_KB,
                )
                return
            tok = secrets.token_hex(8)
            context.user_data["deep_link_order_session"] = {
                "token": tok,
                "order": deepcopy(order),
            }
            preview = _format_user_deep_link_order_message(order)
            await msg.reply_text(
                preview,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "✅",
                                callback_data=f"dlco:{tok}",
                            ),
                            InlineKeyboardButton(
                                "❌",
                                callback_data=f"dlca:{tok}",
                            ),
                        ],
                    ],
                ),
            )
            return
        t = " ".join(args).strip()
        if not t:
            await _send_start_welcome_with_site_button(msg, uid)
            return
        context.user_data.pop("deep_link_order_session", None)
        context.user_data["pending_order"] = t
        preview = _format_deep_link_order_preview(t)
        await msg.reply_text(
            preview,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅",
                            callback_data="confirm_order",
                        ),
                        InlineKeyboardButton("❌", callback_data="cancel_order"),
                    ],
                ],
            ),
        )
        return
    await _send_start_welcome_with_site_button(msg, uid)


async def on_deep_link_confirm_order(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if (q.data or "").strip() != "confirm_order":
        return
    ud = context.user_data
    order_text = (ud.get("pending_order") or "").strip()
    if not order_text:
        await _notify_callback_issue(q, context)
        return
    u = q.from_user
    uname = f"@{u.username}" if u and u.username else "—"
    uid = u.id if u else "—"
    admin_body = "\n".join(["📦", "", order_text, "", f"👤 {uname} · {uid}"])
    if len(admin_body) > 4090:
        admin_body = admin_body[:4086] + "…"
    log = logging.getLogger(__name__)
    try:
        await context.bot.send_message(
            chat_id=ORDER_NOTIFY_TARGET,
            text=admin_body,
            disable_web_page_preview=True,
            reply_markup=None,
        )
    except Exception as e:
        log.exception("deep link order → admin: %s", e)
        await _notify_callback_issue(q, context)
        return
    ud.pop("pending_order", None)
    await q.answer()
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.message.reply_text(ORDER_AUTO_ACK)


async def on_deep_link_cancel_order(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if (q.data or "").strip() != "cancel_order":
        return
    context.user_data.pop("pending_order", None)
    await q.answer()
    try:
        await q.message.edit_text("❌", reply_markup=None)
    except Exception:
        await q.message.reply_text("❌")


async def on_deep_link_structured_submit(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    m = re.match(r"^dlco:([a-fA-F0-9]{16})$", (q.data or "").strip())
    if not m:
        return
    tok = m.group(1)
    ud = context.user_data
    sess = ud.get("deep_link_order_session")
    if not isinstance(sess, dict) or str(sess.get("token")) != tok:
        await _notify_callback_issue(q, context)
        return
    order = sess.get("order")
    if not isinstance(order, dict):
        await _notify_callback_issue(q, context)
        return
    u = q.from_user
    if not u:
        await _notify_callback_issue(q, context)
        return
    lines = list(order.get("items") or [])
    if not lines:
        await _notify_callback_issue(q, context)
        return
    d = order.get("delivery") or {}
    d_label = str(d.get("label") or "—")
    try:
        d_amt = int(d.get("amount") if d.get("amount") is not None else 0)
    except (TypeError, ValueError):
        d_amt = 0
    d_cur = str(d.get("currency") or "BYN")
    d_cc = str(d.get("country") or "")
    goods_total, _ = _cart_totals(list(lines))
    drec = {
        "country": d_cc,
        "label": d_label,
        "amount": d_amt,
        "currency": d_cur,
    }
    order_rec = {
        "id": "0",
        "items": deepcopy(list(lines)),
        "total": int(goods_total),
        "total_goods": int(goods_total),
        "delivery": deepcopy(drec),
        "status": "В обработке",
    }
    if drec.get("country") == "by" and drec.get("currency") == "BYN":
        order_rec["total"] = int(goods_total) + int(drec.get("amount") or 0)
    oid = await _notify_admin_new_order(
        context, u, list(lines), int(order_rec["total"]), deepcopy(drec)
    )
    if oid is None:
        await _notify_callback_issue(q, context)
        return
    ud.pop("deep_link_order_session", None)
    order_rec["id"] = str(oid)
    USER_ORDERS.setdefault(u.id, []).append(order_rec)
    ORDERS[int(oid)]["clear_cart_on_paid"] = False
    ud["awaiting_payment_order_id"] = int(oid)
    ud.pop("payment_pending_method", None)
    await q.answer()
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    tot = int(order_rec["total"])
    await q.message.reply_text(
        _payment_intro_text(tot),
        reply_markup=_kb_payment_methods(),
    )
    users_touch(u.id, "payment")


async def on_deep_link_structured_cancel(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    m = re.match(r"^dlca:([a-fA-F0-9]{16})$", (q.data or "").strip())
    if not m:
        return
    tok = m.group(1)
    ud = context.user_data
    sess = ud.get("deep_link_order_session")
    if not isinstance(sess, dict) or str(sess.get("token")) != tok:
        await _notify_callback_issue(q, context)
        return
    ud.pop("deep_link_order_session", None)
    await q.answer()
    try:
        await q.message.edit_text("❌", reply_markup=None)
    except Exception:
        await q.message.reply_text("❌")


def _kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📦 Заказы", callback_data="adm:orders"),
                InlineKeyboardButton("📈 Статистика", callback_data="adm:stats"),
            ],
        ],
    )


def _kb_admin_orders_list() -> Optional[InlineKeyboardMarkup]:
    if not ORDERS:
        return None
    rows: List[List[InlineKeyboardButton]] = []
    for oid in sorted(ORDERS.keys(), key=lambda k: int(k)):
        o = ORDERS[oid]
        tot = int(o.get("total") or 0)
        label = f"#{oid} — {tot} BYN"
        if len(label) > 60:
            label = label[:57] + "…"
        rows.append(
            [
                InlineKeyboardButton(
                    label, callback_data=f"open_order_{int(oid)}"
                ),
            ],
        )
    return InlineKeyboardMarkup(rows)


def _format_admin_stats() -> str:
    """Сводка по ORDERS в памяти процесса (дата «сегодня» — локальное время сервера)."""
    today_local = datetime.now().date()
    today_count = 0
    revenue_byn = 0
    by_status = {"new": 0, "accepted": 0, "shipped": 0, "done": 0, "canceled": 0}
    for o in ORDERS.values():
        st = str(o.get("status") or "new").strip()
        if st == "cancelled":
            st = "canceled"
        if st not in by_status:
            st = "new"
        by_status[st] += 1
        try:
            ts = float(o.get("created_at") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        if ts > 0 and datetime.fromtimestamp(ts).date() == today_local:
            today_count += 1
        try:
            tot = int(o.get("total") or 0)
        except (TypeError, ValueError):
            tot = 0
        if st != "canceled":
            revenue_byn += tot
    n_all = len(ORDERS)
    lines = [
        "📈 Статистика",
        "",
        f"📅 Сегодня заказов: {today_count}",
        f"📦 Всего заказов: {n_all}",
        f"💰 Выручка: {revenue_byn} BYN",
        "",
        "📊 По статусам:",
        f"🆕 Новые: {by_status['new']}",
        f"✅ Приняты: {by_status['accepted']}",
        f"🚚 Отправлены: {by_status['shipped']}",
        f"🏁 Завершены: {by_status['done']}",
        f"❌ Отменены: {by_status['canceled']}",
    ]
    return "\n".join(lines)


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return
    if not is_admin(u.id):
        await msg.reply_text(ADMIN_ACCESS_DENIED)
        return
    await msg.reply_text(
        "👑 Админ-панель\n\nВыберите раздел 👇",
        reply_markup=_kb_admin_panel(),
    )


async def admin_say_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить текст клиенту по user_id (только админ)."""
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return
    if not is_admin(u.id):
        await msg.reply_text(ADMIN_ACCESS_DENIED)
        return
    args = context.args or []
    if len(args) < 2:
        await msg.reply_text("/say ID текст")
        return
    try:
        target_id = int(args[0])
    except (TypeError, ValueError):
        await msg.reply_text("❌")
        return
    text = " ".join(args[1:]).strip()
    if not text:
        await msg.reply_text("❌")
        return
    ok = await _send_customer_plain(context.bot, target_id, text)
    if not ok:
        await msg.reply_text("❌")
        return
    await msg.reply_text("✅")


async def on_admin_panel_action(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^adm:(orders|stats)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        return
    action = m.group(1)
    await q.answer()
    if action == "orders":
        if not ORDERS:
            await q.message.reply_text(
                "📦 Пока нет заказов\n\n"
                "Как только клиент оформит покупку — заказ появится здесь ✨"
            )
        else:
            body_lines: List[str] = ["📦 Заказы", "", "Выберите заказ 👇", ""]
            for oid in sorted(ORDERS.keys(), key=int):
                o = ORDERS[oid]
                tot = int(o.get("total") or 0)
                body_lines.append(f"#{oid} — {tot} BYN")
            text = "\n".join(body_lines)
            if len(text) > 3500:
                text = text[:3490] + "…"
            await q.message.reply_text(
                text,
                reply_markup=_kb_admin_orders_list(),
            )
    else:
        await q.message.reply_text(_format_admin_stats())


async def on_admin_open_order(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^open_order_(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        return
    try:
        oid = int(m.group(1))
    except ValueError:
        await q.answer()
        return
    o = ORDERS.get(oid)
    if not o:
        try:
            await q.answer("Заказ не найден", show_alert=False)
        except Exception:
            pass
        return
    await q.answer()
    st = str(o.get("status") or "new")
    await q.message.reply_text(
        _format_admin_order_detail_text(oid, o),
        reply_markup=_kb_order_admin_actions(oid, st),
    )


async def on_order_admin_action(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Только 💬 Ответить (oam:rep). Принять / отправлен / отмена — accept_ / sent_ / cancel_."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^oam:rep:(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        return
    try:
        oid = int(m.group(1))
    except ValueError:
        await q.answer()
        return
    o = ORDERS.get(oid)
    if not o:
        try:
            await q.answer("Заказ не найден", show_alert=False)
        except Exception:
            pass
        return
    context.user_data.pop("reply_support_user_id", None)
    context.user_data["reply_to"] = oid
    await q.answer()
    cust = int(o.get("user_id") or 0)
    if cust:
        await _send_customer_plain(context.bot, cust, ADMIN_TYPING_NOTICE)
    await q.message.reply_text("✍️")


async def on_support_reply_activate(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """💬 Ответить под сообщением клиента (поддержка): sup:rep:{user_id}."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^sup:rep:(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        return
    try:
        client_uid = int(m.group(1))
    except ValueError:
        await q.answer()
        return
    if not client_uid:
        await q.answer()
        return
    context.user_data.pop("reply_to", None)
    context.user_data["reply_support_user_id"] = client_uid
    await q.answer()
    await _send_customer_plain(context.bot, client_uid, ADMIN_TYPING_NOTICE)
    await q.message.reply_text("✍️")


async def on_user_order_open(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^user_order_(\d+)$", (q.data or "").strip())
    if not m:
        return
    try:
        oid = int(m.group(1))
    except ValueError:
        return
    uid = q.from_user.id
    o = ORDERS.get(oid)
    if not o or int(o.get("user_id") or 0) != int(uid):
        return
    await q.answer()
    await q.message.reply_text(_format_user_order_detail(oid, o))


async def on_order_status_buttons(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^(accept|sent|cancel|done)_(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        return
    action, oid_s = m.group(1), m.group(2)
    try:
        oid = int(oid_s)
    except ValueError:
        await q.answer()
        return
    o = ORDERS.get(oid)
    if not o:
        try:
            await q.answer("Заказ не найден", show_alert=False)
        except Exception:
            pass
        return
    st = str(o.get("status") or "new")
    terminal = ("done", "canceled", "cancelled")
    if action == "accept":
        if st != "new":
            await q.answer("Уже обработан.", show_alert=False)
            return
        o["status"] = "accepted"
    elif action == "sent":
        if st not in ("new", "accepted"):
            await q.answer("Недоступно.", show_alert=False)
            return
        o["status"] = "shipped"
    elif action == "done":
        if st != "shipped":
            await q.answer("Недоступно.", show_alert=False)
            return
        o["status"] = "done"
    elif action == "cancel":
        if st in terminal:
            await q.answer("Недоступно.", show_alert=False)
            return
        o["status"] = "canceled"
    else:
        return
    await q.answer("✅", show_alert=False)
    notice = _format_customer_order_status_notice(oid, str(o.get("status") or ""))
    await _notify_order_customer(context, o, notice)
    await _refresh_admin_order_message(context, oid)


async def catalog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_catalog(update, context)


async def send_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Категория → редкость → Tinder-просмотр (1 карта, листалка), корзина.
    """
    msg = update.effective_message
    if not msg:
        return
    log = logging.getLogger(__name__)

    cards = await load_products()
    if not cards:
        await msg.reply_text("❌")
        return
    context.bot_data["products"] = cards
    context.bot_data["illucards_synced_at"] = time.time()

    categories = _category_names(cards)
    if not categories:
        await msg.reply_text("❌")
        return
    log.info("Каталог: %d разделов", len(categories))
    if msg.from_user:
        users_touch(msg.from_user.id, "catalog")
    await msg.reply_text(CATALOG_INTRO_TEXT, reply_markup=_kb_categories(categories))


async def send_tinder_mode(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Tinder по всему каталогу (cat_tok all); вызывается из кода, не с reply-клавиатуры."""
    msg = update.effective_message
    if not msg:
        return
    products = await _get_products(context)
    if not products:
        await msg.reply_text("❌")
        return
    in_scope = list(products)
    ok = await _tinder_start_deck(
        context, int(msg.chat_id), in_scope, products, "all"
    )
    if not ok:
        await msg.reply_text("❌")


async def send_popular_deck(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """🔥 Популярное — карточки со скидкой (isSale), иначе весь каталог."""
    msg = update.effective_message
    if not msg:
        return
    products = await _get_products(context)
    if not products:
        await msg.reply_text("❌")
        return
    cats = _category_names(products)
    in_scope, _, _ = _filter_wizard(products, cats, "all", "sale")
    if not in_scope:
        in_scope = list(products)
    ok = await _tinder_start_deck(
        context, int(msg.chat_id), in_scope, products, "all"
    )
    if not ok:
        await msg.reply_text("❌")


async def send_random_card(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """🎁 Случайная карточка — одна позиция в режиме просмотра."""
    msg = update.effective_message
    if not msg:
        return
    products = await _get_products(context)
    if not products:
        await msg.reply_text("❌")
        return
    in_scope = [random.choice(products)]
    ok = await _tinder_start_deck(
        context, int(msg.chat_id), in_scope, products, "all"
    )
    if not ok:
        await msg.reply_text("❌")


async def send_promo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_photo(photo=PROMO_PHOTO, caption="🎁")


async def _get_products(context: ContextTypes.DEFAULT_TYPE) -> List[dict]:
    app = context.application
    products = list(app.bot_data.get("products") or [])
    if not products:
        more = await load_products()
        if more:
            app.bot_data["products"] = more
        products = list(app.bot_data.get("products") or [])
    return products


async def _edit_to_categories(
    q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE
) -> None:
    products = await _get_products(context)
    if not products:
        await q.edit_message_text("❌")
        return
    cats = _category_names(products)
    if not cats:
        await q.edit_message_text("❌")
        return
    await q.edit_message_text(CATALOG_INTRO_TEXT, reply_markup=_kb_categories(cats))
    if q.from_user:
        users_touch(q.from_user.id, "catalog")


async def on_menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or (q.data or "") != "m:0":
        return
    await q.answer()
    await _edit_to_categories(q, context)


async def on_popular_inline(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """🔥 Акции из inline-кнопки (корзина / навигация)."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    if (q.data or "").strip() != "pop:0":
        return
    await q.answer()
    products = await _get_products(context)
    if not products:
        await q.message.reply_text(
            "😔 Сейчас не удалось загрузить каталог\n\nПопробуйте чуть позже 🙏"
        )
        return
    cats = _category_names(products)
    in_scope, _, _ = _filter_wizard(products, cats, "all", "sale")
    if not in_scope:
        in_scope = list(products)
    await _tinder_start(q, context, in_scope, products, "all")


async def on_pick_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    m = re.match(r"^c:(\d+|all)$", (q.data or "").strip())
    if not m:
        return
    key = m.group(1)
    products = await _get_products(context)
    cats = _category_names(products)
    if key == "all":
        cat_tok = "all"
        base, cat_label, _ = _filter_wizard(products, cats, "all", "all")
    else:
        ci = int(key)
        if ci < 0 or ci >= len(cats):
            await q.answer()
            if q.message:
                await q.message.reply_text(FALLBACK_USER_TEXT)
            return
        cat_tok = str(ci)
        base, cat_label, _ = _filter_wizard(products, cats, cat_tok, "all")
    if not cat_label or not base:
        await _notify_callback_issue(q, context)
        return
    await q.answer()
    if _needs_rarity_step(base):
        hdr = "⭐ Выберите редкость:\n\nКакие карточки показать? 👇"
        await q.edit_message_text(hdr, reply_markup=_kb_rarities(cat_tok))
    else:
        await _tinder_start(q, context, base, products, cat_tok)


async def on_pick_rarity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    m = re.match(r"^j:([^:]+):(all|sale|\d+)$", (q.data or "").strip())
    if not m:
        return
    cat_tok, rar_tok = m.group(1), m.group(2)
    products = await _get_products(context)
    cats = _category_names(products)
    in_scope, c_lab, _r_lab = _filter_wizard(products, cats, cat_tok, rar_tok)
    if not c_lab:
        await _notify_callback_issue(q, context)
        return
    if rar_tok != "all" and not in_scope:
        await q.answer()
        if q.message:
            await q.message.reply_text(
                "🔍 В этой категории таких карточек пока нет\n\n"
                "Попробуйте другую редкость или загляните в акции 🔥"
            )
        return
    if not in_scope:
        await q.answer()
        if q.message:
            await q.message.reply_text(
                "🔍 Сейчас здесь пусто\n\n"
                "Загляните в другую категорию или в акции 🔥"
            )
        return
    await q.answer()
    await _tinder_start(q, context, in_scope, products, cat_tok)


async def on_back_rarity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    m = re.match(r"^h:([^:]+)$", (q.data or "").strip())
    if not m:
        return
    await q.answer()
    cat_tok = m.group(1)
    udi = context.user_data
    _tinder_cancel_autoplay(udi)
    udi.pop("tinder_gidxs", None)
    udi.pop("tinder_i", None)
    udi.pop("tinder_cat_tok", None)
    udi.pop("tinder_message_id", None)
    udi.pop("tinder_chat_id", None)
    udi.pop("tinder_autoplay_paused", None)
    products = await _get_products(context)
    cats = _category_names(products)
    base, _, _ = _filter_wizard(products, cats, cat_tok, "all")
    if not base or not _needs_rarity_step(base):
        await _edit_to_categories(q, context)
        return
    hdr_base = "⭐ Выберите редкость:\n\nКакие карточки показать? 👇"
    if cat_tok == "all":
        t = hdr_base
        kb = _kb_rarities("all")
    else:
        try:
            cix = int(cat_tok)
        except ValueError:
            await _edit_to_categories(q, context)
            return
        if cix < 0 or cix >= len(cats):
            await _edit_to_categories(q, context)
            return
        cat = cats[cix]
        t = f"⭐ Категория: {cat}\n\nКакие карточки показать? 👇"
        kb = _kb_rarities(str(cix))
    qm = q.message
    if qm and qm.photo:
        try:
            await qm.delete()
        except Exception:
            pass
        await context.bot.send_message(
            qm.chat_id, text=t, reply_markup=kb
        )
        return
    try:
        if qm:
            await q.edit_message_text(t, reply_markup=kb)
    except Exception:
        if qm:
            await context.bot.send_message(
                qm.chat_id, text=t, reply_markup=kb
            )


async def _edit_cart_message(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = q.from_user.id if q and q.from_user else 0
    if not uid or not q.message:
        return
    _ensure_user_cart(uid, context.user_data)
    lines = _cart_get_lines_uid(uid, context.user_data)
    t = _format_cart_message(lines)
    kb = _kb_cart(lines)
    try:
        await q.edit_message_text(t, reply_markup=kb)
    except Exception:
        try:
            await q.message.reply_text(t, reply_markup=kb)
        except Exception:
            await _notify_callback_issue(q, context)


async def on_add_to_cart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    m = re.match(r"^a:(.+)$", (q.data or "").strip())
    if not m:
        return
    ref = m.group(1).strip()
    app = context.application
    products = list(app.bot_data.get("products") or [])

    product = _product_from_callback(ref, products)
    if product is None:
        extra = await load_products()
        if extra:
            app.bot_data["products"] = extra
            app.bot_data["illucards_synced_at"] = time.time()
        products = list(app.bot_data.get("products") or [])
        product = _product_from_callback(ref, products)
    if product is None or not q.from_user:
        await _notify_callback_issue(q, context)
        return
    uid = q.from_user.id
    _ensure_user_cart(uid, context.user_data)
    gix = _global_product_index(products, product)
    r = _product_ref_for_callback(product, gix)
    _cart_add_line_uid(
        uid,
        context.user_data,
        r,
        product,
        product.get("name") or "—",
        _product_price(product),
    )
    lines = _cart_get_lines_uid(uid, context.user_data)
    tot, npos = _cart_totals(lines)
    nlines = len(lines)
    short = f"{tot} BYN, шт. {npos}"
    users_touch(uid, "cart")
    await q.answer(f"🛒 В корзине: {nlines} поз. · {short}", show_alert=False)


async def on_cart_remove_line(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    m2 = re.match(r"^rm:(\d+)$", (q.data or "").strip())
    if not m2:
        return
    ix = int(m2.group(1))
    uid = q.from_user.id if q.from_user else 0
    _ensure_user_cart(uid, context.user_data)
    if not _cart_remove_line_uid(uid, context.user_data, ix):
        await _notify_callback_issue(q, context)
        return
    users_touch(uid, "cart")
    await q.answer()
    await _edit_cart_message(q, context)


async def on_cart_increment(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    m = re.match(r"^ic:(\d+)$", (q.data or "").strip())
    if not m:
        return
    ix = int(m.group(1))
    uid = q.from_user.id if q.from_user else 0
    _ensure_user_cart(uid, context.user_data)
    lines = _cart_get_lines_uid(uid, context.user_data)
    if not (0 <= ix < len(lines)):
        await _notify_callback_issue(q, context)
        return
    ref = str(lines[ix].get("ref"))
    app = context.application
    products: List[dict] = list(app.bot_data.get("products") or [])
    product = _product_from_callback(ref, products)
    if product is None:
        extra = await load_products()
        if extra:
            app.bot_data["products"] = extra
        products = list(app.bot_data.get("products") or [])
        product = _product_from_callback(ref, products)
    if product is None:
        await _notify_callback_issue(q, context)
        return
    gix = _global_product_index(products, product)
    r = _product_ref_for_callback(product, gix)
    _cart_add_line_uid(
        uid,
        context.user_data,
        r,
        product,
        product.get("name") or "—",
        _product_price(product),
    )
    users_touch(uid, "cart")
    await q.answer()
    await _edit_cart_message(q, context)


async def on_cart_decrement(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    m = re.match(r"^dc:(\d+)$", (q.data or "").strip())
    if not m:
        return
    ix = int(m.group(1))
    uid = q.from_user.id if q.from_user else 0
    _ensure_user_cart(uid, context.user_data)
    if not _cart_dec_line_uid(uid, context.user_data, ix):
        await _notify_callback_issue(q, context)
        return
    users_touch(uid, "cart")
    await q.answer()
    await _edit_cart_message(q, context)


async def on_cart_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if re.match(r"^cz:0$", (q.data or "").strip()) is None:
        return
    uid = q.from_user.id if q.from_user else 0
    ud = context.user_data
    pend = ud.get("awaiting_payment_order_id")
    if pend is not None and uid:
        try:
            po = ORDERS.get(int(pend))
        except (TypeError, ValueError):
            po = None
        if po and not po.get("paid") and int(po.get("user_id") or 0) == int(uid):
            _clear_crypto_auto_watch(po, uid)
            ud.pop("awaiting_payment_order_id", None)
            ud.pop("payment_pending_method", None)
    pr = _user_state_get(uid, "awaiting_proof")
    if pr is not None and uid:
        try:
            pro = ORDERS.get(int(pr))
        except (TypeError, ValueError):
            _user_state_pop(uid, "awaiting_proof")
            pro = None
        if pro and not pro.get("paid") and int(pro.get("user_id") or 0) == int(uid):
            _clear_crypto_auto_watch(pro, uid)
            _user_state_pop(uid, "awaiting_proof")
            ud.pop("payment_pending_method", None)
    if uid:
        _cart_clear_uid(uid)
    if uid:
        users_touch(uid, "cart")
    await q.answer("🧹 Готово", show_alert=False)
    await _edit_cart_message(q, context)


async def on_checkout_ask_username(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """co:0 — выбор страны доставки, затем превью и подтверждение."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if re.match(r"^co:0$", (q.data or "").strip()) is None:
        return
    uid = q.from_user.id if q.from_user else 0
    if not uid:
        await _notify_callback_issue(q, context)
        return
    _ensure_user_cart(uid, context.user_data)
    lines = _cart_get_lines_uid(uid, context.user_data)
    if not lines:
        await q.answer()
        if q.message:
            await q.message.reply_text(
                _format_cart_message([]),
                reply_markup=_kb_cart([]),
            )
        return
    ud = context.user_data
    ud["order_checkout"] = deepcopy(lines)
    ud.pop("delivery_country", None)
    ud.pop("delivery_label", None)
    ud.pop("delivery_amount", None)
    ud.pop("delivery_currency", None)
    users_touch(uid, "checkout")
    await q.answer()
    await q.message.reply_text(
        "🚚 Куда доставить заказ?\n\nВыберите страну 👇",
        reply_markup=_kb_delivery_country(),
    )


async def on_delivery_country_pick(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """dl:by|ru|ua|ot — страна доставки и превью заказа."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    m = re.match(r"^dl:(by|ru|ua|ot)$", (q.data or "").strip())
    if not m:
        return
    code = m.group(1)
    ud = context.user_data
    lines: Optional[List[dict]] = ud.get("order_checkout")
    if not lines:
        await _notify_callback_issue(q, context)
        return
    opt = DELIVERY_OPTIONS.get(code)
    if not opt:
        await _notify_callback_issue(q, context)
        return
    dlabel, damount, dcur = opt[0], opt[1], opt[2]
    ud["delivery_country"] = code
    ud["delivery_label"] = dlabel
    ud["delivery_amount"] = int(damount)
    ud["delivery_currency"] = dcur
    preview = _format_order_preview_with_delivery(ud)
    if not preview:
        await _notify_callback_issue(q, context)
        return
    await q.answer()
    await q.message.reply_text(
        preview,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("✅", callback_data="ta:0")]],
        ),
    )


async def on_send_order_to_admin(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """ta:0 — в чат админа: новый заказ, список, username."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if (q.data or "").strip() != "ta:0":
        return
    ud = context.user_data
    uid_chk = q.from_user.id if q.from_user else 0
    ap = _user_state_get(uid_chk, "awaiting_proof")
    if ap is not None:
        try:
            po = ORDERS.get(int(ap))
        except (TypeError, ValueError):
            _user_state_pop(uid_chk, "awaiting_proof")
            po = None
        if po is not None and not po.get("paid"):
            try:
                await q.answer("📸", show_alert=True)
            except Exception:
                pass
            return
    pend = ud.get("awaiting_payment_order_id")
    if pend is not None:
        try:
            po = ORDERS.get(int(pend))
        except (TypeError, ValueError):
            po = None
            ud.pop("awaiting_payment_order_id", None)
        if po is not None and not po.get("paid"):
            try:
                await q.answer("💳", show_alert=True)
            except Exception:
                pass
            return
    lines: Optional[List[dict]] = ud.get("order_checkout")
    if not lines:
        await _notify_callback_issue(q, context)
        return
    if not ud.get("delivery_country"):
        await _notify_callback_issue(q, context)
        return
    u = q.from_user
    if not u:
        await _notify_callback_issue(q, context)
        return
    uid = u.id
    drec = {
        "country": ud.get("delivery_country"),
        "label": ud.get("delivery_label"),
        "amount": ud.get("delivery_amount"),
        "currency": ud.get("delivery_currency"),
    }
    goods_total, _ = _cart_totals(list(lines))
    order_rec = {
        "id": "0",
        "items": deepcopy(list(lines)),
        "total": int(goods_total),
        "total_goods": int(goods_total),
        "delivery": drec,
        "status": "В обработке",
    }
    if drec.get("country") == "by" and drec.get("currency") == "BYN":
        order_rec["total"] = int(goods_total) + int(drec.get("amount") or 0)
    oid = await _notify_admin_new_order(
        context, u, list(lines), int(order_rec["total"]), deepcopy(drec)
    )
    if oid is None:
        await _notify_callback_issue(q, context)
        return
    order_rec["id"] = str(oid)
    USER_ORDERS.setdefault(uid, []).append(order_rec)
    ORDERS[int(oid)]["clear_cart_on_paid"] = True
    ud["awaiting_payment_order_id"] = int(oid)
    ud.pop("payment_pending_method", None)
    _clear_checkout_delivery(ud)
    await q.answer()
    tot = int(order_rec["total"])
    await q.message.reply_text(
        _payment_intro_text(tot),
        reply_markup=_kb_payment_methods(),
    )
    users_touch(uid, "payment")


async def on_payment_method(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """pay_card | pay_transfer | pay_crypto — реквизиты и кнопка «Я оплатил»."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^pay_(card|transfer|crypto)$", (q.data or "").strip())
    if not m:
        return
    uid = q.from_user.id
    ud = context.user_data
    oid_raw = ud.get("awaiting_payment_order_id")
    if oid_raw is None:
        try:
            await q.answer()
        except Exception:
            pass
        return
    try:
        oid = int(oid_raw)
    except (TypeError, ValueError):
        ud.pop("awaiting_payment_order_id", None)
        try:
            await q.answer()
        except Exception:
            pass
        return
    o = ORDERS.get(oid)
    if not o or int(o.get("user_id") or 0) != int(uid):
        try:
            await q.answer()
        except Exception:
            pass
        return
    if o.get("paid"):
        try:
            await q.answer()
        except Exception:
            pass
        return
    if o.get("payment_proof_submitted") and not o.get("paid"):
        try:
            await q.answer(PAY_PROOF_WAIT, show_alert=True)
        except Exception:
            pass
        return
    if _user_state_get(uid, "awaiting_proof") is not None:
        try:
            await q.answer("📸", show_alert=True)
        except Exception:
            pass
        return
    method = m.group(1)
    ud["payment_pending_method"] = method
    if method == "crypto":
        _user_state_set(uid, "crypto_check", oid)
        o["crypto_auto_active"] = True
        o["crypto_auto_deadline"] = time.time() + random.uniform(120.0, 300.0)
    else:
        _clear_crypto_auto_watch(o, uid)
    body_map = {
        "card": PAY_CARD_BODY,
        "transfer": PAY_TRANSFER_BODY,
        "crypto": PAY_CRYPTO_BODY,
    }
    try:
        await q.answer()
    except Exception:
        pass
    users_touch(uid, "payment")
    await q.message.reply_text(
        body_map[method],
        reply_markup=_kb_paid_confirm(),
    )


async def on_payment_paid(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """paid — подтверждение оплаты клиентом."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    if (q.data or "").strip() != "paid":
        return
    uid = q.from_user.id
    ud = context.user_data
    oid_raw = ud.get("awaiting_payment_order_id")
    if oid_raw is None:
        try:
            await q.answer()
        except Exception:
            pass
        return
    try:
        oid = int(oid_raw)
    except (TypeError, ValueError):
        ud.pop("awaiting_payment_order_id", None)
        try:
            await q.answer()
        except Exception:
            pass
        return
    o = ORDERS.get(oid)
    if not o or int(o.get("user_id") or 0) != int(uid):
        try:
            await q.answer()
        except Exception:
            pass
        return
    if o.get("paid"):
        try:
            await q.answer("✅", show_alert=False)
        except Exception:
            pass
        return
    if o.get("payment_proof_submitted") and not o.get("paid"):
        try:
            await q.answer(PAY_PROOF_WAIT, show_alert=True)
        except Exception:
            pass
        return
    pr_oid = _user_state_get(uid, "awaiting_proof")
    if pr_oid is not None and int(pr_oid) == int(oid):
        try:
            await q.answer()
        except Exception:
            pass
        try:
            await q.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(PAY_PROOF_REQUEST)
        return
    pm = ud.pop("payment_pending_method", None)
    if pm:
        o["payment_pending_method"] = pm
    _clear_crypto_auto_watch(o, uid)
    _user_state_set(uid, "awaiting_proof", int(oid))
    try:
        await q.answer()
    except Exception:
        pass
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.message.reply_text(PAY_PROOF_REQUEST)


async def _send_or_edit_admin_payment_proof(
    context: ContextTypes.DEFAULT_TYPE,
    order_id: int,
    o: dict,
    file_id: str,
    caption: str,
) -> bool:
    """Сообщение админу со скрином и кнопками подтверждения; при повторной отправке — edit."""
    log = logging.getLogger(__name__)
    kb = _kb_payment_admin_review(order_id)
    cid = o.get("payment_proof_admin_chat_id")
    mid = o.get("payment_proof_admin_message_id")
    if cid is not None and mid is not None:
        try:
            await context.bot.edit_message_media(
                chat_id=int(cid),
                message_id=int(mid),
                media=InputMediaPhoto(media=file_id, caption=caption),
                reply_markup=kb,
            )
            return True
        except Exception as e:
            log.info("payment proof edit_media: %s", e)
    try:
        sent = await context.bot.send_photo(
            chat_id=ORDER_NOTIFY_TARGET,
            photo=file_id,
            caption=caption,
            reply_markup=kb,
        )
        o["payment_proof_admin_chat_id"] = int(sent.chat_id)
        o["payment_proof_admin_message_id"] = int(sent.message_id)
        return True
    except Exception:
        log.exception("скрин оплаты → админ order_id=%s", order_id)
        return False


async def on_payment_proof_photo(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Фото-скрин оплаты при awaiting_proof; оплата у заказа — после подтверждения админом."""
    msg = update.effective_message
    if not msg or not msg.photo:
        return
    ud = context.user_data
    uid = msg.from_user.id if msg.from_user else 0
    oid_raw = _user_state_get(uid, "awaiting_proof")
    if oid_raw is None:
        return
    if not uid:
        return
    try:
        oid = int(oid_raw)
    except (TypeError, ValueError):
        _user_state_pop(uid, "awaiting_proof")
        return
    o = ORDERS.get(oid)
    if not o or int(o.get("user_id") or 0) != int(uid):
        _user_state_pop(uid, "awaiting_proof")
        return
    if o.get("paid"):
        _user_state_pop(uid, "awaiting_proof")
        await msg.reply_text("✅")
        return
    _clear_crypto_auto_watch(o, uid)
    file_id = msg.photo[-1].file_id
    cap = f"📸 #{oid} · {uid}"
    ok = await _send_or_edit_admin_payment_proof(context, oid, o, file_id, cap)
    if not ok:
        await msg.reply_text("❌")
        return
    o["payment_proof_submitted"] = True
    o["proof_file_id"] = file_id
    _user_state_pop(uid, "awaiting_proof")
    await msg.reply_text(PAY_PROOF_WAIT)


async def on_admin_confirm_payment(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^confirm_payment_(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        try:
            await q.answer()
        except Exception:
            pass
        return
    try:
        oid = int(m.group(1))
    except ValueError:
        await q.answer()
        return
    o = ORDERS.get(oid)
    if not o:
        await q.answer()
        return
    if o.get("paid"):
        try:
            await q.answer()
        except Exception:
            pass
        return
    if not o.get("payment_proof_submitted"):
        try:
            await q.answer()
        except Exception:
            pass
        return
    o["paid"] = True
    o["paid_at"] = time.time()
    cust = int(o.get("user_id") or 0)
    clear_cart = bool(o.pop("clear_cart_on_paid", False))
    if clear_cart and cust:
        _cart_clear_uid(cust)
    _user_state_clear_payment_states(cust)
    cud = _user_data_for(context.application, cust)
    cud.pop("awaiting_payment_order_id", None)
    o.pop("crypto_auto_active", None)
    o.pop("crypto_auto_deadline", None)
    try:
        await q.answer()
    except Exception:
        pass
    try:
        prev = (q.message.caption or "").strip()
        await q.message.edit_caption(
            caption=prev + "\n\n✅",
            reply_markup=None,
        )
    except Exception:
        pass
    if cust:
        await _send_payment_receipt(context.bot, cust, oid, o)


async def on_receipt_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки чека: Мои заказы / Поддержка."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^rcpt_(orders|support)$", (q.data or "").strip())
    if not m:
        return
    uid = q.from_user.id if q.from_user else 0
    if not uid:
        try:
            await q.answer()
        except Exception:
            pass
        return
    action = m.group(1)
    try:
        await q.answer()
    except Exception:
        pass
    if action == "orders":
        body, kb = _format_mine_orders_text_and_kb(uid)
        await q.message.reply_text(body, reply_markup=kb)
    else:
        user_support_state[uid] = True
        await q.message.reply_text(SUPPORT_INTRO_TEXT)


async def on_admin_reject_payment(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^reject_payment_(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        try:
            await q.answer()
        except Exception:
            pass
        return
    try:
        oid = int(m.group(1))
    except ValueError:
        await q.answer()
        return
    o = ORDERS.get(oid)
    if not o:
        await q.answer()
        return
    if o.get("paid"):
        try:
            await q.answer()
        except Exception:
            pass
        return
    if not o.get("payment_proof_submitted"):
        try:
            await q.answer()
        except Exception:
            pass
        return
    cust = int(o.get("user_id") or 0)
    cud = _user_data_for(context.application, cust) if cust else {}
    _clear_crypto_auto_watch(o, cust)
    o["payment_proof_submitted"] = False
    o.pop("proof_file_id", None)
    o.pop("payment_proof_admin_chat_id", None)
    o.pop("payment_proof_admin_message_id", None)
    if cust:
        _user_state_set(cust, "awaiting_proof", int(oid))
        await _send_customer_plain(
            context.bot, cust, PAY_ADMIN_REJECTED_CLIENT
        )
    try:
        await q.answer()
    except Exception:
        pass
    try:
        prev = (q.message.caption or "").strip()
        await q.message.edit_caption(
            caption=prev + "\n\n❌",
            reply_markup=None,
        )
    except Exception:
        pass


async def on_view_cart_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    q = update.callback_query
    if not q or not q.message or not q.data:
        return
    m = re.match(r"^vc:0$", (q.data or "").strip())
    if not m:
        return
    await q.answer()
    uid = q.from_user.id if q.from_user else 0
    _ensure_user_cart(uid, context.user_data)
    lines = _cart_get_lines_uid(uid, context.user_data)
    t = _format_cart_message(lines)
    kb = _kb_cart(lines)
    await q.message.reply_text(t, reply_markup=kb)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.text:
        return
    text = msg.text.strip()
    user_data = context.user_data
    uid = msg.from_user.id if msg.from_user else 0

    if _user_state_get(uid, "awaiting_proof") is not None:
        if text in REPLY_MENU_TEXTS:
            _user_state_pop(uid, "awaiting_proof")
        else:
            await msg.reply_text("📸")
            return

    cc_raw = _user_state_get(uid, "crypto_check")
    if cc_raw is not None and text in REPLY_MENU_TEXTS:
        try:
            co = ORDERS.get(int(cc_raw))
        except (TypeError, ValueError):
            co = None
            _user_state_pop(uid, "crypto_check")
        if co is not None and not co.get("paid"):
            _clear_crypto_auto_watch(co, uid)

    menu_keys = (
        BTN_CATALOG,
        BTN_CART,
        BTN_CHAT,
        BTN_DELIVERY,
        BTN_MY_ORDERS,
        BTN_POPULAR,
        BTN_RANDOM_CARD,
    )
    sup_uid = user_data.get("reply_support_user_id")
    rep_oid = user_data.get("reply_to")
    if is_admin(uid) and (sup_uid is not None or rep_oid is not None):
        if text in menu_keys:
            user_data.pop("reply_to", None)
            user_data.pop("reply_support_user_id", None)
        elif not text:
            await msg.reply_text("✍️")
            return
        elif sup_uid is not None:
            target = int(sup_uid)
            body = "💬 Поддержка:\n\n" + msg.text
            if len(body) > 4096:
                body = body[:4090] + "…"
            ok = await _send_customer_plain(context.bot, target, body)
            if not ok:
                await msg.reply_text("❌")
                return
            user_data.pop("reply_support_user_id", None)
            await msg.reply_text("✅")
            return
        else:
            oid_raw = user_data.get("reply_to")
            try:
                oid_int = int(oid_raw)
            except (TypeError, ValueError):
                user_data.pop("reply_to", None)
                await msg.reply_text("❌")
                return
            o = ORDERS.get(oid_int)
            if not o:
                user_data.pop("reply_to", None)
                await msg.reply_text("❌")
                return
            target = int(o.get("user_id") or 0)
            if not target:
                user_data.pop("reply_to", None)
                await msg.reply_text("❌")
                return
            body = "💬 Ответ от администратора:\n\n" + msg.text
            if len(body) > 4096:
                body = body[:4090] + "…"
            ok = await _send_customer_plain(context.bot, target, body)
            if not ok:
                await msg.reply_text("❌")
                return
            user_data.pop("reply_to", None)
            await msg.reply_text("✅")
            return

    if text == BTN_CHAT:
        if not uid:
            await msg.reply_text(FALLBACK_USER_TEXT)
            return
        _clear_checkout_delivery(user_data)
        user_data.pop("pending_order", None)
        user_support_state[uid] = True
        await msg.reply_text(SUPPORT_INTRO_TEXT)
        return

    if uid and user_support_state.get(uid):
        if text in REPLY_MENU_TEXTS:
            user_support_state.pop(uid, None)
        elif not text.strip():
            await msg.reply_text("…")
            return
        else:
            body = "💬 Сообщение от клиента:\n\n" + msg.text
            uname = (msg.from_user.username or "").strip() if msg.from_user else ""
            tail = f"\n\n👤 id {uid}"
            if uname:
                tail += f" @{uname}"
            body = (body + tail)[:4096]
            log = logging.getLogger(__name__)
            sup_kb = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "💬",
                            callback_data=f"sup:rep:{uid}",
                        )
                    ],
                ],
            )
            try:
                await context.bot.send_message(
                    ORDER_NOTIFY_TARGET,
                    body,
                    disable_web_page_preview=True,
                    reply_markup=sup_kb,
                )
            except Exception:
                log.exception("клиент → админ (поддержка)")
                await msg.reply_text("❌")
                return
            user_support_state.pop(uid, None)
            await msg.reply_text("✅")
            return

    if text in (
        BTN_CATALOG,
        BTN_MY_ORDERS,
        BTN_DELIVERY,
        BTN_POPULAR,
        BTN_RANDOM_CARD,
    ):
        _clear_checkout_delivery(user_data)
        user_data.pop("pending_order", None)

    if text == BTN_CART:
        _ensure_user_cart(uid, user_data)
        cl = _cart_get_lines_uid(uid, user_data)
        t = _format_cart_message(cl)
        kb = _kb_cart(cl)
        await msg.reply_text(t, reply_markup=kb)
        return

    if text == BTN_MY_ORDERS:
        if not uid:
            await msg.reply_text(FALLBACK_USER_TEXT)
            return
        body, kb = _format_mine_orders_text_and_kb(uid)
        await msg.reply_text(body, reply_markup=kb)
        return

    if text == BTN_CATALOG:
        await send_catalog(update, context)
        return

    if text == BTN_DELIVERY:
        await msg.reply_text(_delivery_info_text())
        return

    if text == BTN_POPULAR:
        await send_popular_deck(update, context)
        return

    if text == BTN_RANDOM_CARD:
        await send_random_card(update, context)
        return

async def post_init(application: Application) -> None:
    log = logging.getLogger(__name__)
    log.info(
        "Уведомления о заказах: target=%s, mention=%s",
        ORDER_NOTIFY_TARGET,
        ORDER_MENTION,
    )
    log.info("Админ-панель: ADMIN_ID=%s", ADMIN_ID)
    try:
        initial = await load_products()
        application.bot_data["products"] = initial
        application.bot_data["illucards_synced_at"] = time.time()
        log.info("Illucards: старт, карточек: %d", len(initial))
    except Exception:
        log.exception("Illucards: ошибка стартовой загрузки")
        application.bot_data["products"] = application.bot_data.get("products") or []
    if application.job_queue and SYNC_EVERY_SEC > 0:
        application.job_queue.run_repeating(
            illucards_sync_job,
            interval=SYNC_EVERY_SEC,
            first=SYNC_EVERY_SEC,
            name="illucards_sync",
        )
        log.info("Фоновая синхронизация illucards: каждые %s с", SYNC_EVERY_SEC)
    if application.job_queue:
        application.job_queue.run_repeating(
            crypto_auto_check_job,
            interval=30,
            first=30,
            name="crypto_auto_check",
        )
        log.info("Крипто mock-проверка: каждые 30 с")
    print("Бот запущен!")
    me = await application.bot.get_me()
    if me.username:
        print(f"https://t.me/{me.username}")
    if os.getenv("LOGIN_API_DISABLE", "").strip() != "1":
        try:
            asyncio.create_task(_run_login_http_api(application.bot))
        except Exception:
            log.exception("Вход через Telegram: HTTP API не запущен")


def main() -> None:
    if not token:
        sys.exit("TELEGRAM_BOT_TOKEN is not set")

    app = Application.builder().token(token).post_init(post_init).build()

    app.add_handler(
        TypeHandler(Update, track_user_activity, block=False),
        group=-1,
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("catalog", catalog_cmd))
    app.add_handler(CommandHandler("promo", send_promo))
    app.add_handler(CommandHandler("swipe", send_tinder_mode))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("say", admin_say_cmd))
    app.add_handler(
        CallbackQueryHandler(
            on_admin_panel_action,
            pattern=re.compile(r"^adm:(orders|stats)$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_user_order_open,
            pattern=re.compile(r"^user_order_\d+$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_admin_open_order,
            pattern=re.compile(r"^open_order_\d+$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_order_status_buttons,
            pattern=re.compile(r"^(accept|sent|cancel|done)_\d+$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_order_admin_action,
            pattern=re.compile(r"^oam:rep:\d+$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_support_reply_activate,
            pattern=re.compile(r"^sup:rep:\d+$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_deep_link_structured_submit,
            pattern=re.compile(r"^dlco:[a-fA-F0-9]{16}$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_deep_link_structured_cancel,
            pattern=re.compile(r"^dlca:[a-fA-F0-9]{16}$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(on_deep_link_confirm_order, pattern=re.compile(r"^confirm_order$"))
    )
    app.add_handler(
        CallbackQueryHandler(on_deep_link_cancel_order, pattern=re.compile(r"^cancel_order$"))
    )
    app.add_handler(CallbackQueryHandler(on_checkout_ask_username, pattern=re.compile(r"^co:0$")))
    app.add_handler(
        CallbackQueryHandler(on_delivery_country_pick, pattern=re.compile(r"^dl:(by|ru|ua|ot)$"))
    )
    app.add_handler(
        CallbackQueryHandler(
            on_payment_method,
            pattern=re.compile(r"^pay_(card|transfer|crypto)$"),
        )
    )
    app.add_handler(CallbackQueryHandler(on_payment_paid, pattern=re.compile(r"^paid$")))
    app.add_handler(
        CallbackQueryHandler(
            on_admin_confirm_payment,
            pattern=re.compile(r"^confirm_payment_\d+$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_admin_reject_payment,
            pattern=re.compile(r"^reject_payment_\d+$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_receipt_callback,
            pattern=re.compile(r"^rcpt_(orders|support)$"),
        )
    )
    app.add_handler(CallbackQueryHandler(on_send_order_to_admin, pattern=re.compile(r"^ta:0$")))
    app.add_handler(CallbackQueryHandler(on_view_cart_callback, pattern=re.compile(r"^vc:0$")))
    app.add_handler(CallbackQueryHandler(on_cart_increment, pattern=re.compile(r"^ic:(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_cart_decrement, pattern=re.compile(r"^dc:(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_cart_remove_line, pattern=re.compile(r"^rm:(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_cart_clear, pattern=re.compile(r"^cz:0$")))
    app.add_handler(CallbackQueryHandler(on_tinder_swipe, pattern=re.compile(r"^t:([pncf])$")))
    app.add_handler(CallbackQueryHandler(on_add_to_cart, pattern=re.compile(r"^a:(.+)$")))
    app.add_handler(CallbackQueryHandler(on_back_rarity, pattern=re.compile(r"^h:([^:]{1,12})$")))
    app.add_handler(CallbackQueryHandler(on_popular_inline, pattern=re.compile(r"^pop:0$")))
    app.add_handler(CallbackQueryHandler(on_menu_main, pattern=re.compile(r"^m:0$")))
    app.add_handler(
        CallbackQueryHandler(
            on_pick_rarity, pattern=re.compile(r"^j:([^:]{1,12}):(all|sale|\d+)$")
        )
    )
    app.add_handler(CallbackQueryHandler(on_pick_category, pattern=re.compile(r"^c:(\d+|all)$")))
    app.add_handler(MessageHandler(filters.PHOTO, on_payment_proof_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
