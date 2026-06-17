# .venv + dotenv (Mac):
#   source .venv/bin/activate
#   pip install python-dotenv
#   pip install -r requirements.txt
#   pip list | grep dotenv
#   python bot.py
# Если pip не найден: python3 -m pip install python-dotenv
import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import re
import secrets
import sys
import time
import urllib.parse
import urllib.request
from copy import deepcopy
from datetime import datetime
from threading import Thread
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

import aiohttp
import redis
from flask import Flask, jsonify, request
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
from telegram.error import Conflict
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
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


def _normalize_bot_token(raw: Optional[str]) -> str:
    """Render/панели часто добавляют пробелы или оборачивают значение в кавычки."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1].strip()
    return s


token = _normalize_bot_token(os.getenv("TELEGRAM_BOT_TOKEN"))


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        logging.getLogger(__name__).warning(
            "%s=%r is not an integer; using %s",
            name,
            raw,
            default,
        )
        return default


def _start_flask_health_server() -> None:
    """Минимальный HTTP-сервер для Render: проверка по / (HEAD/GET) и /health."""
    log = logging.getLogger(__name__)
    app = Flask("health_server")

    @app.route("/", methods=("GET", "HEAD"))
    def root() -> tuple:
        # Render по умолчанию шлёт HEAD / на PORT — без маршрута был 404 в логах.
        if request.method == "HEAD":
            return "", 200
        return jsonify({"ok": True}), 200

    @app.get("/health")
    def health() -> tuple:
        return jsonify({"ok": True}), 200

    raw_port = (os.getenv("PORT") or "10000").strip()
    try:
        port = int(raw_port)
    except ValueError:
        port = 10000
    try:
        # threaded=True, чтобы healthcheck не блокировал процесс бота.
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)
    except Exception:
        log.exception("Flask health server failed on port %s", port)


def _ensure_flask_health_server_thread() -> None:
    t = globals().get("_FLASK_HEALTH_THREAD")
    if isinstance(t, Thread) and t.is_alive():
        return
    thr = Thread(target=_start_flask_health_server, name="flask_health", daemon=True)
    globals()["_FLASK_HEALTH_THREAD"] = thr
    thr.start()


# Админ: id только из env (Render Dashboard), не захардкожен в коде.
def _read_primary_admin_id() -> int:
    for key in (
        "TELEGRAM_ADMIN_ID",
        "TELEGRAM_ADMIN_CHAT_ID",
        "ILLUCARDS_TELEGRAM_ADMIN_CHAT_ID",
        "TELEGRAM_ORDER_NOTIFY_ID",
        "ORDER_NOTIFY_CHAT_ID",
    ):
        raw = (os.getenv(key) or "").strip()
        if raw.isdecimal():
            return int(raw)
    return 0


ADMIN_ID = _read_primary_admin_id()
BOT_BUILD_ID = "2026-05-29-postpaid-checkout-v28"


# Куда бот пишет о новых заказах: по умолчанию ADMIN_ID; переопределение — TELEGRAM_ORDER_NOTIFY_ID.
def _read_order_notify_target():
    """Куда слать заказы: из .env int id или @username; иначе ADMIN_ID из env."""
    s = (os.getenv("TELEGRAM_ORDER_NOTIFY_ID") or os.getenv("ORDER_NOTIFY_CHAT_ID") or "").strip()
    if not s:
        return int(ADMIN_ID) if ADMIN_ID else 0
    if s.startswith("@"):
        return s
    try:
        return int(s)
    except ValueError:
        return s


ORDER_NOTIFY_TARGET = _read_order_notify_target()
ORDER_MENTION = (os.getenv("ORDER_MENTION", "@Daniel_official") or "@Daniel_official").strip()
_ADMIN_IDS_EXTRA: set = set()
for _adm_raw in (os.getenv("TELEGRAM_ADMIN_IDS") or "").split(","):
    _adm_raw = _adm_raw.strip()
    if _adm_raw.isdecimal():
        _ADMIN_IDS_EXTRA.add(int(_adm_raw))
ADMIN_IDS: set = set(_ADMIN_IDS_EXTRA)
if ADMIN_ID:
    ADMIN_IDS.add(int(ADMIN_ID))
ADMIN_ACCESS_DENIED = "Нет доступа: эта команда только для администратора."


def is_admin(user_id) -> bool:
    try:
        return int(user_id) in ADMIN_IDS
    except (TypeError, ValueError):
        return False


def _resolve_admin_chat_id() -> Optional[int]:
    raw = (
        os.getenv("TELEGRAM_ADMIN_CHAT_ID")
        or os.getenv("ILLUCARDS_TELEGRAM_ADMIN_CHAT_ID")
        or os.getenv("TELEGRAM_ORDER_NOTIFY_ID")
        or os.getenv("ORDER_NOTIFY_CHAT_ID")
        or ""
    )
    s = str(raw).strip()
    if not s:
        return None
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _order_belongs_to_telegram_user(order: dict, user_id: int) -> bool:
    try:
        return int(order.get("user_id") or 0) == int(user_id)
    except (TypeError, ValueError):
        return False


# Заказы и админка:
# 1) Клиенты не получают сообщений с admin inline-кнопками (accept_/sent_/cancel_/delmsg_/adm:/oam: и т.д.).
# 2) При оформлении из корзины — только ORDERS; в ORDER_NOTIFY_TARGET карточка заказа с кнопками
#    принять/отправить только после подтверждения оплаты по фото (_send_deferred_admin_order_panel).
#    Первое сообщение админу по заказу — фото чека с кнопками подтвердить/отклонить оплату.
#    «Подтвердить заказ» / ta:0 / confirm_order — в ORDER_NOTIFY_TARGET текст заказа не шлём.
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


# Корзина и оформленные заказы в памяти процесса (ключ — Telegram user_id)
USER_CART: dict = {}
# После входа на сайт по коду: текст черновика до «Подтвердить / Отмена» (user_data может быть пуст)
SITE_LOGIN_PENDING_ORDER: Dict[int, str] = {}
# Последний order_id с сайта по user_id (checkout → sync/cart).
PENDING_SITE_ORDER_BY_USER: Dict[int, str] = {}
# Уже отправленный в чат checkout с сайта (uid:order_id → timestamp), без дублей при /start.
_SITE_CHECKOUT_PUSHED: Dict[str, float] = {}
USER_FAVORITES: dict = {}
# Списки избранного в POST /api/sync/state, /api/sync/cart, /api/verify-code (ключ «items» сюда
# не входит — в sync/cart «items» это корзина). Для POST /api/sync/favorites дополнительно читают «items».
_FAVORITE_SYNC_PAYLOAD_KEYS: Tuple[str, ...] = (
    "favorites",
    "favoriteItems",
    "favorite_list",
    "favoriteList",
    "wishlist",
    "wishList",
    "likedProducts",
    "favoriteIds",
    "favorite_ids",
    "wishlistIds",
    "wishlist_ids",
    "savedFavorites",
    "saved_favorites",
    "heartedProducts",
    "heartedProductIds",
)
# Баннеры витрины с сайта (POST /api/sync/promotions с сайта или GET HOME_PROMOTIONS_JSON_URL)
HOME_PAGE_PROMOTIONS: List[dict] = []
USER_ORDERS: dict = {}
# Глобальный реестр заказов по порядковому id (память процесса)
# ORDERS[id] = { user_id, username, items, total, delivery, status, created_at, admin_* }
# Переписка «адрес после оплаты»: user_states[uid]["postpaid_thread_oid"] = order_id →
# текст/фото уходит админу reply к карточке заказа (admin_message_id), пока заказ не done/canceled.
ORDERS: dict = {}
ORDER_COUNTER = 1
# /start order_<id> — черновики заказов по ссылке (до оформления). Пополняется API/сайтом или register_shared_deep_link_order.
SHARED_DEEP_LINK_ORDERS: dict = {}
# Режим «написать админу»: user_id -> True, пока ждём следующее текстовое сообщение
user_support_state: dict = {}
# Состояния оплаты по user_id (вне user_data — не теряются при смене контекста чата)
# user_states[user_id] = { "awaiting_proof": order_id, "crypto_check": order_id }
user_states: Dict[int, Dict[str, int]] = {}
# Предпочтительная страна доставки/валюта по пользователю (источник: сайт+бот).
USER_PREF_DELIVERY_COUNTRY: Dict[int, str] = {}
# Отслеживание пользователей (память процесса): активность, снимок корзины, флаг заказов
USERS: Dict[int, dict] = {}
USER_MESSAGES: Dict[int, List[dict]] = {}
TEMP_MESSAGE_TTL_SEC = _env_int("TEMP_MESSAGE_TTL_SEC", 180)
STATE_FILE = (os.getenv("BOT_STATE_FILE") or "bot_state.json").strip()
STATE_REDIS_KEY = (
    os.getenv("BOT_STATE_REDIS_KEY")
    or os.getenv("STATE_REDIS_KEY")
    or "illucards:telegram-bot:state"
).strip()
STATE_REDIS_SAVE_BLOCKED = False
STATE_REDIS_CLIENT = None

# Вход на сайт illucards.by: POST /api/send-code, /api/verify-code (память процесса)
LOGIN_CODES: dict = {}
LOGIN_CODE_TTL_SEC = 5 * 60
# нормализованный @username (без @, lower) → telegram user_id (кто писал боту)
USERNAME_TO_USER_ID: Dict[str, int] = {}
SYNC_API_SECRET = (os.getenv("TELEGRAM_SYNC_API_SECRET") or "").strip()


def _normalize_login_username(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("@"):
        s = s[1:]
    return s.lower()


def _register_login_username(user_id: int, username: Optional[str]) -> None:
    """Запомнить @username → user_id для POST /api/send-code с сайта."""
    uid = int(user_id or 0)
    if not uid:
        return
    un = (username or "").strip()
    if not un:
        return
    key = _normalize_login_username(un)
    if key:
        prev_uid = USERNAME_TO_USER_ID.get(key)
        USERNAME_TO_USER_ID[key] = uid
        row = users_ensure(uid)
        prev_username = row.get("username")
        row["username"] = key
        if prev_uid != uid or prev_username != key:
            save_state()


def _int_key_dict(raw: object) -> dict:
    if not isinstance(raw, dict):
        return {}
    out = {}
    for k, v in raw.items():
        try:
            out[int(k)] = v
        except (TypeError, ValueError):
            continue
    return out


def _state_redis_credentials() -> Optional[Tuple[str, str]]:
    url = (
        os.getenv("UPSTASH_REDIS_REST_URL")
        or os.getenv("KV_REST_API_URL")
        or ""
    ).strip().rstrip("/")
    token = (
        os.getenv("UPSTASH_REDIS_REST_TOKEN")
        or os.getenv("KV_REST_API_TOKEN")
        or ""
    ).strip()
    if not url or not token:
        return None
    return url, token


def _state_redis_url_client():
    global STATE_REDIS_CLIENT
    redis_url = (os.getenv("REDIS_URL") or "").strip()
    if not redis_url:
        return None
    if STATE_REDIS_CLIENT is not None:
        return STATE_REDIS_CLIENT
    try:
        print("REDIS URL =", redis_url)
        STATE_REDIS_CLIENT = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            ssl_cert_reqs=None,
        )
        STATE_REDIS_CLIENT.ping()
        print("REDIS CONNECTED SUCCESSFULLY")
        logging.getLogger(__name__).info("Redis state connected via REDIS_URL")
        return STATE_REDIS_CLIENT
    except Exception as e:
        STATE_REDIS_CLIENT = None
        print("REDIS FULL ERROR:", e)
        logging.exception(e)
        return None


def _state_redis_command(args: List[object]) -> Optional[object]:
    cred = _state_redis_credentials()
    if not cred:
        client = _state_redis_url_client()
        if client is None:
            return None
        try:
            cmd = str(args[0] if args else "").upper()
            if cmd == "GET" and len(args) >= 2:
                return client.get(str(args[1]))
            if cmd == "SET" and len(args) >= 3:
                return "OK" if client.set(str(args[1]), str(args[2])) else None
            if cmd == "EXISTS" and len(args) >= 2:
                return int(client.exists(str(args[1])))
            logging.getLogger(__name__).warning("Unsupported REDIS_URL state command: %r", args)
            return None
        except Exception:
            logging.getLogger(__name__).exception("Redis state REDIS_URL command failed")
            return None
    url, token = cred
    try:
        logging.getLogger(__name__).info(
            "Redis state REST command: cmd=%s url_configured=%s token_configured=%s",
            str(args[0] if args else "").upper(),
            bool(url),
            bool(token),
        )
        body = json.dumps(args).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw = resp.read().decode("utf-8")
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and parsed.get("error"):
            logging.getLogger(__name__).warning("Redis state error: %s", parsed.get("error"))
            return None
        return parsed.get("result") if isinstance(parsed, dict) else None
    except Exception:
        logging.getLogger(__name__).exception("Redis state command failed")
        return None


def _build_state_payload() -> dict:
    return {
        "order_counter": int(ORDER_COUNTER),
        "orders": ORDERS,
        "user_orders": USER_ORDERS,
        "user_cart": USER_CART,
        "user_favorites": USER_FAVORITES,
        "user_states": user_states,
        "user_pref_delivery_country": USER_PREF_DELIVERY_COUNTRY,
        "users": USERS,
        "user_messages": USER_MESSAGES,
        "username_to_user_id": USERNAME_TO_USER_ID,
        "pending_site_order_by_user": PENDING_SITE_ORDER_BY_USER,
        "shared_deep_link_orders": SHARED_DEEP_LINK_ORDERS,
    }


def _apply_state_payload(data: dict) -> None:
    global ORDER_COUNTER
    loaded_orders = _int_key_dict(data.get("orders"))
    if loaded_orders or not ORDERS:
        ORDERS.clear()
        ORDERS.update(loaded_orders)
    loaded_user_orders = _int_key_dict(data.get("user_orders"))
    if loaded_user_orders or not USER_ORDERS:
        USER_ORDERS.clear()
        USER_ORDERS.update(loaded_user_orders)
    USER_CART.clear()
    USER_CART.update(_int_key_dict(data.get("user_cart")))
    USER_FAVORITES.clear()
    USER_FAVORITES.update(_int_key_dict(data.get("user_favorites")))
    user_states.clear()
    user_states.update(_int_key_dict(data.get("user_states")))
    USER_PREF_DELIVERY_COUNTRY.clear()
    USER_PREF_DELIVERY_COUNTRY.update(_int_key_dict(data.get("user_pref_delivery_country")))
    USERS.clear()
    USERS.update(_int_key_dict(data.get("users")))
    loaded_user_messages = _int_key_dict(data.get("user_messages"))
    if loaded_user_messages or not USER_MESSAGES:
        USER_MESSAGES.clear()
        USER_MESSAGES.update(loaded_user_messages)
    USERNAME_TO_USER_ID.clear()
    raw_login_map = data.get("username_to_user_id")
    if isinstance(raw_login_map, dict):
        for raw_key, raw_uid in raw_login_map.items():
            key = _normalize_login_username(str(raw_key or ""))
            try:
                uid = int(raw_uid or 0)
            except (TypeError, ValueError):
                uid = 0
            if key and uid:
                USERNAME_TO_USER_ID[key] = uid
    for raw_uid, row in list(USERS.items()):
        if not isinstance(row, dict):
            continue
        key = _normalize_login_username(str(row.get("username") or ""))
        try:
            uid = int(raw_uid or 0)
        except (TypeError, ValueError):
            uid = 0
        if key and uid:
            USERNAME_TO_USER_ID[key] = uid
    loaded_pending_site = data.get("pending_site_order_by_user")
    if isinstance(loaded_pending_site, dict):
        PENDING_SITE_ORDER_BY_USER.clear()
        for raw_uid, raw_oid in loaded_pending_site.items():
            try:
                uid_i = int(raw_uid or 0)
            except (TypeError, ValueError):
                uid_i = 0
            oid_s = str(raw_oid or "").strip()
            if uid_i > 0 and oid_s:
                PENDING_SITE_ORDER_BY_USER[uid_i] = oid_s
    loaded_deep_link = data.get("shared_deep_link_orders")
    if isinstance(loaded_deep_link, dict):
        SHARED_DEEP_LINK_ORDERS.clear()
        for raw_oid, raw_payload in loaded_deep_link.items():
            oid_s = str(raw_oid or "").strip()
            if oid_s and isinstance(raw_payload, dict):
                SHARED_DEEP_LINK_ORDERS[oid_s] = raw_payload
    try:
        restored_counter = int(data.get("order_counter") or 1)
    except (TypeError, ValueError):
        restored_counter = 1
    max_oid = max([0] + [int(x) for x in ORDERS.keys()])
    ORDER_COUNTER = max(restored_counter, max_oid + 1)


def save_state() -> None:
    data = _build_state_payload()
    if STATE_REDIS_SAVE_BLOCKED:
        has_persistent_data = any(
            bool(data.get(k))
            for k in ("orders", "user_orders", "user_cart", "user_favorites")
        )
        if not has_persistent_data:
            logging.getLogger(__name__).error(
                "State save skipped: persistent storage was not loaded and current state is empty."
            )
            return
    if STATE_REDIS_KEY:
        if not _state_redis_credentials() and not (os.getenv("REDIS_URL") or "").strip():
            logging.getLogger(__name__).warning(
                "Redis state is not configured; using local file only. Orders may disappear after deploy."
            )
        elif STATE_REDIS_SAVE_BLOCKED:
            logging.getLogger(__name__).warning(
                "Redis state save is blocked because startup load was not confirmed; local file fallback only."
            )
        else:
            saved = _state_redis_command(["SET", STATE_REDIS_KEY, json.dumps(data, ensure_ascii=False)])
            if saved != "OK":
                logging.getLogger(__name__).warning(
                    "Redis state save did not return OK; using local file fallback. result=%r",
                    saved,
                )
    if not STATE_FILE:
        return
    tmp = f"{STATE_FILE}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except Exception:
        logging.getLogger(__name__).exception("Не удалось сохранить состояние бота")


def _refresh_orders_state_from_redis() -> None:
    """Подтянуть ORDERS и user_states из Redis, не затирая свежее in-memory состояние."""
    if not STATE_REDIS_KEY or STATE_REDIS_SAVE_BLOCKED:
        return
    raw = _state_redis_command(["GET", STATE_REDIS_KEY])
    if not isinstance(raw, str) or not raw.strip():
        return
    try:
        data = json.loads(raw)
    except Exception:
        logging.getLogger(__name__).exception("refresh orders state: json parse failed")
        return
    if not isinstance(data, dict):
        return
    global ORDER_COUNTER
    loaded_orders = _int_key_dict(data.get("orders"))
    for oid, rec in loaded_orders.items():
        if not isinstance(rec, dict):
            continue
        local = ORDERS.get(oid)
        if not isinstance(local, dict):
            ORDERS[oid] = rec
            continue
        for key, val in rec.items():
            if key not in local or local.get(key) in (None, "", 0, False, []):
                local[key] = val
    loaded_user_orders = _int_key_dict(data.get("user_orders"))
    for uid, lst in loaded_user_orders.items():
        if uid not in USER_ORDERS or not USER_ORDERS.get(uid):
            USER_ORDERS[uid] = lst
    loaded_states = _int_key_dict(data.get("user_states"))
    for uid, bucket in loaded_states.items():
        if not isinstance(bucket, dict):
            continue
        local_b = user_states.setdefault(uid, {})
        for key, val in bucket.items():
            if key not in local_b:
                local_b[key] = val
    try:
        restored_counter = int(data.get("order_counter") or 0)
    except (TypeError, ValueError):
        restored_counter = 0
    max_oid = max([0] + [int(x) for x in ORDERS.keys()])
    ORDER_COUNTER = max(int(ORDER_COUNTER), restored_counter, max_oid + 1)


def load_state() -> None:
    global STATE_REDIS_SAVE_BLOCKED
    loaded = False
    redis_configured = bool(_state_redis_credentials() or (os.getenv("REDIS_URL") or "").strip())
    if STATE_REDIS_KEY:
        raw = None
        if not redis_configured:
            STATE_REDIS_SAVE_BLOCKED = True
            logging.getLogger(__name__).warning(
                "Redis state is not configured; trying local file fallback and blocking empty saves."
            )
        else:
            raw = _state_redis_command(["GET", STATE_REDIS_KEY])
            if raw is None:
                exists = _state_redis_command(["EXISTS", STATE_REDIS_KEY])
                if exists in (0, "0"):
                    STATE_REDIS_SAVE_BLOCKED = False
                    logging.getLogger(__name__).info(
                        "Redis state key %r is empty; starting with fresh state and allowing saves.",
                        STATE_REDIS_KEY,
                    )
                elif exists is None:
                    STATE_REDIS_SAVE_BLOCKED = True
                    logging.getLogger(__name__).warning(
                        "Redis state is configured but command failed; blocking Redis saves to avoid overwriting old orders."
                    )
                else:
                    STATE_REDIS_SAVE_BLOCKED = True
                    logging.getLogger(__name__).warning(
                        "Redis state load was not confirmed; blocking Redis saves to avoid overwriting old orders."
                    )
        if isinstance(raw, str) and raw.strip():
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    _apply_state_payload(data)
                    STATE_REDIS_SAVE_BLOCKED = False
                    logging.getLogger(__name__).info(
                        "Состояние загружено из Redis: orders=%d user_orders=%d carts=%d favorites=%d users=%d messages=%d next_order=%d",
                        len(ORDERS),
                        len(USER_ORDERS),
                        len(USER_CART),
                        len(USER_FAVORITES),
                        len(USERS),
                        len(USER_MESSAGES),
                        ORDER_COUNTER,
                    )
                    loaded = True
                    return
            except Exception:
                STATE_REDIS_SAVE_BLOCKED = True
                logging.getLogger(__name__).exception("Не удалось загрузить состояние из Redis")
    if redis_configured and not STATE_REDIS_SAVE_BLOCKED:
        return
    if not STATE_FILE or not os.path.exists(STATE_FILE):
        if not loaded:
            STATE_REDIS_SAVE_BLOCKED = True
            logging.getLogger(__name__).error(
                "No persistent state loaded: Redis unavailable/unconfigured and local state file is missing. Blocking empty saves."
            )
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        logging.getLogger(__name__).exception("Не удалось загрузить состояние бота")
        return
    _apply_state_payload(data)
    STATE_REDIS_SAVE_BLOCKED = False
    logging.getLogger(__name__).info(
        "Состояние загружено: orders=%d user_orders=%d carts=%d favorites=%d users=%d messages=%d next_order=%d",
        len(ORDERS),
        len(USER_ORDERS),
        len(USER_CART),
        len(USER_FAVORITES),
        len(USERS),
        len(USER_MESSAGES),
        ORDER_COUNTER,
    )


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
            "tg_auth_thanked": False,
            "temp_messages": [],
        }
    return USERS[uid]


def _track_temp_message(uid: int, message: Optional[Message]) -> None:
    if not uid or not message:
        return
    row = users_ensure(uid)
    items = list(row.get("temp_messages") or [])
    items.append((int(message.chat_id), int(message.message_id), time.time()))
    # Держим только последние 30 временных сообщений.
    row["temp_messages"] = items[-30:]


async def _delete_user_temp_messages(bot, uid: int) -> None:
    if not uid:
        return
    row = users_ensure(uid)
    items = list(row.get("temp_messages") or [])
    row["temp_messages"] = []
    now = time.time()
    for chat_id, message_id, created_at in items:
        # Старые хвосты просто забываем.
        if (now - float(created_at or 0)) > TEMP_MESSAGE_TTL_SEC:
            continue
        try:
            await bot.delete_message(chat_id=int(chat_id), message_id=int(message_id))
        except Exception:
            pass


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


def _remember_user_message(
    uid: int,
    username: Optional[str],
    kind: str,
    text: str,
    *,
    persist: bool = True,
) -> None:
    uid = int(uid or 0)
    if not uid or is_admin(uid):
        return
    body = str(text or "").strip()
    if not body:
        return
    bucket = USER_MESSAGES.setdefault(uid, [])
    bucket.append(
        {
            "ts": time.time(),
            "username": (username or "").strip(),
            "kind": str(kind or "text").strip() or "text",
            "text": body[:1500],
        }
    )
    if persist:
        try:
            save_state()
        except Exception:
            logging.getLogger(__name__).exception(
                "remember_user_message save_state uid=%s", uid
            )


def _user_display_name(uid: int, username: Optional[str] = None) -> str:
    un = str(username or "").strip().lstrip("@")
    if not un:
        row = USERS.get(int(uid or 0)) or {}
        un = str(row.get("username") or "").strip().lstrip("@")
    return f"@{un}" if un else f"id {int(uid or 0)}"


def _format_user_messages_for_admin(uid: int) -> str:
    rows = list(USER_MESSAGES.get(int(uid or 0)) or [])
    title = f"💬 Сообщения пользователя {_user_display_name(uid)}"
    if not rows:
        return title + "\n\nПока нет сохранённых сообщений от этого пользователя."
    lines: List[str] = [title, ""]
    for rec in rows[-30:]:
        try:
            ts = float(rec.get("ts") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        tss = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts > 0 else "—"
        kind = str(rec.get("kind") or "text")
        body = str(rec.get("text") or "").strip()
        lines.append(f"{tss} · {kind}")
        lines.append(body)
        lines.append("")
    out = "\n".join(lines).strip()
    if len(out) > 4090:
        out = out[:4086] + "…"
    return out


async def track_user_activity(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """При любом update — last_activity и снимки; last_action задаётся в хендлерах."""
    u = update.effective_user
    if not u:
        return
    try:
        uid = int(u.id)
        epoch = context.application.bot_data.get("deploy_epoch")
        if epoch is not None and context.user_data.get("_deploy_seen_epoch") != epoch:
            _apply_post_deploy_session_reset(uid, context.user_data)
            _restore_site_pending_to_user_data(uid, context.user_data)
            context.user_data["_deploy_seen_epoch"] = epoch
        users_touch(uid, activity_only=True)
        _register_login_username(uid, getattr(u, "username", None))
        _sync_user_delivery_country_to_user_data(uid, context.user_data)
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


def _user_state_set(uid: int, key: str, order_id: int, *, persist: bool = True) -> None:
    if not uid:
        return
    b = _user_state_bucket(uid)
    b[key] = int(order_id)
    if persist:
        try:
            save_state()
        except Exception:
            logging.getLogger(__name__).exception(
                "user_state_set save_state uid=%s key=%s", uid, key
            )


def _set_awaiting_payment_order_id(uid: int, user_data: dict, order_id: int) -> None:
    """Сессия оплаты — в user_data и user_states (переживает деплой)."""
    oid = int(order_id)
    if uid and isinstance(user_data, dict):
        user_data["awaiting_payment_order_id"] = oid
    if uid:
        _user_state_set(uid, "awaiting_payment_order_id", oid)


def _clear_awaiting_payment_order_id(
    uid: int, user_data: Optional[dict], *, persist: bool = True
) -> None:
    if isinstance(user_data, dict):
        user_data.pop("awaiting_payment_order_id", None)
        user_data.pop("awaiting_proof", None)
        user_data.pop("payment_pending_method", None)
    if uid:
        _user_state_pop(uid, "awaiting_payment_order_id", persist=persist)


def _proof_order_owned_by_user(uid: int, oid: int) -> bool:
    """Чек только к своему неоплаченному заказу — не к чужому клиенту."""
    try:
        uid_i = int(uid)
        oid_i = int(oid)
    except (TypeError, ValueError):
        return False
    if uid_i <= 0 or oid_i <= 0:
        return False
    existing = ORDERS.get(oid_i)
    if isinstance(existing, dict):
        owner = int(existing.get("user_id") or 0)
        if owner not in (0, uid_i):
            return False
        if existing.get("paid"):
            return False
        return True
    for raw in (
        _user_state_get(uid_i, "awaiting_proof"),
        _user_state_get(uid_i, "awaiting_payment_order_id"),
    ):
        if raw is None:
            continue
        try:
            if int(raw) == oid_i:
                return True
        except (TypeError, ValueError):
            continue
    o = _ensure_order_in_orders(oid_i, uid_i)
    return (
        isinstance(o, dict)
        and int(o.get("user_id") or 0) == uid_i
        and not o.get("paid")
    )


def _find_latest_unpaid_order_id(uid: int) -> Optional[int]:
    """Последний неоплаченный заказ только этого Telegram user_id."""
    if not uid:
        return None
    best_oid: Optional[int] = None
    best_ts = 0.0
    for raw_oid, o in ORDERS.items():
        if not isinstance(o, dict):
            continue
        if int(o.get("user_id") or 0) != int(uid):
            continue
        if o.get("paid"):
            continue
        try:
            ts = float(o.get("created_at") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        try:
            oid_i = int(raw_oid)
        except (TypeError, ValueError):
            continue
        if ts >= best_ts:
            best_ts = ts
            best_oid = oid_i
    return best_oid


def _resolve_awaiting_payment_order_id(uid: int, user_data: Optional[dict]) -> Optional[int]:
    """Активный неоплаченный заказ: user_data → user_states → последний в ORDERS."""
    ud = user_data if isinstance(user_data, dict) else {}
    candidates: List[int] = []
    for raw in (ud.get("awaiting_payment_order_id"), _user_state_get(uid, "awaiting_payment_order_id")):
        if raw is None:
            continue
        try:
            candidates.append(int(raw))
        except (TypeError, ValueError):
            continue
    latest = _find_latest_unpaid_order_id(uid)
    if latest is not None:
        candidates.append(int(latest))
    seen: set = set()
    for oid in reversed(candidates):
        if oid in seen:
            continue
        seen.add(oid)
        o = ORDERS.get(int(oid))
        if not isinstance(o, dict):
            continue
        if int(o.get("user_id") or 0) != int(uid):
            continue
        if o.get("paid"):
            continue
        ud["awaiting_payment_order_id"] = int(oid)
        if uid:
            b = _user_state_bucket(uid)
            b["awaiting_payment_order_id"] = int(oid)
        return int(oid)
    _clear_awaiting_payment_order_id(uid, ud)
    return None


def _user_state_pop(uid: int, key: str, *, persist: bool = True) -> Optional[int]:
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
    if persist:
        try:
            save_state()
        except Exception:
            logging.getLogger(__name__).exception(
                "user_state_pop save_state uid=%s key=%s", uid, key
            )
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
    save_state()


_POST_DEPLOY_USER_DATA_EPHEMERAL: Tuple[str, ...] = (
    "pending_order",
    "pending_site_order_meta",
    "deep_link_order_session",
    "awaiting_payment_order_id",
    "payment_pending_method",
    "reply_to",
    "reply_support_user_id",
)
_TINDER_USER_DATA_KEYS: Tuple[str, ...] = (
    "tinder_autoplay_task",
    "tinder_gidxs",
    "tinder_i",
    "tinder_cat_tok",
    "tinder_message_id",
    "tinder_chat_id",
    "tinder_autoplay_paused",
)


def _apply_post_deploy_session_reset(uid: int, user_data: dict) -> None:
    """После рестарта процесса (деплой): сбросить только user_data и служебные флаги.

    Не трогаем USER_CART и SITE_LOGIN_PENDING_ORDER: корзина могла только что прийти с сайта
    через POST /api/verify-code или sync до первого нажатия пользователя — иначе «🛒 Корзина»
    пустая и «Подтвердить заказ» теряет текст черновика.
    """
    if not uid:
        return
    _clear_checkout_delivery(user_data)
    for k in _POST_DEPLOY_USER_DATA_EPHEMERAL:
        user_data.pop(k, None)
    pend = _user_state_get(uid, "awaiting_payment_order_id")
    if pend is not None:
        po = _ensure_order_in_orders(int(pend), int(uid))
        if (
            isinstance(po, dict)
            and int(po.get("user_id") or 0) == int(uid)
            and not po.get("paid")
        ):
            user_data["awaiting_payment_order_id"] = int(pend)
        else:
            _user_state_pop(uid, "awaiting_payment_order_id")
    proof = _user_state_get(uid, "awaiting_proof")
    if proof is not None:
        po = _restore_order_by_id(int(proof)) or _ensure_order_in_orders(int(proof), int(uid))
        if (
            isinstance(po, dict)
            and int(po.get("user_id") or 0) == int(uid)
            and not po.get("paid")
        ):
            user_data["awaiting_payment_order_id"] = int(proof)
            user_data["awaiting_proof"] = int(proof)
        else:
            _user_state_pop(uid, "awaiting_proof")
    t_task = user_data.get("tinder_autoplay_task")
    if isinstance(t_task, asyncio.Task) and not t_task.done():
        try:
            t_task.cancel()
        except Exception:
            pass
    for k in _TINDER_USER_DATA_KEYS:
        user_data.pop(k, None)
    user_support_state.pop(int(uid), None)


FALLBACK_USER_TEXT = (
    "Действие устарело или сейчас недоступно "
    "(например, прошло время или сообщение уже не то). "
    "Откройте каталог или нужный раздел снова из меню внизу."
)

# Короткие ответы словами (одно эмодзи в сообщении часто показывается как крупная анимация)
MSG_ORDER_PREVIEW_CANCELLED = (
    "Оформление заказа отменено — к этому сообщению кнопки больше не привязаны."
)
MSG_ORDER_DEEPLINK_DECLINED = "Заказ по ссылке отменён."
MSG_CATALOG_LOAD_FAIL = (
    "Не удалось загрузить каталог. Проверьте соединение и попробуйте снова "
    "или напишите в «Связь»."
)
MSG_CATALOG_EMPTY_SECTION = (
    "В этом разделе сейчас нет карточек. Загляните в другой раздел или в акции."
)
MSG_VIEWER_FAIL = "Не удалось открыть просмотр карточек. Попробуйте снова из каталога."
MSG_NO_VITRINA_PROMOS = (
    "Сейчас в боте нет баннеров акций с главной сайта (слайдер под категориями).\n\n"
    "Бот подгружает их с API сайта и из POST /api/sync/promotions. "
    "Зайдите на сайт или попробуйте позже. Если JSON по другому URL — задайте "
    "HOME_PROMOTIONS_JSON_URL на хостинге 👀"
)
MSG_PROMO_PARTICIPATION = "Для участия в акции пришлите нам видео с уже имеющимися карточками."
MSG_ADD_TO_CART_STALE = "Эта карточка устарела в текущем сообщении. Откройте каталог заново и добавьте ещё раз."
MSG_BUY_HINT = "Можно написать: купить <категория> <номер> или купить <название карточки>."
MSG_ADMIN_SAY_BAD_ID = (
    "Не получилось разобрать ID пользователя. Пример: /say 123456789 Здравствуйте"
)
MSG_ADMIN_SAY_NO_TEXT = "После ID нужен текст сообщения. Пример: /say 123456789 Ваш заказ готов"
MSG_ADMIN_SAY_FAIL = (
    "Сообщение не доставлено: проверьте числовой ID и что пользователь писал боту."
)
MSG_ADMIN_SAY_OK = "Сообщение отправлено клиенту."
MSG_FORWARD_FAIL = "Не удалось доставить сообщение получателю."
MSG_ADMIN_REPLY_SESSION_RESET = (
    "Режим ответа устарел или заказ не найден. "
    "Снова откройте заказ в админке и нажмите «Ответить»."
)
MSG_REPLY_MODE_ACTIVE = "Режим ответа включён — напишите одним сообщением текст для клиента."
MSG_EXPECT_PHOTO_PROOF = "Сейчас ждём фото чека оплаты (скриншот), а не текст. Пришлите изображение."
MSG_TYPE_REPLY_TEXT = "Напишите текст ответа обычным сообщением."
MSG_EMPTY_INPUT = "Введите текст сообщения."
MSG_OK = "Готово."
MSG_SUPPORT_THANKS = "Сообщение принято, мы ответим в этом чате."
MSG_SEND_SUPPORT_FAIL = "Не удалось отправить сообщение. Попробуйте позже."
MSG_ORDER_ALREADY_PAID_SKIP_PROOF = "По этому заказу оплата уже учтена, скрин не нужен."
MSG_PAY_PROOF_TO_ADMIN_FAIL = (
    "Не удалось передать скрин администратору. Попробуйте ещё раз или напишите в поддержку."
)
MSG_PAY_PROOF_RECEIVED = "⏳ Получили скрин. Сейчас спросим адрес доставки…"
MSG_PAY_PROOF_OK = (
    "✅ Скрин сохранён. Укажите данные для доставки — после этого заказ уйдёт администратору."
)
MSG_POSTPAID_COLLECTED = (
    "Записали. Можете дописать адрес, ФИО или телефон — или нажмите «✅ Отправить заказ»."
)
MSG_POSTPAID_NEED_DETAILS = (
    "Сначала напишите адрес отделения, ФИО и номер телефона."
)
MSG_POSTPAID_SUBMITTED = (
    "✅ Заказ передан администратору. Ожидайте подтверждения оплаты."
)
MSG_POSTPAID_SUBMIT_FAIL = (
    "Не удалось отправить заказ администратору. Попробуйте ещё раз или напишите в «Связь»."
)
MSG_CALLBACK_CATEGORY_INVALID = "Такой категории нет. Откройте каталог заново."
MSG_CART_CLEARED_TOAST = "Корзина очищена."
MSG_PAY_NEED_PROOF_FIRST = "Сначала пришлите фото чека оплаты."
MSG_PAY_FINISH_CURRENT = "Сначала завершите оплату по текущему заказу."
MSG_PAY_ALREADY_OPEN = (
    "Оплата уже открыта — ниже отправлены актуальные кнопки "
    "(💳 Карта · 📱 Перевод · ₿ Крипта · ❌ Отменить оплату)."
)
MSG_PAY_CANCELLED = (
    "Оплата отменена. Можете снова подтвердить заказ или изменить корзину на сайте."
)
MSG_ORDER_STATUS_UPDATED = "Статус заказа обновлён."
MSG_ORDER_ALREADY_PAID_TOAST = "Заказ уже отмечен как оплаченный."
MSG_PAYMENT_CAPTION_CONFIRMED = "\n\nОплата подтверждена."
MSG_PAYMENT_CAPTION_REJECTED = "\n\nЧек отклонён: пришлите новый скрин оплаты."
MSG_POSTPAID_SHIPPING_BY = (
    "Напишите адрес отделения Европочты, ФИО и номер телефона.\n\n"
    "Можно несколькими сообщениями — всё уйдёт администратору в этот заказ."
)
MSG_POSTPAID_SHIPPING_RU = (
    "Напишите адрес отделения СДЭК или Яндекс, ФИО и номер телефона.\n\n"
    "Можно несколькими сообщениями — всё уйдёт администратору в этот заказ."
)
MSG_POSTPAID_SHIPPING_INTL = (
    "Напишите ваш домашний адрес, ФИО и почтовый индекс латиницей.\n\n"
    "Можно несколькими сообщениями — всё уйдёт администратору в этот заказ."
)
MSG_POSTPAID_FORWARDED_OK = "Передали администратору."
MSG_POSTPAID_THREAD_CLOSED = (
    "По этому заказу оформление завершено. Общие вопросы — кнопка «Связь»."
)
MSG_POSTPAID_THREAD_STALE = "Заказ не найден. Если нужна помощь — «Связь»."
MSG_CALLBACK_STALE_ORDER = (
    "Кнопка от старого сообщения или сессия заказа уже неактивна. "
    "Отправьте /start или откройте ссылку с сайта ещё раз."
)
MSG_CALLBACK_UNKNOWN_ORDER_BUTTON = (
    "Не удалось обработать кнопку. Откройте /admin → «Новые заказы» "
    "или попросите клиента оформить заказ заново."
)
MSG_ADMIN_ORDER_STALE = (
    "Заказ #{oid} не найден в памяти бота (после перезапуска).\n\n"
    "Откройте /admin → «Новые заказы» или попросите клиента оформить заказ заново."
)
MSG_CONFIRM_ORDER_ERROR = (
    "Не удалось подтвердить заказ. Попробуйте /start или откройте заказ с сайта ещё раз."
)
_SITE_CONFIRM_CALLBACKS = frozenset(
    {"confirm_order", "site_confirm", "order_confirm"}
)
_SITE_CANCEL_CALLBACKS = frozenset(
    {
        "cancel_order",
        "site_cancel",
        "order_cancel",
        "order_cancelled",
        "checkout_cancel",
        "cancel_checkout",
    }
)
_SITE_CONFIRM_EXTRA = frozenset(
    {
        "submit_order",
        "order_submit",
        "checkout_confirm",
        "confirm_checkout",
        "order_confirmed",
        "confirm_payment_order",
    }
)
# Сайт часто шлёт confirm_order:<uuid> (до 64 байт), бот раньше ждал только confirm_order.
_RE_SITE_ORDER_BUTTON = re.compile(
    r"^(?P<kind>confirm_order|cancel_order|site_confirm|order_confirm|site_cancel|order_cancel)"
    r"(?P<suffix>[:_](?P<oid>.+))?$",
    re.IGNORECASE,
)
_RE_SITE_ORDER_SHORT_BUTTON = re.compile(
    r"^(?P<action>confirm|cancel)[:_](?P<oid>[a-zA-Z0-9-]{8,})$",
    re.IGNORECASE,
)
_RE_ORDER_ID_IN_MESSAGE = re.compile(
    r"ID\s+заказа:\s*([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
    re.IGNORECASE,
)
MSG_SITE_ORDER_LOAD_FAIL = (
    "Не удалось подтянуть заказ с сайта. Проверьте ILLUCARDS_ORDER_UPDATE_SECRET на Render "
    "и откройте заказ снова: /start order_<номер> или ссылка с сайта."
)
MSG_ORDER_SUBMIT_ADMIN_FAIL = (
    "Не удалось отправить заказ администратору. Попробуйте позже или напишите в «Связь»."
)
MSG_UNKNOWN_TEXT = (
    "Не понял сообщение. Отправьте /start или выберите действие в меню ниже 👇"
)

# Reply-клавиатура: короткие подписи + эмодзи
BTN_CATALOG = "📦 Каталог"
BTN_CART = "🛒 Корзина"
BTN_POPULAR = "🔥 Акции"
BTN_CHAT = "💬 Связь"
BTN_MY_ORDERS = "📋 Мои заказы"
BTN_DELIVERY = "🚚 Доставка"
BTN_RANDOM_CARD = "🎁 Случайная карточка"
BTN_FAVORITES = "💚 Избранное"

# Уведомление клиенту при входе админа в режим ответа
ADMIN_TYPING_NOTICE = "⏳ Администратор печатает..."
# Автоответ после «Подтвердить заказ» (сайт / текст): админу ничего не уходит — только после оплаты.
ORDER_AUTO_ACK = "📦 Принято. Админ увидит заказ в Telegram только после оплаты (скрин чека)."

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
PAY_ADMIN_CONFIRMED_CLIENT = "Оплата подтверждена."
PAY_ADMIN_REJECTED_CLIENT = "Чек не подошёл — пришлите, пожалуйста, новый скрин оплаты."

# /start order_<id> или /start order-<id> (регистр не важен)
_RE_START_ORDER_ARG = re.compile(r"^order[_-](.+)$", re.IGNORECASE)

# Не слать подряд несколько одинаковых reply на «устаревший» callback (одни и те же кнопки).
_LAST_CALLBACK_FALLBACK_CHAT: Dict[int, float] = {}


def _message_shows_card_media(m: Optional[Message]) -> bool:
    """Сообщение с картой для Tinder: фото, анимация или документ-картинка."""
    if not m:
        return False
    if m.photo:
        return True
    if m.animation:
        return True
    d = m.document
    if d and (d.mime_type or "").lower().startswith("image/"):
        return True
    return False


async def _notify_callback_issue(
    q: Optional[CallbackQuery], context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not q:
        return
    try:
        await q.answer()
    except Exception:
        pass
    chat_id = int(q.message.chat_id) if q.message else 0
    now = time.time()
    if chat_id and now - _LAST_CALLBACK_FALLBACK_CHAT.get(chat_id, 0) < 45.0:
        return
    if chat_id:
        _LAST_CALLBACK_FALLBACK_CHAT[chat_id] = now
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


async def _callback_ack(
    q: Optional[CallbackQuery],
    text: str = "",
    *,
    show_alert: bool = False,
) -> bool:
    """Снять loading на inline-кнопке (один answer на callback)."""
    if not q:
        return False
    try:
        await q.answer(
            text=text or None,
            show_alert=bool(show_alert and text),
        )
        return True
    except Exception:
        return False


async def _answer_order_callback_stale(
    q: Optional[CallbackQuery], *, acked: bool = False
) -> None:
    """Снять loading; без текста про «устаревшую кнопку» — только тихий ack."""
    if not q:
        return
    await _callback_ack(q)


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
        "🔐 Ваш код для входа на сайт:\n\n"
        f"{code}\n\n"
        "⏳ Действует 5 минут"
    )


def _issue_login_code(telegram_id: int, username: str = "") -> str:
    """4-значный код; в LOGIN_CODES: user_id, нормализованный username, expires."""
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


def _login_wait_id_from_start_payload(raw: str) -> Optional[str]:
    s = str(raw or "").strip().lower()
    if not s.startswith("web_login_"):
        return None
    wait_id = s[len("web_login_") :].strip()
    if len(wait_id) == 32 and all(c in "0123456789abcdef" for c in wait_id):
        return wait_id
    return None


def _is_web_login_start_payload(raw: str) -> bool:
    s = str(raw or "").strip().lower()
    return s == "web_login" or _login_wait_id_from_start_payload(s) is not None


def _login_code_sync_secret() -> str:
    """Секрет sync-login-code: на Render часто задан только TELEGRAM_SYNC_API_SECRET."""
    return (
        (os.getenv("ILLUCARDS_LOGIN_CODE_SYNC_SECRET") or "").strip()
        or (os.getenv("TELEGRAM_SYNC_API_SECRET") or "").strip()
    )


async def _sync_login_wait_to_site(
    telegram_user_id: int,
    username: str,
    wait_id: str,
) -> bool:
    """Синхронизация автовхода: wait_id + профиль на сайт (без кода)."""
    wid = str(wait_id or "").strip().lower()
    if len(wid) != 32 or not all(c in "0123456789abcdef" for c in wid):
        return False
    return await _sync_login_code_to_site("", int(telegram_user_id), username, wid)


async def _sync_login_code_to_site(
    code: str,
    telegram_user_id: int,
    username: str = "",
    wait_id: Optional[str] = None,
) -> bool:
    """
    Optional production bridge: bot memory has the code, site can mirror it in Redis
    and mark web_login_<wait_id> ready. If env is absent, the bot /api/verify-code
    flow still works.
    """
    secret = _login_code_sync_secret()
    url = (os.getenv("ILLUCARDS_LOGIN_CODE_SYNC_URL") or "").strip()
    if not url:
        url = f"{_illucards_site_base_url()}/api/internal/sync-login-code"
    if not url or not secret:
        logging.getLogger(__name__).warning(
            "sync-login-code: секрет не задан — wait_id не синхронизирован с сайтом"
        )
        return False
    un = str(username or "").strip().lstrip("@")
    payload: dict = {
        "user_id": int(telegram_user_id),
        "username_display": un if un else f"id{int(telegram_user_id)}",
        "username_norm": _normalize_login_username(un) if un else "",
    }
    code_digits = re.sub(r"\D", "", str(code or ""))
    if len(code_digits) == 4:
        payload["code"] = code_digits
    if wait_id:
        wid = _login_wait_id_from_start_payload(f"web_login_{wait_id}") or ""
        if wid:
            payload["wait_id"] = wid
    if "code" not in payload and "wait_id" not in payload:
        return False
    last_err: Optional[str] = None
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.post(
                    url,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {secret}",
                        "Content-Type": "application/json",
                    },
                ) as resp:
                    if resp.status == 200:
                        return True
                    body = await resp.text()
                    last_err = f"HTTP {resp.status}: {body[:300]}"
                    logging.getLogger(__name__).warning(
                        "sync-login-code attempt %s/3 %s",
                        attempt + 1,
                        last_err,
                    )
        except Exception as e:
            last_err = str(e)
            logging.getLogger(__name__).warning(
                "sync-login-code attempt %s/3: %s", attempt + 1, e
            )
        if attempt < 2:
            await asyncio.sleep(1.5)
    if last_err:
        logging.getLogger(__name__).warning(
            "sync-login-code failed: %s url=%s", last_err, url
        )
    return False


def _account_open_markup(
    wait_id: Optional[str] = None,
    telegram_user_id: Optional[int] = None,
) -> InlineKeyboardMarkup:
    """Личный кабинет — user_id для мгновенного входа + tg_wait для синхронизации."""
    wid = str(wait_id or "").strip().lower()
    uid = int(telegram_user_id or 0)
    has_wait = len(wid) == 32 and all(c in "0123456789abcdef" for c in wid)
    base = _illucards_site_base_url()
    if uid > 0 and has_wait:
        url = f"{base}/account?user_id={uid}&tg_wait={wid}"
    elif uid > 0:
        url = f"{base}/account?user_id={uid}"
    elif has_wait:
        url = f"{base}/account?tg_wait={wid}"
    else:
        url = f"{base}/account"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Открыть личный кабинет", url=url)]]
    )


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


def _verify_telegram_widget_auth(payload: dict) -> Tuple[bool, str]:
    """Проверка подписи Telegram Login Widget."""
    if not token:
        return False, "TELEGRAM_BOT_TOKEN не настроен"
    tg_hash = str(payload.get("hash") or "").strip()
    if not tg_hash:
        return False, "Отсутствует подпись Telegram"
    auth_date_raw = str(payload.get("auth_date") or "").strip()
    try:
        auth_date = int(auth_date_raw)
    except (TypeError, ValueError):
        return False, "Некорректный auth_date"
    # Ограничим срок валидности данных виджета (10 минут)
    if auth_date < int(time.time()) - 600:
        return False, "Сессия Telegram устарела"

    pairs: List[str] = []
    for k, v in payload.items():
        if k == "hash" or v is None:
            continue
        sv = str(v)
        if sv == "":
            continue
        pairs.append(f"{k}={sv}")
    pairs.sort()
    data_check_string = "\n".join(pairs)

    secret = hashlib.sha256(token.encode("utf-8")).digest()
    calc = hmac.new(
        secret,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(calc, tg_hash):
        return False, "Подпись Telegram не прошла проверку"
    return True, ""


async def _http_telegram_auth(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return _login_json_response(
            {"success": False, "error": "Некорректный JSON"},
            status=400,
        )
    if not isinstance(data, dict):
        return _login_json_response(
            {"success": False, "error": "Некорректные данные авторизации"},
            status=400,
        )
    ok, err = _verify_telegram_widget_auth(data)
    if not ok:
        return _login_json_response(
            {"success": False, "error": err or "Авторизация Telegram не подтверждена"},
            status=401,
        )
    try:
        uid = int(data.get("id") or 0)
    except (TypeError, ValueError):
        uid = 0
    username = str(data.get("username") or "").strip()
    if uid:
        users_touch(uid, "telegram_widget_auth")
        _register_login_username(uid, username)
    uname = _normalize_login_username(username)
    return _login_json_response(
        {
            "success": True,
            "user_id": uid,
            "username": f"@{uname}" if uname else "",
            "first_name": str(data.get("first_name") or ""),
            "last_name": str(data.get("last_name") or ""),
        }
    )


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
    await _sync_login_code_to_site(code, int(uid), key)
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
    code = str(data.get("code") or "").strip()
    raw_u = data.get("username")
    key = _normalize_login_username(str(raw_u)) if raw_u is not None else ""
    if not code:
        return _login_json_response(
            {"success": False, "error": "Укажите код"},
            status=400,
        )
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
    # Совместимость: часть страниц кабинета отправляет только 4-значный код без @username.
    # Если username передан, проверяем его строго; если нет — достаточно валидного кода.
    if key and str(entry.get("username") or "").lower() != key:
        return _login_json_response(
            {"success": False, "error": "Неверный код или username"},
            status=401,
        )
    uid = int(entry.get("user_id") or 0)
    LOGIN_CODES.pop(code, None)
    del_cc = str(data.get("deliveryCountry") or data.get("delivery") or "BY").strip()
    bot_code, _, _, _ = _delivery_option_for_site_code(del_cc)
    _remember_user_delivery_country(uid, bot_code)
    raw_cart = data.get("cart")
    lines: List[dict] = []
    if isinstance(raw_cart, list) and raw_cart:
        lines = _normalize_sync_cart_items(raw_cart, bot_code)
    bot = request.app.get("bot")
    products: List[dict] = []
    try:
        products = await load_products()
    except Exception:
        products = []
    if products and lines:
        _reconcile_cart_lines_to_catalog(products, lines)
    if lines:
        _cart_set_items_uid(uid, lines)
    if uid:
        _cart_apply_site_pricing_hints(uid, data)
        _apply_optional_favorites_from_site_payload(uid, data, products)
        if "orders" in data:
            _user_orders_merge_site(uid, _normalize_sync_orders(data.get("orders")))
    resp_vc: Dict[str, object] = {
        "success": True,
        "user_id": uid,
        "username": f"@{key}" if key else "",
    }
    return _login_json_response(resp_vc)


def _sync_auth_ok(request: web.Request, data: dict) -> bool:
    """Проверка секрета синхронизации сайта -> бот."""
    secrets = [s for s in (SYNC_API_SECRET, ORDER_STATUS_UPDATE_SECRET) if s]
    if not secrets:
        return True
    header_secret = (request.headers.get("X-Sync-Secret") or "").strip()
    body_secret = str(data.get("secret") or "").strip()
    for candidate in (header_secret, body_secret):
        if not candidate:
            continue
        for secret in secrets:
            if hmac.compare_digest(candidate, secret):
                return True
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        tok = auth[7:].strip()
        if tok:
            for secret in secrets:
                if hmac.compare_digest(tok, secret):
                    return True
    return False


def _site_api_auth_ok(request: web.Request, data: dict) -> bool:
    """Сайт (Vercel): X-Sync-Secret, secret в теле или Bearer (sync / order secret)."""
    if _sync_auth_ok(request, data):
        return True
    auth = (request.headers.get("Authorization") or "").strip()
    if not auth.lower().startswith("bearer "):
        return False
    tok = auth[7:].strip()
    if not tok:
        return False
    for secret in (SYNC_API_SECRET, ORDER_STATUS_UPDATE_SECRET):
        if secret and hmac.compare_digest(tok, secret):
            return True
    return False


def _resolve_sync_uid(data: dict) -> int:
    for key in ("user_id", "telegram_user_id", "telegramUserId"):
        raw_uid = data.get(key)
        try:
            uid = int(raw_uid or 0)
        except (TypeError, ValueError):
            uid = 0
        if uid > 0:
            return uid
    raw_username = data.get("username")
    if raw_username is None:
        return 0
    key = _normalize_login_username(str(raw_username))
    if not key:
        return 0
    try:
        return int(USERNAME_TO_USER_ID.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _resolve_sync_uid_from_request(request: web.Request, data: Optional[dict] = None) -> int:
    payload = dict(data or {})
    for key in ("user_id", "username", "secret"):
        if key not in payload and key in request.query:
            payload[key] = request.query.get(key)
    return _resolve_sync_uid(payload)


def _coerce_loyalty_int(val: object) -> Optional[int]:
    if val is None or isinstance(val, bool):
        return None
    try:
        if isinstance(val, str):
            s = val.replace("\xa0", " ").replace(" ", "").replace(",", ".").strip()
            if not s:
                return None
            v = float(s)
        else:
            v = float(val)
        if v != v:
            return None
        return int(round(v))
    except (TypeError, ValueError):
        return None


_LOYALTY_NEST_KEYS = (
    "user",
    "profile",
    "account",
    "loyalty",
    "wallet",
    "customer",
    "me",
    "member",
    "auth",
    "session",
    "order",
    "lastOrder",
    "checkout",
    "cart",
)


def _loyalty_find_int(data: dict, keys: Tuple[str, ...], depth: int) -> Optional[int]:
    if not isinstance(data, dict) or depth < 0:
        return None
    for k in keys:
        if k in data:
            v = _coerce_loyalty_int(data.get(k))
            if v is not None:
                return v
    for nest in _LOYALTY_NEST_KEYS:
        sub = data.get(nest)
        if isinstance(sub, dict):
            v = _loyalty_find_int(sub, keys, depth - 1)
            if v is not None:
                return v
    return None


def _apply_site_loyalty_from_sync(uid: int, data: dict) -> Optional[str]:
    return None


def _schedule_loyalty_notify(bot, uid: int, text: Optional[str]) -> None:
    pass


def _loyalty_compute_earn_estimate(
    pay_total: int,
    hint: Optional[dict] = None,
    *,
    cart_lines: Optional[List[dict]] = None,
) -> Optional[int]:
    return None


def _loyalty_finalize_order_bonuses_once(o: dict) -> None:
    pass


def _loyalty_cart_footer_lines(uid: int) -> List[str]:
    return []


def _bonus_discount_units(points: int, currency: str) -> int:
    return 0


def _bonus_discount_label(points: int, currency: str) -> str:
    return ""


def _checkout_bonus_cap(uid: int, grand_before_bonus: int, currency: str = "BYN") -> int:
    return 0


def _checkout_bonus_spend_effective(
    user_data: dict, uid: int, grand_before_bonus: int, currency: str = "BYN"
) -> int:
    return 0


def _normalize_sync_cart_items(
    raw_items: object, delivery_bot_code: Optional[str] = None
) -> List[dict]:
    out: List[dict] = []
    if not isinstance(raw_items, list):
        return out
    cc = str(delivery_bot_code or "by").strip().lower()
    if cc not in DELIVERY_OPTIONS:
        cc = "by"
    prefer_rub = cc != "by"
    for x in raw_items:
        if not isinstance(x, dict):
            continue
        name = str(x.get("name") or x.get("title") or "—").strip() or "—"
        ref = str(x.get("ref") or x.get("id") or name).strip()[:120]
        try:
            if prefer_rub:
                # Для RU/UA/OT сайт часто шлёт цену в RUB в поле price без priceRub;
                # priceByn не брать раньше price — иначе в корзине окажутся «белорусские» цифры с подписью RUB.
                price = 0
                if x.get("priceRub") is not None:
                    price = int(float(x.get("priceRub") or 0))
                if price <= 0:
                    for k in ("unitPriceRub", "unit_price_rub", "salePriceRub"):
                        if x.get(k) is not None:
                            price = int(float(x.get(k) or 0))
                            if price > 0:
                                break
                if price <= 0:
                    for k in ("price", "unitPrice", "unit_price"):
                        if x.get(k) is not None:
                            price = int(float(x.get(k) or 0))
                            if price > 0:
                                break
                if price <= 0 and x.get("priceByn") is not None:
                    price = int(float(x.get("priceByn") or 0))
            else:
                if x.get("priceByn") is not None:
                    price = int(float(x.get("priceByn") or 0))
                elif x.get("priceRub") is not None:
                    price = int(float(x.get("priceRub") or 0))
                else:
                    price = int(float(x.get("price") or 0))
        except (TypeError, ValueError):
            price = 0
        try:
            qty = int(
                x.get("qty")
                or x.get("quantity")
                or x.get("count")
                or x.get("amount")
                or 1
            )
        except (TypeError, ValueError):
            qty = 1
        qty = max(1, min(qty, 999))
        lc = _goods_currency_for_delivery_country(cc)
        out.append(
            {
                "ref": ref,
                "name": name[:200],
                "price": max(0, price),
                "qty": qty,
                "from_site": True,
                "line_currency": lc,
            }
        )
    return out


def _normalize_sync_favorites(raw_items: object) -> List[str]:
    refs: List[str] = []
    if not isinstance(raw_items, list):
        return refs
    seen: set = set()
    for x in raw_items:
        if isinstance(x, dict):
            ref = str(x.get("ref") or x.get("id") or "").strip()[:120]
        else:
            ref = str(x or "").strip()[:120]
        if not ref or ref in seen:
            continue
        seen.add(ref)
        refs.append(ref)
    return refs


def _favorites_list_from_sync_favorites_endpoint(
    data: dict, products: Optional[List[dict]] = None
) -> List[str]:
    """Тело POST /api/sync/favorites: списки избранного с сайта → ref каталога (по id или названию)."""
    for key in ("items",) + _FAVORITE_SYNC_PAYLOAD_KEYS:
        raw = data.get(key)
        if isinstance(raw, list):
            return _normalize_sync_favorites_with_catalog(raw, products)
    return []


def _apply_optional_favorites_from_site_payload(
    uid: int, data: dict, products: Optional[List[dict]] = None
) -> bool:
    """Если в JSON явно передан список избранного — обновить USER_FAVORITES. Иначе не трогаем."""
    if not uid:
        return False
    for key in _FAVORITE_SYNC_PAYLOAD_KEYS:
        if key in data and isinstance(data.get(key), list):
            refs = _normalize_sync_favorites_with_catalog(
                data[key], products
            )
            if not refs and not _sync_explicit_clear(data, "clearFavorites", "clear_favorites"):
                return False
            USER_FAVORITES[uid] = refs
            save_state()
            return True
    return False


def _catalog_name_key(raw: object) -> str:
    s = re.sub(r"\s+", " ", str(raw or "").strip()).casefold()
    return s


def _find_product_by_catalog_name(
    products: List[dict], raw_name: str
) -> Optional[dict]:
    key = _catalog_name_key(raw_name)
    if not key:
        return None
    for p in products:
        pn = _catalog_name_key(p.get("name"))
        if pn and pn == key:
            return p
    return None


def _find_product_by_catalog_sku_slug(
    products: List[dict], raw: str
) -> Optional[dict]:
    s = str(raw or "").strip()
    if not s:
        return None
    s_cf = s.casefold()
    for p in products:
        sk = str(p.get("sku") or "").strip()
        sl = str(p.get("slug") or "").strip()
        if sk and (sk == s or sk.casefold() == s_cf):
            return p
        if sl and (sl == s or sl.casefold() == s_cf):
            return p
    return None


def _resolve_favorite_sync_entry_to_ref(
    entry: object, products: List[dict]
) -> Optional[str]:
    """Сопоставить элемент избранного с сайта с карточкой каталога (id/ref или название)."""
    if not products:
        return None
    if isinstance(entry, str):
        s = str(entry).strip()
        if not s:
            return None
        p = _product_from_callback(s, products)
        if not p:
            p = _find_product_by_catalog_sku_slug(products, s)
        if p:
            return _product_ref_for_callback(p, _global_product_index(products, p))
        p = _find_product_by_catalog_name(products, s)
        if p:
            return _product_ref_for_callback(p, _global_product_index(products, p))
        return None
    if isinstance(entry, dict):
        for key in (
            "ref",
            "id",
            "_id",
            "productId",
            "product_id",
            "externalId",
            "external_id",
            "uuid",
            "cardId",
            "card_id",
            "sku",
            "slug",
            "handle",
        ):
            if key not in entry:
                continue
            s = str(entry.get(key) or "").strip()
            if not s:
                continue
            p = _product_from_callback(s, products) or _find_product_by_catalog_sku_slug(
                products, s
            )
            if p:
                return _product_ref_for_callback(p, _global_product_index(products, p))
        for nk in ("name", "title", "label", "productName"):
            if nk not in entry:
                continue
            p = _find_product_by_catalog_name(products, str(entry.get(nk) or ""))
            if p:
                return _product_ref_for_callback(p, _global_product_index(products, p))
        return None
    return None


def _normalize_sync_favorites_with_catalog(
    raw_items: object, products: Optional[List[dict]]
) -> List[str]:
    if not isinstance(raw_items, list):
        return []
    if not products:
        return _normalize_sync_favorites(raw_items)
    seen: set = set()
    out: List[str] = []
    for x in raw_items:
        r = _resolve_favorite_sync_entry_to_ref(x, products)
        if r and r not in seen:
            seen.add(r)
            out.append(r)
    return out


def _reconcile_cart_lines_to_catalog(products: List[dict], lines: List[dict]) -> None:
    """Подставить ref/имя строк корзины с сайта к id карточек каталога (в т.ч. по названию)."""
    if not products or not lines:
        return
    for line in lines:
        if not isinstance(line, dict):
            continue
        ref = str(line.get("ref") or "").strip()
        name = str(line.get("name") or "").strip()
        p = _product_from_callback(ref, products) if ref else None
        if not p and name:
            p = _find_product_by_catalog_name(products, name)
        if not p and ref:
            p = _find_product_by_catalog_name(products, ref)
        if p:
            line["ref"] = _product_ref_for_callback(p, _global_product_index(products, p))
            line["name"] = str(p.get("name") or name or "—")[:200]


def _normalize_sync_delivery(raw: object) -> dict:
    if not isinstance(raw, dict):
        return {}
    try:
        amount = int(float(raw.get("amount") if raw.get("amount") is not None else 0))
    except (TypeError, ValueError):
        amount = 0
    return {
        "country": str(raw.get("country") or raw.get("code") or "").strip().lower(),
        "label": str(raw.get("label") or raw.get("name") or "—").strip() or "—",
        "amount": max(0, amount),
        "currency": str(raw.get("currency") or "BYN").strip() or "BYN",
    }


def _site_status_to_bot_status(raw: object) -> str:
    s = str(raw or "").strip().lower()
    return {
        "confirmed": "accepted",
        "paid": "accepted",
        "shipped": "shipped",
        "sent": "shipped",
        "delivered": "done",
        "cancelled": "canceled",
        "canceled": "canceled",
        "new": "new",
    }.get(s, str(raw or "На сайте").strip() or "На сайте")


def _normalize_sync_site_order(raw: object) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    ext = str(
        raw.get("external_id")
        or raw.get("externalId")
        or raw.get("site_id")
        or raw.get("id")
        or ""
    ).strip()
    if not ext:
        return None
    drec = _normalize_sync_delivery(raw.get("delivery") or raw.get("shipping") or {})
    dc_raw = ""
    if isinstance(raw.get("delivery"), dict):
        dc_raw = str(
            raw.get("delivery", {}).get("country")
            or raw.get("delivery", {}).get("code")
            or ""
        ).strip()
    if not dc_raw:
        dc_raw = str(
            raw.get("deliveryCountry") or raw.get("delivery_country") or "BY"
        ).strip()
    order_bot_code, _, _, _ = _delivery_option_for_site_code(dc_raw or "BY")
    lines = _normalize_sync_cart_items(
        raw.get("items") or raw.get("lines") or [], order_bot_code
    )
    try:
        total_goods, _ = _cart_totals(lines)
    except Exception:
        total_goods = 0
    final_total = _loyalty_find_int(
        raw,
        (
            "finalTotal",
            "final_total",
            "payTotal",
            "pay_total",
            "amountToPay",
            "amount_to_pay",
            "totalAfterBonus",
            "total_after_bonus",
            "totalAfterBonuses",
            "total_after_bonuses",
            "grandTotalAfterBonus",
            "grand_total_after_bonus",
            "paidTotal",
            "paid_total",
        ),
        2,
    )
    total_raw = _loyalty_find_int(raw, ("total", "grandTotal", "grand_total", "orderTotal", "order_total"), 2)
    bonus_applied = _loyalty_find_int(
        raw,
        (
            "bonusApplied",
            "bonus_applied",
            "bonusDiscount",
            "bonus_discount",
            "bonusesApplied",
            "bonuses_applied",
            "pointsDiscount",
            "points_discount",
            "loyaltyDiscount",
            "loyalty_discount",
        ),
        2,
    )
    bonus_spent = _loyalty_find_int(
        raw,
        (
            "bonusPointsSpent",
            "bonus_points_spent",
            "pointsSpent",
            "points_spent",
            "bonusesSpent",
            "bonuses_spent",
            "loyaltyPointsSpent",
            "loyalty_points_spent",
        ),
        2,
    )
    if final_total is not None:
        total = int(final_total)
    elif total_raw is not None and bonus_applied is not None and int(bonus_applied) > 0:
        total = max(0, int(total_raw) - int(bonus_applied))
    elif total_raw is not None:
        total = int(total_raw)
    else:
        total = int(total_goods)
    st = raw.get("status")
    if st is None or str(st).strip() == "":
        status_label = "На сайте"
    else:
        status_label = _site_status_to_bot_status(st)
    rec = {
        "id": ext[:80],
        "external_id": ext[:120],
        "items": lines,
        "total": max(0, int(total)),
        "total_goods": int(total_goods),
        "delivery": drec,
        "status": status_label,
        "sync_source": "site",
    }
    if bonus_applied is not None and int(bonus_applied) > 0:
        rec["bonus_applied"] = int(bonus_applied)
    if bonus_spent is not None and int(bonus_spent) > 0:
        rec["bonus_points_spent"] = int(bonus_spent)
    return rec


def _normalize_sync_orders(raw: object) -> List[dict]:
    out: List[dict] = []
    if not isinstance(raw, list):
        return out
    seen: set = set()
    for x in raw:
        rec = _normalize_sync_site_order(x)
        if not rec:
            continue
        key = str(rec.get("external_id") or rec.get("id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(rec)
    return out


def _user_orders_merge_site(uid: int, site_orders: List[dict]) -> None:
    if not uid:
        return
    if not site_orders:
        return
    existing = list(USER_ORDERS.get(int(uid)) or [])
    merged: List[dict] = []
    seen: set = set()
    for rec in existing + list(site_orders or []):
        if not isinstance(rec, dict):
            continue
        key = str(rec.get("external_id") or rec.get("id") or "").strip()
        if not key:
            key = f"local:{len(merged)}:{id(rec)}"
        if key in seen:
            continue
        seen.add(key)
        merged.append(rec)
    USER_ORDERS[int(uid)] = merged
    save_state()


def _sync_explicit_clear(data: dict, *keys: str) -> bool:
    if not isinstance(data, dict):
        return False
    for key in keys:
        v = data.get(key)
        if v is True:
            return True
        if isinstance(v, str) and v.strip().lower() in ("1", "true", "yes", "y"):
            return True
    return False


def _site_order_start_token(order_id: str, buyer_seq: Optional[int] = None) -> str:
    """Короткий payload для /start (order_N или order_<uuid без дефисов>)."""
    try:
        seq = int(buyer_seq or 0)
    except (TypeError, ValueError):
        seq = 0
    if seq > 0:
        return f"order_{seq}"
    oid = str(order_id or "").strip()
    compact = oid.replace("-", "")
    if re.fullmatch(r"[a-f0-9]{32}", compact, re.I):
        return f"order_{compact.lower()}"
    safe = re.sub(r"[^A-Za-z0-9_-]", "", oid)[:56]
    return f"order_{safe}" if safe else "order"


def _extract_buyer_seq_from_sources(*sources: object) -> Optional[int]:
    for src in sources:
        if not isinstance(src, dict):
            continue
        for key in ("buyer_seq", "buyerSeq"):
            raw = src.get(key)
            try:
                seq = int(raw or 0)
            except (TypeError, ValueError):
                seq = 0
            if seq > 0:
                return seq
    return None


async def _notify_site_order_start_hint(
    bot,
    uid: int,
    order_id: str,
    buyer_seq: Optional[int] = None,
) -> bool:
    """Если полный checkout не открылся — подсказка с /start order_N."""
    bot = _resolve_live_bot(bot)
    if not bot or not uid or not str(order_id or "").strip():
        return False
    tok = _site_order_start_token(order_id, buyer_seq)
    log = logging.getLogger(__name__)
    try:
        await bot.send_message(
            chat_id=int(uid),
            text=(
                "Заказ с сайта IlluCards оформлен.\n\n"
                f"Нажмите или отправьте:\n/start {tok}\n\n"
                "Если сообщение с составом заказа не появилось выше — эта команда откроет его."
            ),
            reply_markup=REPLY_KB,
        )
        log.info(
            "site order start hint uid=%s order=%s token=%s",
            uid,
            order_id,
            tok,
        )
        return True
    except Exception:
        log.exception("site order start hint failed uid=%s order=%s", uid, order_id)
        return False


async def _handle_site_order_from_sync_payload(
    data: dict, uid: int, bot
) -> Tuple[Optional[str], bool]:
    """Запомнить заказ с сайта (order/create → sync/cart) для /start order_<id>."""
    order_id = str(data.get("order_id") or "").strip()
    order_raw = data.get("order")
    if isinstance(order_raw, dict):
        order_id = str(
            order_raw.get("order_id") or order_raw.get("id") or order_id
        ).strip()
    if not order_id:
        return None, False
    merged: dict = dict(order_raw) if isinstance(order_raw, dict) else {}
    merged["user_id"] = int(uid)
    merged.setdefault("id", order_id)
    merged.setdefault("order_id", order_id)
    cart_raw = data.get("cart")
    if not isinstance(cart_raw, list):
        cart_raw = data.get("items")
    if isinstance(cart_raw, list) and cart_raw and not merged.get("items"):
        merged["items"] = deepcopy(cart_raw)
    merged.setdefault("status", "new")
    del_raw = (
        data.get("deliveryCountry")
        or data.get("delivery_country")
        or merged.get("delivery")
    )
    if del_raw is not None and not merged.get("delivery"):
        merged["delivery"] = str(del_raw).strip().upper()
    register_shared_deep_link_order(order_id, merged)
    PENDING_SITE_ORDER_BY_USER[int(uid)] = order_id
    save_state()
    products: List[dict] = []
    try:
        products = await load_products() or []
    except Exception:
        products = []
    norm = _normalize_deep_link_order(deepcopy(merged), order_id, products)
    if norm:
        _apply_site_order_norm_to_user_cart(int(uid), norm)
        preview = _format_site_order_confirm_preview(int(uid), norm, products)
        if preview.strip():
            meta: Dict[str, object] = {
                "external_id": order_id,
                "items": list(norm.get("items") or []),
            }
            _persist_site_pending_order(int(uid), preview, meta, None)
    skip_notify = bool(
        data.get("skip_buyer_notify") or data.get("skipBuyerNotify")
    )
    session = data.get("session") if isinstance(data.get("session"), dict) else {}
    from_checkout = (
        str(session.get("source") or "").strip() == "vercel_order_create"
        or bool(order_id and isinstance(order_raw, dict))
    )
    bot = _resolve_live_bot(bot)
    username = str(data.get("username") or "").strip().lstrip("@") or None
    if not username and isinstance(order_raw, dict):
        username = str(order_raw.get("username") or "").strip().lstrip("@") or None
    buyer_seq = _extract_buyer_seq_from_sources(data, merged, order_raw)
    should_notify = bot and norm and (not skip_notify or from_checkout)
    checkout_pushed = False
    if should_notify:
        try:
            ok = await _present_site_order_checkout_flow(
                bot=bot,
                uid=int(uid),
                norm=norm,
                username=username,
            )
            checkout_pushed = bool(ok)
            if not ok:
                logging.getLogger(__name__).warning(
                    "site order checkout flow failed uid=%s order=%s", uid, order_id
                )
        except Exception:
            logging.getLogger(__name__).exception(
                "site order notify after sync uid=%s order=%s", uid, order_id
            )
    if from_checkout and bot and order_id and not checkout_pushed:
        hinted = await _notify_site_order_start_hint(
            bot, int(uid), order_id, buyer_seq
        )
        checkout_pushed = checkout_pushed or hinted
    logging.getLogger(__name__).info(
        "sync/cart: registered site order uid=%s order_id=%s skip_notify=%s checkout_pushed=%s",
        uid,
        order_id,
        skip_notify,
        checkout_pushed,
    )
    return order_id, checkout_pushed


async def _http_sync_cart(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return _login_json_response({"success": False, "error": "Некорректный JSON"}, status=400)
    if not isinstance(data, dict):
        return _login_json_response({"success": False, "error": "Некорректные данные"}, status=400)
    if not _sync_auth_ok(request, data):
        return _login_json_response({"success": False, "error": "Forbidden"}, status=403)
    uid = _resolve_sync_uid(data)
    if not uid:
        return _login_json_response({"success": False, "error": "Пользователь не найден"}, status=404)
    del_raw = data.get("deliveryCountry") or data.get("delivery_country")
    order_obj = data.get("order")
    if not del_raw and isinstance(order_obj, dict):
        del_raw = order_obj.get("delivery") or order_obj.get("deliveryCountry")
    bot_code, _, _, _ = _delivery_option_for_site_code(str(del_raw or "BY"))
    cart_raw = data.get("cart")
    if not isinstance(cart_raw, list):
        cart_raw = data.get("items")
    lines = _normalize_sync_cart_items(cart_raw, bot_code)
    _remember_user_delivery_country(uid, bot_code)
    try:
        products = await load_products()
    except Exception:
        products = []
    if products and lines:
        _reconcile_cart_lines_to_catalog(products, lines)
    if lines or _sync_explicit_clear(data, "clearCart", "clear_cart"):
        _cart_set_items_uid(uid, lines)
    _cart_apply_site_pricing_hints(uid, data)
    _apply_optional_favorites_from_site_payload(uid, data, products)
    bot_loy = _resolve_live_bot(request.app.get("bot"))
    skip_buyer_notify = bool(
        data.get("skip_buyer_notify") or data.get("skipBuyerNotify")
    )
    site_order_id, checkout_pushed = await _handle_site_order_from_sync_payload(
        data, uid, bot_loy
    )
    users_touch(uid, "cart_sync")
    body_loy: Dict[str, object] = {"success": True, "user_id": uid, "items": len(lines)}
    if site_order_id:
        body_loy["order_id"] = site_order_id
        body_loy["checkout_pushed"] = bool(checkout_pushed)
    return _login_json_response(body_loy)


async def _http_sync_favorites(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return _login_json_response({"success": False, "error": "Некорректный JSON"}, status=400)
    if not isinstance(data, dict):
        return _login_json_response({"success": False, "error": "Некорректные данные"}, status=400)
    if not _sync_auth_ok(request, data):
        return _login_json_response({"success": False, "error": "Forbidden"}, status=403)
    uid = _resolve_sync_uid(data)
    if not uid:
        return _login_json_response({"success": False, "error": "Пользователь не найден"}, status=404)
    try:
        products = await load_products()
    except Exception:
        products = []
    refs = _favorites_list_from_sync_favorites_endpoint(data, products)
    if not refs and not _sync_explicit_clear(data, "clearFavorites", "clear_favorites"):
        return _login_json_response({
            "success": True,
            "user_id": uid,
            "items": len(USER_FAVORITES.get(uid) or []),
            "ignored_empty": True,
        })
    USER_FAVORITES[uid] = refs
    save_state()
    users_touch(uid, "favorites_sync")
    logging.getLogger(__name__).info(
        "sync/favorites: user_id=%s позиций=%s", uid, len(refs)
    )
    return _login_json_response({"success": True, "user_id": uid, "items": len(refs)})


async def _http_get_sync_state(request: web.Request) -> web.Response:
    data = {
        "user_id": request.query.get("user_id"),
        "username": request.query.get("username"),
        "secret": request.query.get("secret"),
    }
    if not _sync_auth_ok(request, data):
        return _login_json_response({"success": False, "error": "Forbidden"}, status=403)
    uid = _resolve_sync_uid_from_request(request, data)
    if not uid:
        return _login_json_response({"success": False, "error": "Пользователь не найден"}, status=404)
    cart_raw = USER_CART.get(uid)
    cart_items = []
    cart_total = 0
    if isinstance(cart_raw, dict):
        cart_items = list(cart_raw.get("items") or [])
        try:
            cart_total = int(cart_raw.get("total") or 0)
        except (TypeError, ValueError):
            cart_total = 0
    if not cart_total and cart_items:
        try:
            cart_total, _ = _cart_totals(cart_items)
        except Exception:
            cart_total = 0
    return _login_json_response(
        {
            "success": True,
            "user_id": uid,
            "cart": cart_items,
            "cart_items": len(cart_items),
            "cart_total": cart_total,
            "favorites": list(USER_FAVORITES.get(uid) or []),
            "favorite_items": len(USER_FAVORITES.get(uid) or []),
        }
    )


async def _http_sync_state(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return _login_json_response({"success": False, "error": "Некорректный JSON"}, status=400)
    if not isinstance(data, dict):
        return _login_json_response({"success": False, "error": "Некорректные данные"}, status=400)
    if not _sync_auth_ok(request, data):
        return _login_json_response({"success": False, "error": "Forbidden"}, status=403)
    uid = _resolve_sync_uid(data)
    if not uid:
        return _login_json_response({"success": False, "error": "Пользователь не найден"}, status=404)
    del_raw = data.get("deliveryCountry") or data.get("delivery_country")
    bot_code, _, _, _ = _delivery_option_for_site_code(str(del_raw or "BY"))
    lines = _normalize_sync_cart_items(data.get("cart"), bot_code)
    _remember_user_delivery_country(uid, bot_code)
    try:
        products = await load_products()
    except Exception:
        products = []
    if products and lines:
        _reconcile_cart_lines_to_catalog(products, lines)
    _apply_optional_favorites_from_site_payload(uid, data, products)
    if lines or _sync_explicit_clear(data, "clearCart", "clear_cart"):
        _cart_set_items_uid(uid, lines)
    _cart_apply_site_pricing_hints(uid, data)
    fav_n = len(USER_FAVORITES.get(uid) or [])
    n_site_orders = 0
    if "orders" in data:
        site_orders = _normalize_sync_orders(data.get("orders"))
        _user_orders_merge_site(uid, site_orders)
        n_site_orders = len(site_orders)
    hp_n = -1
    if "homePromotions" in data or "promotions" in data:
        raw_hp = data.get("homePromotions")
        if raw_hp is None:
            raw_hp = data.get("promotions")
        hp_items = _normalize_home_promotions_list(raw_hp if raw_hp is not None else [])
        apply_home_page_promotions(hp_items)
        hp_n = len(HOME_PAGE_PROMOTIONS)
    users_touch(uid, "sync")
    body = {
        "success": True,
        "user_id": uid,
        "cart_items": len(lines),
        "favorite_items": fav_n,
        "orders": n_site_orders,
    }
    if hp_n >= 0:
        body["home_promotions"] = hp_n
    return _login_json_response(body)


def _illucards_site_base_url() -> str:
    """Базовый URL прод-сайта: ILLUCARDS_SITE_ORIGIN (как на Vercel), затем ILLUCARDS_SITE_URL."""
    return (
        os.getenv("ILLUCARDS_SITE_ORIGIN")
        or os.getenv("ILLUCARDS_SITE_URL")
        or os.getenv("NEXT_PUBLIC_SITE_URL")
        or "https://www.illucards.by"
    ).strip().rstrip("/")


def _resolve_site_media_url(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if s.startswith("//"):
        return "https:" + s
    if s.startswith("http://") or s.startswith("https://"):
        return s
    base = _illucards_site_base_url()
    if s.startswith("/"):
        return base + s
    return s


def _promo_click_absolute_url(href: str) -> Optional[str]:
    h = str(href or "").strip()
    if not h:
        return None
    if h.startswith("http://") or h.startswith("https://"):
        return h
    base = _illucards_site_base_url().rstrip("/")
    if h.startswith("#"):
        return base + h
    if h.startswith("/"):
        return base + h
    return f"{base}/{h}"


def _normalize_home_promotions_list(raw: object) -> List[dict]:
    if isinstance(raw, dict):
        raw = raw.get("items") or raw.get("promotions") or raw.get("homePromotions")
    if not isinstance(raw, list):
        return []
    tmp: List[Tuple[int, dict]] = []
    for i, x in enumerate(raw):
        if not isinstance(x, dict):
            continue
        img_raw = (
            x.get("image")
            or x.get("imageUrl")
            or x.get("image_url")
            or x.get("src")
            or x.get("photo")
            or x.get("banner")
            or x.get("bannerUrl")
            or x.get("banner_url")
            or x.get("thumbnail")
            or x.get("thumbnailUrl")
            or x.get("picture")
            or x.get("frontImage")
            or x.get("desktopImage")
            or x.get("mobileImage")
            or x.get("slideImage")
            or x.get("coverImage")
            or ""
        )
        if not str(img_raw or "").strip():
            bg = x.get("backgroundImage") or x.get("background")
            if isinstance(bg, str) and "url(" in bg:
                um = re.search(r"url\(\s*['\"]?([^'\"\)]+)['\"]?\s*\)", bg)
                if um:
                    img_raw = um.group(1).strip()
        if not str(img_raw or "").strip() and isinstance(x.get("media"), dict):
            m = x.get("media")
            img_raw = m.get("url") or m.get("src") or m.get("image") or ""
        img = _resolve_site_media_url(str(img_raw or "").strip())
        link = str(x.get("link") or x.get("url") or x.get("href") or "").strip()
        title = str(x.get("title") or x.get("name") or x.get("label") or "").strip()
        if not img and not link:
            continue
        try:
            ord_v = int(
                x.get("order")
                if x.get("order") is not None
                else (x.get("sort") if x.get("sort") is not None else i)
            )
        except (TypeError, ValueError):
            ord_v = i
        tmp.append((ord_v, {"image": img, "link": link, "title": title}))
    tmp.sort(key=lambda t: t[0])
    return [t[1] for t in tmp]


def apply_home_page_promotions(items: List[dict]) -> None:
    HOME_PAGE_PROMOTIONS.clear()
    HOME_PAGE_PROMOTIONS.extend(items or [])


def home_promotions_for_ui() -> List[dict]:
    return list(HOME_PAGE_PROMOTIONS)


def _home_promo_kb(i: int, n: int, link: str) -> InlineKeyboardMarkup:
    prev_i = (int(i) - 1) % int(n)
    next_i = (int(i) + 1) % int(n)
    rows: List[List[InlineKeyboardButton]] = []
    abs_url = _promo_click_absolute_url(link)
    if abs_url:
        rows.append(
            [InlineKeyboardButton("🔗 На сайте", url=abs_url)],
        )
    rows.append(
        [
            InlineKeyboardButton("◀", callback_data=f"hp:{prev_i}"),
            InlineKeyboardButton("▶", callback_data=f"hp:{next_i}"),
        ],
    )
    return InlineKeyboardMarkup(rows)


async def fetch_home_page_promotions() -> Optional[List[dict]]:
    """Загрузка баннеров по HOME_PROMOTIONS_JSON_URL.

    None — URL не задан или HTTP/сеть: **не затираем** кэш (в т.ч. после POST /api/sync/promotions).
    [] — явно пустой ответ 200. Список — новые данные.
    """
    url = (HOME_PROMOTIONS_JSON_URL or "").strip()
    if not url:
        return None
    log = logging.getLogger(__name__)
    try:
        to = aiohttp.ClientTimeout(total=25)
        async with aiohttp.ClientSession(timeout=to) as session:
            async with session.get(url, headers={"Accept": "application/json"}) as resp:
                if resp.status != 200:
                    log.info("HOME_PROMOTIONS_JSON_URL: HTTP %s", resp.status)
                    return None
                data = await resp.json()
    except Exception:
        log.exception("HOME_PROMOTIONS_JSON_URL: ошибка загрузки")
        return None
    return _normalize_home_promotions_list(data)


def _parse_next_data_json_from_html(html: str) -> Optional[dict]:
    for q in ('id="__NEXT_DATA__"', "id='__NEXT_DATA__'"):
        i = html.find(q)
        if i < 0:
            continue
        j = html.find(">", i)
        if j < 0:
            continue
        k = html.find("</script>", j)
        if k < 0:
            continue
        raw = html[j + 1 : k].strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def _home_promotions_walk_next_payload(
    obj: object, depth: int, max_depth: int
) -> Optional[List[dict]]:
    if depth > max_depth or not isinstance(obj, dict):
        return None
    keys = (
        "homePromotions",
        "promotions",
        "promoSlides",
        "heroSlides",
        "mainBanners",
        "bannerSlides",
        "homeBanners",
    )
    for pk in keys:
        if pk not in obj:
            continue
        cand = obj[pk]
        if isinstance(cand, dict):
            cand = cand.get("items") or cand.get("slides") or cand.get("data")
        if isinstance(cand, list):
            items = _normalize_home_promotions_list(cand)
            if items:
                return items
    for v in obj.values():
        r = _home_promotions_walk_next_payload(v, depth + 1, max_depth)
        if r:
            return r
    return None


def _home_promotions_from_next_data(payload: dict) -> Optional[List[dict]]:
    props = payload.get("props")
    if isinstance(props, dict):
        pp = props.get("pageProps")
        if isinstance(pp, dict):
            got = _home_promotions_walk_next_payload(pp, 0, 14)
            if got:
                return got
    return _home_promotions_walk_next_payload(payload, 0, 10)


def _scrape_home_promotions_swiper_vitrina(html: str, base: str) -> Optional[List[dict]]:
    """Слайдер витрины (swiper-slide + aspect-video) до секции каталога — как на illucards.by."""
    prefix = html
    for needle in ('id="collection"', "id='collection'"):
        i = html.find(needle)
        if i > 0:
            prefix = html[:i]
            break
    pat_open = re.compile(r'<div class="swiper-slide[^"]*"[^>]*>', re.I)
    base_root = base.rstrip("/")
    default_link = f"{base_root}/"
    pairs: List[Tuple[str, str]] = []
    pos = 0
    while True:
        m = pat_open.search(prefix, pos)
        if not m:
            break
        start = m.end()
        m2 = pat_open.search(prefix, start)
        end = m2.start() if m2 else start + 4500
        chunk = prefix[start:end]
        pos = m.end()
        if "aspect-video" not in chunk:
            continue
        if "catalog-card" in chunk or "card-stack" in chunk:
            continue
        hm = re.search(r'<a\s[^>]*\bhref="([^"]+)"', chunk, re.I)
        im = re.search(r'<img[^>]+src="([^"]+)"', chunk, re.I)
        if not im:
            continue
        src = im.group(1).strip()
        if "/uploads/" not in src and not re.search(
            r"\.(webp|png|jpe?g)(\?|$)", src, re.I
        ):
            continue
        href = hm.group(1).strip() if hm else ""
        pairs.append((href, src))
    if not pairs:
        return None
    seen_img: set = set()
    out: List[dict] = []
    for href, src in pairs:
        img = _resolve_site_media_url(src)
        if not img or img in seen_img:
            continue
        seen_img.add(img)
        link = (href or default_link).strip()
        if not link.startswith("http"):
            link = _promo_click_absolute_url(link) or default_link
        out.append({"image": img, "link": link, "title": "", "order": len(out)})
        if len(out) >= 24:
            break
    return out or None


def _preload_image_href_noise(raw_h: str) -> bool:
    p = raw_h.lower()
    bad = (
        "logo",
        "favicon",
        "sprite",
        "og-image",
        "avatar",
        "32x32",
        "64x64",
        "/fonts/",
        "apple-touch",
    )
    return any(b in p for b in bad)


async def _fetch_home_promotions_scrape_fallback() -> Optional[List[dict]]:
    """Если нет API JSON: Next __NEXT_DATA__, иначе swiper витрины, иначе узкий preload <head>."""
    if HOME_PROMOTIONS_SCRAPE_DISABLE:
        return None
    base = _illucards_site_base_url().strip()
    if not base:
        return None
    log = logging.getLogger(__name__)
    try:
        to = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=to) as session:
            async with session.get(
                f"{base}/",
                headers={"Accept": "text/html", "User-Agent": "IlluCardsBot/1.0"},
            ) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
    except Exception:
        log.debug("Акции: scrape главной — ошибка сети", exc_info=True)
        return None
    nd = _parse_next_data_json_from_html(html)
    if isinstance(nd, dict):
        from_nd = _home_promotions_from_next_data(nd)
        if from_nd:
            log.info("Акции: из HTML главной взято %d баннеров (__NEXT_DATA__)", len(from_nd))
            return from_nd
    sw = _scrape_home_promotions_swiper_vitrina(html, base)
    if sw:
        log.info("Акции: из HTML главной взято %d баннеров (swiper витрина)", len(sw))
        return sw
    low = html.lower()
    he = low.find("</head>")
    chunk = html[:he] if he > 0 else html[:120000]
    out: List[dict] = []
    seen: set = set()
    for m in re.finditer(r"<link[^>]+>", chunk, re.I):
        tag = m.group(0)
        tl = tag.lower()
        if "preload" not in tl or "image" not in tl:
            continue
        hm = re.search(r'href\s*=\s*"([^"]+)"', tag, re.I) or re.search(
            r"href\s*=\s*'([^']+)'", tag, re.I
        )
        if not hm:
            continue
        raw_h = hm.group(1).strip()
        if not raw_h or raw_h in seen or _preload_image_href_noise(raw_h):
            continue
        if "/uploads/" not in raw_h and not raw_h.endswith(
            (".webp", ".png", ".jpg", ".jpeg")
        ):
            continue
        seen.add(raw_h)
        img = _resolve_site_media_url(raw_h)
        if not img:
            continue
        out.append({"image": img, "link": f"{base}/", "title": "", "order": len(out)})
        if len(out) >= 8:
            break
    if not out:
        return None
    log.info("Акции: из HTML главной взято %d баннеров (preload)", len(out))
    return out


async def refresh_home_page_promotions_cache(application: Application) -> int:
    items = await fetch_home_page_promotions()
    if items is None:
        if not HOME_PAGE_PROMOTIONS:
            primed = await _fetch_home_promotions_scrape_fallback()
            if primed:
                apply_home_page_promotions(primed)
                application.bot_data["home_promotions"] = list(HOME_PAGE_PROMOTIONS)
                return len(primed)
        return len(HOME_PAGE_PROMOTIONS)
    apply_home_page_promotions(items)
    application.bot_data["home_promotions"] = list(HOME_PAGE_PROMOTIONS)
    return len(items)


async def _http_sync_home_promotions(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return _login_json_response({"success": False, "error": "Некорректный JSON"}, status=400)
    if not isinstance(data, dict):
        return _login_json_response({"success": False, "error": "Некорректные данные"}, status=400)
    if not _sync_auth_ok(request, data):
        return _login_json_response({"success": False, "error": "Forbidden"}, status=403)
    raw = data.get("items")
    if raw is None:
        raw = data.get("homePromotions") or data.get("promotions")
    items = _normalize_home_promotions_list(raw if raw is not None else [])
    apply_home_page_promotions(items)
    return _login_json_response({"success": True, "items": len(items)})


async def _http_site_notify(request: web.Request) -> web.Response:
    """POST /api/notify — сайт (Vercel) шлёт Telegram через Render, без TELEGRAM_BOT_TOKEN."""
    log = logging.getLogger(__name__)
    try:
        data = await request.json()
    except Exception:
        return _login_json_response({"success": False, "error": "Некорректный JSON"}, status=400)
    if not isinstance(data, dict):
        return _login_json_response({"success": False, "error": "Некорректные данные"}, status=400)
    if not _site_api_auth_ok(request, data):
        return _login_json_response({"success": False, "error": "Forbidden"}, status=403)
    bot = request.app.get("bot")
    if bot is None:
        return _login_json_response({"success": False, "error": "Бот недоступен"}, status=503)

    target = str(data.get("target") or "customer").strip().lower()
    event = str(data.get("event") or "custom").strip().lower()
    raw_text = str(data.get("text") or data.get("message") or "").strip()

    if target == "admin":
        if not raw_text:
            oid_raw = data.get("botOrderId") or data.get("orderId") or data.get("order_id")
            status = str(data.get("status") or "").strip()
            ext = str(data.get("externalOrderId") or data.get("external_id") or "").strip()
            parts = ["📣 Сайт"]
            if oid_raw not in (None, ""):
                parts.append(f"заказ #{oid_raw}")
            if ext:
                parts.append(ext)
            if status:
                parts.append(_order_status_label_ru(status))
            raw_text = " · ".join(parts) if len(parts) > 1 else ""
        if not raw_text:
            return _login_json_response({"success": False, "error": "Укажите text"}, status=400)
        sent = 0
        for tgt in _admin_order_notify_targets() or ([int(ADMIN_ID)] if ADMIN_ID else []):
            try:
                await bot.send_message(
                    chat_id=tgt,
                    text=raw_text[:4090],
                    disable_web_page_preview=True,
                )
                sent += 1
            except Exception:
                log.exception("site notify admin target=%s", tgt)
        if sent == 0:
            return _login_json_response(
                {"success": False, "error": "Не удалось отправить админу"},
                status=502,
            )
        return _login_json_response({"success": True, "sent": sent})

    uid = _resolve_sync_uid(data)
    if not uid:
        return _login_json_response(
            {"success": False, "error": "Пользователь не найден (telegramUserId / username)"},
            status=404,
        )
    if not raw_text and event in ("order_status", "status"):
        try:
            oid = int(data.get("botOrderId") or data.get("orderId") or data.get("order_id") or 0)
        except (TypeError, ValueError):
            oid = 0
        status = str(data.get("status") or "new").strip()
        sk = _norm_bot_order_status(status)
        raw_text = _format_customer_order_status_notice(max(0, oid), sk)
        if oid <= 0:
            raw_text = raw_text.replace("(#0)", "").strip()
    if not raw_text:
        return _login_json_response(
            {"success": False, "error": "Укажите text или status"},
            status=400,
        )
    ok = await _send_customer_plain(bot, int(uid), raw_text[:4090])
    if not ok:
        return _login_json_response(
            {"success": False, "error": "Не удалось отправить клиенту"},
            status=502,
        )
    log.info("site notify customer uid=%s event=%s", uid, event)
    return _login_json_response({"success": True, "user_id": int(uid)})


def _login_api_base_meta(request: web.Request) -> str:
    """Публичный URL API входа для meta login-api-base (CORS + verify на том же процессе, что выдал код)."""
    explicit = (os.getenv("LOGIN_API_PUBLIC_URL") or "").strip().rstrip("/")
    if explicit:
        return explicit
    proto = (request.headers.get("X-Forwarded-Proto") or request.scheme or "https").strip()
    if "," in proto:
        proto = proto.split(",", 1)[0].strip()
    host = (
        (request.headers.get("X-Forwarded-Host") or request.headers.get("Host") or "")
        .strip()
    )
    if "," in host:
        host = host.split(",", 1)[0].strip()
    if not host:
        return ""
    return f"{proto}://{host}".rstrip("/")


async def _http_login_page(request: web.Request) -> web.Response:
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
    bot_username = str(request.app.get("bot_username") or "").strip()
    api_base = _login_api_base_meta(request)
    post_login = (os.getenv("POST_LOGIN_REDIRECT") or "").strip()
    if not post_login:
        post_login = f"{_illucards_site_base_url()}/catalog"
    html = html.replace("__BOT_USERNAME__", bot_username)
    html = html.replace("__LOGIN_API_BASE__", api_base)
    html = html.replace("__POST_LOGIN_REDIRECT__", post_login)
    return web.Response(text=html, content_type="text/html", charset="utf-8")


async def _http_health(request: web.Request) -> web.Response:
    """Лёгкий GET для UptimeRobot / Render / балансировщиков (без HTML и без БД)."""
    return web.json_response({"ok": True})


async def _run_login_http_api(bot) -> None:
    """HTTP: POST /api/send-code, /api/verify-code; страница /login для проверки."""
    log = logging.getLogger(__name__)
    app = web.Application()
    app["bot"] = bot
    try:
        me = await bot.get_me()
        app["bot_username"] = str(me.username or "").strip()
    except Exception:
        app["bot_username"] = ""
    globals()["_BOT_APP_BOT"] = bot
    app.router.add_get("/", _http_login_page)
    app.router.add_get("/login", _http_login_page)
    app.router.add_get("/health", _http_health)
    app.router.add_options("/api/send-code", _http_login_options)
    app.router.add_options("/api/verify-code", _http_login_options)
    app.router.add_options("/api/telegram-auth", _http_login_options)
    app.router.add_options("/api/sync/cart", _http_login_options)
    app.router.add_options("/api/sync/favorites", _http_login_options)
    app.router.add_options("/api/sync/state", _http_login_options)
    app.router.add_options("/api/sync/promotions", _http_login_options)
    app.router.add_options("/api/notify", _http_login_options)
    app.router.add_post("/api/send-code", _http_send_code)
    app.router.add_post("/api/verify-code", _http_verify_code)
    app.router.add_post("/api/telegram-auth", _http_telegram_auth)
    app.router.add_post("/api/sync/cart", _http_sync_cart)
    app.router.add_post("/api/sync/favorites", _http_sync_favorites)
    app.router.add_get("/api/sync/state", _http_get_sync_state)
    app.router.add_post("/api/sync/state", _http_sync_state)
    app.router.add_post("/api/sync/promotions", _http_sync_home_promotions)
    app.router.add_post("/api/notify", _http_site_notify)
    runner = web.AppRunner(app)
    await runner.setup()
    raw_port = (os.getenv("LOGIN_API_PORT") or os.getenv("PORT") or "10000").strip()
    try:
        port = int(raw_port)
    except ValueError:
        port = 8765
    host_raw = (os.getenv("LOGIN_API_HOST") or "").strip()
    if host_raw:
        host = host_raw
    else:
        host = "0.0.0.0"
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    log.info(
        "HTTP API сайта: %s:%s (/, /health, /login, /api/send-code, /api/verify-code, /api/notify, /api/sync/cart)",
        host,
        port,
    )
    stop = asyncio.Event()
    try:
        await stop.wait()
    except asyncio.CancelledError:
        raise
    finally:
        await runner.cleanup()


async def post_shutdown(application: Application) -> None:
    """Корректно гасим aiohttp-сервер входа, чтобы при деплое не оставались pending-task."""
    log = logging.getLogger(__name__)
    task = application.bot_data.get("login_http_api_task")
    if isinstance(task, asyncio.Task) and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Остановка HTTP API входа")
    application.bot_data.pop("login_http_api_task", None)


async def on_ptb_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log = logging.getLogger(__name__)
    err = context.error
    if isinstance(err, Conflict):
        log.error(
            "Telegram 409 Conflict: одновременно идёт getUpdates с этим токеном, либо раньше "
            "был включён webhook. При старте бот вызывает delete_webhook; если ошибка "
            "повторяется — подождите 1–2 мин после деплоя, проверьте второй сервис/воркер "
            "на Render с тем же TELEGRAM_BOT_TOKEN, локальный запуск бота или Cursor-терминал."
        )
        return
    log.error("Необработанная ошибка в обработчике: %s", err, exc_info=err)


ILLUCARDS_BASE = _illucards_site_base_url()
CARDS_JSON_URL = os.getenv("CARDS_JSON_URL", f"{ILLUCARDS_BASE}/api/products")
# Публичный JSON витрины: [{ "image", "link?", "order?" }]; если пусто — только push с сайта POST /api/sync/promotions
HOME_PROMOTIONS_JSON_URL = (os.getenv("HOME_PROMOTIONS_JSON_URL") or "").strip()
# Если нет JSON URL и кэш пуст — один раз подтянуть preload-картинки из <head> главной (слайдер).
HOME_PROMOTIONS_SCRAPE_DISABLE = str(
    os.getenv("HOME_PROMOTIONS_SCRAPE_DISABLE") or ""
).strip().lower() in ("1", "true", "yes", "on")
SYNC_EVERY_SEC = _env_int("ILLUCARDS_SYNC_EVERY_SEC", 900)
ORDER_DEEP_LINK_API_URL = os.getenv(
    "ORDER_DEEP_LINK_API_URL",
    f"{ILLUCARDS_BASE}/api/order/{{id}}",
).strip()
ORDER_USER_ORDERS_API_URL = os.getenv(
    "ORDER_USER_ORDERS_API_URL",
    f"{ILLUCARDS_BASE}/api/orders?user_id={{user_id}}",
).strip()
SITE_USER_STATE_API_URL = os.getenv(
    "SITE_USER_STATE_API_URL",
    f"{ILLUCARDS_BASE}/api/user-state?user_id={{user_id}}",
).strip()
ORDER_STATUS_UPDATE_API_URL = os.getenv(
    "ORDER_STATUS_UPDATE_API_URL",
    f"{ILLUCARDS_BASE}/api/order/update",
).strip()
ORDER_FROM_BOT_API_URL = os.getenv(
    "ORDER_FROM_BOT_API_URL",
    f"{ILLUCARDS_BASE}/api/order/from-bot",
).strip()
ORDER_STATUS_UPDATE_SECRET = os.getenv("ILLUCARDS_ORDER_UPDATE_SECRET", "").strip()
SITE_USER_STATE_SYNC_SECRET = (
    os.getenv("ILLUCARDS_USER_STATE_SYNC_SECRET")
    or os.getenv("ILLUCARDS_ORDER_UPDATE_SECRET")
    or ""
).strip()
# Опционально: POST на ваш URL после нажатия «Оплатить» (callback paid) — очистить корзину на сайте.
ILLUCARDS_CART_CLEAR_ON_PROOF_URL = (os.getenv("ILLUCARDS_CART_CLEAR_ON_PROOF_URL") or "").strip()
# Тот же секрет, что на сайте (Vercel): GET /api/order/{id} и POST /api/order/update — Bearer.
# Если на проде включена проверка без заголовка — 401 и заказ по ссылке не подтянется.
# Tinder-режим каталога: одна карта на экран, смена через editMessageMedia
TINDER_NO_IMAGE = "https://picsum.photos/seed/illu-noimg/400/550"

# Доставка при оформлении из корзины: callback dl:{by|ru|ua|ot}
DELIVERY_OPTIONS: dict = {
    "by": ("🇧🇾 Беларусь", 6, "BYN"),
    "ru": ("🇷🇺 Россия", 600, "RUB"),
    "ua": ("🇺🇦 Украина", 3000, "RUB"),
    "ot": ("🌍 Другие страны", 800, "RUB"),
}

SITE_DELIVERY_TO_BOT: dict = {
    "BY": "by",
    "RU": "ru",
    "UA": "ua",
    "OTHER": "ot",
}


def _remember_user_delivery_country(uid: int, code: str) -> None:
    u = int(uid or 0)
    c = str(code or "").strip().lower()
    if not u or c not in DELIVERY_OPTIONS:
        return
    USER_PREF_DELIVERY_COUNTRY[u] = c
    save_state()


def _sync_user_delivery_country_to_user_data(uid: int, user_data: Optional[dict]) -> None:
    if not user_data:
        return
    try:
        u = int(uid or 0)
    except (TypeError, ValueError):
        u = 0
    if not u:
        return
    if str(user_data.get("preferred_delivery_country") or "").strip().lower() in DELIVERY_OPTIONS:
        return
    c = str(USER_PREF_DELIVERY_COUNTRY.get(u) or "").strip().lower()
    if c in DELIVERY_OPTIONS:
        user_data["preferred_delivery_country"] = c


def _delivery_option_for_site_code(raw: str) -> Tuple[str, str, int, str]:
    s = str(raw or "").strip()
    su = s.upper()
    bot_code = SITE_DELIVERY_TO_BOT.get(su, "")
    if not bot_code:
        low = s.lower()
        if low in ("by", "blr", "belarus", "беларусь", "беларуси", "рб", "bel"):
            bot_code = "by"
        elif low in ("ru", "russia", "россия", "россии", "рф", "rus"):
            bot_code = "ru"
        elif low in ("ua", "ukraine", "украина", "украины", "ukr"):
            bot_code = "ua"
        elif low in ("other", "others", "world", "international", "другие", "другое", "мир"):
            bot_code = "ot"
    if not bot_code:
        bot_code = "by"
    label, amount, currency = DELIVERY_OPTIONS.get(bot_code, DELIVERY_OPTIONS["by"])
    return bot_code, str(label), int(amount), str(currency)


def _coerce_card_price_int(val: object) -> int:
    if val is None:
        return 0
    try:
        return max(0, int(float(val)))
    except (TypeError, ValueError):
        return 0


def _cart_line_qty_coerce(raw: object) -> int:
    """Количество в строке корзины (сайт/JSON может слать строку или float)."""
    try:
        q = int(float(raw))
    except (TypeError, ValueError):
        return 1
    return max(1, q)


def _is_truthy_flag(val: object) -> bool:
    if val is True:
        return True
    if val is False or val is None:
        return False
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "y", "on", "да")


def _card_is_sale_payload(item: dict, price_now: int, legacy_price: int) -> bool:
    """Акция по флагам витрины сайта или по признакам скидки."""
    if not isinstance(item, dict):
        return False
    flag_keys = (
        "isSale",
        "sale",
        "onSale",
        "isPromo",
        "promo",
        "isPromotion",
        "hotPrice",
        "isHotPrice",
    )
    for key in flag_keys:
        if _is_truthy_flag(item.get(key)):
            return True
    for container in (item.get("showcase"), item.get("promo"), item.get("flags"), item.get("badges")):
        if not isinstance(container, dict):
            continue
        for key in flag_keys:
            if _is_truthy_flag(container.get(key)):
                return True
    for key in ("tags", "labels", "badges"):
        raw = item.get(key)
        if not isinstance(raw, list):
            continue
        normalized = {str(x).strip().lower() for x in raw if str(x).strip()}
        if normalized & {"sale", "promo", "discount", "hot", "hotprice", "акция", "горячая цена"}:
            return True
    old_price = _coerce_card_price_int(
        item.get("oldPrice", item.get("compareAtPrice", item.get("priceOld")))
    )
    current = int(price_now or 0) or _coerce_card_price_int(item.get("price")) or int(legacy_price or 0)
    if old_price > 0 and current > 0 and old_price > current:
        return True
    discount = _coerce_card_price_int(item.get("discount", item.get("discountPercent")))
    return discount > 0


def _product_is_sale(p: dict) -> bool:
    if not isinstance(p, dict):
        return False
    if _is_truthy_flag(p.get("isSale")):
        return True
    for key in ("isPromo", "onSale", "promo", "sale"):
        if _is_truthy_flag(p.get(key)):
            return True
    return False


def _goods_currency_for_delivery_country(delivery_country_code: str) -> str:
    """Валюта цен товаров: Беларусь — BYN, иначе RUB."""
    return "BYN" if str(delivery_country_code or "").strip().lower() == "by" else "RUB"


def _goods_price_region_from_user_data(user_data: Optional[dict]) -> str:
    """Код страны dl:* для цены товара; до выбора доставки — BYN (Беларусь)."""
    if not user_data:
        return "by"
    c = str(user_data.get("delivery_country") or "").strip().lower()
    if c not in DELIVERY_OPTIONS:
        c = str(user_data.get("preferred_delivery_country") or "").strip().lower()
    if c not in DELIVERY_OPTIONS:
        c = str(user_data.get("catalog_currency_country") or "").strip().lower()
    return c if c in DELIVERY_OPTIONS else "by"


def _cart_price_region_for_user(uid: int, user_data: Optional[dict]) -> str:
    """Регион цен для корзины: оформление в боте → синк с сайта → валюта каталога."""
    ud = user_data or {}
    c = str(ud.get("delivery_country") or "").strip().lower()
    if c in DELIVERY_OPTIONS:
        return c
    try:
        u = int(uid or 0)
    except (TypeError, ValueError):
        u = 0
    if u:
        c2 = str(USER_PREF_DELIVERY_COUNTRY.get(u) or "").strip().lower()
        if c2 in DELIVERY_OPTIONS:
            return c2
    return _goods_price_region_from_user_data(ud)


def _checkout_start_reprice_region(ud: dict, uid: int) -> str:
    """Регион цен при «Оформить» / сбросе на выбор страны — как в корзине."""
    cc = _cart_price_region_for_user(uid, ud)
    return cc if cc in DELIVERY_OPTIONS else "by"


def _infer_site_grand_total_currency(data: dict) -> str:
    """Валюта итога с сайта (если не указана — из страны доставки в том же JSON)."""
    if not isinstance(data, dict):
        return "BYN"
    s = str(
        data.get("cartGrandTotalCurrency")
        or data.get("totalCurrency")
        or data.get("grandTotalCurrency")
        or ""
    ).strip().upper()
    if s in ("BYN", "RUB"):
        return s
    raw = data.get("deliveryCountry") or data.get("delivery_country") or "BY"
    bot_code, _, _, _ = _delivery_option_for_site_code(str(raw))
    return _goods_currency_for_delivery_country(bot_code)


def _product_unit_price_for_delivery(p: dict, delivery_country_code: str) -> int:
    """Цена единицы товара по стране доставки (из priceByn / priceRub в cards.json)."""
    cc = str(delivery_country_code or "").strip().lower()
    pb = _coerce_card_price_int(p.get("price_byn"))
    pr = _coerce_card_price_int(p.get("price_rub"))
    if cc == "by":
        if pb > 0:
            return pb
        if pr > 0:
            return pr
        return _coerce_card_price_int(p.get("price"))
    if pr > 0:
        return pr
    if pb > 0:
        return pb
    return _coerce_card_price_int(p.get("price"))


def _reprice_lines_for_delivery(
    lines: List[dict],
    products: List[dict],
    delivery_country_code: str,
    *,
    respect_site_lines: bool = True,
) -> None:
    """Обновить line[\"price\"] из каталога по ref и стране доставки.

    Строки с from_site (пришли с сайта) не трогаем, пока пользователь сам не сменит
    валюту в каталоге бота (ccur) — там respect_site_lines=False.
    """
    if not lines or not products:
        return
    for line in lines:
        if respect_site_lines and line.get("from_site"):
            continue
        ref = str(line.get("ref") or "").strip()
        if not ref:
            continue
        p = _product_from_callback(ref, products)
        if p is not None:
            line["price"] = _product_unit_price_for_delivery(p, delivery_country_code)


def _sync_line_currencies_for_delivery_country(
    lines: List[dict], delivery_country_code: str
) -> None:
    """Подпись валюты в строках корзины/чекаута под выбранную страну доставки."""
    cc = str(delivery_country_code or "").strip().lower()
    if cc not in DELIVERY_OPTIONS:
        cc = "by"
    cur = _goods_currency_for_delivery_country(cc)
    for line in lines:
        if isinstance(line, dict):
            line["line_currency"] = cur


def _reprice_order_checkout_for_delivery(
    lines: List[dict],
    products: List[dict],
    delivery_country_code: str,
) -> None:
    """Черновик заказа: цены из каталога и валюта строк по стране (в т.ч. позиции с сайта)."""
    cc = str(delivery_country_code or "").strip().lower()
    if cc not in DELIVERY_OPTIONS:
        cc = "by"
    for line in lines:
        if isinstance(line, dict):
            line.pop("from_site", None)
    _reprice_lines_for_delivery(
        lines, products, cc, respect_site_lines=False
    )
    _sync_line_currencies_for_delivery_country(lines, cc)


def _order_line_currency_from_delivery(d: Optional[dict]) -> str:
    if not d or not isinstance(d, dict):
        return "BYN"
    return _goods_currency_for_delivery_country(str(d.get("country") or ""))


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
    "—": "Без редкости",
    "common": "Обычная",
    "limited": "Лимитированная",
    "novelty": "Новинка",
    "replica": "Реплика",
    "adult": "18+",
}


def _rarity_label_ru(s: str) -> str:
    t = (s or "").strip()
    if t == "—" or t == "":
        return "Без редкости"
    if any("\u0400" <= c <= "\u04ff" for c in t):
        return t
    k = t.lower()
    if k in RARITY_RU:
        return RARITY_RU[k]
    return t

REPLY_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(BTN_CHAT), KeyboardButton(BTN_MY_ORDERS)],
        [KeyboardButton(BTN_DELIVERY)],
    ],
    resize_keyboard=True,
)

# Тексты reply-клавиатуры (сброс режима «чат с админом» при переходе в меню)
REPLY_MENU_TEXTS = frozenset(
    {
        BTN_CHAT,
        BTN_MY_ORDERS,
        BTN_DELIVERY,
    },
)

# Старые ссылки вида ?start=web_login — не показывать как «заказ», обрабатывать как обычный /start
START_IGNORED_DEEP_LINK = frozenset({"web_login"})

START_INTRO_TEXT = (
    "Добро пожаловать в IlluCards!\n\n"
    "Полную коллекцию карточек удобно смотреть на сайте — нажмите «Открыть сайт».\n\n"
    "Если вы оформили заказ на сайте по нашей ссылке, он уже продублирован в этом чате: "
    "останется только подтвердить его здесь."
)

START_ORDER_FROM_SITE_HEADER = (
    "🛒 Вы перешли с сайта с черновиком заказа.\n\n"
    "Ниже — состав и доставка. Если всё верно, нажмите «Подтвердить заказ» — "
    "бот откроет шаги оплаты. В чат администратору заказ уходит только после оплаты (скрин чека). "
    "«Отмена» — можно оформить заново на сайте."
)

START_WELCOME_MENU_TEXT = "Выбери действие в меню ниже 👇"
START_SITE_TRANSITION_TEXT = (
    "Вы перешли с сайта IlluCards в Telegram. Сейчас продолжим здесь."
)


def _illucards_site_open_markup(telegram_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Открыть сайт",
                    url=f"https://illucards.by/?user_id={int(telegram_id)}",
                ),
            ],
        ],
    )


async def _maybe_thank_first_telegram_auth(msg: Message, uid: int) -> None:
    row = users_ensure(uid)
    if not row.get("tg_auth_thanked"):
        row["tg_auth_thanked"] = True
        await msg.reply_text("Спасибо за авторизацию! Рады видеть вас в IlluCards.")


async def _reply_site_transition_notice(msg: Message, uid: int) -> None:
    await msg.reply_text(START_SITE_TRANSITION_TEXT, reply_markup=REPLY_KB)


async def _send_start_intro_with_site_button(
    msg: Message, uid: int, ud: Optional[dict] = None
) -> None:
    if ud is not None:
        ud.pop("pending_order", None)
        ud.pop("deep_link_order_session", None)
    await _maybe_thank_first_telegram_auth(msg, uid)
    m1 = await msg.reply_text(
        START_INTRO_TEXT,
        reply_markup=_illucards_site_open_markup(uid),
    )
    _track_temp_message(uid, m1)
    m2 = await msg.reply_text(START_WELCOME_MENU_TEXT, reply_markup=REPLY_KB)
    _track_temp_message(uid, m2)


CATALOG_INTRO_TEXT = (
    "📦 Вся коллекция\n\n"
    "Выберите категорию карточек 👇"
)
CATALOG_CURRENCY_PICK_TEXT = (
    "💱 Перед просмотром каталога выберите, в какой валюте показывать цены 👇"
)

SUPPORT_INTRO_TEXT = (
    "💬 Связь с нами\n\n"
    "Напишите сюда или пришлите фото, если:\n\n"
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
        front = item.get("frontImage", item.get("image", "")) or ""
        if isinstance(front, str) and front.startswith("/"):
            image = ILLUCARDS_BASE + front
        else:
            image = front
        rar = item.get("rarity", "")
        legacy = _coerce_card_price_int(item.get("price"))
        price_byn = _coerce_card_price_int(item.get("priceByn"))
        price_rub = _coerce_card_price_int(item.get("priceRub"))
        if price_rub <= 0 and legacy > 0:
            price_rub = legacy
        if price_byn <= 0 and legacy > 0:
            price_byn = legacy
        if price_byn <= 0 and price_rub > 0:
            price_byn = price_rub
        if price_rub <= 0 and price_byn > 0:
            price_rub = price_byn
        is_sale = _card_is_sale_payload(item, price_byn or price_rub, legacy)
        sku_raw = item.get("sku") or item.get("SKU") or ""
        slug_raw = item.get("slug") or item.get("handle") or item.get("permalink") or ""
        cards.append(
            {
                "id": item.get("id"),
                "name": item.get("title") or item.get("name") or "Без названия",
                "price_byn": price_byn,
                "price_rub": price_rub,
                "price": price_byn,
                "category": item.get("category", "Без категории"),
                "rarity": (str(rar).strip() or "—"),
                "image": image,
                "isSale": is_sale,
                "sku": str(sku_raw).strip(),
                "slug": str(slug_raw).strip(),
            }
        )
    print(
        f"Загружено карточек: {len(cards)} "
        "(цены: priceByn для BY, priceRub для остальных стран доставки)"
    )
    return cards


def _product_from_callback(ref: str, products: List[dict]) -> Optional[dict]:
    if not ref or not products:
        return None
    s = str(ref).strip()
    s_cf = s.casefold()
    for p in products:
        pid = p.get("id")
        if pid is None:
            continue
        ps = str(pid).strip()
        if ps == s or ps.casefold() == s_cf:
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
    """Цена по умолчанию (BYN-ветка), для обратной совместимости."""
    return _product_unit_price_for_delivery(p, "by")


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
    save_state()


def _reprice_uid_cart(
    user_id: int,
    user_data: Optional[dict],
    products: List[dict],
    delivery_country_code: str,
    *,
    respect_site_lines: bool = True,
) -> None:
    if not user_id or not products:
        return
    lines = _cart_get_lines_uid(user_id, user_data)
    cc = str(delivery_country_code or "").strip().lower()
    if cc not in DELIVERY_OPTIONS:
        cc = "by"
    _reprice_lines_for_delivery(
        lines, products, cc, respect_site_lines=respect_site_lines
    )
    if not respect_site_lines:
        for line in lines:
            if isinstance(line, dict):
                line.pop("from_site", None)
        _sync_line_currencies_for_delivery_country(lines, cc)
    _cart_set_items_uid(user_id, lines)


def _cart_add_line_uid(
    user_id: int,
    user_data: Optional[dict],
    ref: str,
    product: dict,
    name: str,
    price: int,
) -> None:
    _cart_clear_site_pricing_hints(user_id)
    lines = _cart_get_lines_uid(user_id, user_data)
    for line in lines:
        if str(line.get("ref")) == ref:
            line["qty"] = int(line.get("qty") or 1) + 1
            _cart_set_items_uid(user_id, lines)
            return
    lines.append({"ref": ref, "name": name, "price": price, "qty": 1})
    _cart_set_items_uid(user_id, lines)


def _cart_remove_line_uid(user_id: int, user_data: Optional[dict], index: int) -> bool:
    _cart_clear_site_pricing_hints(user_id)
    lines = _cart_get_lines_uid(user_id, user_data)
    if 0 <= index < len(lines):
        lines.pop(index)
        _cart_set_items_uid(user_id, lines)
        return True
    return False


def _cart_dec_line_uid(user_id: int, user_data: Optional[dict], index: int) -> bool:
    _cart_clear_site_pricing_hints(user_id)
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
    save_state()


def _persist_site_pending_order(
    uid: int, preview: str, meta: Optional[dict] = None, user_data: Optional[dict] = None
) -> None:
    """Черновик для confirm_order/cancel_order — в память и в USER_CART (переживает рестарт)."""
    if not uid or not str(preview or "").strip():
        return
    p = str(preview).strip()
    SITE_LOGIN_PENDING_ORDER[int(uid)] = p
    rec = _ensure_user_cart(int(uid))
    rec["site_pending_preview"] = p
    meta_out: dict = deepcopy(meta) if isinstance(meta, dict) else {}
    lines_snap = _cart_get_lines_uid(int(uid), user_data)
    if lines_snap and not meta_out.get("items"):
        meta_out["items"] = deepcopy(lines_snap)
    meta_out = _merge_site_bonus_into_meta(int(uid), meta_out)
    if lines_snap:
        pt, pc, _ = _resolve_site_confirm_pricing(
            int(uid), user_data or {}, list(lines_snap), meta_out
        )
        meta_out["preview_pay_total"] = int(pt)
        meta_out["preview_pay_currency"] = str(pc)
    if meta_out:
        rec["site_pending_meta"] = meta_out
        if user_data is not None:
            user_data["pending_site_order_meta"] = deepcopy(meta_out)
    save_state()


def _get_site_pending_preview(uid: int) -> str:
    p = (SITE_LOGIN_PENDING_ORDER.get(int(uid)) or "").strip()
    if p:
        return p
    rec = USER_CART.get(int(uid))
    if isinstance(rec, dict):
        return str(rec.get("site_pending_preview") or "").strip()
    return ""


def _get_site_pending_meta(uid: int, user_data: Optional[dict] = None) -> Optional[dict]:
    if user_data is not None:
        m = user_data.get("pending_site_order_meta")
        if isinstance(m, dict) and m:
            return m
    rec = USER_CART.get(int(uid))
    if isinstance(rec, dict):
        m = rec.get("site_pending_meta")
        if isinstance(m, dict) and m:
            return m
    return None


def _clear_site_pending_order(uid: int, user_data: Optional[dict] = None) -> None:
    SITE_LOGIN_PENDING_ORDER.pop(int(uid), None)
    rec = USER_CART.get(int(uid))
    if isinstance(rec, dict):
        rec.pop("site_pending_preview", None)
        rec.pop("site_pending_meta", None)
    if user_data is not None:
        user_data.pop("pending_site_order_meta", None)
    save_state()


def _rebuild_site_login_pending_from_carts() -> None:
    """После load_state: восстановить SITE_LOGIN_PENDING_ORDER из сохранённой корзины."""
    SITE_LOGIN_PENDING_ORDER.clear()
    for uid, rec in USER_CART.items():
        if not isinstance(rec, dict):
            continue
        p = str(rec.get("site_pending_preview") or "").strip()
        if p:
            SITE_LOGIN_PENDING_ORDER[int(uid)] = p


def _restore_site_pending_to_user_data(uid: int, user_data: dict) -> None:
    meta = _get_site_pending_meta(uid, user_data)
    if meta:
        user_data["pending_site_order_meta"] = deepcopy(meta)


async def _restore_cart_lines_for_confirm(
    uid: int,
    user_data: Optional[dict],
    context: ContextTypes.DEFAULT_TYPE,
) -> List[dict]:
    """Вернуть позиции корзины для confirm_order (из USER_CART, meta или API заказа)."""
    if not uid:
        return []
    lines = list(_cart_get_lines_uid(uid, user_data))
    if lines:
        return lines
    meta = _get_site_pending_meta(uid, user_data)
    raw_items: List[dict] = []
    ext_id = ""
    if isinstance(meta, dict):
        ri = meta.get("items")
        if isinstance(ri, list):
            raw_items = [x for x in ri if isinstance(x, dict)]
        ext_id = str(meta.get("external_id") or "").strip()
        lh = meta.get("loyalty_hint")
        if not ext_id and isinstance(lh, dict):
            ext_id = str(lh.get("external_id") or "").strip()
    if raw_items:
        cc = str(USER_PREF_DELIVERY_COUNTRY.get(uid) or "by").strip().lower()
        if cc not in DELIVERY_OPTIONS:
            cc = "by"
        _apply_site_order_norm_to_user_cart(
            uid, {"items": deepcopy(raw_items), "delivery": {"country": cc}}
        )
        return list(_cart_get_lines_uid(uid, user_data))
    if ext_id:
        norm = await _fetch_order_for_deep_link(ext_id)
        if norm and norm.get("items"):
            _apply_site_order_norm_to_user_cart(uid, norm)
            try:
                products = await _get_products(context)
            except Exception:
                products = []
            preview = _format_site_order_confirm_preview(uid, norm, products)
            if preview:
                _persist_site_pending_order(
                    uid, preview, _get_site_pending_meta(uid, user_data), user_data
                )
            return list(_cart_get_lines_uid(uid, user_data))
    return []


def _favorites_get_refs_uid(user_id: int, user_data: Optional[dict] = None) -> List[str]:
    uid = int(user_id or 0)
    if not uid:
        return []
    raw = USER_FAVORITES.get(uid)
    if isinstance(raw, list):
        return [str(x).strip()[:120] for x in raw if str(x).strip()]
    return []


def _favorites_add_ref_uid(user_id: int, ref: str) -> int:
    uid = int(user_id or 0)
    r = str(ref or "").strip()[:120]
    if not uid or not r:
        return 0
    cur = list(USER_FAVORITES.get(uid) or [])
    if r not in cur:
        cur.append(r)
    USER_FAVORITES[uid] = cur
    save_state()
    return len(cur)


def _parse_site_cart_grand_total(data: dict) -> Optional[int]:
    """Итог корзины с сайта (уже с доставкой), если передан в JSON синка/verify."""
    if not isinstance(data, dict):
        return None
    for key in ("cartGrandTotal", "cartTotal", "grandTotal", "orderTotal"):
        if key not in data:
            continue
        v = _coerce_card_price_int(data.get(key))
        if v > 0:
            return v
    if "total" in data:
        v = _coerce_card_price_int(data.get("total"))
        if v > 0:
            return v
    return None


def _parse_site_cart_grand_total_smart(data: dict) -> Optional[int]:
    """Итог с сайта: приоритет полей *Rub / *Byn по стране доставки в JSON, иначе общие ключи."""
    if not isinstance(data, dict):
        return None
    inferred = _infer_site_grand_total_currency(data)
    if inferred == "RUB":
        for key in ("cartGrandTotalRub", "grandTotalRub", "totalRub", "cartTotalRub"):
            if key not in data:
                continue
            v = _coerce_card_price_int(data.get(key))
            if v > 0:
                return v
    else:
        for key in ("cartGrandTotalByn", "grandTotalByn", "totalByn", "cartTotalByn"):
            if key not in data:
                continue
            v = _coerce_card_price_int(data.get(key))
            if v > 0:
                return v
    return _parse_site_cart_grand_total(data)


def _parse_site_delivery_included_in_total(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    for key in (
        "deliveryIncludedInTotal",
        "delivery_in_total",
        "deliveryIncluded",
        "shippingIncluded",
    ):
        raw = data.get(key)
        if raw is True:
            return True
        if raw is False or raw is None:
            continue
        s = str(raw).strip().lower()
        if s in ("1", "true", "yes", "on", "да"):
            return True
    return False


def _cart_apply_site_pricing_hints(uid: int, data: dict) -> None:
    """Сохранить с сайта итог/флаг «доставка уже в сумме», чтобы бот не дублировал доставку."""
    if not uid:
        return
    _ensure_user_cart(uid)
    b = USER_CART[int(uid)]
    gt = _parse_site_cart_grand_total_smart(data)
    if gt and gt > 0:
        lines = _cart_get_lines_uid(uid)
        goods, _ = _cart_totals(lines)
        if int(goods) > 0 and int(gt) < int(goods):
            b.pop("site_cart_grand_total", None)
            b.pop("site_cart_grand_currency", None)
        else:
            inc = _parse_site_delivery_included_in_total(data)
            d_amt = _cart_delivery_amount_from_sync_data(data)
            exp = int(goods) if inc else int(goods) + int(d_amt)
            gti = int(gt)
            if not inc and exp > 0 and abs(gti - exp) > max(100, exp // 25):
                gti = exp
            elif inc and int(goods) > 0 and gti < int(goods) - max(50, int(goods) // 40):
                gti = int(goods)
            b["site_cart_grand_total"] = int(gti)
            line_cur = _site_cart_line_currency(
                uid,
                lines,
                str(
                    data.get("deliveryCountry")
                    or data.get("delivery_country")
                    or USER_PREF_DELIVERY_COUNTRY.get(int(uid))
                    or "by"
                ),
            )
            if line_cur in ("BYN", "RUB") and _site_grand_covers_goods(int(gti), int(goods)):
                b["site_cart_grand_currency"] = line_cur
            else:
                b["site_cart_grand_currency"] = _infer_site_grand_total_currency(data)
    else:
        b.pop("site_cart_grand_total", None)
        b.pop("site_cart_grand_currency", None)
    if _parse_site_delivery_included_in_total(data):
        b["site_delivery_included"] = True
    else:
        b.pop("site_delivery_included", None)
    b.pop("site_loyalty_pending_earn", None)


def _cart_clear_site_pricing_hints(uid: int) -> None:
    if not uid:
        return
    b = USER_CART.get(int(uid))
    if not isinstance(b, dict):
        return
    b.pop("site_cart_grand_total", None)
    b.pop("site_cart_grand_currency", None)
    b.pop("site_delivery_included", None)
    b.pop("site_loyalty_pending_earn", None)


def _cart_get_site_loyalty_pending_earn(uid: int) -> Optional[int]:
    return None


def _cart_get_site_grand_total(
    uid: int, display_currency: Optional[str] = None
) -> Optional[int]:
    b = USER_CART.get(int(uid))
    if not isinstance(b, dict):
        return None
    v = _coerce_card_price_int(b.get("site_cart_grand_total"))
    if v <= 0:
        return None
    want = str(display_currency or "").strip().upper()
    stored = str(b.get("site_cart_grand_currency") or "").strip().upper()
    if want and stored and want != stored:
        return None
    return v


def _cart_site_delivery_included(uid: int) -> bool:
    b = USER_CART.get(int(uid))
    if not isinstance(b, dict):
        return False
    return bool(b.get("site_delivery_included"))


def _site_grand_covers_goods(site_gt: int, goods_total: int) -> bool:
    """Итог с сайта не должен быть меньше суммы товаров — иначе это битый JSON или другая валюта."""
    if int(site_gt) <= 0:
        return False
    if int(goods_total) > 0 and int(site_gt) < int(goods_total):
        return False
    return True


def _cart_delivery_amount_from_sync_data(data: dict) -> int:
    """Сумма доставки из JSON синка (или по стране), для сверки с итогом с сайта."""
    if not isinstance(data, dict):
        return 0
    drec = data.get("delivery") or data.get("shipping")
    if isinstance(drec, dict):
        try:
            a = int(drec.get("amount") or 0)
            if a > 0:
                return a
        except (TypeError, ValueError):
            pass
    del_raw = str(
        data.get("deliveryCountry") or data.get("delivery_country") or ""
    ).strip()
    _, _, amt, _ = _delivery_option_for_site_code(del_raw or "BY")
    return int(amt)


def _cart_expected_grand_with_delivery(
    uid: int, user_data: Optional[dict], lines: List[dict]
) -> int:
    """Ожидаемый итог: сумма строк + доставка (или только товары, если доставка уже в сумме на сайте)."""
    goods, _ = _cart_totals(lines)
    inc = _cart_site_delivery_included(uid)
    ud = user_data or {}
    try:
        d_amt = int(ud.get("delivery_amount") or -1)
    except (TypeError, ValueError):
        d_amt = -1
    if d_amt < 0:
        code = str(ud.get("delivery_country") or "").strip().lower()
        if code not in DELIVERY_OPTIONS:
            code = _cart_price_region_for_user(uid, ud)
        _, d_amt, _ = DELIVERY_OPTIONS.get(code, DELIVERY_OPTIONS["by"])
    if inc:
        return max(0, int(goods))
    return max(0, int(goods) + int(d_amt))


def _cart_grand_total_for_display(
    uid: int,
    user_data: Optional[dict],
    lines: List[dict],
    cur: str,
) -> Tuple[int, bool]:
    """
    Итог для текста «🛒 Корзина». Второй элемент True — показываем как «с сайта».
    Если число с сайта явно не сходится с позициями + доставка, показываем расчёт бота.
    """
    exp = _cart_expected_grand_with_delivery(uid, user_data, lines)
    goods_sum, _ = _cart_totals(lines)
    if not uid:
        return int(exp), False
    site_gt = _cart_get_site_grand_total(uid, cur)
    inc = _cart_site_delivery_included(uid)
    if site_gt and int(site_gt) > 0:
        if not _site_grand_covers_goods(int(site_gt), int(goods_sum)):
            return int(exp), False
        if inc:
            if exp > 0 and int(site_gt) < exp - max(50, exp // 40):
                return int(exp), False
            return int(site_gt), True
        if exp > 0 and abs(int(site_gt) - int(exp)) > max(100, exp // 25):
            return int(exp), False
        return int(site_gt), True
    return int(exp), False


def _site_cart_line_currency(
    uid: int, lines: List[dict], delivery_cc: str
) -> str:
    """Валюта строк корзины — как в превью заказа с сайта (не site_cart_grand_currency)."""
    site_labels = {
        str(x.get("line_currency") or "").strip().upper()
        for x in lines
        if x.get("from_site")
        and str(x.get("line_currency") or "").strip().upper() in ("BYN", "RUB")
    }
    if site_labels and all(x.get("from_site") for x in lines) and len(site_labels) == 1:
        c = site_labels.pop()
        if c in ("BYN", "RUB"):
            return c
    bot_code, _, _, _ = _delivery_option_for_site_code(str(delivery_cc or "by"))
    return _goods_currency_for_delivery_country(bot_code)


def _site_cart_checkout_finance(
    uid: int,
    lines: List[dict],
    delivery_cc: str,
    user_data: Optional[dict] = None,
) -> dict:
    """Итог/валюта для превью и шага оплаты — одни правила."""
    bot_code, d_label, d_amt, d_cur = _delivery_option_for_site_code(
        str(delivery_cc or "by")
    )
    goods, _ = _cart_totals(lines)
    g_cur = _site_cart_line_currency(uid, lines, delivery_cc)
    site_gt_raw = _cart_get_site_grand_total(uid, g_cur)
    inc = _cart_site_delivery_included(uid)
    exp_line = int(goods) if inc else int(goods) + int(d_amt)
    trust_site = False
    if site_gt_raw and int(site_gt_raw) > 0:
        sg = int(site_gt_raw)
        if not _site_grand_covers_goods(sg, int(goods)):
            trust_site = False
        elif inc:
            trust_site = not (exp_line > 0 and sg < exp_line - max(50, exp_line // 40))
        else:
            trust_site = exp_line > 0 and abs(sg - exp_line) <= max(100, exp_line // 25)
    use_site = bool(site_gt_raw and trust_site)
    grand_show = int(site_gt_raw) if use_site else int(exp_line)
    return {
        "bot_code": bot_code,
        "g_cur": g_cur,
        "grand_show": int(grand_show),
        "from_site": bool(use_site),
        "goods": int(goods),
        "d_label": d_label,
        "d_amt": int(d_amt),
        "d_cur": d_cur,
        "inc": bool(inc),
    }


def _site_cart_payment_currency(uid: int, lines: List[dict], delivery_cc: str) -> str:
    """Валюта оплаты — как в превью заказа с сайта."""
    g_cur = _site_cart_line_currency(uid, lines, delivery_cc)
    b = USER_CART.get(int(uid))
    if isinstance(b, dict):
        sc = str(b.get("site_cart_grand_currency") or "").strip().upper()
        if sc in ("BYN", "RUB") and sc == g_cur:
            return sc
    return g_cur


def _loyalty_hint_total_currency(lh: dict) -> str:
    for key in ("currency", "totalCurrency", "grandTotalCurrency", "cartGrandTotalCurrency"):
        c = str(lh.get(key) or "").strip().upper()
        if c in ("BYN", "RUB"):
            return c
    return ""


_SITE_BONUS_SPENT_KEYS = (
    "bonusPointsSpent",
    "bonus_points_spent",
    "pointsSpent",
    "points_spent",
    "bonusesSpent",
    "bonuses_spent",
)
_SITE_BONUS_APPLIED_KEYS = (
    "bonusApplied",
    "bonus_applied",
    "bonusDiscount",
    "bonus_discount",
)


def _site_bonus_from_sources(
    uid: int, meta: Optional[dict] = None
) -> Tuple[int, int]:
    return 0, 0


def _site_final_total_from_sources(
    uid: int, meta: Optional[dict], pay_cur: str
) -> Optional[int]:
    """Итог с сайта (finalTotal / cartGrandTotalRub и т.д.)."""
    cur = str(pay_cur or "").strip().upper()
    sources: List[dict] = []
    if isinstance(meta, dict):
        sources.append(meta)
    b = USER_CART.get(int(uid))
    if isinstance(b, dict):
        sources.append(b)
    for src in sources:
        if not isinstance(src, dict):
            continue
        if cur == "RUB":
            for key in (
                "cartGrandTotalRub",
                "grandTotalRub",
                "finalTotalRub",
                "final_total_rub",
                "totalRub",
            ):
                if key not in src:
                    continue
                v = _coerce_card_price_int(src.get(key))
                if v > 0:
                    return int(v)
        if cur == "BYN":
            for key in (
                "cartGrandTotalByn",
                "grandTotalByn",
                "finalTotalByn",
                "final_total_byn",
                "totalByn",
            ):
                if key not in src:
                    continue
                v = _coerce_card_price_int(src.get(key))
                if v > 0:
                    return int(v)
        for key in ("finalTotal", "final_total", "cartGrandTotal", "grandTotal"):
            if key not in src:
                continue
            v = _coerce_card_price_int(src.get(key))
            if v <= 0:
                continue
            hint_cur = _loyalty_hint_total_currency(src) or _infer_site_grand_total_currency(
                src
            )
            if not hint_cur or hint_cur == cur:
                return int(v)
    return None


def _merge_site_bonus_into_meta(uid: int, meta: Optional[dict]) -> dict:
    return deepcopy(meta) if isinstance(meta, dict) else {}


def _parse_grand_total_from_preview_text(text: str) -> Optional[Tuple[int, str]]:
    """Итого из текста превью («💰 Итого: 7 200 RUB») — как видит пользователь."""
    m = re.search(
        r"💰\s*Итого:\s*([\d\s\u00a0]+)\s*(RUB|BYN)",
        str(text or ""),
        re.IGNORECASE,
    )
    if not m:
        return None
    raw_n = re.sub(r"[\s\u00a0]", "", m.group(1) or "")
    try:
        val = int(raw_n)
    except (TypeError, ValueError):
        return None
    cur = str(m.group(2) or "").strip().upper()
    if val > 0 and cur in ("BYN", "RUB"):
        return int(val), cur
    return None


def _order_payment_display(o: dict) -> Tuple[int, str]:
    """Сумма и валюта для шага оплаты — как сохранено в заказе при подтверждении."""
    pay_cur = str(o.get("payment_currency") or "").strip().upper()
    if pay_cur not in ("BYN", "RUB"):
        pay_cur = _payment_currency_for_order(o)
    try:
        stored = int(o.get("total") or 0)
    except (TypeError, ValueError):
        stored = 0
    if stored > 0 and o.get("payment_total_locked"):
        return int(stored), pay_cur
    if stored > 0 and pay_cur in ("BYN", "RUB"):
        return int(stored), pay_cur
    if stored > 0:
        return int(stored), pay_cur
    return int(_order_resolved_grand_total(o)), pay_cur


def _user_has_unpaid_order(uid: int) -> bool:
    return _find_latest_unpaid_order_id(int(uid)) is not None


def _resolve_site_confirm_pricing(
    uid: int,
    ud: dict,
    lines: List[dict],
    meta: Optional[dict],
) -> Tuple[int, str, dict]:
    """Итог и валюта для шага оплаты — те же правила, что в превью корзины."""
    meta_m = _merge_site_bonus_into_meta(uid, meta if isinstance(meta, dict) else {})
    cc = _cart_price_region_for_user(uid, ud)
    if cc not in DELIVERY_OPTIONS:
        cc = "by"
    dlabel, damount, dcur = DELIVERY_OPTIONS.get(cc, DELIVERY_OPTIONS["by"])
    drec = {
        "country": cc,
        "label": dlabel,
        "amount": int(damount),
        "currency": dcur,
    }
    try:
        locked = int(meta_m.get("preview_pay_total") or 0)
    except (TypeError, ValueError):
        locked = 0
    locked_cur = str(meta_m.get("preview_pay_currency") or "").strip().upper()
    if locked > 0 and locked_cur in ("BYN", "RUB"):
        return int(locked), locked_cur, drec
    fin = _site_cart_checkout_finance(uid, list(lines), cc, ud)
    pay_cur = str(fin["g_cur"])
    goods = int(fin["goods"])
    final_total = _site_final_total_from_sources(uid, meta_m, pay_cur)
    if final_total and int(final_total) > 0 and _site_grand_covers_goods(int(final_total), goods):
        total = int(final_total)
    else:
        total = int(fin["grand_show"])
        pts, disc = _site_bonus_from_sources(uid, meta_m)
        if disc <= 0 and pts > 0:
            disc = _bonus_discount_units(pts, pay_cur)
        if disc > 0:
            total = max(0, int(total) - int(disc))
    return int(total), pay_cur, drec


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
    "canceled": "❌",
    "cancelled": "❌",
}


def _order_status_label_ru(status: str) -> str:
    s = str(status or "new").strip()
    return ORDER_STATUS_RU.get(s, s)


def _norm_bot_order_status(status: str) -> str:
    """Единый ключ статуса заказа в ORDERS (cancelled → canceled)."""
    s = str(status or "new").strip().lower()
    aliases = {
        "в обработке": "new",
        "новый": "new",
        "новые": "new",
        "принят": "accepted",
        "в сборке": "accepted",
        "отправлен": "shipped",
        "отправленные": "shipped",
        "завершён": "done",
        "завершен": "done",
        "отменён": "canceled",
        "отменен": "canceled",
    }
    if s in aliases:
        return aliases[s]
    if s == "cancelled":
        return "canceled"
    return s


def _format_customer_order_status_notice(order_id: int, status_key: str) -> str:
    sk = str(status_key or "new").strip()
    if sk == "cancelled":
        sk = "canceled"
    if sk == "done":
        return "Ваш заказ успешно получен. Спасибо, что выбрали нас!"
    body = CUSTOMER_STATUS_BODY.get(sk, "")
    if body:
        return f"{body} (#{order_id})"
    return f"📦 #{order_id}"


def _payment_intro_text(
    total: int,
    currency: str = "BYN",
    *,
    loyalty_earn_estimate: Optional[int] = None,
) -> str:
    """После оформления: сумма и выбор способа оплаты (кнопки — карта / перевод / крипта)."""
    cur = str(currency or "BYN").strip().upper() or "BYN"
    body = (
        f"💰 Итого: {int(total)} {cur}\n\n"
        "Выберите способ оплаты:\n\n"
        "💳 Карта · 📱 Перевод · ₿ Крипта\n\n"
        f"{PAY_FLOW_STEPS}\n\n"
        "👇 Нажмите кнопку ниже"
    )
    return body


def _payment_step_message_text(
    total: int,
    currency: str = "BYN",
    *,
    loyalty_earn_estimate: Optional[int] = None,
) -> str:
    return (
        f"{ORDER_AUTO_ACK}\n\n"
        f"{_payment_intro_text(int(total), currency, loyalty_earn_estimate=loyalty_earn_estimate)}"
    )


async def _reply_payment_step(
    msg: Message,
    total: int,
    currency: str = "BYN",
    *,
    loyalty_earn_estimate: Optional[int] = None,
    order_id: Optional[int] = None,
) -> None:
    await msg.reply_text(
        _payment_step_message_text(
            int(total), currency, loyalty_earn_estimate=loyalty_earn_estimate
        ),
        reply_markup=_kb_payment_methods(order_id),
    )


def _payment_currency_for_order(o: dict) -> str:
    cur = str(o.get("payment_currency") or "").strip().upper()
    if cur in ("BYN", "RUB"):
        return cur
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
    cc = str(d.get("country") or "by").strip().lower()
    dc = str(d.get("currency") or "").strip().upper()
    if cc == "by" and dc == "BYN":
        return "BYN"
    return _goods_currency_for_delivery_country(cc)


def _refresh_unpaid_order_payment(
    o: dict,
    uid: int,
    ud: dict,
    *,
    preview_text: Optional[str] = None,
    lines: Optional[List[dict]] = None,
    meta: Optional[dict] = None,
) -> Tuple[int, str]:
    """Подтянуть итог/валюту оплаты из текущего превью (не из устаревшего ORDERS)."""
    if not isinstance(o, dict):
        return 0, "BYN"
    total = 0
    pay_cur = "BYN"
    parsed: Optional[Tuple[int, str]] = None
    for src in (preview_text, _get_site_pending_preview(uid)):
        parsed = _parse_grand_total_from_preview_text(str(src or ""))
        if parsed:
            break
    if parsed:
        total, pay_cur = int(parsed[0]), str(parsed[1])
    else:
        meta_m = _merge_site_bonus_into_meta(uid, meta if isinstance(meta, dict) else {})
        try:
            locked = int(meta_m.get("preview_pay_total") or 0)
        except (TypeError, ValueError):
            locked = 0
        locked_cur = str(meta_m.get("preview_pay_currency") or "").strip().upper()
        if locked > 0 and locked_cur in ("BYN", "RUB"):
            total, pay_cur = int(locked), locked_cur
        elif lines:
            total, pay_cur, drec = _resolve_site_confirm_pricing(
                uid, ud, list(lines), meta_m
            )
            o["delivery"] = deepcopy(drec)
            goods, _ = _cart_totals(list(lines))
            o["items"] = deepcopy(list(lines))
            o["total_goods"] = int(goods)
        if isinstance(meta_m, dict):
            try:
                b_ap = int(meta_m.get("bonus_applied") or 0)
            except (TypeError, ValueError):
                b_ap = 0
            if b_ap > 0:
                o["bonus_applied"] = int(b_ap)
            try:
                b_ps = int(meta_m.get("bonus_points_spent") or 0)
            except (TypeError, ValueError):
                b_ps = 0
            if b_ps > 0:
                o["bonus_points_spent"] = int(b_ps)
    if total > 0:
        o["total"] = int(total)
        o["payment_currency"] = str(pay_cur)
        o["payment_total_locked"] = True
    return int(o.get("total") or 0), str(o.get("payment_currency") or pay_cur)


def _restore_order_by_id(oid: int) -> Optional[dict]:
    """ORDERS после рестарта: найти заказ по id в USER_ORDERS."""
    try:
        oid_i = int(oid)
    except (TypeError, ValueError):
        return None
    o = ORDERS.get(oid_i)
    if isinstance(o, dict):
        return o
    _refresh_orders_state_from_redis()
    o = ORDERS.get(oid_i)
    if isinstance(o, dict):
        return o
    for uid_k, lst in USER_ORDERS.items():
        try:
            uid_i = int(uid_k)
        except (TypeError, ValueError):
            continue
        for rec in lst or []:
            if not isinstance(rec, dict):
                continue
            try:
                rid = int(str(rec.get("id") or "0"))
            except (TypeError, ValueError):
                continue
            if rid == oid_i:
                return _ensure_order_in_orders(oid_i, uid_i)
    return None


_RE_ADMIN_CARD_ITEM = re.compile(
    r"•\s*(.+?)\s*—\s*(\d+)\s*шт\.\s*×\s*(\d+)\s*(BYN|RUB)\s*=\s*(\d+)\s*(BYN|RUB)",
    re.IGNORECASE,
)
_RE_ADMIN_CARD_DELIVERY = re.compile(r"🚚\s*Доставка:\s*(.+)", re.IGNORECASE)


def _parse_items_from_admin_card(text: str) -> List[dict]:
    items: List[dict] = []
    for m in _RE_ADMIN_CARD_ITEM.finditer(str(text or "")):
        try:
            items.append(
                {
                    "name": str(m.group(1) or "").strip(),
                    "qty": int(m.group(2)),
                    "price": int(m.group(3)),
                }
            )
        except (TypeError, ValueError):
            continue
    return items


def _parse_delivery_from_admin_card(text: str) -> dict:
    m = _RE_ADMIN_CARD_DELIVERY.search(str(text or ""))
    if not m:
        return {}
    line = str(m.group(1) or "").strip()
    amount = 0
    currency = "BYN"
    amt_m = re.search(r"—\s*(\d+)\s*(BYN|RUB)", line, re.IGNORECASE)
    if amt_m:
        try:
            amount = int(amt_m.group(1))
        except (TypeError, ValueError):
            amount = 0
        currency = str(amt_m.group(2) or "BYN").upper()
    country = "by"
    low = line.lower()
    if "росс" in low or "🇷🇺" in line:
        country = "ru"
    elif "укра" in low or "🇺🇦" in line:
        country = "ua"
    elif "бел" in low or "🇧🇾" in line:
        country = "by"
    return {"label": line, "amount": amount, "currency": currency, "country": country}


def _enrich_order_from_admin_card_text(o: dict, oid: int, text: str) -> dict:
    """Дополнить заказ составом и доставкой из текста карточки админа."""
    items = _parse_items_from_admin_card(text)
    if items:
        o["items"] = items
    delivery = _parse_delivery_from_admin_card(text)
    if delivery:
        o["delivery"] = delivery
    pay_m = _RE_ADMIN_CARD_PAY_TOTAL.search(str(text or ""))
    if pay_m:
        try:
            o["total"] = int(pay_m.group(1))
        except (TypeError, ValueError):
            pass
        o["payment_currency"] = str(pay_m.group(2) or "BYN").upper()
    st_m = _RE_ADMIN_CARD_STATUS.search(str(text or ""))
    if st_m:
        o["status"] = _admin_status_key_from_ru(st_m.group(1))
    ORDERS[int(oid)] = o
    return o


_RE_ADMIN_CARD_ORDER_ID = re.compile(
    r"(?:📦\s*Заказ\s*#|Чек\s+оплаты\s*·\s*Заказ\s*#|Заказ\s*#)(\d+)",
    re.IGNORECASE,
)
_RE_ADMIN_CARD_USER_ID = re.compile(r"🆔\s*id:\s*(\d+)", re.IGNORECASE)
_RE_ADMIN_CARD_PAY_TOTAL = re.compile(
    r"💰\s*К\s*оплате:\s*(\d+)\s*(BYN|RUB)", re.IGNORECASE
)
_RE_ADMIN_CARD_STATUS = re.compile(r"📊\s*Статус:\s*(.+)", re.IGNORECASE)
_RE_ADMIN_CARD_USERNAME = re.compile(r"👤\s*Пользователь:\s*(@?\S+)", re.IGNORECASE)


def _message_looks_like_admin_order_card(text: str) -> bool:
    t = str(text or "")
    return bool(_RE_ADMIN_CARD_ORDER_ID.search(t) and _RE_ADMIN_CARD_USER_ID.search(t))


def _admin_status_key_from_ru(label: str) -> str:
    s = str(label or "").strip().lower()
    for ru, key in (
        ("новый", "new"),
        ("принят", "accepted"),
        ("отправлен", "shipped"),
        ("заверш", "done"),
        ("отмен", "canceled"),
    ):
        if ru in s:
            return key
    return "new"


def _rebuild_order_from_admin_card_text(oid: int, text: str) -> Optional[dict]:
    """Восстановить заказ из текста карточки админа после рестарта бота."""
    t = str(text or "")
    if not _message_looks_like_admin_order_card(t):
        return None
    uid_m = _RE_ADMIN_CARD_USER_ID.search(t)
    if not uid_m:
        return None
    try:
        uid_i = int(uid_m.group(1))
    except (TypeError, ValueError):
        return None
    uname = ""
    un_m = _RE_ADMIN_CARD_USERNAME.search(t)
    if un_m:
        uname = str(un_m.group(1) or "").strip().lstrip("@")
    total = 0
    pay_cur = "BYN"
    pay_m = _RE_ADMIN_CARD_PAY_TOTAL.search(t)
    if pay_m:
        try:
            total = int(pay_m.group(1))
        except (TypeError, ValueError):
            pass
        pay_cur = str(pay_m.group(2) or "BYN").upper()
    st = "new"
    st_m = _RE_ADMIN_CARD_STATUS.search(t)
    if st_m:
        st = _admin_status_key_from_ru(st_m.group(1))
    is_proof = "📸" in t and "Чек оплаты" in t
    rebuilt: dict = {
        "user_id": uid_i,
        "username": uname,
        "items": _parse_items_from_admin_card(t),
        "total": total,
        "payment_currency": pay_cur,
        "delivery": _parse_delivery_from_admin_card(t),
        "status": st,
        "created_at": time.time(),
        "paid": False,
        "payment_proof_submitted": is_proof,
        "clear_cart_on_paid": True,
    }
    return _enrich_order_from_admin_card_text(rebuilt, int(oid), t)


def _restore_order_for_admin(q: CallbackQuery, oid: int) -> Optional[dict]:
    """ORDERS / USER_ORDERS или текст карточки в чате админа."""
    _refresh_orders_state_from_redis()
    o = _restore_order_by_id(oid)
    msg = q.message
    text = (msg.text or msg.caption or "").strip() if msg else ""
    if o and text and _message_looks_like_admin_order_card(text):
        if not list(o.get("items") or []):
            o = _enrich_order_from_admin_card_text(o, oid, text)
    if o:
        return o
    if not msg:
        return None
    o = _rebuild_order_from_admin_card_text(oid, text)
    if not o:
        return None
    o["admin_chat_id"] = int(msg.chat_id)
    o["admin_message_id"] = int(msg.message_id)
    if "📸" in text and "Чек оплаты" in text:
        o["payment_proof_submitted"] = True
        o["payment_proof_admin_chat_id"] = int(msg.chat_id)
        o["payment_proof_admin_message_id"] = int(msg.message_id)
    save_state()
    return o


async def _reply_admin_order_stale(q: CallbackQuery, oid: int) -> None:
    try:
        await q.answer("Заказ не найден", show_alert=True)
    except Exception:
        pass
    if q.message:
        await q.message.reply_text(MSG_ADMIN_ORDER_STALE.format(oid=int(oid)))


def _ensure_order_in_orders(oid: int, uid: int) -> Optional[dict]:
    """ORDERS после рестарта: восстановить запись из USER_ORDERS по id."""
    try:
        oid_i = int(oid)
        uid_i = int(uid)
    except (TypeError, ValueError):
        return None
    o = ORDERS.get(oid_i)
    if isinstance(o, dict):
        stored_uid = int(o.get("user_id") or 0)
        if stored_uid in (0, uid_i):
            if stored_uid != uid_i:
                o["user_id"] = uid_i
            return o
    for rec in USER_ORDERS.get(uid_i) or []:
        if not isinstance(rec, dict):
            continue
        try:
            rid = int(str(rec.get("id") or "0"))
        except (TypeError, ValueError):
            continue
        if rid != oid_i:
            continue
        rebuilt: dict = {
            "user_id": uid_i,
            "items": deepcopy(list(rec.get("items") or [])),
            "total": int(rec.get("total") or 0),
            "delivery": deepcopy(rec.get("delivery") if isinstance(rec.get("delivery"), dict) else {}),
            "status": "new",
            "created_at": time.time(),
            "paid": False,
            "payment_proof_submitted": False,
            "clear_cart_on_paid": True,
        }
        if rec.get("total_goods") is not None:
            try:
                rebuilt["total_goods"] = int(rec.get("total_goods"))
            except (TypeError, ValueError):
                pass
        pc = str(rec.get("payment_currency") or "").strip().upper()
        if pc in ("BYN", "RUB"):
            rebuilt["payment_currency"] = pc
        if rec.get("bonus_applied") is not None:
            try:
                rebuilt["bonus_applied"] = int(rec.get("bonus_applied"))
            except (TypeError, ValueError):
                pass
        if rec.get("bonus_points_spent") is not None:
            try:
                rebuilt["bonus_points_spent"] = int(rec.get("bonus_points_spent"))
            except (TypeError, ValueError):
                pass
        if rec.get("loyalty_earn_estimate") is not None:
            try:
                rebuilt["loyalty_earn_estimate"] = int(rec.get("loyalty_earn_estimate"))
            except (TypeError, ValueError):
                pass
        ORDERS[oid_i] = rebuilt
        return rebuilt
    return o if isinstance(o, dict) else None


async def _resend_active_payment_step(
    q: CallbackQuery,
    uid: int,
    ud: dict,
    oid: int,
    o: dict,
    *,
    preview_text: Optional[str] = None,
    lines: Optional[List[dict]] = None,
    meta: Optional[dict] = None,
) -> None:
    """Свежее сообщение с кнопками оплаты (в т.ч. «Отменить») для активного заказа."""
    if not q.message:
        return
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    _refresh_unpaid_order_payment(
        o, uid, ud, preview_text=preview_text, lines=lines, meta=meta
    )
    tot, pay_cur = _order_payment_display(o)
    lo_est = o.get("loyalty_earn_estimate")
    body = (
        f"💳 Оплата по заказу #{int(oid)}\n\n"
        f"{_payment_intro_text(int(tot), pay_cur, loyalty_earn_estimate=lo_est)}"
    )
    await q.message.reply_text(body, reply_markup=_kb_payment_methods(int(oid)))
    _set_awaiting_payment_order_id(uid, ud, int(oid))


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


def _format_order_items_for_admin(lines: List[dict], currency: str = "BYN") -> str:
    cur = str(currency or "BYN").strip().upper() or "BYN"
    rows: List[str] = []
    for x in lines:
        name_raw = str(x.get("name") or "—")
        name = name_raw[:200]
        if len(name_raw) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        rows.append(f"• {name} — {q} шт. × {p} {cur} = {sub} {cur}")
    return "\n".join(rows) if rows else "—"


def _format_admin_order_detail_text(order_id: int, o: dict) -> str:
    """Карточка заказа для админа (уведомление, open_order, правка сообщения)."""
    uid = int(o.get("user_id") or 0)
    un = o.get("username")
    un_s = str(un).strip().lstrip("@") if un else ""
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else None
    line_cur = _order_line_currency_from_delivery(d)
    items_block = _format_order_items_for_admin(list(o.get("items") or []), line_cur)
    tot, pay_cur = _order_payment_display(o)
    if int(tot) <= 0:
        tot = _order_resolved_grand_total(o)
        pay_cur = line_cur
    st = str(o.get("status") or "new")
    st_ru = _order_status_label_ru(st)
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
            f"💰 К оплате: {tot} {pay_cur}",
            "",
            f"📊 Статус: {st_ru}",
            "",
            f"🕐 Создан: {tss}",
        ]
    )
    dd = str(o.get("delivery_details") or "").strip()
    if not dd:
        dd = _postpaid_merged_details(o)
    if dd:
        parts.extend(["", "📍 Адрес / ФИО / телефон:", dd[:1200]])
    body = "\n".join(parts)
    if len(body) > 4090:
        body = body[:4086] + "…"
    return body


def _format_payment_proof_caption(order_id: int, o: dict, uid: int) -> str:
    """Подпись к фото чека для админа (лимит Telegram caption 1024)."""
    body = _format_admin_order_detail_text(order_id, o)
    pm = str(o.get("payment_pending_method") or "").strip().lower()
    pm_ru = {"card": "💳 Карта", "transfer": "📱 Перевод", "crypto": "₿ Крипта"}.get(
        pm, "—"
    )
    un = str(o.get("username") or "").strip().lstrip("@")
    who = f"@{un}" if un else f"id {uid}"
    head = (
        f"📸 Чек оплаты · Заказ #{order_id}\n"
        f"👤 Клиент: {who} · 🆔 {uid} · способ: {pm_ru}\n\n"
    )
    cap = head + body
    if len(cap) <= 1024:
        return cap
    reserve = len(head) + 24
    max_body = max(80, 1024 - reserve)
    trimmed = body[: max_body - 1].rstrip() + "…"
    cap = head + trimmed
    if len(cap) > 1024:
        cap = cap[:1021] + "…"
    return cap


def _admin_order_notify_targets() -> List[object]:
    """Куда слать заказы и чеки — из env (ORDER_NOTIFY, ADMIN_ID, ADMIN_CHAT)."""
    out: List[object] = []
    seen: set = set()
    for raw in (ORDER_NOTIFY_TARGET, ADMIN_ID, _resolve_admin_chat_id()):
        if raw is None or raw == "" or raw == 0:
            continue
        if isinstance(raw, int):
            key = ("i", int(raw))
        else:
            key = ("s", str(raw).strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return out


async def _send_admin_order_panel(
    context: ContextTypes.DEFAULT_TYPE,
    order_id: int,
    o: dict,
    *,
    force_new: bool = False,
    reply_to_message_id: Optional[int] = None,
) -> None:
    """Карточка заказа админу с кнопками Принять / Отправлен / …"""
    log = logging.getLogger(__name__)
    target_ids = _admin_target_chat_ids()
    if not force_new:
        cid = o.get("admin_chat_id")
        mid = o.get("admin_message_id")
        if cid is not None and mid is not None:
            try:
                if int(cid) in target_ids:
                    await _refresh_admin_order_message(context, order_id)
                    return
            except (TypeError, ValueError):
                pass
    o.pop("admin_chat_id", None)
    o.pop("admin_message_id", None)
    text = _format_admin_order_detail_text(order_id, o)
    kb = _kb_order_admin_actions(order_id, str(o.get("status") or "new"))
    m = None
    targets = _admin_order_notify_targets() or ([int(ADMIN_ID)] if ADMIN_ID else [])
    for tgt in targets:
        try:
            kw: dict = {
                "chat_id": tgt,
                "text": text,
                "reply_markup": kb,
                "disable_web_page_preview": True,
            }
            if reply_to_message_id is not None:
                kw["reply_to_message_id"] = int(reply_to_message_id)
            sent = await context.bot.send_message(**kw)
            if m is None:
                m = sent
            log.info("admin order panel → target=%s order_id=%s", tgt, order_id)
        except Exception:
            log.exception(
                "admin order panel → target=%s order_id=%s", tgt, order_id
            )
    if m is None:
        return
    o["admin_chat_id"] = int(m.chat_id)
    o["admin_message_id"] = int(m.message_id)
    save_state()


async def _send_deferred_admin_order_panel(
    context: ContextTypes.DEFAULT_TYPE, order_id: int, o: dict
) -> None:
    """Карточка заказа админу после подтверждения оплаты."""
    await _send_admin_order_panel(context, order_id, o, force_new=True)


def _kb_order_admin_actions(order_id: int, _status: str) -> Optional[InlineKeyboardMarkup]:
    """Кнопки заказа для админа: статус можно менять и удалить карточку из чата в любой момент."""
    rep = InlineKeyboardButton("💬 Ответить", callback_data=f"oam:rep:{order_id}")
    acc = InlineKeyboardButton("✅ Принять", callback_data=f"accept_{order_id}")
    shp = InlineKeyboardButton("🚚 Отправлен", callback_data=f"sent_{order_id}")
    can = InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{order_id}")
    done_btn = InlineKeyboardButton("🏁 Завершён", callback_data=f"done_{order_id}")
    del_btn = InlineKeyboardButton("🗑 Удалить из чата", callback_data=f"delmsg_{order_id}")
    return InlineKeyboardMarkup(
        [
            [rep, acc],
            [shp, done_btn],
            [can, del_btn],
        ]
    )


async def _notify_admin_new_order(
    context: ContextTypes.DEFAULT_TYPE,
    user,
    lines: List[dict],
    total: int,
    delivery: Optional[dict] = None,
    *,
    loyalty_hint_dict: Optional[dict] = None,
    bonus_applied: int = 0,
    bonus_points_spent: int = 0,
) -> Optional[int]:
    """Создать заказ в ORDERS; карточка админу — после подтверждения оплаты по фото чека."""
    global ORDER_COUNTER
    order_id = int(ORDER_COUNTER)
    uid = int(user.id) if user else 0
    uname = (getattr(user, "username", None) or "").strip()
    drec = deepcopy(delivery) if delivery else {}
    now = time.time()
    ORDER_COUNTER = order_id + 1
    rec: dict = {
        "user_id": uid,
        "username": uname,
        "items": deepcopy(list(lines)),
        "total": int(total),
        "delivery": drec,
        "status": "new",
        "created_at": now,
        "admin_chat_id": None,
        "admin_message_id": None,
        "paid": False,
        "payment_proof_submitted": False,
        "clear_cart_on_paid": False,
    }
    earn_est = _loyalty_compute_earn_estimate(
        int(total), loyalty_hint_dict, cart_lines=list(lines)
    )
    if earn_est is not None and int(earn_est) > 0:
        rec["loyalty_earn_estimate"] = int(earn_est)
    if int(bonus_applied) > 0:
        rec["bonus_applied"] = int(bonus_applied)
    if int(bonus_points_spent) > 0:
        rec["bonus_points_spent"] = int(bonus_points_spent)
    ORDERS[order_id] = rec
    if uid:
        users_touch(uid, activity_only=True)
    save_state()
    return order_id


_RE_PAY_METHOD_CB = re.compile(r"^pay_(card|transfer|crypto)(?::(?P<oid>\d+))?$")
_RE_PAY_CANCEL_CB = re.compile(r"^pay_cancel(?::(?P<oid>\d+))?$")
_RE_PAID_CB = re.compile(r"^paid(?::(?P<oid>\d+))?$")


def _pay_cb(base: str, order_id: Optional[int] = None) -> str:
    if order_id is not None:
        try:
            return f"{base}:{int(order_id)}"
        except (TypeError, ValueError):
            pass
    return base


def _oid_from_pay_callback(data: str) -> Optional[int]:
    d = (data or "").strip()
    for pat in (_RE_PAY_METHOD_CB, _RE_PAY_CANCEL_CB, _RE_PAID_CB):
        m = pat.match(d)
        if not m:
            continue
        raw = (m.groupdict().get("oid") or "").strip()
        if not raw:
            continue
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None


def _parse_order_id_from_payment_message(text: str) -> Optional[int]:
    m = re.search(r"Оплата по заказу\s*#(\d+)", str(text or ""), re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def _message_looks_like_order_payment_step(text: str) -> bool:
    t = str(text or "")
    return bool(t.strip()) and (
        "Оплата по заказу" in t or _message_looks_like_payment_step(t)
    )


def _stub_order_for_payment_step(oid: int, uid: int, message_text: str) -> dict:
    """Минимальный заказ из текста шага оплаты — чтобы кнопки работали без ORDERS в RAM."""
    total = 0
    pay_cur = "BYN"
    parsed = _parse_grand_total_from_preview_text(message_text or "")
    if parsed:
        total, pay_cur = int(parsed[0]), str(parsed[1])
    return {
        "user_id": int(uid),
        "items": [],
        "total": int(total),
        "delivery": {},
        "status": "new",
        "created_at": time.time(),
        "paid": False,
        "payment_proof_submitted": False,
        "clear_cart_on_paid": True,
        "payment_currency": str(pay_cur),
        "payment_total_locked": bool(int(total) > 0),
    }


def _image_file_id_from_message(msg: Optional[Message]) -> Optional[str]:
    """file_id фото или картинки, присланной как документ."""
    if not msg:
        return None
    if msg.photo:
        return str(msg.photo[-1].file_id)
    if msg.animation:
        return str(msg.animation.file_id)
    if msg.video:
        thumb = msg.video.thumbnail
        if thumb:
            return str(thumb.file_id)
        return str(msg.video.file_id)
    doc = msg.document
    if not doc:
        return None
    mime = str(doc.mime_type or "").lower()
    fname = str(doc.file_name or "").lower()
    if mime.startswith("image/"):
        return str(doc.file_id)
    if any(
        fname.endswith(ext)
        for ext in (".png", ".jpg", ".jpeg", ".webp", ".heic", ".heif", ".gif", ".bmp")
    ):
        return str(doc.file_id)
    if not mime or mime == "application/octet-stream":
        return str(doc.file_id)
    return None


def _resolve_proof_order_record(
    uid: int, oid: int, ud: Optional[dict] = None
) -> Optional[dict]:
    """Заказ для чека: ORDERS → USER_ORDERS → минимальная запись."""
    try:
        oid_i = int(oid)
        uid_i = int(uid)
    except (TypeError, ValueError):
        return None
    existing = ORDERS.get(oid_i)
    if isinstance(existing, dict):
        owner = int(existing.get("user_id") or 0)
        if owner not in (0, uid_i):
            return None
    o = _ensure_order_in_orders(oid_i, uid_i)
    if isinstance(o, dict) and int(o.get("user_id") or 0) == uid_i:
        if isinstance(ud, dict):
            pm = ud.get("payment_pending_method")
            if pm and not o.get("payment_pending_method"):
                o["payment_pending_method"] = pm
        return o
    if isinstance(existing, dict) and int(existing.get("user_id") or 0) not in (0, uid_i):
        return None
    stub: dict = {
        "user_id": uid_i,
        "items": [],
        "total": 0,
        "delivery": {},
        "status": "new",
        "created_at": time.time(),
        "paid": False,
        "payment_proof_submitted": False,
        "clear_cart_on_paid": True,
    }
    if isinstance(ud, dict):
        pm = ud.get("payment_pending_method")
        if pm:
            stub["payment_pending_method"] = pm
    ORDERS[oid_i] = stub
    return stub


def _resolve_proof_order_id_aggressive(
    uid: int, ud: Optional[dict] = None
) -> Optional[int]:
    """ID заказа для чека: сессия → Redis user_states → последний неоплаченный."""
    _refresh_orders_state_from_redis()
    ud_d = ud if isinstance(ud, dict) else {}
    oid = _resolve_proof_order_id_for_photo(uid, ud_d)
    if oid is not None:
        return int(oid)
    for raw in (
        _user_state_get(uid, "awaiting_proof"),
        ud_d.get("awaiting_proof"),
        _user_state_get(uid, "awaiting_payment_order_id"),
        ud_d.get("awaiting_payment_order_id"),
    ):
        if raw is None:
            continue
        try:
            oid_i = int(raw)
        except (TypeError, ValueError):
            continue
        if _proof_order_owned_by_user(uid, oid_i):
            return oid_i
    latest = _find_latest_unpaid_order_id(uid)
    if latest is not None:
        return int(latest)
    return None


def _resolve_proof_order_id_for_photo(
    uid: int, ud: Optional[dict] = None
) -> Optional[int]:
    """Заказ для чека: сессия клиента → его неоплаченный заказ (не чужой)."""
    ud_d = ud if isinstance(ud, dict) else {}
    seen: set = set()
    for raw in (
        _user_state_get(uid, "awaiting_proof"),
        ud_d.get("awaiting_proof"),
        _user_state_get(uid, "awaiting_payment_order_id"),
        ud_d.get("awaiting_payment_order_id"),
    ):
        if raw is None:
            continue
        try:
            oid_i = int(raw)
        except (TypeError, ValueError):
            continue
        if oid_i in seen:
            continue
        seen.add(oid_i)
        if _proof_order_owned_by_user(uid, oid_i):
            return oid_i
    latest = _find_latest_unpaid_order_id(uid)
    if latest is not None and _proof_order_owned_by_user(uid, int(latest)):
        return int(latest)
    return None


def _ensure_awaiting_proof_session(uid: int, ud: Optional[dict] = None) -> Optional[int]:
    """awaiting_proof: user_states → активный неоплаченный заказ."""
    ud_d = ud if isinstance(ud, dict) else {}
    for raw in (_user_state_get(uid, "awaiting_proof"), ud_d.get("awaiting_proof")):
        if raw is None:
            continue
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    candidates: List[int] = []
    for raw in (
        _user_state_get(uid, "awaiting_payment_order_id"),
        ud_d.get("awaiting_payment_order_id"),
    ):
        if raw is None:
            continue
        try:
            candidates.append(int(raw))
        except (TypeError, ValueError):
            continue
    latest = _find_latest_unpaid_order_id(uid)
    if latest is not None:
        candidates.append(int(latest))
    seen: set = set()
    for oid_i in reversed(candidates):
        if oid_i in seen:
            continue
        seen.add(oid_i)
        if not _proof_order_owned_by_user(uid, oid_i):
            continue
        _user_state_set(uid, "awaiting_proof", oid_i, persist=False)
        return oid_i
    return None


def _set_awaiting_proof_session(uid: int, ud: dict, oid: int) -> None:
    oid_i = int(oid)
    if isinstance(ud, dict):
        ud["awaiting_proof"] = oid_i
        ud["awaiting_payment_order_id"] = oid_i
    b = _user_state_bucket(uid)
    b["proof_deadline"] = int(time.time()) + 7200
    _user_state_set(uid, "awaiting_proof", oid_i)
    _user_state_set(uid, "awaiting_payment_order_id", oid_i)
    _bind_payment_order_session(uid, ud, oid_i)
    save_state()


def _is_reply_to_proof_request(msg: Optional[Message]) -> bool:
    if not msg or not msg.reply_to_message:
        return False
    t = (msg.reply_to_message.text or msg.reply_to_message.caption or "").lower()
    return any(
        x in t
        for x in (
            "отправьте скрин",
            "скрин оплаты",
            "я оплатил",
            "оплата картой",
            "оплата по заказу",
            "перевод на номер",
            "крипто",
        )
    )


def _user_wants_proof_upload(uid: int, ud: dict, msg: Optional[Message]) -> bool:
    def _peek() -> bool:
        if _user_state_get(uid, "awaiting_proof") is not None:
            return True
        if _user_state_get(uid, "awaiting_payment_order_id") is not None:
            return True
        if isinstance(ud, dict) and (
            ud.get("awaiting_proof") or ud.get("awaiting_payment_order_id")
        ):
            return True
        dl = _user_state_get(uid, "proof_deadline")
        if dl is not None and int(dl) > int(time.time()):
            return True
        if _find_latest_unpaid_order_id(uid) is not None:
            return True
        if _is_reply_to_proof_request(msg):
            return True
        return False

    if _peek():
        return True
    _refresh_orders_state_from_redis()
    if _peek():
        return True
    latest = _find_latest_unpaid_order_id(uid)
    if latest is not None:
        o = ORDERS.get(int(latest))
        if isinstance(o, dict) and str(o.get("payment_pending_method") or "").strip():
            return True
    return False


async def _send_proof_received_ack(msg: Message) -> None:
    try:
        await msg.reply_text(
            "⏳ Получили скрин, передаём администратору…",
            reply_markup=REPLY_KB,
        )
    except Exception:
        logging.getLogger(__name__).exception("proof_ack reply failed")


def _bind_payment_order_session(uid: int, ud: dict, oid: int) -> None:
    oid_i = int(oid)
    if isinstance(ud, dict):
        ud["awaiting_payment_order_id"] = oid_i
    if uid:
        b = _user_state_bucket(int(uid))
        b["awaiting_payment_order_id"] = oid_i


def _resolve_payment_order_id(
    uid: int,
    ud: dict,
    *,
    callback_data: Optional[str] = None,
    message_text: Optional[str] = None,
) -> Optional[int]:
    """ID заказа на шаге оплаты: callback_data → текст сообщения → сессия."""
    for oid_raw in (
        _oid_from_pay_callback(callback_data or ""),
        _parse_order_id_from_payment_message(message_text or ""),
    ):
        if oid_raw is None:
            continue
        o = _ensure_order_in_orders(int(oid_raw), int(uid))
        if not isinstance(o, dict) and _message_looks_like_order_payment_step(
            message_text or ""
        ):
            o = _stub_order_for_payment_step(int(oid_raw), int(uid), message_text or "")
            ORDERS[int(oid_raw)] = o
        if (
            isinstance(o, dict)
            and int(o.get("user_id") or 0) == int(uid)
            and not o.get("paid")
        ):
            _bind_payment_order_session(int(uid), ud, int(oid_raw))
            return int(oid_raw)
    return _resolve_awaiting_payment_order_id(uid, ud)


def _kb_payment_methods(order_id: Optional[int] = None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "💳 Карта", callback_data=_pay_cb("pay_card", order_id)
                ),
                InlineKeyboardButton(
                    "📱 Перевод", callback_data=_pay_cb("pay_transfer", order_id)
                ),
            ],
            [
                InlineKeyboardButton(
                    "₿ Крипта", callback_data=_pay_cb("pay_crypto", order_id)
                )
            ],
            [
                InlineKeyboardButton(
                    "❌ Отменить оплату", callback_data=_pay_cb("pay_cancel", order_id)
                )
            ],
        ]
    )


def _payment_total_label(o: dict) -> str:
    tot, cur = _order_payment_display(o)
    if tot > 0:
        return f"{int(tot)} {cur}"
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
    cc = str(d.get("country") or "").strip().lower()
    dcur = str(d.get("currency") or "BYN")
    g_cur = _goods_currency_for_delivery_country(cc)
    try:
        goods = int(o.get("total_goods") if o.get("total_goods") is not None else 0)
    except (TypeError, ValueError):
        goods = 0
    if goods <= 0 and o.get("items"):
        g2, _ = _cart_totals(list(o.get("items") or []))
        goods = int(g2)
    try:
        d_amt = int(d.get("amount") or 0)
    except (TypeError, ValueError):
        d_amt = 0
    if cc == "by" and dcur == "BYN":
        return f"{goods + d_amt} BYN"
    if d_amt > 0:
        return f"{goods + d_amt} {g_cur}"
    return f"{goods} {g_cur}"


def _kb_paid_confirm(
    total_label: str, order_id: Optional[int] = None
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"✅ Оплатить + {total_label}",
                    callback_data=_pay_cb("paid", order_id),
                )
            ]
        ]
    )


def _format_payment_receipt_text(order_id: int, o: dict) -> str:
    """Текст чека клиенту после оплаты."""
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else None
    line_cur = _order_line_currency_from_delivery(d)
    items_block = _format_order_items_for_admin(list(o.get("items") or []), line_cur)
    dline = _format_delivery_block(d)
    if not dline and d:
        cc = str(d.get("country") or "").strip().lower()
        opt = DELIVERY_OPTIONS.get(cc)
        dline = f"🚚 {opt[0]}" if opt else ""
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
    try:
        b_ap = int(o.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        b_ap = 0
    if b_ap > 0:
        lines.append("")
    lines.append("")
    lines.append(f"💰 Сумма: {_order_resolved_grand_total(o)} {line_cur}")
    lines.extend(
        [
            "",
            "📊 Статус: Оплачен",
            "",
            "🙏 Спасибо за покупку!",
        ]
    )
    lines.append("")
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


def _postpaid_shipping_prompt_for_order(o: dict) -> str:
    """Текст запроса реквизитов доставки после скрина оплаты — по стране из заказа."""
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
    cc = str(d.get("country") or "").strip().lower()
    if cc == "by":
        return MSG_POSTPAID_SHIPPING_BY
    if cc == "ru":
        return MSG_POSTPAID_SHIPPING_RU
    return MSG_POSTPAID_SHIPPING_INTL


def _postpaid_append_detail(o: dict, text: str) -> None:
    chunk = str(text or "").strip()
    if not chunk:
        return
    parts = o.setdefault("delivery_details_parts", [])
    if not isinstance(parts, list):
        parts = []
        o["delivery_details_parts"] = parts
    parts.append(chunk)


def _postpaid_merged_details(o: dict) -> str:
    parts = o.get("delivery_details_parts")
    if isinstance(parts, list) and parts:
        return "\n\n".join(
            str(x).strip() for x in parts if str(x).strip()
        ).strip()
    return str(o.get("delivery_details") or "").strip()


def _kb_postpaid_submit(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Отправить заказ",
                    callback_data=f"ppdone:{int(order_id)}",
                )
            ]
        ]
    )


def _kb_admin_postpaid_reply(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("💬 Ответить", callback_data=f"oam:rep:{order_id}")]]
    )


def _clear_postpaid_thread_if_matches(uid: int, order_id: int) -> None:
    tid = _user_state_get(uid, "postpaid_thread_oid")
    if tid is not None and int(tid) == int(order_id):
        _user_state_pop(uid, "postpaid_thread_oid")


async def _forward_postpaid_client_payload_to_admin(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    uid: int,
    oid: int,
    body_text: Optional[str],
    msg: Message,
    photo_file_id: Optional[str] = None,
) -> bool:
    """Переслать текст или фото клиента в тред карточки заказа (reply к admin_message)."""
    log = logging.getLogger(__name__)
    o = ORDERS.get(oid)
    if not o or int(o.get("user_id") or 0) != int(uid):
        _user_state_pop(uid, "postpaid_thread_oid")
        try:
            await msg.reply_text(MSG_POSTPAID_THREAD_STALE)
        except Exception:
            pass
        return True
    st = _norm_bot_order_status(str(o.get("status") or "new"))
    if st in ("done", "canceled"):
        _user_state_pop(uid, "postpaid_thread_oid")
        try:
            await msg.reply_text(MSG_POSTPAID_THREAD_CLOSED)
        except Exception:
            pass
        return True
    admin_cid = o.get("admin_chat_id")
    admin_mid = o.get("admin_message_id")
    tgt = admin_cid if admin_cid is not None else ORDER_NOTIFY_TARGET
    kb = _kb_admin_postpaid_reply(oid)
    uname = (msg.from_user.username or "").strip() if msg.from_user else ""
    head = f"📍 Сообщение по заказу #{oid}\n👤 id {uid}"
    if uname:
        head += f" @{uname}"
    reply_to = int(admin_mid) if admin_cid is not None and admin_mid is not None else None

    async def _send_photo(use_reply: bool) -> None:
        cap = head
        t = (body_text or "").strip()
        if t:
            cap = f"{head}\n\n{t}"
        cap = cap[:1024]
        kw: dict = {
            "chat_id": tgt,
            "photo": photo_file_id,
            "caption": cap,
            "reply_markup": kb,
        }
        if use_reply and reply_to is not None:
            kw["reply_to_message_id"] = reply_to
        await context.bot.send_photo(**kw)

    async def _send_txt(use_reply: bool) -> None:
        body = head + "\n\n" + (body_text or "").strip()
        body = body[:4096]
        kw: dict = {
            "chat_id": tgt,
            "text": body,
            "reply_markup": kb,
            "disable_web_page_preview": True,
        }
        if use_reply and reply_to is not None:
            kw["reply_to_message_id"] = reply_to
        await context.bot.send_message(**kw)

    try:
        if photo_file_id:
            await _send_photo(True)
        else:
            await _send_txt(True)
        return True
    except Exception as e:
        log.info("postpaid forward (with reply) failed: %s", e)
        if reply_to is None:
            return False
        try:
            if photo_file_id:
                await _send_photo(False)
            else:
                await _send_txt(False)
            return True
        except Exception:
            log.exception("postpaid forward order_id=%s", oid)
            return False


async def _collect_postpaid_client_message(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    uid: int,
    oid: int,
    body_text: Optional[str],
    msg: Message,
    photo_file_id: Optional[str] = None,
) -> bool:
    """Собрать адрес/ФИО/телефон до отправки полного заказа админу."""
    o = ORDERS.get(int(oid))
    if not o or int(o.get("user_id") or 0) != int(uid):
        _user_state_pop(uid, "postpaid_thread_oid")
        try:
            await msg.reply_text(MSG_POSTPAID_THREAD_STALE)
        except Exception:
            pass
        return True
    if o.get("checkout_submitted_to_admin"):
        return await _forward_postpaid_client_payload_to_admin(
            context,
            uid=uid,
            oid=int(oid),
            body_text=body_text,
            msg=msg,
            photo_file_id=photo_file_id,
        )
    chunk = str(body_text or "").strip()
    if photo_file_id:
        extra = "[приложено фото]"
        chunk = f"{chunk}\n{extra}".strip() if chunk else extra
    if not chunk:
        try:
            await msg.reply_text(
                MSG_POSTPAID_NEED_DETAILS,
                reply_markup=_kb_postpaid_submit(int(oid)),
            )
        except Exception:
            pass
        return True
    _postpaid_append_detail(o, chunk)
    save_state()
    try:
        await msg.reply_text(
            MSG_POSTPAID_COLLECTED,
            reply_markup=_kb_postpaid_submit(int(oid)),
        )
    except Exception:
        pass
    return True


async def _sync_site_order_checkout_complete(order_id: int, order: dict) -> None:
    """После адреса: paid + delivery_details + file_id на сайте (ЛК и корзина)."""
    ext = str(order.get("external_id") or "").strip()
    if not ext:
        try:
            await _ensure_site_order_for_bot_order(int(order_id), order)
        except Exception:
            logging.getLogger(__name__).exception(
                "site order mirror failed bot_order=%s", order_id
            )
        ext = str(order.get("external_id") or "").strip()
    url = ORDER_STATUS_UPDATE_API_URL
    if not ext or not url:
        return
    details = _postpaid_merged_details(order)
    if not details:
        return
    order["delivery_details"] = details
    headers = {"Content-Type": "application/json"}
    if ORDER_STATUS_UPDATE_SECRET:
        headers["Authorization"] = f"Bearer {ORDER_STATUS_UPDATE_SECRET}"
    payload: Dict[str, object] = {
        "order_id": ext,
        "status": "paid",
        "delivery_details": details,
    }
    fid = str(order.get("proof_file_id") or "").strip()
    if fid:
        payload["telegram_payment_proof_file_id"] = fid
    log = logging.getLogger(__name__)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status >= 300:
                    body = await resp.text()
                    log.warning(
                        "site checkout complete sync failed order=%s HTTP %s %s",
                        ext,
                        resp.status,
                        body[:300],
                    )
    except Exception:
        log.exception("site checkout complete sync failed order=%s", ext)


async def _finalize_order_submission_to_admin(
    context: ContextTypes.DEFAULT_TYPE,
    uid: int,
    oid: int,
) -> Tuple[bool, str]:
    """Скрин + адрес + карточка заказа админу; синк на сайт; очистка корзины."""
    log = logging.getLogger(__name__)
    o = ORDERS.get(int(oid))
    if not o or int(o.get("user_id") or 0) != int(uid):
        return False, "stale"
    if o.get("checkout_submitted_to_admin"):
        return True, "already"
    details = _postpaid_merged_details(o)
    if not details:
        return False, "empty"
    fid = str(o.get("proof_file_id") or "").strip()
    if not fid or not o.get("payment_proof_submitted"):
        return False, "no_proof"
    o["delivery_details"] = details
    try:
        await _sync_site_order_checkout_complete(int(oid), o)
    except Exception:
        log.exception("finalize site sync uid=%s oid=%s", uid, oid)
    try:
        cap = _format_payment_proof_caption(int(oid), o, int(uid))
    except Exception:
        log.exception("finalize proof caption uid=%s oid=%s", uid, oid)
        cap = f"📸 Чек оплаты · Заказ #{oid}\n👤 id: {uid}"
    ok = False
    try:
        ok = await asyncio.wait_for(
            _send_or_edit_admin_payment_proof(
                context, int(oid), o, fid, cap, customer_msg=None
            ),
            timeout=45.0,
        )
    except asyncio.TimeoutError:
        log.error("finalize admin proof timeout uid=%s oid=%s", uid, oid)
    except Exception:
        log.exception("finalize admin proof uid=%s oid=%s", uid, oid)
    if not ok:
        return False, "admin_fail"
    try:
        await _send_admin_order_panel(context, int(oid), o, force_new=True)
    except Exception:
        log.exception("finalize admin panel uid=%s oid=%s", uid, oid)
    o["checkout_submitted_to_admin"] = True
    if o.get("clear_cart_on_paid"):
        _clear_user_cart_after_payment_proof(int(uid), int(oid), o)
        try:
            await _notify_site_cart_cleared_after_proof(int(uid), int(oid), o)
        except Exception:
            log.exception("finalize site cart clear uid=%s oid=%s", uid, oid)
    _clear_postpaid_thread_if_matches(int(uid), int(oid))
    save_state()
    return True, "ok"


def _kb_payment_admin_review(order_id: int, uid: int) -> InlineKeyboardMarkup:
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
            [
                InlineKeyboardButton(
                    "📁 Папка клиента",
                    callback_data=f"adm_user_all_{int(uid)}",
                ),
            ],
        ]
    )


async def _ensure_customer_admin_folder(
    context: ContextTypes.DEFAULT_TYPE, uid: int, username: Optional[str] = None
) -> Tuple[Optional[int], Optional[int]]:
    """Заголовок «папки» клиента у админа — все чеки reply в один тред."""
    try:
        uid_i = int(uid)
    except (TypeError, ValueError):
        return None, None
    if uid_i <= 0:
        return None, None
    row = users_ensure(uid_i)
    cid = row.get("admin_folder_chat_id")
    mid = row.get("admin_folder_message_id")
    targets = _admin_order_notify_targets() or ([int(ADMIN_ID)] if ADMIN_ID else [])
    target_ids = {int(t) for t in targets if isinstance(t, int)}
    if cid is not None and mid is not None:
        try:
            cid_i, mid_i = int(cid), int(mid)
            if cid_i in target_ids:
                return cid_i, mid_i
        except (TypeError, ValueError):
            pass
        row.pop("admin_folder_chat_id", None)
        row.pop("admin_folder_message_id", None)
    name = _user_display_name(uid_i, username)
    text = (
        f"📁 Клиент: {name}\n"
        f"🆔 id: {uid_i}\n\n"
        "Ниже — чеки оплаты и сообщения только этого покупателя."
    )
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📋 Заказы и переписка", callback_data=f"adm_user_all_{uid_i}")]]
    )
    log = logging.getLogger(__name__)
    for tgt in _admin_order_notify_targets() or [int(ADMIN_ID)]:
        try:
            sent = await context.bot.send_message(
                chat_id=tgt,
                text=text,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            row["admin_folder_chat_id"] = int(sent.chat_id)
            row["admin_folder_message_id"] = int(sent.message_id)
            log.info("admin customer folder uid=%s chat=%s msg=%s", uid_i, sent.chat_id, sent.message_id)
            return int(sent.chat_id), int(sent.message_id)
        except Exception:
            log.exception("admin customer folder create uid=%s target=%s", uid_i, tgt)
    return None, None


def _user_data_for(application: Application, user_id: int) -> dict:
    """user_data другого пользователя (для сброса awaiting_* после действия админа)."""
    raw = application.user_data
    if user_id not in raw:
        raw[user_id] = {}
    return raw[user_id]


def _clear_crypto_auto_watch(o: dict, uid: int, *, persist: bool = True) -> None:
    """Снять mock-наблюдение за крипто-оплатой (карта/перевод/ручной скрин)."""
    o.pop("crypto_auto_active", None)
    o.pop("crypto_auto_deadline", None)
    _user_state_pop(uid, "crypto_check", persist=persist)


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


async def _admin_delete_order_chat_message(
    context: ContextTypes.DEFAULT_TYPE, q: CallbackQuery, order_id: int
) -> None:
    """Удалить сообщение с карточкой заказа; если это основное admin_message — сбросить ссылку."""
    log = logging.getLogger(__name__)
    o = ORDERS.get(order_id)
    if not o or not q.message:
        try:
            await q.answer("Заказ не найден", show_alert=False)
        except Exception:
            pass
        return
    chat_id = int(q.message.chat_id)
    mid = int(q.message.message_id)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=mid)
    except Exception:
        log.info("delete_message order #%s chat=%s mid=%s", order_id, chat_id, mid)
        try:
            await q.answer("Не удалось удалить (нет прав или сообщение уже удалено).", show_alert=True)
        except Exception:
            pass
        return
    if int(o.get("admin_message_id") or 0) == mid and int(o.get("admin_chat_id") or 0) == chat_id:
        o["admin_message_id"] = None
        o["admin_chat_id"] = None
        save_state()
    try:
        await q.answer("Сообщение удалено.", show_alert=False)
    except Exception:
        pass


def _cart_totals(lines: List[dict]) -> Tuple[int, int]:
    t = 0
    n = 0
    for x in lines:
        if not isinstance(x, dict):
            continue
        q = _cart_line_qty_coerce(x.get("qty"))
        p = _coerce_card_price_int(x.get("price"))
        t += p * q
        n += q
    return t, n


def _order_computed_grand_total(o: dict) -> int:
    """Итог по строкам заказа + сумма доставки."""
    goods, _ = _cart_totals(list(o.get("items") or []))
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
    try:
        d_amt = int(d.get("amount") or 0)
    except (TypeError, ValueError):
        d_amt = 0
    return max(0, int(goods) + int(d_amt))


def _order_resolved_grand_total(o: dict) -> int:
    """Итог для отображения и ORDERS: при явном расхождении с позициями — исправляем o['total']."""
    comp = _order_computed_grand_total(o)
    try:
        stored = int(o.get("total") or 0)
    except (TypeError, ValueError):
        stored = 0
    if comp <= 0:
        return max(0, stored)
    if stored <= 0:
        o["total"] = int(comp)
        return int(comp)
    tol = max(100, max(comp, stored) // 15)
    if abs(stored - comp) > tol:
        o["total"] = int(comp)
        return int(comp)
    return int(stored)


def _format_cart_message(
    lines: List[dict],
    user_data: Optional[dict] = None,
    *,
    cart_uid: int = 0,
) -> str:
    if not lines:
        return (
            "🛒 Ваша корзина пуста\n\n"
            "Вы ещё не добавили ни одной карточки\n"
            "Перейдите в каталог 👇"
        )
    default_cur = _goods_currency_for_delivery_country(
        _cart_price_region_for_user(cart_uid, user_data or {})
    )
    site_labels = {
        str(x.get("line_currency") or "").strip().upper()
        for x in lines
        if isinstance(x, dict)
        and x.get("from_site")
        and str(x.get("line_currency") or "").strip().upper() in ("BYN", "RUB")
    }
    if site_labels and all(
        isinstance(x, dict) and x.get("from_site") for x in lines
    ) and len(site_labels) == 1:
        cur = site_labels.pop()
    else:
        cur = default_cur
    shown_grand, from_site_hint = _cart_grand_total_for_display(
        cart_uid, user_data, lines, cur
    )
    out: List[str] = [
        "🛒 Ваша корзина:",
        "",
    ]
    for x in lines:
        if not isinstance(x, dict):
            continue
        name_raw = str(x.get("name") or "—")
        name = name_raw[:200]
        if len(name_raw) > 200:
            name = name.rstrip() + "…"
        p = _coerce_card_price_int(x.get("price"))
        q = _cart_line_qty_coerce(x.get("qty"))
        lc = str(x.get("line_currency") or "").strip().upper()
        line_cur = lc if lc in ("BYN", "RUB") else cur
        out.append(f"• {name} — {q} шт. × {p} {line_cur} = {p * q} {line_cur}")
    if from_site_hint:
        out += ["", f"💰 Итого (как на сайте, с доставкой): {shown_grand} {cur}"]
    else:
        out += ["", f"💰 Итого: {shown_grand} {cur}"]
    foot_loy = _loyalty_cart_footer_lines(cart_uid)
    if foot_loy:
        out.append("")
        out.extend(foot_loy)
    s = "\n".join(out)
    if len(s) > 3900:
        s = s[:3890] + "…"
    return s


def _format_login_site_cart_pending_text(
    uid: int, delivery_cc: str, products: List[dict]
) -> str:
    """Текст черновика заказа из корзины на сайте (после verify-code), для админа и кнопок в TG."""
    lines = deepcopy(_cart_get_lines_uid(uid))
    if not lines:
        return ""
    fin = _site_cart_checkout_finance(uid, lines, delivery_cc)
    g_cur = fin["g_cur"]
    goods = fin["goods"]
    d_label = fin["d_label"]
    d_amt = fin["d_amt"]
    d_cur = fin["d_cur"]
    inc = fin["inc"]
    use_site = fin["from_site"]
    meta = _merge_site_bonus_into_meta(uid, _get_site_pending_meta(uid))
    grand_show, pay_cur, _ = _resolve_site_confirm_pricing(uid, {}, lines, meta)
    out: List[str] = []
    for x in lines:
        name_raw = str(x.get("name") or "—")
        name = name_raw[:200]
        if len(name_raw) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        lc = str(x.get("line_currency") or "").strip().upper()
        xcur = lc if lc in ("BYN", "RUB") else g_cur
        out.append(f"• {name} — {q} шт. × {p} {xcur} = {p * q} {xcur}")
    out.append("")
    site_gt_raw = _cart_get_site_grand_total(uid, g_cur)
    if inc and not site_gt_raw:
        out.append(f"🚚 Доставка: {d_label} (уже в сумме на сайте)")
        out.append(f"💰 Итого: {grand_show} {pay_cur}")
    elif use_site:
        out.append(f"🚚 Доставка: {d_label} (+{d_amt} {d_cur})")
        out.append(f"💰 Итого: {grand_show} {g_cur} (как на сайте)")
    elif str(d_cur).upper() == "BYN" and g_cur == "BYN":
        out.append(f"🚚 Доставка: {d_label} (+{d_amt} {d_cur})")
        out.append(f"💰 Итого: {grand_show} BYN")
    elif str(d_cur).upper() == "RUB" and g_cur == "RUB":
        out.append(f"🚚 Доставка: {d_label} (+{d_amt} {d_cur})")
        out.append(f"💰 Итого: {grand_show} RUB")
    else:
        out.append(f"🚚 Доставка: {d_label} (+{d_amt} {d_cur})")
        out.append(f"💰 Товары: {goods} {g_cur}; доставка: {d_amt} {d_cur}")
        out.append(f"💰 Итого: {grand_show} {pay_cur}")
    s = "\n".join(out)
    if len(s) > 3500:
        s = s[:3490] + "…"
    return s


def _kb_site_order_confirm_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Подтвердить заказ",
                    callback_data="confirm_order",
                ),
                InlineKeyboardButton(
                    "❌ Отменить",
                    callback_data="cancel_order",
                ),
            ],
        ]
    )


async def _send_site_cart_confirm_prompt(
    bot, uid: int, preview_inner: str, *, intro_kind: str = "draft"
) -> None:
    """Сообщение с inline «Подтвердить заказ» / «Отмена» (корзина с сайта)."""
    log = logging.getLogger(__name__)
    if not preview_inner.strip():
        return
    intro = (
        "Проверьте состав и доставку. Нажмите «Подтвердить заказ» — откроются шаги оплаты. "
        "Заказ уходит админу только после подтверждения оплаты со скрином чека."
    )
    body = f"{intro}\n\n📦 Ваш заказ\n\n{preview_inner.strip()}"
    if len(body) > 4090:
        body = body[:4082] + "…"
    try:
        await bot.send_message(
            chat_id=int(uid),
            text=body,
            disable_web_page_preview=True,
            reply_markup=_kb_site_order_confirm_cancel(),
        )
    except Exception:
        log.exception("site cart confirm prompt → user_id=%s", uid)


def _schedule_site_cart_confirm_prompt(bot, uid: int, *, intro_kind: str = "verified") -> None:
    """После sync с сайта — превью заказа с кнопками в Telegram (если нет активной оплаты)."""
    if not bot or not uid:
        return

    async def _job() -> None:
        if _user_state_get(int(uid), "awaiting_proof") is not None:
            return
        if _user_has_unpaid_order(int(uid)):
            return
        await _maybe_prompt_site_cart_confirmation(
            bot, int(uid), None, intro_kind=intro_kind
        )

    try:
        asyncio.get_running_loop().create_task(_job())
    except RuntimeError:
        pass


async def _maybe_prompt_site_cart_confirmation(
    bot,
    uid: int,
    user_data: Optional[dict] = None,
    *,
    intro_kind: str = "draft",
) -> bool:
    """Если есть корзина с сайта — отправить превью с кнопками подтверждения."""
    if not uid:
        return False
    if _user_has_unpaid_order(int(uid)):
        return False
    lines = _cart_get_lines_uid(uid, user_data)
    preview = _get_site_pending_preview(int(uid))
    if not preview and lines:
        cc = str(USER_PREF_DELIVERY_COUNTRY.get(int(uid)) or "by")
        try:
            products = await load_products()
        except Exception:
            products = []
        preview = _format_login_site_cart_pending_text(int(uid), cc, products)
    if not lines:
        return False
    if not preview:
        return False
    meta = _merge_site_bonus_into_meta(int(uid), _get_site_pending_meta(int(uid), user_data))
    if lines and not meta.get("items"):
        meta["items"] = deepcopy(lines)
    _persist_site_pending_order(int(uid), preview, meta, user_data)
    await _send_site_cart_confirm_prompt(
        bot, int(uid), preview, intro_kind=intro_kind
    )
    return True


async def _send_site_login_cart_order_message(bot, uid: int, preview_inner: str) -> None:
    """Сообщение в Telegram после успешного входа на сайт по коду."""
    await _send_site_cart_confirm_prompt(bot, uid, preview_inner, intro_kind="verified")


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
        gcur = _order_line_currency_from_delivery(d if isinstance(d, dict) else None)
        lines.append(f"• #{oid} — {st}")
        lines.append(f"  Товары: {tg} {gcur} · {dtxt}")
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


def _user_site_orders_for_list(user_id: int) -> List[dict]:
    return [
        r
        for r in (USER_ORDERS.get(int(user_id)) or [])
        if str(r.get("sync_source") or "") == "site"
    ]


def _site_order_rec_from_deep_link_shape(order_id: str, order: dict, status: object = "") -> dict:
    lines = deepcopy(list(order.get("items") or []))
    drec = deepcopy(order.get("delivery") or {})
    try:
        total_goods, _ = _cart_totals(lines)
    except Exception:
        total_goods = 0
    final_total = _loyalty_find_int(
        order,
        (
            "finalTotal",
            "final_total",
            "payTotal",
            "pay_total",
            "amountToPay",
            "amount_to_pay",
            "totalAfterBonus",
            "total_after_bonus",
            "totalAfterBonuses",
            "total_after_bonuses",
            "grandTotalAfterBonus",
            "grand_total_after_bonus",
            "paidTotal",
            "paid_total",
        ),
        2,
    )
    total_raw = _loyalty_find_int(
        order,
        ("total", "grandTotal", "grand_total", "orderTotal", "order_total", "site_grand_total_hint"),
        2,
    )
    bonus_applied = _loyalty_find_int(
        order,
        (
            "bonusApplied",
            "bonus_applied",
            "bonusDiscount",
            "bonus_discount",
            "bonusesApplied",
            "bonuses_applied",
            "pointsDiscount",
            "points_discount",
            "loyaltyDiscount",
            "loyalty_discount",
        ),
        2,
    )
    bonus_spent = _loyalty_find_int(
        order,
        (
            "bonusPointsSpent",
            "bonus_points_spent",
            "pointsSpent",
            "points_spent",
            "bonusesSpent",
            "bonuses_spent",
            "loyaltyPointsSpent",
            "loyalty_points_spent",
        ),
        2,
    )
    if final_total is not None:
        total = int(final_total)
    elif total_raw is not None and bonus_applied is not None and int(bonus_applied) > 0:
        total = max(0, int(total_raw) - int(bonus_applied))
    elif total_raw is not None:
        total = int(total_raw)
    else:
        total = 0
    if total <= 0 and bonus_applied is None:
        try:
            total = int(total_goods) + int(drec.get("amount") or 0)
        except (TypeError, ValueError):
            total = int(total_goods)
    ext = str(order.get("external_id") or order_id).strip()
    rec = {
        "id": ext[:80],
        "external_id": ext[:120],
        "items": lines,
        "total": max(0, int(total)),
        "total_goods": int(total_goods),
        "delivery": drec,
        "status": _site_status_to_bot_status(status),
        "sync_source": "site",
    }
    if bonus_applied is not None and int(bonus_applied) > 0:
        rec["bonus_applied"] = int(bonus_applied)
    if bonus_spent is not None and int(bonus_spent) > 0:
        rec["bonus_points_spent"] = int(bonus_spent)
    return rec


async def _refresh_user_site_orders_from_site(user_id: int) -> int:
    """Pull durable order history from IlluCards site before showing Telegram 'Мои заказы'."""
    uid = int(user_id or 0)
    if not uid or not ORDER_USER_ORDERS_API_URL:
        return 0
    safe_uid = urllib.parse.quote(str(uid), safe="")
    if "{user_id}" in ORDER_USER_ORDERS_API_URL:
        url = ORDER_USER_ORDERS_API_URL.replace("{user_id}", safe_uid)
    else:
        sep = "&" if "?" in ORDER_USER_ORDERS_API_URL else "?"
        url = f"{ORDER_USER_ORDERS_API_URL}{sep}user_id={safe_uid}"
    log = logging.getLogger(__name__)
    headers: dict = {}
    if ORDER_STATUS_UPDATE_SECRET:
        headers["Authorization"] = f"Bearer {ORDER_STATUS_UPDATE_SECRET}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(url, headers=headers or None) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("IlluCards user orders sync HTTP %s: %s", resp.status, body[:300])
                    return 0
                data = await resp.json()
    except Exception:
        log.exception("IlluCards user orders sync failed user_id=%s", uid)
        return 0
    raw_orders = data.get("orders") if isinstance(data, dict) else data
    if not isinstance(raw_orders, list):
        return 0
    out: List[dict] = []
    for raw in raw_orders[:30]:
        if not isinstance(raw, dict):
            continue
        ext = str(raw.get("external_id") or raw.get("id") or "").strip()
        rec: Optional[dict] = None
        if ext:
            detail = await _fetch_order_from_deep_link_api(ext)
            if detail:
                rec = _site_order_rec_from_deep_link_shape(ext, detail, raw.get("status"))
        if rec is None:
            rec = _normalize_sync_site_order(raw)
        if rec:
            out.append(rec)
    if out:
        _user_orders_merge_site(uid, out)
    return len(out)


async def _refresh_user_state_from_site(user_id: int) -> bool:
    """Pull cart/favorites/bonus state from IlluCards site before showing it in Telegram."""
    uid = int(user_id or 0)
    if not uid or not SITE_USER_STATE_API_URL or not SITE_USER_STATE_SYNC_SECRET:
        return False
    safe_uid = urllib.parse.quote(str(uid), safe="")
    if "{user_id}" in SITE_USER_STATE_API_URL:
        url = SITE_USER_STATE_API_URL.replace("{user_id}", safe_uid)
    else:
        sep = "&" if "?" in SITE_USER_STATE_API_URL else "?"
        url = f"{SITE_USER_STATE_API_URL}{sep}user_id={safe_uid}"
    log = logging.getLogger(__name__)
    headers = {"Authorization": f"Bearer {SITE_USER_STATE_SYNC_SECRET}"}
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("IlluCards user-state sync HTTP %s: %s", resp.status, body[:300])
                    return False
                data = await resp.json()
    except Exception:
        log.exception("IlluCards user-state sync failed user_id=%s", uid)
        return False
    if not isinstance(data, dict):
        return False
    del_raw = data.get("deliveryCountry") or data.get("delivery_country") or "BY"
    bot_code, _, _, _ = _delivery_option_for_site_code(str(del_raw or "BY"))
    try:
        products = await load_products()
    except Exception:
        products = []
    changed = False
    if "cart" in data:
        lines = _normalize_sync_cart_items(data.get("cart"), bot_code)
        if products and lines:
            _reconcile_cart_lines_to_catalog(products, lines)
        _cart_set_items_uid(uid, lines)
        _remember_user_delivery_country(uid, bot_code)
        _cart_apply_site_pricing_hints(uid, data)
        changed = True
    if "favorites" in data:
        refs = _normalize_sync_favorites_with_catalog(data.get("favorites"), products)
        USER_FAVORITES[uid] = refs
        changed = True
    _apply_site_loyalty_from_sync(uid, data)
    if changed:
        users_touch(uid, "site_state_pull")
    save_state()
    return True


def _site_user_order_token(uid: int, rec: dict) -> str:
    key = str(rec.get("external_id") or rec.get("id") or "")
    return hashlib.sha256(f"{int(uid)}:{key}".encode("utf-8")).hexdigest()[:12]


def _find_user_site_order_by_token(uid: int, token: str) -> Optional[dict]:
    tok = (token or "").strip().lower()
    if len(tok) != 12:
        return None
    for rec in USER_ORDERS.get(int(uid)) or []:
        if str(rec.get("sync_source") or "") != "site":
            continue
        if _site_user_order_token(int(uid), rec).lower() == tok:
            return rec
    return None


async def _format_mine_orders_text_and_kb(
    user_id: int,
) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    await _refresh_user_site_orders_from_site(user_id)
    reg = _user_orders_registry_for_user(user_id)
    site_recs = _user_site_orders_for_list(user_id)
    if not reg and not site_recs:
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
    for oid, o in reg[:25]:
        tot = _order_resolved_grand_total(o)
        _, qty = _cart_totals(list(o.get("items") or []))
        st = str(o.get("status") or "new")
        badge = _user_order_status_badge(st)
        cur = _order_line_currency_from_delivery(
            o.get("delivery") if isinstance(o.get("delivery"), dict) else None
        )
        lines.append(f"#{oid} — {qty} шт. — {tot} {cur} — {badge}")
        rows.append(
            [
                InlineKeyboardButton(f"📦 #{oid}", callback_data=f"user_order_{oid}"),
            ],
        )
    for rec in site_recs[:20]:
        if len(rows) >= 30:
            break
        tot = int(rec.get("total") or 0)
        _, qty = _cart_totals(list(rec.get("items") or []))
        st_site = _site_status_to_bot_status(str(rec.get("status") or ""))
        badge = _user_order_status_badge(st_site) if st_site else "🌐 на сайте"
        drec = rec.get("delivery") if isinstance(rec.get("delivery"), dict) else None
        cur = _order_line_currency_from_delivery(drec)
        disp = str(rec.get("external_id") or rec.get("id") or "")[:20]
        lines.append(f"🌐 {disp} — {qty} шт. — {tot} {cur} — {badge}")
        tok = _site_user_order_token(int(user_id), rec)
        rows.append(
            [
                InlineKeyboardButton(f"📦 {disp or 'заказ'}", callback_data=f"uos:{tok}"),
            ],
        )
    text = "\n".join(lines)
    if len(text) > 3500:
        text = text[:3490] + "…"
        rows = rows[:25]
    return (text, InlineKeyboardMarkup(rows))


def _format_user_order_detail(order_id: object, o: dict) -> str:
    oid_disp = str(order_id).strip() or "—"
    drec = o.get("delivery") if isinstance(o.get("delivery"), dict) else None
    line_cur = _order_line_currency_from_delivery(drec)
    items_block = _format_order_items_for_admin(list(o.get("items") or []), line_cur)
    _, qty = _cart_totals(list(o.get("items") or []))
    tot = _order_resolved_grand_total(o)
    st = str(o.get("status") or "new")
    sk = "canceled" if st == "cancelled" else st
    st_ru = _order_status_label_ru(sk)
    dline = _format_delivery_block(
        o.get("delivery") if isinstance(o.get("delivery"), dict) else None
    )
    parts: List[str] = [
        f"📦 Заказ #{oid_disp}",
        "",
        f"📊 Статус: {st_ru}",
        "",
        "📦 Состав:",
        items_block,
        "",
    ]
    try:
        b_ap = int(o.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        b_ap = 0
    if b_ap > 0:
        parts.append("")
    parts.append(f"📦 Количество карточек: {qty} шт.")
    parts.append(f"💰 Итого: {tot} {line_cur}")
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


def _format_checkout_preview_for_user(
    lines: List[dict], user_data: Optional[dict] = None, *, cart_uid: int = 0
) -> str:
    total, _ = _cart_totals(lines)
    default_cur = _goods_currency_for_delivery_country(
        _cart_price_region_for_user(cart_uid, user_data or {})
    )
    site_labels = {
        str(x.get("line_currency") or "").strip().upper()
        for x in lines
        if x.get("from_site") and str(x.get("line_currency") or "").strip().upper() in ("BYN", "RUB")
    }
    if site_labels and all(x.get("from_site") for x in lines) and len(site_labels) == 1:
        cur = site_labels.pop()
    else:
        cur = default_cur
    out: List[str] = ["🛒", ""]
    for x in lines:
        name_raw = str(x.get("name") or "—")
        name = name_raw[:200]
        if len(name_raw) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        lc = str(x.get("line_currency") or "").strip().upper()
        line_cur = lc if lc in ("BYN", "RUB") else cur
        out.append(f"• {name} — {q} шт. × {p} {line_cur} = {p * q} {line_cur}")
    out += ["", f"💰 {total} {cur}"]
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


def _format_order_preview_with_delivery(
    user_data: dict, checkout_uid: int = 0
) -> str:
    fin = _checkout_preview_finance(user_data, checkout_uid)
    if not fin:
        return ""
    lines = fin["lines"]
    g_cur = str(fin["g_cur"])
    code = str(fin["code"])
    dlabel = str(fin["dlabel"])
    dcur = str(fin["dcur"])
    inc = bool(fin["inc"])
    use_site = bool(fin["use_site"])
    site_gt_raw = fin.get("site_gt_raw")
    grand_show = int(fin["grand_show"])
    grand_final = int(grand_show)
    out: List[str] = [
        "📦 Ваш заказ:",
        "",
    ]
    for x in lines:
        name_raw = str(x.get("name") or "—")
        name = name_raw[:200]
        if len(name_raw) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        lc = str(x.get("line_currency") or "").strip().upper()
        xcur = lc if lc in ("BYN", "RUB") else g_cur
        out.append(f"• {name} — {q} шт. × {p} {xcur} = {sub} {xcur}")
    out.append("")
    if inc and not site_gt_raw:
        out.append(f"🚚 Доставка: {dlabel} (уже в сумме на сайте)")
        out.append("")
        out.append(f"💰 Итого: {grand_final} {g_cur}")
    elif use_site:
        out.append(f"🚚 Доставка: {dlabel}")
        out.append("")
        out.append(f"💰 Итого: {grand_final} {g_cur} (как на сайте)")
    else:
        out.append(f"🚚 Доставка: {dlabel}")
        out.append("")
        if code == "by" and dcur == "BYN":
            out.append(f"💰 Итого: {grand_final} BYN")
        else:
            out.append(f"💰 Итого: {grand_final} RUB")
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
        "checkout_bonus_spend",
    ):
        user_data.pop(k, None)


def _void_unpaid_pending_order_and_restore_checkout(uid: int, ud: dict) -> bool:
    """Вернуть неоплаченный заказ в черновик/корзину без удаления заказа."""
    oid_raw = ud.get("awaiting_payment_order_id")
    if oid_raw is None:
        return False
    try:
        oid = int(oid_raw)
    except (TypeError, ValueError):
        ud.pop("awaiting_payment_order_id", None)
        return False
    o = ORDERS.get(oid)
    if not o:
        ud.pop("awaiting_payment_order_id", None)
        return False
    if int(o.get("user_id") or 0) != int(uid):
        return False
    if o.get("paid"):
        return False
    if o.get("payment_proof_submitted"):
        return False
    items = deepcopy(list(o.get("items") or []))
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
    ud["order_checkout"] = items
    cc = str(d.get("country") or "").strip().lower()
    if cc in DELIVERY_OPTIONS:
        ud["delivery_country"] = cc
        ud["delivery_label"] = d.get("label")
        try:
            ud["delivery_amount"] = int(d.get("amount") or 0)
        except (TypeError, ValueError):
            ud["delivery_amount"] = 0
        ud["delivery_currency"] = d.get("currency")
    _clear_crypto_auto_watch(o, uid)
    _user_state_pop(uid, "awaiting_proof")
    _cart_set_items_uid(uid, items)
    ud.pop("awaiting_payment_order_id", None)
    ud.pop("payment_pending_method", None)
    o.pop("payment_pending_method", None)
    save_state()
    return True


def _kb_delivery_country_with_back() -> InlineKeyboardMarkup:
    rows = list(_kb_delivery_country().inline_keyboard)
    rows.append(
        [InlineKeyboardButton("◀️ В корзину", callback_data="chk:country_to_cart")]
    )
    return InlineKeyboardMarkup(rows)


def _kb_order_preview_actions(uid: int, user_data: dict) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("✅ Подтвердить заказ", callback_data="ta:0")],
        [
            InlineKeyboardButton(
                "◀️ Изменить страну", callback_data="chk:preview_to_country"
            )
        ],
    ]
    return InlineKeyboardMarkup(rows)


def _kb_payment_methods_with_back(
    order_id: Optional[int] = None,
) -> InlineKeyboardMarkup:
    """Только способы оплаты (без «назад к подтверждению» — шаг уже пройден)."""
    return _kb_payment_methods(order_id)


def _kb_paid_confirm_with_back(
    total_label: str, order_id: Optional[int] = None
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"✅ Оплатить + {total_label}",
                    callback_data=_pay_cb("paid", order_id),
                )
            ],
            [
                InlineKeyboardButton(
                    "◀️ Другой способ оплаты", callback_data="chk:pay_to_methods"
                )
            ],
        ]
    )


def _kb_proof_step_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "◀️ Другой способ оплаты", callback_data="chk:pay_to_methods"
                )
            ],
        ]
    )


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


def _kb_catalog_currency_pick() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇧🇾 BYN", callback_data="ccur:by"),
                InlineKeyboardButton("🇷🇺 RUB", callback_data="ccur:ru"),
            ],
        ]
    )


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
        out = [p for p in base if _product_is_sale(p)]
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


def _product_category_number(products: List[dict], target: dict) -> int:
    cat = str(target.get("category", "") or "")
    tid = str(target.get("id", "") or "")
    n = 0
    for p in products:
        if str(p.get("category", "") or "") != cat:
            continue
        n += 1
        if tid and str(p.get("id", "") or "") == tid:
            return n
        if p is target:
            return n
    return max(1, n)


def _product_category_label(products: List[dict], target: dict) -> str:
    cat = str(target.get("category", "Без категории") or "Без категории")
    no = _product_category_number(products, target)
    return f"{cat} {no}"


def _tinder_caption(
    p: dict,
    cur_1: int,
    n_total: int,
    products: List[dict],
    user_data: Optional[dict],
) -> str:
    name = (p.get("name") or "—")
    r_raw = str(p.get("rarity", "") or "").strip() or "—"
    r = _rarity_label_ru(r_raw)
    reg = _goods_price_region_from_user_data(user_data)
    cur = _goods_currency_for_delivery_country(reg)
    v = _product_unit_price_for_delivery(p, reg)
    try:
        pstr = f"{int(v)} {cur}"
    except (TypeError, ValueError):
        pstr = "—"
    cat = str(p.get("category", "Без категории") or "Без категории")
    card_label = _product_category_label(products, p)
    if len(name) > 200:
        name = name[:197] + "…"
    c = (
        f"{name}\n\n"
        f"💰 {pstr}\n"
        f"⭐ {r}\n"
        f"📂 {cat}\n"
        f"🔢 Карточка: {card_label}\n\n"
        f"🔎 {cur_1} из {n_total}"
    )
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
                InlineKeyboardButton("◀️", callback_data="t:p"),
                InlineKeyboardButton("💚", callback_data="t:v"),
                InlineKeyboardButton("🛍️", callback_data="t:c"),
                InlineKeyboardButton("▶️", callback_data="t:n"),
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
    cap = _tinder_caption(p, i + 1, n, products, ud)
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
    cap = _tinder_caption(p0, 1, n, products, ud)
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
    """t:p t:n t:c t:v t:f — листалка Tinder; t:v избранное; t:f пауза/продолжить."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    s = (q.data or "").strip()
    m = re.match(r"^t:([pncvf])$", s)
    if not m:
        return
    op = m.group(1)
    if not _message_shows_card_media(q.message) and op != "f":
        try:
            await q.answer(
                "Сообщение без картинки — откройте каталог снова из меню внизу.",
                show_alert=True,
            )
        except Exception:
            pass
        return
    ud = context.user_data
    _tinder_cancel_autoplay(ud)
    gixs: List[int] = list(ud.get("tinder_gidxs") or [])
    products: List[dict] = list(context.application.bot_data.get("products") or [])
    if not gixs or not products:
        try:
            await q.answer(
                "Просмотр каталога сброшен (например, после обновления бота). "
                "Нажмите «📦 Каталог» внизу и зайдите в раздел снова.",
                show_alert=True,
            )
        except Exception:
            pass
        return
    n = len(gixs)
    if n == 0:
        return
    if op == "f":
        if not _message_shows_card_media(q.message):
            try:
                await q.answer("Откройте каталог заново из меню.", show_alert=True)
            except Exception:
                pass
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
        await q.answer(
            "Автолистание на паузе." if new_paused else "Автолистание: следующая карта через 3 с."
        )
        if not new_paused:
            _tinder_start_autoplay(context, ud)
        return
    i = int(ud.get("tinder_i", 0)) % n
    if op == "n":
        i = (i + 1) % n
    elif op == "p":
        i = (i - 1) % n
    elif op in ("c", "v"):
        gix_cur = gixs[i]
        p_cur = products[gix_cur] if 0 <= gix_cur < len(products) else None
        if p_cur is None:
            await _notify_callback_issue(q, context)
            return
        ref = _product_ref_for_callback(p_cur, gix_cur)
        uid_t = q.from_user.id if q.from_user else 0
        if op == "c":
            reg_t = _cart_price_region_for_user(uid_t, ud)
            px_t = _product_unit_price_for_delivery(p_cur, reg_t)
            _cart_add_line_uid(
                uid_t,
                ud,
                ref,
                p_cur,
                p_cur.get("name") or "—",
                px_t,
            )
            if uid_t:
                users_touch(uid_t, "cart")
        else:
            cnt = _favorites_add_ref_uid(uid_t, ref)
            if uid_t:
                users_touch(uid_t, "favorites")
        i = (i + 1) % n
    else:
        await _notify_callback_issue(q, context)
        return
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
        cur_t = _goods_currency_for_delivery_country(
            _cart_price_region_for_user(uid_t, ud)
        )
        short = f"{len(lines)} поз. · {tot} {cur_t}"
        await q.answer(f"🛒 Корзина: {short}", show_alert=False)
    elif op == "v":
        uid_t = q.from_user.id if q.from_user else 0
        cnt = len(_favorites_get_refs_uid(uid_t, ud))
        await q.answer(f"💚 В избранном: {cnt}", show_alert=False)
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
    try:
        nhp = await refresh_home_page_promotions_cache(app)
        log.info("Illucards: акции главной, баннеров: %d", nhp)
    except Exception:
        log.exception("Illucards: ошибка загрузки акций главной")


async def crypto_auto_check_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Через 2–5 мин после выбора ₿ — напоминание прислать скрин (проверка каждые 30 с)."""
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
        if cust:
            try:
                await bot.send_message(
                    chat_id=cust,
                    text=(
                        "⏱ Если перевод в криптовалюте уже отправлен — пришлите сюда "
                        "скрин из кошелька для проверки администратором."
                    ),
                    disable_web_page_preview=True,
                )
            except Exception:
                log.exception("crypto auto reminder user_id=%s", cust)


def _extract_total_from_order_text(text: str) -> Optional[str]:
    if not (text or "").strip():
        return None
    for cur, pats in (
        (
            "BYN",
            (
                r"💰\s*Итого\s*:\s*([0-9]+(?:[.,][0-9]+)?)\s*BYN",
                r"Итого\s*:\s*([0-9]+(?:[.,][0-9]+)?)\s*BYN",
                r"Итого\s+([0-9]+(?:[.,][0-9]+)?)\s*BYN",
            ),
        ),
        (
            "RUB",
            (
                r"💰\s*Итого\s*:\s*([0-9]+(?:[.,][0-9]+)?)\s*RUB",
                r"Итого\s*:\s*([0-9]+(?:[.,][0-9]+)?)\s*RUB",
                r"Итого\s+([0-9]+(?:[.,][0-9]+)?)\s*RUB",
            ),
        ),
    ):
        for pat in pats:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                num = m.group(1).replace(",", ".").replace(" ", "")
                return f"{num} {cur}"
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
    return _parse_order_id_from_start_args([first_arg] if first_arg else [])


def _parse_order_id_from_start_args(args: List[str]) -> Optional[str]:
    """ID заказа из payload /start: один токен order_…, или несколько слов после «order»."""
    if not args:
        return None
    parts = [str(a or "").strip() for a in args if str(a or "").strip()]
    if not parts:
        return None
    joined = " ".join(parts)
    m = _RE_START_ORDER_ARG.match(joined)
    if m:
        return (m.group(1) or "").strip() or None
    if len(parts) >= 2 and parts[0].lower() == "order":
        tail = "_".join(parts[1:]).strip()
        return tail or None
    m2 = _RE_START_ORDER_ARG.match(parts[0])
    if m2:
        return (m2.group(1) or "").strip() or None
    return None


def _deep_link_candidate_dicts(raw: dict) -> List[dict]:
    """Корень и вложенные объекты, где сайт может хранить delivery / items."""
    if not isinstance(raw, dict):
        return []
    out: List[dict] = [raw]
    for k in ("order", "data", "result", "payload", "cart", "checkout"):
        v = raw.get(k)
        if isinstance(v, dict):
            out.append(v)
    return out


def _deep_link_find_first(raw: dict, keys: tuple) -> object:
    """Первое непустое значение по ключам в raw и вложенных объектах заказа."""
    if not isinstance(raw, dict):
        return None
    for cand in _deep_link_candidate_dicts(raw):
        for key in keys:
            if key not in cand:
                continue
            val = cand.get(key)
            if val is None:
                continue
            if isinstance(val, str) and not val.strip():
                continue
            return val
    return None


def _deep_link_raw_grand_total(raw: dict) -> int:
    """Итог заказа из JSON сайта (если есть), для сверки с суммой по строкам."""
    for cand in _deep_link_candidate_dicts(raw):
        for key in (
            "grandTotal",
            "grandTotalRub",
            "grand_total",
            "total",
            "cartGrandTotal",
            "orderTotal",
            "amountDue",
        ):
            if cand.get(key) is None:
                continue
            v = _coerce_card_price_int(cand.get(key))
            if v > 0:
                return v
    return 0


_DEEP_LINK_ITEM_CONTAINER_KEYS = frozenset(
    {"items", "lines", "lineitems", "cart", "products"}
)


def _deep_link_has_explicit_delivery_country(raw: dict) -> bool:
    """Сайт явно задал страну доставки — не переопределять регион по priceRub в строках."""
    if not isinstance(raw, dict):
        return False
    for cand in _deep_link_candidate_dicts(raw):
        d = cand.get("delivery")
        if isinstance(d, dict):
            for key in (
                "country",
                "code",
                "countryCode",
                "country_code",
                "region",
            ):
                v = d.get(key)
                if v is None:
                    continue
                if str(v).strip():
                    return True
        elif d is not None and str(d).strip():
            return True
        for key in ("delivery_country", "deliveryCountry", "deliveryRegion"):
            v = cand.get(key)
            if v is None:
                continue
            if str(v).strip():
                return True
    return False


def _deep_link_force_region_from_payload(raw: dict) -> str:
    seen = False

    def walk(node: object, depth: int, *, ru_only: bool) -> str:
        nonlocal seen
        if depth < 0:
            return ""
        if isinstance(node, dict):
            for k, v in node.items():
                ks = str(k or "").strip().lower()
                if ks in _DEEP_LINK_ITEM_CONTAINER_KEYS:
                    continue
                if ks in (
                    "pricerub",
                    "price_rub",
                    "unitpricerub",
                    "unit_price_rub",
                    "grandtotalrub",
                    "grand_total_rub",
                    "linetotalrub",
                    "line_total_rub",
                ):
                    return "ru"
                if not ru_only and ks in ("pricebyn", "price_byn", "grandtotalbyn", "grand_total_byn"):
                    seen = True
                r = walk(v, depth - 1, ru_only=ru_only)
                if r:
                    return r
        elif isinstance(node, list):
            for v in node[:50]:
                r = walk(v, depth - 1, ru_only=ru_only)
                if r:
                    return r
        else:
            s = str(node or "").strip().lower()
            if (
                s in ("rub", "₽", "руб", "рубль", "рублей", "russia", "ru", "rus", "россия", "россии", "рф")
                or " rub" in f" {s} "
                or "руб" in s
                or "russia" in s
                or "росси" in s
            ):
                return "ru"
            if not ru_only and (
                s in ("byn", "беларусь", "беларуси", "by", "blr", "рб") or "беларус" in s
            ):
                seen = True
        return ""

    forced = walk(raw, 5, ru_only=True)
    if forced:
        return forced
    forced = walk(raw, 5, ru_only=False)
    if forced:
        return forced
    return "by" if seen else ""


def _deep_link_delivery_bot_code(raw: dict) -> str:
    """Код доставки by|ru|ua|ot из JSON заказа с сайта (в т.ч. вложенный order/data)."""
    cc = ""
    for cand in _deep_link_candidate_dicts(raw):
        d = cand.get("delivery")
        if isinstance(d, dict):
            cc = str(
                d.get("country")
                or d.get("code")
                or d.get("countryCode")
                or d.get("country_code")
                or d.get("region")
                or d.get("label")
                or d.get("name")
                or ""
            ).strip()
            if cc:
                break
        if not cc:
            for key in (
                "delivery_country",
                "deliveryCountry",
                "deliveryRegion",
                "delivery_label",
                "deliveryLabel",
                "delivery_name",
                "deliveryName",
                "shipping_label",
                "shippingLabel",
            ):
                v = cand.get(key)
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    cc = s
                    break
        if cc:
            break
    if not cc:
        for cand in _deep_link_candidate_dicts(raw):
            dv = cand.get("delivery")
            if dv is not None and not isinstance(dv, dict):
                cc = str(dv).strip()
                if cc:
                    break
    bot_code, _, _, _ = _delivery_option_for_site_code(cc or "BY")
    return bot_code


def _deep_link_raw_items_list(raw: dict) -> List[object]:
    """Список позиций заказа: items / lines / cart.items / order.items / data.*."""
    if not isinstance(raw, dict):
        return []
    for cand in _deep_link_candidate_dicts(raw):
        for key in ("items", "lines", "lineItems"):
            v = cand.get(key)
            if isinstance(v, list) and v:
                return v
        cart = cand.get("cart")
        if isinstance(cart, dict):
            for key in ("items", "lines", "lineItems"):
                v = cart.get(key)
                if isinstance(v, list) and v:
                    return v
    return []


_DEEP_LINK_PRICE_OVERLAY_KEYS = frozenset(
    {
        "priceRub",
        "price_rub",
        "unitPriceRub",
        "unit_price_rub",
        "salePriceRub",
        "discountedUnitPriceRub",
        "finalUnitPriceRub",
        "unitPrice",
        "discountedUnitPrice",
        "finalUnitPrice",
        "salePrice",
        "listPrice",
        "unit_price",
        "lineTotal",
        "line_total",
        "lineTotalRub",
        "line_total_rub",
        "lineTotalByn",
        "line_total_byn",
        "totalPrice",
        "extendedPrice",
        "priceByn",
        "price_byn",
    }
)


def _deep_link_flatten_line_item(it: dict) -> dict:
    """Поля цены часто во вложенном product: там RUB, на корне строки — устаревший price (BYN)."""
    if not isinstance(it, dict):
        return {}
    base = dict(it)
    for key in ("product", "variant", "sku", "merchandise", "card", "lineItem"):
        inner = it.get(key)
        if isinstance(inner, dict):
            for k, v in inner.items():
                if k in _DEEP_LINK_PRICE_OVERLAY_KEYS:
                    base[k] = v
                elif k not in base or base.get(k) is None or base.get(k) == "":
                    base[k] = v
        elif isinstance(inner, list) and inner and isinstance(inner[0], dict):
            for k, v in inner[0].items():
                if k in _DEEP_LINK_PRICE_OVERLAY_KEYS:
                    base[k] = v
                elif k not in base or base.get(k) is None:
                    base[k] = v
    return base


def _deep_link_item_unit_price(it: dict, region_bot: str, qty: int) -> int:
    """Цена строки заказа с сайта в валюте региона (RUB для RU, BYN для BY).

    Сайт часто шлёт camelCase unitPrice/lineTotal в валюте корзины без суффикса Rub/Byn;
    цены могут лежать во вложенном product — см. _deep_link_flatten_line_item.
    Для RU поле price на корне часто BYN из каталога, поэтому его берём только после явных RUB-полей.
    """
    q = max(1, int(qty))
    use_rub = str(region_bot or "by").strip().lower() != "by"
    src = _deep_link_flatten_line_item(it)

    def pick(keys: tuple) -> int:
        for k in keys:
            if src.get(k) is None:
                continue
            v = _coerce_card_price_int(src.get(k))
            if v > 0:
                return v
        return 0

    def pick_line(keys: tuple) -> int:
        for k in keys:
            if src.get(k) is None:
                continue
            try:
                v = int(round(float(src.get(k)) / q))
                if v > 0:
                    return v
            except (TypeError, ValueError, ZeroDivisionError):
                pass
        return 0

    if use_rub:
        v = pick(
            (
                "priceRub",
                "price_rub",
                "unitPriceRub",
                "unit_price_rub",
                "salePriceRub",
                "discountedUnitPriceRub",
                "finalUnitPriceRub",
            )
        )
        if v:
            return v
        v = pick_line(("lineTotalRub", "line_total_rub"))
        if v:
            return v
        v = pick(
            (
                "unitPrice",
                "discountedUnitPrice",
                "finalUnitPrice",
                "salePrice",
                "listPrice",
                "unit_price",
                "price",
            )
        )
        if v:
            return v
        v = pick_line(("lineTotal", "line_total", "totalPrice", "extendedPrice"))
        if v:
            return v
        return 0
    v = pick(("priceByn", "price_byn"))
    if v:
        return v
    v = pick_line(("lineTotalByn", "line_total_byn"))
    if v:
        return v
    return pick(("price", "unitPrice", "unit_price"))


def _normalize_deep_link_order(
    raw: dict,
    external_id: str,
    products: Optional[List[dict]] = None,
) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    raw_items = _deep_link_raw_items_list(raw)
    if not raw_items:
        return None
    region_bot = _deep_link_delivery_bot_code(raw)
    explicit_delivery = _deep_link_has_explicit_delivery_country(raw)
    forced_region = ""
    if not explicit_delivery:
        forced_region = _deep_link_force_region_from_payload(raw)
        if forced_region:
            region_bot = forced_region
    total_currency_hint = str(
        _deep_link_find_first(
            raw,
            (
                "currency",
                "totalCurrency",
                "total_currency",
                "grandTotalCurrency",
                "grand_total_currency",
                "cartGrandTotalCurrency",
                "cart_grand_total_currency",
            ),
        )
        or ""
    ).strip().upper()
    if not explicit_delivery and region_bot == "by" and total_currency_hint == "RUB":
        region_bot = "ru"
    items_out: List[dict] = []
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or it.get("title") or "—")[:200]
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
        price = _deep_link_item_unit_price(it, region_bot, qty)
        ref = str(
            it.get("ref")
            or it.get("id")
            or it.get("productId")
            or it.get("product_id")
            or name
        )[:120]
        lc = _goods_currency_for_delivery_country(region_bot)
        items_out.append(
            {
                "name": name,
                "price": price,
                "qty": qty,
                "ref": ref,
                "from_site": True,
                "line_currency": lc,
            }
        )
    if not items_out:
        return None
    if products:
        _reconcile_cart_lines_to_catalog(products, items_out)
    source = raw
    for cand in _deep_link_candidate_dicts(raw):
        if isinstance(cand.get("delivery"), (dict, str)) or any(
            cand.get(k) is not None
            for k in ("delivery_label", "delivery_name", "shipping_label", "delivery_amount", "shipping")
        ):
            source = cand
            break
    d = source.get("delivery")
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
        if not country:
            country = str(
                d.get("code")
                or d.get("countryCode")
                or d.get("country_code")
                or d.get("region")
                or label
                or ""
            ).strip()
    elif isinstance(d, str) and d.strip():
        country, label, amount, currency = _delivery_option_for_site_code(d)
    else:
        label = str(
            source.get("delivery_label")
            or source.get("delivery_name")
            or source.get("shipping_label")
            or ""
        ).strip()
        try:
            amount = int(
                source.get("delivery_amount")
                if source.get("delivery_amount") is not None
                else source.get("shipping")
                or 0
            )
        except (TypeError, ValueError):
            amount = 0
        currency = str(source.get("delivery_currency") or "BYN").strip() or "BYN"
        country = str(source.get("delivery_country") or "").strip()
        if not country:
            country = label
    norm_country, norm_label, norm_amount, norm_currency = _delivery_option_for_site_code(
        country or label or region_bot
    )
    if forced_region and not explicit_delivery:
        norm_country = forced_region
        norm_currency = _goods_currency_for_delivery_country(forced_region)
    if norm_country != region_bot:
        region_bot = norm_country
        lc_norm = _goods_currency_for_delivery_country(region_bot)
        for idx, line in enumerate(items_out):
            if isinstance(line, dict):
                line["line_currency"] = lc_norm
                if idx < len(raw_items) and isinstance(raw_items[idx], dict):
                    try:
                        q_reprice = int(line.get("qty") or 1)
                    except (TypeError, ValueError):
                        q_reprice = 1
                    new_price = _deep_link_item_unit_price(raw_items[idx], region_bot, q_reprice)
                    if new_price > 0:
                        line["price"] = int(new_price)
    country = norm_country
    if not label:
        opt = DELIVERY_OPTIONS.get(region_bot, DELIVERY_OPTIONS["by"])
        label, amount, currency = opt[0], int(opt[1]), str(opt[2])
    elif amount <= 0 and norm_amount > 0:
        amount = int(norm_amount)
    if not currency or currency.upper() not in ("BYN", "RUB"):
        currency = norm_currency
    if region_bot != "by":
        _, _, currency = DELIVERY_OPTIONS.get(region_bot, DELIVERY_OPTIONS["ru"])
    total_goods, _ = _cart_totals(items_out)
    bonus_applied = _loyalty_find_int(
        raw,
        (
            "bonusApplied",
            "bonus_applied",
            "bonusDiscount",
            "bonus_discount",
            "bonusesApplied",
            "bonuses_applied",
            "pointsDiscount",
            "points_discount",
            "loyaltyDiscount",
            "loyalty_discount",
        ),
        2,
    )
    bonus_spent = _loyalty_find_int(
        raw,
        (
            "bonusPointsSpent",
            "bonus_points_spent",
            "pointsSpent",
            "points_spent",
            "bonusesSpent",
            "bonuses_spent",
            "loyaltyPointsSpent",
            "loyalty_points_spent",
        ),
        2,
    )
    final_total = _loyalty_find_int(
        raw,
        (
            "finalTotal",
            "final_total",
            "payTotal",
            "pay_total",
            "amountToPay",
            "amount_to_pay",
            "totalAfterBonus",
            "total_after_bonus",
            "totalAfterBonuses",
            "total_after_bonuses",
            "grandTotalAfterBonus",
            "grand_total_after_bonus",
            "paidTotal",
            "paid_total",
        ),
        2,
    )
    total_raw = _loyalty_find_int(
        raw,
        (
            "total",
            "grandTotal",
            "grandTotalRub",
            "grand_total",
            "cartGrandTotal",
            "orderTotal",
            "order_total",
            "amountDue",
            "amount_due",
        ),
        3,
    )
    computed_total = int(total_goods) + int(amount)
    site_hint = _deep_link_raw_grand_total(raw)
    out = {
        "items": items_out,
        "delivery": {
            "country": region_bot,
            "label": label,
            "amount": int(amount),
            "currency": currency,
        },
        "external_id": str(raw.get("id") or external_id),
        "total": int(total_raw) if total_raw is not None else raw.get("total"),
        "site_grand_total_hint": int(site_hint),
    }
    if bonus_applied is not None and int(bonus_applied) > 0:
        out["bonus_applied"] = int(bonus_applied)
    if bonus_spent is not None and int(bonus_spent) > 0:
        out["bonus_points_spent"] = int(bonus_spent)
    if final_total is not None and int(final_total) >= 0:
        out["final_total"] = int(final_total)
        out["total"] = int(final_total)
    elif bonus_applied is not None and int(bonus_applied) > 0:
        out["total"] = max(0, int(computed_total) - int(bonus_applied))
        out["site_grand_total_hint"] = int(out["total"])
    elif total_raw is not None and 0 < int(total_raw) < int(computed_total):
        out["final_total"] = int(total_raw)
        out["total"] = int(total_raw)
        out["site_grand_total_hint"] = int(total_raw)
    return out


def _order_record_to_deep_link_shape(rec: dict, fallback_id: str) -> Optional[dict]:
    items = list(rec.get("items") or [])
    if not items:
        return None
    composed = {"items": items, "delivery": rec.get("delivery") or {}}
    return _normalize_deep_link_order(composed, str(rec.get("id") or fallback_id), None)


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
        return _normalize_deep_link_order(deepcopy(raw), str(order_id), None)
    return None


def _site_api_request_headers() -> dict:
    """Bearer для GET /api/order и /api/orders."""
    for secret in (ORDER_STATUS_UPDATE_SECRET, SYNC_API_SECRET):
        if secret:
            return {"Authorization": f"Bearer {secret}"}
    return {}


async def _fetch_site_orders_for_user(uid: int) -> List[dict]:
    user_id = int(uid or 0)
    if not user_id or not ORDER_USER_ORDERS_API_URL:
        return []
    safe_uid = urllib.parse.quote(str(user_id), safe="")
    if "{user_id}" in ORDER_USER_ORDERS_API_URL:
        url = ORDER_USER_ORDERS_API_URL.replace("{user_id}", safe_uid)
    else:
        sep = "&" if "?" in ORDER_USER_ORDERS_API_URL else "?"
        url = f"{ORDER_USER_ORDERS_API_URL}{sep}user_id={safe_uid}"
    headers = _site_api_request_headers()
    log = logging.getLogger(__name__)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(url, headers=headers or None) as resp:
                if resp.status != 200:
                    if resp.status == 401:
                        log.warning(
                            "site orders API HTTP 401 uid=%s — проверьте ILLUCARDS_ORDER_UPDATE_SECRET "
                            "на Render (тот же Bearer, что на Vercel).",
                            user_id,
                        )
                    else:
                        log.warning(
                            "site orders API HTTP %s uid=%s url=%s",
                            resp.status,
                            user_id,
                            url,
                        )
                    return []
                data = await resp.json()
    except Exception:
        log.exception("site orders API failed uid=%s", user_id)
        return []
    raw_orders = data.get("orders") if isinstance(data, dict) else data
    if not isinstance(raw_orders, list):
        return []
    return [x for x in raw_orders if isinstance(x, dict)]


async def _resolve_start_order_id_for_user(uid: int, token: str) -> Optional[str]:
    """order_3 → UUID по buyer_seq; 32 hex → UUID с дефисами."""
    tok = str(token or "").strip()
    if not tok:
        return None
    if re.fullmatch(r"\d{1,6}", tok):
        want = int(tok)
        for raw in await _fetch_site_orders_for_user(int(uid)):
            try:
                seq = int(raw.get("buyer_seq") or 0)
            except (TypeError, ValueError):
                seq = 0
            if seq != want:
                continue
            oid = str(raw.get("id") or raw.get("order_id") or "").strip()
            if oid:
                return oid
        return None
    if re.fullmatch(r"[a-f0-9]{32}", tok, re.I):
        t = tok.lower()
        return f"{t[0:8]}-{t[8:12]}-{t[12:16]}-{t[16:20]}-{t[20:32]}"
    return tok


async def _fetch_order_from_deep_link_api(
    order_id: str, products: Optional[List[dict]] = None
) -> Optional[dict]:
    template = ORDER_DEEP_LINK_API_URL
    if not template or "{id}" not in template:
        return None
    safe_id = urllib.parse.quote(str(order_id), safe="")
    url = template.replace("{id}", safe_id)
    log = logging.getLogger(__name__)
    headers: dict = {}
    headers.update(_site_api_request_headers())
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=12),
                headers=headers or None,
            ) as resp:
                if resp.status != 200:
                    if resp.status == 401:
                        log.warning(
                            "ORDER_DEEP_LINK_API_URL: HTTP 401 для заказа %s — "
                            "проверьте ILLUCARDS_ORDER_UPDATE_SECRET (тот же Bearer, что на сайте).",
                            order_id,
                        )
                    return None
                data = await resp.json()
    except Exception:
        log.exception("ORDER_DEEP_LINK_API_URL: не удалось загрузить заказ %s", order_id)
        return None
    if not isinstance(data, dict):
        return None
    if products is None:
        try:
            products = await load_products() or []
        except Exception:
            products = []
    return _normalize_deep_link_order(data, str(order_id), products)


def _deep_link_apply_catalog_prices(order: dict, products: List[dict]) -> None:
    """Сопоставить позиции заказа с каталогом (ref/название), цены с сайта не меняем."""
    if not isinstance(order, dict) or not products:
        return
    raw_items = order.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        return
    region_bot = str((order.get("delivery") or {}).get("country") or "by").strip().lower()
    if region_bot not in DELIVERY_OPTIONS:
        region_bot = "by"
    _reconcile_cart_lines_to_catalog(products, raw_items)


async def _fetch_order_for_deep_link(
    order_id: str, uid: Optional[int] = None
) -> Optional[dict]:
    products: List[dict] = []
    try:
        products = await load_products() or []
    except Exception:
        products = []
    oid = str(order_id or "").strip()
    if uid and oid:
        resolved = await _resolve_start_order_id_for_user(int(uid), oid)
        if resolved:
            oid = resolved
    o = _fetch_order_from_shared_memory(oid)
    if o:
        if products:
            _deep_link_apply_catalog_prices(o, products)
        return o
    o = await _fetch_order_from_deep_link_api(oid, products)
    if o:
        return o
    o = _find_user_order_snapshot_normalized(oid)
    if o and products:
        _deep_link_apply_catalog_prices(o, products)
    return o


def _site_order_pricing_hint_payload(norm: dict) -> dict:
    """Подсказки цен в USER_CART после перехода order_* с сайта."""
    d = norm.get("delivery") if isinstance(norm.get("delivery"), dict) else {}
    cc = str(d.get("country") or "by").strip().lower()
    g_cur = _goods_currency_for_delivery_country(cc)
    tot_raw = norm.get("final_total")
    if tot_raw is None:
        tot_raw = norm.get("total")
    if tot_raw is None:
        tot_raw = norm.get("site_grand_total_hint")
    try:
        tot_i = int(tot_raw) if tot_raw is not None else 0
    except (TypeError, ValueError):
        tot_i = 0
    payload: Dict[str, object] = {
        "items": list(norm.get("items") or []),
        "delivery": d,
        "deliveryCountry": cc,
    }
    if tot_i > 0:
        payload["grandTotal"] = tot_i
        payload["cartGrandTotal"] = tot_i
        if g_cur == "RUB":
            payload["grandTotalRub"] = tot_i
        else:
            payload["grandTotalByn"] = tot_i
    return payload


def _apply_site_order_norm_to_user_cart(uid: int, norm: dict) -> None:
    lines = deepcopy(list(norm.get("items") or []))
    if not lines:
        return
    d = norm.get("delivery") if isinstance(norm.get("delivery"), dict) else {}
    cc = str(d.get("country") or "by").strip().lower()
    if cc not in DELIVERY_OPTIONS:
        cc = "by"
    _remember_user_delivery_country(uid, cc)
    _cart_set_items_uid(uid, lines)
    _cart_apply_site_pricing_hints(uid, _site_order_pricing_hint_payload(norm))


def _resolve_live_bot(bot=None):
    if bot is not None:
        return bot
    return globals().get("_BOT_APP_BOT")


class _BotOnlyContext:
    """Минимальный context для HTTP sync (без Telegram Update)."""

    def __init__(self, bot):
        self.bot = bot
        self.application = type("_AppShim", (), {"bot_data": {}})()
        self.user_data: dict = {}


def _site_checkout_username_label(uid: int, username: Optional[str] = None) -> str:
    un = str(username or "").strip().lstrip("@")
    if not un:
        row = users_ensure(int(uid))
        un = str(row.get("username") or "").strip().lstrip("@")
    return un or f"id{int(uid)}"


def _find_unpaid_bot_order_by_external_id(uid: int, external_id: str) -> Optional[int]:
    ext = str(external_id or "").strip()
    if not ext or not uid:
        return None
    for oid_raw, rec in ORDERS.items():
        if not isinstance(rec, dict):
            continue
        try:
            oid = int(oid_raw)
        except (TypeError, ValueError):
            continue
        if int(rec.get("user_id") or 0) != int(uid):
            continue
        if rec.get("paid"):
            continue
        if str(rec.get("external_id") or "").strip() == ext:
            return oid
    return None


def _prepare_site_checkout_meta(uid: int, norm: dict) -> dict:
    meta: Dict[str, object] = {
        "loyalty_hint": deepcopy(norm),
        "external_id": str(norm.get("external_id") or norm.get("order_id") or "").strip(),
    }
    for src, dst in (
        ("bonus_applied", "bonus_applied"),
        ("bonus_points_spent", "bonus_points_spent"),
        ("bonusWillEarn", "bonus_will_earn"),
        ("bonus_will_earn", "bonus_will_earn"),
    ):
        if norm.get(src) is None:
            continue
        try:
            meta[dst] = int(norm.get(src) or 0)
        except (TypeError, ValueError):
            pass
    norm_items = norm.get("items")
    if isinstance(norm_items, list) and norm_items:
        meta["items"] = deepcopy(list(norm_items))
    return _merge_site_bonus_into_meta(uid, meta)


def _format_site_checkout_order_body(
    uid: int,
    norm: dict,
    products: Optional[List[dict]] = None,
    *,
    user_data: Optional[dict] = None,
) -> str:
    """Текст заказа с сайта — как в старом checkout (скрины)."""
    _apply_site_order_norm_to_user_cart(int(uid), norm)
    if products:
        lines = deepcopy(list(norm.get("items") or _cart_get_lines_uid(uid, user_data)))
        if lines:
            _reconcile_cart_lines_to_catalog(products, lines)
            _cart_set_items_uid(int(uid), lines)
    lines = deepcopy(list(norm.get("items") or _cart_get_lines_uid(uid, user_data)))
    if not lines:
        return ""
    d = norm.get("delivery") if isinstance(norm.get("delivery"), dict) else {}
    cc = str(d.get("country") or "by").strip().lower()
    if cc not in DELIVERY_OPTIONS:
        cc = _cart_price_region_for_user(int(uid), user_data or {})
    fin = _site_cart_checkout_finance(int(uid), lines, cc, user_data)
    meta = _prepare_site_checkout_meta(int(uid), norm)
    grand_show, pay_cur, _ = _resolve_site_confirm_pricing(
        int(uid), user_data or {}, lines, meta
    )
    d_label = fin["d_label"]
    d_amt = fin["d_amt"]
    d_cur = fin["d_cur"]
    inc = fin["inc"]
    out: List[str] = ["📦 Ваш заказ", ""]
    g_cur = fin["g_cur"]
    for x in lines:
        if not isinstance(x, dict):
            continue
        name_raw = str(x.get("name") or "—")
        name = name_raw[:200]
        if len(name_raw) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        lc = str(x.get("line_currency") or "").strip().upper()
        xcur = lc if lc in ("BYN", "RUB") else g_cur
        out.append(f"• {name} — {q} шт. × {p} {xcur} = {p * q} {xcur}")
    out.append("")
    if inc:
        out.append(f"🚚 Доставка: {d_label} (уже в сумме на сайте)")
    else:
        out.append(f"🚚 Доставка: {d_label} — {d_amt} {d_cur}")
    out.append(f"💰 Итого: {grand_show} {pay_cur}")
    s = "\n".join(out)
    if len(s) > 3900:
        s = s[:3890] + "…"
    return s


def _site_checkout_payment_message(uid: int, username: Optional[str] = None) -> str:
    un = _site_checkout_username_label(int(uid), username)
    return (
        "Выберите способ оплаты:\n\n"
        "💳 Карта -> 💵 Перевод -> ₿ Крипта\n"
        "💳 Оплата -> 📸 Скрин -> 🔎 Проверка -> ✅ Готово\n\n"
        f"Заказ {un}"
    )


async def _begin_site_checkout_order(
    context,
    uid: int,
    norm: dict,
    user_data: Optional[dict],
    *,
    username: Optional[str] = None,
) -> Optional[Tuple[int, int, str]]:
    """Создать заказ в боте и открыть шаг оплаты (без кнопки «Подтвердить»)."""
    ud = user_data if isinstance(user_data, dict) else {}
    _apply_site_order_norm_to_user_cart(int(uid), norm)
    lines = list(_cart_get_lines_uid(int(uid), ud))
    if not lines:
        return None
    meta = _prepare_site_checkout_meta(int(uid), norm)
    ext_id = str(meta.get("external_id") or "").strip()
    existing = _find_unpaid_bot_order_by_external_id(int(uid), ext_id) if ext_id else None
    if existing is not None:
        po = ORDERS.get(int(existing)) or {}
        tot, pay_cur = _order_payment_display(po)
        _set_awaiting_payment_order_id(int(uid), ud, int(existing))
        save_state()
        return int(existing), int(tot), str(pay_cur)
    total, pay_cur, drec = _resolve_site_confirm_pricing(int(uid), ud, lines, meta)
    goods_total, _ = _cart_totals(list(lines))
    dl_bonus_applied = 0
    dl_bonus_points_spent = 0
    lo_hint = deepcopy(norm)
    try:
        dl_bonus_applied = int(meta.get("bonus_applied") or norm.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        dl_bonus_applied = 0
    try:
        dl_bonus_points_spent = int(
            meta.get("bonus_points_spent") or norm.get("bonus_points_spent") or 0
        )
    except (TypeError, ValueError):
        dl_bonus_points_spent = 0
    if dl_bonus_points_spent <= 0:
        dl_bonus_points_spent = dl_bonus_applied

    class _CheckoutUser:
        def __init__(self, user_id: int, uname: Optional[str]):
            self.id = int(user_id)
            self.username = str(uname or "").strip().lstrip("@")

    user = _CheckoutUser(int(uid), username or _site_checkout_username_label(int(uid), username))
    oid = await _notify_admin_new_order(
        context,
        user,
        list(lines),
        int(total),
        deepcopy(drec),
        loyalty_hint_dict=lo_hint,
        bonus_applied=int(dl_bonus_applied),
        bonus_points_spent=int(dl_bonus_points_spent),
    )
    if oid is None:
        return None
    if ext_id:
        ORDERS[int(oid)]["external_id"] = ext_id
    ORDERS[int(oid)]["clear_cart_on_paid"] = True
    ORDERS[int(oid)]["total_goods"] = int(goods_total)
    ORDERS[int(oid)]["payment_currency"] = str(pay_cur)
    ORDERS[int(oid)]["payment_total_locked"] = True
    if int(dl_bonus_applied) > 0:
        ORDERS[int(oid)]["bonus_applied"] = int(dl_bonus_applied)
    if int(dl_bonus_points_spent) > 0:
        ORDERS[int(oid)]["bonus_points_spent"] = int(dl_bonus_points_spent)
    order_rec = {
        "id": str(oid),
        "items": deepcopy(list(lines)),
        "total": int(total),
        "total_goods": int(goods_total),
        "delivery": deepcopy(drec),
        "status": "В обработке",
    }
    if ext_id:
        order_rec["external_id"] = ext_id
    USER_ORDERS.setdefault(int(uid), []).append(order_rec)
    _set_awaiting_payment_order_id(int(uid), ud, int(oid))
    ud.pop("payment_pending_method", None)
    ud.pop("pending_order", None)
    _clear_site_pending_order(int(uid), ud)
    save_state()
    return int(oid), int(total), str(pay_cur)


async def _present_site_order_checkout_flow(
    *,
    bot=None,
    uid: int,
    norm: dict,
    username: Optional[str] = None,
    msg: Optional[Message] = None,
    context: Optional[ContextTypes.DEFAULT_TYPE] = None,
) -> bool:
    """Checkout с сайта: переход → состав заказа → выбор оплаты (как раньше)."""
    bot = _resolve_live_bot(bot)
    if not bot or not uid or not isinstance(norm, dict):
        return False
    log = logging.getLogger(__name__)
    products: List[dict] = []
    try:
        products = await load_products() or []
    except Exception:
        products = []
    ud = context.user_data if context is not None else {}
    body = _format_site_checkout_order_body(
        int(uid), norm, products, user_data=ud
    )
    if not body.strip():
        return False
    ctx = context if context is not None else _BotOnlyContext(bot)
    result = await _begin_site_checkout_order(
        ctx, int(uid), norm, ud, username=username
    )
    if not result:
        log.warning("site checkout: order not created uid=%s", uid)
        return False
    bot_oid, _total, _pay_cur = result

    async def _send(text: str, **kwargs) -> None:
        if msg is not None:
            await msg.reply_text(text, **kwargs)
        else:
            await bot.send_message(chat_id=int(uid), text=text, **kwargs)

    try:
        await _send(START_SITE_TRANSITION_TEXT, reply_markup=REPLY_KB)
        await _send(body, disable_web_page_preview=True)
        await _send(
            _site_checkout_payment_message(int(uid), username),
            reply_markup=_kb_payment_methods(bot_oid),
            disable_web_page_preview=True,
        )
        ext_id = str(norm.get("external_id") or norm.get("order_id") or "").strip()
        if ext_id:
            _SITE_CHECKOUT_PUSHED[f"{int(uid)}:{ext_id}"] = time.time()
        users_touch(int(uid), "payment")
        return True
    except Exception:
        log.exception("site checkout flow failed uid=%s", uid)
        return False


def _format_site_order_confirm_preview(
    uid: int, norm: dict, products: Optional[List[dict]] = None
) -> str:
    d = norm.get("delivery") if isinstance(norm.get("delivery"), dict) else {}
    cc = str(d.get("country") or "BY").strip()
    preview = _format_login_site_cart_pending_text(uid, cc, products or [])
    if not preview.strip():
        return ""
    return preview


async def _present_site_order_confirm_prompt(
    msg: Message, context: ContextTypes.DEFAULT_TYPE, uid: int, norm: dict
) -> None:
    """Черновик с сайта: «Подтвердить заказ» / «Отменить», без мгновенной оплаты."""
    ud = context.user_data
    ud.pop("awaiting_payment_order_id", None)
    ud.pop("payment_pending_method", None)
    ud.pop("deep_link_order_session", None)
    _apply_site_order_norm_to_user_cart(uid, norm)
    products: List[dict] = []
    try:
        products = await load_products() or []
    except Exception:
        products = []
    preview = _format_site_order_confirm_preview(uid, norm, products)
    if not preview.strip():
        await msg.reply_text(
            "Не удалось показать состав заказа. Попробуйте снова с сайта.",
            reply_markup=REPLY_KB,
        )
        return
    meta: Dict[str, object] = {
        "loyalty_hint": deepcopy(norm),
        "external_id": str(norm.get("external_id") or "").strip(),
    }
    try:
        b_ap = int(norm.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        b_ap = 0
    if b_ap > 0:
        meta["bonus_applied"] = b_ap
    try:
        b_ps = int(norm.get("bonus_points_spent") or 0)
    except (TypeError, ValueError):
        b_ps = 0
    if b_ps <= 0:
        b_ps = b_ap
    if b_ps > 0:
        meta["bonus_points_spent"] = b_ps
    norm_items = norm.get("items")
    if isinstance(norm_items, list) and norm_items:
        meta["items"] = deepcopy(list(norm_items))
    _persist_site_pending_order(int(uid), preview, meta, ud)
    body = f"{START_ORDER_FROM_SITE_HEADER}\n\n{preview}"
    if len(body) > 4000:
        body = body[:3990] + "…"
    await msg.reply_text(body, reply_markup=_kb_site_order_confirm_cancel())
    await msg.reply_text(START_WELCOME_MENU_TEXT, reply_markup=REPLY_KB)


def _site_status_from_bot_status(status: str) -> Optional[str]:
    return {
        "new": "new",
        "accepted": "confirmed",
        "shipped": "shipped",
        "done": "delivered",
        "canceled": "cancelled",
        "cancelled": "cancelled",
    }.get(str(status or "").strip().lower())


def _clear_user_cart_after_payment_proof(uid: int, oid: int, o: dict) -> None:
    """TG-корзина — после «Отправить заказ» (адрес + скрин), если clear_cart_on_paid."""
    if not uid:
        return
    if not o.get("clear_cart_on_paid"):
        return
    if o.get("_cart_cleared_on_proof"):
        return
    _cart_clear_uid(int(uid))
    _cart_clear_site_pricing_hints(int(uid))
    o["_cart_cleared_on_proof"] = True


async def _notify_site_cart_cleared_after_proof(uid: int, oid: int, o: dict) -> None:
    """Если задан ILLUCARDS_CART_CLEAR_ON_PROOF_URL — очистить корзину на сайте (после accept админом)."""
    if not ILLUCARDS_CART_CLEAR_ON_PROOF_URL:
        return
    if not uid:
        return
    headers = {"Content-Type": "application/json"}
    if ORDER_STATUS_UPDATE_SECRET:
        headers["Authorization"] = f"Bearer {ORDER_STATUS_UPDATE_SECRET}"
    ext = str(o.get("external_id") or "").strip()
    payload = {
        "telegramUserId": int(uid),
        "botOrderId": int(oid),
        "externalOrderId": ext or None,
        "event": "payment_pay_clicked",
    }
    log = logging.getLogger(__name__)
    try:
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                ILLUCARDS_CART_CLEAR_ON_PROOF_URL,
                json=payload,
                headers=headers,
            ) as resp:
                if resp.status >= 300:
                    body = await resp.text()
                    log.warning(
                        "POST cart clear webhook: uid=%s oid=%s HTTP %s %s",
                        uid,
                        oid,
                        resp.status,
                        body[:200],
                    )
    except Exception:
        log.exception("cart clear webhook failed uid=%s oid=%s", uid, oid)


async def _sync_site_order_status(order: dict) -> None:
    external_id = str(order.get("external_id") or "").strip()
    status = _site_status_from_bot_status(str(order.get("status") or ""))
    url = ORDER_STATUS_UPDATE_API_URL
    if not external_id or not status or not url:
        return
    headers = {"Content-Type": "application/json"}
    if ORDER_STATUS_UPDATE_SECRET:
        headers["Authorization"] = f"Bearer {ORDER_STATUS_UPDATE_SECRET}"
    payload = {"order_id": external_id, "status": status}
    log = logging.getLogger(__name__)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status >= 300:
                    body = await resp.text()
                    log.warning(
                        "IlluCards order status sync failed: order=%s status=%s HTTP %s %s",
                        external_id,
                        status,
                        resp.status,
                        body[:300],
                    )
    except Exception:
        log.exception("IlluCards order status sync failed: order=%s", external_id)


def _site_delivery_code_from_bot_order(order: dict) -> str:
    d = order.get("delivery") if isinstance(order.get("delivery"), dict) else {}
    raw = str((d or {}).get("country") or "").strip().lower()
    return {
        "by": "BY",
        "belarus": "BY",
        "ru": "RU",
        "russia": "RU",
        "ua": "UA",
        "ukraine": "UA",
        "ot": "OTHER",
        "other": "OTHER",
    }.get(raw, "BY")


def _site_order_items_from_bot_order(order: dict) -> List[dict]:
    out: List[dict] = []
    for row in list(order.get("items") or []):
        if not isinstance(row, dict):
            continue
        ref = str(row.get("ref") or row.get("id") or row.get("sku") or row.get("name") or "").strip()
        name = str(row.get("name") or row.get("title") or ref).strip()
        if not ref or not name:
            continue
        try:
            qty = max(1, int(row.get("qty") or row.get("quantity") or 1))
        except (TypeError, ValueError):
            qty = 1
        try:
            price = float(row.get("price") or row.get("priceByn") or 0)
        except (TypeError, ValueError):
            price = 0.0
        try:
            price_rub = float(row.get("price_rub") or row.get("priceRub") or 0)
        except (TypeError, ValueError):
            price_rub = 0.0
        out.append(
            {
                "id": ref[:120],
                "title": name[:300],
                "quantity": qty,
                "priceByn": price,
                "priceRub": round(price_rub),
            }
        )
    return out


async def _ensure_site_order_for_bot_order(order_id: int, order: dict) -> Optional[str]:
    """Для заказов, оформленных в Telegram: создать зеркало заказа на сайте."""
    existing = str(order.get("external_id") or "").strip()
    if existing:
        return existing
    if not ORDER_FROM_BOT_API_URL:
        return None
    uid = int(order.get("user_id") or 0)
    if not uid:
        return None
    items = _site_order_items_from_bot_order(order)
    if not items:
        return None
    headers = {"Content-Type": "application/json"}
    if ORDER_STATUS_UPDATE_SECRET:
        headers["Authorization"] = f"Bearer {ORDER_STATUS_UPDATE_SECRET}"
    payload = {
        "bot_order_id": int(order_id),
        "user_id": uid,
        "username": str(order.get("username") or "").strip(),
        "items": items,
        "total": _order_resolved_grand_total(order),
        "delivery": _site_delivery_code_from_bot_order(order),
        "status": "paid" if order.get("paid") else "new",
        "bonus_points_spent": int(order.get("bonus_points_spent") or 0),
        "bonus_discount": int(order.get("bonus_applied") or 0),
    }
    log = logging.getLogger(__name__)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
            async with session.post(
                ORDER_FROM_BOT_API_URL, json=payload, headers=headers
            ) as resp:
                data = await resp.json(content_type=None)
                if resp.status >= 300:
                    log.warning(
                        "IlluCards bot order import failed: bot_order=%s HTTP %s %s",
                        order_id,
                        resp.status,
                        str(data)[:300],
                    )
                    return None
    except Exception:
        log.exception("IlluCards bot order import failed: bot_order=%s", order_id)
        return None
    external_id = str(data.get("order_id") or "").strip() if isinstance(data, dict) else ""
    if external_id:
        order["external_id"] = external_id
    if isinstance(data, dict):
        note = _apply_site_loyalty_from_sync(uid, data)
        bot = globals().get("_BOT_APP_BOT")
        _schedule_loyalty_notify(bot, uid, note)
    return external_id or None


def _format_user_deep_link_order_message(order: dict) -> str:
    lines = list(order.get("items") or [])
    d = order.get("delivery") or {}
    label = str(d.get("label") or "—")
    amount = int(d.get("amount") or 0)
    country = str(d.get("country") or "")
    goods_total, _ = _cart_totals(lines)
    site_labels = {
        str(x.get("line_currency") or "").strip().upper()
        for x in lines
        if x.get("from_site") and str(x.get("line_currency") or "").strip().upper() in ("BYN", "RUB")
    }
    if site_labels and all(x.get("from_site") for x in lines) and len(site_labels) == 1:
        g_cur = site_labels.pop()
    else:
        g_cur = _goods_currency_for_delivery_country(country)
    out: List[str] = [
        "📦 Ваш заказ",
        "",
    ]
    for x in lines:
        name_raw = str(x.get("name") or "—")
        name = name_raw[:200]
        if len(name_raw) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        lc = str(x.get("line_currency") or "").strip().upper()
        xcur = lc if lc in ("BYN", "RUB") else g_cur
        out.append(f"• {name} — {q} шт. × {p} {xcur} = {sub} {xcur}")
    out.append("")
    out.append(f"🚚 Доставка: {label}")
    out.append("")
    hint = _coerce_card_price_int(order.get("site_grand_total_hint") or 0)
    computed = int(goods_total) + int(amount)
    total_show = computed
    try:
        bonus_applied = int(order.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        bonus_applied = 0
    try:
        final_total = int(order.get("final_total") or 0)
    except (TypeError, ValueError):
        final_total = 0
    # Подсказку с сайта берём только если она не меньше суммы товаров и близка к
    # «товары + доставка» — иначе в JSON часто попадает чужое поле (скидка, BYN и т.д.).
    if final_total > 0:
        total_show = int(final_total)
    elif bonus_applied > 0:
        total_show = max(0, int(computed) - int(bonus_applied))
    elif (
        hint > 0
        and _site_grand_covers_goods(hint, int(goods_total))
        and computed > 0
        and abs(int(hint) - int(computed)) <= max(100, int(computed) // 25)
    ):
        total_show = int(hint)
    if g_cur == "BYN":
        out.append(f"💰 Итого: {total_show} BYN")
    else:
        out.append(f"💰 Итого: {total_show} RUB")
    s = "\n".join(out)
    if len(s) > 4000:
        s = s[:3990] + "…"
    return s


def register_shared_deep_link_order(order_id: str, payload: dict) -> None:
    """Сохранить черновик заказа для ссылки /start order_<order_id> (JSON как из API: items, delivery, …)."""
    SHARED_DEEP_LINK_ORDERS[str(order_id)] = deepcopy(payload)


async def _create_order_from_synced_site_cart(
    msg: Message, context: ContextTypes.DEFAULT_TYPE, uid: int
) -> bool:
    lines = _cart_get_lines_uid(uid, context.user_data)
    if not lines:
        return False
    cc = str(USER_PREF_DELIVERY_COUNTRY.get(uid) or "by").strip().lower()
    if cc not in DELIVERY_OPTIONS:
        cc = "by"
    dlabel, damount, dcur = DELIVERY_OPTIONS.get(cc, DELIVERY_OPTIONS["by"])
    drec = {
        "country": cc,
        "label": dlabel,
        "amount": int(damount),
        "currency": dcur,
    }
    pay_cur = "BYN" if cc == "by" and dcur == "BYN" else "RUB"
    products_dl = await _get_products(context)
    if products_dl and lines:
        _reprice_lines_for_delivery(lines, products_dl, cc)
    goods_total, _ = _cart_totals(list(lines))
    site_gt = _cart_get_site_grand_total(uid, pay_cur)
    if site_gt and not _site_grand_covers_goods(int(site_gt), int(goods_total)):
        site_gt = None
    total = int(site_gt) if site_gt and int(site_gt) > 0 else int(goods_total)
    pe = _cart_get_site_loyalty_pending_earn(uid)
    lo_hint = {"bonusWillEarn": int(pe)} if pe is not None and int(pe) > 0 else None
    oid = await _notify_admin_new_order(
        context,
        msg.from_user,
        list(lines),
        int(total),
        deepcopy(drec),
        loyalty_hint_dict=lo_hint,
    )
    if oid is None:
        logging.getLogger(__name__).warning(
            "site cart fallback: failed to create admin order uid=%s items=%s",
            uid,
            len(lines),
        )
        return False
    order_rec = {
        "id": str(oid),
        "items": deepcopy(list(lines)),
        "total": int(total),
        "total_goods": int(goods_total),
        "delivery": deepcopy(drec),
        "status": "В обработке",
    }
    USER_ORDERS.setdefault(uid, []).append(order_rec)
    ORDERS[int(oid)]["clear_cart_on_paid"] = True
    ORDERS[int(oid)]["total_goods"] = int(goods_total)
    save_state()
    context.user_data.pop("pending_order", None)
    SITE_LOGIN_PENDING_ORDER.pop(uid, None)
    _set_awaiting_payment_order_id(uid, context.user_data, int(oid))
    context.user_data.pop("payment_pending_method", None)
    lo_est = (ORDERS.get(int(oid)) or {}).get("loyalty_earn_estimate")
    await _reply_payment_step(
        msg, int(total), pay_cur, loyalty_earn_estimate=lo_est, order_id=int(oid)
    )
    users_touch(uid, "payment")
    return True


async def _fetch_latest_new_site_order_id(uid: int) -> Optional[str]:
    """Последний заказ со статусом new на сайте (если sync/cart не дошёл до бота)."""
    user_id = int(uid or 0)
    if not user_id:
        return None
    log = logging.getLogger(__name__)
    for raw in await _fetch_site_orders_for_user(user_id):
        if not isinstance(raw, dict):
            continue
        st = str(raw.get("status") or "new").strip().lower()
        if st != "new":
            continue
        oid = str(raw.get("id") or raw.get("order_id") or "").strip()
        if oid:
            return oid
    return None


async def _resolve_pending_site_order_id(uid: int) -> Optional[str]:
    """order_id из памяти бота или последний new-заказ с сайта."""
    oid = (PENDING_SITE_ORDER_BY_USER.get(int(uid)) or "").strip()
    if oid:
        return oid
    return await _fetch_latest_new_site_order_id(int(uid))


async def _try_present_pending_site_order(
    msg: Message,
    context: ContextTypes.DEFAULT_TYPE,
    uid: int,
    *,
    username: Optional[str] = None,
) -> bool:
    """Показать заказ с сайта, если он уже синхронизирован или есть в памяти бота."""
    oid = await _resolve_pending_site_order_id(int(uid))
    if not oid:
        return False
    PENDING_SITE_ORDER_BY_USER[int(uid)] = oid
    push_key = f"{int(uid)}:{oid}"
    pushed_at = _SITE_CHECKOUT_PUSHED.get(push_key)
    recently_pushed = (
        isinstance(pushed_at, (int, float))
        and pushed_at > 0
        and (time.time() - float(pushed_at)) < 10 * 60
    )
    pend = _resolve_awaiting_payment_order_id(int(uid), context.user_data)
    if recently_pushed and pend is not None:
        await msg.reply_text(
            "Заказ уже в этом чате выше 👆 Выберите способ оплаты.",
            reply_markup=REPLY_KB,
        )
        return True
    order = await _fetch_order_for_deep_link(oid, int(uid))
    if not order:
        return False
    un = username or _site_checkout_username_label(int(uid), username)
    ok = await _present_site_order_checkout_flow(
        msg=msg,
        context=context,
        uid=int(uid),
        norm=order,
        username=un,
    )
    return bool(ok)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.from_user:
        return
    uid = msg.from_user.id
    users_touch(uid, "start")
    _register_login_username(uid, msg.from_user.username)
    args = context.args or []
    first = (args[0] or "").strip() if args else ""
    joined = " ".join(args).strip() if args else ""
    jl = joined.lower()
    fl = (first or "").strip().lower()
    is_web_login = bool(args) and (
        _is_web_login_start_payload(jl) or _is_web_login_start_payload(fl)
    )
    is_order_link = bool(args) and bool(_parse_order_id_from_start_args(list(args)))
    if not is_web_login and not is_order_link:
        await _reply_site_transition_notice(msg, uid)
    if args:
        if is_web_login:
            await _maybe_thank_first_telegram_auth(msg, uid)
            un = (msg.from_user.username or "").strip()
            wait_id = _login_wait_id_from_start_payload(jl) or _login_wait_id_from_start_payload(fl)
            if not wait_id:
                await msg.reply_text(
                    "Для входа на сайт нажмите «Войти через Telegram» в личном кабинете на "
                    f"{_illucards_site_base_url()}/account",
                    reply_markup=REPLY_KB,
                )
                return
            synced = await _sync_login_wait_to_site(uid, un, wait_id)
            if not synced:
                await msg.reply_text(
                    "Сервис входа временно недоступен. Попробуйте ещё раз через минуту.",
                    reply_markup=REPLY_KB,
                )
                return
            un = (msg.from_user.username or "").strip().lstrip("@") or None
            if await _try_present_pending_site_order(
                msg, context, uid, username=un
            ):
                return
            await msg.reply_text(
                "✅ Вход подтверждён.\n\n"
                "Нажмите кнопку ниже — откроется личный кабинет на сайте (вход уже выполнен).\n\n"
                "Или вернитесь на вкладку с сайтом — вход завершится автоматически.",
                reply_markup=_account_open_markup(wait_id, uid),
            )
            if PENDING_SITE_ORDER_BY_USER.get(int(uid)):
                await msg.reply_text(
                    "На сайте есть незавершённый заказ, но бот не смог его подтянуть.\n\n"
                    "Вернитесь в корзину на illucards.by и снова нажмите "
                    "«Оформить заказ через телеграм бот».",
                    reply_markup=REPLY_KB,
                )
            return
        oid = _parse_order_id_from_start_args(list(args))
        if not oid:
            oid = (PENDING_SITE_ORDER_BY_USER.get(uid) or "").strip() or None
        if oid:
            push_key = f"{uid}:{str(oid).strip()}"
            pushed_at = _SITE_CHECKOUT_PUSHED.get(push_key)
            recently_pushed = (
                isinstance(pushed_at, (int, float))
                and pushed_at > 0
                and (time.time() - float(pushed_at)) < 10 * 60
            )
            pend = _resolve_awaiting_payment_order_id(uid, context.user_data)
            if recently_pushed and pend is not None:
                await msg.reply_text(
                    "Заказ уже в этом чате выше 👆 Выберите способ оплаты.",
                    reply_markup=REPLY_KB,
                )
                return

            order = await _fetch_order_for_deep_link(oid, int(uid))
            if not order:
                await msg.reply_text(
                    "Не удалось связаться с сайтом или подтянуть заказ по ссылке.\n\n"
                    "Если на сайте включена защита API секретом, в боте должен быть тот же "
                    "ILLUCARDS_ORDER_UPDATE_SECRET, что в Vercel, и верный URL сайта "
                    "(ILLUCARDS_SITE_ORIGIN).\n\n"
                    "Откройте корзину на сайте снова или напишите в «Связь». "
                    "Ссылка: order_НОМЕР_ЗАКАЗА",
                    reply_markup=REPLY_KB,
                )
                await _send_start_intro_with_site_button(msg, uid, context.user_data)
                return
            await _maybe_thank_first_telegram_auth(msg, uid)
            un = (msg.from_user.username or "").strip().lstrip("@") or None
            ok = await _present_site_order_checkout_flow(
                msg=msg,
                context=context,
                uid=uid,
                norm=order,
                username=un,
            )
            if not ok:
                await msg.reply_text(
                    "Не удалось показать заказ. Попробуйте снова с сайта или напишите в «Связь».",
                    reply_markup=REPLY_KB,
                )
            return
        if first.lower() == "login":
            un = (msg.from_user.username or "").strip()
            code = _issue_login_code(uid, un)
            await _sync_login_code_to_site(code, uid, un)
            await msg.reply_text(
                _telegram_login_code_message(code),
                reply_markup=REPLY_KB,
            )
            return
        if jl in START_IGNORED_DEEP_LINK or fl in START_IGNORED_DEEP_LINK:
            if await _maybe_prompt_site_cart_confirmation(
                context.bot, uid, context.user_data, intro_kind="draft"
            ):
                await msg.reply_text(START_WELCOME_MENU_TEXT, reply_markup=REPLY_KB)
                return
            if await _create_order_from_synced_site_cart(msg, context, uid):
                return
            await _send_start_intro_with_site_button(msg, uid, context.user_data)
            return
        t = joined
        if not t:
            await _send_start_intro_with_site_button(msg, uid, context.user_data)
            return
        context.user_data.pop("deep_link_order_session", None)
        context.user_data["pending_order"] = t
        preview = _format_deep_link_order_preview(t)
        await _maybe_thank_first_telegram_auth(msg, uid)
        body = f"{START_ORDER_FROM_SITE_HEADER}\n\n{preview}"
        if len(body) > 4000:
            body = body[:3990] + "…"
        await msg.reply_text(body, reply_markup=_kb_site_order_confirm_cancel())
        await msg.reply_text(
            "Полную коллекцию можно открыть на сайте:",
            reply_markup=_illucards_site_open_markup(uid),
        )
        await msg.reply_text(START_WELCOME_MENU_TEXT, reply_markup=REPLY_KB)
        return
    if await _maybe_prompt_site_cart_confirmation(
        context.bot, uid, context.user_data, intro_kind="draft"
    ):
        await msg.reply_text(START_WELCOME_MENU_TEXT, reply_markup=REPLY_KB)
        return
    if await _create_order_from_synced_site_cart(msg, context, uid):
        return
    un_plain = (msg.from_user.username or "").strip().lstrip("@") or None
    if await _try_present_pending_site_order(
        msg, context, uid, username=un_plain
    ):
        return
    pending_oid = await _resolve_pending_site_order_id(int(uid))
    if pending_oid:
        buyer_seq = None
        for raw in await _fetch_site_orders_for_user(int(uid)):
            oid = str(raw.get("id") or raw.get("order_id") or "").strip()
            if oid == pending_oid:
                buyer_seq = _extract_buyer_seq_from_sources(raw)
                break
        tok = _site_order_start_token(pending_oid, buyer_seq)
        await msg.reply_text(
            "На сайте есть незавершённый заказ, но бот не смог его загрузить.\n\n"
            f"Отправьте команду:\n/start {tok}\n\n"
            "Если не помогло — снова нажмите «Оформить заказ через телеграм бот» на сайте.",
            reply_markup=REPLY_KB,
        )
        await _send_start_intro_with_site_button(msg, uid, context.user_data)
        return
    logging.getLogger(__name__).warning(
        "start without order payload and no synced cart: uid=%s args=%s cart=%s pending=%s",
        uid,
        args,
        len(_cart_get_lines_uid(uid, context.user_data)),
        bool(SITE_LOGIN_PENDING_ORDER.get(uid)),
    )
    await _send_start_intro_with_site_button(msg, uid, context.user_data)


def _inline_buttons_flat(message: Optional[Message]) -> List[object]:
    if not message or not getattr(message, "reply_markup", None):
        return []
    rm = message.reply_markup
    rows = getattr(rm, "inline_keyboard", None) or []
    out: List[object] = []
    for row in rows:
        for btn in row:
            out.append(btn)
    return out


def _infer_site_order_action_from_button_label(message: Optional[Message], data: str) -> Optional[str]:
    """Сайт может слать любой callback_data — смотрим текст нажатой inline-кнопки."""
    want = (data or "").strip()
    for btn in _inline_buttons_flat(message):
        if (getattr(btn, "callback_data", None) or "").strip() != want:
            continue
        text = str(getattr(btn, "text", None) or "")
        low = text.lower()
        if "отмен" in low or "❌" in text:
            return "cancel"
        # «Оплатить + 1200 RUB» — шаг оплаты, не confirm заказа с сайта.
        if "оплат" in low and ("+" in text or "руб" in low or "byn" in low):
            return None
        if "подтверд" in low and "заказ" in low:
            return "confirm"
        if "подтверд" in low and "оплат" not in low:
            return "confirm"
    return None


def _infer_site_order_action_from_keyboard_layout(message: Optional[Message], data: str) -> Optional[str]:
    """Две кнопки в ряд: первая — подтверждение, вторая — отмена (типичная вёрстка сайта/бота)."""
    flat = _inline_buttons_flat(message)
    if not flat:
        return None
    msg_text = (message.text or message.caption or "") if message else ""
    if _message_looks_like_admin_order_card(msg_text):
        return None
    want = (data or "").strip()
    idx = None
    for i, btn in enumerate(flat):
        if (getattr(btn, "callback_data", None) or "").strip() == want:
            idx = i
            break
    if idx is None:
        return None
    btn_text = str(getattr(flat[idx], "text", None) or "").lower()
    if _is_bot_checkout_payment_callback(want) or "оплат" in btn_text:
        return None
    if len(flat) == 2:
        return "cancel" if idx == 1 else "confirm"
    if len(flat) == 1:
        return "confirm"
    return None


def _message_looks_like_order_preview(text: str) -> bool:
    t = str(text or "")
    if not t.strip():
        return False
    if _message_looks_like_admin_order_card(t):
        return False
    if _message_looks_like_payment_step(t):
        return False
    markers = (
        "ID заказа:",
        "Итого:",
        "Доставка:",
        "Подтвердить заказ",
        "состав и доставку",
        "черновиком заказа",
        "шт. ×",
        " шт. × ",
        "Вы перешли с сайта",
        "Заказ с сайта",
        "Нажмите «Подтвердить заказ»",
        "подтянут в бота",
        "Вход на сайт подтверждён",
        "💰 Итого:",
    )
    return any(m in t for m in markers)


def _message_looks_like_payment_step(text: str) -> bool:
    """Шаг выбора способа оплаты (не превью заказа с сайта)."""
    t = str(text or "")
    if not t.strip():
        return False
    markers = (
        "Выберите способ оплаты",
        PAY_FLOW_STEPS,
        "👇 Нажмите кнопку ниже",
        "💳 Карта · 📱 Перевод",
        ORDER_AUTO_ACK,
        "Оплата по заказу",
    )
    return any(m in t for m in markers)


_BOT_CHECKOUT_CALLBACK_EXACT = frozenset(
    {"pay_card", "pay_transfer", "pay_crypto", "pay_cancel", "paid"}
)
_BOT_CHECKOUT_CALLBACK_PREFIXES = (
    "pay_",
    "chk:",
    "bo:",
    "co:",
    "dl:",
    "ta:",
    "vc:",
    "ic:",
    "dc:",
    "rm:",
    "cz:",
    "m:",
    "c:",
    "j:",
    "hp:",
    "pop:",
    "ccur:",
    "accept_",
    "cancel_",
    "sent_",
    "done_",
    "delmsg_",
    "oam:",
    "user_order_",
    "uos:",
    "open_order_",
    "sup:",
    "adm:",
    "confirm_payment_",
    "reject_payment_",
    "rcpt_",
    "dlco:",
    "dlca:",
)


def _is_bot_checkout_payment_callback(data: str) -> bool:
    """Кнопки оплаты и чекаута бота — не путать с confirm/cancel заказа с сайта."""
    d = (data or "").strip()
    if not d:
        return False
    if d in _BOT_CHECKOUT_CALLBACK_EXACT:
        return True
    if (
        _RE_PAY_METHOD_CB.match(d)
        or _RE_PAY_CANCEL_CB.match(d)
        or _RE_PAID_CB.match(d)
    ):
        return True
    low = d.lower()
    return any(low.startswith(p) for p in _BOT_CHECKOUT_CALLBACK_PREFIXES)


def _resolve_site_order_action(q: CallbackQuery) -> Optional[Tuple[str, Optional[str]]]:
    """confirm/cancel + external_id из callback_data, разметки кнопки или текста сообщения."""
    data = (q.data or "").strip()
    if not data:
        return None
    if _is_bot_checkout_payment_callback(data):
        return None
    parsed = _parse_site_order_callback_data(data)
    if parsed:
        return parsed
    msg = q.message
    label_action = _infer_site_order_action_from_button_label(msg, data)
    if not label_action:
        label_action = _infer_site_order_action_from_keyboard_layout(msg, data)
    if label_action:
        ext = _extract_order_id_from_telegram_message(
            (msg.text or msg.caption or "") if msg else ""
        )
        return (label_action, ext)
    msg_text = (msg.text or msg.caption or "") if msg else ""
    if msg and _message_looks_like_admin_order_card(msg_text):
        return None
    if msg and _message_looks_like_payment_step(msg_text):
        return None
    if msg and _message_looks_like_order_preview(msg_text):
        low = data.lower()
        if re.search(r"cancel|отмен|reject|decline", low):
            ext = _extract_order_id_from_telegram_message(msg_text)
            return ("cancel", ext)
        if re.search(r"confirm|submit|accept|approve|подтверд|оформ", low):
            ext = _extract_order_id_from_telegram_message(msg_text)
            return ("confirm", ext)
    return None


def _parse_site_order_callback_data(data: str) -> Optional[Tuple[str, Optional[str]]]:
    """(action confirm|cancel, external_id или None) из callback_data кнопки сайта/бота."""
    d = (data or "").strip()
    if not d:
        return None
    if d in _SITE_CONFIRM_EXTRA:
        return ("confirm", None)
    if d in _SITE_CANCEL_CALLBACKS:
        return ("cancel", None)
    m = _RE_SITE_ORDER_BUTTON.match(d)
    if m:
        kind = (m.group("kind") or "").lower()
        oid = (m.group("oid") or "").strip() or None
        if kind.startswith("confirm") or kind in ("site_confirm", "order_confirm"):
            return ("confirm", oid)
        return ("cancel", oid)
    m2 = _RE_SITE_ORDER_SHORT_BUTTON.match(d)
    if m2:
        return (str(m2.group("action") or "").lower(), (m2.group("oid") or "").strip() or None)
    if d in _SITE_CONFIRM_CALLBACKS:
        return ("confirm", None)
    if d in _SITE_CANCEL_CALLBACKS:
        return ("cancel", None)
    return _parse_site_order_callback_loose(d)


def _parse_site_order_callback_loose(data: str) -> Optional[Tuple[str, Optional[str]]]:
    """Если сайт прислал нестандартный callback, но в нём есть confirm/cancel и UUID заказа."""
    d = (data or "").strip()
    if not d or not re.search(r"confirm|cancel", d, re.IGNORECASE):
        return None
    uid_m = re.search(
        r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
        d,
        re.IGNORECASE,
    )
    oid = (uid_m.group(1) or "").strip() if uid_m else None
    low = d.lower()
    if "cancel" in low and "confirm" not in low:
        return ("cancel", oid)
    if "confirm" in low:
        return ("confirm", oid)
    return None


_RE_SITE_ORDER_CB_PATTERN = re.compile(
    r"^(?P<kind>confirm_order|cancel_order|site_confirm|order_confirm|site_cancel|order_cancel)"
    r"([:_].+)?$|^(?P<action>confirm|cancel)[:_][\w-]{8,}$|"
    r"^(submit_order|order_submit|checkout_confirm|confirm_checkout|checkout_cancel|cancel_checkout)$",
    re.IGNORECASE,
)


async def _try_handle_site_order_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ранний роутер: кнопки заказа с сайта (любой callback_data + подпись кнопки)."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    data = (q.data or "").strip()
    if _is_bot_checkout_payment_callback(data):
        return
    msg_text = (q.message.text or q.message.caption or "") if q.message else ""
    if _message_looks_like_admin_order_card(msg_text):
        return
    if _RE_SITE_ORDER_CB_PATTERN.match(data):
        return
    parsed = _resolve_site_order_action(q)
    if not parsed:
        return
    await on_site_order_button_callback(update, context, parsed=parsed)
    raise ApplicationHandlerStop


def _extract_order_id_from_telegram_message(text: str) -> Optional[str]:
    m = _RE_ORDER_ID_IN_MESSAGE.search(str(text or ""))
    return (m.group(1) or "").strip() if m else None


async def _hydrate_site_order_for_uid(
    uid: int,
    external_id: str,
    user_data: dict,
    context: ContextTypes.DEFAULT_TYPE,
) -> bool:
    """Подтянуть заказ с сайта в корзину и черновик (кнопки сайта с ID в callback или в тексте)."""
    ext = str(external_id or "").strip()
    if not uid or not ext:
        return False
    norm = await _fetch_order_for_deep_link(ext)
    if not norm or not norm.get("items"):
        return False
    _apply_site_order_norm_to_user_cart(uid, norm)
    try:
        products = await _get_products(context)
    except Exception:
        products = []
    preview = _format_site_order_confirm_preview(uid, norm, products)
    meta: Dict[str, object] = {
        "external_id": ext,
        "loyalty_hint": deepcopy(norm),
        "items": deepcopy(list(norm.get("items") or [])),
    }
    try:
        b_ap = int(norm.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        b_ap = 0
    if b_ap > 0:
        meta["bonus_applied"] = b_ap
    try:
        b_ps = int(norm.get("bonus_points_spent") or 0)
    except (TypeError, ValueError):
        b_ps = 0
    if b_ps <= 0:
        b_ps = b_ap
    if b_ps > 0:
        meta["bonus_points_spent"] = b_ps
    if not preview.strip():
        cc = str(USER_PREF_DELIVERY_COUNTRY.get(uid) or "by")
        preview = _format_login_site_cart_pending_text(uid, cc, products)
    if preview.strip():
        _persist_site_pending_order(uid, preview, meta, user_data)
    return bool(_cart_get_lines_uid(uid, user_data))


async def _run_site_cancel_order(
    q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, *, acked: bool
) -> None:
    ud = context.user_data
    uid_cb = int(q.from_user.id) if q.from_user else 0
    has_ud = bool((ud.get("pending_order") or "").strip())
    has_site = bool(uid_cb and _get_site_pending_preview(uid_cb))
    has_cart = bool(uid_cb and _cart_get_lines_uid(uid_cb, ud))
    if not has_ud and not has_site and not has_cart:
        await _answer_order_callback_stale(q, acked=acked)
        return
    ud.pop("pending_order", None)
    if uid_cb:
        _clear_site_pending_order(uid_cb, ud)
    try:
        await q.message.edit_text(MSG_ORDER_PREVIEW_CANCELLED, reply_markup=None)
    except Exception:
        await q.message.reply_text(MSG_ORDER_PREVIEW_CANCELLED)


async def on_site_order_button_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    parsed: Optional[Tuple[str, Optional[str]]] = None,
) -> None:
    """Подтвердить/отменить: confirm_order, confirm_order:<uuid>, кнопки с сайта."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if parsed is None:
        parsed = _resolve_site_order_action(q)
    if not parsed:
        return
    ud = context.user_data
    cqid = str(getattr(q, "id", "") or "")
    if cqid and ud.get(f"_handled_cq_{cqid}"):
        await _callback_ack(q)
        return
    if cqid:
        ud[f"_handled_cq_{cqid}"] = time.time()
    log = logging.getLogger(__name__)
    log.info("site order button uid=%s data=%r", q.from_user.id if q.from_user else 0, q.data)
    action, ext_id = parsed
    acked = await _callback_ack(q)
    uid = int(q.from_user.id) if q.from_user else 0
    if uid and action == "confirm":
        if not ext_id:
            ext_id = _extract_order_id_from_telegram_message(
                q.message.text or q.message.caption or ""
            )
        if ext_id and not _cart_get_lines_uid(uid, context.user_data):
            ok = await _hydrate_site_order_for_uid(
                uid, ext_id, context.user_data, context
            )
            if not ok:
                log.warning(
                    "site order hydrate failed uid=%s ext_id=%s cb=%r",
                    uid,
                    ext_id,
                    q.data,
                )
                if q.message:
                    await q.message.reply_text(MSG_SITE_ORDER_LOAD_FAIL)
                return
    if action == "confirm":
        try:
            await _run_site_confirm_order(q, context, acked=acked, log=log)
        except Exception:
            log.exception("confirm_order failed uid=%s data=%r", uid, q.data)
            if q.message:
                await q.message.reply_text(MSG_CONFIRM_ORDER_ERROR)
    else:
        await _run_site_cancel_order(q, context, acked=acked)


async def _run_site_confirm_order(
    q: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    acked: bool,
    log: logging.Logger,
) -> None:
    ud = context.user_data
    u = q.from_user
    uid_cb = int(u.id) if u else 0
    order_text = (ud.get("pending_order") or "").strip()
    if not order_text and uid_cb:
        order_text = _get_site_pending_preview(uid_cb)
    if q.message:
        msg_from_btn = (q.message.text or q.message.caption or "").strip()
        if msg_from_btn and _message_looks_like_order_preview(msg_from_btn):
            order_text = msg_from_btn
    lines_peek = _cart_get_lines_uid(uid_cb, ud) if uid_cb else []
    if uid_cb and not lines_peek:
        lines_peek = await _restore_cart_lines_for_confirm(uid_cb, ud, context)
    if not order_text and lines_peek:
        cc_peek = _cart_price_region_for_user(uid_cb, ud)
        try:
            products_peek = await _get_products(context)
        except Exception:
            products_peek = []
        order_text = _format_login_site_cart_pending_text(uid_cb, cc_peek, products_peek)
    if not order_text and not lines_peek:
        await _answer_order_callback_stale(q, acked=acked)
        return
    oid: Optional[int] = None
    total = 0
    pay_cur = "BYN"
    if uid_cb:
        ap = _user_state_get(uid_cb, "awaiting_proof")
        if ap is not None:
            try:
                po = ORDERS.get(int(ap))
            except (TypeError, ValueError):
                po = None
            if not po or int(po.get("user_id") or 0) != int(uid_cb) or po.get("paid"):
                _user_state_pop(uid_cb, "awaiting_proof")
            else:
                if acked and q.message:
                    await q.message.reply_text(MSG_PAY_NEED_PROOF_FIRST)
                else:
                    await _callback_ack(q, MSG_PAY_NEED_PROOF_FIRST, show_alert=True)
                return
        pend = _resolve_awaiting_payment_order_id(uid_cb, ud)
        if pend is not None:
            po = _ensure_order_in_orders(int(pend), uid_cb) or ORDERS.get(int(pend))
            if not po or int(po.get("user_id") or 0) != int(uid_cb) or po.get("paid"):
                _clear_awaiting_payment_order_id(uid_cb, ud)
            else:
                lines_peek = await _restore_cart_lines_for_confirm(uid_cb, ud, context)
                meta_peek = _merge_site_bonus_into_meta(
                    uid_cb, _get_site_pending_meta(uid_cb, ud)
                )
                await _resend_active_payment_step(
                    q,
                    uid_cb,
                    ud,
                    int(pend),
                    po,
                    preview_text=order_text,
                    lines=lines_peek,
                    meta=meta_peek,
                )
                save_state()
                return
        lines = await _restore_cart_lines_for_confirm(uid_cb, ud, context)
        if not lines:
            log.warning(
                "confirm_order: no cart lines uid=%s preview=%s meta_items=%s",
                uid_cb,
                bool(_get_site_pending_preview(uid_cb)),
                bool((_get_site_pending_meta(uid_cb, ud) or {}).get("items")),
            )
            await _answer_order_callback_stale(q, acked=acked)
            return
        meta = _merge_site_bonus_into_meta(uid_cb, _get_site_pending_meta(uid_cb, ud))
        total, pay_cur, drec = _resolve_site_confirm_pricing(
            uid_cb, ud, list(lines), meta
        )
        parsed_preview = _parse_grand_total_from_preview_text(order_text)
        if parsed_preview:
            total, pay_cur = int(parsed_preview[0]), str(parsed_preview[1])
        goods_total, _ = _cart_totals(list(lines))
        dl_bonus_applied = 0
        dl_bonus_points_spent = 0
        lo_hint = None
        ext_id = ""
        if isinstance(meta, dict):
            try:
                dl_bonus_applied = int(meta.get("bonus_applied") or 0)
            except (TypeError, ValueError):
                dl_bonus_applied = 0
            try:
                dl_bonus_points_spent = int(meta.get("bonus_points_spent") or 0)
            except (TypeError, ValueError):
                dl_bonus_points_spent = 0
            if dl_bonus_points_spent <= 0:
                dl_bonus_points_spent = dl_bonus_applied
            lh = meta.get("loyalty_hint")
            if isinstance(lh, dict):
                lo_hint = lh
                ext_id = str(meta.get("external_id") or lh.get("external_id") or "").strip()
        if lo_hint is None:
            pe = _cart_get_site_loyalty_pending_earn(uid_cb)
            if pe is not None and int(pe) > 0:
                lo_hint = {"bonusWillEarn": int(pe)}
        oid = await _notify_admin_new_order(
            context,
            u,
            list(lines),
            int(total),
            deepcopy(drec),
            loyalty_hint_dict=lo_hint,
            bonus_applied=int(dl_bonus_applied),
            bonus_points_spent=int(dl_bonus_points_spent),
        )
        if oid is None:
            await _notify_callback_issue(q, context)
            return
    else:
        await _answer_order_callback_stale(q, acked=acked)
        return
    order_rec = {
        "id": str(oid),
        "items": deepcopy(list(lines)),
        "total": int(total),
        "total_goods": int(goods_total),
        "delivery": deepcopy(drec),
        "status": "В обработке",
    }
    if ext_id:
        order_rec["external_id"] = ext_id
        ORDERS[int(oid)]["external_id"] = ext_id
    if int(dl_bonus_applied) > 0:
        order_rec["bonus_applied"] = int(dl_bonus_applied)
        ORDERS[int(oid)]["bonus_applied"] = int(dl_bonus_applied)
    if int(dl_bonus_points_spent) > 0:
        order_rec["bonus_points_spent"] = int(dl_bonus_points_spent)
        ORDERS[int(oid)]["bonus_points_spent"] = int(dl_bonus_points_spent)
    USER_ORDERS.setdefault(uid_cb, []).append(order_rec)
    ORDERS[int(oid)]["clear_cart_on_paid"] = True
    ORDERS[int(oid)]["total_goods"] = int(goods_total)
    ORDERS[int(oid)]["payment_currency"] = str(pay_cur)
    ORDERS[int(oid)]["payment_total_locked"] = True
    save_state()
    _set_awaiting_payment_order_id(uid_cb, ud, int(oid))
    ud.pop("payment_pending_method", None)
    ud.pop("pending_order", None)
    _clear_site_pending_order(uid_cb, ud)
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    try:
        lo_est = (ORDERS.get(int(oid)) or {}).get("loyalty_earn_estimate")
        await _reply_payment_step(
            q.message,
            int(total),
            pay_cur,
            loyalty_earn_estimate=lo_est,
            order_id=int(oid),
        )
        users_touch(uid_cb, "payment")
    except Exception:
        log.exception("confirm_order follow-up uid=%s oid=%s", uid_cb, oid)
        await q.message.reply_text(
            "Заказ создан, но не удалось открыть оплату. Нажмите «📋 Мои заказы» или /start."
        )
    log.info("confirm_order ok uid=%s oid=%s total=%s", uid_cb, oid, total)


async def on_callback_query_unhandled(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Любой callback без хендлера — снять loading и подсказать, если похоже на заказ с сайта."""
    q = update.callback_query
    if not q:
        return
    data = (q.data or "").strip()
    uid = int(q.from_user.id) if q.from_user else 0
    log = logging.getLogger(__name__)
    log.warning(
        "unhandled callback_query uid=%s data=%r chat_msg=%s",
        uid,
        data,
        q.message.message_id if q.message else None,
    )
    if _RE_PAY_METHOD_CB.match(data):
        await on_payment_method(update, context)
        return
    if _RE_PAY_CANCEL_CB.match(data):
        await on_payment_cancel(update, context)
        return
    if _RE_PAID_CB.match(data):
        await on_payment_paid(update, context)
        return
    if re.match(r"^(accept|sent|cancel|done|delmsg)_\d+$", data):
        await on_order_status_buttons(update, context)
        return
    if re.match(r"^confirm_payment_\d+$", data):
        await on_admin_confirm_payment(update, context)
        return
    if re.match(r"^reject_payment_\d+$", data):
        await on_admin_reject_payment(update, context)
        return
    if re.match(r"^oam:rep:\d+$", data):
        await on_order_admin_action(update, context)
        return
    if _is_bot_checkout_payment_callback(data):
        await _callback_ack(q)
        return
    msg_text = (q.message.text or q.message.caption or "") if q.message else ""
    if _message_looks_like_admin_order_card(msg_text):
        await _callback_ack(q)
        return
    parsed = _resolve_site_order_action(q)
    if parsed:
        await on_site_order_button_callback(update, context, parsed=parsed)
        return
    await _callback_ack(q)
    if not q.message:
        return
    if re.match(
        r"^(accept|sent|cancel|done|delmsg|confirm_payment|reject_payment|oam:rep)[:_]",
        data,
    ):
        return
    if _message_looks_like_order_preview(q.message.text or q.message.caption or ""):
        logging.getLogger(__name__).error(
            "order preview callback still unresolved uid=%s data=%r buttons=%s",
            uid,
            data,
            [
                (getattr(b, "text", None), getattr(b, "callback_data", None))
                for b in _inline_buttons_flat(q.message)
            ],
        )
        await q.message.reply_text(MSG_CALLBACK_UNKNOWN_ORDER_BUTTON)


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
        await _answer_order_callback_stale(q)
        return
    order = sess.get("order")
    if not isinstance(order, dict):
        await _answer_order_callback_stale(q)
        return
    u = q.from_user
    if not u:
        await _answer_order_callback_stale(q)
        return
    lines = deepcopy(list(order.get("items") or []))
    if not lines:
        await _answer_order_callback_stale(q)
        return
    products_dl = list(context.application.bot_data.get("products") or [])
    if not products_dl:
        products_dl = await load_products() or []
        if products_dl:
            context.application.bot_data["products"] = products_dl
    d = order.get("delivery") or {}
    d_label = str(d.get("label") or "—")
    try:
        d_amt = int(d.get("amount") if d.get("amount") is not None else 0)
    except (TypeError, ValueError):
        d_amt = 0
    d_cur = str(d.get("currency") or "BYN")
    d_cc = str(d.get("country") or "")
    if products_dl and d_cc in DELIVERY_OPTIONS:
        _reprice_lines_for_delivery(lines, products_dl, d_cc)
    goods_total, _ = _cart_totals(list(lines))
    drec = {
        "country": d_cc,
        "label": d_label,
        "amount": d_amt,
        "currency": d_cur,
    }
    order_rec = {
        "id": "0",
        "external_id": str(order.get("external_id") or ""),
        "items": deepcopy(list(lines)),
        "total": int(goods_total),
        "total_goods": int(goods_total),
        "delivery": deepcopy(drec),
        "status": "В обработке",
    }
    try:
        dl_bonus_applied = int(order.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        dl_bonus_applied = 0
    try:
        dl_bonus_points_spent = int(order.get("bonus_points_spent") or 0)
    except (TypeError, ValueError):
        dl_bonus_points_spent = 0
    if dl_bonus_points_spent <= 0:
        dl_bonus_points_spent = dl_bonus_applied
    if dl_bonus_applied > 0:
        order_rec["bonus_applied"] = int(dl_bonus_applied)
    if dl_bonus_points_spent > 0:
        order_rec["bonus_points_spent"] = int(dl_bonus_points_spent)
    baseline_dl = int(goods_total) + int(d_amt)
    try:
        external_total = int(round(float(order.get("total"))))
    except (TypeError, ValueError):
        external_total = 0
    if external_total > 0:
        if baseline_dl > 0:
            tol_dl = max(100, baseline_dl // 25)
            if abs(int(external_total) - int(baseline_dl)) <= tol_dl:
                order_rec["total"] = int(external_total)
            else:
                order_rec["total"] = int(baseline_dl)
        else:
            order_rec["total"] = int(external_total)
        pay_cur_dl = (
            "BYN" if str(drec.get("country") or "").strip().lower() == "by" else "RUB"
        )
    elif drec.get("country") == "by" and drec.get("currency") == "BYN":
        order_rec["total"] = int(goods_total) + int(d_amt)
        pay_cur_dl = "BYN"
    else:
        order_rec["total"] = int(goods_total) + int(d_amt)
        pay_cur_dl = "RUB"
    oid = await _notify_admin_new_order(
        context,
        u,
        list(lines),
        int(order_rec["total"]),
        deepcopy(drec),
        loyalty_hint_dict=order,
        bonus_applied=int(dl_bonus_applied),
        bonus_points_spent=int(dl_bonus_points_spent),
    )
    if oid is None:
        try:
            await q.answer(MSG_ORDER_SUBMIT_ADMIN_FAIL, show_alert=True)
        except Exception:
            pass
        return
    ud.pop("deep_link_order_session", None)
    order_rec["id"] = str(oid)
    USER_ORDERS.setdefault(u.id, []).append(order_rec)
    ORDERS[int(oid)]["clear_cart_on_paid"] = True
    ORDERS[int(oid)]["total_goods"] = int(goods_total)
    if int(dl_bonus_applied) > 0:
        ORDERS[int(oid)]["bonus_applied"] = int(dl_bonus_applied)
    if int(dl_bonus_points_spent) > 0:
        ORDERS[int(oid)]["bonus_points_spent"] = int(dl_bonus_points_spent)
    if order_rec.get("external_id"):
        ORDERS[int(oid)]["external_id"] = str(order_rec["external_id"])
    save_state()
    _set_awaiting_payment_order_id(u.id if u else 0, ud, int(oid))
    ud.pop("payment_pending_method", None)
    await q.answer()
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    tot = int(order_rec["total"])
    lo_est_dl = (ORDERS.get(int(oid)) or {}).get("loyalty_earn_estimate")
    await _reply_payment_step(
        q.message,
        tot,
        pay_cur_dl,
        loyalty_earn_estimate=lo_est_dl,
        order_id=int(oid),
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
        await _answer_order_callback_stale(q)
        return
    ud.pop("deep_link_order_session", None)
    await q.answer()
    try:
        await q.message.edit_text(MSG_ORDER_DEEPLINK_DECLINED, reply_markup=None)
    except Exception:
        await q.message.reply_text(MSG_ORDER_DEEPLINK_DECLINED)


def _kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🆕 Новые заказы", callback_data="adm:orders_new"),
                InlineKeyboardButton("📦 Заказы", callback_data="adm:orders_all"),
            ],
            [
                InlineKeyboardButton("🚚 Отправленные", callback_data="adm:orders_shipped"),
                InlineKeyboardButton("📈 Статистика", callback_data="adm:stats"),
            ],
        ],
    )


def _admin_orders_for_section(section: str) -> List[Tuple[int, dict]]:
    rows: List[Tuple[int, dict]] = []
    sec = str(section or "all").strip()
    for oid in sorted(ORDERS.keys(), key=lambda k: int(k)):
        o = ORDERS[oid]
        st = _norm_bot_order_status(str(o.get("status") or "new"))
        if sec == "new" and st in ("accepted", "shipped", "done", "canceled"):
            continue
        if sec == "shipped" and st != "shipped":
            continue
        rows.append((int(oid), o))
    return rows


def _admin_customer_rows_for_section(section: str = "all") -> List[Tuple[int, str, int, int]]:
    admin_orders = _admin_orders_for_section(section)
    grouped: Dict[int, dict] = {}
    for _oid, o in admin_orders:
        uid = int(o.get("user_id") or 0)
        if not uid:
            continue
        rec = grouped.setdefault(
            uid,
            {
                "name": _user_display_name(uid, o.get("username")),
                "count": 0,
                "last_ts": 0.0,
            },
        )
        rec["count"] = int(rec.get("count") or 0) + 1
        try:
            ts = float(o.get("created_at") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        rec["last_ts"] = max(float(rec.get("last_ts") or 0.0), ts)
    rows: List[Tuple[int, str, int, int]] = []
    for uid, rec in grouped.items():
        rows.append((int(uid), str(rec.get("name") or f"id {uid}"), int(rec.get("count") or 0), int(rec.get("last_ts") or 0)))
    rows.sort(key=lambda x: (-x[3], x[1].lower()))
    return rows


def _kb_admin_orders_list(section: str = "all") -> Optional[InlineKeyboardMarkup]:
    customers = _admin_customer_rows_for_section(section)
    if not customers:
        return None
    rows: List[List[InlineKeyboardButton]] = []
    sec = str(section or "all").strip() or "all"
    for uid, name, count, _last_ts in customers:
        label = f"{name} · заказов: {count}"
        if len(label) > 60:
            label = label[:57] + "…"
        rows.append([InlineKeyboardButton(label, callback_data=f"adm_user_{sec}_{uid}")])
    return InlineKeyboardMarkup(rows)


def _format_admin_stats() -> str:
    """Сводка по всем ORDERS за всё время."""
    revenue_byn = 0
    revenue_rub = 0
    by_status = {"new": 0, "accepted": 0, "shipped": 0, "done": 0, "canceled": 0}
    for o in ORDERS.values():
        st = _norm_bot_order_status(str(o.get("status") or "new"))
        if st not in by_status:
            st = "new"
        by_status[st] += 1
        tot = _order_resolved_grand_total(o)
        if st != "canceled":
            d_st = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
            if str(d_st.get("country") or "").strip().lower() == "by":
                revenue_byn += tot
            else:
                revenue_rub += tot
    n_all = len(ORDERS)
    lines = [
        "📈 Статистика",
        "",
        "📅 Период: всё время",
        f"📦 Всего заказов за всё время: {n_all}",
        f"💰 Выручка за всё время: {revenue_byn} BYN + {revenue_rub} RUB",
        "",
        "📊 По статусам:",
        f"🆕 Новые: {by_status['new']}",
        f"✅ Приняты: {by_status['accepted']}",
        f"🚚 Отправлены: {by_status['shipped']}",
        f"🏁 Завершены: {by_status['done']}",
        f"❌ Отменены: {by_status['canceled']}",
    ]
    return "\n".join(lines)


def _admin_user_orders_for_section(uid: int, section: str = "all") -> List[Tuple[int, dict]]:
    rows: List[Tuple[int, dict]] = []
    for oid, o in _admin_orders_for_section(section):
        if int(o.get("user_id") or 0) == int(uid):
            rows.append((int(oid), o))
    rows.sort(key=lambda x: float(x[1].get("created_at") or 0), reverse=True)
    return rows


def _format_admin_customer_detail(uid: int, section: str = "all") -> str:
    title_map = {
        "new": "новые/не принятые",
        "all": "все",
        "shipped": "отправленные",
    }
    orders = _admin_user_orders_for_section(uid, section)
    name = _user_display_name(uid)
    if orders:
        name = _user_display_name(uid, orders[0][1].get("username"))
    parts: List[str] = [
        f"👤 Клиент: {name}",
        f"🆔 id: {int(uid)}",
        f"📦 Заказы: {title_map.get(str(section or 'all'), 'все')}",
        "",
    ]
    if not orders:
        parts.append("Заказов в этом разделе нет.")
    else:
        for oid, o in orders[:25]:
            tot = _order_resolved_grand_total(o)
            cur = _order_line_currency_from_delivery(
                o.get("delivery") if isinstance(o.get("delivery"), dict) else None
            )
            st_ru = _order_status_label_ru(_norm_bot_order_status(str(o.get("status") or "new")))
            try:
                ts = float(o.get("created_at") or 0)
            except (TypeError, ValueError):
                ts = 0.0
            tss = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts > 0 else "—"
            proof_mark = "—"
            if o.get("paid"):
                proof_mark = "✅ оплачен"
            elif o.get("payment_proof_submitted"):
                proof_mark = "📸 чек получен"
            else:
                proof_mark = "⏳ ждём чек"
            parts.append(f"#{oid} — {tot} {cur} — {st_ru} — {tss} — {proof_mark}")
    parts.extend(["", _format_user_messages_for_admin(uid)])
    body = "\n".join(parts)
    if len(body) > 4090:
        body = body[:4086] + "…"
    return body


def _kb_admin_customer_detail(uid: int, section: str = "all") -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for oid, _o in _admin_user_orders_for_section(uid, section)[:20]:
        rows.append([InlineKeyboardButton(f"📦 Открыть заказ #{oid}", callback_data=f"open_order_{oid}")])
    rows.append([InlineKeyboardButton("💬 Ответить клиенту", callback_data=f"sup:rep:{int(uid)}")])
    return InlineKeyboardMarkup(rows)


async def myid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать Telegram user id (для настройки TELEGRAM_ADMIN_ID на Render)."""
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return
    uname = f"@{u.username}" if u.username else "—"
    await msg.reply_text(
        f"Ваш Telegram ID: `{u.id}`\n"
        f"Username: {uname}\n\n"
        "Этот id нужно указать в Render → Environment:\n"
        "`TELEGRAM_ADMIN_ID`, `TELEGRAM_ORDER_NOTIFY_ID`, `TELEGRAM_ADMIN_CHAT_ID`"
    )


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return
    if not is_admin(u.id):
        await msg.reply_text(
            f"{ADMIN_ACCESS_DENIED}\n\n"
            f"Ваш Telegram ID: {u.id}\n"
            "Отправьте /myid — скопируйте id и добавьте в Render → Environment "
            "(TELEGRAM_ADMIN_ID и TELEGRAM_ORDER_NOTIFY_ID), затем перезапустите сервис."
        )
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
        await msg.reply_text(
            f"{ADMIN_ACCESS_DENIED}\n\n"
            f"Ваш Telegram ID: {u.id}\n"
            "Отправьте /myid — скопируйте id и добавьте в Render → Environment "
            "(TELEGRAM_ADMIN_ID и TELEGRAM_ORDER_NOTIFY_ID), затем перезапустите сервис."
        )
        return
    args = context.args or []
    if len(args) < 2:
        await msg.reply_text(
            "Формат: /say TELEGRAM_USER_ID текст, который увидит клиент."
        )
        return
    try:
        target_id = int(args[0])
    except (TypeError, ValueError):
        await msg.reply_text(MSG_ADMIN_SAY_BAD_ID)
        return
    text = " ".join(args[1:]).strip()
    if not text:
        await msg.reply_text(MSG_ADMIN_SAY_NO_TEXT)
        return
    ok = await _send_customer_plain(context.bot, target_id, text)
    if not ok:
        await msg.reply_text(MSG_ADMIN_SAY_FAIL)
        return
    await msg.reply_text(MSG_ADMIN_SAY_OK)


async def on_admin_panel_action(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^adm:(orders_new|orders_all|orders_shipped|stats)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        return
    action = m.group(1)
    await q.answer()
    if action.startswith("orders_"):
        section = {
            "orders_new": "new",
            "orders_all": "all",
            "orders_shipped": "shipped",
        }.get(action, "all")
        title = {
            "new": "🆕 Новые заказы",
            "all": "📦 Заказы",
            "shipped": "🚚 Отправленные",
        }.get(section, "📦 Заказы")
        admin_orders = _admin_orders_for_section(section)
        if not admin_orders:
            await q.message.reply_text(
                f"{title}\n\nПока нет заказов\n\n"
                "Как только клиент оформит покупку — заказ появится здесь ✨"
            )
        else:
            body_lines: List[str] = [title, "", "Выберите клиента 👇", ""]
            for uid, name, count, _last_ts in _admin_customer_rows_for_section(section):
                body_lines.append(f"👤 {name} — заказов: {count}")
            text = "\n".join(body_lines)
            if len(text) > 3500:
                text = text[:3490] + "…"
            await q.message.reply_text(
                text,
                reply_markup=_kb_admin_orders_list(section),
            )
    else:
        await q.message.reply_text(_format_admin_stats())


async def on_admin_user_messages(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^adm_user_(new|all|shipped)_(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        return
    try:
        section = m.group(1)
        uid = int(m.group(2))
    except ValueError:
        await q.answer()
        return
    await q.answer()
    await q.message.reply_text(
        _format_admin_customer_detail(uid, section),
        reply_markup=_kb_admin_customer_detail(uid, section),
        disable_web_page_preview=True,
    )


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
    o = _restore_order_for_admin(q, oid)
    if not o:
        await _reply_admin_order_stale(q, oid)
        raise ApplicationHandlerStop
    await q.answer()
    st = str(o.get("status") or "new")
    await q.message.reply_text(
        _format_admin_order_detail_text(oid, o),
        reply_markup=_kb_order_admin_actions(oid, st),
    )
    raise ApplicationHandlerStop


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
    o = _restore_order_for_admin(q, oid)
    if not o:
        await _reply_admin_order_stale(q, oid)
        raise ApplicationHandlerStop
    context.user_data.pop("reply_support_user_id", None)
    context.user_data["reply_to"] = oid
    await q.answer()
    cust = int(o.get("user_id") or 0)
    if cust:
        await _send_customer_plain(context.bot, cust, ADMIN_TYPING_NOTICE)
    await q.message.reply_text(MSG_REPLY_MODE_ACTIVE)
    raise ApplicationHandlerStop


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
    await q.message.reply_text(MSG_REPLY_MODE_ACTIVE)


async def on_user_order_open(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    raw = (q.data or "").strip()
    m = re.match(r"^user_order_(\d+)$", raw)
    if m:
        try:
            oid = int(m.group(1))
        except ValueError:
            return
        uid = q.from_user.id
        o = ORDERS.get(oid)
        admin_chat_id = _resolve_admin_chat_id()
        if not o:
            return
        if (
            not _order_belongs_to_telegram_user(o, uid)
            and not is_admin(uid)
            and (admin_chat_id is None or int(q.message.chat_id) != int(admin_chat_id))
        ):
            return
        await q.answer()
        await q.message.reply_text(_format_user_order_detail(oid, o))
        return
    m2 = re.match(r"^uos:([a-f0-9]{12})$", raw, re.IGNORECASE)
    if not m2:
        return
    uid = q.from_user.id
    rec = _find_user_site_order_by_token(uid, m2.group(1))
    if not rec:
        await _refresh_user_site_orders_from_site(uid)
        rec = _find_user_site_order_by_token(uid, m2.group(1))
    if not rec:
        try:
            await q.answer("Заказ не найден или устарел", show_alert=False)
        except Exception:
            pass
        return
    await q.answer()
    disp = str(rec.get("external_id") or rec.get("id") or "—")
    await q.message.reply_text(_format_user_order_detail(disp, rec))


async def on_order_status_buttons(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^(accept|sent|cancel|done|delmsg)_(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        try:
            await q.answer("Эта кнопка только для администратора.", show_alert=True)
        except Exception:
            pass
        return
    action, oid_s = m.group(1), m.group(2)
    try:
        oid = int(oid_s)
    except ValueError:
        await q.answer()
        return
    o = _restore_order_for_admin(q, oid)
    if not o:
        await _reply_admin_order_stale(q, oid)
        raise ApplicationHandlerStop
    if text := ((q.message.text or q.message.caption or "").strip() if q.message else ""):
        if _message_looks_like_admin_order_card(text) and not list(o.get("items") or []):
            o = _enrich_order_from_admin_card_text(o, oid, text)
    if action == "delmsg":
        await _admin_delete_order_chat_message(context, q, oid)
        raise ApplicationHandlerStop
    want = {
        "accept": "accepted",
        "sent": "shipped",
        "done": "done",
        "cancel": "canceled",
    }.get(action)
    if not want:
        return
    st_norm = _norm_bot_order_status(str(o.get("status") or "new"))
    if st_norm == want:
        try:
            await q.answer("Статус уже такой.", show_alert=False)
        except Exception:
            pass
        await _refresh_admin_order_message(context, oid)
        return
    o["status"] = want
    save_state()
    if want in ("done", "canceled"):
        cuid = int(o.get("user_id") or 0)
        if cuid:
            _clear_postpaid_thread_if_matches(cuid, oid)
    if not str(o.get("external_id") or "").strip() and o.get("paid"):
        await _ensure_site_order_for_bot_order(oid, o)
    await _sync_site_order_status(o)
    await q.answer(MSG_ORDER_STATUS_UPDATED, show_alert=False)
    notice = _format_customer_order_status_notice(oid, str(o.get("status") or ""))
    await _notify_order_customer(context, o, notice)
    await _refresh_admin_order_message(context, oid)
    raise ApplicationHandlerStop


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
        await msg.reply_text(MSG_CATALOG_LOAD_FAIL)
        return
    context.bot_data["products"] = cards
    context.bot_data["illucards_synced_at"] = time.time()

    categories = _category_names(cards)
    if not categories:
        await msg.reply_text(MSG_CATALOG_EMPTY_SECTION)
        return
    log.info("Каталог: %d разделов, ожидаем выбор валюты", len(categories))
    if msg.from_user:
        users_touch(msg.from_user.id, "catalog")
    await msg.reply_text(
        CATALOG_CURRENCY_PICK_TEXT,
        reply_markup=_kb_catalog_currency_pick(),
    )


async def send_tinder_mode(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Tinder по всему каталогу (cat_tok all); вызывается из кода, не с reply-клавиатуры."""
    msg = update.effective_message
    if not msg:
        return
    products = await _get_products(context)
    if not products:
        await msg.reply_text(MSG_CATALOG_LOAD_FAIL)
        return
    in_scope = list(products)
    ok = await _tinder_start_deck(
        context, int(msg.chat_id), in_scope, products, "all"
    )
    if not ok:
        await msg.reply_text(MSG_VIEWER_FAIL)
        return
    await msg.reply_text(MSG_PROMO_PARTICIPATION)


async def _deliver_home_promotions_if_any(
    message: Message, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    """Баннеры витрины (слайдер под категориями на сайте), пришедшие через синк или JSON URL."""
    promos = home_promotions_for_ui()
    if not promos:
        return False
    n = len(promos)
    for idx in range(n):
        item = promos[idx]
        img = str(item.get("image") or "").strip()
        if not img:
            continue
        title = str(item.get("title") or "").strip()
        cap = f"🔥 Акции с витрины сайта — {idx + 1}/{n}"
        if title:
            cap = f"{title}\n\n{cap}"
        kb = _home_promo_kb(idx, n, str(item.get("link") or ""))
        await message.reply_photo(
            photo=img,
            caption=cap[:1024],
            reply_markup=kb,
        )
        if message.from_user:
            users_touch(message.from_user.id, "promo_home")
        return True
    rows: List[List[InlineKeyboardButton]] = []
    for idx, item in enumerate(promos[:8]):
        u = _promo_click_absolute_url(str(item.get("link") or ""))
        if not u:
            continue
        lab = str(item.get("title") or f"Акция {idx + 1}")[:64]
        rows.append([InlineKeyboardButton(lab, url=u)])
    if not rows:
        return False
    cap = (
        "🔥 Акции с витрины сайта\n\n"
        "Картинок в данных нет — откройте ссылку на сайте 👇"
    )
    await message.reply_text(cap, reply_markup=InlineKeyboardMarkup(rows))
    if message.from_user:
        users_touch(message.from_user.id, "promo_home")
    return True


async def on_home_promo_nav(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """hp:{i} — листание баннеров акций главной."""
    log = logging.getLogger(__name__)
    q = update.callback_query
    if not q or not q.message or not q.data:
        return
    m = re.match(r"^hp:(\d+)$", (q.data or "").strip())
    if not m:
        return
    promos = home_promotions_for_ui()
    if not promos:
        try:
            await q.answer("Баннеры не загружены", show_alert=False)
        except Exception:
            pass
        return
    n = len(promos)
    try:
        idx = int(m.group(1)) % n
    except ValueError:
        return
    item = promos[idx]
    img = str(item.get("image") or "").strip()
    if not img:
        await q.answer()
        return
    cap = f"🔥 Акции с витрины сайта — {idx + 1}/{n}"
    kb = _home_promo_kb(idx, n, str(item.get("link") or ""))
    media = InputMediaPhoto(media=img, caption=cap)
    try:
        await q.message.edit_media(media=media, reply_markup=kb)
    except Exception:
        log.info("home promo: edit_media fallback — новое сообщение")
        try:
            await q.message.reply_photo(photo=img, caption=cap, reply_markup=kb)
        except Exception:
            log.exception("home promo: reply_photo")
    await q.answer()


async def _refresh_vitrina_promos_then_try_deliver(
    message: Message, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    """Обновить кэш баннеров (JSON с сайта по умолчанию или env) и показать витрину."""
    app = context.application
    try:
        await refresh_home_page_promotions_cache(app)
    except Exception:
        logging.getLogger(__name__).exception(
            "Акции витрины: ошибка refresh_home_page_promotions_cache"
        )
    return await _deliver_home_promotions_if_any(message, context)


async def send_popular_deck(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """🔥 Акции — только витрина сайта (слайдер под категориями), не «горячая цена»."""
    msg = update.effective_message
    if not msg:
        return
    if await _refresh_vitrina_promos_then_try_deliver(msg, context):
        return
    await msg.reply_text(MSG_NO_VITRINA_PROMOS)


async def send_favorites_deck(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """💚 Избранное — карточки из USER_FAVORITES (в т.ч. после синка с сайта)."""
    msg = update.effective_message
    if not msg or not msg.from_user:
        return
    uid = int(msg.from_user.id)
    products = await load_products()
    if products:
        context.application.bot_data["products"] = products
        context.application.bot_data["illucards_synced_at"] = time.time()
    else:
        products = await _get_products(context)
    if not products:
        await msg.reply_text(MSG_CATALOG_LOAD_FAIL)
        return
    await _refresh_user_state_from_site(uid)
    refs = set(_favorites_get_refs_uid(uid, context.user_data))
    if not refs:
        await msg.reply_text(
            "💚 В избранном пока пусто.\n\n"
            "Если вы добавили карточки на сайте, обновите страницу после привязки Telegram.\n\n"
            "Или добавьте карточки в избранное прямо в каталоге бота."
        )
        return
    in_scope: List[dict] = []
    for i, p in enumerate(products):
        ref = _product_ref_for_callback(p, i)
        if ref in refs:
            in_scope.append(p)
    if not in_scope:
        await msg.reply_text(
            "В избранном есть позиции, но совпадений с каталогом бота нет.\n\n"
            "Откройте каталог или дождитесь синхронизации с сайта."
        )
        return
    ok = await _tinder_start_deck(
        context, int(msg.chat_id), in_scope, products, "all"
    )
    if not ok:
        await msg.reply_text(MSG_VIEWER_FAIL)
        return
    users_touch(uid, "favorites")


async def send_random_card(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """🎁 Случайная карточка — одна позиция в режиме просмотра."""
    msg = update.effective_message
    if not msg:
        return
    products = await _get_products(context)
    if not products:
        await msg.reply_text(MSG_CATALOG_LOAD_FAIL)
        return
    in_scope = [random.choice(products)]
    ok = await _tinder_start_deck(
        context, int(msg.chat_id), in_scope, products, "all"
    )
    if not ok:
        await msg.reply_text(MSG_VIEWER_FAIL)


async def send_promo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /promo — то же, что кнопка «🔥 Акции»: только витрина сайта."""
    await send_popular_deck(update, context)


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
        await q.edit_message_text(MSG_CATALOG_LOAD_FAIL)
        return
    cats = _category_names(products)
    if not cats:
        await q.edit_message_text(MSG_CATALOG_EMPTY_SECTION)
        return
    await q.edit_message_text(CATALOG_INTRO_TEXT, reply_markup=_kb_categories(cats))
    if q.from_user:
        users_touch(q.from_user.id, "catalog")


async def on_menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or (q.data or "") != "m:0":
        return
    await q.answer()
    await q.edit_message_text(
        CATALOG_CURRENCY_PICK_TEXT,
        reply_markup=_kb_catalog_currency_pick(),
    )


async def on_catalog_currency_pick(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    m = re.match(r"^ccur:(by|ru)$", (q.data or "").strip())
    if not m:
        return
    code = m.group(1)
    ud = context.user_data
    # Валюта каталога влияет только на отображение цен, не запускает оформление.
    ud["catalog_currency_country"] = code
    _remember_user_delivery_country(int(q.from_user.id) if q.from_user else 0, code)
    ud["preferred_delivery_country"] = code
    uid_cc = int(q.from_user.id) if q.from_user else 0
    if uid_cc:
        products = await _get_products(context)
        if products:
            lines_cc = _cart_get_lines_uid(uid_cc, ud)
            for ln in lines_cc:
                ln.pop("from_site", None)
                ln.pop("line_currency", None)
            _reprice_lines_for_delivery(
                lines_cc, products, code, respect_site_lines=False
            )
            _cart_set_items_uid(uid_cc, lines_cc)
    await q.answer("Цены обновлены")
    await _edit_to_categories(q, context)


async def on_popular_inline(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """🔥 Акции из inline-кнопки — только витрина сайта (слайдер под категориями)."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    if (q.data or "").strip() != "pop:0":
        return
    await q.answer()
    if await _refresh_vitrina_promos_then_try_deliver(q.message, context):
        return
    await q.message.reply_text(MSG_NO_VITRINA_PROMOS)


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
                await q.message.reply_text(MSG_CALLBACK_CATEGORY_INVALID)
            return
        cat_tok = str(ci)
        base, cat_label, _ = _filter_wizard(products, cats, cat_tok, "all")
    if not cat_label or not base:
        await _notify_callback_issue(q, context)
        return
    await q.answer()
    if _needs_rarity_step(base):
        hdr = (
            f"✅ Вы выбрали категорию: {cat_label}\n\n"
            "Теперь выберите редкость 👇"
        )
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
    ud = context.user_data
    lines = _cart_get_lines_uid(uid, ud)
    products = list(context.application.bot_data.get("products") or [])
    if not products:
        products = await load_products() or []
        if products:
            context.application.bot_data["products"] = products
    if products:
        _reprice_lines_for_delivery(
            lines, products, _cart_price_region_for_user(uid, ud)
        )
        _cart_set_items_uid(uid, lines)
    t = _format_cart_message(lines, ud, cart_uid=uid)
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
        try:
            await q.answer(MSG_ADD_TO_CART_STALE, show_alert=True)
        except Exception:
            if q.message:
                await q.message.reply_text(MSG_ADD_TO_CART_STALE)
        return
    uid = q.from_user.id
    _ensure_user_cart(uid, context.user_data)
    gix = _global_product_index(products, product)
    r = _product_ref_for_callback(product, gix)
    reg = _cart_price_region_for_user(uid, context.user_data)
    px = _product_unit_price_for_delivery(product, reg)
    _cart_add_line_uid(
        uid,
        context.user_data,
        r,
        product,
        product.get("name") or "—",
        px,
    )
    lines = _cart_get_lines_uid(uid, context.user_data)
    tot, npos = _cart_totals(lines)
    nlines = len(lines)
    cur = _goods_currency_for_delivery_country(reg)
    short = f"{tot} {cur}, шт. {npos}"
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
    reg_ic = _cart_price_region_for_user(uid, context.user_data)
    px_ic = _product_unit_price_for_delivery(product, reg_ic)
    _cart_add_line_uid(
        uid,
        context.user_data,
        r,
        product,
        product.get("name") or "—",
        px_ic,
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
        _clear_site_pending_order(uid, ud)
    if uid:
        users_touch(uid, "cart")
    await q.answer(MSG_CART_CLEARED_TOAST, show_alert=False)
    await _edit_cart_message(q, context)


async def on_checkout_nav_back(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """chk:* — шаг назад в оформлении / оплате (корзина, страна, подтверждение, способ оплаты)."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(
        r"^chk:(country_to_cart|preview_to_country|pay_to_preview|pay_to_methods)$",
        (q.data or "").strip(),
    )
    if not m:
        return
    action = m.group(1)
    uid = q.from_user.id
    ud = context.user_data
    if action == "country_to_cart":
        oid_raw = ud.get("awaiting_payment_order_id")
        if oid_raw:
            try:
                ox = ORDERS.get(int(oid_raw))
            except (TypeError, ValueError):
                ox = None
            if ox and ox.get("payment_proof_submitted") and not ox.get("paid"):
                try:
                    await q.answer(PAY_PROOF_WAIT, show_alert=True)
                except Exception:
                    pass
                return
            _void_unpaid_pending_order_and_restore_checkout(uid, ud)
        _clear_checkout_delivery(ud)
        _ensure_user_cart(uid, ud)
        lines = _cart_get_lines_uid(uid, ud)
        products = await _get_products(context)
        if products:
            _reprice_lines_for_delivery(
                lines, products, _cart_price_region_for_user(uid, ud)
            )
            _cart_set_items_uid(uid, lines)
        try:
            await q.answer()
        except Exception:
            pass
        await q.message.reply_text(
            _format_cart_message(lines, ud, cart_uid=uid),
            reply_markup=_kb_cart(lines),
        )
        return
    if action == "preview_to_country":
        oid_raw = ud.get("awaiting_payment_order_id")
        if oid_raw:
            try:
                ox = ORDERS.get(int(oid_raw))
            except (TypeError, ValueError):
                ox = None
            if ox and ox.get("payment_proof_submitted") and not ox.get("paid"):
                try:
                    await q.answer(PAY_PROOF_WAIT, show_alert=True)
                except Exception:
                    pass
                return
            if not _void_unpaid_pending_order_and_restore_checkout(uid, ud):
                try:
                    await q.answer(
                        "Не удалось вернуться к выбору страны.", show_alert=True
                    )
                except Exception:
                    pass
                return
        if not ud.get("order_checkout"):
            try:
                await q.answer()
            except Exception:
                pass
            await q.message.reply_text(
                "Сессия оформления устарела. Откройте корзину и нажмите «Оформить заказ» снова.",
                reply_markup=_kb_cart([]),
            )
            return
        ud.pop("delivery_country", None)
        ud.pop("delivery_label", None)
        ud.pop("delivery_amount", None)
        ud.pop("delivery_currency", None)
        products = await _get_products(context)
        if products and ud.get("order_checkout"):
            _reprice_order_checkout_for_delivery(
                ud["order_checkout"],
                products,
                _checkout_start_reprice_region(ud, uid),
            )
        try:
            await q.answer()
        except Exception:
            pass
        await q.message.reply_text(
            "🚚 Куда доставить заказ?\n\nВыберите страну 👇",
            reply_markup=_kb_delivery_country_with_back(),
        )
        return
    if action == "pay_to_preview":
        oid_raw = ud.get("awaiting_payment_order_id")
        if not oid_raw:
            try:
                await q.answer("Нет активного шага оплаты.", show_alert=True)
            except Exception:
                pass
            return
        try:
            ox = ORDERS.get(int(oid_raw))
        except (TypeError, ValueError):
            ox = None
        if ox and ox.get("payment_proof_submitted") and not ox.get("paid"):
            try:
                await q.answer(PAY_PROOF_WAIT, show_alert=True)
            except Exception:
                pass
            return
        if not _void_unpaid_pending_order_and_restore_checkout(uid, ud):
            try:
                await q.answer("Не удалось вернуться к заказу.", show_alert=True)
            except Exception:
                pass
            return
        products = await _get_products(context)
        oc = ud.get("order_checkout")
        if products and isinstance(oc, list) and oc:
            cc = str(ud.get("delivery_country") or "by")
            if cc not in DELIVERY_OPTIONS:
                cc = "by"
            _reprice_order_checkout_for_delivery(oc, products, cc)
        preview = _format_order_preview_with_delivery(ud, uid)
        if not preview:
            try:
                await q.answer()
            except Exception:
                pass
            await q.message.reply_text(
                "Не удалось показать превью заказа. Откройте корзину снова.",
                reply_markup=_kb_cart(_cart_get_lines_uid(uid, ud)),
            )
            return
        try:
            await q.answer()
        except Exception:
            pass
        await q.message.reply_text(
            preview,
            reply_markup=_kb_order_preview_actions(uid, ud),
        )
        return
    if action == "pay_to_methods":
        msg_txt = (q.message.text or q.message.caption or "") if q.message else ""
        oid = _resolve_payment_order_id(
            uid, ud, message_text=msg_txt
        )
        if oid is None:
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
                await q.answer(MSG_ORDER_ALREADY_PAID_TOAST, show_alert=False)
            except Exception:
                pass
            return
        if o.get("payment_proof_submitted") and not o.get("paid"):
            try:
                await q.answer(PAY_PROOF_WAIT, show_alert=True)
            except Exception:
                pass
            return
        pr = _user_state_get(uid, "awaiting_proof")
        if pr is not None and int(pr) == int(oid):
            _user_state_pop(uid, "awaiting_proof")
        o.pop("payment_pending_method", None)
        ud.pop("payment_pending_method", None)
        _clear_crypto_auto_watch(o, uid)
        tot = _order_resolved_grand_total(o)
        d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
        pay_cur = _order_line_currency_from_delivery(d)
        try:
            await q.answer()
        except Exception:
            pass
        lo_est_pm = (ORDERS.get(int(oid)) or {}).get("loyalty_earn_estimate")
        await q.message.reply_text(
            _payment_intro_text(tot, pay_cur, loyalty_earn_estimate=lo_est_pm),
            reply_markup=_kb_payment_methods(int(oid)),
        )
        return


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
    ud["checkout_bonus_spend"] = 0
    ud.pop("delivery_country", None)
    ud.pop("delivery_label", None)
    ud.pop("delivery_amount", None)
    ud.pop("delivery_currency", None)
    products = list(context.application.bot_data.get("products") or [])
    if not products:
        products = await load_products() or []
        if products:
            context.application.bot_data["products"] = products
    if products:
        _reprice_order_checkout_for_delivery(
            ud["order_checkout"],
            products,
            _checkout_start_reprice_region(ud, uid),
        )
    users_touch(uid, "checkout")
    await q.answer()
    await q.message.reply_text(
        "🚚 Куда доставить заказ?\n\nВыберите страну 👇",
        reply_markup=_kb_delivery_country_with_back(),
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
    uid_pick = q.from_user.id if q.from_user else 0
    prev_cc = str(USER_PREF_DELIVERY_COUNTRY.get(uid_pick) or "").strip().lower()
    if uid_pick and code != prev_cc:
        _cart_clear_site_pricing_hints(uid_pick)
    ud["delivery_country"] = code
    ud["preferred_delivery_country"] = code
    ud["delivery_label"] = dlabel
    ud["delivery_amount"] = int(damount)
    ud["delivery_currency"] = dcur
    _remember_user_delivery_country(uid_pick, code)
    products = list(context.application.bot_data.get("products") or [])
    if not products:
        products = await load_products() or []
        if products:
            context.application.bot_data["products"] = products
    if products and lines:
        _reprice_order_checkout_for_delivery(lines, products, code)
        ud["order_checkout"] = lines
    if uid_pick and products:
        _reprice_uid_cart(uid_pick, ud, products, code, respect_site_lines=False)
    preview = _format_order_preview_with_delivery(ud, uid_pick)
    if not preview:
        await _notify_callback_issue(q, context)
        return
    await q.answer()
    await q.message.reply_text(
        preview,
        reply_markup=_kb_order_preview_actions(uid_pick, ud),
    )


async def on_send_order_to_admin(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """ta:0 — подтверждение превью: заказ в ORDERS без уведомления админу; дальше оплата, админ — после чека."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if (q.data or "").strip() != "ta:0":
        return
    acked = await _callback_ack(q)
    ud = context.user_data
    uid_chk = q.from_user.id if q.from_user else 0
    ap = _user_state_get(uid_chk, "awaiting_proof")
    if ap is not None:
        try:
            po = ORDERS.get(int(ap))
        except (TypeError, ValueError):
            _user_state_pop(uid_chk, "awaiting_proof")
            po = None
        if po is None or int(po.get("user_id") or 0) != int(uid_chk) or po.get("paid"):
            _user_state_pop(uid_chk, "awaiting_proof")
        elif po is not None and not po.get("paid"):
            if acked and q.message:
                await q.message.reply_text(MSG_PAY_NEED_PROOF_FIRST)
            else:
                await _callback_ack(q, MSG_PAY_NEED_PROOF_FIRST, show_alert=True)
            return
    pend = _resolve_awaiting_payment_order_id(uid_chk, ud)
    if pend is not None:
        po = ORDERS.get(int(pend))
        if po is None or int(po.get("user_id") or 0) != int(uid_chk) or po.get("paid"):
            _clear_awaiting_payment_order_id(uid_chk, ud)
        elif po is not None and not po.get("paid"):
            po = _ensure_order_in_orders(int(pend), int(uid_chk)) or po
            lines_peek = (
                await _restore_cart_lines_for_confirm(uid_chk, ud, context)
                if uid_chk
                else []
            )
            meta_peek = _merge_site_bonus_into_meta(
                uid_chk, _get_site_pending_meta(uid_chk, ud)
            )
            await _resend_active_payment_step(
                q,
                int(uid_chk),
                ud,
                int(pend),
                po,
                preview_text=_get_site_pending_preview(uid_chk),
                lines=lines_peek,
                meta=meta_peek,
            )
            save_state()
            return
    lines: Optional[List[dict]] = ud.get("order_checkout")
    if not lines:
        # Восстанавливаемся после устаревшей сессии: берём актуальные позиции из корзины.
        fallback_lines = (
            await _restore_cart_lines_for_confirm(uid_chk, context.user_data, context)
            if uid_chk
            else []
        )
        if not fallback_lines:
            if not acked:
                await _callback_ack(q)
            await q.message.reply_text(
                "Корзина пустая или шаг оформления устарел. Добавьте карточки и попробуйте снова.",
                reply_markup=_kb_cart([]),
            )
            return
        lines = deepcopy(fallback_lines)
        ud["order_checkout"] = deepcopy(lines)
    if not ud.get("delivery_country"):
        await q.message.reply_text(
            "Нужно заново выбрать страну доставки перед подтверждением заказа 👇",
            reply_markup=_kb_delivery_country_with_back(),
        )
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
    del_code = str(ud.get("delivery_country") or "by")
    if del_code not in DELIVERY_OPTIONS:
        del_code = "by"
    products_cf = await _get_products(context)
    if products_cf and lines:
        _reprice_order_checkout_for_delivery(lines, products_cf, del_code)
    try:
        d_amt = int(drec.get("amount") or 0)
    except (TypeError, ValueError):
        d_amt = 0
    pay_cur = (
        "BYN" if drec.get("country") == "by" and drec.get("currency") == "BYN" else "RUB"
    )
    goods_total, _ = _cart_totals(list(lines))
    site_gt = _cart_get_site_grand_total(uid, pay_cur)
    if site_gt and not _site_grand_covers_goods(int(site_gt), int(goods_total)):
        site_gt = None
    inc = _cart_site_delivery_included(uid)
    if inc:
        baseline_total = int(goods_total)
    else:
        baseline_total = int(goods_total) + int(d_amt)
    base_tot = int(goods_total)
    if site_gt and site_gt > 0 and baseline_total > 0:
        tol = max(100, baseline_total // 25)
        if abs(int(site_gt) - int(baseline_total)) <= tol:
            base_tot = int(site_gt)
        else:
            base_tot = int(baseline_total)
    elif site_gt and site_gt > 0:
        base_tot = int(site_gt)
    elif inc:
        base_tot = int(goods_total)
    elif drec.get("country") == "by" and drec.get("currency") == "BYN":
        base_tot = int(goods_total) + d_amt
    else:
        base_tot = int(goods_total) + d_amt
    pay_total = int(base_tot)
    spend_points = 0
    spend = 0
    order_rec = {
        "id": "0",
        "items": deepcopy(list(lines)),
        "total": int(pay_total),
        "total_goods": int(goods_total),
        "delivery": drec,
        "status": "В обработке",
        "bonus_applied": int(spend),
        "bonus_points_spent": int(spend_points),
    }
    lo_hint_ta = None
    pe_ta = _cart_get_site_loyalty_pending_earn(uid)
    if pe_ta is not None and int(pe_ta) > 0:
        lo_hint_ta = {"bonusWillEarn": int(pe_ta)}
    oid = await _notify_admin_new_order(
        context,
        u,
        list(lines),
        int(pay_total),
        deepcopy(drec),
        loyalty_hint_dict=lo_hint_ta,
        bonus_applied=int(spend),
        bonus_points_spent=int(spend_points),
    )
    if oid is None:
        await _notify_callback_issue(q, context)
        return
    order_rec["id"] = str(oid)
    USER_ORDERS.setdefault(uid, []).append(order_rec)
    ORDERS[int(oid)]["clear_cart_on_paid"] = True
    ORDERS[int(oid)]["total_goods"] = int(goods_total)
    ORDERS[int(oid)]["bonus_points_spent"] = int(spend_points)
    if int(spend) > 0:
        ORDERS[int(oid)]["bonus_applied"] = int(spend)
    save_state()
    _set_awaiting_payment_order_id(uid, ud, int(oid))
    await q.answer()
    tot = int(order_rec["total"])
    lo_est_ta = (ORDERS.get(int(oid)) or {}).get("loyalty_earn_estimate")
    await _reply_payment_step(
        q.message,
        tot,
        pay_cur,
        loyalty_earn_estimate=lo_est_ta,
        order_id=int(oid),
    )
    users_touch(uid, "payment")


async def on_payment_cancel(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """pay_cancel — отмена шага оплаты, возврат корзины."""
    q = update.callback_query
    if not q or not q.from_user or not q.message:
        return
    if not _RE_PAY_CANCEL_CB.match((q.data or "").strip()):
        return
    uid = int(q.from_user.id)
    ud = context.user_data
    await _callback_ack(q)
    msg_txt = (q.message.text or q.message.caption or "") if q.message else ""
    oid = _resolve_payment_order_id(
        uid, ud, callback_data=q.data, message_text=msg_txt
    )
    if oid is not None:
        o = _ensure_order_in_orders(int(oid), int(uid))
        if (
            isinstance(o, dict)
            and int(o.get("user_id") or 0) == int(uid)
            and not o.get("paid")
            and not o.get("payment_proof_submitted")
        ):
            o["status"] = "canceled"
            _void_unpaid_pending_order_and_restore_checkout(uid, ud)
    _clear_awaiting_payment_order_id(uid, ud)
    save_state()
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.message.reply_text(MSG_PAY_CANCELLED, reply_markup=REPLY_KB)
    users_touch(uid, "payment_cancel")


async def on_payment_method(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """pay_card | pay_transfer | pay_crypto — реквизиты и кнопка «Я оплатил»."""
    log = logging.getLogger(__name__)
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = _RE_PAY_METHOD_CB.match((q.data or "").strip())
    if not m:
        return
    uid = int(q.from_user.id)
    ud = context.user_data if isinstance(context.user_data, dict) else {}
    await _callback_ack(q)
    msg_txt = (q.message.text or q.message.caption or "") if q.message else ""
    log.info("payment_method uid=%s data=%r msg_oid=%s", uid, q.data, _parse_order_id_from_payment_message(msg_txt))
    try:
        oid = _resolve_payment_order_id(
            uid, ud, callback_data=q.data, message_text=msg_txt
        )
        if oid is None:
            await q.message.reply_text(
                "Сессия оплаты устарела. Нажмите «Подтвердить заказ» на превью ещё раз или «📋 Мои заказы».",
                reply_markup=REPLY_KB,
            )
            return
        o = _ensure_order_in_orders(int(oid), uid)
        if not isinstance(o, dict) and _message_looks_like_order_payment_step(msg_txt):
            o = _stub_order_for_payment_step(int(oid), uid, msg_txt)
            ORDERS[int(oid)] = o
        if not o or int(o.get("user_id") or 0) != uid:
            _clear_awaiting_payment_order_id(uid, ud)
            await q.message.reply_text(
                "Заказ не найден. Оформите заказ с сайта ещё раз или напишите в «Связь».",
                reply_markup=REPLY_KB,
            )
            return
        _refresh_unpaid_order_payment(
            o,
            uid,
            ud,
            preview_text=_get_site_pending_preview(uid) or msg_txt,
            lines=_cart_get_lines_uid(uid, ud),
            meta=_get_site_pending_meta(uid, ud),
        )
        if o.get("paid"):
            await q.message.reply_text(MSG_ORDER_ALREADY_PAID_TOAST, reply_markup=REPLY_KB)
            return
        if o.get("payment_proof_submitted") and not o.get("paid"):
            await q.message.reply_text(PAY_PROOF_WAIT, reply_markup=REPLY_KB)
            return
        pr_same = _user_state_get(uid, "awaiting_proof")
        if pr_same is not None and int(pr_same) == int(oid):
            _user_state_pop(uid, "awaiting_proof")
        method = m.group(1)
        ud["payment_pending_method"] = method
        _bind_payment_order_session(uid, ud, int(oid))
        if method == "crypto":
            _user_state_bucket(uid)["crypto_check"] = int(oid)
            o["crypto_auto_active"] = True
            o["crypto_auto_deadline"] = time.time() + random.uniform(120.0, 300.0)
        else:
            _clear_crypto_auto_watch(o, uid)
        body_map = {
            "card": PAY_CARD_BODY,
            "transfer": PAY_TRANSFER_BODY,
            "crypto": PAY_CRYPTO_BODY,
        }
        users_touch(uid, "payment")
        total_label = _payment_total_label(o)
        await q.message.reply_text(
            body_map[method],
            reply_markup=_kb_paid_confirm_with_back(total_label, int(oid)),
        )
        try:
            save_state()
        except Exception:
            log.exception("payment_method save_state uid=%s oid=%s", uid, oid)
    except Exception:
        log.exception("payment_method failed uid=%s data=%r", uid, q.data)
        try:
            await q.message.reply_text(
                "Не удалось открыть способ оплаты. Нажмите кнопку ещё раз или «Подтвердить заказ» на превью.",
                reply_markup=REPLY_KB,
            )
        except Exception:
            pass


async def on_payment_paid(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """paid — подтверждение оплаты клиентом."""
    log = logging.getLogger(__name__)
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    if not _RE_PAID_CB.match((q.data or "").strip()):
        return
    uid = int(q.from_user.id)
    ud = context.user_data if isinstance(context.user_data, dict) else {}
    _refresh_orders_state_from_redis()
    await _callback_ack(q)
    msg_txt = (q.message.text or q.message.caption or "") if q.message else ""
    oid = _resolve_payment_order_id(
        uid, ud, callback_data=q.data, message_text=msg_txt
    )
    if oid is None:
        oid_cb = _oid_from_pay_callback(q.data)
        if oid_cb is not None:
            oid = int(oid_cb)
            _bind_payment_order_session(uid, ud, oid)
    if oid is None:
        await q.message.reply_text(
            "Не удалось открыть шаг со скрином. Нажмите «✅ Оплатить» на свежем сообщении с реквизитами.",
            reply_markup=REPLY_KB,
        )
        return
    o = _resolve_proof_order_record(uid, int(oid), ud)
    if not o or int(o.get("user_id") or 0) != uid:
        await q.message.reply_text(
            "Заказ не найден. Нажмите «✅ Оплатить» на сообщении с реквизитами ещё раз.",
            reply_markup=REPLY_KB,
        )
        return
    _refresh_unpaid_order_payment(
        o,
        int(uid),
        ud,
        preview_text=_get_site_pending_preview(int(uid)),
        lines=_cart_get_lines_uid(int(uid), ud),
        meta=_get_site_pending_meta(int(uid), ud),
    )
    save_state()
    if o.get("paid"):
        await q.message.reply_text(MSG_ORDER_ALREADY_PAID_TOAST, reply_markup=REPLY_KB)
        return
    if (
        o.get("payment_proof_submitted")
        and not o.get("paid")
        and o.get("payment_proof_admin_message_id")
    ):
        await q.message.reply_text(PAY_PROOF_WAIT, reply_markup=REPLY_KB)
        return
    if o.get("payment_proof_submitted") and not o.get("checkout_submitted_to_admin"):
        await q.message.reply_text(
            MSG_POSTPAID_NEED_DETAILS,
            reply_markup=_kb_postpaid_submit(int(oid)),
        )
        _user_state_set(uid, "postpaid_thread_oid", int(oid))
        return
    if (
        o.get("payment_proof_submitted")
        and not o.get("paid")
        and o.get("checkout_submitted_to_admin")
    ):
        await q.message.reply_text(PAY_PROOF_WAIT, reply_markup=REPLY_KB)
        return
    _set_awaiting_proof_session(uid, ud, int(oid))
    try:
        save_state()
    except Exception:
        log.exception("on_payment_paid proof session save uid=%s oid=%s", uid, oid)
    pm = ud.pop("payment_pending_method", None)
    if pm:
        o["payment_pending_method"] = pm
    _clear_crypto_auto_watch(o, uid)
    _clear_checkout_delivery(ud)
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.message.reply_text(
        PAY_PROOF_REQUEST, reply_markup=_kb_proof_step_back()
    )
    try:
        save_state()
    except Exception:
        logging.getLogger(__name__).exception(
            "on_payment_paid save_state uid=%s oid=%s", uid, oid
        )


def _telegram_sent_ids(
    sent: object, *, fallback_chat_id: Optional[int] = None
) -> Tuple[int, int]:
    """chat_id и message_id из Message или MessageId (python-telegram-bot 20+)."""
    if hasattr(sent, "chat_id") and getattr(sent, "chat_id", None) is not None:
        chat_id = int(sent.chat_id)  # type: ignore[attr-defined]
    elif hasattr(sent, "chat"):
        chat = getattr(sent, "chat")
        chat_id = int(chat.id if hasattr(chat, "id") else chat)
    elif fallback_chat_id is not None:
        chat_id = int(fallback_chat_id)
    else:
        raise TypeError(f"cannot resolve chat_id from {type(sent)!r}")
    return chat_id, int(getattr(sent, "message_id"))


def _admin_target_chat_ids() -> set:
    ids: set = set()
    for raw in _admin_order_notify_targets() or ([int(ADMIN_ID)] if ADMIN_ID else []):
        if isinstance(raw, int) and int(raw) > 0:
            ids.add(int(raw))
    return ids


def _proof_stored_in_current_admin_chat(o: dict) -> bool:
    """Чек уже лежит у текущего админа (не у прошлого chat_id)."""
    cid = o.get("payment_proof_admin_chat_id")
    mid = o.get("payment_proof_admin_message_id")
    if cid is None or mid is None:
        return False
    try:
        return int(cid) in _admin_target_chat_ids()
    except (TypeError, ValueError):
        return False


async def _send_or_edit_admin_payment_proof(
    context: ContextTypes.DEFAULT_TYPE,
    order_id: int,
    o: dict,
    file_id: str,
    caption: str,
    *,
    customer_msg: Optional[Message] = None,
) -> bool:
    """Скрин оплаты админу: copy_message → send_photo → forward (без reply в «папку»)."""
    log = logging.getLogger(__name__)
    if len(caption) > 1024:
        caption = caption[:1021] + "…"
    cust_uid = int(o.get("user_id") or 0)
    kb = _kb_payment_admin_review(order_id, cust_uid)
    targets = _admin_order_notify_targets() or ([int(ADMIN_ID)] if ADMIN_ID else [])
    if not targets:
        log.error("payment proof: admin targets empty order_id=%s", order_id)
        return False
    log.info(
        "payment proof send order_id=%s uid=%s targets=%s admin_id=%s",
        order_id,
        cust_uid,
        targets,
        ADMIN_ID,
    )
    o.pop("payment_proof_admin_chat_id", None)
    o.pop("payment_proof_admin_message_id", None)
    sent = None
    sent_chat_id: Optional[int] = None
    last_err: Optional[Exception] = None

    async def _try_copy(tgt: object) -> Optional[Message]:
        if customer_msg is None:
            return None
        try:
            return await context.bot.copy_message(
                chat_id=tgt,
                from_chat_id=int(customer_msg.chat_id),
                message_id=int(customer_msg.message_id),
                caption=caption,
                reply_markup=kb,
            )
        except Exception as e:
            nonlocal last_err
            last_err = e
            log.warning("payment proof copy_message target=%s order=%s: %s", tgt, order_id, e)
            return None

    async def _try_photo(tgt: object) -> Optional[Message]:
        try:
            return await context.bot.send_photo(
                chat_id=tgt,
                photo=file_id,
                caption=caption,
                reply_markup=kb,
            )
        except Exception as e:
            nonlocal last_err
            last_err = e
            log.warning("payment proof send_photo target=%s order=%s: %s", tgt, order_id, e)
            return None

    async def _try_forward(tgt: object) -> Optional[Message]:
        if customer_msg is None:
            return None
        try:
            fwd = await context.bot.forward_message(
                chat_id=tgt,
                from_chat_id=int(customer_msg.chat_id),
                message_id=int(customer_msg.message_id),
            )
            await context.bot.send_message(
                chat_id=tgt,
                text=caption[:4090],
                reply_markup=kb,
                reply_to_message_id=int(fwd.message_id),
            )
            return fwd
        except Exception as e:
            nonlocal last_err
            last_err = e
            log.exception("payment proof forward target=%s order=%s", tgt, order_id)
            return None

    for tgt in targets:
        tgt_chat = int(tgt) if isinstance(tgt, int) else None
        sent = await _try_photo(tgt)
        if sent is not None:
            sent_chat_id = tgt_chat
            break
        sent = await _try_copy(tgt)
        if sent is not None:
            sent_chat_id = tgt_chat
            break
        sent = await _try_forward(tgt)
        if sent is not None:
            sent_chat_id = tgt_chat
            break

    if sent is None:
        short = f"📸 Чек · Заказ #{order_id} · клиент id {cust_uid}\n\nФото не удалось прикрепить автоматически."
        for tgt in targets:
            try:
                alert = await context.bot.send_message(
                    chat_id=tgt,
                    text=short,
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
                if customer_msg is not None:
                    sent = await context.bot.copy_message(
                        chat_id=tgt,
                        from_chat_id=int(customer_msg.chat_id),
                        message_id=int(customer_msg.message_id),
                        reply_to_message_id=int(alert.message_id),
                    )
                else:
                    sent = await context.bot.send_photo(chat_id=tgt, photo=file_id)
                sent_chat_id = tgt_chat if isinstance(tgt, int) else None
                log.info(
                    "payment proof alert+copy target=%s order=%s msg=%s",
                    tgt,
                    order_id,
                    sent.message_id if sent else None,
                )
                break
            except Exception as e:
                last_err = e
                log.exception("payment proof alert target=%s order=%s", tgt, order_id)

    if sent is None:
        log.error(
            "payment proof all methods failed order_id=%s uid=%s last_err=%s",
            order_id,
            cust_uid,
            last_err,
        )
        return False

    try:
        chat_id, message_id = _telegram_sent_ids(
            sent, fallback_chat_id=sent_chat_id
        )
    except (TypeError, ValueError) as e:
        log.exception("payment proof sent ids order_id=%s: %s", order_id, e)
        return False

    o["payment_proof_admin_chat_id"] = chat_id
    o["payment_proof_admin_message_id"] = message_id
    log.info(
        "payment proof delivered chat=%s msg=%s order_id=%s",
        chat_id,
        message_id,
        order_id,
    )
    return True


async def _forward_support_photo_to_admin(
    context: ContextTypes.DEFAULT_TYPE, msg: Message, uid: int
) -> bool:
    """Фото из режима «Связь» → админ (как текст), с кнопкой ответа."""
    log = logging.getLogger(__name__)
    file_id = msg.photo[-1].file_id
    uc = (msg.caption or "").strip()
    uname = (msg.from_user.username or "").strip() if msg.from_user else ""
    head = "📸 Фото от клиента"
    tail = f"\n\n👤 id {uid}"
    if uname:
        tail += f" @{uname}"
    mid = f"\n\n{uc}" if uc else ""
    cap = (head + mid + tail)[:1024]
    sup_kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("💬", callback_data=f"sup:rep:{uid}")]]
    )
    try:
        await context.bot.send_photo(
            chat_id=ORDER_NOTIFY_TARGET,
            photo=file_id,
            caption=cap,
            reply_markup=sup_kb,
        )
        return True
    except Exception:
        log.exception("клиент → поддержка (фото): target=%s", ORDER_NOTIFY_TARGET)
        try:
            await context.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=file_id,
                caption=cap,
                reply_markup=sup_kb,
            )
            return True
        except Exception:
            log.exception("клиент → поддержка (фото) fallback admin_id=%s", ADMIN_ID)
            return False


_PROOF_MEDIA_FILTER = (
    filters.PHOTO | filters.Document.ALL | filters.ANIMATION | filters.VIDEO
)


async def on_customer_proof_media(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Единая точка входа для фото/картинок клиента (block=True, без гонок block=False)."""
    log = logging.getLogger(__name__)
    msg = update.effective_message
    if not msg or not msg.from_user:
        return
    uid = int(msg.from_user.id)
    if is_admin(uid):
        return
    ud = context.user_data if isinstance(context.user_data, dict) else {}
    file_id = _image_file_id_from_message(msg)
    log.info(
        "proof_media uid=%s msg_id=%s photo=%s doc=%s file=%s",
        uid,
        msg.message_id,
        bool(msg.photo),
        bool(msg.document),
        bool(file_id),
    )

    if user_support_state.get(uid):
        ok = await _forward_support_photo_to_admin(context, msg, uid)
        if ok:
            user_support_state.pop(uid, None)
            m_ok = await msg.reply_text(MSG_SUPPORT_THANKS)
            _track_temp_message(uid, m_ok)
        else:
            await msg.reply_text(MSG_SEND_SUPPORT_FAIL)
        raise ApplicationHandlerStop

    pp_oid = _user_state_get(uid, "postpaid_thread_oid")
    if pp_oid is not None:
        cap = (msg.caption or "").strip() if msg.caption else ""
        fid = file_id or (msg.photo[-1].file_id if msg.photo else None)
        if not fid:
            await msg.reply_text(
                "Пришлите фото или картинку (PNG/JPG), не PDF.",
                reply_markup=REPLY_KB,
            )
        else:
            ok = await _collect_postpaid_client_message(
                context,
                uid=uid,
                oid=int(pp_oid),
                body_text=cap or None,
                msg=msg,
                photo_file_id=fid,
            )
            if not ok:
                await msg.reply_text(MSG_SEND_SUPPORT_FAIL)
        raise ApplicationHandlerStop

    if not file_id:
        if _user_wants_proof_upload(uid, ud, msg):
            await msg.reply_text(
                "Не вижу изображение. Пришлите скрин как фото (PNG/JPG), не PDF.",
                reply_markup=REPLY_KB,
            )
        else:
            await msg.reply_text(
                "Сначала выберите способ оплаты и нажмите «✅ Я оплатил», затем пришлите скрин.",
                reply_markup=REPLY_KB,
            )
        raise ApplicationHandlerStop

    wants_proof = _user_wants_proof_upload(uid, ud, msg)
    if not wants_proof:
        await msg.reply_text(
            "Сначала нажмите «✅ Оплатить» на сообщении с реквизитами, затем пришлите скрин.",
            reply_markup=REPLY_KB,
        )
        raise ApplicationHandlerStop

    try:
        await msg.reply_text(MSG_PAY_PROOF_RECEIVED, reply_markup=REPLY_KB)
    except Exception:
        log.exception("proof_media ack uid=%s", uid)
    try:
        await _on_payment_proof_photo_body(update, context)
    except Exception:
        log.exception("proof_media body uid=%s", uid)
        try:
            await msg.reply_text(MSG_PAY_PROOF_TO_ADMIN_FAIL, reply_markup=REPLY_KB)
        except Exception:
            pass
    raise ApplicationHandlerStop


async def on_user_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Фото/картинка: скрин оплаты (awaiting_proof), postpaid_thread, иначе «Связь»."""
    log = logging.getLogger(__name__)
    msg = update.effective_message
    if not msg:
        return
    try:
        await _on_user_photo_body(update, context)
    except Exception:
        log.exception("on_user_photo failed uid=%s", msg.from_user.id if msg.from_user else 0)
        try:
            await msg.reply_text(MSG_PAY_PROOF_TO_ADMIN_FAIL, reply_markup=REPLY_KB)
        except Exception:
            pass


async def _on_user_photo_body(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    log = logging.getLogger(__name__)
    msg = update.effective_message
    if not msg:
        return
    uid = msg.from_user.id if msg.from_user else 0
    ud = context.user_data if isinstance(context.user_data, dict) else {}
    file_id_peek = _image_file_id_from_message(msg)
    proof_peek = _resolve_proof_order_id_aggressive(uid, ud) if uid else None
    log.info(
        "user_photo uid=%s proof_oid=%s has_file=%s photo=%s doc=%s",
        uid,
        proof_peek,
        bool(file_id_peek),
        bool(msg.photo),
        bool(msg.document),
    )
    if not file_id_peek:
        if proof_peek is not None:
            await msg.reply_text(
                "Не вижу изображение. Пришлите скрин оплаты как фото (не PDF).",
                reply_markup=REPLY_KB,
            )
        elif uid and _find_latest_unpaid_order_id(uid) is not None:
            await msg.reply_text(
                "Не вижу изображение. Пришлите скрин как фото или картинку (PNG/JPG), не PDF.",
                reply_markup=REPLY_KB,
            )
        return
    proof_oid = _resolve_proof_order_id_aggressive(uid, ud) if uid else None
    if uid and file_id_peek:
        if proof_oid is not None:
            await on_payment_proof_photo(update, context)
            return
        sess_raw = (
            _user_state_get(uid, "awaiting_proof")
            or ud.get("awaiting_proof")
            or _user_state_get(uid, "awaiting_payment_order_id")
            or ud.get("awaiting_payment_order_id")
        )
        if sess_raw is not None:
            try:
                if isinstance(ud, dict):
                    ud["awaiting_proof"] = int(sess_raw)
                _user_state_set(uid, "awaiting_proof", int(sess_raw), persist=False)
            except (TypeError, ValueError):
                pass
            else:
                await on_payment_proof_photo(update, context)
                return
        if _find_latest_unpaid_order_id(uid) is not None:
            await on_payment_proof_photo(update, context)
            return
        await msg.reply_text(
            "Сначала нажмите «✅ Оплатить» на сообщении с реквизитами, затем пришлите скрин.",
            reply_markup=REPLY_KB,
        )
        return
    pp_oid = _user_state_get(uid, "postpaid_thread_oid")
    if uid and pp_oid is not None and not is_admin(uid):
        cap = (msg.caption or "").strip() if msg.caption else ""
        fid = msg.photo[-1].file_id if msg.photo else None
        if not fid:
            return
        ok = await _collect_postpaid_client_message(
            context,
            uid=uid,
            oid=int(pp_oid),
            body_text=cap or None,
            msg=msg,
            photo_file_id=fid,
        )
        if not ok:
            try:
                await msg.reply_text(MSG_SEND_SUPPORT_FAIL)
            except Exception:
                pass
        return
    if uid and user_support_state.get(uid):
        ok = await _forward_support_photo_to_admin(context, msg, uid)
        if ok:
            user_support_state.pop(uid, None)
            m_ok = await msg.reply_text(MSG_SUPPORT_THANKS)
            _track_temp_message(uid, m_ok)
        else:
            await msg.reply_text(MSG_SEND_SUPPORT_FAIL)
        return


async def on_payment_flow_non_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Нефото-вложения на шаге чека — подсказка, не молчим."""
    msg = update.effective_message
    if not msg or not msg.from_user or msg.text:
        return
    if _image_file_id_from_message(msg):
        await on_user_photo(update, context)
        return
    uid = int(msg.from_user.id)
    ud = context.user_data if isinstance(context.user_data, dict) else {}
    if _resolve_proof_order_id_aggressive(uid, ud) is None:
        return
    await msg.reply_text(
        "Пришлите скрин оплаты как фото или картинку (PNG/JPG), не стикер и не PDF.",
        reply_markup=REPLY_KB,
    )
    raise ApplicationHandlerStop


async def on_payment_proof_photo(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Фото-скрин оплаты при awaiting_proof; оплата у заказа — после подтверждения админом."""
    log = logging.getLogger(__name__)
    msg = update.effective_message
    try:
        await _on_payment_proof_photo_body(update, context)
    except Exception:
        log.exception("on_payment_proof_photo failed uid=%s", msg.from_user.id if msg and msg.from_user else 0)
        if msg:
            try:
                await msg.reply_text(MSG_PAY_PROOF_TO_ADMIN_FAIL, reply_markup=REPLY_KB)
            except Exception:
                pass


async def _on_payment_proof_photo_body(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Тело обработки скрина оплаты."""
    log = logging.getLogger(__name__)
    msg = update.effective_message
    file_id = _image_file_id_from_message(msg)
    if not msg or not file_id:
        if msg:
            await msg.reply_text(
                "Не удалось прочитать файл. Отправьте скрин как фото.",
                reply_markup=REPLY_KB,
            )
        return
    ud = context.user_data if isinstance(context.user_data, dict) else {}
    uid = msg.from_user.id if msg.from_user else 0
    if not uid:
        return
    _refresh_orders_state_from_redis()
    oid_raw = _resolve_proof_order_id_aggressive(uid, ud)
    if oid_raw is None:
        await msg.reply_text(MSG_EXPECT_PHOTO_PROOF)
        return
    try:
        oid = int(oid_raw)
    except (TypeError, ValueError):
        _user_state_pop(uid, "awaiting_proof", persist=False)
        if isinstance(ud, dict):
            ud.pop("awaiting_proof", None)
        await msg.reply_text(MSG_EXPECT_PHOTO_PROOF)
        return
    if not _proof_order_owned_by_user(uid, oid):
        _user_state_pop(uid, "awaiting_proof", persist=False)
        if isinstance(ud, dict):
            ud.pop("awaiting_proof", None)
        await msg.reply_text(
            "Этот чек не привязан к вашему заказу. Нажмите «✅ Оплатить» и пришлите скрин снова.",
            reply_markup=REPLY_KB,
        )
        return
    o = _resolve_proof_order_record(uid, oid, ud)
    if not o or int(o.get("user_id") or 0) != int(uid):
        _user_state_pop(uid, "awaiting_proof", persist=False)
        if isinstance(ud, dict):
            ud.pop("awaiting_proof", None)
        await msg.reply_text(
            "Не удалось привязать чек к заказу. Нажмите «✅ Оплатить» на шаге оплаты и пришлите скрин снова.",
            reply_markup=REPLY_KB,
        )
        return
    if o.get("checkout_submitted_to_admin") and o.get("payment_proof_submitted"):
        _user_state_set(uid, "postpaid_thread_oid", int(oid))
        await msg.reply_text(
            MSG_POSTPAID_COLLECTED,
            reply_markup=_kb_postpaid_submit(int(oid)),
        )
        return
    if o.get("paid"):
        _user_state_pop(uid, "awaiting_proof", persist=False)
        if isinstance(ud, dict):
            ud.pop("awaiting_proof", None)
        await msg.reply_text(MSG_ORDER_ALREADY_PAID_SKIP_PROOF)
        return
    if o.get("payment_proof_submitted") and not o.get("paid"):
        log.info(
            "payment proof resubmit uid=%s oid=%s prev_chat=%s",
            uid,
            oid,
            o.get("payment_proof_admin_chat_id"),
        )
        o.pop("payment_proof_admin_chat_id", None)
        o.pop("payment_proof_admin_message_id", None)
    _clear_crypto_auto_watch(o, uid, persist=False)
    o["proof_file_id"] = file_id
    o["payment_proof_submitted"] = True
    o.pop("checkout_submitted_to_admin", None)
    o.pop("delivery_details_parts", None)
    o.pop("delivery_details", None)
    _user_state_pop(uid, "awaiting_proof", persist=False)
    _clear_awaiting_payment_order_id(uid, ud, persist=False)
    try:
        asyncio.get_running_loop().create_task(
            _ensure_customer_admin_folder(
                context,
                uid,
                getattr(msg.from_user, "username", None) if msg.from_user else None,
            )
        )
    except RuntimeError:
        pass
    uname = getattr(msg.from_user, "username", None) if msg.from_user else None
    try:
        _remember_user_message(
            uid,
            uname,
            "photo",
            f"📸 Чек оплаты заказ #{oid}",
            persist=False,
        )
    except Exception:
        log.exception("remember payment proof uid=%s oid=%s", uid, oid)
    try:
        save_state()
    except Exception:
        log.exception("payment_proof_photo save_state uid=%s oid=%s", uid, oid)
    prompt = _postpaid_shipping_prompt_for_order(o)
    try:
        await msg.reply_text(
            prompt,
            reply_markup=_kb_postpaid_submit(int(oid)),
        )
        _user_state_set(uid, "postpaid_thread_oid", int(oid))
    except Exception:
        log.exception("payment_proof_photo shipping prompt uid=%s oid=%s", uid, oid)
    try:
        save_state()
    except Exception:
        log.exception("payment_proof_photo save_state2 uid=%s oid=%s", uid, oid)
    log.info("payment_proof_photo ok uid=%s oid=%s awaiting_shipping", uid, oid)


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
    o = _restore_order_for_admin(q, oid)
    if not o:
        await _reply_admin_order_stale(q, oid)
        raise ApplicationHandlerStop
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
    await _ensure_site_order_for_bot_order(oid, o)
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
            caption=prev + MSG_PAYMENT_CAPTION_CONFIRMED,
            reply_markup=None,
        )
    except Exception:
        pass
    # Карточка заказа админу — если ещё не отправляли на шаге «Отправить заказ».
    if not o.get("admin_message_id"):
        try:
            panel_text = _format_admin_order_detail_text(oid, o)
            panel_kb = _kb_order_admin_actions(oid, str(o.get("status") or "new"))
            panel = await q.message.reply_text(
                panel_text,
                reply_markup=panel_kb,
                disable_web_page_preview=True,
            )
            o["admin_chat_id"] = int(panel.chat_id)
            o["admin_message_id"] = int(panel.message_id)
        except Exception:
            logging.getLogger(__name__).exception(
                "confirm_payment admin panel reply order_id=%s", oid
            )
            await _send_admin_order_panel(context, oid, o, force_new=True)
    if cust:
        await _send_payment_receipt(context.bot, cust, oid, o)
    save_state()
    raise ApplicationHandlerStop


async def on_postpaid_submit(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Клиент нажал «✅ Отправить заказ» после адреса/ФИО/телефона."""
    q = update.callback_query
    if not q or not q.from_user or not q.data:
        return
    m = re.match(r"^ppdone:(\d+)$", (q.data or "").strip())
    if not m:
        return
    uid = int(q.from_user.id)
    try:
        oid = int(m.group(1))
    except ValueError:
        try:
            await q.answer()
        except Exception:
            pass
        return
    o = ORDERS.get(oid)
    if not o or int(o.get("user_id") or 0) != uid:
        try:
            await q.answer(MSG_POSTPAID_THREAD_STALE, show_alert=True)
        except Exception:
            pass
        return
    if not _postpaid_merged_details(o):
        try:
            await q.answer(MSG_POSTPAID_NEED_DETAILS, show_alert=True)
        except Exception:
            pass
        if q.message:
            try:
                await q.message.reply_text(
                    MSG_POSTPAID_NEED_DETAILS,
                    reply_markup=_kb_postpaid_submit(oid),
                )
            except Exception:
                pass
        return
    try:
        await q.answer("Отправляем заказ…")
    except Exception:
        pass
    ok, reason = await _finalize_order_submission_to_admin(context, uid, oid)
    if not ok:
        if q.message:
            try:
                await q.message.reply_text(MSG_POSTPAID_SUBMIT_FAIL, reply_markup=REPLY_KB)
            except Exception:
                pass
        return
    if q.message:
        try:
            await q.message.reply_text(MSG_POSTPAID_SUBMITTED, reply_markup=REPLY_KB)
        except Exception:
            pass
    raise ApplicationHandlerStop


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
        body, kb = await _format_mine_orders_text_and_kb(uid)
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
    o = _restore_order_for_admin(q, oid)
    if not o:
        await _reply_admin_order_stale(q, oid)
        raise ApplicationHandlerStop
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
    o.pop("checkout_submitted_to_admin", None)
    o.pop("delivery_details_parts", None)
    o.pop("delivery_details", None)
    o.pop("payment_proof_admin_chat_id", None)
    o.pop("payment_proof_admin_message_id", None)
    if cust:
        _user_state_pop(cust, "postpaid_thread_oid")
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
            caption=prev + MSG_PAYMENT_CAPTION_REJECTED,
            reply_markup=None,
        )
    except Exception:
        pass
    save_state()
    raise ApplicationHandlerStop


def _norm_search(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def _category_items(products: List[dict], category_name: str) -> List[dict]:
    return [p for p in products if _norm_search(p.get("category")) == _norm_search(category_name)]


def _find_category_by_text(products: List[dict], text: str) -> Optional[str]:
    q = _norm_search(text)
    if not q:
        return None
    cats = _category_names(products)
    for c in cats:
        if _norm_search(c) == q:
            return c
    for c in cats:
        if q in _norm_search(c):
            return c
    return None


async def _handle_buy_quick_text(msg: Message, context: ContextTypes.DEFAULT_TYPE, text: str, uid: int) -> bool:
    t = (text or "").strip()
    m = re.match(r"^(купить|buy)\s+(.+)$", t, re.IGNORECASE)
    if not m:
        return False
    query = m.group(2).strip()
    if not query:
        await msg.reply_text(MSG_BUY_HINT)
        return True
    products = await _get_products(context)
    if not products:
        await msg.reply_text(MSG_CATALOG_LOAD_FAIL)
        return True

    # Вариант: купить <категория> <номер>
    m_num = re.match(r"^(.+?)\s+#?(\d{1,4})$", query)
    if m_num:
        cat_txt = m_num.group(1).strip()
        idx = int(m_num.group(2))
        cat = _find_category_by_text(products, cat_txt)
        if cat:
            items = _category_items(products, cat)
            if 1 <= idx <= len(items):
                p = items[idx - 1]
                ref = _product_ref_for_callback(p, _global_product_index(products, p))
                reg_b = _cart_price_region_for_user(uid, context.user_data)
                px_b = _product_unit_price_for_delivery(p, reg_b)
                _cart_add_line_uid(
                    uid, context.user_data, ref, p, p.get("name") or "—", px_b
                )
                users_touch(uid, "cart")
                await msg.reply_text(f"Добавил в корзину: {p.get('name') or '—'} ({cat} №{idx}).")
                return True
            await msg.reply_text(f"В категории «{cat}» нет карточки с номером {idx}.")
            return True

    # Вариант: купить <название>
    qn = _norm_search(query)
    candidates = [p for p in products if qn and qn in _norm_search(p.get("name"))]
    if not candidates:
        await msg.reply_text("Не нашёл карточку по такому запросу. " + MSG_BUY_HINT)
        return True
    if len(candidates) > 1:
        top = candidates[:5]
        lines = ["Нашёл несколько карточек. Уточните запрос или используйте формат: купить <категория> <номер>", ""]
        for p in top:
            cat = str(p.get("category", "Без категории") or "Без категории")
            no = _product_category_number(products, p)
            lines.append(f"• {p.get('name') or '—'} — {cat} №{no}")
        await msg.reply_text("\n".join(lines))
        return True
    p = candidates[0]
    cat = str(p.get("category", "Без категории") or "Без категории")
    no = _product_category_number(products, p)
    ref = _product_ref_for_callback(p, _global_product_index(products, p))
    reg = _cart_price_region_for_user(uid, context.user_data)
    px = _product_unit_price_for_delivery(p, reg)
    _cart_add_line_uid(uid, context.user_data, ref, p, p.get("name") or "—", px)
    users_touch(uid, "cart")
    await msg.reply_text(f"Добавил в корзину: {p.get('name') or '—'} ({cat} №{no}).")
    return True


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
    log_vc = logging.getLogger(__name__)
    try:
        _ensure_user_cart(uid, context.user_data)
        ud = context.user_data
        lines = _cart_get_lines_uid(uid, ud)
        products = list(context.application.bot_data.get("products") or [])
        if not products:
            products = await load_products() or []
            if products:
                context.application.bot_data["products"] = products
        if products:
            _reprice_lines_for_delivery(
                lines, products, _cart_price_region_for_user(uid, ud)
            )
            _cart_set_items_uid(uid, lines)
        t = _format_cart_message(lines, ud, cart_uid=uid)
        kb = _kb_cart(lines)
        await q.message.reply_text(t, reply_markup=kb)
    except Exception:
        log_vc.exception("vc:0 корзина user_id=%s", uid)
        try:
            await q.message.reply_text(
                "Не удалось показать корзину. Попробуйте кнопку «🛒 Корзина» внизу или «Связь».",
                reply_markup=REPLY_KB,
            )
        except Exception:
            log_vc.exception("vc:0 fallback reply failed")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.text:
        return
    text = msg.text.strip()
    user_data = context.user_data
    uid = msg.from_user.id if msg.from_user else 0
    if uid:
        _remember_user_message(
            uid,
            getattr(msg.from_user, "username", None) if msg.from_user else None,
            "text",
            msg.text,
        )

    if _user_state_get(uid, "awaiting_proof") is not None:
        if text in REPLY_MENU_TEXTS:
            _user_state_pop(uid, "awaiting_proof")
        else:
            await msg.reply_text(MSG_EXPECT_PHOTO_PROOF)
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
        BTN_FAVORITES,
        BTN_RANDOM_CARD,
    )
    sup_uid = user_data.get("reply_support_user_id")
    rep_oid = user_data.get("reply_to")
    if is_admin(uid) and (sup_uid is not None or rep_oid is not None):
        if text in menu_keys:
            user_data.pop("reply_to", None)
            user_data.pop("reply_support_user_id", None)
        elif not text:
            await msg.reply_text(MSG_TYPE_REPLY_TEXT)
            return
        elif sup_uid is not None:
            target = int(sup_uid)
            body = "💬 Поддержка:\n\n" + msg.text
            if len(body) > 4096:
                body = body[:4090] + "…"
            ok = await _send_customer_plain(context.bot, target, body)
            if not ok:
                await msg.reply_text(MSG_FORWARD_FAIL)
                return
            user_data.pop("reply_support_user_id", None)
            await msg.reply_text(MSG_ADMIN_SAY_OK)
            return
        else:
            oid_raw = user_data.get("reply_to")
            try:
                oid_int = int(oid_raw)
            except (TypeError, ValueError):
                user_data.pop("reply_to", None)
                await msg.reply_text(MSG_ADMIN_REPLY_SESSION_RESET)
                return
            o = ORDERS.get(oid_int)
            if not o:
                user_data.pop("reply_to", None)
                await msg.reply_text(MSG_ADMIN_REPLY_SESSION_RESET)
                return
            target = int(o.get("user_id") or 0)
            if not target:
                user_data.pop("reply_to", None)
                await msg.reply_text(MSG_ADMIN_REPLY_SESSION_RESET)
                return
            body = "💬 Ответ от администратора:\n\n" + msg.text
            if len(body) > 4096:
                body = body[:4090] + "…"
            ok = await _send_customer_plain(context.bot, target, body)
            if not ok:
                await msg.reply_text(MSG_FORWARD_FAIL)
                return
            user_data.pop("reply_to", None)
            await msg.reply_text(MSG_ADMIN_SAY_OK)
            return

    thread_oid = _user_state_get(uid, "postpaid_thread_oid")
    if thread_oid is not None and uid and not is_admin(uid):
        if text in REPLY_MENU_TEXTS:
            _user_state_pop(uid, "postpaid_thread_oid")
        elif not text.strip():
            await msg.reply_text(MSG_EMPTY_INPUT)
            return
        else:
            ok = await _collect_postpaid_client_message(
                context,
                uid=uid,
                oid=int(thread_oid),
                body_text=text,
                msg=msg,
            )
            if not ok:
                try:
                    await msg.reply_text(MSG_SEND_SUPPORT_FAIL)
                except Exception:
                    pass
            return

    if text == BTN_CHAT:
        if not uid:
            await msg.reply_text(FALLBACK_USER_TEXT)
            return
        await _delete_user_temp_messages(context.bot, uid)
        _clear_checkout_delivery(user_data)
        user_data.pop("pending_order", None)
        _user_state_pop(uid, "postpaid_thread_oid")
        user_support_state[uid] = True
        await msg.reply_text(SUPPORT_INTRO_TEXT)
        return

    if uid and user_support_state.get(uid):
        if text in REPLY_MENU_TEXTS:
            user_support_state.pop(uid, None)
        elif not text.strip():
            await msg.reply_text(MSG_EMPTY_INPUT)
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
                log.exception("клиент → поддержка: target=%s", ORDER_NOTIFY_TARGET)
                # Fallback: если кастомный target недоступен, шлём админу по user_id.
                try:
                    await context.bot.send_message(
                        ADMIN_ID,
                        body,
                        disable_web_page_preview=True,
                        reply_markup=sup_kb,
                    )
                except Exception:
                    log.exception("клиент → поддержка fallback admin_id=%s", ADMIN_ID)
                    await msg.reply_text(MSG_SEND_SUPPORT_FAIL)
                    return
            user_support_state.pop(uid, None)
            m_ok = await msg.reply_text(MSG_SUPPORT_THANKS)
            _track_temp_message(uid, m_ok)
            return

    if uid and await _handle_buy_quick_text(msg, context, text, uid):
        return

    if text in (
        BTN_CATALOG,
        BTN_MY_ORDERS,
        BTN_DELIVERY,
        BTN_POPULAR,
        BTN_FAVORITES,
        BTN_RANDOM_CARD,
    ):
        _clear_checkout_delivery(user_data)
        user_data.pop("pending_order", None)

    if text == BTN_CART:
        log_cart = logging.getLogger(__name__)
        try:
            await _delete_user_temp_messages(context.bot, uid)
            await _refresh_user_state_from_site(uid)
            _ensure_user_cart(uid, user_data)
            cl = _cart_get_lines_uid(uid, user_data)
            products = list(context.application.bot_data.get("products") or [])
            if not products:
                products = await load_products() or []
                if products:
                    context.application.bot_data["products"] = products
            if products:
                try:
                    _reprice_lines_for_delivery(
                        cl, products, _cart_price_region_for_user(uid, user_data)
                    )
                    _cart_set_items_uid(uid, cl)
                except Exception:
                    log_cart.exception("Корзина: не удалось пересчитать цены user_id=%s", uid)
            t = _format_cart_message(cl, user_data, cart_uid=uid)
            kb = _kb_cart(cl)
            await msg.reply_text(t, reply_markup=kb)
        except Exception:
            log_cart.exception("Корзина: сбой при ответе user_id=%s", uid)
            try:
                await msg.reply_text(
                    "Не удалось показать корзину (сбой данных). "
                    "Попробуйте ещё раз или откройте каталог; если повторяется — напишите в «Связь».",
                    reply_markup=REPLY_KB,
                )
            except Exception:
                log_cart.exception("Корзина: не удалось отправить сообщение об ошибке")
        return

    if text == BTN_MY_ORDERS:
        await _delete_user_temp_messages(context.bot, uid)
        if not uid:
            await msg.reply_text(FALLBACK_USER_TEXT)
            return
        body, kb = await _format_mine_orders_text_and_kb(uid)
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

    if text == BTN_FAVORITES:
        await send_favorites_deck(update, context)
        return

    if text == BTN_RANDOM_CARD:
        await send_random_card(update, context)
        return

    await msg.reply_text(MSG_UNKNOWN_TEXT, reply_markup=REPLY_KB)


def _ensure_login_http_api_task(application: Application) -> None:
    """Держим HTTP API входа живым (может быть отменён при сбоях polling)."""
    if os.getenv("LOGIN_API_DISABLE", "").strip() == "1":
        return
    log = logging.getLogger(__name__)
    task = application.bot_data.get("login_http_api_task")
    if isinstance(task, asyncio.Task) and not task.done():
        return
    if isinstance(task, asyncio.Task) and task.done():
        try:
            err = task.exception()
        except asyncio.CancelledError:
            err = "cancelled"
        except Exception:
            err = "unknown"
        log.warning("HTTP API входа был остановлен (%s), перезапуск", err)
    application.bot_data["login_http_api_task"] = asyncio.create_task(
        _run_login_http_api(application.bot)
    )
    log.info("HTTP API входа запущен в фоне")


async def login_http_api_watchdog_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    if app:
        _ensure_login_http_api_task(app)


async def post_init(application: Application) -> None:
    log = logging.getLogger(__name__)
    load_state()
    _rebuild_site_login_pending_from_carts()
    application.bot_data["deploy_epoch"] = time.time_ns()
    n_sl = len(SITE_LOGIN_PENDING_ORDER)
    n_lc = len(LOGIN_CODES)
    LOGIN_CODES.clear()
    log.info(
        "Рестарт процесса: build=%s deploy_epoch=%s; восстановлено черновиков заказа=%d, сброшено LOGIN_CODES=%d",
        BOT_BUILD_ID,
        application.bot_data["deploy_epoch"],
        n_sl,
        n_lc,
    )
    try:
        startup_text = f"🟢 Бот перезапущен · {BOT_BUILD_ID}"
        notified: set = set()
        for tgt in _admin_order_notify_targets():
            key = int(tgt) if isinstance(tgt, int) else str(tgt).strip().lower()
            if key in notified:
                continue
            notified.add(key)
            try:
                await application.bot.send_message(chat_id=tgt, text=startup_text)
            except Exception:
                log.exception("startup notify target=%s build=%s", tgt, BOT_BUILD_ID)
    except Exception:
        log.exception("startup notify admin build=%s", BOT_BUILD_ID)
    # Иначе Telegram отдаёт 409, если у бота остался webhook (getUpdates + webhook несовместимы).
    try:
        await application.bot.delete_webhook(drop_pending_updates=False)
        log.info("Telegram: webhook снят перед long polling")
    except Exception:
        log.exception("Telegram: не удалось выполнить delete_webhook")
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
    try:
        nhp = await refresh_home_page_promotions_cache(application)
        log.info("Акции главной (старт): %d баннеров", nhp)
    except Exception:
        log.exception("Акции главной: ошибка стартовой загрузки")
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
        application.job_queue.run_repeating(
            login_http_api_watchdog_job,
            interval=20,
            first=5,
            name="login_http_api_watchdog",
        )
        log.info("Крипто mock-проверка: каждые 30 с")
    print("Бот запущен!")
    me = await application.bot.get_me()
    if me.username:
        print(f"https://t.me/{me.username}")
    try:
        _ensure_login_http_api_task(application)
    except Exception:
        log.exception("HTTP API входа на сайт не запущен")


def main() -> None:
    if not token:
        sys.exit("TELEGRAM_BOT_TOKEN is not set")

    app = (
        Application.builder()
        .token(token)
        .concurrent_updates(False)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.add_error_handler(on_ptb_error)

    app.add_handler(
        TypeHandler(Update, track_user_activity, block=False),
        group=-1,
    )
    app.add_handler(
        MessageHandler(_PROOF_MEDIA_FILTER, on_customer_proof_media),
        group=-1,
    )
    app.add_handler(
        MessageHandler((~filters.TEXT & ~filters.COMMAND), on_payment_flow_non_text),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_order_status_buttons,
            pattern=re.compile(r"^(accept|sent|cancel|done|delmsg)_\d+$"),
        ),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_order_admin_action,
            pattern=re.compile(r"^oam:rep:\d+$"),
        ),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_postpaid_submit,
            pattern=re.compile(r"^ppdone:\d+$"),
        ),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_admin_confirm_payment,
            pattern=re.compile(r"^confirm_payment_\d+$"),
        ),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_admin_reject_payment,
            pattern=re.compile(r"^reject_payment_\d+$"),
        ),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_payment_cancel,
            pattern=re.compile(r"^pay_cancel(:\d+)?$"),
        ),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_payment_method,
            pattern=re.compile(r"^pay_(card|transfer|crypto)(:\d+)?$"),
        ),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_payment_paid, pattern=re.compile(r"^paid(:\d+)?$")
        ),
        group=-1,
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("myid", myid_cmd))
    app.add_handler(CommandHandler("catalog", catalog_cmd))
    app.add_handler(CommandHandler("promo", send_promo))
    app.add_handler(CommandHandler("swipe", send_tinder_mode))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("say", admin_say_cmd))
    # confirm_order / cancel_order — явный pattern (нельзя передавать UpdateFilter как pattern).
    app.add_handler(
        CallbackQueryHandler(
            on_site_order_button_callback,
            pattern=_RE_SITE_ORDER_CB_PATTERN,
        )
    )
    app.add_handler(
        TypeHandler(Update, _try_handle_site_order_callback, block=False),
        group=0,
    )
    app.add_handler(
        CallbackQueryHandler(
            on_admin_panel_action,
            pattern=re.compile(r"^adm:(orders_new|orders_all|orders_shipped|stats)$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_user_order_open,
            pattern=re.compile(r"^(user_order_\d+|uos:[a-fA-F0-9]{12})$"),
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
            on_admin_user_messages,
            pattern=re.compile(r"^adm_user_(new|all|shipped)_\d+$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_order_status_buttons,
            pattern=re.compile(r"^(accept|sent|cancel|done|delmsg)_\d+$"),
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
        CallbackQueryHandler(
            on_checkout_nav_back,
            pattern=re.compile(
                r"^chk:(country_to_cart|preview_to_country|pay_to_preview|pay_to_methods)$"
            ),
        )
    )
    app.add_handler(CallbackQueryHandler(on_checkout_ask_username, pattern=re.compile(r"^co:0$")))
    app.add_handler(
        CallbackQueryHandler(on_delivery_country_pick, pattern=re.compile(r"^dl:(by|ru|ua|ot)$"))
    )
    app.add_handler(
        CallbackQueryHandler(
            on_payment_method,
            pattern=re.compile(r"^pay_(card|transfer|crypto)(:\d+)?$"),
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_payment_paid, pattern=re.compile(r"^paid(:\d+)?$")
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            on_postpaid_submit,
            pattern=re.compile(r"^ppdone:\d+$"),
        )
    )
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
    app.add_handler(CallbackQueryHandler(on_tinder_swipe, pattern=re.compile(r"^t:([pncvf])$")))
    app.add_handler(CallbackQueryHandler(on_add_to_cart, pattern=re.compile(r"^a:(.+)$")))
    app.add_handler(CallbackQueryHandler(on_back_rarity, pattern=re.compile(r"^h:([^:]{1,12})$")))
    app.add_handler(CallbackQueryHandler(on_home_promo_nav, pattern=re.compile(r"^hp:\d+$")))
    app.add_handler(CallbackQueryHandler(on_popular_inline, pattern=re.compile(r"^pop:0$")))
    app.add_handler(
        CallbackQueryHandler(on_catalog_currency_pick, pattern=re.compile(r"^ccur:(by|ru)$"))
    )
    app.add_handler(CallbackQueryHandler(on_menu_main, pattern=re.compile(r"^m:0$")))
    app.add_handler(
        CallbackQueryHandler(
            on_pick_rarity, pattern=re.compile(r"^j:([^:]{1,12}):(all|sale|\d+)$")
        )
    )
    app.add_handler(CallbackQueryHandler(on_pick_category, pattern=re.compile(r"^c:(\d+|all)$")))
    app.add_handler(CallbackQueryHandler(on_callback_query_unhandled), group=99)
    app.add_handler(MessageHandler(_PROOF_MEDIA_FILTER, on_user_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # Краткие сбои Telegram / наложение деплоев: повторить bootstrap (delete_webhook и т.д.).
    poll_bootstrap_retries = _env_int("TELEGRAM_POLLING_BOOTSTRAP_RETRIES", 35)
    app.run_polling(allowed_updates=Update.ALL_TYPES, bootstrap_retries=poll_bootstrap_retries)


if __name__ == "__main__":
    main()
