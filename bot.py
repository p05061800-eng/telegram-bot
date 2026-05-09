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
from copy import deepcopy
from datetime import datetime
from threading import Thread
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

import aiohttp
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
ADMIN_ACCESS_DENIED = "Нет доступа: эта команда только для администратора."


def is_admin(user_id):
    return user_id == ADMIN_ID


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
# Бонусы с сайта: в POST /api/sync/cart, /api/sync/state и verify-code JSON передавайте
# bonusPoints / bonusBalance; при начислении — bonusEarned. Кнопка «⭐ Бонусы» и списание в превью заказа.
USER_SITE_LOYALTY: Dict[int, dict] = {}
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
        USERNAME_TO_USER_ID[key] = uid


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


def _remember_user_message(uid: int, username: Optional[str], kind: str, text: str) -> None:
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
    if len(bucket) > 100:
        del bucket[:-100]


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


_POST_DEPLOY_USER_DATA_EPHEMERAL: Tuple[str, ...] = (
    "pending_order",
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
    через POST /api/verify-code или sync до первого нажатия пользователя — иначе «💚 Корзина»
    пустая и «Подтвердить заказ» теряет текст черновика.
    """
    if not uid:
        return
    _clear_checkout_delivery(user_data)
    for k in _POST_DEPLOY_USER_DATA_EPHEMERAL:
        user_data.pop(k, None)
    t_task = user_data.get("tinder_autoplay_task")
    if isinstance(t_task, asyncio.Task) and not t_task.done():
        try:
            t_task.cancel()
        except Exception:
            pass
    for k in _TINDER_USER_DATA_KEYS:
        user_data.pop(k, None)
    user_states.pop(int(uid), None)
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
MSG_LOYALTY_MENU = (
    "⭐ Бонусная программа IlluCards\n\n"
    "Баланс приходит с сайта (вход по коду, синк корзины / состояния): поля вроде "
    "bonusPoints, bonusBalance. В боте 1 бонус = 1 BYN или 1 RUB к сумме заказа "
    "(та же валюта, что у выбранной доставки).\n\n"
    "Как списать: оформите заказ из «💚 Корзина» — после выбора страны в превью заказа "
    "появятся кнопки «Выкл» / «50%» / «Макс». К оплате — уже с учётом бонусов. "
    "После успешной оплаты баланс в боте уменьшается; точное значение на сайте обновит ваш backend.\n\n"
    "Если баланс не виден — войдите на сайт через Telegram-код, чтобы сайт передал бонусы в бот."
)
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
MSG_CALLBACK_CATEGORY_INVALID = "Такой категории нет. Откройте каталог заново."
MSG_CART_CLEARED_TOAST = "Корзина очищена."
MSG_PAY_NEED_PROOF_FIRST = "Сначала пришлите фото чека оплаты."
MSG_PAY_FINISH_CURRENT = "Сначала завершите оплату по текущему заказу."
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
MSG_ORDER_SUBMIT_ADMIN_FAIL = (
    "Не удалось отправить заказ администратору. Попробуйте позже или напишите в «Связь»."
)
MSG_UNKNOWN_TEXT = (
    "Не понял сообщение. Отправьте /start или выберите действие в меню ниже 👇"
)

# Reply-клавиатура: короткие подписи + эмодзи
BTN_CATALOG = "📦 Каталог"
BTN_CART = "💚 Корзина"
BTN_POPULAR = "🔥 Акции"
BTN_CHAT = "💬 Связь"
BTN_MY_ORDERS = "📋 Мои заказы"
BTN_DELIVERY = "🚚 Доставка"
BTN_RANDOM_CARD = "🎁 Случайная карточка"
BTN_FAVORITES = "💚 Избранное"
BTN_BONUSES = "⭐ Бонусы"

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


async def _answer_order_callback_stale(q: Optional[CallbackQuery]) -> None:
    if not q:
        return
    try:
        await q.answer(MSG_CALLBACK_STALE_ORDER, show_alert=True)
    except Exception:
        pass


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
    url = (os.getenv("ILLUCARDS_LOGIN_CODE_SYNC_URL") or "").strip()
    secret = (os.getenv("ILLUCARDS_LOGIN_CODE_SYNC_SECRET") or "").strip()
    if not url:
        try:
            url = f"{_illucards_site_base_url()}/api/internal/sync-login-code"
        except Exception:
            url = ""
    if not url or not secret:
        return False
    un = str(username or "").strip().lstrip("@")
    payload: dict = {
        "code": str(code),
        "user_id": int(telegram_user_id),
        "username_display": un if un else f"id{int(telegram_user_id)}",
        "username_norm": _normalize_login_username(un) if un else "",
    }
    if wait_id:
        wid = _login_wait_id_from_start_payload(f"web_login_{wait_id}") or ""
        if wid:
            payload["wait_id"] = wid
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
            async with session.post(
                url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {secret}",
                    "Content-Type": "application/json",
                },
            ) as resp:
                if resp.status >= 300:
                    body = await resp.text()
                    logging.getLogger(__name__).warning(
                        "sync-login-code HTTP %s: %s", resp.status, body[:300]
                    )
                    return False
                return True
    except Exception:
        logging.getLogger(__name__).exception("sync-login-code failed")
        return False


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
        note_vc = _apply_site_loyalty_from_sync(uid, data)
        _schedule_loyalty_notify(bot, uid, note_vc)
    preview = (
        _format_login_site_cart_pending_text(uid, del_cc, products) if uid else ""
    )
    if bot and uid and preview:
        SITE_LOGIN_PENDING_ORDER[uid] = preview
        try:
            # await, а не create_task: иначе клиент может сразу уйти с страницы и задача не успевает.
            await _send_site_login_cart_order_message(bot, uid, preview)
        except Exception:
            logging.getLogger(__name__).exception(
                "verify-code → не удалось отправить корзину в Telegram user_id=%s",
                uid,
            )
    resp_vc: Dict[str, object] = {
        "success": True,
        "user_id": uid,
        "username": f"@{key}" if key else "",
    }
    lb_vc = USER_SITE_LOYALTY.get(uid, {}).get("balance") if uid else None
    if lb_vc is not None:
        try:
            resp_vc["bonus_balance"] = int(lb_vc)
        except (TypeError, ValueError):
            pass
    return _login_json_response(resp_vc)


def _sync_auth_ok(request: web.Request, data: dict) -> bool:
    """Проверка секрета синхронизации сайта -> бот."""
    if not SYNC_API_SECRET:
        return True
    header_secret = (request.headers.get("X-Sync-Secret") or "").strip()
    body_secret = str(data.get("secret") or "").strip()
    return hmac.compare_digest(header_secret or body_secret, SYNC_API_SECRET)


def _resolve_sync_uid(data: dict) -> int:
    raw_uid = data.get("user_id")
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


_LOYALTY_PENDING_EARN_TEXT_KEYS: Tuple[str, ...] = (
    "bonusMessage",
    "loyaltyMessage",
    "bonusNotice",
    "loyalty_note",
    "bonusInfo",
    "bonusText",
    "loyaltyText",
    "bonusHint",
    "bonus_hint",
)


def _loyalty_pending_earn_from_text(raw: object) -> Optional[int]:
    """Вытащить ожидаемое начисление из текста вида «За эту покупку: 300 баллов»."""
    if raw is None:
        return None
    s = str(raw).replace("\xa0", " ").strip()
    if not s:
        return None
    pats = (
        r"за\s+эту\s+покупк\w*[^0-9]{0,30}([0-9][0-9\s]{0,8})",
        r"за\s+заказ[^0-9]{0,30}([0-9][0-9\s]{0,8})",
        r"(?:будет\s+начисл\w*|начисл\w*)[^0-9]{0,30}([0-9][0-9\s]{0,8})",
        r"(?:will\s+earn|earned?\s+for\s+(?:this\s+)?order)[^0-9]{0,30}([0-9][0-9\s]{0,8})",
    )
    for pat in pats:
        m = re.search(pat, s, re.IGNORECASE)
        if not m:
            continue
        val = _coerce_loyalty_int((m.group(1) or "").replace(" ", ""))
        if val is not None and int(val) > 0:
            return int(val)
    return None


def _loyalty_pending_earn_from_text_fields(data: dict) -> Optional[int]:
    if not isinstance(data, dict):
        return None
    for k in _LOYALTY_PENDING_EARN_TEXT_KEYS:
        if k in data:
            v = _loyalty_pending_earn_from_text(data.get(k))
            if v is not None:
                return v
    for nest in _LOYALTY_NEST_KEYS:
        sub = data.get(nest)
        if isinstance(sub, dict):
            v = _loyalty_pending_earn_from_text_fields(sub)
            if v is not None:
                return v
    return None


_LOYALTY_ITEM_EARN_KEYS: Tuple[str, ...] = (
    "bonusWillEarn",
    "bonusForOrder",
    "bonusForThisOrder",
    "bonusToEarn",
    "expectedBonus",
    "orderBonusEstimate",
    "orderBonus",
    "bonusPoints",
    "pointsEarned",
    "pointsToEarn",
    "pointsForOrder",
    "cashbackEarned",
    "cashbackEstimate",
)


def _loyalty_pending_earn_from_cart_items(raw_items: object) -> Optional[int]:
    """Ожидаемое начисление из строк корзины, если сайт не дал top-level поле."""
    if not isinstance(raw_items, list):
        return None
    total = 0
    seen = False
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        qty = _cart_line_qty_coerce(it.get("qty") or it.get("quantity") or 1)
        per_item = None
        line_total = None
        for k in _LOYALTY_ITEM_EARN_KEYS:
            if k not in it:
                continue
            val = _coerce_loyalty_int(it.get(k))
            if val is None or int(val) <= 0:
                continue
            # lineBonus / orderBonusTotal обычно уже за всю строку, pointsToEarn чаще за штуку.
            lk = str(k).lower()
            if "line" in lk or "total" in lk or "order" in lk:
                line_total = int(val)
            else:
                per_item = int(val)
            seen = True
            break
        if line_total is not None:
            total += int(line_total)
        elif per_item is not None:
            total += int(per_item) * int(qty)
    if seen and total > 0:
        return int(total)
    return None


def _parse_site_loyalty_snapshot(data: dict) -> dict:
    """Сайт может слать бонусы в sync/login JSON — поддерживаем несколько имён полей."""
    if not isinstance(data, dict):
        return {}
    bal_k = (
        "bonusBalance",
        "bonus_balance",
        "loyaltyBalance",
        "loyalty_balance",
        "bonusPoints",
        "bonus_points",
        "loyaltyPoints",
        "loyalty_points",
        "pointsBalance",
        "walletBalance",
        "userBonus",
        "bonusesTotal",
        "bonusWallet",
        "cashbackBalance",
    )
    earned_k = (
        "bonusEarned",
        "bonus_earned",
        "pointsEarned",
        "loyaltyEarned",
        "earnedBonus",
        "bonusesAdded",
        "orderBonusEarned",
        "cashbackEarned",
        "bonusEarnedThisOrder",
        "pointsEarnedThisOrder",
        "creditEarned",
    )
    msg = ""
    for k in ("bonusMessage", "loyaltyMessage", "bonusNotice", "loyalty_note"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            msg = v.strip()[:500]
            break
    if not msg:
        for nest in _LOYALTY_NEST_KEYS:
            sub = data.get(nest)
            if not isinstance(sub, dict):
                continue
            for k in ("bonusMessage", "loyaltyMessage", "bonusNotice"):
                v = sub.get(k)
                if isinstance(v, str) and v.strip():
                    msg = v.strip()[:500]
                    break
            if msg:
                break
    return {
        "balance": _loyalty_find_int(data, bal_k, 2),
        "earned": _loyalty_find_int(data, earned_k, 2),
        "message": msg,
    }


def _apply_site_loyalty_from_sync(uid: int, data: dict) -> Optional[str]:
    """
    Обновляет USER_SITE_LOYALTY. Возвращает текст для уведомления в Telegram,
    если в payload есть положительное начисление (bonusEarned и т.п.).
    """
    if not uid:
        return None
    snap = _parse_site_loyalty_snapshot(data)
    earned_i = snap.get("earned")
    bal_i = snap.get("balance")
    msg = str(snap.get("message") or "").strip()
    prev = USER_SITE_LOYALTY.get(int(uid))
    if not isinstance(prev, dict):
        prev = {}
    prev_bal = prev.get("balance")
    try:
        prev_bal_i = int(prev_bal) if prev_bal is not None else None
    except (TypeError, ValueError):
        prev_bal_i = None
    notify: Optional[str] = None
    if earned_i is not None and int(earned_i) > 0:
        parts = [f"⭐ Зачислено бонусов: +{int(earned_i)}."]
        if bal_i is not None:
            parts.append(f"Всего на бонусном счёте: {int(bal_i)}.")
        elif prev_bal_i is not None:
            parts.append(
                f"Всего на бонусном счёте: {int(prev_bal_i) + int(earned_i)} (по данным бота)."
            )
        if msg:
            parts.append(msg)
        notify = "\n".join(parts)
    new_bal = bal_i if bal_i is not None else prev_bal_i
    if new_bal is None and prev_bal_i is not None and earned_i is not None and earned_i > 0:
        new_bal = int(prev_bal_i) + int(earned_i)
    hint = msg or str(prev.get("hint") or "").strip()[:400]
    USER_SITE_LOYALTY[int(uid)] = {
        "balance": new_bal,
        "hint": hint,
        "updated": time.time(),
        "last_earned": int(earned_i) if earned_i is not None and earned_i > 0 else prev.get("last_earned"),
    }
    return notify


def _loyalty_cart_footer_lines(uid: int) -> List[str]:
    rec = USER_SITE_LOYALTY.get(int(uid))
    if not isinstance(rec, dict):
        return []
    out: List[str] = []
    bal = rec.get("balance")
    if bal is not None:
        try:
            out.append(f"⭐ Бонусный счёт: {int(bal)}")
        except (TypeError, ValueError):
            pass
    hint = str(rec.get("hint") or "").strip()
    if hint and len(hint) < 300:
        out.append(hint)
    return out


def _loyalty_balance_int(uid: int) -> int:
    rec = USER_SITE_LOYALTY.get(int(uid))
    if not isinstance(rec, dict):
        return 0
    try:
        return max(0, int(rec.get("balance") or 0))
    except (TypeError, ValueError):
        return 0


def _loyalty_apply_local_debit(uid: int, amount: int) -> None:
    """После оплаты: уменьшить локальный баланс (сайт пришлёт актуализацию в sync)."""
    if not uid or int(amount) <= 0:
        return
    rec = USER_SITE_LOYALTY.get(int(uid))
    if not isinstance(rec, dict):
        return
    try:
        bal = max(0, int(rec.get("balance") or 0))
    except (TypeError, ValueError):
        bal = 0
    rec["balance"] = max(0, bal - int(amount))
    rec["updated"] = time.time()


def _loyalty_apply_local_credit(uid: int, amount: int) -> None:
    """Fallback для заказов из бота: сайт может быть временно недоступен для синка."""
    if not uid or int(amount) <= 0:
        return
    rec = USER_SITE_LOYALTY.get(int(uid))
    if not isinstance(rec, dict):
        rec = {}
        USER_SITE_LOYALTY[int(uid)] = rec
    try:
        bal = max(0, int(rec.get("balance") or 0))
    except (TypeError, ValueError):
        bal = 0
    rec["balance"] = bal + int(amount)
    rec["last_earned"] = int(amount)
    rec["updated"] = time.time()


_LOYALTY_PENDING_EARN_KEYS: Tuple[str, ...] = (
    "bonusWillEarn",
    "bonusesWillEarn",
    "bonusForOrder",
    "bonusForThisOrder",
    "bonusToEarn",
    "expectedBonus",
    "orderBonusEstimate",
    "orderBonus",
    "loyaltyPointsToEarn",
    "pointsToEarn",
    "pointsForOrder",
    "cashbackEstimate",
    "bonusAccrualEstimate",
    "orderBonusAccrual",
)


def _loyalty_pending_earn_from_dict(data: dict) -> Optional[int]:
    """Явная оценка начисления из JSON заказа/сайта (если есть)."""
    if not isinstance(data, dict):
        return None
    v = _loyalty_find_int(data, _LOYALTY_PENDING_EARN_KEYS, 3)
    if v is not None and int(v) > 0:
        return int(v)
    return _loyalty_pending_earn_from_text_fields(data)


def _loyalty_earn_percent_from_env() -> int:
    """Доля суммы к оплате для оценки начисления, 0…100. 0 — только явные поля заказа; по умолчанию 5."""
    return max(0, min(100, _env_int("ILLUCARDS_LOYALTY_EARN_PERCENT", 5)))


def _loyalty_points_per_unit_from_env() -> int:
    """Как на сайте IlluCards: баллов за 1 единицу товара. 0 — не использовать правило qty×балл."""
    return max(0, _env_int("ILLUCARDS_LOYALTY_POINTS_PER_UNIT", 100))


def _cart_total_units(lines: List[dict]) -> int:
    n = 0
    for x in lines:
        if isinstance(x, dict):
            n += int(_cart_line_qty_coerce(x.get("qty")))
    return max(0, int(n))


def _loyalty_compute_earn_estimate(
    pay_total: int,
    hint: Optional[dict] = None,
    *,
    cart_lines: Optional[List[dict]] = None,
) -> Optional[int]:
    """
    Оценка бонусов с заказа:
    1) поля/текст сайта (hint + sync);
    2) сумма по позициям, если в строках корзины есть числа;
    3) qty × ILLUCARDS_LOYALTY_POINTS_PER_UNIT (по умолчанию 100 — как витрина «за единицу»);
    4) иначе процент от суммы (ILLUCARDS_LOYALTY_EARN_PERCENT).
    """
    pt = max(0, int(pay_total or 0))
    if pt <= 0:
        return None
    cap = max(pt * 3, 500_000)
    if isinstance(hint, dict):
        raw = _loyalty_pending_earn_from_dict(hint)
        if raw is not None:
            if raw <= cap:
                return int(raw)
    if cart_lines:
        item_sum = _loyalty_pending_earn_from_cart_items(cart_lines)
        if item_sum is not None and int(item_sum) > 0:
            if int(item_sum) <= cap:
                return int(item_sum)
    ppu = _loyalty_points_per_unit_from_env()
    if ppu > 0 and cart_lines:
        units = _cart_total_units(list(cart_lines))
        if units > 0:
            return int(units) * int(ppu)
    pct = _loyalty_earn_percent_from_env()
    if pct > 0:
        return (pt * pct) // 100
    return None


def _checkout_bonus_cap(uid: int, grand_before_bonus: int) -> int:
    return min(_loyalty_balance_int(uid), max(0, int(grand_before_bonus)))


def _checkout_bonus_spend_effective(
    user_data: dict, uid: int, grand_before_bonus: int
) -> int:
    cap = _checkout_bonus_cap(uid, grand_before_bonus)
    try:
        want = int(user_data.get("checkout_bonus_spend") or 0)
    except (TypeError, ValueError):
        want = 0
    return max(0, min(max(0, want), cap))


def _checkout_preview_finance(
    user_data: dict, checkout_uid: int
) -> Optional[dict]:
    """Сумма до списания бонусов и валюта для превью оформления."""
    lines: List[dict] = list(user_data.get("order_checkout") or [])
    if not lines:
        return None
    code = str(user_data.get("delivery_country") or "")
    opt = DELIVERY_OPTIONS.get(code)
    if not opt:
        return None
    dlabel, damount, dcur = opt[0], opt[1], opt[2]
    goods_total, _ = _cart_totals(lines)
    site_labels = {
        str(x.get("line_currency") or "").strip().upper()
        for x in lines
        if x.get("from_site") and str(x.get("line_currency") or "").strip().upper() in ("BYN", "RUB")
    }
    if site_labels and all(x.get("from_site") for x in lines) and len(site_labels) == 1:
        g_cur = site_labels.pop()
    else:
        g_cur = _goods_currency_for_delivery_country(code)
    site_gt_raw = (
        _cart_get_site_grand_total(checkout_uid, g_cur) if checkout_uid else None
    )
    inc = _cart_site_delivery_included(checkout_uid) if checkout_uid else False
    exp_line = int(goods_total) if inc else int(goods_total) + int(damount)
    trust_site = False
    if checkout_uid and site_gt_raw and int(site_gt_raw) > 0:
        sg = int(site_gt_raw)
        if not _site_grand_covers_goods(sg, int(goods_total)):
            trust_site = False
        elif inc:
            trust_site = not (exp_line > 0 and sg < exp_line - max(50, exp_line // 40))
        else:
            trust_site = exp_line > 0 and abs(sg - exp_line) <= max(100, exp_line // 25)
    use_site = bool(site_gt_raw and trust_site)
    grand_show = int(site_gt_raw) if use_site else exp_line
    return {
        "lines": lines,
        "grand_show": int(grand_show),
        "g_cur": g_cur,
        "goods_total": int(goods_total),
        "code": code,
        "dlabel": dlabel,
        "dcur": dcur,
        "damount": int(damount),
        "inc": bool(inc),
        "use_site": bool(use_site),
        "site_gt_raw": int(site_gt_raw) if site_gt_raw else None,
    }


async def _notify_loyalty_earned(bot, uid: int, text: str) -> None:
    if not bot or not uid or not (text or "").strip():
        return
    log = logging.getLogger(__name__)
    try:
        await bot.send_message(
            int(uid),
            str(text).strip()[:3900],
            disable_web_page_preview=True,
        )
    except Exception:
        log.exception("loyalty notify → uid=%s", uid)


def _schedule_loyalty_notify(bot, uid: int, text: Optional[str]) -> None:
    if not bot or not uid or not text:
        return
    try:
        asyncio.get_running_loop().create_task(_notify_loyalty_earned(bot, int(uid), text))
    except RuntimeError:
        pass


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
            qty = int(x.get("qty") or 1)
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
            USER_FAVORITES[uid] = _normalize_sync_favorites_with_catalog(
                data[key], products
            )
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
    try:
        total = int(float(raw.get("total") or total_goods or 0))
    except (TypeError, ValueError):
        total = int(total_goods)
    st = raw.get("status")
    if st is None or str(st).strip() == "":
        status_label = "На сайте"
    else:
        status_label = str(st).strip()
    return {
        "id": ext[:80],
        "external_id": ext[:120],
        "items": lines,
        "total": max(0, int(total)),
        "total_goods": int(total_goods),
        "delivery": drec,
        "status": status_label,
        "sync_source": "site",
    }


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
    existing = list(USER_ORDERS.get(int(uid)) or [])
    kept = [r for r in existing if str(r.get("sync_source") or "") != "site"]
    USER_ORDERS[int(uid)] = kept + list(site_orders or [])


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
    bot_code, _, _, _ = _delivery_option_for_site_code(str(del_raw or "BY"))
    lines = _normalize_sync_cart_items(data.get("items"), bot_code)
    _remember_user_delivery_country(uid, bot_code)
    try:
        products = await load_products()
    except Exception:
        products = []
    if products and lines:
        _reconcile_cart_lines_to_catalog(products, lines)
    _cart_set_items_uid(uid, lines)
    _cart_apply_site_pricing_hints(uid, data)
    _apply_optional_favorites_from_site_payload(uid, data, products)
    note_loy = _apply_site_loyalty_from_sync(uid, data)
    bot_loy = request.app.get("bot")
    _schedule_loyalty_notify(bot_loy, uid, note_loy)
    users_touch(uid, "cart_sync")
    body_loy: Dict[str, object] = {"success": True, "user_id": uid, "items": len(lines)}
    lb_loy = USER_SITE_LOYALTY.get(uid, {}).get("balance")
    if lb_loy is not None:
        try:
            body_loy["bonus_balance"] = int(lb_loy)
        except (TypeError, ValueError):
            pass
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
    USER_FAVORITES[uid] = refs
    users_touch(uid, "favorites_sync")
    logging.getLogger(__name__).info(
        "sync/favorites: user_id=%s позиций=%s", uid, len(refs)
    )
    return _login_json_response({"success": True, "user_id": uid, "items": len(refs)})


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
    note_st = _apply_site_loyalty_from_sync(uid, data)
    bot_st = request.app.get("bot")
    _schedule_loyalty_notify(bot_st, uid, note_st)
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
    lb_st = USER_SITE_LOYALTY.get(uid, {}).get("balance")
    if lb_st is not None:
        try:
            body["bonus_balance"] = int(lb_st)
        except (TypeError, ValueError):
            pass
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
    app.router.add_post("/api/send-code", _http_send_code)
    app.router.add_post("/api/verify-code", _http_verify_code)
    app.router.add_post("/api/telegram-auth", _http_telegram_auth)
    app.router.add_post("/api/sync/cart", _http_sync_cart)
    app.router.add_post("/api/sync/favorites", _http_sync_favorites)
    app.router.add_post("/api/sync/state", _http_sync_state)
    app.router.add_post("/api/sync/promotions", _http_sync_home_promotions)
    runner = web.AppRunner(app)
    await runner.setup()
    # Логин API держим на отдельном порту, чтобы не конфликтовать с Flask /health на PORT.
    raw_port = (os.getenv("LOGIN_API_PORT") or "8765").strip()
    try:
        port = int(raw_port)
    except ValueError:
        port = 8765
    host_raw = (os.getenv("LOGIN_API_HOST") or "").strip()
    if host_raw:
        host = host_raw
    else:
        host = "127.0.0.1"
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    log.info("Вход на сайт: HTTP %s:%s (/, /login, /api/send-code, /api/verify-code)", host, port)
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
ORDER_STATUS_UPDATE_API_URL = os.getenv(
    "ORDER_STATUS_UPDATE_API_URL",
    f"{ILLUCARDS_BASE}/api/order/update",
).strip()
ORDER_FROM_BOT_API_URL = os.getenv(
    "ORDER_FROM_BOT_API_URL",
    f"{ILLUCARDS_BASE}/api/order/from-bot",
).strip()
ORDER_STATUS_UPDATE_SECRET = os.getenv("ILLUCARDS_ORDER_UPDATE_SECRET", "").strip()
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
    try:
        u = int(uid or 0)
    except (TypeError, ValueError):
        u = 0
    c = str(code or "").strip().lower()
    if not u or c not in DELIVERY_OPTIONS:
        return
    USER_PREF_DELIVERY_COUNTRY[u] = c


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
        [KeyboardButton(BTN_CATALOG), KeyboardButton(BTN_CART)],
        [KeyboardButton(BTN_POPULAR), KeyboardButton(BTN_CHAT)],
        [KeyboardButton(BTN_MY_ORDERS), KeyboardButton(BTN_DELIVERY)],
        [KeyboardButton(BTN_FAVORITES), KeyboardButton(BTN_RANDOM_CARD)],
        [KeyboardButton(BTN_BONUSES)],
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
        BTN_FAVORITES,
        BTN_RANDOM_CARD,
        BTN_BONUSES,
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
            b["site_cart_grand_currency"] = _infer_site_grand_total_currency(data)
    else:
        b.pop("site_cart_grand_total", None)
        b.pop("site_cart_grand_currency", None)
    if _parse_site_delivery_included_in_total(data):
        b["site_delivery_included"] = True
    else:
        b.pop("site_delivery_included", None)
    pending = _loyalty_pending_earn_from_dict(data)
    if pending is None:
        pending = _loyalty_pending_earn_from_cart_items(
            data.get("cart") if isinstance(data, dict) else None
        )
    if pending is None:
        pending = _loyalty_pending_earn_from_cart_items(
            data.get("items") if isinstance(data, dict) else None
        )
    if pending is not None and int(pending) > 0:
        b["site_loyalty_pending_earn"] = int(pending)
    else:
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
    b = USER_CART.get(int(uid))
    if not isinstance(b, dict):
        return None
    v = _coerce_card_price_int(b.get("site_loyalty_pending_earn"))
    if v <= 0:
        return None
    return int(v)


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
        _, _, d_amt, _ = DELIVERY_OPTIONS.get(code, DELIVERY_OPTIONS["by"])
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
    Итог для текста «💚 Корзина». Второй элемент True — показываем как «с сайта».
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
    try:
        est = int(loyalty_earn_estimate) if loyalty_earn_estimate is not None else 0
    except (TypeError, ValueError):
        est = 0
    if est > 0:
        body += (
            f"\n\n⭐ Ориентировочно начислится бонусов с заказа: ~{est}. "
            "Точное значение подтвердит сайт после оплаты."
        )
    return body


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
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        rows.append(f"• {name} — {sub} {cur}")
    return "\n".join(rows) if rows else "—"


def _format_admin_order_detail_text(order_id: int, o: dict) -> str:
    """Карточка заказа для админа (уведомление, open_order, правка сообщения)."""
    uid = int(o.get("user_id") or 0)
    un = o.get("username")
    un_s = str(un).strip().lstrip("@") if un else ""
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else None
    line_cur = _order_line_currency_from_delivery(d)
    items_block = _format_order_items_for_admin(list(o.get("items") or []), line_cur)
    tot = _order_resolved_grand_total(o)
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
    try:
        b_ap = int(o.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        b_ap = 0
    if b_ap > 0:
        parts.extend(["", f"⭐ Списано бонусов: {b_ap} {line_cur}"])
    parts.extend(
        [
            "",
            f"💰 К оплате: {tot} {line_cur}",
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


def _format_payment_proof_caption(order_id: int, o: dict, uid: int) -> str:
    """Подпись к фото чека для админа (лимит Telegram caption 1024)."""
    body = _format_admin_order_detail_text(order_id, o)
    pm = str(o.get("payment_pending_method") or "").strip().lower()
    pm_ru = {"card": "💳 Карта", "transfer": "📱 Перевод", "crypto": "₿ Крипта"}.get(
        pm, "—"
    )
    head = f"📸 Чек оплаты · Заказ #{order_id}\n👤 id: {uid} · способ: {pm_ru}\n\n"
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


async def _send_deferred_admin_order_panel(
    context: ContextTypes.DEFAULT_TYPE, order_id: int, o: dict
) -> None:
    """Карточка заказа админу (принять/отправить/…), если при оформлении не отправляли."""
    # Жёсткий барьер: никакой карточки админу до подтверждения оплаты.
    if not o.get("paid"):
        return
    if o.get("admin_chat_id") is not None and o.get("admin_message_id") is not None:
        return
    log = logging.getLogger(__name__)
    text = _format_admin_order_detail_text(order_id, o)
    kb = _kb_order_admin_actions(order_id, str(o.get("status") or "new"))
    m = None
    try:
        m = await context.bot.send_message(
            chat_id=ORDER_NOTIFY_TARGET,
            text=text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        log.exception(
            "post-payment order panel → ORDER_NOTIFY_TARGET order_id=%s", order_id
        )
    if m is None:
        try:
            m = await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=text,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception:
            log.exception("post-payment order panel → ADMIN_ID order_id=%s", order_id)
            return
    o["admin_chat_id"] = int(m.chat_id)
    o["admin_message_id"] = int(m.message_id)


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
) -> Optional[int]:
    """Новый заказ только в ORDERS; в ORDER_NOTIFY_TARGET карточка заказа — после оплаты (_send_deferred_admin_order_panel)."""
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
    ORDERS[order_id] = rec
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


def _payment_total_label(o: dict) -> str:
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
    cur = str(d.get("currency") or "BYN")
    cc = str(d.get("country") or "").strip().lower()
    stored = _order_resolved_grand_total(o)
    g_cur = _goods_currency_for_delivery_country(cc)
    if stored > 0:
        if cc == "by" and cur == "BYN":
            return f"{stored} BYN"
        return f"{stored} {g_cur}"
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
    if cc == "by" and cur == "BYN":
        return f"{goods + d_amt} BYN"
    if d_amt > 0:
        return f"{goods + d_amt} {g_cur}"
    return f"{goods} {g_cur}"


def _kb_paid_confirm(total_label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(f"✅ Оплатить + {total_label}", callback_data="paid")]]
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
        lines.append(f"⭐ Списано бонусов: {b_ap} {line_cur}")
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
    cust_rc = int(o.get("user_id") or 0)
    try:
        earn_est = int(o.get("loyalty_earn_estimate") or 0)
    except (TypeError, ValueError):
        earn_est = 0
    if earn_est > 0:
        lines.append("")
        lines.append(
            f"⭐ Ожидаемое начисление с этого заказа: ~{earn_est} бонусов "
            "(фактическое начисление пришлёт сайт; мы пришлём сообщение при синхронизации)."
        )
    rec_loy = USER_SITE_LOYALTY.get(cust_rc) if cust_rc else None
    lines.append("")
    has_known_balance = isinstance(rec_loy, dict) and rec_loy.get("balance") is not None
    if has_known_balance:
        bal_rc = _loyalty_balance_int(cust_rc)
        lines.append(f"⭐ На бонусном счёте сейчас: {bal_rc}.")
    else:
        lines.append(
            "⭐ Баланс бонусного счёта обновится после синхронизации с сайтом."
        )
    if isinstance(rec_loy, dict):
        hint_loy = str(rec_loy.get("hint") or "").strip()
        if hint_loy and len(hint_loy) < 300:
            lines.append(hint_loy)
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
    """Текст запроса реквизитов доставки после подтверждения оплаты — по стране из заказа."""
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
    cc = str(d.get("country") or "").strip().lower()
    if cc == "by":
        return MSG_POSTPAID_SHIPPING_BY
    if cc == "ru":
        return MSG_POSTPAID_SHIPPING_RU
    return MSG_POSTPAID_SHIPPING_INTL


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
    """Итог по строкам заказа + сумма доставки минус списанные бонусы."""
    goods, _ = _cart_totals(list(o.get("items") or []))
    d = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
    try:
        d_amt = int(d.get("amount") or 0)
    except (TypeError, ValueError):
        d_amt = 0
    raw = max(0, int(goods) + int(d_amt))
    try:
        b_ap = max(0, int(o.get("bonus_applied") or 0))
    except (TypeError, ValueError):
        b_ap = 0
    if b_ap > raw:
        b_ap = raw
    return max(0, raw - b_ap)


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
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = _coerce_card_price_int(x.get("price"))
        q = _cart_line_qty_coerce(x.get("qty"))
        lc = str(x.get("line_currency") or "").strip().upper()
        line_cur = lc if lc in ("BYN", "RUB") else cur
        if q <= 1:
            out.append(f"• {name} — {p} {line_cur}")
        else:
            out.append(f"• {name} — {p * q} {line_cur} (×{q})")
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
    bot_code, d_label, d_amt, d_cur = _delivery_option_for_site_code(delivery_cc)
    goods, _ = _cart_totals(lines)
    site_labels = {
        str(x.get("line_currency") or "").strip().upper()
        for x in lines
        if x.get("from_site") and str(x.get("line_currency") or "").strip().upper() in ("BYN", "RUB")
    }
    if site_labels and all(x.get("from_site") for x in lines) and len(site_labels) == 1:
        g_cur = site_labels.pop()
    else:
        g_cur = _goods_currency_for_delivery_country(bot_code)
    out: List[str] = []
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        lc = str(x.get("line_currency") or "").strip().upper()
        xcur = lc if lc in ("BYN", "RUB") else g_cur
        if q <= 1:
            out.append(f"• {name} — {p} {xcur}")
        else:
            out.append(f"• {name} — {p * q} {xcur} (×{q})")
    out.append("")
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
    grand_show = int(site_gt_raw) if use_site else exp_line
    if inc and not site_gt_raw:
        out.append(f"🚚 Доставка: {d_label} (уже в сумме на сайте)")
        out.append(f"💰 Итого: {grand_show} {g_cur}")
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
        out.append(f"💰 Итого: {grand_show} {g_cur}")
    foot_pv = _loyalty_cart_footer_lines(uid)
    if foot_pv:
        out.append("")
        out.extend(foot_pv)
    s = "\n".join(out)
    if len(s) > 3500:
        s = s[:3490] + "…"
    return s


async def _send_site_login_cart_order_message(bot, uid: int, preview_inner: str) -> None:
    """Сообщение в Telegram после успешного входа на сайт по коду."""
    log = logging.getLogger(__name__)
    if not preview_inner.strip():
        return
    intro = (
        "✅ Вход на сайт подтверждён.\n\n"
        "Состав из корзины сайта уже в «💚 Корзина» бота. "
        "«Подтвердить заказ» только закрывает черновик здесь — в чат админа ничего не отправляется; "
        "администратор увидит заказ после оплаты (оформите из «💚 Корзина» → доставка → оплата → скрин). "
        "«Отмена» — без подтверждения."
    )
    body = f"{intro}\n\n{preview_inner.strip()}"
    if len(body) > 4090:
        body = body[:4082] + "…"
    try:
        await bot.send_message(
            chat_id=int(uid),
            text=body,
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ Подтвердить заказ",
                            callback_data="confirm_order",
                        ),
                        InlineKeyboardButton(
                            "Отмена",
                            callback_data="cancel_order",
                        ),
                    ],
                ],
            ),
        )
    except Exception:
        log.exception("verify-code → не удалось отправить корзину user_id=%s", uid)


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


def _format_mine_orders_text_and_kb(
    user_id: int,
) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
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
        st = str(o.get("status") or "new")
        badge = _user_order_status_badge(st)
        cur = _order_line_currency_from_delivery(
            o.get("delivery") if isinstance(o.get("delivery"), dict) else None
        )
        lines.append(f"#{oid} — {tot} {cur} — {badge}")
        rows.append(
            [
                InlineKeyboardButton("📦", callback_data=f"user_order_{oid}"),
            ],
        )
    for rec in site_recs[:20]:
        if len(rows) >= 30:
            break
        tot = int(rec.get("total") or 0)
        st_site = str(rec.get("status") or "на сайте").strip()
        badge = f"🌐 {st_site[:36]}" if st_site else "🌐 на сайте"
        drec = rec.get("delivery") if isinstance(rec.get("delivery"), dict) else None
        cur = _order_line_currency_from_delivery(drec)
        disp = str(rec.get("external_id") or rec.get("id") or "")[:20]
        lines.append(f"🌐 {disp} — {tot} {cur} — {badge}")
        tok = _site_user_order_token(int(user_id), rec)
        rows.append(
            [
                InlineKeyboardButton("📦", callback_data=f"uos:{tok}"),
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
        parts.append(f"⭐ Списано бонусов: {b_ap} {line_cur}")
        parts.append("")
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
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        lc = str(x.get("line_currency") or "").strip().upper()
        line_cur = lc if lc in ("BYN", "RUB") else cur
        out.append(f"• {name} — {p} {line_cur} × {q}")
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
    spend = 0
    if checkout_uid:
        spend = _checkout_bonus_spend_effective(user_data, checkout_uid, grand_show)
    grand_final = max(0, int(grand_show) - int(spend))
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
        lc = str(x.get("line_currency") or "").strip().upper()
        xcur = lc if lc in ("BYN", "RUB") else g_cur
        out.append(f"• {name} — {sub} {xcur}")
    out.append("")
    if inc and not site_gt_raw:
        out.append(f"🚚 Доставка: {dlabel} (уже в сумме на сайте)")
        out.append("")
        if spend > 0:
            out.append(f"⭐ Списание бонусов: −{spend} {g_cur}")
            out.append("")
        out.append(f"💰 Итого: {grand_final} {g_cur}")
    elif use_site:
        out.append(f"🚚 Доставка: {dlabel}")
        out.append("")
        if spend > 0:
            out.append(f"⭐ Списание бонусов: −{spend} {g_cur}")
            out.append("")
        out.append(f"💰 Итого: {grand_final} {g_cur} (как на сайте)")
    else:
        out.append(f"🚚 Доставка: {dlabel}")
        out.append("")
        if spend > 0:
            out.append(f"⭐ Списание бонусов: −{spend} {g_cur}")
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
    """Отменить неоплаченный заказ в ORDERS и вернуть черновик в user_data (для «Назад»)."""
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
    ORDERS.pop(oid, None)
    lst = list(USER_ORDERS.get(uid) or [])
    USER_ORDERS[uid] = [x for x in lst if str(x.get("id") or "") != str(oid)]
    if not USER_ORDERS[uid]:
        USER_ORDERS.pop(uid, None)
    ud.pop("awaiting_payment_order_id", None)
    ud.pop("payment_pending_method", None)
    o.pop("payment_pending_method", None)
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
    fin = _checkout_preview_finance(user_data, uid)
    if fin and uid and _checkout_bonus_cap(uid, int(fin["grand_show"])) > 0:
        rows.append(
            [
                InlineKeyboardButton("⭐ Выкл", callback_data="bo:s:0"),
                InlineKeyboardButton("⭐ 50%", callback_data="bo:s:h"),
                InlineKeyboardButton("⭐ Макс", callback_data="bo:s:m"),
            ]
        )
    return InlineKeyboardMarkup(rows)


def _kb_payment_methods_with_back() -> InlineKeyboardMarkup:
    rows = list(_kb_payment_methods().inline_keyboard)
    rows.append(
        [
            InlineKeyboardButton(
                "◀️ К подтверждению заказа", callback_data="chk:pay_to_preview"
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


def _kb_paid_confirm_with_back(total_label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"✅ Оплатить + {total_label}", callback_data="paid"
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
        await q.answer(f"💚 Корзина: {short}", show_alert=False)
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
    for k in ("order", "data", "result", "payload"):
        v = raw.get(k)
        if isinstance(v, dict):
            out.append(v)
    return out


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


def _deep_link_delivery_bot_code(raw: dict) -> str:
    """Код доставки by|ru|ua|ot из JSON заказа с сайта (в т.ч. вложенный order/data)."""
    cc = ""
    for cand in _deep_link_candidate_dicts(raw):
        d = cand.get("delivery")
        if isinstance(d, dict):
            cc = str(d.get("country") or d.get("code") or "").strip()
            if cc:
                break
        if not cc:
            for key in ("delivery_country", "deliveryCountry", "deliveryRegion"):
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
    elif isinstance(d, str) and d.strip():
        country, label, amount, currency = _delivery_option_for_site_code(d)
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
        opt = DELIVERY_OPTIONS.get(region_bot, DELIVERY_OPTIONS["by"])
        label, amount, currency = opt[0], int(opt[1]), str(opt[2])
    if region_bot != "by":
        _, _, currency = DELIVERY_OPTIONS.get(region_bot, DELIVERY_OPTIONS["ru"])
    return {
        "items": items_out,
        "delivery": {
            "country": region_bot,
            "label": label,
            "amount": int(amount),
            "currency": currency,
        },
        "external_id": str(raw.get("id") or external_id),
        "total": raw.get("total"),
        "site_grand_total_hint": _deep_link_raw_grand_total(raw),
    }


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
    if ORDER_STATUS_UPDATE_SECRET:
        headers["Authorization"] = f"Bearer {ORDER_STATUS_UPDATE_SECRET}"
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


async def _fetch_order_for_deep_link(order_id: str) -> Optional[dict]:
    products: List[dict] = []
    try:
        products = await load_products() or []
    except Exception:
        products = []
    o = _fetch_order_from_shared_memory(order_id)
    if o:
        if products:
            _deep_link_apply_catalog_prices(o, products)
        return o
    o = await _fetch_order_from_deep_link_api(order_id, products)
    if o:
        return o
    o = _find_user_order_snapshot_normalized(order_id)
    if o and products:
        _deep_link_apply_catalog_prices(o, products)
    return o


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
    """TG-корзина — после нажатия «✅ Оплатить» (callback paid), если у заказа clear_cart_on_paid."""
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
    """Если задан ILLUCARDS_CART_CLEAR_ON_PROOF_URL — сообщить сайту очистить корзину пользователя."""
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
        "bonus_points_spent": int(order.get("bonus_applied") or 0),
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
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        lc = str(x.get("line_currency") or "").strip().upper()
        xcur = lc if lc in ("BYN", "RUB") else g_cur
        out.append(f"• {name} — {sub} {xcur}")
    out.append("")
    out.append(f"🚚 Доставка: {label}")
    out.append("")
    hint = _coerce_card_price_int(order.get("site_grand_total_hint") or 0)
    computed = int(goods_total) + int(amount)
    total_show = computed
    # Подсказку с сайта берём только если она не меньше суммы товаров и близка к
    # «товары + доставка» — иначе в JSON часто попадает чужое поле (скидка, BYN и т.д.).
    if (
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.from_user:
        return
    uid = msg.from_user.id
    users_touch(uid, "start")
    _register_login_username(uid, msg.from_user.username)
    args = context.args or []
    await _reply_site_transition_notice(msg, uid)
    if args:
        first = (args[0] or "").strip()
        joined = " ".join(args).strip()
        jl = joined.lower()
        fl = (first or "").strip().lower()
        if _is_web_login_start_payload(jl) or _is_web_login_start_payload(fl):
            await _maybe_thank_first_telegram_auth(msg, uid)
            un = (msg.from_user.username or "").strip()
            wait_id = _login_wait_id_from_start_payload(jl) or _login_wait_id_from_start_payload(fl)
            code = _issue_login_code(uid, un)
            await _sync_login_code_to_site(code, uid, un, wait_id=wait_id)
            await msg.reply_text(
                _telegram_login_code_message(code),
                reply_markup=REPLY_KB,
            )
            await msg.reply_text(
                "Код уже отправлен. Вернитесь на сайт, вставьте его и нажмите «Войти».",
                reply_markup=REPLY_KB,
            )
            return
        oid = _parse_order_id_from_start_args(list(args))
        if oid:
            order = await _fetch_order_for_deep_link(oid)
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
            tok = secrets.token_hex(8)
            context.user_data["deep_link_order_session"] = {
                "token": tok,
                "order": deepcopy(order),
            }
            preview = _format_user_deep_link_order_message(order)
            body = f"{START_ORDER_FROM_SITE_HEADER}\n\n{preview}"
            if len(body) > 4000:
                body = body[:3990] + "…"
            await msg.reply_text(
                body,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "✅ Подтвердить заказ",
                                callback_data=f"dlco:{tok}",
                            ),
                            InlineKeyboardButton(
                                "Отмена",
                                callback_data=f"dlca:{tok}",
                            ),
                        ],
                    ],
                ),
            )
            await msg.reply_text(
                "Полную коллекцию можно открыть на сайте:",
                reply_markup=_illucards_site_open_markup(uid),
            )
            await msg.reply_text(START_WELCOME_MENU_TEXT, reply_markup=REPLY_KB)
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
        await msg.reply_text(
            body,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ Подтвердить и отправить",
                            callback_data="confirm_order",
                        ),
                        InlineKeyboardButton(
                            "Отмена",
                            callback_data="cancel_order",
                        ),
                    ],
                ],
            ),
        )
        await msg.reply_text(
            "Полную коллекцию можно открыть на сайте:",
            reply_markup=_illucards_site_open_markup(uid),
        )
        await msg.reply_text(START_WELCOME_MENU_TEXT, reply_markup=REPLY_KB)
        return
    await _send_start_intro_with_site_button(msg, uid, context.user_data)


async def on_deep_link_confirm_order(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if (q.data or "").strip() != "confirm_order":
        return
    ud = context.user_data
    u = q.from_user
    uid_cb = int(u.id) if u else 0
    order_text = (ud.get("pending_order") or "").strip()
    if not order_text and uid_cb:
        order_text = (SITE_LOGIN_PENDING_ORDER.get(uid_cb) or "").strip()
    if not order_text:
        await _answer_order_callback_stale(q)
        return
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
                try:
                    await q.answer(MSG_PAY_NEED_PROOF_FIRST, show_alert=True)
                except Exception:
                    pass
                return
        pend = ud.get("awaiting_payment_order_id")
        if pend is not None:
            try:
                po = ORDERS.get(int(pend))
            except (TypeError, ValueError):
                po = None
            if not po or int(po.get("user_id") or 0) != int(uid_cb) or po.get("paid"):
                ud.pop("awaiting_payment_order_id", None)
                ud.pop("payment_pending_method", None)
            else:
                try:
                    await q.answer(MSG_PAY_FINISH_CURRENT, show_alert=True)
                except Exception:
                    pass
                return
        lines = _cart_get_lines_uid(uid_cb, ud)
        if not lines:
            await _answer_order_callback_stale(q)
            return
        cc = str(USER_PREF_DELIVERY_COUNTRY.get(uid_cb) or "by").strip().lower()
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
        site_gt = _cart_get_site_grand_total(uid_cb, pay_cur)
        if site_gt and not _site_grand_covers_goods(int(site_gt), int(goods_total)):
            site_gt = None
        total_from_text: Optional[int] = None
        total_text = _extract_total_from_order_text(order_text)
        if total_text:
            m_total = re.search(r"([0-9]+(?:[.,][0-9]+)?)", total_text)
            if m_total:
                try:
                    total_from_text = int(float(m_total.group(1).replace(",", ".")))
                except (TypeError, ValueError):
                    total_from_text = None
        # Для заказа «с сайта» итог считаем уже финальным (без повторного +доставки).
        if total_from_text and total_from_text > 0:
            total = int(total_from_text)
        elif site_gt and site_gt > 0:
            total = int(site_gt)
        else:
            total = int(goods_total)
        lo_hint = None
        pe = _cart_get_site_loyalty_pending_earn(uid_cb)
        if pe is not None and int(pe) > 0:
            lo_hint = {"bonusWillEarn": int(pe)}
        oid = await _notify_admin_new_order(
            context, u, list(lines), int(total), deepcopy(drec), loyalty_hint_dict=lo_hint
        )
        if oid is None:
            await _notify_callback_issue(q, context)
            return
        USER_ORDERS.setdefault(uid_cb, []).append(
            {
                "id": str(oid),
                "items": deepcopy(list(lines)),
                "total": int(total),
                "total_goods": int(goods_total),
                "delivery": deepcopy(drec),
                "status": "В обработке",
            }
        )
        ORDERS[int(oid)]["clear_cart_on_paid"] = True
        ORDERS[int(oid)]["total_goods"] = int(goods_total)
        ud["awaiting_payment_order_id"] = int(oid)
        ud.pop("payment_pending_method", None)
    ud.pop("pending_order", None)
    if uid_cb:
        SITE_LOGIN_PENDING_ORDER.pop(uid_cb, None)
    await q.answer()
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.message.reply_text(ORDER_AUTO_ACK)
    if uid_cb:
        lo_est = (ORDERS.get(int(oid)) or {}).get("loyalty_earn_estimate")
        await q.message.reply_text(
            _payment_intro_text(
                int(total), pay_cur, loyalty_earn_estimate=lo_est
            ),
            reply_markup=_kb_payment_methods_with_back(),
        )
        users_touch(uid_cb, "payment")


async def on_deep_link_cancel_order(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if (q.data or "").strip() != "cancel_order":
        return
    ud = context.user_data
    u = q.from_user
    uid_cb = int(u.id) if u else 0
    has_ud = bool((ud.get("pending_order") or "").strip())
    has_site = bool(uid_cb and (SITE_LOGIN_PENDING_ORDER.get(uid_cb) or "").strip())
    if not has_ud and not has_site:
        await _answer_order_callback_stale(q)
        return
    ud.pop("pending_order", None)
    if uid_cb:
        SITE_LOGIN_PENDING_ORDER.pop(uid_cb, None)
    await q.answer()
    try:
        await q.message.edit_text(MSG_ORDER_PREVIEW_CANCELLED, reply_markup=None)
    except Exception:
        await q.message.reply_text(MSG_ORDER_PREVIEW_CANCELLED)


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
    if order_rec.get("external_id"):
        ORDERS[int(oid)]["external_id"] = str(order_rec["external_id"])
    ud["awaiting_payment_order_id"] = int(oid)
    ud.pop("payment_pending_method", None)
    await q.answer()
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    tot = int(order_rec["total"])
    lo_est_dl = (ORDERS.get(int(oid)) or {}).get("loyalty_earn_estimate")
    await q.message.reply_text(
        _payment_intro_text(tot, pay_cur_dl, loyalty_earn_estimate=lo_est_dl),
        reply_markup=_kb_payment_methods_with_back(),
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
        tot = _order_resolved_grand_total(o)
        cur_l = _order_line_currency_from_delivery(
            o.get("delivery") if isinstance(o.get("delivery"), dict) else None
        )
        uid = int(o.get("user_id") or 0)
        label = f"#{oid} · {_user_display_name(uid, o.get('username'))} · {tot} {cur_l}"
        if len(label) > 60:
            label = label[:57] + "…"
        rows.append([InlineKeyboardButton(label, callback_data=f"open_order_{int(oid)}")])
        if uid:
            rows.append(
                [
                    InlineKeyboardButton(
                        f"💬 {_user_display_name(uid, o.get('username'))}",
                        callback_data=f"adm_user_msgs_{uid}",
                    )
                ]
            )
    return InlineKeyboardMarkup(rows)


def _format_admin_stats() -> str:
    """Сводка по ORDERS в памяти процесса (дата «сегодня» — локальное время сервера)."""
    today_local = datetime.now().date()
    today_count = 0
    revenue_byn = 0
    revenue_rub = 0
    by_status = {"new": 0, "accepted": 0, "shipped": 0, "done": 0, "canceled": 0}
    for o in ORDERS.values():
        raw_st = str(o.get("status") or "new").strip().lower()
        st = raw_st
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
        tot = _order_resolved_grand_total(o)
        if raw_st not in ("canceled", "cancelled"):
            d_st = o.get("delivery") if isinstance(o.get("delivery"), dict) else {}
            if str(d_st.get("country") or "").strip().lower() == "by":
                revenue_byn += tot
            else:
                revenue_rub += tot
    n_all = len(ORDERS)
    lines = [
        "📈 Статистика",
        "",
        f"📅 Сегодня заказов: {today_count}",
        f"📦 Всего заказов: {n_all}",
        f"💰 Выручка: {revenue_byn} BYN + {revenue_rub} RUB",
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
            body_lines: List[str] = ["📦 Заказы", "", "Выберите заказ или username 👇", ""]
            for oid in sorted(ORDERS.keys(), key=int):
                o = ORDERS[oid]
                tot = _order_resolved_grand_total(o)
                cur_o = _order_line_currency_from_delivery(
                    o.get("delivery") if isinstance(o.get("delivery"), dict) else None
                )
                uid = int(o.get("user_id") or 0)
                uname = _user_display_name(uid, o.get("username"))
                st_ru = _order_status_label_ru(_norm_bot_order_status(str(o.get("status") or "new")))
                body_lines.append(f"#{oid} — {uname} — {tot} {cur_o} — {st_ru}")
            text = "\n".join(body_lines)
            if len(text) > 3500:
                text = text[:3490] + "…"
            await q.message.reply_text(
                text,
                reply_markup=_kb_admin_orders_list(),
            )
    else:
        await q.message.reply_text(_format_admin_stats())


async def on_admin_user_messages(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^adm_user_msgs_(\d+)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        return
    try:
        uid = int(m.group(1))
    except ValueError:
        await q.answer()
        return
    await q.answer()
    await q.message.reply_text(
        _format_user_messages_for_admin(uid),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("💬 Ответить", callback_data=f"sup:rep:{uid}")]]
        ),
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
    await q.message.reply_text(MSG_REPLY_MODE_ACTIVE)


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
        if not o or int(o.get("user_id") or 0) != int(uid):
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
    if action == "delmsg":
        await _admin_delete_order_chat_message(context, q, oid)
        return
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
    refs = set(_favorites_get_refs_uid(uid, context.user_data))
    if not refs:
        await msg.reply_text(
            "💚 В избранном пока пусто.\n\n"
            "С сайта список подгружается только если сайт отправляет его в бот при синхронизации "
            "(те же endpoints, что и для корзины: favorites / state / вход по коду). "
            "Обновите страницу после привязки Telegram.\n\n"
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
            reply_markup=_kb_payment_methods_with_back(),
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


async def on_bonus_spend_pick(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """bo:s:0|h|m — списание бонусов к оплате в превью заказа."""
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^bo:s:(0|h|m)$", (q.data or "").strip())
    if not m:
        return
    mode = m.group(1)
    uid = q.from_user.id
    ud = context.user_data
    fin = _checkout_preview_finance(ud, uid)
    if not fin:
        try:
            await q.answer("Сначала выберите страну доставки.", show_alert=True)
        except Exception:
            pass
        return
    cap = _checkout_bonus_cap(uid, int(fin["grand_show"]))
    if mode == "0":
        ud["checkout_bonus_spend"] = 0
    elif mode == "h":
        ud["checkout_bonus_spend"] = max(0, cap // 2)
    else:
        ud["checkout_bonus_spend"] = int(cap)
    preview = _format_order_preview_with_delivery(ud, uid)
    if not preview:
        try:
            await q.answer()
        except Exception:
            pass
        return
    try:
        await q.answer()
    except Exception:
        pass
    try:
        await q.message.edit_text(
            preview,
            reply_markup=_kb_order_preview_actions(uid, ud),
            disable_web_page_preview=True,
        )
    except Exception:
        try:
            await q.message.reply_text(
                preview,
                reply_markup=_kb_order_preview_actions(uid, ud),
            )
        except Exception:
            pass


async def on_send_order_to_admin(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """ta:0 — подтверждение превью: заказ в ORDERS без уведомления админу; дальше оплата, админ — после чека."""
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
        if po is None or int(po.get("user_id") or 0) != int(uid_chk) or po.get("paid"):
            _user_state_pop(uid_chk, "awaiting_proof")
        elif po is not None and not po.get("paid"):
            try:
                await q.answer(MSG_PAY_NEED_PROOF_FIRST, show_alert=True)
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
        if po is None or int(po.get("user_id") or 0) != int(uid_chk) or po.get("paid"):
            ud.pop("awaiting_payment_order_id", None)
            ud.pop("payment_pending_method", None)
        elif po is not None and not po.get("paid"):
            try:
                await q.answer(MSG_PAY_FINISH_CURRENT, show_alert=True)
            except Exception:
                pass
            return
    lines: Optional[List[dict]] = ud.get("order_checkout")
    if not lines:
        # Восстанавливаемся после устаревшей сессии: берём актуальные позиции из корзины.
        fallback_lines = _cart_get_lines_uid(uid_chk, context.user_data) if uid_chk else []
        if not fallback_lines:
            await q.answer()
            await q.message.reply_text(
                "Корзина пустая или шаг оформления устарел. Добавьте карточки и попробуйте снова.",
                reply_markup=_kb_cart([]),
            )
            return
        lines = deepcopy(fallback_lines)
        ud["order_checkout"] = deepcopy(lines)
    if not ud.get("delivery_country"):
        await q.answer()
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
    fin_bo = _checkout_preview_finance(ud, uid)
    gref = int(fin_bo["grand_show"]) if fin_bo else int(base_tot)
    spend_raw = _checkout_bonus_spend_effective(ud, uid, gref)
    spend = min(int(spend_raw), max(0, int(base_tot)))
    pay_total = max(0, int(base_tot) - int(spend))
    order_rec = {
        "id": "0",
        "items": deepcopy(list(lines)),
        "total": int(pay_total),
        "total_goods": int(goods_total),
        "delivery": drec,
        "status": "В обработке",
        "bonus_applied": int(spend),
    }
    lo_hint_ta = None
    pe_ta = _cart_get_site_loyalty_pending_earn(uid)
    if pe_ta is not None and int(pe_ta) > 0:
        lo_hint_ta = {"bonusWillEarn": int(pe_ta)}
    oid = await _notify_admin_new_order(
        context, u, list(lines), int(pay_total), deepcopy(drec), loyalty_hint_dict=lo_hint_ta
    )
    if oid is None:
        await _notify_callback_issue(q, context)
        return
    order_rec["id"] = str(oid)
    USER_ORDERS.setdefault(uid, []).append(order_rec)
    ORDERS[int(oid)]["clear_cart_on_paid"] = True
    ORDERS[int(oid)]["total_goods"] = int(goods_total)
    if int(spend) > 0:
        ORDERS[int(oid)]["bonus_applied"] = int(spend)
    ud["awaiting_payment_order_id"] = int(oid)
    ud.pop("payment_pending_method", None)
    await q.answer()
    tot = int(order_rec["total"])
    lo_est_ta = (ORDERS.get(int(oid)) or {}).get("loyalty_earn_estimate")
    await q.message.reply_text(
        _payment_intro_text(tot, pay_cur, loyalty_earn_estimate=lo_est_ta),
        reply_markup=_kb_payment_methods_with_back(),
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
    pr_same = _user_state_get(uid, "awaiting_proof")
    if pr_same is not None and int(pr_same) == int(oid):
        _user_state_pop(uid, "awaiting_proof")
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
    total_label = _payment_total_label(o)
    await q.message.reply_text(
        body_map[method],
        reply_markup=_kb_paid_confirm_with_back(total_label),
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
        await q.message.reply_text(
            PAY_PROOF_REQUEST, reply_markup=_kb_proof_step_back()
        )
        return
    pm = ud.pop("payment_pending_method", None)
    if pm:
        o["payment_pending_method"] = pm
    _clear_crypto_auto_watch(o, uid)
    _user_state_set(uid, "awaiting_proof", int(oid))
    _clear_checkout_delivery(ud)
    _cart_clear_site_pricing_hints(uid)
    _clear_user_cart_after_payment_proof(uid, int(oid), o)
    try:
        asyncio.get_running_loop().create_task(
            _notify_site_cart_cleared_after_proof(uid, int(oid), o)
        )
    except RuntimeError:
        pass
    try:
        await q.answer()
    except Exception:
        pass
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.message.reply_text(
        PAY_PROOF_REQUEST, reply_markup=_kb_proof_step_back()
    )


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
    except Exception:
        log.exception("скрин оплаты → ORDER_NOTIFY_TARGET order_id=%s", order_id)
        try:
            sent = await context.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=file_id,
                caption=caption,
                reply_markup=kb,
            )
        except Exception:
            log.exception("скрин оплаты → ADMIN_ID order_id=%s", order_id)
            return False
    o["payment_proof_admin_chat_id"] = int(sent.chat_id)
    o["payment_proof_admin_message_id"] = int(sent.message_id)
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


async def on_user_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Фото: скрин оплаты (awaiting_proof), данные к заказу после оплаты (postpaid_thread), иначе «Связь»."""
    msg = update.effective_message
    if not msg or not msg.photo:
        return
    uid = msg.from_user.id if msg.from_user else 0
    if uid and _user_state_get(uid, "awaiting_proof") is not None:
        await on_payment_proof_photo(update, context)
        return
    pp_oid = _user_state_get(uid, "postpaid_thread_oid")
    if uid and pp_oid is not None and not is_admin(uid):
        cap = (msg.caption or "").strip() if msg.caption else ""
        fid = msg.photo[-1].file_id if msg.photo else None
        if not fid:
            return
        ok = await _forward_postpaid_client_payload_to_admin(
            context,
            uid=uid,
            oid=int(pp_oid),
            body_text=cap or None,
            msg=msg,
            photo_file_id=fid,
        )
        if ok:
            try:
                await msg.reply_text(MSG_POSTPAID_FORWARDED_OK)
            except Exception:
                pass
        else:
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
        await msg.reply_text(MSG_ORDER_ALREADY_PAID_SKIP_PROOF)
        return
    _clear_crypto_auto_watch(o, uid)
    file_id = msg.photo[-1].file_id
    cap = _format_payment_proof_caption(oid, o, uid)
    ok = await _send_or_edit_admin_payment_proof(context, oid, o, file_id, cap)
    if not ok:
        await msg.reply_text(MSG_PAY_PROOF_TO_ADMIN_FAIL)
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
    try:
        b_sp = int(o.get("bonus_applied") or 0)
    except (TypeError, ValueError):
        b_sp = 0
    cust = int(o.get("user_id") or 0)
    if b_sp > 0 and cust:
        _loyalty_apply_local_debit(cust, b_sp)
    site_order_id = await _ensure_site_order_for_bot_order(oid, o)
    if not site_order_id and cust:
        try:
            earn_est = int(o.get("loyalty_earn_estimate") or 0)
        except (TypeError, ValueError):
            earn_est = 0
        if earn_est > 0:
            _loyalty_apply_local_credit(cust, earn_est)
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
    # Сначала карточка заказа у админа — чтобы ответы клиента шли reply в один тред.
    await _send_deferred_admin_order_panel(context, oid, o)
    if cust:
        await _send_payment_receipt(context.bot, cust, oid, o)
        await _send_customer_plain(
            context.bot, cust, _postpaid_shipping_prompt_for_order(o)
        )
        _user_state_set(cust, "postpaid_thread_oid", int(oid))


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
            caption=prev + MSG_PAYMENT_CAPTION_REJECTED,
            reply_markup=None,
        )
    except Exception:
        pass


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
                "Не удалось показать корзину. Попробуйте кнопку «💚 Корзина» внизу или «Связь».",
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
            ok = await _forward_postpaid_client_payload_to_admin(
                context,
                uid=uid,
                oid=int(thread_oid),
                body_text=text,
                msg=msg,
            )
            if ok:
                try:
                    await msg.reply_text(MSG_POSTPAID_FORWARDED_OK)
                except Exception:
                    pass
            else:
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
            _ensure_user_cart(uid, user_data)
            cl = _cart_get_lines_uid(uid, user_data)
            products = list(context.application.bot_data.get("products") or [])
            if not products:
                products = await load_products() or []
                if products:
                    context.application.bot_data["products"] = products
            if products:
                _reprice_lines_for_delivery(
                    cl, products, _cart_price_region_for_user(uid, user_data)
                )
                _cart_set_items_uid(uid, cl)
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

    if text == BTN_FAVORITES:
        await send_favorites_deck(update, context)
        return

    if text == BTN_RANDOM_CARD:
        await send_random_card(update, context)
        return

    if text == BTN_BONUSES:
        bal = _loyalty_balance_int(uid)
        bal_line = (
            f"Текущий баланс: {bal} бонусов.\n\n"
            if bal > 0
            else "Баланс пока не получен с сайта (0) — войдите на illucards.by через код в Telegram.\n\n"
        )
        await msg.reply_text(
            (bal_line + MSG_LOYALTY_MENU).strip()[:4090],
            reply_markup=REPLY_KB,
            disable_web_page_preview=True,
        )
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
    application.bot_data["deploy_epoch"] = time.time_ns()
    n_sl = len(SITE_LOGIN_PENDING_ORDER)
    n_lc = len(LOGIN_CODES)
    SITE_LOGIN_PENDING_ORDER.clear()
    LOGIN_CODES.clear()
    log.info(
        "Рестарт процесса: deploy_epoch=%s; сброшены SITE_LOGIN_PENDING_ORDER=%d, LOGIN_CODES=%d",
        application.bot_data["deploy_epoch"],
        n_sl,
        n_lc,
    )
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
    _ensure_flask_health_server_thread()

    app = (
        Application.builder()
        .token(token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.add_error_handler(on_ptb_error)

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
        CallbackQueryHandler(on_deep_link_confirm_order, pattern=re.compile(r"^confirm_order$"))
    )
    app.add_handler(
        CallbackQueryHandler(on_deep_link_cancel_order, pattern=re.compile(r"^cancel_order$"))
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
    app.add_handler(
        CallbackQueryHandler(
            on_bonus_spend_pick, pattern=re.compile(r"^bo:s:(0|h|m)$")
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
    app.add_handler(MessageHandler(filters.PHOTO, on_user_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # Краткие сбои Telegram / наложение деплоев: повторить bootstrap (delete_webhook и т.д.).
    poll_bootstrap_retries = _env_int("TELEGRAM_POLLING_BOOTSTRAP_RETRIES", 35)
    app.run_polling(allowed_updates=Update.ALL_TYPES, bootstrap_retries=poll_bootstrap_retries)


if __name__ == "__main__":
    main()
