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
import re
import secrets
import sys
import time
import urllib.parse
from copy import deepcopy
from typing import List, Optional, Tuple

from dotenv import load_dotenv

import aiohttp
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

# Сообщения о заказе из t.me/bot?start=... (числовой Telegram id админа)
try:
    ADMIN_ID: int = int((os.getenv("ADMIN_ID") or "0").strip() or "0")
except ValueError:
    ADMIN_ID = 0


def is_admin(user_id: int) -> bool:
    return int(user_id) == int(ADMIN_ID)


# Одноразовые коды для входа на сайт: код (строка) -> {user_id, expires (unix time)}
LOGIN_CODES: dict = {}
LOGIN_CODE_TTL_SEC = 5 * 60

# Корзина и оформленные заказы в памяти процесса (ключ — Telegram user_id)
USER_CART: dict = {}
USER_ORDERS: dict = {}
# /start order_<id> — черновики заказов по ссылке (до оформления). Пополняется API/сайтом или register_shared_deep_link_order.
SHARED_DEEP_LINK_ORDERS: dict = {}

FALLBACK_USER_TEXT = "Что-то пошло не так, открой каталог"

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
                    q.message.chat_id, FALLBACK_USER_TEXT
                )
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
        if v.get("user_id") == telegram_id:
            del LOGIN_CODES[k]


def _issue_login_code(telegram_id: int) -> str:
    _cleanup_expired_login_codes()
    _invalidate_user_login_codes(telegram_id)
    for _ in range(50):
        c = f"{secrets.randbelow(9000) + 1000:04d}"
        if c not in LOGIN_CODES:
            LOGIN_CODES[c] = {
                "user_id": telegram_id,
                "expires": time.time() + LOGIN_CODE_TTL_SEC,
            }
            return c
    c = f"{int(time.time() * 1000) % 10000:04d}"
    LOGIN_CODES[c] = {
        "user_id": telegram_id,
        "expires": time.time() + LOGIN_CODE_TTL_SEC,
    }
    return c


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
        [KeyboardButton("📦 Каталог"), KeyboardButton("🛒 Корзина")],
        [KeyboardButton("📦 Ваши заказы")],
        [KeyboardButton("🔥 Смотреть карточки")],
        [KeyboardButton("🔥 Акции"), KeyboardButton("💬 Связь")],
    ],
    resize_keyboard=True,
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
        return "🛒 Твоя корзина:\n\nПока пусто.\n\nНажми 📦 Каталог — выбери карточки и «🛒 В корзину»."
    total, _ = _cart_totals(lines)
    out: List[str] = [
        "🛒 Твоя корзина:",
        "",
    ]
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        out.append(f"• {name} — {p} BYN × {q}")
    out += ["", f"💰 Итого: {total} BYN"]
    s = "\n".join(out)
    if len(s) > 3900:
        s = s[:3890] + "…"
    return s


def _format_user_orders_message(user_id: int) -> str:
    orders = list(USER_ORDERS.get(user_id) or [])
    if not orders:
        return "📦 Ваши заказы:\n\nПока нет оформленных заказов."
    lines: List[str] = ["📦 Ваши заказы:", ""]
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


def _kb_cart(lines: List[dict]) -> Optional[InlineKeyboardMarkup]:
    if not lines:
        return None
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
    return InlineKeyboardMarkup(rows)


def _format_checkout_preview_for_user(lines: List[dict]) -> str:
    total, _ = _cart_totals(lines)
    out: List[str] = ["📦 Твой заказ:", ""]
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        out.append(f"• {name} — {p} BYN × {q}")
    out += ["", f"💰 Итого: {total} BYN"]
    return "\n".join(out)


def _kb_delivery_country() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇧🇾 Беларусь", callback_data="dl:by"),
                InlineKeyboardButton("🇷🇺 Россия", callback_data="dl:ru"),
            ],
            [
                InlineKeyboardButton("🇺🇦 Украина", callback_data="dl:ua"),
                InlineKeyboardButton("🌍 Другие страны", callback_data="dl:ot"),
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
    out: List[str] = ["📦 Твой заказ:", ""]
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        out.append(f"• {name} — {sub}")
    out.append("")
    out.append(f"🚚 Доставка: {dlabel} — {damount} {dcur}")
    if code == "by" and dcur == "BYN":
        out.append(f"💰 Итого: {goods_total + damount} BYN")
    else:
        out.append(f"💰 Товары: {goods_total} BYN")
        out.append(f"📌 Доставка: {damount} {dcur}")
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


async def _send_new_order_to_admin(
    context: ContextTypes.DEFAULT_TYPE,
    user,
    lines: List[dict],
    delivery_label: Optional[str] = None,
    delivery_amount: Optional[int] = None,
    delivery_currency: Optional[str] = None,
    delivery_country: Optional[str] = None,
) -> bool:
    """Заказ в Telegram (по умолчанию @Daniel_official)."""
    chat_id = ORDER_NOTIFY_TARGET
    goods_total, _ = _cart_totals(lines)
    body: List[str] = ["🔥 НОВЫЙ ЗАКАЗ", ""]
    for x in lines:
        n = (x.get("name") or "—")[:200]
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        body.append(f"• {n} — {sub} BYN")
    body.append("")
    if delivery_label and delivery_amount is not None and delivery_currency:
        body.append(f"💰 Товары: {goods_total} BYN")
        body.append(
            f"🚚 Доставка: {delivery_label} — {delivery_amount} {delivery_currency}"
        )
        if delivery_country == "by" and delivery_currency == "BYN":
            body.append(f"💰 Итого: {goods_total + int(delivery_amount)} BYN")
        else:
            body.append(
                f"📌 Товары {goods_total} BYN + доставка {delivery_amount} {delivery_currency}"
            )
    else:
        body.append(f"💰 Итого: {goods_total} BYN")
    body.append("")
    u = user
    uline = f"@{u.username}" if u and u.username else "— (нет @username)"
    body.append(f"👤 {uline} · id {u.id if u else '—'}")
    text = "\n".join(body)
    if len(text) > 4090:
        text = text[:4086] + "…"
    log = logging.getLogger(__name__)
    try:
        await context.bot.send_message(
            chat_id=chat_id, text=text, disable_web_page_preview=True
        )
    except Exception as e:
        log.exception("Ошибка отправки заказа админу: %s", e)
        return False
    return True


def _category_names(products: List[dict]) -> List[str]:
    s = {str(p.get("category", "Без категории") or "Без категории") for p in products}
    return sorted(s, key=str.lower)


def _btn_label(s: str, max_len: int = 22) -> str:
    t = s.strip() or "—"
    return t if len(t) <= max_len else t[: max_len - 1] + "…"


def _kb_categories(categories: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🔥 Все категории", callback_data="c:all")],
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
            [InlineKeyboardButton("⭐ Все редкости", callback_data=f"j:{c}:all")],
            [InlineKeyboardButton("🔥 Горячая цена", callback_data=f"j:{c}:sale")],
            [
                InlineKeyboardButton("Обычная", callback_data=f"j:{c}:0"),
                InlineKeyboardButton("Лимитированная", callback_data=f"j:{c}:1"),
            ],
            [
                InlineKeyboardButton("Новинка", callback_data=f"j:{c}:2"),
                InlineKeyboardButton("Реплика", callback_data=f"j:{c}:3"),
            ],
            [InlineKeyboardButton("18+", callback_data=f"j:{c}:4")],
            [InlineKeyboardButton("⬅ К категориям", callback_data="m:0")],
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
    c = f"{name}\n\n💰 {pstr}\n⭐ {r}\n\n{cur_1} / {n_total}"
    if len(c) > 1024:
        c = c[:1020] + "…"
    return c


def _tinder_keyboard(cat_tok: str, user_data: dict) -> InlineKeyboardMarkup:
    c = str(cat_tok)[:12]
    if user_data.get("tinder_autoplay_paused", False):
        auto_btn = InlineKeyboardButton("▶️ Продолжить", callback_data="t:f")
    else:
        auto_btn = InlineKeyboardButton("⏸ Пауза", callback_data="t:f")
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬅️ Назад", callback_data="t:p"),
                InlineKeyboardButton("❤️ В корзину", callback_data="t:c"),
                InlineKeyboardButton("➡️ Далее", callback_data="t:n"),
            ],
            [auto_btn, InlineKeyboardButton("⬅️ К редкости", callback_data=f"h:{c}")],
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
        short = f"{len(lines)} п. · {tot} BYN"
        await q.answer(f"В корзине: {short}", show_alert=False)
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
        return "📦 Твой заказ:\n\n—"
    lines = ["📦 Твой заказ:", "", ot]
    tot = _extract_total_from_order_text(ot)
    if tot:
        lines.extend(["", f"💰 Итого: {tot}"])
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
    out: List[str] = ["📦 Ваш заказ:", ""]
    for x in lines:
        name = (x.get("name") or "—")[:200]
        if len((x.get("name") or "")) > 200:
            name = name.rstrip() + "…"
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        sub = p * q
        out.append(f"• {name} — {sub}")
    out.append("")
    out.append(f"🚚 Доставка: {label} — {amount} {cur}")
    if country == "by" and cur == "BYN":
        out.append(f"💰 Итого: {goods_total + amount} BYN")
    else:
        out.append(f"💰 Товары: {goods_total} BYN")
        out.append(f"📌 Доставка: {amount} {cur}")
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
    print(update.effective_user.id)
    uid = msg.from_user.id
    args = context.args or []
    if args:
        first = (args[0] or "").strip()
        oid = _parse_order_id_from_start_arg(first)
        if oid:
            order = await _fetch_order_for_deep_link(oid)
            if not order:
                await msg.reply_text(
                    "Заказ не найден или ссылка недействительна. Откройте каталог.",
                    reply_markup=REPLY_KB,
                )
            else:
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
                                    "✅ Оформить заказ",
                                    callback_data=f"dlco:{tok}",
                                ),
                                InlineKeyboardButton(
                                    "❌ Отменить",
                                    callback_data=f"dlca:{tok}",
                                ),
                            ],
                        ],
                    ),
                )
        else:
            t = " ".join(args).strip()
            if not t:
                await msg.reply_text("Привет!", reply_markup=REPLY_KB)
            else:
                context.user_data.pop("deep_link_order_session", None)
                context.user_data["pending_order"] = t
                preview = _format_deep_link_order_preview(t)
                await msg.reply_text(
                    preview,
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    "✅ Подтвердить заказ",
                                    callback_data="confirm_order",
                                ),
                                InlineKeyboardButton(
                                    "❌ Отменить", callback_data="cancel_order"
                                ),
                            ],
                        ],
                    ),
                )
    else:
        await msg.reply_text("Привет!", reply_markup=REPLY_KB)
    code = _issue_login_code(uid)
    await msg.reply_text(f"Твой код для входа на сайт: {code}")


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
    admin_body = "\n".join(
        [
            "🔥 НОВЫЙ ЗАКАЗ",
            "",
            order_text,
            "",
            f"👤 {uname} (ID: {uid})",
        ]
    )
    if len(admin_body) > 4090:
        admin_body = admin_body[:4086] + "…"
    log = logging.getLogger(__name__)
    try:
        await context.bot.send_message(
            chat_id=ORDER_NOTIFY_TARGET,
            text=admin_body,
            disable_web_page_preview=True,
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
    await q.message.reply_text("✅ Заказ отправлен!")


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
        await q.message.edit_text("❌ Заказ отменён", reply_markup=None)
    except Exception:
        await q.message.reply_text("❌ Заказ отменён")


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
    ok = await _send_new_order_to_admin(
        context,
        u,
        list(lines),
        delivery_label=d_label,
        delivery_amount=d_amt,
        delivery_currency=d_cur,
        delivery_country=d_cc or None,
    )
    if not ok:
        await _notify_callback_issue(q, context)
        return
    ud.pop("deep_link_order_session", None)
    goods_total, _ = _cart_totals(list(lines))
    drec = {
        "country": d_cc,
        "label": d_label,
        "amount": d_amt,
        "currency": d_cur,
    }
    order_rec = {
        "id": str(order.get("external_id") or secrets.token_urlsafe(10)),
        "items": deepcopy(list(lines)),
        "total": int(goods_total),
        "total_goods": int(goods_total),
        "delivery": deepcopy(drec),
        "status": "В обработке",
    }
    if drec.get("country") == "by" and drec.get("currency") == "BYN":
        order_rec["total"] = int(goods_total) + int(drec.get("amount") or 0)
    USER_ORDERS.setdefault(u.id, []).append(order_rec)
    await q.answer()
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.message.reply_text(
        "✅ Заказ оформлен!\n"
        "С вами свяжется администратор 👇\n"
        f"{ORDER_MENTION}"
    )


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
        await q.message.edit_text("❌ Заказ отменён", reply_markup=None)
    except Exception:
        await q.message.reply_text("❌ Заказ отменён")


def _kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📦 Заказы", callback_data="adm:orders")],
            [InlineKeyboardButton("📈 Статистика", callback_data="adm:stats")],
            [InlineKeyboardButton("📩 Написать клиенту", callback_data="adm:msg")],
        ],
    )


def _format_admin_orders_summary() -> str:
    flat: List[Tuple[int, dict]] = []
    for uid, lst in USER_ORDERS.items():
        for rec in lst or []:
            flat.append((int(uid), rec))
    if not flat:
        return "📦 Заказы\n\nПока нет заказов в памяти бота."
    lines: List[str] = ["📦 Заказы:", ""]
    for uid, rec in flat[-25:]:
        oid = str(rec.get("id") or "")[:18]
        st = str(rec.get("status") or "—")
        tg = rec.get("total_goods", rec.get("total", "?"))
        lines.append(f"• <code>{uid}</code> · #{oid} · {st} · {tg} BYN")
    if len(flat) > 25:
        lines.append("")
        lines.append(f"… всего записей: {len(flat)}")
    s = "\n".join(lines)
    if len(s) > 3900:
        s = s[:3890] + "…"
    return s


def _format_admin_stats() -> str:
    n_orders = sum(len(lst or []) for lst in USER_ORDERS.values())
    n_users_orders = sum(1 for lst in USER_ORDERS.values() if lst)
    n_carts_nonempty = 0
    for c in USER_CART.values():
        if not isinstance(c, dict):
            continue
        if list(c.get("items") or []):
            n_carts_nonempty += 1
    n_shared_dl = len(SHARED_DEEP_LINK_ORDERS)
    return (
        "📈 Статистика (в памяти процесса)\n\n"
        f"• Заказов всего: {n_orders}\n"
        f"• Пользователей с заказами: {n_users_orders}\n"
        f"• Непустых корзин: {n_carts_nonempty}\n"
        f"• Черновиков по deep link: {n_shared_dl}\n"
    )


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return
    if not is_admin(u.id):
        await msg.reply_text("Нет доступа")
        return
    await msg.reply_text("📊 Админ-панель", reply_markup=_kb_admin_panel())


async def admin_say_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить текст клиенту по user_id (только админ)."""
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return
    if not is_admin(u.id):
        await msg.reply_text("Нет доступа")
        return
    args = context.args or []
    if len(args) < 2:
        await msg.reply_text(
            "Формат: /say ID_пользователя текст сообщения\n"
            "Пример: /say 123456789 Здравствуйте!"
        )
        return
    try:
        target_id = int(args[0])
    except (TypeError, ValueError):
        await msg.reply_text("Некорректный ID пользователя.")
        return
    text = " ".join(args[1:]).strip()
    if not text:
        await msg.reply_text("Пустой текст.")
        return
    log = logging.getLogger(__name__)
    try:
        await context.bot.send_message(chat_id=target_id, text=text)
    except Exception as e:
        log.exception("admin /say → user %s", target_id)
        await msg.reply_text("Не удалось отправить сообщение. Проверьте ID и то, что пользователь уже писал боту.")
        return
    await msg.reply_text("✅ Сообщение отправлено.")


async def on_admin_panel_action(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data or not q.message:
        return
    m = re.match(r"^adm:(orders|stats|msg)$", (q.data or "").strip())
    if not m:
        return
    if not is_admin(q.from_user.id):
        try:
            await q.answer("Нет доступа")
        except Exception:
            pass
        return
    action = m.group(1)
    await q.answer()
    if action == "orders":
        body = _format_admin_orders_summary()
        await q.message.reply_text(body, parse_mode="HTML")
    elif action == "stats":
        await q.message.reply_text(_format_admin_stats())
    else:
        await q.message.reply_text(
            "📩 Написать клиенту\n\n"
            "Отправьте команду:\n"
            "<code>/say ID_пользователя ваш текст</code>\n\n"
            "Клиент должен был хотя бы раз написать боту, иначе доставка сообщения невозможна.",
            parse_mode="HTML",
        )


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
        await msg.reply_text("Не удалось загрузить товары")
        return
    context.bot_data["products"] = cards
    context.bot_data["illucards_synced_at"] = time.time()

    categories = _category_names(cards)
    if not categories:
        await msg.reply_text("Категории не найдены")
        return
    log.info("Каталог: %d разделов", len(categories))
    await msg.reply_text(
        "🔥 Коллекция",
        reply_markup=_kb_categories(categories),
    )


async def send_tinder_mode(
    update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопка «🔥 Смотреть карточки» — Tinder по всему каталогу (cat_tok all)."""
    msg = update.effective_message
    if not msg:
        return
    products = await _get_products(context)
    if not products:
        await msg.reply_text(
            "Пока нет карточек. Сначала открой «📦 Каталог» или подожди загрузки."
        )
        return
    in_scope = list(products)
    ok = await _tinder_start_deck(
        context, int(msg.chat_id), in_scope, products, "all"
    )
    if not ok:
        await msg.reply_text("Не получилось открыть просмотр. Попробуй ещё раз.")


async def send_promo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_photo(
        photo=PROMO_PHOTO,
        caption="При покупке всей коллекции — подарок 🎁",
    )


async def send_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_text("Напишите: @Daniel_official")


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
        await q.edit_message_text("Каталог пуст. Нажми 📦 Каталог ещё раз.")
        return
    cats = _category_names(products)
    if not cats:
        await q.edit_message_text("Категорий нет")
        return
    await q.edit_message_text(
        "🔥 Коллекция",
        reply_markup=_kb_categories(cats),
    )


async def on_menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or (q.data or "") != "m:0":
        return
    await q.answer()
    await _edit_to_categories(q, context)


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
        t = "🔥 Все разделы\n\n⭐ Выбери редкость:"
        await q.answer()
        await q.edit_message_text(t, reply_markup=_kb_rarities("all"))
        return
    ci = int(key)
    if ci < 0 or ci >= len(cats):
        await q.answer()
        if q.message:
            await q.message.reply_text(FALLBACK_USER_TEXT)
        return
    cat = cats[ci]
    t = f"🔥 {cat}\n\n⭐ Выбери редкость:"
    await q.answer()
    await q.edit_message_text(t, reply_markup=_kb_rarities(str(ci)))


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
            await q.message.reply_text("По этому фильтру пока пусто.")
        return
    if not in_scope:
        await q.answer()
        if q.message:
            await q.message.reply_text("В выборе пока нет карточек.")
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
    if cat_tok == "all":
        t = "🔥 Все разделы\n\n⭐ Выбери редкость:"
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
        t = f"🔥 {cat}\n\n⭐ Выбери редкость:"
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
    kb = _kb_cart(lines) if lines else None
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
    await q.answer(f"В корзине: {nlines} п. · {short}", show_alert=False)


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
    await q.answer()
    await _edit_cart_message(q, context)


async def on_cart_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if re.match(r"^cz:0$", (q.data or "").strip()) is None:
        return
    uid = q.from_user.id if q.from_user else 0
    if uid:
        _cart_clear_uid(uid)
    await q.answer("Корзина пуста", show_alert=False)
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
            await q.message.reply_text("Корзина пуста. Добавь карточки из каталога.")
        return
    ud = context.user_data
    ud["order_checkout"] = deepcopy(lines)
    ud.pop("delivery_country", None)
    ud.pop("delivery_label", None)
    ud.pop("delivery_amount", None)
    ud.pop("delivery_currency", None)
    await q.answer()
    await q.message.reply_text(
        "Выберите страну доставки:",
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
            [[InlineKeyboardButton("✅ Подтвердить заказ", callback_data="ta:0")]],
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
    ok = await _send_new_order_to_admin(
        context,
        u,
        list(lines),
        delivery_label=ud.get("delivery_label"),
        delivery_amount=ud.get("delivery_amount"),
        delivery_currency=ud.get("delivery_currency"),
        delivery_country=str(ud.get("delivery_country") or ""),
    )
    if not ok:
        await _notify_callback_issue(q, context)
        return
    drec = {
        "country": ud.get("delivery_country"),
        "label": ud.get("delivery_label"),
        "amount": ud.get("delivery_amount"),
        "currency": ud.get("delivery_currency"),
    }
    goods_total, _ = _cart_totals(list(lines))
    order_rec = {
        "id": secrets.token_urlsafe(10),
        "items": deepcopy(list(lines)),
        "total": int(goods_total),
        "total_goods": int(goods_total),
        "delivery": drec,
        "status": "В обработке",
    }
    if drec.get("country") == "by" and drec.get("currency") == "BYN":
        order_rec["total"] = int(goods_total) + int(drec.get("amount") or 0)
    USER_ORDERS.setdefault(uid, []).append(order_rec)
    if uid:
        _cart_clear_uid(uid)
    _clear_checkout_delivery(ud)
    await q.answer("Готово")
    await q.message.reply_text("✅ Заказ отправлен!")


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
    kb = _kb_cart(lines) if lines else None
    await q.message.reply_text(t, reply_markup=kb)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.text:
        return
    text = msg.text.strip()
    user_data = context.user_data

    if text in ("📦 Каталог", "🔥 Смотреть карточки", "🔥 Акции", "💬 Связь"):
        _clear_checkout_delivery(user_data)
        user_data.pop("pending_order", None)

    uid = msg.from_user.id if msg.from_user else 0

    if text == "🛒 Корзина":
        _ensure_user_cart(uid, user_data)
        cl = _cart_get_lines_uid(uid, user_data)
        t = _format_cart_message(cl)
        kb = _kb_cart(cl) if cl else None
        await msg.reply_text(t, reply_markup=kb)
        return

    if text == "📦 Ваши заказы":
        if uid:
            await msg.reply_text(_format_user_orders_message(uid))
        else:
            await msg.reply_text(FALLBACK_USER_TEXT)
        return

    if text == "📦 Каталог":
        await send_catalog(update, context)
    elif text == "🔥 Смотреть карточки":
        await send_tinder_mode(update, context)
    elif text == "🔥 Акции":
        await send_promo(update, context)
    elif text == "💬 Связь":
        await send_contact(update, context)


async def post_init(application: Application) -> None:
    log = logging.getLogger(__name__)
    log.info(
        "Уведомления о заказах: target=%s, mention=%s",
        ORDER_NOTIFY_TARGET,
        ORDER_MENTION,
    )
    if not ADMIN_ID:
        log.warning("ADMIN_ID=0 — legacy /start?start= через ADMIN_ID не используется")
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
    print("Бот запущен!")
    me = await application.bot.get_me()
    if me.username:
        print(f"https://t.me/{me.username}")


def main() -> None:
    if not token:
        sys.exit("TELEGRAM_BOT_TOKEN is not set")

    app = Application.builder().token(token).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("catalog", catalog_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("say", admin_say_cmd))
    app.add_handler(
        CallbackQueryHandler(
            on_admin_panel_action,
            pattern=re.compile(r"^adm:(orders|stats|msg)$"),
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
    app.add_handler(CallbackQueryHandler(on_send_order_to_admin, pattern=re.compile(r"^ta:0$")))
    app.add_handler(CallbackQueryHandler(on_view_cart_callback, pattern=re.compile(r"^vc:0$")))
    app.add_handler(CallbackQueryHandler(on_cart_increment, pattern=re.compile(r"^ic:(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_cart_decrement, pattern=re.compile(r"^dc:(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_cart_remove_line, pattern=re.compile(r"^rm:(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_cart_clear, pattern=re.compile(r"^cz:0$")))
    app.add_handler(CallbackQueryHandler(on_tinder_swipe, pattern=re.compile(r"^t:([pncf])$")))
    app.add_handler(CallbackQueryHandler(on_add_to_cart, pattern=re.compile(r"^a:(.+)$")))
    app.add_handler(CallbackQueryHandler(on_back_rarity, pattern=re.compile(r"^h:([^:]{1,12})$")))
    app.add_handler(CallbackQueryHandler(on_menu_main, pattern=re.compile(r"^m:0$")))
    app.add_handler(
        CallbackQueryHandler(
            on_pick_rarity, pattern=re.compile(r"^j:([^:]{1,12}):(all|sale|\d+)$")
        )
    )
    app.add_handler(CallbackQueryHandler(on_pick_category, pattern=re.compile(r"^c:(\d+|all)$")))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
