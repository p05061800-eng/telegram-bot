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
import sys
import time
from copy import deepcopy
from typing import List, Optional, Tuple

from dotenv import load_dotenv

import aiohttp
from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
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
# Куда бот пишет о новых заказах (ваш user id, id группы с ботом или @username через getChat).
# Можно узнать у @userinfobot, у группы: добавить бота и /chatid (или похожие). Должен быть int.
def _read_notify_chat() -> Optional[int]:
    s = (os.getenv("TELEGRAM_ORDER_NOTIFY_ID") or os.getenv("ORDER_NOTIFY_CHAT_ID") or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


ORDER_NOTIFY_CHAT_ID: Optional[int] = _read_notify_chat()
ORDER_MENTION = (os.getenv("ORDER_MENTION", "@Daniel_official") or "@Daniel_official").strip()

# Уведомления о заказе из deep link t.me/bot?start=... (числовой user id в Telegram)
try:
    ADMIN_ID: int = int((os.getenv("ADMIN_ID") or "0").strip() or "0")
except ValueError:
    ADMIN_ID = 0

# callback для «Оформить» после /start <текст заказа> (лимит 64 байт)
DEEPLINK_ORDER_CB = "dl:ok"

CARDS_JSON_URL = os.getenv("CARDS_JSON_URL", "https://www.illucards.by/cards.json")

PROMO_PHOTO = "https://picsum.photos/seed/promo/400/300"
ILLUCARDS_BASE = "https://www.illucards.by"
SYNC_EVERY_SEC = int(os.getenv("ILLUCARDS_SYNC_EVERY_SEC", "900"))
# Шаг 3: столько пунктов (№ + название) в одном сообщении (пагинация при большом списке)
LIST_ITEMS_PER_PAGE = 32

# Редкости из API часто на английском — в интерфейсе показываем по-русски
RARITY_RU: dict = {
    "—": "б/р",
    "common": "Обычная",
    "uncommon": "Необычная",
    "rare": "Редкая",
    "epic": "Эпическая",
    "legendary": "Легендарная",
    "mythic": "Мифическая",
    "foil": "Фойл",
    "foiled": "Фойл",
    "promo": "Промо",
    "promotion": "Промо",
    "special": "Особая",
    "secret": "Секретная",
    "default": "Стандартная",
    "basic": "Базовая",
    "holo": "Голо",
    "holographic": "Голографическая",
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
        f"Категория: {category}",
        f"Редкость: {_rarity_label_ru(r_raw)}",
        f"Цена: {price_str}",
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
        cards.append(
            {
                "id": item.get("id"),
                "name": item.get("title", "Без названия"),
                "price": item.get("priceRub", 0),
                "category": item.get("category", "Без категории"),
                "rarity": (str(rar).strip() or "—"),
                "image": image,
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


def _cart_get_lines(user_data: dict) -> List[dict]:
    c = user_data.get("cart")
    if not isinstance(c, list):
        return []
    return c


def _cart_add_line(
    user_data: dict, ref: str, product: dict, name: str, price: int
) -> None:
    lines = _cart_get_lines(user_data)
    for line in lines:
        if str(line.get("ref")) == ref:
            line["qty"] = int(line.get("qty") or 1) + 1
            user_data["cart"] = lines
            return
    lines.append({"ref": ref, "name": name, "price": price, "qty": 1})
    user_data["cart"] = lines


def _cart_remove_line(user_data: dict, index: int) -> bool:
    lines = _cart_get_lines(user_data)
    if 0 <= index < len(lines):
        lines.pop(index)
        user_data["cart"] = lines
        return True
    return False


def _cart_clear(user_data: dict) -> None:
    user_data["cart"] = []


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
        return "Корзина пуста. Добавь карточки из коллекции (кнопка «➕ В корзину» под снимками) или нажми 📦 Каталог."
    total, npos = _cart_totals(lines)
    parts: List[str] = [
        "🛒 Корзина",
    ]
    if len(lines) > 20:
        parts.append(
            "Кнопки «➖» — только для первых 20 поз.; если больше, нажмите «Очистить» и соберите снова."
        )
    parts += [
        f"Позиций: {len(lines)} | штук: {npos} | сумма: {total} ₽",
        "",
    ]
    for i, x in enumerate(lines, 1):
        name = (x.get("name") or "—")[:120]
        if len((x.get("name") or "")) > 120:
            name += "…"
        q = int(x.get("qty") or 1)
        p = int(x.get("price") or 0)
        sub = p * q
        parts.append(f"{i}) {name}")
        parts.append(f"   {p} ₽ × {q} = {sub} ₽")
    parts.append("")
    parts.append(
        f"Снизу — убрать позицию, «Очистить» или «Оформить заказ». "
        f"После оформления бот попросит твой @username: заказ и сумма уйдут {ORDER_MENTION} и администратору; с тобой свяжутся в ближайшее время."
    )
    s = "\n".join(parts)
    if len(s) > 3900:
        s = s[:3890] + "…"
    return s


def _kb_cart(lines: List[dict]) -> Optional[InlineKeyboardMarkup]:
    if not lines:
        return None
    rows: List[List[InlineKeyboardButton]] = []
    n_show = min(20, len(lines))
    for i in range(n_show):
        x = lines[i]
        nm = (x.get("name") or "—")[:28]
        if len((x.get("name") or "")) > 28:
            nm = nm.rstrip() + "…"
        q = int(x.get("qty") or 1)
        rows.append(
            [
                InlineKeyboardButton(
                    f"➖ {i + 1}. {nm} (×{q})",
                    callback_data=f"rm:{i}",
                )
            ],
        )
    rows.append(
        [
            InlineKeyboardButton("🧹 Очистить", callback_data="cz:0"),
            InlineKeyboardButton("✅ Оформить заказ", callback_data="co:0"),
        ],
    )
    return InlineKeyboardMarkup(rows)


def _order_snapshot_for_notify(lines: List[dict]) -> Tuple[str, int]:
    total, _ = _cart_totals(lines)
    b = f"Сумма: {total} ₽\n\n"
    for i, x in enumerate(lines, 1):
        n = (x.get("name") or "—")[:200]
        p = int(x.get("price") or 0)
        q = int(x.get("qty") or 1)
        b += f"{i}) {n} — {p} ₽ × {q} = {p * q} ₽\n"
    return b, total


async def _notify_admins(
    context: ContextTypes.DEFAULT_TYPE,
    user,
    client_username: str,
    order_body: str,
) -> bool:
    """Пишет в TELEGRAM_ORDER_NOTIFY_ID: заказ, сумма, @Daniel_official (пинг), данные покупателя."""
    chat_id = ORDER_NOTIFY_CHAT_ID
    if not chat_id:
        return False
    u = user or None
    uid = u.id if u else "—"
    t_un = f"@{u.username}" if u and u.username else "нет @username"
    uname = (
        f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip()
        if u
        else "—"
    )
    if not uname:
        uname = "—"
    text = (
        f"🛒 Новый заказ\n{ORDER_MENTION}\n\n"
        f"Логин для заказа: {client_username}\n"
        f"В боте: {t_un} | id: {uid} | {uname}\n\n"
        f"{order_body}\n"
        f"---\n"
        f"Свяжитесь в ближайшее время. {ORDER_MENTION}"
    )
    if len(text) > 4090:
        text = text[:4086] + "…"
    log = logging.getLogger(__name__)
    try:
        await context.bot.send_message(
            chat_id=chat_id, text=text, disable_web_page_preview=True
        )
    except Exception as e:
        log.exception("Ошибка отправки уведомления о заказе: %s", e)
        return False
    return True


def _category_names(products: List[dict]) -> List[str]:
    s = {str(p.get("category", "Без категории") or "Без категории") for p in products}
    return sorted(s, key=str.lower)


def _rarities_in_category(products: List[dict], category: str) -> List[str]:
    u: set = set()
    for p in products:
        if str(p.get("category", "Без категории")) != category:
            continue
        u.add(str(p.get("rarity", "—") or "—"))
    return sorted(u, key=str.lower)


def _filter_cards(products: List[dict], category: str, rarity: str) -> List[dict]:
    out: List[dict] = []
    for p in products:
        if str(p.get("category", "Без категории")) != category:
            continue
        pr = str(p.get("rarity", "—") or "—")
        if pr == rarity:
            out.append(p)
    return out


def _rarities_globally(products: List[dict]) -> List[str]:
    u: set = {str(p.get("rarity", "—") or "—") for p in products}
    return sorted(u, key=str.lower)


def _btn_label(s: str, max_len: int = 22) -> str:
    t = s.strip() or "—"
    return t if len(t) <= max_len else t[: max_len - 1] + "…"


def _kb_categories(categories: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🌐 Все категории", callback_data="c:all")],
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


def _kb_rarities(cat_tok: str, rarities: List[str]) -> InlineKeyboardMarkup:
    """
    j:{cat_tok}:all  — все редкости в рамках выбранных категорий
    j:{cat_tok}:0,1,…  — индекс в rarities
    """
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🎲 Все редкости", callback_data=f"j:{cat_tok}:all")],
    ]
    row: List[InlineKeyboardButton] = []
    for i, r in enumerate(rarities):
        label = _rarity_label_ru(r)[:20]
        row.append(InlineKeyboardButton(_btn_label(label, 20), callback_data=f"j:{cat_tok}:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [InlineKeyboardButton("⬅ К категориям", callback_data="m:0")],
    )
    return InlineKeyboardMarkup(rows)


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
    """cat_tok: "all" или индекс категории. rar_tok: "all" или индекс в списке редкостей."""
    if cat_tok == "all":
        base = list(products)
        cat_label = "Все категории"
        rlist = _rarities_globally(products) or ["—"]
    else:
        cix = int(cat_tok)
        if cix < 0 or cix >= len(cats):
            return [], "", ""
        cn = cats[cix]
        cat_label = cn
        base = [p for p in products if str(p.get("category", "Без категории")) == cn]
        rlist = _rarities_in_category(products, cn) or ["—"]
    if rar_tok == "all":
        return base, cat_label, "все редкости"
    rix = int(rar_tok)
    if rix < 0 or rix >= len(rlist):
        return [], cat_label, ""
    rname = rlist[rix]
    out = [p for p in base if str(p.get("rarity", "—") or "—") == rname]
    rlab = _rarity_label_ru(rname)
    return out, cat_label, rlab


def _numbered_list_view(
    in_scope: List[dict],
    cat_tok: str,
    rar_tok: str,
    page: int,
    cat_label: str,
    rar_label: str,
) -> tuple[str, InlineKeyboardMarkup]:
    """
    Шаг 3: сверху — выбранный фильтр, затем полный перечень «№. название» (по страницам);
    картинки — только на шаге 4, по кнопке «Показать карточки».
    """
    if not in_scope:
        t = (
            f"Выбранные фильтры\n"
            f"• Категория: {cat_label}\n"
            f"• Редкость: {rar_label}\n\n"
            f"По этим настройкам карт нет. Выбери другой фильтр (шаг 1 или 2)."
        )
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅ К выбору редкости", callback_data=f"h:{cat_tok}")],
            ]
        )
        return t, kb
    total = max(1, (len(in_scope) + LIST_ITEMS_PER_PAGE - 1) // LIST_ITEMS_PER_PAGE)
    page = max(0, min(page, total - 1))
    start = page * LIST_ITEMS_PER_PAGE
    chunk = in_scope[start : start + LIST_ITEMS_PER_PAGE]

    parts: List[str] = [
        "Коллекция (шаг 3 из 4) — полный перечень по выбранному фильтру",
        "",
        "Выбранные фильтры",
        f"• Категория: {cat_label}",
        f"• Редкость: {rar_label}",
        "",
        "Список карт (номер — как в общей нумерации в пределах фильтра):",
        f"позиции {start + 1}–{start + len(chunk)} из {len(in_scope)}; страница {page + 1} из {total}.",
        "",
    ]
    for k, p in enumerate(chunk, start=1 + start):
        name = p.get("name") or "—"
        if len(name) > 200:
            name = name[:197] + "…"
        parts.append(f"{k}. {name}")
    text = "\n".join(parts)
    if len(text) > 3900:
        text = text[:3890] + "\n…"
    follow = (
        f"\n\nШаг 4: «Показать карточки» — {len(in_scope)} снимков, «➕ в корзину», затем «🛒 Корзина» для суммы и заказа."
    )
    if len(text) + len(follow) <= 4090:
        text = text + follow

    rows: List[List[InlineKeyboardButton]] = []
    cb_g = f"g:{cat_tok}:{rar_tok}"
    if len(cb_g.encode("utf-8")) <= 64:
        rows.append(
            [
                InlineKeyboardButton(
                    f"🖼 Показать карточки ({len(in_scope)} шт.)",
                    callback_data=cb_g,
                )
            ],
        )
    nav: List[InlineKeyboardButton] = []
    p_prev = f"p:{cat_tok}:{rar_tok}:{page - 1}"
    p_next = f"p:{cat_tok}:{rar_tok}:{page + 1}"
    if page > 0 and len(p_prev) <= 64:
        nav.append(InlineKeyboardButton("◀️", callback_data=p_prev))
    if page < total - 1 and len(p_next) <= 64:
        nav.append(InlineKeyboardButton("▶️", callback_data=p_next))
    if nav:
        rows.append(nav)
    rows.append(
        [InlineKeyboardButton("⬅ К выбору редкости", callback_data=f"h:{cat_tok}")],
    )
    return text, InlineKeyboardMarkup(rows)


async def illucards_sync_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    log = logging.getLogger(__name__)
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg:
        return
    args = context.args or []
    if args:
        order_text = " ".join(args).strip()
        if not order_text:
            await msg.reply_text("Привет!", reply_markup=REPLY_KB)
            return
        context.user_data["deeplink_order"] = order_text
        out = f"🛒 Ваш заказ:\n{order_text}"
        if len(out) > 4096:
            out = out[:4090] + "…"
        await msg.reply_text(
            out,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ Оформить заказ",
                            callback_data=DEEPLINK_ORDER_CB,
                        ),
                    ],
                ],
            ),
        )
        return
    await msg.reply_text("Привет!", reply_markup=REPLY_KB)


async def on_deeplink_order_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Кнопка после /start <payload> — уведомление админу (ADMIN_ID)."""
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if (q.data or "").strip() != DEEPLINK_ORDER_CB:
        return
    order = (context.user_data or {}).get("deeplink_order")
    if not order or not str(order).strip():
        await q.answer("Сначала откройте ссылку с заказом ещё раз.", show_alert=True)
        return
    if not ADMIN_ID:
        await q.answer("ADMIN_ID не настроен в боте", show_alert=True)
        return
    u = q.from_user
    un = f"@{u.username}" if u and u.username else "—"
    name = f"{(u.first_name or '')} {(u.last_name or '')}".strip() or "—"
    uid = u.id if u else "—"
    text = f"🔥 Новый заказ:\n{order}\n\nОт: {un} | {name} | id={uid}"
    if len(text) > 4090:
        text = text[:4086] + "…"
    log = logging.getLogger(__name__)
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=text,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.exception("Deep link order → админ: %s", e)
        await q.answer("Не получилось отправить админу", show_alert=True)
        return
    context.user_data.pop("deeplink_order", None)
    await q.answer("Заказ отправлен", show_alert=False)
    try:
        await q.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await q.message.reply_text(
        "Готово. Админ получит уведомление о заказе. Скоро с вами свяжутся."
    )


async def catalog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_catalog(update, context)


async def send_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Мастер: категория → редкость (подписи на рус.) → полный перечень названий (шаг 3) →
    по кнопке «Показать карточки» — фото с ценой.
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
    log.info("Каталог: шаг 1, категорий=%d (фото — только после кнопки в шаге 4)", len(categories))
    await msg.reply_text(
        "Коллекция (illucards)\n"
        "1) Категория (или «Все категории»)\n"
        "2) Редкость на русском в кнопках, или «Все редкости»\n"
        "3) Полный список: сверху фильтр, ниже — номер и название (при длине списка — листайте ◀▶)\n"
        "4) «Показать карточки» — снимки, «➕ в корзину»; «🛒 Корзина» — сумма, оформление, @username",
        reply_markup=_kb_categories(categories),
    )


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
        "Коллекция (illucards). Шаг 1 — категория (или «Все категории»).",
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
        rlist = _rarities_globally(products) or ["—"]
        t = "Коллекция: все категории\n\nШаг 2 — выберите редкость (подписи на русском) или «Все редкости»"
        await q.answer()
        await q.edit_message_text(t, reply_markup=_kb_rarities("all", rlist))
        return
    ci = int(key)
    if ci < 0 or ci >= len(cats):
        await q.answer("Категория не найдена", show_alert=True)
        return
    cat = cats[ci]
    rlist = _rarities_in_category(products, cat) or ["—"]
    t = f"Коллекция: {cat}\n\nШаг 2 — редкость (подписи на русском) или «Все редкости»"
    await q.answer()
    await q.edit_message_text(t, reply_markup=_kb_rarities(str(ci), rlist))


async def on_pick_rarity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    m = re.match(r"^j:([^:]+):(all|\d+)$", (q.data or "").strip())
    if not m:
        return
    cat_tok, rar_tok = m.group(1), m.group(2)
    products = await _get_products(context)
    cats = _category_names(products)
    in_scope, c_lab, r_lab = _filter_wizard(products, cats, cat_tok, rar_tok)
    if not c_lab:
        await q.answer("Сначала выбери шаг 1 (категория).", show_alert=True)
        return
    if rar_tok != "all" and not in_scope:
        await q.answer("По такой редкости пусто.", show_alert=True)
        return
    body, kb = _numbered_list_view(
        in_scope, cat_tok, rar_tok, 0, c_lab, r_lab
    )
    await q.answer()
    await q.edit_message_text(body, reply_markup=kb)


async def on_card_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    m = re.match(r"^p:([^:]+):([^:]+):(\d+)$", (q.data or "").strip())
    if not m:
        return
    cat_tok, rar_tok, pg = m.group(1), m.group(2), int(m.group(3))
    if pg < 0 or pg > 2000:
        await q.answer("Список устарел", show_alert=True)
        return
    products = await _get_products(context)
    cats = _category_names(products)
    in_scope, c_lab, r_lab = _filter_wizard(products, cats, cat_tok, rar_tok)
    if not c_lab:
        await q.answer("Список устарел", show_alert=True)
        return
    body, kb = _numbered_list_view(
        in_scope, cat_tok, rar_tok, pg, c_lab, r_lab
    )
    await q.answer()
    await q.edit_message_text(body, reply_markup=kb)


async def on_back_rarity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    m = re.match(r"^h:([^:]+)$", (q.data or "").strip())
    if not m:
        return
    await q.answer()
    cat_tok = m.group(1)
    products = await _get_products(context)
    cats = _category_names(products)
    if cat_tok == "all":
        rlist = _rarities_globally(products) or ["—"]
        t = "Коллекция: все категории (шаг 2 — редкость, подписи на русском)"
        await q.edit_message_text(t, reply_markup=_kb_rarities("all", rlist))
        return
    cix = int(cat_tok)
    if cix < 0 or cix >= len(cats):
        await _edit_to_categories(q, context)
        return
    cat = cats[cix]
    rlist = _rarities_in_category(products, cat) or ["—"]
    await q.edit_message_text(
        f"Коллекция: {cat}\n(шаг 2 — редкость, подписи на русском)",
        reply_markup=_kb_rarities(str(cix), rlist),
    )


async def on_send_all_cards(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """g:{cat_tok}:{rar_tok} — после выбора фильтра: все карточки из подборки (фото, цена, купить)."""
    q = update.callback_query
    if not q or not q.message or not q.data:
        return
    m = re.match(r"^g:([^:]+):([^:]+)$", (q.data or "").strip())
    if not m:
        return
    cat_tok, rar_tok = m.group(1), m.group(2)
    products = await _get_products(context)
    if not products:
        await q.answer("Каталог пуст. Откройте «📦 Каталог» снова.", show_alert=True)
        return
    cats = _category_names(products)
    in_scope, c_lab, r_lab = _filter_wizard(products, cats, cat_tok, rar_tok)
    if not c_lab:
        await q.answer("Список устарел, начните с каталога.", show_alert=True)
        return
    if not in_scope:
        await q.answer("По этому фильтру нет карт.", show_alert=True)
        return
    n = len(in_scope)
    await q.answer("Отправляю карточки…", show_alert=False)
    if n > 20:
        await q.message.reply_text(
            f"Сейчас пошлём {n} карточек: фото, «➕ в корзину» и дальше — «🛒 Корзина» для суммы и заказа."
        )
    site_line = ILLUCARDS_BASE.removeprefix("https://").removeprefix("http://")
    for i, p in enumerate(in_scope, 1):
        gidx = _global_product_index(products, p)
        cap = f"Карта {i} из {n}\nФильтр: {c_lab} • {r_lab}\n\n" + _format_caption(
            p
        ) + f"\n🌐 {site_line}"
        if len(cap) > 1020:
            cap = cap[:1016] + "…"
        ref = _product_ref_for_callback(p, gidx)
        a_cb = f"a:{ref}"
        if len(a_cb.encode("utf-8")) > 64:
            a_cb = f"a:{gidx}"
        vc_cb = "vc:0"
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("➕ В корзину", callback_data=a_cb)],
                [InlineKeyboardButton("🛒 Открыть корзину", callback_data=vc_cb)],
            ],
        )
        photo = str(p.get("image") or "")
        try:
            if photo:
                await q.message.reply_photo(photo=photo, caption=cap, reply_markup=kb)
            else:
                await q.message.reply_text(cap, reply_markup=kb)
        except Exception:
            await q.message.reply_text(cap, reply_markup=kb)
        if i < n and (i % 3 == 0 or n > 30):
            await asyncio.sleep(0.12)
    if n:
        await q.message.reply_text(
            "Соберите заказ: «➕» в корзину, внизу — «🛒 Корзина» — там сумма, оформление и @username."
        )


async def _edit_cart_message(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = _cart_get_lines(context.user_data)
    t = _format_cart_message(lines)
    kb = _kb_cart(lines) if lines else None
    if not q.message:
        return
    try:
        await q.edit_message_text(t, reply_markup=kb)
    except Exception:
        await q.message.reply_text(t, reply_markup=kb)


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
    if product is None:
        await q.answer("Карта не в каталоге, обнови раздел «Каталог»", show_alert=True)
        return
    gix = _global_product_index(products, product)
    r = _product_ref_for_callback(product, gix)
    _cart_add_line(
        context.user_data, r, product, product.get("name") or "—", _product_price(product)
    )
    lines = _cart_get_lines(context.user_data)
    tot, npos = _cart_totals(lines)
    nlines = len(lines)
    short = f"Сумма {tot} ₽, шт. {npos}"
    await q.answer(f"➕ В корзине {nlines} п. · {short}", show_alert=False)


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
    if not _cart_remove_line(context.user_data, ix):
        await q.answer("Позиция не найдена, открой корзину снова", show_alert=True)
        return
    await q.answer()
    await _edit_cart_message(q, context)


async def on_cart_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if re.match(r"^cz:0$", (q.data or "").strip()) is None:
        return
    _cart_clear(context.user_data)
    await q.answer("Корзина пуста", show_alert=False)
    await _edit_cart_message(q, context)


async def on_checkout_ask_username(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    q = update.callback_query
    if not q or not q.data or not q.message:
        return
    if re.match(r"^co:0$", (q.data or "").strip()) is None:
        return
    lines = _cart_get_lines(context.user_data)
    if not lines:
        await q.answer("Корзина пуста", show_alert=True)
        return
    tot, npos = _cart_totals(lines)
    context.user_data["order_checkout"] = deepcopy(lines)
    context.user_data["awaiting_order_username"] = True
    await q.answer()
    await q.message.reply_text(
        f"Сумма заказа: {tot} ₽ (всего {npos} шт., поз. {len(lines)}).\n"
        f"Напиши ниже свой @username (как в Telegram) — по нему с тобой свяжутся.\n"
        f"Состав и сумма уйдут админу, с упоминанием {ORDER_MENTION}."
    )


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
    lines = _cart_get_lines(context.user_data)
    t = _format_cart_message(lines)
    kb = _kb_cart(lines) if lines else None
    await q.message.reply_text(t, reply_markup=kb)


def _is_username_suggestion(s: str) -> bool:
    t = s.strip()
    if not t.startswith("@"):
        return False
    u = t[1:]
    if not (5 <= len(u) <= 32) or not u[0].isalpha():
        return False
    return all(c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for c in u)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.text:
        return
    text = msg.text.strip()
    user_data = context.user_data

    if text in ("📦 Каталог", "🔥 Акции", "💬 Связь"):
        user_data.pop("awaiting_order_username", None)
        user_data.pop("order_checkout", None)

    if user_data.get("awaiting_order_username"):
        if text in ("🛒 Корзина",):
            await msg.reply_text(
                "Сначала напиши свой @username (одна строка), чтобы завершить заказ — или сброс: «📦 Каталог»."
            )
            return
        if not _is_username_suggestion(text):
            await msg.reply_text("Напиши Telegram-логин с @, например @myname (одной строкой).")
            return
        lines: Optional[List[dict]] = user_data.get("order_checkout")
        user = msg.from_user
        user_data.pop("awaiting_order_username", None)
        user_data.pop("order_checkout", None)
        if not lines:
            await msg.reply_text("Сессия оформления сброшена. Соберите «🛒 Корзина» снова.")
            return
        ob, tot = _order_snapshot_for_notify(lines)
        _cart_clear(user_data)
        ok = await _notify_admins(context, user, text, ob)
        log = logging.getLogger(__name__)
        if ok:
            note = (
                f"Принято! Заказ на {tot} ₽. Скоро с вами свяжутся. "
                f"Админу и {ORDER_MENTION} ушло уведомление с составом заказа."
            )
        else:
            if not ORDER_NOTIFY_CHAT_ID:
                log.warning("TELEGRAM_ORDER_NOTIFY_ID не настроен — уведомление в чат не отправлено")
            note = (
                f"Принято! Заказ на {tot} ₽, логин для связи: {text}.\n"
                f"Сообщение боту не доставилось (проверьте TELEGRAM_ORDER_NOTIFY_ID в .env) — "
                f"напишите {ORDER_MENTION} вручную, если вас не наберут."
            )
        await msg.reply_text(note)
        return

    if text == "🛒 Корзина":
        cl = _cart_get_lines(user_data)
        t = _format_cart_message(cl)
        kb = _kb_cart(cl) if cl else None
        await msg.reply_text(t, reply_markup=kb)
        return

    if text == "📦 Каталог":
        await send_catalog(update, context)
    elif text == "🔥 Акции":
        await send_promo(update, context)
    elif text == "💬 Связь":
        await send_contact(update, context)


async def post_init(application: Application) -> None:
    log = logging.getLogger(__name__)
    if ORDER_NOTIFY_CHAT_ID is not None:
        log.info("Уведомления о заказах: chat_id=%s, mention=%s", ORDER_NOTIFY_CHAT_ID, ORDER_MENTION)
    else:
        log.warning(
            "TELEGRAM_ORDER_NOTIFY_ID не задан — бот не сможет присылать заказы в чат (см. .env)"
        )
    if ADMIN_ID:
        log.info("Deep link /start-заказы: ADMIN_ID=%s", ADMIN_ID)
    else:
        log.warning("ADMIN_ID=0 — кнопка «Оформить» с ссылки t.me/...?start= не сможет уведомить админа")
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
    app.add_handler(
        CallbackQueryHandler(
            on_deeplink_order_confirm, pattern=re.compile(r"^dl:ok$")
        )
    )
    app.add_handler(CallbackQueryHandler(on_checkout_ask_username, pattern=re.compile(r"^co:0$")))
    app.add_handler(CallbackQueryHandler(on_view_cart_callback, pattern=re.compile(r"^vc:0$")))
    app.add_handler(CallbackQueryHandler(on_cart_remove_line, pattern=re.compile(r"^rm:(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_cart_clear, pattern=re.compile(r"^cz:0$")))
    app.add_handler(CallbackQueryHandler(on_add_to_cart, pattern=re.compile(r"^a:(.+)$")))
    app.add_handler(CallbackQueryHandler(on_send_all_cards, pattern=re.compile(r"^g:([^:]{1,12}):([^:]{1,6})$")))
    app.add_handler(CallbackQueryHandler(on_card_page, pattern=re.compile(r"^p:([^:]{1,12}):([^:]{1,6}):(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_back_rarity, pattern=re.compile(r"^h:([^:]{1,12})$")))
    app.add_handler(CallbackQueryHandler(on_menu_main, pattern=re.compile(r"^m:0$")))
    app.add_handler(CallbackQueryHandler(on_pick_rarity, pattern=re.compile(r"^j:([^:]{1,12}):(all|\d+)$")))
    app.add_handler(CallbackQueryHandler(on_pick_category, pattern=re.compile(r"^c:(\d+|all)$")))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
