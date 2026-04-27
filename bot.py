# .venv + dotenv (Mac):
#   source .venv/bin/activate
#   pip install python-dotenv
#   pip install -r requirements.txt
#   pip list | grep dotenv
#   python bot.py
# Если pip не найден: python3 -m pip install python-dotenv
import logging
import os
import re
import sys
import time
from typing import List, Optional

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

CARDS_JSON_URL = os.getenv("CARDS_JSON_URL", "https://www.illucards.by/cards.json")

PROMO_PHOTO = "https://picsum.photos/seed/promo/400/300"
ILLUCARDS_BASE = "https://www.illucards.by"
SYNC_EVERY_SEC = int(os.getenv("ILLUCARDS_SYNC_EVERY_SEC", "900"))
CARDS_PER_PAGE = 5

REPLY_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton("📦 Каталог")],
        [KeyboardButton("🔥 Акции")],
        [KeyboardButton("💬 Связь")],
    ],
    resize_keyboard=True,
)


def _format_caption(p: dict) -> str:
    name = p.get("name") or "Без названия"
    category = p.get("category") or ""
    rarity = str(p.get("rarity", "") or "").strip()
    price = p.get("price")
    price_str = str(price) if price is not None and price != "" else "—"
    lines = [f"{name}", f"Категория: {category}"]
    if rarity and rarity != "—":
        lines.append(f"Редкость: {rarity}")
    lines.append(f"Цена: {price_str}")
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


def _format_buy_callback(p: dict, index: int) -> str:
    """До 64 байт. Сначала id с сайта, иначе индекс (совместимость)."""
    pid = p.get("id")
    if pid is not None and str(pid).strip() != "":
        return f"buy:{pid}"
    return f"buy:{index}"


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
        label = (r if r != "—" else "б/р")[:16]
        row.append(InlineKeyboardButton(_btn_label(label, 16), callback_data=f"j:{cat_tok}:{i}"))
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
    rlab = rname if rname != "—" else "б/р"
    return out, cat_label, rlab


def _numbered_list_view(
    in_scope: List[dict],
    all_products: List[dict],
    cat_tok: str,
    rar_tok: str,
    page: int,
    cat_label: str,
    rar_label: str,
) -> tuple[str, InlineKeyboardMarkup]:
    """Шаг 3: только нумерованные названия; кнопки-цифры — потом v: (фото и цена)."""
    if not in_scope:
        t = f"Коллекция: {cat_label}\nРедкость: {rar_label}\n\nПо выбранным фильтрам пусто."
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅ К редкости", callback_data=f"h:{cat_tok}")],
            ]
        )
        return t, kb
    total = max(1, (len(in_scope) + CARDS_PER_PAGE - 1) // CARDS_PER_PAGE)
    page = max(0, min(page, total - 1))
    start = page * CARDS_PER_PAGE
    chunk = in_scope[start : start + CARDS_PER_PAGE]

    parts: List[str] = [
        f"Коллекция: {cat_label}",
        f"Редкость: {rar_label}",
        "",
        f"Шаг 3. Список № и название (без фото). Позиции {start + 1}–{start + len(chunk)} из {len(in_scope)}. Стр. {page + 1}/{total}.",
        "Ниже — цифры. Нажми номер, чтобы в следующем шаге пришла карта, цена и картинка.",
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

    rows: List[List[InlineKeyboardButton]] = []
    num_row: List[InlineKeyboardButton] = []
    for k, p in enumerate(chunk):
        n = 1 + start + k
        gidx = _global_product_index(all_products, p)
        cb = f"v:{gidx}"
        if len(cb.encode("utf-8")) > 64:
            continue
        num_row.append(InlineKeyboardButton(str(n), callback_data=cb))
        if len(num_row) == 5:
            rows.append(num_row)
            num_row = []
    if num_row:
        rows.append(num_row)

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
        [InlineKeyboardButton("⬅ К редкости", callback_data=f"h:{cat_tok}")],
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
    if update.message:
        await update.message.reply_text("Привет!", reply_markup=REPLY_KB)


async def catalog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_catalog(update, context)


async def send_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Только мастер: категория → редкость → список с пагинацией → фото по клику.
    Не отправляет пачку фото (это никогда не делаем здесь).
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
    log.info("Каталог: шаг 1, категорий=%d (без массовой рассылки фото)", len(categories))
    await msg.reply_text(
        "Коллекция (illucards)\n"
        "Шаг 1 — какая категория? (или «Все категории»)\n"
        "Шаг 2 — редкость, или «Все редкости»\n"
        "Шаг 3 — список только номеров и названий\n"
        "Шаг 4 — нажмёшь номер, пришлём картинку, цену, кнопку «Купить»",
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


def _rarity_caption_name(r: str) -> str:
    if r == "—" or not str(r).strip():
        return "все / б/р"
    return str(r)


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
        "Коллекция (illucards). Шаг 1 — какая категория? (либо «Все категории»)",
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
        t = "Коллекция: **все категории**\n\nШаг 2 — редкость? (либо «Все редкости»)"
        t = t.replace("**", "")
        await q.answer()
        await q.edit_message_text(t, reply_markup=_kb_rarities("all", rlist))
        return
    ci = int(key)
    if ci < 0 or ci >= len(cats):
        await q.answer("Категория не найдена", show_alert=True)
        return
    cat = cats[ci]
    rlist = _rarities_in_category(products, cat) or ["—"]
    t = f"Коллекция: {cat}\n\nШаг 2 — редкость? (либо «Все редкости»)"
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
        in_scope, products, cat_tok, rar_tok, 0, c_lab, r_lab
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
        in_scope, products, cat_tok, rar_tok, pg, c_lab, r_lab
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
        t = "Коллекция: **все категории** (шаг 2 — редкость)".replace("**", "")
        await q.edit_message_text(t, reply_markup=_kb_rarities("all", rlist))
        return
    cix = int(cat_tok)
    if cix < 0 or cix >= len(cats):
        await _edit_to_categories(q, context)
        return
    cat = cats[cix]
    rlist = _rarities_in_category(products, cat) or ["—"]
    await q.edit_message_text(
        f"Коллекция: {cat}\n(шаг 2 — редкость)",
        reply_markup=_kb_rarities(str(cix), rlist),
    )


async def on_view_card(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.message or not q.data:
        return
    m = re.match(r"^v:(\d+)$", (q.data or "").strip())
    if not m:
        return
    idx = int(m.group(1))
    app = context.application
    products = list(app.bot_data.get("products") or [])
    if not (0 <= idx < len(products)) or not products:
        extra = await load_products()
        if extra:
            app.bot_data["products"] = extra
        products = list(app.bot_data.get("products") or [])
    if not (0 <= idx < len(products)):
        await q.answer("Список устарел. Открой Каталог снова", show_alert=True)
        return
    await q.answer()
    p = products[idx]
    site_line = ILLUCARDS_BASE.removeprefix("https://").removeprefix("http://")
    cap = "Шаг 4. Карта, цена, картинка\n\n" + _format_caption(p) + f"\n🌐 {site_line}"
    if len(cap) > 1020:
        cap = cap[:1016] + "…"
    bcb = _format_buy_callback(p, idx)
    if len(bcb.encode("utf-8")) > 64:
        bcb = f"buy:{idx}"
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Купить", callback_data=bcb)],
        ]
    )
    photo = p.get("image") or ""
    if photo:
        try:
            await q.message.reply_photo(photo=photo, caption=cap, reply_markup=kb)
        except Exception:
            await q.message.reply_text(cap, reply_markup=kb)
    else:
        await q.message.reply_text(cap, reply_markup=kb)


async def on_buy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.message:
        return

    m = re.match(r"^buy:(.+)$", (query.data or "").strip())
    if not m:
        return
    ref = m.group(1)
    app = context.application
    products: List[dict] = list(app.bot_data.get("products") or [])

    product = _product_from_callback(ref, products)
    if product is None:
        extra = await load_products()
        if extra:
            app.bot_data["products"] = extra
            app.bot_data["illucards_synced_at"] = time.time()
        products = list(app.bot_data.get("products") or [])
        product = _product_from_callback(ref, products)
    if product is None:
        await query.answer("Карточка не найдена. Обнови каталог.", show_alert=True)
        return
    context.user_data["awaiting_order_username"] = True
    context.user_data["pending_order_product_name"] = product.get("name") or ""

    await query.answer()
    await query.message.reply_text("Напиши свой @username для заказа")


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

    if text in ("📦 Каталог", "🔥 Акции", "💬 Связь"):
        context.user_data.pop("awaiting_order_username", None)
        context.user_data.pop("pending_order_product_name", None)

    if context.user_data.get("awaiting_order_username"):
        if not _is_username_suggestion(text):
            await msg.reply_text("Напиши Telegram-логин с @, например @yourname")
            return
        name = str(context.user_data.get("pending_order_product_name", "")).strip()
        context.user_data.pop("awaiting_order_username", None)
        context.user_data.pop("pending_order_product_name", None)
        extra = f" по товару: {name}" if name else ""
        await msg.reply_text(
            f"Принято! Заказ оформлен{extra}, username: {text}."
        )
        return

    if text == "📦 Каталог":
        await send_catalog(update, context)
    elif text == "🔥 Акции":
        await send_promo(update, context)
    elif text == "💬 Связь":
        await send_contact(update, context)


async def post_init(application: Application) -> None:
    log = logging.getLogger(__name__)
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
    app.add_handler(CallbackQueryHandler(on_view_card, pattern=re.compile(r"^v:\d+$")))
    app.add_handler(CallbackQueryHandler(on_card_page, pattern=re.compile(r"^p:([^:]{1,12}):([^:]{1,6}):(\d+)$")))
    app.add_handler(CallbackQueryHandler(on_back_rarity, pattern=re.compile(r"^h:([^:]{1,12})$")))
    app.add_handler(CallbackQueryHandler(on_menu_main, pattern=re.compile(r"^m:0$")))
    app.add_handler(CallbackQueryHandler(on_pick_rarity, pattern=re.compile(r"^j:([^:]{1,12}):(all|\d+)$")))
    app.add_handler(CallbackQueryHandler(on_pick_category, pattern=re.compile(r"^c:(\d+|all)$")))
    app.add_handler(CallbackQueryHandler(on_buy, pattern=re.compile(r"^buy:")))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
