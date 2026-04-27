import json
import logging
import os
import re
import sys
from typing import Any, List

import aiohttp
from telegram import (
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

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

CARDS_JSON_URL = "https://www.illucards.by/cards.json"

PROMO_PHOTO = "https://picsum.photos/seed/promo/400/300"

REPLY_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton("📦 Каталог")],
        [KeyboardButton("🔥 Акции")],
        [KeyboardButton("💬 Связь")],
    ],
    resize_keyboard=True,
)


def _as_product_list(data: Any) -> List[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("cards", "products", "items", "data", "results"):
            v = data.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _pick_str(obj: dict, *keys: str) -> str:
    for k in keys:
        v = obj.get(k)
        if v is None:
            continue
        if isinstance(v, (str, int, float)):
            s = str(v).strip()
            if s:
                return s
    return ""


def _normalize_products(raw: List[dict]) -> List[dict]:
    out: List[dict] = []
    for item in raw:
        image = _pick_str(item, "image", "imageUrl", "img", "photo", "picture", "src", "url")
        name = _pick_str(item, "name", "title", "label")
        category = _pick_str(item, "category", "type", "group")
        price = _pick_str(item, "price", "cost", "amount")
        if not name and not image:
            continue
        out.append(
            {
                "image": image,
                "name": name or "Без названия",
                "category": category or "—",
                "price": price or "—",
            }
        )
    return out


def _format_caption(p: dict) -> str:
    name = p.get("name") or ""
    category = p.get("category") or ""
    price = p.get("price") or ""
    return f"{name}\nКатегория: {category}\nЦена: {price}"


async def load_products(context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        timeout = aiohttp.ClientTimeout(total=45)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                CARDS_JSON_URL,
                headers={"Accept": "application/json"},
            ) as resp:
                if resp.status != 200:
                    return False
                body = await resp.read()
                text = body.decode("utf-8", errors="replace").lstrip()
                if text.startswith("<"):
                    return False
                data = json.loads(text)
    except (aiohttp.ClientError, json.JSONDecodeError, UnicodeDecodeError):
        return False
    except Exception:
        return False

    products = _normalize_products(_as_product_list(data))
    if not products:
        return False
    context.bot_data["products"] = products
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text("Привет!", reply_markup=REPLY_KB)


async def send_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return

    if not await load_products(context):
        await msg.reply_text("Не удалось загрузить товары")
        return

    products: List[dict] = context.bot_data.get("products") or []
    if not products:
        await msg.reply_text("Не удалось загрузить товары")
        return

    for i, p in enumerate(products):
        caption = _format_caption(p)
        if len(caption) > 1024:
            caption = caption[:1021] + "…"
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🛒 В корзину",
                        callback_data=f"add_to_cart:{i}",
                    )
                ]
            ]
        )
        image = p.get("image") or ""
        try:
            if image:
                await msg.reply_photo(photo=image, caption=caption, reply_markup=kb)
            else:
                await msg.reply_text(caption, reply_markup=kb)
        except Exception:
            await msg.reply_text(caption, reply_markup=kb)


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


async def on_add_to_cart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return

    m = re.match(r"^add_to_cart:(\d+)$", query.data)
    if not m:
        return

    idx = int(m.group(1))
    products: List[dict] = context.bot_data.get("products") or []
    if idx < 0 or idx >= len(products):
        await query.answer("Товар не найден", show_alert=True)
        return

    product = products[idx]
    cart: List[dict] = context.user_data.setdefault("cart", [])
    cart.append(dict(product))

    await query.answer("Добавлено в корзину")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg or not msg.text:
        return
    text = msg.text.strip()
    if text == "📦 Каталог":
        await send_catalog(update, context)
    elif text == "🔥 Акции":
        await send_promo(update, context)
    elif text == "💬 Связь":
        await send_contact(update, context)


async def post_init(application: Application) -> None:
    print("Бот запущен!")
    me = await application.bot.get_me()
    if me.username:
        print(f"https://t.me/{me.username}")


def main() -> None:
    if not TOKEN:
        sys.exit("TELEGRAM_BOT_TOKEN is not set")

    app = Application.builder().token(TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(
        CallbackQueryHandler(on_add_to_cart, pattern=re.compile(r"^add_to_cart:\d+$"))
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
