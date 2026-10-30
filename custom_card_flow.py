"""Индивидуальная карточка по фото: /start custom_card_order и согласование макета.

Параметр start не пересекается с оплатой заказа с сайта (order_…),
входом (web_login, login) и поддержкой (support). Старый custom_card тоже принимается.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.ext import (
    ApplicationHandlerStop,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

log = logging.getLogger(__name__)

# Отдельный deep link для кнопки «Заказать» в акции.
PRIMARY_START_PAYLOAD = "custom_card_order"
# Заготовка на сайте (telegramCustomCardOrderUrl) уже использует custom_card.
LEGACY_START_PAYLOAD = "custom_card"
START_PAYLOADS = frozenset({PRIMARY_START_PAYLOAD, LEGACY_START_PAYLOAD})
DRAFT_KEY = "cc_draft"
SEND_KEY = "cc_send_oid"
SUPPORT_KEY = "cc_support_oid"

_RE_CB = re.compile(r"^cc:(.+)$")


def _b():
    import bot as bot_mod

    return bot_mod


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _peek_draft(uid: int) -> Optional[dict]:
    B = _b()
    bucket = B.user_states.get(int(uid))
    if not isinstance(bucket, dict):
        return None
    raw = bucket.get(DRAFT_KEY)
    return raw if isinstance(raw, dict) else None


def _draft(uid: int) -> dict:
    B = _b()
    bucket = B.user_states.setdefault(int(uid), {})
    raw = bucket.get(DRAFT_KEY)
    if not isinstance(raw, dict):
        raw = {}
        bucket[DRAFT_KEY] = raw
    return raw


def _set_draft(uid: int, data: dict) -> None:
    B = _b()
    bucket = B.user_states.setdefault(int(uid), {})
    bucket[DRAFT_KEY] = data
    B.save_state()


def _clear_draft(uid: int) -> None:
    B = _b()
    bucket = B.user_states.get(int(uid))
    if isinstance(bucket, dict):
        bucket.pop(DRAFT_KEY, None)
        if not bucket:
            B.user_states.pop(int(uid), None)
    B.save_state()


def _append_history(order: dict, event: str) -> None:
    hist = order.setdefault("history", [])
    if not isinstance(hist, list):
        hist = []
        order["history"] = hist
    hist.append({"at": _now_iso(), "event": event})


def _parse_tail_int(data: str, prefix: str) -> Optional[int]:
    raw = data[len(prefix) :]
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _next_cc_id() -> int:
    B = _b()
    n = int(getattr(B, "CC_COUNTER", 1) or 1)
    existing = [int(k) for k in getattr(B, "CUSTOM_CARD_ORDERS", {}) if str(k).isdigit()]
    if existing:
        n = max(n, max(existing) + 1)
    B.CC_COUNTER = n + 1
    return n


def _order(oid: int) -> Optional[dict]:
    B = _b()
    raw = getattr(B, "CUSTOM_CARD_ORDERS", {}).get(int(oid))
    return raw if isinstance(raw, dict) else None


PRICE_FULL_RUB = 2000
PRICE_FULL_BYN = 70
PRICE_COPY_RUB = 1000
PRICE_COPY_BYN = 35


def _price_blurb() -> str:
    return (
        f"Полноценная: {PRICE_FULL_RUB} ₽ / {PRICE_FULL_BYN} BYN\n"
        f"Копия этого макета: −50% ({PRICE_COPY_RUB} ₽ / {PRICE_COPY_BYN} BYN)"
    )


def _queue_len(d: dict) -> int:
    queue = d.get("queue") if isinstance(d.get("queue"), list) else []
    return max(1, len(queue)) if queue else 1


def _queue_index(d: dict) -> int:
    try:
        return max(0, int(d.get("queue_i") or 0))
    except (TypeError, ValueError):
        return 0


def _card_title(d: dict) -> str:
    it = _current_queue_item(d) if _is_site_attach(d) else None
    if it:
        name = str(it.get("name") or "").strip()
        if name:
            return name[:200]
    return "Карточка на заказ"


def _copies_from_draft(d: dict) -> int:
    it = _current_queue_item(d) if _is_site_attach(d) else None
    raw = d.get("copies")
    if raw is None and it:
        raw = it.get("copies")
    try:
        n = int(raw or 0)
    except (TypeError, ValueError):
        n = 0
    return max(0, min(n, 99))


def _card_header(d: dict) -> str:
    if not _is_site_attach(d):
        return "Карточка на заказ"
    i = _queue_index(d) + 1
    return f"Карточка номер {i}:"


def _brief_block(source: dict) -> str:
    idea = str(source.get("idea") or "").strip()
    if idea:
        return f"Идея:\n{idea[:900]}"
    people = source.get("people") if isinstance(source.get("people"), list) else []
    return f"Люди на фото:\n{_fmt_people(people)}"


def _fmt_people(people: list) -> str:
    if not people:
        return "—"
    lines = []
    for i, p in enumerate(people, 1):
        if not isinstance(p, dict):
            continue
        u = str(p.get("universe") or "—").strip() or "—"
        c = str(p.get("character") or "—").strip() or "—"
        lines.append(f"{i}. {u} — {c}")
    return "\n".join(lines) or "—"


def _delivery_prompt(code: str) -> str:
    B = _b()
    if code == "by":
        return (
            "Напишите адрес отделения Европочты, ФИО и номер телефона одним сообщением."
        )
    if code in ("ru", "ua"):
        return "Напишите адрес отделения СДЭК или Яндекс, ФИО и номер телефона одним сообщением."
    label, _amt, _cur = B.DELIVERY_OPTIONS.get(code, B.DELIVERY_OPTIONS["ot"])
    return f"Напишите домашний адрес, ФИО и почтовый индекс одним сообщением.\n\nСпособ: {label}"


def _kb_universes(names: List[str], page: int = 0) -> InlineKeyboardMarkup:
    per = 8
    chunk = names[page * per : (page + 1) * per]
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    start = page * per
    for i, name in enumerate(chunk):
        idx = start + i
        row.append(
            InlineKeyboardButton(
                B_btn(name, 28),
                callback_data=f"cc:U:{idx}",
            )
        )
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"cc:UP:{page - 1}"))
    if (page + 1) * per < len(names):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"cc:UP:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append(
        [InlineKeyboardButton("✍️ Написать вручную", callback_data="cc:UT")]
    )
    rows.append([InlineKeyboardButton("✖️ Отмена", callback_data="cc:X")])
    return InlineKeyboardMarkup(rows)


def B_btn(s: str, max_len: int = 22) -> str:
    t = (s or "").strip() or "—"
    return t if len(t) <= max_len else t[: max_len - 1] + "…"


def _kb_characters(names: List[str], page: int = 0) -> InlineKeyboardMarkup:
    per = 8
    chunk = names[page * per : (page + 1) * per]
    rows: List[List[InlineKeyboardButton]] = []
    start = page * per
    for i, name in enumerate(chunk):
        idx = start + i
        rows.append(
            [InlineKeyboardButton(B_btn(name, 40), callback_data=f"cc:C:{idx}")]
        )
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"cc:CP:{page - 1}"))
    if (page + 1) * per < len(names):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"cc:CP:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append(
        [InlineKeyboardButton("✍️ Написать вручную", callback_data="cc:CT")]
    )
    rows.append(
        [
            InlineKeyboardButton("↩️ Вселенная", callback_data="cc:UB"),
            InlineKeyboardButton("✖️ Отмена", callback_data="cc:X"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _kb_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✖️ Отмена", callback_data="cc:X")]]
    )


def _idea_prompt() -> str:
    return (
        "Фото сохранено.\n\n"
        "Напишите, по какой вселенной карточка и какие персонажи.\n"
        "Или опишите идею целиком — что должно быть на карточке."
    )


def _universe_prompt(person_no: int, *, saved_photo: bool = False) -> str:
    if saved_photo:
        return _idea_prompt()
    n = max(1, int(person_no))
    who = "Напишите вселенную." if n <= 1 else f"Напишите вселенную для {n}-го человека на фото."
    return (
        f"{who}\n"
        "Следующим сообщением напишите персонажей."
    )


def _character_prompt(universe: str) -> str:
    uni = (universe or "").strip() or "—"
    return (
        f"Вселенная: {uni}\n\n"
        "Напишите персонажа.\n"
        "Одного — одним сообщением. Нескольких — через запятую или с новой строки."
    )


def _split_character_names(text: str) -> List[str]:
    names: List[str] = []
    seen = set()
    for chunk in re.split(r"[\n,;]+", text or ""):
        name = " ".join(chunk.split()).strip()
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        names.append(name[:80])
    return names


def _is_site_attach(d: Optional[dict]) -> bool:
    return bool(d) and str(d.get("mode") or "") == "site_order_attach"


def _item_is_custom_order(it: dict) -> bool:
    cat = str(it.get("category") or "").strip().lower()
    if "на заказ" in cat:
        return True
    try:
        copies = int(
            it.get("copyQuantity")
            if it.get("copyQuantity") is not None
            else it.get("copy_quantity") or 0
        )
    except (TypeError, ValueError):
        copies = 0
    return copies > 0


def _current_queue_item(d: dict) -> Optional[dict]:
    queue = d.get("queue") if isinstance(d.get("queue"), list) else []
    try:
        i = int(d.get("queue_i") or 0)
    except (TypeError, ValueError):
        i = 0
    if i < 0 or i >= len(queue):
        return None
    it = queue[i]
    return it if isinstance(it, dict) else None


def _photo_prompt(d: Optional[dict] = None) -> str:
    d = d if isinstance(d, dict) else {}
    if _is_site_attach(d):
        return (
            f"{_card_header(d)}\n"
            "Пришлите фото, по которому собрать эту карточку.\n"
            "Обычным фото, не файлом-документом."
        )
    return (
        "Карточка на заказ\n\n"
        f"{_price_blurb()}\n\n"
        "Пришлите фото, по которому собрать карточку.\n"
        "Обычным фото, не файлом-документом.\n"
        "Если на фото несколько человек — напишем персонажа для каждого."
    )


def _copies_prompt(d: dict) -> str:
    n = _copies_from_draft(d)
    extra = ""
    if _is_site_attach(d):
        extra = f"\nС сайта: копий — {n}." if n else "\nС сайта копий нет."
    return (
        f"{_card_header(d)}\n\n"
        "Эта карточка идёт как полноценная.\n\n"
        "Сколько копий этого же макета добавить со скидкой 50%?\n"
        "0 — без копий."
        f"{extra}"
    )


def _people_more_text(d: dict) -> str:
    people = d.get("people") if isinstance(d.get("people"), list) else []
    if _is_site_attach(d):
        return (
            f"{_card_header(d)}\n\n"
            f"Люди на фото:\n{_fmt_people(people)}\n\n"
            "Если есть ещё человек на фото — добавьте.\n"
            "Иначе переходите к копиям этой карточки."
        )
    return (
        f"Люди на фото:\n{_fmt_people(people)}\n\n"
        "Если есть ещё человек на фото — добавьте.\n"
        "Иначе переходите к количеству карточек."
    )


def _kb_after_person(d: Optional[dict] = None) -> InlineKeyboardMarkup:
    if _is_site_attach(d):
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("➕ Ещё человек на фото", callback_data="cc:NP")],
                [InlineKeyboardButton("➡️ К копиям этой карточки", callback_data="cc:QT")],
                [InlineKeyboardButton("↩️ Исправить выбор", callback_data="cc:UB")],
            ]
        )
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Ещё человек на фото", callback_data="cc:NP")],
            [InlineKeyboardButton("➡️ К количеству карточек", callback_data="cc:QT")],
            [InlineKeyboardButton("↩️ Исправить выбор", callback_data="cc:UB")],
        ]
    )


def _kb_copies(d: Optional[dict] = None) -> InlineKeyboardMarkup:
    n = _copies_from_draft(d or {})
    row1 = [
        InlineKeyboardButton(str(i), callback_data=f"cc:Y:{i}") for i in (0, 1, 2, 3)
    ]
    rows: List[List[InlineKeyboardButton]] = [row1]
    extra: List[InlineKeyboardButton] = [
        InlineKeyboardButton("Другое число", callback_data="cc:YO"),
    ]
    if n > 3:
        extra.insert(
            0, InlineKeyboardButton(f"Оставить {n}", callback_data=f"cc:Y:{n}")
        )
    rows.append(extra)
    rows.append([InlineKeyboardButton("↩️ Назад", callback_data="cc:UB")])
    return InlineKeyboardMarkup(rows)


def _kb_qty() -> InlineKeyboardMarkup:
    row1 = [
        InlineKeyboardButton(str(n), callback_data=f"cc:Q:{n}") for n in (1, 2, 3)
    ]
    row2 = [
        InlineKeyboardButton(str(n), callback_data=f"cc:Q:{n}") for n in (4, 5, 6)
    ]
    return InlineKeyboardMarkup(
        [
            row1,
            row2,
            [InlineKeyboardButton("Другое число", callback_data="cc:QO")],
            [InlineKeyboardButton("↩️ Назад", callback_data="cc:UB")],
        ]
    )


def _kb_delivery() -> InlineKeyboardMarkup:
    B = _b()
    rows = []
    for code, (label, _amt, _cur) in B.DELIVERY_OPTIONS.items():
        rows.append(
            [InlineKeyboardButton(str(label), callback_data=f"cc:D:{code}")]
        )
    rows.append([InlineKeyboardButton("↩️ Назад", callback_data="cc:QT")])
    return InlineKeyboardMarkup(rows)


def _kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Подтвердить заявку", callback_data="cc:OK")],
            [InlineKeyboardButton("✏️ Исправить данные", callback_data="cc:ED")],
            [InlineKeyboardButton("✖️ Отмена", callback_data="cc:X")],
        ]
    )


def _kb_client_proof(oid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Одобрить", callback_data=f"cc:OKA:{int(oid)}")],
            [
                InlineKeyboardButton(
                    "Связаться с администратором",
                    callback_data=f"cc:SUP:{int(oid)}",
                )
            ],
        ]
    )


def _kb_admin_order(oid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Отправить изображение клиенту",
                    callback_data=f"cc:SND:{int(oid)}",
                )
            ]
        ]
    )


def _summary_text(d: dict) -> str:
    B = _b()
    qty = d.get("qty") or "—"
    code = str(d.get("delivery") or "")
    label = B.DELIVERY_OPTIONS.get(code, (code or "—", 0, ""))[0]
    addr = str(d.get("address") or "—").strip() or "—"
    copies = _copies_from_draft(d)
    return (
        "📋 Проверьте заявку и подтвердите её.\n\n"
        f"{_price_blurb()}\n\n"
        f"{_brief_block(d)}\n\n"
        f"Полноценных карточек: {qty}\n"
        f"Копий этого макета (−50%): {copies}\n"
        f"Доставка: {label}\n"
        f"Получатель:\n{addr[:700]}"
    )


async def _catalog_universes(context: ContextTypes.DEFAULT_TYPE) -> List[str]:
    B = _b()
    try:
        products = await B._get_products(context)
    except Exception:
        log.exception("custom_card catalog load")
        return []
    return B._category_names(products)


async def _catalog_characters(
    context: ContextTypes.DEFAULT_TYPE, universe: str
) -> List[str]:
    B = _b()
    try:
        products = await B._get_products(context)
    except Exception:
        return []
    names: List[str] = []
    seen = set()
    for p in products:
        cat = str(p.get("category") or "").strip()
        if cat != universe:
            continue
        name = str(p.get("name") or "").strip()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return names


async def begin_custom_card_flow(
    msg: Message, context: ContextTypes.DEFAULT_TYPE, uid: int
) -> None:
    B = _b()
    B.user_support_state.pop(int(uid), None)
    bucket = B.user_states.get(int(uid))
    if isinstance(bucket, dict):
        bucket.pop(SUPPORT_KEY, None)
    _set_draft(
        uid,
        {
            "step": "photo",
            "people": [],
            "photo_file_id": "",
            "qty": None,
            "delivery": "",
            "address": "",
            "universe": "",
            "char_page": 0,
            "uni_page": 0,
        },
    )
    await msg.reply_text(
        _photo_prompt({}),
        reply_markup=B.REPLY_KB,
    )


async def begin_site_custom_cards_queue(
    bot, uid: int, items: list, *, pay_oid: Optional[int] = None
) -> bool:
    """После заказа с сайта: опрос по каждой карточке на заказ отдельно.

    Обычные карточки в том же заказе не спрашиваем. Оплату всего заказа
    открываем после последней карточки на заказ.
    """
    queue = []
    regular_n = 0
    for it in items or []:
        if not isinstance(it, dict):
            continue
        if not _item_is_custom_order(it):
            regular_n += 1
            continue
        try:
            qty = int(it.get("qty") if it.get("qty") is not None else it.get("quantity") or 1)
        except (TypeError, ValueError):
            qty = 1
        try:
            copies = int(
                it.get("copyQuantity")
                if it.get("copyQuantity") is not None
                else it.get("copy_quantity") or 0
            )
        except (TypeError, ValueError):
            copies = 0
        qty = max(1, qty)
        copies = max(0, copies)
        name = str(it.get("name") or it.get("title") or "Карточка на заказ")[:200]
        ref = str(it.get("ref") or it.get("id") or "")[:120]
        slots = [0] * qty
        for i in range(copies):
            slots[i % qty] += 1
        for copies_on in slots:
            queue.append(
                {
                    "ref": ref,
                    "name": name,
                    "qty": 1,
                    "copies": copies_on,
                }
            )
    if not queue:
        return False
    B = _b()
    B.user_support_state.pop(int(uid), None)
    bucket = B.user_states.get(int(uid))
    if isinstance(bucket, dict):
        bucket.pop(SUPPORT_KEY, None)
    draft = {
        "mode": "site_order_attach",
        "queue": queue,
        "queue_i": 0,
        "step": "photo",
        "people": [],
        "photo_file_id": "",
        "qty": queue[0].get("qty"),
        "copies": queue[0].get("copies"),
        "delivery": "",
        "address": "",
        "universe": "",
        "char_page": 0,
        "uni_page": 0,
        "has_regular": regular_n > 0,
    }
    if pay_oid is not None:
        try:
            draft["pay_oid"] = int(pay_oid)
        except (TypeError, ValueError):
            pass
    _set_draft(uid, draft)
    mixed = regular_n > 0
    if mixed:
        intro = (
            "В заказе обычные карточки и карточки на заказ.\n"
            "Сначала уточним каждую карточку на заказ (фото и персонажи).\n"
        )
        if len(queue) > 1:
            intro += "Карточек на заказ несколько — спросим про каждую отдельно.\n"
    elif len(queue) > 1:
        intro = (
            "В заказе несколько карточек на заказ. Спросим про каждую отдельно.\n"
            "Данные доставки уже в заказе с сайта — повторно не спрашиваем.\n"
            "Оплату откроем после данных по всем карточкам.\n\n"
        )
    else:
        intro = (
            "Нужно уточнить данные по карточке на заказ.\n"
            "Данные доставки уже в заказе с сайта — повторно не спрашиваем.\n"
            "Оплату откроем после фото и персонажей.\n\n"
        )
    await bot.send_message(
        chat_id=int(uid),
        text=intro + _photo_prompt(draft),
        reply_markup=B.REPLY_KB,
    )
    return True


def is_custom_card_start(first: str, joined: str) -> bool:
    """True для ?start=custom_card_order и совместимого ?start=custom_card.

    Не совпадает с order_<id>, web_login, login и support.
    """
    fl = (first or "").strip().lower()
    jl = (joined or "").strip().lower()
    if fl in START_PAYLOADS or jl in START_PAYLOADS:
        return True
    return fl.startswith(PRIMARY_START_PAYLOAD + "_") or fl.startswith(
        LEGACY_START_PAYLOAD + "_"
    )


def _still_image_message(msg: Optional[Message]) -> bool:
    if not msg:
        return False
    if msg.photo:
        return True
    doc = msg.document
    if not doc:
        return False
    mime = str(doc.mime_type or "").lower()
    name = str(doc.file_name or "").lower()
    if mime.startswith("image/"):
        return True
    return any(
        name.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".webp", ".heic", ".gif")
    )


def _other_flow_needs_this_media(uid: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Не забирать скрин оплаты, адрес после оплаты и сообщения из «Связи»."""
    B = _b()
    if B.user_support_state.get(int(uid)):
        return True
    ud = context.user_data if isinstance(getattr(context, "user_data", None), dict) else {}
    pp = B._user_state_get(int(uid), "postpaid_thread_oid")
    if pp is None:
        pp = ud.get("postpaid_thread_oid")
    if pp is not None:
        try:
            if B._postpaid_shipping_collect_active(int(uid), int(pp)):
                return True
        except Exception:
            return True
    raw = None
    for key in ("awaiting_proof", "awaiting_payment_order_id"):
        raw = B._user_state_get(int(uid), key)
        if raw is None:
            raw = ud.get(key)
        if raw is not None:
            break
    if raw is None:
        return False
    try:
        oid = int(raw)
    except (TypeError, ValueError):
        return False
    order = B.ORDERS.get(oid)
    if not isinstance(order, dict):
        return False
    if order.get("paid") or order.get("payment_proof_submitted"):
        return False
    if int(order.get("user_id") or 0) not in (0, int(uid)):
        return False
    pending = str(
        order.get("payment_pending_method") or ud.get("payment_pending_method") or ""
    ).strip()
    if pending:
        return True
    dl = B._user_state_get(int(uid), "proof_deadline")
    try:
        return dl is not None and int(dl) > int(time.time())
    except (TypeError, ValueError):
        return False


async def try_handle_custom_card_photo(
    update: Update, context: ContextTypes.DEFAULT_TYPE, uid: int, file_id: str
) -> bool:
    B = _b()
    if B.is_admin(uid):
        send_oid = B.user_states.get(int(uid), {}).get(SEND_KEY)
        if send_oid is not None:
            msg = update.effective_message
            if msg and not _still_image_message(msg):
                await msg.reply_text(
                    "Нужно изображение макета: отправьте его как фото, не как видео или файл другого типа."
                )
                return True
            ok = await _admin_send_proof(context, uid, int(send_oid), file_id)
            if not ok and msg:
                await msg.reply_text(
                    "Не удалось отправить изображение. Пришлите его ещё раз как фото."
                )
            return True

    if _other_flow_needs_this_media(uid, context):
        return False

    d = _peek_draft(uid)
    if not d:
        return False
    step = str(d.get("step") or "")
    msg = update.effective_message
    if step and step != "photo":
        if msg:
            await msg.reply_text(
                "Сейчас нужен ответ текстом или кнопками, а не новое фото. "
                "Если хотите заменить фото — нажмите «Исправить данные» "
                "или откройте ссылку с сайта заново."
            )
        return True
    if step != "photo":
        return False
    if msg and not _still_image_message(msg):
        await msg.reply_text(
            "Нужна фотография. Пришлите её как обычное фото, не файлом-документом.",
            reply_markup=B.REPLY_KB,
        )
        return True
    d["photo_file_id"] = file_id
    d["step"] = "universe"
    d["people"] = []
    d["universe"] = ""
    _set_draft(uid, d)
    await update.effective_message.reply_text(
        _universe_prompt(1, saved_photo=True),
        reply_markup=_kb_cancel(),
    )
    return True


def _cb_index(data: str) -> Optional[int]:
    raw = data.split(":", 1)[1] if ":" in data else ""
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def _message_has_media(msg: Optional[Message]) -> bool:
    if not msg:
        return False
    return bool(msg.photo or msg.document or msg.video or msg.animation)


async def _present(q, text: str, reply_markup=None) -> None:
    """Текст с кнопками: правка текстового сообщения или новое сообщение под фото."""
    msg = q.message if q else None
    if _message_has_media(msg):
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        if msg:
            await msg.reply_text(text, reply_markup=reply_markup)
        return
    try:
        await q.edit_message_text(text, reply_markup=reply_markup)
    except Exception:
        log.exception("custom_card edit prompt")
        if msg:
            await msg.reply_text(text, reply_markup=reply_markup)


async def _ask_characters(q, d: dict, uid: int) -> None:
    d["step"] = "character"
    _set_draft(uid, d)
    await _present(q, _character_prompt(str(d.get("universe") or "")), _kb_cancel())


async def _ask_characters_text(target, uid: int, d: dict) -> None:
    d["step"] = "character"
    _set_draft(uid, d)
    await target.reply_text(
        _character_prompt(str(d.get("universe") or "")),
        reply_markup=_kb_cancel(),
    )


async def _ask_qty(q_or_msg, uid: int, d: dict) -> None:
    d["step"] = "qty"
    _set_draft(uid, d)
    text = (
        "Сколько полноценных карточек этого макета?\n"
        "Каждая будет отдельной карточкой в заказе (Карточка 1, 2, 3…).\n\n"
        f"{_price_blurb()}"
    )
    markup = _kb_qty()
    if hasattr(q_or_msg, "edit_message_text") and not _message_has_media(
        getattr(q_or_msg, "message", None)
    ):
        await _present(q_or_msg, text, markup)
    else:
        await q_or_msg.reply_text(text, reply_markup=markup)


async def _ask_copies(q_or_msg, uid: int, d: dict) -> None:
    d["step"] = "copies"
    _set_draft(uid, d)
    text = _copies_prompt(d)
    markup = _kb_copies(d)
    if hasattr(q_or_msg, "edit_message_text") and not _message_has_media(
        getattr(q_or_msg, "message", None)
    ):
        await _present(q_or_msg, text, markup)
    else:
        await q_or_msg.reply_text(text, reply_markup=markup)


async def on_custom_card_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await _on_custom_card_callback(update, context)
    except ApplicationHandlerStop:
        raise
    except Exception:
        log.exception("custom_card callback")
        q = update.callback_query
        if q:
            try:
                await q.answer()
            except Exception:
                pass
        if q and q.message:
            try:
                await q.message.reply_text(
                    "Не удалось обработать кнопку. Откройте ссылку заказа с сайта ещё раз."
                )
            except Exception:
                pass
    raise ApplicationHandlerStop


async def _on_custom_card_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user or not q.data:
        return
    uid = int(q.from_user.id)
    data = q.data[3:]
    B = _b()

    if data.startswith("SND:"):
        if not B.is_admin(uid):
            await q.answer("Недостаточно прав.", show_alert=True)
            return
        oid = _parse_tail_int(data, "SND:")
        if oid is None:
            await q.answer("Некорректная заявка.", show_alert=True)
            return
        order = _order(oid)
        if not order:
            await q.answer("Заявка не найдена.", show_alert=True)
            return
        await q.answer()
        bucket = B.user_states.setdefault(int(uid), {})
        bucket[SEND_KEY] = oid
        B.save_state()
        await q.message.reply_text(
            f"Пришлите готовое изображение для заказа IC-{oid} следующим сообщением."
        )
        return

    if data.startswith("OKA:"):
        oid = _parse_tail_int(data, "OKA:")
        if oid is None:
            await q.answer("Некорректная заявка.", show_alert=True)
            return
        await _client_approve(q, context, uid, oid)
        return
    if data.startswith("SUP:"):
        oid = _parse_tail_int(data, "SUP:")
        if oid is None:
            await q.answer("Некорректная заявка.", show_alert=True)
            return
        await _client_support(q, context, uid, oid)
        return

    d = _peek_draft(uid) or {}
    if not d.get("step") and not data.startswith(("OKA", "SUP", "SND")):
        await q.answer("Сессия оформления истекла. Откройте ссылку с сайта снова.")
        return

    if data == "OK":
        if (
            not d.get("photo_file_id")
            or not d.get("people")
            or not d.get("qty")
            or not str(d.get("delivery") or "").strip()
            or not str(d.get("address") or "").strip()
        ):
            await q.answer("Заполните все шаги.", show_alert=True)
            return
        await q.answer()
        await _confirm_order(q, context, uid, d)
        return

    await q.answer()

    if data == "X":
        _clear_draft(uid)
        await _present(q, "Оформление карточки на заказ отменено.")
        return
    if data == "ED":
        d["step"] = "universe"
        d["people"] = []
        d["qty"] = None
        d["delivery"] = ""
        d["address"] = ""
        _set_draft(uid, d)
        _set_draft(uid, d)
        await _present(q, _universe_prompt(1), _kb_cancel())
        return
    if data == "UT":
        d["step"] = "universe_text"
        _set_draft(uid, d)
        await _present(
            q,
            "Напишите вселенную текстом (Marvel, DC, аниме и т.д.).",
        )
        return
    if data == "CT":
        d["step"] = "character_text"
        _set_draft(uid, d)
        await _present(q, "Напишите персонажа текстом.")
        return
    if data.startswith("UP:"):
        page = _cb_index(data)
        if page is None:
            return
        names = d.get("universes") or await _catalog_universes(context)
        await q.edit_message_reply_markup(reply_markup=_kb_universes(names, page))
        return
    if data.startswith("U:"):
        idx = _cb_index(data)
        names = d.get("universes") or await _catalog_universes(context)
        if idx is None or idx >= len(names):
            return
        d["universe"] = names[idx]
        _set_draft(uid, d)
        await _ask_characters(q, d, uid)
        return
    if data == "UB":
        if str(d.get("step") or "") == "people_more" and d.get("people"):
            d["people"] = list(d.get("people") or [])[:-1]
            _set_draft(uid, d)
        if str(d.get("step") or "") in ("qty", "copies", "copies_text") and d.get("people"):
            d["step"] = "people_more"
            _set_draft(uid, d)
            await _present(q, _people_more_text(d), _kb_after_person(d))
            return
        d["step"] = "universe"
        n = len(d.get("people") or []) + 1
        await _present(q, _universe_prompt(n), _kb_cancel())
        return
    if data.startswith("CP:"):
        page = _cb_index(data)
        if page is None:
            return
        names = d.get("characters") or []
        await q.edit_message_reply_markup(reply_markup=_kb_characters(names, page))
        return
    if data.startswith("C:"):
        idx = _cb_index(data)
        names = d.get("characters") or []
        if idx is None or idx >= len(names):
            return
        people = list(d.get("people") or [])
        people.append(
            {
                "universe": str(d.get("universe") or ""),
                "character": names[idx],
            }
        )
        d["people"] = people
        d["step"] = "people_more"
        _set_draft(uid, d)
        await _present(q, _people_more_text(d), _kb_after_person(d))
        return
    if data == "NP":
        d["step"] = "universe"
        d["universe"] = ""
        n = len(d.get("people") or []) + 1
        await _present(q, _universe_prompt(n), _kb_cancel())
        return
    if data == "QT":
        if not d.get("people"):
            await _present(q, "Сначала укажите хотя бы одного персонажа.")
            return
        if _is_site_attach(d):
            await _ask_copies(q, uid, d)
            return
        await _ask_qty(q, uid, d)
        return
    if data == "QO":
        d["step"] = "qty_text"
        _set_draft(uid, d)
        await _present(
            q,
            "Напишите числом, сколько полноценных карточек нужно (от 1 до 99).",
        )
        return
    if data == "YO":
        d["step"] = "copies_text"
        _set_draft(uid, d)
        await _present(
            q,
            "Напишите числом, сколько копий этого макета добавить (0–99).",
        )
        return
    if data.startswith("Y:"):
        copies = _cb_index(data)
        if copies is None or copies < 0 or copies > 99:
            return
        d["copies"] = copies
        it = _current_queue_item(d)
        if isinstance(it, dict):
            it["copies"] = copies
        _set_draft(uid, d)
        if _is_site_attach(d):
            await _commit_site_card_and_next(q, context, uid, d)
            return
        d["step"] = "delivery"
        _set_draft(uid, d)
        await _present(
            q,
            "Выберите способ доставки.",
            _kb_delivery(),
        )
        return
    if data.startswith("Q:"):
        qty = _cb_index(data)
        if qty is None or qty < 1 or qty > 99:
            return
        d["qty"] = qty
        await _ask_copies(q, uid, d)
        return
    if data.startswith("D:"):
        code = data.split(":", 1)[1]
        if code not in B.DELIVERY_OPTIONS:
            return
        d["delivery"] = code
        d["step"] = "address"
        _set_draft(uid, d)
        await _present(q, _delivery_prompt(code))
        return


async def _commit_site_card_and_next(q, context, uid: int, d: dict) -> None:
    B = _b()
    item = _current_queue_item(d) or {}
    oid = _next_cc_id()
    uname = ""
    full_name = ""
    from_user = getattr(q, "from_user", None)
    if from_user:
        if from_user.username:
            uname = str(from_user.username)
        full_name = " ".join(
            part
            for part in (
                str(from_user.first_name or "").strip(),
                str(from_user.last_name or "").strip(),
            )
            if part
        )
    try:
        qty = int(item.get("qty") or d.get("qty") or 1)
    except (TypeError, ValueError):
        qty = 1
    try:
        copies = int(item.get("copies") or d.get("copies") or 0)
    except (TypeError, ValueError):
        copies = 0
    catalog_name = str(item.get("name") or "")[:200]
    order = {
        "id": oid,
        "kind": "custom_card",
        "user_id": int(uid),
        "username": uname,
        "full_name": full_name,
        "photo_file_id": d.get("photo_file_id"),
        "people": list(d.get("people") or []),
        "idea": str(d.get("idea") or "")[:1500],
        "qty": qty,
        "copies": copies,
        "catalog_ref": str(item.get("ref") or "")[:120],
        "catalog_name": catalog_name,
        "attached_to_site": True,
        "delivery": d.get("delivery") or "",
        "address": d.get("address") or "",
        "status": "new",
        "created_at": _now_iso(),
        "history": [{"at": _now_iso(), "event": "created_site_attach"}],
        "approved_at": None,
    }
    B.CUSTOM_CARD_ORDERS[int(oid)] = order
    B.save_state()
    who = f"id {uid}"
    if full_name:
        who += f" {full_name}"
    if uname:
        who += f" @{uname}"
    cap = (
        f"🆕 Карточка на заказ (с сайта)\n"
        f"Номер заявки: IC-{oid}\n"
        f"Макет: {catalog_name or '—'}\n"
        f"📅 {order['created_at']}\n"
        f"👤 {who}\n\n"
        f"{_brief_block(order)}\n"
        f"Полноценных: {qty}, копий (−50%): {copies}"
    )
    await B._broadcast_admin_message(
        context,
        photo_file_id=str(order.get("photo_file_id") or "") or None,
        caption=cap,
        reply_markup=_kb_admin_order(oid),
        log_tag="custom_card_site_attach",
    )
    queue = d.get("queue") if isinstance(d.get("queue"), list) else []
    try:
        i = int(d.get("queue_i") or 0) + 1
    except (TypeError, ValueError):
        i = 1
    saved = (
        f"✅ Карточка {_queue_index(d) + 1} сохранена (IC-{oid}).\n"
        f"Макет: {catalog_name or '—'}\n"
        f"{_brief_block(order)}\n"
        f"Копии: {copies}"
    )
    async def _out(text: str) -> None:
        if hasattr(q, "edit_message_text"):
            await _present(q, text)
        else:
            await q.reply_text(text)

    if i >= len(queue):
        pay_oid = d.get("pay_oid")
        mixed = bool(d.get("has_regular"))
        _clear_draft(uid)
        if mixed:
            done = (
                f"{saved}\n\n"
                "Данные по карточкам на заказ собраны.\n"
                "Обычные карточки уже в этом же заказе.\n"
                "Дальше оплата всего заказа одним платежом.\n"
                "Когда макеты будут готовы — пришлём сюда на согласование."
            )
        else:
            done = (
                f"{saved}\n\n"
                "Данные по всем карточкам на заказ собраны.\n"
                "Дальше оплата заказа.\n"
                "Когда макеты будут готовы — пришлём сюда на согласование."
            )
        await _out(done)
        try:
            oid = int(pay_oid) if pay_oid is not None else 0
        except (TypeError, ValueError):
            oid = 0
        if oid > 0:
            bot = getattr(context, "bot", None)
            if bot is not None:
                try:
                    await B.offer_site_checkout_payment(bot, int(uid), oid)
                except Exception:
                    log.exception(
                        "site checkout payment after custom cards uid=%s oid=%s",
                        uid,
                        oid,
                    )
        return
    nxt = queue[i] if isinstance(queue[i], dict) else {}
    d["queue_i"] = i
    d["step"] = "photo"
    d["people"] = []
    d["idea"] = ""
    d["photo_file_id"] = ""
    d["universe"] = ""
    d["qty"] = nxt.get("qty")
    d["copies"] = nxt.get("copies")
    d["char_page"] = 0
    d["uni_page"] = 0
    _set_draft(uid, d)
    await _out(
        f"{saved}\n\n{_photo_prompt(d)}",
    )


async def _confirm_order(q, context, uid: int, d: dict) -> None:
    B = _b()
    oid = _next_cc_id()
    uname = ""
    full_name = ""
    if q.from_user:
        if q.from_user.username:
            uname = str(q.from_user.username)
        full_name = " ".join(
            part
            for part in (
                str(q.from_user.first_name or "").strip(),
                str(q.from_user.last_name or "").strip(),
            )
            if part
        )
    order = {
        "id": oid,
        "kind": "custom_card",
        "user_id": int(uid),
        "username": uname,
        "full_name": full_name,
        "photo_file_id": d.get("photo_file_id"),
        "people": list(d.get("people") or []),
        "idea": str(d.get("idea") or "")[:1500],
        "qty": d.get("qty"),
        "copies": _copies_from_draft(d),
        "delivery": d.get("delivery"),
        "address": d.get("address"),
        "status": "new",
        "created_at": _now_iso(),
        "history": [{"at": _now_iso(), "event": "created"}],
        "approved_at": None,
    }
    B.CUSTOM_CARD_ORDERS[int(oid)] = order
    _clear_draft(uid)
    B.save_state()
    code = str(order.get("delivery") or "")
    label = B.DELIVERY_OPTIONS.get(code, (code, 0, ""))[0]
    who = f"id {uid}"
    if full_name:
        who += f" {full_name}"
    if uname:
        who += f" @{uname}"
    cap = (
        f"🆕 Индивидуальная карточка\n"
        f"Номер заказа: IC-{oid}\n"
        f"📅 {order['created_at']}\n"
        f"👤 {who}\n\n"
        f"{_brief_block(order)}\n"
        f"Полноценных: {order['qty']}, копий (−50%): {order.get('copies') or 0}\n"
        f"Доставка: {label}\n"
        f"Получатель:\n{str(order.get('address') or '')[:800]}"
    )
    await B._broadcast_admin_message(
        context,
        photo_file_id=str(order.get("photo_file_id") or "") or None,
        caption=cap,
        reply_markup=_kb_admin_order(oid),
        log_tag="custom_card_order",
    )
    await _present(
        q,
        f"✅ Заявка принята.\nНомер заказа: IC-{oid}\n\n"
        "Когда макеты будут готовы — пришлём сюда на согласование. "
        "Можно будет одобрить или написать администратору по этому номеру.",
    )


async def _admin_send_proof(
    context: ContextTypes.DEFAULT_TYPE, admin_uid: int, oid: int, file_id: str
) -> bool:
    B = _b()
    if not B.is_admin(admin_uid):
        return False
    order = _order(oid)
    if not order:
        return False
    client_id = int(order.get("user_id") or 0)
    if not client_id:
        return False
    caption = (
        f"Макет по заказу IC-{oid} готов.\n\n"
        "Если всё верно — нажмите «Одобрить». "
        "Если нужны правки — «Связаться с администратором»."
    )
    markup = _kb_client_proof(oid)
    sent = False
    try:
        await context.bot.send_photo(
            client_id,
            file_id,
            caption=caption,
            reply_markup=markup,
        )
        sent = True
    except Exception:
        log.exception("custom_card send photo oid=%s client=%s", oid, client_id)
    if not sent:
        try:
            await context.bot.send_document(
                client_id,
                file_id,
                caption=caption,
                reply_markup=markup,
            )
            sent = True
        except Exception:
            log.exception("custom_card send document oid=%s client=%s", oid, client_id)
            return False
    bucket = B.user_states.get(int(admin_uid))
    if isinstance(bucket, dict):
        bucket.pop(SEND_KEY, None)
    order["proof_file_id"] = file_id
    order["status"] = "proof_sent"
    _append_history(order, "proof_sent")
    B.save_state()
    try:
        await context.bot.send_message(
            admin_uid,
            f"Изображение заказа IC-{oid} отправлено клиенту id {client_id}.",
        )
    except Exception:
        log.exception("custom_card admin ack oid=%s", oid)
    return True


async def _client_approve(q, context, uid: int, oid: int) -> None:
    B = _b()
    order = _order(oid)
    if not order or int(order.get("user_id") or 0) != int(uid):
        await q.answer("Это не ваша заявка.", show_alert=True)
        return
    if order.get("status") == "approved":
        await q.answer("Уже одобрено.")
        return
    await q.answer()
    order["status"] = "approved"
    order["approved_at"] = _now_iso()
    _append_history(order, "approved")
    B.save_state()
    uname = str(order.get("username") or "")
    await B._broadcast_admin_message(
        context,
        text=(
            f"✅ Клиент одобрил изображение заказа IC-{oid}\n"
            f"👤 id {uid}" + (f" @{uname}" if uname else "")
        ),
        log_tag="custom_card_approved",
    )
    try:
        await q.edit_message_caption(
            caption=f"Заказ IC-{oid} одобрен. Спасибо!",
            reply_markup=None,
        )
    except Exception:
        await q.message.reply_text(f"Заказ IC-{oid} одобрен. Спасибо!")


async def _client_support(q, context, uid: int, oid: int) -> None:
    B = _b()
    order = _order(oid)
    if not order or int(order.get("user_id") or 0) != int(uid):
        await q.answer("Это не ваша заявка.", show_alert=True)
        return
    await q.answer()
    bucket = B.user_states.setdefault(int(uid), {})
    bucket[SUPPORT_KEY] = int(oid)
    B.save_state()
    _append_history(order, "support_opened")
    B.save_state()
    msg = q.message
    await B._activate_user_support_chat(msg, context, uid, reply_markup=B.REPLY_KB)
    await msg.reply_text(
        f"Напишите сообщение по заказу IC-{oid}. "
        "Администратор получит его вместе с этим номером."
    )


_TEXT_STEPS = frozenset(
    {
        "photo",
        "universe",
        "universe_text",
        "character",
        "character_text",
        "people_more",
        "qty",
        "qty_text",
        "copies",
        "copies_text",
        "delivery",
        "address",
        "confirm",
    }
)


def _is_exact_menu_text(text: str) -> bool:
    B = _b()
    return str(text or "").strip() in B.REPLY_MENU_TEXTS


async def _save_person_and_ask_more(
    msg: Message, uid: int, d: dict, characters: List[str]
) -> None:
    people = list(d.get("people") or [])
    universe = str(d.get("universe") or "")
    for character in characters:
        name = str(character or "").strip()[:80]
        if not name:
            continue
        people.append({"universe": universe, "character": name})
    d["people"] = people
    d["step"] = "people_more"
    _set_draft(uid, d)
    await msg.reply_text(
        _people_more_text(d),
        reply_markup=_kb_after_person(d),
    )


async def _save_qty_and_ask_delivery(msg: Message, uid: int, d: dict, qty: int) -> None:
    d["qty"] = qty
    d["step"] = "delivery"
    _set_draft(uid, d)
    await msg.reply_text(
        "Выберите способ доставки.",
        reply_markup=_kb_delivery(),
    )


async def try_handle_custom_card_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    msg = update.effective_message
    if not msg or not msg.from_user or not msg.text:
        return False
    uid = int(msg.from_user.id)
    B = _b()
    if B.is_admin(uid):
        return False
    d = _peek_draft(uid)
    if not d:
        return False
    step = str(d.get("step") or "")
    text = msg.text.strip()
    if not text or step not in _TEXT_STEPS:
        return False
    if _is_exact_menu_text(text):
        return False
    if step in ("qty", "qty_text"):
        try:
            n = int(text.replace(" ", ""))
        except ValueError:
            await msg.reply_text(
                "Нужно целое число, например 2. Или выберите количество кнопкой."
                if step == "qty"
                else "Нужно целое число, например 2."
            )
            return True
        if n < 1 or n > 99:
            await msg.reply_text("Укажите количество полноценных карточек от 1 до 99.")
            return True
        d["qty"] = n
        await _ask_copies(msg, uid, d)
        return True
    if step in ("copies", "copies_text"):
        try:
            n = int(text.replace(" ", ""))
        except ValueError:
            await msg.reply_text("Нужно целое число, например 0 или 1.")
            return True
        if n < 0 or n > 99:
            await msg.reply_text("Укажите число копий от 0 до 99.")
            return True
        d["copies"] = n
        it = _current_queue_item(d)
        if isinstance(it, dict):
            it["copies"] = n
        _set_draft(uid, d)
        if _is_site_attach(d):
            await _commit_site_card_and_next(msg, context, uid, d)
            return True
        await _save_qty_and_ask_delivery(msg, uid, d, int(d.get("qty") or 1))
        return True
    if step == "address":
        if len(text) < 5:
            await msg.reply_text(
                "Напишите адрес, ФИО и телефон одним сообщением."
            )
            return True
        d["address"] = text[:1500]
        d["step"] = "confirm"
        _set_draft(uid, d)
        cap = _summary_text(d)
        fid = str(d.get("photo_file_id") or "")
        if fid:
            await msg.reply_photo(
                fid, caption=cap[:1024], reply_markup=_kb_confirm()
            )
        else:
            await msg.reply_text(cap, reply_markup=_kb_confirm())
        return True
    if step in ("universe", "universe_text"):
        # Первый ответ после фото — вселенная и персонажи или идея целиком.
        if not str(d.get("universe") or "").strip() and not d.get("people"):
            idea = text[:1500]
            d["idea"] = idea
            d["universe"] = idea[:80]
            d["people"] = [{"universe": "Идея", "character": idea[:300]}]
            _set_draft(uid, d)
            if _is_site_attach(d):
                await _ask_copies(msg, uid, d)
            else:
                await _ask_qty(msg, uid, d)
            return True
        d["universe"] = text[:80]
        _set_draft(uid, d)
        await _ask_characters_text(msg, uid, d)
        return True
    if step in ("character", "character_text"):
        names = _split_character_names(text)
        if not names:
            await msg.reply_text("Напишите имя персонажа текстом.")
            return True
        await _save_person_and_ask_more(msg, uid, d, names)
        return True
    if step == "photo":
        await msg.reply_text(
            "Пришлите фото как обычное фото, не текстом.",
            reply_markup=B.REPLY_KB,
        )
        return True
    await msg.reply_text("Используйте кнопки под предыдущим сообщением.")
    return True


class _CustomCardTextFilter(filters.MessageFilter):
    def filter(self, message) -> bool:
        if not message or not getattr(message, "text", None) or not message.from_user:
            return False
        if _is_exact_menu_text(message.text):
            return False
        uid = int(message.from_user.id)
        B = _b()
        if B.is_admin(uid):
            return False
        d = (B.user_states.get(uid) or {}).get(DRAFT_KEY)
        if not isinstance(d, dict):
            return False
        return str(d.get("step") or "") in _TEXT_STEPS


CUSTOM_CARD_TEXT_FILTER = _CustomCardTextFilter()


def support_order_suffix(uid: int) -> str:
    B = _b()
    oid = (B.user_states.get(int(uid)) or {}).get(SUPPORT_KEY)
    if oid is None:
        return ""
    return f"\n📦 Номер заказа: IC-{int(oid)}"


def register_custom_card_handlers(app) -> None:
    app.add_handler(
        CallbackQueryHandler(on_custom_card_callback, pattern=_RE_CB),
        group=-1,
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & CUSTOM_CARD_TEXT_FILTER,
            _on_cc_text,
        ),
        group=-2,
    )


async def _on_cc_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    handled = await try_handle_custom_card_text(update, context)
    if handled:
        raise ApplicationHandlerStop
