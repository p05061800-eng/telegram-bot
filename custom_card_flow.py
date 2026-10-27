"""Оформление индивидуальной карточки: /start custom_card и согласование макета."""

from __future__ import annotations

import logging
import re
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

START_PAYLOAD = "custom_card"
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
        [
            InlineKeyboardButton("↩️ Вселенная", callback_data="cc:UB"),
            InlineKeyboardButton("✖️ Отмена", callback_data="cc:X"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _kb_after_person() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Ещё человек на фото", callback_data="cc:NP")],
            [InlineKeyboardButton("➡️ К количеству карточек", callback_data="cc:QT")],
            [InlineKeyboardButton("↩️ Исправить выбор", callback_data="cc:UB")],
        ]
    )


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
                    "Отправить макет клиенту",
                    callback_data=f"cc:SND:{int(oid)}",
                )
            ]
        ]
    )


def _summary_text(d: dict) -> str:
    B = _b()
    people = d.get("people") if isinstance(d.get("people"), list) else []
    qty = d.get("qty") or "—"
    code = str(d.get("delivery") or "")
    label = B.DELIVERY_OPTIONS.get(code, (code or "—", 0, ""))[0]
    addr = str(d.get("address") or "—").strip() or "—"
    return (
        "📋 Заявка на индивидуальную карточку\n\n"
        f"Персонажи:\n{_fmt_people(people)}\n\n"
        f"Количество карточек: {qty}\n"
        f"Доставка: {label}\n"
        f"Получатель:\n{addr[:1200]}"
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
        "✨ Оформление индивидуальной карточки\n\n"
        "Шаг 1. Пришлите фотографию, по которой нужно создать карточку "
        "(как обычное фото, не файл-документ).",
        reply_markup=B.REPLY_KB,
    )


def is_custom_card_start(first: str, joined: str) -> bool:
    fl = (first or "").strip().lower()
    jl = (joined or "").strip().lower()
    return fl == START_PAYLOAD or jl == START_PAYLOAD or fl.startswith(
        START_PAYLOAD + "_"
    )


async def try_handle_custom_card_photo(
    update: Update, context: ContextTypes.DEFAULT_TYPE, uid: int, file_id: str
) -> bool:
    B = _b()
    if B.is_admin(uid):
        send_oid = B.user_states.get(int(uid), {}).get(SEND_KEY)
        if send_oid is not None:
            ok = await _admin_send_proof(context, uid, int(send_oid), file_id)
            if not ok and update.effective_message:
                await update.effective_message.reply_text(
                    "Не удалось отправить макет. Пришлите изображение ещё раз."
                )
            return True

    d = _peek_draft(uid)
    if not d:
        return False
    step = str(d.get("step") or "")
    if step and step != "photo":
        if update.effective_message:
            await update.effective_message.reply_text(
                "Сейчас нужен ответ текстом или кнопками, а не новое фото. "
                "Если хотите заменить фото — нажмите «Исправить данные» в сводке "
                "или откройте ссылку с сайта заново."
            )
        return True
    if step != "photo":
        return False
    d["photo_file_id"] = file_id
    d["step"] = "universe"
    d["people"] = []
    _set_draft(uid, d)
    names = await _catalog_universes(context)
    d["universes"] = names
    _set_draft(uid, d)
    await update.effective_message.reply_text(
        "Фото сохранено.\n\nШаг 2. Выберите вселенную для 1-го человека на фото."
        + (
            ""
            if names
            else "\n\nКаталог сейчас недоступен — напишите вселенную текстом."
        ),
        reply_markup=_kb_universes(names) if names else None,
    )
    return True


async def _ask_characters(q, context, d: dict, uid: int) -> None:
    uni = str(d.get("universe") or "")
    names = await _catalog_characters(context, uni)
    d["characters"] = names
    d["char_page"] = 0
    d["step"] = "character"
    _set_draft(uid, d)
    text = (
        f"Вселенная: {uni}\n\nВыберите персонажа."
        if names
        else f"Вселенная: {uni}\n\nНапишите имя персонажа текстом."
    )
    await q.edit_message_text(
        text, reply_markup=_kb_characters(names) if names else None
    )


async def _ask_qty(q_or_msg, uid: int, d: dict) -> None:
    d["step"] = "qty"
    _set_draft(uid, d)
    text = "Шаг 3. Сколько карточек нужно?"
    markup = _kb_qty()
    if hasattr(q_or_msg, "edit_message_text"):
        await q_or_msg.edit_message_text(text, reply_markup=markup)
    else:
        await q_or_msg.reply_text(text, reply_markup=markup)


async def on_custom_card_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _on_custom_card_callback(update, context)
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
            f"Пришлите изображение макета для заявки IC-{oid} следующим сообщением."
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

    await q.answer()

    if data == "X":
        _clear_draft(uid)
        await q.edit_message_text("Оформление индивидуальной карточки отменено.")
        return
    if data == "ED":
        d["step"] = "universe"
        d["people"] = []
        d["qty"] = None
        d["delivery"] = ""
        d["address"] = ""
        _set_draft(uid, d)
        names = d.get("universes") or await _catalog_universes(context)
        d["universes"] = names
        _set_draft(uid, d)
        await q.edit_message_text(
            "Выберите вселенную заново.",
            reply_markup=_kb_universes(names) if names else None,
        )
        return
    if data.startswith("UP:"):
        page = int(data.split(":", 1)[1])
        names = d.get("universes") or await _catalog_universes(context)
        await q.edit_message_reply_markup(reply_markup=_kb_universes(names, page))
        return
    if data.startswith("U:"):
        idx = int(data.split(":", 1)[1])
        names = d.get("universes") or await _catalog_universes(context)
        if idx < 0 or idx >= len(names):
            return
        d["universe"] = names[idx]
        _set_draft(uid, d)
        await _ask_characters(q, context, d, uid)
        return
    if data == "UB":
        if str(d.get("step") or "") == "people_more" and d.get("people"):
            d["people"] = list(d.get("people") or [])[:-1]
            _set_draft(uid, d)
        if str(d.get("step") or "") == "qty" and d.get("people"):
            d["step"] = "people_more"
            _set_draft(uid, d)
            await q.edit_message_text(
                f"Добавлено:\n{_fmt_people(d.get('people') or [])}\n\nЕщё человек на фото?",
                reply_markup=_kb_after_person(),
            )
            return
        d["step"] = "universe"
        names = d.get("universes") or await _catalog_universes(context)
        await q.edit_message_text(
            "Выберите вселенную.",
            reply_markup=_kb_universes(names) if names else None,
        )
        return
    if data.startswith("CP:"):
        page = int(data.split(":", 1)[1])
        names = d.get("characters") or []
        await q.edit_message_reply_markup(reply_markup=_kb_characters(names, page))
        return
    if data.startswith("C:"):
        idx = int(data.split(":", 1)[1])
        names = d.get("characters") or []
        if idx < 0 or idx >= len(names):
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
        await q.edit_message_text(
            f"Добавлено:\n{_fmt_people(people)}\n\nЕщё человек на фото?",
            reply_markup=_kb_after_person(),
        )
        return
    if data == "NP":
        d["step"] = "universe"
        d["universe"] = ""
        names = d.get("universes") or await _catalog_universes(context)
        n = len(d.get("people") or []) + 1
        await q.edit_message_text(
            f"Выберите вселенную для {n}-го человека.",
            reply_markup=_kb_universes(names) if names else None,
        )
        return
    if data == "QT":
        await _ask_qty(q, uid, d)
        return
    if data == "QO":
        d["step"] = "qty_text"
        _set_draft(uid, d)
        await q.edit_message_text("Напишите нужное количество карточек числом.")
        return
    if data.startswith("Q:"):
        qty = int(data.split(":", 1)[1])
        d["qty"] = qty
        d["step"] = "delivery"
        _set_draft(uid, d)
        await q.edit_message_text(
            "Шаг 4. Выберите способ доставки.",
            reply_markup=_kb_delivery(),
        )
        return
    if data.startswith("D:"):
        code = data.split(":", 1)[1]
        if code not in B.DELIVERY_OPTIONS:
            return
        d["delivery"] = code
        d["step"] = "address"
        _set_draft(uid, d)
        await q.edit_message_text(_delivery_prompt(code))
        return
    if data == "OK":
        await _confirm_order(q, context, uid, d)
        return


async def _confirm_order(q, context, uid: int, d: dict) -> None:
    B = _b()
    if not d.get("photo_file_id") or not d.get("people") or not d.get("qty"):
        await q.answer("Заполните все шаги.", show_alert=True)
        return
    oid = _next_cc_id()
    uname = ""
    if q.from_user and q.from_user.username:
        uname = str(q.from_user.username)
    order = {
        "id": oid,
        "kind": "custom_card",
        "user_id": int(uid),
        "username": uname,
        "photo_file_id": d.get("photo_file_id"),
        "people": list(d.get("people") or []),
        "qty": d.get("qty"),
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
    cap = (
        f"🆕 Индивидуальная карточка IC-{oid}\n"
        f"📅 {order['created_at']}\n"
        f"👤 id {uid}"
        + (f" @{uname}" if uname else "")
        + f"\n\nПерсонажи:\n{_fmt_people(order['people'])}\n"
        f"Количество: {order['qty']}\n"
        f"Доставка: {label}\n"
        f"{str(order.get('address') or '')[:800]}"
    )
    await B._broadcast_admin_message(
        context,
        photo_file_id=str(order.get("photo_file_id") or "") or None,
        caption=cap,
        reply_markup=_kb_admin_order(oid),
        log_tag="custom_card_order",
    )
    await q.edit_message_text(
        f"✅ Заявка IC-{oid} отправлена.\n\n"
        "Когда макет будет готов, мы пришлём его сюда на согласование."
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
    try:
        await context.bot.send_photo(
            client_id,
            file_id,
            caption=(
                f"Макет индивидуальной карточки IC-{oid}.\n\n"
                "Если всё верно — нажмите «Одобрить». "
                "Если нужны правки — «Связаться с администратором»."
            ),
            reply_markup=_kb_client_proof(oid),
        )
    except Exception:
        log.exception("custom_card send proof oid=%s client=%s", oid, client_id)
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
            f"Макет IC-{oid} отправлен клиенту id {client_id}.",
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
            f"✅ Клиент одобрил макет IC-{oid}\n"
            f"👤 id {uid}" + (f" @{uname}" if uname else "")
        ),
        log_tag="custom_card_approved",
    )
    try:
        await q.edit_message_caption(
            caption=f"Макет IC-{oid} одобрен. Спасибо!",
            reply_markup=None,
        )
    except Exception:
        await q.message.reply_text(f"Макет IC-{oid} одобрен. Спасибо!")


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
        f"Напишите сообщение по заявке IC-{oid}. Оно уйдёт администратору с номером заказа."
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
    if step == "qty_text":
        try:
            n = int(text.replace(" ", ""))
        except ValueError:
            await msg.reply_text("Нужно целое число, например 2.")
            return True
        if n < 1 or n > 99:
            await msg.reply_text("Укажите количество от 1 до 99.")
            return True
        d["qty"] = n
        d["step"] = "delivery"
        _set_draft(uid, d)
        await msg.reply_text(
            "Шаг 4. Выберите способ доставки.",
            reply_markup=_kb_delivery(),
        )
        return True
    if step == "address":
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
    if step == "universe" and text and not (d.get("universes") or []):
        d["universe"] = text[:80]
        d["step"] = "character"
        _set_draft(uid, d)
        await msg.reply_text("Напишите персонажа для этой вселенной.")
        return True
    if step == "character" and text and not (d.get("characters") or []):
        people = list(d.get("people") or [])
        people.append(
            {"universe": str(d.get("universe") or ""), "character": text[:80]}
        )
        d["people"] = people
        d["step"] = "people_more"
        _set_draft(uid, d)
        await msg.reply_text(
            f"Добавлено:\n{_fmt_people(people)}\n\nЕщё человек на фото?",
            reply_markup=_kb_after_person(),
        )
        return True
    return False


class _CustomCardTextFilter(filters.MessageFilter):
    def filter(self, message) -> bool:
        if not message or not getattr(message, "text", None) or not message.from_user:
            return False
        uid = int(message.from_user.id)
        B = _b()
        if B.is_admin(uid):
            return False
        d = (B.user_states.get(uid) or {}).get(DRAFT_KEY)
        if not isinstance(d, dict):
            return False
        return str(d.get("step") or "") in (
            "qty_text",
            "address",
            "universe",
            "character",
        )


CUSTOM_CARD_TEXT_FILTER = _CustomCardTextFilter()


def support_order_suffix(uid: int) -> str:
    B = _b()
    oid = (B.user_states.get(int(uid)) or {}).get(SUPPORT_KEY)
    if oid is None:
        return ""
    return f"\n📦 Индивидуальная карточка IC-{int(oid)}"


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
