"""Банковские реквизиты и порядок их показа в уже существующей оплате бота.

Криптоадрес здесь не хранится: его текст остаётся в bot.PAY_CRYPTO_BODY.
"""

from __future__ import annotations

import html
import re
from copy import deepcopy
from typing import List, Optional, Tuple

DEFAULT_REQUISITES: List[dict] = [
    {
        "id": "alfa_phone",
        "title": "Перевод по номеру телефона",
        "kind": "phone",
        "bank": "Альфа-Банк",
        "country": "Беларусь",
        "value": "+375298124337",
        "recipient": "DANIIL PARFIONAU",
        "instruction": "",
        "sort": 10,
        "active": True,
    },
    {
        "id": "alfa_card",
        "title": "Оплата картой — Альфа-Банк",
        "kind": "card",
        "bank": "Альфа-Банк",
        "country": "Беларусь",
        "value": "9112 3810 1138 6125",
        "recipient": "DANIIL PARFIONAU",
        "instruction": "",
        "sort": 20,
        "active": True,
    },
    {
        "id": "bsb_card",
        "title": "Оплата картой — БСБ Банк",
        "kind": "card",
        "bank": "БСБ Банк",
        "country": "Беларусь",
        "value": "5144 2102 0126 0883",
        "recipient": "DANIIL PARFIONAU",
        "instruction": "",
        "sort": 30,
        "active": True,
    },
    {
        "id": "prior_card",
        "title": "Оплата картой — Приорбанк",
        "kind": "card",
        "bank": "Приорбанк",
        "country": "Беларусь",
        "value": "4916 9896 7357 2128",
        "recipient": "DANIIL PARFIONAU",
        "instruction": "",
        "sort": 40,
        "active": True,
    },
]

_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,15}$")

REQUISITES: List[dict] = []
CRYPTO_ENABLED: bool = True
REQ_SEQ: int = 1
_STATE_READY = False

REASSURE = (
    "Не получилось? Не переживайте — попробуйте следующие реквизиты. "
    "Иногда перевод проходит через другой банк. Ваш заказ сохранён."
)

def crypto_enabled() -> bool:
    return bool(CRYPTO_ENABLED)


def export_state() -> dict:
    ensure_loaded()
    return {
        "payment_requisites": deepcopy(REQUISITES),
        "payment_crypto_enabled": bool(CRYPTO_ENABLED),
        "payment_req_seq": int(REQ_SEQ),
    }


def import_state(blob: Optional[dict]) -> None:
    """Подхватить реквизиты из постоянного состояния. Пустой список сохраняется."""
    global REQUISITES, CRYPTO_ENABLED, REQ_SEQ, _STATE_READY
    data = blob if isinstance(blob, dict) else {}
    if "payment_requisites" not in data:
        REQUISITES = deepcopy(DEFAULT_REQUISITES)
        CRYPTO_ENABLED = True
        REQ_SEQ = 1
        _STATE_READY = True
        return
    raw = data.get("payment_requisites")
    cleaned: List[dict] = []
    if isinstance(raw, list):
        for item in raw:
            norm = normalize_requisite(item)
            if norm:
                cleaned.append(norm)
    REQUISITES = cleaned
    CRYPTO_ENABLED = bool(data.get("payment_crypto_enabled", True))
    try:
        REQ_SEQ = max(1, int(data.get("payment_req_seq") or 1))
    except (TypeError, ValueError):
        REQ_SEQ = 1
    _STATE_READY = True


def ensure_loaded() -> None:
    if not _STATE_READY:
        import_state({})


def normalize_requisite(raw: object) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    rid = str(raw.get("id") or "").strip().lower()
    if not _ID_RE.match(rid):
        return None
    kind = str(raw.get("kind") or "").strip().lower()
    if kind not in ("phone", "card"):
        return None
    title = str(raw.get("title") or "").strip()
    bank = str(raw.get("bank") or "").strip()
    country = str(raw.get("country") or "").strip()
    recipient = str(raw.get("recipient") or "").strip()
    value = _normalize_value(kind, raw.get("value"))
    if not title or not bank or not country or not recipient or not value:
        return None
    try:
        sort = int(raw.get("sort") or 0)
    except (TypeError, ValueError):
        sort = 0
    return {
        "id": rid,
        "title": title[:80],
        "kind": kind,
        "bank": bank[:80],
        "country": country[:80],
        "value": value,
        "recipient": recipient[:80],
        "instruction": str(raw.get("instruction") or "").strip()[:500],
        "sort": sort,
        "active": bool(raw.get("active", True)),
    }


def _normalize_value(kind: str, raw: object) -> str:
    text = str(raw or "").strip()
    if kind == "card":
        digits = re.sub(r"\D", "", text)
        if len(digits) < 12 or len(digits) > 19:
            return ""
        return " ".join(digits[i : i + 4] for i in range(0, len(digits), 4))
    digits = re.sub(r"\D", "", text)
    if len(digits) < 6 or len(digits) > 15:
        return ""
    if text.startswith("+"):
        return "+" + digits
    return "+" + digits


def mask_value(kind: str, value: str) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) < 4:
        return "****"
    tail = digits[-4:]
    if kind == "card":
        return f"**** {tail}"
    return f"+***{tail}"


def log_requisite(req: dict) -> dict:
    """Поля для логов: без полного номера карты или телефона."""
    return {
        "id": req.get("id"),
        "kind": req.get("kind"),
        "bank": req.get("bank"),
        "active": bool(req.get("active", True)),
        "value": mask_value(str(req.get("kind") or ""), str(req.get("value") or "")),
    }


def sorted_requisites(reqs: Optional[List[dict]] = None) -> List[dict]:
    src = REQUISITES if reqs is None else reqs
    return sorted(
        [r for r in src if isinstance(r, dict)],
        key=lambda r: (int(r.get("sort") or 0), str(r.get("id") or "")),
    )


def active_requisites(
    reqs: Optional[List[dict]] = None, kind: Optional[str] = None
) -> List[dict]:
    out = []
    for req in sorted_requisites(reqs):
        if not req.get("active", True):
            continue
        if kind and req.get("kind") != kind:
            continue
        out.append(req)
    return out


def find_requisite(req_id: str, reqs: Optional[List[dict]] = None) -> Optional[dict]:
    want = str(req_id or "").strip()
    if not want or want == "crypto":
        return None
    for req in sorted_requisites(reqs):
        if req.get("id") == want:
            return req
    return None


def next_bank_id(current_id: str, reqs: Optional[List[dict]] = None) -> Optional[str]:
    ids = [str(r["id"]) for r in active_requisites(reqs)]
    if not ids:
        return None
    if current_id not in ids:
        return None
    idx = ids.index(current_id)
    if idx + 1 < len(ids):
        return ids[idx + 1]
    return None


def prev_bank_id(current_id: str, reqs: Optional[List[dict]] = None) -> Optional[str]:
    ids = [str(r["id"]) for r in active_requisites(reqs)]
    if current_id not in ids:
        return None
    idx = ids.index(current_id)
    if idx <= 0:
        return None
    return ids[idx - 1]


def is_last_bank(current_id: str, reqs: Optional[List[dict]] = None) -> bool:
    ids = [str(r["id"]) for r in active_requisites(reqs)]
    return bool(ids) and current_id == ids[-1]


def phone_entry_id(reqs: Optional[List[dict]] = None) -> Optional[str]:
    phones = active_requisites(reqs, "phone")
    if phones:
        return str(phones[0]["id"])
    banks = active_requisites(reqs)
    return str(banks[0]["id"]) if banks else None


def card_entry_id(
    current_id: str,
    reqs: Optional[List[dict]] = None,
    bank_req_id: str = "",
) -> Optional[str]:
    ids = [str(r["id"]) for r in active_requisites(reqs, "card")]
    if current_id in ids:
        return current_id
    if bank_req_id in ids:
        return bank_req_id
    return ids[0] if ids else None


def new_progress(order_id: int, total: int, currency: str) -> dict:
    return {
        "order_id": int(order_id),
        "total": int(total),
        "currency": str(currency or "BYN"),
        "warning_shown": False,
        "req_id": "",
        "bank_req_id": "",
        "paid": False,
        "proof": False,
        "pending_method": "",
        "req_label": "",
        "status": "new",
    }


def progress_from_order(order: dict, order_id: int, total: int, currency: str) -> dict:
    prog = new_progress(order_id, total, currency)
    if not isinstance(order, dict):
        return prog
    prog["warning_shown"] = bool(order.get("payment_warning_shown"))
    prog["req_id"] = str(order.get("payment_req_id") or "")
    prog["bank_req_id"] = str(order.get("payment_bank_req_id") or "")
    prog["paid"] = bool(order.get("paid"))
    prog["proof"] = bool(order.get("payment_proof_submitted"))
    prog["pending_method"] = str(order.get("payment_pending_method") or "")
    prog["req_label"] = str(order.get("payment_req_label") or "")
    prog["status"] = str(order.get("status") or "new")
    return prog


def apply_progress(order: dict, progress: dict) -> None:
    """Записать только шаг оплаты. Состав, суммы и id заказа не меняются."""
    if not isinstance(order, dict):
        return
    order["payment_warning_shown"] = bool(progress.get("warning_shown"))
    order["payment_req_id"] = str(progress.get("req_id") or "")
    order["payment_bank_req_id"] = str(progress.get("bank_req_id") or "")
    if progress.get("pending_method"):
        order["payment_pending_method"] = str(progress.get("pending_method"))
    if progress.get("req_label"):
        order["payment_req_label"] = str(progress.get("req_label"))


def _copy(progress: dict) -> dict:
    return dict(progress)


def _show_bank(progress: dict, req: dict) -> Tuple[dict, str]:
    prog = _copy(progress)
    prog["req_id"] = str(req["id"])
    prog["bank_req_id"] = str(req["id"])
    prog["pending_method"] = "transfer" if req.get("kind") == "phone" else "card"
    kind_ru = "телефон" if req.get("kind") == "phone" else "карта"
    prog["req_label"] = f"{req.get('bank')} · {kind_ru}"
    prog["warning_shown"] = True
    return prog, "bank"


def _show_crypto(progress: dict) -> Tuple[dict, str]:
    prog = _copy(progress)
    if prog.get("req_id") and prog.get("req_id") != "crypto":
        prog["bank_req_id"] = str(prog["req_id"])
    prog["req_id"] = "crypto"
    prog["pending_method"] = "crypto"
    prog["req_label"] = "Криптовалюта"
    prog["warning_shown"] = True
    return prog, "crypto"


def _fallback_after_banks(
    progress: dict, *, crypto_on: bool
) -> Tuple[dict, str]:
    if crypto_on:
        return _show_crypto(progress)
    prog = _copy(progress)
    prog["warning_shown"] = True
    if prog.get("req_id") and prog.get("req_id") != "crypto":
        prog["bank_req_id"] = str(prog["req_id"])
    prog["req_id"] = ""
    return prog, "support"


def advance(
    progress: dict,
    action: str,
    reqs: Optional[List[dict]] = None,
    *,
    crypto_on: Optional[bool] = None,
) -> Tuple[dict, str]:
    """Следующий экран оплаты. order_id и total не меняются, новый заказ не создаётся."""
    prog = _copy(progress)
    crypto = CRYPTO_ENABLED if crypto_on is None else bool(crypto_on)
    act = str(action or "").strip()
    if prog.get("paid"):
        return prog, "already_paid"
    # Реквизиты можно открыть снова. «Проверяем оплату» — только повторное «Я оплатил».
    if prog.get("proof") and act == "paid":
        return prog, "already_proof"
    if act in ("open_phone", "open_card", "open_crypto") and not prog.get("warning_shown"):
        prog["warning_shown"] = True
        return prog, "warning"
    if act == "to_menu":
        return prog, "menu"
    if act == "paid":
        return prog, "need_proof"
    if act == "cancel":
        if not prog.get("proof"):
            prog["status"] = "canceled"
        return prog, "cancelled"
    if act == "support":
        return prog, "support"
    if act == "warn_go":
        prog["warning_shown"] = True
        rid = phone_entry_id(reqs)
        req = find_requisite(rid or "", reqs)
        if req:
            return _show_bank(prog, req)
        return _fallback_after_banks(prog, crypto_on=crypto)
    if act == "open_phone":
        rid = phone_entry_id(reqs)
        req = find_requisite(rid or "", reqs)
        if req:
            return _show_bank(prog, req)
        return _fallback_after_banks(prog, crypto_on=crypto)
    if act == "open_card":
        rid = card_entry_id(
            str(prog.get("req_id") or ""),
            reqs,
            str(prog.get("bank_req_id") or ""),
        )
        req = find_requisite(rid or "", reqs)
        if req:
            return _show_bank(prog, req)
        return _fallback_after_banks(prog, crypto_on=crypto)
    if act == "open_crypto":
        if not crypto:
            return prog, "support"
        return _show_crypto(prog)
    if act == "next":
        current = str(prog.get("req_id") or "")
        if current and current != "crypto":
            nid = next_bank_id(current, reqs)
            if nid:
                req = find_requisite(nid, reqs)
                if req:
                    return _show_bank(prog, req)
            return _fallback_after_banks(prog, crypto_on=crypto)
        return prog, "support"
    if act == "prev":
        current = str(prog.get("req_id") or "")
        if current == "crypto" or not current:
            target = str(prog.get("bank_req_id") or "")
            if not find_requisite(target, reqs) or not _is_active(target, reqs):
                banks = active_requisites(reqs)
                target = str(banks[-1]["id"]) if banks else ""
            req = find_requisite(target, reqs)
            if req:
                return _show_bank(prog, req)
            return prog, "support"
        pid = prev_bank_id(current, reqs)
        if not pid:
            return prog, "bank"
        req = find_requisite(pid, reqs)
        if req:
            return _show_bank(prog, req)
        return prog, "bank"
    return prog, "menu"


def _is_active(req_id: str, reqs: Optional[List[dict]]) -> bool:
    req = find_requisite(req_id, reqs)
    return bool(req and req.get("active", True))


def _e(text: object) -> str:
    return html.escape(str(text or ""), quote=False)


def _amount_line(progress: dict) -> str:
    return (
        f"Оплата по заказу #{int(progress.get('order_id') or 0)}\n"
        f"Сумма заказа: {int(progress.get('total') or 0)} "
        f"{_e(progress.get('currency') or 'BYN')}"
    )


def render_warning(*, crypto_on: bool) -> str:
    lines = [
        "⚠️ <b>ВАЖНО! ПЕРЕВОД МОЖЕТ НЕ ПРОЙТИ С ПЕРВОГО РАЗА</b>",
        "",
        "При переводе из России в Беларусь некоторые банки могут отклонять операции "
        "или не поддерживать перевод на определённую карту.",
        "",
        "Не переживайте и не отменяйте заказ! У нас есть несколько альтернативных реквизитов.",
        "",
        "💳 Сначала попробуйте перевод по номеру телефона.",
        "",
        "Если не получится, последовательно попробуйте карты Альфа-Банка, БСБ Банка и Приорбанка.",
    ]
    if crypto_on:
        lines.extend(["", "₿ Также доступна оплата криптовалютой."])
    lines.extend(
        [
            "",
            "<b>Если один способ не сработал, просто нажмите кнопку перехода к следующим "
            "реквизитам. Ваш заказ и его сумма сохранятся.</b>",
        ]
    )
    return "\n".join(lines)


def _extra(req: dict) -> str:
    text = str(req.get("instruction") or "").strip()
    if not text:
        return ""
    return "\n\n" + _e(text)


def render_bank(progress: dict, req: dict) -> str:
    amount = _amount_line(progress)
    extra = _extra(req)
    if req.get("kind") == "phone":
        body = (
            "📱 <b>ОПЛАТА ПО НОМЕРУ ТЕЛЕФОНА</b>\n\n"
            f"{amount}\n\n"
            f"Банк: {_e(req.get('bank'))}, {_e(req.get('country'))}\n"
            f"Телефон: <code>{_e(req.get('value'))}</code>\n"
            f"Получатель: {_e(req.get('recipient'))}\n\n"
            "1. Откройте приложение своего банка.\n"
            "2. Выберите перевод по номеру телефона.\n"
            "3. Укажите номер и проверьте данные получателя.\n"
            "4. Переведите точную сумму заказа.\n"
            "5. После перевода вернитесь в бот и отправьте скриншот подтверждения.\n\n"
            "Не отправляйте перевод, если банк показывает другое имя получателя "
            "или данные выглядят подозрительно. Нажмите «Следующие реквизиты» "
            "или кнопку «💬 Связь».\n\n"
            f"{REASSURE}"
        )
        return body + extra
    bank_up = _e(str(req.get("bank") or "").upper())
    low = str(req.get("bank") or "").lower()
    if "бсб" in low:
        guide = (
            "Попробуйте перевести точную сумму заказа. Перед отправкой денег "
            "проверьте имя получателя и данные платежа.\n\n"
            "После перевода отправьте скриншот в бот."
        )
    elif "приор" in low:
        guide = (
            "Попробуйте выполнить перевод. Если банк не поддерживает операцию, "
            "не нужно повторять один и тот же неудачный платёж много раз.\n\n"
            "Можно перейти к криптовалюте или обратиться в поддержку.\n\n"
            "После успешного перевода отправьте скриншот в бот."
        )
    else:
        guide = (
            "Попробуйте перевести сумму заказа на указанную карту. Перед подтверждением "
            "обязательно проверьте имя получателя в банковском приложении.\n\n"
            "После оплаты вернитесь в бот и отправьте скриншот перевода."
        )
    body = (
        f"💳 <b>ОПЛАТА КАРТОЙ — {bank_up}</b>\n\n"
        f"{amount}\n\n"
        f"Страна: {_e(req.get('country'))}\n"
        f"Номер карты: <code>{_e(req.get('value'))}</code>\n"
        f"Получатель: {_e(req.get('recipient'))}\n\n"
        f"{guide}\n\n"
        f"{REASSURE}"
    )
    return body + extra


def render_crypto(progress: dict, crypto_body: str) -> str:
    return f"{_amount_line(progress)}\n\n{_e(crypto_body)}"


def render_support(progress: dict, *, crypto_on: bool) -> str:
    why = (
        "Если криптовалюта не подходит или сейчас недоступна, напишите в поддержку."
        if crypto_on
        else "Криптооплата сейчас выключена. Напишите в поддержку — мы поможем с переводом."
    )
    return (
        f"{_amount_line(progress)}\n\n"
        f"{why}\n\n"
        "Заказ и его сумма сохранены. Сообщение уйдёт администратору с номером заказа."
    )


def keyboard_for(
    screen: str,
    progress: dict,
    reqs: Optional[List[dict]] = None,
    *,
    crypto_on: Optional[bool] = None,
) -> List[List[Tuple[str, str]]]:
    crypto = CRYPTO_ENABLED if crypto_on is None else bool(crypto_on)
    if screen == "warning":
        rows: List[List[Tuple[str, str]]] = [
            [("➡️ Перейти к оплате", "warn_go")]
        ]
        if crypto:
            rows.append([("₿ Оплатить криптовалютой", "open_crypto")])
        rows.append([("❌ Отменить оплату", "cancel")])
        return rows
    if screen == "crypto":
        rows = [[("✅ Я оплатил", "paid")]]
        if active_requisites(reqs):
            rows.append([("◀️ Вернуться к банковским способам", "prev")])
        rows.append([("💬 Связаться с поддержкой", "support")])
        rows.append([("❌ Отменить оплату", "cancel")])
        return rows
    if screen == "support":
        rows = [[("💬 Связаться с поддержкой", "support")]]
        if progress.get("req_id") or progress.get("bank_req_id"):
            rows.append([("◀️ Предыдущий способ", "prev")])
        rows.append([("❌ Отменить оплату", "cancel")])
        return rows
    if screen != "bank":
        return []
    req = find_requisite(str(progress.get("req_id") or ""), reqs)
    if not req:
        return [[("❌ Отменить оплату", "cancel")]]
    last = is_last_bank(str(req["id"]), reqs)
    first = prev_bank_id(str(req["id"]), reqs) is None
    rows = [[("✅ Я оплатил", "paid")]]
    if not last:
        rows.append(
            [("❌ Не получается перевести? Следующие реквизиты", "next")]
        )
    elif crypto:
        rows.append([("₿ Перейти к оплате криптовалютой", "next")])
    if not first:
        rows.append([("◀️ Предыдущий способ", "prev")])
    if not last and crypto:
        rows.append([("₿ Оплатить криптовалютой", "open_crypto")])
    if last:
        rows.append([("💬 Связаться с поддержкой", "support")])
    rows.append([("❌ Отменить оплату", "cancel")])
    return rows


def callback_for(action: str, order_id: int) -> str:
    oid = int(order_id)
    mapping = {
        "paid": f"paid:{oid}",
        "next": f"pn:{oid}",
        "prev": f"pp:{oid}",
        "cancel": f"pay_cancel:{oid}",
        "support": f"ps:{oid}",
        "warn_go": f"pw:{oid}",
        "open_crypto": f"pay_crypto:{oid}",
        "open_phone": f"pay_transfer:{oid}",
        "open_card": f"pay_card:{oid}",
        "to_menu": "chk:pay_to_methods",
    }
    data = mapping.get(action, "")
    if len(data.encode("utf-8")) > 64:
        raise ValueError(f"callback too long: {data}")
    return data


def add_requisite(fields: dict) -> Tuple[Optional[dict], str]:
    global REQ_SEQ, REQUISITES
    REQ_SEQ += 1
    draft = dict(fields)
    draft["id"] = f"r{REQ_SEQ}"
    if "sort" not in draft:
        sorts = [int(r.get("sort") or 0) for r in REQUISITES]
        draft["sort"] = (max(sorts) if sorts else 0) + 10
    draft["active"] = True
    norm = normalize_requisite(draft)
    if not norm:
        REQ_SEQ -= 1
        return None, "Проверьте тип, банк, страну, номер и получателя."
    REQUISITES.append(norm)
    return norm, ""


def update_requisite(req_id: str, **fields) -> Tuple[Optional[dict], str]:
    req = find_requisite(req_id, REQUISITES)
    if not req:
        return None, "Реквизит не найден."
    merged = dict(req)
    merged.update(fields)
    norm = normalize_requisite(merged)
    if not norm:
        return None, "Некорректное значение."
    req.clear()
    req.update(norm)
    return req, ""


def delete_requisite(req_id: str) -> bool:
    global REQUISITES
    before = len(REQUISITES)
    REQUISITES = [r for r in REQUISITES if r.get("id") != req_id]
    return len(REQUISITES) != before


def toggle_requisite(req_id: str) -> Optional[dict]:
    req = find_requisite(req_id, REQUISITES)
    if not req:
        return None
    req["active"] = not bool(req.get("active", True))
    return req


def move_requisite(req_id: str, direction: int) -> bool:
    global REQUISITES
    ordered = sorted_requisites(REQUISITES)
    ids = [str(r["id"]) for r in ordered]
    if req_id not in ids:
        return False
    idx = ids.index(req_id)
    nxt = idx + (1 if direction > 0 else -1)
    if nxt < 0 or nxt >= len(ordered):
        return False
    ordered[idx], ordered[nxt] = ordered[nxt], ordered[idx]
    for i, req in enumerate(ordered, 1):
        req["sort"] = i * 10
    REQUISITES = ordered
    return True


def toggle_crypto() -> bool:
    global CRYPTO_ENABLED
    CRYPTO_ENABLED = not CRYPTO_ENABLED
    return CRYPTO_ENABLED


def set_crypto_enabled(enabled: bool) -> None:
    global CRYPTO_ENABLED
    CRYPTO_ENABLED = bool(enabled)
