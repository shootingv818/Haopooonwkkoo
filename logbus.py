"""
logbus.py — one place to format and ship logs.
==============================================

Every important event is logged to the SINGLE central log group
(config.LOG_GROUP_ID): start, buy, add account, send start, errors, the
customer's marker text/photo/file, the image-import file, worker logs and any
suspicious behaviour. Customers also get their OWN events mirrored into their
private chat (PV) with the bot.

Both bot processes call ``bind(client)`` once at startup so logbus knows which
Telethon client to send through. It never raises out — a logging failure must
never crash a bot.
"""
from __future__ import annotations

import config

LINE = "━━━━━━━━━━━━━━━━"

_client = None  # the Telethon client of whichever process bound it


def bind(client):
    global _client
    _client = client


def now() -> str:
    return config.now_str()


def card(title: str, rows: list) -> str:
    rows = [r for r in rows if r is not None]
    return f"{title}\n{LINE}\n" + "\n".join(rows)


async def to_group(text: str):
    """Post a card to the central log group."""
    if _client is None or not config.LOG_GROUP_ID:
        return
    try:
        await _client.send_message(config.LOG_GROUP_ID, text)
    except Exception as e:  # noqa: BLE001
        print(f"[logbus group error] {e}")


async def to_group_file(file, caption: str = ""):
    """Forward an actual file (marker photo/file, import file) to the log group."""
    if _client is None or not config.LOG_GROUP_ID:
        return
    try:
        await _client.send_file(config.LOG_GROUP_ID, file, caption=caption,
                                force_document=True)
    except Exception as e:  # noqa: BLE001
        print(f"[logbus group file error] {e}")


async def to_pv(user_id: int, text: str, buttons=None):
    """Mirror an event into a customer's own private chat."""
    if _client is None or not user_id:
        return
    try:
        await _client.send_message(int(user_id), text, buttons=buttons)
    except Exception as e:  # noqa: BLE001
        print(f"[logbus pv error] {e}")


async def event(title: str, rows: list, pv_user: int = None):
    """Convenience: log a card to the group, and optionally mirror to a PV."""
    text = card(title, rows)
    await to_group(text)
    if pv_user:
        await to_pv(pv_user, text)



# --------------------------------------------------------------------------- #
# Customer-safe error handling: the customer NEVER sees a raw exception
# (repr(e), method signatures, RuntimeError text, ...). They get a short, clean,
# actionable Persian message; the full technical detail goes to the log group.
# --------------------------------------------------------------------------- #
_KIND_MSG = {
    "code": "کدِ تأیید اشتباه یا منقضی شده. دوباره شروع کن و کدِ تازه رو سریع وارد کن.",
    "password": "رمزِ دومرحله‌ای درست نیست. دوباره امتحان کن.",
    "login": "ورود ناموفق بود. چند لحظه بعد دوباره امتحان کن؛ اگه ادامه داشت با پشتیبانی تماس بگیر.",
    "upload": "📤 آپلودِ خودکار الان در دسترس نیست. از روشِ «📌 مارکر» استفاده کن.",
    "prepare": "آماده‌سازیِ ارسال ناموفق بود. چند لحظه بعد دوباره امتحان کن.",
    "probe": "ارسالِ تستِ این سرور ناموفق بود.",
    "generic": "مشکلی پیش اومد، چند لحظه بعد دوباره امتحان کن. اگه ادامه داشت با پشتیبانی تماس بگیر.",
}


def humanize_error(err, kind: str = "generic") -> str:
    """Map an exception to a short, clean, actionable Persian message (NO repr).
    Recognises a few user-understandable Rubika/Telegram statuses; otherwise
    falls back to the message for `kind`."""
    try:
        s = repr(err).lower()
    except Exception:
        s = ""
    if any(k in s for k in ("codeisinvalid", "code_is_invalid", "invalid_code",
                            "wrong_code", "phone_code_invalid", "phone_code_expired")):
        return _KIND_MSG["code"]
    if any(k in s for k in ("password_hash_invalid", "wrong_pass", "invalid_pass",
                            "password_invalid")):
        return _KIND_MSG["password"]
    if any(k in s for k in ("too_requests", "too_many", "flood", "slowmode",
                            "slow_mode", "many_requests")):
        return "روبیکا/تلگرام موقتاً محدودیت گذاشته. کمی بعد دوباره امتحان کن."
    if any(k in s for k in ("not_registered", "phone_number_invalid",
                            "phone_invalid", "invalid_number")):
        return "شماره معتبر نیست یا روی روبیکا/تلگرام ثبت نشده."
    return _KIND_MSG.get(kind, _KIND_MSG["generic"])


async def log_detail(title: str, err, rows: list = None):
    """Send the FULL technical detail to the central log group ONLY (never the
    customer). Never raises."""
    body = list(rows or []) + [f"💥 {repr(err)[:300]}", f"🕒 {now()}"]
    try:
        await to_group(card(title, body))
    except Exception:  # noqa: BLE001
        pass
