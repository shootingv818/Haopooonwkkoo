"""
tg_panel.py — the TELEGRAM section of the customer bot.
=======================================================

A fully self-contained, decoupled module that adds a Telegram user-account
panel ALONGSIDE the existing Rubika side. It NEVER touches the Rubika code
paths: it has its own conversation state, its own login clients and its own
DB tables (tg_accounts / tg_settings via db.py).

It is wired in by customer_bot.amain() calling ``tg_panel.setup(bot)`` once the
shared Telethon bot client is created. All callbacks are namespaced ``tg_*`` and
its NewMessage router only acts when the user is in a *Telegram* conversation
step (the Rubika router uses a separate ``state`` dict, so the two never clash).

Send logic mirrors the reference panel: forward ONE pre-set content (text /
photo / file) to the account's mutual contacts + groups, sequentially, with
live progress, a stop button, FloodWait tolerance and a configurable error cap
(config.TG_MAX_ERRORS). Every event is logged to the central group AND mirrored
to the customer's own chat (logbus.event(..., pv_user=uid)).
"""
import asyncio
import os
import time as _time
from datetime import datetime

from telethon import events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    FloodWaitError,
    PhoneNumberInvalidError,
)
from telethon.tl.functions.contacts import GetContactsRequest

import config
import db
import logbus
import ratelimit
import forcedjoin
import telegram_multi_send as multi

# Telethon is imported lazily inside setup to keep this module importable even
# if a tool only wants the helpers.
TelegramClient = None  # set in setup()

LINE = logbus.LINE
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
TG_MEDIA_DIR = os.path.join(DATA_DIR, "tg_media")
os.makedirs(TG_MEDIA_DIR, exist_ok=True)

bot = None  # the shared Telethon bot client (injected by setup)

# Telegram-only conversation state (separate from customer_bot.state).
_state: dict = {}
# Login clients mid-flow: uid -> {"client","phone","hash"}
_pending: dict = {}
# Manual stop flags: account_id -> True
_stop: dict = {}
# account_ids currently sending (avoid double-enqueue)
_active: set = set()
# Contacts-export stop flags + active guard: account_id -> True / {account_id}
_export_stop: dict = {}
_export_active: set = set()
# "My accounts" list pagination: uid -> current page index
_acc_page: dict = {}
_ACC_PAGE_SIZE = 15
# reference to customer_bot's Rubika conversation-state dict (set in setup), so
# entering the Telegram section can clear any half-finished Rubika flow and vice
# versa — prevents BOTH NewMessage routers acting on the same message.
_rubika_state = None


def now() -> str:
    return config.now_str()


def _customer_active_tg(customer_id: int, exclude_aid: int = None):
    """Return a Telegram account (dict) owned by `customer_id` that is CURRENTLY
    sending — optionally excluding `exclude_aid` — or None.

    Enforces (within the Telegram section) the rule that ONE customer may run
    only ONE send at a time. Fully guarded — never raises."""
    if not _active:
        return None
    try:
        for a in db.list_tg_accounts(customer_id):
            aid = a["id"]
            if exclude_aid is not None and aid == exclude_aid:
                continue
            if aid in _active:
                return a
    except Exception:
        return None
    return None


def card(title, rows):
    return logbus.card(title, rows)


# --------------------------------------------------------------------------- #
# Gate (shared free/time/block model, decoupled implementation).
# --------------------------------------------------------------------------- #
async def _gate(event) -> bool:
    uid = event.sender_id
    if db.is_blocked(uid):
        return False
    user = await event.get_sender()
    name = getattr(user, "first_name", "") or ""
    username = getattr(user, "username", "") or ""
    db.ensure_customer(uid, name, username)
    if db.maintenance_on():
        await _respond(event, "🛠 ربات در حال تعمیر است. کمی بعد دوباره امتحان کن.")
        return False
    if not await ratelimit.guard(uid, name):
        await _respond(event, "⛔ به‌خاطر فعالیت بیش از حد، حساب شما مسدود شد.")
        return False
    if not await forcedjoin.enforce(bot, event):
        return False
    cust = db.get_customer(uid) or {}
    if config.FREE_MODE:
        # free for everyone unless the owner set a time that has now passed
        if (cust.get("expires_at") or "") and db.seconds_left(uid) <= 0:
            await _respond(event, "🔴 زمانِ دسترسی‌ات تموم شده. با پشتیبانی تماس بگیر.")
            return False
    else:
        if not db.is_active(uid):
            await _respond(event, "🔴 دسترسی فعال نیست. با پشتیبانی تماس بگیر.")
            return False
    return True


async def _respond(event, text, buttons=None):
    try:
        if isinstance(event, events.CallbackQuery.Event):
            await event.edit(text, buttons=buttons)
        else:
            await event.respond(text, buttons=buttons)
    except Exception:
        try:
            await bot.send_message(event.sender_id, text, buttons=buttons)
        except Exception:
            pass


async def _safe_edit(uid, msg_id, text, buttons=None):
    try:
        await bot.edit_message(uid, msg_id, text, buttons=buttons)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Menu + small UI helpers (kept "book-like": consistent cards, breadcrumbs,
# 🔙/🏠 buttons everywhere).
# --------------------------------------------------------------------------- #
def _menu():
    return [
        [Button.inline("🚀 ارسال", b"tg_accounts"),
         Button.inline("➕ افزودن اکانت", b"tg_addacc")],
        [Button.inline("📨 ارسال چند اکانته", b"tgm_open")],
        [Button.inline("👤 اکانت‌های من", b"tg_accounts"),
         Button.inline("🩺 چک‌حساب", b"tg_health")],
        [Button.inline("✍️ محتوا", b"tg_content"),
         Button.inline("⚙️ سرعت/تاخیر", b"tg_speed")],
        [Button.inline("🎯 مقصد ارسال", b"tg_target"),
         Button.inline("🧹 پاک‌سازی بعد از ارسال", b"tg_delset")],
        [Button.inline("📊 آمار من", b"tg_stats"),
         Button.inline("📖 راهنما", b"tg_help")],
        [Button.url("🆘 پشتیبانی", "https://t.me/tux_pv")],
        [Button.inline("🏠 منوی اصلی", b"mainmenu")],
    ]


def _target_label(mode: str) -> str:
    return {
        "both": "دوطرفه‌ها + گروه‌ها",
        "contacts": "فقط دوطرفه‌ها",
        "groups": "فقط گروه‌ها",
    }.get(mode or "both", "دوطرفه‌ها + گروه‌ها")


def _back_home():
    return [[Button.inline("🔙 تلگرام", b"tg_home"),
             Button.inline("🏠 منوی اصلی", b"mainmenu")]]


def _stop_btn(account_id):
    return [[Button.inline("⛔ توقف ارسال", f"tg_stop_{account_id}".encode())]]


def _content_summary(s: dict) -> str:
    ct = s.get("content_type")
    if not ct:
        return "هنوز محتوایی تنظیم نشده ❌"
    if ct == "text":
        return f"📝 متن:\n{s.get('content_text') or ''}"
    label = "🖼 عکس" if ct == "photo" else "📎 فایل"
    cap = s.get("content_text")
    return label + (f"\n📝 کپشن: {cap}" if cap else " (بدون کپشن)")


def _bar(done, total):
    if total <= 0:
        return "…"
    frac = max(0.0, min(1.0, done / total))
    n = 10
    filled = int(frac * n)
    return "▓" * filled + "░" * (n - filled) + f" {int(frac * 100)}%"


def _progress_card(acc, ok, fail, total, done):
    return card("🚀 تلگرام › ارسال (زنده)", [
        f"📱 {acc['phone']}  ({acc.get('name') or '-'})",
        f"📊 {_bar(done, total)}",
        f"✅ موفق : {ok}    ❌ ناموفق : {fail}",
        f"🎯 کل گیرنده : {total}",
        f"🕒 {now()}",
    ])


# --------------------------------------------------------------------------- #
# Home / cancel
# --------------------------------------------------------------------------- #
async def tg_home_cb(event):
    if not await _gate(event):
        return
    _state.pop(event.sender_id, None)
    if _rubika_state is not None:
        _rubika_state.pop(event.sender_id, None)  # leave any Rubika flow
    await _respond(event, card("📨 پنل تلگرام", [
        "اکانت‌های تلگرامِ خودت رو اضافه کن و محتوا بفرست.",
        "یکی از گزینه‌ها رو انتخاب کن:",
    ]), buttons=_menu())


async def tg_cancel_cb(event):
    uid = event.sender_id
    p = _pending.pop(uid, None)
    if p:
        try:
            await p["client"].disconnect()
        except Exception:
            pass
    _state.pop(uid, None)
    if _rubika_state is not None:
        _rubika_state.pop(uid, None)
    await _respond(event, "لغو شد.", buttons=_menu())


# --------------------------------------------------------------------------- #
# Add account (phone -> code -> optional 2FA)
# --------------------------------------------------------------------------- #
async def tg_addacc_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    cap = config.TG_MAX_ACCOUNTS
    if cap and db.count_customer_tg_accounts(uid) >= cap:
        await _respond(event, card("➕ افزودن اکانت", [
            f"به سقفِ {cap} اکانت رسیدی.",
            "برای افزایش، با پشتیبانی تماس بگیر.",
        ]), buttons=_back_home())
        return
    _state[uid] = {"step": "tg_await_phone"}
    await _respond(event, card("➕ تلگرام › افزودن اکانت", [
        "📱 شمارهٔ اکانت تلگرام رو با کد کشور بفرست.",
        "مثال: `+989121234567`",
    ]), buttons=[[Button.inline("🔙 لغو", b"tg_cancel")]])


async def _handle_phone(event, st):
    uid = event.sender_id
    phone = (event.raw_text or "").strip().replace(" ", "")
    client = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except PhoneNumberInvalidError:
        await event.respond("❌ شماره نامعتبره. دوباره با کد کشور بفرست (مثل +98...).")
        try:
            await client.disconnect()
        except Exception:
            pass
        return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ TG LOGIN START ERROR", e,
                                [f"🆔 {uid}", f"📱 {phone}"])
        await event.respond("❌ " + logbus.humanize_error(e, "login"))
        try:
            await client.disconnect()
        except Exception:
            pass
        return
    _pending[uid] = {"client": client, "phone": phone, "hash": sent.phone_code_hash}
    st["step"] = "tg_await_code"
    await event.respond(card("✉️ تلگرام › کد تأیید", [
        "کدی که تلگرام فرستاد رو بفرست.",
        "با فاصله یا بی‌فاصله، هر دو قبوله.",
    ]), buttons=[[Button.inline("🔙 لغو", b"tg_cancel")]])


async def _handle_code(event, st):
    uid = event.sender_id
    p = _pending.get(uid)
    if not p:
        _state.pop(uid, None)
        await event.respond("نشست لاگین منقضی شد. دوباره «افزودن اکانت» رو بزن.",
                            buttons=_menu())
        return
    code = "".join(ch for ch in (event.raw_text or "") if ch.isdigit())
    try:
        await p["client"].sign_in(phone=p["phone"], code=code,
                                  phone_code_hash=p["hash"])
    except SessionPasswordNeededError:
        st["step"] = "tg_await_password"
        await event.respond("🔐 این اکانت رمز دومرحله‌ای داره. رمز رو بفرست.",
                            buttons=[[Button.inline("🔙 لغو", b"tg_cancel")]])
        return
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        await event.respond("❌ کد اشتباه یا منقضیه. دوباره کد رو بفرست (یا لغو کن).")
        return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ TG LOGIN CODE ERROR", e,
                                [f"🆔 {uid}", f"📱 {p.get('phone')}"])
        await event.respond("❌ " + logbus.humanize_error(e, "code"))
        return
    await _finish_login(event)


async def _handle_password(event, st):
    uid = event.sender_id
    p = _pending.get(uid)
    if not p:
        _state.pop(uid, None)
        await event.respond("نشست لاگین منقضی شد. دوباره «افزودن اکانت» رو بزن.",
                            buttons=_menu())
        return
    try:
        await p["client"].sign_in(password=(event.raw_text or "").strip())
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ TG LOGIN PASSWORD ERROR", e,
                                [f"🆔 {uid}", f"📱 {p.get('phone')}"])
        await event.respond("❌ " + logbus.humanize_error(e, "password"))
        return
    await _finish_login(event)


async def _finish_login(event):
    uid = event.sender_id
    p = _pending.pop(uid, None)
    _state.pop(uid, None)
    if not p:
        return
    client = p["client"]
    phone = p["phone"]
    try:
        me = await client.get_me()
        session_str = client.session.save()
        full_name = " ".join(filter(None, [me.first_name, me.last_name])) or "-"
        username = getattr(me, "username", "") or ""
        try:
            result = await client(GetContactsRequest(hash=0))
            users = result.users
            mutual = [u for u in users if getattr(u, "mutual_contact", False)]
            groups = await _get_groups(client)
            n_contacts, n_mutual, n_groups = len(users), len(mutual), len(groups)
        except Exception:
            n_contacts = n_mutual = n_groups = 0

        aid = db.add_tg_account(uid, phone, full_name, username, me.id, session_str)

        await _respond(event, card("✅ اکانت تلگرام اضافه شد", [
            f"📛 {full_name}" + (f"  (@{username})" if username else ""),
            f"📱 {phone}",
            f"👥 مخاطبین : {n_contacts}   ↔️ دوطرفه : {n_mutual}",
            f"💬 گروه‌ها : {n_groups}",
            LINE,
            "حالا «✍️ محتوا» رو تنظیم کن، بعد «🚀 ارسال».",
        ]), buttons=[[Button.inline("✍️ تنظیم محتوا", b"tg_content")],
                     [Button.inline("🔙 تلگرام", b"tg_home")]])

        await logbus.event("➕ TG ADD ACCOUNT", [
            f"🆔 Customer : {uid}",
            f"📱 {phone}  ({full_name})",
            f"👥 مخاطبین : {n_contacts}   ↔️ دوطرفه : {n_mutual}",
            f"💬 گروه‌ها : {n_groups}",
            f"🕒 {now()}"], pv_user=uid)
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ TG POST-LOGIN ERROR", e,
                                [f"🆔 {uid}", f"📱 {phone}"])
        await event.respond("❌ " + logbus.humanize_error(e, "login"))
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# My accounts + detail + delete
# --------------------------------------------------------------------------- #
async def tg_accounts_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    _acc_page[uid] = 0                         # always open on the first page
    await _render_accounts(event, uid)


async def tg_apage_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    try:
        page = int(event.pattern_match.group(1).decode())
    except Exception:
        page = 0
    _acc_page[uid] = max(0, page)
    await _render_accounts(event, uid)


async def _render_accounts(event, uid):
    accounts = db.list_tg_accounts(uid)
    if not accounts:
        await _respond(event, card("👤 تلگرام › اکانت‌های من", [
            "هنوز اکانتی اضافه نکردی."]),
            buttons=[[Button.inline("➕ افزودن اکانت", b"tg_addacc")],
                     [Button.inline("🔙 تلگرام", b"tg_home")]])
        return
    total = len(accounts)
    pages = max(1, (total + _ACC_PAGE_SIZE - 1) // _ACC_PAGE_SIZE)
    page = max(0, min(int(_acc_page.get(uid, 0)), pages - 1))
    _acc_page[uid] = page
    start = page * _ACC_PAGE_SIZE
    page_accounts = accounts[start:start + _ACC_PAGE_SIZE]
    rows = []
    for i, acc in enumerate(page_accounts, start + 1):
        emoji = "🟢" if acc.get("status") == "active" else "🔴"
        rows.append([Button.inline(f"{emoji} {i}- {acc['phone']}",
                                   f"tg_acc_{acc['id']}".encode())])
    # page navigation: no «صفحه قبل» on the first page (only from page 2 on).
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️ صفحه قبل", f"tg_apage_{page - 1}".encode()))
    if page < pages - 1:
        nav.append(Button.inline("صفحه بعد ➡️", f"tg_apage_{page + 1}".encode()))
    if nav:
        rows.append(nav)
    rows.append([Button.inline("🔙 تلگرام", b"tg_home"),
                 Button.inline("🏠 منوی اصلی", b"mainmenu")])
    await _respond(event, card("👤 تلگرام › اکانت‌های من", [
        "یه اکانت رو انتخاب کن:",
        f"📄 صفحه {page + 1} از {pages}   |   👤 کل: {total}",
    ]), buttons=rows)


async def tg_acc_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_tg_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    status = "فعال 🟢" if acc.get("status") == "active" else "غیرفعال 🔴 (سشن باطل)"
    await _respond(event, card(f"👤 تلگرام › {acc['phone']}", [
        f"📛 نام : {acc.get('name') or '-'}",
        (f"🔗 @{acc['username']}" if acc.get("username") else "🔗 یوزرنیم : -"),
        f"📱 شماره : {acc['phone']}",
        f"📅 افزوده‌شده : {acc.get('added_at') or '-'}",
        f"⭐️ وضعیت : {status}",
    ]), buttons=[
        [Button.inline("🚀 شروع ارسال", f"tg_send_{account_id}".encode())],
        [Button.inline("📥 دریافت مخاطبان TXT", f"tg_ct_{account_id}".encode())],
        [Button.inline("🩺 چک‌حساب", f"tg_chk_{account_id}".encode()),
         Button.inline("🗑 حذف", f"tg_del_{account_id}".encode())],
        [Button.inline("🔙 اکانت‌ها", b"tg_accounts")],
    ])


async def tg_del_cb(event):
    if not await _gate(event):
        return
    account_id = int(event.pattern_match.group(1))
    await _respond(event, "از حذف این اکانت مطمئنی؟",
                   buttons=[[Button.inline("✅ بله، حذف کن",
                                           f"tg_delyes_{account_id}".encode())],
                            [Button.inline("🔙 خیر", f"tg_acc_{account_id}".encode())]])


async def tg_delyes_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_tg_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    db.delete_tg_account(account_id)
    await _respond(event, "اکانت حذف شد. ✅",
                   buttons=[[Button.inline("🔙 اکانت‌ها", b"tg_accounts")]])
    await logbus.event("🗑 TG DELETE ACCOUNT", [
        f"🆔 {uid}", f"📱 {acc['phone']}", f"🕒 {now()}"], pv_user=uid)


# --------------------------------------------------------------------------- #
# Contacts export (TXT: phone numbers only, in order) — per account, stoppable.
# --------------------------------------------------------------------------- #
def _mask_phone(phone) -> str:
    p = "".join(ch for ch in (phone or "") if ch.isdigit())
    if len(p) <= 7:
        return "***"
    return f"{p[:5]}***{p[-3:]}"


async def tg_ct_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_tg_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    if account_id in _export_active:
        await event.answer("همین حالا در حال گرفتن مخاطبین این اکانته.", alert=True)
        return
    if account_id in _active:
        await event.answer("این اکانت همین الان در حال ارساله؛ بعد از اتمام دوباره بزن.",
                           alert=True)
        return
    # hold both the export guard AND the send-busy guard so a send can't open a
    # second connection on the same session while we export (Telegram would
    # revoke it).
    _export_active.add(account_id)
    _active.add(account_id)
    _export_stop.pop(account_id, None)
    pm = await bot.send_message(uid, card("📥 تلگرام › دریافت مخاطبان", [
        f"📱 {acc['phone']}", "⏳ در حال آماده‌سازی ..."]),
        buttons=[[Button.inline("⛔ توقف", f"tg_ctstop_{account_id}".encode())]])
    await event.answer("شروع شد.")
    asyncio.create_task(_run_contacts_export(uid, account_id, pm.id))


async def tg_ctstop_cb(event):
    account_id = int(event.pattern_match.group(1))
    _export_stop[account_id] = True
    await event.answer("درخواست توقف ثبت شد.", alert=True)


async def _run_contacts_export(uid, account_id, msg_id):
    acc = db.get_tg_account_owned(account_id, uid)
    if not acc:
        _export_active.discard(account_id)
        _active.discard(account_id)
        return
    back = [[Button.inline("🔙 اکانت", f"tg_acc_{account_id}".encode())]]
    client = TelegramClient(StringSession(acc.get("session") or ""),
                            config.API_ID, config.API_HASH)
    path = os.path.join(TG_MEDIA_DIR,
                        f"contacts_{uid}_{account_id}_{os.urandom(6).hex()}.txt")
    numbers = []
    stopped = False
    try:
        await client.connect()
        if not await client.is_user_authorized():
            db.set_tg_status(account_id, "dead")
            await _safe_edit(uid, msg_id, card("⚠️ اکانت در دسترس نیست", [
                f"📱 {acc['phone']}", "سشن باطل/خارج‌شده. دوباره اضافه‌اش کن."]),
                buttons=back)
            return
        result = await client(GetContactsRequest(hash=0))
        users = list(getattr(result, "users", []) or [])
        total = len(users)
        seen = set()
        last_edit = 0.0
        for i, u in enumerate(users, 1):
            if _export_stop.get(account_id):
                stopped = True
                break
            ph = "".join(ch for ch in (getattr(u, "phone", "") or "") if ch.isdigit())
            if ph and ph not in seen:
                seen.add(ph)
                numbers.append(ph)
            t = _time.time()
            if t - last_edit >= 2:
                last_edit = t
                await _safe_edit(uid, msg_id, card("📥 تلگرام › دریافت مخاطبان", [
                    f"📱 {acc['phone']}", f"🔢 پردازش‌شده : {i}/{total}",
                    f"✅ شماره‌ها : {len(numbers)}"]),
                    buttons=[[Button.inline("⛔ توقف",
                                            f"tg_ctstop_{account_id}".encode())]])
        if stopped:
            await _safe_edit(uid, msg_id, card("⛔ متوقف شد", [
                f"📱 {acc['phone']}", "دریافت مخاطبین متوقف شد. فایلی ارسال نشد."]),
                buttons=back)
            await logbus.event("⛔ TG CONTACTS EXPORT STOP", [
                f"🆔 {uid}", f"📱 {_mask_phone(acc['phone'])}",
                f"🔢 قبل از توقف : {len(numbers)}", f"🕒 {now()}"], pv_user=uid)
            return
        if not numbers:
            await _safe_edit(uid, msg_id, card("📥 مخاطبی یافت نشد", [
                f"📱 {acc['phone']}", "شماره‌ای برای خروجی نبود."]), buttons=back)
            return
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(numbers) + "\n")
        await bot.send_file(uid, path, caption=card("📥 مخاطبان تلگرام", [
            f"📱 {acc['phone']}", f"🔢 تعداد شماره : {len(numbers)}"]),
            force_document=True)
        await _safe_edit(uid, msg_id, card("✅ فایل مخاطبان ارسال شد", [
            f"📱 {acc['phone']}", f"🔢 {len(numbers)} شماره"]), buttons=back)
        await logbus.event("📥 TG CONTACTS EXPORT", [
            f"🆔 {uid}", f"📱 {_mask_phone(acc['phone'])}",
            f"🔢 تعداد : {len(numbers)}", f"🕒 {now()}"], pv_user=uid)
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ TG CONTACTS EXPORT ERROR", e,
                                [f"🆔 {uid}", f"📱 {_mask_phone(acc.get('phone'))}"])
        await _safe_edit(uid, msg_id, card("❌ خطا", [
            logbus.humanize_error(e, "generic")]), buttons=back)
    finally:
        _export_stop.pop(account_id, None)
        _export_active.discard(account_id)
        _active.discard(account_id)
        try:
            await client.disconnect()
        except Exception:
            pass
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Health check (one account, or all)
# --------------------------------------------------------------------------- #
async def _check_one(acc) -> dict:
    client = TelegramClient(StringSession(acc.get("session") or ""),
                            config.API_ID, config.API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            db.set_tg_status(acc["id"], "dead")
            return {"ok": False, "reason": "سشن باطل/خارج‌شده"}
        me = await client.get_me()
        db.set_tg_status(acc["id"], "active")
        uname = getattr(me, "username", "") or ""
        name = " ".join(filter(None, [me.first_name, me.last_name])) or "-"
        return {"ok": True, "name": name, "username": uname}
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ TG ACCOUNT CHECK ERROR", e, [f"📱 {acc.get('phone')}"])
        return {"ok": False, "reason": "بررسی ناموفق بود"}
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def tg_health_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    accounts = db.list_tg_accounts(uid)
    if not accounts:
        await _respond(event, card("🩺 چک‌حساب", ["اکانتی نداری."]),
                       buttons=_back_home())
        return
    await _respond(event, "🩺 در حال بررسی اکانت‌ها ... کمی صبر کن.")
    rows = []
    for acc in accounts:
        r = await _check_one(acc)
        if r["ok"]:
            tag = f"🟢 {acc['phone']} — {r['name']}"
            if r.get("username"):
                tag += f" (@{r['username']})"
        else:
            tag = f"🔴 {acc['phone']} — {r.get('reason')}"
        rows.append(tag)
    await _respond(event, card("🩺 تلگرام › چک‌حساب", rows), buttons=_back_home())


async def tg_chk_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_tg_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    await _respond(event, "🩺 در حال بررسی ...")
    r = await _check_one(acc)
    if r["ok"]:
        body = [f"🟢 سالم", f"📛 {r['name']}",
                (f"🔗 @{r['username']}" if r.get("username") else "🔗 -"),
                f"📱 {acc['phone']}"]
    else:
        body = [f"🔴 مشکل : {r.get('reason')}", f"📱 {acc['phone']}",
                "اگه سشن باطله، اکانت رو دوباره اضافه کن."]
    await _respond(event, card("🩺 چک‌حساب", body),
                   buttons=[[Button.inline("🔙 اکانت", f"tg_acc_{account_id}".encode())]])


# --------------------------------------------------------------------------- #
# Content (set text / photo / file)
# --------------------------------------------------------------------------- #
async def tg_content_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    s = db.get_tg_settings(uid)
    _state[uid] = {"step": "tg_await_content"}
    await _respond(event, card("✍️ تلگرام › محتوا", [
        "محتوای فعلی:",
        _content_summary(s),
        LINE,
        "محتوای جدید رو بفرست (متن، یا عکس/فایل با کپشن دلخواه).",
        "همین یک‌بار ست می‌شه و ذخیره می‌مونه.",
    ]), buttons=[[Button.inline("🔙 لغو", b"tg_cancel")]])


async def _handle_content(event, st):
    uid = event.sender_id
    msg = event.message
    cap = (msg.text or "").strip()
    try:
        if msg.photo:
            path = await msg.download_media(file=TG_MEDIA_DIR)
            db.set_tg_content(uid, "photo", msg.text or None, path)
            label = "🖼 عکس"
        elif msg.document:
            path = await msg.download_media(file=TG_MEDIA_DIR)
            db.set_tg_content(uid, "file", msg.text or None, path)
            label = "📎 فایل"
        elif msg.text:
            db.set_tg_content(uid, "text", msg.text, None)
            label = "📝 متن"
        else:
            await event.respond("❌ این نوع محتوا پشتیبانی نمی‌شه. متن، عکس یا فایل بفرست.")
            return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ TG CONTENT SAVE ERROR", e, [f"🆔 {uid}"])
        await event.respond("❌ ذخیرهٔ محتوا ناموفق بود. دوباره امتحان کن.")
        return
    _state.pop(uid, None)
    # show the customer exactly what was saved
    confirm = [f"{label} به‌عنوان محتوای ارسالی ثبت شد."]
    if cap:
        confirm += [LINE, "📝 متنِ ذخیره‌شده:", cap]
    await event.respond(card("✅ محتوا ذخیره شد", confirm), buttons=_menu())
    # log the FULL content (what the user actually set), to group + DM
    log_rows = [f"🆔 {uid}", f"📦 نوع : {label}"]
    if cap:
        log_rows.append(f"📝 متن : {cap[:900]}")
    else:
        log_rows.append("📝 متن : (بدون متن/کپشن)")
    log_rows.append(f"🕒 {now()}")
    await logbus.event("✍️ TG CONTENT SET", log_rows, pv_user=uid)


# --------------------------------------------------------------------------- #
# Speed / delay
# --------------------------------------------------------------------------- #
async def tg_speed_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    cur = db.get_tg_delay(uid)
    rows = [[Button.inline(("✅ " if abs(cur - v) < 0.01 else "") + f"{v}s",
                           f"tg_spd_{v}".encode())]
            for v in (0.5, 1, 2, 3, 5)]
    rows.append([Button.inline("🔙 تلگرام", b"tg_home")])
    await _respond(event, card("⚙️ تلگرام › سرعت/تاخیر ارسال", [
        f"تاخیر فعلی بین هر ارسال : {cur}s",
        "هرچه بیشتر، امن‌تر (کمتر FloodWait).",
    ]), buttons=rows)


async def tg_spd_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    val = event.pattern_match.group(1).decode()
    db.set_tg_delay(uid, float(val))
    await tg_speed_cb(event)


# --------------------------------------------------------------------------- #
# Target mode (where to send: mutual contacts, groups, or both) — selectable.
# --------------------------------------------------------------------------- #
async def tg_target_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    cur = db.get_tg_settings(uid).get("target_mode") or "both"

    def _mk(m, label):
        return Button.inline(("✅ " if cur == m else "") + label,
                             f"tg_tgt_{m}".encode())

    rows = [
        [_mk("both", "دوطرفه‌ها + گروه‌ها")],
        [_mk("contacts", "فقط دوطرفه‌ها")],
        [_mk("groups", "فقط گروه‌ها")],
        [Button.inline("🔙 تلگرام", b"tg_home")],
    ]
    await _respond(event, card("🎯 تلگرام › مقصد ارسال", [
        f"مقصد فعلی : {_target_label(cur)}",
        LINE,
        "محتوا به کجا ارسال بشه؟",
        "• دوطرفه‌ها + گروه‌ها (پیش‌فرض)",
        "• فقط دوطرفه‌ها (کم‌ریسک‌تر؛ گروه‌ها نادیده می‌شن)",
        "• فقط گروه‌ها",
    ]), buttons=rows)


async def tg_tgt_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    mode = event.data.decode().rsplit("_", 1)[-1]   # both | contacts | groups
    db.set_tg_target_mode(uid, mode)
    await logbus.event("🎯 TG TARGET MODE", [
        f"🆔 {uid}", f"مقصد : {_target_label(mode)}", f"🕒 {now()}"], pv_user=uid)
    await tg_target_cb(event)


# --------------------------------------------------------------------------- #
# One-sided delete-after-send toggle (delete only on the sender's side).
# --------------------------------------------------------------------------- #
async def tg_delset_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    on = db.get_tg_delete_after(uid)
    rows = [
        [Button.inline(("✅ " if not on else "") + "خاموش", b"tg_delset_off")],
        [Button.inline(("✅ " if on else "") + "حذف فقط برای من", b"tg_delset_on")],
        [Button.inline("🔙 تلگرام", b"tg_home")],
    ]
    await _respond(event, card("🧹 تلگرام › پاک‌سازی بعد از ارسال", [
        f"وضعیت فعلی : {'حذف گفتگو (فقط برای من) ✅' if on else 'خاموش'}",
        LINE,
        "وقتی روشن باشه، بعد از هر ارسالِ موفق به یک مخاطب، کلِ گفتگو با اون",
        "مخاطب فقط از سمت اکانت خودت حذف می‌شه و از لیست چت‌هات میره —",
        "برای گیرنده کاملاً باقی می‌مونه (حذف یک‌طرفهٔ گفتگو).",
        "فقط روی چت‌های خصوصیه؛ گروه/کانال دست‌نخورده می‌مونه.",
        "روی ارسال تکی و چنداکانته، هر دو، اعمال می‌شه.",
    ]), buttons=rows)


async def tg_delset_set_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    on = event.data.decode().endswith("_on")
    db.set_tg_delete_after(uid, on)
    await logbus.event("🧹 TG DELETE-AFTER", [
        f"🆔 {uid}", f"وضعیت : {'حذف فقط برای من' if on else 'خاموش'}",
        f"🕒 {now()}"], pv_user=uid)
    await tg_delset_cb(event)


# --------------------------------------------------------------------------- #
# My stats
# --------------------------------------------------------------------------- #
async def tg_stats_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    s = db.get_tg_settings(uid)
    n_acc = db.count_customer_tg_accounts(uid)
    await _respond(event, card("📊 تلگرام › آمار من", [
        f"👤 اکانت‌های تلگرام : {n_acc}",
        f"📤 کل ارسال‌ها : {int(s.get('total_sends') or 0)}",
        f"📦 محتوا : {'تنظیم‌شده ✅' if s.get('content_type') else 'تنظیم‌نشده ❌'}",
        f"⚙️ تاخیر : {config.clamp_tg_delay(s.get('send_delay'))}s",
    ]), buttons=_back_home())


# --------------------------------------------------------------------------- #
# Help
# --------------------------------------------------------------------------- #
async def tg_help_cb(event):
    if not await _gate(event):
        return
    await _respond(event, card("📖 راهنمای بخش تلگرام", [
        "➕ افزودن اکانت : شماره → کد → (در صورت لزوم) رمز دومرحله‌ای.",
        "✍️ محتوا : متن یا عکس/فایل با کپشن که ارسال می‌شه.",
        "🚀 ارسال : محتوا به مقصدی که انتخاب کردی می‌ره (دوطرفه‌ها/گروه‌ها/هردو)؛",
        "   پیشرفت زنده نشون داده می‌شه و دکمهٔ «⛔ توقف» داری.",
        f"   فقط اگه به {config.TG_MAX_ERRORS} خطای پیاپی برسه متوقف می‌شه.",
        "🎯 مقصد ارسال : انتخاب کن به دوطرفه‌ها، گروه‌ها یا هردو ارسال شه.",
        "🩺 چک‌حساب : زنده‌بودن سشنِ اکانت‌ها رو بررسی می‌کنه.",
        "⚙️ سرعت/تاخیر : فاصلهٔ بین ارسال‌ها (برای کم‌کردن محدودیت).",
        "📊 آمار من : تعداد اکانت و کل ارسال‌ها.",
        LINE,
        "⚠️ ارسالِ انبوه ممکنه باعث محدودیتِ اکانت توسط تلگرام بشه؛ "
        "تاخیر مناسب بذار.",
    ]), buttons=_back_home())


# --------------------------------------------------------------------------- #
# Send (confirm -> enqueue -> sequential worker with live progress)
# --------------------------------------------------------------------------- #
async def tg_send_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_tg_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    s = db.get_tg_settings(uid)
    if not s.get("content_type"):
        await event.answer("اول محتوا رو تنظیم کن.", alert=True)
        return
    await _respond(event, card("🚀 تلگرام › تأیید ارسال", [
        f"📱 اکانت : {acc['phone']}",
        "محتوایی که ارسال می‌شه:",
        _content_summary(s),
        LINE,
        f"🎯 مقصد : {_target_label(s.get('target_mode') or 'both')}",
        "مطمئنی؟ (از «🎯 مقصد ارسال» می‌تونی عوضش کنی)",
    ]), buttons=[[Button.inline("✅ بله، شروع کن", f"tg_go_{account_id}".encode())],
                 [Button.inline("🔙 خیر", f"tg_acc_{account_id}".encode())]])


async def tg_go_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_tg_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    if not db.get_tg_settings(uid).get("content_type"):
        await event.answer("اول محتوا رو تنظیم کن.", alert=True)
        return
    if account_id in _active:
        await event.answer("این اکانت همین الان در حال ارساله.", alert=True)
        return
    busy = _customer_active_tg(uid, exclude_aid=account_id)
    if busy:
        await event.answer(
            f"⛔ همین حالا یک ارسال با اکانت {busy['phone']} در جریانه. "
            "هر مشتری هم‌زمان فقط یک ارسال می‌تونه داشته باشه — "
            "اول اون تموم یا متوقف بشه.", alert=True)
        return
    _active.add(account_id)
    _stop[account_id] = False
    await _respond(event, card("🚀 تلگرام › ارسال", [
        "✅ شروع شد. پیشرفت در پیامِ پایین نشون داده می‌شه."]))
    pm = await bot.send_message(uid, card("🚀 تلگرام › ارسال (زنده)", [
        f"📱 {acc['phone']}", "⏳ آماده‌سازی ..."]), buttons=_stop_btn(account_id))
    # Each send runs as its OWN task (mirrors the Rubika side's
    # asyncio.create_task(run_send)), so one customer's long send never blocks
    # another's behind a single global queue. The per-account _active guard
    # already prevents the same account from sending twice concurrently.
    asyncio.create_task(_send_task({"account_id": account_id, "uid": uid,
                                    "msg_id": pm.id}))


async def tg_stop_cb(event):
    account_id = int(event.pattern_match.group(1))
    _stop[account_id] = True
    await event.answer("درخواست توقف ثبت شد. بعد از ارسالِ جاری متوقف می‌شه.",
                       alert=True)


async def _get_groups(client):
    groups = []
    async for dialog in client.iter_dialogs():
        if dialog.is_group:
            groups.append(dialog.entity)
    return groups


async def _collect_recipients(client, mode: str = "both"):
    """Build the recipient list per the customer's chosen target mode:
    'both' = mutual contacts + groups, 'contacts' = mutual contacts only,
    'groups' = groups only."""
    mutual = []
    groups = []
    if mode in ("both", "contacts"):
        result = await client(GetContactsRequest(hash=0))
        mutual = [u for u in result.users if getattr(u, "mutual_contact", False)]
    if mode in ("both", "groups"):
        groups = await _get_groups(client)
    return list(mutual) + list(groups)


async def _sleep_or_stop(account_id, seconds: float, step: float = 2.0) -> bool:
    """Sleep up to `seconds` but bail out early (return True) if the customer
    pressed STOP — keeps the stop button responsive during a FloodWait wait."""
    waited = 0.0
    while waited < seconds:
        if _stop.get(account_id):
            return True
        d = min(step, seconds - waited)
        await asyncio.sleep(d)
        waited += d
    return False


async def _prepare_media(client, s):
    ct = s.get("content_type")
    if ct == "text":
        return None
    caption = s.get("content_text") or ""
    force_doc = ct == "file"
    sent = await client.send_file("me", s["media_path"], caption=caption,
                                  force_document=force_doc)
    return sent.media


async def _send_one(client, peer, s, prepared_media):
    """Send the configured content to one peer and RETURN the sent Message
    (needed for the one-sided delete-after feature)."""
    ct = s.get("content_type")
    caption = s.get("content_text") or ""
    if ct == "text":
        return await client.send_message(peer, s.get("content_text") or "")
    return await client.send_file(peer, prepared_media, caption=caption)


async def _delete_own_after(client, peer, sent):
    """One-sided conversation delete: after a successful PRIVATE send, remove the
    WHOLE conversation with that contact from the SENDER's side only
    (revoke=False) — the dialog disappears from the account's chat list, while
    the recipient keeps everything. Groups/channels are never touched."""
    try:
        if sent is None or not getattr(sent, "is_private", False):
            return
        await client.delete_dialog(peer, revoke=False)
    except Exception:
        pass


async def _do_send(job):
    account_id = job["account_id"]
    uid = job["uid"]
    msg_id = job["msg_id"]
    acc = db.get_tg_account(account_id)
    if not acc:
        return
    s = db.get_tg_settings(uid)
    if not s.get("content_type"):
        await _safe_edit(uid, msg_id, "⚠️ محتوایی تنظیم نشده.")
        return
    delay = config.clamp_tg_delay(s.get("send_delay"))
    del_after = bool(s.get("delete_after"))
    client = TelegramClient(StringSession(acc.get("session") or ""),
                            config.API_ID, config.API_HASH)
    ok = fail = total = 0
    stopped = False
    hit_max = False
    flood_stop = 0          # >0 => stopped because Telegram asked too long a wait
    started = datetime.now()
    try:
        await client.connect()
        if not await client.is_user_authorized():
            db.set_tg_status(account_id, "dead")
            await _safe_edit(uid, msg_id, card("⚠️ اکانت در دسترس نیست", [
                f"📱 {acc['phone']}", "سشن باطل/خارج‌شده. دوباره اضافه‌اش کن."]))
            await logbus.event("⚠️ TG ACCOUNT DEAD", [
                f"🆔 {uid}", f"📱 {acc['phone']}", f"🕒 {now()}"], pv_user=uid)
            return

        mode = s.get("target_mode") or "both"
        recipients = []
        prepared = None
        # Honour the stop button DURING the "preparing" phase too (connect /
        # collect recipients / upload media), not only inside the send loop —
        # otherwise pressing stop while it shows "⏳ آماده‌سازی ..." does nothing.
        if _stop.get(account_id):
            stopped = True
        else:
            recipients = await _collect_recipients(client, mode)
            total = len(recipients)
            if _stop.get(account_id):
                stopped = True
            else:
                prepared = await _prepare_media(client, s)

        if not stopped:
            await logbus.event("🚀 TG SEND START", [
                f"🆔 {uid}", f"📱 {acc['phone']}", f"🎯 گیرنده : {total}",
                f"🎯 مقصد : {_target_label(mode)}",
                f"🕒 {now()}"], pv_user=uid)
            await _safe_edit(uid, msg_id, _progress_card(acc, 0, 0, total, 0),
                             buttons=_stop_btn(account_id))

        last_edit = 0.0
        consec_fail = 0          # CONSECUTIVE failures (resets on each success)
        for i, peer in enumerate(recipients, 1):
            if _stop.get(account_id):
                stopped = True
                break
            try:
                sent = await asyncio.wait_for(_send_one(client, peer, s, prepared),
                                              timeout=config.TG_SEND_TIMEOUT)
                ok += 1
                consec_fail = 0
                if del_after:
                    await _delete_own_after(client, peer, sent)
            except FloodWaitError as fw:
                wait_s = int(getattr(fw, "seconds", 5))
                # too long -> don't freeze silently; stop and tell the customer
                if wait_s > config.TG_FLOODWAIT_MAX:
                    flood_stop = wait_s
                    break
                # short enough -> wait it out, but stay responsive to STOP
                await logbus.event("⏸ TG FLOODWAIT", [
                    f"🆔 {uid}", f"📱 {acc['phone']}",
                    f"⏳ تلگرام {wait_s}s محدودیت گذاشت — صبر و ادامه",
                    f"📊 ✅ {ok}  ❌ {fail}  از {total}", f"🕒 {now()}"], pv_user=uid)
                await _safe_edit(uid, msg_id, card("⏸ تلگرام › محدودیت موقت", [
                    f"📱 {acc['phone']}",
                    f"⏳ تلگرام {wait_s} ثانیه محدودیت گذاشت.",
                    "بعد از این مدت ارسال خودکار ادامه پیدا می‌کنه.",
                    f"📊 ✅ {ok}   ❌ {fail}   از {total}",
                ]), buttons=_stop_btn(account_id))
                if await _sleep_or_stop(account_id, wait_s + 1):
                    stopped = True
                    break
                # one retry of the SAME peer after the wait
                try:
                    sent = await asyncio.wait_for(_send_one(client, peer, s, prepared),
                                                  timeout=config.TG_SEND_TIMEOUT)
                    ok += 1
                    consec_fail = 0
                    if del_after:
                        await _delete_own_after(client, peer, sent)
                except Exception:  # noqa: BLE001
                    fail += 1
                    consec_fail += 1
            except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                fail += 1
                consec_fail += 1
            # stop only on CONSECUTIVE errors (scattered per-peer fails are normal)
            if consec_fail >= config.TG_MAX_ERRORS:
                hit_max = True
                break
            t = _time.time()
            if t - last_edit >= 2:
                last_edit = t
                await _safe_edit(uid, msg_id,
                                 _progress_card(acc, ok, fail, total, i),
                                 buttons=_stop_btn(account_id))
            await asyncio.sleep(delay)

        if ok:
            db.incr_tg_sends(uid, ok)
        duration = int((datetime.now() - started).total_seconds())
        rate = f"{(ok / total * 100):.0f}%" if total else "0%"
        if flood_stop:
            head = "🛑 TG SEND STOPPED (محدودیت تلگرام)"
            note = (f"تلگرام این اکانت رو {flood_stop}s محدود کرد (FloodWait). "
                    "ارسال متوقف شد؛ کمی بعد دوباره بزن یا تأخیر رو بیشتر کن.")
        elif hit_max:
            head = "🛑 TG SEND STOPPED (سقف خطا)"
            note = f"به {config.TG_MAX_ERRORS} خطای پیاپی رسید و متوقف شد."
        elif stopped:
            head = "🛑 TG SEND STOPPED (توسط کاربر)"
            note = "ارسال به‌درخواستِ کاربر متوقف شد."
        else:
            head = "🏁 TG SEND FINISHED"
            note = "ارسال کامل شد."
        rows = [f"🆔 {uid}", f"📱 {acc['phone']}", note,
                f"✅ موفق : {ok}    ❌ ناموفق : {fail}",
                f"🎯 کل : {total}    📊 {rate}",
                f"⏱ {duration}s    🕒 {now()}"]
        await _safe_edit(uid, msg_id, card(head, rows[1:]),
                         buttons=[[Button.inline("🔙 تلگرام", b"tg_home")]])
        await logbus.event(head, rows, pv_user=uid)
    except Exception as e:  # noqa: BLE001
        await logbus.to_group(card("❌ TG SEND ERROR", [
            f"🆔 {uid}", f"📱 {acc['phone']}", f"💥 {repr(e)[:160]}",
            f"🕒 {now()}"]))
        await _safe_edit(uid, msg_id, card("❌ خطا در ارسال", [
            f"📱 {acc['phone']}", logbus.humanize_error(e, "generic")]),
            buttons=[[Button.inline("🔙 تلگرام", b"tg_home")]])
    finally:
        _stop.pop(account_id, None)
        _active.discard(account_id)
        try:
            await client.disconnect()
        except Exception:
            pass


async def _send_task(job):
    """Run ONE send safely. Mirrors the guard the removed single-queue worker
    used to provide: never let an exception escape as an unretrieved-task error,
    and ALWAYS release the per-account flags so the account can't get stuck in
    `_active` ('این اکانت همین الان در حال ارساله') after a failure."""
    account_id = job["account_id"]
    try:
        await _do_send(job)
    except Exception as e:  # noqa: BLE001
        print(f"[tg send_task] {e}")
    finally:
        _stop.pop(account_id, None)
        _active.discard(account_id)


# --------------------------------------------------------------------------- #
# Multi-account send (ported engine: telegram_multi_send). Sequential per
# account, own contacts (mutual-first), FloodWait cooldown, abandon-bad-account,
# restart-safe, anti-duplicate, stop. Content REUSES the customer's "✍️ محتوا".
# Selection state is per-customer (uid -> [account_id, ...]).
# --------------------------------------------------------------------------- #
_multi_sel: dict = {}
_multi_page: dict = {}          # uid -> current page index in the select view
_MULTI_PAGE_SIZE = 15           # accounts shown per page in the select view
_MULTI_ACTIVE_STATES = ("queued", "running", "waiting", "stop_requested")


def _multi_content_items(uid: int):
    """Build the ported engine's content items from the customer's saved
    Telegram content (the same one the single sender uses). None if unset."""
    s = db.get_tg_settings(uid)
    ct = s.get("content_type")
    if ct == "text":
        txt = s.get("content_text") or ""
        return [{"type": "text", "text": txt}] if txt else None
    if ct in ("photo", "file"):
        path = s.get("media_path")
        if not path or not os.path.exists(path):
            return None
        return [{"type": "media", "media": path,
                 "caption": s.get("content_text") or ""}]
    return None


def _multi_select_view(uid: int):
    accounts = [a for a in db.list_tg_accounts(uid)
                if a.get("status") == "active" and a.get("session")]
    valid = {int(a["id"]) for a in accounts}
    chosen = _multi_sel.setdefault(uid, [])
    chosen[:] = [aid for aid in chosen if aid in valid]

    total = len(accounts)
    pages = max(1, (total + _MULTI_PAGE_SIZE - 1) // _MULTI_PAGE_SIZE)
    page = max(0, min(int(_multi_page.get(uid, 0)), pages - 1))
    _multi_page[uid] = page
    start = page * _MULTI_PAGE_SIZE
    page_accounts = accounts[start:start + _MULTI_PAGE_SIZE]

    rows = []
    for a in page_accounts:
        mark = "✅" if int(a["id"]) in chosen else "▫️"
        rows.append([Button.inline(f"{mark} {a['phone']} — {a.get('name') or '-'}",
                                   f"tgm_sel_{a['id']}".encode())])
    body = ["اکانت‌هایی که می‌خوای هم‌زمان ارسال کنن رو تیک بزن.",
            "هر اکانت فقط به مخاطبین خودش، به‌ترتیب و دونه‌دونه می‌فرسته."]
    if not accounts:
        body.append(LINE)
        body.append("اکانت فعالی نداری. اول یک اکانت اضافه کن.")
    else:
        body.append(LINE)
        body.append(f"📄 صفحه {page + 1} از {pages}   |   👤 کل: {total}"
                    f"   |   ✅ انتخاب‌شده: {len(chosen)}")
    # page navigation: no «صفحه قبل» on the first page (only from page 2 on).
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️ صفحه قبل", f"tgm_page_{page - 1}".encode()))
    if page < pages - 1:
        nav.append(Button.inline("صفحه بعد ➡️", f"tgm_page_{page + 1}".encode()))
    if nav:
        rows.append(nav)
    if chosen:
        rows.append([Button.inline(f"🚀 شروع با {len(chosen)} اکانت",
                                   b"tgm_go")])
    rows.append([Button.inline("📊 وضعیت ارسال‌ها", b"tgm_jobs")])
    rows.append([Button.inline("🔙 تلگرام", b"tg_home"),
                 Button.inline("🏠 منوی اصلی", b"mainmenu")])
    return card("📨 تلگرام › ارسال چند اکانته", body), rows


async def tgm_open_cb(event):
    if not await _gate(event):
        return
    _state.pop(event.sender_id, None)
    _multi_page[event.sender_id] = 0           # always open on the first page
    text, rows = _multi_select_view(event.sender_id)
    await _respond(event, text, buttons=rows)


async def tgm_page_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    try:
        page = int(event.pattern_match.group(1).decode())
    except Exception:
        page = 0
    _multi_page[uid] = max(0, page)
    text, rows = _multi_select_view(uid)
    await _respond(event, text, buttons=rows)


async def tgm_sel_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    acc = db.get_tg_account_owned(aid, uid)
    if not acc or acc.get("status") != "active" or not acc.get("session"):
        await event.answer("اکانت فعال پیدا نشد.", alert=True)
        return
    chosen = _multi_sel.setdefault(uid, [])
    if aid in chosen:
        chosen.remove(aid)
    else:
        chosen.append(aid)
    text, rows = _multi_select_view(uid)
    await _respond(event, text, buttons=rows)


async def tgm_go_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    chosen = list(_multi_sel.get(uid, []))
    if not chosen:
        await event.answer("حداقل یک اکانت انتخاب کن.", alert=True)
        return
    items = _multi_content_items(uid)
    if not items:
        await event.answer("اول از «✍️ محتوا» یک متن یا عکس/فایل تنظیم کن.",
                           alert=True)
        return
    # finalize any of THIS customer's jobs that are mid-stop, so a fresh send
    # isn't wrongly blocked by a job that's still settling after a stop.
    try:
        for j in multi.list_jobs(customer_id=uid, limit=20):
            if j.get("state") == "stop_requested":
                await multi.stop(j["job_id"])
    except Exception:
        pass
    # one operation per customer: block if a single send is running on any of
    # this customer's accounts, or the customer already has an active multi job.
    if _customer_active_tg(uid):
        await event.answer("همین حالا یک ارسال دیگه از تو در جریانه. "
                           "اول اون تموم یا متوقف بشه.", alert=True)
        return
    try:
        actives = multi.list_jobs(customer_id=uid, limit=20)
    except Exception:
        actives = []
    if any(j.get("state") in _MULTI_ACTIVE_STATES for j in actives):
        await event.answer("یک ارسال چنداکانته‌ی فعال داری. اول از «📊 وضعیت "
                           "ارسال‌ها» متوقفش کن، بعد دوباره بزن.", alert=True)
        return
    await _respond(event, card("📨 تلگرام › ارسال چند اکانته", [
        "⏳ در حال آماده‌سازی مخاطبین هر اکانت (اول دوطرفه‌ها) ..."]))
    try:
        job = await multi.create_job(customer_id=uid, account_ids=chosen,
                                     content={"items": items})
        await multi.start(job["job_id"])
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ TG MULTI START ERROR", e, [f"🆔 {uid}"])
        await _respond(event, card("❌ شروع ناموفق", [
            logbus.humanize_error(e, "generic")]),
            buttons=[[Button.inline("🔙 بازگشت", b"tgm_open")]])
        return
    _multi_sel.pop(uid, None)
    await logbus.event("📨 TG MULTI SEND START", [
        f"🆔 Customer : {uid}",
        f"📱 اکانت‌ها : {len(chosen)}",
        f"🎯 کل مخاطبین : {job.get('total', 0)}",
        f"🤝 دوطرفه : {job.get('mutual_total', 0)}",
        f"🕒 {now()}"], pv_user=uid)
    await _respond(event, card("✅ ارسال چند اکانته شروع شد", [
        f"📱 اکانت‌ها : {len(chosen)}",
        f"🎯 کل مخاطبین : {job.get('total', 0)}",
        f"🤝 دوطرفه : {job.get('mutual_total', 0)}",
        LINE,
        "گزارش زنده در گروه لاگ نمایش داده می‌شه.",
    ]), buttons=[[Button.inline("📊 وضعیت ارسال‌ها", b"tgm_jobs")],
                 [Button.inline("🔙 تلگرام", b"tg_home")]])


def _multi_jobs_view(uid: int):
    try:
        jobs = multi.list_jobs(customer_id=uid, limit=6)
    except Exception:
        jobs = []
    body = []
    rows = []
    if not jobs:
        body.append("هنوز ارسال چنداکانته‌ای نداشتی.")
    _fa = {"queued": "در صف", "running": "در حال ارسال", "waiting": "انتظار",
           "stop_requested": "در حال توقف", "paused": "متوقف",
           "completed": "پایان", "failed": "خطا"}
    for j in jobs:
        jid = j["job_id"]
        st = _fa.get(j["state"], j["state"])
        body.append(
            f"• {jid[:8]} | {st} | ✅ {j.get('sent_count', 0)}/{j.get('total', 0)} "
            f"| ❌ {j.get('failed_count', 0)}")
        if j["state"] in _MULTI_ACTIVE_STATES:
            rows.append([Button.inline(f"⛔ توقف {jid[:8]}",
                                       f"tgm_stop_{jid}".encode())])
        elif j["state"] in ("paused", "failed"):
            rows.append([Button.inline(f"▶️ ادامه {jid[:8]}",
                                       f"tgm_resume_{jid}".encode())])
    rows.append([Button.inline("♻️ بروزرسانی", b"tgm_jobs")])
    rows.append([Button.inline("🔙 تلگرام", b"tg_home")])
    return card("📊 تلگرام › ارسال‌های چند اکانته", body), rows


async def tgm_jobs_cb(event):
    if not await _gate(event):
        return
    text, rows = _multi_jobs_view(event.sender_id)
    await _respond(event, text, buttons=rows)


def _multi_owns_job(uid: int, jid: str) -> bool:
    try:
        return int(multi.status(jid).get("customer_id") or 0) == int(uid)
    except Exception:
        return False


async def tgm_stop_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    jid = event.pattern_match.group(1).decode()
    if not _multi_owns_job(uid, jid):
        await event.answer("این ارسال متعلق به تو نیست.", alert=True)
        return
    try:
        await multi.stop(jid)
    except Exception:
        pass
    await logbus.event("⛔ TG MULTI SEND STOP", [
        f"🆔 {uid}", f"🔖 {jid[:8]}", f"🕒 {now()}"], pv_user=uid)
    # Show ONLY a clean confirmation for the job that was just stopped — do NOT
    # dump the whole list of previous/paused jobs (that pile of old stopped
    # sends popping up on every stop was the reported annoyance).
    await _respond(event, card("⛔ ارسال چند اکانته متوقف شد", [
        f"🔖 {jid[:8]}",
        "ارسالِ جاری متوقف شد.",
        f"🕒 {now()}",
    ]), buttons=[[Button.inline("📊 وضعیت ارسال‌ها", b"tgm_jobs")],
                 [Button.inline("🔙 تلگرام", b"tg_home")]])


async def tgm_resume_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    jid = event.pattern_match.group(1).decode()
    if not _multi_owns_job(uid, jid):
        await event.answer("این ارسال متعلق به تو نیست.", alert=True)
        return
    if _customer_active_tg(uid):
        await event.answer("یک ارسال دیگه از تو در جریانه.", alert=True)
        return
    try:
        await multi.resume(jid)
    except Exception:
        pass
    text, rows = _multi_jobs_view(uid)
    await _respond(event, text, buttons=rows)


# --------------------------------------------------------------------------- #
# NewMessage router (only acts on Telegram conversation steps)
# --------------------------------------------------------------------------- #
async def _msg_router(event):
    uid = event.sender_id
    if db.is_blocked(uid):
        return
    txt = event.raw_text or ""
    if txt.startswith("/"):
        return  # commands handled by customer_bot
    st = _state.get(uid)
    if not st:
        return  # not in a Telegram flow -> ignore (Rubika router handles its own)
    # If a Rubika flow also owns this user (shouldn't happen, but be safe),
    # defer to the Rubika router so the message is handled exactly once.
    if _rubika_state is not None and _rubika_state.get(uid):
        return
    if db.maintenance_on():
        await event.respond("🛠 ربات در حال تعمیر است.")
        return
    user = await event.get_sender()
    if not await ratelimit.guard(uid, getattr(user, "first_name", "") or ""):
        await event.respond("⛔ به‌خاطر فعالیت بیش از حد، حساب شما مسدود شد.")
        return
    step = st.get("step")
    if step == "tg_await_phone":
        await _handle_phone(event, st)
    elif step == "tg_await_code":
        await _handle_code(event, st)
    elif step == "tg_await_password":
        await _handle_password(event, st)
    elif step == "tg_await_content":
        await _handle_content(event, st)


# --------------------------------------------------------------------------- #
# Wiring
# --------------------------------------------------------------------------- #
def setup(shared_bot, rubika_state=None):
    """Register all Telegram-section handlers on the shared bot. Called once
    from customer_bot.amain(). Each send runs as its own asyncio task (see
    tg_go_cb), so sends no longer share a single global queue.
    rubika_state is customer_bot's conversation-state dict (for cross-section
    mutual exclusion)."""
    global bot, TelegramClient, _rubika_state
    bot = shared_bot
    _rubika_state = rubika_state
    from telethon import TelegramClient as _TC
    TelegramClient = _TC

    add = bot.add_event_handler
    add(tg_home_cb, events.CallbackQuery(data=b"tg_home"))
    add(tg_cancel_cb, events.CallbackQuery(data=b"tg_cancel"))
    add(tg_addacc_cb, events.CallbackQuery(data=b"tg_addacc"))
    add(tg_accounts_cb, events.CallbackQuery(data=b"tg_accounts"))
    add(tg_apage_cb, events.CallbackQuery(pattern=b"tg_apage_(\\d+)"))
    add(tg_acc_cb, events.CallbackQuery(pattern=b"tg_acc_(\\d+)"))
    add(tg_del_cb, events.CallbackQuery(pattern=b"tg_del_(\\d+)"))
    add(tg_delyes_cb, events.CallbackQuery(pattern=b"tg_delyes_(\\d+)"))
    add(tg_health_cb, events.CallbackQuery(data=b"tg_health"))
    add(tg_chk_cb, events.CallbackQuery(pattern=b"tg_chk_(\\d+)"))
    add(tg_ct_cb, events.CallbackQuery(pattern=b"tg_ct_(\\d+)"))
    add(tg_ctstop_cb, events.CallbackQuery(pattern=b"tg_ctstop_(\\d+)"))
    add(tg_content_cb, events.CallbackQuery(data=b"tg_content"))
    add(tg_speed_cb, events.CallbackQuery(data=b"tg_speed"))
    add(tg_spd_cb, events.CallbackQuery(pattern=b"tg_spd_([0-9.]+)"))
    add(tg_target_cb, events.CallbackQuery(data=b"tg_target"))
    add(tg_tgt_cb, events.CallbackQuery(pattern=b"tg_tgt_(both|contacts|groups)"))
    add(tg_delset_cb, events.CallbackQuery(data=b"tg_delset"))
    add(tg_delset_set_cb, events.CallbackQuery(pattern=b"tg_delset_(on|off)"))
    add(tg_stats_cb, events.CallbackQuery(data=b"tg_stats"))
    add(tg_help_cb, events.CallbackQuery(data=b"tg_help"))
    add(tg_send_cb, events.CallbackQuery(pattern=b"tg_send_(\\d+)"))
    add(tg_go_cb, events.CallbackQuery(pattern=b"tg_go_(\\d+)"))
    add(tg_stop_cb, events.CallbackQuery(pattern=b"tg_stop_(\\d+)"))
    # multi-account send (ported engine)
    multi.setup(panel_active=_active)
    add(tgm_open_cb, events.CallbackQuery(data=b"tgm_open"))
    add(tgm_page_cb, events.CallbackQuery(pattern=b"tgm_page_(\\d+)"))
    add(tgm_sel_cb, events.CallbackQuery(pattern=b"tgm_sel_(\\d+)"))
    add(tgm_go_cb, events.CallbackQuery(data=b"tgm_go"))
    add(tgm_jobs_cb, events.CallbackQuery(data=b"tgm_jobs"))
    add(tgm_stop_cb, events.CallbackQuery(pattern=b"tgm_stop_([a-f0-9]+)"))
    add(tgm_resume_cb, events.CallbackQuery(pattern=b"tgm_resume_([a-f0-9]+)"))
    add(_msg_router, events.NewMessage())
