# -*- coding: utf-8 -*-
"""
bale_panel.py — the BALE section of the customer bot (via aiobale).
===================================================================

A fully self-contained, decoupled module that adds a Bale user-account panel
ALONGSIDE the Rubika and Telegram sections. It NEVER touches their code paths:
own conversation state, own login clients, own DB tables (bale_accounts /
bale_settings via db.py).

Wired in by customer_bot.amain() calling ``bale_panel.setup(bot, rubika_state,
tg_state)`` once the shared Telethon bot client is created. All callbacks are
namespaced ``bale_*`` and its NewMessage router only acts when the user is in a
*Bale* conversation step (and defers if a Rubika/Telegram flow owns the user).

Design (per the proven Telegram section + the aiobale probe results):
  * Login IN-BOT (phone -> code -> optional 2FA) via aiobale's programmatic
    auth (start_phone_auth / validate_code / validate_password). NEVER the CLI.
  * Session stored as a per-account ``.bale`` FILE under data/bale_sessions/.
  * Connect ON DEMAND: each operation opens a client, does its work, closes it.
  * Recipients read via RAW requests (aiobale's pydantic models crash on bot
    dialogs / some groups); we extract peer id + type ourselves and FILTER OUT
    bots (from PVs) and channels (from groups).
  * Selectable target: contacts (default) / pv / groups / all.
  * Send loop mirrors the FIXED Telegram loop: CONSECUTIVE-error cap, per-send
    timeout, rate-limit cooldown, live progress, stop button.
  * EVERYTHING is logged to the central group AND the customer's own chat.

Runs on the MASTER (in-process). Nothing else is touched.
"""

import asyncio
import os
import time as _time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime

from telethon import events, Button

import config
import db
import logbus
import ratelimit
import forcedjoin

# aiobale is imported lazily in setup() so the module stays importable even if
# the package isn't installed yet (the rest of the bot keeps working).
Client = None
AuthErrors = ChatType = PeerType = GroupType = None
FileInput = None
LoadDialogs = GetContacts = None
SendMessage = MessageContent = TextMessage = None
SendType = DocumentMessage = MessageCaption = DocumentsExt = PhotoExt = None
_generate_id = None
_add_header = _clean_grpc = None
_BALE_CLIENTS = None   # ref to aiobale's global client registry (to avoid leaks)

LINE = logbus.LINE
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
BALE_SESS_DIR = os.path.join(DATA_DIR, "bale_sessions")
BALE_MEDIA_DIR = os.path.join(DATA_DIR, "bale_media")
os.makedirs(BALE_SESS_DIR, exist_ok=True)
os.makedirs(BALE_MEDIA_DIR, exist_ok=True)

bot = None

# Bale-only conversation state (separate from the other sections).
_state: dict = {}
# Login clients mid-flow: uid -> {"client","phone","tx"}
_pending: dict = {}
# manual stop flags: account_id -> True
_stop: dict = {}
# account_ids currently sending (busy guard)
_active: set = set()
# references to the other sections' state dicts (for mutual exclusion)
_rubika_state = None
_tg_state = None


def now() -> str:
    return config.now_str()


def card(title, rows):
    return logbus.card(title, rows)


def _sess_path(phone: str) -> str:
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    return os.path.join(BALE_SESS_DIR, f"{digits}.bale")


def _phone_int(phone: str) -> int:
    return int("".join(ch for ch in str(phone) if ch.isdigit()))


# Persian/Arabic digits -> ASCII (Bale rejects non-ASCII / mis-formatted numbers)
_DIGIT_MAP = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def _norm_phone(text: str) -> str:
    """Normalize user input to Bale's expected MSISDN: ASCII digits, country
    code 98, no leading 0 / 00 / '+'. Handles Persian/Arabic digits and the
    common Iranian formats (0930..., +98..., 0098..., 9..., 98...)."""
    s = (text or "").translate(_DIGIT_MAP)
    d = "".join(ch for ch in s if ch.isdigit())
    if d.startswith("00"):
        d = d[2:]
    if d.startswith("0"):
        d = "98" + d[1:]
    elif len(d) == 10 and d.startswith("9"):
        d = "98" + d
    return d


def _drop_client(client):
    """Remove a client from aiobale's global registry so connect-on-demand
    clients (which never call start()) don't pile up there (memory leak)."""
    try:
        if _BALE_CLIENTS is not None:
            _BALE_CLIENTS.discard(client)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Gate (free/time/block model — same shape as the Telegram section).
# --------------------------------------------------------------------------- #
async def _gate(event) -> bool:
    uid = event.sender_id
    if db.is_blocked(uid):
        return False
    # --- Bale section is TEMPORARILY DISABLED ---
    # Every Bale action goes through _gate, so short-circuiting here disables the
    # whole section (menu / send / add / health / content). It never touches
    # aiobale, so it can't block or crash the bot.
    await _respond(event, card("🔵 بخش بله", [
        "⚠️ بخش بله موقتاً غیرفعاله.",
        "بعداً دوباره فعال می‌شه — ممنون از صبرت.",
    ]))
    return False
    # (everything below is intentionally unreachable while disabled)
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


async def _safe_send(uid, text, buttons=None):
    try:
        return await bot.send_message(uid, text, buttons=buttons)
    except Exception:
        return None


def _logx(tag: str, exc: Exception):
    """Log an exception (with short traceback) to console — group logs are sent
    by the callers via logbus."""
    print(f"[bale] {tag}: {type(exc).__name__}: {exc}")
    print(traceback.format_exc())


# --------------------------------------------------------------------------- #
# Menu / UI helpers.
# --------------------------------------------------------------------------- #
def _menu():
    return [
        [Button.inline("🚀 ارسال", b"bale_accounts"),
         Button.inline("➕ افزودن اکانت", b"bale_addacc")],
        [Button.inline("👤 اکانت‌های من", b"bale_accounts"),
         Button.inline("🩺 چک‌حساب", b"bale_health")],
        [Button.inline("✍️ محتوا", b"bale_content"),
         Button.inline("⚙️ سرعت/تاخیر", b"bale_speed")],
        [Button.inline("🎯 مقصد ارسال", b"bale_target")],
        [Button.inline("📊 آمار من", b"bale_stats"),
         Button.inline("📖 راهنما", b"bale_help")],
        [Button.inline("🏠 منوی اصلی", b"mainmenu")],
    ]


def _back_home():
    return [[Button.inline("🔙 بله", b"bale_home"),
             Button.inline("🏠 منوی اصلی", b"mainmenu")]]


def _stop_btn(account_id):
    return [[Button.inline("⛔ توقف ارسال", f"bale_stop_{account_id}".encode())]]


def _target_label(mode: str) -> str:
    return {
        "contacts": "مخاطبین (دوطرفه‌ها)",
        "pv": "پیوی‌ها",
        "groups": "گروه‌ها",
        "all": "همه (مخاطبین + پیوی + گروه)",
    }.get(mode or "contacts", "مخاطبین (دوطرفه‌ها)")


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
    return card("🚀 بله › ارسال (زنده)", [
        f"📱 {acc['phone']}  ({acc.get('name') or '-'})",
        f"📊 {_bar(done, total)}",
        f"✅ موفق : {ok}    ❌ ناموفق : {fail}",
        f"🎯 کل گیرنده : {total}",
        f"🕒 {now()}",
    ])


# --------------------------------------------------------------------------- #
# Connection ON DEMAND: open a started client, do work, always close it.
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def _session(phone: str):
    client = Client(session_file=_sess_path(phone))
    # never trigger aiobale's interactive CLI fallback on a missing token
    if getattr(client, "_Client__token", None) is None:
        _drop_client(client)
        raise RuntimeError("no_session")
    try:
        yield client
    finally:
        try:
            s = client.session
            if s and not s.is_closed():
                await s.close()
        except Exception:
            pass
        _drop_client(client)


async def _raw_request(client, method, timeout: int = 30):
    """Fire a method over plain HTTP and return the RAW decoded dict (skips
    aiobale's pydantic models, which crash on bot dialogs / some groups)."""
    import aiohttp
    sess = client.session
    if sess.session is None or sess.session.closed:
        sess.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout), proxy=sess.proxy)
    token = getattr(client, "_Client__token", None)
    headers = {
        "User-Agent": sess.user_agent,
        "Origin": "https://web.bale.ai",
        "content-type": "application/grpc-web+proto",
    }
    try:
        headers.update({k[0].upper() + k[1:]: v for k, v in sess._get_meta().items()})
    except Exception:
        pass
    if token:
        headers.update(sess._build_headers(token))
    url = f"{sess.post_url}/{method.__service__}/{method.__method__}"
    data = method.model_dump(by_alias=True, exclude_none=True)
    payload = _add_header(sess.encoder(data))
    req = await sess.session.post(url=url, headers=headers, data=payload)
    content = await req.read()
    gm = req.headers.get("grpc-message")
    if gm is not None:
        raise RuntimeError(f"bale grpc: {gm}")
    return sess.decoder(_clean_grpc(content))


def _g(d, *keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
        if str(k) in d:
            return d[str(k)]
    return None


def _extract_dialog_peers(raw: dict):
    """raw['3'] = list of dialogs; peer at key '1' = {'1':type,'2':id};
    subtype at '13'.'1' (1=user, 2=group, 4=bot)."""
    dialogs = _g(raw, "3", 3) or []
    if isinstance(dialogs, dict):
        dialogs = [dialogs]
    out = []
    for d in dialogs:
        peer = _g(d, "1", 1) or {}
        ptype = _g(peer, "1", 1)
        pid = _g(peer, "2", 2)
        sub = _g(_g(d, "13", 13) or {}, "1", 1)
        if pid is None:
            continue
        out.append({"id": int(pid),
                    "type": int(ptype) if ptype is not None else 0,
                    "ctype": int(sub) if sub is not None else 0})
    return out


async def _is_channel(client, gid: int) -> bool:
    """Best-effort: True if the group is a CHANNEL (members can't post). If we
    can't determine it (aiobale model bug), assume it's a normal group."""
    try:
        fg = await client.get_full_group(int(gid))
        return int(getattr(fg, "group_type", 0)) == int(GroupType.CHANNEL)
    except Exception:
        return False


async def _collect_recipients(client, mode: str, me_id: int):
    """Return a de-duplicated list of {'id', 'ct'} (ct = ChatType) for the chosen
    target mode. Bots are dropped from PVs; channels are dropped from groups."""
    out = []

    if mode in ("contacts", "all"):
        try:
            rawc = await _raw_request(client, GetContacts())
            clist = _g(rawc, "3", 3) or []
            if isinstance(clist, dict):
                clist = [clist]
            for c in clist:
                cid = _g(c, "1", 1)
                if cid is not None and int(cid) != me_id:
                    out.append({"id": int(cid), "ct": ChatType.PRIVATE})
        except Exception as e:  # noqa: BLE001
            _logx("collect contacts", e)

    if mode in ("pv", "groups", "all"):
        raw = await _raw_request(
            client, LoadDialogs(offset_date=-1, limit=500, exclude_pinned=False))
        peers = _extract_dialog_peers(raw)
        if mode in ("pv", "all"):
            for p in peers:
                if (p["type"] == int(PeerType.PRIVATE)
                        and p["ctype"] != 4            # drop bots/services
                        and p["id"] != me_id):
                    out.append({"id": p["id"], "ct": ChatType.PRIVATE})
        if mode in ("groups", "all"):
            for p in peers:
                if p["type"] == int(PeerType.GROUP):
                    if not await _is_channel(client, p["id"]):   # drop channels
                        out.append({"id": p["id"], "ct": ChatType.GROUP})

    seen, uniq = set(), []
    for r in out:
        k = (r["id"], int(r["ct"]))
        if k not in seen:
            seen.add(k)
            uniq.append(r)
    return uniq


async def _counts(client, me_id):
    """Return (contacts, pv, groups) counts for the login summary (PVs exclude
    bots; groups counts type-2 dialogs)."""
    nc = npv = ng = 0
    try:
        rawc = await _raw_request(client, GetContacts())
        clist = _g(rawc, "3", 3) or []
        if isinstance(clist, dict):
            clist = [clist]
        nc = sum(1 for c in clist if _g(c, "1", 1) is not None)
    except Exception as e:  # noqa: BLE001
        _logx("counts contacts", e)
    try:
        raw = await _raw_request(
            client, LoadDialogs(offset_date=-1, limit=500, exclude_pinned=False))
        for p in _extract_dialog_peers(raw):
            if (p["type"] == int(PeerType.PRIVATE) and p["ctype"] != 4
                    and p["id"] != me_id):
                npv += 1
            elif p["type"] == int(PeerType.GROUP):
                ng += 1
    except Exception as e:  # noqa: BLE001
        _logx("counts dialogs", e)
    return nc, npv, ng


# --------------------------------------------------------------------------- #
# Home / cancel
# --------------------------------------------------------------------------- #
async def bale_home_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    _state.pop(uid, None)
    if _rubika_state is not None:
        _rubika_state.pop(uid, None)
    if _tg_state is not None:
        _tg_state.pop(uid, None)
    await _respond(event, card("🔵 پنل بله", [
        "اکانت بله‌ت رو اضافه کن و محتوا بفرست.",
        "یکی از گزینه‌ها رو انتخاب کن:",
    ]), buttons=_menu())


async def bale_cancel_cb(event):
    uid = event.sender_id
    p = _pending.pop(uid, None)
    if p:
        try:
            await p["client"].stop()
        except Exception:
            pass
        _drop_client(p["client"])
    _state.pop(uid, None)
    await _respond(event, "لغو شد.", buttons=_menu())


# --------------------------------------------------------------------------- #
# Add account (phone -> code -> optional 2FA) — aiobale programmatic auth.
# --------------------------------------------------------------------------- #
async def bale_addacc_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    # TEMPORARILY DISABLED: aiobale's login flow blocks the Telethon event loop
    # and crashes the entire customer bot (database-is-locked cascade). Login
    # must be run in a subprocess to be safe. Use the test script for now:
    #   systemctl stop goao-customer
    #   ./venv/bin/python bale_send_test.py   (or add manually via script)
    #   systemctl start goao-customer
    await _respond(event, card("➕ بله › افزودن اکانت", [
        "⚠️ افزودنِ اکانت بله موقتاً غیرفعاله (باعث ناپایداری ربات می‌شد).",
        "برای افزودن اکانت با پشتیبانی تماس بگیر.",
    ]), buttons=_back_home())
    return
    # --- original code below (will be re-enabled after subprocess fix) ---
    cap = config.BALE_MAX_ACCOUNTS
    if cap and db.count_customer_bale_accounts(uid) >= cap:
        await _respond(event, card("➕ افزودن اکانت", [
            f"به سقفِ {cap} اکانت رسیدی.",
            "برای افزایش با پشتیبانی تماس بگیر.",
        ]), buttons=_back_home())
        return
    _state[uid] = {"step": "bale_await_phone"}
    if _rubika_state is not None:
        _rubika_state.pop(uid, None)
    if _tg_state is not None:
        _tg_state.pop(uid, None)
    await _respond(event, card("➕ بله › افزودن اکانت", [
        "📱 شماره‌ی اکانت بله‌ت رو با کد کشور بفرست.",
        "مثال: `989121234567`",
    ]), buttons=[[Button.inline("🔙 لغو", b"bale_cancel")]])


async def _handle_phone(event, st):
    uid = event.sender_id
    phone = _norm_phone(event.raw_text)
    if not phone or len(phone) < 11 or not phone.startswith("98"):
        await event.respond("شماره نامعتبره. با کد کشور و انگلیسی بفرست، بدون ۰ و +.\n"
                            "مثال: `989121234567`")
        return
    client = Client(session_file=_sess_path(phone))
    try:
        resp = await asyncio.wait_for(
            client.start_phone_auth(_phone_int(phone)), timeout=30)
    except asyncio.TimeoutError:
        await event.respond("❌ بله پاسخ نداد (تایم‌اوت). بعداً دوباره امتحان کن.",
                            buttons=_menu())
        try:
            await client.stop()
        except Exception:
            pass
        _drop_client(client)
        _state.pop(uid, None)
        return
    except Exception as e:  # noqa: BLE001
        _logx("start_phone_auth", e)
        await event.respond(f"❌ خطا در ارسال کد: {repr(e)[:140]}")
        try:
            await client.stop()
        except Exception:
            pass
        _drop_client(client)
        return
    if isinstance(resp, AuthErrors):
        try:
            await client.stop()
        except Exception:
            pass
        _drop_client(client)
        name = getattr(resp, "name", "UNKNOWN")
        if name == "RATE_LIMIT":
            msg = ("⏳ بله درخواست کد برای این شماره رو موقتاً محدود کرده "
                   "(تعداد تلاش زیاد). چند دقیقه — گاهی تا یک ساعت — صبر کن، "
                   "بعد دوباره امتحان کن.")
        elif name == "INVALID":
            msg = ("❌ شماره نامعتبره یا روی بله ثبت نشده. درست بفرست: کد کشور + "
                   "شماره، انگلیسی، بدون ۰ و + (مثل `989121234567`).")
        else:
            msg = f"❌ ارسال کد ناموفق: {name}\nکمی بعد دوباره امتحان کن یا لغو کن."
        await event.respond(msg)
        return
    _pending[uid] = {"client": client, "phone": phone,
                     "tx": getattr(resp, "transaction_hash", None)}
    st["step"] = "bale_await_code"
    await event.respond(card("✉️ بله › کد تأیید", [
        "کدی که بله فرستاد رو بفرست.",
    ]), buttons=[[Button.inline("🔙 لغو", b"bale_cancel")]])


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
        res = await asyncio.wait_for(
            p["client"].validate_code(code, p["tx"]), timeout=30)
    except asyncio.TimeoutError:
        await event.respond("❌ بله بیش از حد طول کشید. لغو شد — بعداً دوباره امتحان کن.",
                            buttons=_menu())
        _pending.pop(uid, None)
        _state.pop(uid, None)
        try:
            await p["client"].stop()
        except Exception:
            pass
        _drop_client(p["client"])
        return
    except Exception as e:  # noqa: BLE001
        _logx("validate_code", e)
        await event.respond(f"❌ خطا در بررسی کد: {repr(e)[:140]}\nدوباره کد رو بفرست.")
        return
    if isinstance(res, AuthErrors):
        if res == AuthErrors.PASSWORD_NEEDED:
            st["step"] = "bale_await_password"
            await event.respond("🔐 این اکانت رمز دومرحله‌ای داره. رمز رو بفرست.",
                                buttons=[[Button.inline("🔙 لغو", b"bale_cancel")]])
            return
        if res == AuthErrors.SIGN_UP_NEEDED:
            _pending.pop(uid, None)
            _state.pop(uid, None)
            await event.respond("❌ این شماره تو بله ثبت‌نام نشده. اول تو اپ بله بساز.",
                                buttons=_menu())
            return
        await event.respond(f"❌ کد اشتباهه ({res.name}). دوباره بفرست یا لغو کن.")
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
    pwd = (event.raw_text or "").strip()
    try:
        res = await asyncio.wait_for(
            p["client"].validate_password(pwd, p["tx"]), timeout=30)
    except asyncio.TimeoutError:
        await event.respond("❌ بله بیش از حد طول کشید. لغو شد — بعداً دوباره امتحان کن.",
                            buttons=_menu())
        _pending.pop(uid, None)
        _state.pop(uid, None)
        try:
            await p["client"].stop()
        except Exception:
            pass
        _drop_client(p["client"])
        return
    except Exception as e:  # noqa: BLE001
        _logx("validate_password", e)
        await event.respond(f"❌ خطا در بررسی رمز: {repr(e)[:140]}\nدوباره رمز رو بفرست.")
        return
    if isinstance(res, AuthErrors):
        await event.respond(f"❌ رمز اشتباهه ({res.name}). دوباره بفرست یا لغو کن.")
        return
    await _finish_login(event)


async def _finish_login(event):
    uid = event.sender_id
    p = _pending.pop(uid, None)
    _state.pop(uid, None)
    if not p:
        return
    phone = p["phone"]
    client = p["client"]
    try:
        me = getattr(client, "me", None)
        uid_bale = getattr(me, "id", "-")
        name = getattr(me, "name", None) or "-"
    except Exception:
        uid_bale, name = "-", "-"
    # the .bale session file is already written by validate_code/password
    try:
        await client.stop()
    except Exception:
        pass
    _drop_client(client)

    aid = db.add_bale_account(uid, phone, name, "", str(uid_bale), _sess_path(phone))

    # read the account's contacts / PV / groups counts — shown to the customer
    # AND logged to the central group (for the owner).
    nc = npv = ng = -1
    try:
        async with _session(phone) as client:
            me_id = getattr(client, "id", None)
            nc, npv, ng = await asyncio.wait_for(
                _counts(client, me_id), timeout=30)
    except Exception as e:  # noqa: BLE001
        _logx("login counts", e)

    def _c(x):
        return str(x) if x is not None and x >= 0 else "نامشخص"

    await _respond(event, card("✅ اکانت بله اضافه شد", [
        f"📛 نام : {name}",
        f"📱 {phone}",
        f"👥 مخاطبین : {_c(nc)}",
        f"📥 پیوی‌ها : {_c(npv)}",
        f"👨‍👩‍👧 گروه‌ها : {_c(ng)}",
        LINE,
        "حالا «✍️ محتوا» رو تنظیم کن، «🎯 مقصد» رو انتخاب کن، بعد «🚀 ارسال».",
    ]), buttons=[[Button.inline("✍️ تنظیم محتوا", b"bale_content")],
                 [Button.inline("🔙 بله", b"bale_home")]])
    await logbus.event("➕ BALE ADD ACCOUNT", [
        f"🆔 Customer : {uid}", f"📱 {phone}  ({name})",
        f"👥 مخاطبین : {_c(nc)}   📥 پیوی : {_c(npv)}   👨‍👩‍👧 گروه : {_c(ng)}",
        f"🕒 {now()}"], pv_user=uid)


# --------------------------------------------------------------------------- #
# My accounts + detail + delete
# --------------------------------------------------------------------------- #
async def bale_accounts_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    accounts = db.list_bale_accounts(uid)
    if not accounts:
        await _respond(event, card("👤 بله › اکانت‌های من", ["هنوز اکانتی اضافه نکردی."]),
                       buttons=[[Button.inline("➕ افزودن اکانت", b"bale_addacc")],
                                [Button.inline("🔙 بله", b"bale_home")]])
        return
    rows = []
    for i, acc in enumerate(accounts, 1):
        emoji = "🟢" if acc.get("status") == "active" else "🔴"
        rows.append([Button.inline(f"{emoji} {i}- {acc['phone']}",
                                   f"bale_acc_{acc['id']}".encode())])
    rows.append([Button.inline("🔙 بله", b"bale_home"),
                 Button.inline("🏠 منوی اصلی", b"mainmenu")])
    await _respond(event, card("👤 بله › اکانت‌های من", ["یه اکانت رو انتخاب کن:"]),
                   buttons=rows)


async def bale_acc_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_bale_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    status = "فعال 🟢" if acc.get("status") == "active" else "غیرفعال 🔴 (سشن باطل)"
    await _respond(event, card(f"👤 بله › {acc['phone']}", [
        f"📛 نام : {acc.get('name') or '-'}",
        f"📱 شماره : {acc['phone']}",
        f"📅 افزوده‌شده : {acc.get('added_at') or '-'}",
        f"⭐️ وضعیت : {status}",
    ]), buttons=[
        [Button.inline("🚀 شروع ارسال", f"bale_send_{account_id}".encode())],
        [Button.inline("🩺 چک‌حساب", f"bale_chk_{account_id}".encode()),
         Button.inline("🗑 حذف", f"bale_del_{account_id}".encode())],
        [Button.inline("🔙 اکانت‌ها", b"bale_accounts")],
    ])


async def bale_del_cb(event):
    if not await _gate(event):
        return
    account_id = int(event.pattern_match.group(1))
    await _respond(event, "از حذف این اکانت مطمئنی؟",
                   buttons=[[Button.inline("✅ بله، حذف کن",
                                           f"bale_delyes_{account_id}".encode())],
                            [Button.inline("🔙 خیر", f"bale_acc_{account_id}".encode())]])


async def bale_delyes_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_bale_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    db.delete_bale_account(account_id)
    try:
        os.remove(_sess_path(acc["phone"]))
    except Exception:
        pass
    await _respond(event, "اکانت حذف شد. ✅",
                   buttons=[[Button.inline("🔙 اکانت‌ها", b"bale_accounts")]])
    await logbus.event("🗑 BALE DELETE ACCOUNT", [
        f"🆔 {uid}", f"📱 {acc['phone']}", f"🕒 {now()}"], pv_user=uid)


# --------------------------------------------------------------------------- #
# Health check
# --------------------------------------------------------------------------- #
async def _check_one(acc) -> dict:
    try:
        async with _session(acc["phone"]) as client:
            me = getattr(client, "me", None)
            if me is None:
                return {"ok": False, "reason": "سشن باطل/خارج‌شده"}
            return {"ok": True, "name": getattr(me, "name", "-")}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "reason": repr(e)[:80]}


async def bale_health_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    accounts = db.list_bale_accounts(uid)
    if not accounts:
        await _respond(event, card("🩺 چک‌حساب", ["اکانتی نداری."]), buttons=_back_home())
        return
    await _respond(event, "🩺 در حال بررسی اکانت‌ها ... کمی صبر کن.")
    asyncio.create_task(_run_health(uid, accounts))


async def _run_health(uid, accounts):
    rows = []
    for acc in accounts:
        if acc["id"] in _active:
            rows.append(f"• {acc['phone']} : 🟡 در حال ارسال — رد شد")
            continue
        r = await _check_one(acc)
        if r["ok"]:
            db.set_bale_status(acc["id"], "active")
            rows.append(f"• {acc['phone']} : 🟢 سالم ({r.get('name')})")
        else:
            db.set_bale_status(acc["id"], "inactive")
            rows.append(f"• {acc['phone']} : 🔴 {r.get('reason')}")
    await logbus.event("🩺 BALE HEALTH", [f"🆔 {uid}", *rows, f"🕒 {now()}"], pv_user=uid)
    await _safe_send(uid, card("🩺 بله › چک‌حساب", rows), buttons=_menu())


async def bale_chk_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_bale_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    if account_id in _active:
        await event.answer("روی این اکانت ارسال در حال اجراست.", alert=True)
        return
    await _respond(event, "🩺 در حال بررسی ...")
    r = await _check_one(acc)
    if r["ok"]:
        db.set_bale_status(account_id, "active")
        body = ["🟢 سالم", f"📛 {r.get('name')}", f"📱 {acc['phone']}"]
    else:
        db.set_bale_status(account_id, "inactive")
        body = [f"🔴 {r.get('reason')}", f"📱 {acc['phone']}",
                "اگه سشن باطله، اکانت رو دوباره اضافه کن."]
    await _respond(event, card("🩺 چک‌حساب", body),
                   buttons=[[Button.inline("🔙 اکانت", f"bale_acc_{account_id}".encode())]])


# --------------------------------------------------------------------------- #
# Content (text / photo / file)
# --------------------------------------------------------------------------- #
async def bale_content_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    s = db.get_bale_settings(uid)
    _state[uid] = {"step": "bale_await_content"}
    if _rubika_state is not None:
        _rubika_state.pop(uid, None)
    if _tg_state is not None:
        _tg_state.pop(uid, None)
    await _respond(event, card("✍️ بله › محتوا", [
        "محتوای فعلی:",
        _content_summary(s),
        LINE,
        "محتوای جدید رو بفرست (متن، یا عکس/فایل با کپشن دلخواه).",
    ]), buttons=[[Button.inline("🔙 لغو", b"bale_cancel")]])


async def _handle_content(event, st):
    uid = event.sender_id
    msg = event.message
    cap = (msg.text or "").strip()
    try:
        if msg.photo:
            path = await msg.download_media(file=BALE_MEDIA_DIR)
            db.set_bale_content(uid, "photo", msg.text or None, path)
            label = "🖼 عکس"
        elif msg.document:
            path = await msg.download_media(file=BALE_MEDIA_DIR)
            db.set_bale_content(uid, "file", msg.text or None, path)
            label = "📎 فایل"
        elif msg.text:
            db.set_bale_content(uid, "text", msg.text, None)
            label = "📝 متن"
        else:
            await event.respond("❌ این نوع محتوا پشتیبانی نمی‌شه. متن، عکس یا فایل بفرست.")
            return
    except Exception as e:  # noqa: BLE001
        await event.respond(f"❌ خطا در ذخیرهٔ محتوا: {repr(e)[:120]}")
        return
    _state.pop(uid, None)
    confirm = [f"{label} به‌عنوان محتوای ارسالی ثبت شد."]
    if cap:
        confirm += [LINE, "📝 متنِ ذخیره‌شده:", cap]
    await event.respond(card("✅ محتوا ذخیره شد", confirm), buttons=_menu())
    log_rows = [f"🆔 {uid}", f"📦 نوع : {label}"]
    log_rows.append(f"📝 متن : {cap[:900]}" if cap else "📝 متن : (بدون متن/کپشن)")
    log_rows.append(f"🕒 {now()}")
    await logbus.event("✍️ BALE CONTENT SET", log_rows, pv_user=uid)


# --------------------------------------------------------------------------- #
# Speed / delay
# --------------------------------------------------------------------------- #
async def bale_speed_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    cur = db.get_bale_delay(uid)
    rows = [[Button.inline(("✅ " if abs(cur - v) < 0.01 else "") + f"{v}s",
                           f"bale_spd_{v}".encode())]
            for v in (1, 2, 3, 5, 10)]
    rows.append([Button.inline("🔙 بله", b"bale_home")])
    await _respond(event, card("⚙️ بله › سرعت/تاخیر ارسال", [
        f"تاخیر فعلی بین هر ارسال : {cur}s",
        "هرچه بیشتر، امن‌تر (کمتر محدودیت).",
    ]), buttons=rows)


async def bale_spd_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    val = event.pattern_match.group(1).decode()
    db.set_bale_delay(uid, float(val))
    await bale_speed_cb(event)


# --------------------------------------------------------------------------- #
# Target mode (contacts / pv / groups / all) — selectable.
# --------------------------------------------------------------------------- #
async def bale_target_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    cur = db.get_bale_settings(uid).get("target_mode") or "contacts"

    def _mk(m, label):
        return Button.inline(("✅ " if cur == m else "") + label,
                             f"bale_tgt_{m}".encode())

    rows = [
        [_mk("contacts", "👥 مخاطبین (دوطرفه‌ها)")],
        [_mk("pv", "📥 پیوی‌ها")],
        [_mk("groups", "👨‍👩‍👧 گروه‌ها")],
        [_mk("all", "📦 همه")],
        [Button.inline("🔙 بله", b"bale_home")],
    ]
    await _respond(event, card("🎯 بله › مقصد ارسال", [
        f"مقصد فعلی : {_target_label(cur)}",
        LINE,
        "محتوا به کجا ارسال بشه؟ (بات‌ها و کانال‌ها خودکار حذف می‌شن)",
    ]), buttons=rows)


async def bale_tgt_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    mode = event.data.decode().rsplit("_", 1)[-1]
    db.set_bale_target_mode(uid, mode)
    await logbus.event("🎯 BALE TARGET MODE", [
        f"🆔 {uid}", f"مقصد : {_target_label(mode)}", f"🕒 {now()}"], pv_user=uid)
    await bale_target_cb(event)


# --------------------------------------------------------------------------- #
# Stats / help
# --------------------------------------------------------------------------- #
async def bale_stats_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    s = db.get_bale_settings(uid)
    n_acc = db.count_customer_bale_accounts(uid)
    await _respond(event, card("📊 بله › آمار من", [
        f"👤 اکانت‌های بله : {n_acc}",
        f"📤 کل ارسال‌ها : {int(s.get('total_sends') or 0)}",
        f"📦 محتوا : {'تنظیم‌شده ✅' if s.get('content_type') else 'تنظیم‌نشده ❌'}",
        f"🎯 مقصد : {_target_label(s.get('target_mode'))}",
        f"⚙️ تاخیر : {config.clamp_bale_delay(s.get('send_delay'))}s",
    ]), buttons=_back_home())


async def bale_help_cb(event):
    if not await _gate(event):
        return
    await _respond(event, card("📖 راهنمای بخش بله", [
        "➕ افزودن اکانت : شماره → کد → (در صورت لزوم) رمز دومرحله‌ای.",
        "✍️ محتوا : متن یا عکس/فایل با کپشن که ارسال می‌شه.",
        "🎯 مقصد ارسال : مخاطبین / پیوی‌ها / گروه‌ها / همه.",
        "🚀 ارسال : محتوا به مقصد انتخابی می‌ره؛ پیشرفت زنده + دکمهٔ توقف.",
        f"   فقط اگه به {config.BALE_MAX_ERRORS} خطای پیاپی برسه متوقف می‌شه.",
        "🩺 چک‌حساب : زنده‌بودن سشنِ اکانت‌ها.",
        "⚙️ سرعت/تاخیر : فاصلهٔ بین ارسال‌ها (برای کم‌کردن محدودیت).",
        LINE,
        "⚠️ ارسالِ انبوه ممکنه باعث محدودیتِ موقتِ اکانت توسط بله بشه؛ "
        "تاخیر مناسب بذار.",
    ]), buttons=_back_home())


# --------------------------------------------------------------------------- #
# Send: confirm -> run (connect on demand, send loop)
# --------------------------------------------------------------------------- #
async def bale_send_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_bale_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    s = db.get_bale_settings(uid)
    if not s.get("content_type"):
        await event.answer("اول محتوا رو تنظیم کن.", alert=True)
        return
    await _respond(event, card("🚀 بله › تأیید ارسال", [
        f"📱 اکانت : {acc['phone']}",
        "محتوایی که ارسال می‌شه:",
        _content_summary(s),
        LINE,
        f"🎯 مقصد : {_target_label(s.get('target_mode') or 'contacts')}",
        "مطمئنی؟ (از «🎯 مقصد ارسال» می‌تونی عوضش کنی)",
    ]), buttons=[[Button.inline("✅ بله، شروع کن", f"bale_go_{account_id}".encode())],
                 [Button.inline("🔙 خیر", f"bale_acc_{account_id}".encode())]])


async def bale_go_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    account_id = int(event.pattern_match.group(1))
    acc = db.get_bale_account_owned(account_id, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    if not db.get_bale_settings(uid).get("content_type"):
        await event.answer("اول محتوا رو تنظیم کن.", alert=True)
        return
    if _active:
        # Bale is heavy (it took the bot offline): allow only ONE Bale send at a
        # time across ALL customers. Others must wait.
        await event.answer("⏳ یه کاربر دیگه همین الان داره از بخش بله ارسال می‌کنه. "
                           "بله سنگینه و هم‌زمان فقط یک ارسال ممکنه — چند لحظه بعد "
                           "دوباره امتحان کن.", alert=True)
        return
    _active.add(account_id)
    _stop[account_id] = False
    await _respond(event, card("🚀 بله › ارسال", [
        "✅ شروع شد. پیشرفت در پیامِ پایین نشون داده می‌شه."]))
    pm = await bot.send_message(uid, card("🚀 بله › ارسال (زنده)", [
        f"📱 {acc['phone']}", "⏳ آماده‌سازی ..."]), buttons=_stop_btn(account_id))
    asyncio.create_task(_run_send(uid, acc, pm.id))


async def bale_stop_cb(event):
    account_id = int(event.pattern_match.group(1))
    _stop[account_id] = True
    await event.answer("درخواست توقف ثبت شد. بعد از ارسالِ جاری متوقف می‌شه.", alert=True)


def _is_rate_error(e: Exception) -> bool:
    s = repr(e).upper()
    return any(k in s for k in ("FLOOD", "RATE", "TOO MANY", "TOO_REQUESTS",
                                "LIMIT", "SLOWMODE", "SLOW_MODE", "MANY_REQUESTS"))


async def _sleep_or_stop(account_id, seconds: float, step: float = 2.0) -> bool:
    waited = 0.0
    while waited < seconds:
        if _stop.get(account_id):
            return True
        d = min(step, seconds - waited)
        await asyncio.sleep(d)
        waited += d
    return False


async def _send_one(client, r, s):
    cid = int(r["id"])
    ctype = r["ct"]
    text = s.get("content_text") or ""
    ct = s.get("content_type")
    path = s.get("media_path")
    chat = client._build_chat(cid, ctype)
    peer = client._resolve_peer(chat)
    if ct in ("photo", "file") and path:
        # PHOTO/FILE: upload the file (this path works connection-less), then
        # build the DocumentMessage exactly like aiobale's _send_file_message
        # does and send it via the SAME raw path used for text/reads — which
        # bypasses aiobale's response parser that crashes without a started
        # client. Proven working end-to-end.
        is_photo = ct == "photo"
        send_type = SendType.PHOTO if is_photo else SendType.DOCUMENT
        fi = await client.upload_file(file=FileInput(path), chat_id=cid,
                                      chat_type=ctype, send_type=send_type)
        caption = MessageCaption(content=text) if text else None
        ext = DocumentsExt(photo=PhotoExt(w=1000, h=1000)) if is_photo else None
        document = DocumentMessage(
            file_id=fi.file_id, size=fi.size, name=fi.name,
            mime_type=fi.mime_type, access_hash=fi.access_hash,
            caption=caption, thumb=None, ext=ext)
        content = MessageContent(document=document)
    else:
        # TEXT
        content = MessageContent(text=TextMessage(value=text))
    call = SendMessage(peer=peer, message_id=_generate_id(),
                       content=content, chat=chat)
    await _raw_request(client, call)


async def _run_send(uid, acc, msg_id):
    account_id = acc["id"]
    phone = acc["phone"]
    s = db.get_bale_settings(uid)
    delay = config.clamp_bale_delay(s.get("send_delay"))
    mode = s.get("target_mode") or "contacts"
    ok = fail = total = 0
    stopped = hit_max = False
    rate_stop = False
    fatal = False
    started = datetime.now()

    try:
        async with _session(phone) as client:
            me_id = getattr(client, "id", None)
            recipients = await _collect_recipients(client, mode, me_id)
            total = len(recipients)

            await logbus.event("🚀 BALE SEND START", [
                f"🆔 Customer : {uid}", f"📱 Phone : {phone}",
                f"🎯 مقصد : {_target_label(mode)}",
                f"🎯 گیرنده : {total}", f"⏱ تأخیر : {delay}s",
                f"🕒 {now()}"], pv_user=uid)
            await _safe_edit(uid, msg_id, _progress_card(acc, 0, 0, total, 0),
                             buttons=_stop_btn(account_id))

            if total == 0:
                await _safe_edit(uid, msg_id, card("ℹ️ گیرنده‌ای پیدا نشد", [
                    f"📱 {phone}", f"🎯 {_target_label(mode)}",
                    "مقصد دیگه‌ای انتخاب کن یا مطمئن شو مخاطب/گروه داری."]),
                    buttons=_menu())
                return

            last_edit = 0.0
            consec_fail = 0
            rate_hits = 0
            for i, r in enumerate(recipients, 1):
                if _stop.get(account_id):
                    stopped = True
                    break
                try:
                    await asyncio.wait_for(_send_one(client, r, s),
                                           timeout=config.BALE_SEND_TIMEOUT)
                    ok += 1
                    consec_fail = 0
                except Exception as e:  # noqa: BLE001
                    if _is_rate_error(e):
                        rate_hits += 1
                        if rate_hits > config.BALE_MAX_RATE_HITS:
                            rate_stop = True
                            break
                        await logbus.event("⏸ BALE محدودیت", [
                            f"🆔 {uid}", f"📱 {phone}",
                            f"⏳ بله محدودیت گذاشت — {config.BALE_RATE_COOLDOWN}s صبر و ادامه",
                            f"📊 ✅ {ok}  ❌ {fail}  از {total}", f"🕒 {now()}"],
                            pv_user=uid)
                        await _safe_edit(uid, msg_id, card("⏸ بله › محدودیت موقت", [
                            f"📱 {phone}",
                            f"⏳ بله محدودیت گذاشت؛ {config.BALE_RATE_COOLDOWN} ثانیه صبر.",
                            "بعدش خودکار ادامه می‌ده.",
                            f"📊 ✅ {ok}   ❌ {fail}   از {total}"]),
                            buttons=_stop_btn(account_id))
                        if await _sleep_or_stop(account_id, config.BALE_RATE_COOLDOWN):
                            stopped = True
                            break
                        try:
                            await asyncio.wait_for(_send_one(client, r, s),
                                                   timeout=config.BALE_SEND_TIMEOUT)
                            ok += 1
                            consec_fail = 0
                        except Exception:  # noqa: BLE001
                            fail += 1
                            consec_fail += 1
                    else:
                        fail += 1
                        consec_fail += 1
                        await logbus.to_group(card("❌ BALE SEND ERROR", [
                            f"📱 {phone}", f"🆔 {uid}", f"🎯 {r['id']}",
                            f"💥 {repr(e)[:160]}"]))
                if consec_fail >= config.BALE_MAX_ERRORS:
                    hit_max = True
                    break
                t = _time.time()
                if t - last_edit >= 2:
                    last_edit = t
                    await _safe_edit(uid, msg_id,
                                     _progress_card(acc, ok, fail, total, i),
                                     buttons=_stop_btn(account_id))
                await asyncio.sleep(delay)
    except Exception as e:  # noqa: BLE001
        _logx("run_send", e)
        await logbus.event("❌ BALE SEND FATAL", [
            f"🆔 {uid}", f"📱 {phone}", f"💥 {repr(e)[:160]}",
            "اگه سشن باطله، اکانت رو دوباره اضافه کن.", f"🕒 {now()}"], pv_user=uid)
        await _safe_edit(uid, msg_id, card("❌ خطا در ارسال", [
            f"📱 {phone}", f"💥 {repr(e)[:140]}"]), buttons=_menu())
        fatal = True
    finally:
        _active.discard(account_id)
        _stop.pop(account_id, None)

    if fatal:
        return
    if ok:
        db.incr_bale_sends(uid, ok)

    dur = int((datetime.now() - started).total_seconds())
    if rate_stop:
        head = "🛑 BALE SEND STOPPED (محدودیت)"
        note = "بله چند بار پشت‌سرهم محدودیت گذاشت. بعداً دوباره بزن یا تأخیر رو بیشتر کن."
    elif hit_max:
        head = "🛑 BALE SEND STOPPED (سقف خطا)"
        note = f"به {config.BALE_MAX_ERRORS} خطای پیاپی رسید و متوقف شد."
    elif stopped:
        head = "🛑 BALE SEND STOPPED (توسط کاربر)"
        note = "ارسال به‌درخواستِ کاربر متوقف شد."
    else:
        head = "🏁 BALE SEND FINISHED"
        note = "ارسال کامل شد."
    rows = [f"🆔 {uid}", f"📱 {phone}", note,
            f"✅ موفق : {ok}    ❌ ناموفق : {fail}", f"🎯 کل : {total}",
            f"⏱ {dur}s    🕒 {now()}"]
    await _safe_edit(uid, msg_id, card(head, rows[1:]),
                     buttons=[[Button.inline("🔙 بله", b"bale_home")]])
    await logbus.event(head, rows, pv_user=uid)


# --------------------------------------------------------------------------- #
# NewMessage router (only acts on Bale conversation steps).
# --------------------------------------------------------------------------- #
async def _msg_router(event):
    uid = event.sender_id
    if db.is_blocked(uid):
        return
    if (event.raw_text or "").startswith("/"):
        return
    st = _state.get(uid)
    if not st:
        return
    # defer if a Rubika/Telegram flow owns this user (handled exactly once)
    if _rubika_state is not None and _rubika_state.get(uid):
        return
    if _tg_state is not None and _tg_state.get(uid):
        return
    if db.maintenance_on():
        await event.respond("🛠 ربات در حال تعمیر است.")
        return
    user = await event.get_sender()
    if not await ratelimit.guard(uid, getattr(user, "first_name", "") or ""):
        await event.respond("⛔ به‌خاطر فعالیت بیش از حد، حساب شما مسدود شد.")
        return
    step = st.get("step")
    if step == "bale_await_phone":
        await _handle_phone(event, st)
    elif step == "bale_await_code":
        await _handle_code(event, st)
    elif step == "bale_await_password":
        await _handle_password(event, st)
    elif step == "bale_await_content":
        await _handle_content(event, st)


# --------------------------------------------------------------------------- #
# Wiring
# --------------------------------------------------------------------------- #
def setup(shared_bot, rubika_state=None, tg_state=None):
    """Register all Bale handlers on the shared bot. Called once from
    customer_bot.amain(). rubika_state / tg_state are the other sections' state
    dicts (for cross-section mutual exclusion)."""
    global bot, _rubika_state, _tg_state
    global Client, AuthErrors, ChatType, PeerType, GroupType, FileInput
    global LoadDialogs, GetContacts, _add_header, _clean_grpc, _BALE_CLIENTS
    global SendMessage, MessageContent, TextMessage, _generate_id
    global SendType, DocumentMessage, MessageCaption, DocumentsExt, PhotoExt

    bot = shared_bot
    _rubika_state = rubika_state
    _tg_state = tg_state

    try:
        from aiobale import Client as _Client
        from aiobale.enums import (AuthErrors as _AE, ChatType as _CT,
                                    PeerType as _PT, GroupType as _GT,
                                    SendType as _ST)
        from aiobale.types import (FileInput as _FI, MessageContent as _MC,
                                    TextMessage as _TM, DocumentMessage as _DM,
                                    MessageCaption as _MCAP, DocumentsExt as _DE,
                                    PhotoExt as _PE)
        from aiobale.methods import (LoadDialogs as _LD, GetContacts as _GC,
                                     SendMessage as _SM)
        from aiobale.utils import add_header as _AH, clean_grpc as _CG
        try:
            from aiobale.utils import generate_id as _GID
        except Exception:  # noqa: BLE001
            import random
            def _GID():
                return random.randint(1, 2 ** 31)
    except Exception as e:  # noqa: BLE001
        print(f"[bale] aiobale not available, Bale section disabled: {e!r}")
        return False

    Client, AuthErrors, ChatType, PeerType, GroupType, FileInput = (
        _Client, _AE, _CT, _PT, _GT, _FI)
    LoadDialogs, GetContacts = _LD, _GC
    SendMessage, MessageContent, TextMessage, _generate_id = _SM, _MC, _TM, _GID
    SendType, DocumentMessage, MessageCaption, DocumentsExt, PhotoExt = (
        _ST, _DM, _MCAP, _DE, _PE)
    _add_header, _clean_grpc = _AH, _CG
    try:
        from aiobale.client.client import _CLIENTS as _BC
        _BALE_CLIENTS = _BC
    except Exception:
        _BALE_CLIENTS = None

    add = bot.add_event_handler
    add(bale_home_cb, events.CallbackQuery(data=b"bale_home"))
    add(bale_cancel_cb, events.CallbackQuery(data=b"bale_cancel"))
    add(bale_addacc_cb, events.CallbackQuery(data=b"bale_addacc"))
    add(bale_accounts_cb, events.CallbackQuery(data=b"bale_accounts"))
    add(bale_acc_cb, events.CallbackQuery(pattern=b"bale_acc_(\\d+)"))
    add(bale_del_cb, events.CallbackQuery(pattern=b"bale_del_(\\d+)"))
    add(bale_delyes_cb, events.CallbackQuery(pattern=b"bale_delyes_(\\d+)"))
    add(bale_health_cb, events.CallbackQuery(data=b"bale_health"))
    add(bale_chk_cb, events.CallbackQuery(pattern=b"bale_chk_(\\d+)"))
    add(bale_content_cb, events.CallbackQuery(data=b"bale_content"))
    add(bale_speed_cb, events.CallbackQuery(data=b"bale_speed"))
    add(bale_spd_cb, events.CallbackQuery(pattern=b"bale_spd_([0-9.]+)"))
    add(bale_target_cb, events.CallbackQuery(data=b"bale_target"))
    add(bale_tgt_cb, events.CallbackQuery(pattern=b"bale_tgt_(contacts|pv|groups|all)"))
    add(bale_stats_cb, events.CallbackQuery(data=b"bale_stats"))
    add(bale_help_cb, events.CallbackQuery(data=b"bale_help"))
    add(bale_send_cb, events.CallbackQuery(pattern=b"bale_send_(\\d+)"))
    add(bale_go_cb, events.CallbackQuery(pattern=b"bale_go_(\\d+)"))
    add(bale_stop_cb, events.CallbackQuery(pattern=b"bale_stop_(\\d+)"))
    add(_msg_router, events.NewMessage())
    print("[bale] Bale section wired up.")
    return True
