"""
adbot.py — the "مدیریت تبچی‌ها" (group-advertiser) section of the OWNER panel.
=============================================================================

A fully self-contained, decoupled module that adds an owner-only advertiser
panel to owner_bot.py. It NEVER touches the customer Telegram/Rubika code
paths: it owns its own conversation state, its own login clients and its own
DB tables (adbot_accounts / adbot_settings), stored in the SAME sqlite file
(db.DB_PATH) but with names that never collide with the customer tables.

What it does (kept intentionally simple and robust):

  * The owner adds Telegram USER accounts ("تبچی") by phone → code → 2FA.
  * On add, the account's group list is fetched and LOGGED to the central group
    so the owner can confirm the groups were read.
  * The owner sets ONE advertising text and a cycle interval (5/10/15 minutes).
  * A background engine, every cycle, makes EACH active account post that text
    to ALL the groups it is a member of. Accounts are STAGGERED by ~30 seconds
    from one another so they never fire at the same instant. Three or four run
    safely because each account uses its OWN session/client (fully isolated).
  * If an account is banned/deactivated/session-revoked, it is marked and NEVER
    retried, and that event is logged to the central group.

All callbacks are namespaced ``adb_*`` and its NewMessage router only acts when
the owner is inside an *advertiser* conversation step, so it never clashes with
owner_bot's own text_router (they use separate state dicts).
"""
import asyncio
import contextlib
import os
import sqlite3
import time

from telethon import events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PhoneNumberInvalidError,
    FloodWaitError,
)

import config
import db
import logbus

# Telethon client class is imported lazily in setup() (mirrors tg_panel).
TelegramClient = None  # set in setup()

LINE = logbus.LINE

bot = None                     # the owner Telethon bot client (injected by setup)
_owner_state = None            # owner_bot.state (for cross-flow mutual exclusion)

# Advertiser-only conversation state: uid -> {"step": ...}
_state: dict = {}
# Login clients mid-flow: uid -> {"client","phone","hash"}
_pending: dict = {}
# Accounts currently sending this cycle (extra guard against double-run).
_running: set = set()
# The single background engine task.
_engine_task = None

# Defaults (overridable via config / .env).
_DEF_INTERVAL = int(getattr(config, "ADBOT_INTERVAL_MIN", 10))
_DEF_STAGGER = int(getattr(config, "ADBOT_STAGGER_SEC", 30))
_GROUP_DELAY = float(getattr(config, "ADBOT_GROUP_DELAY", 3.0))
_MAX_ACCOUNTS = int(getattr(config, "ADBOT_MAX_ACCOUNTS", 0))
_FLOOD_MAX = int(getattr(config, "TG_FLOODWAIT_MAX", 300))
_INTERVAL_CHOICES = (5, 10, 15)


def now() -> str:
    return config.now_str()


def card(title, rows):
    return logbus.card(title, rows)


def _is_owner(event) -> bool:
    return bool(config.OWNER_ID) and event.sender_id == config.OWNER_ID


# --------------------------------------------------------------------------- #
# Database (self-contained; own tables in the shared sqlite file).
# --------------------------------------------------------------------------- #
def _connect() -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(db.DB_PATH))
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db.DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _init() -> None:
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS adbot_accounts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                phone        TEXT,
                name         TEXT,
                username     TEXT,
                user_id      TEXT,
                session      TEXT,
                status       TEXT NOT NULL DEFAULT 'active',
                groups_count INTEGER NOT NULL DEFAULT 0,
                sent_total   INTEGER NOT NULL DEFAULT 0,
                last_error   TEXT DEFAULT '',
                last_run_at  TEXT,
                added_at     TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS adbot_settings (
                id           INTEGER PRIMARY KEY CHECK (id = 1),
                ad_text      TEXT DEFAULT '',
                interval_min INTEGER NOT NULL DEFAULT 10,
                stagger_sec  INTEGER NOT NULL DEFAULT 30,
                enabled      INTEGER NOT NULL DEFAULT 0,
                updated_at   TEXT
            )
        """)
        conn.execute(
            "INSERT OR IGNORE INTO adbot_settings(id,ad_text,interval_min,stagger_sec,enabled,updated_at) "
            "VALUES(1,'',?,?,0,?)", (_DEF_INTERVAL, _DEF_STAGGER, now()))


def _get_settings() -> dict:
    _init()
    with _connect() as conn:
        row = conn.execute("SELECT * FROM adbot_settings WHERE id=1").fetchone()
    if not row:
        return {"ad_text": "", "interval_min": _DEF_INTERVAL,
                "stagger_sec": _DEF_STAGGER, "enabled": 0}
    return dict(row)


def _set_settings(**kw) -> None:
    if not kw:
        return
    _init()
    cols = []
    vals = []
    for key in ("ad_text", "interval_min", "stagger_sec", "enabled"):
        if key in kw:
            cols.append(f"{key}=?")
            vals.append(kw[key])
    if not cols:
        return
    cols.append("updated_at=?")
    vals.append(now())
    with _connect() as conn:
        conn.execute(f"UPDATE adbot_settings SET {', '.join(cols)} WHERE id=1", tuple(vals))


def _enabled() -> bool:
    return bool(_get_settings().get("enabled"))


def _list_accounts() -> list:
    _init()
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM adbot_accounts ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def _get_account(account_id: int):
    _init()
    with _connect() as conn:
        row = conn.execute("SELECT * FROM adbot_accounts WHERE id=?",
                           (int(account_id),)).fetchone()
    return dict(row) if row else None


def _add_account(phone: str, name: str, username: str, user_id: str,
                 session: str, groups_count: int) -> int:
    _init()
    with _connect() as conn:
        row = conn.execute("SELECT id FROM adbot_accounts WHERE phone=?",
                           (phone,)).fetchone()
        if row:
            conn.execute(
                "UPDATE adbot_accounts SET name=?,username=?,user_id=?,session=?,"
                "status='active',groups_count=?,last_error='' WHERE id=?",
                (name, username, str(user_id), session, int(groups_count), int(row["id"])))
            return int(row["id"])
        cur = conn.execute(
            "INSERT INTO adbot_accounts(phone,name,username,user_id,session,status,"
            "groups_count,added_at) VALUES(?,?,?,?,?,'active',?,?)",
            (phone, name, username, str(user_id), session, int(groups_count), now()))
        return int(cur.lastrowid)


def _set_status(account_id: int, status: str, last_error: str = "") -> None:
    _init()
    with _connect() as conn:
        conn.execute(
            "UPDATE adbot_accounts SET status=?,last_error=? WHERE id=?",
            (status, (last_error or "")[:240], int(account_id)))


def _mark_run(account_id: int, sent: int, groups_count: int) -> None:
    _init()
    with _connect() as conn:
        conn.execute(
            "UPDATE adbot_accounts SET sent_total=sent_total+?,groups_count=?,"
            "last_run_at=?,last_error='' WHERE id=?",
            (int(sent), int(groups_count), now(), int(account_id)))


def _delete_account(account_id: int) -> None:
    _init()
    with _connect() as conn:
        conn.execute("DELETE FROM adbot_accounts WHERE id=?", (int(account_id),))


# --------------------------------------------------------------------------- #
# Logging helper.
# --------------------------------------------------------------------------- #
async def _log(title: str, rows: list) -> None:
    with contextlib.suppress(Exception):
        await logbus.to_group(logbus.card(title, list(rows) + [f"🕒 {now()}"]))


# --------------------------------------------------------------------------- #
# UI helpers.
# --------------------------------------------------------------------------- #
_STATUS_FA = {"active": "🟢 فعال", "banned": "⛔ بن‌شده",
              "dead": "🔴 سشن باطل"}


def _status_fa(s: str) -> str:
    return _STATUS_FA.get(s, s or "-")


def _menu():
    s = _get_settings()
    en = bool(s.get("enabled"))
    toggle = (Button.inline("⏹ توقف ارسال خودکار", b"adb_stop") if en
              else Button.inline("▶️ شروع ارسال خودکار", b"adb_start"))
    return [
        [Button.inline("➕ افزودن اکانت تبچی", b"adb_addacc")],
        [Button.inline("👤 اکانت‌های تبچی", b"adb_accounts")],
        [Button.inline("📝 متن تبلیغ", b"adb_text"),
         Button.inline("⏱ بازهٔ زمانی", b"adb_interval")],
        [toggle],
        [Button.inline("📊 وضعیت", b"adb_status")],
        [Button.inline("🔙 بازگشت", b"home")],
    ]


def _home_text() -> str:
    s = _get_settings()
    accounts = _list_accounts()
    active = sum(1 for a in accounts if a.get("status") == "active")
    txt = (s.get("ad_text") or "").strip()
    txt_state = "✅ تنظیم‌شده" if txt else "❌ تنظیم‌نشده"
    run_state = "🟢 روشن (در حال ارسال)" if s.get("enabled") else "⏸ خاموش"
    return card("📢 مدیریت تبچی‌ها", [
        f"👤 اکانت‌ها : {len(accounts)}  (فعال: {active})",
        f"📝 متن تبلیغ : {txt_state}",
        f"⏱ بازه : هر {int(s.get('interval_min') or _DEF_INTERVAL)} دقیقه"
        f"  |  اختلاف اکانت‌ها : {int(s.get('stagger_sec') or _DEF_STAGGER)}s",
        f"⚙️ وضعیت ارسال خودکار : {run_state}",
        LINE,
        "اکانت اضافه کن، متن و بازه رو تنظیم کن، بعد «شروع ارسال خودکار» رو بزن.",
    ])


async def _respond(event, text, buttons=None):
    try:
        await event.edit(text, buttons=buttons)
    except Exception:
        with contextlib.suppress(Exception):
            await bot.send_message(event.sender_id, text, buttons=buttons)


# --------------------------------------------------------------------------- #
# Home / cancel.
# --------------------------------------------------------------------------- #
async def adb_home_cb(event):
    if not _is_owner(event):
        return
    _state.pop(event.sender_id, None)
    if _owner_state is not None:
        _owner_state.pop(event.sender_id, None)   # leave any owner flow
    await _respond(event, _home_text(), buttons=_menu())


async def adb_cancel_cb(event):
    if not _is_owner(event):
        return
    uid = event.sender_id
    p = _pending.pop(uid, None)
    if p:
        with contextlib.suppress(Exception):
            await p["client"].disconnect()
    _state.pop(uid, None)
    await _respond(event, "لغو شد.", buttons=_menu())


# --------------------------------------------------------------------------- #
# Add account (phone -> code -> optional 2FA).
# --------------------------------------------------------------------------- #
async def adb_addacc_cb(event):
    if not _is_owner(event):
        return
    uid = event.sender_id
    if _MAX_ACCOUNTS and len(_list_accounts()) >= _MAX_ACCOUNTS:
        await event.answer(f"به سقف {_MAX_ACCOUNTS} اکانت رسیدی.", alert=True)
        return
    if _owner_state is not None:
        _owner_state.pop(uid, None)
    _state[uid] = {"step": "adb_await_phone"}
    await _respond(event, card("➕ تبچی › افزودن اکانت", [
        "📱 شمارهٔ اکانت تلگرام رو با کد کشور بفرست.",
        "مثال: `+989121234567`",
    ]), buttons=[[Button.inline("🔙 لغو", b"adb_cancel")]])


async def _handle_phone(event, st):
    uid = event.sender_id
    phone = (event.raw_text or "").strip().replace(" ", "")
    client = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except PhoneNumberInvalidError:
        await event.respond("❌ شماره نامعتبره. با کد کشور بفرست (مثل +98...).")
        with contextlib.suppress(Exception):
            await client.disconnect()
        return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ ADBOT LOGIN START ERROR", e, [f"📱 {phone}"])
        await event.respond("❌ " + logbus.humanize_error(e, "login"))
        with contextlib.suppress(Exception):
            await client.disconnect()
        return
    _pending[uid] = {"client": client, "phone": phone, "hash": sent.phone_code_hash}
    st["step"] = "adb_await_code"
    await event.respond(card("✉️ تبچی › کد تأیید", [
        "کدی که تلگرام فرستاد رو بفرست (با فاصله یا بی‌فاصله).",
    ]), buttons=[[Button.inline("🔙 لغو", b"adb_cancel")]])


async def _handle_code(event, st):
    uid = event.sender_id
    p = _pending.get(uid)
    if not p:
        _state.pop(uid, None)
        await event.respond("نشست لاگین منقضی شد. دوباره «افزودن اکانت تبچی» رو بزن.",
                            buttons=_menu())
        return
    code = "".join(ch for ch in (event.raw_text or "") if ch.isdigit())
    try:
        await p["client"].sign_in(phone=p["phone"], code=code,
                                  phone_code_hash=p["hash"])
    except SessionPasswordNeededError:
        st["step"] = "adb_await_password"
        await event.respond("🔐 این اکانت رمز دومرحله‌ای داره. رمز رو بفرست.",
                            buttons=[[Button.inline("🔙 لغو", b"adb_cancel")]])
        return
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        await event.respond("❌ کد اشتباه یا منقضیه. دوباره کد رو بفرست (یا لغو کن).")
        return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ ADBOT LOGIN CODE ERROR", e, [f"📱 {p.get('phone')}"])
        await event.respond("❌ " + logbus.humanize_error(e, "code"))
        return
    await _finish_login(event)


async def _handle_password(event, st):
    uid = event.sender_id
    p = _pending.get(uid)
    if not p:
        _state.pop(uid, None)
        await event.respond("نشست لاگین منقضی شد. دوباره «افزودن اکانت تبچی» رو بزن.",
                            buttons=_menu())
        return
    try:
        await p["client"].sign_in(password=(event.raw_text or "").strip())
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ ADBOT LOGIN PASSWORD ERROR", e, [f"📱 {p.get('phone')}"])
        await event.respond("❌ " + logbus.humanize_error(e, "password"))
        return
    await _finish_login(event)


async def _collect_groups(client) -> list:
    """Return the group/supergroup entities this account is a member of."""
    groups = []
    async for dialog in client.iter_dialogs():
        if dialog.is_group:
            groups.append(dialog.entity)
    return groups


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
        # Fetch the account's groups so the owner can confirm they were read.
        try:
            groups = await _collect_groups(client)
        except Exception:
            groups = []
        n_groups = len(groups)
        sample = []
        for g in groups[:10]:
            title = getattr(g, "title", "") or "-"
            sample.append(f"• {title[:40]}")

        _add_account(phone, full_name, username, me.id, session_str, n_groups)

        await _respond(event, card("✅ اکانت تبچی اضافه شد", [
            f"📛 {full_name}" + (f"  (@{username})" if username else ""),
            f"📱 {phone}",
            f"💬 گروه‌ها : {n_groups}",
            LINE,
            "متن تبلیغ و بازه رو تنظیم کن، بعد «شروع ارسال خودکار».",
        ]), buttons=[[Button.inline("📝 متن تبلیغ", b"adb_text")],
                     [Button.inline("🔙 تبچی", b"adb_home")]])

        # Log to the central group so the owner SEES the groups were read.
        await _log("➕ ADBOT ADD ACCOUNT", [
            f"📱 {phone}  ({full_name})",
            f"💬 گروه‌های خوانده‌شده : {n_groups}",
        ] + (sample if sample else ["(گروهی یافت نشد)"])
            + (["… و بیشتر"] if n_groups > 10 else []))
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ ADBOT POST-LOGIN ERROR", e, [f"📱 {phone}"])
        await event.respond("❌ " + logbus.humanize_error(e, "login"))
    finally:
        with contextlib.suppress(Exception):
            await client.disconnect()


# --------------------------------------------------------------------------- #
# Accounts list + detail + delete + reactivate.
# --------------------------------------------------------------------------- #
async def adb_accounts_cb(event):
    if not _is_owner(event):
        return
    accounts = _list_accounts()
    if not accounts:
        await _respond(event, card("👤 تبچی › اکانت‌ها", [
            "هنوز اکانتی اضافه نکردی."]),
            buttons=[[Button.inline("➕ افزودن اکانت تبچی", b"adb_addacc")],
                     [Button.inline("🔙 تبچی", b"adb_home")]])
        return
    rows = []
    for i, acc in enumerate(accounts, 1):
        badge = {"active": "🟢", "banned": "⛔", "dead": "🔴"}.get(acc.get("status"), "▫️")
        rows.append([Button.inline(
            f"{badge} {i}- {acc['phone']}  ({acc.get('groups_count', 0)} گروه)",
            f"adb_acc_{acc['id']}".encode())])
    rows.append([Button.inline("🔙 تبچی", b"adb_home")])
    await _respond(event, card("👤 تبچی › اکانت‌ها",
                               ["یه اکانت رو انتخاب کن:"]), buttons=rows)


async def adb_acc_cb(event):
    if not _is_owner(event):
        return
    account_id = int(event.pattern_match.group(1))
    acc = _get_account(account_id)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    body = [
        f"📛 نام : {acc.get('name') or '-'}",
        (f"🔗 @{acc['username']}" if acc.get("username") else "🔗 یوزرنیم : -"),
        f"📱 شماره : {acc['phone']}",
        f"💬 گروه‌ها : {acc.get('groups_count', 0)}",
        f"📤 کل ارسال : {acc.get('sent_total', 0)}",
        f"🕒 آخرین ارسال : {acc.get('last_run_at') or '-'}",
        f"⭐️ وضعیت : {_status_fa(acc.get('status'))}",
    ]
    if acc.get("last_error"):
        body.append(f"⚠️ آخرین خطا : {str(acc['last_error'])[:120]}")
    btns = []
    if acc.get("status") != "active":
        btns.append([Button.inline("🔄 فعال‌سازی مجدد", f"adb_react_{account_id}".encode())])
    btns.append([Button.inline("🗑 حذف", f"adb_del_{account_id}".encode())])
    btns.append([Button.inline("🔙 اکانت‌ها", b"adb_accounts")])
    await _respond(event, card(f"👤 تبچی › {acc['phone']}", body), buttons=btns)


async def adb_react_cb(event):
    if not _is_owner(event):
        return
    account_id = int(event.pattern_match.group(1))
    acc = _get_account(account_id)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    _set_status(account_id, "active", "")
    await event.answer("اکانت دوباره فعال شد.", alert=True)
    await adb_acc_cb(event)


async def adb_del_cb(event):
    if not _is_owner(event):
        return
    account_id = int(event.pattern_match.group(1))
    await _respond(event, "از حذف این اکانت تبچی مطمئنی؟",
                   buttons=[[Button.inline("✅ بله، حذف کن",
                                           f"adb_delyes_{account_id}".encode())],
                            [Button.inline("🔙 خیر", f"adb_acc_{account_id}".encode())]])


async def adb_delyes_cb(event):
    if not _is_owner(event):
        return
    account_id = int(event.pattern_match.group(1))
    acc = _get_account(account_id)
    _delete_account(account_id)
    await _respond(event, "اکانت حذف شد. ✅",
                   buttons=[[Button.inline("🔙 اکانت‌ها", b"adb_accounts")]])
    if acc:
        await _log("🗑 ADBOT DELETE ACCOUNT", [f"📱 {acc.get('phone')}"])


# --------------------------------------------------------------------------- #
# Ad text.
# --------------------------------------------------------------------------- #
async def adb_text_cb(event):
    if not _is_owner(event):
        return
    uid = event.sender_id
    if _owner_state is not None:
        _owner_state.pop(uid, None)
    cur = (_get_settings().get("ad_text") or "").strip()
    _state[uid] = {"step": "adb_await_text"}
    await _respond(event, card("📝 تبچی › متن تبلیغ", [
        "متن فعلی:",
        (cur[:900] if cur else "— تنظیم‌نشده —"),
        LINE,
        "متن جدید تبلیغ رو بفرست (فقط متن). همین متن تو همهٔ گروه‌ها ارسال می‌شه.",
    ]), buttons=[[Button.inline("🔙 لغو", b"adb_cancel")]])


async def _handle_text(event):
    uid = event.sender_id
    _state.pop(uid, None)
    text = (event.raw_text or "").strip()
    if not text:
        await event.respond("❌ متن خالیه. دوباره یه متن بفرست.", buttons=_menu())
        return
    _set_settings(ad_text=text)
    await event.respond(card("✅ متن تبلیغ ذخیره شد", [
        text[:900], LINE, "حالا بازه رو تنظیم کن یا «شروع ارسال خودکار» رو بزن.",
    ]), buttons=_menu())
    await _log("📝 ADBOT AD TEXT SET", [f"📝 {text[:900]}"])


# --------------------------------------------------------------------------- #
# Interval (5 / 10 / 15 minutes).
# --------------------------------------------------------------------------- #
async def adb_interval_cb(event):
    if not _is_owner(event):
        return
    cur = int(_get_settings().get("interval_min") or _DEF_INTERVAL)
    rows = [[Button.inline(("✅ " if cur == v else "") + f"هر {v} دقیقه",
                           f"adb_int_{v}".encode())] for v in _INTERVAL_CHOICES]
    rows.append([Button.inline("🔙 تبچی", b"adb_home")])
    await _respond(event, card("⏱ تبچی › بازهٔ زمانی", [
        f"بازهٔ فعلی : هر {cur} دقیقه",
        f"اختلاف بین اکانت‌ها : {int(_get_settings().get('stagger_sec') or _DEF_STAGGER)} ثانیه",
        LINE,
        "هر چند وقت یک‌بار تبلیغ تو گروه‌ها ارسال بشه؟",
    ]), buttons=rows)


async def adb_int_cb(event):
    if not _is_owner(event):
        return
    try:
        val = int(event.pattern_match.group(1).decode())
    except Exception:
        val = _DEF_INTERVAL
    if val not in _INTERVAL_CHOICES:
        val = _DEF_INTERVAL
    _set_settings(interval_min=val)
    await _log("⏱ ADBOT INTERVAL SET", [f"⏱ هر {val} دقیقه"])
    await adb_interval_cb(event)


# --------------------------------------------------------------------------- #
# Start / stop the engine.
# --------------------------------------------------------------------------- #
async def adb_start_cb(event):
    if not _is_owner(event):
        return
    accounts = [a for a in _list_accounts() if a.get("status") == "active"]
    if not accounts:
        await event.answer("اکانت فعالی نداری. اول یک اکانت اضافه کن.", alert=True)
        return
    if not (_get_settings().get("ad_text") or "").strip():
        await event.answer("اول متن تبلیغ رو تنظیم کن.", alert=True)
        return
    _set_settings(enabled=1)
    start_engine()          # ensure the loop is alive
    s = _get_settings()
    await _log("▶️ ADBOT STARTED", [
        f"👤 اکانت‌های فعال : {len(accounts)}",
        f"⏱ هر {int(s.get('interval_min') or _DEF_INTERVAL)} دقیقه",
        f"↔️ اختلاف اکانت‌ها : {int(s.get('stagger_sec') or _DEF_STAGGER)}s"])
    await _respond(event, _home_text(), buttons=_menu())


async def adb_stop_cb(event):
    if not _is_owner(event):
        return
    _set_settings(enabled=0)
    await _log("⏹ ADBOT STOPPED", ["ارسال خودکار خاموش شد."])
    await _respond(event, _home_text(), buttons=_menu())


# --------------------------------------------------------------------------- #
# Status.
# --------------------------------------------------------------------------- #
async def adb_status_cb(event):
    if not _is_owner(event):
        return
    s = _get_settings()
    accounts = _list_accounts()
    active = sum(1 for a in accounts if a.get("status") == "active")
    banned = sum(1 for a in accounts if a.get("status") == "banned")
    dead = sum(1 for a in accounts if a.get("status") == "dead")
    total_sent = sum(int(a.get("sent_total") or 0) for a in accounts)
    body = [
        f"⚙️ ارسال خودکار : {'🟢 روشن' if s.get('enabled') else '⏸ خاموش'}",
        f"⏱ بازه : هر {int(s.get('interval_min') or _DEF_INTERVAL)} دقیقه",
        f"↔️ اختلاف اکانت‌ها : {int(s.get('stagger_sec') or _DEF_STAGGER)} ثانیه",
        LINE,
        f"👤 کل اکانت‌ها : {len(accounts)}",
        f"🟢 فعال : {active}   ⛔ بن‌شده : {banned}   🔴 باطل : {dead}",
        f"📤 مجموع ارسال : {total_sent}",
    ]
    await _respond(event, card("📊 تبچی › وضعیت", body),
                   buttons=[[Button.inline("🔄 تازه‌سازی", b"adb_status")],
                            [Button.inline("🔙 تبچی", b"adb_home")]])


# --------------------------------------------------------------------------- #
# NewMessage router (only acts on advertiser conversation steps).
# --------------------------------------------------------------------------- #
async def _msg_router(event):
    if not _is_owner(event):
        return
    if not event.is_private:
        return
    txt = event.raw_text or ""
    if txt.startswith("/"):
        return
    st = _state.get(event.sender_id)
    if not st:
        return                  # not in an advertiser flow -> ignore
    # If owner_bot's own text flow also owns this user, defer to it so the
    # message is handled exactly once (owner flow takes precedence).
    if _owner_state is not None and _owner_state.get(event.sender_id):
        return
    step = st.get("step")
    if step == "adb_await_phone":
        await _handle_phone(event, st)
    elif step == "adb_await_code":
        await _handle_code(event, st)
    elif step == "adb_await_password":
        await _handle_password(event, st)
    elif step == "adb_await_text":
        await _handle_text(event)


# --------------------------------------------------------------------------- #
# Engine: periodic staggered advertising.
# --------------------------------------------------------------------------- #
# Account-level ban / dead tokens. Deliberately specific so a per-CHANNEL ban
# (e.g. UserBannedInChannelError) does NOT wrongly kill the whole account.
_BAN_TOKENS = (
    "userdeactivated", "phonenumberbanned", "phone_number_banned",
    "authkeyunregistered", "auth_key_unregistered", "authkeyduplicated",
    "sessionrevoked", "session_revoked", "session revoked", "sessionexpired",
    "authkeyinvalid", "auth_key_invalid", "unauthorized", "userdeactivatedban",
)


def _is_ban(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}:{exc}".lower()
    return any(tok in text for tok in _BAN_TOKENS)


async def _sleep_while_enabled(seconds: float) -> bool:
    """Sleep up to `seconds`, but return False early the moment auto-send is
    switched off (keeps STOP responsive)."""
    end = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < end:
        if not _enabled():
            return False
        await asyncio.sleep(min(2.0, max(0.05, end - time.monotonic())))
    return True


async def _run_account_once(account_id: int, text: str) -> None:
    """One account posts `text` to ALL its groups, once. Isolated per session so
    3-4 accounts can run concurrently. A ban stops this account permanently."""
    if account_id in _running:
        return
    acc = _get_account(account_id)
    if not acc or acc.get("status") != "active" or not acc.get("session"):
        return
    _running.add(account_id)
    phone = acc.get("phone")
    client = TelegramClient(StringSession(acc["session"]), config.API_ID, config.API_HASH)
    sent = failed = 0
    groups_n = 0
    try:
        await client.connect()
        if not await client.is_user_authorized():
            _set_status(account_id, "dead", "unauthorized")
            await _log("🔴 تبچی — سشن باطل شد", [
                f"📱 {phone}", "این اکانت دیگه ارسال نمی‌کنه؛ دوباره اضافه‌اش کن."])
            return
        groups = await _collect_groups(client)
        groups_n = len(groups)
        for ent in groups:
            if not _enabled():
                break
            try:
                await client.send_message(ent, text)
                sent += 1
            except FloodWaitError as fw:
                wait = int(getattr(fw, "seconds", 5))
                if wait > _FLOOD_MAX:
                    break                       # too long -> skip rest this cycle
                await asyncio.sleep(wait + 1)
                try:
                    await client.send_message(ent, text)
                    sent += 1
                except Exception as e2:         # noqa: BLE001
                    if _is_ban(e2):
                        raise
                    failed += 1
            except Exception as e:              # noqa: BLE001
                if _is_ban(e):
                    raise                       # bubble to the ban handler below
                failed += 1                     # per-group forbidden/slowmode -> skip
            await asyncio.sleep(_GROUP_DELAY)
        _mark_run(account_id, sent, groups_n)
        await _log("✈️ تبچی — ارسال انجام شد", [
            f"📱 {phone}",
            f"✅ ارسال‌شده : {sent}   ❌ ناموفق : {failed}   💬 گروه‌ها : {groups_n}"])
    except asyncio.CancelledError:
        raise
    except Exception as e:                      # noqa: BLE001
        if _is_ban(e):
            _set_status(account_id, "banned", f"{type(e).__name__}: {e}")
            await _log("⛔ تبچی — اکانت بن شد", [
                f"📱 {phone}",
                f"📛 {type(e).__name__}",
                "🚫 ارسال این اکانت متوقف شد و دیگه تلاش نمی‌شه."])
        else:
            await logbus.log_detail("❌ ADBOT SEND ERROR", e, [f"📱 {phone}"])
    finally:
        _running.discard(account_id)
        with contextlib.suppress(Exception):
            await client.disconnect()


async def _engine_loop() -> None:
    while True:
        try:
            s = _get_settings()
            if not s.get("enabled"):
                await asyncio.sleep(5)
                continue
            interval = max(1, int(s.get("interval_min") or _DEF_INTERVAL)) * 60
            stagger = max(0, int(s.get("stagger_sec") or _DEF_STAGGER))
            text = (s.get("ad_text") or "").strip()
            accounts = [a for a in _list_accounts() if a.get("status") == "active"]
            if not text or not accounts:
                await _sleep_while_enabled(min(interval, 30))
                continue
            cycle_start = time.monotonic()
            # Launch each account STAGGERED by ~30s so they never fire together.
            tasks = []
            for i, acc in enumerate(accounts):
                if not _enabled():
                    break
                tasks.append(asyncio.create_task(_run_account_once(acc["id"], text)))
                if i < len(accounts) - 1:
                    if not await _sleep_while_enabled(stagger):
                        break
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            # Wait out the rest of the interval before the next cycle.
            remaining = interval - (time.monotonic() - cycle_start)
            if remaining > 0:
                await _sleep_while_enabled(remaining)
        except asyncio.CancelledError:
            raise
        except Exception as e:                  # noqa: BLE001
            print(f"[adbot engine] {e}")
            await asyncio.sleep(10)


def start_engine() -> None:
    """Start the single background engine task if it isn't already running.
    Safe to call multiple times (idempotent)."""
    global _engine_task
    if _engine_task is not None and not _engine_task.done():
        return
    _init()
    _engine_task = asyncio.create_task(_engine_loop(), name="adbot-engine")


# --------------------------------------------------------------------------- #
# Wiring.
# --------------------------------------------------------------------------- #
def setup(shared_bot, owner_state=None):
    """Register all advertiser handlers on the owner bot. Called once from
    owner_bot.amain(). `owner_state` is owner_bot.state (for cross-flow mutual
    exclusion so the owner's other flows and this one never fight over a message).
    """
    global bot, TelegramClient, _owner_state
    bot = shared_bot
    _owner_state = owner_state
    from telethon import TelegramClient as _TC
    TelegramClient = _TC
    _init()

    add = bot.add_event_handler
    add(adb_home_cb, events.CallbackQuery(data=b"adb_home"))
    add(adb_cancel_cb, events.CallbackQuery(data=b"adb_cancel"))
    add(adb_addacc_cb, events.CallbackQuery(data=b"adb_addacc"))
    add(adb_accounts_cb, events.CallbackQuery(data=b"adb_accounts"))
    add(adb_acc_cb, events.CallbackQuery(pattern=b"adb_acc_(\\d+)"))
    add(adb_react_cb, events.CallbackQuery(pattern=b"adb_react_(\\d+)"))
    add(adb_del_cb, events.CallbackQuery(pattern=b"adb_del_(\\d+)"))
    add(adb_delyes_cb, events.CallbackQuery(pattern=b"adb_delyes_(\\d+)"))
    add(adb_text_cb, events.CallbackQuery(data=b"adb_text"))
    add(adb_interval_cb, events.CallbackQuery(data=b"adb_interval"))
    add(adb_int_cb, events.CallbackQuery(pattern=b"adb_int_(\\d+)"))
    add(adb_start_cb, events.CallbackQuery(data=b"adb_start"))
    add(adb_stop_cb, events.CallbackQuery(data=b"adb_stop"))
    add(adb_status_cb, events.CallbackQuery(data=b"adb_status"))
    add(_msg_router, events.NewMessage())
