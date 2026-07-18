"""
customer_bot.py — the CUSTOMER subscription bot.
================================================

Customers /start this bot, see how many days are left (or "expired"), buy a
subscription (3-day / weekly / monthly, paid in TRX with balance system and
auto-verified via TronGrid), and use the tools: add account, send (marker),
import PV photos -> PDF, balance, stats, and account health check.
No limit on the number of accounts.

Payment flow:
  1. Customer selects a plan -> TRX price shown (from CoinGecko).
  2. If balance >= cost -> deduct and credit days immediately.
  3. If balance < cost -> show deficit, prompt deposit, verify on-chain.
  4. After deposit, auto-check if balance covers plan. If yes, deduct and credit.
  5. Standalone deposit flow also available (charge balance anytime).

Isolation: this process uses ONLY its own operational database (db.py), always
scoped to the requesting telegram_id, and NEVER imports the owner-only
central_db. Every customer event is mirrored to the customer's own PV and to the
single central log group.
"""
import asyncio
import math
import os
import time as _time
from datetime import datetime

from telethon import TelegramClient, events, Button

import account_conn
import config
import db
import logbus
import pdf_export
import ratelimit
import rubika_client as rb
import tg_panel
import bale_panel
import group_panel
import forcedjoin
import tron
import worker

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

bot = TelegramClient(os.path.join(DATA_DIR, "customer_bot"),
                     config.API_ID, config.API_HASH)

LINE = logbus.LINE

# per-user conversation state
state: dict = {}
# rubpy login clients mid-flow: user_id -> ctx
pending_login: dict = {}
# prepared sends awaiting confirmation: account_id -> payload
pending_send: dict = {}
# manual stop flags: account_id -> True
stop_flags: dict = {}
# accounts currently running a job
active_jobs: set = set()
# customers currently running a PV image export (memory-heavy; globally capped)
pv_export_jobs: set = set()
# customers who pressed "stop" on their running PV export (collect-so-far)
pv_export_stop: set = set()
# pending Rubika worker-transfer relogins: uid -> {"aid": int}
pending_xfer: dict = {}
# CHANNEL MODE (isolated): a just-created channel awaiting the "add members"
# step. Keyed by uid -> {account_id, phone, channel_name, channel_guid, remote, worker}
pending_channel: dict = {}


def now() -> str:
    return config.now_str()


def card(title, rows):
    return logbus.card(title, rows)


def customer_active_account(customer_id: int, exclude_aid: int = None):
    """Return an account (dict) owned by `customer_id` that is CURRENTLY running
    a send job — optionally excluding `exclude_aid` — or None.

    Enforces the rule: ONE customer may run only ONE send at a time. A customer
    sending from two different accounts at once would put two simultaneous
    Rubika sessions in flight for the same person, which we don't allow.
    Fully guarded — a failure here must never block a legitimate send."""
    if not active_jobs:
        return None
    try:
        for a in db.list_accounts(customer_id):
            aid = a["id"]
            if exclude_aid is not None and aid == exclude_aid:
                continue
            if aid in active_jobs:
                return a
    except Exception:
        return None
    return None


# --------------------------------------------------------------------------- #
# Send progress: a periodic "SEND PROGRESS" card (every N sends) posted to the
# originating chat (the customer's group if started there, else their PV) AND
# always to the central log group. Fully guarded: a logging failure must never
# affect the actual send.
# --------------------------------------------------------------------------- #
def _pct(ok, total):
    return int((ok / total) * 100) if total else 0


def _progress_card(payload, ok, fail, total):
    """The periodic 'SEND PROGRESS' card (posted every N sends — NOT live-edited)."""
    phone = payload.get("phone")
    w = payload.get("worker") or {}
    tag = (w.get("tag") or w.get("id")) if w else None
    head = f"📱 {phone}" + (f" (ورکر #{tag})" if tag else "")
    done = ok + fail
    remaining = max(0, total - done)
    return card("📊 SEND PROGRESS", [
        head,
        f"✅ {ok} از {total} — {_pct(ok, total)}%",
        f"⏳ باقی‌مونده : {remaining}",
        f"🕒 {now()}",
    ])


async def _post_progress(payload, ok, fail, total):
    """Post the progress card to the ORIGINATING chat (the customer's group if the
    send started there, or their PV) AND always to the central log group.
    Fully guarded — never affects the actual send."""
    text = _progress_card(payload, ok, fail, total)
    chat = payload.get("notify_chat")
    if chat:
        try:
            await bot.send_message(chat, text)
        except Exception:
            pass
    try:
        await logbus.to_group(text)
    except Exception:
        pass


def _final_report_card(payload, ok, fail, total, dur, reason=None, retry_count=0):
    phone = payload.get("phone")
    w = payload.get("worker") or {}
    tag = (w.get("tag") or w.get("id")) if w else None
    rows = [
        f"✓ موفق: {ok} ({_pct(ok, total)}%)",
        f"✗ ناموفق: {fail}",
        f"📱 اکانت: {phone}" + (f" (ورکر #{tag})" if tag else ""),
        f"🎯 کل: {total}",
        f"⏱ مدت: {dur}",
    ]
    if retry_count:
        rows.append(f"🔄 ادامه‌ی خودکار: {retry_count} بار")
    if reason:
        rows.append(f"⛔ دلیلِ توقف: {reason}")
    rows.append(f"🕒 {now()}")
    return card("▪️ گزارش ارسال", rows)


async def _post_final_report(payload, ok, fail, total, dur, reason=None, retry_count=0):
    """Final send report — posted whenever a send ends OR stops (any reason) to
    the originating chat (group/PV) AND the central log group. Never raises."""
    text = _final_report_card(payload, ok, fail, total, dur, reason, retry_count)
    chat = payload.get("notify_chat")
    if chat:
        try:
            await bot.send_message(chat, text, buttons=main_menu()
                                   if chat == payload.get("customer_id") else None)
        except Exception:
            try:
                await bot.send_message(chat, text)
            except Exception:
                pass
    try:
        await logbus.to_group(text)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Subscription / access helpers.
# --------------------------------------------------------------------------- #
def _sub_line(uid: int) -> str:
    if db.is_blocked(uid):
        return "⛔ حساب شما مسدود است."
    d = db.days_left(uid)
    if config.FREE_MODE:
        # Free: no expiry set => unlimited free access; expiry set => show it.
        cust = db.get_customer(uid) or {}
        if not (cust.get("expires_at") or ""):
            return "🟢 دسترسی آزاد (رایگان)."
        if d <= 0:
            return "🔴 زمان دسترسی‌ات تموم شده."
        return f"🟢 {d} روز دسترسی باقی مونده."
    if d <= 0:
        return "🔴 اشتراک شما منقضی شده."
    return f"🟢 {d} روز از اشتراک شما باقی مونده."


def root_menu():
    """Root: Rubika + Telegram side by side, with a big Bale button below."""
    return [
        [Button.inline("🟣 روبیکا", b"rubika_open"),
         Button.inline("📨 تلگرام", b"tg_home")],
        [Button.inline("🔵 بـلـه", b"bale_home")],
    ]


def main_menu():
    rows = [
        [Button.inline("🚀 ارسال", b"send_menu"),
         Button.inline("➕ افزودن اکانت", b"addacc")],
        [Button.inline("👤 اکانت‌های من", b"accounts"),
         Button.inline("🩺 چک‌حساب", b"health")],
        [Button.inline("🖼 ایمپورت عکس پیوی (PDF)", b"pvexport")],
        [Button.inline("📌 مارکر", b"marker"),
         Button.inline("⚙️ سرعت ارسال", b"speed")],
    ]
    if not config.FREE_MODE:
        rows.append([Button.inline("🛒 خرید اشتراک", b"buy"),
                     Button.inline("💰 موجودی", b"balance")])
    rows.append([Button.inline("📊 آمار من", b"mystats"),
                 Button.inline("📖 راهنما", b"help")])
    rows.append([Button.inline("⚙️ تنظیمات گروه", b"gconf")])
    rows.append([Button.inline("🏠 منوی اصلی", b"mainmenu")])
    return rows


async def build_buy_menu():
    """Build the buy menu with TRX prices fetched from CoinGecko."""
    rows = []
    try:
        trx_price = await tron.get_trx_price_usd()
    except Exception:
        trx_price = 0
    for key in ("3day", "weekly", "monthly"):
        p = config.PLANS[key]
        usd = db.get_plan_price(key) or p["price"]
        if trx_price > 0:
            trx_amount = math.floor(usd / trx_price)
            label = f"{p['title']} -- {trx_amount} TRX (~{usd:g}$)"
        else:
            label = f"{p['title']} -- ~{usd:g}$"
        rows.append([Button.inline(label, f"plan_{key}".encode())])
    rows.append([Button.inline("🔙 بازگشت", b"home")])
    return rows


async def _gate(event, *, need_active: bool = True, count_action: bool = True) -> bool:
    """Common entry guard for every customer action. Returns True if the action
    may proceed. Handles maintenance, rate-limit/auto-block, and (optionally)
    subscription validity."""
    uid = event.sender_id
    # Blocked users are fully ignored: no reply, no log, no processing (anti-spam).
    if db.is_blocked(uid):
        return False
    user = await event.get_sender()
    name = getattr(user, "first_name", "") or ""
    username = getattr(user, "username", "") or ""
    db.ensure_customer(uid, name, username)

    # maintenance mode (read via shared flag file, not central_db)
    if db.maintenance_on():
        await _respond(event, "🛠 ربات در حال تعمیر است. کمی بعد دوباره امتحان کن.")
        return False

    # anti-flood: counts every COUNTED action; auto-blocks on exceed. Pure menu
    # navigation (home/mainmenu/rubika_open) passes count_action=False so a user
    # browsing menus can't auto-block themselves; blocked users are already
    # rejected above, so skipping the count here is safe.
    if count_action:
        if not await ratelimit.guard(uid, name):
            await _respond(event, "⛔ به‌خاطر فعالیت بیش از حد، حساب شما مسدود شد. "
                                  "برای رفع مسدودی با پشتیبانی در تماس باش.")
            return False

    # forced-join: must be a member of all required channels (owner exempt)
    if not await forcedjoin.enforce(bot, event):
        return False

    if need_active:
        if config.FREE_MODE:
            # Free: allowed unless the owner set an expiry that has now passed.
            cust = db.get_customer(uid) or {}
            if (cust.get("expires_at") or "") and db.seconds_left(uid) <= 0:
                await _respond(event, "🔴 زمانِ دسترسی‌ات تموم شده. با پشتیبانی تماس بگیر.")
                return False
        elif not db.is_active(uid):
            if db.is_blocked(uid):
                await _respond(event, "⛔ حساب شما مسدود است.")
            else:
                buy_buttons = await build_buy_menu()
                await _respond(event,
                               "🔴 برای استفاده از امکانات، اول اشتراک تهیه کن.",
                               buttons=buy_buttons)
            return False
    return True


async def _respond(event, text, buttons=None):
    """Reply whether the event is a message or a callback."""
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


# --------------------------------------------------------------------------- #
# /start
# --------------------------------------------------------------------------- #
@bot.on(events.NewMessage(pattern="/start", func=lambda e: e.is_private))
async def start_handler(event):
    uid = event.sender_id
    # Blocked users are fully ignored: no reply, no log, no processing (anti-spam).
    if db.is_blocked(uid):
        return
    user = await event.get_sender()
    name = getattr(user, "first_name", "") or ""
    username = getattr(user, "username", "") or ""
    fresh = db.get_customer(uid) is None
    db.ensure_customer(uid, name, username)
    state.pop(uid, None)
    tg_panel._state.pop(uid, None)

    if db.maintenance_on():
        await event.respond("🛠 ربات در حال تعمیر است. کمی بعد دوباره /start بزن.")
        return

    await ratelimit.guard(uid, name)  # count the action (won't block on first)

    await logbus.event("🟢 START", [
        f"👤 {name} (@{username})" if username else f"👤 {name}",
        f"🆔 {uid}",
        ("🆕 مشتری جدید" if fresh else "↩️ بازگشت مشتری"),
        f"🕒 {now()}"])

    header = _sub_line(uid)
    await event.respond(
        f"🤖 پنل ربات\n{LINE}\n{header}\n\n"
        "کدوم بخش؟ 📨 تلگرام یا 🟣 روبیکا:",
        buttons=root_menu())


@bot.on(events.CallbackQuery(data=b"mainmenu"))
async def mainmenu_cb(event):
    if not await _gate(event, need_active=False, count_action=False):
        return
    state.pop(event.sender_id, None)
    header = _sub_line(event.sender_id)
    await _respond(event, f"🤖 پنل ربات\n{LINE}\n{header}\n\n"
                          "کدوم بخش؟ 📨 تلگرام یا 🟣 روبیکا:",
                   buttons=root_menu())


@bot.on(events.CallbackQuery(data=b"rubika_open"))
async def rubika_open_cb(event):
    if not await _gate(event, need_active=False, count_action=False):
        return
    state.pop(event.sender_id, None)
    tg_panel._state.pop(event.sender_id, None)
    header = _sub_line(event.sender_id)
    await _respond(event, f"🟣 پنل روبیکا\n{LINE}\n{header}\n\nیکی از گزینه‌ها رو انتخاب کن:",
                   buttons=main_menu())


@bot.on(events.CallbackQuery(data=b"home"))
async def home_cb(event):
    if not await _gate(event, need_active=False, count_action=False):
        return
    state.pop(event.sender_id, None)
    tg_panel._state.pop(event.sender_id, None)
    header = _sub_line(event.sender_id)
    await _respond(event, f"🤖 روبیکا تولز\n{LINE}\n{header}\n\nیکی از گزینه‌ها رو انتخاب کن:",
                   buttons=main_menu())


@bot.on(events.CallbackQuery(data=b"fj_check"))
async def fj_check_cb(event):
    """Re-check forced-join membership after the user pressed «عضو شدم»."""
    uid = event.sender_id
    if db.is_blocked(uid):
        return
    missing = await forcedjoin.missing_for(bot, uid)
    if missing:
        await event.answer("هنوز عضو همهٔ کانال‌ها نشدی! بعد از عضویت دوباره بزن.",
                           alert=True)
        return
    await event.answer("✅ عضو شدی! خوش اومدی.")
    state.pop(uid, None)
    tg_panel._state.pop(uid, None)
    try:
        bale_panel._state.pop(uid, None)
    except Exception:
        pass
    header = _sub_line(uid)
    await _respond(event, f"🤖 پنل ربات\n{LINE}\n{header}\n\n"
                          "کدوم بخش؟ 🟣 روبیکا / 📨 تلگرام / 🔵 بله:",
                   buttons=root_menu())


@bot.on(events.CallbackQuery(data=b"cancel"))
async def cancel_cb(event):
    uid = event.sender_id
    p = pending_login.pop(uid, None)
    if p:
        try:
            await p["client"].disconnect()
        except Exception:
            pass
    state.pop(uid, None)
    pending_xfer.pop(uid, None)
    await _respond(event, "لغو شد.", buttons=main_menu())


# --------------------------------------------------------------------------- #
# Buy subscription + payment verification.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"buy"))
async def buy_cb(event):
    if not await _gate(event, need_active=False):
        return
    buy_buttons = await build_buy_menu()
    await _respond(event,
                   "🛒 یکی از پلن‌ها رو انتخاب کن (پرداخت با TRX):",
                   buttons=buy_buttons)


@bot.on(events.CallbackQuery(pattern=b"plan_(.+)"))
async def plan_cb(event):
    if not await _gate(event, need_active=False):
        return
    uid = event.sender_id
    key = event.pattern_match.group(1).decode()
    plan = config.PLANS.get(key)
    if not plan:
        await event.answer("پلن نامعتبر.", alert=True)
        return

    # Calculate TRX cost for this plan
    usd = db.get_plan_price(key) or plan["price"]
    try:
        trx_needed = await tron.calc_trx_amount_async(usd)
    except Exception:
        await _respond(event, "❌ خطا در دریافت قیمت TRX. بعدا دوباره تلاش کن.",
                       buttons=[[Button.inline("🔙 بازگشت", b"buy")]])
        return

    balance = db.get_balance(uid)

    if balance >= trx_needed:
        # Balance is sufficient -> deduct first (atomic), then record payment
        ok = db.deduct_balance(uid, trx_needed)
        if not ok:
            await _respond(event, "❌ موجودی کافی نیست. دوباره تلاش کن.",
                           buttons=main_menu())
            return
        pseudo_hash = f"balance_{uid}_{int(_time.time())}_{os.urandom(4).hex()}"
        if not db.record_payment(uid, pseudo_hash, key, float(trx_needed), plan["days"]):
            db.add_balance(uid, trx_needed)  # refund on failure
            await _respond(event, "❌ خطا در ثبت خرید. دوباره تلاش کن.",
                           buttons=main_menu())
            return
        state.pop(uid, None)
        await _respond(event, card("✅ خرید موفق", [
            f"📦 {plan['title']}",
            f"💰 {trx_needed} TRX از موجودی کسر شد.",
            f"⏳ {plan['days']} روز اضافه شد.",
            f"📅 {_sub_line(uid)}",
        ]), buttons=main_menu())
        await logbus.event("💰 خرید اشتراک (از موجودی)", [
            f"🆔 {uid}", f"📦 {plan['title']}", f"💰 {trx_needed} TRX",
            f"📅 {_sub_line(uid)}", f"🕒 {now()}"], pv_user=uid)
    else:
        # Balance insufficient -> tell them the deficit and ask for deposit
        deficit = trx_needed - int(balance)
        state[uid] = {"step": "await_txhash", "plan": key, "trx_needed": trx_needed}
        await _respond(event, card("🧾 پرداخت اشتراک", [
            f"📦 پلن : {plan['title']}",
            f"⏳ مدت : {plan['days']} روز",
            f"💰 هزینه : {trx_needed} TRX (~{usd:g}$)",
            f"💳 موجودی فعلی : {int(balance)} TRX",
            f"📊 کمبود : {deficit} TRX دیگه شارژ کن",
            LINE,
            "1️⃣ مبلغ رو به آدرس زیر بفرست (شبکه TRON - TRX):",
            f"`{config.WALLET_ADDRESS}`",
            "2️⃣ بعد از پرداخت، هشِ تراکنش (TxID) رو همینجا بفرست.",
            LINE,
            "⚠️ هر تراکنش فقط یک‌بار قابل استفاده‌ست.",
        ]), buttons=[[Button.inline("🔙 بازگشت", b"buy")]])


async def handle_txhash(event, st):
    """Process a TRX deposit tx hash (during plan purchase flow).

    After verifying the deposit, adds TRX to balance.  Then checks if balance
    now covers the selected plan and auto-purchases if so.
    """
    uid = event.sender_id
    key = st.get("plan")
    trx_needed = st.get("trx_needed", 0)
    plan = config.PLANS.get(key) if key else None
    if not plan:
        state.pop(uid, None)
        await event.respond("پلن نامعتبر. دوباره از «خرید اشتراک» شروع کن.",
                            buttons=main_menu())
        return
    tx_hash = tron.extract_tx_hash(event.raw_text or "")
    if not tx_hash:
        await event.respond("هشِ تراکنش معتبر پیدا نشد. فقط هشِ ۶۴ کاراکتری تراکنش "
                            "(یا لینک tronscan همون تراکنش) رو بفرست.")
        return

    # fast anti-fraud: reject an already-used hash before hitting the network
    if db.payment_exists(tx_hash):
        state.pop(uid, None)
        await logbus.event("♻️ پرداخت تکراری", [
            f"🆔 {uid}", f"🔗 {tx_hash[:24]}...",
            "این هش قبلا استفاده شده.", f"🕒 {now()}"], pv_user=uid)
        await event.respond("❌ این هشِ تراکنش قبلا استفاده شده.", buttons=main_menu())
        return

    # Check deposits table for already-used hash (without inserting)
    if db.deposit_exists(tx_hash):
        state.pop(uid, None)
        await logbus.event("♻️ پرداخت تکراری", [
            f"🆔 {uid}", f"🔗 {tx_hash[:24]}...",
            "این هش قبلا استفاده شده.", f"🕒 {now()}"], pv_user=uid)
        await event.respond("❌ این هشِ تراکنش قبلا استفاده شده.", buttons=main_menu())
        return

    msg = await event.respond("⏳ در حال بررسی تراکنش روی شبکه TRON ...")
    # We verify it is a valid TransferContract to our wallet; expected_trx=0
    # means accept any amount (deposit mode).
    res = await tron.verify_trx_payment(tx_hash, 0)
    if not res.ok:
        await msg.edit(f"❌ تایید نشد: {res.reason}",
                       buttons=[[Button.inline("🔁 تلاش دوباره", f"plan_{key}".encode())],
                                [Button.inline("🏠 منو", b"home")]])
        await logbus.event("⚠️ واریز ناموفق", [
            f"🆔 {uid}", f"📦 {plan['title']}", f"🔗 {tx_hash[:24]}...",
            f"💥 {res.reason}", f"🕒 {now()}"])
        return

    # Check minimum deposit threshold
    actual_trx = res.amount
    if actual_trx < config.MIN_DEPOSIT_TRX:
        await msg.edit(f"❌ حداقل واریز {config.MIN_DEPOSIT_TRX} TRX است.",
                       buttons=[[Button.inline("🔁 تلاش دوباره", f"plan_{key}".encode())],
                                [Button.inline("🏠 منو", b"home")]])
        return

    # Deposit verified -> record it now (UNIQUE constraint catches race condition)
    if not db.record_deposit(uid, tx_hash, actual_trx):
        # Another concurrent request already recorded this hash
        state.pop(uid, None)
        await msg.edit("❌ این هشِ تراکنش قبلا استفاده شده.", buttons=main_menu())
        return

    db.add_balance(uid, actual_trx)

    await logbus.event("💳 واریز TRX", [
        f"🆔 {uid}", f"💰 {actual_trx:g} TRX",
        f"🔗 {tx_hash[:24]}...", f"🕒 {now()}"], pv_user=uid)

    # Check if balance now covers the plan
    new_balance = db.get_balance(uid)
    if new_balance >= trx_needed:
        # Auto-purchase: deduct balance first (atomic), then record payment
        ok = db.deduct_balance(uid, trx_needed)
        if not ok:
            state.pop(uid, None)
            await msg.edit("❌ خطا در کسر موجودی.", buttons=main_menu())
            return
        pseudo_hash = f"balance_{uid}_{int(_time.time())}_{os.urandom(4).hex()}"
        if not db.record_payment(uid, pseudo_hash, key, float(trx_needed), plan["days"]):
            db.add_balance(uid, trx_needed)  # refund on failure
            state.pop(uid, None)
            await msg.edit("❌ خطا در ثبت خرید.", buttons=main_menu())
            return
        state.pop(uid, None)
        await msg.edit(card("✅ واریز و خرید موفق", [
            f"💰 واریز: {actual_trx:g} TRX",
            f"📦 {plan['title']} فعال شد!",
            f"💰 {trx_needed} TRX از موجودی کسر شد.",
            f"⏳ {plan['days']} روز اضافه شد.",
            f"📅 {_sub_line(uid)}",
        ]), buttons=main_menu())
        await logbus.event("💰 خرید اشتراک (پس از واریز)", [
            f"🆔 {uid}", f"📦 {plan['title']}", f"💰 {trx_needed} TRX",
            f"📅 {_sub_line(uid)}", f"🕒 {now()}"], pv_user=uid)
    else:
        # Still not enough
        still_need = trx_needed - int(new_balance)
        await msg.edit(card("✅ واریز ثبت شد", [
            f"💰 واریز: {actual_trx:g} TRX",
            f"💳 موجودی جدید: {int(new_balance)} TRX",
            f"📊 هنوز {still_need} TRX دیگه برای پلن «{plan['title']}» نیاز داری.",
            LINE,
            "هش تراکنش بعدی رو بفرست یا از منو برگرد.",
        ]), buttons=[[Button.inline("🏠 منو", b"home")]])


# --------------------------------------------------------------------------- #
# Balance (موجودی).
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"balance"))
async def balance_cb(event):
    if not await _gate(event, need_active=False):
        return
    uid = event.sender_id
    balance = db.get_balance(uid)
    deposits = db.list_deposits(uid)[:5]
    rows = [
        f"💰 موجودی فعلی : {int(balance)} TRX",
        LINE,
        "آخرین واریزها:",
    ]
    if deposits:
        for d in deposits:
            rows.append(f"• {d['trx_amount']:g} TRX -- {d['created_at']}")
    else:
        rows.append("-- هنوز واریزی ثبت نشده --")
    await _respond(event, card("💰 موجودی", rows),
                   buttons=[[Button.inline("💳 شارژ حساب", b"deposit")],
                            [Button.inline("🛒 خرید اشتراک", b"buy")],
                            [Button.inline("🔙 بازگشت", b"home")]])


# --------------------------------------------------------------------------- #
# Standalone deposit flow (شارژ حساب).
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"deposit"))
async def deposit_cb(event):
    if not await _gate(event, need_active=False):
        return
    uid = event.sender_id
    state[uid] = {"step": "await_deposit_txhash"}
    await _respond(event, card("💳 شارژ حساب", [
        "مبلغ دلخواه TRX رو به آدرس زیر بفرست:",
        f"`{config.WALLET_ADDRESS}`",
        LINE,
        "بعد از پرداخت، هشِ تراکنش (TxID) رو همینجا بفرست.",
        "⚠️ هر تراکنش فقط یک‌بار قابل استفاده‌ست.",
    ]), buttons=[[Button.inline("🔙 لغو", b"cancel")]])


async def handle_deposit_txhash(event, st):
    """Process a standalone TRX deposit (not tied to a specific plan)."""
    uid = event.sender_id
    tx_hash = tron.extract_tx_hash(event.raw_text or "")
    if not tx_hash:
        await event.respond("هشِ تراکنش معتبر پیدا نشد. فقط هشِ ۶۴ کاراکتری تراکنش "
                            "(یا لینک tronscan همون تراکنش) رو بفرست.")
        return

    # anti-fraud: reject already-used hash (check without inserting)
    if db.payment_exists(tx_hash):
        state.pop(uid, None)
        await logbus.event("♻️ واریز تکراری", [
            f"🆔 {uid}", f"🔗 {tx_hash[:24]}...",
            "این هش قبلا استفاده شده.", f"🕒 {now()}"], pv_user=uid)
        await event.respond("❌ این هشِ تراکنش قبلا استفاده شده.", buttons=main_menu())
        return

    if db.deposit_exists(tx_hash):
        state.pop(uid, None)
        await logbus.event("♻️ واریز تکراری", [
            f"🆔 {uid}", f"🔗 {tx_hash[:24]}...",
            "این هش قبلا استفاده شده.", f"🕒 {now()}"], pv_user=uid)
        await event.respond("❌ این هشِ تراکنش قبلا استفاده شده.", buttons=main_menu())
        return

    msg = await event.respond("⏳ در حال بررسی تراکنش روی شبکه TRON ...")
    res = await tron.verify_trx_payment(tx_hash, 0)
    if not res.ok:
        await msg.edit(f"❌ تایید نشد: {res.reason}",
                       buttons=[[Button.inline("💳 شارژ حساب", b"deposit")],
                                [Button.inline("🏠 منو", b"home")]])
        await logbus.event("⚠️ واریز ناموفق", [
            f"🆔 {uid}", f"🔗 {tx_hash[:24]}...",
            f"💥 {res.reason}", f"🕒 {now()}"])
        return

    # Check minimum deposit threshold
    actual_trx = res.amount
    if actual_trx < config.MIN_DEPOSIT_TRX:
        await msg.edit(f"❌ حداقل واریز {config.MIN_DEPOSIT_TRX} TRX است.",
                       buttons=[[Button.inline("💳 شارژ حساب", b"deposit")],
                                [Button.inline("🏠 منو", b"home")]])
        return

    # Deposit verified -> record it now (UNIQUE constraint catches race condition)
    if not db.record_deposit(uid, tx_hash, actual_trx):
        state.pop(uid, None)
        await msg.edit("❌ این هشِ تراکنش قبلا استفاده شده.", buttons=main_menu())
        return

    db.add_balance(uid, actual_trx)
    state.pop(uid, None)

    new_balance = db.get_balance(uid)
    await msg.edit(card("✅ واریز ثبت شد", [
        f"💰 واریز: {actual_trx:g} TRX",
        f"💳 موجودی جدید: {int(new_balance)} TRX",
    ]), buttons=main_menu())
    await logbus.event("💳 واریز TRX (شارژ مستقیم)", [
        f"🆔 {uid}", f"💰 {actual_trx:g} TRX",
        f"🔗 {tx_hash[:24]}...", f"🕒 {now()}"], pv_user=uid)


# --------------------------------------------------------------------------- #
# My Stats (آمار من).
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"mystats"))
async def mystats_cb(event):
    if not await _gate(event, need_active=False):
        return
    uid = event.sender_id
    cust = db.get_customer(uid) or {}
    total_sends = int(cust.get("total_sends") or 0)
    balance = db.get_balance(uid)
    d = db.days_left(uid)
    sub_status = f"🟢 فعال ({d} روز مانده)" if d > 0 else "🔴 منقضی"

    # Today's sends: query from customer stats
    # We don't have a per-day sends tracker, so show total_sends
    rows = [
        f"📤 کل ارسال‌ها : {total_sends}",
        f"💰 موجودی : {int(balance)} TRX",
        f"📅 وضعیت اشتراک : {sub_status}",
    ]
    if d > 0:
        rows.append(f"⏳ روزهای باقیمانده : {d}")

    await _respond(event, card("📊 آمار من", rows),
                   buttons=[[Button.inline("🔙 بازگشت", b"home")]])


# --------------------------------------------------------------------------- #
# Help / guide (راهنما).
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"help"))
async def help_cb(event):
    if not await _gate(event, need_active=False):
        return
    text = (
        "📖 راهنمای ربات\n" + LINE + "\n"
        "🛒 خرید اشتراک / تمدید: یکی از پلن‌ها رو می‌زنی، مبلغ به TRX نشون داده "
        "می‌شه. خرید وقتی اشتراک فعال داری = تمدید (روزها روی هم جمع می‌شن).\n\n"
        "💰 موجودی: کیف پول داخلی توئه. هر مقدار TRX شارژ کنی اینجا جمع می‌شه و "
        "موقع خرید پلن از همین کم می‌شه.\n\n"
        "💳 شارژ حساب: مبلغ دلخواه TRX به آدرس ولت می‌فرستی، بعد هشِ تراکنش "
        "(یا لینک tronscan) رو می‌دی تا به موجودیت اضافه شه.\n\n"
        "➕ افزودن اکانت: اکانت روبیکات رو با شماره + کد (و رمز دومرحله‌ای اگه "
        "داشت) وصل می‌کنی.\n\n"
        "👤 اکانت‌های من: لیست اکانت‌های وصل‌شده + حذف.\n\n"
        "🩺 چک‌حساب: سالم بودن سشن همه‌ی اکانت‌هات رو بررسی می‌کنه.\n\n"
        "🚀 ارسال: پیامِ نشان‌دار (مارکر) رو از یه اکانت به همه‌ی مخاطبینش "
        "فوروارد می‌کنه. موقع ارسال یه نوارِ پیشرفتِ زنده می‌بینی و با دکمه‌ی "
        "«⛔ توقف» هر لحظه می‌تونی قطعش کنی. توجه: هر زمان فقط یک ارسالِ "
        "هم‌زمان مجازه؛ تا ارسالِ فعلی تموم/متوقف نشه، ارسالِ جدید رد می‌شه.\n\n"
        "⚙️ سرعت ارسال: فاصله‌ی زمانی بین هر ارسال (هرچی بیشتر، امن‌تر).\n\n"
        "🖼 ایمپورت عکس پیوی (PDF): عکس‌های پیویِ یه اکانت رو جمع و به PDF تبدیل "
        "می‌کنه.\n\n"
        "📊 آمار من: کل ارسال‌ها، موجودی، و وضعیت اشتراکت.\n\n"
        "📌 مارکر: مهم‌ترین بخش — دکمه‌ی پایین رو بزن.\n" + LINE + "\n"
        "🔒 امنیت: همه‌چیز برای هر کاربر کاملاً جداست؛ هیچکس اکانت‌ها یا "
        "اطلاعات تو رو نمی‌بینه."
    )
    await _respond(event, text,
                   buttons=[[Button.inline("📌 راهنمای مارکر", b"help_marker")],
                            [Button.inline("🔄 انتقال ورکر", b"help_xfer")],
                            [Button.inline("🔙 بازگشت", b"home")]])


@bot.on(events.CallbackQuery(data=b"help_xfer"))
async def help_xfer_cb(event):
    if not await _gate(event, need_active=False):
        return
    await _respond(event, card("🔄 راهنمای انتقالِ ورکر", [
        "بعد از افزودنِ هر اکانت، ربات خودکار یه «تستِ ارسال» به چند مخاطب می‌زنه",
        "تا مطمئن شه ارسالِ این سرور سالمه.",
        LINE,
        "✅ اگه موفق بود: همون سرور خوبه، کاری لازم نیست.",
        "⚠️ اگه سرور اجازهٔ ارسال نداد (مثلاً محدودیتِ آی‌پی / TOO_REQUESTS):",
        "   دکمهٔ «🔄 انتقال به ورکر دیگه و لاگین مجدد» میاد.",
        "   با زدنش، اکانت به یه ورکرِ دیگه منتقل می‌شه و باید **دوباره** با",
        "   شماره و کد لاگین کنی (سشنِ ورکرِ قبلی هم تمیز بسته می‌شه).",
        LINE,
        "ℹ️ این قابلیت وقتی کار می‌کنه که ورکرِ سالمِ دیگه‌ای موجود باشه.",
    ]), buttons=[[Button.inline("🔙 بازگشت", b"help")]])


@bot.on(events.CallbackQuery(data=b"help_marker"))
async def help_marker_cb(event):
    if not await _gate(event, need_active=False):
        return
    text = (
        "📌 مارکر چیه و چطور کار می‌کنه؟\n" + LINE + "\n"
        "مارکر یه «کلمه‌ی نشانه» است که آخرِ کپشنِ یه پیام توی «پیام‌های "
        "ذخیره‌شده» (Saved Messages) اکانت روبیکات می‌ذاری. ربات می‌گرده، اون "
        "پیامِ خاص رو پیدا می‌کنه و همونو برای همه‌ی مخاطبینت فوروارد می‌کنه.\n\n"
        "یعنی خودِ پیام (متن/عکس) رو ربات نمی‌سازه — تو می‌سازیش، ربات فقط همون "
        "پیامِ نشان‌دارت رو پخش می‌کنه.\n" + LINE + "\n"
        "🔧 مرحله به مرحله:\n"
        "1️⃣ توی روبیکا برو به «پیام‌های ذخیره‌شده» خودت.\n"
        "2️⃣ پیامی که می‌خوای پخش بشه (متن یا عکس با کپشن) رو بفرست.\n"
        "3️⃣ آخرِ کپشن همون پیام، مارکرت رو بنویس (مثلاً: کد135).\n"
        "4️⃣ توی ربات دکمه‌ی «📌 مارکر» رو بزن و دقیقاً همون کلمه رو ثبت کن.\n"
        "5️⃣ حالا «🚀 ارسال» بزن؛ ربات اون پیام رو پیدا و پخش می‌کنه.\n" + LINE + "\n"
        "⚠️ نکته‌ها:\n"
        "• مارکر باید دقیقاً مثل هم باشه (همون حروف و فاصله‌ها).\n"
        "• اگه چند پیام مارکر دارن، آخرین پیامِ نشان‌دار ملاکه.\n"
        "• می‌تونی هر وقت خواستی پیامِ مارکردارت رو عوض کنی؛ نیازی به تغییر "
        "تنظیمات ربات نیست تا وقتی همون کلمه‌ی مارکر باشه."
    )
    await _respond(event, text,
                   buttons=[[Button.inline("🔙 بازگشت", b"help")]])


# --------------------------------------------------------------------------- #
# Add account (phone -> code -> optional 2FA).  LOCAL login (master-as-worker).
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"addacc"))
async def addacc_cb(event):
    if not await _gate(event):
        return
    state[event.sender_id] = {"step": "await_phone"}
    await _respond(event,
                   "📱 شماره اکانت روبیکای خودت رو بفرست.\nمثال: `09123456789`",
                   buttons=[[Button.inline("🔙 لغو", b"cancel")]])


async def handle_phone(event, st):
    uid = event.sender_id
    phone = rb.normalize_phone(event.raw_text.strip())
    if not phone or len(phone) < 10:
        await event.respond("شماره نامعتبره. دوباره بفرست.")
        return

    # Choose the least-loaded healthy worker (the local master may be chosen if
    # enabled). The chosen worker is persisted in the login state so the SAME
    # worker handles phone -> code -> optional 2FA password.  A worker-transfer
    # relogin forces a specific (new) worker via st["force_worker"].
    forced = st.get("force_worker")
    if forced:
        w = forced
    else:
        try:
            w = await worker.pick_worker_for_login()
        except Exception:  # noqa: BLE001
            w = None
        if not w:
            w = worker.ensure_master_worker()
    st["worker"] = w

    # ----- REMOTE worker login relay -----
    if w and not worker.is_local(w):
        msg = await event.respond("⏳ در حال ارسال کد ورود ...")
        try:
            data = await worker.api_call(w, "POST", "/login/start", {"phone": phone})
        except Exception as e:  # noqa: BLE001
            state.pop(uid, None)
            pending_login.pop(uid, None)
            pending_xfer.pop(uid, None)
            await _worker_error(uid, "شروع لاگین روی ورکر", e, phone)
            await msg.edit("❌ ارتباط با ورکر برقرار نشد. کمی بعد دوباره تلاش کن.",
                           buttons=main_menu())
            return
        pending_login[uid] = {"phone": phone, "remote": True, "worker": w}
        if data.get("needs_password"):
            st["step"] = "await_password"
            await msg.edit("🔐 این اکانت رمز دومرحله‌ای داره. رمز رو بفرست:",
                           buttons=[[Button.inline("🔙 لغو", b"cancel")]])
        else:
            st["step"] = "await_code"
            await msg.edit("✉️ کدی که روبیکا فرستاده رو بفرست:",
                           buttons=[[Button.inline("🔙 لغو", b"cancel")]])
        return

    # ----- LOCAL (master-as-worker) login: unchanged in-process flow -----
    await account_conn.close(phone)
    msg = await event.respond("⏳ در حال ارسال کد ورود ...")
    try:
        ctx = await rb.start_login(phone)
    except Exception as e:  # noqa: BLE001
        state.pop(uid, None)
        pending_xfer.pop(uid, None)
        await logbus.log_detail("❌ LOGIN START ERROR", e, [f"🆔 {uid}", f"📱 {phone}"])
        await msg.edit("❌ " + logbus.humanize_error(e, "login"), buttons=main_menu())
        return
    status = str(ctx.get("status") or "").upper()
    pending_login[uid] = ctx
    if "PASS" in status:
        st["step"] = "await_password"
        await msg.edit("🔐 این اکانت رمز دومرحله‌ای داره. رمز رو بفرست:",
                       buttons=[[Button.inline("🔙 لغو", b"cancel")]])
    else:
        st["step"] = "await_code"
        await msg.edit("✉️ کدی که روبیکا فرستاده رو بفرست:",
                       buttons=[[Button.inline("🔙 لغو", b"cancel")]])


async def handle_password(event, st):
    uid = event.sender_id
    pwd = event.raw_text.strip()
    ctx = pending_login.get(uid)
    if not ctx:
        state.pop(uid, None)
        await event.respond("نشست لاگین منقضی شد. دوباره «افزودن اکانت» رو بزن.",
                            buttons=main_menu())
        return

    # ----- REMOTE worker login relay -----
    if isinstance(ctx, dict) and ctx.get("remote"):
        w = ctx.get("worker") or st.get("worker")
        phone = ctx["phone"]
        msg = await event.respond("⏳ ارسال کد با رمز دومرحله‌ای ...")
        try:
            await worker.api_call(w, "POST", "/login/password",
                                  {"phone": phone, "password": pwd})
        except Exception as e:  # noqa: BLE001
            await _worker_error(uid, "ارسال رمز روی ورکر", e, phone)
            await msg.edit("❌ رمز پذیرفته نشد یا ارتباط با ورکر قطع شد. دوباره بفرست.")
            return
        st["step"] = "await_code"
        await msg.edit("✉️ کدی که روبیکا فرستاده رو بفرست:",
                       buttons=[[Button.inline("🔙 لغو", b"cancel")]])
        return

    # ----- LOCAL (master-as-worker): unchanged -----
    phone = ctx["phone"]
    msg = await event.respond("⏳ ارسال کد با رمز دومرحله‌ای ...")
    try:
        ctx = await rb.start_login(phone, pass_key=pwd)
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ LOGIN PASSWORD ERROR", e, [f"🆔 {uid}", f"📱 {phone}"])
        await msg.edit("❌ " + logbus.humanize_error(e, "password"))
        return
    pending_login[uid] = ctx
    st["step"] = "await_code"
    await msg.edit("✉️ کدی که روبیکا فرستاده رو بفرست:",
                   buttons=[[Button.inline("🔙 لغو", b"cancel")]])


async def handle_code(event, st):
    uid = event.sender_id
    ctx = pending_login.get(uid)
    if not ctx:
        state.pop(uid, None)
        await event.respond("نشست لاگین منقضی شد. دوباره «افزودن اکانت» رو بزن.",
                            buttons=main_menu())
        return
    code = "".join(ch for ch in event.raw_text if ch.isdigit())

    # ----- REMOTE worker login relay -----
    if isinstance(ctx, dict) and ctx.get("remote"):
        w = ctx.get("worker") or st.get("worker") or {}
        phone = ctx["phone"]
        msg = await event.respond("⏳ در حال ورود ...")
        try:
            data = await worker.api_call(w, "POST", "/login/code",
                                         {"phone": phone, "code": code})
        except Exception as e:  # noqa: BLE001
            pending_login.pop(uid, None)
            state.pop(uid, None)
            pending_xfer.pop(uid, None)
            await _worker_error(uid, "ورود با کد روی ورکر", e, phone)
            await msg.edit("❌ ورود ناموفق (ارتباط با ورکر قطع شد). دوباره تلاش کن.",
                           buttons=main_menu())
            return
        pending_login.pop(uid, None)
        state.pop(uid, None)
        if not data.get("ok"):
            pending_xfer.pop(uid, None)
            await msg.edit("❌ ورود ناموفق.", buttons=main_menu())
            return
        name = data.get("name") or "-"
        guid = data.get("guid") or "-"
        contacts = data.get("contacts", 0)
        with_chat = data.get("with_chat", 0)
        aid = db.add_account(uid, phone, name, str(guid))
        if w.get("id"):
            db.set_account_worker(aid, w["id"])
        await msg.edit(card("✅ اکانت اضافه شد", [
            f"📛 {name}",
            f"📱 {phone}",
            f"👥 مخاطبین : {contacts}",
            f"💬 چت‌ها : {with_chat}",
        ]), buttons=main_menu())
        await logbus.event("➕ ADD ACCOUNT", [
            f"🆔 Customer : {uid}",
            f"📱 {phone}  ({name})",
            f"👥 مخاطبین : {contacts}",
            f"🖥 Worker : {w.get('tag') or w.get('id')}",
            f"🕒 {now()}"], pv_user=uid)
        await _rubika_post_add(uid, aid, phone, w)
        return

    # ----- LOCAL (master-as-worker): unchanged in-process flow -----
    phone = ctx["phone"]
    msg = await event.respond("⏳ در حال ورود ...")
    try:
        await rb.finish_login(ctx, code)
        client = ctx["client"]
        me = await client.get_me()
        guid = rb._guid_of(me) or "-"
        name = rb._name_of(me)
        _ordered, stats = await rb.get_ordered_recipients(client)
        try:
            await client.disconnect()
        except Exception:
            pass
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ LOGIN CODE ERROR", e, [f"🆔 {uid}", f"📱 {phone}"])
        await msg.edit("❌ " + logbus.humanize_error(e, "code"))
        pending_xfer.pop(uid, None)
        return
    finally:
        pending_login.pop(uid, None)
        state.pop(uid, None)

    # Assign the account to the worker chosen at the start of the login flow
    # (the local master here) instead of always forcing the master row.
    w = st.get("worker") or worker.ensure_master_worker() or {}
    aid = db.add_account(uid, phone, name, str(guid))
    if w.get("id"):
        db.set_account_worker(aid, w["id"])

    await msg.edit(card("✅ اکانت اضافه شد", [
        f"📛 {name}",
        f"📱 {phone}",
        f"👥 مخاطبین : {stats.get('contacts', 0)}",
        f"💬 چت‌ها : {stats.get('with_chat', 0)}",
    ]), buttons=main_menu())
    await logbus.event("➕ ADD ACCOUNT", [
        f"🆔 Customer : {uid}",
        f"📱 {phone}  ({name})",
        f"👥 مخاطبین : {stats.get('contacts', 0)}",
        f"🖥 Worker : {w.get('tag') or w.get('id')}",
        f"🕒 {now()}"], pv_user=uid)
    await _rubika_post_add(uid, aid, phone, w)


# --------------------------------------------------------------------------- #
# Rubika send-test (probe) after add + worker transfer.
# --------------------------------------------------------------------------- #
async def _local_session_delete(phone):
    """Close + delete the LOCAL session file(s) for a phone on the master."""
    import glob
    phone = rb.normalize_phone(phone)
    try:
        await account_conn.close(phone)
    except Exception:
        pass
    try:
        for f in glob.glob(rb.session_path(phone) + "*"):
            try:
                os.remove(f)
            except Exception:
                pass
    except Exception:
        pass


async def _local_terminate_bots(phone):
    """Terminate OTHER bot sessions (PWA + file-path device) from the local
    master, without touching the customer's real phone/app session."""
    async def _do(client):
        terminated = 0
        try:
            res = await client.get_my_sessions()
            data = None
            for attr in ("to_dict", "original_update"):
                obj = getattr(res, attr, None)
                if callable(obj):
                    try:
                        data = obj()
                        break
                    except Exception:
                        pass
                elif obj is not None:
                    data = obj
                    break
            for s in ((data or {}).get("other_sessions") or []):
                dev = str(s.get("device") or "")
                app_type = str(s.get("app_type") or "").lower()
                key = s.get("key")
                is_bot = app_type == "pwa" and (dev.startswith("/") or "acc_" in dev)
                if key and s.get("terminatable", True) and is_bot:
                    try:
                        await client.terminate_session(key)
                        terminated += 1
                    except Exception:
                        pass
        except Exception:
            pass
        return terminated
    try:
        return await account_conn.call(phone, _do, timeout=120)
    except Exception:
        return 0


async def _run_send_probe(uid, phone, w, marker, count):
    """Forward the marker to a few contacts to verify the worker/IP can send.
    Runs on the remote worker (/sendprobe) or in-process for a local account."""
    if w and not worker.is_local(w):
        try:
            return await worker.api_call(w, "POST", "/sendprobe",
                                         {"phone": phone, "marker": marker,
                                          "count": count}, timeout=300)
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": repr(e)[:140]}

    async def _do(client):
        saved_guid, mid = await rb.find_marked_message(
            client, marker or config.FORWARD_MARKER)
        if not mid:
            return {"ok": False, "error": "marker_not_found"}
        ordered, _stats = await rb.get_ordered_recipients(client)
        targets = [r["guid"] for r in ordered][:max(1, int(count))]
        sent = too = other = 0
        for guid in targets:
            try:
                await asyncio.wait_for(
                    rb.forward_message(client, saved_guid, guid, mid),
                    timeout=config.SEND_TIMEOUT)
                sent += 1
            except Exception as e:  # noqa: BLE001
                if "too_requests" in repr(e).lower():
                    too += 1
                else:
                    other += 1
        return {"ok": True, "sent": sent, "too_requests": too,
                "other": other, "total": len(targets)}
    try:
        return await account_conn.call(phone, _do, timeout=300)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": repr(e)[:140]}


async def _rubika_post_add(uid, aid, phone, w):
    """After a Rubika account logs in: if this was a transfer, terminate the old
    worker's session from the NEW worker; then auto-run a send-test and, if the
    server is blocked, offer to transfer to another worker."""
    xfer = pending_xfer.pop(uid, None)
    if xfer:
        terminated = None
        try:
            if w and not worker.is_local(w):
                r = await worker.api_call(w, "POST", "/session/terminate_bots",
                                          {"phone": phone}, timeout=120)
                terminated = (r or {}).get("terminated")
            else:
                terminated = await _local_terminate_bots(phone)
        except Exception as e:  # noqa: BLE001
            await _worker_error(uid, "بستن سشن قدیمی روی ورکر جدید", e, phone)
        rows = [f"🆔 {uid}", f"📱 {phone}",
                f"🧹 سشن‌های قدیمی بسته‌شده : "
                f"{terminated if terminated is not None else '?'}"]
        if not terminated:
            rows.append("⚠️ هیچ سشنِ قدیمی‌ای بسته نشد (دستی از اپ بررسی کن).")
        rows.append(f"🕒 {now()}")
        await logbus.event("🔄 RB TRANSFER LOGOUT", rows)

    if not config.RB_SENDPROBE_ENABLED:
        return
    marker = db.get_marker(uid)
    try:
        pm = await bot.send_message(uid, "🧪 در حال تستِ ارسالِ این سرور (تا ۳ مخاطب) ...")
    except Exception:
        pm = None
    res = await _run_send_probe(uid, phone, w, marker, config.RB_SENDPROBE_COUNT)
    sent = int(res.get("sent") or 0)
    too = int(res.get("too_requests") or 0)
    err = res.get("error")

    buttons = None
    if res.get("ok") and too == 0 and sent > 0:
        text = card("✅ تستِ ارسال موفق", [
            f"📱 {phone}", f"✅ {sent} ارسالِ موفق",
            "این سرور برای ارسال سالمه.",
            LINE,
            f"📌 مارکری که پیدا شد: «{marker}»",
            "برای ارسالِ انبوه، «🚀 ارسال» رو بزن.",
        ])
        buttons = [[Button.inline("🚀 ارسال", b"send_menu")],
                   [Button.inline("🔙 روبیکا", b"rubika_open")]]
        await logbus.event("✅ RB SEND PROBE OK", [
            f"🆔 {uid}", f"📱 {phone}", f"✅ {sent}", f"📌 {marker}",
            f"🕒 {now()}"])
    elif err == "marker_not_found":
        text = card("🧪 تست انجام نشد — مارکر پیدا نشد", [
            f"📱 {phone}",
            "پیامِ مارکر توی Saved Messages این اکانت نیست.",
            LINE,
            "برای اینکه ارسال کار کنه:",
            "۱) توی Saved Messages همین اکانت یه پیام بذار (متن یا عکس).",
            f"۲) آخرِ متن/کپشنش دقیقاً بنویس:  {marker}",
            "۳) بعد دوباره «🚀 ارسال» رو بزن.",
        ])
        buttons = [[Button.inline("📌 تنظیم/تغییر مارکر", b"marker")],
                   [Button.inline("📖 راهنمای مارکر", b"help_marker")]]
        await logbus.event("🧪 RB SEND PROBE SKIPPED", [
            f"🆔 {uid}", f"📱 {phone}",
            f"📌 مارکرِ «{marker}» توی Saved Messages نبود",
            "→ مشتری باید پیامِ مارکر بذاره و دوباره ارسال بزنه",
            f"🕒 {now()}"], pv_user=uid)
    else:
        reason = (f"🚫 {too} بار TOO_REQUESTS" if too
                  else "💥 ارسالِ تستِ این سرور ناموفق بود")
        text = card("⚠️ ارسالِ این سرور مشکل داره", [
            f"📱 {phone}", reason,
            "احتمالاً محدودیتِ آی‌پیِ این سرور. می‌تونی اکانت رو به ورکر دیگه "
            "منتقل کنی و دوباره لاگین کنی."])
        buttons = [[Button.inline("🔄 انتقال به ورکر دیگه و لاگین مجدد",
                                  f"rbxfer_{aid}".encode())]]
        # full detail (incl. raw error) -> central log group ONLY, never the customer
        await logbus.to_group(card("⚠️ RB SEND PROBE FAILED", [
            f"🆔 {uid}", f"📱 {phone}", f"🚫 too={too}  ✅ sent={sent}",
            f"💥 {err or '-'}", f"🕒 {now()}"]))

    try:
        if pm is not None:
            await pm.edit(text, buttons=buttons)
        else:
            await bot.send_message(uid, text, buttons=buttons)
    except Exception:
        try:
            await bot.send_message(uid, text, buttons=buttons)
        except Exception:
            pass


@bot.on(events.CallbackQuery(pattern=b"rbxfer_(\\d+)"))
async def rbxfer_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    acc = db.get_account_owned(aid, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    old_w = worker.worker_for_account(acc)
    old_id = old_w.get("id") if old_w else None
    try:
        new_w = await worker.pick_worker_for_login(exclude_id=old_id)
    except Exception:  # noqa: BLE001
        new_w = None
    if not new_w:
        await _respond(event, card("🔄 انتقال به ورکر دیگه", [
            "الان ورکرِ سالمِ دیگه‌ای برای انتقال نیست.",
            "اول یه ورکرِ دیگه (مثلاً با آی‌پی ایران) اضافه کن."]),
            buttons=[[Button.inline("🔙 بازگشت", b"home")]])
        return
    phone = acc["phone"]
    # delete the OLD worker's session so it can never use this account again
    try:
        if old_w and not worker.is_local(old_w):
            await worker.api_call(old_w, "POST", "/session/delete",
                                  {"phone": phone}, timeout=60)
        else:
            await _local_session_delete(phone)
    except Exception as e:  # noqa: BLE001
        await _worker_error(uid, "حذف سشن روی ورکر قدیمی", e, phone)

    db.set_account_worker(aid, new_w.get("id"))
    pending_xfer[uid] = {"aid": aid}
    state[uid] = {"step": "await_phone", "force_worker": new_w, "xfer_aid": aid}
    await _respond(event, card("🔄 انتقال به ورکر دیگه", [
        f"📱 {phone}",
        f"🖥 ورکرِ جدید : {new_w.get('tag') or new_w.get('id')}",
        LINE,
        "حالا شمارهٔ همون اکانت رو دوباره بفرست تا روی ورکرِ جدید لاگین شه.",
    ]), buttons=[[Button.inline("🔙 لغو", b"cancel")]])


# --------------------------------------------------------------------------- #
# Remote-worker error helper: log to the group, never crash the bot.
# --------------------------------------------------------------------------- #
async def _worker_error(uid, action: str, exc, phone: str = None):
    """Log a remote-worker (tunnel/HTTP) failure to the central group. The
    caller is responsible for telling the customer (so the wording fits the
    step). Never raises."""
    rows = [f"🆔 Customer : {uid}", f"🛠 عملیات : {action}"]
    if phone:
        rows.append(f"📱 {phone}")
    rows.append(f"💥 {repr(exc)[:160]}")
    rows.append(f"🕒 {now()}")
    try:
        await logbus.to_group(card("⚠️ WORKER ERROR", rows))
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Accounts list + per-account menu.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"accounts"))
async def accounts_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    accounts = db.list_accounts(uid)
    if not accounts:
        await _respond(event, "هنوز اکانتی اضافه نکردی.",
                       buttons=[[Button.inline("➕ افزودن اکانت", b"addacc")],
                                [Button.inline("🔙 بازگشت", b"home")]])
        return
    rows = []
    for i, a in enumerate(accounts, 1):
        mark = "" if a["status"] == "active" else " ⚠️"
        rows.append([Button.inline(f"{i}- {a['phone']}{mark}",
                                   f"acc_{a['id']}".encode())])
    rows.append([Button.inline("🔙 بازگشت", b"home")])
    await _respond(event, "👤 اکانت‌های تو:", buttons=rows)


@bot.on(events.CallbackQuery(pattern=b"acc_(\\d+)"))
async def account_menu_cb(event):
    if not await _gate(event):
        return
    aid = int(event.pattern_match.group(1))
    acc = db.get_account_owned(aid, event.sender_id)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    status = "فعال ✅" if acc["status"] == "active" else "غیرفعال ⚠️ (سشن باطل)"
    await _respond(event, card("👤 اکانت", [
        f"📛 نام : {acc['name'] or '-'}",
        f"📱 شماره : {acc['phone']}",
        f"⭐️ وضعیت : {status}",
    ]), buttons=[
        [Button.inline("🚀 ارسال", f"send_{aid}".encode())],
        [Button.inline("🗑 حذف اکانت", f"del_{aid}".encode())],
        [Button.inline("🔙 بازگشت", b"accounts")],
    ])


@bot.on(events.CallbackQuery(pattern=b"del_(\\d+)"))
async def del_confirm_cb(event):
    if not await _gate(event):
        return
    aid = int(event.pattern_match.group(1))
    if not db.get_account_owned(aid, event.sender_id):
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    await _respond(event, "از حذف این اکانت مطمئنی؟",
                   buttons=[[Button.inline("✅ بله", f"delyes_{aid}".encode())],
                            [Button.inline("🔙 خیر", f"acc_{aid}".encode())]])


@bot.on(events.CallbackQuery(pattern=b"delyes_(\\d+)"))
async def del_do_cb(event):
    if not await _gate(event):
        return
    aid = int(event.pattern_match.group(1))
    acc = db.get_account_owned(aid, event.sender_id)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    try:
        await account_conn.close(acc["phone"])
    except Exception:
        pass
    db.delete_account(aid)
    await _respond(event, "اکانت حذف شد. ✅",
                   buttons=[[Button.inline("🔙 بازگشت", b"accounts")]])


# --------------------------------------------------------------------------- #
# Account health check (چک‌حساب): count + verify each session.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"health")) 
async def health_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    accounts = db.list_accounts(uid)
    if not accounts:
        await _respond(event, "هنوز اکانتی اضافه نکردی.",
                       buttons=[[Button.inline("➕ افزودن اکانت", b"addacc")],
                                [Button.inline("🔙 بازگشت", b"home")]])
        return
    await _respond(event, "🩺 در حال بررسی سلامت اکانت‌ها ... کمی صبر کن.")
    asyncio.create_task(run_health_check(uid))


async def run_health_check(uid: int):
    accounts = db.list_accounts(uid)
    alive = 0
    dead = 0
    rows = []
    for a in accounts:
        phone = a["phone"]
        # skip an account that's mid-send / mid-export: verifying it opens a
        # SECOND connection on the same session, which makes Rubika revoke it
        # (invalid_auth). It's already known to be in use, so it's alive.
        if a["id"] in active_jobs:
            rows.append(f"• {phone} : 🟡 در حال ارسال/پردازش — رد شد")
            continue
        w = worker.worker_for_account(a)
        try:
            if w and not worker.is_local(w):
                data = await worker.api_call(w, "POST", "/account/verify",
                                             {"phone": phone}, timeout=60)
                is_dead = bool(data.get("dead"))
            else:
                # hard timeout so one stuck/dead session can't hang the whole
                # health check (local connect can otherwise block forever).
                is_dead = await asyncio.wait_for(
                    account_conn.verify_session_dead(phone), timeout=45)
        except asyncio.TimeoutError:
            rows.append(f"• {phone} : ❔ بررسی نشد (تایم‌اوت)")
            continue
        except Exception:
            is_dead = False
        if is_dead:
            dead += 1
            db.set_status(a["id"], "inactive")
            rows.append(f"• {phone} : 🔴 سشن پریده")
        else:
            alive += 1
            if a["status"] != "active":
                db.set_status(a["id"], "active")
            rows.append(f"• {phone} : 🟢 سالم")
    summary = card("🩺 چک‌حساب", [
        f"📊 تعداد کل : {len(accounts)}",
        f"🟢 سالم : {alive}   🔴 پریده : {dead}",
        LINE, *rows, f"🕒 {now()}"])
    await logbus.event("🩺 ACCOUNT HEALTH", [
        f"🆔 {uid}", f"کل {len(accounts)} | 🟢 {alive} | 🔴 {dead}",
        f"🕒 {now()}"])
    try:
        await bot.send_message(uid, summary, buttons=main_menu())
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Marker + speed settings (per customer).
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"marker"))
async def marker_cb(event):
    if not await _gate(event):
        return
    await _show_content_menu(event, event.sender_id)


async def _show_content_menu(event, uid):
    marker = db.get_marker(uid)
    up = db.get_upload_file(uid)
    up_label = f"«{up['name']}» ✅" if up else "تنظیم‌نشده"
    rows = [
        [Button.inline("✏️ تغییر مارکر", b"mkr_text")],
        [Button.inline("📤 تنظیم فایلِ آپلودِ خودکار", b"mkr_file")],
    ]
    if up:
        rows.append([Button.inline("🗑 حذف فایلِ آپلود", b"mkr_filedel")])
    rows.append([Button.inline("🔙 بازگشت", b"home")])
    await _respond(event, card("📌 محتوای ارسال", [
        f"📌 مارکر فعلی : «{marker}»",
        f"📤 فایلِ آپلودِ خودکار : {up_label}",
        LINE,
        "موقعِ ارسال انتخاب می‌کنی «مارکر» بره یا همین «فایلِ ثبت‌شده».",
        "فایل یک‌بار ثبت می‌شه؛ بعد هر ارسال فقط تأیید می‌کنی.",
    ]), buttons=rows)


@bot.on(events.CallbackQuery(data=b"mkr_text"))
async def marker_set_text_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    state[uid] = {"step": "await_marker"}
    await _respond(event,
                   f"📌 مارکر فعلی: «{db.get_marker(uid)}»\n{LINE}\n"
                   "مارکر جدید رو بفرست (همون متنی که آخرِ کپشنِ پیامِ نشان‌دار توی "
                   "Saved Messages روبیکاته):",
                   buttons=[[Button.inline("🔙 لغو", b"cancel")]])


@bot.on(events.CallbackQuery(data=b"mkr_file"))
async def marker_set_file_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    state[uid] = {"step": "await_upload_config"}
    await _respond(event, card("📤 تنظیم فایلِ آپلودِ خودکار", [
        "فایل (یا عکس) رو همین‌جا بفرست تا ثبت بشه.",
        "📝 هر کپشنی که همراهِ فایل بنویسی، ذخیره و موقعِ ارسال هم فرستاده می‌شه.",
        "👉 برای فایلِ دست‌نخورده، به‌صورتِ «فایل» (نه عکسِ فشرده) بفرست.",
        "از این به بعد موقعِ ارسال می‌تونی به‌جای مارکر همین فایل رو بفرستی.",
    ]), buttons=[[Button.inline("🔙 لغو", b"cancel")]])


@bot.on(events.CallbackQuery(data=b"mkr_filedel"))
async def marker_del_file_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    try:
        old = db.get_upload_file(uid)
        if old and os.path.exists(old["path"]):
            os.remove(old["path"])
    except Exception:
        pass
    db.clear_upload_file(uid)
    await event.answer("فایلِ آپلود حذف شد.", alert=True)
    await _show_content_menu(event, uid)


async def handle_marker(event, st):
    uid = event.sender_id
    marker = event.raw_text.strip()
    state.pop(uid, None)
    if not marker:
        await event.respond("مارکر نمی‌تونه خالی باشه.", buttons=main_menu())
        return
    db.set_marker(uid, marker)
    await event.respond(f"✅ مارکر روی «{marker}» تنظیم شد.", buttons=main_menu())
    await logbus.event("📌 MARKER SET", [
        f"🆔 {uid}", f"📌 متن مارکر مشتری: «{marker}»", f"🕒 {now()}"])


def speed_buttons():
    return [
        [Button.inline("0.2s", b"sp_0.2"), Button.inline("0.5s", b"sp_0.5"),
         Button.inline("1s", b"sp_1")],
        [Button.inline("2s", b"sp_2"), Button.inline("5s", b"sp_5"),
         Button.inline("10s", b"sp_10")],
        [Button.inline("🔙 بازگشت", b"home")],
    ]


@bot.on(events.CallbackQuery(data=b"speed"))
async def speed_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    await _respond(event,
                   f"⏱ تأخیر فعلی: {db.get_delay(uid)} ثانیه\n{LINE}\n"
                   "یک سرعت انتخاب کن:", buttons=speed_buttons())


@bot.on(events.CallbackQuery(pattern=b"sp_([0-9.]+)"))
async def speed_set_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    val = config.clamp_delay(event.pattern_match.group(1).decode())
    db.set_delay(uid, val)
    await _respond(event, f"✅ تأخیر روی {val} ثانیه تنظیم شد.",
                   buttons=[[Button.inline("🔙 منو", b"home")]])


# --------------------------------------------------------------------------- #
# Send (marker) — SAME proven logic as the previous project's run_send.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"send_menu"))
async def send_menu_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    accounts = db.list_accounts(uid)
    if not accounts:
        await _respond(event, "اول یک اکانت اضافه کن.",
                       buttons=[[Button.inline("➕ افزودن اکانت", b"addacc")],
                                [Button.inline("🔙 بازگشت", b"home")]])
        return
    rows = [[Button.inline(f"🚀 {a['phone']}", f"send_{a['id']}".encode())]
            for a in accounts if a["status"] == "active"]
    if not rows:
        await _respond(event, "اکانت فعالی نداری. اول «🩺 چک‌حساب» یا افزودن اکانت.",
                       buttons=[[Button.inline("🔙 بازگشت", b"home")]])
        return
    rows.append([Button.inline("🔙 بازگشت", b"home")])
    await _respond(event, "🚀 از کدوم اکانت ارسال بشه؟", buttons=rows)


@bot.on(events.CallbackQuery(pattern=b"send_(\\d+)"))
async def send_prepare_cb(event):
    """Account chosen -> ask HOW to send: marker (current) or auto-upload a file."""
    if not await _gate(event):
        return
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    acc = db.get_account_owned(aid, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    if aid in active_jobs:
        await event.answer("یک ارسال روی این اکانت در حال اجراست.", alert=True)
        return
    busy = customer_active_account(uid, exclude_aid=aid)
    if busy:
        await event.answer(
            f"⛔ همین حالا یک ارسال با اکانت {busy['phone']} در جریانه. "
            "هر مشتری هم‌زمان فقط یک ارسال می‌تونه داشته باشه — "
            "اول اون تموم یا متوقف بشه.", alert=True)
        return
    up = db.get_upload_file(uid)
    rows = [[Button.inline("📌 مارکر", f"sendmk_{aid}".encode())]]
    if up:
        rows.append([Button.inline(f"📤 آپلودِ فایلِ «{up['name']}»",
                                    f"sendup_{aid}".encode())])
    rows.append([Button.inline("📢 ساخت کانال + اد مخاطب", f"chan_{aid}".encode())])
    rows.append([Button.inline("🔙 لغو", b"home")])
    note = (f"📤 فایلِ آپلود: «{up['name']}» ثبت‌شده." if up
            else "📤 فایلِ آپلود ثبت نشده — از بخشِ «📌 مارکر» می‌تونی تنظیمش کنی.")
    await _respond(event, card("🚀 ارسال", [
        f"📱 {acc['phone']}",
        "شیوه‌ی ارسال رو انتخاب کن:",
        "📌 مارکر — پیامی که خودت توی Saved علامت زدی فوروارد می‌شه.",
        note,
    ]), buttons=rows)


@bot.on(events.CallbackQuery(pattern=b"sendmk_(\\d+)"))
async def send_marker_prep_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    acc = db.get_account_owned(aid, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    if aid in active_jobs:
        await event.answer("یک ارسال روی این اکانت در حال اجراست.", alert=True)
        return
    # reserve during prep too (it opens a Rubika connection) so two rapid taps
    # can't open two connections to the same session. send_go_cb re-reserves for
    # the actual run; here we release as soon as prep is done.
    active_jobs.add(aid)
    try:
        await _do_marker_prep(event, uid, aid, acc)
    finally:
        active_jobs.discard(aid)


async def _do_marker_prep(event, uid, aid, acc):
    """Marker path: find the marked message in Saved + build the send payload.
    (Unchanged behaviour — just extracted so the upload path can sit beside it.)"""
    marker = db.get_marker(uid)
    await _respond(event, f"⏳ پیدا کردن پیامِ نشان‌دار «{marker}» و آماده‌سازی لیست ...")

    w = worker.worker_for_account(acc)

    # ----- REMOTE worker: prepare via the worker API -----
    if w and not worker.is_local(w):
        try:
            data = await worker.api_call(w, "POST", "/prepare",
                                         {"phone": acc["phone"], "marker": marker},
                                         timeout=180)
        except Exception as e:  # noqa: BLE001
            await _worker_error(uid, "آماده‌سازی ارسال روی ورکر", e, acc["phone"])
            await bot.send_message(uid, "❌ ارتباط با ورکر برقرار نشد. کمی بعد دوباره تلاش کن.",
                                   buttons=main_menu())
            return
        if not data.get("marker_found"):
            await bot.send_message(uid,
                f"❌ هیچ پیامی با مارکر «{marker}» توی Saved Messages پیدا نشد.",
                buttons=main_menu())
            return
        total = data.get("total", 0)
        pending_send[aid] = {"customer_id": uid, "account_id": aid,
                             "phone": acc["phone"], "remote": True,
                             "worker": w, "total": total}
        await bot.send_message(uid, card("🚀 آماده‌ی ارسال", [
            f"📱 {acc['phone']}",
            f"📌 مارکر «{marker}» پیدا شد ✅",
            f"🎯 تعداد گیرنده : {total}",
            f"⏱ تأخیر : {db.get_delay(uid)}s",
            f"🖥 Worker : {w.get('tag') or w.get('id')}",
        ]), buttons=[[Button.inline("✅ شروع ارسال", f"sendgo_{aid}".encode())],
                     [Button.inline("🔙 لغو", b"home")]])
        return

    # ----- LOCAL (master-as-worker): unchanged in-process flow -----
    await account_conn.close(acc["phone"])
    client = rb.open_client(acc["phone"])
    try:
        await rb.connect_ready(client)
        saved_guid, mid = await rb.find_marked_message(client, marker)
        if not mid:
            await bot.send_message(uid,
                f"❌ هیچ پیامی با مارکر «{marker}» توی Saved Messages پیدا نشد.",
                buttons=main_menu())
            return
        ordered, _stats = await rb.get_ordered_recipients(client)
    except account_conn.InvalidAuthError:
        db.set_status(aid, "inactive")
        await bot.send_message(uid, "🔴 سشن این اکانت باطله. دوباره اضافه‌اش کن.",
                               buttons=main_menu())
        return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ RB PREPARE ERROR", e,
                                [f"🆔 {uid}", f"📱 {acc.get('phone')}"])
        await bot.send_message(uid, "❌ " + logbus.humanize_error(e, "prepare"),
                               buttons=main_menu())
        return
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    recipients = [r["guid"] for r in ordered]
    pending_send[aid] = {"customer_id": uid, "account_id": aid,
                         "phone": acc["phone"], "saved_guid": saved_guid,
                         "mid": mid, "recipients": recipients}
    await bot.send_message(uid, card("🚀 آماده‌ی ارسال", [
        f"📱 {acc['phone']}",
        f"📌 مارکر «{marker}» پیدا شد ✅",
        f"🎯 تعداد گیرنده : {len(recipients)}",
        f"⏱ تأخیر : {db.get_delay(uid)}s",
    ]), buttons=[[Button.inline("✅ شروع ارسال", f"sendgo_{aid}".encode())],
                 [Button.inline("🔙 لغو", b"home")]])


@bot.on(events.CallbackQuery(pattern=b"sendup_(\\d+)"))
async def send_upload_start_cb(event):
    """Auto-upload path: use the customer's CONFIGURED file (set once in the
    marker section) — upload it to the account's Saved, then prep the send."""
    if not await _gate(event):
        return
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    acc = db.get_account_owned(aid, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    if aid in active_jobs:
        await event.answer("یک ارسال روی این اکانت در حال اجراست.", alert=True)
        return
    up = db.get_upload_file(uid)
    if not up:
        await _respond(event, card("📤 آپلودِ خودکار", [
            "هیچ فایلی ثبت نشده.",
            "👉 اول از بخشِ «📌 مارکر» → «تنظیم فایلِ آپلودِ خودکار» فایل رو ثبت کن.",
        ]), buttons=[[Button.inline("📌 بخشِ مارکر", b"marker")],
                     [Button.inline("🔙 منو", b"home")]])
        return
    # reserve during prep (opens a Rubika connection); release once prep done.
    # send_go_cb re-reserves for the actual run.
    active_jobs.add(aid)
    try:
        await _do_upload_prep(event, uid, aid, acc, up)
    finally:
        active_jobs.discard(aid)


async def _do_upload_prep(event, uid, aid, acc, up):
    """Upload the configured file to the account's Saved + build the send payload.
    Works for LOCAL and REMOTE (worker) accounts. On any failure -> marker."""
    await _respond(event, f"⏳ آپلودِ فایلِ «{up['name']}» روی اکانت و آماده‌سازی لیست ...")
    w = worker.worker_for_account(acc)

    # ----- REMOTE worker: upload via the worker API then prepare -----
    if w and not worker.is_local(w):
        try:
            payload = await _remote_upload_prepare(uid, aid, acc, w, up)
        except Exception as e:  # noqa: BLE001
            await logbus.log_detail("❌ RB AUTO-UPLOAD ERROR", e,
                                    [f"🆔 {uid}", f"📱 {acc['phone']}"])
            await bot.send_message(uid, card("❌ آپلودِ خودکار ناموفق بود", [
                logbus.humanize_error(e, "upload"), "👉 از روشِ «📌 مارکر» استفاده کن."]),
                buttons=[[Button.inline("📌 استفاده از مارکر", f"sendmk_{aid}".encode())],
                         [Button.inline("🔙 منو", b"home")]])
            return
        if payload is None:
            return
        pending_send[aid] = payload
        await bot.send_message(uid, card("🚀 آماده‌ی ارسال", [
            f"📱 {acc['phone']}", f"📤 فایلِ «{up['name']}» آپلود شد ✅",
            f"🎯 تعداد گیرنده : {payload.get('total', 0)}",
            f"⏱ تأخیر : {db.get_delay(uid)}s",
            f"🖥 Worker : {w.get('tag') or w.get('id')}",
        ]), buttons=[[Button.inline("✅ شروع ارسال", f"sendgo_{aid}".encode())],
                     [Button.inline("🔙 لغو", b"home")]])
        return

    # ----- LOCAL (master-as-worker) upload -----
    await account_conn.close(acc["phone"])
    client = rb.open_client(acc["phone"])
    saved_guid = mid = None
    ordered = []
    try:
        await rb.connect_ready(client)
        try:
            saved_guid, mid = await asyncio.wait_for(
                rb.upload_file_to_self(client, up["path"], caption=up.get("caption") or "",
                                       file_name=up["name"]),
                timeout=300)
        except Exception as e:  # noqa: BLE001 — upload failed -> marker fallback
            await logbus.log_detail("❌ RB AUTO-UPLOAD ERROR", e,
                                    [f"🆔 {uid}", f"📱 {acc['phone']}"])
            await bot.send_message(uid, card("❌ آپلودِ خودکار ناموفق بود", [
                logbus.humanize_error(e, "upload"), "👉 از روشِ «📌 مارکر» استفاده کن."]),
                buttons=[[Button.inline("📌 استفاده از مارکر", f"sendmk_{aid}".encode())],
                         [Button.inline("🔙 منو", b"home")]])
            return
        ordered, _stats = await rb.get_ordered_recipients(client)
    except account_conn.InvalidAuthError:
        db.set_status(aid, "inactive")
        await bot.send_message(uid, "🔴 سشن این اکانت باطله. دوباره اضافه‌اش کن.",
                               buttons=main_menu())
        return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ RB PREPARE ERROR", e,
                                [f"🆔 {uid}", f"📱 {acc.get('phone')}"])
        await bot.send_message(uid, "❌ " + logbus.humanize_error(e, "prepare"),
                               buttons=main_menu())
        return
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    recipients = [r["guid"] for r in ordered]
    pending_send[aid] = {"customer_id": uid, "account_id": aid,
                         "phone": acc["phone"], "saved_guid": saved_guid,
                         "mid": mid, "recipients": recipients}
    await logbus.event("📤 RB AUTO-UPLOAD OK", [
        f"🆔 {uid}", f"📱 {acc['phone']}",
        f"📎 {up['name']}", f"🎯 {len(recipients)}", f"🕒 {now()}"], pv_user=uid)
    await bot.send_message(uid, card("🚀 آماده‌ی ارسال", [
        f"📱 {acc['phone']}", f"📤 فایلِ «{up['name']}» آپلود شد ✅",
        f"🎯 تعداد گیرنده : {len(recipients)}",
        f"⏱ تأخیر : {db.get_delay(uid)}s",
    ]), buttons=[[Button.inline("✅ شروع ارسال", f"sendgo_{aid}".encode())],
                 [Button.inline("🔙 لغو", b"home")]])


async def _remote_upload_prepare(uid, aid, acc, w, up):
    """Upload the configured file to a REMOTE worker's account (Saved Messages)
    and return a send payload that forwards THAT message. Raises on failure so
    the caller falls back to the marker flow. (Worker must run updated code with
    the /upload/prepare endpoint — update workers from the owner panel.)"""
    import base64
    with open(up["path"], "rb") as f:
        file_b64 = base64.b64encode(f.read()).decode()
    data = await worker.api_call(w, "POST", "/upload/prepare", {
        "phone": acc["phone"], "file_b64": file_b64,
        "file_name": up["name"], "caption": up.get("caption") or ""}, timeout=300)
    if not data.get("ok") or not data.get("message_id"):
        raise RuntimeError(data.get("error") or "worker upload failed")
    return {"customer_id": uid, "account_id": aid, "phone": acc["phone"],
            "remote": True, "worker": w, "total": data.get("total", 0),
            "upload_mid": data.get("message_id"),
            "upload_saved": data.get("saved_guid")}


@bot.on(events.NewMessage(func=lambda e: e.is_private and e.file is not None))
async def upload_config_capture(event):
    """Store the file the customer sends in the marker section as their persistent
    auto-upload file (set once, reused every send)."""
    uid = event.sender_id
    st = state.get(uid)
    if not st or st.get("step") != "await_upload_config":
        return  # not our flow -> let other handlers deal with it
    # consume the conversation state up-front so a blocked/maintenance bounce
    # can't leave the customer stuck in await_upload_config.
    state.pop(uid, None)
    if db.is_blocked(uid) or db.maintenance_on():
        return
    try:
        fname = event.file.name
    except Exception:
        fname = None
    caption = (event.raw_text or "").strip()
    msg = await event.respond("⏳ در حال ذخیره‌ی فایل ...")

    # remove any previously stored file for this customer first
    try:
        old = db.get_upload_file(uid)
        if old and os.path.exists(old["path"]):
            os.remove(old["path"])
    except Exception:
        pass
    store_dir = os.path.join(DATA_DIR, "upload_store", str(uid))
    os.makedirs(store_dir, exist_ok=True)
    try:
        path = await event.download_media(file=store_dir)
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ UPLOAD FILE DOWNLOAD ERROR", e, [f"🆔 {uid}"])
        await msg.edit("❌ دریافتِ فایل ناموفق بود. دوباره بفرست.", buttons=main_menu())
        return
    if not path:
        await msg.edit("❌ فایل دریافت نشد. دوباره تلاش کن.", buttons=main_menu())
        return
    name = fname or os.path.basename(path)
    db.set_upload_file(uid, path, name, caption=caption)
    await msg.edit(card("✅ فایلِ آپلودِ خودکار ثبت شد", [
        f"📎 {name}",
        (f"📝 کپشن: «{caption[:60]}»" if caption else "📝 بدونِ کپشن"),
        "حالا موقعِ ارسال می‌تونی به‌جای مارکر، همین فایل رو بفرستی —",
        "هر بار فقط تأیید می‌کنی، دیگه لازم نیست دوباره بفرستی.",
    ]), buttons=main_menu())
    await logbus.event("📤 UPLOAD FILE SET", [
        f"🆔 {uid}", f"📎 {name}",
        (f"📝 {caption[:80]}" if caption else "📝 بدون کپشن"),
        f"🕒 {now()}"], pv_user=uid)


# =========================================================================== #
# CHANNEL MODE (ISOLATED + ADDITIVE) — ported from the previous project.
# Create a channel with the chosen account, forward the marked message into it,
# then add the account's OWN contacts as members (batched, up to a target).
# REUSES the existing engine: rb.create_channel / rb.forward_message /
# rb.seed_channel_with_contacts and the worker endpoints /channel/create,
# /channel/add. Touches nothing else — delete this block to revert cleanly.
# =========================================================================== #
@bot.on(events.CallbackQuery(pattern=b"chan_(\\d+)"))
async def channel_start_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    acc = db.get_account_owned(aid, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    if aid in active_jobs:
        await event.answer("یک ارسال روی این اکانت در حال اجراست.", alert=True)
        return
    busy = customer_active_account(uid, exclude_aid=aid)
    if busy:
        await event.answer(
            f"⛔ همین حالا یک ارسال با اکانت {busy['phone']} در جریانه. "
            "اول اون تموم یا متوقف بشه.", alert=True)
        return
    state[uid] = {"step": "await_channel_name", "account_id": aid}
    await _respond(event, card("📢 ساخت کانال", [
        f"📱 {acc['phone']}",
        "اسمِ کانالی که می‌خوای ساخته بشه رو بفرست. مثال: کانالِ من",
    ]), buttons=[[Button.inline("🔙 لغو", b"cancel")]])


async def handle_channel_name(event, st):
    uid = event.sender_id
    aid = st.get("account_id")
    name = (event.raw_text or "").strip()
    state.pop(uid, None)
    if not name:
        await event.respond("اسمِ کانال نمی‌تونه خالی باشه. دوباره از «📢 ساخت کانال» شروع کن.",
                            buttons=main_menu())
        return
    acc = db.get_account_owned(aid, uid)
    if not acc:
        await event.respond("اکانت پیدا نشد.", buttons=main_menu())
        return
    if aid in active_jobs:
        await event.respond("یک ارسال روی این اکانت در حال اجراست. کمی بعد امتحان کن.",
                            buttons=main_menu())
        return
    marker = db.get_marker(uid)
    w = worker.worker_for_account(acc)
    # reserve while we hold a Rubika connection (single-connection rule).
    active_jobs.add(aid)
    try:
        if w and not worker.is_local(w):
            await _channel_create_remote(event, uid, acc, w, name, marker)
        else:
            await _channel_create_local(event, uid, acc, name, marker)
    finally:
        active_jobs.discard(aid)


def _channel_ready_buttons(aid):
    return [[Button.inline("👥 شروع عضو کردن مخاطبین", f"chadd_{aid}".encode())],
            [Button.inline("🏠 منوی اصلی", b"home")]]


def _channel_ready_card(name, marker, forwarded):
    return card("📢 کانال ساخته شد ✅", [
        f"🎛 کانال : {name}",
        (f"📎 پیامِ نشان‌دار «{marker}» ارسال شد ✅" if forwarded
         else f"⚠️ پیامِ نشان‌دار «{marker}» ارسال نشد (ولی کانال ساخته شد)"),
        LINE,
        f"حالا می‌تونی مخاطب‌ها رو {config.CHANNEL_ADD_BATCH}تا‌{config.CHANNEL_ADD_BATCH}تا "
        f"تا سقفِ {config.CHANNEL_MEMBER_TARGET} عضو کنی.",
    ])


async def _channel_create_local(event, uid, acc, name, marker):
    msg = await event.respond(f"⏳ در حال ساختِ کانال «{name}» ...")
    await account_conn.close(acc["phone"])
    client = rb.open_client(acc["phone"])
    channel_guid = None
    forwarded = False
    try:
        await rb.connect_ready(client)
        saved_guid, mid = await rb.find_marked_message(client, marker)
        channel_guid = await rb.create_channel(client, name)
        if mid and channel_guid:
            try:
                await rb.forward_message(client, saved_guid, channel_guid, mid)
                forwarded = True
            except Exception:
                forwarded = False
    except account_conn.InvalidAuthError:
        db.set_status(acc["id"], "inactive")
        try:
            await msg.edit("🔴 سشن این اکانت باطله. دوباره اضافه‌اش کن.", buttons=main_menu())
        except Exception:
            pass
        try:
            await client.disconnect()
        except Exception:
            pass
        return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ CHANNEL CREATE ERROR", e,
                                [f"🆔 {uid}", f"📱 {acc['phone']}"])
        try:
            await msg.edit("❌ " + logbus.humanize_error(e, "generic"), buttons=main_menu())
        except Exception:
            pass
        try:
            await client.disconnect()
        except Exception:
            pass
        return
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
    if not channel_guid:
        await msg.edit("❌ ساختِ کانال ناموفق بود. دوباره امتحان کن.", buttons=main_menu())
        return
    pending_channel[uid] = {"account_id": acc["id"], "phone": acc["phone"],
                            "channel_name": name, "channel_guid": channel_guid,
                            "remote": False}
    await msg.edit(_channel_ready_card(name, marker, forwarded),
                   buttons=_channel_ready_buttons(acc["id"]))


async def _channel_create_remote(event, uid, acc, w, name, marker):
    msg = await event.respond(f"⏳ ساختِ کانال «{name}» روی ورکر ...")
    try:
        res = await worker.api_call(w, "POST", "/channel/create",
                                    {"phone": acc["phone"], "marker": marker,
                                     "title": name}, timeout=120)
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ CHANNEL CREATE ERROR (remote)", e,
                                [f"🆔 {uid}", f"📱 {acc['phone']}"])
        await msg.edit("❌ " + logbus.humanize_error(e, "generic"), buttons=main_menu())
        return
    if not res.get("ok") or not res.get("channel_guid"):
        await msg.edit("❌ ساختِ کانال روی ورکر ناموفق بود. دوباره امتحان کن.",
                       buttons=main_menu())
        return
    pending_channel[uid] = {"account_id": acc["id"], "phone": acc["phone"],
                            "channel_name": name, "channel_guid": res["channel_guid"],
                            "remote": True, "worker": w}
    await msg.edit(_channel_ready_card(name, marker, res.get("forwarded")),
                   buttons=_channel_ready_buttons(acc["id"]))


@bot.on(events.CallbackQuery(pattern=b"chadd_(\\d+)"))
async def channel_add_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    payload = pending_channel.get(uid)
    if not payload or payload["account_id"] != aid:
        await event.answer("اطلاعاتِ کانال منقضی شده. دوباره از «📢 ساخت کانال» شروع کن.",
                           alert=True)
        return
    if aid in active_jobs:
        await event.answer("یک ارسال روی این اکانت در حال اجراست.", alert=True)
        return
    # reserve atomically before launching the background add task.
    active_jobs.add(aid)
    await _respond(event, card("👥 عضو کردنِ مخاطبین", [
        f"🎛 کانال : {payload['channel_name']}",
        f"دسته‌های {config.CHANNEL_ADD_BATCH}تایی تا سقفِ {config.CHANNEL_MEMBER_TARGET}.",
        "گزارشِ پایان همین‌جا و در گروهِ لاگ میاد.",
    ]))
    if payload.get("remote"):
        asyncio.create_task(_run_channel_add_remote(uid, payload))
    else:
        asyncio.create_task(_run_channel_add_local(uid, payload))


async def _channel_add_report(uid, phone, name, added):
    await logbus.event("📢 CHANNEL MEMBERS ADDED", [
        f"🆔 {uid}", f"📱 {phone}", f"🎛 {name}", f"✅ {added}", f"🕒 {now()}"], pv_user=uid)
    try:
        await bot.send_message(
            uid, f"✅ عضو کردنِ مخاطبینِ کانال «{name}» تمام شد. تعداد: {added}",
            buttons=main_menu())
    except Exception:
        pass


async def _run_channel_add_local(uid, payload):
    aid = payload["account_id"]
    phone = payload["phone"]
    name = payload["channel_name"]
    guid = payload["channel_guid"]
    added = 0
    try:
        await account_conn.close(phone)
        client = rb.open_client(phone)
        try:
            await rb.connect_ready(client)
            added = await rb.seed_channel_with_contacts(
                client, guid, target=config.CHANNEL_MEMBER_TARGET,
                batch=config.CHANNEL_ADD_BATCH, delay=config.CHANNEL_ADD_DELAY)
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ CHANNEL ADD ERROR", e, [f"🆔 {uid}", f"📱 {phone}"])
    finally:
        active_jobs.discard(aid)
        pending_channel.pop(uid, None)
    await _channel_add_report(uid, phone, name, added)


async def _run_channel_add_remote(uid, payload):
    aid = payload["account_id"]
    phone = payload["phone"]
    name = payload["channel_name"]
    w = payload.get("worker")
    added = 0
    try:
        res = await worker.api_call(w, "POST", "/channel/add", {
            "phone": phone, "channel_guid": payload["channel_guid"],
            "target": config.CHANNEL_MEMBER_TARGET,
            "batch": config.CHANNEL_ADD_BATCH,
            "delay": config.CHANNEL_ADD_DELAY}, timeout=600)
        added = res.get("added", 0)
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ CHANNEL ADD ERROR (remote)", e,
                                [f"🆔 {uid}", f"📱 {phone}"])
    finally:
        active_jobs.discard(aid)
        pending_channel.pop(uid, None)
    await _channel_add_report(uid, phone, name, added)


@bot.on(events.CallbackQuery(pattern=b"sendgo_(\\d+)"))
async def send_go_cb(event):
    if not await _gate(event):
        return
    aid = int(event.pattern_match.group(1))
    payload = pending_send.get(aid)
    if not payload or payload["customer_id"] != event.sender_id:
        await event.answer("اطلاعات ارسال منقضی شده. دوباره شروع کن.", alert=True)
        return
    if aid in active_jobs:
        await event.answer("ارسال در حال اجراست.", alert=True)
        return
    busy = customer_active_account(event.sender_id, exclude_aid=aid)
    if busy:
        await event.answer(
            f"⛔ همین حالا یک ارسال با اکانت {busy['phone']} در جریانه. "
            "هر مشتری هم‌زمان فقط یک ارسال می‌تونه داشته باشه — "
            "اول اون تموم یا متوقف بشه.", alert=True)
        return
    # RESERVE this account NOW — the check above + this add happen with NO await
    # in between, so two rapid starts (on this OR another account of the same
    # customer) can't both pass the guard. run_send re-adds it idempotently and
    # owns its release in its finally; _safe_launch_send guarantees release even
    # if run_send raises before reaching its own finally.
    active_jobs.add(aid)
    # show a simple 'started' message (with stop button); progress comes as a
    # periodic SEND PROGRESS card here + in the log group (no live editing).
    try:
        await event.edit("🚀 ارسال شروع شد. گزارشِ پیشرفت (هر ۵۰ تا) همین‌جا و در "
                         "گروهِ لاگ میاد.",
                         buttons=[[Button.inline("⛔ توقف", f"sendstop_{aid}".encode())]])
    except Exception:
        pass
    payload["notify_chat"] = event.sender_id
    asyncio.create_task(_safe_launch_send(payload))


@bot.on(events.CallbackQuery(pattern=b"sendstop_(\\d+)"))
async def send_stop_cb(event):
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    # only the OWNER of this account may stop its send (the previous check
    # `event.sender_id != event.sender_id` was always False -> no check at all).
    if not db.get_account_owned(aid, uid):
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    stop_flags[aid] = True
    await event.answer("درخواست توقف ثبت شد.")


async def _wait_or_stop(account_id: int, seconds: float, step: float = 2.0) -> bool:
    waited = 0.0
    while waited < seconds:
        if stop_flags.get(account_id):
            return True
        d = min(step, seconds - waited)
        await asyncio.sleep(d)
        waited += d
    return False


async def _safe_launch_send(payload: dict):
    """Launch run_send so the per-account reservation in active_jobs is ALWAYS
    released — even if run_send raises before reaching its own finally. run_send
    discards the reservation itself in the normal flow; this is the safety net."""
    account_id = payload.get("account_id")
    try:
        await run_send(payload)
    except Exception as e:  # noqa: BLE001
        try:
            await logbus.to_group(card("❌ SEND LAUNCH ERROR", [
                f"🆔 {payload.get('customer_id')}",
                f"📱 {payload.get('phone')}",
                f"💥 {repr(e)[:160]}", f"🕒 {now()}"]))
        except Exception:
            pass
    finally:
        if account_id is not None:
            active_jobs.discard(account_id)


async def run_send(payload: dict):
    """Reused send loop from the previous project (forward the marked message to
    every recipient, MAX_ERRORS cap, auto-resume)."""
    # Remote accounts delegate the whole send to their worker over the API.
    w = payload.get("worker")
    if payload.get("remote") and w and not worker.is_local(w):
        await _run_send_remote(payload)
        return
    uid = payload["customer_id"]
    account_id = payload["account_id"]
    phone = payload["phone"]
    saved_guid = payload["saved_guid"]
    mid = payload["mid"]
    recipients = payload["recipients"]
    marker = db.get_marker(uid)
    delay = db.get_delay(uid)

    total = len(recipients)
    ok = 0
    fail = 0
    prog_logged = 0
    started = datetime.now()
    reason = None
    stop_flags.pop(account_id, None)
    active_jobs.add(account_id)

    await logbus.event("🚀 SEND STARTED", [
        f"🆔 Customer : {uid}",
        f"📱 Phone : {phone}",
        f"🎯 Targets : {total}",
        f"⏱ Delay : {delay}s",
        f"📌 Marker : «{marker}» ✅",
        f"🕒 {now()}"], pv_user=uid)

    n = total
    idx = 0
    retry_count = 0
    await account_conn.close(phone)
    client = rb.open_client(phone)
    try:
        await rb.connect_ready(client)
        while True:
            attempt_fail = 0
            hit_max = False
            while idx < n:
                if stop_flags.get(account_id):
                    reason = "توقف دستی توسط کاربر"
                    break
                guid = recipients[idx]
                idx += 1
                try:
                    await asyncio.wait_for(
                        rb.forward_message(client, saved_guid, guid, mid),
                        timeout=config.SEND_TIMEOUT)
                    ok += 1
                    attempt_fail = 0   # count CONSECUTIVE errors only
                    db.incr_customer_sends(uid, 1)
                    w = worker.ensure_master_worker()
                    if w:
                        db.incr_worker_sent(w["id"], 1)
                except Exception as e:  # noqa: BLE001
                    err_txt = repr(e)[:200]
                    # An auth-looking error is NOT proof the session is dead.
                    # CONFIRM on a fresh connection. CRITICAL: do NOT disconnect
                    # and reopen our send client on a transient auth error — that
                    # rapid reconnect is exactly what makes Rubika revoke the
                    # session (this is the ONLY behavioural difference from the
                    # proven YoudonoaAx build, which keeps the SAME connection).
                    if account_conn.is_auth_error(e):
                        really_dead = False
                        try:
                            really_dead = await asyncio.wait_for(
                                account_conn.verify_session_dead(phone), timeout=45)
                        except Exception:
                            really_dead = False
                        if really_dead:
                            reason = "🔴 سشنِ اکانت باطل شده — دوباره اضافه‌اش کن"
                            db.set_status(account_id, "inactive")
                            try:
                                await logbus.to_group(card(
                                    "🔎 AUTH-ERROR (سشن مرده اعلام شد)", [
                                        f"📱 {phone}", f"🆔 {uid}",
                                        f"💥 {err_txt}",
                                        "verify_session_dead=True (اتصال تازه هم auth-error داد)",
                                        f"🕒 {now()}"]))
                            except Exception:
                                pass
                            break
                        # confirmed ALIVE -> transient hiccup; KEEP the same
                        # connection and continue (no disconnect, no reopen).
                    fail += 1
                    attempt_fail += 1
                    try:
                        await logbus.to_group(card("❌ SEND ERROR", [
                            f"📱 اکانت : {phone}",
                            f"🆔 مشتری : {uid}",
                            f"💥 {err_txt}",
                            f"🕒 {now()}"]))
                    except Exception:
                        pass
                    if attempt_fail >= config.MAX_ERRORS:
                        hit_max = True
                        break
                # periodic progress card (every N sends) -> originating chat + log group
                step = config.SEND_PROGRESS_STEP or 50
                if ok - prog_logged >= step:
                    prog_logged = ok
                    await _post_progress(payload, ok, fail, total)
                await asyncio.sleep(delay)

            if reason:
                break
            if not hit_max:
                break
            if retry_count >= config.RESUME_MAX_RETRIES:
                reason = f"رسیدن به سقف خطا ({config.MAX_ERRORS})"
                break
            retry_count += 1
            await logbus.event("⏸ ارسال موقتاً متوقف شد", [
                f"🆔 {uid}", f"📱 {phone}",
                f"⚠️ {config.MAX_ERRORS} خطای پشت‌سرهم",
                f"⏳ {config.RESUME_WAIT // 60} دقیقه صبر، بعد خودکار ادامه می‌ده",
                f"📊 ✅ {ok}   ❌ {fail}   📁 {total}",
                f"🕒 {now()}"], pv_user=uid)
            if await _wait_or_stop(account_id, config.RESUME_WAIT):
                reason = "توقف دستی توسط کاربر"
                break
            await logbus.event("▶️ ارسال ادامه یافت", [
                f"🆔 {uid}", f"📱 {phone}", f"🕒 {now()}"], pv_user=uid)
            try:
                await client.disconnect()
            except Exception:
                pass
            client = rb.open_client(phone)
            await rb.connect_ready(client)
    except account_conn.InvalidAuthError:
        reason = "سشن باطل شد (نیاز به افزودن مجدد اکانت)"
        db.set_status(account_id, "inactive")
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ SEND LOOP ERROR", e, [f"🆔 {uid}", f"📱 {phone}"])
        reason = "خطای داخلی هنگام ارسال"
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        active_jobs.discard(account_id)
        stop_flags.pop(account_id, None)
        pending_send.pop(account_id, None)

    dur = str(datetime.now() - started).split(".")[0]
    await _post_final_report(payload, ok, fail, total, dur, reason, retry_count)
    if reason:
        await logbus.event("⛔ SEND STOPPED", [
            f"🆔 {uid}", f"📱 {phone}",
            f"📊 ✅ {ok}   ❌ {fail}   📁 {total}",
            f"⚠️ {reason}", f"⏱ {dur}", f"🕒 {now()}"])
    else:
        await logbus.event("✅ SEND FINISHED", [
            f"🆔 {uid}", f"📱 {phone}",
            f"✅ {ok}   ❌ {fail}   📁 {total}", f"⏱ {dur}", f"🕒 {now()}"])


async def _safe_send(uid, text):
    try:
        await bot.send_message(uid, text, buttons=main_menu())
    except Exception:
        pass


async def _run_send_remote(payload: dict):
    """Run a send on a REMOTE worker: start the job, then poll its status (and
    relay a manual stop). Customer messages + group log cards work exactly like
    the local path. Any tunnel/HTTP failure is logged and reported, never fatal."""
    uid = payload["customer_id"]
    account_id = payload["account_id"]
    phone = payload["phone"]
    w = payload["worker"]
    marker = db.get_marker(uid)
    delay = db.get_delay(uid)
    total = payload.get("total", 0)

    started = datetime.now()
    reason = None
    ok = 0
    fail = 0
    prog_logged = 0
    fail_logged = 0
    last_state = "sending"
    stop_flags.pop(account_id, None)
    active_jobs.add(account_id)

    await logbus.event("🚀 SEND STARTED", [
        f"🆔 Customer : {uid}",
        f"📱 Phone : {phone}",
        f"🎯 Targets : {total}",
        f"⏱ Delay : {delay}s",
        f"📌 Marker : «{marker}» ✅",
        f"🖥 Worker : {w.get('tag') or w.get('id')}",
        f"🕒 {now()}"], pv_user=uid)

    try:
        start_body = {
            "phone": phone, "marker": marker, "delay": delay,
            "max_errors": config.MAX_ERRORS, "send_timeout": config.SEND_TIMEOUT,
            "resume_wait": config.RESUME_WAIT, "max_retries": config.RESUME_MAX_RETRIES,
        }
        # only for auto-upload sends — never send null keys (an int-typed field
        # on the worker rejects explicit null => 422; also keeps marker sends
        # working on workers that predate these fields).
        if payload.get("upload_mid"):
            start_body["message_id"] = payload["upload_mid"]
            start_body["saved_guid"] = payload.get("upload_saved")
        data = await worker.api_call(w, "POST", "/send/start", start_body,
                                     timeout=120)
        if not data.get("ok") or not data.get("marker_found"):
            reason = "مارکر پیدا نشد یا ورکر ارسال رو شروع نکرد"
        else:
            job_id = data.get("job_id")
            total = data.get("total", total)
            poll_errors = 0
            while True:
                if stop_flags.get(account_id):
                    try:
                        await worker.api_call(w, "POST", f"/send/stop/{job_id}",
                                              timeout=30)
                    except Exception:
                        pass
                    reason = "توقف دستی توسط کاربر"
                    try:
                        stt = await worker.api_call(w, "GET",
                                                    f"/send/status/{job_id}", timeout=30)
                        ok = stt.get("ok", ok)
                        fail = stt.get("fail", fail)
                    except Exception:
                        pass
                    break
                await asyncio.sleep(5)
                try:
                    stt = await worker.api_call(w, "GET",
                                                f"/send/status/{job_id}", timeout=30)
                    poll_errors = 0
                except Exception as e:  # noqa: BLE001
                    poll_errors += 1
                    if poll_errors >= 6:
                        reason = "ارتباط با ورکر در حین ارسال قطع شد"
                        await _worker_error(uid, "پایش وضعیت ارسال روی ورکر", e, phone)
                        break
                    continue
                ok = stt.get("ok", ok)
                fail = stt.get("fail", fail)
                # per-error -> ONLY the central log group (which account + customer)
                wtag = w.get("tag") or w.get("id")
                while fail_logged < fail:
                    fail_logged += 1
                    try:
                        await logbus.to_group(card("❌ SEND ERROR", [
                            f"📱 اکانت : {phone}",
                            f"🆔 مشتری : {uid}",
                            f"🖥 ورکر : {wtag}",
                            f"🕒 {now()}"]))
                    except Exception:
                        pass
                # pause / resume -> log to BOTH the owner group and the customer PV
                cur_state = stt.get("state")
                if cur_state == "waiting" and last_state != "waiting":
                    await logbus.event("⏸ ارسال موقتاً متوقف شد", [
                        f"🆔 {uid}", f"📱 {phone}",
                        f"⚠️ {config.MAX_ERRORS} خطای پشت‌سرهم",
                        f"⏳ {config.RESUME_WAIT // 60} دقیقه صبر، بعد خودکار ادامه می‌ده",
                        f"📊 ✅ {ok}   ❌ {fail}   📁 {total}",
                        f"🕒 {now()}"], pv_user=uid)
                elif cur_state == "sending" and last_state == "waiting":
                    await logbus.event("▶️ ارسال ادامه یافت", [
                        f"🆔 {uid}", f"📱 {phone}",
                        f"📊 ✅ {ok}   ❌ {fail}   📁 {total}",
                        f"🕒 {now()}"], pv_user=uid)
                last_state = cur_state
                # periodic progress card (every N sends) -> originating chat + log group
                step = config.SEND_PROGRESS_STEP or 50
                if total and ok - prog_logged >= step:
                    prog_logged = ok
                    await _post_progress(payload, ok, fail, total)
                if stt.get("done"):
                    raw = stt.get("reason")
                    if raw == "manual_stop":
                        reason = "توقف دستی توسط کاربر"
                    elif raw and str(raw).startswith("invalid_auth"):
                        reason = "🔴 سشنِ اکانت باطل شده — دوباره اضافه‌اش کن"
                        try:
                            db.set_status(account_id, "inactive")
                        except Exception:
                            pass
                    elif raw:
                        reason = f"رسیدن به سقف خطا ({config.MAX_ERRORS})" \
                            if str(raw).startswith("max_errors") else str(raw)
                    break
    except Exception as e:  # noqa: BLE001
        reason = "خطای ارتباط با ورکر هنگام ارسال"
        await _worker_error(uid, "ارسال روی ورکر", e, phone)
    finally:
        active_jobs.discard(account_id)
        stop_flags.pop(account_id, None)
        pending_send.pop(account_id, None)

    # mirror counters onto the master DB (worker doesn't touch the master DB)
    if ok:
        db.incr_customer_sends(uid, ok)
        try:
            db.incr_worker_sent(w["id"], ok)
        except Exception:
            pass

    dur = str(datetime.now() - started).split(".")[0]
    await _post_final_report(payload, ok, fail, total, dur, reason)
    if reason:
        await logbus.event("⛔ SEND STOPPED", [
            f"🆔 {uid}", f"📱 {phone}",
            f"📊 ✅ {ok}   ❌ {fail}   📁 {total}",
            f"⚠️ {reason}", f"⏱ {dur}", f"🕒 {now()}"])
    else:
        await logbus.event("✅ SEND FINISHED", [
            f"🆔 {uid}", f"📱 {phone}",
            f"✅ {ok}   ❌ {fail}   📁 {total}", f"⏱ {dur}", f"🕒 {now()}"])


# --------------------------------------------------------------------------- #
# PV image -> PDF export (reused logic).
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"pvexport"))
async def pvexport_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    accounts = db.list_accounts(uid)
    if not accounts:
        await event.answer("اول یک اکانت اضافه کن.", alert=True)
        return
    rows = [[Button.inline(f"🖼 {a['phone']}", f"pvx_{a['id']}".encode())]
            for a in accounts if a["status"] == "active"]
    rows.append([Button.inline("🔙 بازگشت", b"home")])
    await _respond(event,
                   "🖼 از کدوم اکانت عکس‌های پیوی جمع بشه و PDF بشه؟\n"
                   "(فقط عکس — فیلم/گیف نه.)", buttons=rows)


@bot.on(events.CallbackQuery(pattern=b"pvx_(\\d+)"))
async def pvexport_run_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    aid = int(event.pattern_match.group(1))
    acc = db.get_account_owned(aid, uid)
    if not acc:
        await event.answer("اکانت پیدا نشد.", alert=True)
        return
    # don't open a connection on an account that's mid-send (second connection
    # on one session => Rubika revokes it / invalid_auth).
    if aid in active_jobs:
        await event.answer("روی این اکانت ارسال در حال اجراست. بعد از اتمام دوباره بزن.",
                           alert=True)
        return
    # PV export is memory-heavy; cap concurrency to protect the server (anti-OOM).
    if uid in pv_export_jobs:
        await event.answer("ایمپورت قبلی‌ات هنوز در حال اجراست. صبر کن تموم بشه.",
                           alert=True)
        return
    if len(pv_export_jobs) >= config.PV_EXPORT_MAX_CONCURRENT:
        await event.answer("📸 ایمپورت تصویر الان در دسترس نیست — یه مشتری دیگه در "
                           "حال استفاده‌ست. چند دقیقه بعد دوباره امتحان کن.",
                           alert=True)
        return
    pv_export_jobs.add(uid)
    await _respond(event,
                   f"⏳ جمع‌آوری عکس‌های پیویِ {acc['phone']} ... ممکنه چند دقیقه طول بکشه.",
                   buttons=[[Button.inline("⛔ توقف و دریافت همینا", b"pvstop")],
                            [Button.inline("🏠 منو", b"home")]])
    asyncio.create_task(run_pv_export(uid, acc))


async def run_pv_export(uid: int, acc):
    # mark the account busy for the whole export so a concurrent send /
    # health-check can't open a SECOND connection on the same session.
    active_jobs.add(acc["id"])
    # Always release the global export slot + stop flag, however this finishes.
    try:
        await _run_pv_export(uid, acc)
    finally:
        active_jobs.discard(acc["id"])
        pv_export_jobs.discard(uid)
        pv_export_stop.discard(uid)


@bot.on(events.CallbackQuery(data=b"pvstop"))
async def pvexport_stop_cb(event):
    """Customer asked to stop their running PV export and get what's collected."""
    uid = event.sender_id
    if uid in pv_export_jobs:
        pv_export_stop.add(uid)
        await event.answer("⛔ توقف ثبت شد — همین عکس‌های جمع‌شده آماده و ارسال می‌شن.")
    else:
        await event.answer("ایمپورت فعالی نداری.", alert=True)


async def _safe_msg(uid, text, buttons=None):
    """Send a message and return the message object (for later edits), or None."""
    try:
        return await bot.send_message(uid, text, buttons=buttons)
    except Exception:
        return None


class _PvDelivery:
    """Collects prepared (lighter) JPEGs and delivers CUMULATIVE PDFs to the
    customer: a new PDF every PV_EXPORT_PDF_BATCH photos (PDF #1 = first 20,
    PDF #2 = first 40, ...), and a final COMPLETE PDF (also logged to the central
    group). Shared by the local, remote and legacy paths so behaviour is
    identical everywhere.

    Each photo is decoded/downscaled/re-encoded to a light JPEG exactly ONCE
    (prepare_image), so rebuilding the cumulative PDF over and over stays cheap
    and memory stays low (lighter than the raw blobs). Everything is sequential —
    no parallel downloads/builds — to protect a small server."""

    def __init__(self, uid, phone, progress_msg, where=""):
        self.uid = uid
        self.phone = phone
        self.progress_msg = progress_msg
        self.where = where            # title suffix on the progress card
        self.jpegs = []               # prepared JPEG blobs (the cumulative archive)
        self.sent = 0                 # photo count covered by the last PDF sent
        self.seq = 0                  # customer-facing PDF counter
        self._last_t = 0.0
        self._last_n = -1

    @property
    def found(self):
        return len(self.jpegs)

    async def add(self, blob):
        """Prepare ONE photo, and if we've crossed a batch boundary push a
        cumulative PDF to the customer."""
        jp = await asyncio.to_thread(
            pdf_export.prepare_image, blob,
            config.PV_EXPORT_PDF_QUALITY, config.PV_EXPORT_PDF_MAX_EDGE)
        if not jp:
            return
        self.jpegs.append(jp)
        batch = config.PV_EXPORT_PDF_BATCH
        if batch and (len(self.jpegs) - self.sent) >= batch:
            await self._deliver(final=False)

    async def progress(self, scanned, total_chats, force=False):
        if self.progress_msg is None or scanned is None:
            return
        t = _time.time()
        n = len(self.jpegs)
        # throttle: at most every ~3s, or whenever +25 new photos are found
        if not force and (t - self._last_t < 3 and n - self._last_n < 25):
            return
        self._last_t = t
        self._last_n = n
        try:
            await self.progress_msg.edit(card(f"🖼 در حال جمع‌آوری{self.where}", [
                f"📱 {self.phone}",
                f"💬 چت اسکن‌شده : {scanned}/{total_chats}",
                f"🖼 عکس پیداشده : {n}",
                f"📄 PDF ارسالی : {self.seq}",
            ]), buttons=[[Button.inline("⛔ توقف و دریافت همینا", b"pvstop")]])
        except Exception:
            pass

    async def _deliver(self, final):
        n = len(self.jpegs)
        if n == 0:
            return
        new_for_customer = n > self.sent
        if not final and not new_for_customer:
            return
        out_path = os.path.join(
            DATA_DIR,
            f"pv_{self.phone}_{int(datetime.now().timestamp())}_{self.seq + 1}.pdf")
        try:
            built = await asyncio.to_thread(
                pdf_export.build_pdf_from_jpegs, list(self.jpegs), out_path)
            if new_for_customer:
                self.seq += 1
                tag = "نهایی" if final else "تجمیعی"
                await bot.send_file(
                    self.uid, out_path,
                    caption=f"🖼 PDF شماره {self.seq} ({tag}) — {built} عکس از {self.phone}",
                    force_document=True)
            if final:
                # only the COMPLETE archive is mirrored to the central log group
                rows = [f"🆔 {self.uid}", f"📱 {self.phone}", f"🖼 {built} عکس"]
                sc = getattr(self, "_scanned", None)
                if sc is not None:
                    rows.append(f"💬 چت اسکن‌شده : {sc}/{getattr(self, '_total_chats', '?')}")
                rows.append(f"🕒 {now()}")
                await logbus.event("🖼 PV IMAGE EXPORT", rows)
                await logbus.to_group_file(
                    out_path,
                    caption=f"🖼 فایل ایمپورت تصاویر — {self.phone} ({built})")
            self.sent = n
        except Exception as e:  # noqa: BLE001
            await logbus.log_detail("❌ PDF EXPORT ERROR", e, [f"🆔 {self.uid}"])
            await _safe_send(self.uid, "❌ ساخت/ارسالِ PDF ناموفق بود. دوباره امتحان کن.")
        finally:
            try:
                os.remove(out_path)
            except Exception:
                pass

    async def finish(self, scanned, total_chats, stopped):
        """Send the final complete PDF (and log it). Tells the customer if no
        photo was found."""
        self._scanned = scanned
        self._total_chats = total_chats
        await self.progress(scanned, total_chats, force=True)
        if self.found == 0:
            extra = ""
            if scanned is not None:
                extra = f"\n(چت‌های اسکن‌شده: {scanned} از {total_chats})"
            await _safe_send(self.uid,
                             f"ℹ️ هیچ عکسی در پیوی‌های {self.phone} پیدا نشد.{extra}")
            return
        if stopped:
            await _safe_send(
                self.uid,
                f"⛔ جمع‌آوری متوقف شد. {self.found} عکسِ جمع‌شده آماده می‌شه.")
        if self.progress_msg is not None:
            try:
                await self.progress_msg.edit(card("🖼 ساخت PDF نهایی", [
                    f"📱 {self.phone}",
                    f"🖼 {self.found} عکس جمع شد.",
                    "⏳ در حال ساخت و ارسال PDF نهایی ...",
                ]))
            except Exception:
                pass
        await self._deliver(final=True)
        await _safe_send(self.uid, "✅ آرشیو کامل ارسال شد.")


async def _run_pv_export_remote_legacy(uid, acc, w, delivery):
    """Fallback for workers that predate job-based export: use the old blocking
    /pvexport/run, then feed the photos through the SAME cumulative delivery."""
    import base64
    phone = acc["phone"]
    try:
        data = await worker.api_call(w, "POST", "/pvexport/run", {
            "phone": phone, "max_chats": config.PV_EXPORT_MAX_CHATS,
            "max_photos": config.PV_EXPORT_MAX_PHOTOS,
        }, timeout=1800)
    except Exception as e:  # noqa: BLE001
        await _worker_error(uid, "ایمپورت عکس پیوی روی ورکر", e, phone)
        await _safe_send(uid, "❌ ارتباط با ورکر در ایمپورت عکس قطع شد. بعداً دوباره تلاش کن.")
        return
    if not data.get("ok"):
        await _safe_send(uid, f"❌ جمع‌آوری ناموفق: {str(data.get('error', ''))[:140]}")
        return
    for b64 in data.get("photos_b64", []):
        try:
            blob = base64.b64decode(b64)
        except Exception:
            continue
        await delivery.add(blob)
    await delivery.finish(None, None, False)


async def _run_pv_export_remote(uid, acc, w):
    """REMOTE PV export — RESILIENT job-based. The worker collects photos in the
    background; the master polls, DRAINS photos incrementally (so a disconnect
    can't lose what's already collected), shows live progress, honours the stop
    button, and delivers cumulative PDFs. Falls back to the old blocking
    /pvexport/run if the worker hasn't been updated yet (HTTP 404)."""
    import base64
    phone = acc["phone"]
    pv_export_stop.discard(uid)
    stop_btn = [[Button.inline("⛔ توقف و دریافت همینا", b"pvstop")]]
    progress_msg = await _safe_msg(uid, card("🖼 جمع‌آوری روی ورکر", [
        f"📱 {phone}",
        f"🖥 Worker : {w.get('tag') or w.get('id')}",
        "⏳ در حال جمع‌آوری عکس‌ها روی ورکر ...",
    ]), buttons=stop_btn)
    delivery = _PvDelivery(uid, phone, progress_msg, " (ورکر)")

    # start the job; an old worker has no /pvexport/start -> 404 -> fallback
    try:
        start = await worker.api_call(w, "POST", "/pvexport/start", {
            "phone": phone, "max_chats": config.PV_EXPORT_MAX_CHATS,
            "max_photos": config.PV_EXPORT_MAX_PHOTOS,
        }, timeout=60)
        job_id = start.get("job_id")
    except Exception as e:  # noqa: BLE001
        if getattr(getattr(e, "response", None), "status_code", None) == 404:
            await _run_pv_export_remote_legacy(uid, acc, w, delivery)
            return
        await _worker_error(uid, "شروع ایمپورت عکس روی ورکر", e, phone)
        await _safe_send(uid, "❌ ارتباط با ورکر برای شروع ایمپورت برقرار نشد. بعداً دوباره تلاش کن.")
        return
    if not job_id:
        await _safe_send(uid, "❌ ورکر job ایمپورت رو شروع نکرد.")
        return

    scanned = total_chats = 0
    stopped = False
    fails = 0
    while True:
        # forward the stop request to the worker if the customer pressed it
        if uid in pv_export_stop:
            try:
                await worker.api_call(w, "POST", f"/pvexport/stop/{job_id}", timeout=30)
            except Exception:
                pass
        try:
            st = await worker.api_call(w, "GET", f"/pvexport/status/{job_id}", timeout=60)
            fails = 0
        except Exception as e:  # noqa: BLE001
            # transient disconnect: the worker keeps collecting and the tunnel
            # auto-reopens, so retry a few times before giving up with what we
            # already drained (resilient to brief outages).
            fails += 1
            if fails >= config.PV_EXPORT_MAX_POLL_FAILS:
                await _worker_error(uid, "دریافت پیشرفت ایمپورت از ورکر", e, phone)
                break
            await asyncio.sleep(config.PV_EXPORT_POLL_SEC)
            continue
        scanned = st.get("scanned", scanned)
        total_chats = st.get("total_chats", total_chats)
        for b64 in st.get("photos_b64", []):
            try:
                blob = base64.b64decode(b64)
            except Exception:
                continue
            await delivery.add(blob)
        await delivery.progress(scanned, total_chats)
        if st.get("stopped"):
            stopped = True
        if st.get("done"):
            break
        await asyncio.sleep(config.PV_EXPORT_POLL_SEC)

    await delivery.finish(scanned, total_chats, stopped)


async def _run_pv_export(uid: int, acc):
    phone = acc["phone"]
    w = worker.worker_for_account(acc)

    # ----- REMOTE worker path -----
    if w and not worker.is_local(w):
        await _run_pv_export_remote(uid, acc, w)
        return

    # ----- LOCAL (master-as-worker) path: live progress + stop + cumulative PDFs
    pv_export_stop.discard(uid)
    stop_btn = [[Button.inline("⛔ توقف و دریافت همینا", b"pvstop")]]
    progress_msg = await _safe_msg(uid, card("🖼 جمع‌آوری عکس‌های پیوی", [
        f"📱 {phone}", "⏳ شروع اسکن چت‌ها ..."]), buttons=stop_btn)
    delivery = _PvDelivery(uid, phone, progress_msg, "")

    async def _do(client):
        guids = await rb.get_chat_list_guids(client, only_users=True)
        total_chats = len(guids)
        scanned = 0
        stopped = False
        for g in guids[:config.PV_EXPORT_MAX_CHATS]:
            if uid in pv_export_stop:
                stopped = True
                break
            scanned += 1
            async for _mid, fi in rb.iter_chat_photos(client, g):
                if uid in pv_export_stop:
                    stopped = True
                    break
                try:
                    blob = await asyncio.wait_for(
                        rb.download_photo(client, fi),
                        timeout=config.PV_DOWNLOAD_TIMEOUT)
                except Exception:
                    # timeout / decode / network error on ONE photo -> skip it
                    continue
                if blob:
                    await delivery.add(blob)
                    await delivery.progress(scanned, total_chats)
                if delivery.found >= config.PV_EXPORT_MAX_PHOTOS:
                    return total_chats, scanned, False
            if stopped:
                break
            await delivery.progress(scanned, total_chats)
        return total_chats, scanned, stopped

    try:
        total_chats, scanned_chats, stopped = await account_conn.call(
            phone, _do, timeout=1800)
    except account_conn.InvalidAuthError:
        db.set_status(acc["id"], "inactive")
        await _safe_send(uid, "🔴 سشن این اکانت باطله. دوباره اضافه‌اش کن.")
        return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ PV COLLECT ERROR", e, [f"🆔 {uid}"])
        await _safe_send(uid, "❌ جمع‌آوریِ عکس‌ها ناموفق بود. دوباره امتحان کن.")
        return

    await delivery.finish(scanned_chats, total_chats, stopped)


# --------------------------------------------------------------------------- #
# Free-text router (drives the conversation state machine).
GROUP_MEDIA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "data", "group_media")
os.makedirs(GROUP_MEDIA_DIR, exist_ok=True)


async def _gconf_menu(event, uid):
    cfgs = db.list_group_configs(uid)
    rows = []
    for c in cfgs:
        en = "🟢" if c.get("enabled") else "🔴"
        rows.append([Button.inline(f"{en} گروه {c['group_id']}",
                                   f"gconf_g_{c['group_id']}".encode())])
    rows.append([Button.inline("➕ افزودن گروه", b"gconf_add")])
    rows.append([Button.inline("🏠 منوی اصلی", b"mainmenu")])
    body = ["گروه‌های ثبت‌شده‌ت رو اینجا مدیریت کن.",
            "اول گروه رو اضافه کن، آیدی ادمین‌ها رو بذار، محتوا رو ست کن،",
            "بعد ربات رو ادمینِ گروهت کن تا «نصب شد» بفرسته."]
    if not cfgs:
        body.append(LINE)
        body.append("هنوز گروهی اضافه نکردی.")
    await _respond(event, card("⚙️ تنظیمات گروه", body), buttons=rows)


@bot.on(events.CallbackQuery(data=b"gconf"))
async def gconf_cb(event):
    if not await _gate(event):
        return
    await _gconf_menu(event, event.sender_id)


@bot.on(events.CallbackQuery(data=b"gconf_add"))
async def gconf_add_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    if db.count_group_configs(uid) >= config.GROUP_MAX_PER_CUSTOMER:
        await event.answer(f"به سقفِ {config.GROUP_MAX_PER_CUSTOMER} گروه رسیدی.",
                           alert=True)
        return
    state[uid] = {"step": "await_gconf_group"}
    await _respond(event, card("➕ افزودن گروه", [
        "آیدیِ عددیِ گروهت رو بفرست (مثل `-1001234567890`).",
        "آیدی رو می‌تونی از رباتایی مثل @username_to_id_bot بگیری.",
    ]), buttons=[[Button.inline("🔙 لغو", b"gconf")]])


async def _gconf_handle_group(event, st):
    uid = event.sender_id
    raw = (event.raw_text or "").strip().replace(" ", "")
    if not raw.lstrip("-").isdigit():
        await event.respond("آیدی نامعتبره. یک عددِ گروه بفرست (مثل -1001234567890).")
        return
    gid = int(raw)
    existing = db.get_group_config(gid)
    if existing and existing.get("customer_id") != uid:
        state.pop(uid, None)
        await event.respond("این گروه قبلاً توسط حساب دیگه‌ای ثبت شده.",
                            buttons=main_menu())
        return
    if not existing and db.count_group_configs(uid) >= config.GROUP_MAX_PER_CUSTOMER:
        state.pop(uid, None)
        await event.respond(f"به سقفِ {config.GROUP_MAX_PER_CUSTOMER} گروه رسیدی.",
                            buttons=main_menu())
        return
    db.upsert_group_config(gid, uid)
    st["gid"] = gid
    st["step"] = "await_gconf_admins"
    await event.respond(card("👤 ادمین‌ها", [
        f"گروه {gid} ثبت شد ✅",
        "حالا آیدیِ عددیِ ادمین‌ها رو بفرست (خودت + هر کسی که اجازه‌ی دستوره).",
        "چند آیدی رو با ویرگول جدا کن. مثال: `12345, 67890`",
    ]), buttons=[[Button.inline("🔙 انصراف", b"gconf")]])
    await logbus.event("⚙️ GROUP CONFIG ADD", [
        f"🆔 {uid}", f"💬 گروه : {gid}", f"🕒 {now()}"], pv_user=uid)


async def _gconf_handle_admins(event, st):
    uid = event.sender_id
    gid = st.get("gid")
    if not gid:
        state.pop(uid, None)
        await event.respond("نشست منقضی شد. دوباره از «تنظیمات گروه» شروع کن.",
                            buttons=main_menu())
        return
    ids = []
    for part in (event.raw_text or "").replace(" ", "").split(","):
        if part.lstrip("-").isdigit():
            ids.append(part)
    ids = ids[:config.GROUP_MAX_ADMINS]
    if not ids:
        await event.respond("هیچ آیدیِ معتبری نفرستادی. دوباره بفرست (مثل 12345,67890).")
        return
    db.set_group_admins(gid, ",".join(ids))
    state.pop(uid, None)
    await event.respond(card("✅ ادمین‌ها ثبت شد", [
        f"💬 گروه : {gid}",
        f"👤 ادمین‌ها : {', '.join(ids)}",
        LINE,
        "محتوای ارسالی از روی «📌 مارکر» تعیین می‌شه (هم گروه هم پیوی).",
        "مارکر رو از منوی اصلی PV → «📌 مارکر» ست کن، بعد ربات رو ادمینِ گروهت کن.",
    ]), buttons=[[Button.inline("🔍 بررسی نصب", f"gconf_verify_{gid}".encode())],
                 [Button.inline("🔙 تنظیمات گروه", b"gconf")]])


@bot.on(events.CallbackQuery(pattern=b"gconf_g_(-?\\d+)"))
async def gconf_group_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    gid = int(event.pattern_match.group(1).decode())
    cfg = db.get_group_config(gid)
    if not cfg or cfg.get("customer_id") != uid:
        await event.answer("گروه پیدا نشد.", alert=True)
        return
    admins = ", ".join(str(x) for x in sorted(db.group_admin_ids(cfg))) or "-"
    marker = db.get_marker(uid)
    await _respond(event, card(f"⚙️ گروه {gid}", [
        f"👤 ادمین‌ها : {admins}",
        f"📌 مارکر : «{marker}»",
        f"📥 نصب‌شده : {'بله ✅' if cfg.get('installed') else 'هنوز نه'}",
        f"🔄 وضعیت : {'🟢 روشن' if cfg.get('enabled') else '🔴 خاموش'}",
    ]), buttons=[
        [Button.inline("👤 ادمین‌ها", f"gconf_admins_{gid}".encode()),
         Button.inline("🔍 بررسی نصب", f"gconf_verify_{gid}".encode())],
        [Button.inline("🔴 خاموش" if cfg.get("enabled") else "🟢 روشن",
                       f"gconf_toggle_{gid}".encode()),
         Button.inline("🗑 حذف", f"gconf_del_{gid}".encode())],
        [Button.inline("🔙 تنظیمات گروه", b"gconf")]])


@bot.on(events.CallbackQuery(pattern=b"gconf_verify_(-?\\d+)"))
async def gconf_verify_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    gid = int(event.pattern_match.group(1).decode())
    cfg = db.get_group_config(gid)
    if not cfg or cfg.get("customer_id") != uid:
        await event.answer("گروه پیدا نشد.", alert=True)
        return
    await _respond(event, "🔍 در حال بررسیِ عضویت و ادمین‌بودنِ ربات در گروه ...")
    in_group = is_admin = False
    err = ""
    try:
        me = await bot.get_me()
        perms = await asyncio.wait_for(bot.get_permissions(gid, me.id), timeout=25)
        in_group = True
        is_admin = bool(getattr(perms, "is_admin", False))
    except Exception as e:  # noqa: BLE001
        in_group = False
        err = repr(e)[:120]
        await logbus.log_detail("❌ GROUP VERIFY ERROR", e, [f"🆔 {uid}", f"💬 {gid}"])
    db.set_group_installed(gid, in_group and is_admin)
    if in_group and is_admin:
        body = ["✅ ربات در گروه هست و ادمینه. آماده‌ی کاره!"]
        # also post the welcome/install card INTO the group now — so it appears
        # even if the bot was added before the group was configured (the
        # ChatAction 'added' event may have fired with no config back then).
        try:
            await group_panel.send_install_card(gid)
        except Exception:
            pass
    elif in_group:
        body = ["⚠️ ربات در گروه هست ولی ادمین نیست.",
                "برای فرستادن پیام و جواب‌دادن به دستورات، ربات رو ادمینِ گروه کن."]
    else:
        body = ["❌ ربات در این گروه نیست (یا آیدی اشتباهه).",
                "اول ربات رو به گروه اضافه و ادمین کن، بعد دوباره «بررسی نصب» رو بزن."]
    await _respond(event, card("🔍 بررسی نصب", [f"💬 گروه : {gid}"] + body),
                   buttons=[[Button.inline("🔁 بررسی دوباره", f"gconf_verify_{gid}".encode())],
                            [Button.inline("🔙 گروه", f"gconf_g_{gid}".encode())]])
    await logbus.event("🔍 GROUP VERIFY", [
        f"🆔 {uid}", f"💬 گروه : {gid}",
        f"عضو: {in_group} | ادمین: {is_admin}", f"🕒 {now()}"], pv_user=uid)


@bot.on(events.CallbackQuery(pattern=b"gconf_admins_(-?\\d+)"))
async def gconf_admins_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    gid = int(event.pattern_match.group(1).decode())
    cfg = db.get_group_config(gid)
    if not cfg or cfg.get("customer_id") != uid:
        await event.answer("گروه پیدا نشد.", alert=True)
        return
    state[uid] = {"step": "await_gconf_admins", "gid": gid}
    await _respond(event, card("👤 ویرایش ادمین‌ها", [
        f"💬 گروه : {gid}",
        "آیدیِ عددیِ ادمین‌ها رو با ویرگول بفرست. مثال: `12345, 67890`",
    ]), buttons=[[Button.inline("🔙 انصراف", f"gconf_g_{gid}".encode())]])


@bot.on(events.CallbackQuery(pattern=b"gconf_content_(-?\\d+)"))
async def gconf_content_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    gid = int(event.pattern_match.group(1).decode())
    cfg = db.get_group_config(gid)
    if not cfg or cfg.get("customer_id") != uid:
        await event.answer("گروه پیدا نشد.", alert=True)
        return
    state[uid] = {"step": "await_gconf_content", "gid": gid}
    await _respond(event, card("📦 ست محتوای ارسالی", [
        f"💬 گروه : {gid}",
        "محتوای ارسالی رو بفرست: متن، یا عکس/فایل با کپشن دلخواه.",
        "لینک هم می‌تونه داخل متن باشه.",
    ]), buttons=[[Button.inline("🔙 انصراف", f"gconf_g_{gid}".encode())]])


async def _gconf_handle_content(event, st):
    uid = event.sender_id
    gid = st.get("gid")
    if not gid or not db.get_group_config(gid):
        state.pop(uid, None)
        await event.respond("نشست منقضی شد. دوباره از «تنظیمات گروه» شروع کن.",
                            buttons=main_menu())
        return
    msg = event.message
    cap = (msg.text or "").strip()
    try:
        if msg.photo:
            path = await msg.download_media(file=GROUP_MEDIA_DIR)
            db.set_group_content(gid, "photo", msg.text or None, path)
            label = "🖼 عکس"
        elif msg.document:
            path = await msg.download_media(file=GROUP_MEDIA_DIR)
            db.set_group_content(gid, "file", msg.text or None, path)
            label = "📎 فایل"
        elif msg.text:
            db.set_group_content(gid, "text", msg.text, None)
            label = "📝 متن"
        else:
            await event.respond("❌ این نوع محتوا پشتیبانی نمی‌شه. متن/عکس/فایل بفرست.")
            return
    except Exception as e:  # noqa: BLE001
        await logbus.log_detail("❌ GROUP CONTENT SAVE ERROR", e, [f"🆔 {event.sender_id}"])
        await event.respond("❌ ذخیرهٔ محتوا ناموفق بود. دوباره امتحان کن.")
        return
    state.pop(uid, None)
    await event.respond(card("✅ محتوا ذخیره شد", [
        f"💬 گروه : {gid}", f"📦 نوع : {label}"]),
        buttons=[[Button.inline("🔙 گروه", f"gconf_g_{gid}".encode())]])
    await logbus.event("📦 GROUP CONTENT SET", [
        f"🆔 {uid}", f"💬 گروه : {gid}", f"📦 {label}", f"🕒 {now()}"], pv_user=uid)


@bot.on(events.CallbackQuery(pattern=b"gconf_toggle_(-?\\d+)"))
async def gconf_toggle_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    gid = int(event.pattern_match.group(1).decode())
    cfg = db.get_group_config(gid)
    if not cfg or cfg.get("customer_id") != uid:
        await event.answer("گروه پیدا نشد.", alert=True)
        return
    db.set_group_enabled(gid, not cfg.get("enabled"))
    await gconf_group_cb(event)


@bot.on(events.CallbackQuery(pattern=b"gconf_del_(-?\\d+)"))
async def gconf_del_cb(event):
    if not await _gate(event):
        return
    uid = event.sender_id
    gid = int(event.pattern_match.group(1).decode())
    cfg = db.get_group_config(gid)
    if not cfg or cfg.get("customer_id") != uid:
        await event.answer("گروه پیدا نشد.", alert=True)
        return
    db.delete_group_config(gid)
    await _respond(event, "گروه حذف شد. ✅",
                   buttons=[[Button.inline("🔙 تنظیمات گروه", b"gconf")]])
    await logbus.event("🗑 GROUP CONFIG DELETE", [
        f"🆔 {uid}", f"💬 گروه : {gid}", f"🕒 {now()}"], pv_user=uid)


# --------------------------------------------------------------------------- #
@bot.on(events.NewMessage(func=lambda e: e.is_private and not (e.raw_text or "").startswith("/")))
async def text_router(event):
    uid = event.sender_id
    # Blocked users are fully ignored: no reply, no log, no processing (anti-spam).
    if db.is_blocked(uid):
        return
    st = state.get(uid)
    if not st:
        return
    # maintenance + anti-flood still apply to free-text steps
    if db.maintenance_on():
        await event.respond("🛠 ربات در حال تعمیر است.")
        return
    user = await event.get_sender()
    if not await ratelimit.guard(uid, getattr(user, "first_name", "") or ""):
        await event.respond("⛔ به‌خاطر فعالیت بیش از حد، حساب شما مسدود شد.")
        return

    step = st.get("step")
    if step == "await_txhash":
        await handle_txhash(event, st)
    elif step == "await_deposit_txhash":
        await handle_deposit_txhash(event, st)
    elif step == "await_phone":
        await handle_phone(event, st)
    elif step == "await_password":
        await handle_password(event, st)
    elif step == "await_code":
        await handle_code(event, st)
    elif step == "await_marker":
        await handle_marker(event, st)
    elif step == "await_channel_name":
        await handle_channel_name(event, st)
    elif step == "await_gconf_group":
        await _gconf_handle_group(event, st)
    elif step == "await_gconf_admins":
        await _gconf_handle_admins(event, st)
    elif step == "await_gconf_content":
        await _gconf_handle_content(event, st)


# --------------------------------------------------------------------------- #
# Background loop: expiry warning (2 days) + auto-expire notice.
# --------------------------------------------------------------------------- #
async def expiry_loop():
    while True:
        try:
            for cu in db.list_customers():
                uid = cu["telegram_id"]
                if cu.get("blocked"):
                    continue
                d = db.days_left(uid)
                if 0 < d <= config.EXPIRY_WARN_DAYS and not cu.get("warned"):
                    db.set_warned(uid, True)
                    await logbus.to_pv(uid,
                        f"⏰ هشدار: فقط {d} روز از اشتراکت مونده. "
                        "برای تمدید «🛒 خرید اشتراک» رو بزن.")
                    await logbus.to_group(card("⏰ EXPIRY WARNING", [
                        f"🆔 {uid}", f"{d} روز مونده", f"🕒 {now()}"]))
        except Exception as e:  # noqa: BLE001
            print(f"[expiry loop] {e}")
        await asyncio.sleep(3600)


# --------------------------------------------------------------------------- #
# Background loop: deliver owner->customer notifications.
# The owner bot can't DM customers (separate token), so it enqueues messages in
# db.notifications and THIS (customer) bot — which the customer has started —
# delivers them. Best-effort, at-most-once.
# --------------------------------------------------------------------------- #
async def notification_loop():
    while True:
        try:
            for n in db.fetch_unsent_notifications(50):
                try:
                    await bot.send_message(int(n["customer_id"]), n["text"])
                except Exception:
                    pass
                db.mark_notification_sent(n["id"])
        except Exception as e:  # noqa: BLE001
            print(f"[notification loop] {e}")
        await asyncio.sleep(10)


# --------------------------------------------------------------------------- #
# Entrypoint.
# --------------------------------------------------------------------------- #
async def amain():
    problems = config.validate_customer()
    if problems:
        raise SystemExit("تنظیمات ناقصه (.env): " + ", ".join(problems))
    db.init()
    account_conn.set_invalid_auth_handler(_on_invalid_auth)
    account_conn.start_janitor()
    worker.ensure_master_worker()
    await bot.start(bot_token=config.CUSTOMER_BOT_TOKEN)
    logbus.bind(bot)
    tg_panel.setup(bot, state)   # register the decoupled Telegram section
    bale_panel.setup(bot, state, tg_panel._state)   # decoupled Bale section
    group_panel.setup(bot, run_send=run_send, active_jobs=active_jobs,
                      stop_flags=stop_flags, pending_send=pending_send,
                      customer_active_account=customer_active_account,
                      remote_upload_prepare=_remote_upload_prepare)
    await logbus.to_group(card("🤖 CUSTOMER BOT ONLINE", [
        f"🏷 Version : {config.VERSION}", f"🕒 {now()}"]))
    asyncio.create_task(expiry_loop())
    asyncio.create_task(notification_loop())
    print("customer bot running")
    await bot.run_until_disconnected()


async def _on_invalid_auth(phone: str):
    try:
        for a in db.list_accounts():
            if rb.normalize_phone(a["phone"]) == rb.normalize_phone(phone):
                db.set_status(a["id"], "inactive")
                break
    except Exception:
        pass
