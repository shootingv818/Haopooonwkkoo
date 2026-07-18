"""
owner_bot.py — the CENTRAL PANEL bot (owner only).
==================================================

Only the configured OWNER may use it. It manages the whole service:

  * dashboard stats (customers, accounts, sends, revenue, overall health)
  * per-customer profile + customer search + customer ranking
  * add a customer manually + add/subtract subscription time + instant block /
    unblock
  * broadcast a message to every customer
  * plan price management + TRX price settings
  * transaction list
  * worker management + worker logs (reused worker subsystem)
  * maintenance mode toggle + on-demand system backup

The owner panel is the admin: it reads/writes BOTH the operational customer DB
(db.py) and the owner-only central DB (central_db.py). A bot can only DM users
who started IT, and customers start the CUSTOMER bot — not this one. So owner
actions that must reach a customer (time change / block / unblock / broadcast)
are written to the db.notifications outbox and delivered by the customer bot's
notification loop. Privileged actions are logged to the central group and the
audit log.
"""
import asyncio
import os
import time
import zipfile

from telethon import TelegramClient, events, Button

import backup
import central_db
import config
import crypto_util
import db
import logbus
import tron
import worker

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

bot = TelegramClient(os.path.join(DATA_DIR, "owner_bot"),
                     config.API_ID, config.API_HASH)

LINE = logbus.LINE
state: dict = {}


def now() -> str:
    return config.now_str()


def card(title, rows):
    return logbus.card(title, rows)


def is_owner(event) -> bool:
    return config.OWNER_ID and event.sender_id == config.OWNER_ID


async def safe_edit(event, text, buttons=None):
    try:
        await event.edit(text, buttons=buttons)
    except Exception:
        try:
            await bot.send_message(event.sender_id, text, buttons=buttons)
        except Exception:
            pass


def main_menu():
    return [
        [Button.inline("📊 داشبورد", b"dash"),
         Button.inline("👥 مشتری‌ها", b"customers")],
        [Button.inline("🔎 جستجوی مشتری", b"search"),
         Button.inline("🏆 رتبه‌بندی", b"ranking")],
        [Button.inline("➕ افزودن مشتری", b"addcust"),
         Button.inline("📣 پیام همگانی", b"broadcast")],
        [Button.inline("💲 تنظیمات قیمت", b"prices"),
         Button.inline("📋 تراکنش‌ها", b"txlist")],
        [Button.inline("🛠 ورکرها", b"workers"),
         Button.inline("🧰 تعمیر/بکاپ", b"sys")],
        [Button.inline("📨 آمار تلگرام", b"tg_owner")],
        [Button.inline("📢 عضویت اجباری", b"forcedjoin")],
    ]


WELCOME = "🎛 پنل مرکزی روبیکا تولز\nیکی از گزینه‌ها رو انتخاب کن:"


@bot.on(events.NewMessage(pattern="/start"))
async def start_handler(event):
    if not is_owner(event):
        await event.respond("⛔ این پنل فقط برای مالکه.")
        return
    state.pop(event.sender_id, None)
    await event.respond(WELCOME, buttons=main_menu())


@bot.on(events.CallbackQuery(data=b"home"))
async def home_cb(event):
    if not is_owner(event):
        return
    state.pop(event.sender_id, None)
    await safe_edit(event, WELCOME, buttons=main_menu())


@bot.on(events.CallbackQuery(data=b"cancel"))
async def cancel_cb(event):
    if not is_owner(event):
        return
    state.pop(event.sender_id, None)
    await safe_edit(event, "لغو شد.", buttons=main_menu())


# --------------------------------------------------------------------------- #
# Dashboard.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"dash"))
async def dash_cb(event):
    if not is_owner(event):
        return
    s = db.stats()
    workers = db.list_workers()
    healthy = sum(1 for w in workers if w.get("status") == "ok")
    health_pct = (s["active_accounts"] / s["accounts"] * 100) if s["accounts"] else 0

    # Financial stats (TRX)
    today_trx = db.today_revenue_trx()
    week_trx = db.week_revenue_trx()
    month_trx = db.month_revenue_trx()

    # TRX price
    try:
        trx_price = await tron.get_trx_price_usd()
    except Exception:
        trx_price = 0.0

    await safe_edit(event, card("📊 داشبورد", [
        f"👥 مشتری‌ها : {s['customers']}  (فعال: {s['active_subs']} | مسدود: {s['blocked']})",
        f"📱 اکانت‌ها : {s['accounts']}  (فعال: {s['active_accounts']})",
        f"🚀 کل ارسال : {s['sends']}",
        f"💰 درآمد کل : {s['revenue']:g} TRX",
        f"📈 امروز : {today_trx:g} TRX | هفته : {week_trx:g} TRX | ماه : {month_trx:g} TRX",
        f"💲 قیمت TRX : {trx_price:g} USD" if trx_price > 0 else "💲 قیمت TRX : نامشخص",
        f"🩺 سلامت کلی اکانت‌ها : {health_pct:.0f}%",
        f"🛠 ورکرها : {len(workers)} (سالم: {healthy})",
        f"🕒 {now()}",
    ]), buttons=[[Button.inline("🔄 تازه‌سازی", b"dash")],
                 [Button.inline("🔙 بازگشت", b"home")]])


# --------------------------------------------------------------------------- #
# Customers list / profile.
# --------------------------------------------------------------------------- #
def _cust_buttons(customers):
    rows = []
    for c in customers[:30]:
        d = db.days_left(c["telegram_id"])
        tag = "⛔" if c.get("blocked") else ("🟢" if d > 0 else "🔴")
        label = f"{tag} {c.get('name') or c['telegram_id']} ({d}d)"
        rows.append([Button.inline(label, f"cust_{c['telegram_id']}".encode())])
    rows.append([Button.inline("🔙 بازگشت", b"home")])
    return rows


@bot.on(events.CallbackQuery(data=b"customers"))
async def customers_cb(event):
    if not is_owner(event):
        return
    customers = db.list_customers()
    if not customers:
        await safe_edit(event, "هنوز مشتری‌ای نداری.",
                        buttons=[[Button.inline("🔙 بازگشت", b"home")]])
        return
    await safe_edit(event, f"👥 مشتری‌ها ({len(customers)}):",
                    buttons=_cust_buttons(customers))


def _profile_text(c) -> str:
    uid = c["telegram_id"]
    d = db.days_left(uid)
    status = "⛔ مسدود" if c.get("blocked") else ("🟢 فعال" if d > 0 else "🔴 منقضی")
    return card("👤 پروفایل مشتری", [
        f"📛 نام : {c.get('name') or '-'}",
        f"🔗 یوزرنیم : @{c['username']}" if c.get("username") else "🔗 یوزرنیم : -",
        f"🆔 آیدی : {uid}",
        f"⭐️ وضعیت : {status}",
        f"⏳ روز باقی‌مانده : {d}",
        f"📅 انقضا : {c.get('expires_at') or '-'}",
        f"📱 اکانت‌ها : {db.count_customer_accounts(uid)}",
        f"🚀 کل ارسال : {c.get('total_sends') or 0}",
        f"💵 مجموع پرداخت : {float(c.get('total_paid') or 0):g} TRX",
        f"📝 یادداشت : {c.get('note') or '-'}",
    ])


def _profile_buttons(uid, blocked):
    rows = [
        [Button.inline("➕۳ روز", f"addt_{uid}_3".encode()),
         Button.inline("➕۷ روز", f"addt_{uid}_7".encode()),
         Button.inline("➕۳۰ روز", f"addt_{uid}_30".encode())],
        [Button.inline("➖۱ روز", f"addt_{uid}_-1".encode()),
         Button.inline("➖۷ روز", f"addt_{uid}_-7".encode()),
         Button.inline("⌨️ زمان دلخواه", f"custt_{uid}".encode())],
    ]
    if blocked:
        rows.append([Button.inline("✅ رفع مسدودی", f"unblock_{uid}".encode())])
    else:
        rows.append([Button.inline("⛔ مسدودسازی فوری", f"block_{uid}".encode())])
    rows.append([Button.inline("📝 یادداشت", f"note_{uid}".encode())])
    rows.append([Button.inline("🔙 بازگشت", b"customers")])
    return rows


@bot.on(events.CallbackQuery(pattern=b"cust_(\\d+)"))
async def cust_profile_cb(event):
    if not is_owner(event):
        return
    uid = int(event.pattern_match.group(1))
    c = db.get_customer(uid)
    if not c:
        await event.answer("مشتری پیدا نشد.", alert=True)
        return
    await safe_edit(event, _profile_text(c),
                    buttons=_profile_buttons(uid, c.get("blocked")))


async def _refresh_profile(event, uid):
    c = db.get_customer(uid)
    if c:
        await safe_edit(event, _profile_text(c),
                        buttons=_profile_buttons(uid, c.get("blocked")))


@bot.on(events.CallbackQuery(pattern=b"addt_(\\d+)_(-?\\d+)"))
async def addtime_cb(event):
    if not is_owner(event):
        return
    uid = int(event.pattern_match.group(1))
    days = int(event.pattern_match.group(2))
    db.ensure_customer(uid)
    new_exp = db.add_days(uid, days)
    central_db.audit("adjust_time", f"{uid}: {days:+d} -> {new_exp}")
    await logbus.to_group(card("🕒 تغییر زمان اشتراک", [
        f"🆔 {uid}", f"{days:+d} روز", f"📅 انقضا : {new_exp}", f"🕒 {now()}"]))
    try:
        db.enqueue_notification(uid,
            (f"🎁 {days} روز به اشتراکت اضافه شد." if days >= 0
             else f"⏬ {abs(days)} روز از اشتراکت کم شد.") +
            f"\n📅 انقضا: {new_exp}")
    except Exception:
        pass
    await _refresh_profile(event, uid)


@bot.on(events.CallbackQuery(pattern=b"custt_(\\d+)"))
async def custtime_cb(event):
    if not is_owner(event):
        return
    uid = int(event.pattern_match.group(1))
    state[event.sender_id] = {"step": "await_custtime", "uid": uid}
    await safe_edit(event,
                    "⌨️ چند روز اضافه/کم بشه؟ یک عدد بفرست (مثلاً 14 یا -5):",
                    buttons=[[Button.inline("🔙 لغو", f"cust_{uid}".encode())]])


@bot.on(events.CallbackQuery(pattern=b"block_(\\d+)"))
async def block_cb(event):
    if not is_owner(event):
        return
    uid = int(event.pattern_match.group(1))
    db.set_blocked(uid, True)
    central_db.audit("block", str(uid))
    await logbus.to_group(card("⛔ مسدودسازی فوری", [f"🆔 {uid}", f"🕒 {now()}"]))
    try:
        db.enqueue_notification(uid, "⛔ حساب شما توسط مدیریت مسدود شد.")
    except Exception:
        pass
    await _refresh_profile(event, uid)


@bot.on(events.CallbackQuery(pattern=b"unblock_(\\d+)"))
async def unblock_cb(event):
    if not is_owner(event):
        return
    uid = int(event.pattern_match.group(1))
    db.set_blocked(uid, False)
    db.rate_reset(uid)
    central_db.audit("unblock", str(uid))
    await logbus.to_group(card("✅ رفع مسدودی", [f"🆔 {uid}", f"🕒 {now()}"]))
    try:
        db.enqueue_notification(uid, "✅ مسدودی حساب شما برداشته شد.")
    except Exception:
        pass
    await _refresh_profile(event, uid)


@bot.on(events.CallbackQuery(pattern=b"note_(\\d+)"))
async def note_cb(event):
    if not is_owner(event):
        return
    uid = int(event.pattern_match.group(1))
    state[event.sender_id] = {"step": "await_note", "uid": uid}
    await safe_edit(event, "📝 یادداشت این مشتری رو بفرست:",
                    buttons=[[Button.inline("🔙 لغو", f"cust_{uid}".encode())]])


# --------------------------------------------------------------------------- #
# Add customer manually.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"addcust"))
async def addcust_cb(event):
    if not is_owner(event):
        return
    state[event.sender_id] = {"step": "await_addcust"}
    await safe_edit(event,
                    "➕ آیدی عددیِ مشتری رو بفرست (اختیاری: بعدش با فاصله، تعداد روز).\n"
                    "مثال: `123456789 30`",
                    buttons=[[Button.inline("🔙 لغو", b"home")]])


# --------------------------------------------------------------------------- #
# Search + ranking.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"search"))
async def search_cb(event):
    if not is_owner(event):
        return
    state[event.sender_id] = {"step": "await_search"}
    await safe_edit(event, "🔎 بخشی از آیدی/نام/یوزرنیم مشتری رو بفرست:",
                    buttons=[[Button.inline("🔙 لغو", b"home")]])


@bot.on(events.CallbackQuery(data=b"ranking"))
async def ranking_cb(event):
    if not is_owner(event):
        return
    customers = db.list_customers()
    ranked = sorted(customers, key=lambda c: (float(c.get("total_paid") or 0),
                                              int(c.get("total_sends") or 0)),
                    reverse=True)[:15]
    rows = []
    for i, c in enumerate(ranked, 1):
        rows.append(f"{i}. {c.get('name') or c['telegram_id']} — "
                    f"{float(c.get('total_paid') or 0):g}$ / {c.get('total_sends') or 0} ارسال")
    await safe_edit(event, card("🏆 رتبه‌بندی مشتری‌ها", rows or ["— خالی —"]),
                    buttons=[[Button.inline("🔙 بازگشت", b"home")]])


# --------------------------------------------------------------------------- #
# Broadcast.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"broadcast"))
async def broadcast_cb(event):
    if not is_owner(event):
        return
    state[event.sender_id] = {"step": "await_broadcast"}
    await safe_edit(event, "📣 متنِ پیام همگانی رو بفرست (به همه‌ی مشتری‌ها ارسال می‌شه):",
                    buttons=[[Button.inline("🔙 لغو", b"home")]])


@bot.on(events.CallbackQuery(data=b"broadcast_confirm"))
async def broadcast_confirm_cb(event):
    if not is_owner(event):
        return
    st = state.get(event.sender_id)
    if not st or st.get("step") != "await_broadcast_confirm":
        await safe_edit(event, "خطا: متنی ذخیره نشده.", buttons=main_menu())
        return
    text = st.get("broadcast_text", "")
    state.pop(event.sender_id, None)
    await safe_edit(event, "📣 در حال ارسال ...")
    await do_broadcast(event, text)


@bot.on(events.CallbackQuery(data=b"broadcast_cancel"))
async def broadcast_cancel_cb(event):
    if not is_owner(event):
        return
    state.pop(event.sender_id, None)
    await safe_edit(event, "لغو شد.", buttons=main_menu())


async def do_broadcast(event, text):
    customers = db.list_customers()
    ok = 0
    fail = 0
    for c in customers:
        try:
            db.enqueue_notification(c["telegram_id"], f"📣 {text}")
            ok += 1
        except Exception:
            fail += 1
    central_db.record_broadcast(text, ok, fail)
    central_db.audit("broadcast", f"ok={ok} fail={fail}")
    await logbus.to_group(card("📣 BROADCAST", [
        f"✅ {ok}   ❌ {fail}   👥 {len(customers)}", f"🕒 {now()}"]))
    await event.respond(f"📣 در صفِ ارسال قرار گرفت. ✅ {ok} / ❌ {fail}",
                        buttons=main_menu())


# --------------------------------------------------------------------------- #
# Price Management.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"prices"))
async def price_settings_cb(event):
    if not is_owner(event):
        return
    rows = []
    for key, plan in config.PLANS.items():
        override = db.get_plan_price(key)
        current = override if override is not None else plan["price"]
        label = f"{plan['title']} : {current:g} USD"
        if override is not None:
            label += " (تغییر داده‌شده)"
        rows.append(label)

    btns = []
    for key, plan in config.PLANS.items():
        btns.append([Button.inline(f"✏️ تغییر {plan['title']}", f"setprice_{key}".encode())])
    btns.append([Button.inline("⚙️ تنظیمات TRX", b"trx_settings")])
    btns.append([Button.inline("🔙 بازگشت", b"home")])

    await safe_edit(event, card("💲 تنظیمات قیمت", rows), buttons=btns)


@bot.on(events.CallbackQuery(pattern=b"setprice_(.+)"))
async def setprice_cb(event):
    if not is_owner(event):
        return
    plan_key = event.pattern_match.group(1).decode()
    if plan_key not in config.PLANS:
        await event.answer("پلن نامعتبر.", alert=True)
        return
    plan = config.PLANS[plan_key]
    state[event.sender_id] = {"step": "await_price_input", "plan_key": plan_key}
    override = db.get_plan_price(plan_key)
    current = override if override is not None else plan["price"]
    await safe_edit(event,
                    f"✏️ قیمت فعلی {plan['title']}: {current:g} USD\n"
                    f"قیمت جدید رو بفرست (عدد به دلار):",
                    buttons=[[Button.inline("🔙 لغو", b"prices")]])


@bot.on(events.CallbackQuery(data=b"trx_settings"))
async def trx_settings_cb(event):
    if not is_owner(event):
        return
    try:
        trx_price = await tron.get_trx_price_usd()
    except Exception:
        trx_price = 0.0

    override_val = db.get_setting("trx_price_override", "0")
    tolerance_val = db.get_setting("payment_tolerance_percent",
                                   str(config.PAYMENT_TOLERANCE_PERCENT))

    rows = [
        f"💲 قیمت لحظه‌ای TRX : {trx_price:g} USD" if trx_price > 0 else "💲 قیمت TRX : نامشخص",
        f"🔧 اورراید قیمت : {override_val} USD" + (" (غیرفعال)" if float(override_val or 0) <= 0 else " (فعال)"),
        f"📏 تلرانس پرداخت : {tolerance_val}%",
    ]
    btns = [
        [Button.inline("✏️ تنظیم اورراید قیمت", b"set_trx_override")],
        [Button.inline("✏️ تنظیم تلرانس", b"set_tolerance")],
        [Button.inline("🔙 بازگشت", b"prices")],
    ]
    await safe_edit(event, card("⚙️ تنظیمات TRX", rows), buttons=btns)


@bot.on(events.CallbackQuery(data=b"set_trx_override"))
async def set_trx_override_cb(event):
    if not is_owner(event):
        return
    current = db.get_setting("trx_price_override", "0")
    state[event.sender_id] = {"step": "await_trx_override"}
    await safe_edit(event,
                    f"🔧 مقدار فعلی اورراید: {current} USD\n"
                    f"قیمت جدید TRX رو بفرست (عدد). برای غیرفعال کردن 0 بفرست:",
                    buttons=[[Button.inline("🔙 لغو", b"trx_settings")]])


@bot.on(events.CallbackQuery(data=b"set_tolerance"))
async def set_tolerance_cb(event):
    if not is_owner(event):
        return
    current = db.get_setting("payment_tolerance_percent",
                             str(config.PAYMENT_TOLERANCE_PERCENT))
    state[event.sender_id] = {"step": "await_tolerance"}
    await safe_edit(event,
                    f"📏 تلرانس فعلی: {current}%\n"
                    f"درصد تلرانس جدید رو بفرست (0 تا 50):",
                    buttons=[[Button.inline("🔙 لغو", b"trx_settings")]])


# --------------------------------------------------------------------------- #
# Transaction List.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"txlist"))
async def transactions_cb(event):
    if not is_owner(event):
        return
    await _show_transactions(event, offset=0)


@bot.on(events.CallbackQuery(pattern=b"txlist_(\\d+)"))
async def transactions_page_cb(event):
    if not is_owner(event):
        return
    offset = int(event.pattern_match.group(1))
    await _show_transactions(event, offset=offset)


async def _show_transactions(event, offset: int = 0):
    all_payments = db.list_payments()
    page_size = 20
    page = all_payments[offset:offset + page_size]

    if not all_payments:
        await safe_edit(event, "📋 هنوز تراکنشی ثبت نشده.",
                        buttons=[[Button.inline("🔙 بازگشت", b"home")]])
        return

    rows = [f"📋 تراکنش‌ها ({offset + 1}-{offset + len(page)} از {len(all_payments)}):"]
    for p in page:
        cid = p.get("customer_id", "?")
        plan = p.get("plan", "?")
        amount = float(p.get("amount", 0))
        date = (p.get("created_at") or "")[:10]
        rows.append(f"  {cid} | {plan} | {amount:g} TRX | {date}")

    btns = []
    nav_row = []
    if offset > 0:
        nav_row.append(Button.inline("⬅️ قبلی", f"txlist_{max(0, offset - page_size)}".encode()))
    if offset + page_size < len(all_payments):
        nav_row.append(Button.inline("➡️ بعدی", f"txlist_{offset + page_size}".encode()))
    if nav_row:
        btns.append(nav_row)
    btns.append([Button.inline("🔙 بازگشت", b"home")])

    await safe_edit(event, "\n".join(rows), buttons=btns)


# --------------------------------------------------------------------------- #
# System: maintenance toggle + backup now.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"sys"))
async def sys_cb(event):
    if not is_owner(event):
        return
    m = central_db.get_maintenance()
    last = central_db.get_last_backup() or "—"
    await safe_edit(event, card("🧰 تعمیر و بکاپ", [
        f"🛠 حالت تعمیر : {'روشن 🔴' if m else 'خاموش 🟢'}",
        f"💾 آخرین بکاپ : {last}",
    ]), buttons=[
        [Button.inline("🛠 تغییر حالت تعمیر", b"maint_toggle")],
        [Button.inline("💾 بکاپ فوری", b"backup_now")],
        [Button.inline("🗄 بکاپ کامل (سشن+محتوا+تنظیمات)", b"fullbackup")],
        [Button.inline("🔙 بازگشت", b"home")]])


@bot.on(events.CallbackQuery(data=b"maint_toggle"))
async def maint_toggle_cb(event):
    if not is_owner(event):
        return
    new = not central_db.get_maintenance()
    central_db.set_maintenance(new)
    central_db.audit("maintenance", "on" if new else "off")
    await logbus.to_group(card("🛠 MAINTENANCE", [
        ("روشن شد 🔴" if new else "خاموش شد 🟢"), f"🕒 {now()}"]))
    await sys_cb(event)


@bot.on(events.CallbackQuery(data=b"backup_now"))
async def backup_now_cb(event):
    if not is_owner(event):
        return
    await event.answer("در حال ساخت بکاپ ...")
    ok = await backup.run_backup(notify_user=event.sender_id)
    if not ok:
        await bot.send_message(event.sender_id, "هنوز چیزی برای بکاپ نیست.")


# --------------------------------------------------------------------------- #
# Telegram section: owner stats (per-customer + content) + full backup.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"tg_owner"))
async def tg_owner_cb(event):
    if not is_owner(event):
        return
    accounts = db.list_tg_accounts()
    # group by customer
    by_cust = {}
    for a in accounts:
        by_cust.setdefault(a["customer_id"], []).append(a)
    total_sends = 0
    rows = [
        f"📨 کل اکانت‌های تلگرام : {len(accounts)}",
        f"👥 مشتری‌های دارای اکانت : {len(by_cust)}",
        LINE,
    ]
    cust_names = {c["telegram_id"]: (c.get("name") or "-")
                  for c in db.list_customers()}
    shown = 0
    for cid, accs in by_cust.items():
        s = db.get_tg_settings(cid)
        sends = int(s.get("total_sends") or 0)
        total_sends += sends
        if shown < 25:
            content = "✅" if s.get("content_type") else "❌"
            alive = sum(1 for a in accs if a.get("status") == "active")
            rows.append(
                f"🆔 {cid} ({cust_names.get(cid, '-')}) — "
                f"📱{len(accs)} (🟢{alive}) — 📤{sends} — 📦{content}")
            shown += 1
    rows.insert(2, f"📤 کل ارسال‌های تلگرام : {total_sends}")
    if len(by_cust) > 25:
        rows.append(f"… و {len(by_cust) - 25} مشتری دیگه")
    await safe_edit(event, card("📨 آمار تلگرام", rows),
                    buttons=[[Button.inline("🔄 تازه‌سازی", b"tg_owner")],
                             [Button.inline("🗄 بکاپ کامل", b"fullbackup")],
                             [Button.inline("🔙 بازگشت", b"home")]])


async def _build_full_backup() -> str:
    """Zip EVERYTHING the owner needs to restore: the customer DB (rubika+tg
    accounts, customers, content/settings), the central DB, the local media +
    session files, AND every remote worker's session files. Returns the path."""
    out = os.path.join(DATA_DIR, f"full_backup_{int(time.time())}.zip")
    db_paths = [getattr(db, "DB_PATH", None), getattr(central_db, "DB_PATH", None)]
    dirs = [os.path.join(DATA_DIR, "sessions"), os.path.join(DATA_DIR, "tg_media")]
    zf = zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED)
    try:
        for p in db_paths:
            if p and os.path.exists(p):
                zf.write(p, os.path.basename(p))
        for d in dirs:
            if os.path.isdir(d):
                for root, _dirs, files in os.walk(d):
                    for f in files:
                        fp = os.path.join(root, f)
                        zf.write(fp, os.path.relpath(fp, DATA_DIR))
        # pull every remote worker's Rubika session files into the archive too
        try:
            await worker.collect_sessions_into_zip(zf)
        except Exception as e:  # noqa: BLE001
            await logbus.to_group(f"⚠️ بکاپ سشن ورکرها ناقص ماند: {repr(e)[:150]}")
    finally:
        zf.close()
    return out


@bot.on(events.CallbackQuery(data=b"fullbackup"))
async def fullbackup_cb(event):
    if not is_owner(event):
        return
    await event.answer("در حال ساخت بکاپ کامل ...")
    path = None
    try:
        path = await _build_full_backup()
        await bot.send_file(event.sender_id, path,
                            caption=f"🗄 بکاپ کامل (سشن + محتوا + تنظیمات) • {now()}",
                            force_document=True)
        await logbus.to_group(card("🗄 FULL BACKUP", [f"🕒 {now()}"]))
    except Exception as e:  # noqa: BLE001
        await bot.send_message(event.sender_id, f"❌ خطا در بکاپ کامل: {repr(e)[:140]}")
    finally:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# Workers (reused worker subsystem).
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"workers"))
async def workers_cb(event):
    if not is_owner(event):
        return
    workers = db.list_workers()
    healthy = sum(1 for w in workers if w.get("status") == "ok")
    remotes = sum(1 for w in workers if not w.get("is_master"))
    total_accs = sum(db.count_accounts_on_worker(w["id"]) for w in workers)

    header = card("🛠 مدیریت ورکرها", [
        f"📦 کل ورکرها : {len(workers)}  (مستر: {len(workers) - remotes} | ریموت: {remotes})",
        f"🟢 سالم : {healthy} / {len(workers)}",
        f"📱 کل اکانت‌ها : {total_accs}",
        LINE,
        "برای جزئیات هر ورکر روی دکمه‌اش بزن:" if workers else "— هنوز ورکری اضافه نشده —",
    ])

    rows = []
    for w in workers:
        emoji = worker.status_emoji(w)
        ping = w.get("ping_ms")
        ping_txt = f"{ping}ms" if (ping is not None and ping >= 0) else "—"
        accs = db.count_accounts_on_worker(w["id"])
        master = "👑" if w.get("is_master") else "🖥"
        off = "" if w.get("enabled") else " ⏸"
        label = f"{emoji}{master} {w['tag']} · 📱{accs} · 📶{ping_txt}{off}"
        rows.append([Button.inline(label, f"w_{w['id']}".encode())])
    rows.append([Button.inline("➕ افزودن ورکر", b"w_add"),
                 Button.inline("🩺 بررسی همه", b"w_checkall")])
    rows.append([Button.inline("⬆️ آپدیت همه", b"w_updall"),
                 Button.inline("📦 نسخه‌ها", b"w_versions")])
    rows.append([Button.inline("🔙 بازگشت", b"home")])
    await safe_edit(event, header, buttons=rows)


@bot.on(events.CallbackQuery(data=b"w_versions"))
async def w_versions_cb(event):
    if not is_owner(event):
        return
    await event.answer("در حال خواندن نسخه‌ی هر ورکر ...")
    master_v = worker.master_code_version()
    workers = db.list_workers()
    rows = [f"👑 مستر : `{master_v}`  (برنچ: {config.GIT_BRANCH})", LINE]
    for w in workers:
        if w.get("is_master"):
            rows.append(f"👑 {w['tag']} : `{master_v}` ✅ (مستر)")
            continue
        v = await worker.worker_code_version(w)
        if v == master_v and v != "?":
            mark = "✅ آخرین"
        elif v == "?":
            mark = "❔ نامشخص (در دسترس نیست)"
        else:
            mark = "⚠️ قدیمی — آپدیت کن"
        rows.append(f"🖥 {w['tag']} : `{v}`  {mark}")
    await safe_edit(event, card("📦 نسخه‌ی کدِ ورکرها", rows),
                    buttons=[[Button.inline("⬆️ آپدیت همه", b"w_updall")],
                             [Button.inline("🔄 تازه‌سازی", b"w_versions"),
                              Button.inline("🔙 بازگشت", b"workers")]])


@bot.on(events.CallbackQuery(data=b"w_updall"))
async def w_updall_cb(event):
    if not is_owner(event):
        return
    workers = [w for w in db.list_workers() if not w.get("is_master")]
    if not workers:
        await event.answer("ورکرِ ریموتی برای آپدیت نیست.", alert=True)
        return
    await event.answer("شروعِ آپدیتِ همه‌ی ورکرها ... ممکنه چند دقیقه طول بکشه.")
    target = worker.master_code_version()
    pm = await bot.send_message(event.sender_id,
                                f"⬆️ آپدیتِ {len(workers)} ورکر به نسخه‌ی `{target}` ...")
    rows = []
    for w in workers:
        try:
            code, out, err = await worker.update_worker(w)
            ok = (code == 0)
        except Exception as e:  # noqa: BLE001
            ok, err = False, repr(e)[:160]
        v = await worker.worker_code_version(w) if ok else "?"
        if ok and v == target:
            rows.append(f"✅ {w['tag']} → `{v}`")
        elif ok:
            rows.append(f"⚠️ {w['tag']} → `{v}` (هنوز برابرِ مستر نیست)")
        else:
            rows.append(f"❌ {w['tag']} : {str(err)[-120:]}")
        try:
            await bot.edit_message(event.sender_id, pm.id,
                card("⬆️ آپدیتِ همه‌ی ورکرها", rows + [f"🕒 {now()}"]))
        except Exception:
            pass
    await logbus.to_group(card("⬆️ WORKER UPDATE ALL", rows + [f"🎯 target `{target}`",
                                                               f"🕒 {now()}"]))
    await bot.send_message(event.sender_id, "تمام شد.",
                           buttons=[[Button.inline("📦 نسخه‌ها", b"w_versions")],
                                    [Button.inline("🔙 ورکرها", b"workers")]])


@bot.on(events.CallbackQuery(data=b"w_checkall"))
async def w_checkall_cb(event):
    if not is_owner(event):
        return
    await event.answer("در حال بررسی ...")
    results = await worker.check_all()
    rows = []
    for r in results:
        emoji = "🟢" if r.get("status") == "ok" else ("🟡" if r.get("status") == "blocked" else "🔴")
        ping = r.get("ping_ms")
        ping_txt = f"{ping}ms" if (ping is not None and ping >= 0) else "—"
        line = f"{emoji} {r.get('tag')} · {r.get('status')} · 📶{ping_txt}"
        if r.get("detail"):
            line += f"\n   ↳ {r.get('detail')}"
        rows.append(line)
    await logbus.to_group(card("🛠 STATU WORKER ALL", rows + [f"🕒 {now()}"]))
    await safe_edit(event, card("🛠 وضعیت ورکرها", rows or ["— ورکری نیست —"]),
                    buttons=[[Button.inline("🔙 بازگشت", b"workers")]])


@bot.on(events.CallbackQuery(pattern=b"w_(\\d+)"))
async def w_menu_cb(event):
    if not is_owner(event):
        return
    wid = int(event.pattern_match.group(1))
    w = db.get_worker(wid)
    if not w:
        await event.answer("ورکر پیدا نشد.", alert=True)
        return
    accs = db.count_accounts_on_worker(wid)
    sent_today = db.worker_sent_today(wid)
    emoji = worker.status_emoji(w)
    ping = w.get("ping_ms")
    ping_txt = f"{ping}ms" if (ping is not None and ping >= 0) else "—"
    kind = "👑 مستر (محلی)" if w.get("is_master") else "🖥 ریموت"
    rows = [
        f"🏷 تگ : {w['tag']}",
        f"🔧 نوع : {kind}",
        f"🌐 IP : {w.get('ip')}",
        f"{emoji} وضعیت : {w.get('status')}  ·  📶 {ping_txt}",
        f"📁 فایل : {worker.file_label(w)}",
        f"📱 اکانت‌ها : {accs}",
        f"📤 ارسال امروز : {sent_today}",
        f"🔌 فعال : {'بله ✅' if w.get('enabled') else 'خیر ⏸'}",
        f"🕒 آخرین بررسی : {w.get('last_checked') or '—'}",
    ]
    detail = worker.health_detail(wid)
    if detail:
        rows.append(f"ℹ️ {detail}")
    btns = [[Button.inline("🩺 بررسی", f"wchk_{wid}".encode())]]
    if not w.get("is_master"):
        btns.append([Button.inline("🔄 ری‌استارت", f"wrst_{wid}".encode()),
                     Button.inline("⬆️ آپدیت", f"wupd_{wid}".encode())])
        toggle = "⏸ غیرفعال‌کردن" if w.get("enabled") else "▶️ فعال‌کردن"
        btns.append([Button.inline(toggle, f"wtog_{wid}".encode()),
                     Button.inline("🗑 حذف", f"wdel_{wid}".encode())])
    btns.append([Button.inline("🔙 بازگشت", b"workers")])
    await safe_edit(event, card("🛠 جزئیات ورکر", rows), buttons=btns)


@bot.on(events.CallbackQuery(pattern=b"wchk_(\\d+)"))
async def wchk_cb(event):
    if not is_owner(event):
        return
    wid = int(event.pattern_match.group(1))
    w = db.get_worker(wid)
    if not w:
        await event.answer("ورکر پیدا نشد.", alert=True)
        return
    await event.answer("در حال بررسی ...")
    r = await worker.check_worker(w)
    await logbus.to_group(card("🛠 WORKER CHECK", [
        f"{r.get('tag')} : {r.get('status')} ({r.get('ping_ms')}ms)"
        + (f" — {r.get('detail')}" if r.get("detail") else ""), f"🕒 {now()}"]))
    await w_menu_cb(event)


@bot.on(events.CallbackQuery(pattern=b"wtog_(\\d+)"))
async def wtog_cb(event):
    if not is_owner(event):
        return
    wid = int(event.pattern_match.group(1))
    w = db.get_worker(wid)
    if not w:
        await event.answer("ورکر پیدا نشد.", alert=True)
        return
    db.set_worker_enabled(wid, not w.get("enabled"))
    await w_menu_cb(event)


@bot.on(events.CallbackQuery(pattern=b"wrst_(\\d+)"))
async def wrst_cb(event):
    """Restart a remote worker's Docker container over SSH."""
    if not is_owner(event):
        return
    wid = int(event.pattern_match.group(1))
    w = db.get_worker(wid)
    if not w:
        await event.answer("ورکر پیدا نشد.", alert=True)
        return
    if w.get("is_master"):
        await event.answer("ورکر محلی (مستر) از پنل ری‌استارت نمی‌شه.", alert=True)
        return
    await event.answer("در حال ری‌استارت کانتینر ورکر ...")
    try:
        code, _out, err = await worker.restart_worker(w)
        ok = (code == 0)
    except Exception as e:  # noqa: BLE001
        ok, err = False, repr(e)[:160]
    await logbus.to_group(card("🔄 WORKER RESTART", [
        f"🏷 {w['tag']}", ("✅ موفق" if ok else f"❌ {str(err)[:160]}"), f"🕒 {now()}"]))
    await bot.send_message(event.sender_id,
        "✅ ورکر ری‌استارت شد." if ok else f"❌ ری‌استارت ناموفق: {str(err)[:160]}")
    await w_menu_cb(event)


@bot.on(events.CallbackQuery(pattern=b"wupd_(\\d+)"))
async def wupd_cb(event):
    """Update a remote worker: git pull + rebuild image + recreate container."""
    if not is_owner(event):
        return
    wid = int(event.pattern_match.group(1))
    w = db.get_worker(wid)
    if not w:
        await event.answer("ورکر پیدا نشد.", alert=True)
        return
    if w.get("is_master"):
        await event.answer("ورکر محلی (مستر) رو باید از خودِ سرورت آپدیت کنی.",
                           alert=True)
        return
    await event.answer("در حال آپدیت ورکر ... ممکنه چند دقیقه طول بکشه.")
    msg = await bot.send_message(
        event.sender_id,
        f"⬆️ در حال آپدیت ورکر {w['tag']} (git pull + ساخت ایمیج + اجرای مجدد) ...")
    try:
        code, out, err = await worker.update_worker(w)
        ok = (code == 0)
    except Exception as e:  # noqa: BLE001
        ok, err, out = False, repr(e)[:200], ""
    await logbus.to_group(card("⬆️ WORKER UPDATE", [
        f"🏷 {w['tag']}",
        ("✅ موفق" if ok else f"❌ {(str(err) or str(out))[-200:]}"),
        f"🕒 {now()}"]))
    try:
        await msg.edit("✅ ورکر آپدیت و دوباره اجرا شد."
                       if ok else f"❌ آپدیت ناموفق: {(str(err) or str(out))[-300:]}")
    except Exception:
        pass
    try:
        await worker.check_worker(w)
    except Exception:
        pass


@bot.on(events.CallbackQuery(pattern=b"wdel_(\\d+)"))
async def wdel_cb(event):
    if not is_owner(event):
        return
    wid = int(event.pattern_match.group(1))
    w = db.get_worker(wid)
    if not w:
        await event.answer("ورکر پیدا نشد.", alert=True)
        return
    try:
        await worker.teardown_worker(w)
    except Exception:
        pass
    db.delete_worker(wid)
    await logbus.to_group(card("🗑 WORKER REMOVED", [f"🏷 {w['tag']}", f"🕒 {now()}"]))
    await workers_cb(event)


@bot.on(events.CallbackQuery(data=b"w_add"))
async def w_add_cb(event):
    if not is_owner(event):
        return
    if not crypto_util.is_configured():
        await event.answer("اول WORKER_SECRET رو توی .env تنظیم کن.", alert=True)
        return
    state[event.sender_id] = {"step": "w_ip", "w": {}}
    await safe_edit(event, "🌐 IP سرور ورکر رو بفرست:",
                    buttons=[[Button.inline("🔙 لغو", b"workers")]])


async def _worker_add_flow(event, st):
    step = st["step"]
    txt = event.raw_text.strip()
    w = st["w"]
    if step == "w_ip":
        w["ip"] = txt
        st["step"] = "w_port"
        await event.respond("🔌 پورت SSH (پیش‌فرض 22):")
    elif step == "w_port":
        w["port"] = txt or "22"
        st["step"] = "w_user"
        await event.respond("👤 یوزرنیم SSH:")
    elif step == "w_user":
        w["user"] = txt
        st["step"] = "w_pass"
        await event.respond("🔑 پسورد SSH:")
    elif step == "w_pass":
        w["pass"] = txt
        state.pop(event.sender_id, None)
        msg = await event.respond("⏳ در حال راه‌اندازی ورکر (نصب Docker و سورس) ...")

        async def progress(s):
            try:
                await msg.edit(s)
            except Exception:
                pass
        prov = await worker.provision_worker(w["ip"], w["port"], w["user"],
                                             w["pass"], on_progress=progress)
        if not prov.get("ok"):
            await msg.edit(f"❌ راه‌اندازی ناموفق: {prov.get('error')}",
                           buttons=[[Button.inline("🔙 ورکرها", b"workers")]])
            return
        wid = await worker.register_provisioned(w["ip"], w["port"], w["user"],
                                                w["pass"], prov)
        await logbus.to_group(card("🛠 ADDED WORKER", [
            f"🏷 {prov['tag']}", f"🌐 {w['ip']}", f"🕒 {now()}"]))
        new_w = db.get_worker(wid)
        try:
            await worker.check_worker(new_w)
        except Exception:
            pass
        await msg.edit(f"✅ ورکر {prov['tag']} اضافه شد.",
                       buttons=[[Button.inline("🛠 ورکرها", b"workers")]])


# --------------------------------------------------------------------------- #
# Forced-join channels management.
# --------------------------------------------------------------------------- #
@bot.on(events.CallbackQuery(data=b"forcedjoin"))
async def forcedjoin_cb(event):
    if not is_owner(event):
        return
    state.pop(event.sender_id, None)
    chans = db.list_forced_channels()
    lines = []
    rows = []
    if not chans:
        lines.append("هیچ کانالی تنظیم نشده — عضویت اجباری خاموشه.")
    else:
        for c in chans:
            mark = "🟢 فعال" if c["enabled"] else "🔴 خاموش"
            lines.append(f"{mark} — {c.get('title') or c['chat']}  ({c['chat']})")
            rows.append([
                Button.inline("🔴 خاموش" if c["enabled"] else "🟢 روشن",
                              f"fjtog_{c['id']}".encode()),
                Button.inline("🗑 حذف", f"fjdel_{c['id']}".encode()),
            ])
    lines += [LINE, "⚠️ ربات مشتری باید ادمینِ کانال‌ها باشه تا عضویت چک بشه."]
    rows.append([Button.inline("➕ افزودن کانال", b"fj_add")])
    rows.append([Button.inline("🔙 منوی اصلی", b"home")])
    await safe_edit(event, card("📢 عضویت اجباری", lines), buttons=rows)


@bot.on(events.CallbackQuery(data=b"fj_add"))
async def fj_add_cb(event):
    if not is_owner(event):
        return
    state[event.sender_id] = {"step": "await_fj_channel"}
    await safe_edit(event,
                    "یوزرنیمِ کانال رو بفرست (مثلاً `@mychannel` یا `t.me/mychannel`).\n"
                    "⚠️ حتماً ربات مشتری رو ادمینِ اون کانال کن.",
                    buttons=[[Button.inline("🔙 لغو", b"forcedjoin")]])


async def _handle_fj_add(event, st):
    state.pop(event.sender_id, None)
    raw = (event.raw_text or "").strip()
    u = raw
    for pre in ("https://", "http://", "t.me/", "telegram.me/"):
        u = u.replace(pre, "")
    u = u.lstrip("@").strip().split("/")[0].split("?")[0]
    if not u:
        await event.respond("یوزرنیمِ کانال نامعتبره.", buttons=main_menu())
        return
    chat = "@" + u
    ok = db.add_forced_channel(chat, u, f"https://t.me/{u}")
    if not ok:
        await event.respond("این کانال قبلاً اضافه شده.",
                            buttons=[[Button.inline("📢 عضویت اجباری", b"forcedjoin")]])
        return
    await logbus.to_group(card("📢 FORCED CHANNEL ADDED", [f"📢 {chat}", f"🕒 {now()}"]))
    await event.respond(card("✅ کانال اضافه شد", [
        f"📢 {chat}", LINE,
        "⚠️ مطمئن شو ربات مشتری ادمینِ این کاناله، وگرنه چک عضویت کار نمی‌کنه."]),
        buttons=[[Button.inline("📢 عضویت اجباری", b"forcedjoin")],
                 [Button.inline("🔙 منوی اصلی", b"home")]])


@bot.on(events.CallbackQuery(pattern=b"fjtog_(\\d+)"))
async def fjtog_cb(event):
    if not is_owner(event):
        return
    cid = int(event.pattern_match.group(1))
    c = db.get_forced_channel(cid)
    if c:
        db.set_forced_channel_enabled(cid, not c["enabled"])
    await forcedjoin_cb(event)


@bot.on(events.CallbackQuery(pattern=b"fjdel_(\\d+)"))
async def fjdel_cb(event):
    if not is_owner(event):
        return
    cid = int(event.pattern_match.group(1))
    db.delete_forced_channel(cid)
    await forcedjoin_cb(event)


# --------------------------------------------------------------------------- #
# Free-text router.
# --------------------------------------------------------------------------- #
@bot.on(events.NewMessage(func=lambda e: e.is_private and not (e.raw_text or "").startswith("/")))
async def text_router(event):
    if not is_owner(event):
        return
    st = state.get(event.sender_id)
    if not st:
        return
    step = st.get("step")
    if step == "await_addcust":
        await _handle_addcust(event)
    elif step == "await_custtime":
        await _handle_custtime(event, st)
    elif step == "await_note":
        await _handle_note(event, st)
    elif step == "await_search":
        await _handle_search(event)
    elif step == "await_broadcast":
        await _handle_broadcast_preview(event)
    elif step == "await_broadcast_confirm":
        # If user types anything during confirm step, treat as cancel
        state.pop(event.sender_id, None)
        await event.respond("لغو شد. از دکمه‌ها استفاده کن.", buttons=main_menu())
    elif step == "await_price_input":
        await _handle_price_input(event, st)
    elif step == "await_trx_override":
        await _handle_trx_override(event)
    elif step == "await_tolerance":
        await _handle_tolerance(event)
    elif step in ("w_ip", "w_port", "w_user", "w_pass"):
        await _worker_add_flow(event, st)
    elif step == "await_fj_channel":
        await _handle_fj_add(event, st)


async def _handle_addcust(event):
    state.pop(event.sender_id, None)
    parts = event.raw_text.strip().split()
    if not parts or not parts[0].lstrip("-").isdigit():
        await event.respond("آیدی نامعتبره.", buttons=main_menu())
        return
    uid = int(parts[0])
    days = int(parts[1]) if len(parts) > 1 and parts[1].lstrip("-").isdigit() else 0
    db.ensure_customer(uid)
    if days:
        db.add_days(uid, days)
    central_db.audit("add_customer", f"{uid} +{days}d")
    await logbus.to_group(card("➕ ADD CUSTOMER (دستی)", [
        f"🆔 {uid}", f"⏳ {days} روز", f"🕒 {now()}"]))
    c = db.get_customer(uid)
    await event.respond("✅ مشتری اضافه شد.", buttons=main_menu())
    await bot.send_message(event.sender_id, _profile_text(c),
                           buttons=_profile_buttons(uid, c.get("blocked")))


async def _handle_custtime(event, st):
    uid = st["uid"]
    state.pop(event.sender_id, None)
    txt = event.raw_text.strip()
    if not txt.lstrip("-").isdigit():
        await event.respond("عدد نامعتبره.")
        return
    days = int(txt)
    new_exp = db.add_days(uid, days)
    central_db.audit("adjust_time", f"{uid}: {days:+d} -> {new_exp}")
    await logbus.to_group(card("🕒 تغییر زمان اشتراک", [
        f"🆔 {uid}", f"{days:+d} روز", f"📅 {new_exp}", f"🕒 {now()}"]))
    try:
        db.enqueue_notification(uid, f"📅 زمان اشتراکت به‌روزرسانی شد. انقضا: {new_exp}")
    except Exception:
        pass
    c = db.get_customer(uid)
    await event.respond("✅ انجام شد.", buttons=main_menu())
    await bot.send_message(event.sender_id, _profile_text(c),
                           buttons=_profile_buttons(uid, c.get("blocked")))


async def _handle_note(event, st):
    uid = st["uid"]
    state.pop(event.sender_id, None)
    db.set_note(uid, event.raw_text.strip())
    c = db.get_customer(uid)
    await event.respond("✅ یادداشت ذخیره شد.", buttons=main_menu())
    await bot.send_message(event.sender_id, _profile_text(c),
                           buttons=_profile_buttons(uid, c.get("blocked")))


async def _handle_search(event):
    state.pop(event.sender_id, None)
    results = db.search_customers(event.raw_text.strip())
    if not results:
        await event.respond("چیزی پیدا نشد.", buttons=main_menu())
        return
    await event.respond(f"🔎 {len(results)} نتیجه:", buttons=_cust_buttons(results))


async def _handle_broadcast_preview(event):
    """Show broadcast preview with count before sending."""
    text = event.raw_text.strip()
    if not text:
        await event.respond("متن خالیه.", buttons=main_menu())
        state.pop(event.sender_id, None)
        return
    customers = db.list_customers()
    count = len(customers)
    state[event.sender_id] = {"step": "await_broadcast_confirm", "broadcast_text": text}
    await event.respond(
        f"📣 {count} نفر دریافت می‌کنند. ادامه بده؟",
        buttons=[
            [Button.inline("✅ ارسال", b"broadcast_confirm"),
             Button.inline("❌ لغو", b"broadcast_cancel")],
        ]
    )


async def _handle_price_input(event, st):
    plan_key = st.get("plan_key")
    state.pop(event.sender_id, None)
    txt = event.raw_text.strip()
    try:
        new_price = float(txt)
        if new_price <= 0:
            raise ValueError("price must be positive")
    except (TypeError, ValueError):
        await event.respond("عدد نامعتبره. لطفا یک عدد مثبت بفرست.", buttons=main_menu())
        return
    db.set_plan_price(plan_key, new_price)
    plan_title = config.PLANS.get(plan_key, {}).get("title", plan_key)
    central_db.audit("set_plan_price", f"{plan_key}={new_price}")
    await logbus.to_group(card("💲 PRICE CHANGE", [
        f"📦 {plan_title}", f"💲 قیمت جدید: {new_price:g} USD", f"🕒 {now()}"]))
    await event.respond(f"✅ قیمت {plan_title} به {new_price:g} USD تغییر کرد.",
                        buttons=main_menu())


async def _handle_trx_override(event):
    state.pop(event.sender_id, None)
    txt = event.raw_text.strip()
    try:
        value = float(txt)
        if value < 0:
            raise ValueError("cannot be negative")
    except (TypeError, ValueError):
        await event.respond("عدد نامعتبره.", buttons=main_menu())
        return
    db.set_setting("trx_price_override", str(value))
    central_db.audit("set_trx_override", f"{value}")
    if value > 0:
        await logbus.to_group(card("⚙️ TRX OVERRIDE", [
            f"💲 قیمت: {value:g} USD", f"🕒 {now()}"]))
        await event.respond(f"✅ اورراید قیمت TRX به {value:g} USD تنظیم شد.",
                            buttons=main_menu())
    else:
        await logbus.to_group(card("⚙️ TRX OVERRIDE OFF", [f"🕒 {now()}"]))
        await event.respond("✅ اورراید قیمت TRX غیرفعال شد (از CoinGecko استفاده می‌شه).",
                            buttons=main_menu())


async def _handle_tolerance(event):
    state.pop(event.sender_id, None)
    txt = event.raw_text.strip()
    try:
        value = float(txt)
        if value < 0 or value > 50:
            raise ValueError("must be 0-50")
    except (TypeError, ValueError):
        await event.respond("عدد نامعتبره (بین 0 تا 50 درصد).", buttons=main_menu())
        return
    db.set_setting("payment_tolerance_percent", str(value))
    central_db.audit("set_tolerance", f"{value}%")
    await logbus.to_group(card("⚙️ TOLERANCE CHANGE", [
        f"📏 تلرانس جدید: {value:g}%", f"🕒 {now()}"]))
    await event.respond(f"✅ تلرانس پرداخت به {value:g}% تغییر کرد.",
                        buttons=main_menu())


# --------------------------------------------------------------------------- #
# Worker health report loop + entrypoint.
# --------------------------------------------------------------------------- #
async def worker_report_loop():
    interval = int(config.HEALTH_INTERVAL or 0)
    if interval <= 0:
        return
    while True:
        await asyncio.sleep(interval)
        try:
            workers = db.list_workers()
            remotes = [w for w in workers if not w.get("is_master")]
            if not remotes:
                continue
            results = await worker.check_all(workers)
            rows = [f"{r.get('tag')} : {r.get('status')} ({r.get('ping_ms')}ms)"
                    for r in results]
            await logbus.to_group(card("🛠 STATU WORKER ALL", rows + [f"🕒 {now()}"]))
        except Exception as e:  # noqa: BLE001
            print(f"[worker report] {e}")


async def amain():
    problems = config.validate_owner()
    if problems:
        raise SystemExit("تنظیمات ناقصه (.env): " + ", ".join(problems))
    db.init()
    central_db.init()
    worker.ensure_master_worker()
    await bot.start(bot_token=config.OWNER_BOT_TOKEN)
    logbus.bind(bot)
    await logbus.to_group(card("🎛 OWNER PANEL ONLINE", [
        f"🏷 Version : {config.VERSION}", f"🕒 {now()}"]))
    asyncio.create_task(worker_report_loop())
    asyncio.create_task(backup.backup_loop())
    print("owner bot running")
    await bot.run_until_disconnected()
