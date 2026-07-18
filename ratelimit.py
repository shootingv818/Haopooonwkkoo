"""
ratelimit.py — anti-flood guard (15 actions / 2 minutes by default).
====================================================================

A customer who performs more than ``config.RATE_LIMIT_MAX`` rate-limited
actions within ``config.RATE_LIMIT_WINDOW`` seconds is AUTO-BLOCKED and the
abuse is logged to the central group. The counting/window lives in the customer
DB (db.rate_hit) so it survives restarts; this module wires the count to the
block + log side-effects.
"""
from __future__ import annotations

import config
import db
import logbus


async def guard(customer_id: int, name: str = "") -> bool:
    """Record one action for the customer. Returns True if allowed, or False if
    the customer just exceeded the limit — in which case they are blocked and
    the event is logged. Already-blocked customers always return False."""
    if db.is_blocked(customer_id):
        return False

    allowed, count = db.rate_hit(customer_id)
    if allowed:
        return True

    # over the limit -> auto-block + log (only act on the first time over)
    db.set_blocked(customer_id, True)
    await logbus.event("🚫 RATE-LIMIT — مسدودیِ خودکار", [
        f"👤 Customer : {name or customer_id}",
        f"🆔 ID : {customer_id}",
        f"📈 بیش از {config.RATE_LIMIT_MAX} اکشن در "
        f"{config.RATE_LIMIT_WINDOW} ثانیه ({count} اکشن).",
        "⛔ مشتری به‌صورت خودکار مسدود شد.",
        f"🕒 {logbus.now()}",
    ], pv_user=customer_id)
    return False
