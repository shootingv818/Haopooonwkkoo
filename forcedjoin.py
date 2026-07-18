# -*- coding: utf-8 -*-
"""
forcedjoin.py — shared "forced channel membership" gate.
=========================================================

The owner configures required channels in the OWNER panel (stored in db via
forced_channels). The CUSTOMER bot enforces membership before letting a user
do anything (in every section's gate: Rubika / Telegram / Bale).

HARD REQUIREMENT: the CUSTOMER bot must be an ADMIN in each required channel,
otherwise Telegram won't let it query a user's membership. If membership can't
be verified (bot not admin / channel unresolved), we DO NOT block the user —
so a misconfiguration can never lock everyone out.
"""

import config
import db

from telethon import Button
from telethon.errors import UserNotParticipantError


async def missing_for(client, uid: int) -> list:
    """Return the list of enabled required channels the user is NOT a member of.
    Unverifiable channels are skipped (never block on uncertainty)."""
    if uid == config.OWNER_ID:          # never gate the owner
        return []
    out = []
    for c in db.list_forced_channels(only_enabled=True):
        target = c.get("chat")
        if not target:
            continue
        try:
            await client.get_permissions(target, uid)   # raises if not a member
        except UserNotParticipantError:
            out.append(c)
        except Exception:
            # bot not admin / channel unresolved -> can't verify -> don't block
            continue
    return out


def _channel_link(c: dict) -> str:
    link = (c.get("link") or "").strip()
    if link:
        return link
    chat = (c.get("chat") or "").lstrip("@")
    return f"https://t.me/{chat}" if chat else ""


def prompt_buttons(missing: list, check_data: bytes = b"fj_check"):
    rows = []
    for c in missing:
        link = _channel_link(c)
        if link:
            rows.append([Button.url(f"📢 {c.get('title') or c.get('chat')}", link)])
    rows.append([Button.inline("✅ عضو شدم، بررسی کن", check_data)])
    return rows


async def enforce(client, event) -> bool:
    """Return True if the user may proceed; otherwise show the join prompt and
    return False."""
    uid = event.sender_id
    missing = await missing_for(client, uid)
    if not missing:
        return True
    text = ("🔒 برای استفاده از ربات، اول عضو این کانال‌(ها) شو، "
            "بعد دکمهٔ «✅ عضو شدم» رو بزن:")
    buttons = prompt_buttons(missing)
    try:
        await event.respond(text, buttons=buttons)
    except Exception:
        try:
            await client.send_message(uid, text, buttons=buttons)
        except Exception:
            pass
    return False
