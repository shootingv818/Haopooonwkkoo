"""
telegram_client.py — thin Telethon warm-client adapter for the multi-account
sender (telegram_multi_send.py).
============================================================================

This is the ADAPTATION LAYER that lets the ported Meow multi-send engine run
inside this multi-customer project without changing how the rest of the
Telegram section works:

  * The engine identifies each account by a stable KEY. Here the key is the
    ``tg_accounts.id`` (as a string), because a phone number is NOT globally
    unique in this project (each customer can add the same phone). Using the
    row id keeps accounts isolated per customer and avoids lock/registry
    collisions between two customers who added the same number.
  * Sessions are stored as PLAINTEXT StringSession in tg_accounts.session
    (exactly like tg_panel.py already does) — no crypto layer here.
  * Only the helpers the engine actually calls are implemented:
    get_client / drop_client / get_contacts_ordered / upload_to_saved /
    send_saved_media / send_media / send_text.

It NEVER touches the Rubika side.
"""
from __future__ import annotations

import asyncio

from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

import config
import db

# Warm client registry (one persistent client per account key).
_clients: dict = {}        # acckey (str) -> TelegramClient
_locks: dict = {}          # acckey (str) -> asyncio.Lock


def _lock(acckey: str) -> asyncio.Lock:
    lk = _locks.get(acckey)
    if lk is None:
        lk = asyncio.Lock()
        _locks[acckey] = lk
    return lk


def _new_client(session_str: str = "") -> TelegramClient:
    return TelegramClient(StringSession(session_str or None),
                          config.API_ID, config.API_HASH)


async def get_client(acckey: str) -> TelegramClient:
    """Return a connected+authorized warm client for the account, opening it
    lazily from the stored (plaintext) StringSession. Raises RuntimeError if the
    account row is missing / has no session / is not authorized (needs
    re-login)."""
    acckey = str(acckey)
    c = _clients.get(acckey)
    if c is not None and c.is_connected():
        return c
    try:
        acc = db.get_tg_account(int(acckey))
    except (TypeError, ValueError):
        acc = None
    if not acc or not acc.get("session"):
        raise RuntimeError("no_session")
    c = _new_client(acc["session"])
    await c.connect()
    if not await c.is_user_authorized():
        try:
            await c.disconnect()
        except Exception:
            pass
        db.set_tg_status(int(acckey), "dead")
        raise RuntimeError("unauthorized")
    _clients[acckey] = c
    return c


async def drop_client(acckey: str):
    c = _clients.pop(str(acckey), None)
    if c is not None:
        try:
            await c.disconnect()
        except Exception:
            pass


async def close_all():
    for acckey in list(_clients.keys()):
        await drop_client(acckey)


# --------------------------------------------------------------------------- #
# FloodWait-aware call wrapper (mirrors the reference telegram_client).
# --------------------------------------------------------------------------- #
async def safe_call(coro_factory, *, retries: int = 1):
    attempt = 0
    while True:
        try:
            return await coro_factory()
        except FloodWaitError as e:
            wait = min(int(getattr(e, "seconds", 5)) + 1,
                       int(getattr(config, "TG_FLOODWAIT_MAX", 300)))
            if attempt >= retries:
                raise
            attempt += 1
            await asyncio.sleep(wait)


# --------------------------------------------------------------------------- #
# Contacts, mutual-first ordering.
# --------------------------------------------------------------------------- #
async def get_contacts(client: TelegramClient) -> list:
    res = await client(functions.contacts.GetContactsRequest(hash=0))
    return list(getattr(res, "users", []) or [])


async def get_contacts_ordered(client: TelegramClient) -> tuple:
    """Return (ordered_targets, mutual_count). Mutual contacts FIRST, then the
    remaining (non-mutual) contacts — so a send hits two-way contacts before
    everyone else."""
    users = await get_contacts(client)
    mutuals = [u for u in users if getattr(u, "mutual_contact", False)]
    rest = [u for u in users if not getattr(u, "mutual_contact", False)]
    return mutuals + rest, len(mutuals)


# --------------------------------------------------------------------------- #
# Sending (text + media). Media is uploaded ONCE to Saved Messages and then
# re-sent from that file reference to every recipient (no re-upload per chat).
# --------------------------------------------------------------------------- #
async def send_text(client: TelegramClient, entity, text: str, typing: float = 0.0):
    return await safe_call(lambda: client.send_message(entity, text))


async def send_media(client: TelegramClient, entity, file_path: str,
                     caption: str = "", typing: float = 0.0):
    return await safe_call(
        lambda: client.send_file(entity, file_path, caption=caption or None))


async def upload_to_saved(client: TelegramClient, file_path: str, caption: str = ""):
    return await safe_call(
        lambda: client.send_file("me", file_path, caption=caption or None))


async def send_saved_media(client: TelegramClient, entity, saved_msg, caption: str = ""):
    """Re-send the media of an already-uploaded Saved-Messages message WITHOUT a
    'Forwarded from' header and WITHOUT re-uploading the file. Falls back to a
    plain forward if the media can't be reused."""
    media = getattr(saved_msg, "media", None)
    if media is not None:
        try:
            return await safe_call(
                lambda: client.send_file(entity, media, caption=caption or None))
        except Exception:
            pass
    return await safe_call(lambda: client.forward_messages(entity, saved_msg))
