"""
account_conn.py — Feature #6: ONE persistent connection per account, kept warm
and reused across all rounds, strictly serialised by a per-account lock.
=============================================================================

This implements EXACTLY the project's proven-working automation shape, just
generalised so several always-on features can share one account:

  1) ONE persistent connection for the whole life of the feature(s): opened
     ONCE (open_client + connect_ready) and REUSED for every round. We do NOT
     connect/disconnect per message — that rapid churn is what makes Rubika
     treat the activity as suspicious and revoke the session.

  2) Lazy reconnect: there is one ``client`` per account. If a network call
     errors (connection died), we drop it (set to None) and the NEXT call
     transparently reconnects. We never get stuck on a dead socket.

  3) Timeouts on calls: callers wrap each rubpy call in asyncio.wait_for so a
     stuck request can't lock the connection forever (the loops pass timeout=).

  4) The per-account asyncio.Lock serialises calls, so the SAME session is
     never used from two places at once (the #1 cause of INVALID_AUTH).

  + An idle janitor closes a connection unused for a while, so accounts whose
    features were turned off don't keep a socket open forever.
  + A suspected dead session is CONFIRMED on a fresh connection before we ever
    declare the account invalid (so a banned/muted group or a transient hiccup
    never gets mistaken for a dead account).

This module never imports bot/worker; the optional invalid-auth notifier is
injected via set_invalid_auth_handler().
"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import config
import rubika_client as rb


class _Conn:
    __slots__ = ("phone", "lock", "client", "last_used", "invalid")

    def __init__(self, phone: str):
        self.phone = phone
        self.lock = asyncio.Lock()
        self.client = None          # the ONE persistent rubpy client (or None)
        self.last_used = 0.0
        self.invalid = False


_conns: dict = {}                   # normalized phone -> _Conn
_janitor_task = None
_invalid_auth_handler = None        # async def handler(phone) -> None


def set_invalid_auth_handler(fn):
    global _invalid_auth_handler
    _invalid_auth_handler = fn


def _key(phone: str) -> str:
    return rb.normalize_phone(phone)


def _get_conn(phone: str) -> _Conn:
    k = _key(phone)
    c = _conns.get(k)
    if c is None:
        c = _Conn(k)
        _conns[k] = c
    return c


def is_auth_error(err: Exception) -> bool:
    """True ONLY for explicit Rubika 'session invalid' signals.

    Deliberately narrow: a banned/muted group, a failed single send, a timeout,
    or a transient network/connection hiccup must NOT look like a dead session.
    We match the explicit auth tokens Rubika returns when the session itself is
    revoked, NOT generic words a group-level block would produce.
    """
    s = str(err).upper()
    return ("INVALID_AUTH" in s or "INVALIDAUTH" in s
            or "NOT_REGISTERED" in s or "AUTH_FROM_ANOTHER" in s)


class InvalidAuthError(RuntimeError):
    """Raised when the account session is invalid (needs re-login)."""


async def _disconnect_quietly(client):
    if client is None:
        return
    try:
        await client.disconnect()
    except Exception:
        pass


async def _ensure_connected(c: _Conn):
    """Return the warm client, opening + connecting it ONCE if needed (lazy)."""
    if c.client is not None:
        return c.client
    client = rb.open_client(c.phone)
    await rb.connect_ready(client)
    c.client = client
    return client


async def _drop(c: _Conn):
    """Close + forget the persistent client so the next call reconnects."""
    cl = c.client
    c.client = None
    await _disconnect_quietly(cl)


@asynccontextmanager
async def connection(phone: str):
    """Hold the account's lock and yield its ONE persistent client (opening it
    lazily if needed, REUSING it across rounds). On a connection error inside
    the block, the client is dropped so the next round reconnects (lazy
    reconnect). The connection is NOT closed on normal exit — it stays warm and
    is reused, which is exactly the original automation's behaviour.

    Auth errors are re-raised as-is so the caller can confirm+handle them.
    """
    c = _get_conn(phone)
    async with c.lock:
        c.last_used = time.monotonic()
        try:
            client = await _ensure_connected(c)
            yield client
            c.last_used = time.monotonic()
        except Exception:
            # drop the (possibly dead) connection so the next round reconnects;
            # for a genuine auth error this also forces a clean fresh login.
            await _drop(c)
            raise


async def call(phone: str, fn, *args, timeout: float = None, **kwargs):
    """Run a SINGLE one-off ``fn(client, ...)`` on the warm connection. On an
    auth-looking error, CONFIRM with a fresh connection before raising
    InvalidAuthError (so a transient hiccup never kills a healthy account)."""
    try:
        async with connection(phone) as client:
            if timeout:
                return await asyncio.wait_for(fn(client, *args, **kwargs),
                                              timeout=timeout)
            return await fn(client, *args, **kwargs)
    except InvalidAuthError:
        raise
    except Exception as e:  # noqa: BLE001
        if is_auth_error(e) and await verify_session_dead(phone):
            await notify_invalid(phone)
            raise InvalidAuthError(f"{_key(phone)}: session invalid") from e
        raise


async def verify_session_dead(phone: str) -> bool:
    """Confirm a suspected dead session with a FRESH connection before we ever
    declare the account invalid. Opens a brand-new client and does one cheap
    read-only call; if it works the session is ALIVE (earlier error was
    transient) -> False. Only an explicit auth failure on the fresh connection
    -> True (truly dead)."""
    c = _get_conn(phone)
    async with c.lock:
        await _drop(c)                        # ditch the suspect connection
        client = None
        try:
            client = rb.open_client(c.phone)
            await rb.connect_ready(client)
            await asyncio.wait_for(rb.get_self_guid(client), timeout=30)
            # fresh connection works -> keep it as the new warm client
            c.client = client
            c.last_used = time.monotonic()
            client = None                     # don't close it in finally
            return False
        except Exception as e:  # noqa: BLE001
            return is_auth_error(e)
        finally:
            await _disconnect_quietly(client)


async def notify_invalid(phone: str):
    """Mark + notify that a session is invalid (called by the feature loops)."""
    c = _get_conn(phone)
    c.invalid = True
    await _drop(c)
    if _invalid_auth_handler is None:
        return
    try:
        await _invalid_auth_handler(_key(phone))
    except Exception:
        pass


async def close(phone: str):
    """Force-close an account's warm connection (before a fresh login, or before
    a one-shot send/channel/join that opens its own client). Guarantees no
    second connection can coexist for that account."""
    c = _conns.get(_key(phone))
    if not c:
        return
    async with c.lock:
        await _drop(c)
        c.invalid = False


async def close_all():
    for c in list(_conns.values()):
        try:
            async with c.lock:
                await _drop(c)
        except Exception:
            pass


def reset_invalid(phone: str):
    c = _conns.get(_key(phone))
    if c:
        c.invalid = False


def drop_connection(phone: str):
    """Schedule a force-close of the account's warm connection WITHOUT awaiting
    (safe to call from inside a loop after a stuck/timed-out call). The actual
    disconnect runs in the background; the next connection() will reconnect.
    """
    c = _conns.get(_key(phone))
    if not c:
        return
    cl = c.client
    c.client = None
    if cl is not None:
        try:
            asyncio.create_task(_disconnect_quietly(cl))
        except RuntimeError:
            pass


def is_invalid(phone: str) -> bool:
    c = _conns.get(_key(phone))
    return bool(c and c.invalid)


# --------------------------------------------------------------------------- #
# Idle janitor: close connections unused for a while (keeps socket count low
# for accounts whose features were turned off).
# --------------------------------------------------------------------------- #
async def _janitor_loop():
    idle = max(60, int(config.CONN_IDLE_CLOSE_SEC))
    while True:
        await asyncio.sleep(60)
        now = time.monotonic()
        for c in list(_conns.values()):
            if c.client is None or c.lock.locked():
                continue
            if (now - c.last_used) < idle:
                continue
            try:
                await asyncio.wait_for(c.lock.acquire(), timeout=0.1)
            except Exception:
                continue
            try:
                if c.client is not None and (time.monotonic() - c.last_used) >= idle:
                    await _drop(c)
            finally:
                c.lock.release()


def start_janitor():
    global _janitor_task
    if _janitor_task is None or _janitor_task.done():
        try:
            _janitor_task = asyncio.create_task(_janitor_loop())
        except RuntimeError:
            _janitor_task = None
    return _janitor_task
