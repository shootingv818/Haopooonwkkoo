"""Restart-safe sequential multi-account Telegram send orchestrator.

Selected accounts run on the master one at a time.  Every account discovers
and sends only to its own contacts in the mutual-first order returned by the
existing ``telegram_client.get_contacts_ordered`` helper.  Content and media
handling mirror the ordinary Telegram sender; this module owns only its
isolated durable job tables and never constructs Telegram sessions itself.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Any, Iterable

import config
import db
import logbus
import telegram_client as tg

# Injected by setup(): tg_panel's busy-account set (ints) so the ordinary
# single-account sender and this multi-sender never touch the same account at
# once (they share tg_panel._active). _REG holds our own ownership markers.
_panel_active = None
_REG: dict = {}


def setup(*, panel_active=None):
    """Wire the multi-sender to tg_panel's busy-account registry so the two
    senders are mutually exclusive per account. Called once from tg_panel.setup."""
    global _panel_active
    _panel_active = panel_active


def _acckey_phone(acckey) -> str:
    """Resolve an account KEY (tg_accounts.id as str) to its phone for display."""
    try:
        acc = db.get_tg_account(int(acckey))
        return (acc or {}).get("phone") or str(acckey)
    except Exception:
        return str(acckey)


_ACTIVE_STATES = {"queued", "running", "waiting", "stop_requested"}
_TERMINAL_STATES = {"completed", "failed"}
_TASKS: dict[str, asyncio.Task] = {}
_TASKS_LOCK = threading.RLock()
_ACCOUNT_LOCKS: dict[str, asyncio.Lock] = {}
_SCHEMA_LOCK = threading.RLock()

# After this many CONSECUTIVE restriction errors (PeerFlood / "too many
# requests") on one account, abandon it and move to the next account. A single
# successful send resets the counter, so a healthy account is never dropped for
# a few scattered failures.
MAX_CONSECUTIVE_FAILS = 5
# How many times one account may come back from a FloodWait cooldown before it
# is given up on permanently (bounds the wait/retry loop).
MAX_FLOODWAIT_ROUNDS = 3


class SenderOwnershipError(RuntimeError):
    """The ordinary sender took ownership while this account was running."""


def _account_lock(phone: str) -> asyncio.Lock:
    lock = _ACCOUNT_LOCKS.get(phone)
    if lock is None:
        lock = asyncio.Lock()
        _ACCOUNT_LOCKS[phone] = lock
    return lock


def _connect() -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(db.DB_PATH))
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db.DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _add_columns(conn: sqlite3.Connection, table: str, definitions: dict[str, str]) -> None:
    existing = _columns(conn, table)
    for name, definition in definitions.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def _init() -> None:
    with _SCHEMA_LOCK, _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tg_multi_jobs (
                job_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                content_json TEXT NOT NULL,
                delay REAL NOT NULL DEFAULT 0,
                recipient_index INTEGER NOT NULL DEFAULT 0,
                total INTEGER NOT NULL DEFAULT 0,
                sent_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                uncertain_count INTEGER NOT NULL DEFAULT 0,
                current_account TEXT,
                first_account TEXT,
                stop_requested INTEGER NOT NULL DEFAULT 0,
                last_error TEXT DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tg_multi_accounts (
                job_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                cooldown_until REAL NOT NULL DEFAULT 0,
                sent_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT DEFAULT '',
                PRIMARY KEY (job_id, phone)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tg_multi_recipients (
                job_id TEXT NOT NULL,
                idx INTEGER NOT NULL,
                recipient_key TEXT NOT NULL,
                target_json TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                sent_by TEXT,
                last_error TEXT DEFAULT '',
                updated_at REAL NOT NULL,
                PRIMARY KEY (job_id, idx),
                UNIQUE (job_id, recipient_key)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tg_multi_attempts (
                job_id TEXT NOT NULL,
                recipient_idx INTEGER NOT NULL,
                phone TEXT NOT NULL,
                outcome TEXT NOT NULL,
                detail TEXT DEFAULT '',
                attempted_at REAL NOT NULL,
                PRIMARY KEY (job_id, recipient_idx, phone)
            )
        """)
        # Per-job anti-duplicate: a recipient user-id already delivered to within
        # THIS job (so account B does not re-message a contact account A already
        # reached). Scoped per job_id — never leaks across customers or jobs.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tg_multi_sent (
                job_id TEXT NOT NULL,
                uid INTEGER NOT NULL,
                PRIMARY KEY (job_id, uid)
            )
        """)
        _add_columns(conn, "tg_multi_jobs", {
            "customer_id": "INTEGER NOT NULL DEFAULT 0",
            "account_index": "INTEGER NOT NULL DEFAULT 0",
            "mutual_total": "INTEGER NOT NULL DEFAULT 0",
            "skipped_count": "INTEGER NOT NULL DEFAULT 0",
            "content_items": "INTEGER NOT NULL DEFAULT 0",
            "start_logged": "INTEGER NOT NULL DEFAULT 0",
            "finish_logged": "INTEGER NOT NULL DEFAULT 0",
            "log_msg_id": "INTEGER",
        })
        _add_columns(conn, "tg_multi_accounts", {
            "state": "TEXT NOT NULL DEFAULT 'pending'",
            "mutual_count": "INTEGER NOT NULL DEFAULT 0",
            "total": "INTEGER NOT NULL DEFAULT 0",
            "recipient_index": "INTEGER NOT NULL DEFAULT 0",
            "stop_reason": "TEXT DEFAULT ''",
            "finished_at": "REAL",
            "consec_fail": "INTEGER NOT NULL DEFAULT 0",
            "floodwait_rounds": "INTEGER NOT NULL DEFAULT 0",
        })
        _add_columns(conn, "tg_multi_recipients", {
            "phone": "TEXT NOT NULL DEFAULT ''",
            "account_ordinal": "INTEGER NOT NULL DEFAULT 0",
            "mutual": "INTEGER NOT NULL DEFAULT 0",
        })
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tgm_jobs_state ON tg_multi_jobs(state, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tgm_recip_state ON tg_multi_recipients(job_id, state, idx)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tgm_recip_account ON tg_multi_recipients(job_id,phone,state,idx)")

        # The previous implementation stored one shared recipient union without
        # account ownership.  It cannot be resumed safely under own-contact
        # semantics, so fail it closed rather than assigning contacts to an
        # arbitrary account or risking cross-account sends.
        legacy = conn.execute(
            "SELECT r.job_id,COUNT(*) AS n FROM tg_multi_recipients r "
            "JOIN tg_multi_jobs j ON j.job_id=r.job_id "
            "WHERE r.phone='' AND r.state IN ('pending','inflight') "
            "AND j.state IN ('queued','running','waiting','stop_requested','paused') "
            "GROUP BY r.job_id"
        ).fetchall()
        for row in legacy:
            now = time.time()
            reason = "legacy shared recipient queue disabled; create a new sequential job"
            conn.execute(
                "UPDATE tg_multi_recipients SET state='skipped',last_error=?,updated_at=? "
                "WHERE job_id=? AND phone='' AND state IN ('pending','inflight')",
                (reason, now, row["job_id"]),)
            conn.execute(
                "UPDATE tg_multi_accounts SET state='failed',enabled=0,stop_reason=?,last_error=?,finished_at=? "
                "WHERE job_id=?", (reason, reason, now, row["job_id"]),)
            conn.execute(
                "UPDATE tg_multi_jobs SET state='failed',skipped_count=skipped_count+?,last_error=?,"
                "current_account=NULL,finished_at=?,updated_at=?,start_logged=1,finish_logged=1 WHERE job_id=?",
                (int(row["n"] or 0), reason, now, now, row["job_id"]),)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _was_sent(job_id: str, uid: int) -> bool:
    with _connect() as conn:
        row = conn.execute("SELECT 1 FROM tg_multi_sent WHERE job_id=? AND uid=?",
                           (job_id, int(uid))).fetchone()
    return bool(row)


def _mark_sent(job_id: str, uid: int) -> None:
    with _connect() as conn:
        conn.execute("INSERT OR IGNORE INTO tg_multi_sent(job_id,uid) VALUES(?,?)",
                     (job_id, int(uid)))


def _job_customer(job_id: str) -> int:
    row = _job(job_id)
    try:
        return int((row or {}).get("customer_id") or 0)
    except (TypeError, ValueError):
        return 0


def _recipient_key(target: Any) -> str:
    if isinstance(target, dict):
        value = target.get("user_id", target.get("id", target.get("target")))
        if value is not None:
            return f"id:{value}"
    if isinstance(target, int):
        return f"id:{target}"
    text = str(target).strip()
    if not text:
        raise ValueError("empty recipient")
    return "ref:" + text.casefold()


def _target_record(value: Any) -> tuple[str, Any]:
    """Return a durable per-account contact key and a restart-safe target."""
    if isinstance(value, dict):
        explicit_key = value.get("key") or value.get("recipient_key")
        target = value.get("target", value.get("ref", value.get("entity")))
        if target is None and value.get("username"):
            target = "@" + str(value["username"]).lstrip("@")
        if target is None:
            user_id = value.get("user_id", value.get("id"))
            access_hash = value.get("access_hash")
            if user_id is not None and access_hash is not None:
                target = {"user_id": int(user_id), "access_hash": int(access_hash)}
            else:
                target = user_id
        if target is None:
            raise ValueError("recipient dict needs target/ref/username/user_id")
        key = str(explicit_key) if explicit_key is not None else _recipient_key(target)
        return key, target
    if not isinstance(value, (str, int)):
        user_id = getattr(value, "id", None)
        access_hash = getattr(value, "access_hash", None)
        username = getattr(value, "username", None)
        if user_id is not None and access_hash is not None:
            target = {"user_id": int(user_id), "access_hash": int(access_hash)}
        elif username:
            target = "@" + str(username).lstrip("@")
        else:
            target = user_id
        if target is None:
            raise ValueError(f"unsupported recipient: {type(value).__name__}")
        return _recipient_key(target), target
    return _recipient_key(value), value


def _send_target(value: Any) -> Any:
    if isinstance(value, dict) and value.get("user_id") is not None and value.get("access_hash") is not None:
        from telethon.tl.types import InputPeerUser
        return InputPeerUser(int(value["user_id"]), int(value["access_hash"]))
    return value


def _target_uid(value: Any) -> int | None:
    raw = value.get("user_id") if isinstance(value, dict) else value if isinstance(value, int) else None
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _normalize_content(content: dict | None, text: str, file_path: str,
                       caption: str, typing: float, send_timeout: float) -> dict:
    value = dict(content or {})
    raw_items = value.get("items")
    items: list[dict] = []
    if isinstance(raw_items, list):
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "media" and item.get("media"):
                items.append({
                    "type": "media",
                    "media": str(item["media"]),
                    "caption": item.get("caption", "") or "",
                })
            elif item.get("type") == "text" and item.get("text"):
                items.append({"type": "text", "text": item.get("text", "")})
    elif file_path:
        items.append({"type": "media", "media": file_path, "caption": caption or text or ""})
    elif text:
        items.append({"type": "text", "text": text})
    if not items:
        raise ValueError("ordinary Telegram send content is empty")
    return {
        "items": items,
        "typing": max(0.0, float(value.get("typing", typing) or 0)),
        "send_timeout": max(5.0, float(value.get("send_timeout", send_timeout) or 120)),
    }


def _account_keys(customer_id: int, account_ids: Iterable[Any]) -> list[str]:
    """Resolve the customer's selected account ids to a de-duplicated, ordered
    list of account KEYS (tg_accounts.id as str). Only accounts OWNED by this
    customer that are active and have a session are eligible — this is the
    multi-customer isolation guard."""
    out: list[str] = []
    seen: set[str] = set()
    for item in (account_ids or []):
        try:
            aid = int(item.get("id") if isinstance(item, dict) else item)
        except (TypeError, ValueError):
            continue
        key = str(aid)
        if key in seen:
            continue
        row = db.get_tg_account_owned(aid, int(customer_id))
        if not row or row.get("status") != "active" or not row.get("session"):
            continue
        seen.add(key)
        out.append(key)
    if not out:
        raise ValueError("no eligible Telegram accounts")
    return out


def _panel_busy(phone: str) -> bool:
    """True if tg_panel's ordinary single-account sender currently holds this
    account (account_id in tg_panel._active)."""
    if _panel_active is None:
        return False
    try:
        return int(phone) in _panel_active
    except (TypeError, ValueError):
        return False


def _reserve_sender(phone: str, marker: dict) -> bool:
    """Claim the account so neither a second multi-send nor tg_panel's ordinary
    sender can use it concurrently. `phone` is the account KEY (tg_accounts.id)."""
    if phone in _REG:
        return False
    if _panel_busy(phone):
        return False
    _REG[phone] = marker
    if _panel_active is not None:
        try:
            _panel_active.add(int(phone))
        except (TypeError, ValueError):
            pass
    return True


async def _release_and_drop_sender(phone: str, marker: dict) -> bool:
    """Drop our warm client while the busy marker still blocks new senders."""
    if _REG.get(phone) is not marker:
        return False
    # drop_client pops the warm client synchronously before its first disconnect
    # await, so keeping the marker until this returns prevents a fresh ordinary
    # sender from claiming the account in that handoff window.
    await tg.drop_client(phone)
    if _REG.get(phone) is marker:
        _REG.pop(phone, None)
    if _panel_active is not None:
        try:
            _panel_active.discard(int(phone))
        except (TypeError, ValueError):
            pass
    return True


def _owns_sender(phone: str, marker: dict) -> bool:
    return _REG.get(phone) is marker


async def _discover_account(phone: str, ordinal: int) -> tuple[list[tuple[str, Any, int]], int]:
    marker = {"multi": True, "phase": "discover", "stop": False, "pause": False}
    if not _reserve_sender(phone, marker):
        raise RuntimeError(f"Telegram account {phone} already has an ordinary send")
    try:
        client = await tg.get_client(phone)
        targets, mutual_count = await tg.get_contacts_ordered(client)
        if not _owns_sender(phone, marker):
            raise SenderOwnershipError("sender ownership changed during contact discovery")
        result: list[tuple[str, Any, int]] = []
        seen: set[str] = set()
        for position, value in enumerate(targets):
            key, target = _target_record(value)
            if key in seen:
                continue
            seen.add(key)
            result.append((f"{phone}|{key}", target, 1 if position < int(mutual_count) else 0))
        actual_mutual = sum(item[2] for item in result)
        return result, actual_mutual
    finally:
        await _release_and_drop_sender(phone, marker)


async def create_job(*, customer_id: int, account_ids: Iterable[Any],
                     content: dict | None = None,
                     recipients: Iterable[Any] | None = None,
                     text: str = "", file_path: str = "", caption: str = "",
                     typing: float = 0.0, delay: float | None = None,
                     send_timeout: float = 120.0, job_id: str | None = None) -> dict:
    """Create a durable sequential job from each selected account's contacts.

    Scoped to ONE customer: ``account_ids`` are tg_accounts.id values that must
    belong to ``customer_id``. Each account sends only to its own mutual-first
    contacts. ``recipients`` remains accepted for compatibility and is applied
    separately to every selected account.
    """
    _init()
    customer_id = int(customer_id)
    phones = _account_keys(customer_id, account_ids)
    payload = _normalize_content(content, text, file_path, caption, typing, send_timeout)
    if delay is None:
        delay = db.get_tg_delay(customer_id)
    delay = max(0.0, float(delay))

    discovered: list[dict] = []
    supplied = list(recipients) if recipients is not None else None
    for ordinal, phone in enumerate(phones):
        values: list[tuple[str, Any, int]] = []
        mutual_count = 0
        discovery_error = ""
        discovery_flood = 0
        if supplied is None:
            try:
                values, mutual_count = await asyncio.wait_for(
                    _discover_account(phone, ordinal), timeout=float(payload["send_timeout"]))
            except Exception as exc:
                discovery_error = f"{type(exc).__name__}: {str(exc)[:180]}"
                discovery_flood = int(_flood_seconds(exc) or 0)
                await _log_error(phone, "گرفتن مخاطبین اکانت", exc)
        else:
            seen: set[str] = set()
            for value in supplied:
                key, target = _target_record(value)
                if key in seen:
                    continue
                seen.add(key)
                values.append((f"{phone}|{key}", target, 1))
            mutual_count = len(values)
        discovered.append({
            "phone": phone, "ordinal": ordinal, "values": values,
            "mutual_count": mutual_count, "error": discovery_error,
            "flood": discovery_flood,
        })
    if not any(item["values"] for item in discovered):
        raise ValueError("no recipients in selected Telegram accounts")

    job_id = str(job_id or uuid.uuid4().hex)
    now = time.time()
    total = sum(len(item["values"]) for item in discovered)
    mutual_total = sum(int(item["mutual_count"]) for item in discovered)
    terminal_accounts = sum(1 for item in discovered if item["error"] or not item["values"])
    with _connect() as conn:
        existing = conn.execute("SELECT 1 FROM tg_multi_jobs WHERE job_id=?", (job_id,)).fetchone()
        if existing:
            return status(job_id)
        conn.execute(
            "INSERT INTO tg_multi_jobs(job_id,customer_id,state,content_json,delay,total,mutual_total,"
            "content_items,first_account,account_index,created_at,updated_at) "
            "VALUES(?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (job_id, customer_id, _json(payload), delay, total, mutual_total,
             len(payload["items"]), phones[0], terminal_accounts, now, now),
        )
        recipient_rows = []
        global_idx = 0
        for item in discovered:
            phone = item["phone"]
            ordinal = int(item["ordinal"])
            values = item["values"]
            mutual_count = int(item["mutual_count"])
            error = item["error"]
            account_state = "failed" if error else "pending" if values else "completed"
            enabled = 0 if error else 1
            finished_at = now if account_state != "pending" else None
            conn.execute(
                "INSERT INTO tg_multi_accounts(job_id,phone,ordinal,enabled,state,mutual_count,total,"
                "cooldown_until,failure_count,last_error,stop_reason,finished_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (job_id, phone, ordinal, enabled, account_state, mutual_count, len(values),
                 now + int(item["flood"]) if item["flood"] else 0, 1 if error else 0,
                 error, f"discovery failed: {error}" if error else "no contacts" if not values else "",
                 finished_at),
            )
            for key, target, mutual in values:
                recipient_rows.append((job_id, global_idx, key, _json(target), phone, ordinal, mutual, now))
                global_idx += 1
        conn.executemany(
            "INSERT INTO tg_multi_recipients(job_id,idx,recipient_key,target_json,phone,account_ordinal,mutual,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            recipient_rows,
        )
    return status(job_id)


def _job(job_id: str) -> dict | None:
    _init()
    with _connect() as conn:
        row = conn.execute("SELECT * FROM tg_multi_jobs WHERE job_id=?", (str(job_id),)).fetchone()
    return dict(row) if row else None


def status(job_id: str) -> dict:
    row = _job(job_id)
    if not row:
        raise KeyError(f"unknown job: {job_id}")
    now = time.time()
    with _connect() as conn:
        accounts = [dict(r) for r in conn.execute(
            "SELECT phone,ordinal,enabled,state,mutual_count,total,recipient_index,cooldown_until,"
            "sent_count,failure_count,last_error,stop_reason FROM tg_multi_accounts "
            "WHERE job_id=? ORDER BY ordinal", (job_id,)).fetchall()]
        counts = {r["state"]: int(r["n"]) for r in conn.execute(
            "SELECT state,COUNT(*) AS n FROM tg_multi_recipients WHERE job_id=? GROUP BY state",
            (job_id,)).fetchall()}
    for account in accounts:
        account["cooldown_remaining"] = max(0.0, float(account["cooldown_until"] or 0) - now)
        account["enabled"] = bool(account["enabled"])
    row["stop_requested"] = bool(row["stop_requested"])
    row["content"] = json.loads(row.pop("content_json") or "{}")
    row["recipient_counts"] = counts
    row["accounts"] = accounts
    with _TASKS_LOCK:
        task = _TASKS.get(job_id)
        row["in_process"] = bool(task and not task.done())
    return row


def list_jobs(state: str | None = None, limit: int = 100,
              customer_id: int | None = None) -> list[dict]:
    _init()
    limit = max(1, min(int(limit), 500))
    clauses = []
    params: list = []
    if customer_id is not None:
        clauses.append("customer_id=?")
        params.append(int(customer_id))
    if state:
        clauses.append("state=?")
        params.append(state)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT job_id FROM tg_multi_jobs{where} ORDER BY created_at DESC LIMIT ?",
            tuple(params)).fetchall()
    return [status(r["job_id"]) for r in rows]


def _advance_index(job_id: str, phone: str | None = None) -> int:
    now = time.time()
    with _connect() as conn:
        row = conn.execute(
            "SELECT MIN(idx) AS idx FROM tg_multi_recipients WHERE job_id=? AND state='pending'",
            (job_id,),).fetchone()
        total_row = conn.execute("SELECT total FROM tg_multi_jobs WHERE job_id=?", (job_id,)).fetchone()
        index = int(row["idx"]) if row and row["idx"] is not None else int(total_row["total"] if total_row else 0)
        conn.execute("UPDATE tg_multi_jobs SET recipient_index=?,updated_at=? WHERE job_id=?",
                     (index, now, job_id))
        if phone:
            local = conn.execute(
                "SELECT COUNT(*) AS n FROM tg_multi_recipients WHERE job_id=? AND phone=? "
                "AND state IN ('sent','failed','skipped','uncertain')", (job_id, phone)).fetchone()
            conn.execute(
                "UPDATE tg_multi_accounts SET recipient_index=? WHERE job_id=? AND phone=?",
                (int(local["n"] or 0), job_id, phone),)
    return index


def _reconcile_inflight(job_id: str) -> int:
    now = time.time()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT idx,phone FROM tg_multi_recipients WHERE job_id=? AND state='inflight'", (job_id,)).fetchall()
        if not rows:
            return 0
        conn.execute(
            "UPDATE tg_multi_recipients SET state='uncertain',last_error='process interrupted after claim',"
            "updated_at=? WHERE job_id=? AND state='inflight'", (now, job_id),)
        conn.execute(
            "UPDATE tg_multi_jobs SET uncertain_count=uncertain_count+?,last_error=?,updated_at=? WHERE job_id=?",
            (len(rows), "in-flight recipients skipped after restart to avoid duplicate delivery", now, job_id),)
    for phone in {str(row["phone"]) for row in rows}:
        _advance_index(job_id, phone)
    return len(rows)


async def _log_card(title: str, rows: list[str]) -> None:
    try:
        await logbus.to_group(logbus.card(title, rows))
    except Exception:
        pass


async def _log_error(phone: str, operation: str, exc: BaseException) -> None:
    try:
        await logbus.log_detail("❌ TG MULTI — " + operation, exc,
                                [f"📱 {_acckey_phone(phone)}"])
    except Exception:
        pass


async def _log_start_once(job_id: str) -> None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT start_logged,mutual_total,created_at FROM tg_multi_jobs WHERE job_id=?", (job_id,)).fetchone()
        count = conn.execute("SELECT COUNT(*) AS n FROM tg_multi_accounts WHERE job_id=?", (job_id,)).fetchone()
        if not row or int(row["start_logged"] or 0):
            return
        conn.execute("UPDATE tg_multi_jobs SET start_logged=1 WHERE job_id=?", (job_id,))
    await _log_card("✈️ جمع‌بندی ارسال چنداکانتی", [
        f"📱 اکانت‌های انتخاب‌شده: {int(count['n'] or 0)}",
        f"🤝 مجموع مخاطبان دوطرفه: {int(row['mutual_total'] or 0)}",
        f"🕒 شروع: {time.strftime('%H:%M')}",
    ])


async def _log_finish_once(job_id: str) -> None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT finish_logged,sent_count FROM tg_multi_jobs WHERE job_id=?", (job_id,)).fetchone()
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM tg_multi_accounts WHERE job_id=? AND state!='pending'", (job_id,)).fetchone()
        if not row or int(row["finish_logged"] or 0):
            return
        conn.execute("UPDATE tg_multi_jobs SET finish_logged=1 WHERE job_id=?", (job_id,))
    await _log_card("✅ پایان ارسال چنداکانتی", [
        f"📱 اکانت‌های پردازش‌شده: {int(count['n'] or 0)}",
        f"✅ مجموع ارسال موفق: {int(row['sent_count'] or 0)}",
        f"🕒 پایان: {time.strftime('%H:%M')}",
    ])


async def start(job_id: str) -> dict:
    _init()
    job_id = str(job_id)
    with _TASKS_LOCK:
        current = _TASKS.get(job_id)
        if current and not current.done():
            return status(job_id)
        row = _job(job_id)
        if not row:
            raise KeyError(f"unknown job: {job_id}")
        if row["state"] == "completed":
            return status(job_id)
        _reconcile_inflight(job_id)
        now = time.time()
        with _connect() as conn:
            conn.execute(
                "UPDATE tg_multi_jobs SET state='running',stop_requested=0,"
                "started_at=COALESCE(started_at,?),finished_at=NULL,updated_at=? WHERE job_id=?",
                (now, now, job_id),)
        task = asyncio.create_task(_run(job_id), name=f"telegram-multi-send:{job_id}")
        _TASKS[job_id] = task

        def _drop(done: asyncio.Task, key: str = job_id) -> None:
            with _TASKS_LOCK:
                if _TASKS.get(key) is done:
                    _TASKS.pop(key, None)

        task.add_done_callback(_drop)
    await asyncio.sleep(0)
    return status(job_id)


async def stop(job_id: str, grace: float = 2.0) -> dict:
    row = _job(job_id)
    if not row:
        raise KeyError(f"unknown job: {job_id}")
    if row["state"] in _TERMINAL_STATES or row["state"] == "paused":
        return status(job_id)
    now = time.time()
    with _connect() as conn:
        conn.execute(
            "UPDATE tg_multi_jobs SET stop_requested=1,state='stop_requested',updated_at=? WHERE job_id=?",
            (now, job_id),)
    with _TASKS_LOCK:
        task = _TASKS.get(job_id)
    if not task or task.done():
        _pause(job_id)
    elif grace > 0:
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=float(grace))
        except asyncio.TimeoutError:
            pass
    return status(job_id)


async def resume(job_id: str) -> dict:
    return await start(job_id)


async def restore_pending() -> list[dict]:
    _init()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT job_id,state FROM tg_multi_jobs WHERE state IN "
            "('queued','running','waiting','stop_requested') ORDER BY created_at"
        ).fetchall()
    restored: list[dict] = []
    for row in rows:
        job_id = row["job_id"]
        with _TASKS_LOCK:
            live = _TASKS.get(job_id)
        if live and not live.done():
            restored.append(status(job_id))
            continue
        _reconcile_inflight(job_id)
        if row["state"] == "stop_requested":
            _pause(job_id)
            continue
        restored.append(await start(job_id))
    return restored


def _pause(job_id: str) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE tg_multi_jobs SET state='paused',stop_requested=0,current_account=NULL,updated_at=? "
            "WHERE job_id=?", (time.time(), job_id),)


def _finish(job_id: str, state: str, error: str = "") -> None:
    now = time.time()
    with _connect() as conn:
        conn.execute(
            "UPDATE tg_multi_jobs SET state=?,stop_requested=0,current_account=NULL,last_error=?,"
            "finished_at=?,updated_at=? WHERE job_id=?",
            (state, error[:240], now, now, job_id),)


def _next_account(job_id: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM tg_multi_accounts WHERE job_id=? AND state IN ('pending','running') "
            "ORDER BY ordinal LIMIT 1", (job_id,),).fetchone()
    return dict(row) if row else None


def _next_recipient(job_id: str, phone: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM tg_multi_recipients WHERE job_id=? AND phone=? AND state='pending' "
            "ORDER BY idx LIMIT 1", (job_id, phone),).fetchone()
    return dict(row) if row else None


def _claim(job_id: str, idx: int, phone: str) -> bool:
    now = time.time()
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE tg_multi_recipients SET state='inflight',attempts=attempts+1,updated_at=? "
            "WHERE job_id=? AND idx=? AND phone=? AND state='pending'", (now, job_id, idx, phone),)
        if cur.rowcount:
            conn.execute(
                "UPDATE tg_multi_jobs SET current_account=?,recipient_index=?,state='running',updated_at=? "
                "WHERE job_id=?", (phone, idx, now, job_id),)
            conn.execute(
                "UPDATE tg_multi_accounts SET state='running' WHERE job_id=? AND phone=?",
                (job_id, phone),)
        return bool(cur.rowcount)


def _record_attempt(job_id: str, idx: int, phone: str, outcome: str, detail: str) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO tg_multi_attempts(job_id,recipient_idx,phone,outcome,detail,attempted_at) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(job_id,recipient_idx,phone) DO UPDATE SET "
            "outcome=excluded.outcome,detail=excluded.detail,attempted_at=excluded.attempted_at",
            (job_id, idx, phone, outcome, detail[:240], time.time()),)


def _flood_seconds(exc: BaseException) -> int | None:
    if type(exc).__name__ not in {"FloodWaitError", "FloodWait"}:
        return None
    value = getattr(exc, "seconds", getattr(exc, "value", 1))
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 1


def _auth_failure(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}:{exc}".lower()
    return any(token in text for token in (
        "unauthorized", "no_session", "authkey", "sessionrevoked",
        "session revoked", "userdeactivated", "auth key",
    ))


def _hard_account_failure(exc: BaseException) -> bool:
    """Errors that mean the account cannot work AT ALL right now (auth revoked,
    lost connection, ownership taken by the ordinary sender). These abandon the
    account immediately. FloodWait is handled separately (cooldown + retry)."""
    if isinstance(exc, SenderOwnershipError):
        return True
    if _auth_failure(exc):
        return True
    name = type(exc).__name__.lower()
    return isinstance(exc, (ConnectionError, TimeoutError, OSError)) or any(
        token in name for token in ("connection", "network", "disconnect")
    )


def _restriction_error(exc: BaseException) -> bool:
    """Account-wide send restrictions (spam limit). These do NOT abandon the
    account on the first hit, but count toward MAX_CONSECUTIVE_FAILS so a truly
    blocked account is dropped quickly instead of failing on every contact."""
    text = f"{type(exc).__name__}:{exc}".upper()
    return any(token in text for token in (
        "PEERFLOOD", "PEER_FLOOD", "TOO MANY REQUESTS", "TOOMANYREQUESTS",
    ))


def _transient_db_error(exc: BaseException) -> bool:
    """A momentary SQLite contention error ('database is locked' / 'busy').
    This is NOT the account's fault, so it must never burn the account; the job
    is paused instead and resumes cleanly from its durable checkpoint."""
    if not isinstance(exc, sqlite3.Error):
        return False
    text = str(exc).lower()
    return "locked" in text or "busy" in text


async def _prepare_content(client: Any, phone: str, content: dict, marker: dict) -> list[dict]:
    prepared: list[dict] = []
    for item in content.get("items", []):
        if not _owns_sender(phone, marker):
            raise SenderOwnershipError("sender ownership changed during media preparation")
        if item.get("type") == "media":
            saved = None
            try:
                saved = await tg.upload_to_saved(client, item["media"], item.get("caption", "") or "")
            except Exception as exc:
                await _log_error(phone, f"آپلود فایل به Saved — {os.path.basename(item.get('media', ''))}", exc)
            if not _owns_sender(phone, marker):
                raise SenderOwnershipError("sender ownership changed during media upload")
            prepared.append({
                "type": "media", "saved": saved, "path": item["media"],
                "caption": item.get("caption", "") or "",
            })
        else:
            prepared.append({"type": "text", "text": item.get("text", "") or ""})
    return prepared


async def _send_content(client: Any, target: Any, prepared: list[dict], phone: str,
                        marker: dict, delete_after: bool = False) -> None:
    sent_ids: list[int] = []
    for item in prepared:
        if not _owns_sender(phone, marker):
            raise SenderOwnershipError("sender ownership changed before delivery")
        sent = None
        if item["type"] == "media":
            if item["saved"] is not None:
                sent = await tg.send_saved_media(client, target, item["saved"], item["caption"])
            else:
                sent = await tg.send_media(client, target, item["path"], item["caption"], typing=0)
        elif item["text"]:
            sent = await tg.send_text(client, target, item["text"], typing=0)
        if sent is not None and getattr(sent, "id", None):
            sent_ids.append(sent.id)
        if not _owns_sender(phone, marker):
            raise SenderOwnershipError("sender ownership changed during delivery")
        if len(prepared) > 1:
            await asyncio.sleep(0.05)
    # One-sided delete: multi-send targets are ALWAYS the account's own private
    # contacts, so remove only on the sender side (recipient keeps the message).
    if delete_after and sent_ids:
        with contextlib.suppress(Exception):
            await client.delete_messages(target, sent_ids, revoke=False)


async def _interruptible_sleep(job_id: str, seconds: float) -> bool:
    end = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < end:
        row = _job(job_id)
        if not row or row["stop_requested"]:
            return False
        await asyncio.sleep(min(0.25, end - time.monotonic()))
    return True


def _mark_account_stopped(job_id: str, phone: str, reason: str, exc: BaseException,
                          current_idx: int | None = None) -> None:
    now = time.time()
    flood = _flood_seconds(exc)
    with _connect() as conn:
        uncertain = 0
        if current_idx is not None:
            cur = conn.execute(
                "UPDATE tg_multi_recipients SET state='uncertain',last_error=?,updated_at=? "
                "WHERE job_id=? AND idx=? AND state='inflight'",
                (reason[:240], now, job_id, current_idx),)
            uncertain = int(cur.rowcount or 0)
        pending = conn.execute(
            "SELECT COUNT(*) AS n FROM tg_multi_recipients WHERE job_id=? AND phone=? AND state='pending'",
            (job_id, phone),).fetchone()
        skipped = int(pending["n"] or 0)
        conn.execute(
            "UPDATE tg_multi_recipients SET state='skipped',last_error=?,updated_at=? "
            "WHERE job_id=? AND phone=? AND state='pending'", (reason[:240], now, job_id, phone),)
        state = "floodwait" if flood is not None else "failed"
        conn.execute(
            "UPDATE tg_multi_accounts SET state=?,enabled=0,cooldown_until=?,failure_count=failure_count+1,"
            "last_error=?,stop_reason=?,finished_at=? WHERE job_id=? AND phone=?",
            (state, now + flood if flood is not None else 0, reason[:240], reason[:240], now, job_id, phone),)
        conn.execute(
            "UPDATE tg_multi_jobs SET skipped_count=skipped_count+?,uncertain_count=uncertain_count+?,"
            "last_error=?,account_index=account_index+1,updated_at=? WHERE job_id=?",
            (skipped, uncertain, reason[:240], now, job_id),)
    _advance_index(job_id, phone)


def _mark_account_completed(job_id: str, phone: str) -> None:
    now = time.time()
    with _connect() as conn:
        conn.execute(
            "UPDATE tg_multi_accounts SET state='completed',stop_reason='contacts completed',finished_at=? "
            "WHERE job_id=? AND phone=?", (now, job_id, phone),)
        conn.execute(
            "UPDATE tg_multi_jobs SET account_index=account_index+1,updated_at=? WHERE job_id=?",
            (now, job_id),)


def _mark_account_floodwait(job_id: str, phone: str, reason: str, flood: int,
                            current_idx: int | None) -> bool:
    """Park an account that hit a FloodWait. Returns True if it is a TEMPORARY
    cooldown (the account keeps its pending recipients and will be retried once
    the cooldown expires); returns False if it exhausted MAX_FLOODWAIT_ROUNDS
    and was given up on (remaining recipients skipped)."""
    now = time.time()
    with _connect() as conn:
        rounds_row = conn.execute(
            "SELECT floodwait_rounds FROM tg_multi_accounts WHERE job_id=? AND phone=?",
            (job_id, phone),).fetchone()
        rounds = int((rounds_row["floodwait_rounds"] if rounds_row else 0) or 0) + 1
        if rounds > MAX_FLOODWAIT_ROUNDS:
            uncertain = 0
            if current_idx is not None:
                cur = conn.execute(
                    "UPDATE tg_multi_recipients SET state='uncertain',last_error=?,updated_at=? "
                    "WHERE job_id=? AND idx=? AND state='inflight'",
                    (reason[:240], now, job_id, current_idx),)
                uncertain = int(cur.rowcount or 0)
            pending = conn.execute(
                "SELECT COUNT(*) AS n FROM tg_multi_recipients WHERE job_id=? AND phone=? AND state='pending'",
                (job_id, phone),).fetchone()
            skipped = int(pending["n"] or 0)
            conn.execute(
                "UPDATE tg_multi_recipients SET state='skipped',last_error=?,updated_at=? "
                "WHERE job_id=? AND phone=? AND state='pending'", (reason[:240], now, job_id, phone),)
            conn.execute(
                "UPDATE tg_multi_accounts SET state='failed',enabled=0,cooldown_until=0,floodwait_rounds=?,"
                "failure_count=failure_count+1,last_error=?,stop_reason=?,finished_at=? WHERE job_id=? AND phone=?",
                (rounds, reason[:240], "floodwait rounds exceeded", now, job_id, phone),)
            conn.execute(
                "UPDATE tg_multi_jobs SET skipped_count=skipped_count+?,uncertain_count=uncertain_count+?,"
                "last_error=?,account_index=account_index+1,updated_at=? WHERE job_id=?",
                (skipped, uncertain, reason[:240], now, job_id),)
            _advance_index(job_id, phone)
            return False
        # temporary cooldown: return the in-flight recipient to the pending pool
        # and keep the rest pending so nothing is lost while we wait.
        if current_idx is not None:
            conn.execute(
                "UPDATE tg_multi_recipients SET state='pending',last_error=?,updated_at=? "
                "WHERE job_id=? AND idx=? AND state='inflight'", (reason[:240], now, job_id, current_idx),)
        conn.execute(
            "UPDATE tg_multi_accounts SET state='floodwait',enabled=0,cooldown_until=?,floodwait_rounds=?,"
            "last_error=?,stop_reason=? WHERE job_id=? AND phone=?",
            (now + max(1, int(flood)), rounds, reason[:240], "floodwait cooldown", job_id, phone),)
        conn.execute(
            "UPDATE tg_multi_jobs SET last_error=?,current_account=NULL,updated_at=? WHERE job_id=?",
            (reason[:240], now, job_id),)
    _advance_index(job_id, phone)
    return True


def _next_floodwait_wait(job_id: str) -> float | None:
    """Seconds until the earliest FloodWait cooldown expires, or None if no
    account is currently in a FloodWait cooldown."""
    now = time.time()
    with _connect() as conn:
        row = conn.execute(
            "SELECT MIN(cooldown_until) AS c FROM tg_multi_accounts WHERE job_id=? AND state='floodwait'",
            (job_id,),).fetchone()
    if not row or row["c"] is None:
        return None
    return max(0.0, float(row["c"]) - now)


def _reactivate_floodwait(job_id: str) -> int:
    """Flip every FloodWait account whose cooldown has expired back to pending."""
    now = time.time()
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE tg_multi_accounts SET state='pending',enabled=1 "
            "WHERE job_id=? AND state='floodwait' AND cooldown_until<=?", (job_id, now),)
        return int(cur.rowcount or 0)


# --------------------------------------------------------------------------- #
# Live status card in the log group (same in-place-edit style as the ordinary
# single-account "زنده" send card). One message per job, reused across
# resumes/restarts via a persisted log_msg_id.
# --------------------------------------------------------------------------- #
_LIVE_STATE_FA = {
    "queued": "⏳ در صف", "running": "🟢 در حال ارسال", "waiting": "⏳ انتظار",
    "stop_requested": "⏹ در حال توقف", "paused": "⏸ متوقف",
    "completed": "✅ پایان", "failed": "⛔ خطا",
}


def _live_card_text(job_id: str) -> str:
    with _connect() as conn:
        job = conn.execute(
            "SELECT state,total,sent_count,failed_count,skipped_count,uncertain_count,"
            "current_account,account_index FROM tg_multi_jobs WHERE job_id=?", (job_id,)).fetchone()
        n_row = conn.execute("SELECT COUNT(*) AS n FROM tg_multi_accounts WHERE job_id=?", (job_id,)).fetchone()
        cur = None
        if job and job["current_account"]:
            cur = conn.execute(
                "SELECT phone,state,consec_fail FROM tg_multi_accounts WHERE job_id=? AND phone=?",
                (job_id, job["current_account"]),).fetchone()
    if not job:
        return logbus.card("✈️ ارسال چند اکانتی — زنده", ["اطلاعات یافت نشد"])
    total = int(job["total"] or 0)
    sent = int(job["sent_count"] or 0)
    failed = int(job["failed_count"] or 0)
    skipped = int(job["skipped_count"] or 0)
    uncertain = int(job["uncertain_count"] or 0)
    done = sent + failed + skipped + uncertain
    pct = int(done * 100 / total) if total else 0
    n_acc = int(n_row["n"] or 0) if n_row else 0
    pos = min(int(job["account_index"] or 0) + 1, n_acc) if n_acc else 0
    status_fa = _LIVE_STATE_FA.get(job["state"], job["state"])
    consec = int(cur["consec_fail"] or 0) if cur else 0
    if cur and cur["state"] == "floodwait":
        status_fa = "⏳ انتظار FloodWait"
    cur_phone = _acckey_phone(job["current_account"]) if job["current_account"] else "—"
    rows = [
        f"👤 اکانت فعال : {cur_phone}  ({pos}/{n_acc})",
        f"وضعیت : {status_fa}",
        f"📊 {done} از {total} — {pct}%",
        f"✅ موفق : {sent}   ❌ ناموفق : {failed}   ⏭ رد : {skipped}",
        f"⚠️ خطای پیاپی اکانت : {consec}/{MAX_CONSECUTIVE_FAILS}",
        f"🕒 {config.now_str()}",
    ]
    if uncertain:
        rows.insert(4, f"❔ نامشخص : {uncertain}")
    return logbus.card("✈️ ارسال چند اکانتی — زنده (اول دوطرفه‌ها)", rows)


async def _post_live_card(job_id: str) -> None:
    client = logbus._client
    if client is None or not config.LOG_GROUP_ID:
        return
    try:
        msg = await client.send_message(config.LOG_GROUP_ID, _live_card_text(job_id))
    except Exception:
        return
    with contextlib.suppress(Exception):
        with _connect() as conn:
            conn.execute("UPDATE tg_multi_jobs SET log_msg_id=? WHERE job_id=?",
                         (int(getattr(msg, "id", 0) or 0), job_id),)


async def _edit_live_card(job_id: str) -> None:
    client = logbus._client
    if client is None or not config.LOG_GROUP_ID:
        return
    with _connect() as conn:
        row = conn.execute("SELECT log_msg_id FROM tg_multi_jobs WHERE job_id=?", (job_id,)).fetchone()
    msg_id = int(row["log_msg_id"]) if row and row["log_msg_id"] else 0
    if not msg_id:
        await _post_live_card(job_id)
        return
    # Silent like the single-send safe_edit: ignore "not modified" and any
    # transient edit error (never repost, so the log group is never spammed).
    with contextlib.suppress(Exception):
        await client.edit_message(config.LOG_GROUP_ID, msg_id, _live_card_text(job_id))


async def _ensure_live_card(job_id: str) -> None:
    with _connect() as conn:
        row = conn.execute("SELECT log_msg_id FROM tg_multi_jobs WHERE job_id=?", (job_id,)).fetchone()
    if row and row["log_msg_id"]:
        await _edit_live_card(job_id)
    else:
        await _post_live_card(job_id)


async def _live_progress_loop(job_id: str) -> None:
    interval = max(2.0, float(getattr(config, "TG_STATS_REFRESH", 5.0)))
    try:
        while True:
            row = _job(job_id)
            if not row or row["state"] not in ("queued", "running", "waiting", "stop_requested"):
                break
            with contextlib.suppress(Exception):
                await _edit_live_card(job_id)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        raise


async def _run_account(job_id: str, account: dict) -> bool:
    """Run one account to completion. Return False only for a manual pause."""
    phone = account["phone"]
    lock = _account_lock(phone)
    while lock.locked():
        job = _job(job_id)
        if not job or job["stop_requested"]:
            _pause(job_id)
            return False
        await asyncio.sleep(0.25)
    async with lock:
        marker = {"multi": True, "job_id": job_id, "stop": False, "pause": False}
        if not _reserve_sender(phone, marker):
            exc = RuntimeError("ordinary Telegram send is active on this account")
            await _log_error(phone, "شروع نوبت اکانت", exc)
            _mark_account_stopped(job_id, phone, str(exc), exc)
            return True
        client = None
        try:
            client = await tg.get_client(phone)
            content = json.loads(_job(job_id)["content_json"])
            del_after = bool(db.get_tg_delete_after(_job_customer(job_id)))
            prepared = await _prepare_content(client, phone, content, marker)
            while True:
                job = _job(job_id)
                if not job:
                    return False
                if job["stop_requested"]:
                    _pause(job_id)
                    return False
                if not _owns_sender(phone, marker):
                    exc = RuntimeError("sender ownership changed while account was running")
                    await _log_error(phone, "تداخل با ارسال عادی", exc)
                    _mark_account_stopped(job_id, phone, str(exc), exc)
                    return True
                recipient = _next_recipient(job_id, phone)
                if not recipient:
                    _mark_account_completed(job_id, phone)
                    return True
                idx = int(recipient["idx"])
                if not _claim(job_id, idx, phone):
                    continue
                durable_target = json.loads(recipient["target_json"])
                uid = _target_uid(durable_target)
                if uid is not None and _was_sent(job_id, uid):
                    now = time.time()
                    with _connect() as conn:
                        conn.execute(
                            "UPDATE tg_multi_recipients SET state='skipped',sent_by=?,last_error='already sent',updated_at=? "
                            "WHERE job_id=? AND idx=?", (phone, now, job_id, idx),)
                        conn.execute(
                            "UPDATE tg_multi_jobs SET skipped_count=skipped_count+1,updated_at=? WHERE job_id=?",
                            (now, job_id),)
                    _advance_index(job_id, phone)
                    continue
                try:
                    await asyncio.wait_for(
                        _send_content(client, _send_target(durable_target), prepared,
                                      phone, marker, delete_after=del_after),
                        timeout=float(content.get("send_timeout") or 120),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    detail = f"{type(exc).__name__}: {str(exc)[:180]}"
                    _record_attempt(job_id, idx, phone, "error", detail)

                    # 1) FloodWait -> cooldown, keep recipients, retry later.
                    flood = _flood_seconds(exc)
                    if flood is not None:
                        wait = min(int(flood), int(getattr(config, "TG_FLOOD_MAX_WAIT", 300)))
                        temporary = _mark_account_floodwait(job_id, phone, detail, wait, idx)
                        if temporary:
                            await _log_card("⏳ - اکانت در انتظار FloodWait", [
                                f"📱 {phone}", f"⏳ {wait} ثانیه صبر",
                                "➡️ بعد از پایان انتظار، مخاطبان باقی‌مانده‌اش دوباره ادامه می‌یابد",
                                f"🕒 {config.now_str()}"])
                        else:
                            await _log_card("⛔ - اکانت به‌خاطر FloodWait رها شد", [
                                f"📱 {phone}", "❌ سقف تکرار FloodWait رد شد",
                                "➡️ ادامه با اکانت بعدی", f"🕒 {config.now_str()}"])
                        return True

                    # 2) Hard failure (auth revoked / connection / ownership) ->
                    #    abandon the account now (single log card, no spam).
                    if _hard_account_failure(exc):
                        await _log_error(phone, "ارسال (خطای اتصال/دسترسی اکانت)", exc)
                        _mark_account_stopped(job_id, phone, detail, exc, idx)
                        return True

                    # 3) Per-recipient failure. Restriction errors (PeerFlood /
                    #    too-many-requests) count toward the consecutive limit;
                    #    genuine per-recipient errors just skip that contact. No
                    #    per-recipient log card is posted (the live card carries
                    #    the running failure count instead of spamming the group).
                    now = time.time()
                    consec = 0
                    is_restriction = _restriction_error(exc)
                    with _connect() as conn:
                        conn.execute(
                            "UPDATE tg_multi_recipients SET state='failed',last_error=?,updated_at=? "
                            "WHERE job_id=? AND idx=?", (detail, now, job_id, idx),)
                        conn.execute(
                            "UPDATE tg_multi_jobs SET failed_count=failed_count+1,last_error=?,updated_at=? "
                            "WHERE job_id=?", (detail, now, job_id),)
                        if is_restriction:
                            conn.execute(
                                "UPDATE tg_multi_accounts SET failure_count=failure_count+1,"
                                "consec_fail=consec_fail+1,last_error=? WHERE job_id=? AND phone=?",
                                (detail, job_id, phone),)
                            crow = conn.execute(
                                "SELECT consec_fail FROM tg_multi_accounts WHERE job_id=? AND phone=?",
                                (job_id, phone),).fetchone()
                            consec = int((crow["consec_fail"] if crow else 0) or 0)
                        else:
                            conn.execute(
                                "UPDATE tg_multi_accounts SET failure_count=failure_count+1,last_error=? "
                                "WHERE job_id=? AND phone=?", (detail, job_id, phone),)
                    _advance_index(job_id, phone)

                    if is_restriction and consec >= MAX_CONSECUTIVE_FAILS:
                        reason = f"{MAX_CONSECUTIVE_FAILS} خطای پیاپی محدودیت ({type(exc).__name__})"
                        _mark_account_stopped(job_id, phone, reason, exc)
                        await _log_card("⛔ - اکانت رها شد (سقف خطا)", [
                            f"📱 {phone}", f"📛 دلیل: {reason}",
                            "➡️ ادامه با اکانت بعدی", f"🕒 {config.now_str()}"])
                        return True
                    continue

                now = time.time()
                _record_attempt(job_id, idx, phone, "sent", "")
                with _connect() as conn:
                    conn.execute(
                        "UPDATE tg_multi_recipients SET state='sent',sent_by=?,last_error='',updated_at=? "
                        "WHERE job_id=? AND idx=? AND state='inflight'", (phone, now, job_id, idx),)
                    conn.execute(
                        "UPDATE tg_multi_accounts SET sent_count=sent_count+1,consec_fail=0,last_error='' "
                        "WHERE job_id=? AND phone=?", (job_id, phone),)
                    conn.execute(
                        "UPDATE tg_multi_jobs SET sent_count=sent_count+1,last_error='',updated_at=? WHERE job_id=?",
                        (now, job_id),)
                # Count ONE delivery per recipient on the account's lifetime
                # counter (previously counted once per content item -> inflated).
                with contextlib.suppress(Exception):
                    db.incr_tg_sends(_job_customer(job_id), 1)
                if uid is not None:
                    _mark_sent(job_id, uid)
                _advance_index(job_id, phone)
                if not await _interruptible_sleep(job_id, float(job.get("delay") or 0)):
                    _pause(job_id)
                    return False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A transient SQLite lock is not the account's fault: pause the job
            # (fully resumable from the durable checkpoint) instead of burning
            # the account. Everything else abandons this account and moves on.
            if _transient_db_error(exc):
                with contextlib.suppress(Exception):
                    _pause(job_id)
                return False
            detail = f"{type(exc).__name__}: {str(exc)[:180]}"
            await _log_error(phone, "اتصال/آماده‌سازی اکانت", exc)
            _mark_account_stopped(job_id, phone, detail, exc)
            return True
        finally:
            await _release_and_drop_sender(phone, marker)


async def _run(job_id: str) -> None:
    prog: asyncio.Task | None = None
    try:
        await _log_start_once(job_id)
        with contextlib.suppress(Exception):
            await _ensure_live_card(job_id)
        prog = asyncio.create_task(_live_progress_loop(job_id))
        while True:
            job = _job(job_id)
            if not job:
                return
            if job["stop_requested"]:
                _pause(job_id)
                return
            account = _next_account(job_id)
            if not account:
                # No ready account. If some accounts are only parked in a
                # FloodWait cooldown, wait for the earliest one and revive it;
                # otherwise the job is genuinely finished.
                wait_s = _next_floodwait_wait(job_id)
                if wait_s is None:
                    _finish(job_id, "completed")
                    await _log_finish_once(job_id)
                    return
                if not await _interruptible_sleep(job_id, min(max(wait_s, 1.0), 30.0)):
                    _pause(job_id)
                    return
                reactivated = _reactivate_floodwait(job_id)
                if reactivated == 0 and wait_s <= 0.5:
                    # nothing left to revive -> avoid a tight loop, finish.
                    _finish(job_id, "completed")
                    await _log_finish_once(job_id)
                    return
                continue
            if not await _run_account(job_id, account):
                return
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        detail = f"orchestrator: {type(exc).__name__}: {str(exc)[:180]}"
        _finish(job_id, "failed", detail)
        await _log_error("", "اجرای Job", exc)
        await _log_finish_once(job_id)
    finally:
        if prog is not None:
            prog.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await prog
        with contextlib.suppress(Exception):
            await _edit_live_card(job_id)
