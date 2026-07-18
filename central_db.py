"""
central_db.py — the OWNER-only central panel database (data/central.db).
========================================================================

This is the SECOND of the project's two logically separate databases. It holds
data that belongs purely to the central panel and that the CUSTOMER bot must
never be able to read:

  * owner_state   — maintenance flag, last automatic backup time
  * broadcasts    — a record of every broadcast the owner sent
  * audit_log     — every privileged owner action (add customer, +/- time,
                    block/unblock, broadcast, maintenance toggle, backup)

The customer bot process NEVER imports this module. Only owner_bot.py and the
shared backup/maintenance helpers (run on the owner side) do.
"""
import os
import sqlite3

import config

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "central.db")


def _now() -> str:
    return config.now_str()


def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # timeout + busy_timeout: wait for a held write lock instead of instantly
    # raising "database is locked" under concurrent access.
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init():
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS owner_state (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            maintenance INTEGER DEFAULT 0,
            last_backup TEXT DEFAULT ''
        )
        """
    )
    c.execute(
        "INSERT OR IGNORE INTO owner_state (id, maintenance, last_backup) "
        "VALUES (1, ?, '')",
        (1 if config.MAINTENANCE_DEFAULT else 0,),
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS broadcasts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            text       TEXT,
            sent_ok    INTEGER DEFAULT 0,
            sent_fail  INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            action     TEXT,
            detail     TEXT,
            created_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()


# ---- maintenance mode ----
def get_maintenance() -> bool:
    conn = _conn()
    row = conn.execute("SELECT maintenance FROM owner_state WHERE id = 1").fetchone()
    conn.close()
    return bool(row["maintenance"]) if row else False


def _maintenance_flag_path() -> str:
    return os.path.join(os.path.dirname(DB_PATH), "maintenance.flag")


def set_maintenance(on: bool):
    conn = _conn()
    conn.execute("UPDATE owner_state SET maintenance = ? WHERE id = 1",
                 (1 if on else 0,))
    conn.commit()
    conn.close()
    # Mirror to a tiny flag file so the CUSTOMER bot can see maintenance state
    # WITHOUT importing this owner-only central database.
    path = _maintenance_flag_path()
    try:
        if on:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write("1")
        elif os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# ---- automatic backup bookkeeping ----
def get_last_backup() -> str:
    conn = _conn()
    row = conn.execute("SELECT last_backup FROM owner_state WHERE id = 1").fetchone()
    conn.close()
    return (row["last_backup"] if row else "") or ""


def set_last_backup(ts: str = None):
    conn = _conn()
    conn.execute("UPDATE owner_state SET last_backup = ? WHERE id = 1",
                 (ts or _now(),))
    conn.commit()
    conn.close()


# ---- broadcasts ----
def record_broadcast(text: str, sent_ok: int, sent_fail: int) -> int:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO broadcasts (text, sent_ok, sent_fail, created_at) "
        "VALUES (?, ?, ?, ?)",
        (text or "", int(sent_ok), int(sent_fail), _now()),
    )
    conn.commit()
    bid = c.lastrowid
    conn.close()
    return bid


def list_broadcasts(limit: int = 20) -> list:
    conn = _conn()
    rows = conn.execute("SELECT * FROM broadcasts ORDER BY id DESC LIMIT ?",
                        (int(limit),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---- audit log ----
def audit(action: str, detail: str = ""):
    conn = _conn()
    conn.execute("INSERT INTO audit_log (action, detail, created_at) VALUES (?, ?, ?)",
                 (action or "", detail or "", _now()))
    conn.commit()
    conn.close()


def list_audit(limit: int = 50) -> list:
    conn = _conn()
    rows = conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT ?",
                        (int(limit),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
