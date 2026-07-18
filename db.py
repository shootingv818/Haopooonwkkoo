"""
db.py — the CUSTOMER operational database (data/customer.db).
=============================================================

This is one of the project's TWO logically separate databases. It holds
everything the CUSTOMER bot needs and nothing the customer must not see:

  * customers / subscriptions   (telegram_id, expiry, blocked, warned)
  * accounts                    (each customer's Rubika accounts + worker affinity)
  * payments                    (verified TRC20 transactions; each tx hash ONCE)
  * customer_settings           (per-customer marker + send delay)
  * rate_limit                  (per-customer anti-flood window)
  * workers / worker_daily      (the reused worker subsystem stores its rows here,
                                 because worker.py needs workers AND account counts
                                 in one place; the customer bot has NO worker UI)
  * clock_state                 (anti-tamper server-time guard)

The OWNER panel (owner_bot.py) is the admin and may read/write all of this.
The CUSTOMER bot only ever calls the scoped helpers (always keyed by the
requesting telegram_id) and NEVER imports central_db.py — that is where the
owner-only "central panel" data lives.
"""
import os
import sqlite3
import time
from datetime import timedelta

import config

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "customer.db")


def _now() -> str:
    return config.now_str()


def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # timeout + busy_timeout: owner and customer processes share customer.db, so
    # wait for a held write lock instead of instantly raising "database is locked".
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init():
    conn = _conn()
    c = conn.cursor()

    # ---- customers / subscription ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            telegram_id INTEGER PRIMARY KEY,
            name        TEXT,
            username    TEXT,
            created_at  TEXT,
            expires_at  TEXT DEFAULT '',
            blocked     INTEGER DEFAULT 0,
            warned      INTEGER DEFAULT 0,
            total_paid  REAL DEFAULT 0,
            total_sends INTEGER DEFAULT 0,
            note        TEXT DEFAULT ''
        )
        """
    )

    # ---- a customer's Rubika accounts (unlimited count) ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            phone       TEXT UNIQUE,
            name        TEXT,
            user_id     TEXT,
            session     TEXT,
            added_at    TEXT,
            status      TEXT DEFAULT 'active',
            worker_id   INTEGER
        )
        """
    )

    # ---- verified payments (anti-fraud: tx_hash is UNIQUE = used once) ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            tx_hash     TEXT UNIQUE,
            plan        TEXT,
            amount      REAL,
            days        INTEGER,
            status      TEXT DEFAULT 'confirmed',
            created_at  TEXT
        )
        """
    )

    # ---- per-customer settings (marker + send delay) ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_settings (
            customer_id INTEGER PRIMARY KEY,
            marker      TEXT,
            send_delay  REAL
        )
        """
    )

    # ---- per-customer anti-flood window ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS rate_limit (
            customer_id  INTEGER PRIMARY KEY,
            window_start REAL DEFAULT 0,
            count        INTEGER DEFAULT 0
        )
        """
    )

    # ---- worker subsystem (reused worker.py stores rows here) ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS workers (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            tag          TEXT UNIQUE,
            ip           TEXT,
            ssh_port     INTEGER DEFAULT 22,
            ssh_user     TEXT,
            ssh_pass_enc TEXT,
            api_port     INTEGER,
            api_token_enc TEXT,
            is_master    INTEGER DEFAULT 0,
            enabled      INTEGER DEFAULT 1,
            status       TEXT DEFAULT 'unknown',
            ping_ms      INTEGER DEFAULT -1,
            file_ok      INTEGER DEFAULT 0,
            last_checked TEXT,
            created_at   TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS worker_daily (
            worker_id INTEGER,
            day       TEXT,
            sent      INTEGER DEFAULT 0,
            PRIMARY KEY (worker_id, day)
        )
        """
    )

    # ---- anti-tamper clock guard (single row) ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS clock_state (
            id        INTEGER PRIMARY KEY CHECK (id = 1),
            last_seen REAL DEFAULT 0
        )
        """
    )
    c.execute("INSERT OR IGNORE INTO clock_state (id, last_seen) VALUES (1, 0)")

    # ---- deposits table (TRX deposits with UNIQUE tx_hash) ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS deposits (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            tx_hash     TEXT UNIQUE,
            trx_amount  REAL,
            created_at  TEXT
        )
        """
    )

    # ---- plan_overrides table (per-plan price override by owner) ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS plan_overrides (
            plan_key TEXT PRIMARY KEY,
            price    REAL
        )
        """
    )

    # ---- settings table (key-value store for runtime configuration) ----
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    # ---- TELEGRAM section (independent of the Rubika `accounts` table) ----
    # Each customer's Telegram USER accounts (StringSession stored here).
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS tg_accounts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            phone       TEXT,
            name        TEXT,
            username    TEXT,
            user_id     TEXT,
            session     TEXT,
            label       TEXT DEFAULT '',
            added_at    TEXT,
            status      TEXT DEFAULT 'active'
        )
        """
    )
    # Per-customer Telegram content + send settings.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS tg_settings (
            customer_id  INTEGER PRIMARY KEY,
            content_type TEXT,
            content_text TEXT,
            media_path   TEXT,
            send_delay   REAL,
            target_mode  TEXT DEFAULT 'both',
            total_sends  INTEGER DEFAULT 0
        )
        """
    )

    # Each customer's Bale accounts (session stored as a .bale FILE on disk;
    # we keep only the file path here).
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS bale_accounts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id  INTEGER,
            phone        TEXT,
            name         TEXT,
            username     TEXT,
            user_id      TEXT,
            session_path TEXT,
            added_at     TEXT,
            status       TEXT DEFAULT 'active'
        )
        """
    )
    # Per-customer Bale content + send settings (default target = contacts).
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS bale_settings (
            customer_id  INTEGER PRIMARY KEY,
            content_type TEXT,
            content_text TEXT,
            media_path   TEXT,
            send_delay   REAL,
            target_mode  TEXT DEFAULT 'contacts',
            total_sends  INTEGER DEFAULT 0
        )
        """
    )

    # Forced-join channels: customers must be members before using the bot.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS forced_channels (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            chat      TEXT UNIQUE,
            title     TEXT,
            link      TEXT,
            enabled   INTEGER DEFAULT 1,
            added_at  TEXT
        )
        """
    )

    # ---- Add balance column to customers if not present ----
    try:
        c.execute("ALTER TABLE customers ADD COLUMN balance REAL DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # column already exists

    # ---- Add auto-upload file config to customer_settings if not present ----
    # When set, sends can forward THIS file (auto-uploaded to the account's Saved)
    # instead of the marked message. Configured once (like the marker), not per send.
    for _col in ("upload_path TEXT", "upload_name TEXT", "upload_caption TEXT"):
        try:
            c.execute(f"ALTER TABLE customer_settings ADD COLUMN {_col}")
        except sqlite3.OperationalError:
            pass  # column already exists

    # ---- owner->customer notification outbox ----
    # The owner bot can't DM a customer (separate token), so owner-side actions
    # (time change / block / unblock / broadcast) enqueue here and the CUSTOMER
    # bot delivers them through its own chat with the user.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            text        TEXT,
            sent        INTEGER DEFAULT 0,
            created_at  TEXT
        )
        """
    )

    # ---- group panel config (Config section): per-group install for a customer
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS group_config (
            group_id     INTEGER PRIMARY KEY,
            customer_id  INTEGER,
            admin_ids    TEXT DEFAULT '',
            content_type TEXT,
            content_text TEXT,
            media_path   TEXT,
            enabled      INTEGER DEFAULT 1,
            installed    INTEGER DEFAULT 0,
            last_send_at TEXT,
            created_at   TEXT
        )
        """
    )

    conn.commit()
    conn.close()


# =========================================================================== #
# Anti-tamper server clock.
# =========================================================================== #
def guarded_now() -> float:
    """Return a monotonic-guarded wall-clock epoch.

    Subscription math is anchored to ABSOLUTE expiry timestamps, so the only way
    to cheat is to rewind the server clock. We persist the highest epoch ever
    seen; if the wall clock jumps backwards past the tolerance we DO NOT let the
    rewind take effect — we keep returning the last seen time. Returns the value
    used. Callers that detect a rewind can log it via clock_tampered()."""
    wall = time.time()
    conn = _conn()
    row = conn.execute("SELECT last_seen FROM clock_state WHERE id = 1").fetchone()
    last = float(row["last_seen"]) if row else 0.0
    effective = wall
    if wall < last - config.CLOCK_BACKWARD_TOLERANCE:
        # suspicious rewind -> ignore it, hold the line at last_seen
        effective = last
    if effective > last:
        conn.execute("UPDATE clock_state SET last_seen = ? WHERE id = 1", (effective,))
        conn.commit()
    conn.close()
    return effective


def clock_tampered() -> bool:
    """True if the wall clock is currently behind the recorded last_seen by more
    than the tolerance (i.e. someone moved the clock backwards)."""
    wall = time.time()
    conn = _conn()
    row = conn.execute("SELECT last_seen FROM clock_state WHERE id = 1").fetchone()
    conn.close()
    last = float(row["last_seen"]) if row else 0.0
    return wall < last - config.CLOCK_BACKWARD_TOLERANCE


# =========================================================================== #
# Customers / subscription.
# =========================================================================== #
def ensure_customer(telegram_id: int, name: str = "", username: str = "") -> dict:
    """Create the customer row on first /start (idempotent); return it."""
    conn = _conn()
    conn.execute(
        "INSERT OR IGNORE INTO customers (telegram_id, name, username, created_at) "
        "VALUES (?, ?, ?, ?)",
        (int(telegram_id), name or "", username or "", _now()),
    )
    # keep name/username fresh
    conn.execute(
        "UPDATE customers SET name = ?, username = ? WHERE telegram_id = ?",
        (name or "", username or "", int(telegram_id)),
    )
    conn.commit()
    conn.close()
    return get_customer(telegram_id)


def get_customer(telegram_id: int):
    conn = _conn()
    row = conn.execute("SELECT * FROM customers WHERE telegram_id = ?",
                       (int(telegram_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def list_customers() -> list:
    conn = _conn()
    rows = conn.execute("SELECT * FROM customers ORDER BY created_at").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def search_customers(term: str) -> list:
    term = (term or "").strip()
    like = f"%{term}%"
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM customers WHERE CAST(telegram_id AS TEXT) LIKE ? "
        "OR name LIKE ? OR username LIKE ? ORDER BY created_at",
        (like, like, like),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _parse_expiry(expires_at: str) -> float:
    """Parse the stored expiry string into an epoch, or 0 if none/invalid."""
    if not expires_at:
        return 0.0
    from datetime import datetime
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(expires_at, fmt)
            return dt.timestamp()
        except ValueError:
            continue
    return 0.0


def seconds_left(telegram_id: int) -> float:
    """Seconds remaining on the subscription (<= 0 means expired)."""
    cust = get_customer(telegram_id)
    if not cust:
        return 0.0
    exp = _parse_expiry(cust.get("expires_at") or "")
    if exp <= 0:
        return 0.0
    return exp - guarded_now()


def days_left(telegram_id: int) -> int:
    """Whole days remaining (rounded up), 0 if expired."""
    import math
    sec = seconds_left(telegram_id)
    if sec <= 0:
        return 0
    return int(math.ceil(sec / 86400.0))


def is_active(telegram_id: int) -> bool:
    """True if the customer has time left AND is not blocked."""
    cust = get_customer(telegram_id)
    if not cust or cust.get("blocked"):
        return False
    return seconds_left(telegram_id) > 0


def is_blocked(telegram_id: int) -> bool:
    cust = get_customer(telegram_id)
    return bool(cust and cust.get("blocked"))


def add_days(telegram_id: int, days: int) -> str:
    """Extend (or reduce, if days<0) the subscription. If expired/empty, the new
    period starts from NOW; otherwise it stacks on the remaining time. Returns
    the new expiry string. Reset the 2-day warning flag on any extension."""
    from datetime import datetime
    ensure_customer(telegram_id)
    now_epoch = guarded_now()
    cur = _parse_expiry(get_customer(telegram_id).get("expires_at") or "")
    base = max(cur, now_epoch)
    new_epoch = base + int(days) * 86400
    if new_epoch < now_epoch:
        new_epoch = now_epoch  # never below "now" (an over-reduction = expire now)
    new_str = datetime.fromtimestamp(new_epoch).strftime("%Y-%m-%d %H:%M:%S")
    conn = _conn()
    conn.execute(
        "UPDATE customers SET expires_at = ?, warned = 0 WHERE telegram_id = ?",
        (new_str, int(telegram_id)),
    )
    conn.commit()
    conn.close()
    return new_str


def set_blocked(telegram_id: int, blocked: bool):
    ensure_customer(telegram_id)
    conn = _conn()
    conn.execute("UPDATE customers SET blocked = ? WHERE telegram_id = ?",
                 (1 if blocked else 0, int(telegram_id)))
    conn.commit()
    conn.close()


def set_warned(telegram_id: int, warned: bool):
    conn = _conn()
    conn.execute("UPDATE customers SET warned = ? WHERE telegram_id = ?",
                 (1 if warned else 0, int(telegram_id)))
    conn.commit()
    conn.close()


def set_note(telegram_id: int, note: str):
    ensure_customer(telegram_id)
    conn = _conn()
    conn.execute("UPDATE customers SET note = ? WHERE telegram_id = ?",
                 (note or "", int(telegram_id)))
    conn.commit()
    conn.close()


def incr_customer_sends(telegram_id: int, n: int = 1):
    conn = _conn()
    conn.execute("UPDATE customers SET total_sends = total_sends + ? WHERE telegram_id = ?",
                 (int(n), int(telegram_id)))
    conn.commit()
    conn.close()


def _incr_customer_paid(conn, telegram_id: int, amount: float):
    conn.execute("UPDATE customers SET total_paid = total_paid + ? WHERE telegram_id = ?",
                 (float(amount), int(telegram_id)))


# =========================================================================== #
# Payments (anti-fraud: each tx hash can be recorded ONCE).
# =========================================================================== #
def payment_exists(tx_hash: str) -> bool:
    conn = _conn()
    row = conn.execute("SELECT 1 FROM payments WHERE tx_hash = ?",
                       (str(tx_hash),)).fetchone()
    conn.close()
    return bool(row)


def record_payment(telegram_id: int, tx_hash: str, plan: str, amount: float,
                   days: int) -> bool:
    """Atomically record a verified payment, credit the days, and bump revenue.
    Returns False if the tx hash was already used (anti-fraud), True on success.
    """
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO payments (customer_id, tx_hash, plan, amount, days, "
            "status, created_at) VALUES (?, ?, ?, ?, ?, 'confirmed', ?)",
            (int(telegram_id), str(tx_hash), plan, float(amount), int(days), _now()),
        )
    except sqlite3.IntegrityError:
        conn.close()
        return False  # tx hash already used -> reject (anti-fraud)
    _incr_customer_paid(conn, telegram_id, amount)
    conn.commit()
    conn.close()
    # credit the subscription time AFTER the unique insert succeeded
    add_days(telegram_id, days)
    return True


def list_payments(telegram_id: int = None) -> list:
    conn = _conn()
    if telegram_id is None:
        rows = conn.execute("SELECT * FROM payments ORDER BY id DESC").fetchall()
    else:
        rows = conn.execute("SELECT * FROM payments WHERE customer_id = ? "
                            "ORDER BY id DESC", (int(telegram_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def total_revenue() -> float:
    conn = _conn()
    row = conn.execute("SELECT COALESCE(SUM(amount), 0) AS s FROM payments").fetchone()
    conn.close()
    return float(row["s"]) if row else 0.0


# =========================================================================== #
# Accounts (per customer; unlimited).
# =========================================================================== #
def add_account(customer_id: int, phone: str, name: str, user_id: str,
                session: str = "") -> int:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO accounts (customer_id, phone, name, user_id, session, added_at, status)
        VALUES (?, ?, ?, ?, ?, ?, 'active')
        ON CONFLICT(phone) DO UPDATE SET
            customer_id=excluded.customer_id,
            name=excluded.name,
            user_id=excluded.user_id,
            session=excluded.session,
            status='active'
        """,
        (int(customer_id), phone, name, user_id, session, _now()),
    )
    conn.commit()
    row = c.execute("SELECT id FROM accounts WHERE phone = ?", (phone,)).fetchone()
    conn.close()
    return row["id"]


def list_accounts(customer_id: int = None) -> list:
    conn = _conn()
    if customer_id is None:
        rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
    else:
        rows = conn.execute("SELECT * FROM accounts WHERE customer_id = ? ORDER BY id",
                            (int(customer_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_account(account_id: int):
    conn = _conn()
    row = conn.execute("SELECT * FROM accounts WHERE id = ?", (int(account_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_account_owned(account_id: int, customer_id: int):
    """Fetch an account only if it belongs to this customer (scoping guard)."""
    conn = _conn()
    row = conn.execute("SELECT * FROM accounts WHERE id = ? AND customer_id = ?",
                       (int(account_id), int(customer_id))).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_account(account_id: int):
    conn = _conn()
    conn.execute("DELETE FROM accounts WHERE id = ?", (int(account_id),))
    conn.commit()
    conn.close()


def set_status(account_id: int, status: str):
    conn = _conn()
    conn.execute("UPDATE accounts SET status = ? WHERE id = ?", (status, int(account_id)))
    conn.commit()
    conn.close()


def count_accounts() -> int:
    conn = _conn()
    row = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()
    conn.close()
    return int(row["n"]) if row else 0


def count_customer_accounts(customer_id: int) -> int:
    conn = _conn()
    row = conn.execute("SELECT COUNT(*) AS n FROM accounts WHERE customer_id = ?",
                       (int(customer_id),)).fetchone()
    conn.close()
    return int(row["n"]) if row else 0


def set_account_worker(account_id: int, worker_id):
    conn = _conn()
    conn.execute("UPDATE accounts SET worker_id = ? WHERE id = ?",
                 (worker_id, int(account_id)))
    conn.commit()
    conn.close()


# =========================================================================== #
# Per-customer settings (marker + send delay).
# =========================================================================== #
def _ensure_settings(c, customer_id: int):
    c.execute(
        "INSERT OR IGNORE INTO customer_settings (customer_id, marker, send_delay) "
        "VALUES (?, ?, ?)",
        (int(customer_id), config.FORWARD_MARKER, config.DEFAULT_DELAY),
    )


def get_settings(customer_id: int) -> dict:
    conn = _conn()
    c = conn.cursor()
    _ensure_settings(c, customer_id)
    conn.commit()
    row = c.execute("SELECT * FROM customer_settings WHERE customer_id = ?",
                    (int(customer_id),)).fetchone()
    conn.close()
    return dict(row) if row else {"marker": config.FORWARD_MARKER,
                                  "send_delay": config.DEFAULT_DELAY}


def get_marker(customer_id: int) -> str:
    return (get_settings(customer_id).get("marker") or config.FORWARD_MARKER).strip()


def set_marker(customer_id: int, marker: str):
    conn = _conn()
    c = conn.cursor()
    _ensure_settings(c, customer_id)
    c.execute("UPDATE customer_settings SET marker = ? WHERE customer_id = ?",
              (marker.strip(), int(customer_id)))
    conn.commit()
    conn.close()


def get_upload_file(customer_id: int):
    """Return the customer's configured auto-upload file as {path, name, caption},
    or None if none is set (or the stored file no longer exists on disk)."""
    s = get_settings(customer_id)
    path = (s.get("upload_path") or "").strip()
    if not path:
        return None
    import os as _os
    if not _os.path.exists(path):
        return None
    return {"path": path, "name": (s.get("upload_name") or _os.path.basename(path)),
            "caption": (s.get("upload_caption") or "")}


def set_upload_file(customer_id: int, path: str, name: str, caption: str = ""):
    conn = _conn()
    c = conn.cursor()
    _ensure_settings(c, customer_id)
    c.execute("UPDATE customer_settings SET upload_path = ?, upload_name = ?, "
              "upload_caption = ? WHERE customer_id = ?",
              (path, name, caption or "", int(customer_id)))
    conn.commit()
    conn.close()


def clear_upload_file(customer_id: int):
    conn = _conn()
    c = conn.cursor()
    _ensure_settings(c, customer_id)
    c.execute("UPDATE customer_settings SET upload_path = NULL, upload_name = NULL, "
              "upload_caption = NULL WHERE customer_id = ?", (int(customer_id),))
    conn.commit()
    conn.close()


def get_delay(customer_id: int) -> float:
    return config.clamp_delay(get_settings(customer_id).get("send_delay"))


def set_delay(customer_id: int, value: float):
    conn = _conn()
    c = conn.cursor()
    _ensure_settings(c, customer_id)
    c.execute("UPDATE customer_settings SET send_delay = ? WHERE customer_id = ?",
              (config.clamp_delay(value), int(customer_id)))
    conn.commit()
    conn.close()


# =========================================================================== #
# Rate-limit (anti-flood). Returns (allowed, count_in_window).
# =========================================================================== #
def rate_hit(customer_id: int) -> tuple:
    """Record one action; return (allowed, count). allowed=False means the
    customer exceeded RATE_LIMIT_MAX within RATE_LIMIT_WINDOW seconds."""
    now = time.time()
    conn = _conn()
    row = conn.execute("SELECT window_start, count FROM rate_limit WHERE customer_id = ?",
                       (int(customer_id),)).fetchone()
    if not row:
        conn.execute("INSERT INTO rate_limit (customer_id, window_start, count) "
                     "VALUES (?, ?, 1)", (int(customer_id), now))
        conn.commit()
        conn.close()
        return True, 1
    win = float(row["window_start"])
    cnt = int(row["count"])
    if now - win > config.RATE_LIMIT_WINDOW:
        # window expired -> reset
        conn.execute("UPDATE rate_limit SET window_start = ?, count = 1 "
                     "WHERE customer_id = ?", (now, int(customer_id)))
        conn.commit()
        conn.close()
        return True, 1
    cnt += 1
    conn.execute("UPDATE rate_limit SET count = ? WHERE customer_id = ?",
                 (cnt, int(customer_id)))
    conn.commit()
    conn.close()
    return (cnt <= config.RATE_LIMIT_MAX), cnt


def rate_reset(customer_id: int):
    conn = _conn()
    conn.execute("DELETE FROM rate_limit WHERE customer_id = ?", (int(customer_id),))
    conn.commit()
    conn.close()


# =========================================================================== #
# Balance management.
# =========================================================================== #
def get_balance(telegram_id: int) -> float:
    """Return the customer's current TRX balance."""
    conn = _conn()
    row = conn.execute("SELECT balance FROM customers WHERE telegram_id = ?",
                       (int(telegram_id),)).fetchone()
    conn.close()
    return float(row["balance"]) if row else 0.0


def add_balance(telegram_id: int, amount: float):
    """Add amount to the customer's balance."""
    ensure_customer(telegram_id)
    conn = _conn()
    conn.execute("UPDATE customers SET balance = balance + ? WHERE telegram_id = ?",
                 (float(amount), int(telegram_id)))
    conn.commit()
    conn.close()


def deduct_balance(telegram_id: int, amount: float) -> bool:
    """Atomically deduct amount from the customer's balance.

    Uses a single UPDATE with a WHERE balance >= ? guard to prevent TOCTOU race
    conditions.  Returns False if balance is insufficient or user not found.
    """
    conn = _conn()
    cur = conn.execute(
        "UPDATE customers SET balance = balance - ? "
        "WHERE telegram_id = ? AND balance >= ?",
        (float(amount), int(telegram_id), float(amount)),
    )
    conn.commit()
    success = cur.rowcount == 1
    conn.close()
    return success


# =========================================================================== #
# Deposits (TRX deposits with unique tx_hash).
# =========================================================================== #
def deposit_exists(tx_hash: str) -> bool:
    """Check if a deposit with this tx_hash already exists."""
    conn = _conn()
    row = conn.execute("SELECT 1 FROM deposits WHERE tx_hash = ?",
                       (str(tx_hash),)).fetchone()
    conn.close()
    return bool(row)


def record_deposit(telegram_id: int, tx_hash: str, trx_amount: float) -> bool:
    """Record a TRX deposit. Returns False if tx_hash already used (anti-fraud)."""
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO deposits (customer_id, tx_hash, trx_amount, created_at) "
            "VALUES (?, ?, ?, ?)",
            (int(telegram_id), str(tx_hash), float(trx_amount), _now()),
        )
    except sqlite3.IntegrityError:
        conn.close()
        return False
    conn.commit()
    conn.close()
    return True


def list_deposits(telegram_id: int) -> list:
    """List all deposits for a customer."""
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM deposits WHERE customer_id = ? ORDER BY id DESC",
        (int(telegram_id),)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# =========================================================================== #
# Revenue helpers (TRX, based on deposits table).
# =========================================================================== #
def today_revenue_trx() -> float:
    """Total TRX revenue from deposits today."""
    conn = _conn()
    today = config.now_dt().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COALESCE(SUM(trx_amount), 0) AS s FROM deposits "
        "WHERE created_at LIKE ?", (f"{today}%",)
    ).fetchone()
    conn.close()
    return float(row["s"]) if row else 0.0


def week_revenue_trx() -> float:
    """Total TRX revenue from deposits in the last 7 days."""
    conn = _conn()
    from datetime import datetime, timedelta as td
    week_ago = (config.now_dt() - td(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    row = conn.execute(
        "SELECT COALESCE(SUM(trx_amount), 0) AS s FROM deposits "
        "WHERE created_at >= ?", (week_ago,)
    ).fetchone()
    conn.close()
    return float(row["s"]) if row else 0.0


def month_revenue_trx() -> float:
    """Total TRX revenue from deposits in the last 30 days."""
    conn = _conn()
    from datetime import datetime, timedelta as td
    month_ago = (config.now_dt() - td(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    row = conn.execute(
        "SELECT COALESCE(SUM(trx_amount), 0) AS s FROM deposits "
        "WHERE created_at >= ?", (month_ago,)
    ).fetchone()
    conn.close()
    return float(row["s"]) if row else 0.0


# =========================================================================== #
# Plan overrides (owner can change plan prices at runtime).
# =========================================================================== #
def get_plan_price(plan_key: str) -> float | None:
    """Get the overridden price for a plan, or None if not overridden."""
    conn = _conn()
    row = conn.execute("SELECT price FROM plan_overrides WHERE plan_key = ?",
                       (plan_key,)).fetchone()
    conn.close()
    return float(row["price"]) if row else None


def set_plan_price(plan_key: str, price: float):
    """Set or update a plan price override."""
    conn = _conn()
    conn.execute(
        "INSERT INTO plan_overrides (plan_key, price) VALUES (?, ?) "
        "ON CONFLICT(plan_key) DO UPDATE SET price = ?",
        (plan_key, float(price), float(price)),
    )
    conn.commit()
    conn.close()


# =========================================================================== #
# Settings (generic key-value store for runtime configuration).
# =========================================================================== #
def get_setting(key: str, default: str = "") -> str:
    """Get a setting value by key, or return default if not found."""
    conn = _conn()
    row = conn.execute("SELECT value FROM settings WHERE key = ?",
                       (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key: str, value: str):
    """Set or update a setting."""
    conn = _conn()
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = ?",
        (key, str(value), str(value)),
    )
    conn.commit()
    conn.close()


# =========================================================================== #
# Stats helpers for the owner dashboard.
# =========================================================================== #
def stats() -> dict:
    conn = _conn()
    customers = conn.execute("SELECT COUNT(*) AS n FROM customers").fetchone()["n"]
    accounts = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]
    active_acc = conn.execute(
        "SELECT COUNT(*) AS n FROM accounts WHERE status = 'active'").fetchone()["n"]
    sends = conn.execute(
        "SELECT COALESCE(SUM(total_sends), 0) AS n FROM customers").fetchone()["n"]
    revenue = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) AS s FROM payments").fetchone()["s"]
    blocked = conn.execute(
        "SELECT COUNT(*) AS n FROM customers WHERE blocked = 1").fetchone()["n"]
    conn.close()
    active_subs = sum(1 for cu in list_customers() if is_active(cu["telegram_id"]))
    return {
        "customers": int(customers),
        "active_subs": int(active_subs),
        "blocked": int(blocked),
        "accounts": int(accounts),
        "active_accounts": int(active_acc),
        "sends": int(sends),
        "revenue": float(revenue),
    }


# =========================================================================== #
# Workers (used by the reused worker.py / worker subsystem).
# =========================================================================== #
def add_worker(tag: str, ip: str, ssh_port: int, ssh_user: str,
               ssh_pass_enc: str, api_port: int, api_token_enc: str,
               is_master: int = 0) -> int:
    conn = _conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO workers (tag, ip, ssh_port, ssh_user, ssh_pass_enc,
                             api_port, api_token_enc, is_master, enabled,
                             status, ping_ms, file_ok, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'unknown', -1, 0, ?)
        """,
        (tag, ip, int(ssh_port or 22), ssh_user, ssh_pass_enc,
         int(api_port), api_token_enc, int(is_master), _now()),
    )
    conn.commit()
    wid = c.lastrowid
    conn.close()
    return wid


def list_workers() -> list:
    conn = _conn()
    rows = conn.execute("SELECT * FROM workers ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def list_enabled_workers() -> list:
    conn = _conn()
    rows = conn.execute("SELECT * FROM workers WHERE enabled = 1 ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_worker(worker_id: int):
    conn = _conn()
    row = conn.execute("SELECT * FROM workers WHERE id = ?", (int(worker_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_worker_by_tag(tag: str):
    conn = _conn()
    row = conn.execute("SELECT * FROM workers WHERE tag = ?", (tag,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_master_worker():
    conn = _conn()
    row = conn.execute("SELECT * FROM workers WHERE is_master = 1 LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else None


def delete_worker(worker_id: int):
    conn = _conn()
    conn.execute("DELETE FROM workers WHERE id = ?", (int(worker_id),))
    conn.execute("DELETE FROM worker_daily WHERE worker_id = ?", (int(worker_id),))
    conn.execute("UPDATE accounts SET worker_id = NULL WHERE worker_id = ?",
                 (int(worker_id),))
    conn.commit()
    conn.close()


def set_worker_enabled(worker_id: int, enabled: bool):
    conn = _conn()
    conn.execute("UPDATE workers SET enabled = ? WHERE id = ?",
                 (1 if enabled else 0, int(worker_id)))
    conn.commit()
    conn.close()


def update_worker_health(worker_id: int, status: str, ping_ms: int, file_ok: bool):
    conn = _conn()
    conn.execute(
        "UPDATE workers SET status = ?, ping_ms = ?, file_ok = ?, last_checked = ? "
        "WHERE id = ?",
        (status, int(ping_ms), 1 if file_ok else 0, _now(), int(worker_id)),
    )
    conn.commit()
    conn.close()


def count_accounts_on_worker(worker_id: int) -> int:
    conn = _conn()
    row = conn.execute("SELECT COUNT(*) AS n FROM accounts WHERE worker_id = ?",
                       (int(worker_id),)).fetchone()
    conn.close()
    return int(row["n"]) if row else 0


def _today() -> str:
    return config.now_dt().strftime("%Y-%m-%d")


def incr_worker_sent(worker_id: int, n: int = 1):
    conn = _conn()
    day = _today()
    conn.execute(
        "INSERT INTO worker_daily (worker_id, day, sent) VALUES (?, ?, ?) "
        "ON CONFLICT(worker_id, day) DO UPDATE SET sent = sent + ?",
        (int(worker_id), day, int(n), int(n)),
    )
    conn.commit()
    conn.close()


def worker_sent_today(worker_id: int) -> int:
    conn = _conn()
    row = conn.execute("SELECT sent FROM worker_daily WHERE worker_id = ? AND day = ?",
                       (int(worker_id), _today())).fetchone()
    conn.close()
    return int(row["sent"]) if row else 0


# =========================================================================== #
# Maintenance flag (read-only mirror).
# =========================================================================== #
def maintenance_on() -> bool:
    """Read the maintenance flag from the shared flag file. The owner panel
    writes this file (via central_db.set_maintenance); the customer bot reads it
    here so it never needs to touch the owner-only central database."""
    path = os.path.join(os.path.dirname(DB_PATH), "maintenance.flag")
    return os.path.exists(path)



# =========================================================================== #
# TELEGRAM section helpers (separate tables; fully decoupled from Rubika).
# =========================================================================== #
def add_tg_account(customer_id: int, phone: str, name: str, username: str,
                   user_id: str, session: str) -> int:
    """Insert or update (per customer + phone) a Telegram user account."""
    conn = _conn()
    c = conn.cursor()
    row = c.execute(
        "SELECT id FROM tg_accounts WHERE customer_id = ? AND phone = ?",
        (int(customer_id), phone),
    ).fetchone()
    if row:
        c.execute(
            "UPDATE tg_accounts SET name = ?, username = ?, user_id = ?, "
            "session = ?, status = 'active' WHERE id = ?",
            (name, username, str(user_id), session, int(row["id"])),
        )
        aid = int(row["id"])
    else:
        c.execute(
            "INSERT INTO tg_accounts (customer_id, phone, name, username, "
            "user_id, session, added_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'active')",
            (int(customer_id), phone, name, username, str(user_id), session, _now()),
        )
        aid = int(c.lastrowid)
    conn.commit()
    conn.close()
    return aid


def list_tg_accounts(customer_id: int = None) -> list:
    conn = _conn()
    if customer_id is None:
        rows = conn.execute("SELECT * FROM tg_accounts ORDER BY id").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM tg_accounts WHERE customer_id = ? ORDER BY id",
            (int(customer_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_tg_account(account_id: int):
    conn = _conn()
    row = conn.execute("SELECT * FROM tg_accounts WHERE id = ?",
                       (int(account_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_tg_account_owned(account_id: int, customer_id: int):
    conn = _conn()
    row = conn.execute(
        "SELECT * FROM tg_accounts WHERE id = ? AND customer_id = ?",
        (int(account_id), int(customer_id))).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_tg_account(account_id: int):
    conn = _conn()
    conn.execute("DELETE FROM tg_accounts WHERE id = ?", (int(account_id),))
    conn.commit()
    conn.close()


def set_tg_status(account_id: int, status: str):
    conn = _conn()
    conn.execute("UPDATE tg_accounts SET status = ? WHERE id = ?",
                 (status, int(account_id)))
    conn.commit()
    conn.close()


def set_tg_label(account_id: int, label: str):
    conn = _conn()
    conn.execute("UPDATE tg_accounts SET label = ? WHERE id = ?",
                 ((label or "").strip()[:40], int(account_id)))
    conn.commit()
    conn.close()


def count_tg_accounts() -> int:
    conn = _conn()
    row = conn.execute("SELECT COUNT(*) AS n FROM tg_accounts").fetchone()
    conn.close()
    return int(row["n"]) if row else 0


def count_customer_tg_accounts(customer_id: int) -> int:
    conn = _conn()
    row = conn.execute("SELECT COUNT(*) AS n FROM tg_accounts WHERE customer_id = ?",
                       (int(customer_id),)).fetchone()
    conn.close()
    return int(row["n"]) if row else 0


def _ensure_tg_settings(c, customer_id: int):
    c.execute(
        "INSERT OR IGNORE INTO tg_settings (customer_id, content_type, "
        "content_text, media_path, send_delay, target_mode, total_sends) "
        "VALUES (?, NULL, NULL, NULL, ?, 'both', 0)",
        (int(customer_id), config.TG_SEND_DELAY),
    )


def get_tg_settings(customer_id: int) -> dict:
    conn = _conn()
    c = conn.cursor()
    _ensure_tg_settings(c, customer_id)
    conn.commit()
    row = c.execute("SELECT * FROM tg_settings WHERE customer_id = ?",
                    (int(customer_id),)).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {"customer_id": int(customer_id), "content_type": None,
            "content_text": None, "media_path": None,
            "send_delay": config.TG_SEND_DELAY, "target_mode": "both",
            "total_sends": 0}


def set_tg_content(customer_id: int, content_type, content_text, media_path):
    conn = _conn()
    c = conn.cursor()
    _ensure_tg_settings(c, customer_id)
    c.execute(
        "UPDATE tg_settings SET content_type = ?, content_text = ?, "
        "media_path = ? WHERE customer_id = ?",
        (content_type, content_text, media_path, int(customer_id)),
    )
    conn.commit()
    conn.close()


def get_tg_delay(customer_id: int) -> float:
    return config.clamp_tg_delay(get_tg_settings(customer_id).get("send_delay"))


def set_tg_delay(customer_id: int, value: float):
    conn = _conn()
    c = conn.cursor()
    _ensure_tg_settings(c, customer_id)
    c.execute("UPDATE tg_settings SET send_delay = ? WHERE customer_id = ?",
              (config.clamp_tg_delay(value), int(customer_id)))
    conn.commit()
    conn.close()


def set_tg_target_mode(customer_id: int, mode: str):
    if mode not in ("both", "contacts", "groups"):
        mode = "both"
    conn = _conn()
    c = conn.cursor()
    _ensure_tg_settings(c, customer_id)
    c.execute("UPDATE tg_settings SET target_mode = ? WHERE customer_id = ?",
              (mode, int(customer_id)))
    conn.commit()
    conn.close()


def incr_tg_sends(customer_id: int, n: int = 1):
    conn = _conn()
    c = conn.cursor()
    _ensure_tg_settings(c, customer_id)
    c.execute("UPDATE tg_settings SET total_sends = total_sends + ? "
              "WHERE customer_id = ?", (int(n), int(customer_id)))
    conn.commit()
    conn.close()



# =========================================================================== #
# BALE section helpers (separate tables; fully decoupled from Rubika/Telegram).
# =========================================================================== #
def add_bale_account(customer_id: int, phone: str, name: str, username: str,
                     user_id: str, session_path: str) -> int:
    """Insert or update (per customer + phone) a Bale account."""
    conn = _conn()
    c = conn.cursor()
    row = c.execute(
        "SELECT id FROM bale_accounts WHERE customer_id = ? AND phone = ?",
        (int(customer_id), phone),
    ).fetchone()
    if row:
        c.execute(
            "UPDATE bale_accounts SET name = ?, username = ?, user_id = ?, "
            "session_path = ?, status = 'active' WHERE id = ?",
            (name, username, str(user_id), session_path, int(row["id"])),
        )
        aid = int(row["id"])
    else:
        c.execute(
            "INSERT INTO bale_accounts (customer_id, phone, name, username, "
            "user_id, session_path, added_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'active')",
            (int(customer_id), phone, name, username, str(user_id),
             session_path, _now()),
        )
        aid = int(c.lastrowid)
    conn.commit()
    conn.close()
    return aid


def list_bale_accounts(customer_id: int = None) -> list:
    conn = _conn()
    if customer_id is None:
        rows = conn.execute("SELECT * FROM bale_accounts ORDER BY id").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM bale_accounts WHERE customer_id = ? ORDER BY id",
            (int(customer_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_bale_account(account_id: int):
    conn = _conn()
    row = conn.execute("SELECT * FROM bale_accounts WHERE id = ?",
                       (int(account_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_bale_account_owned(account_id: int, customer_id: int):
    conn = _conn()
    row = conn.execute(
        "SELECT * FROM bale_accounts WHERE id = ? AND customer_id = ?",
        (int(account_id), int(customer_id))).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_bale_account(account_id: int):
    conn = _conn()
    conn.execute("DELETE FROM bale_accounts WHERE id = ?", (int(account_id),))
    conn.commit()
    conn.close()


def set_bale_status(account_id: int, status: str):
    conn = _conn()
    conn.execute("UPDATE bale_accounts SET status = ? WHERE id = ?",
                 (status, int(account_id)))
    conn.commit()
    conn.close()


def count_customer_bale_accounts(customer_id: int) -> int:
    conn = _conn()
    row = conn.execute("SELECT COUNT(*) AS n FROM bale_accounts WHERE customer_id = ?",
                       (int(customer_id),)).fetchone()
    conn.close()
    return int(row["n"]) if row else 0


def _ensure_bale_settings(c, customer_id: int):
    c.execute(
        "INSERT OR IGNORE INTO bale_settings (customer_id, content_type, "
        "content_text, media_path, send_delay, target_mode, total_sends) "
        "VALUES (?, NULL, NULL, NULL, ?, 'contacts', 0)",
        (int(customer_id), config.BALE_SEND_DELAY),
    )


def get_bale_settings(customer_id: int) -> dict:
    conn = _conn()
    c = conn.cursor()
    _ensure_bale_settings(c, customer_id)
    conn.commit()
    row = c.execute("SELECT * FROM bale_settings WHERE customer_id = ?",
                    (int(customer_id),)).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {"customer_id": int(customer_id), "content_type": None,
            "content_text": None, "media_path": None,
            "send_delay": config.BALE_SEND_DELAY, "target_mode": "contacts",
            "total_sends": 0}


def set_bale_content(customer_id: int, content_type, content_text, media_path):
    conn = _conn()
    c = conn.cursor()
    _ensure_bale_settings(c, customer_id)
    c.execute(
        "UPDATE bale_settings SET content_type = ?, content_text = ?, "
        "media_path = ? WHERE customer_id = ?",
        (content_type, content_text, media_path, int(customer_id)),
    )
    conn.commit()
    conn.close()


def get_bale_delay(customer_id: int) -> float:
    return config.clamp_bale_delay(get_bale_settings(customer_id).get("send_delay"))


def set_bale_delay(customer_id: int, value: float):
    conn = _conn()
    c = conn.cursor()
    _ensure_bale_settings(c, customer_id)
    c.execute("UPDATE bale_settings SET send_delay = ? WHERE customer_id = ?",
              (config.clamp_bale_delay(value), int(customer_id)))
    conn.commit()
    conn.close()


def set_bale_target_mode(customer_id: int, mode: str):
    if mode not in ("contacts", "pv", "groups", "all"):
        mode = "contacts"
    conn = _conn()
    c = conn.cursor()
    _ensure_bale_settings(c, customer_id)
    c.execute("UPDATE bale_settings SET target_mode = ? WHERE customer_id = ?",
              (mode, int(customer_id)))
    conn.commit()
    conn.close()


def incr_bale_sends(customer_id: int, n: int = 1):
    conn = _conn()
    c = conn.cursor()
    _ensure_bale_settings(c, customer_id)
    c.execute("UPDATE bale_settings SET total_sends = total_sends + ? "
              "WHERE customer_id = ?", (int(n), int(customer_id)))
    conn.commit()
    conn.close()



# =========================================================================== #
# Forced-join channels (customers must join before using the bot).
# =========================================================================== #
def add_forced_channel(chat: str, title: str = "", link: str = "") -> bool:
    """Add a required channel (chat = @username). Returns False if it already
    exists."""
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO forced_channels (chat, title, link, enabled, added_at) "
            "VALUES (?, ?, ?, 1, ?)",
            (chat, title or chat, link, _now()),
        )
        conn.commit()
        ok = True
    except sqlite3.IntegrityError:
        ok = False
    conn.close()
    return ok


def list_forced_channels(only_enabled: bool = False) -> list:
    conn = _conn()
    if only_enabled:
        rows = conn.execute(
            "SELECT * FROM forced_channels WHERE enabled = 1 ORDER BY id").fetchall()
    else:
        rows = conn.execute("SELECT * FROM forced_channels ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_forced_channel(channel_id: int):
    conn = _conn()
    row = conn.execute("SELECT * FROM forced_channels WHERE id = ?",
                       (int(channel_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_forced_channel(channel_id: int):
    conn = _conn()
    conn.execute("DELETE FROM forced_channels WHERE id = ?", (int(channel_id),))
    conn.commit()
    conn.close()


def set_forced_channel_enabled(channel_id: int, enabled: bool):
    conn = _conn()
    conn.execute("UPDATE forced_channels SET enabled = ? WHERE id = ?",
                 (1 if enabled else 0, int(channel_id)))
    conn.commit()
    conn.close()



# =========================================================================== #
# Owner -> customer notification outbox (delivered by the customer bot).
# =========================================================================== #
def enqueue_notification(customer_id: int, text: str):
    """Queue a message for a customer. The owner bot calls this; the customer
    bot's notification loop delivers it (the owner bot itself can't DM the user)."""
    conn = _conn()
    conn.execute(
        "INSERT INTO notifications (customer_id, text, sent, created_at) "
        "VALUES (?, ?, 0, ?)",
        (int(customer_id), str(text), config.now_str()))
    conn.commit()
    conn.close()


def fetch_unsent_notifications(limit: int = 50):
    """Return up to `limit` undelivered notifications (oldest first)."""
    conn = _conn()
    rows = conn.execute(
        "SELECT id, customer_id, text FROM notifications "
        "WHERE sent = 0 ORDER BY id ASC LIMIT ?", (int(limit),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_notification_sent(notif_id: int):
    conn = _conn()
    conn.execute("UPDATE notifications SET sent = 1 WHERE id = ?", (int(notif_id),))
    conn.commit()
    conn.close()



# =========================================================================== #
# Group panel config (Config section).
# =========================================================================== #
def get_group_config(group_id: int):
    conn = _conn()
    row = conn.execute("SELECT * FROM group_config WHERE group_id = ?",
                       (int(group_id),)).fetchone()
    conn.close()
    return dict(row) if row else None


def list_group_configs(customer_id: int) -> list:
    conn = _conn()
    rows = conn.execute("SELECT * FROM group_config WHERE customer_id = ? "
                        "ORDER BY group_id", (int(customer_id),)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def count_group_configs(customer_id: int) -> int:
    conn = _conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM group_config WHERE customer_id = ?",
                     (int(customer_id),)).fetchone()["n"]
    conn.close()
    return int(n)


def upsert_group_config(group_id: int, customer_id: int):
    """Create the row for a group owned by this customer (no-op if exists with a
    DIFFERENT owner — caller must check ownership first)."""
    conn = _conn()
    conn.execute(
        "INSERT INTO group_config (group_id, customer_id, created_at) "
        "VALUES (?, ?, ?) ON CONFLICT(group_id) DO NOTHING",
        (int(group_id), int(customer_id), config.now_str()))
    conn.commit()
    conn.close()


def set_group_admins(group_id: int, admin_ids: str):
    conn = _conn()
    conn.execute("UPDATE group_config SET admin_ids = ? WHERE group_id = ?",
                 (str(admin_ids), int(group_id)))
    conn.commit()
    conn.close()


def set_group_content(group_id: int, ctype, text, media_path):
    conn = _conn()
    conn.execute("UPDATE group_config SET content_type = ?, content_text = ?, "
                 "media_path = ? WHERE group_id = ?",
                 (ctype, text, media_path, int(group_id)))
    conn.commit()
    conn.close()


def set_group_enabled(group_id: int, enabled: bool):
    conn = _conn()
    conn.execute("UPDATE group_config SET enabled = ? WHERE group_id = ?",
                 (1 if enabled else 0, int(group_id)))
    conn.commit()
    conn.close()


def set_group_installed(group_id: int, installed: bool):
    conn = _conn()
    conn.execute("UPDATE group_config SET installed = ? WHERE group_id = ?",
                 (1 if installed else 0, int(group_id)))
    conn.commit()
    conn.close()


def touch_group_send(group_id: int):
    conn = _conn()
    conn.execute("UPDATE group_config SET last_send_at = ? WHERE group_id = ?",
                 (config.now_str(), int(group_id)))
    conn.commit()
    conn.close()


def delete_group_config(group_id: int):
    conn = _conn()
    conn.execute("DELETE FROM group_config WHERE group_id = ?", (int(group_id),))
    conn.commit()
    conn.close()


def group_admin_ids(cfg: dict) -> set:
    """Parse the comma-separated admin_ids of a group_config row into a set of int."""
    out = set()
    for part in str((cfg or {}).get("admin_ids") or "").replace(" ", "").split(","):
        if part.lstrip("-").isdigit():
            out.add(int(part))
    return out
