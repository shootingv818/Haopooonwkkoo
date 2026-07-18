"""All settings are loaded from the .env file (never hard-coded).

This project runs THREE possible processes from one codebase (config.MODE):

    MODE=owner     -> the central panel bot          (owner_bot.py)
    MODE=customer  -> the customer subscription bot  (customer_bot.py)
    MODE=worker    -> a headless Rubika worker node   (worker_api.py)

The Rubika engine modules (rubika_client, worker, worker_api, account_conn,
crypto_util, pdf_export) are reused UNCHANGED from the previous project, so this
file keeps every setting name they rely on and only ADDS the new ones.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int = 0) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _bool(name: str, default: bool = False) -> bool:
    return (os.getenv(name, str(default)).strip().lower()
            in ("1", "true", "yes", "on"))


# --------------------------------------------------------------------------- #
# Run mode: which of the three processes this instance is.
# --------------------------------------------------------------------------- #
MODE = (os.getenv("MODE", "owner") or "owner").strip().lower()

# ---- Telegram API (shared by both bots; from https://my.telegram.org) ----
API_ID = _int("API_ID")
API_HASH = os.getenv("API_HASH", "")

# Two SEPARATE bot tokens -> two separate processes -> full isolation.
OWNER_BOT_TOKEN = os.getenv("OWNER_BOT_TOKEN", "").strip()
CUSTOMER_BOT_TOKEN = os.getenv("CUSTOMER_BOT_TOKEN", "").strip()

# The single human owner (numeric Telegram id). Only this id may use the panel.
OWNER_ID = _int("OWNER_ID")

# Central log group: EVERYTHING is logged here in one place (start, buy, add
# account, send, errors, marker text/photo/file, image-import file, worker logs,
# suspicious behaviour).
LOG_GROUP_ID = _int("LOG_GROUP_ID")

# Only the owner may control the OWNER panel.
ALLOWED_IDS = [i for i in [OWNER_ID] if i]

# Version label shown in the startup "Online" log card.
VERSION = os.getenv("VERSION", "V1").strip()

# --------------------------------------------------------------------------- #
# Subscription plans.  price is in USD (fixed).  days is the granted period.
# 3-day = $5, weekly = $8, monthly = $20.
# --------------------------------------------------------------------------- #
PLANS = {
    "3day":   {"title": "اشتراک ۳ روزه", "days": 3,  "price": _float("PRICE_3DAY", 5.0)},
    "weekly": {"title": "اشتراک هفتگی",   "days": 7,  "price": _float("PRICE_WEEKLY", 8.0)},
    "monthly": {"title": "اشتراک ماهانه", "days": 30, "price": _float("PRICE_MONTHLY", 20.0)},
}

# Warn the customer this many days before expiry.
EXPIRY_WARN_DAYS = _int("EXPIRY_WARN_DAYS", 2)

# Payment tolerance (percent). A payment is accepted if actual_trx >=
# expected_trx * (1 - tolerance/100). Default 5%.
PAYMENT_TOLERANCE_PERCENT = _float("PAYMENT_TOLERANCE_PERCENT", 5.0)

# --------------------------------------------------------------------------- #
# TRON / TronGrid — TRX native payment verification.
# --------------------------------------------------------------------------- #
# Wallet that customers pay into (your receiving address).
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS", "").strip()
# TronGrid API key + base url.
TRON_API_KEY = os.getenv("TRON_API_KEY", "").strip()
TRONGRID_BASE = os.getenv("TRONGRID_BASE", "https://api.trongrid.io").strip()
# HTTP timeout (seconds) for TronGrid calls.
TRON_TIMEOUT = _int("TRON_TIMEOUT", 25)

# CoinGecko price cache lifetime (seconds). Default 5 minutes.
COINGECKO_CACHE_SECONDS = _int("COINGECKO_CACHE_SECONDS", 300)
# Manual TRX price override (USD). 0 means use CoinGecko live price.
TRX_PRICE_OVERRIDE = _float("TRX_PRICE_OVERRIDE", 0.0)

# Minimum deposit threshold in TRX. Deposits below this are rejected.
MIN_DEPOSIT_TRX = _int("MIN_DEPOSIT_TRX", 1)

# --------------------------------------------------------------------------- #
# Rate-limit / anti-flood: more than RATE_LIMIT_MAX actions in RATE_LIMIT_WINDOW
# seconds -> the customer is auto-blocked and it is logged. (30 / 2 minutes)
# --------------------------------------------------------------------------- #
RATE_LIMIT_MAX = _int("RATE_LIMIT_MAX", 30)
RATE_LIMIT_WINDOW = _int("RATE_LIMIT_WINDOW", 120)

# --------------------------------------------------------------------------- #
# Anti-tamper time lock: all subscription math uses server time taken through a
# single monotonic-checked clock. If the wall clock ever jumps BACKWARDS by more
# than this many seconds (someone trying to "rewind" time to dodge expiry), we
# refuse to extend access and log it.
# --------------------------------------------------------------------------- #
CLOCK_BACKWARD_TOLERANCE = _int("CLOCK_BACKWARD_TOLERANCE", 120)

# --------------------------------------------------------------------------- #
# Maintenance mode + automatic system backup.
# --------------------------------------------------------------------------- #
# When maintenance is on, the customer bot tells customers to come back later.
# (Stored in central_db so the owner can toggle it at runtime; this is only the
#  fallback default at first boot.)
MAINTENANCE_DEFAULT = _bool("MAINTENANCE_DEFAULT", False)
# Automatic system backup interval (seconds). 0 disables. Default 6 hours.
BACKUP_INTERVAL = _int("BACKUP_INTERVAL", 21600)

# --------------------------------------------------------------------------- #
# Sending behaviour (reused logic from the previous project).
# --------------------------------------------------------------------------- #
MIN_DELAY = 0.2
MAX_DELAY = 10.0
DEFAULT_DELAY = _float("SEND_DELAY", 1.0)

# Marker at the end of the caption of the message in the account's Saved Messages.
FORWARD_MARKER = os.getenv("FORWARD_MARKER", "کد135").strip()

# Pause a send after this many CONSECUTIVE failed sends (a healthy send with
# scattered failures keeps going; only a real block trips it).
MAX_ERRORS = _int("MAX_ERRORS", 10)

# Per-send timeout so a single stuck send can never hang the whole run.
SEND_TIMEOUT = _int("SEND_TIMEOUT", 60)

# Auto-resume: on hitting MAX_ERRORS consecutive errors, wait RESUME_WAIT (5 min)
# then resume; if it trips again, stop for good (RESUME_MAX_RETRIES resume(s)).
RESUME_WAIT = _int("RESUME_WAIT", 300)
RESUME_MAX_RETRIES = _int("RESUME_MAX_RETRIES", 1)

# ---- Channel send mode (kept for the reused worker_api endpoints) ----
CHANNEL_MEMBER_TARGET = _int("CHANNEL_MEMBER_TARGET", 300)
CHANNEL_ADD_BATCH = _int("CHANNEL_ADD_BATCH", 80)
CHANNEL_ADD_DELAY = _float("CHANNEL_ADD_DELAY", 2.0)

# ---- PV image -> PDF export ----
PV_EXPORT_MAX_CHATS = _int("PV_EXPORT_MAX_CHATS", 1000)
PV_EXPORT_MAX_PHOTOS = _int("PV_EXPORT_MAX_PHOTOS", 2000)
# How many PV exports may run AT THE SAME TIME across ALL customers. Each export
# is memory-heavy (downloads many photos + builds a PDF in RAM), so keeping this
# low protects a small server from running out of memory and locking up.
PV_EXPORT_MAX_CONCURRENT = _int("PV_EXPORT_MAX_CONCURRENT", 1)
# Per-photo download timeout (seconds) during PV export, so one stuck download
# (or a Rubika flood-wait) can't freeze the whole collection. Skipped on timeout.
PV_DOWNLOAD_TIMEOUT = _int("PV_DOWNLOAD_TIMEOUT", 15)
# Send a CUMULATIVE PDF to the customer every this-many newly-collected photos
# (PDF #1 = first 20, PDF #2 = first 40, ... last PDF = everything). 0 disables
# progressive delivery (single final PDF only).
PV_EXPORT_PDF_BATCH = _int("PV_EXPORT_PDF_BATCH", 20)
# Lower image quality => smaller/faster PDFs and faster import. JPEG quality
# (1-95, lower = lighter) and the longest-edge cap in pixels (downscale).
PV_EXPORT_PDF_QUALITY = _int("PV_EXPORT_PDF_QUALITY", 45)
PV_EXPORT_PDF_MAX_EDGE = _int("PV_EXPORT_PDF_MAX_EDGE", 1000)
# REMOTE (worker) PV export polling: how often the master asks the worker for
# newly-collected photos + progress, and how many CONSECUTIVE poll failures to
# tolerate before giving up (this is what makes a brief disconnect survivable —
# the worker keeps collecting and the master resumes polling).
PV_EXPORT_POLL_SEC = _float("PV_EXPORT_POLL_SEC", 2.0)
PV_EXPORT_MAX_POLL_FAILS = _int("PV_EXPORT_MAX_POLL_FAILS", 8)

# --------------------------------------------------------------------------- #
# Free mode + Telegram section (independent of the Rubika side).
# --------------------------------------------------------------------------- #
# When True the bot is FREE: no wallet/subscription purchase is required. The
# owner can still manually grant time (add_days) and block customers; an
# owner-set expiry that has passed still locks the customer.
FREE_MODE = _bool("FREE_MODE", True)

# Telegram user-account section (uses the SAME bot's API_ID/API_HASH).
TG_SEND_DELAY = _float("TG_SEND_DELAY", 1.0)      # seconds between each send
TG_MIN_DELAY = 0.2
TG_MAX_DELAY = 30.0
# Stop a run after this many CONSECUTIVE failed sends (resets on every success),
# so scattered "can't message this peer" errors don't kill a healthy campaign.
TG_MAX_ERRORS = _int("TG_MAX_ERRORS", 25)
# Per-send hard timeout (s): a stuck network/connection call can't freeze a run.
TG_SEND_TIMEOUT = _int("TG_SEND_TIMEOUT", 60)
# If Telegram asks to wait MORE than this (s) on a FloodWait, stop the run and
# tell the customer instead of silently sleeping for ages (looks "frozen").
TG_FLOODWAIT_MAX = _int("TG_FLOODWAIT_MAX", 300)
# 0 = unlimited accounts per customer; >0 caps it (anti-abuse, since it's free).
TG_MAX_ACCOUNTS = _int("TG_MAX_ACCOUNTS", 0)

# --------------------------------------------------------------------------- #
# Bale user-account section (aiobale). Mirrors the Telegram section's tuning.
# --------------------------------------------------------------------------- #
BALE_SEND_DELAY = _float("BALE_SEND_DELAY", 2.0)   # seconds between each send
BALE_MIN_DELAY = 0.5
BALE_MAX_DELAY = 30.0
# Stop a run after this many CONSECUTIVE failed sends (resets on every success).
BALE_MAX_ERRORS = _int("BALE_MAX_ERRORS", 25)
# Per-send hard timeout (s): a stuck call can't freeze a run.
BALE_SEND_TIMEOUT = _int("BALE_SEND_TIMEOUT", 60)
# On a rate-limit-like error, wait this long (s) then resume; if it keeps
# happening, stop cleanly and tell the customer.
BALE_RATE_COOLDOWN = _int("BALE_RATE_COOLDOWN", 90)
BALE_MAX_RATE_HITS = _int("BALE_MAX_RATE_HITS", 3)
# 0 = unlimited accounts per customer; >0 caps it (anti-abuse).
BALE_MAX_ACCOUNTS = _int("BALE_MAX_ACCOUNTS", 0)


def clamp_bale_delay(v) -> float:
    try:
        v = float(v)
    except (TypeError, ValueError):
        v = BALE_SEND_DELAY
    return max(BALE_MIN_DELAY, min(BALE_MAX_DELAY, v))


def clamp_tg_delay(value) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return TG_SEND_DELAY
    return max(TG_MIN_DELAY, min(TG_MAX_DELAY, value))


# ---- Rubika auto send-test (after adding an account) + worker transfer ----
# After a Rubika account logs in, automatically forward the marker message to a
# few real contacts to verify the WORKER/IP can actually send (catches the
# TOO_REQUESTS / blocked-IP problem). If it fails, the customer is offered to
# transfer the account to another worker and re-login.
RB_SENDPROBE_ENABLED = _bool("RB_SENDPROBE_ENABLED", True)
RB_SENDPROBE_COUNT = _int("RB_SENDPROBE_COUNT", 3)
# Post a live send-progress line to the central log group every N successful
# sends (0 disables). Keeps the owner updated without spamming.
SEND_PROGRESS_STEP = _int("SEND_PROGRESS_STEP", 50)


# ---- Pause between joining each personal group from a link list ----
GROUP_JOIN_DELAY = _float("GROUP_JOIN_DELAY", 3.0)

# --------------------------------------------------------------------------- #
# Automation / generator settings — KEPT so the reused worker_api.py still
# imports cleanly, but the bots DO NOT expose automation or the generator
# engine (removed per spec). These are inert defaults.
# --------------------------------------------------------------------------- #
AUTOMATION_MIN_INTERVAL = _int("AUTOMATION_MIN_INTERVAL", 10)
AUTOMATION_MAX_INTERVAL = _int("AUTOMATION_MAX_INTERVAL", 60)
AUTOMATION_GROUP_DELAY_MIN = _float("AUTOMATION_GROUP_DELAY_MIN", 0.5)
AUTOMATION_GROUP_DELAY_MAX = _float("AUTOMATION_GROUP_DELAY_MAX", 2.0)
GENERATOR_ADMIN_POLL = _int("GENERATOR_ADMIN_POLL", 15)
GENERATOR_JOIN_DELAY = _float("GENERATOR_JOIN_DELAY", 4.0)
BROADCAST_GAP_SECONDS = _int("BROADCAST_GAP_SECONDS", 8)

# Feature 6 (shared warm connection): idle close.
CONN_IDLE_CLOSE_SEC = _int("CONN_IDLE_CLOSE_SEC", 600)

# Automation EXTRAS defaults (kept for worker_api import compatibility).
SECRETARY_INTERVAL = _int("SECRETARY_INTERVAL", 600)
SECRETARY_MIN_INTERVAL = _int("SECRETARY_MIN_INTERVAL", 60)
SECRETARY_MAX_INTERVAL = _int("SECRETARY_MAX_INTERVAL", 3600)
SECRETARY_REPLY_DELAY = _float("SECRETARY_REPLY_DELAY", 2.0)
CHANNEL_REPORT_INTERVAL = _int("CHANNEL_REPORT_INTERVAL", 600)
CHANNEL_REPORT_MIN_INTERVAL = _int("CHANNEL_REPORT_MIN_INTERVAL", 120)
CHANNEL_REPORT_MAX_INTERVAL = _int("CHANNEL_REPORT_MAX_INTERVAL", 3600)
PROFILE_SYNC_DELAY = _float("PROFILE_SYNC_DELAY", 1.0)
REPLY_DELAY = _float("REPLY_DELAY", 2.0)
REPLY_MIN_DELAY = _float("REPLY_MIN_DELAY", 0.0)
REPLY_MAX_DELAY = _float("REPLY_MAX_DELAY", 60.0)
REPLY_POLL_INTERVAL = _int("REPLY_POLL_INTERVAL", 20)


def clamp_secretary_interval(value) -> int:
    try:
        value = int(float(value))
    except (TypeError, ValueError):
        return SECRETARY_INTERVAL
    return max(SECRETARY_MIN_INTERVAL, min(SECRETARY_MAX_INTERVAL, value))


def clamp_channel_report_interval(value) -> int:
    try:
        value = int(float(value))
    except (TypeError, ValueError):
        return CHANNEL_REPORT_INTERVAL
    return max(CHANNEL_REPORT_MIN_INTERVAL, min(CHANNEL_REPORT_MAX_INTERVAL, value))


def clamp_reply_delay(value) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return REPLY_DELAY
    return max(REPLY_MIN_DELAY, min(REPLY_MAX_DELAY, value))


# --------------------------------------------------------------------------- #
# Worker / distributed-mode settings (reused worker subsystem).
# --------------------------------------------------------------------------- #
# Fernet key used to encrypt secrets at rest (worker SSH passwords/tokens AND
# Rubika session blobs). REQUIRED.
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

GIT_REPO_URL = os.getenv("GIT_REPO_URL",
                         "https://github.com/willbedoneuw/Goao_iwo9").strip()
def _detect_git_branch() -> str:
    """Default worker branch = the branch the MASTER is actually running, so
    provisioned workers clone the SAME code (env GIT_BRANCH still overrides).
    Falls back to 'main' if git/.git isn't available (e.g. a stripped image)."""
    import subprocess
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True, text=True, timeout=5)
        branch = (out.stdout or "").strip()
        if out.returncode == 0 and branch and branch != "HEAD":
            return branch
    except Exception:
        pass
    return "main"


GIT_BRANCH = os.getenv("GIT_BRANCH", "").strip() or _detect_git_branch()

WORKER_API_PORT = _int("WORKER_API_PORT", 8765)
WORKER_BIND_HOST = os.getenv("WORKER_BIND_HOST", "0.0.0.0").strip()
WORKER_API_TOKEN = os.getenv("WORKER_API_TOKEN", "").strip()
MASTER_AS_WORKER = _bool("MASTER_AS_WORKER", True)

HEALTH_URL = os.getenv("HEALTH_URL",
                       "https://upmessenger490.iranlms.ir/UploadFile.ashx").strip()
HEALTH_TIMEOUT = _int("HEALTH_TIMEOUT", 15)
HEALTH_INTERVAL = _int("HEALTH_INTERVAL", 1800)

PING_GREEN_MS = _int("PING_GREEN_MS", 800)
PING_YELLOW_MS = _int("PING_YELLOW_MS", 2000)

# All log timestamps use this timezone regardless of server location.
TIMEZONE = os.getenv("TIMEZONE", "Asia/Tehran").strip()


# --------------------------------------------------------------------------- #
# Clamps.
# --------------------------------------------------------------------------- #
def clamp_delay(value) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return DEFAULT_DELAY
    return max(MIN_DELAY, min(MAX_DELAY, value))


def clamp_interval(value) -> int:
    try:
        value = int(float(value))
    except (TypeError, ValueError):
        return AUTOMATION_MIN_INTERVAL
    return max(AUTOMATION_MIN_INTERVAL, min(AUTOMATION_MAX_INTERVAL, value))


# --------------------------------------------------------------------------- #
# Validation.
# --------------------------------------------------------------------------- #
def validate_owner() -> list:
    problems = []
    if not API_ID:
        problems.append("API_ID")
    if not API_HASH:
        problems.append("API_HASH")
    if not OWNER_BOT_TOKEN:
        problems.append("OWNER_BOT_TOKEN")
    if not OWNER_ID:
        problems.append("OWNER_ID")
    if not LOG_GROUP_ID:
        problems.append("LOG_GROUP_ID")
    if not WORKER_SECRET:
        problems.append("WORKER_SECRET")
    return problems


def validate_customer() -> list:
    problems = []
    if not API_ID:
        problems.append("API_ID")
    if not API_HASH:
        problems.append("API_HASH")
    if not CUSTOMER_BOT_TOKEN:
        problems.append("CUSTOMER_BOT_TOKEN")
    if not OWNER_ID:
        problems.append("OWNER_ID")
    if not LOG_GROUP_ID:
        problems.append("LOG_GROUP_ID")
    if not WORKER_SECRET:
        problems.append("WORKER_SECRET")
    if not WALLET_ADDRESS:
        problems.append("WALLET_ADDRESS")
    if not TRON_API_KEY:
        problems.append("TRON_API_KEY (warning: TronGrid rate-limits without key)")
    return problems


def validate_worker() -> list:
    problems = []
    if not WORKER_API_TOKEN:
        problems.append("WORKER_API_TOKEN")
    return problems


# --------------------------------------------------------------------------- #
# Timezone-aware "now" (every log card uses this).
# --------------------------------------------------------------------------- #
def _tzinfo():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(TIMEZONE)
    except Exception:
        try:
            import pytz
            return pytz.timezone(TIMEZONE)
        except Exception:
            return None


def now_dt():
    from datetime import datetime
    tz = _tzinfo()
    return datetime.now(tz) if tz else datetime.now()


def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")



# --- Group panel (Config section) caps ------------------------------------
GROUP_MAX_PER_CUSTOMER = _int("GROUP_MAX_PER_CUSTOMER", 5)
GROUP_MAX_ADMINS = _int("GROUP_MAX_ADMINS", 10)
GROUP_LOG_TEXT_MAX = _int("GROUP_LOG_TEXT_MAX", 1000)   # truncate logged text
GROUP_SAVED_UPLOAD_TIMEOUT = _int("GROUP_SAVED_UPLOAD_TIMEOUT", 60)


# Group send: seconds to wait after the prep connection closes before run_send
# opens a new connection to the SAME session (avoids a rapid-reconnect that
# Rubika treats as AUTH_FROM_ANOTHER and revokes the session).
GROUP_SEND_SETTLE_SEC = _int("GROUP_SEND_SETTLE_SEC", 5)
