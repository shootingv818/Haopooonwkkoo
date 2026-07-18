"""
backup.py — automatic system backup (sessions + users) + maintenance helpers.
=============================================================================

Backup bundles BOTH databases (customers + central) and ALL Rubika session
files into one zip, then ships it to the central log group. A periodic loop
runs every ``config.BACKUP_INTERVAL`` seconds; the owner can also trigger one
on demand from the panel. Worker session files are pulled in too when remote
workers exist (best-effort, via the reused worker subsystem).

Manual per-account backup from the OLD project is intentionally removed; this
is the automatic, system-wide backup only.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
import zipfile

import config
import central_db
import db
import logbus
import rubika_client as rb

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def _add_dir_to_zip(zf: zipfile.ZipFile, src_dir: str, arc_prefix: str):
    if not os.path.isdir(src_dir):
        return
    for root, _dirs, files in os.walk(src_dir):
        for fname in files:
            full = os.path.join(root, fname)
            rel = os.path.relpath(full, src_dir)
            zf.write(full, arcname=os.path.join(arc_prefix, rel))


async def _add_worker_sessions(zf: zipfile.ZipFile):
    """Pull remote worker session files into the zip (no-op without workers)."""
    try:
        import worker
        await worker.collect_sessions_into_zip(zf)
    except Exception as e:  # noqa: BLE001
        await logbus.to_group(f"⚠️ بکاپ سشن ورکرها ناقص ماند: {repr(e)[:150]}")


async def build_archive():
    """Build the system backup zip; returns its path (caller deletes) or None."""
    os.makedirs(DATA_DIR, exist_ok=True)
    has_cust = os.path.exists(db.DB_PATH)
    has_central = os.path.exists(central_db.DB_PATH)
    has_sessions = os.path.isdir(rb.SESSIONS_DIR) and any(os.scandir(rb.SESSIONS_DIR))
    if not (has_cust or has_central or has_sessions):
        return None

    fd, zip_path = tempfile.mkstemp(prefix="backup_", suffix=".zip", dir=DATA_DIR)
    os.close(fd)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        if has_cust:
            zf.write(db.DB_PATH, arcname="customer.db")
        if has_central:
            zf.write(central_db.DB_PATH, arcname="central.db")
        _add_dir_to_zip(zf, rb.SESSIONS_DIR, "sessions/local")
        await _add_worker_sessions(zf)
    return zip_path


async def run_backup(notify_user: int = None) -> bool:
    """Build + ship a backup to the log group (and optionally a user). Returns
    True if a backup was produced."""
    try:
        path = await build_archive()
    except Exception as e:  # noqa: BLE001
        await logbus.to_group(logbus.card("💾 BACKUP — خطا", [
            f"💥 {repr(e)[:160]}", f"🕒 {logbus.now()}"]))
        return False
    if not path:
        return False
    try:
        await logbus.to_group_file(
            path, caption=("💾 بکاپ خودکار سیستم • " + logbus.now() +
                           "\nشامل: دیتابیس مشتری‌ها + دیتابیس مرکزی + سشن اکانت‌ها"))
        if notify_user:
            await logbus.to_pv(notify_user, "✅ بکاپ سیستم ساخته و در گروه لاگ ارسال شد.")
        central_db.set_last_backup()
        return True
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


async def backup_loop():
    """Periodic automatic backup loop (owner process only)."""
    interval = int(config.BACKUP_INTERVAL or 0)
    if interval <= 0:
        return
    while True:
        await asyncio.sleep(interval)
        try:
            await run_backup()
        except Exception as e:  # noqa: BLE001
            print(f"[backup loop] {e}")


# ---- maintenance helpers ----
def maintenance_on() -> bool:
    try:
        return central_db.get_maintenance()
    except Exception:
        return False
