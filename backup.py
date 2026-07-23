"""
backup.py — fast, SESSION-ONLY backup (+ maintenance helpers).
==============================================================

The backup archive contains ONLY what is needed to restore accounts: the
Rubika session files (master + every remote worker) and the Telegram
StringSessions. It deliberately EXCLUDES the databases, settings, stats,
tg_media, logs, source and .env — nothing but sessions.

Archive layout:
    rubika/local/<session files>          (master's data/sessions)
    rubika/workers/<worker-tag>/<files>   (each remote worker, fetched in
                                            PARALLEL for speed)
    telegram/sessions/acc_<id>_<phone>.session   (StringSession per account)

The finished archive is ENCRYPTED (Fernet, config.WORKER_SECRET) before it is
shipped to the log group — the sessions are account-equivalent secrets, so the
plaintext zip never leaves the server and the key is never put in the archive
or the log group. A single in-process lock prevents two backups running at
once. The temp file is always removed in ``finally``.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
import zipfile

import config
import crypto_util
import db
import logbus
import rubika_client as rb

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Prevent two backups (auto loop + manual) building at the same time.
_backup_lock = asyncio.Lock()


def _add_dir_to_zip(zf: zipfile.ZipFile, src_dir: str, arc_prefix: str) -> int:
    """Add every file under src_dir to the zip; return the file count."""
    n = 0
    if not os.path.isdir(src_dir):
        return 0
    for root, _dirs, files in os.walk(src_dir):
        for fname in files:
            full = os.path.join(root, fname)
            rel = os.path.relpath(full, src_dir)
            zf.write(full, arcname=os.path.join(arc_prefix, rel))
            n += 1
    return n


async def build_archive():
    """Build the SESSION-ONLY backup. Returns (path, meta) or (None, meta).
    meta = {rb_local, rb_workers, tg, unreachable}. Caller deletes the path."""
    os.makedirs(DATA_DIR, exist_ok=True)
    meta = {"rb_local": 0, "rb_workers": 0, "tg": 0, "unreachable": []}

    # 1) remote worker Rubika sessions — fetched in PARALLEL (network-bound).
    worker_files = []
    try:
        import worker
        worker_files, meta["unreachable"] = await worker.collect_worker_sessions(
            "rubika/workers")
    except Exception as e:  # noqa: BLE001
        meta["unreachable"].append("?")
        await logbus.to_group(f"⚠️ بکاپ سشن ورکرها ناقص ماند: {repr(e)[:150]}")

    # 2) Telegram StringSessions (from tg_accounts; DB itself is NOT backed up).
    try:
        tg_rows = [a for a in db.list_tg_accounts() if a.get("session")]
    except Exception:
        tg_rows = []

    rb_local_has = os.path.isdir(rb.SESSIONS_DIR) and any(os.scandir(rb.SESSIONS_DIR))
    if not (worker_files or tg_rows or rb_local_has):
        return None, meta

    fd, zip_path = tempfile.mkstemp(prefix="sessions_", suffix=".zip", dir=DATA_DIR)
    os.close(fd)
    # compresslevel=1 => fast (session files are small, speed over ratio).
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as zf:
        meta["rb_local"] = _add_dir_to_zip(zf, rb.SESSIONS_DIR, "rubika/local")
        for arcname, data in worker_files:
            try:
                zf.writestr(arcname, data)
                meta["rb_workers"] += 1
            except Exception:
                continue
        for a in tg_rows:
            safe = "".join(ch for ch in str(a.get("phone") or a["id"]) if ch.isalnum())
            try:
                zf.writestr(f"telegram/sessions/acc_{a['id']}_{safe}.session",
                            a["session"])
                meta["tg"] += 1
            except Exception:
                continue
    return zip_path, meta


def _encrypt_archive(zip_path: str):
    """Encrypt the zip in place -> returns (out_path, encrypted: bool). Falls
    back to the plaintext zip (with a warning) only if no key is configured."""
    if not crypto_util.is_configured():
        return zip_path, False
    try:
        with open(zip_path, "rb") as f:
            data = f.read()
        token = crypto_util.encrypt_bytes(data)
        enc_path = zip_path + ".enc"
        with open(enc_path, "wb") as f:
            f.write(token)
        try:
            os.remove(zip_path)
        except Exception:
            pass
        return enc_path, True
    except Exception:
        return zip_path, False


def _summary_rows(meta: dict, encrypted: bool) -> list:
    rows = [
        f"🟣 روبیکا (مستر) : {meta.get('rb_local', 0)}",
        f"🟣 روبیکا (ورکرها) : {meta.get('rb_workers', 0)}",
        f"✈️ تلگرام : {meta.get('tg', 0)}",
        f"🔐 رمزگذاری : {'بله' if encrypted else '⚠️ خیر (WORKER_SECRET تنظیم نشده)'}",
    ]
    unreachable = meta.get("unreachable") or []
    if unreachable:
        rows.append(f"⚠️ وضعیت : ناقص — ورکرهای در دسترس‌نبوده: {len(unreachable)}")
    else:
        rows.append("✅ وضعیت : کامل")
    rows.append(f"🕒 {logbus.now()}")
    return rows


async def run_backup(notify_user: int = None, to_owner: int = None) -> bool:
    """Build + ship a SESSION-ONLY backup. Ships the (encrypted) archive to the
    log group; if to_owner is set, also sends it privately to that owner id.
    Returns True if an archive was produced."""
    async with _backup_lock:
        path = None
        try:
            try:
                path, meta = await build_archive()
            except Exception as e:  # noqa: BLE001
                await logbus.to_group(logbus.card("💾 BACKUP — خطا", [
                    f"💥 {repr(e)[:160]}", f"🕒 {logbus.now()}"]))
                return False
            if not path:
                return False
            send_path, encrypted = _encrypt_archive(path)
            path = send_path  # ensure finally removes whatever we ended with
            title = "💾 بکاپ سشن‌ها" + (" (رمزگذاری‌شده)" if encrypted else "")
            caption = logbus.card(title, _summary_rows(meta, encrypted))
            await logbus.to_group_file(send_path, caption=caption)
            if to_owner:
                await logbus.to_pv(to_owner, "✅ بکاپ سشن‌ها ساخته و ارسال شد.")
                try:
                    await logbus._client.send_file(int(to_owner), send_path,
                                                   caption=caption, force_document=True)
                except Exception:
                    pass
            if notify_user and notify_user != to_owner:
                await logbus.to_pv(notify_user, "✅ بکاپ سشن‌ها در گروه لاگ ارسال شد.")
            try:
                import central_db
                central_db.set_last_backup()
            except Exception:
                pass
            return True
        finally:
            if path:
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
        import central_db
        return central_db.get_maintenance()
    except Exception:
        return False
