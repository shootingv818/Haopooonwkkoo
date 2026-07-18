#!/usr/bin/env python3
"""Tell whether an account runs LOCAL (master) or on a REMOTE worker.

Read-only DB lookup — changes NOTHING. Run it on the MASTER (where the DB is):
    TEST_PHONE=989178320427 python3 where_account.py
    # or:  python3 where_account.py 989178320427

It does NOT need rubpy (no Rubika connection), only the local DB.
"""
import os
import sys
import types

# config.py does `from dotenv import load_dotenv`; if python-dotenv isn't here,
# inject a tiny stand-in that parses .env by hand so this still runs.
try:
    import dotenv  # noqa: F401
except ModuleNotFoundError:
    def _manual_load_dotenv(path=None, *a, **k):
        for p in ([path] if path else [".env", os.path.join(
                os.path.dirname(os.path.abspath(__file__)), ".env")]):
            try:
                with open(p) as fh:
                    for line in fh:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        os.environ.setdefault(
                            k.strip(), v.strip().strip('"').strip("'"))
                return True
            except FileNotFoundError:
                continue
        return False
    _shim = types.ModuleType("dotenv")
    _shim.load_dotenv = _manual_load_dotenv
    sys.modules["dotenv"] = _shim

import db


def norm(p):
    """Lightweight phone normalizer (no rubpy needed)."""
    p = (p or "").strip().replace("+", "").replace(" ", "").replace("-", "")
    if p.startswith("0098"):
        p = p[2:]
    if p.startswith("00"):
        p = p[2:]
    if p.startswith("0"):
        p = "98" + p[1:]
    return p


def main():
    raw = os.getenv("TEST_PHONE") or (sys.argv[1] if len(sys.argv) > 1 else "")
    phone = norm(raw)
    if not phone:
        print("❌ شماره بده:  TEST_PHONE=989178320427 python3 where_account.py")
        return

    accounts = db.list_accounts()
    acc = next((a for a in accounts if norm(a.get("phone", "")) == phone), None)
    if not acc:
        print(f"❌ اکانت {phone} توی دیتابیسِ این ماشین نیست.")
        print("   یعنی یا روی یه ماشین دیگه‌ست، یا اصلاً اضافه نشده.")
        print(f"   (تعداد کل اکانت‌های این دیتابیس: {len(accounts)})")
        return

    wid = acc.get("worker_id")
    print(f"📱 شماره       : {phone}")
    print(f"🆔 account_id  : {acc.get('id')}    customer_id : {acc.get('customer_id')}")
    print(f"📊 وضعیت اکانت : {acc.get('status')}")
    print(f"🔗 worker_id   : {wid}")

    workers = {w["id"]: w for w in db.list_workers()}

    if wid is None:
        print("➡️ به هیچ ورکری وصل نیست → روی «مستر (لوکال)» اجرا می‌شه.")
        print("   ارسالش از آی‌پیِ همین سرورِ مستر می‌ره بیرون.")
        return

    w = workers.get(wid)
    if not w:
        print(f"⚠️ ورکر #{wid} توی جدولِ ورکرها نیست (شاید حذف شده).")
        return

    if w.get("is_master"):
        print(f"➡️ روی «مستر / لوکال» اجرا می‌شه.  (tag={w.get('tag')})")
        print("   ارسالش از آی‌پیِ همین سرورِ مستر می‌ره بیرون.")
    else:
        host = w.get("ssh_host") or w.get("host") or "-"
        print("➡️ روی «ورکرِ ریموت» اجرا می‌شه. 🖥")
        print(f"   tag={w.get('tag')}   host={host}   "
              f"status={w.get('status')}   enabled={w.get('enabled')}")
        print("   برای تستِ ارسال، اسکریپت تشخیص رو داخل کانتینرِ همین ورکر بزن:")
        print(f"   docker exec -e TEST_PHONE={phone} -it v2rubby-worker "
              "python3 diagnose_send.py")


if __name__ == "__main__":
    main()
