#!/usr/bin/env python3
"""Standalone SEND diagnostic — does NOT change anything in the app.

Why this exists
---------------
When sending you sometimes get `TooRequests / TOO_REQUESTS`. That is Rubika
rate-limiting (flood) the ACCOUNT, not a bug in the bot. With MAX_ERRORS=3 the
run hits its error cap after only a few throttles, pauses 5 min and eventually
stops — which is why "the bot stops sending".

This script measures, WITHOUT touching the app or the DB:
  1. the machine's public IP + country/ISP  (Rubika throttles non-Iran IPs hard)
  2. how fast we can connect + find the marked message
  3. how many forwards SUCCEED before the first TOO_REQUESTS, and the pattern
     after it (does it recover? always fail?), with per-send latency
  4. (optional) a DELAY SWEEP to find a delay that avoids throttling

IMPORTANT
---------
* Run it on the SAME machine that hosts the account's session:
    - local account  -> on the master:   python3 diagnose_send.py
    - remote account -> inside the worker container:
        docker exec -it v2rubby-worker python3 diagnose_send.py
  (because Rubika sees THAT machine's IP)
* By default it forwards the marked message to your OWN Saved Messages
  (TEST_TARGET=self) so it never spams real contacts. You can delete those
  test messages afterwards. Set TEST_TARGET=contacts for a real-world test.
* Run it when no real send is in progress for that account (shared session).

Configure via environment variables (all optional except TEST_PHONE):
  TEST_PHONE        (required) account phone, e.g. 989131528613
  TEST_TARGET       self (default) | contacts
  TEST_COUNT        forwards in the single run (default 30)
  TEST_DELAY        seconds between forwards in the single run (default = config.DEFAULT_DELAY)
  TEST_MARKER       marker text (default = config.FORWARD_MARKER)
  TEST_SINGLE       1 (default) | 0  -> skip the single run
  TEST_SWEEP        1 (default) | 0  -> skip the delay sweep
  TEST_SWEEP_DELAYS comma list (default "0.5,1,2,3,5")
  TEST_SWEEP_BATCH  forwards per delay in the sweep (default 8)

ONE command runs the WHOLE battery (IP+geo, connect, marker, stats, single
run, AND the delay sweep). Just: TEST_PHONE=... python3 diagnose_send.py
"""
import asyncio
import os
import subprocess
import sys
import time
import types

# --------------------------------------------------------------------------- #
# Self-bootstrap: if the engine deps (rubpy/httpx) aren't installed for THIS
# interpreter, install them once (from requirements.txt if present) and re-exec.
# This makes the diagnostic "everything in one file" — no manual pip needed.
# (For a REMOTE account it's still best to run INSIDE the worker container,
#  where rubpy + the session already exist; see the header notes.)
# --------------------------------------------------------------------------- #
def _bootstrap_deps():
    if os.getenv("DIAG_BOOTSTRAPPED") == "1":
        return  # already tried once — don't loop forever
    missing = []
    for mod in ("rubpy", "httpx"):
        try:
            __import__(mod)
        except Exception:  # noqa: BLE001
            missing.append(mod)
    if not missing:
        return
    here = os.path.dirname(os.path.abspath(__file__))
    req = os.path.join(here, "requirements.txt")
    print(f"📦 ماژول‌های لازم نصب نیستن: {missing} — نصب خودکار ...")
    cmd = [sys.executable, "-m", "pip", "install", "-q"]
    cmd += (["-r", req] if os.path.exists(req)
            else ["rubpy", "httpx", "python-dotenv"])
    try:
        subprocess.check_call(cmd)
    except Exception as e:  # noqa: BLE001
        print(f"❌ نصب خودکار نشد: {e!r}\n"
              "   دستی نصب کن:  pip3 install -r requirements.txt\n"
              "   یا اگه اکانت روی ورکره، داخل کانتینر اجرا کن:\n"
              "   docker exec -e TEST_PHONE=... -it v2rubby-worker python3 diagnose_send.py")
        sys.exit(1)
    os.environ["DIAG_BOOTSTRAPPED"] = "1"  # inherited by the re-exec'd process
    print("🔁 نصب شد؛ اسکریپت دوباره اجرا می‌شه ...")
    os.execv(sys.executable, [sys.executable] + sys.argv)


_bootstrap_deps()

# --------------------------------------------------------------------------- #
# Make this diagnostic self-sufficient: config.py does `from dotenv import
# load_dotenv`. If python-dotenv isn't installed for THIS interpreter (e.g. the
# bot runs in a venv/container), inject a tiny stand-in that parses .env by hand
# so the script still runs WITHOUT installing anything or touching the app.
# --------------------------------------------------------------------------- #
try:
    import dotenv  # noqa: F401
except ModuleNotFoundError:
    def _manual_load_dotenv(path=None, *args, **kwargs):
        for p in ([path] if path else [".env", os.path.join(os.path.dirname(
                os.path.abspath(__file__)), ".env")]):
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
    print("ℹ️ python-dotenv نصب نیست؛ .env را دستی لود می‌کنم (اشکالی نداره).")

import config
import rubika_client as rb


def _env(name, default=None):
    v = os.getenv(name)
    return v if (v is not None and v != "") else default


def _is_too_requests(exc) -> bool:
    s = repr(exc).lower()
    return "too_requests" in s or "toorequests" in s


def _line(c="─", n=60):
    print(c * n)


def _marker_from_db(phone):
    """Look up THIS account's customer marker from the local DB (read-only).
    Returns the marker string or None. Safe if the DB isn't here (e.g. worker)."""
    try:
        import db
        for a in db.list_accounts():
            if rb.normalize_phone(a.get("phone", "")) == phone:
                cid = a.get("customer_id")
                if cid is not None:
                    return (db.get_settings(int(cid)) or {}).get("marker")
    except Exception:  # noqa: BLE001
        return None
    return None


async def _latest_saved_message(client):
    """Fallback when no marker matches: grab a recent message id from the
    account's OWN Saved Messages so the rate test can still run. Forwarding ANY
    message to self exercises the same send/throttle path."""
    saved_guid = await rb.get_self_guid(client)
    try:
        result = await client.get_messages(saved_guid, "0", "20")
        messages = getattr(result, "messages", None)
        if messages is None and isinstance(result, dict):
            messages = result.get("messages", [])
        if messages:
            return saved_guid, rb._msg_id_of(messages[0])
    except Exception:  # noqa: BLE001
        pass
    return saved_guid, None


async def show_public_ip():
    """Best-effort public IP + geo. Needs outbound internet on this machine."""
    print("🌐 آی‌پی و موقعیت سرور (همون چیزی که روبیکا می‌بینه):")
    try:
        import httpx
    except Exception as e:  # noqa: BLE001
        print(f"   httpx در دسترس نیست: {e!r}")
        return
    for url in ("http://ip-api.com/json/?fields=query,country,city,isp,org,as",
                "https://ipinfo.io/json"):
        try:
            async with httpx.AsyncClient(timeout=10) as cl:
                r = await cl.get(url)
                r.raise_for_status()
                d = r.json()
            ip = d.get("query") or d.get("ip")
            country = d.get("country")
            city = d.get("city")
            isp = d.get("isp") or d.get("org")
            print(f"   IP      : {ip}")
            print(f"   Country : {country}    City: {city}")
            print(f"   ISP/Org : {isp}")
            if country and str(country).lower() not in ("iran", "ir",
                                                        "islamic republic of iran"):
                print("   ⚠️ این آی‌پی ایران نیست — روبیکا معمولاً آی‌پی غیرایران رو "
                      "شدید ریت‌لیمیت می‌کنه. این می‌تونه علتِ اصلیِ TOO_REQUESTS باشه.")
            return
        except Exception as e:  # noqa: BLE001
            print(f"   ({url} نشد: {e!r})")
    print("   نتونستم آی‌پی عمومی رو بگیرم (شاید این ماشین اینترنت خروجی نداره).")


async def connect_account(phone):
    print(f"🔌 اتصال به اکانت {phone} ...")
    client = rb.open_client(phone)
    t0 = time.monotonic()
    await rb.connect_ready(client)
    dt = (time.monotonic() - t0) * 1000
    print(f"   connect_ready در {dt:.0f}ms")
    return client


async def show_recipient_stats(client):
    try:
        t0 = time.monotonic()
        ordered, stats = await rb.get_ordered_recipients(client)
        dt = (time.monotonic() - t0) * 1000
        print("👥 وضعیت مخاطبین (با ترتیب فعلی):")
        print(f"   کل مخاطب : {stats.get('contacts')}   "
              f"آنلاین : {stats.get('online')}   "
              f"چت‌دار : {stats.get('with_chat')}   "
              f"گروه : {stats.get('groups')}")
        print(f"   ساخت لیست در {dt:.0f}ms")
        return ordered
    except Exception as e:  # noqa: BLE001
        print(f"   نتونستم لیست مخاطبین رو بسازم: {e!r}")
        return []


async def do_forwards(client, saved_guid, mid, targets, delay, label):
    """Forward the marked message to each target, NEVER aborting, recording the
    outcome + latency of each one. Returns a result dict."""
    ok = too = other = 0
    first_too_at = None
    latencies = []
    print(f"\n▶️ تست ارسال [{label}] — تعداد={len(targets)}  تاخیر={delay}s")
    _line()
    for i, guid in enumerate(targets, 1):
        t0 = time.monotonic()
        try:
            await asyncio.wait_for(
                rb.forward_message(client, saved_guid, guid, mid),
                timeout=config.SEND_TIMEOUT)
            dt = (time.monotonic() - t0) * 1000
            latencies.append(dt)
            ok += 1
            print(f"  #{i:>3}  ✅ ok        {dt:>6.0f}ms")
        except Exception as e:  # noqa: BLE001
            dt = (time.monotonic() - t0) * 1000
            if _is_too_requests(e):
                too += 1
                if first_too_at is None:
                    first_too_at = i
                print(f"  #{i:>3}  🚫 TOO_REQ    {dt:>6.0f}ms")
            else:
                other += 1
                print(f"  #{i:>3}  ❌ {repr(e)[:60]}")
        await asyncio.sleep(delay)
    _line()
    avg = sum(latencies) / len(latencies) if latencies else 0
    print(f"  جمع‌بندی [{label}]: ✅{ok}  🚫TOO_REQUESTS={too}  ❌خطای‌دیگر={other}"
          f"  | اولین throttle در #{first_too_at}  | میانگین تاخیر موفق‌ها {avg:.0f}ms")
    return {"ok": ok, "too": too, "other": other,
            "first_too_at": first_too_at, "avg_ms": avg, "n": len(targets)}


async def main():
    phone = _env("TEST_PHONE")
    if not phone:
        print("❌ TEST_PHONE تنظیم نشده. مثال:\n"
              "   TEST_PHONE=989131528613 python3 diagnose_send.py")
        return
    phone = rb.normalize_phone(phone)
    target_mode = (_env("TEST_TARGET", "self") or "self").lower()
    count = int(_env("TEST_COUNT", "30"))
    delay = float(_env("TEST_DELAY", str(config.DEFAULT_DELAY)))
    # marker resolution: explicit TEST_MARKER > customer's marker in DB > default
    marker_env = _env("TEST_MARKER")
    db_marker = None if marker_env else _marker_from_db(phone)
    marker = marker_env or db_marker or config.FORWARD_MARKER
    marker_src = ("TEST_MARKER" if marker_env else
                  ("دیتابیس" if db_marker else "پیش‌فرض"))
    # one command runs the FULL battery by default; set these to 0 to skip a part
    run_single = _env("TEST_SINGLE", "1") != "0"
    run_sweep = _env("TEST_SWEEP", "1") != "0"

    print("=" * 60)
    print("  SEND DIAGNOSTIC  (هیچ‌چیزی از برنامه عوض نمی‌شه)")
    print("=" * 60)
    print(f"🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⚙️ MAX_ERRORS={config.MAX_ERRORS}  SEND_TIMEOUT={config.SEND_TIMEOUT}s  "
          f"RESUME_WAIT={config.RESUME_WAIT}s  RESUME_MAX_RETRIES={config.RESUME_MAX_RETRIES}")
    print(f"⚙️ delay پیش‌فرض={config.DEFAULT_DELAY}s  marker=«{marker}» "
          f"(منبع: {marker_src})  target={target_mode}")
    _line("=")

    await show_public_ip()
    _line()

    client = None
    try:
        client = await connect_account(phone)

        # show recipient stats FIRST so the contact count is ALWAYS visible,
        # even when there's no message in Saved Messages to test-send.
        ordered = await show_recipient_stats(client)

        # find the marked message in Saved Messages; if none, fall back to the
        # latest Saved Messages message so the rate test can still run.
        print(f"🔎 جستجوی مارکر «{marker}» در Saved Messages ...")
        saved_guid, mid = await rb.find_marked_message(client, marker)
        used = "پیامِ مارکر"
        if not mid:
            print("   مارکر پیدا نشد؛ به «آخرین پیامِ Saved Messages» برمی‌گردم تا "
                  "تستِ نرخِ ارسال بازم اجرا شه ...")
            saved_guid, mid = await _latest_saved_message(client)
            used = "آخرین پیامِ Saved Messages"
        if not mid:
            print("   ❌ هیچ پیامی توی Saved Messages این اکانت نیست. (تعدادِ مخاطب "
                  "بالا ☝️ اومد) برای تستِ ارسال، یه پیام توی Saved Messages بذار.")
            return
        print(f"   ✅ پیامِ تست انتخاب شد: message_id={mid}  (منبع: {used})")

        # choose targets
        if target_mode == "contacts":
            if not ordered:
                print("❌ مخاطبی برای تست واقعی نیست.")
                return
            targets_all = [r["guid"] for r in ordered]
            print("⚠️ حالت contacts: این ارسالِ واقعی به مخاطب‌های واقعیه!")
        else:
            # SAFE default: forward the marked message back to OWN Saved Messages
            targets_all = [saved_guid]

        # ============ PART 1: single controlled run ============
        single_res = None
        if run_single:
            print("\n" + "█" * 60)
            print("  بخش ۱: تستِ ارسالِ پشت‌سرهم (single run)")
            print("█" * 60)
            if target_mode == "contacts":
                targets = targets_all[:count]
            else:
                targets = [saved_guid] * count
            single_res = await do_forwards(client, saved_guid, mid, targets,
                                           delay, label="run")

        # ============ PART 2: delay sweep ============
        sweep_results = None
        delays = []
        if run_sweep:
            print("\n" + "█" * 60)
            print("  بخش ۲: SWEEP تاخیرها (کدوم تاخیر throttle نمی‌خوره)")
            print("█" * 60)
            # cool down a bit so PART 1 throttling doesn't bleed into the sweep
            print("  (۱۰ ثانیه استراحت قبل از sweep تا اثر بخش ۱ خنثی شه...)")
            await asyncio.sleep(10)
            delays = [float(x) for x in
                      (_env("TEST_SWEEP_DELAYS", "0.5,1,2,3,5")).split(",")]
            batch = int(_env("TEST_SWEEP_BATCH", "8"))
            sweep_results = {}
            for d in delays:
                if target_mode == "contacts":
                    tg = targets_all[:batch]
                else:
                    tg = [saved_guid] * batch
                sweep_results[d] = await do_forwards(client, saved_guid, mid, tg,
                                                     d, label=f"delay={d}s")
                await asyncio.sleep(3)

        # ============ FINAL REPORT ============
        print("\n" + "=" * 60)
        print("  📊 گزارش نهایی")
        print("=" * 60)
        if single_res is not None:
            r = single_res
            if r["too"] == 0 and r["other"] == 0:
                print(f"• single: هر {r['n']} ارسال موفق با تاخیر {delay}s — "
                      "تو این شرایط throttle نخوردی.")
            elif r["too"]:
                done = (r["first_too_at"] - 1) if r["first_too_at"] else 0
                print(f"• single: بعد از {done} ارسالِ موفق، TOO_REQUESTS اومد. "
                      f"چون MAX_ERRORS={config.MAX_ERRORS}، تو ارسال واقعی خیلی زود "
                      "به سقف خطا می‌خوره و مکث/توقف می‌کنه.")
            if r["other"]:
                print(f"• single: {r['other']} خطای غیرِ throttle (به خطاهای بالا "
                      "نگاه کن، مثلاً INVALID_AUTH = سشن پریده).")
        if sweep_results is not None:
            print("• sweep:")
            for d in delays:
                rr = sweep_results[d]
                verdict = "خوب ✅" if rr["too"] == 0 else f"throttle از #{rr['first_too_at']} 🚫"
                print(f"    delay={d:>4}s -> ✅{rr['ok']}/{rr['n']}  "
                      f"TOO_REQ={rr['too']}  {verdict}")
            good = [d for d in delays if sweep_results[d]["too"] == 0]
            if good:
                print(f"  ✅ پیشنهاد: با تاخیر ≥ {min(good)}s تو این تست throttle نخورد.")
            else:
                print("  ⚠️ تو همهٔ تاخیرها throttle خورد — احتمالاً مشکل از آی‌پی "
                      "سرور یا محدودیتِ شدیدِ همین اکانته، نه از تاخیر.")
    except Exception as e:  # noqa: BLE001
        print(f"\n💥 خطای کلی در تشخیص: {e!r}")
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass
        print("\n✅ تمام شد. (چیزی در برنامه/دیتابیس تغییر نکرد)")


if __name__ == "__main__":
    asyncio.run(main())
