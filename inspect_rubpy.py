#!/usr/bin/env python3
"""READ-ONLY inspection of what session/logout tools rubpy gives us.

Changes NOTHING (no logout, no delete, no send). It just prints:
  1. rubpy version + install path
  2. whether Client has logout / terminate-session / list-sessions methods
  3. every method on Client whose name hints at session/device/logout
  4. where session files live + which ones exist

Run it on the machine where rubpy is installed:
  * master:  python3 inspect_rubpy.py
  * worker:  docker exec -it v2rubby-worker python3 inspect_rubpy.py

If rubpy isn't installed for this interpreter, it auto-installs once and re-runs.
"""
import glob
import inspect
import os
import subprocess
import sys
import types


# --- self-bootstrap: install rubpy once if missing, then re-exec ----------- #
def _bootstrap():
    if os.getenv("DIAG_BOOTSTRAPPED") == "1":
        return
    try:
        import rubpy  # noqa: F401
        return
    except Exception:  # noqa: BLE001
        pass
    here = os.path.dirname(os.path.abspath(__file__))
    req = os.path.join(here, "requirements.txt")
    print("📦 rubpy نصب نیست — نصب خودکار ...")
    cmd = [sys.executable, "-m", "pip", "install", "-q"]
    cmd += (["-r", req] if os.path.exists(req) else ["rubpy"])
    try:
        subprocess.check_call(cmd)
    except Exception as e:  # noqa: BLE001
        print(f"❌ نصب نشد: {e!r}\n   دستی نصب کن: pip3 install -r requirements.txt")
        sys.exit(1)
    os.environ["DIAG_BOOTSTRAPPED"] = "1"
    print("🔁 نصب شد؛ دوباره اجرا می‌شه ...")
    os.execv(sys.executable, [sys.executable] + sys.argv)


_bootstrap()

# config.py (imported indirectly via rubika_client) needs dotenv; shim it.
try:
    import dotenv  # noqa: F401
except ModuleNotFoundError:
    def _ld(path=None, *a, **k):
        for p in ([path] if path else [".env", os.path.join(
                os.path.dirname(os.path.abspath(__file__)), ".env")]):
            try:
                with open(p) as fh:
                    for line in fh:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        kk, vv = line.split("=", 1)
                        os.environ.setdefault(
                            kk.strip(), vv.strip().strip('"').strip("'"))
                return True
            except FileNotFoundError:
                continue
        return False
    _m = types.ModuleType("dotenv")
    _m.load_dotenv = _ld
    sys.modules["dotenv"] = _m


KEYWORDS = ("logout", "log_out", "signout", "sign_out", "terminate",
            "session", "sessions", "device", "devices", "kill", "revoke",
            "disconnect")

# the exact methods we care about for a CLEAN transfer (logout old worker)
WANTED = ("logout", "log_out", "sign_out", "signout",
          "terminate_session", "terminate_sessions",
          "get_my_sessions", "get_sessions", "get_active_sessions",
          "delete_session", "revoke_session", "disconnect")


def _line(c="─", n=64):
    print(c * n)


def show_related(obj, label):
    print(f"\n=== متدهای مرتبط با سشن/لاگ‌اوت در {label} ===")
    hits = [n for n in sorted(set(dir(obj)))
            if any(k in n.lower() for k in KEYWORDS)]
    if not hits:
        print("  (هیچ متدِ مرتبطی پیدا نشد)")
        return
    for n in hits:
        try:
            attr = getattr(obj, n)
            sig = ""
            if callable(attr):
                try:
                    sig = str(inspect.signature(attr))
                except (ValueError, TypeError):
                    sig = "(...)"
            print(f"  • {n}{sig}")
        except Exception as e:  # noqa: BLE001
            print(f"  • {n}  (?: {e!r})")


def main():
    print("=" * 64)
    print("  RUBPY SESSION/LOGOUT INSPECTION  (فقط‌خواندنی — هیچی عوض نمی‌شه)")
    print("=" * 64)

    import rubpy
    from rubpy import Client
    print(f"rubpy version : {getattr(rubpy, '__version__', '?')}")
    print(f"rubpy path    : {getattr(rubpy, '__file__', '?')}")
    print(f"python        : {sys.version.split()[0]}  @ {sys.executable}")

    # crisp yes/no for the methods that matter
    print("\n=== آیا متدهای کلیدی روی Client هست؟ ===")
    for name in WANTED:
        print(f"  has {name:<20}: {hasattr(Client, name)}")

    # everything session/logout-ish, with signatures
    show_related(Client, "rubpy.Client (کلاس)")

    # full method list (grep-able), so we don't miss an oddly-named one
    print("\n=== همهٔ متدهای Client (برای مرورِ دستی) ===")
    allm = [n for n in sorted(dir(Client)) if not n.startswith("_")]
    print("  " + ", ".join(allm))

    # session files on disk
    sd = None
    try:
        import rubika_client as rb
        sd = getattr(rb, "SESSIONS_DIR", None)
    except Exception as e:  # noqa: BLE001
        print(f"\n(نتونستم rubika_client رو لود کنم تا SESSIONS_DIR رو بگیرم: {e!r})")
    if sd:
        _line()
        print(f"📁 SESSIONS_DIR : {sd}")
        files = sorted(glob.glob(os.path.join(sd, "*")))
        if files:
            print(f"   {len(files)} مورد:")
            for f in files:
                try:
                    sz = os.path.getsize(f)
                except OSError:
                    sz = -1
                print(f"   • {os.path.basename(f)}  ({sz} bytes)")
        else:
            print("   (خالیه یا مسیر وجود نداره)")

    print("\n✅ تمام شد. (هیچ‌چیزی تغییر نکرد)")


def dump_sessions(phone):
    """READ-ONLY: connect ONE account and print get_my_sessions() raw shape so we
    can see how to identify the CURRENT session key. Terminates nothing."""
    import asyncio
    import rubika_client as rb

    async def _go():
        print("\n" + "=" * 64)
        print(f"  DUMP get_my_sessions() برای {phone}  (فقط چاپ — هیچی بسته نمی‌شه)")
        print("=" * 64)
        client = rb.open_client(rb.normalize_phone(phone))
        await rb.connect_ready(client)
        try:
            res = await client.get_my_sessions()
            # try the common shapes without assuming the exact type
            data = None
            for attr in ("to_dict", "original_update"):
                obj = getattr(res, attr, None)
                if callable(obj):
                    try:
                        data = obj()
                        break
                    except Exception:  # noqa: BLE001
                        pass
                elif obj is not None:
                    data = obj
                    break
            print("repr:", repr(res)[:1500])
            if data is not None:
                import json
                try:
                    print("\nas dict/json:")
                    print(json.dumps(data, ensure_ascii=False, indent=2)[:3500])
                except Exception:
                    print("data:", str(data)[:3500])
            for attr in ("sessions", "session", "current_session"):
                val = getattr(res, attr, None)
                if val is not None:
                    print(f"\nres.{attr} =", str(val)[:2000])
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
        print("\n✅ تمام شد. (هیچ سشنی بسته نشد)")

    asyncio.run(_go())


if __name__ == "__main__":
    main()
    _ph = os.getenv("TEST_PHONE") or (sys.argv[1] if len(sys.argv) > 1 else "")
    if _ph:
        dump_sessions(_ph)
