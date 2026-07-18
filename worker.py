"""
Worker subsystem (master side).
================================

This module lets the MASTER orchestrate one or more WORKER servers so that
login / send jobs are spread across several clean IPs. It deliberately does
NOT touch the sending/login logic in `rubika_client.py`; workers run that
SAME unchanged code behind a small API (see `worker_api.py`).

Responsibilities here (master side):
  * generate worker tags  (e.g. "#W8_819"),
  * provision a fresh server over SSH + Docker,
  * keep a secure SSH tunnel to each worker's loopback-only API,
  * call the worker API (login relay, send, health),
  * round-robin selection of a worker for a NEW account, with failover,
  * health checks (run in parallel) + a tiny in-memory cache,
  * pull worker session files into a backup zip.

Heavy third-party deps (asyncssh, httpx) are imported lazily INSIDE the
functions that need them, so simply importing this module (e.g. from the
backup hook in bot.py) never fails on a machine without them installed.
"""
from __future__ import annotations

import asyncio
import random
import secrets
import time

import config
import crypto_util
import db

# Where the worker checkout / data live on the remote server.
REMOTE_DIR = "~/v2rubby_worker"
REMOTE_DATA = "~/v2rubby_worker_data"
CONTAINER = "v2rubby-worker"
IMAGE = "v2rubby-worker"

# in-memory: worker_id -> {"conn":.., "listener":.., "local_port":int}
_tunnels: dict = {}
_tunnel_locks: dict = {}
# in-memory health cache: worker_id -> {"status","ping_ms","file_ok","ts"}
_health_cache: dict = {}
# in-memory last failure reason per worker (diagnostic), worker_id -> str|None
_health_detail: dict = {}


# --------------------------------------------------------------------------- #
# Tags
# --------------------------------------------------------------------------- #
def gen_tag(is_master: bool = False) -> str:
    """Random worker tag like '#W8_819'. Master uses the '#W0_xxx' family."""
    existing = {w["tag"] for w in db.list_workers()}
    for _ in range(200):
        lead = "0" if is_master else str(random.randint(1, 9))
        tag = f"#W{lead}_{random.randint(100, 999)}"
        if tag not in existing:
            return tag
    return f"#W{secrets.token_hex(2)}"


# --------------------------------------------------------------------------- #
# Master-as-worker bootstrap
# --------------------------------------------------------------------------- #
def ensure_master_worker() -> dict:
    """Make sure a local 'master' worker row exists (runs jobs in-process)."""
    m = db.get_master_worker()
    if m:
        return m
    if not config.MASTER_AS_WORKER:
        return None
    tag = gen_tag(is_master=True)
    wid = db.add_worker(
        tag=tag, ip="local", ssh_port=0, ssh_user="", ssh_pass_enc="",
        api_port=0, api_token_enc="", is_master=1,
    )
    return db.get_worker(wid)


def is_local(worker: dict) -> bool:
    return bool(worker and worker.get("is_master"))


# --------------------------------------------------------------------------- #
# Colour / formatting helpers (shared with bot.py logging)
# --------------------------------------------------------------------------- #
def status_emoji(worker: dict) -> str:
    if not worker.get("file_ok"):
        return "🔴"
    ping = worker.get("ping_ms", -1)
    if ping is None or ping < 0:
        return "🟡"
    if ping <= config.PING_GREEN_MS:
        return "🟢"
    if ping <= config.PING_YELLOW_MS:
        return "🟡"
    return "🔴"


def file_label(worker: dict) -> str:
    return "File ok" if worker.get("file_ok") else "Blocked"


# --------------------------------------------------------------------------- #
# Low-level SSH helpers (asyncssh, lazy import)
# --------------------------------------------------------------------------- #
async def _ssh_connect(ip: str, port: int, user: str, password: str):
    import asyncssh  # lazy
    return await asyncssh.connect(
        host=ip, port=int(port or 22), username=user, password=password,
        known_hosts=None,  # personal tool: trust on first use
    )


async def _run(conn, command: str, check: bool = False):
    """Run a command over an open SSH connection -> (exit_status, stdout, stderr)."""
    res = await conn.run(command, check=check)
    return res.exit_status, (res.stdout or ""), (res.stderr or "")


# --------------------------------------------------------------------------- #
# Provisioning: SSH in, install Docker, clone repo, build + run worker.
# `on_progress` is an async callback(str) for live updates in Telegram.
# Returns dict {ok, tag, api_port, api_token, error}.
# --------------------------------------------------------------------------- #
async def provision_worker(ip: str, ssh_port: int, ssh_user: str, ssh_pass: str,
                           tag: str = None, on_progress=None) -> dict:
    async def say(msg: str):
        if on_progress:
            try:
                await on_progress(msg)
            except Exception:
                pass

    api_port = config.WORKER_API_PORT
    api_token = secrets.token_urlsafe(24)
    tag = tag or gen_tag()

    try:
        import asyncssh  # noqa: F401  (fail early with a clear message)
    except ImportError:
        return {"ok": False, "error": "بسته‌ی asyncssh روی مستر نصب نیست (pip install asyncssh)."}

    conn = None
    try:
        await say("🔌 اتصال SSH به سرور ...")
        conn = await _ssh_connect(ip, ssh_port, ssh_user, ssh_pass)

        await say("🐳 بررسی/نصب Docker (با صبر برای قفلِ apt) ...")
        # Fresh Ubuntu servers run unattended-upgrades right after boot, which
        # holds the dpkg lock for minutes; that previously made the docker
        # install silently fail and the later `docker build` die with
        # "docker: command not found". So: stop auto-upgrades for this run, tell
        # apt to WAIT for the lock (DPkg::Lock::Timeout), install docker from the
        # distro repo (most reliable), fall back to get.docker.com, then VERIFY.
        install_script = (
            "export DEBIAN_FRONTEND=noninteractive\n"
            "systemctl stop unattended-upgrades >/dev/null 2>&1 || true\n"
            "if ! command -v docker >/dev/null 2>&1; then\n"
            "  apt-get -o DPkg::Lock::Timeout=180 update -qq || true\n"
            "  apt-get -o DPkg::Lock::Timeout=180 install -y -qq "
            "ca-certificates curl git docker.io || true\n"
            "fi\n"
            "command -v docker >/dev/null 2>&1 || "
            "{ curl -fsSL https://get.docker.com | sh; } || true\n"
            "command -v git >/dev/null 2>&1 || "
            "apt-get -o DPkg::Lock::Timeout=180 install -y -qq git || true\n"
            "systemctl enable --now docker >/dev/null 2>&1 || true\n"
            "if command -v docker >/dev/null 2>&1; then docker --version; "
            "echo DOCKER_OK; else echo DOCKER_MISSING; fi\n"
        )
        code, out, err = await _run(conn, install_script)
        if "DOCKER_OK" not in (out or ""):
            return {"ok": False,
                    "error": ("نصبِ Docker روی سرور ناموفق بود (احتمالاً قفلِ apt یا "
                              "نبودِ اینترنت). روی همون سرور دستی بزن:  "
                              "apt-get install -y docker.io && systemctl enable --now docker  "
                              "بعد دوباره «افزودن ورکر». جزئیات: "
                              + ((err or out) or "")[-300:])}

        await say("📥 دریافت سورس از گیت‌هاب ...")
        code, out, err = await _run(
            conn,
            f"rm -rf {REMOTE_DIR} && "
            f"git clone --depth 1 -b {config.GIT_BRANCH} {config.GIT_REPO_URL} {REMOTE_DIR}",
        )
        if code != 0:
            return {"ok": False, "error": f"git clone شکست خورد: {err[:200] or out[:200]}"}

        await say("📝 نوشتن تنظیمات ورکر (.env) ...")
        env_lines = (
            "MODE=worker\n"
            f"WORKER_API_TOKEN={api_token}\n"
            f"WORKER_API_PORT={api_port}\n"
            # With host networking the API binds to the host's loopback, which
            # is private (only the master's SSH tunnel reaches it).
            "WORKER_BIND_HOST=127.0.0.1\n"
            f"TIMEZONE={config.TIMEZONE}\n"
        )
        # write .env safely via a heredoc
        await _run(conn, f"mkdir -p {REMOTE_DATA}")
        await _run(
            conn,
            f"cat > {REMOTE_DIR}/.env <<'ENVEOF'\n{env_lines}ENVEOF",
        )

        await say("🏗 ساخت ایمیج Docker (ممکنه چند دقیقه طول بکشه) ...")
        # --network=host lets build steps use the SERVER's network/DNS, which
        # avoids the common "Docker build container can't resolve DNS / reach
        # PyPI" failure on fresh servers.
        code, out, err = await _run(
            conn, f"cd {REMOTE_DIR} && docker build --network=host -t {IMAGE} .")
        if code != 0:
            return {"ok": False,
                    "error": f"docker build شکست خورد: {(err or out)[-600:]}"}

        await say("🚀 اجرای کانتینر ورکر ...")
        # --network=host so the container uses the SERVER's DNS/network. The
        # default bridge network has broken DNS on many fresh servers, which
        # would make the worker unable to resolve Rubika (-> always "Blocked").
        # With host networking, WORKER_BIND_HOST=127.0.0.1 keeps the API private
        # (only the master's SSH tunnel can reach it).
        run_cmd = (
            f"docker rm -f {CONTAINER} 2>/dev/null; "
            f"docker run -d --name {CONTAINER} --restart always "
            f"--network=host "
            f"--env-file {REMOTE_DIR}/.env "
            f"-v {REMOTE_DATA}:/app/data {IMAGE}"
        )
        code, out, err = await _run(conn, run_cmd)
        if code != 0:
            return {"ok": False, "error": f"docker run شکست خورد: {err[:200] or out[:200]}"}

        await say("✅ نصب کامل شد.")
        return {"ok": True, "tag": tag, "api_port": api_port, "api_token": api_token}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


async def register_provisioned(ip, ssh_port, ssh_user, ssh_pass, prov: dict) -> int:
    """Persist a successfully provisioned worker (encrypting secrets)."""
    return db.add_worker(
        tag=prov["tag"], ip=ip, ssh_port=int(ssh_port or 22), ssh_user=ssh_user,
        ssh_pass_enc=crypto_util.encrypt(ssh_pass),
        api_port=int(prov["api_port"]),
        api_token_enc=crypto_util.encrypt(prov["api_token"]),
        is_master=0,
    )


# --------------------------------------------------------------------------- #
# Remote lifecycle ops (restart / update / teardown) over SSH.
# --------------------------------------------------------------------------- #
async def _with_conn(worker: dict):
    return await _ssh_connect(
        worker["ip"], worker["ssh_port"], worker["ssh_user"],
        crypto_util.decrypt(worker["ssh_pass_enc"]),
    )


async def restart_worker(worker: dict) -> tuple:
    conn = await _with_conn(worker)
    try:
        return await _run(conn, f"docker restart {CONTAINER}")
    finally:
        conn.close()


async def update_worker(worker: dict) -> tuple:
    """Force the worker checkout to config.GIT_BRANCH's latest, rebuild the
    image, recreate the container. Robust to a worker stuck on the wrong branch
    (uses fetch + checkout -B FETCH_HEAD) and surfaces a build failure as a
    non-zero exit (so a silent old image isn't reported as success)."""
    conn = await _with_conn(worker)
    try:
        br = config.GIT_BRANCH
        cmd = (
            f"cd {REMOTE_DIR} && "
            f"git fetch origin {br} && "
            f"git checkout -B {br} FETCH_HEAD && "
            f"docker build --network=host -t {IMAGE} . && "
            f"(docker rm -f {CONTAINER} 2>/dev/null || true) && "
            f"docker run -d --name {CONTAINER} --restart always --network=host "
            f"--env-file {REMOTE_DIR}/.env -v {REMOTE_DATA}:/app/data {IMAGE}"
        )
        return await _run(conn, cmd)
    finally:
        conn.close()


def master_code_version() -> str:
    """Short git commit of the MASTER's own checkout (what workers should match)."""
    import os
    import subprocess
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True, text=True, timeout=5)
        return (out.stdout or "").strip() or "?"
    except Exception:
        return "?"


async def worker_code_version(worker: dict) -> str:
    """SSH into a remote worker and return its checkout's short commit (or '?')."""
    if worker.get("is_master"):
        return master_code_version()
    conn = None
    try:
        conn = await _with_conn(worker)
        _code, out, _err = await _run(
            conn, f"cd {REMOTE_DIR} && git rev-parse --short HEAD 2>/dev/null")
        return (out or "").strip() or "?"
    except Exception:
        return "?"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


async def teardown_worker(worker: dict):
    """Stop + remove the container and the checkout on the remote server."""
    await close_tunnel(worker["id"])
    try:
        conn = await _with_conn(worker)
        try:
            await _run(conn, f"docker rm -f {CONTAINER} 2>/dev/null; rm -rf {REMOTE_DIR}")
        finally:
            conn.close()
    except Exception:
        pass  # best-effort cleanup; still remove from DB by caller


# --------------------------------------------------------------------------- #
# SSH tunnel to the worker's loopback-only API.
# --------------------------------------------------------------------------- #
def _lock_for(worker_id: int) -> asyncio.Lock:
    if worker_id not in _tunnel_locks:
        _tunnel_locks[worker_id] = asyncio.Lock()
    return _tunnel_locks[worker_id]


async def open_tunnel(worker: dict) -> int:
    """Open (or reuse) an SSH local-port-forward to the worker API.
    Returns the local port on the master that maps to the worker's API.
    """
    wid = worker["id"]
    async with _lock_for(wid):
        existing = _tunnels.get(wid)
        if existing:
            return existing["local_port"]
        conn = await _with_conn(worker)
        listener = await conn.forward_local_port(
            "127.0.0.1", 0, "127.0.0.1", int(worker["api_port"]),
        )
        local_port = listener.get_port()
        _tunnels[wid] = {"conn": conn, "listener": listener, "local_port": local_port}
        return local_port


async def close_tunnel(worker_id: int):
    t = _tunnels.pop(worker_id, None)
    if not t:
        return
    try:
        t["listener"].close()
    except Exception:
        pass
    try:
        t["conn"].close()
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# API client (master -> worker, through the tunnel).
# --------------------------------------------------------------------------- #
async def api_call(worker: dict, method: str, path: str, payload: dict = None,
                   timeout: int = 120) -> dict:
    """Call the worker API. Raises on transport/HTTP error."""
    import httpx  # lazy
    local_port = await open_tunnel(worker)
    token = crypto_util.decrypt(worker["api_token_enc"])
    url = f"http://127.0.0.1:{local_port}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, json=payload, headers=headers)
            resp.raise_for_status()
            return resp.json()
    except Exception:
        # a broken tunnel is the usual cause -> drop it so next call reopens
        await close_tunnel(worker["id"])
        raise


# --------------------------------------------------------------------------- #
# Health checks (parallel + cache).
# --------------------------------------------------------------------------- #
async def _tcp_ping(host: str, port: int, timeout: float = 5.0) -> int:
    """Return latency in ms to open a TCP connection, or -1 on failure."""
    start = time.monotonic()
    try:
        fut = asyncio.open_connection(host, int(port or 22))
        reader, writer = await asyncio.wait_for(fut, timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return int((time.monotonic() - start) * 1000)
    except Exception:
        return -1


async def _local_file_ok() -> bool:
    """Master-side check: GET HEALTH_URL directly (200/404 ok, 503 blocked)."""
    import httpx  # lazy
    try:
        async with httpx.AsyncClient(timeout=config.HEALTH_TIMEOUT) as c:
            r = await c.get(config.HEALTH_URL)
            return r.status_code in (200, 404)
    except Exception:
        return False


async def check_worker(worker: dict) -> dict:
    """Measure one worker's health, persist it, update cache, return summary."""
    wid = worker["id"]
    if is_local(worker):
        ping = await _tcp_ping("127.0.0.1", config.WORKER_API_PORT or 22)
        if ping < 0:
            ping = 1  # localhost is reachable even if API port closed
        file_ok = await _local_file_ok()
    else:
        ping = await _tcp_ping(worker["ip"], worker["ssh_port"])
        file_ok = False
        detail = None
        if ping < 0:
            detail = "ssh unreachable"
        else:
            try:
                data = await api_call(worker, "GET", "/health",
                                      timeout=config.HEALTH_TIMEOUT + 10)
                file_ok = bool(data.get("file_ok"))
                if not file_ok:
                    # API answered but Rubika check failed -> show its status code
                    detail = f"rubika http={data.get('status_code')}"
            except Exception as e:  # noqa: BLE001
                # API itself unreachable through the tunnel
                detail = f"api error: {type(e).__name__}: {str(e)[:120]}"
        _health_detail[wid] = detail

    status = "ok" if (ping >= 0 and file_ok) else ("blocked" if ping >= 0 else "down")
    db.update_worker_health(wid, status, ping, file_ok)
    summary = {"id": wid, "tag": worker["tag"], "ip": worker["ip"],
               "status": status, "ping_ms": ping, "file_ok": file_ok,
               "detail": _health_detail.get(wid), "ts": config.now_str()}
    _health_cache[wid] = summary
    return summary


async def check_all(workers: list = None) -> list:
    """Run health checks for all (enabled) workers IN PARALLEL."""
    if workers is None:
        workers = db.list_workers()
    if not workers:
        return []
    results = await asyncio.gather(*[check_worker(w) for w in workers],
                                   return_exceptions=True)
    out = []
    for w, r in zip(workers, results):
        if isinstance(r, Exception):
            out.append({"id": w["id"], "tag": w["tag"], "ip": w["ip"],
                        "status": "down", "ping_ms": -1, "file_ok": False,
                        "detail": f"check crashed: {type(r).__name__}",
                        "ts": config.now_str()})
        else:
            out.append(r)
    return out


def health_detail(worker_id: int):
    """Last diagnostic reason for a worker being unhealthy (or None)."""
    return _health_detail.get(worker_id)


def cached_health(worker_id: int):
    return _health_cache.get(worker_id)


def is_healthy(worker: dict) -> bool:
    c = _health_cache.get(worker["id"])
    if c:
        return c["status"] == "ok"
    # never checked yet -> treat enabled workers as tentatively usable
    return bool(worker.get("enabled"))


# --------------------------------------------------------------------------- #
# Selection: round-robin with failover for a NEW account login.
# --------------------------------------------------------------------------- #
async def pick_worker_for_login(verify: bool = True, exclude_id: int = None) -> dict:
    """Choose the healthy enabled worker with the fewest accounts (= round-robin
    as accounts are added one at a time). Verifies health right before use.
    Pass exclude_id to skip a specific worker (used by the transfer flow).
    Returns a worker dict or None if none are usable.
    """
    # Make sure a master row exists (creates it once if missing), but routing
    # uses only ENABLED workers, so a disabled local master is respected.
    ensure_master_worker()
    workers = db.list_enabled_workers()
    if not workers:
        return None

    remotes = [w for w in workers if not is_local(w)]
    # Only spend time on health checks when there are real remote workers;
    # a local-only (master-as-worker) setup behaves exactly like before.
    if verify and remotes:
        await check_all(workers)
        workers = db.list_enabled_workers()  # reload fresh health

    def load(w):
        return db.count_accounts_on_worker(w["id"])

    # local master is always usable; remotes must be healthy ("ok").
    pool = [w for w in workers if (is_local(w) or w.get("status") == "ok")]
    if exclude_id is not None:
        pool = [w for w in pool if w.get("id") != exclude_id]
    if not pool:
        return None
    pool.sort(key=lambda w: (load(w), w["id"]))
    return pool[0]


def worker_for_account(account: dict) -> dict:
    """The worker that owns an account (session affinity)."""
    wid = account.get("worker_id")
    if wid:
        return db.get_worker(int(wid))
    return db.get_master_worker()


# --------------------------------------------------------------------------- #
# Backup hook: pull each remote worker's session files into the zip.
# Called by bot.build_backup_archive() via _add_worker_sessions().
# --------------------------------------------------------------------------- #
async def collect_sessions_into_zip(zf):
    """Download every non-master worker's session files into zf under
    'sessions/<tag>/'. Best-effort; never raises out."""
    try:
        import asyncssh  # noqa: F401
    except ImportError:
        return
    for w in db.list_workers():
        if is_local(w):
            continue
        try:
            conn = await _with_conn(w)
        except Exception:
            continue
        try:
            sftp = await conn.start_sftp_client()
            remote_sessions = f"{REMOTE_DATA}/sessions"
            try:
                names = await sftp.listdir(remote_sessions)
            except Exception:
                names = []
            for name in names:
                if name in (".", ".."):
                    continue
                rpath = f"{remote_sessions}/{name}"
                try:
                    data = await _read_remote_file(sftp, rpath)
                    safe_tag = w["tag"].replace("#", "").replace("/", "_")
                    zf.writestr(f"sessions/{safe_tag}/{name}", data)
                except Exception:
                    continue
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass


async def _read_remote_file(sftp, path: str) -> bytes:
    async with sftp.open(path, "rb") as f:
        return await f.read()


async def shutdown():
    """Close all open tunnels (call on master shutdown)."""
    for wid in list(_tunnels.keys()):
        await close_tunnel(wid)
