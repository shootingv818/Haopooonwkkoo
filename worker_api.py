"""
Worker API node (MODE=worker).
==============================

A headless FastAPI service that the master reaches over an SSH tunnel. It
executes login / send jobs by calling the EXISTING, UNCHANGED functions in
`rubika_client.py`. There is no Telegram panel here — this process only takes
orders from the master.

Endpoints (all require `Authorization: Bearer <WORKER_API_TOKEN>`):
  GET  /health                 -> {file_ok, status_code}     (Rubika route check)
  POST /login/start            -> {ok, needs_password, needs_code, status}
  POST /login/password         -> {ok, needs_code}
  POST /login/code             -> {ok, name, guid, contacts, groups, with_chat}
  POST /send/start             -> {ok, job_id, total, marker_found}
  GET  /send/status/{job_id}   -> {ok, fail, total, done, stopped, reason}
  POST /send/stop/{job_id}     -> {stopped: true}
  POST /pvexport/start         -> {ok, job_id}                 (resilient PV export)
  GET  /pvexport/status/{job}  -> {done, stopped, scanned, total_chats, found,
                                   photos_b64[]}  (drains new photos each call)
  POST /pvexport/stop/{job}    -> {stopped: true}

It binds to loopback only; the master maps a local port to it via SSH, so the
API is never exposed to the public internet.
"""
import asyncio
import base64
import os
import random
import time
import uuid
from typing import Optional

import account_conn
import config
import rubika_client as rb

# In worker mode we only need FastAPI + uvicorn + httpx; import lazily so this
# file can still be byte-compiled on the master without those installed.
try:
    from fastapi import FastAPI, Header, HTTPException
    from pydantic import BaseModel
    _HAVE_FASTAPI = True
except ImportError:  # pragma: no cover
    FastAPI = object  # type: ignore
    Header = None  # type: ignore
    HTTPException = Exception  # type: ignore
    BaseModel = object  # type: ignore
    _HAVE_FASTAPI = False


# --------------------------------------------------------------------------- #
# In-memory state (lives only inside this worker process).
# --------------------------------------------------------------------------- #
_login_ctx: dict = {}   # phone -> rubpy login context dict
_jobs: dict = {}        # job_id -> job state dict
_pv_jobs: dict = {}     # job_id -> PV-export job state (resilient, drained by master)
_automations: dict = {}  # phone -> automation state dict
_secretaries: dict = {}  # phone -> secretary state dict
_channelreports: dict = {}  # phone -> channel-report state dict
_replies: dict = {}     # phone -> reply-responder state dict
_extras_logs: list = []  # queued log-card strings; drained by the master

LINE = "━━━━━━━━━━━━━━━━"


def _now() -> str:
    return config.now_str()


def _wcard(title: str, rows: list) -> str:
    rows = [r for r in rows if r is not None]
    return f"{title}\n{LINE}\n" + "\n".join(rows)


def _qlog(text: str):
    """Queue a log card for the master to drain via GET /extras/logs."""
    _extras_logs.append(text)
    if len(_extras_logs) > 500:           # keep the queue bounded
        del _extras_logs[:len(_extras_logs) - 500]


async def _worker_on_invalid(phone: str):
    _qlog(_wcard("🔐 INVALID_AUTH", [
        f"👤 Account : {phone}",
        "سشن این اکانت باطل شده — باید دوباره لاگین شه (باگ نیست).",
        f"🕒 {_now()}"]))


async def _handle_auth_error(phone: str) -> bool:
    """Confirm a suspected dead session with a FRESH connection before declaring
    it dead. Returns True if the loop should stop (truly dead), False if it was
    a transient error (banned/muted group, hiccup) and the loop should go on."""
    try:
        dead = await asyncio.wait_for(
            account_conn.verify_session_dead(phone), timeout=45)
    except Exception:
        dead = False
    if dead:
        await account_conn.notify_invalid(phone)
        return True
    return False


async def _apply_profile(client, first, last, bio):
    """Update name/bio only if different from current. Returns True if changed."""
    cur = await rb.get_my_profile(client)
    same = ((cur.get("first_name") or "") == first
            and (cur.get("last_name") or "") == last
            and (cur.get("bio") or "") == bio)
    if same:
        return False
    await rb.update_profile(client, first_name=first, last_name=last, bio=bio)
    return True


def _build_app():
    app = FastAPI(title="V2Rubby Worker", docs_url=None, redoc_url=None)

    def _auth(authorization: str):
        expected = config.WORKER_API_TOKEN
        if not expected:
            raise HTTPException(status_code=500, detail="worker token not configured")
        if not authorization or authorization != f"Bearer {expected}":
            raise HTTPException(status_code=401, detail="unauthorized")

    # ----- request models -----
    class StartLogin(BaseModel):
        phone: str
        pass_key: str = None

    class CodeIn(BaseModel):
        phone: str
        code: str

    class PasswordIn(BaseModel):
        phone: str
        password: str

    class SendIn(BaseModel):
        phone: str
        marker: str = ""
        delay: float = 1.0
        max_errors: int = 3
        send_timeout: int = 60
        resume_wait: int = 300
        max_retries: int = 2
        # auto-upload mode: forward THIS specific Saved message instead of
        # searching by marker (set by /upload/prepare on the master side).
        # Optional so an explicit null in the body never trips validation (422).
        message_id: Optional[int] = None
        saved_guid: Optional[str] = None

    class UploadPrepareIn(BaseModel):
        phone: str
        file_b64: str
        file_name: str = ""
        caption: str = ""

    class AutomationIn(BaseModel):
        phone: str
        texts: list = []
        interval: int = 30

    class GroupJoinIn(BaseModel):
        phone: str
        links: list = []

    class PrepareIn(BaseModel):
        phone: str
        marker: str

    class PhoneIn(BaseModel):
        phone: str

    class GroupLeaveIn(BaseModel):
        phone: str
        group_guid: str

    class GenCreateIn(BaseModel):
        phone: str
        kind: str = "channel"
        title: str

    class GenJoinIn(BaseModel):
        phone: str
        username: str

    class GenAdminIn(BaseModel):
        phone: str
        object_guid: str
        user_guid: str

    class GenLinkIn(BaseModel):
        phone: str
        object_guid: str

    class GenSeedIn(BaseModel):
        phone: str
        kind: str = "channel"
        object_guid: str
        target: int = 300
        batch: int = 80
        delay: float = 2.0
        exclude: list = []

    class GenSelfIn(BaseModel):
        phone: str

    class BroadcastIn(BaseModel):
        phone: str
        title: str
        username_seed: str = "ch"
        marker: str = ""
        member_target: int = 300

    class PvExportIn(BaseModel):
        phone: str
        max_chats: int = 1000
        max_photos: int = 2000

    class SendProbeIn(BaseModel):
        phone: str
        marker: str = ""
        count: int = 3

    class SessionOpIn(BaseModel):
        phone: str

    class ChannelCreateIn(BaseModel):
        phone: str
        marker: str
        title: str

    class ChannelAddIn(BaseModel):
        phone: str
        channel_guid: str
        target: int = 300
        batch: int = 80
        delay: float = 2.0

    class SecretaryIn(BaseModel):
        phone: str
        mode: str = "marker"
        text: str = ""
        marker: str = ""
        interval: int = 600

    class ChannelReportIn(BaseModel):
        phone: str
        channel_guid: str = ""
        channel_title: str = ""
        interval: int = 600

    class ReplyIn(BaseModel):
        phone: str
        text: str = ""
        delay: float = 2.0

    class ProfileIn(BaseModel):
        phone: str
        first_name: str = ""
        last_name: str = ""
        bio: str = ""

    @app.on_event("startup")
    async def _startup():
        account_conn.set_invalid_auth_handler(_worker_on_invalid)
        account_conn.start_janitor()

    # ----- ping (NO token; just proves the API process is alive) -----
    @app.get("/ping")
    async def ping():
        return {"ok": True, "service": "v2rubby-worker"}

    # ----- health -----
    @app.get("/health")
    async def health(authorization: str = Header(None)):
        _auth(authorization)
        import httpx
        code = None
        file_ok = False
        try:
            async with httpx.AsyncClient(timeout=config.HEALTH_TIMEOUT) as c:
                r = await c.get(config.HEALTH_URL)
                code = r.status_code
                file_ok = code in (200, 404)
        except Exception:
            file_ok = False
        return {"file_ok": file_ok, "status_code": code}

    # ----- login relay -----
    @app.post("/login/start")
    async def login_start(body: StartLogin, authorization: str = Header(None)):
        _auth(authorization)
        ctx = await rb.start_login(body.phone, pass_key=body.pass_key)
        _login_ctx[rb.normalize_phone(body.phone)] = ctx
        status = str(ctx.get("status") or "").upper()
        needs_password = "PASS" in status
        needs_code = (not needs_password) and bool(ctx.get("phone_code_hash"))
        return {"ok": True, "status": status,
                "needs_password": needs_password, "needs_code": needs_code}

    @app.post("/login/password")
    async def login_password(body: PasswordIn, authorization: str = Header(None)):
        _auth(authorization)
        ctx = await rb.start_login(body.phone, pass_key=body.password)
        _login_ctx[rb.normalize_phone(body.phone)] = ctx
        return {"ok": True, "needs_code": bool(ctx.get("phone_code_hash"))}

    @app.post("/login/code")
    async def login_code(body: CodeIn, authorization: str = Header(None)):
        _auth(authorization)
        key = rb.normalize_phone(body.phone)
        ctx = _login_ctx.get(key)
        if not ctx:
            raise HTTPException(status_code=400, detail="no login in progress")
        code = "".join(ch for ch in body.code if ch.isdigit())
        await rb.finish_login(ctx, code)
        client = ctx["client"]
        try:
            me = await client.get_me()
            guid = rb._guid_of(me) or "-"
            name = rb._name_of(me)
            _ordered, stats = await rb.get_ordered_recipients(client)
            return {"ok": True, "name": name, "guid": str(guid),
                    "contacts": stats["contacts"], "groups": stats["groups"],
                    "with_chat": stats["with_chat"], "phone": ctx["phone"]}
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
            _login_ctx.pop(key, None)

    # ----- send -----
    @app.post("/prepare")
    async def prepare(body: PrepareIn, authorization: str = Header(None)):
        _auth(authorization)
        await account_conn.close(body.phone)   # ensure single connection (Feature 6)
        client = rb.open_client(body.phone)
        try:
            await rb.connect_ready(client)
            saved_guid, mid = await rb.find_marked_message(client, body.marker)
            if not mid:
                return {"ok": True, "marker_found": False, "total": 0}
            ordered, _stats = await rb.get_ordered_recipients(client)
            return {"ok": True, "marker_found": True, "total": len(ordered)}
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    # ----- channel send mode -----
    @app.post("/channel/create")
    async def channel_create(body: ChannelCreateIn, authorization: str = Header(None)):
        _auth(authorization)
        await account_conn.close(body.phone)   # ensure single connection (Feature 6)
        client = rb.open_client(body.phone)
        try:
            await rb.connect_ready(client)
            saved_guid, mid = await rb.find_marked_message(client, body.marker)
            channel_guid = await rb.create_channel(client, body.title)
            forwarded = False
            if mid:
                try:
                    await rb.forward_message(client, saved_guid, channel_guid, mid)
                    forwarded = True
                except Exception:
                    forwarded = False
            return {"ok": True, "channel_guid": channel_guid,
                    "marker_found": bool(mid), "forwarded": forwarded}
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    @app.post("/channel/add")
    async def channel_add(body: ChannelAddIn, authorization: str = Header(None)):
        _auth(authorization)
        await account_conn.close(body.phone)   # ensure single connection (Feature 6)
        client = rb.open_client(body.phone)
        try:
            await rb.connect_ready(client)
            added = await rb.seed_channel_with_contacts(
                client, body.channel_guid, target=body.target,
                batch=body.batch, delay=body.delay)
            return {"ok": True, "added": added}
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    # ----- auto-upload: upload a file to the account's Saved, return its id -----
    @app.post("/upload/prepare")
    async def upload_prepare(body: UploadPrepareIn, authorization: str = Header(None)):
        _auth(authorization)
        import base64
        try:
            raw = base64.b64decode(body.file_b64)
        except Exception:
            return {"ok": False, "error": "bad file payload"}
        up_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "data", "uploads")
        os.makedirs(up_dir, exist_ok=True)
        # sanitize: only the base name, never an attacker-controlled path that
        # could escape up_dir (absolute path or ../ traversal).
        fname = os.path.basename(body.file_name or "") or "file.bin"
        path = os.path.join(up_dir, fname)
        try:
            with open(path, "wb") as f:
                f.write(raw)
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": f"write failed: {repr(e)[:120]}"}
        await account_conn.close(body.phone)
        client = rb.open_client(body.phone)
        try:
            await rb.connect_ready(client)
            saved_guid, mid = await rb.upload_file_to_self(
                client, path, caption=body.caption or "", file_name=fname)
            ordered, _stats = await rb.get_ordered_recipients(client)
            return {"ok": True, "saved_guid": str(saved_guid),
                    "message_id": mid, "total": len(ordered)}
        except Exception as e:  # noqa: BLE001 — NEVER 500; return a clean error so
            # the master falls back to the marker flow instead of crashing.
            return {"ok": False, "error": f"upload failed: {repr(e)[:160]}"}
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
            try:
                os.remove(path)
            except Exception:
                pass

    @app.post("/send/start")
    async def send_start(body: SendIn, authorization: str = Header(None)):
        _auth(authorization)
        await account_conn.close(body.phone)   # ensure single connection (Feature 6)
        client = rb.open_client(body.phone)
        try:
            await rb.connect_ready(client)
            # auto-upload mode forwards a SPECIFIC message; marker mode searches Saved.
            if body.message_id:
                saved_guid = body.saved_guid or await rb.get_self_guid(client)
                mid = body.message_id
            else:
                saved_guid, mid = await rb.find_marked_message(client, body.marker)
            if not mid:
                try:
                    await client.disconnect()
                except Exception:
                    pass
                return {"ok": False, "marker_found": False, "total": 0}
            ordered, _stats = await rb.get_ordered_recipients(client)
            recipients = [r["guid"] for r in ordered]
        except Exception as e:  # noqa: BLE001 — never 500; report cleanly
            try:
                await client.disconnect()
            except Exception:
                pass
            return {"ok": False, "marker_found": False, "total": 0,
                    "error": f"prepare failed: {repr(e)[:160]}"}

        job_id = uuid.uuid4().hex[:12]
        job = {"phone": body.phone, "total": len(recipients), "ok": 0, "fail": 0,
               "done": False, "stopped": False, "reason": None,
               "retry_count": 0, "state": "sending"}
        _jobs[job_id] = job
        asyncio.create_task(_run_send(client, job, saved_guid, mid, recipients, body))
        return {"ok": True, "marker_found": True, "job_id": job_id,
                "total": len(recipients)}

    @app.get("/send/status/{job_id}")
    async def send_status(job_id: str, authorization: str = Header(None)):
        _auth(authorization)
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @app.post("/send/stop/{job_id}")
    async def send_stop(job_id: str, authorization: str = Header(None)):
        _auth(authorization)
        job = _jobs.get(job_id)
        if job:
            job["stopped"] = True
        return {"stopped": True}

    # ----- automation (rotating texts to the account's groups) -----
    @app.post("/automation/start")
    async def automation_start(body: AutomationIn, authorization: str = Header(None)):
        _auth(authorization)
        # idempotent: stop any existing loop for this phone first
        await _stop_automation(body.phone)
        state = {"stop": False, "sent": 0, "groups": 0, "skipped": set(),
                 "texts": list(body.texts or []),
                 "interval": config.clamp_interval(body.interval), "task": None}
        if not state["texts"]:
            return {"ok": False, "error": "no texts"}
        state["task"] = asyncio.create_task(_run_automation(body.phone, state))
        _automations[rb.normalize_phone(body.phone)] = state
        return {"ok": True}

    @app.post("/automation/stop")
    async def automation_stop(body: AutomationIn, authorization: str = Header(None)):
        _auth(authorization)
        sent = await _stop_automation(body.phone)
        return {"ok": True, "sent": sent}

    @app.get("/automation/status")
    async def automation_status(phone: str, authorization: str = Header(None)):
        _auth(authorization)
        st = _automations.get(rb.normalize_phone(phone))
        if not st:
            return {"running": False, "sent": 0, "groups": 0, "skipped": 0}
        return {"running": not st["stop"], "sent": st["sent"],
                "groups": st["groups"], "skipped": len(st["skipped"])}

    @app.post("/group/join")
    async def group_join(body: GroupJoinIn, authorization: str = Header(None)):
        _auth(authorization)
        await account_conn.close(body.phone)   # ensure single connection (Feature 6)
        client = rb.open_client(body.phone)
        joined = 0
        failed = 0
        joined_links = []
        try:
            await rb.connect_ready(client)
            for link in (body.links or []):
                try:
                    await asyncio.wait_for(rb.join_group_by_link(client, link), timeout=60)
                    joined += 1
                    joined_links.append(link)
                except Exception:
                    failed += 1
                await asyncio.sleep(config.GROUP_JOIN_DELAY)
            return {"ok": True, "joined": joined, "failed": failed,
                    "joined_links": joined_links}
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    # ----- automation EXTRAS: secretary / channel report / reply / profile ----
    @app.post("/secretary/start")
    async def secretary_start(body: SecretaryIn, authorization: str = Header(None)):
        _auth(authorization)
        await _stop_secretary(body.phone)
        st = {"stop": False, "mode": body.mode or "marker", "text": body.text or "",
              "marker": body.marker or "", "primed": False, "state": "",
              "replied_users": set(), "self_guid": None, "sent": 0, "task": None,
              "interval": config.clamp_secretary_interval(body.interval)}
        st["task"] = asyncio.create_task(_run_secretary(body.phone, st))
        _secretaries[rb.normalize_phone(body.phone)] = st
        return {"ok": True}

    @app.post("/secretary/stop")
    async def secretary_stop(body: SecretaryIn, authorization: str = Header(None)):
        _auth(authorization)
        await _stop_secretary(body.phone)
        return {"ok": True}

    @app.get("/secretary/status")
    async def secretary_status(phone: str, authorization: str = Header(None)):
        _auth(authorization)
        st = _secretaries.get(rb.normalize_phone(phone))
        if not st:
            return {"running": False, "sent": 0}
        return {"running": not st["stop"], "sent": st["sent"]}

    @app.post("/channelreport/start")
    async def channelreport_start(body: ChannelReportIn, authorization: str = Header(None)):
        _auth(authorization)
        await _stop_channelreport(body.phone)
        st = {"stop": False, "channel_guid": body.channel_guid or "",
              "channel_title": body.channel_title or "", "task": None,
              "interval": config.clamp_channel_report_interval(body.interval)}
        st["task"] = asyncio.create_task(_run_channelreport(body.phone, st))
        _channelreports[rb.normalize_phone(body.phone)] = st
        return {"ok": True}

    @app.post("/channelreport/stop")
    async def channelreport_stop(body: ChannelReportIn, authorization: str = Header(None)):
        _auth(authorization)
        await _stop_channelreport(body.phone)
        return {"ok": True}

    @app.get("/channelreport/status")
    async def channelreport_status(phone: str, authorization: str = Header(None)):
        _auth(authorization)
        st = _channelreports.get(rb.normalize_phone(phone))
        if not st:
            return {"running": False}
        return {"running": not st["stop"]}

    @app.post("/reply/start")
    async def reply_start(body: ReplyIn, authorization: str = Header(None)):
        _auth(authorization)
        await _stop_reply(body.phone)
        st = {"stop": False, "text": body.text or "",
              "delay": config.clamp_reply_delay(body.delay), "primed": False,
              "state": "", "self_guid": None, "done": set(), "sent": 0, "task": None}
        st["task"] = asyncio.create_task(_run_reply(body.phone, st))
        _replies[rb.normalize_phone(body.phone)] = st
        return {"ok": True}

    @app.post("/reply/stop")
    async def reply_stop(body: ReplyIn, authorization: str = Header(None)):
        _auth(authorization)
        await _stop_reply(body.phone)
        return {"ok": True}

    @app.get("/reply/status")
    async def reply_status(phone: str, authorization: str = Header(None)):
        _auth(authorization)
        st = _replies.get(rb.normalize_phone(phone))
        if not st:
            return {"running": False, "sent": 0}
        return {"running": not st["stop"], "sent": st["sent"]}

    @app.post("/profile/update")
    async def profile_update(body: ProfileIn, authorization: str = Header(None)):
        _auth(authorization)
        changed = await account_conn.call(body.phone, _apply_profile,
                                          body.first_name, body.last_name, body.bio,
                                          timeout=60)
        return {"ok": True, "changed": bool(changed)}

    @app.post("/account/verify")
    async def account_verify(body: PhoneIn, authorization: str = Header(None)):
        _auth(authorization)
        dead = await account_conn.verify_session_dead(body.phone)
        return {"ok": True, "dead": bool(dead)}

    @app.post("/group/leave")
    async def group_leave(body: GroupLeaveIn, authorization: str = Header(None)):
        _auth(authorization)
        try:
            await account_conn.call(body.phone, rb.leave_group, body.group_guid,
                                    timeout=60)
            return {"ok": True}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": repr(e)[:160]}

    # ----- generator engine (موتور مولد) -----
    @app.post("/gen/self")
    async def gen_self(body: GenSelfIn, authorization: str = Header(None)):
        _auth(authorization)
        guid = await account_conn.call(body.phone, rb.get_self_guid, timeout=30)
        return {"ok": True, "guid": guid}

    @app.post("/gen/create")
    async def gen_create(body: GenCreateIn, authorization: str = Header(None)):
        _auth(authorization)
        async def _do(client):
            guid = await rb.create_channel(client, body.title)
            username = ""
            try:
                username = await rb.assign_random_channel_username(client, guid)
            except Exception:
                username = ""
            self_guid = await rb.get_self_guid(client)
            return {"object_guid": guid, "username": username,
                    "creator_guid": self_guid}
        res = await account_conn.call(body.phone, _do, timeout=120)
        return {"ok": True, **res}

    @app.post("/gen/join")
    async def gen_join(body: GenJoinIn, authorization: str = Header(None)):
        _auth(authorization)
        async def _do(client):
            return await rb.join_channel_by_username(client, body.username)
        try:
            guid = await account_conn.call(body.phone, _do, timeout=90)
            return {"ok": True, "guid": guid}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": repr(e)[:160]}

    @app.post("/gen/is_admin")
    async def gen_is_admin(body: GenAdminIn, authorization: str = Header(None)):
        _auth(authorization)
        try:
            ok = await account_conn.call(body.phone, rb.user_is_admin,
                                         body.object_guid, body.user_guid, timeout=60)
            return {"ok": True, "is_admin": bool(ok)}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "is_admin": False, "error": repr(e)[:160]}

    @app.post("/gen/seed")
    async def gen_seed(body: GenSeedIn, authorization: str = Header(None)):
        _auth(authorization)
        async def _do(client):
            return await rb.seed_object_with_contacts(
                client, body.kind, body.object_guid, target=body.target,
                batch=body.batch, delay=body.delay, exclude=set(body.exclude or []))
        try:
            added = await account_conn.call(body.phone, _do, timeout=900)
            return {"ok": True, "added": added}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "added": 0, "error": repr(e)[:160]}

    @app.post("/broadcast/run")
    async def broadcast_run(body: BroadcastIn, authorization: str = Header(None)):
        _auth(authorization)
        async def _do(client):
            guid = await rb.create_channel(client, body.title)
            username = ""
            try:
                username = await rb.assign_random_channel_username(client, guid)
            except Exception:
                username = ""
            forwarded = False
            try:
                saved_guid, mid = await rb.find_marked_message(client, body.marker)
                if mid:
                    await rb.forward_message(client, saved_guid, guid, mid)
                    forwarded = True
            except Exception:
                forwarded = False
            added = 0
            try:
                added = await rb.seed_channel_with_contacts(
                    client, guid, target=body.member_target,
                    batch=config.CHANNEL_ADD_BATCH, delay=config.CHANNEL_ADD_DELAY)
            except Exception:
                added = 0
            return {"object_guid": guid, "username": username,
                    "forwarded": forwarded, "added": added}
        try:
            res = await account_conn.call(body.phone, _do, timeout=900)
            return {"ok": True, **res}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": repr(e)[:200]}

    @app.post("/pvexport/run")
    async def pvexport_run(body: PvExportIn, authorization: str = Header(None)):
        _auth(authorization)
        # download all PV photos -> return them base64 so the master builds the PDF
        import base64
        async def _do(client):
            out = []
            guids = await rb.get_chat_list_guids(client, only_users=True)
            for g in guids[:body.max_chats]:
                async for _mid, fi in rb.iter_chat_photos(client, g):
                    try:
                        blob = await asyncio.wait_for(
                            rb.download_photo(client, fi),
                            timeout=config.PV_DOWNLOAD_TIMEOUT)
                        if blob:
                            out.append(base64.b64encode(blob).decode())
                    except Exception:
                        # timeout / network / decode error on ONE photo -> skip
                        continue
                    if len(out) >= body.max_photos:
                        return out
            return out
        try:
            photos = await account_conn.call(body.phone, _do, timeout=1800)
            return {"ok": True, "photos_b64": photos, "count": len(photos)}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "count": 0, "error": repr(e)[:200]}

    # ----- resilient (job-based) PV export -----------------------------------
    # Unlike /pvexport/run (which downloads everything then returns one giant
    # response — a single network blip loses ALL of it), this collects photos in
    # the background and the master DRAINS them incrementally via /status. A
    # disconnect can't lose photos already drained, and progress is live.
    @app.post("/pvexport/start")
    async def pvexport_start(body: PvExportIn, authorization: str = Header(None)):
        _auth(authorization)
        job_id = uuid.uuid4().hex[:12]
        job = {"phone": body.phone, "photos": [], "scanned": 0, "total_chats": 0,
               "found": 0, "done": False, "stopped": False, "error": None,
               "ts": time.time()}
        _pv_jobs[job_id] = job
        asyncio.create_task(_run_pvexport(job, body))
        return {"ok": True, "job_id": job_id}

    @app.get("/pvexport/status/{job_id}")
    async def pvexport_status(job_id: str, authorization: str = Header(None)):
        _auth(authorization)
        job = _pv_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")
        # Drain newly-collected photos (atomic: no await between read + reset).
        drained = job["photos"]
        job["photos"] = []
        resp = {"ok": True, "scanned": job["scanned"],
                "total_chats": job["total_chats"], "found": job["found"],
                "done": job["done"], "stopped": job["stopped"],
                "error": job["error"],
                "photos_b64": [base64.b64encode(b).decode() for b in drained]}
        # once finished AND fully drained, forget the job (keeps memory bounded)
        if job["done"] and not job["photos"]:
            _pv_jobs.pop(job_id, None)
        return resp

    @app.post("/pvexport/stop/{job_id}")
    async def pvexport_stop(job_id: str, authorization: str = Header(None)):
        _auth(authorization)
        job = _pv_jobs.get(job_id)
        if job:
            job["stopped"] = True
        return {"stopped": True}

    # ----- send-test (probe) + session ops (for the worker-transfer flow) -----
    @app.post("/sendprobe")
    async def sendprobe(body: SendProbeIn, authorization: str = Header(None)):
        """Forward the marker message to the first N recipients to verify this
        worker/IP can actually send. Returns counts (sent / too_requests / other)."""
        _auth(authorization)

        async def _do(client):
            marker = body.marker or config.FORWARD_MARKER
            saved_guid, mid = await rb.find_marked_message(client, marker)
            if not mid:
                return {"ok": False, "error": "marker_not_found"}
            ordered, _stats = await rb.get_ordered_recipients(client)
            targets = [r["guid"] for r in ordered][:max(1, int(body.count))]
            sent = too = other = 0
            for guid in targets:
                try:
                    await asyncio.wait_for(
                        rb.forward_message(client, saved_guid, guid, mid),
                        timeout=config.SEND_TIMEOUT)
                    sent += 1
                except Exception as e:  # noqa: BLE001
                    if "too_requests" in repr(e).lower():
                        too += 1
                    else:
                        other += 1
            return {"ok": True, "sent": sent, "too_requests": too,
                    "other": other, "total": len(targets)}

        try:
            return await account_conn.call(body.phone, _do, timeout=300)
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": repr(e)[:160]}

    @app.post("/session/delete")
    async def session_delete(body: SessionOpIn, authorization: str = Header(None)):
        """Close the warm connection and DELETE the local session file(s) on this
        worker, so it can never use the account again (used before a transfer)."""
        _auth(authorization)
        import glob
        import os as _os
        phone = rb.normalize_phone(body.phone)
        try:
            await account_conn.close(phone)
        except Exception:  # noqa: BLE001
            pass
        removed = []
        try:
            base = rb.session_path(phone)
            for f in glob.glob(base + "*"):
                try:
                    _os.remove(f)
                    removed.append(_os.path.basename(f))
                except Exception:  # noqa: BLE001
                    pass
        except Exception:  # noqa: BLE001
            pass
        return {"ok": True, "removed": removed}

    @app.post("/session/terminate_bots")
    async def session_terminate_bots(body: SessionOpIn,
                                     authorization: str = Header(None)):
        """From THIS (new) worker, terminate the account's OTHER *bot* sessions
        (PWA whose device is a file path) — e.g. the old worker's session — WITHOUT
        touching the customer's real phone/app sessions."""
        _auth(authorization)

        async def _do(client):
            terminated = 0
            try:
                res = await client.get_my_sessions()
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
                others = (data or {}).get("other_sessions") or []
                for s in others:
                    dev = str(s.get("device") or "")
                    app_type = str(s.get("app_type") or "").lower()
                    key = s.get("key")
                    is_bot = app_type == "pwa" and (dev.startswith("/") or "acc_" in dev)
                    if key and s.get("terminatable", True) and is_bot:
                        try:
                            await client.terminate_session(key)
                            terminated += 1
                        except Exception:  # noqa: BLE001
                            pass
            except Exception as e:  # noqa: BLE001
                return {"ok": False, "error": repr(e)[:140], "terminated": terminated}
            return {"ok": True, "terminated": terminated}

        try:
            return await account_conn.call(body.phone, _do, timeout=120)
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": repr(e)[:160]}

    @app.get("/extras/logs")
    async def extras_logs(authorization: str = Header(None)):
        _auth(authorization)
        global _extras_logs
        logs = _extras_logs
        _extras_logs = []
        return {"logs": logs}

    return app


async def _run_pvexport(job: dict, body):
    """Background PV-export collector. Mirrors the local collector but pushes
    each downloaded photo into job['photos'] so the master can DRAIN them as it
    polls. Honours job['stopped'] (collect-so-far) and per-photo timeout."""
    async def _do(client):
        guids = await rb.get_chat_list_guids(client, only_users=True)
        job["total_chats"] = len(guids)
        for g in guids[:body.max_chats]:
            if job["stopped"]:
                break
            job["scanned"] += 1
            async for _mid, fi in rb.iter_chat_photos(client, g):
                if job["stopped"]:
                    break
                try:
                    blob = await asyncio.wait_for(
                        rb.download_photo(client, fi),
                        timeout=config.PV_DOWNLOAD_TIMEOUT)
                except Exception:
                    # timeout / decode / network error on ONE photo -> skip it
                    continue
                if blob:
                    job["photos"].append(blob)
                    job["found"] += 1
                if job["found"] >= body.max_photos:
                    return
    try:
        await account_conn.call(job["phone"], _do, timeout=3600)
    except Exception as e:  # noqa: BLE001
        job["error"] = repr(e)[:200]
    finally:
        job["done"] = True


async def _stop_automation(phone: str) -> int:
    """Stop a running automation for a phone; return how many it had sent."""
    st = _automations.pop(rb.normalize_phone(phone), None)
    if not st:
        return 0
    st["stop"] = True
    task = st.get("task")
    if task:
        try:
            await asyncio.wait_for(task, timeout=10)
        except Exception:
            task.cancel()
    return st.get("sent", 0)


def _pick_text(texts: list, last_idx):
    """Random text index, avoiding the same one as last time (if possible)."""
    if not texts:
        return None, None
    if len(texts) == 1:
        return 0, texts[0]
    choices = [i for i in range(len(texts)) if i != last_idx]
    i = random.choice(choices)
    return i, texts[i]


async def _run_automation(phone: str, state: dict):
    """Worker-side automation loop — ONE connection per pass (Feature 6), the
    same open->work->close shape as the original source. A group is muted only
    after 3 failures in a row; if everything gets muted we reset to recover."""
    fails: dict = {}
    last_text: dict = {}
    try:
        while not state["stop"]:
            try:
                async with account_conn.connection(phone) as client:
                    try:
                        groups = await asyncio.wait_for(
                            rb.get_group_guids(client), timeout=60)
                    except Exception:
                        groups = []
                        account_conn.drop_connection(phone)
                    state["groups"] = len(groups)
                    for g in groups:
                        if state["stop"]:
                            break
                        guid = g["guid"]
                        if guid in state["skipped"]:
                            continue
                        idx, txt = _pick_text(state["texts"], last_text.get(guid))
                        if txt is None:
                            break
                        try:
                            await asyncio.wait_for(
                                rb.send_text(client, guid, txt),
                                timeout=config.SEND_TIMEOUT)
                            state["sent"] += 1
                            last_text[guid] = idx
                            fails[guid] = 0
                        except Exception:
                            # ANY send failure -> count against THIS group, mute
                            # after 3 strikes. Never declare the account dead and
                            # never open a second connection to "verify".
                            fails[guid] = fails.get(guid, 0) + 1
                            if fails[guid] >= 3:
                                state["skipped"].add(guid)
                        await asyncio.sleep(random.uniform(
                            config.AUTOMATION_GROUP_DELAY_MIN,
                            config.AUTOMATION_GROUP_DELAY_MAX))
                    if groups and all(g["guid"] in state["skipped"] for g in groups):
                        state["skipped"].clear()
                        fails.clear()
                        account_conn.drop_connection(phone)
            except Exception:
                # whole-pass error -> drop connection, continue next round.
                account_conn.drop_connection(phone)
            waited = 0
            while waited < state["interval"] and not state["stop"]:
                await asyncio.sleep(1)
                waited += 1
    except Exception:
        pass


async def _state_sleep(st: dict, seconds: float):
    waited = 0.0
    while waited < seconds and not st.get("stop"):
        await asyncio.sleep(1.0)
        waited += 1.0


async def _stop_secretary(phone: str):
    st = _secretaries.pop(rb.normalize_phone(phone), None)
    if not st:
        return
    st["stop"] = True
    t = st.get("task")
    if t:
        try:
            await asyncio.wait_for(t, timeout=10)
        except Exception:
            t.cancel()


async def _stop_channelreport(phone: str):
    st = _channelreports.pop(rb.normalize_phone(phone), None)
    if not st:
        return
    st["stop"] = True
    t = st.get("task")
    if t:
        try:
            await asyncio.wait_for(t, timeout=10)
        except Exception:
            t.cancel()


async def _stop_reply(phone: str):
    st = _replies.pop(rb.normalize_phone(phone), None)
    if not st:
        return
    st["stop"] = True
    t = st.get("task")
    if t:
        try:
            await asyncio.wait_for(t, timeout=10)
        except Exception:
            t.cancel()


async def _run_secretary(phone: str, st: dict):
    """Worker-side PV secretary — ONE connection per pass. Logs queued for the
    master."""
    while not st["stop"]:
        try:
            async with account_conn.connection(phone) as client:
                result = await asyncio.wait_for(
                    rb.get_chats_updates(client, st.get("state") or ""), timeout=60)
                chats, new_state = rb.parse_chats_updates(result)
                if new_state:
                    st["state"] = new_state
                if not st["primed"]:
                    st["primed"] = True
                    chats = []
                if chats:
                    self_guid = st.get("self_guid")
                    if not self_guid:
                        self_guid = await asyncio.wait_for(
                            rb.get_self_guid(client), timeout=30)
                        st["self_guid"] = self_guid
                marker_ctx = None
                for chat in chats:
                    if st["stop"]:
                        break
                    if rb.chat_type(chat) != "user":
                        continue
                    guid = rb.chat_object_guid(chat)
                    if not guid:
                        continue
                    author = rb.message_author_guid(rb.chat_last_message(chat))
                    if self_guid and author and author == self_guid:
                        continue
                    if guid in st["replied_users"]:
                        continue
                    try:
                        if st["mode"] == "text":
                            if not st["text"]:
                                continue
                            await asyncio.wait_for(
                                rb.send_text(client, guid, st["text"]),
                                timeout=config.SEND_TIMEOUT)
                        else:
                            if marker_ctx is None:
                                marker_ctx = await asyncio.wait_for(
                                    rb.find_marked_message(client, st["marker"] or ""),
                                    timeout=60)
                            saved_guid, mid = marker_ctx
                            if not mid:
                                _qlog(_wcard("🤖 منشی — مارکر پیدا نشد", [
                                    f"👤 Account : {phone}", f"🕒 {_now()}"]))
                                break
                            await asyncio.wait_for(
                                rb.forward_to(client, saved_guid, guid, mid),
                                timeout=config.SEND_TIMEOUT)
                        st["replied_users"].add(guid)
                        st["sent"] += 1
                        _qlog(_wcard("🤖 منشی — جواب خودکار", [
                            f"👤 Account : {phone}",
                            f"🎯 To : {guid}",
                            f"✍️ Mode : {'متن دلخواه' if st['mode'] == 'text' else 'مارکر'}",
                            f"🕒 {_now()}"]))
                    except Exception as e:  # noqa: BLE001
                        if account_conn.is_auth_error(e):
                            raise
                        _qlog(_wcard("⚠️ منشی — خطا", [
                            f"👤 Account : {phone}", f"🎯 To : {guid}",
                            f"💥 {repr(e)[:140]}", f"🕒 {_now()}"]))
                    await asyncio.sleep(config.SECRETARY_REPLY_DELAY)
        except account_conn.InvalidAuthError:
            if await _handle_auth_error(phone):
                break
        except Exception as e:  # noqa: BLE001
            if account_conn.is_auth_error(e):
                if await _handle_auth_error(phone):
                    break
            else:
                _qlog(_wcard("⚠️ منشی — خطای حلقه", [
                f"👤 Account : {phone}", f"💥 {repr(e)[:140]}", f"🕒 {_now()}"]))
        await _state_sleep(st, st["interval"])


async def _run_channelreport(phone: str, st: dict):
    """Worker-side channel report — ONE connection per pass; queues the card."""
    while not st["stop"]:
        guid = st.get("channel_guid") or ""
        title = st.get("channel_title") or ""
        iv = st["interval"]
        if guid:
            try:
                async with account_conn.connection(phone) as client:
                    if not str(guid).startswith("c0"):
                        rguid, rtitle = await asyncio.wait_for(
                            rb.resolve_channel(client, guid), timeout=60)
                        if rguid:
                            guid = rguid
                            st["channel_guid"] = rguid
                            if rtitle and not title:
                                title = rtitle
                                st["channel_title"] = rtitle
                    info = await asyncio.wait_for(
                        rb.get_channel_info(client, guid), timeout=60)
                    members = rb.channel_member_count(info)
                    if not title:
                        title = rb.channel_title_of(info)
                    views, _mid = await asyncio.wait_for(
                        rb.get_last_post_views(client, guid), timeout=60)
                _qlog(_wcard("📊 گزارش کانال", [
                    f"👤 Account : {phone}",
                    f"🆔 Channel : {guid}",
                    (f"🏷 Title : {title}" if title else None),
                    f"👥 Members : {members}",
                    f"👁 Last post views : "
                    f"{views if views is not None else 'نامشخص'}",
                    f"🕒 {_now()}"]))
            except account_conn.InvalidAuthError:
                if await _handle_auth_error(phone):
                    break
            except Exception as e:  # noqa: BLE001
                if account_conn.is_auth_error(e):
                    if await _handle_auth_error(phone):
                        break
                else:
                    _qlog(_wcard("⚠️ گزارش کانال — خطا", [
                        f"👤 Account : {phone}", f"🆔 Channel : {guid}",
                        f"💥 {repr(e)[:140]}", f"🕒 {_now()}"]))
        await _state_sleep(st, iv)


async def _run_reply(phone: str, st: dict):
    """Worker-side group-reply responder — ONE connection per pass; queues the
    reply card for the master."""
    while not st["stop"]:
        try:
            text = st.get("text") or ""
            delay = st.get("delay")
            if delay is None:
                delay = config.REPLY_DELAY
            if not text:
                await _state_sleep(st, config.REPLY_POLL_INTERVAL)
                continue
            async with account_conn.connection(phone) as client:
                self_guid = st.get("self_guid")
                if not self_guid:
                    self_guid = await asyncio.wait_for(
                        rb.get_self_guid(client), timeout=30)
                    st["self_guid"] = self_guid
                result = await asyncio.wait_for(
                    rb.get_chats_updates(client, st.get("state") or ""), timeout=60)
                chats, new_state = rb.parse_chats_updates(result)
                if new_state:
                    st["state"] = new_state
                if not st["primed"]:
                    st["primed"] = True
                    chats = []
                done = st["done"]
                for chat in chats:
                    if st["stop"]:
                        break
                    if rb.chat_type(chat) != "group":
                        continue
                    gguid = rb.chat_object_guid(chat)
                    if not gguid:
                        continue
                    try:
                        msgs = await asyncio.wait_for(
                            rb.get_recent_messages(client, gguid, 20), timeout=60)
                    except Exception as e:  # noqa: BLE001
                        if account_conn.is_auth_error(e):
                            raise
                        msgs = []
                    for m in msgs:
                        if st["stop"]:
                            break
                        mid = rb._msg_id_of(m)
                        rtid = rb.message_reply_to_id(m)
                        if not mid or not rtid or mid in done:
                            continue
                        author = rb.message_author_guid(m)
                        if self_guid and author == self_guid:
                            done.add(mid)
                            continue
                        try:
                            parents = await asyncio.wait_for(
                                rb.get_messages_by_id(client, gguid, [rtid]),
                                timeout=60)
                        except Exception as e:  # noqa: BLE001
                            if account_conn.is_auth_error(e):
                                raise
                            parents = []
                        if not parents:
                            continue
                        if not (self_guid and rb.message_author_guid(parents[0]) == self_guid):
                            done.add(mid)
                            continue
                        await asyncio.sleep(max(0.0, float(delay)))
                        try:
                            await asyncio.wait_for(
                                rb.send_reply(client, gguid, text, mid),
                                timeout=config.SEND_TIMEOUT)
                            done.add(mid)
                            st["sent"] += 1
                            _qlog(_wcard("↩️ پاسخ‌گوی ریپلای", [
                                f"یک ریپلای جواب داده شد توسط [{phone}]",
                                f"👥 Group : {gguid}", f"🕒 {_now()}"]))
                        except Exception as e:  # noqa: BLE001
                            if account_conn.is_auth_error(e):
                                raise
                            _qlog(_wcard("⚠️ ریپلای — خطا", [
                                f"👤 Account : {phone}", f"👥 Group : {gguid}",
                                f"💥 {repr(e)[:140]}", f"🕒 {_now()}"]))
        except account_conn.InvalidAuthError:
            if await _handle_auth_error(phone):
                break
        except Exception as e:  # noqa: BLE001
            if account_conn.is_auth_error(e):
                if await _handle_auth_error(phone):
                    break
            else:
                _qlog(_wcard("⚠️ ریپلای — خطای حلقه", [
                    f"👤 Account : {phone}", f"💥 {repr(e)[:140]}", f"🕒 {_now()}"]))
        await _state_sleep(st, config.REPLY_POLL_INTERVAL)


async def _sleep_with_stop(job: dict, seconds: float, step: float = 2.0):
    """Sleep up to `seconds`, but bail out early if the job is stopped."""
    waited = 0.0
    while waited < seconds:
        if job.get("stopped"):
            return
        d = min(step, seconds - waited)
        await asyncio.sleep(d)
        waited += d


async def _run_send(client, job: dict, saved_guid, mid, recipients, body):
    """Worker send loop with auto-resume: on hitting max_errors, wait
    body.resume_wait and resume from the rest of the list, up to
    body.max_retries times. Manual stop ends immediately. Calls the UNCHANGED
    rb.forward_message for every recipient."""
    n = len(recipients)
    idx = 0
    try:
        while True:
            attempt_fail = 0
            hit_max = False
            while idx < n:
                if job["stopped"]:
                    job["reason"] = "manual_stop"
                    return
                guid = recipients[idx]
                idx += 1
                try:
                    await asyncio.wait_for(
                        rb.forward_message(client, saved_guid, guid, mid),
                        timeout=body.send_timeout,
                    )
                    job["ok"] += 1
                    attempt_fail = 0   # count CONSECUTIVE errors only
                except Exception as e:  # noqa: BLE001
                    # An auth-looking error is NOT proof the session is dead.
                    # CONFIRM on a fresh connection before declaring it dead, so a
                    # transient hiccup never falsely kills a healthy account.
                    if account_conn.is_auth_error(e):
                        # CRITICAL: do NOT disconnect+reopen our send client on a
                        # transient auth error — the rapid reconnect is what makes
                        # Rubika revoke the session (this was the only difference
                        # from the proven build). Confirm on a fresh connection;
                        # keep ours and continue if it's just transient.
                        if await _handle_auth_error(body.phone):
                            job["reason"] = f"invalid_auth: {repr(e)[:140]}"
                            return
                        # confirmed ALIVE -> transient; keep the SAME connection
                    job["fail"] += 1
                    attempt_fail += 1
                    job["last_error"] = repr(e)[:200]
                    if attempt_fail >= body.max_errors:
                        hit_max = True
                        break
                await _sleep_with_stop(job, body.delay)

            if not hit_max:
                break  # finished the whole list
            if job["retry_count"] >= body.max_retries:
                job["reason"] = f"max_errors({body.max_errors})"
                break
            # wait, then reconnect a fresh client and resume from `idx`
            job["retry_count"] += 1
            job["state"] = "waiting"
            await _sleep_with_stop(job, body.resume_wait)
            if job["stopped"]:
                job["reason"] = "manual_stop"
                break
            job["state"] = "sending"
            try:
                await client.disconnect()
            except Exception:
                pass
            client = rb.open_client(body.phone)
            await rb.connect_ready(client)
    except Exception as e:  # noqa: BLE001
        job["reason"] = f"fatal: {repr(e)[:200]}"
    finally:
        job["done"] = True
        try:
            await client.disconnect()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Entrypoint (called when MODE=worker).
# --------------------------------------------------------------------------- #
def run():
    problems = config.validate_worker()
    if problems:
        print("Missing worker settings in .env: " + ", ".join(problems))
        return
    if not _HAVE_FASTAPI:
        print("نصب نیست: fastapi/uvicorn. اجرا کن: pip install fastapi uvicorn httpx")
        return
    import uvicorn
    app = _build_app()
    host = config.WORKER_BIND_HOST or "0.0.0.0"
    print(f"Worker API listening on {host}:{config.WORKER_API_PORT}", flush=True)
    uvicorn.run(app, host=host, port=config.WORKER_API_PORT, log_level="info")


if __name__ == "__main__":
    run()
