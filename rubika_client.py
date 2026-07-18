"""
Rubika integration layer (wraps the `rubpy` library, v7.x).
===========================================================

Scope of THIS project (on purpose):
  * logs into the USER'S OWN account (phone + code + optional 2FA),
  * reads the account's own contacts (paginated),
  * finds a message the user marked in their OWN Saved Messages,
  * FORWARDS that message to a list of the user's own contacts.

There is intentionally NO proxy support, NO multi-account orchestration and
NO batching/anti-rate-limit machinery here. This is a small personal tool.

All rubpy-specific calls live in this file. rubpy is unofficial, so method
names / response shapes can differ between versions; the helpers below are
written defensively for that reason.
"""
import asyncio
import inspect
import os

from rubpy import Client
from rubpy.crypto import Crypto

SESSIONS_DIR = os.path.join(os.path.dirname(__file__), "data", "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)


def session_path(phone: str) -> str:
    safe = phone.replace("+", "").replace(" ", "")
    return os.path.join(SESSIONS_DIR, f"acc_{safe}")


def normalize_phone(phone: str) -> str:
    """Rubika expects digits with country code, no '+' and no leading 0.
    '+989121234567' -> '989121234567', '09121234567' -> '989121234567'
    """
    p = "".join(ch for ch in phone if ch.isdigit())
    if p.startswith("0"):
        p = "98" + p[1:]
    return p


def _make_client(name: str) -> Client:
    return Client(name=name)


def open_client(phone: str) -> Client:
    """Return a rubpy client bound to the account's SAVED session."""
    return _make_client(session_path(phone))


# --------------------------------------------------------------------------- #
# Connect + rebuild the signing material that rubpy's connect() can omit.
# --------------------------------------------------------------------------- #
async def connect_ready(client: Client):
    await client.connect()
    auth = getattr(client, "auth", None)
    private_key = getattr(client, "private_key", None)
    try:
        if auth is not None and getattr(client, "key", None) in (None, ""):
            client.key = Crypto.passphrase(auth)
    except Exception:
        pass
    try:
        if auth is not None:
            client.decode_auth = Crypto.decode_auth(auth)
    except Exception:
        pass
    try:
        if private_key is not None and getattr(client, "import_key", None) is None:
            ik = _import_key_from_private(private_key)
            if ik is not None:
                client.import_key = ik
    except Exception:
        pass
    return client


# --------------------------------------------------------------------------- #
# Programmatic login (mirrors rubpy's own start.py flow)
# --------------------------------------------------------------------------- #
def _get(obj, *names):
    for n in names:
        v = getattr(obj, n, None)
        if v not in (None, ""):
            return v
        if isinstance(obj, dict) and obj.get(n) not in (None, ""):
            return obj.get(n)
    return None


def _import_key_from_private(private_key):
    """Build the signing key exactly like rubpy start.py does."""
    try:
        from Crypto.PublicKey import RSA
        from Crypto.Signature import pkcs1_15
        if private_key is not None:
            return pkcs1_15.new(RSA.import_key(private_key.encode()))
    except Exception:
        pass
    return None


async def start_login(phone: str, pass_key: str = None) -> dict:
    """Phase 1: connect + request the login code (handles 2FA pass_key)."""
    phone = normalize_phone(phone)
    client = _make_client(session_path(phone))
    await client.connect()

    public_key, private_key = Crypto.create_keys()

    if pass_key:
        result = await client.send_code(phone_number=phone, pass_key=pass_key)
    else:
        result = await client.send_code(phone_number=phone)

    return {
        "client": client,
        "phone": phone,
        "status": _get(result, "status") or "",
        "phone_code_hash": _get(result, "phone_code_hash"),
        "hint": _get(result, "hint_pass_key"),
        "public_key": public_key,
        "private_key": private_key,
    }


async def finish_login(ctx: dict, code: str):
    """Phase 2: sign in with the code, then replicate rubpy start.py steps."""
    client: Client = ctx["client"]
    phone = ctx["phone"]
    private_key = ctx["private_key"]

    result = await client.sign_in(
        phone_code=code,
        phone_number=phone,
        phone_code_hash=ctx["phone_code_hash"],
        public_key=ctx["public_key"],
    )

    status = _get(result, "status") or ""
    if str(status).upper() not in ("OK", ""):
        raise RuntimeError(f"sign_in status: {status}")

    enc_auth = _get(result, "auth")
    decrypted = Crypto.decrypt_RSA_OAEP(private_key, enc_auth)

    client.private_key = private_key
    client.key = Crypto.passphrase(decrypted)
    client.auth = decrypted
    try:
        client.decode_auth = Crypto.decode_auth(client.auth)
    except Exception:
        pass
    ik = _import_key_from_private(private_key)
    if ik is not None:
        client.import_key = ik

    try:
        user = _get(result, "user")
        guid = _guid_of(user) or _guid_of(result)
        phone_number = _get(user, "phone") or phone
        user_agent = getattr(client, "user_agent", None)
        client.session.insert(
            auth=client.auth,
            guid=guid,
            user_agent=user_agent,
            phone_number=phone_number,
            private_key=private_key,
        )
    except Exception:
        pass

    try:
        await client.register_device(device_model=getattr(client, "name", "RubikaBot"))
    except Exception:
        try:
            await client.register_device()
        except Exception:
            pass

    return result


# --------------------------------------------------------------------------- #
# Tolerant field extractors (shapes vary across rubpy versions)
# --------------------------------------------------------------------------- #
def _data_of(obj):
    for attr in ("original_update", "to_dict"):
        v = getattr(obj, attr, None)
        if isinstance(v, dict):
            return v
    if isinstance(obj, dict):
        return obj
    return {}


def _guid_of(obj):
    if obj is None:
        return None
    d = _data_of(obj)
    for key in ("object_guid", "user_guid", "guid"):
        if d.get(key):
            return d[key]
    for attr in ("object_guid", "user_guid", "guid"):
        v = getattr(obj, attr, None)
        if v:
            return v
    user = getattr(obj, "user", None)
    if user is not None and user is not obj:
        return _guid_of(user)
    if isinstance(d.get("user"), dict):
        u = d["user"]
        for key in ("object_guid", "user_guid", "guid"):
            if u.get(key):
                return u[key]
    return None


def _name_of(obj, default="-"):
    d = _data_of(obj)
    first = d.get("first_name") or ""
    last = d.get("last_name") or ""
    name = (str(first) + " " + str(last)).strip()
    if name:
        return name
    for key in ("name", "title", "first_name"):
        if d.get(key):
            return d[key]
    for attr in ("first_name", "name", "title"):
        v = getattr(obj, attr, None)
        if v:
            return v
    return default


def _type_of(obj):
    d = _data_of(obj)
    t = d.get("type")
    if not t and isinstance(d.get("abs_object"), dict):
        t = d["abs_object"].get("type")
    if not t:
        abs_obj = getattr(obj, "abs_object", None) or obj
        t = getattr(abs_obj, "type", None)
        if t is None and isinstance(abs_obj, dict):
            t = abs_obj.get("type")
    return (t or "").lower()


def _last_online_of(u):
    d = _data_of(u)
    v = d.get("last_online")
    if v is None:
        ot = d.get("online_time")
        if isinstance(ot, dict):
            v = ot.get("exact_time")
    try:
        return int(v) if v is not None else 0
    except (TypeError, ValueError):
        return 0


def _is_online(u):
    d = _data_of(u)
    status = (d.get("status") or "").lower()
    return status == "online"


# --------------------------------------------------------------------------- #
# Contacts (paginated; Rubika returns ~100 per page)
# --------------------------------------------------------------------------- #
def _next_start_id(result):
    return _get(result, "next_start_id") or _get(result, "next_start_index")


async def get_contacts_full(client: Client) -> list:
    """Return ALL contacts as dicts {guid, name, last_online, online}, paginated."""
    out = []
    seen = set()
    start_id = None
    for _ in range(200):  # safety cap (200 * ~100 = 20k)
        result = await client.get_contacts(start_id) if start_id else await client.get_contacts()
        users = getattr(result, "users", None)
        if users is None and isinstance(result, dict):
            users = result.get("users", [])
        for u in users or []:
            guid = _guid_of(u)
            if guid and guid not in seen:
                seen.add(guid)
                out.append({
                    "guid": guid,
                    "name": _name_of(u),
                    "last_online": _last_online_of(u),
                    "online": _is_online(u),
                })
        start_id = _next_start_id(result)
        if not start_id or not users:
            break
    return out


async def get_chats_user_guids(client: Client):
    """Return an ORDERED list of guids of USER chats (most recent activity first)
    and the total number of groups the account is in.
    """
    user_chats = []
    seen_u = set()
    n_groups = 0
    seen_g = set()
    start_id = None
    for _ in range(200):
        result = await client.get_chats(start_id) if start_id else await client.get_chats()
        chats = getattr(result, "chats", None)
        if chats is None and isinstance(result, dict):
            chats = result.get("chats", [])
        for chat in chats or []:
            ctype = _type_of(chat)
            guid = _guid_of(chat)
            if not guid:
                continue
            if ctype == "user" and guid not in seen_u:
                seen_u.add(guid)
                user_chats.append(guid)
            elif ctype == "group" and guid not in seen_g:
                seen_g.add(guid)
                n_groups += 1
        start_id = _next_start_id(result)
        if not start_id or not chats:
            break
    return user_chats, n_groups


async def get_ordered_recipients(client: Client):
    """Build the recipient list for the account's OWN CONTACTS only.

    Priority order requested by the user:
      1) contacts who are ONLINE right now
      2) then contacts we already have a chat with (most recent activity first)
      3) then the rest, by last-seen / recent visit (most recent first)

    Returns (ordered: list of {guid, name}, stats: dict).
    """
    contacts = await get_contacts_full(client)
    user_chats, n_groups = await get_chats_user_guids(client)

    by_guid = {c["guid"]: c for c in contacts if c["guid"]}

    online_set = {g for g in by_guid if by_guid[g]["online"]}

    # recent-activity order of the chats we have (drives tiers 1 + 2)
    chat_order = [g for g in user_chats if g in by_guid]
    chat_rank = {g: i for i, g in enumerate(chat_order)}

    def _last(g):
        return by_guid[g]["last_online"] or 0

    # 1) ONLINE now — within this tier, the ones we chat with come first (by
    #    recent activity), then the rest of the online ones by most-recent seen.
    tier1 = sorted(
        online_set,
        key=lambda g: (chat_rank.get(g, len(chat_rank)), -_last(g)))

    # 2) NOT online, but we HAVE A CHAT with them (recent activity first)
    tier2 = [g for g in chat_order if g not in online_set]

    # 3) everyone else, by last-seen / recent visit (most recent first)
    placed = online_set | set(tier2)
    tier3 = sorted((g for g in by_guid if g not in placed),
                   key=lambda g: -_last(g))

    ordered_guids = tier1 + tier2 + tier3
    ordered = [{"guid": g, "name": by_guid[g]["name"]} for g in ordered_guids]

    stats = {
        "contacts": len(contacts),
        "groups": n_groups,
        "with_chat": len(chat_order),
        "online": len(online_set),
    }
    return ordered, stats


# --------------------------------------------------------------------------- #
# Find a marked message in the account's OWN Saved Messages.
# --------------------------------------------------------------------------- #
def _msg_id_of(msg):
    return _get(msg, "message_id", "id")


def _msg_text_of(msg):
    return _get(msg, "text", "caption") or ""


async def get_self_guid(client: Client) -> str:
    me = await client.get_me()
    guid = _guid_of(me)
    if not guid:
        raise RuntimeError("could not resolve self guid")
    return guid


async def find_marked_message(client: Client, marker: str):
    """Search Saved Messages for a message whose text/caption contains `marker`.
    Returns (saved_guid, message_id) or (saved_guid, None).
    """
    saved_guid = await get_self_guid(client)
    max_id = None
    for _ in range(50):  # up to ~50 pages of recent saved messages
        try:
            if max_id:
                result = await client.get_messages(saved_guid, max_id, "20")
            else:
                result = await client.get_messages(saved_guid, "0", "20")
        except Exception:
            break
        messages = getattr(result, "messages", None)
        if messages is None and isinstance(result, dict):
            messages = result.get("messages", [])
        if not messages:
            break
        for msg in messages:
            if marker in _msg_text_of(msg):
                return saved_guid, _msg_id_of(msg)
        last = messages[-1]
        max_id = _msg_id_of(last)
        if not max_id:
            break
    return saved_guid, None


# --------------------------------------------------------------------------- #
# Forwarding (version-tolerant): forward the marked message to one recipient.
# Forwarding reuses media already uploaded from the user's phone, so the bot
# never needs to upload anything itself.
# --------------------------------------------------------------------------- #
async def forward_message(client: Client, from_guid: str, to_guid: str, message_id):
    """Forward one message, adapting to whatever signature this rubpy build uses."""
    fn = getattr(client, "forward_messages", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no forward_messages()")

    mids = [message_id]
    try:
        params = [p for p in inspect.signature(fn).parameters.keys() if p != "self"]
    except (TypeError, ValueError):
        params = []

    if params:
        kwargs = {}
        for p in params:
            lp = p.lower()
            if "from" in lp and "guid" in lp:
                kwargs[p] = from_guid
            elif "to" in lp and "guid" in lp:
                kwargs[p] = to_guid
            elif lp in ("object_guid", "from_object_guid"):
                kwargs[p] = from_guid
            elif "message_ids" in lp or lp in ("messages", "message_ids"):
                kwargs[p] = mids
            elif "message_id" in lp:
                kwargs[p] = message_id
        # Only use kwargs if we matched the from/to/message params sensibly.
        if kwargs.get(_first_match(params, "from"), None) is not None:
            try:
                return await fn(**kwargs)
            except TypeError:
                pass

    # Fallbacks: try the two most common positional orders.
    try:
        return await fn(from_guid, to_guid, mids)
    except TypeError:
        return await fn(from_guid, mids, to_guid)


def _first_match(params, needle):
    for p in params:
        if needle in p.lower():
            return p
    return None


# --------------------------------------------------------------------------- #
# Channels (version-tolerant, like forward_message above).
# rubpy is unofficial, so method names / signatures differ between versions;
# we try the most common shapes and fail with a clear message otherwise.
# --------------------------------------------------------------------------- #
def _channel_guid_of(obj):
    """Pull a channel guid out of whatever shape create_channel returned."""
    d = _data_of(obj)
    for key in ("channel_guid", "object_guid", "guid"):
        if d.get(key):
            return d[key]
    # sometimes nested under "channel"
    ch = d.get("channel")
    if isinstance(ch, dict):
        for key in ("channel_guid", "object_guid", "guid"):
            if ch.get(key):
                return ch[key]
    for attr in ("channel_guid", "object_guid", "guid"):
        v = getattr(obj, attr, None)
        if v:
            return v
    ch = getattr(obj, "channel", None)
    if ch is not None and ch is not obj:
        return _channel_guid_of(ch)
    return None


async def _try_call(fn, attempts):
    """Call `fn` trying several arg shapes; return first non-TypeError result."""
    last_err = None
    for make_args in attempts:
        args, kwargs = make_args()
        try:
            return await fn(*args, **kwargs)
        except TypeError as e:  # signature mismatch -> try the next shape
            last_err = e
            continue
    raise RuntimeError(f"signature mismatch: {last_err}")


async def create_channel(client: Client, title: str, description: str = None) -> str:
    """Create a channel and return its guid. Tolerant of rubpy version diffs.

    IMPORTANT: never pass an empty-string description — Rubika's addChannel
    rejects it with INVALID_INPUT. Omit it (None) when there is no description.
    Verified against rubpy 7.3.5 where the method is `add_channel(title, ...)`.
    """
    fn = getattr(client, "add_channel", None) or getattr(client, "create_channel", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no add_channel()/create_channel()")
    desc = description or None  # turn "" into None
    if desc:
        attempts = [
            lambda: ((), {"title": title, "description": desc}),
            lambda: ((title, desc), {}),
            lambda: ((), {"title": title}),
        ]
    else:
        attempts = [
            lambda: ((), {"title": title}),
            lambda: ((title,), {}),
        ]
    result = await _try_call(fn, attempts)
    guid = _channel_guid_of(result)
    if not guid:
        raise RuntimeError("channel created but its guid was not found in the response")
    return guid


async def add_channel_members(client: Client, channel_guid: str, member_guids: list):
    """Add a batch of member guids to a channel. Tolerant of rubpy version diffs."""
    if not member_guids:
        return None
    fn = (getattr(client, "add_channel_members", None)
          or getattr(client, "add_channel_member", None))
    if fn is None:
        raise RuntimeError("this rubpy build has no add_channel_members()")
    return await _try_call(fn, [
        lambda: ((channel_guid, member_guids), {}),
        lambda: ((), {"channel_guid": channel_guid, "member_guids": member_guids}),
        lambda: ((), {"object_guid": channel_guid, "member_guids": member_guids}),
        lambda: ((), {"channel_guid": channel_guid, "user_ids": member_guids}),
    ])


async def seed_channel_with_contacts(client: Client, channel_guid: str,
                                     target: int = 300, batch: int = 80,
                                     delay: float = 2.0) -> int:
    """Add the account's OWN contacts to `channel_guid`, in chunks of `batch`,
    until `target` is reached. Returns how many members were added.

    Contacts are read with get_contacts_full() which already paginates Rubika's
    ~100-per-page contact list, so we transparently walk past the 100 limit.
    """
    contacts = await get_contacts_full(client)            # paginated read
    guids = [c["guid"] for c in contacts if c.get("guid")][:max(0, int(target))]
    added = 0
    for i in range(0, len(guids), max(1, int(batch))):
        chunk = guids[i:i + batch]
        try:
            await add_channel_members(client, channel_guid, chunk)
            added += len(chunk)
        except Exception:
            # best-effort: skip a failed batch and keep going to the next one
            pass
        if i + batch < len(guids):
            await asyncio.sleep(delay)
    return added


# --------------------------------------------------------------------------- #
# Plain text send + group listing (for the Automation feature).
# Verified against rubpy 7.3.5: send_message(object_guid, text=...).
# --------------------------------------------------------------------------- #
async def send_text(client: Client, object_guid: str, text: str):
    """Send a plain text message to a chat/group. Tolerant of rubpy diffs."""
    fn = getattr(client, "send_message", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no send_message()")
    return await _try_call(fn, [
        lambda: ((object_guid, text), {}),
        lambda: ((), {"object_guid": object_guid, "text": text}),
    ])


async def get_group_guids(client: Client) -> list:
    """Return ALL groups the account is in as {guid, name}, paginated."""
    out = []
    seen = set()
    start_id = None
    for _ in range(200):  # safety cap
        result = await client.get_chats(start_id) if start_id else await client.get_chats()
        chats = getattr(result, "chats", None)
        if chats is None and isinstance(result, dict):
            chats = result.get("chats", [])
        for ch in chats or []:
            if _type_of(ch) == "group":
                g = _guid_of(ch)
                if g and g not in seen:
                    seen.add(g)
                    out.append({"guid": g, "name": _name_of(ch)})
        start_id = _next_start_id(result)
        if not start_id or not chats:
            break
    return out


async def join_group_by_link(client: Client, link: str):
    """Join a group/channel via its invite link. Tolerant of rubpy diffs:
    tries join_group / join_chat / join_channel_by_link with link or hash."""
    link = (link or "").strip()
    if not link:
        raise RuntimeError("empty link")
    # the join "hash" is the last path segment of the invite link
    hash_part = link.rstrip("/").split("/")[-1]
    candidates = ("join_group", "join_chat", "join_channel_by_link",
                  "join_channel_action")
    last_err = None
    for name in candidates:
        fn = getattr(client, name, None)
        if fn is None:
            continue
        for arg in (link, hash_part):
            for make in (lambda a=arg: ((a,), {}),
                         lambda a=arg: ((), {"link": a}),
                         lambda a=arg: ((), {"hash": a})):
                args, kwargs = make()
                try:
                    return await fn(*args, **kwargs)
                except TypeError as e:
                    last_err = e
                    continue
                except Exception as e:   # wrong arg value for THIS method; try next
                    last_err = e
                    break
    raise RuntimeError(f"could not join via link: {last_err}")



# =========================================================================== #
# ADDITIVE helpers for the automation EXTRAS (secretary / channel report /
# profile sync / reply responder). These DO NOT change any existing function;
# they only add new, version-tolerant wrappers around rubpy 7.3.5 methods that
# were verified on the owner's account:
#   update_profile(first_name, last_name, bio)
#   get_channel_info(channel_guid)            -> channel.count_members
#   get_messages(object_guid, max_id, limit)  -> message.count_seen (views)
#   get_chats_updates(state)                  -> chats + new_state
# =========================================================================== #
def _find_first_key(obj, needles, _depth=0):
    """Recursively return the first scalar value whose key name contains any of
    `needles` (case-insensitive). Used as a defensive fallback when a field is
    nested differently across rubpy versions."""
    if _depth > 6:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if (any(n in str(k).lower() for n in needles)
                    and not isinstance(v, (dict, list))):
                return v
        for v in obj.values():
            r = _find_first_key(v, needles, _depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _find_first_key(v, needles, _depth + 1)
            if r is not None:
                return r
    return None


# --------------------------------------------------------------------------- #
# Feature 3: profile (name + bio) — read current + update.
# --------------------------------------------------------------------------- #
async def get_my_profile(client: Client) -> dict:
    """Return the account's current {first_name, last_name, bio}."""
    me = await client.get_me()
    d = _data_of(me)
    u = d.get("user") if isinstance(d.get("user"), dict) else d
    return {
        "first_name": (u.get("first_name") or "") if isinstance(u, dict) else "",
        "last_name": (u.get("last_name") or "") if isinstance(u, dict) else "",
        "bio": (u.get("bio") or "") if isinstance(u, dict) else "",
    }


async def update_profile(client: Client, first_name=None, last_name=None, bio=None):
    """Update name/bio. Verified shape: update_profile(first_name, last_name, bio).
    Only sends the fields that are not None; tolerant of small signature diffs."""
    fn = getattr(client, "update_profile", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no update_profile()")
    kwargs = {}
    if first_name is not None:
        kwargs["first_name"] = first_name
    if last_name is not None:
        kwargs["last_name"] = last_name
    if bio is not None:
        kwargs["bio"] = bio
    try:
        return await fn(**kwargs)
    except TypeError:
        # positional fallback (first_name, last_name, bio)
        return await fn(first_name or "", last_name or "", bio if bio is not None else "")


# --------------------------------------------------------------------------- #
# Feature 2: channel info (member count) + last post views, + resolve a
# link/@username/guid into a channel guid.
# --------------------------------------------------------------------------- #
async def get_channel_info(client: Client, channel_guid: str):
    fn = getattr(client, "get_channel_info", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no get_channel_info()")
    return await _try_call(fn, [
        lambda: ((channel_guid,), {}),
        lambda: ((), {"channel_guid": channel_guid}),
        lambda: ((), {"object_guid": channel_guid}),
    ])


def channel_member_count(info) -> int:
    """Member count from get_channel_info(). Verified field: channel.count_members."""
    d = _data_of(info)
    ch = d.get("channel") if isinstance(d.get("channel"), dict) else d
    for key in ("count_members", "member_count", "members_count", "subscriber_count"):
        if isinstance(ch, dict) and ch.get(key) is not None:
            try:
                return int(ch[key])
            except (TypeError, ValueError):
                return ch[key]
    v = _find_first_key(d, ("member", "subscriber"))
    try:
        return int(v) if v is not None else 0
    except (TypeError, ValueError):
        return 0


def channel_title_of(info) -> str:
    d = _data_of(info)
    ch = d.get("channel") if isinstance(d.get("channel"), dict) else d
    if isinstance(ch, dict):
        return ch.get("channel_title") or ch.get("title") or ""
    return ""


async def resolve_channel(client: Client, ref: str):
    """Turn a guid / @username / link into (channel_guid, channel_title).
    Raises if it cannot be resolved."""
    ref = (ref or "").strip()
    if ref.startswith("c0"):
        return ref, ""
    if ref.startswith("@"):
        username = ref[1:]
    elif ref.startswith("http"):
        username = ref.rstrip("/").split("/")[-1].lstrip("@")
    else:
        username = ref.lstrip("@")
    for name in ("get_object_by_username", "get_info_by_username",
                 "get_channel_info_by_username"):
        fn = getattr(client, name, None)
        if fn is None:
            continue
        try:
            res = await _try_call(fn, [
                lambda u=username: ((u,), {}),
                lambda u=username: ((), {"username": u}),
            ])
        except Exception:
            continue
        d = _data_of(res)
        ch = d.get("channel") if isinstance(d.get("channel"), dict) else {}
        guid = (ch.get("channel_guid") if isinstance(ch, dict) else None) \
            or _channel_guid_of(res) or _guid_of(res)
        title = (ch.get("channel_title") if isinstance(ch, dict) else "") or ""
        if guid:
            return guid, title
    raise RuntimeError("could not resolve channel (use the channel guid 'c0...')")


async def get_last_post_views(client: Client, channel_guid: str):
    """Return (views, message_id) for the channel's newest post. Verified field:
    message.count_seen. Falls back to a recursive search, then (None, mid)."""
    result = await client.get_messages(channel_guid, "0", "1")
    messages = getattr(result, "messages", None)
    if messages is None and isinstance(result, dict):
        messages = result.get("messages", [])
    if not messages:
        return None, None
    m = messages[0]
    md = _data_of(m)
    mid = _msg_id_of(m)
    for key in ("count_seen", "views", "view_count", "count_views", "seen_count"):
        if md.get(key) is not None:
            return md.get(key), mid
    v = _find_first_key(md, ("seen", "view"))
    return v, mid


# --------------------------------------------------------------------------- #
# Feature 1 & 5: chat updates polling (new PVs / new group messages).
# --------------------------------------------------------------------------- #
async def get_chats_updates(client: Client, state):
    """Call get_chats_updates(state). rubpy expects an INTEGER state (unix
    seconds). An empty/None/0 state means 'first run': we pass nothing and let
    rubpy default it (it uses ~now-200s, so we don't pull the whole history).
    Passing '' directly makes rubpy run int('') and crash, so we coerce here."""
    fn = getattr(client, "get_chats_updates", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no get_chats_updates()")
    s = None
    if state not in (None, "", "0", 0):
        try:
            s = int(state)
        except (TypeError, ValueError):
            s = None
    try:
        return await fn() if s is None else await fn(s)
    except TypeError:
        try:
            return await fn(state=s) if s is not None else await fn()
        except TypeError:
            return await fn()


def parse_chats_updates(result):
    """Return (chats: list, new_state: str|None) from a get_chats_updates() result."""
    d = _data_of(result)
    chats = None
    for key in ("chats", "chats_updates", "updated_chats", "chat_updates"):
        v = d.get(key)
        if isinstance(v, list):
            chats = v
            break
    new_state = (d.get("new_state") or d.get("state") or d.get("next_state")
                 or d.get("timestamp"))
    return (chats or []), new_state


def chat_object_guid(chat):
    d = _data_of(chat)
    return d.get("object_guid") or _guid_of(chat)


def chat_type(chat) -> str:
    """Lower-case chat type ('user' / 'group' / 'channel' / ...)."""
    return _type_of(chat)


def chat_last_message(chat) -> dict:
    d = _data_of(chat)
    lm = d.get("last_message")
    return lm if isinstance(lm, dict) else {}


def chat_last_message_id(chat):
    d = _data_of(chat)
    return d.get("last_message_id") or chat_last_message(chat).get("message_id")


def message_author_guid(msg) -> str:
    d = _data_of(msg)
    for key in ("author_object_guid", "author_guid", "author_object_id"):
        if d.get(key):
            return d[key]
    a = d.get("author")
    if isinstance(a, dict):
        return a.get("object_guid") or a.get("guid") or ""
    return ""


def message_reply_to_id(msg):
    d = _data_of(msg)
    return d.get("reply_to_message_id") or d.get("reply_to_object")


# --------------------------------------------------------------------------- #
# Feature 5 helpers: read recent messages, fetch a message by id, send a reply.
# --------------------------------------------------------------------------- #
async def get_recent_messages(client: Client, object_guid: str, limit: int = 20) -> list:
    """Return up to `limit` recent messages of a chat (newest first)."""
    result = await client.get_messages(object_guid, "0", str(limit))
    messages = getattr(result, "messages", None)
    if messages is None and isinstance(result, dict):
        messages = result.get("messages", [])
    return messages or []


async def get_messages_by_id(client: Client, object_guid: str, message_ids: list):
    """Fetch specific messages by id (tolerant). Returns a list of messages or []"""
    for name in ("get_messages_by_id", "get_message_by_id", "get_messages_by_ID"):
        fn = getattr(client, name, None)
        if fn is None:
            continue
        try:
            res = await _try_call(fn, [
                lambda: ((object_guid, message_ids), {}),
                lambda: ((), {"object_guid": object_guid, "message_ids": message_ids}),
            ])
        except Exception:
            continue
        messages = getattr(res, "messages", None)
        if messages is None and isinstance(res, dict):
            messages = res.get("messages", [])
        return messages or []
    return []


async def send_reply(client: Client, object_guid: str, text: str, reply_to_message_id):
    """Send a text message as a reply to a specific message. Tolerant of diffs."""
    fn = getattr(client, "send_message", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no send_message()")
    return await _try_call(fn, [
        lambda: ((), {"object_guid": object_guid, "text": text,
                      "reply_to_message_id": reply_to_message_id}),
        lambda: ((object_guid, text), {"reply_to_message_id": reply_to_message_id}),
        lambda: ((object_guid, text), {}),
    ])


async def forward_to(client: Client, from_guid: str, to_guid: str, message_id):
    """Alias kept for clarity in the secretary 'marker' mode (forward the marked
    Saved-Messages post to a single new PV). Delegates to forward_message()."""
    return await forward_message(client, from_guid, to_guid, message_id)



async def leave_group(client: Client, group_guid: str):
    """Leave a group. Tolerant of rubpy version differences in method name."""
    for name in ("leave_group", "leave_chat", "left_group"):
        fn = getattr(client, name, None)
        if fn is None:
            continue
        return await _try_call(fn, [
            lambda: ((group_guid,), {}),
            lambda: ((), {"group_guid": group_guid}),
            lambda: ((), {"object_guid": group_guid}),
        ])
    raise RuntimeError("this rubpy build has no leave_group()")



# =========================================================================== #
# ADDITIVE helpers for the GENERATOR engine (موتور مولد). Verified method names
# against rubpy 7.3.5 via scripts/test_generator.py:
#   add_channel(title, description, member_guids)
#   add_group(title, member_guids)            (create a group)
#   join_group(link) / join_channel_by_link(link)
#   user_is_admin(object_guid, user_guid)     (check admin status)
#   add_channel_members / add_group_members
#   create_join_link(object_guid, ...)        (to invite other accounts)
# These DO NOT change any existing function.
# =========================================================================== #
async def create_group(client: Client, title: str, member_guids: list = None) -> str:
    """Create a group and return its guid. Tolerant of rubpy version diffs.

    Rubika's addGroup REQUIRES at least one member guid; sending an empty list
    returns INVALID_INPUT. So if no members are given we seed the group with the
    account ITSELF (the verified test created a group exactly this way)."""
    fn = getattr(client, "add_group", None) or getattr(client, "create_group", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no add_group()/create_group()")
    members = list(member_guids or [])
    if not members:
        try:
            members = [await get_self_guid(client)]
        except Exception:
            members = []
    result = await _try_call(fn, [
        lambda: ((), {"title": title, "member_guids": members}),
        lambda: ((title, members), {}),
        lambda: ((), {"title": title}),
        lambda: ((title,), {}),
    ])
    guid = _guid_of(result) or _channel_guid_of(result)
    if not guid:
        raise RuntimeError("group created but its guid was not found in the response")
    return guid


async def create_object(client: Client, kind: str, title: str) -> str:
    """Create a channel OR group depending on `kind` ('channel'/'group')."""
    if str(kind).lower() == "group":
        return await create_group(client, title)
    return await create_channel(client, title)


async def make_join_link(client: Client, object_guid: str) -> str:
    """Create (or fetch) an invite link for a channel/group so OTHER accounts
    can join it. Tolerant of rubpy version diffs."""
    fn = getattr(client, "create_join_link", None)
    if fn is not None:
        try:
            res = await _try_call(fn, [
                lambda: ((), {"object_guid": object_guid}),
                lambda: ((object_guid,), {}),
            ])
            d = _data_of(res)
            for key in ("join_link", "invite_link", "link"):
                if d.get(key):
                    return d[key]
            jl = d.get("join_link") or d.get("link")
            if isinstance(jl, dict):
                for key in ("join_link", "invite_link", "link", "url"):
                    if jl.get(key):
                        return jl[key]
        except Exception:
            pass
    # fall back to get_join_links
    fn2 = getattr(client, "get_join_links", None)
    if fn2 is not None:
        try:
            res = await _try_call(fn2, [
                lambda: ((), {"object_guid": object_guid}),
                lambda: ((object_guid,), {}),
            ])
            d = _data_of(res)
            links = d.get("join_links") or d.get("links") or []
            if isinstance(links, list) and links:
                first = links[0]
                if isinstance(first, dict):
                    for key in ("join_link", "invite_link", "link", "url"):
                        if first.get(key):
                            return first[key]
                elif isinstance(first, str):
                    return first
        except Exception:
            pass
    raise RuntimeError("could not create/get a join link for this object")


async def user_is_admin(client: Client, object_guid: str, user_guid: str) -> bool:
    """Return True if user_guid is an admin of object_guid. Tolerant of diffs."""
    fn = getattr(client, "user_is_admin", None)
    if fn is not None:
        try:
            res = await _try_call(fn, [
                lambda: ((object_guid, user_guid), {}),
                lambda: ((), {"object_guid": object_guid, "user_guid": user_guid}),
            ])
            if isinstance(res, bool):
                return res
            d = _data_of(res)
            for key in ("is_admin", "user_is_admin", "result"):
                if isinstance(d.get(key), bool):
                    return d[key]
            # some builds return the access list when admin, nothing when not
            if d.get("access_list") or d.get("admin_access_list"):
                return True
        except Exception:
            pass
    # fallback: scan the admin members list
    for name in ("get_channel_admin_members", "get_group_admin_members"):
        f = getattr(client, name, None)
        if f is None:
            continue
        try:
            res = await _try_call(f, [
                lambda: ((object_guid,), {}),
                lambda: ((), {"channel_guid": object_guid}),
                lambda: ((), {"group_guid": object_guid}),
            ])
            d = _data_of(res)
            admins = d.get("in_chat_members") or d.get("admins") or d.get("members") or []
            for a in admins:
                ad = _data_of(a) if not isinstance(a, str) else {}
                g = (ad.get("member_guid") or ad.get("object_guid")
                     or ad.get("user_guid")) if ad else a
                if g == user_guid:
                    return True
            return False
        except Exception:
            continue
    return False


async def add_members_to_object(client: Client, kind: str, object_guid: str,
                                 member_guids: list):
    """Add members to a channel OR group depending on `kind`."""
    if not member_guids:
        return None
    if str(kind).lower() == "group":
        fn = getattr(client, "add_group_members", None)
        if fn is not None:
            return await _try_call(fn, [
                lambda: ((object_guid, member_guids), {}),
                lambda: ((), {"group_guid": object_guid, "member_guids": member_guids}),
            ])
    return await add_channel_members(client, object_guid, member_guids)


async def seed_object_with_contacts(client: Client, kind: str, object_guid: str,
                                    target: int = 300, batch: int = 80,
                                    delay: float = 2.0,
                                    exclude: set = None) -> int:
    """Add the account's OWN contacts to a channel/group in chunks, up to
    `target`. Skips guids in `exclude` (anti-duplicate across accounts).
    Returns how many were added."""
    contacts = await get_contacts_full(client)
    exclude = exclude or set()
    guids = [c["guid"] for c in contacts
             if c.get("guid") and c["guid"] not in exclude][:max(0, int(target))]
    added = 0
    for i in range(0, len(guids), max(1, int(batch))):
        chunk = guids[i:i + batch]
        try:
            await add_members_to_object(client, kind, object_guid, chunk)
            added += len(chunk)
        except Exception:
            pass
        if i + batch < len(guids):
            await asyncio.sleep(delay)
    return added



# --------------------------------------------------------------------------- #
# Generator (channel-only) helpers — VERIFIED against rubpy 7.3.5:
#   check_channel_username(username) -> {"exist": bool}
#   update_channel_username(channel_guid, username)
#   get_object_by_username(username) -> {channel:{channel_guid}}
#   join_channel_action(channel_guid, 'Join')
# Used to make a fresh channel public with a RANDOM username so the other
# accounts can join it by that username (private invite links don't work for
# joining reliably, per the project owner's testing).
# --------------------------------------------------------------------------- #
import random as _random
import string as _string


def random_username(prefix: str = "ch", length: int = 18) -> str:
    """A random public username, e.g. 'ch6bsmf11lxmz91yin76' (letters+digits)."""
    body = "".join(_random.choices(_string.ascii_lowercase + _string.digits,
                                   k=max(5, length)))
    return f"{prefix}{body}"[:32]


async def channel_username_free(client: Client, username: str) -> bool:
    """True if the channel username is available. check returns {'exist': False}
    when it's FREE."""
    fn = getattr(client, "check_channel_username", None)
    if fn is None:
        return True
    try:
        res = await _try_call(fn, [
            lambda: ((username,), {}),
            lambda: ((), {"username": username}),
        ])
        d = _data_of(res)
        if "exist" in d:
            return not bool(d.get("exist"))
    except Exception:
        pass
    return True


async def set_channel_username(client: Client, channel_guid: str, username: str):
    """Set a public username on a channel."""
    fn = getattr(client, "update_channel_username", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no update_channel_username()")
    return await _try_call(fn, [
        lambda: ((channel_guid, username), {}),
        lambda: ((), {"channel_guid": channel_guid, "username": username}),
    ])


async def assign_random_channel_username(client: Client, channel_guid: str,
                                         tries: int = 6) -> str:
    """Pick a free random username and set it on the channel. Returns the
    username that was set (or raises if none worked)."""
    last_err = None
    for _ in range(max(1, tries)):
        u = random_username()
        try:
            if await channel_username_free(client, u):
                await set_channel_username(client, channel_guid, u)
                return u
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue
    raise RuntimeError(f"could not assign a username: {last_err}")


async def resolve_username_to_guid(client: Client, username: str) -> str:
    """Turn a public username into a channel guid via get_object_by_username."""
    fn = getattr(client, "get_object_by_username", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no get_object_by_username()")
    res = await _try_call(fn, [
        lambda: ((username,), {}),
        lambda: ((), {"username": username}),
    ])
    d = _data_of(res)
    ch = d.get("channel") if isinstance(d.get("channel"), dict) else {}
    guid = (ch.get("channel_guid") if isinstance(ch, dict) else None) \
        or _channel_guid_of(res) or _guid_of(res)
    if not guid:
        raise RuntimeError("could not resolve username to a channel guid")
    return guid


async def join_channel_by_guid(client: Client, channel_guid: str):
    """Join a channel by its guid (the verified working way). Tolerant of diffs."""
    fn = getattr(client, "join_channel_action", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no join_channel_action()")
    return await _try_call(fn, [
        lambda: ((channel_guid, "Join"), {}),
        lambda: ((), {"channel_guid": channel_guid, "action": "Join"}),
    ])


async def join_channel_by_username(client: Client, username: str) -> str:
    """Resolve a username to a guid then join it. Returns the channel guid."""
    guid = await resolve_username_to_guid(client, username)
    await join_channel_by_guid(client, guid)
    return guid



# =========================================================================== #
# ADDITIVE helpers for: (1) "channel broadcast" engine (each account makes its
# OWN channel, forwards the marked post, seeds its own contacts), and (2) the
# PV image -> PDF export. All verified-method-based, no existing func changed.
# =========================================================================== #

# ---- channel broadcast: set title is via add_channel; seed via existing
#      seed_channel_with_contacts; forward via existing forward_message ----

async def get_chat_list_guids(client: Client, only_users: bool = True) -> list:
    """Return guids of chats. If only_users, just private (user) chats — used by
    the PV image export. Paginated."""
    out = []
    seen = set()
    start_id = None
    for _ in range(200):
        result = await client.get_chats(start_id) if start_id else await client.get_chats()
        chats = getattr(result, "chats", None)
        if chats is None and isinstance(result, dict):
            chats = result.get("chats", [])
        for ch in chats or []:
            g = _guid_of(ch)
            if not g or g in seen:
                continue
            if only_users and _type_of(ch) != "user":
                continue
            seen.add(g)
            out.append(g)
        start_id = _next_start_id(result)
        if not start_id or not chats:
            break
    return out


def _msg_is_photo(msg) -> bool:
    """True if a message carries a PHOTO (not video/gif/file)."""
    d = _data_of(msg)
    # file_inline holds media metadata in rubpy
    fi = d.get("file_inline") or {}
    if isinstance(fi, dict):
        t = (fi.get("type") or "").lower()
        if t:
            return t == "image"          # 'Image' for photos; 'Video'/'Gif'/'File' otherwise
        mime = (fi.get("mime") or "").lower()
        if mime:
            return mime in ("jpg", "jpeg", "png", "webp", "bmp")
    return False


def _file_inline_of(msg):
    d = _data_of(msg)
    fi = d.get("file_inline")
    return fi if isinstance(fi, dict) else None


async def iter_chat_photos(client: Client, object_guid: str, max_pages: int = 200):
    """Yield (message_id, file_inline) for every PHOTO message in a chat,
    walking the whole history page by page (oldest pagination via max_id)."""
    max_id = None
    for _ in range(max_pages):
        try:
            if max_id:
                result = await client.get_messages(object_guid, max_id, "50")
            else:
                result = await client.get_messages(object_guid, "0", "50")
        except Exception:
            break
        messages = getattr(result, "messages", None)
        if messages is None and isinstance(result, dict):
            messages = result.get("messages", [])
        if not messages:
            break
        for m in messages:
            if _msg_is_photo(m):
                fi = _file_inline_of(m)
                if fi:
                    yield _msg_id_of(m), fi
        last = messages[-1]
        nxt = _msg_id_of(last)
        if not nxt or nxt == max_id:
            break
        max_id = nxt


async def download_photo(client: Client, file_inline) -> bytes:
    """Download a photo's bytes from its file_inline. Tolerant of rubpy diffs."""
    fn = getattr(client, "download", None)
    if fn is None:
        raise RuntimeError("this rubpy build has no download()")
    # rubpy's download usually accepts the file_inline dict/object directly
    res = await _try_call(fn, [
        lambda: ((file_inline,), {}),
        lambda: ((), {"file_inline": file_inline}),
    ])
    if isinstance(res, (bytes, bytearray)):
        return bytes(res)
    # some builds return an object with .data / bytes
    for attr in ("data", "content", "bytes"):
        v = getattr(res, attr, None)
        if isinstance(v, (bytes, bytearray)):
            return bytes(v)
    if isinstance(res, dict):
        for k in ("data", "content", "bytes"):
            if isinstance(res.get(k), (bytes, bytearray)):
                return bytes(res[k])
    raise RuntimeError("download() returned no bytes")



# =========================================================================== #
# ADDITIVE + ISOLATED: auto-upload a file to the account's OWN Saved Messages
# so the proven send engine can then FORWARD it exactly like a marked message.
# Nothing above is modified. rubpy 7.3.5 is unofficial, so we (a) try the known
# high-level senders defensively and (b) DO NOT rely on the send result's shape:
# we confirm success by detecting a NEW message at the top of Saved Messages.
# =========================================================================== #
async def _newest_saved_message_id(client: Client, saved_guid: str):
    """Return the message_id of the most recent message in Saved Messages
    (or None). Reuses the same get_messages() shape as find_marked_message()."""
    try:
        result = await client.get_messages(saved_guid, "0", "5")
    except Exception:
        return None
    messages = getattr(result, "messages", None)
    if messages is None and isinstance(result, dict):
        messages = result.get("messages", [])
    if not messages:
        return None
    # messages come newest-first in the builds we target; pick the max id to be safe
    ids = []
    for m in messages:
        mid = _msg_id_of(m)
        try:
            ids.append(int(mid))
        except (TypeError, ValueError):
            continue  # keep only numeric ids so the result is always int|None
    return max(ids) if ids else None


async def upload_file_to_self(client: Client, file_path: str, caption: str = "",
                              file_name: str = None, as_document: bool = True):
    """Upload a local file to the account's OWN Saved Messages and return
    (saved_guid, message_id) — ready to hand to the existing forward engine.

    Defensive: tries the rubpy 7.3.5 high-level senders in turn, and confirms
    the upload by waiting for a NEW top message in Saved (so we never forward a
    stale message even if a send method silently no-ops). Raises on failure so
    the caller can fall back to the marker flow."""
    if not file_path or not os.path.exists(file_path):
        raise RuntimeError("file not found for upload")
    saved_guid = await get_self_guid(client)
    text = caption or None
    name = file_name or os.path.basename(file_path)

    before = await _newest_saved_message_id(client, saved_guid)

    # candidate (method, args-factory) pairs in likely order for rubpy 7.3.5.
    # Prefer document/file senders that keep the original name+extension.
    attempts = []
    for mname in ("send_document", "send_file"):
        fn = getattr(client, mname, None)
        if fn is not None:
            attempts += [
                (fn, lambda fn=fn: ((saved_guid, file_path),
                                    {"caption": text, "file_name": name})),
                (fn, lambda fn=fn: ((saved_guid, file_path), {"caption": text})),
                (fn, lambda fn=fn: ((), {"object_guid": saved_guid, "file": file_path,
                                         "caption": text, "file_name": name})),
                (fn, lambda fn=fn: ((), {"object_guid": saved_guid, "path": file_path,
                                         "caption": text})),
            ]
    sm = getattr(client, "send_message", None)
    if sm is not None:
        attempts += [
            (sm, lambda: ((), {"object_guid": saved_guid, "text": text,
                               "file_inline": file_path})),
            (sm, lambda: ((), {"object_guid": saved_guid, "file_inline": file_path,
                               "caption": text})),
        ]
    for mname in ("send_media", "send_photo", "send_video", "send_music", "send_gif"):
        fn = getattr(client, mname, None)
        if fn is not None:
            attempts += [
                (fn, lambda fn=fn: ((saved_guid, file_path), {"caption": text})),
                (fn, lambda fn=fn: ((), {"object_guid": saved_guid, "file": file_path,
                                         "caption": text})),
            ]

    last_err = "no compatible send method found in this rubpy build"
    for fn, make in attempts:
        try:
            args, kwargs = make()
            kwargs = {k: v for k, v in kwargs.items()
                      if v is not None or k in ("text", "caption")}
            await fn(*args, **kwargs)
        except TypeError as e:        # wrong signature for this build -> next shape
            last_err = e
            continue
        except Exception as e:        # value/other error -> try the next shape too
            last_err = e
            continue
        # sent without raising: confirm a NEW message appeared at the top of Saved
        for _ in range(12):
            await asyncio.sleep(1.0)
            after = await _newest_saved_message_id(client, saved_guid)
            if after is not None and after != before:
                return saved_guid, after
        # method returned but no new message showed up -> treat as failure
        last_err = "send returned but no new message appeared in Saved"
    raise RuntimeError(f"upload_file_to_self failed: {last_err}")
