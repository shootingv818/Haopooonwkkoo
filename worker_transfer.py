"""
worker_transfer.py — Isolated worker selection for TRANSFER (not first login).
==============================================================================

When the customer presses "🔄 انتقال ورکر" repeatedly, this module ensures the
bot picks a worker that has NEVER been tried before for THIS account's send —
not just a worker different from the current one.

Why this exists (the bug it fixes):
  ``worker.pick_worker_for_login(exclude_id=...)`` excludes only ONE worker (the
  current one) and then sorts by LOAD (fewest accounts first). So repeatedly
  transferring across workers A→B→A could land BACK on a worker already tried
  and failed. This module tracks the FULL set of tried workers per account and
  excludes all of them, so every transfer moves to a genuinely untried worker
  until they run out.

Differences from ``worker.pick_worker_for_login``:
  1. ``exclude_ids`` is a LIST (all previously tried workers), not a single ID.
  2. No load-balancing sort ("fewest accounts first") — pick the first healthy
     worker by ID order (deterministic, moves forward).
  3. Tracks tried workers per account_id (in-memory).

Does NOT modify or import the send/login internals; it only reuses
``worker`` (health + selection helpers) and ``db`` (enabled-worker list).
Opens no Rubika/Telegram connection or session of its own.
"""
from __future__ import annotations

import worker
import db


# --------------------------------------------------------------------------- #
# In-memory tracking: account_id -> list of tried worker IDs (for the current
# transfer chain of that account). Reset on a successful send-probe.
# --------------------------------------------------------------------------- #
_account_tried: dict = {}


def get_tried(account_id: int) -> list:
    """Return the list of tried worker IDs for this account's current send."""
    return list(_account_tried.get(int(account_id), []))


def add_tried(account_id: int, worker_id) -> None:
    """Mark a worker as tried for this account (idempotent)."""
    if worker_id is None:
        return
    aid = int(account_id)
    wid = int(worker_id)
    lst = _account_tried.setdefault(aid, [])
    if wid not in lst:
        lst.append(wid)


def set_tried(account_id: int, ids: list) -> None:
    """Restore the tried list (e.g. from a persisted payload)."""
    _account_tried[int(account_id)] = [int(x) for x in (ids or [])]


def clear_tried(account_id: int) -> None:
    """Clear tried history (send succeeded / cancelled / account removed)."""
    _account_tried.pop(int(account_id), None)


# --------------------------------------------------------------------------- #
# Worker selection for transfer — excludes ALL previously tried workers.
# --------------------------------------------------------------------------- #
async def pick_worker_for_transfer(exclude_ids: list = None,
                                   verify: bool = True):
    """Find a healthy enabled worker whose ID is NOT in ``exclude_ids``.

    No load-balancing — returns the first available worker by ID order, so
    repeated transfers deterministically move forward through untried workers.
    Returns a worker dict or None if no suitable worker exists.
    """
    exclude_set = {int(x) for x in (exclude_ids or [])}

    worker.ensure_master_worker()
    workers = db.list_enabled_workers()
    if not workers:
        return None

    remotes = [w for w in workers if not worker.is_local(w)]
    # Only health-check when there are real remote workers.
    if verify and remotes:
        await worker.check_all(workers)
        workers = db.list_enabled_workers()   # reload with fresh health

    # Local master is always usable; remotes must be healthy ("ok").
    pool = [w for w in workers if (worker.is_local(w) or w.get("status") == "ok")]
    # Exclude ALL previously tried workers for this account.
    pool = [w for w in pool if w["id"] not in exclude_set]
    if not pool:
        return None

    # No load sort — deterministic by ID (always moves forward).
    pool.sort(key=lambda w: w["id"])
    return pool[0]
