"""
iran_numbers.py — Iranian mobile-number helpers (pure / no I/O).
================================================================

Used by:
  * Tools » Iranian number generator   (build a TXT of numbers from a prefix)
  * Rubika » build contacts by prefix  (clean the user prefix + fill digits)

The 091x prefixes were historically allocated by province for Hamrah-e-Avval
(MCI); the region label reflects the ALLOCATION region of the prefix, NOT the
SIM's current physical location (numbers are portable). Everything here is
deterministic/pure so it can be unit-tested without a network or DB.
"""
from __future__ import annotations

import random

# --------------------------------------------------------------------------- #
# operator + regional allocation for the well-known 4-digit mobile prefixes.
# (operator, region)
# --------------------------------------------------------------------------- #
PREFIX_INFO = {
    # ---- همراه اول (MCI) — 091x historically province-allocated ----
    "0910": ("همراه اول", "سراسری"),
    "0911": ("همراه اول", "مازندران / گلستان"),
    "0912": ("همراه اول", "تهران / البرز"),
    "0913": ("همراه اول", "اصفهان / یزد / چهارمحال"),
    "0914": ("همراه اول", "آذربایجان شرقی / اردبیل (تبریز)"),
    "0915": ("همراه اول", "خراسان (مشهد)"),
    "0916": ("همراه اول", "خوزستان / لرستان (اهواز)"),
    "0917": ("همراه اول", "فارس / بوشهر / هرمزگان (شیراز)"),
    "0918": ("همراه اول", "کرمانشاه / کردستان / همدان / ایلام"),
    "0919": ("همراه اول", "تهران / البرز"),
    "0990": ("همراه اول", "سراسری"),
    "0991": ("همراه اول", "سراسری"),
    "0992": ("همراه اول", "سراسری"),
    "0993": ("همراه اول", "سراسری"),
    "0994": ("همراه اول", "سراسری"),
    # ---- ایرانسل (MTN Irancell) — nationwide ----
    "0900": ("ایرانسل", "سراسری"),
    "0901": ("ایرانسل", "سراسری"),
    "0902": ("ایرانسل", "سراسری"),
    "0903": ("ایرانسل", "سراسری"),
    "0904": ("ایرانسل", "سراسری"),
    "0905": ("ایرانسل", "سراسری"),
    "0930": ("ایرانسل", "سراسری"),
    "0933": ("ایرانسل", "سراسری"),
    "0935": ("ایرانسل", "سراسری"),
    "0936": ("ایرانسل", "سراسری"),
    "0937": ("ایرانسل", "سراسری"),
    "0938": ("ایرانسل", "سراسری"),
    "0939": ("ایرانسل", "سراسری"),
    # ---- رایتل (Rightel) — nationwide ----
    "0920": ("رایتل", "سراسری"),
    "0921": ("رایتل", "سراسری"),
    "0922": ("رایتل", "سراسری"),
    # ---- سایر اپراتورها / مجازی ----
    "0931": ("سایر / اپراتور مجازی", "سراسری"),
    "0932": ("تله‌کیش", "کیش"),
    "0934": ("تالیا", "سراسری"),
    "0998": ("اپراتور مجازی", "سراسری"),
    "0999": ("اپراتور مجازی", "سراسری"),
}

PHONE_LEN = 11  # Iranian mobile numbers are 11 digits incl. the leading 0.


def clean_prefix(raw):
    """Normalise a user-supplied prefix into a 0-leading mobile prefix (<=11).

    Accepts inputs like '0913', '9139', '98913...', '0098913...', '+98913...'
    and returns e.g. '0913' — or None when there are no digits at all.
    """
    digits = "".join(ch for ch in (raw or "") if ch.isdigit())
    if not digits:
        return None
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("98") and len(digits) > 2 and digits[2] == "9":
        # country code 98 followed by an operator '9...' -> local 0-leading
        digits = "0" + digits[2:]
    if not digits.startswith("0"):
        digits = "0" + digits
    return digits[:PHONE_LEN]


def detect(prefix):
    """Return (operator, region) for a prefix, or (None, None) if unknown."""
    p = clean_prefix(prefix) or ""
    return PREFIX_INFO.get(p[:4], (None, None))


def is_valid_prefix(prefix) -> bool:
    """True when the prefix is a recognised Iranian mobile prefix (09xx...)."""
    p = clean_prefix(prefix) or ""
    return len(p) >= 4 and p.startswith("09") and p[:4] in PREFIX_INFO


def gen_number(prefix: str) -> str:
    """Build one 11-digit number by filling the remaining digits at random."""
    prefix = prefix or ""
    need = PHONE_LEN - len(prefix)
    if need <= 0:
        return prefix[:PHONE_LEN]
    suffix = "".join(random.choice("0123456789") for _ in range(need))
    return prefix + suffix


def gen_unique(prefix, count, existing=None):
    """Return up to ``count`` UNIQUE 11-digit numbers built from ``prefix``.

    * A partial prefix (e.g. '0913613') only has its remaining digits filled.
    * If the search space (10 ** remaining-digits) is smaller than ``count``,
      as many unique numbers as exist are returned (no infinite loop).
    * ``existing`` (iterable of numbers) are treated as already-used and skipped.
    """
    prefix = clean_prefix(prefix) or ""
    count = max(0, int(count))
    if not prefix or count == 0:
        return []
    if len(prefix) >= PHONE_LEN:
        return [prefix[:PHONE_LEN]]
    need = PHONE_LEN - len(prefix)
    space = 10 ** need
    target = min(count, space)
    seen = set(existing or [])
    out = []
    attempts = 0
    max_attempts = target * 40 + 2000
    while len(out) < target and attempts < max_attempts:
        attempts += 1
        n = gen_number(prefix)
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out
