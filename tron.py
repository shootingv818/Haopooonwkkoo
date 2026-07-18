"""
tron.py -- TRX native payment verification via TronGrid.
========================================================

A customer pays the configured wallet in TRX (native transfer) and sends us the
transaction hash. We confirm the payment ENTIRELY on-chain:

  1) the transaction exists and is CONFIRMED (contractRet == SUCCESS),
  2) it is a native TRX transfer (TransferContract, NOT TriggerSmartContract),
  3) the destination address equals OUR wallet,
  4) the transferred amount (in TRX) is within the configured tolerance of the
     expected amount.

Additionally provides:
  - get_trx_price_usd(): fetches TRX/USD from CoinGecko with 5-min cache
  - calc_trx_amount(usd): converts a USD price to TRX (floor, integer)

Anti-fraud "each hash only once" is enforced by db.record_deposit (the tx_hash
column is UNIQUE), so even concurrent submissions cannot double-credit.
"""
from __future__ import annotations

import asyncio
import hashlib
import math
import re
import time

import httpx

import config
import db
import logbus

# --------------------------------------------------------------------------- #
# TRX price cache (module-level, simple dict).
# --------------------------------------------------------------------------- #
_price_cache: dict = {"price": 0.0, "ts": 0.0}

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# A TRON transaction hash is exactly 64 hexadecimal characters.
_TX_HASH_RE = re.compile(r"[0-9a-fA-F]{64}")


def extract_tx_hash(text: str) -> str:
    """Extract a 64-char hex TRON tx hash from arbitrary user input.

    Customers often paste a full tronscan URL (e.g.
    https://tronscan.org/#/transaction/<hash>) or a '0x'-prefixed value instead
    of the bare hash. We strip a leading '0x' and pull the first 64-hex run we
    find, so both a raw hash AND a pasted link/URL work. Returns '' if none.
    """
    if not text:
        return ""
    cleaned = text.strip().replace("0x", "").replace("0X", "")
    m = _TX_HASH_RE.search(cleaned)
    return m.group(0).lower() if m else ""


# --------------------------------------------------------------------------- #
# base58check <-> hex TRON address helpers.
# --------------------------------------------------------------------------- #
def _b58decode(s: str) -> bytes:
    num = 0
    for ch in s:
        num = num * 58 + _B58_ALPHABET.index(ch)
    full = num.to_bytes((num.bit_length() + 7) // 8, "big")
    pad = 0
    for ch in s:
        if ch == "1":
            pad += 1
        else:
            break
    return b"\x00" * pad + full


def base58_to_hex(addr: str) -> str:
    """Convert a base58check TRON address (T...) to its 41-prefixed hex form."""
    raw = _b58decode(addr)
    if len(raw) != 25:
        raise ValueError("bad TRON address length")
    payload, checksum = raw[:-4], raw[-4:]
    digest = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    if digest != checksum:
        raise ValueError("bad TRON address checksum")
    return payload.hex()  # starts with '41'


def hex_to_base58(hex_addr: str) -> str:
    """Convert a 41-prefixed hex TRON address to base58check (T...)."""
    payload = bytes.fromhex(hex_addr)
    digest = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    raw = payload + digest
    num = int.from_bytes(raw, "big")
    out = ""
    while num > 0:
        num, rem = divmod(num, 58)
        out = _B58_ALPHABET[rem] + out
    pad = 0
    for b in raw:
        if b == 0:
            pad += 1
        else:
            break
    return "1" * pad + out


# --------------------------------------------------------------------------- #
# CoinGecko TRX price fetcher with 5-minute cache.
# --------------------------------------------------------------------------- #
async def get_trx_price_usd() -> float:
    """Return the current TRX price in USD.

    Priority:
      1) db setting 'trx_price_override' (runtime override from owner panel)
      2) config.TRX_PRICE_OVERRIDE (env-level override)
      3) CoinGecko API with 5-minute in-memory cache
    """
    # 1) Runtime override from owner panel (db setting)
    try:
        db_override = float(db.get_setting("trx_price_override", "0"))
        if db_override > 0:
            return db_override
    except (TypeError, ValueError):
        pass

    # 2) Env-level override
    if config.TRX_PRICE_OVERRIDE > 0:
        return config.TRX_PRICE_OVERRIDE

    # 3) CoinGecko with cache
    now = time.time()
    if (_price_cache["price"] > 0
            and now - _price_cache["ts"] < config.COINGECKO_CACHE_SECONDS):
        return _price_cache["price"]

    url = "https://api.coingecko.com/api/v3/simple/price?ids=tron&vs_currencies=usd"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            price = float(data["tron"]["usd"])
            _price_cache["price"] = price
            _price_cache["ts"] = time.time()
            return price
    except Exception:
        # If fetch fails but we have a stale cache, use it
        if _price_cache["price"] > 0:
            return _price_cache["price"]
        raise


def calc_trx_amount(usd_price: float) -> int:
    """Convert a USD price to TRX amount (floor, integer -- customer-friendly).

    This is a synchronous wrapper that calls the async price fetcher via the
    event loop. For async contexts, use: trx_price = await get_trx_price_usd()
    and then math.floor(usd_price / trx_price).
    """
    # For use in async context, callers should use calc_trx_amount_async instead.
    # This exists for API compatibility.
    raise RuntimeError("Use calc_trx_amount_async in async context")


async def calc_trx_amount_async(usd_price: float) -> int:
    """Convert a USD price to TRX amount (floor, integer -- customer-friendly)."""
    trx_price = await get_trx_price_usd()
    if trx_price <= 0:
        raise ValueError("TRX price is zero or negative")
    return math.floor(usd_price / trx_price)


# --------------------------------------------------------------------------- #
# TronGrid API helpers.
# --------------------------------------------------------------------------- #
def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if config.TRON_API_KEY:
        h["TRON-PRO-API-KEY"] = config.TRON_API_KEY
    return h


async def _get_tx(client: httpx.AsyncClient, tx_hash: str) -> tuple:
    """Fetch transaction by ID. Returns (response_dict, status_code, body_text)."""
    url = f"{config.TRONGRID_BASE}/wallet/gettransactionbyid"
    r = await client.post(url, json={"value": tx_hash}, headers=_headers())
    body_text = r.text
    r.raise_for_status()
    return r.json() or {}, r.status_code, body_text


# --------------------------------------------------------------------------- #
# VerifyResult.
# --------------------------------------------------------------------------- #
class VerifyResult:
    def __init__(self, ok: bool, reason: str = "", amount: float = 0.0,
                 to_address: str = "", confirmed: bool = False):
        self.ok = ok
        self.reason = reason
        self.amount = amount
        self.to_address = to_address
        self.confirmed = confirmed


# --------------------------------------------------------------------------- #
# Main verification function: verify_trx_payment.
# --------------------------------------------------------------------------- #
async def verify_trx_payment(tx_hash: str, expected_trx: int) -> VerifyResult:
    """Verify ONE native TRX transfer (TransferContract) on-chain.

    Returns VerifyResult(ok=True, ...) only when the transaction is a confirmed
    native TRX transfer of at least `expected_trx` (within tolerance) into the
    configured wallet. `ok=False` carries a human-readable Persian `reason`.

    Retries up to 3 times with 3s delay if the transaction is not found.
    """
    tx_hash = (tx_hash or "").strip().lower().replace("0x", "")
    if not tx_hash or len(tx_hash) < 60:
        return VerifyResult(False, "هش تراکنش نامعتبره.")
    if not config.WALLET_ADDRESS:
        return VerifyResult(False, "آدرس ولت مقصد تنظیم نشده.")

    try:
        wallet_hex = base58_to_hex(config.WALLET_ADDRESS).lower()
    except Exception:
        return VerifyResult(False, "آدرس ولت مقصد نامعتبره.")

    # Retry logic: up to 3 attempts with 3s delay
    tx = None
    last_error = ""
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=config.TRON_TIMEOUT) as client:
                tx_data, status_code, body_text = await _get_tx(client, tx_hash)

                # Log raw TronGrid response for anti-fraud audit
                log_text = (
                    f"[TronGrid] verify tx={tx_hash[:16]}...\n"
                    f"status={status_code}\n"
                    f"body={body_text[:500]}"
                )
                await logbus.to_group(log_text)

                if tx_data and "raw_data" in tx_data:
                    tx = tx_data
                    break
                else:
                    last_error = "تراکنش پیدا نشد (هنوز ثبت نشده؟)."
        except Exception as e:
            last_error = f"خطا در ارتباط با TronGrid: {repr(e)[:140]}"

        if attempt < 2:
            await asyncio.sleep(3)

    if tx is None:
        return VerifyResult(False, last_error)

    try:
        # 1) Check contractRet == SUCCESS
        ret = (tx.get("ret") or [{}])[0]
        if str(ret.get("contractRet", "")).upper() != "SUCCESS":
            return VerifyResult(False, "تراکنش روی شبکه موفق/تاییدشده نیست.")

        # 2) Check contract type is TransferContract (native TRX)
        contracts = tx.get("raw_data", {}).get("contract", [])
        if not contracts:
            return VerifyResult(False, "محتوای تراکنش نامعتبره.")
        c0 = contracts[0]
        if c0.get("type") != "TransferContract":
            return VerifyResult(False, "این تراکنش یک انتقال TRX نیست (نوع اشتباه).")

        # 3) Extract to_address and amount from TransferContract parameters
        value = c0.get("parameter", {}).get("value", {})
        to_address_hex = str(value.get("to_address", "")).lower()
        amount_sun = int(value.get("amount", 0))

        if not to_address_hex:
            return VerifyResult(False, "آدرس مقصد در تراکنش یافت نشد.")

        # 4) Compare destination to our wallet (both in hex form)
        if to_address_hex != wallet_hex:
            # Try to convert for display
            try:
                to_display = hex_to_base58(to_address_hex)
            except Exception:
                to_display = to_address_hex
            return VerifyResult(
                False, "مقصد تراکنش با آدرس ولت ما یکی نیست.",
                to_address=to_display)

        # 5) Convert SUN to TRX (1 TRX = 1,000,000 SUN)
        actual_trx = amount_sun / 1_000_000

        # 6) Check amount with tolerance
        tolerance_str = db.get_setting(
            "payment_tolerance_percent",
            str(config.PAYMENT_TOLERANCE_PERCENT)
        )
        try:
            tolerance = float(tolerance_str)
        except (TypeError, ValueError):
            tolerance = config.PAYMENT_TOLERANCE_PERCENT

        min_acceptable = expected_trx * (1 - tolerance / 100)
        if actual_trx < min_acceptable:
            return VerifyResult(
                False,
                f"مبلغ تراکنش ({actual_trx:g} TRX) کمتر از حد مجاز "
                f"({expected_trx} TRX با {tolerance}% تلرانس) است.",
                amount=actual_trx, to_address=config.WALLET_ADDRESS,
                confirmed=True)

        return VerifyResult(True, "", amount=actual_trx,
                            to_address=config.WALLET_ADDRESS, confirmed=True)

    except Exception as e:
        return VerifyResult(False, f"خطا در پردازش تراکنش: {repr(e)[:140]}")
