#!/usr/bin/env python3
import os
import time
import hmac
import hashlib
import json
import math
from typing import Any, Dict, List, Optional, Tuple

import requests

# ---------------- Configuration ----------------

API_KEY = os.getenv("BFX_KEY", "")
API_SECRET = os.getenv("BFX_SEC", "")

ASSET_CODE = "UST"  # currency code (USDT) for balances/cancel
SYMBOL = "fUST"     # funding symbol
MIN_OFFER = 150.0   # Bitfinex minimum per offer (do not change below 150)
CHUNK_SIZE = 500.0  # your requested chunk size
DURATION_D = 2      # loan period in days (2–120)
AUTORENEW = True    # auto-renew enabled
RUN_IDEAL_EVERY_MIN = 10  # purely informational for logs

# Laddered FRR-Delta offsets in daily rate decimals (0.0001 = 1 bp/day)
# Keep a zero-offset leg to maximize fill probability; add positive offsets for better rates.
FRR_LADDER_OFFSETS = [0.0, 0.0002, 0.0005]  # 0, +2 bps/day, +5 bps/day

# Maker leg: try to capture a slightly better fixed rate for one chunk
ENABLE_MAKER_LEG = True
MAKER_CHUNKS = 1          # number of maker chunks
MAKER_EPS = 0.000001      # 0.0001 bp/day adjustment to step inside top of book

# Minimum acceptable APY guard (annualized from daily). If FRR-derived APY < guard:
# - we still post one FRR=0 leg for liquidity, but skip extra offsets & maker.
MIN_APY_GUARD = float(os.getenv("MIN_APY_GUARD", "0"))  # e.g., "5" for 5% APY; default 0 disables guard

# Idle warning threshold: alert if free balance remains above this after placing offers
IDLE_WARN_THRESHOLD = 200.0

BASE_URL = "https://api.bitfinex.com"

# ---------------- Utilities ----------------

def _nonce() -> str:
    return str(int(time.time() * 1000))

def _sign_payload(path: str, body: Dict[str, Any]) -> Dict[str, str]:
    """
    Bitfinex v2 auth headers: POST /api/v2/<path>
    X-BFX-APIKEY, X-BFX-PAYLOAD (raw JSON), X-BFX-SIGNATURE (HMAC SHA384)
    """
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Missing API credentials: set BFX_KEY and BFX_SEC")

    j = json.dumps(body)
    sig = f"/api{path}{j}{body['nonce']}"
    h = hmac.new(API_SECRET.encode(), sig.encode(), hashlib.sha384).hexdigest()
    return {
        "bfx-nonce": body["nonce"],
        "bfx-apikey": API_KEY,
        "bfx-signature": h,
        "content-type": "application/json",
    }

def _post(path: str, body: Dict[str, Any], auth: bool = False) -> Any:
    url = BASE_URL + path
    headers = {"content-type": "application/json"}
    if auth:
        body = {**body, "nonce": _nonce()}
        headers = _sign_payload(path, body)
    r = requests.post(url, data=json.dumps(body), headers=headers, timeout=20)
    r.raise_for_status()
    return r.json()

def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = BASE_URL + path
    r = requests.get(url, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()

def daily_to_apy(d: float) -> float:
    # APY approx (1 + d)^365 - 1
    return (1.0 + d) ** 365 - 1.0

def apy_to_str(apy: float) -> str:
    return f"{apy*100:.2f}%"

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# ---------------- Bitfinex funding helpers ----------------

def cancel_all_usdt_offers() -> None:
    """Cancel all open USDT funding offers."""
    path = "/v2/auth/w/funding/offer/cancel/all"
    body = {"symbol": ASSET_CODE}
    try:
        resp = _post(path, body, auth=True)
        print("Canceled all open USDT funding offers:", resp)
    except Exception as e:
        print("Cancel-all error:", e)

def get_free_usdt_balance() -> float:
    """Return free (available) USDT in funding wallet."""
    path = "/v2/auth/r/wallets"
    try:
        resp = _post(path, {}, auth=True)
        free = 0.0
        # Wallet entry: [TYPE, CURRENCY, BALANCE, UNCONF, BALANCE_AVAILABLE] in v2 REST
        for w in resp:
            wtype = w[0] if len(w) > 0 else ""
            currency = w[1] if len(w) > 1 else ""
            bal_av = safe_float(w[4] if len(w) > 4 else 0.0)
            if wtype == "funding" and currency == ASSET_CODE:
                free = max(free, bal_av)
        return free
    except Exception as e:
        print("Balance fetch error:", e)
        return 0.0

def fetch_frr_daily_rate() -> float:
    """Fetch the most recent FRR daily rate."""
    try:
        data = _get(f"/v2/funding/stats/{SYMBOL}/last")
        if isinstance(data, list) and len(data) >= 1:
            frr = safe_float(data[0], 0.0)
            if frr > 0:
                return frr
    except Exception as e:
        print("FRR fetch error:", e)

    # Fallback: estimate from top-of-book
    best_bid, best_ask = funding_best_bid_ask()
    if best_bid and best_ask:
        return (best_bid + best_ask) / 2.0
    return max(best_bid or 0.0002, 0.0002)  # default 2 bps/day fallback

def funding_best_bid_ask() -> Tuple[Optional[float], Optional[float]]:
    """Return (best_bid, best_ask) for funding book."""
    try:
        book = _get(f"/v2/book/funding/{SYMBOL}/R0", params={"len": 25})
        bids = []
        asks = []
        for row in book:
            rate = safe_float(row[0])
            amount = safe_float(row[3])
            if amount > 0:
                bids.append(rate)
            elif amount < 0:
                asks.append(rate)
        best_bid = max(bids) if bids else None
        best_ask = min(asks) if asks else None
        return best_bid, best_ask
    except Exception as e:
        print("Book fetch error:", e)
        return None, None

def submit_offer(amount: float, rate: float, period: int, oftype: str, flags: int = 0) -> Any:
    """Submit a funding offer."""
    path = "/v2/auth/w/funding/offer/submit"
    body = {
        "type": oftype,      # "LIMIT" or "FRRDELTA"
        "symbol": SYMBOL,    # fUST
        "amount": f"{amount:.8f}",
        "rate": f"{rate:.8f}", # daily rate or FRR delta offset
        "period": period,    # in days
        "flags": flags,
    }
    return _post(path, body, auth=True)

def auto_renew_flag() -> int:
    """Return Bitfinex auto-renew flag value."""
    # 1024 (0x400) is the classic value; 16384 (0x4000) is also sometimes mentioned.
    return 1024 if AUTORENEW else 0

# ---------------- Strategy ----------------

def place_laddered_frr_offers(free_bal: float, base_frr: float, min_apy_guard: float) -> float:
    """
    Places FRR-Delta variable offers across the ladder offsets.
    Returns remaining free balance after submissions.
    """
    flags = auto_renew_flag()
    remaining = free_bal

    base_apy = daily_to_apy(base_frr) if base_frr > 0 else 0.0
    low_env = (min_apy_guard > 0 and base_apy*100 < min_apy_guard)
    ladder = [0.0] if low_env else FRR_LADDER_OFFSETS

    for i, offset in enumerate(ladder):
        if remaining < MIN_OFFER:
            break
        amt = min(CHUNK_SIZE, remaining)
        if amt < MIN_OFFER:
            break
        try:
            resp = submit_offer(amt, offset, DURATION_D, "FRRDELTA", flags)
            print(
                f"FRR-Delta leg {i+1}: amount={amt:.2f} "
                f"offset={offset:.6f} (daily), APY~{apy_to_str(daily_to_apy(base_frr+offset))} -> {resp}"
            )
            remaining -= amt
        except Exception as e:
            print(f"Error placing FRR-Delta leg {i+1}: {e}")

    if low_env:
        print(
            f"Low APY environment detected (base APY {apy_to_str(base_apy)} < guard {min_apy_guard:.2f}%). "
            "Restricted to single FRR leg."
        )
    return remaining

def place_maker_legs(remaining: float, best_bid: Optional[float], best_ask: Optional[float]) -> float:
    """
    Places up to MAKER_CHUNKS fixed-rate LIMIT offers targeting top-of-book.
    """
    if not ENABLE_MAKER_LEG or MAKER_CHUNKS <= 0 or remaining < MIN_OFFER:
        return remaining

    flags = auto_renew_flag()
    chunks = min(MAKER_CHUNKS, int(remaining // CHUNK_SIZE))
    if chunks <= 0:
        return remaining

    # Choose a competitive daily rate: near best bid; if not available, fallback to FRR estimate
    if best_bid:
        target_rate = max(best_bid - MAKER_EPS, 0.000001)
    else:
        target_rate = max(fetch_frr_daily_rate(), 0.0002)

    for i in range(chunks):
        if remaining < MIN_OFFER:
            break
        amt = min(CHUNK_SIZE, remaining)
        if amt < MIN_OFFER:
            break
        try:
            resp = submit_offer(amt, target_rate, DURATION_D, "LIMIT", flags)
            print(
                f"Maker leg {i+1}: amount={amt:.2f} rate={target_rate:.6f} (daily), "
                f"APY~{apy_to_str(daily_to_apy(target_rate))} -> {resp}"
            )
            remaining -= amt
        except Exception as e:
            print(f"Error placing maker leg {i+1}: {e}")
    return remaining

def main():
    print("---- Bitfinex USDT Lending Bot (serverless) ----")
    # 1) Cancel stale offers
    cancel_all_usdt_offers()

    # 2) Check free balance
    free_bal = get_free_usdt_balance()
    print(f"Free USDT (funding wallet): {free_bal:.2f}")

    if free_bal < MIN_OFFER:
        print("Nothing to lend (below minimum offer).")
        return

    # 3) Market context
    best_bid, best_ask = funding_best_bid_ask()
    if best_bid:
        print(f"Top-of-book: bestBid={best_bid:.6f}/day ({apy_to_str(daily_to_apy(best_bid))})")
    if best_ask:
        print(f"Top-of-book: bestAsk={best_ask:.6f}/day ({apy_to_str(daily_to_apy(best_ask))})")
    frr = fetch_frr_daily_rate()
    if frr:
        print(f"Estimated FRR daily: {frr:.6f} ({apy_to_str(daily_to_apy(frr))})")

    # 4) Place FRR ladder first for continuous fill
    remaining = place_laddered_frr_offers(free_bal, frr, MIN_APY_GUARD)

    # 5) Optional maker leg for a chunk (captures spikes)
    remaining = place_maker_legs(remaining, best_bid, best_ask)

    # 6) If any balance remains ≥ min offer, dump to FRR=0 for maximal fill probability
    if remaining >= MIN_OFFER:
        flags = auto_renew_flag()
        amt = math.floor(remaining / CHUNK_SIZE) * CHUNK_SIZE
        if amt < MIN_OFFER and remaining >= MIN_OFFER:
            amt = remaining  # use whatever is left if at least min offer
        if amt >= MIN_OFFER:
            try:
                resp = submit_offer(amt, 0.0, DURATION_D, "FRRDELTA", flags)
                print(f"Final sweep FRR leg: amount={amt:.2f} offset=0.0 -> {resp}")
                remaining -= amt
            except Exception as e:
                print(f"Final sweep error: {e}")

    # 7) Warn if significant idle remains
    if remaining >= IDLE_WARN_THRESHOLD:
        print(
            f"Warning: {remaining:.2f} USDT still idle after this cycle. "
            "Consider increasing ladder size, maker chunks, or reducing MIN_APY_GUARD."
        )

    print("Run complete.")

if __name__ == "__main__":
    main()
