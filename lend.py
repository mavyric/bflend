#!/usr/bin/env python3
import os
import time
import hmac
import hashlib
import json
import math
from typing import Any, Dict, Optional, Tuple, List

import requests

# ---------------- Configuration ----------------

API_KEY = os.getenv("BFX_KEY", "")
API_SECRET = os.getenv("BFX_SEC", "")
# Default to UST/fUST; will auto-switch to USDT/fUSDT if detected in wallets
ASSET_CODE = "UST"  # wallet currency code (UST or USDT)
SYMBOL = "fUST"     # funding symbol (fUST or fUSDT)

MIN_OFFER = 150.0       # Bitfinex minimum per offer
CHUNK_SIZE = 500.0      # per-requested chunk size
DURATION_D = 2          # loan period in days (2–120)
AUTORENEW = True        # auto-renew enabled

# Laddered FRR-Delta offsets in daily rate decimals (0.0001 = 1 bp/day)
FRR_LADDER_OFFSETS = [0.0, 0.0002, 0.0005]  # 0, +2 bps/day, +5 bps/day

# Maker leg near top-of-book (fixed rate LIMIT)
ENABLE_MAKER_LEG = True
MAKER_CHUNKS = 1
MAKER_EPS = 0.000001    # step inside top of book

# Minimum acceptable APY guard (annualized from daily). If FRR APY < guard:
# only a single FRR=0 leg is placed for liquidity.
MIN_APY_GUARD = float(os.getenv("MIN_APY_GUARD", "0"))  # e.g., 5 for 5% APY

IDLE_WARN_THRESHOLD = 200.0

BASE_URL = "https://api.bitfinex.com"

# ---------------- Utilities ----------------

def _nonce() -> str:
    return str(int(time.time() * 1000))

def _sign_headers(path_no_slash: str, raw_body: str, nonce: str) -> Dict[str, str]:
    # Signature must be: "/api/" + path + nonce + raw_body, HMAC-SHA384(secret)
    sig_str = "/api/" + path_no_slash + nonce + raw_body
    signature = hmac.new(API_SECRET.encode("utf-8"), sig_str.encode("utf-8"), hashlib.sha384).hexdigest()
    return {
        "bfx-nonce": nonce,
        "bfx-apikey": API_KEY,
        "bfx-signature": signature,
        "content-type": "application/json",
    }

def _post_private(path_no_slash: str, body: Dict[str, Any]) -> Any:
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Missing API credentials: set BFX_KEY and BFX_SEC")
    nonce = _nonce()
    raw_body = json.dumps({**body, "nonce": nonce})
    headers = _sign_headers(path_no_slash, raw_body, nonce)
    url = BASE_URL + "/" + path_no_slash
    r = requests.post(url, data=raw_body, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def _get_public(path_with_leading_slash: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = BASE_URL + path_with_leading_slash
    r = requests.get(url, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def daily_to_apy(d: float) -> float:
    return (1.0 + d) ** 365 - 1.0

def apy_to_str(apy: float) -> str:
    return f"{apy*100:.2f}%"

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# ---------------- Bitfinex helpers ----------------

def autodetect_symbols_from_wallets(rows: List[List[Any]]) -> None:
    # If wallets contain USDT instead of UST, switch globals accordingly.
    global ASSET_CODE, SYMBOL
    codes = {row[1] for row in rows if isinstance(row, list) and len(row) >= 2}
    if "USDT" in codes:
        ASSET_CODE = "USDT"
        SYMBOL = "fUSDT"
    elif "UST" in codes:
        ASSET_CODE = "UST"
        SYMBOL = "fUST"

def get_free_usdt_balance() -> float:
    # POST v2/auth/r/wallets
    try:
        resp = _post_private("v2/auth/r/wallets", {})
        free = 0.0
        valid_rows = []
        for w in resp:
            if isinstance(w, list) and len(w) >= 5:
                valid_rows.append(w)
        if valid_rows:
            autodetect_symbols_from_wallets(valid_rows)
            for w in valid_rows:
                wtype = w[0]
                currency = w[1]
                available = safe_float(w[4], 0.0)
                if wtype == "funding" and currency == ASSET_CODE:
                    free = max(free, available)
        return free
    except Exception as e:
        print("Balance fetch error:", e)
        return 0.0

def cancel_all_usdt_offers() -> None:
    # POST v2/auth/w/funding/offer/cancel/all
    try:
        # Body can be {} or {"symbol": ASSET_CODE}. Use symbol to limit to USDT.
        body = {"symbol": ASSET_CODE}
        resp = _post_private("v2/auth/w/funding/offer/cancel/all", body)
        print("Canceled open funding offers:", resp)
    except Exception as e:
        print("Cancel-all error:", e)

def funding_best_bid_ask() -> Tuple[Optional[float], Optional[float]]:
    # GET /v2/book/funding/{SYMBOL}/R0
    try:
        book = _get_public(f"/v2/book/funding/{SYMBOL}/R0", params={"len": 25})
        bids, asks = [], []
        for row in book:
            # expected: [RATE, PERIOD, COUNT, AMOUNT]
            if isinstance(row, list) and len(row) >= 4:
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

def fetch_frr_daily_rate() -> float:
    # GET /v2/funding/stats/{SYMBOL}/last
    try:
        data = _get_public(f"/v2/funding/stats/{SYMBOL}/last")
        # Some variants return [MTS, VALUE], others just VALUE
        if isinstance(data, list):
            if len(data) >= 2:
                val = safe_float(data[1], 0.0)
                if val > 0:
                    return val
            elif len(data) == 1:
                val = safe_float(data[0], 0.0)
                if val > 0:
                    return val
        else:
            val = safe_float(data, 0.0)
            if val > 0:
                return val
    except Exception as e:
        print("FRR fetch error:", e)
    # Fallback to book mid or a conservative default
    best_bid, best_ask = funding_best_bid_ask()
    if best_bid and best_ask:
        return (best_bid + best_ask) / 2.0
    return max(best_bid or 0.0002, 0.0002)

def submit_offer(amount: float, rate: float, period: int, oftype: str, flags: int = 0) -> Any:
    # POST v2/auth/w/funding/offer/submit
    body = {
        "type": oftype,    # "LIMIT" or "FRRDELTA"
        "symbol": SYMBOL,  # fUST or fUSDT
        "amount": f"{amount:.6f}",
        "rate": f"{rate:.6f}",  # daily rate (LIMIT) or offset (FRRDELTA)
        "period": period,
        "flags": flags,
    }
    return _post_private("v2/auth/w/funding/offer/submit", body)

def auto_renew_flag() -> int:
    # Commonly used funding auto-renew flag
    return 1024 if AUTORENEW else 0

# ---------------- Strategy ----------------

def place_laddered_frr_offers(free_bal: float, base_frr: float, min_apy_guard: float) -> float:
    flags = auto_renew_flag()
    remaining = free_bal
    base_apy = daily_to_apy(base_frr) if base_frr > 0 else 0.0
    low_env = (min_apy_guard > 0 and base_apy * 100 < min_apy_guard)
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
                f"FRR-Delta leg {i+1}: amount={amt:.2f} offset={offset:.6f} "
                f"(daily), APY~{apy_to_str(daily_to_apy(max(base_frr+offset, 0.0)))} -> {resp}"
            )
            remaining -= amt
        except Exception as e:
            print(f"Error placing FRR-Delta leg {i+1}: {e}")

    if low_env:
        print(f"Low APY environment: base APY {apy_to_str(base_apy)} < guard {min_apy_guard:.2f}%. Only FRR=0 leg placed.")
    return remaining

def place_maker_legs(remaining: float, best_bid: Optional[float], best_ask: Optional[float]) -> float:
    if not ENABLE_MAKER_LEG or MAKER_CHUNKS <= 0 or remaining < MIN_OFFER:
        return remaining
    flags = auto_renew_flag()
    chunks = min(MAKER_CHUNKS, int(remaining // CHUNK_SIZE))
    if chunks <= 0:
        return remaining

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
                f"Maker leg {i+1}: amount={amt:.2f} rate={target_rate:.6f} "
                f"(daily), APY~{apy_to_str(daily_to_apy(target_rate))} -> {resp}"
            )
            remaining -= amt
        except Exception as e:
            print(f"Error placing maker leg {i+1}: {e}")
    return remaining

def main():
    print("---- Bitfinex USDT Lending Bot ----")

    # 1) Cancel stale offers (best-effort; do not exit on error)
    cancel_all_usdt_offers()

    # 2) Check free balance (also auto-detects UST vs USDT)
    free_bal = get_free_usdt_balance()
    print(f"Using currency: {ASSET_CODE}, symbol: {SYMBOL}")
    print(f"Free USDT (funding wallet): {free_bal:.2f}")
    if free_bal < MIN_OFFER:
        print("Nothing to lend (below minimum offer).")
        return

    # 3) Market context
    best_bid, best_ask = funding_best_bid_ask()
    if best_bid:
        print(f"Best bid/day: {best_bid:.6f} ({apy_to_str(daily_to_apy(best_bid))})")
    if best_ask:
        print(f"Best ask/day: {best_ask:.6f} ({apy_to_str(daily_to_apy(best_ask))})")
    frr = fetch_frr_daily_rate()
    if frr:
        print(f"FRR estimate/day: {frr:.6f} ({apy_to_str(daily_to_apy(frr))})")

    # 4) Place FRR ladder first
    remaining = place_laddered_frr_offers(free_bal, frr, MIN_APY_GUARD)

    # 5) Maker leg (optional)
    remaining = place_maker_legs(remaining, best_bid, best_ask)

    # 6) Final sweep to FRR=0
    if remaining >= MIN_OFFER:
        flags = auto_renew_flag()
        amt = math.floor(remaining / CHUNK_SIZE) * CHUNK_SIZE
        if amt < MIN_OFFER and remaining >= MIN_OFFER:
            amt = remaining
        if amt >= MIN_OFFER:
            try:
                resp = submit_offer(amt, 0.0, DURATION_D, "FRRDELTA", flags)
                print(f"Final sweep FRR leg: amount={amt:.2f} offset=0.0 -> {resp}")
                remaining -= amt
            except Exception as e:
                print(f"Final sweep error: {e}")

    # 7) Idle warning
    if remaining >= IDLE_WARN_THRESHOLD:
        print("Warning:", f"{remaining:.2f} USDT still idle. Consider widening ladder/maker or lowering MIN_APY_GUARD.")

    print("Run complete.")

if __name__ == "__main__":
    main()
