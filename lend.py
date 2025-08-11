#!/usr/bin/env python3
import os
import time
import hmac
import hashlib
import json
from typing import Any, Dict, Optional, Tuple, List

import requests

# -------- Config --------

API_KEY = os.getenv("BFX_KEY", "")
API_SECRET = os.getenv("BFX_SEC", "")
# Wallet currency for balance/cancel scope will be auto-detected as UST or USDT
ASSET_CODE = "UST"
# Public market endpoints: use fUSDT to avoid 404s on fUST in some routes
MARKET_SYMBOL_PUBLIC = "fUSDT"
# Private submit: try fUSDT first; on error, retry once with fUST
SYMBOL_PREFERRED = "fUSDT"
SYMBOL_FALLBACK = "fUST"

MIN_OFFER = 150.0
CHUNK_SIZE = 1000.0
DURATION_D = 2
AUTORENEW = True

# Small symmetric spread around last matched rate (bps/day = 0.0001)
SPREAD_OFFSETS = [-0.0005, -0.0003, -0.0001, 0.0, 0.0001, 0.0003, 0.0005]
# Optional APY guard (informational only)
MIN_APY_GUARD = float(os.getenv("MIN_APY_GUARD", "0"))
IDLE_WARN_THRESHOLD = 200.0

BASE_URL = "https://api.bitfinex.com"

# -------- Utilities --------

def _nonce() -> str:
    return str(int(time.time() * 1000))

def _sign_headers(path_no_slash: str, raw_body: str, nonce: str) -> Dict[str, str]:
    # Signature = HMAC_SHA384(secret, "/api/" + path + nonce + raw_body)
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

# -------- Wallet & symbols --------

def autodetect_wallet_currency(rows: List[List[Any]]) -> None:
    global ASSET_CODE
    seen = set()
    for w in rows:
        try:
            code = str(w[1]).upper() if len(w) > 1 else str(w).upper()
            seen.add(code)
        except Exception:
            continue
    if "USDT" in seen:
        ASSET_CODE = "USDT"
    elif "UST" in seen:
        ASSET_CODE = "UST"

def get_free_usdt_balance() -> float:
    try:
        resp = _post_private("v2/auth/r/wallets", {})
        free = 0.0
        valid_rows = [w for w in resp if isinstance(w, list) and len(w) >= 5]
        if valid_rows:
            autodetect_wallet_currency(valid_rows)
            for w in valid_rows:
                try:
                    wtype = str(w[0]).lower()
                    currency = str(w[1]).upper()
                    available = safe_float(w[2], 0.0)
                    if wtype == "funding" and currency == ASSET_CODE:
                        free = max(free, available)
                except Exception:
                    continue
        return free
    except Exception as e:
        print("Balance fetch error:", e)
        return 0.0

def cancel_all_usdt_offers() -> None:
    try:
        body = {"symbol": ASSET_CODE}
        resp = _post_private("v2/auth/w/funding/offer/cancel/all", body)
        print("Canceled open funding offers:", resp)
    except Exception as e:
        print("Cancel-all error:", e)

# -------- Market data (public) --------

def funding_best_bid_ask() -> Tuple[Optional[float], Optional[float]]:
    try:
        # /v2/book/funding/{symbol}/R0 returns [RATE, PERIOD, COUNT, AMOUNT] rows for funding
        book = _get_public(f"/v2/book/funding/{MARKET_SYMBOL_PUBLIC}/R0", params={"len": 25})
        bids, asks = [], []
        for row in book:
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

def fetch_last_matched_daily_rate() -> Optional[float]:
    """
    Latest matched funding trade rate:
    GET /v2/trades/funding/{symbol}/hist?limit=1 → [[ID, MTS, AMOUNT, RATE, PERIOD]] for funding.
    """
    try:
        trades = _get_public(f"/v2/trades/funding/{MARKET_SYMBOL_PUBLIC}/hist", params={"limit": 1, "sort": -1})
        if isinstance(trades, list) and trades:
            row = trades[0]
            if isinstance(row, list) and len(row) >= 5:
                rate = safe_float(row[3], 0.0)
                if rate > 0:
                    return rate
    except Exception as e:
        print("Last matched fetch error:", e)
    return None

def fetch_frr_daily_rate() -> float:
    # Public funding stats "last" for FRR and related values; use the main rate as a fallback anchor
    try:
        data = _get_public(f"/v2/funding/stats/{MARKET_SYMBOL_PUBLIC}/last")
        # Docs vary; accept scalar or [MTS, VALUE]
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
    best_bid, best_ask = funding_best_bid_ask()
    if best_bid and best_ask:
        return (best_bid + best_ask) / 2.0
    return max(best_bid or 0.0002, 0.0002)

# -------- Submissions (private) --------

def submit_offer_with_symbol(amount: float, rate: float, period: int, oftype: str, flags: int, symbol: str) -> Any:
    # POST /v2/auth/w/funding/offer/submit
    body = {
        "type": oftype,  # "LIMIT" or "FRRDELTA"
        "symbol": symbol,  # fUSDT (preferred) or fUST (fallback)
        "amount": f"{amount:.6f}",
        "rate": f"{rate:.6f}",
        "period": period,
        "flags": flags,
    }
    return _post_private("v2/auth/w/funding/offer/submit", body)

def submit_offer(amount: float, rate: float, period: int, oftype: str, flags: int = 0) -> Any:
    # Try preferred symbol first; on error retry with fallback once
    try:
        return submit_offer_with_symbol(amount, rate, period, oftype, flags, SYMBOL_PREFERRED)
    except Exception as e1:
        print(f"Submit with {SYMBOL_PREFERRED} failed: {e1}. Retrying with {SYMBOL_FALLBACK}...")
        return submit_offer_with_symbol(amount, rate, period, oftype, flags, SYMBOL_FALLBACK)

def auto_renew_flag() -> int:
    return 1024 if AUTORENEW else 0

# -------- Strategy --------

def place_limit_offers_around_last_rate(free_bal: float, last_rate: float) -> float:
    """
    Split all available funds into $1000 chunks and place LIMIT offers at:
    last_rate * (1 + offset), cycling offsets round-robin.
    """
    flags = auto_renew_flag()
    remaining = free_bal
    idx = 0
    while remaining >= MIN_OFFER:
        amt = min(CHUNK_SIZE, remaining)
        if amt < MIN_OFFER:
            break
        offset = SPREAD_OFFSETS[idx % len(SPREAD_OFFSETS)]
        target_rate = max(last_rate * (1.0 + offset), 0.000001)
        try:
            resp = submit_offer(amt, target_rate, DURATION_D, "LIMIT", flags)
            print(
                f"LIMIT chunk {idx+1}: amount={amt:.2f} rate={target_rate:.6f} "
                f"(offset {offset:+.4f}), APY~{apy_to_str(daily_to_apy(target_rate))} -> {resp}"
            )
            remaining -= amt
            idx += 1
        except Exception as e:
            print(f"Error placing LIMIT chunk {idx+1}: {e}")
            break
    return remaining

def main():
    print("---- Bitfinex USDT Lending Bot ----")

    # 1) Cancel stale offers
    cancel_all_usdt_offers()

    # 2) Free balance
    free_bal = get_free_usdt_balance()
    print(f"Using wallet currency: {ASSET_CODE} | public market symbol: {MARKET_SYMBOL_PUBLIC}")
    print(f"Free USDT (funding wallet): {free_bal:.2f}")
    if free_bal < MIN_OFFER:
        print("Nothing to lend (below minimum offer).")
        return

    # 3) Anchor: latest matched funding trade rate; fallback to FRR/book
    last_rate = fetch_last_matched_daily_rate()
    if last_rate:
        print(f"Last matched daily rate: {last_rate:.6f} ({apy_to_str(daily_to_apy(last_rate))})")
    else:
        last_rate = fetch_frr_daily_rate()
        print(f"Using fallback daily rate: {last_rate:.6f} ({apy_to_str(daily_to_apy(last_rate))})")

    if MIN_APY_GUARD > 0:
        apy = daily_to_apy(last_rate) * 100
        if apy < MIN_APY_GUARD:
            print(f"Warning: Last-rate APY {apy:.2f}% below guard {MIN_APY_GUARD:.2f}%. Proceeding with placement around last rate.")

    # 4) Place multiple $1000 LIMIT offers around last rate
    remaining = place_limit_offers_around_last_rate(free_bal, last_rate)

    # 5) Final sweep: any leftover >=150 goes exactly at last_rate
    if remaining >= MIN_OFFER:
        flags = auto_renew_flag()
        amt = remaining
        try:
            resp = submit_offer(amt, max(last_rate, 0.000001), DURATION_D, "LIMIT", flags)
            print(f"Final sweep LIMIT: amount={amt:.2f} rate={last_rate:.6f} -> {resp}")
            remaining = 0.0
        except Exception as e:
            print(f"Final sweep error: {e}")

    if remaining >= IDLE_WARN_THRESHOLD:
        print("Warning:", f"{remaining:.2f} USDT still idle. Consider adjusting SPREAD_OFFSETS or CHUNK_SIZE.")

    print("Run complete.")

if __name__ == "__main__":
    main()

# Notes:
# - Latest matched rate source: /v2/trades/funding/fUSDT/hist returns funding trades as [ID, MTS, AMOUNT, RATE, PERIOD].
# - FRR fallback: /v2/funding/stats/fUSDT/last or book mid from /v2/book/funding/fUSDT/R0 when needed.
# - Submissions: /v2/auth/w/funding/offer/submit expects LIMIT or FRRDELTA; we use LIMIT with daily rate decimals (e.g., 0.0005=5bps/day).
# - Auth signing follows Bitfinex v2 rules; private endpoints all use POST with raw JSON body and “/api/” prefix in the signature string.
