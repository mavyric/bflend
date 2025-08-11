#!/usr/bin/env python3
"""
Bitfinex USDT Lending Bot

Fixes:
- Use fUSDT for public market data to avoid 404 on fUST
- Submit offers preferring fUSDT; on failure retry once with fUST
- Increase maker legs so more funds deploy if FRR legs fail
- Keep correct v2 auth and wallet autodetection
"""

import os
import time
import hmac
import hashlib
import json
import math
from typing import Any, Dict, Optional, Tuple, List

import requests

API_KEY = os.getenv("BFX_KEY", "")
API_SECRET = os.getenv("BFX_SEC", "")

# Wallet currency autodetected (UST or USDT) for balance/cancel scope
ASSET_CODE = "UST"

# Market symbol handling:
# - Public endpoints: use fUSDT to avoid 404
# - Private submits: prefer fUSDT; on 4xx/5xx, retry once with fUST
MARKET_SYMBOL_PUBLIC = "fUSDT"
SYMBOL_PREFERRED = "fUSDT"
SYMBOL_FALLBACK = "fUST"

MIN_OFFER = 150.0
CHUNK_SIZE = 500.0
DURATION_D = 2
AUTORENEW = True

# FRR-Delta offsets (bps/day = 0.0001): broad spread
FRR_LADDER_OFFSETS = [0.0, 0.0002, 0.0005, 0.0008, 0.0012]

# Maker legs near best bid
ENABLE_MAKER_LEG = True
MAKER_MAX_CHUNKS_PER_RUN = int(os.getenv("MAKER_MAX_CHUNKS", "6"))  # increased from 2 to 6
MAKER_EPS = 0.00001

MIN_APY_GUARD = float(os.getenv("MIN_APY_GUARD", "0"))
IDLE_WARN_THRESHOLD = 200.0

BASE_URL = "https://api.bitfinex.com"


def _nonce() -> str:
    return str(int(time.time() * 1000))


def _sign_headers(path_no_slash: str, raw_body: str, nonce: str) -> Dict[str, str]:
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


def autodetect_wallet_currency(rows: List[List[Any]]) -> None:
    global ASSET_CODE
    seen = set()
    for w in rows:
        try:
            code = str(w[1]).upper()
            seen.add(code)
        except Exception:
            continue
    # Prefer USDT if present
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
                    if wtype == "funding" and currency in ("USDT", "UST") and currency == ASSET_CODE:
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


def funding_best_bid_ask() -> Tuple[Optional[float], Optional[float]]:
    try:
        book = _get_public(f"/v2/book/funding/{MARKET_SYMBOL_PUBLIC}/R0", params={"len": 25})
        bids, asks = [], []
        for row in book:
            if isinstance(row, list) and len(row) >= 4:
                rate = safe_float(row[1])
                amount = safe_float(row[2])
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
    try:
        data = _get_public(f"/v2/funding/stats/{MARKET_SYMBOL_PUBLIC}/last")
        # Some deployments return [MTS, VALUE], others just VALUE
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


def submit_offer_with_symbol(amount: float, rate: float, period: int, oftype: str, flags: int, symbol: str) -> Any:
    body = {
        "type": oftype,
        "symbol": symbol,
        "amount": f"{amount:.6f}",
        "rate": f"{rate:.6f}",
        "period": period,
        "flags": flags,
    }
    return _post_private("v2/auth/w/funding/offer/submit", body)


def submit_offer(amount: float, rate: float, period: int, oftype: str, flags: int = 0) -> Any:
    # Try preferred symbol first; on error, retry once with fallback
    try:
        return submit_offer_with_symbol(amount, rate, period, oftype, flags, SYMBOL_PREFERRED)
    except Exception as e1:
        print(f"Submit with {SYMBOL_PREFERRED} failed: {e1}. Retrying with {SYMBOL_FALLBACK}...")
        return submit_offer_with_symbol(amount, rate, period, oftype, flags, SYMBOL_FALLBACK)


def auto_renew_flag() -> int:
    return 1024 if AUTORENEW else 0


def place_multiple_frr_offers_all_funds(free_bal: float, base_frr: float, min_apy_guard: float) -> float:
    flags = auto_renew_flag()
    remaining = free_bal
    base_apy = daily_to_apy(base_frr) if base_frr > 0 else 0.0
    low_env = (min_apy_guard > 0 and base_apy * 100 < min_apy_guard)
    offsets = [0.0] if low_env else FRR_LADDER_OFFSETS

    chunk_idx = 0
    while remaining >= MIN_OFFER:
        amt = min(CHUNK_SIZE, remaining)
        if amt < MIN_OFFER:
            break
        offset = offsets[chunk_idx % len(offsets)]
        try:
            resp = submit_offer(amt, base_frr + offset, DURATION_D, "FRRDELTA", flags)
            eff_daily = max(base_frr + offset, 0.0)
            print(
                f"FRR-Delta chunk {chunk_idx + 1}: amount={amt:.2f} offset={offset:.6f} "
                f"(daily), APY~{apy_to_str(daily_to_apy(eff_daily))} -> {resp}"
            )
            remaining -= amt
            chunk_idx += 1
        except Exception as e:
            print(f"Error placing FRR-Delta chunk {chunk_idx + 1}: {e}")
            # Break to avoid looping on repeated errors; maker legs will try next
            break

    if low_env:
        print(f"Low APY environment: base APY {apy_to_str(base_apy)} < guard {min_apy_guard:.2f}%. Placed multiple FRR=0 chunks.")
    return remaining


def place_maker_legs(remaining: float, best_bid: Optional[float]) -> float:
    if not ENABLE_MAKER_LEG or remaining < MIN_OFFER:
        return remaining
    flags = auto_renew_flag()
    max_chunks = min(MAKER_MAX_CHUNKS_PER_RUN, int(remaining // CHUNK_SIZE))
    if max_chunks <= 0:
        return remaining

    if best_bid:
        target_rate = max(best_bid - MAKER_EPS, 0.000001)
    else:
        target_rate = max(fetch_frr_daily_rate(), 0.0002)

    for i in range(max_chunks):
        if remaining < MIN_OFFER:
            break
        amt = min(CHUNK_SIZE, remaining)
        if amt < MIN_OFFER:
            break
        try:
            resp = submit_offer(amt, target_rate, DURATION_D, "LIMIT", flags)
            print(
                f"Maker leg {i + 1}: amount={amt:.2f} rate={target_rate:.6f} "
                f"(daily), APY~{apy_to_str(daily_to_apy(target_rate))} -> {resp}"
            )
            remaining -= amt
        except Exception as e:
            print(f"Error placing maker leg {i + 1}: {e}")
            break
    return remaining


def main():
    print("---- Bitfinex USDT Lending Bot ----")

    cancel_all_usdt_offers()

    free_bal = get_free_usdt_balance()
    print(f"Using currency: {ASSET_CODE}, market symbol (public): {MARKET_SYMBOL_PUBLIC}")
    print(f"Free USDT (funding wallet): {free_bal:.2f}")
    if free_bal < MIN_OFFER:
        print("Nothing to lend (below minimum offer).")
        return

    best_bid, best_ask = funding_best_bid_ask()
    if best_bid:
        print(f"Best bid/day: {best_bid:.6f} ({apy_to_str(daily_to_apy(best_bid))})")
    if best_ask:
        print(f"Best ask/day: {best_ask:.6f} ({apy_to_str(daily_to_apy(best_ask))})")
    frr = fetch_frr_daily_rate()
    if frr:
        print(f"FRR estimate/day: {frr:.6f} ({apy_to_str(daily_to_apy(frr))})")

    remaining = place_multiple_frr_offers_all_funds(free_bal, frr, MIN_APY_GUARD)
    remaining = place_maker_legs(remaining, best_bid)

    if remaining >= MIN_OFFER:
        flags = auto_renew_flag()
        amt = remaining
        try:
            resp = submit_offer(amt, 0.0, DURATION_D, "FRRDELTA", flags)
            print(f"Final sweep FRR leg: amount={amt:.2f} offset=0.0 -> {resp}")
            remaining = 0.0
        except Exception as e:
            print(f"Final sweep error: {e}")

    if remaining >= IDLE_WARN_THRESHOLD:
        print("Warning:", f"{remaining:.2f} USDT still idle. Consider increasing maker legs or lowering MIN_APY_GUARD.")

    print("Run complete.")


if __name__ == "__main__":
    main()
