#!/usr/bin/env python3
"""
Bitfinex USDT Lending Bot — mid-price anchor with LAST_PRICE lean and spread

Public funding ticker: GET https://api-pub.bitfinex.com/v2/tickers?symbols=fUST
Private submit: POST /v2/auth/w/funding/offer/submit
Cancel open offers: POST /v2/auth/w/funding/offer/cancel/all
Wallets: POST /v2/auth/r/wallets
Signing: HMAC-SHA384 over "/api/" + path + nonce + raw_body
"""

import os
import time
import hmac
import hashlib
import json
from typing import Any, Dict, Optional, List

import requests

# ---------- Config ----------

API_KEY = os.getenv("BFX_KEY", "")
API_SECRET = os.getenv("BFX_SEC", "")
# Wallet currency autodetected (UST or USDT) for balance/cancel
ASSET_CODE = "UST"
# Public funding ticker symbol (as per your spec)
PUBLIC_TICKERS_URL = "https://api-pub.bitfinex.com/v2/tickers?symbols=fUST"
# Private submission symbols: prefer fUST (per your note)
SYMBOL_PREFERRED = "fUST"
SYMBOL_FALLBACK = "fUSDT"  # retry once with fUSDT if exchange expects that

MIN_OFFER = 150.0
CHUNK_SIZE = 1000.0
DURATION_D = 2
AUTORENEW = True

# Blend weights: anchor = w_mid * mid + w_last * LAST_PRICE (w_mid + w_last = 1)
W_MID = 0.7
W_LAST = 0.3

# Spread ladder (daily rate offsets) around the blended anchor, in decimals (bps/day = 0.0001)
# Slightly tighter negative side to improve fill, moderate positive side for profitability
SPREAD_OFFSETS = [-0.0005, -0.0003, -0.0002, 0.0, 0.0002, 0.0004, 0.0006]

IDLE_WARN_THRESHOLD = 200.0
BASE_URL = "https://api.bitfinex.com"

# ---------- Utilities ----------

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

def daily_to_apy(d: float) -> float:
    return (1.0 + d) ** 365 - 1.0

def apy_to_str(apy: float) -> str:
    return f"{apy*100:.2f}%"

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# ---------- Wallets / Cancel ----------

def autodetect_wallet_currency(rows: List[List[Any]]) -> None:
    global ASSET_CODE
    seen = set()
    for w in rows:
        try:
            # Expect [WALLET_TYPE, CURRENCY, BALANCE, UNCONF, AVAILABLE, ...]
            if isinstance(w, list) and len(w) >= 2:
                seen.add(str(w[1]).upper())
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

# ---------- Public ticker anchor ----------

def fetch_funding_ticker_fust() -> Optional[List[Any]]:
    """
    GET https://api-pub.bitfinex.com/v2/tickers?symbols=fUST
    Funding tickers array-of-arrays; for fUST row, fields (docs):
    [ SYMBOL, FRR, BID, BID_PERIOD, BID_SIZE, ASK, ASK_PERIOD, ASK_SIZE,
      DAILY_CHANGE, DAILY_CHANGE_PERC, LAST_PRICE, VOLUME, HIGH, LOW, ... ]
    """
    try:
        r = requests.get(PUBLIC_TICKERS_URL, timeout=15)
        r.raise_for_status()
        data = r.json()
        # Return the fUST row if present (first row)
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
            return data[0]
    except Exception as e:
        print("Public ticker fetch error:", e)
    return None

def derive_anchor_rate_from_ticker(row: List[Any]) -> Optional[float]:
    # Safely parse BID, ASK, LAST_PRICE by documented indices
    try:
        # Indices per provided schema (funding ticker)
        # SYMBOL, FRR, BID, BID_PERIOD, BID_SIZE, ASK, ASK_PERIOD, ASK_SIZE, DAILY_CHANGE, DAILY_CHANGE_PERC, LAST_PRICE, ...
        bid = safe_float(row[2], 0.0)
        ask = safe_float(row[5], 0.0)
        last = safe_float(row[10], 0.0)
        # Require valid bid/ask to form mid; if one side missing, fallback to the other
        if bid > 0 and ask > 0:
            mid = 0.5 * (bid + ask)
        elif bid > 0:
            mid = bid
        elif ask > 0:
            mid = ask
        else:
            # If both missing, but last>0, use last as anchor; else None
            return last if last > 0 else None
        # Blend toward LAST_PRICE slightly for responsiveness
        anchor = max(W_MID * mid + W_LAST * last, 0.000001)
        return anchor
    except Exception:
        return None

# ---------- Submissions ----------

def submit_offer_with_symbol(amount: float, rate: float, period: int, oftype: str, flags: int, symbol: str) -> Any:
    body = {
        "type": oftype,  # "LIMIT" or "FRRDELTA"
        "symbol": symbol,  # fUST preferred per user spec
        "amount": f"{amount:.6f}",
        "rate": f"{rate:.6f}",  # daily rate
        "period": period,
        "flags": flags,
    }
    return _post_private("v2/auth/w/funding/offer/submit", body)

def submit_offer(amount: float, rate: float, period: int, oftype: str, flags: int = 0) -> Any:
    try:
        return submit_offer_with_symbol(amount, rate, period, oftype, flags, SYMBOL_PREFERRED)
    except Exception as e1:
        print(f"Submit with {SYMBOL_PREFERRED} failed: {e1}. Retrying with {SYMBOL_FALLBACK}...")
        return submit_offer_with_symbol(amount, rate, period, oftype, flags, SYMBOL_FALLBACK)

def auto_renew_flag() -> int:
    return 1024 if AUTORENEW else 0

# ---------- Strategy ----------

def place_spread_offers_around_anchor(free_bal: float, anchor_rate: float) -> float:
    """
    Split all funds into $1000 chunks and place LIMIT offers at anchor*(1+offset).
    Negative offsets likely fill faster; positive offsets improve yield.
    """
    flags = auto_renew_flag()
    remaining = free_bal
    idx = 0
    while remaining >= MIN_OFFER:
        amt = min(CHUNK_SIZE, remaining)
        if amt < MIN_OFFER:
            break
        offset = SPREAD_OFFSETS[idx % len(SPREAD_OFFSETS)]
        target = max(anchor_rate * (1.0 + offset), 0.000001)
        try:
            resp = submit_offer(amt, target, DURATION_D, "LIMIT", flags)
            print(
                f"LIMIT chunk {idx+1}: amount={amt:.2f} rate={target:.6f} "
                f"(anchor {anchor_rate:.6f}, offset {offset:+.4f}), "
                f"APY~{apy_to_str(daily_to_apy(target))} -> {resp}"
            )
            remaining -= amt
            idx += 1
        except Exception as e:
            print(f"Error placing LIMIT chunk {idx+1}: {e}")
            break
    return remaining

def main():
    print("---- Bitfinex USDT Lending Bot (mid+last anchor, spread) ----")

    # 1) Cancel stale offers (best-effort)
    cancel_all_usdt_offers()

    # 2) Free balance and wallet currency
    free_bal = get_free_usdt_balance()
    print(f"Wallet currency detected: {ASSET_CODE}")
    print(f"Free USDT (funding wallet): {free_bal:.2f}")
    if free_bal < MIN_OFFER:
        print("Nothing to lend (below minimum offer).")
        return

    # 3) Fetch funding ticker for fUST and derive anchor rate
    row = fetch_funding_ticker_fust()
    anchor = None
    if row:
        anchor = derive_anchor_rate_from_ticker(row)
        # Also log FRR, BID, ASK, LAST for visibility
        frr = safe_float(row[1], 0.0)
        bid = safe_float(row[2], 0.0)
        ask = safe_float(row[5], 0.0)
        last = safe_float(row[10], 0.0)
        print(
            f"Ticker fUST: FRR={frr:.6f}, BID={bid:.6f}, ASK={ask:.6f}, "
            f"LAST={last:.6f}"
        )
    if not anchor or anchor <= 0:
        print("Warning: Could not derive anchor from ticker; aborting to avoid bad quotes.")
        return
    print(f"Anchor daily rate (blend mid→last): {anchor:.6f} ({apy_to_str(daily_to_apy(anchor))})")

    # 4) Place spread LIMIT offers around the anchor
    remaining = place_spread_offers_around_anchor(free_bal, anchor)

    # 5) Final sweep: any leftover ≥$150 at exact anchor
    if remaining >= MIN_OFFER:
        flags = auto_renew_flag()
        amt = remaining
        try:
            resp = submit_offer(amt, max(anchor, 0.000001), DURATION_D, "LIMIT", flags)
            print(f"Final sweep LIMIT: amount={amt:.2f} rate={anchor:.6f} -> {resp}")
            remaining = 0.0
        except Exception as e:
            print(f"Final sweep error: {e}")

    if remaining >= IDLE_WARN_THRESHOLD:
        print("Warning:", f"{remaining:.2f} USDT still idle. Consider widening SPREAD_OFFSETS or reducing CHUNK_SIZE.")

    print("Run complete.")

if __name__ == "__main__":
    main()
