import random
import re
from collections import deque
import secrets
import sys
import threading
import time
import uuid
import feedparser

# Ensure stdout/stderr can handle Unicode (Korean) on Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
from datetime import datetime, timedelta, timezone, time as _time
from typing import Optional

import yfinance as yf
import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from database import get_db, hash_password, init_db, verify_password
from stock_data import (
    calc_shares,
    detect_market,
    get_chart_data,
    get_exchange_rate,
    get_related_stocks,
    get_stock_info,
    get_stock_stats,
    is_korean_market_open,
    is_us_market_open,
)

app = FastAPI(title="PaperTrade")

USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{2,20}$")

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────

US_STOCK_FEE_RATE  = 0.0025   # 0.25%  both buy and sell
KRX_STOCK_FEE_RATE = 0.00015  # 0.015% both buy and sell
KRX_SELL_TAX_RATE  = 0.0018   # 0.18%  KRX sell only (증권거래세)
CRYPTO_FEE_RATE    = 0.001    # 0.1%   both buy and sell
FX_SPREAD_FEE      = 0.0175   # 1.75%  both directions
SEC_FEE_PER_DOLLAR = 0.0000278  # $27.80 per $1,000,000 of proceeds (2024 rate)

# Per-market leverage daily interest rates
KRX_LEVERAGE_INTEREST    = 0.00022   # ~8% annually
US_LEVERAGE_INTEREST     = 0.00018   # ~6.5% annually
CRYPTO_LEVERAGE_INTEREST = 0.0003    # ~11% annually

# Per-market leverage allowed values
_LEVERAGE_ALLOWED = {
    "KR":     (1.5, 2.0, 2.5),
    "US":     (2.0, 4.0),
}

# Fee tiers based on cumulative monthly trading volume (USD equivalent)
FEE_TIERS = [
    {"name": "Bronze",   "min_vol":       0, "us_fee": 0.0025,  "kr_fee": 0.00015},
    {"name": "Silver",   "min_vol":  10_000, "us_fee": 0.0020,  "kr_fee": 0.00012},
    {"name": "Gold",     "min_vol":  50_000, "us_fee": 0.0015,  "kr_fee": 0.00010},
    {"name": "Platinum", "min_vol": 200_000, "us_fee": 0.0010,  "kr_fee": 0.00008},
]


# ─────────────────────────────────────────
# Fee helpers
# ─────────────────────────────────────────

def _fee_rates(market: str) -> tuple[float, float]:
    """Returns (fee_rate, sell_tax_rate) — base Bronze rates, tier-unaware."""
    if market == "KR":
        return KRX_STOCK_FEE_RATE, KRX_SELL_TAX_RATE
    elif market == "US":
        return US_STOCK_FEE_RATE, 0.0
    else:
        return CRYPTO_FEE_RATE, 0.0


def get_user_monthly_volume(user_id: int) -> float:
    """Return this user's total trade volume for the current calendar month, in USD equivalent."""
    now         = datetime.now()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    fx_rate     = get_exchange_rate()["rate"]   # KRW per 1 USD, cached 60 s
    with get_db() as conn:
        rows = conn.execute(
            "SELECT market, quantity, price FROM transactions "
            "WHERE user_id = ? AND timestamp >= ? AND action IN ('buy', 'sell')",
            (user_id, month_start),
        ).fetchall()
    total_usd = 0.0
    for r in rows:
        amt = abs(r["quantity"] * r["price"])   # gross notional (execution price × qty)
        if r["market"] == "KR":
            amt /= fx_rate
        total_usd += amt
    return total_usd


def get_fee_tier_info(volume_usd: float) -> dict:
    """Return fee tier details for the given monthly USD volume."""
    tier = FEE_TIERS[0]
    for t in FEE_TIERS:
        if volume_usd >= t["min_vol"]:
            tier = t
    idx       = FEE_TIERS.index(tier)
    next_tier = FEE_TIERS[idx + 1] if idx + 1 < len(FEE_TIERS) else None
    return {
        "name":           tier["name"],
        "us_fee":         tier["us_fee"],
        "kr_fee":         tier["kr_fee"],
        "monthly_volume": round(volume_usd, 2),
        "next_threshold": next_tier["min_vol"] if next_tier else None,
        "to_next":        round(next_tier["min_vol"] - volume_usd, 2) if next_tier else None,
    }


def get_fee_rate(user_id: int, market: str) -> float:
    """Return the tiered broker fee rate for this user and market."""
    if market == "CRYPTO":
        return CRYPTO_FEE_RATE
    volume = get_user_monthly_volume(user_id)
    tier   = get_fee_tier_info(volume)
    return tier["kr_fee"] if market == "KR" else tier["us_fee"]


def calculate_slippage(price: float, quantity: float, market: str, action: str,
                       fx_rate: float = 1.0) -> float:
    """Return a random unfavorable slippage fraction for a market order.

    Tiers (order value in USD equivalent):
      < $500       → 0%
      $500–$5k     → uniform [0, 0.05%]
      $5k–$50k     → uniform [0, 0.10%]
      ≥ $50k       → uniform [0, 0.20%]
    CRYPTO gets a 2× multiplier.
    """
    order_value_usd = price * quantity
    if market == "KR":
        order_value_usd /= fx_rate  # convert KRW → USD

    if order_value_usd < 500:
        max_slip = 0.0
    elif order_value_usd < 5_000:
        max_slip = 0.0005
    elif order_value_usd < 50_000:
        max_slip = 0.001
    else:
        max_slip = 0.002

    if market == "CRYPTO":
        max_slip *= 2

    return 0.0 if max_slip == 0.0 else random.uniform(0, max_slip)


# ─────────────────────────────────────────
# Limit order fill loop
# ─────────────────────────────────────────

def _fill_order_inner(conn, order: dict, price: float):
    """Execute a pending order fill within an active DB connection."""
    oid      = order["id"]
    user_id  = order["user_id"]
    symbol   = order["ticker"]
    market   = order["market"]
    qty      = order["quantity"]
    lp       = order["limit_price"]
    currency = order["currency"]
    otype    = order["order_type"]

    fee_rate    = get_fee_rate(user_id, market)
    action      = "buy" if otype == "LIMIT_BUY" else "sell"
    tax_rate    = KRX_SELL_TAX_RATE if (action == "sell" and market == "KR") else 0.0

    rnd        = 2 if currency == "USD" else 0
    base_total = round(qty * price, rnd)
    fee_amount = round(base_total * fee_rate, rnd)
    tax_amount = round(base_total * tax_rate, rnd)

    # SEC fee: US sell only, minimum $0.01
    if action == "sell" and market == "US":
        sec_fee = max(0.01, round(base_total * SEC_FEE_PER_DOLLAR, 2))
    else:
        sec_fee = 0.0

    now = datetime.now().isoformat()

    if action == "buy":
        reserved    = round(qty * lp * (1 + fee_rate), rnd)
        actual      = round(base_total + fee_amount, rnd)
        refund      = round(reserved - actual, rnd)
        if refund > 0:
            conn.execute(
                "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
                (refund, user_id, currency),
            )
        existing = conn.execute(
            "SELECT quantity, avg_price FROM holdings WHERE user_id = ? AND symbol = ?",
            (user_id, symbol),
        ).fetchone()
        if existing:
            new_qty = existing["quantity"] + qty
            new_avg = (existing["avg_price"] * existing["quantity"] + price * qty) / new_qty
            conn.execute(
                "UPDATE holdings SET quantity = ?, avg_price = ? WHERE user_id = ? AND symbol = ?",
                (new_qty, new_avg, user_id, symbol),
            )
        else:
            conn.execute(
                "INSERT INTO holdings VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, symbol, market, qty, price, currency),
            )
        final_total = round(base_total + fee_amount, rnd)
    else:
        final_total = round(base_total - fee_amount - tax_amount - sec_fee, rnd)
        conn.execute(
            "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
            (final_total, user_id, currency),
        )

    conn.execute("UPDATE pending_orders SET status = 'FILLED' WHERE id = ?", (oid,))
    conn.execute(
        "INSERT INTO transactions "
        "(user_id, symbol, market, action, quantity, price, total, currency, timestamp, fee_amount, tax_amount, sec_fee) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, symbol, market, action, qty, price, final_total,
         currency, now, fee_amount, tax_amount, sec_fee),
    )

    # OCO: cancel sibling order and restore its holdings
    oco_id = order.get("oco_group_id")
    if oco_id:
        siblings = conn.execute(
            "SELECT * FROM pending_orders WHERE oco_group_id = ? AND id != ? AND status = 'PENDING'",
            (oco_id, oid),
        ).fetchall()
        for sib in siblings:
            sib = dict(sib)
            conn.execute("UPDATE pending_orders SET status = 'CANCELLED' WHERE id = ?", (sib["id"],))
            conn.execute(
                "UPDATE holdings SET quantity = quantity + ? WHERE user_id = ? AND symbol = ?",
                (sib["quantity"], user_id, sib["ticker"]),
            )


def _process_pending_orders():
    try:
        with get_db() as conn:
            orders = [dict(r) for r in conn.execute(
                "SELECT * FROM pending_orders WHERE status = 'PENDING'"
            ).fetchall()]

        for order in orders:
            market = order["market"]
            if market == "KR" and not is_korean_market_open():
                continue
            if market == "US" and not is_us_market_open():
                continue

            info = get_stock_info(order["ticker"], market)
            if not info:
                continue
            price        = info["price"]
            otype        = order["order_type"]
            lp           = order["limit_price"]
            trigger_type = order.get("trigger_type")

            # Fill conditions
            if otype == "LIMIT_BUY":
                should_fill = price <= lp
            elif trigger_type == "STOP_LOSS":
                should_fill = price <= lp
            else:  # LIMIT_SELL or TAKE_PROFIT
                should_fill = price >= lp

            if not should_fill:
                continue

            try:
                with get_db() as conn:
                    row = conn.execute(
                        "SELECT status FROM pending_orders WHERE id = ?", (order["id"],)
                    ).fetchone()
                    if not row or row["status"] != "PENDING":
                        continue
                    _fill_order_inner(conn, order, price)
                    label = trigger_type or otype
                    print(f"[Fill] order {order['id']} filled: {order['ticker']} {label} {order['quantity']} @ {price}")
            except Exception as e:
                print(f"[Fill order {order['id']}] {e}")
    except Exception as e:
        print(f"[Process pending orders] {e}")


def _fill_loop():
    while True:
        time.sleep(30)
        _process_pending_orders()


# ─────────────────────────────────────────
# Short sell background tasks
# ─────────────────────────────────────────

def _force_close_short(pos: dict, current_price: float):
    """Force-close a short position (margin call)."""
    try:
        user_id     = pos["user_id"]
        qty         = pos["quantity"]
        entry_price = pos["entry_price"]
        currency    = pos["currency"]
        rnd         = 2 if currency == "USD" else 0

        fee_rate   = get_fee_rate(user_id, pos["market"])
        base_total = round(qty * current_price, rnd)
        fee_amount = round(base_total * fee_rate, rnd)
        pnl        = round((entry_price - current_price) * qty - fee_amount, rnd)
        collateral = round(entry_price * qty * 1.5, rnd)
        returned   = max(0.0, round(collateral + pnl, rnd))
        now        = datetime.now().isoformat()

        with get_db() as conn:
            conn.execute(
                "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
                (returned, user_id, currency),
            )
            conn.execute(
                "UPDATE short_positions SET status = 'CLOSED' WHERE id = ?", (pos["id"],)
            )
            conn.execute(
                "INSERT INTO transactions "
                "(user_id, symbol, market, action, quantity, price, total, currency, timestamp, "
                "fee_amount, position_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (user_id, pos["ticker"], pos["market"], "short_margin_call",
                 qty, current_price, returned, currency, now, fee_amount, "SHORT"),
            )
        print(f"[Margin call] {pos['ticker']} pos {pos['id']} closed @ {current_price}")
    except Exception as e:
        print(f"[Force close short {pos['id']}] {e}")


def _process_short_fees():
    try:
        with get_db() as conn:
            positions = [dict(r) for r in conn.execute(
                "SELECT * FROM short_positions WHERE status = 'OPEN'"
            ).fetchall()]

        for pos in positions:
            rnd            = 2 if pos["currency"] == "USD" else 0
            fee_per_minute = round(
                pos["daily_borrow_rate"] * pos["entry_price"] * pos["quantity"] / 1440, rnd
            )

            # Check margin call
            info = get_stock_info(pos["ticker"], pos["market"])
            if info:
                current_price    = info["price"]
                collateral       = pos["entry_price"] * pos["quantity"] * 1.5
                unrealized_loss  = max(0.0, (current_price - pos["entry_price"]) * pos["quantity"])
                if unrealized_loss > collateral * 0.5:
                    _force_close_short(pos, current_price)
                    continue

            if fee_per_minute > 0:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE balances SET amount = amount - ? "
                        "WHERE user_id = ? AND currency = ?",
                        (fee_per_minute, pos["user_id"], pos["currency"]),
                    )
    except Exception as e:
        print(f"[Short fee loop] {e}")


def _short_fee_loop():
    while True:
        time.sleep(60)
        _process_short_fees()


# ─────────────────────────────────────────
# Leverage background tasks
# ─────────────────────────────────────────

def _liq_price(entry_price: float, leverage: float) -> float:
    return entry_price * (1 - 1 / leverage + 0.005)


def _force_liquidate(pos: dict, current_price: float):
    """Force-liquidate a leveraged position."""
    try:
        user_id        = pos["user_id"]
        qty            = pos["quantity"]
        entry_price    = pos["entry_price"]
        currency       = pos["currency"]
        margin_amount  = pos["margin_amount"]
        borrowed_amount = pos["borrowed_amount"]
        rnd            = 2 if currency == "USD" else 0

        fee_rate      = get_fee_rate(user_id, pos["market"])
        current_value = round(qty * current_price, rnd)
        fee_amount    = round(current_value * fee_rate, rnd)
        gross_pnl     = round(current_value - qty * entry_price, rnd)
        net_proceeds  = max(0.0, round(margin_amount + gross_pnl - fee_amount, rnd))
        now           = datetime.now().isoformat()

        with get_db() as conn:
            conn.execute(
                "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
                (net_proceeds, user_id, currency),
            )
            conn.execute(
                "UPDATE leveraged_positions SET status = 'LIQUIDATED' WHERE id = ?", (pos["id"],)
            )
            conn.execute(
                "INSERT INTO transactions "
                "(user_id, symbol, market, action, quantity, price, total, currency, timestamp, "
                "fee_amount, position_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (user_id, pos["ticker"], pos["market"], "lev_liquidated",
                 qty, current_price, net_proceeds, currency, now, fee_amount, "LEVERAGE"),
            )
        print(f"[Liquidated] {pos['ticker']} pos {pos['id']} @ {current_price}, returned {net_proceeds}")
    except Exception as e:
        print(f"[Force liquidate {pos['id']}] {e}")


def _process_leverage_fees():
    try:
        with get_db() as conn:
            positions = [dict(r) for r in conn.execute(
                "SELECT * FROM leveraged_positions WHERE status = 'OPEN'"
            ).fetchall()]

        for pos in positions:
            rnd              = 2 if pos["currency"] == "USD" else 0
            int_per_minute   = round(
                pos["daily_interest_rate"] * pos["borrowed_amount"] / 1440, rnd
            )

            info = get_stock_info(pos["ticker"], pos["market"])
            if info:
                current_price = info["price"]
                liq           = _liq_price(pos["entry_price"], pos["leverage"])
                if current_price <= liq:
                    _force_liquidate(pos, current_price)
                    continue

            if int_per_minute > 0:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE balances SET amount = amount - ? "
                        "WHERE user_id = ? AND currency = ?",
                        (int_per_minute, pos["user_id"], pos["currency"]),
                    )
    except Exception as e:
        print(f"[Leverage fee loop] {e}")


def _leverage_fee_loop():
    while True:
        time.sleep(60)
        _process_leverage_fees()


# ─────────────────────────────────────────
# Dividend simulation
# ─────────────────────────────────────────

# pending_dividends[user_id] = [{"symbol": ..., "amount": ..., "currency": ..., "name": ...}, ...]
_pending_dividend_toasts: dict[int, list[dict]] = {}
_dividend_toast_lock = threading.Lock()


def _add_dividend_toast(user_id: int, symbol: str, amount: float,
                        currency: str, name: str, quantity: float):
    entry = {
        "symbol":   symbol,
        "amount":   amount,       # per share
        "total":    round(amount * quantity, 2 if currency == "USD" else 0),
        "currency": currency,
        "name":     name,
        "quantity": quantity,
    }
    with _dividend_toast_lock:
        _pending_dividend_toasts.setdefault(user_id, []).append(entry)


def check_dividends():
    """Credit dividends for all held tickers whose ex-date fell in the last 7 days."""
    try:
        now_utc = datetime.now(timezone.utc)
        cutoff  = now_utc - timedelta(days=7)

        with get_db() as conn:
            tickers = [r[0] for r in conn.execute(
                "SELECT DISTINCT symbol FROM holdings WHERE quantity > 0"
            ).fetchall()]

        for ticker in tickers:
            try:
                _process_ticker_dividends(ticker, cutoff, now_utc)
            except Exception as e:
                print(f"[Dividend] {ticker}: {e}")
    except Exception as e:
        print(f"[Dividend check] {e}")


def _process_ticker_dividends(ticker: str, cutoff: datetime, now_utc: datetime):
    # Determine market / currency
    from stock_data import detect_market
    market   = detect_market(ticker)
    currency = "KRW" if market == "KR" else "USD"

    # Check last-processed timestamp for this ticker
    with get_db() as conn:
        row = conn.execute(
            "SELECT last_checked FROM dividend_checks WHERE ticker = ?", (ticker,)
        ).fetchone()
        last_checked = (
            datetime.fromisoformat(row["last_checked"]).replace(tzinfo=timezone.utc)
            if row else cutoff
        )

    # Fetch dividend history via yfinance (1-year window covers quarterly)
    yf_ticker = yf.Ticker(ticker)
    try:
        divs = yf_ticker.dividends  # pandas Series, index = timezone-aware dates
    except Exception:
        return

    if divs is None or len(divs) == 0:
        _upsert_dividend_check(ticker, now_utc)
        return

    # Filter dividends whose ex-date is between last_checked and now
    for ex_date, amount in divs.items():
        # ex_date is a pandas Timestamp; normalise to UTC-aware datetime
        try:
            ex_dt = ex_date.to_pydatetime()
            if ex_dt.tzinfo is None:
                ex_dt = ex_dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        if not (last_checked < ex_dt <= now_utc):
            continue

        if amount <= 0:
            continue

        # Credit all users who held this ticker on ex_date
        ex_date_str = ex_dt.date().isoformat()
        with get_db() as conn:
            holders = conn.execute(
                "SELECT user_id, quantity FROM holdings WHERE symbol = ? AND quantity > 0",
                (ticker,),
            ).fetchall()

        if not holders:
            continue

        # Fetch stock name (best-effort)
        try:
            name = (yf_ticker.info or {}).get("shortName") or ticker
        except Exception:
            name = ticker

        rnd = 2 if currency == "USD" else 0
        now_iso = datetime.now().isoformat()

        for holder in holders:
            user_id  = holder["user_id"]
            quantity = holder["quantity"]
            total    = round(float(amount) * quantity, rnd)
            if total <= 0:
                continue

            with get_db() as conn:
                conn.execute(
                    "UPDATE balances SET amount = amount + ? "
                    "WHERE user_id = ? AND currency = ?",
                    (total, user_id, currency),
                )
                conn.execute(
                    "INSERT INTO transactions "
                    "(user_id, symbol, market, action, quantity, price, total, "
                    "currency, timestamp, position_type) "
                    "VALUES (?, ?, ?, 'DIVIDEND', ?, ?, ?, ?, ?, 'LONG')",
                    (user_id, ticker, market, quantity, float(amount), total,
                     currency, now_iso),
                )

            _add_dividend_toast(user_id, ticker, float(amount), currency, name, quantity)
            print(f"[Dividend] {ticker} ex={ex_date_str}: credited {total} {currency} "
                  f"to user {user_id} ({quantity} sh × {amount})")

    _upsert_dividend_check(ticker, now_utc)


def _upsert_dividend_check(ticker: str, ts: datetime):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO dividend_checks (ticker, last_checked) VALUES (?, ?) "
            "ON CONFLICT(ticker) DO UPDATE SET last_checked = excluded.last_checked",
            (ticker, ts.isoformat()),
        )


def _daily_snapshot_loop():
    """Record an equity snapshot for every user at local midnight each day."""
    while True:
        now = datetime.now()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        time.sleep((next_midnight - now).total_seconds())
        try:
            with get_db() as conn:
                user_ids = [r[0] for r in conn.execute("SELECT id FROM users").fetchall()]
            for uid in user_ids:
                try:
                    with get_db() as conn:
                        _record_snapshot(uid, conn)
                except Exception:
                    pass
        except Exception:
            pass


def _dividend_loop():
    """Run check_dividends() once per day, shortly after both markets close."""
    import pytz
    KST = pytz.timezone("Asia/Seoul")
    EST = pytz.timezone("America/New_York")

    while True:
        now_kst = datetime.now(KST)
        now_est = datetime.now(EST)

        kr_closed = now_kst.hour > 15 or (now_kst.hour == 15 and now_kst.minute >= 35)
        us_closed = now_est.hour > 16 or (now_est.hour == 16 and now_est.minute >= 5)

        if kr_closed and us_closed:
            check_dividends()
            # Sleep until next day (sleep 23 h to avoid re-running in the same window)
            time.sleep(23 * 3600)
        else:
            time.sleep(300)   # check every 5 min until both markets are closed


@app.on_event("startup")
def startup():
    init_db()
    threading.Thread(target=_fill_loop,             daemon=True).start()
    threading.Thread(target=_short_fee_loop,        daemon=True).start()
    threading.Thread(target=_leverage_fee_loop,     daemon=True).start()
    threading.Thread(target=_dividend_loop,         daemon=True).start()
    threading.Thread(target=_daily_snapshot_loop,   daemon=True).start()


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
def root():
    return FileResponse("static/index.html")


# ─────────────────────────────────────────
# Auth dependency
# ─────────────────────────────────────────

def require_auth(authorization: Optional[str] = Header(None)) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    token = authorization[7:]
    with get_db() as conn:
        row = conn.execute(
            "SELECT user_id FROM sessions WHERE token = ?", (token,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="세션이 만료되었습니다. 다시 로그인하세요.")
    return row["user_id"]


# ─────────────────────────────────────────
# Auth endpoints
# ─────────────────────────────────────────

class AuthRequest(BaseModel):
    username: str
    password: str


@app.post("/api/auth/register")
def api_register(req: AuthRequest):
    if not USERNAME_RE.match(req.username):
        raise HTTPException(
            status_code=400,
            detail="아이디는 2~20자의 영문, 숫자, 밑줄(_)만 사용 가능합니다."
        )
    if len(req.password) < 4:
        raise HTTPException(status_code=400, detail="비밀번호는 4자 이상이어야 합니다.")

    pw_hash = hash_password(req.password)
    token   = secrets.token_urlsafe(32)
    now     = datetime.now().isoformat()

    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM users WHERE username = ?", (req.username,)
        ).fetchone()
        if existing:
            raise HTTPException(status_code=409, detail="이미 사용 중인 아이디입니다.")

        conn.execute(
            "INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)",
            (req.username, pw_hash, now),
        )
        user_id = conn.execute(
            "SELECT id FROM users WHERE username = ?", (req.username,)
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)",
            (token, user_id, now),
        )

    return {"token": token, "username": req.username}


@app.post("/api/auth/login")
def api_login(req: AuthRequest):
    with get_db() as conn:
        user = conn.execute(
            "SELECT id, password_hash FROM users WHERE username = ?", (req.username,)
        ).fetchone()

    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 틀렸습니다.")

    token = secrets.token_urlsafe(32)
    now   = datetime.now().isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)",
            (token, user["id"], now),
        )

    return {"token": token, "username": req.username}


@app.post("/api/auth/logout")
def api_logout(authorization: Optional[str] = Header(None)):
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        with get_db() as conn:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
    return {"success": True}


@app.get("/api/auth/me")
def api_me(user_id: int = Depends(require_auth)):
    with get_db() as conn:
        user = conn.execute(
            "SELECT username, created_at FROM users WHERE id = ?", (user_id,)
        ).fetchone()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    return {"user_id": user_id, "username": user["username"], "created_at": user["created_at"]}


# ─────────────────────────────────────────
# Setup
# ─────────────────────────────────────────

class SetupRequest(BaseModel):
    krw: float
    usd: float


@app.get("/api/setup")
def api_get_setup(user_id: int = Depends(require_auth)):
    with get_db() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE user_id = ? AND key = 'setup_complete'",
            (user_id,),
        ).fetchone()
        if row and row["value"] == "1":
            bals = {
                b["currency"]: b["amount"]
                for b in conn.execute(
                    "SELECT currency, amount FROM balances WHERE user_id = ?", (user_id,)
                ).fetchall()
            }
            return {"setup_complete": True, "balances": bals}
    return {"setup_complete": False}


@app.post("/api/setup")
def api_post_setup(req: SetupRequest, user_id: int = Depends(require_auth)):
    if req.krw < 0 or req.usd < 0:
        raise HTTPException(status_code=400, detail="잔고는 0 이상이어야 합니다.")
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings VALUES (?, 'setup_complete', '1')", (user_id,)
        )
        conn.execute(
            "INSERT OR REPLACE INTO balances VALUES (?, 'KRW', ?)", (user_id, req.krw)
        )
        conn.execute(
            "INSERT OR REPLACE INTO balances VALUES (?, 'USD', ?)", (user_id, req.usd)
        )
        _record_snapshot(user_id, conn)
    return {"success": True}


# ─────────────────────────────────────────
# Market status (public)
# ─────────────────────────────────────────

@app.get("/api/market/status")
def api_market_status():
    return {"kr_open": is_korean_market_open(), "us_open": is_us_market_open()}


# ─────────────────────────────────────────
# Stock info + chart (public)
# ─────────────────────────────────────────

@app.get("/api/stock/{symbol}")
def api_stock_info(symbol: str, market: str = "auto"):
    info = get_stock_info(symbol.upper(), market.upper() if market != "auto" else "auto")
    if not info:
        raise HTTPException(status_code=404, detail=f"종목을 찾을 수 없습니다: {symbol}")
    return info


@app.get("/api/stock/{symbol}/chart")
def api_chart(symbol: str, market: str = "auto", period: str = "1mo"):
    result = get_chart_data(symbol.upper(), market.upper() if market != "auto" else "auto", period)
    if not result.get("data"):
        raise HTTPException(status_code=404, detail="차트 데이터가 없습니다.")
    return result


@app.get("/api/stock/{symbol}/stats")
def api_stock_stats(symbol: str, market: str = "auto"):
    return get_stock_stats(symbol.upper(), market.upper() if market != "auto" else "auto")


@app.get("/api/stock/{symbol}/related")
def api_stock_related(symbol: str, market: str = "auto"):
    return get_related_stocks(symbol.upper(), market.upper() if market != "auto" else "auto")


# ─────────────────────────────────────────
# Portfolio
# ─────────────────────────────────────────

@app.get("/api/portfolio")
def api_portfolio(user_id: int = Depends(require_auth)):
    with get_db() as conn:
        balances = {
            b["currency"]: b["amount"]
            for b in conn.execute(
                "SELECT currency, amount FROM balances WHERE user_id = ?", (user_id,)
            ).fetchall()
        }
        holdings = [
            dict(h)
            for h in conn.execute(
                "SELECT * FROM holdings WHERE user_id = ? AND quantity > 0", (user_id,)
            ).fetchall()
        ]
    volume   = get_user_monthly_volume(user_id)
    fee_tier = get_fee_tier_info(volume)
    return {"balances": balances, "holdings": holdings, "fee_tier": fee_tier}


@app.get("/api/portfolio/live")
def api_portfolio_live(user_id: int = Depends(require_auth)):
    with get_db() as conn:
        balances = {
            b["currency"]: b["amount"]
            for b in conn.execute(
                "SELECT currency, amount FROM balances WHERE user_id = ?", (user_id,)
            ).fetchall()
        }
        holdings = [
            dict(h)
            for h in conn.execute(
                "SELECT * FROM holdings WHERE user_id = ? AND quantity > 0", (user_id,)
            ).fetchall()
        ]

    enriched = []
    for h in holdings:
        info = get_stock_info(h["symbol"], h["market"])
        if info:
            cp  = info["price"]
            pl  = (cp - h["avg_price"]) * h["quantity"]
            pct = (cp - h["avg_price"]) / h["avg_price"] * 100 if h["avg_price"] else 0
            enriched.append({**h, "name": info["name"], "current_price": cp, "pl": pl, "pl_pct": pct})
        else:
            enriched.append({**h, "name": h["symbol"], "current_price": h["avg_price"], "pl": 0, "pl_pct": 0})

    volume   = get_user_monthly_volume(user_id)
    fee_tier = get_fee_tier_info(volume)
    return {"balances": balances, "holdings": enriched, "fee_tier": fee_tier}


# ─────────────────────────────────────────
# Orders
# ─────────────────────────────────────────

class OrderRequest(BaseModel):
    symbol:   str
    market:   str = "auto"
    action:   str
    quantity: Optional[float] = None
    amount:   Optional[float] = None


@app.post("/api/order")
def api_order(req: OrderRequest, user_id: int = Depends(require_auth)):
    symbol = req.symbol.upper()
    market = req.market.upper() if req.market != "auto" else detect_market(symbol)

    info = get_stock_info(symbol, market)
    if not info:
        raise HTTPException(status_code=404, detail=f"종목을 찾을 수 없습니다: {symbol}")

    price    = info["price"]
    currency = info["currency"]

    if market == "KR" and not is_korean_market_open():
        raise HTTPException(status_code=400, detail="한국 장이 마감되어 거래할 수 없습니다.")
    if market == "US" and not is_us_market_open():
        raise HTTPException(status_code=400, detail="미국 장이 마감되어 거래할 수 없습니다.")

    try:
        qty, _ = calc_shares(price, market, req.quantity, req.amount)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    action = req.action.lower()
    if action not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="action은 'buy' 또는 'sell'이어야 합니다.")

    # ── Slippage (market orders only) ─────────
    fx_rate_val = get_exchange_rate()["rate"] if market == "KR" else 1.0
    slip_pct    = calculate_slippage(price, qty, market, action, fx_rate_val)
    if action == "buy":
        execution_price = price * (1 + slip_pct)
    else:
        execution_price = price * (1 - slip_pct)

    rnd             = 2 if currency == "USD" else 0
    slippage_amount = round(abs(execution_price - price) * qty, rnd)
    price           = execution_price   # use execution price for all downstream calcs

    # ── Fee & tax calculation ──────────────────
    fee_rate = get_fee_rate(user_id, market)
    tax_rate = KRX_SELL_TAX_RATE if (action == "sell" and market == "KR") else 0.0

    base_total = round(qty * price, rnd)
    fee_amount = round(base_total * fee_rate, rnd)
    tax_amount = round(base_total * tax_rate, rnd)

    # SEC fee: US sell only, minimum $0.01
    if action == "sell" and market == "US":
        sec_fee = max(0.01, round(base_total * SEC_FEE_PER_DOLLAR, 2))
    else:
        sec_fee = 0.0

    if action == "buy":
        total = round(base_total + fee_amount, rnd)
    else:
        total = round(base_total - fee_amount - tax_amount - sec_fee, rnd)

    with get_db() as conn:
        bal_row = conn.execute(
            "SELECT amount FROM balances WHERE user_id = ? AND currency = ?", (user_id, currency)
        ).fetchone()
        if not bal_row:
            raise HTTPException(status_code=400, detail="잔고 정보를 찾을 수 없습니다.")
        balance = bal_row["amount"]

        if action == "buy":
            if balance < total:
                raise HTTPException(
                    status_code=400,
                    detail=f"잔고 부족: {currency} {balance:,.2f} (필요 {total:,.2f})"
                )
            conn.execute(
                "UPDATE balances SET amount = amount - ? WHERE user_id = ? AND currency = ?",
                (total, user_id, currency),
            )
            existing = conn.execute(
                "SELECT quantity, avg_price FROM holdings WHERE user_id = ? AND symbol = ?",
                (user_id, symbol),
            ).fetchone()
            if existing:
                new_qty = existing["quantity"] + qty
                new_avg = (existing["avg_price"] * existing["quantity"] + price * qty) / new_qty
                conn.execute(
                    "UPDATE holdings SET quantity = ?, avg_price = ? WHERE user_id = ? AND symbol = ?",
                    (new_qty, new_avg, user_id, symbol),
                )
            else:
                conn.execute(
                    "INSERT INTO holdings VALUES (?, ?, ?, ?, ?, ?)",
                    (user_id, symbol, market, qty, price, currency),
                )

        elif action == "sell":
            existing = conn.execute(
                "SELECT quantity FROM holdings WHERE user_id = ? AND symbol = ?",
                (user_id, symbol),
            ).fetchone()
            if not existing or existing["quantity"] < qty:
                held = existing["quantity"] if existing else 0
                raise HTTPException(
                    status_code=400,
                    detail=f"보유 수량 부족: {held:.4f}주 (매도 요청 {qty:.4f}주)"
                )
            conn.execute(
                "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
                (total, user_id, currency),
            )
            conn.execute(
                "UPDATE holdings SET quantity = quantity - ? WHERE user_id = ? AND symbol = ?",
                (qty, user_id, symbol),
            )

        conn.execute(
            "INSERT INTO transactions "
            "(user_id, symbol, market, action, quantity, price, total, currency, timestamp, "
            "fee_amount, tax_amount, slippage_amount, sec_fee) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, action, qty, price, total, currency,
             datetime.now().isoformat(), fee_amount, tax_amount, slippage_amount, sec_fee),
        )
        _record_snapshot(user_id, conn)

    label = "매수" if action == "buy" else "매도"
    return {
        "success":         True,
        "message":         f"{symbol} {label} 체결: {qty:.4f}주 @ {price:,.2f} {currency}",
        "quantity":        qty,
        "price":           price,
        "base_total":      base_total,
        "fee_amount":      fee_amount,
        "tax_amount":      tax_amount,
        "slippage_amount": slippage_amount,
        "sec_fee":         sec_fee,
        "total":           total,
        "currency":        currency,
    }


# ─────────────────────────────────────────
# Limit orders
# ─────────────────────────────────────────

class LimitOrderRequest(BaseModel):
    symbol:      str
    market:      str = "auto"
    order_type:  str   # "LIMIT_BUY" | "LIMIT_SELL"
    quantity:    float
    limit_price: float


@app.post("/api/order/limit")
def api_limit_order(req: LimitOrderRequest, user_id: int = Depends(require_auth)):
    symbol = req.symbol.upper()
    market = req.market.upper() if req.market != "auto" else detect_market(symbol)

    info = get_stock_info(symbol, market)
    if not info:
        raise HTTPException(status_code=404, detail=f"종목을 찾을 수 없습니다: {symbol}")
    currency = info["currency"]

    if req.order_type not in ("LIMIT_BUY", "LIMIT_SELL"):
        raise HTTPException(status_code=400, detail="order_type은 'LIMIT_BUY' 또는 'LIMIT_SELL'이어야 합니다.")
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="수량은 0보다 커야 합니다.")
    if req.limit_price <= 0:
        raise HTTPException(status_code=400, detail="지정가는 0보다 커야 합니다.")

    fee_rate = get_fee_rate(user_id, market)
    rnd      = 2 if currency == "USD" else 0
    now      = datetime.now().isoformat()

    with get_db() as conn:
        if req.order_type == "LIMIT_BUY":
            reserved = round(req.quantity * req.limit_price * (1 + fee_rate), rnd)
            bal_row  = conn.execute(
                "SELECT amount FROM balances WHERE user_id = ? AND currency = ?", (user_id, currency)
            ).fetchone()
            if not bal_row or bal_row["amount"] < reserved:
                bal = bal_row["amount"] if bal_row else 0.0
                raise HTTPException(
                    status_code=400,
                    detail=f"잔고 부족: {currency} {bal:,.2f} (필요 {reserved:,.2f})"
                )
            conn.execute(
                "UPDATE balances SET amount = amount - ? WHERE user_id = ? AND currency = ?",
                (reserved, user_id, currency),
            )
        else:  # LIMIT_SELL
            holding = conn.execute(
                "SELECT quantity FROM holdings WHERE user_id = ? AND symbol = ?",
                (user_id, symbol),
            ).fetchone()
            if not holding or holding["quantity"] < req.quantity:
                held = holding["quantity"] if holding else 0.0
                raise HTTPException(
                    status_code=400,
                    detail=f"보유 수량 부족: {held:.4f}주 (매도 요청 {req.quantity:.4f}주)"
                )
            conn.execute(
                "UPDATE holdings SET quantity = quantity - ? WHERE user_id = ? AND symbol = ?",
                (req.quantity, user_id, symbol),
            )

        conn.execute(
            "INSERT INTO pending_orders (user_id, ticker, market, order_type, quantity, limit_price, currency, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, req.order_type, req.quantity, req.limit_price, currency, now),
        )
        order_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    label = "매수" if req.order_type == "LIMIT_BUY" else "매도"
    return {
        "success": True,
        "id":      order_id,
        "message": f"지정가 {label} 주문 등록: {symbol} {req.quantity}주 @ {req.limit_price:,.2f} {currency}",
    }


@app.delete("/api/order/{order_id}")
def api_cancel_order(order_id: int, user_id: int = Depends(require_auth)):
    with get_db() as conn:
        order = conn.execute(
            "SELECT * FROM pending_orders WHERE id = ? AND user_id = ? AND status = 'PENDING'",
            (order_id, user_id),
        ).fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="주문을 찾을 수 없습니다.")

        order    = dict(order)
        fee_rate = get_fee_rate(user_id, order["market"])
        rnd      = 2 if order["currency"] == "USD" else 0

        if order["order_type"] == "LIMIT_BUY":
            reserved = round(order["quantity"] * order["limit_price"] * (1 + fee_rate), rnd)
            conn.execute(
                "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
                (reserved, user_id, order["currency"]),
            )
        else:
            conn.execute(
                "UPDATE holdings SET quantity = quantity + ? WHERE user_id = ? AND symbol = ?",
                (order["quantity"], user_id, order["ticker"]),
            )

        conn.execute("UPDATE pending_orders SET status = 'CANCELLED' WHERE id = ?", (order_id,))

    return {"success": True, "message": "주문이 취소되었습니다."}


@app.get("/api/orders/pending")
def api_pending_orders(user_id: int = Depends(require_auth)):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM pending_orders WHERE user_id = ? AND status = 'PENDING' ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────
# Stop-loss / Take-profit orders
# ─────────────────────────────────────────

class TriggerOrderRequest(BaseModel):
    symbol:          str
    market:          str = "auto"
    quantity:        float
    trigger_price:   float
    parent_order_id: Optional[int] = None


def _place_trigger_order(req: TriggerOrderRequest, trigger_type: str, user_id: int) -> dict:
    """Shared logic for stop-loss and take-profit order placement."""
    symbol = req.symbol.upper()
    market = req.market.upper() if req.market != "auto" else detect_market(symbol)

    info = get_stock_info(symbol, market)
    if not info:
        raise HTTPException(status_code=404, detail=f"종목을 찾을 수 없습니다: {symbol}")
    current_price = info["price"]
    currency      = info["currency"]

    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="수량은 0보다 커야 합니다.")
    if req.trigger_price <= 0:
        raise HTTPException(status_code=400, detail="트리거 가격은 0보다 커야 합니다.")

    if trigger_type == "STOP_LOSS" and req.trigger_price >= current_price:
        raise HTTPException(
            status_code=400,
            detail=f"손절가({req.trigger_price:,.2f})는 현재가({current_price:,.2f})보다 낮아야 합니다."
        )
    if trigger_type == "TAKE_PROFIT" and req.trigger_price <= current_price:
        raise HTTPException(
            status_code=400,
            detail=f"익절가({req.trigger_price:,.2f})는 현재가({current_price:,.2f})보다 높아야 합니다."
        )

    now = datetime.now().isoformat()

    with get_db() as conn:
        holding = conn.execute(
            "SELECT quantity FROM holdings WHERE user_id = ? AND symbol = ?",
            (user_id, symbol),
        ).fetchone()
        if not holding or holding["quantity"] < req.quantity:
            held = holding["quantity"] if holding else 0.0
            raise HTTPException(
                status_code=400,
                detail=f"보유 수량 부족: {held:.4f}주 (요청 {req.quantity:.4f}주)"
            )
        conn.execute(
            "UPDATE holdings SET quantity = quantity - ? WHERE user_id = ? AND symbol = ?",
            (req.quantity, user_id, symbol),
        )
        conn.execute(
            "INSERT INTO pending_orders "
            "(user_id, ticker, market, order_type, quantity, limit_price, currency, created_at, trigger_type, parent_order_id) "
            "VALUES (?, ?, ?, 'LIMIT_SELL', ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, req.quantity, req.trigger_price, currency, now,
             trigger_type, req.parent_order_id),
        )
        order_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    label   = "손절" if trigger_type == "STOP_LOSS" else "익절"
    cur_sym = "₩" if currency == "KRW" else "$"
    return {
        "success": True,
        "id":      order_id,
        "message": f"{label} 주문 등록: {symbol} {req.quantity}주 @ {cur_sym}{req.trigger_price:,.2f}",
    }


@app.post("/api/order/stop-loss")
def api_stop_loss(req: TriggerOrderRequest, user_id: int = Depends(require_auth)):
    return _place_trigger_order(req, "STOP_LOSS", user_id)


@app.post("/api/order/take-profit")
def api_take_profit(req: TriggerOrderRequest, user_id: int = Depends(require_auth)):
    return _place_trigger_order(req, "TAKE_PROFIT", user_id)


# ─────────────────────────────────────────
# OCO (One-Cancels-the-Other) orders
# ─────────────────────────────────────────

class OcoOrderRequest(BaseModel):
    symbol:   str
    market:   str = "auto"
    quantity: float
    sl_price: float
    tp_price: float


@app.post("/api/order/oco")
def api_oco_order(req: OcoOrderRequest, user_id: int = Depends(require_auth)):
    symbol = req.symbol.upper()
    market = req.market.upper() if req.market != "auto" else detect_market(symbol)

    info = get_stock_info(symbol, market)
    if not info:
        raise HTTPException(status_code=404, detail=f"종목을 찾을 수 없습니다: {symbol}")
    current_price = info["price"]
    currency      = info["currency"]

    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="수량은 0보다 커야 합니다.")
    if req.sl_price <= 0 or req.tp_price <= 0:
        raise HTTPException(status_code=400, detail="SL/TP 가격은 0보다 커야 합니다.")
    if req.sl_price >= current_price:
        raise HTTPException(
            status_code=400,
            detail=f"손절가({req.sl_price:,.2f})는 현재가({current_price:,.2f})보다 낮아야 합니다."
        )
    if req.tp_price <= current_price:
        raise HTTPException(
            status_code=400,
            detail=f"익절가({req.tp_price:,.2f})는 현재가({current_price:,.2f})보다 높아야 합니다."
        )

    oco_id = str(uuid.uuid4())
    now    = datetime.now().isoformat()

    with get_db() as conn:
        holding = conn.execute(
            "SELECT quantity FROM holdings WHERE user_id = ? AND symbol = ?",
            (user_id, symbol),
        ).fetchone()
        if not holding or holding["quantity"] < req.quantity:
            held = holding["quantity"] if holding else 0.0
            raise HTTPException(
                status_code=400,
                detail=f"보유 수량 부족: {held:.4f}주 (요청 {req.quantity:.4f}주)"
            )

        # Deduct holdings once (both legs share the same underlying shares)
        conn.execute(
            "UPDATE holdings SET quantity = quantity - ? WHERE user_id = ? AND symbol = ?",
            (req.quantity, user_id, symbol),
        )

        # SL leg
        conn.execute(
            "INSERT INTO pending_orders "
            "(user_id, ticker, market, order_type, quantity, limit_price, currency, created_at, trigger_type, oco_group_id) "
            "VALUES (?, ?, ?, 'LIMIT_SELL', ?, ?, ?, ?, 'STOP_LOSS', ?)",
            (user_id, symbol, market, req.quantity, req.sl_price, currency, now, oco_id),
        )
        sl_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # TP leg
        conn.execute(
            "INSERT INTO pending_orders "
            "(user_id, ticker, market, order_type, quantity, limit_price, currency, created_at, trigger_type, oco_group_id) "
            "VALUES (?, ?, ?, 'LIMIT_SELL', ?, ?, ?, ?, 'TAKE_PROFIT', ?)",
            (user_id, symbol, market, req.quantity, req.tp_price, currency, now, oco_id),
        )
        tp_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    cur_sym = "₩" if currency == "KRW" else "$"
    return {
        "success":      True,
        "oco_group_id": oco_id,
        "sl_id":        sl_id,
        "tp_id":        tp_id,
        "message": (
            f"OCO 주문 등록: {symbol} {req.quantity}주 "
            f"| SL {cur_sym}{req.sl_price:,.2f} / TP {cur_sym}{req.tp_price:,.2f}"
        ),
    }


# ─────────────────────────────────────────
# Exchange
# ─────────────────────────────────────────

@app.get("/api/exchange/rate")
def api_exchange_rate():
    fx = get_exchange_rate()
    applied = round(fx["rate"] * (1 - FX_SPREAD_FEE), 4)
    return {**fx, "fee_rate": FX_SPREAD_FEE, "applied_rate": applied}


class ExchangeRequest(BaseModel):
    direction: str   # "KRW_TO_USD" | "USD_TO_KRW"
    amount: float    # source currency amount


@app.post("/api/exchange")
def api_exchange(req: ExchangeRequest, user_id: int = Depends(require_auth)):
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="금액은 0보다 커야 합니다.")

    fx           = get_exchange_rate()
    mid_rate     = fx["rate"]                              # KRW per 1 USD (mid-market)
    applied_rate = mid_rate * (1 - FX_SPREAD_FEE)         # rate actually used for conversion
    now          = datetime.now().isoformat()

    with get_db() as conn:
        def get_bal(cur):
            row = conn.execute(
                "SELECT amount FROM balances WHERE user_id=? AND currency=?", (user_id, cur)
            ).fetchone()
            return row["amount"] if row else 0.0

        def add_bal(cur, delta):
            rows = conn.execute(
                "UPDATE balances SET amount = amount + ? WHERE user_id=? AND currency=?",
                (delta, user_id, cur),
            ).rowcount
            if rows == 0:
                conn.execute(
                    "INSERT INTO balances (user_id, currency, amount) VALUES (?,?,?)",
                    (user_id, cur, delta),
                )

        if req.direction == "KRW_TO_USD":
            krw_out    = req.amount
            usd_in     = krw_out / applied_rate
            fee_amount = round(krw_out * FX_SPREAD_FEE, 2)   # fee in KRW
            bal_krw = get_bal("KRW")
            if bal_krw < krw_out:
                raise HTTPException(
                    status_code=400,
                    detail=f"KRW 잔고 부족: ₩{bal_krw:,.0f} (필요 ₩{krw_out:,.0f})"
                )
            add_bal("KRW", -krw_out)
            add_bal("USD",  usd_in)
            conn.execute(
                "INSERT INTO transactions (user_id,symbol,market,action,quantity,price,total,currency,timestamp,fee)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (user_id, "KRW/USD", "FX", "exchange", round(usd_in, 6), mid_rate, krw_out, "KRW", now, fee_amount),
            )
            _record_snapshot(user_id, conn)
            return {
                "success":       True,
                "from_currency": "KRW", "from_amount": krw_out,
                "to_currency":   "USD", "to_amount":   usd_in,
                "mid_rate":      mid_rate,
                "applied_rate":  applied_rate,
                "fee_rate":      FX_SPREAD_FEE,
                "fee_amount":    fee_amount,
                "fee_currency":  "KRW",
            }

        elif req.direction == "USD_TO_KRW":
            usd_out    = req.amount
            krw_in     = usd_out * applied_rate
            fee_amount = round(usd_out * FX_SPREAD_FEE, 6)   # fee in USD
            bal_usd = get_bal("USD")
            if bal_usd < usd_out:
                raise HTTPException(
                    status_code=400,
                    detail=f"USD 잔고 부족: ${bal_usd:,.2f} (필요 ${usd_out:,.2f})"
                )
            add_bal("USD", -usd_out)
            add_bal("KRW",  krw_in)
            conn.execute(
                "INSERT INTO transactions (user_id,symbol,market,action,quantity,price,total,currency,timestamp,fee)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (user_id, "USD/KRW", "FX", "exchange", round(krw_in, 2), mid_rate, usd_out, "USD", now, fee_amount),
            )
            _record_snapshot(user_id, conn)
            return {
                "success":       True,
                "from_currency": "USD", "from_amount": usd_out,
                "to_currency":   "KRW", "to_amount":   krw_in,
                "mid_rate":      mid_rate,
                "applied_rate":  applied_rate,
                "fee_rate":      FX_SPREAD_FEE,
                "fee_amount":    fee_amount,
                "fee_currency":  "USD",
            }
        else:
            raise HTTPException(status_code=400, detail="direction 값이 올바르지 않습니다.")


# ─────────────────────────────────────────
# Transactions
# ─────────────────────────────────────────

@app.get("/api/transactions")
def api_transactions(limit: int = 100, user_id: int = Depends(require_auth)):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM transactions WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────
# Short selling
# ─────────────────────────────────────────

class ShortOpenRequest(BaseModel):
    symbol:   str
    market:   str = "auto"
    quantity: float
    leverage: float = 1.0


class ShortCloseRequest(BaseModel):
    position_id: int


@app.post("/api/short/open")
def api_short_open(req: ShortOpenRequest, user_id: int = Depends(require_auth)):
    symbol = req.symbol.upper()
    market = req.market.upper() if req.market != "auto" else detect_market(symbol)

    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="수량은 0보다 커야 합니다.")

    info = get_stock_info(symbol, market)
    if not info:
        raise HTTPException(status_code=404, detail=f"종목을 찾을 수 없습니다: {symbol}")

    price    = info["price"]
    currency = info["currency"]

    if market == "KR" and not is_korean_market_open():
        raise HTTPException(status_code=400, detail="한국 장이 마감되어 거래할 수 없습니다.")
    if market == "US" and not is_us_market_open():
        raise HTTPException(status_code=400, detail="미국 장이 마감되어 거래할 수 없습니다.")

    leverage = round(req.leverage, 1)
    is_leveraged = leverage > 1.0

    if is_leveraged:
        if market in _LEVERAGE_ALLOWED:
            allowed = _LEVERAGE_ALLOWED[market]
            if leverage not in allowed:
                names = ", ".join(f"{x}×" for x in allowed)
                raise HTTPException(status_code=400,
                    detail=f"{market} 숏 레버리지는 {names} 중 하나여야 합니다.")
        else:  # CRYPTO
            if not (1.0 <= leverage <= 125.0):
                raise HTTPException(status_code=400,
                    detail="CRYPTO 레버리지는 1.0 ~ 125.0 사이여야 합니다.")

    qty = req.quantity

    # Slippage (short open = sell, unfavorable = lower execution price)
    fx_rate_val     = get_exchange_rate()["rate"] if market == "KR" else 1.0
    slip_pct        = calculate_slippage(price, qty, market, "sell", fx_rate_val)
    execution_price = price * (1 - slip_pct)

    rnd             = 2 if currency == "USD" else 0
    slippage_amount = round(abs(execution_price - price) * qty, rnd)
    price           = execution_price

    fee_rate   = get_fee_rate(user_id, market)
    tax_rate   = KRX_SELL_TAX_RATE if market == "KR" else 0.0
    base_total = round(qty * price, rnd)
    fee_amount = round(base_total * fee_rate, rnd)
    tax_amount = round(base_total * tax_rate, rnd)

    if market == "US":
        sec_fee = max(0.01, round(base_total * SEC_FEE_PER_DOLLAR, 2))
    else:
        sec_fee = 0.0

    # Margin: leveraged → position_value/leverage; plain → 1.5× position_value
    position_value = round(qty * price, rnd)
    if is_leveraged:
        margin_amount = round(position_value / leverage, rnd)
    else:
        margin_amount = round(position_value * 1.5, rnd)

    daily_borrow_rate = {
        "KR": KRX_LEVERAGE_INTEREST,
        "US": US_LEVERAGE_INTEREST,
    }.get(market, CRYPTO_LEVERAGE_INTEREST)

    with get_db() as conn:
        bal_row = conn.execute(
            "SELECT amount FROM balances WHERE user_id = ? AND currency = ?", (user_id, currency)
        ).fetchone()
        bal = bal_row["amount"] if bal_row else 0.0
        if bal < margin_amount:
            raise HTTPException(
                status_code=400,
                detail=f"담보금 부족: {currency} {bal:,.2f} (필요 {margin_amount:,.2f})"
            )

        conn.execute(
            "UPDATE balances SET amount = amount - ? WHERE user_id = ? AND currency = ?",
            (margin_amount, user_id, currency),
        )
        now = datetime.now().isoformat()
        conn.execute(
            "INSERT INTO short_positions "
            "(user_id, ticker, market, quantity, entry_price, currency, leverage, margin_amount, daily_borrow_rate) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, qty, price, currency, leverage, margin_amount, daily_borrow_rate),
        )
        pos_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO transactions "
            "(user_id, symbol, market, action, quantity, price, total, currency, timestamp, "
            "fee_amount, tax_amount, slippage_amount, sec_fee, position_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, "short_open", qty, price, base_total,
             currency, now, fee_amount, tax_amount, slippage_amount, sec_fee, "SHORT"),
        )

    liq_price = round(price * (1 + 1 / leverage), rnd) if is_leveraged else None

    return {
        "success":      True,
        "message":      f"{symbol} {'%g×' % leverage + ' 레버리지 ' if is_leveraged else ''}공매도 개시: {qty:.4f}주 @ {price:,.2f} {currency}",
        "position_id":  pos_id,
        "entry_price":  price,
        "quantity":     qty,
        "leverage":     leverage,
        "margin_amount": margin_amount,
        "liquidation_price": liq_price,
        "fee_amount":   fee_amount,
        "currency":     currency,
    }


@app.post("/api/short/close")
def api_short_close(req: ShortCloseRequest, user_id: int = Depends(require_auth)):
    with get_db() as conn:
        pos = conn.execute(
            "SELECT * FROM short_positions WHERE id = ? AND user_id = ? AND status = 'OPEN'",
            (req.position_id, user_id),
        ).fetchone()
        if not pos:
            raise HTTPException(status_code=404, detail="공매도 포지션을 찾을 수 없습니다.")
        pos = dict(pos)

    symbol      = pos["ticker"]
    market      = pos["market"]
    currency    = pos["currency"]
    qty         = pos["quantity"]
    entry_price = pos["entry_price"]

    if market == "KR" and not is_korean_market_open():
        raise HTTPException(status_code=400, detail="한국 장이 마감되어 거래할 수 없습니다.")
    if market == "US" and not is_us_market_open():
        raise HTTPException(status_code=400, detail="미국 장이 마감되어 거래할 수 없습니다.")

    info = get_stock_info(symbol, market)
    if not info:
        raise HTTPException(status_code=404, detail="종목 정보를 불러올 수 없습니다.")

    current_price = info["price"]

    # Slippage (close = buy-to-cover, unfavorable = higher price)
    fx_rate_val     = get_exchange_rate()["rate"] if market == "KR" else 1.0
    slip_pct        = calculate_slippage(current_price, qty, market, "buy", fx_rate_val)
    execution_price = current_price * (1 + slip_pct)

    rnd             = 2 if currency == "USD" else 0
    slippage_amount = round(abs(execution_price - current_price) * qty, rnd)
    current_price   = execution_price

    leverage       = float(pos.get("leverage") or 1.0)
    stored_margin  = float(pos.get("margin_amount") or 0.0)
    is_leveraged   = leverage > 1.0

    fee_rate   = get_fee_rate(user_id, market)
    base_total = round(qty * current_price, rnd)
    fee_amount = round(base_total * fee_rate, rnd)

    # Margin: use stored value; fallback to 1.5× for old un-migrated positions
    if stored_margin > 0:
        margin = stored_margin
    else:
        margin = round(entry_price * qty * 1.5, rnd)

    # Interest on borrowed portion (only for leveraged shorts)
    if is_leveraged:
        opened_at        = datetime.fromisoformat(pos["opened_at"])
        minutes_held     = (datetime.now() - opened_at).total_seconds() / 60
        borrowed         = round(entry_price * qty * (1 - 1 / leverage), rnd)
        interest_accrued = round(pos["daily_borrow_rate"] * borrowed * (minutes_held / 1440), rnd)
    else:
        interest_accrued = 0.0

    pnl      = round((entry_price - current_price) * qty - fee_amount - interest_accrued, rnd)
    returned = round(margin + pnl, rnd)
    now      = datetime.now().isoformat()

    with get_db() as conn:
        conn.execute(
            "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
            (returned, user_id, currency),
        )
        conn.execute(
            "UPDATE short_positions SET status = 'CLOSED' WHERE id = ?", (req.position_id,)
        )
        conn.execute(
            "INSERT INTO transactions "
            "(user_id, symbol, market, action, quantity, price, total, currency, timestamp, "
            "fee_amount, slippage_amount, position_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, "short_close", qty, current_price, returned,
             currency, now, fee_amount, slippage_amount, "SHORT"),
        )

    return {
        "success":            True,
        "message":            f"{symbol} 공매도 청산: {qty:.4f}주 @ {current_price:,.2f} {currency}",
        "entry_price":        entry_price,
        "close_price":        current_price,
        "pnl":                pnl,
        "interest_accrued":   interest_accrued,
        "collateral_released": margin,
        "fee_amount":         fee_amount,
        "currency":           currency,
    }


@app.get("/api/short/positions")
def api_short_positions(user_id: int = Depends(require_auth)):
    with get_db() as conn:
        positions = [dict(r) for r in conn.execute(
            "SELECT * FROM short_positions WHERE user_id = ? AND status = 'OPEN' "
            "ORDER BY opened_at DESC",
            (user_id,),
        ).fetchall()]

    enriched = []
    for pos in positions:
        info          = get_stock_info(pos["ticker"], pos["market"])
        current_price = info["price"] if info else pos["entry_price"]

        qty           = pos["quantity"]
        entry         = pos["entry_price"]
        leverage      = float(pos.get("leverage") or 1.0)
        stored_margin = float(pos.get("margin_amount") or 0.0)
        is_leveraged  = leverage > 1.0

        # Margin used
        margin = stored_margin if stored_margin > 0 else round(entry * qty * 1.5, 2)

        unrealized_pl   = (entry - current_price) * qty
        unrealized_loss = max(0.0, -unrealized_pl)
        margin_call     = unrealized_loss > margin * 0.5

        opened_at  = datetime.fromisoformat(pos["opened_at"])
        minutes_held = (datetime.now() - opened_at).total_seconds() / 60

        if is_leveraged:
            borrowed = entry * qty * (1 - 1 / leverage)
            borrow_fee_accrued = pos["daily_borrow_rate"] * borrowed * (minutes_held / 1440)
            liq_price = round(entry * (1 + 1 / leverage), 2 if pos["currency"] == "USD" else 0)
        else:
            borrow_fee_accrued = pos["daily_borrow_rate"] * entry * qty * (minutes_held / 1440)
            liq_price = None

        rnd = 2 if pos["currency"] == "USD" else 0
        enriched.append({
            **pos,
            "leverage":           leverage,
            "name":               info["name"] if info else pos["ticker"],
            "current_price":      round(current_price, rnd),
            "unrealized_pl":      round(unrealized_pl, rnd),
            "margin":             round(margin, rnd),
            "liquidation_price":  liq_price,
            "borrow_fee_accrued": round(borrow_fee_accrued, rnd),
            "margin_call":        margin_call,
        })

    return enriched


# ─────────────────────────────────────────
# Leverage trading
# ─────────────────────────────────────────

class LeverageOpenRequest(BaseModel):
    symbol:   str
    market:   str = "auto"
    quantity: float
    leverage: float


class LeverageCloseRequest(BaseModel):
    position_id: int


@app.post("/api/leverage/open")
def api_leverage_open(req: LeverageOpenRequest, user_id: int = Depends(require_auth)):
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="수량은 0보다 커야 합니다.")

    symbol = req.symbol.upper()
    market = req.market.upper() if req.market != "auto" else detect_market(symbol)

    # Market-specific leverage validation
    if market in _LEVERAGE_ALLOWED:
        lev = round(req.leverage, 1)
        allowed = _LEVERAGE_ALLOWED[market]
        if lev not in allowed:
            names = ", ".join(f"{x}×" for x in allowed)
            raise HTTPException(
                status_code=400,
                detail=f"{market} 레버리지는 {names} 중 하나여야 합니다."
            )
    else:  # CRYPTO — any float 1.0–125.0
        lev = round(req.leverage, 1)
        if not (1.0 <= lev <= 125.0):
            raise HTTPException(
                status_code=400,
                detail="CRYPTO 레버리지는 1.0 ~ 125.0 사이여야 합니다."
            )

    info = get_stock_info(symbol, market)
    if not info:
        raise HTTPException(status_code=404, detail=f"종목을 찾을 수 없습니다: {symbol}")

    price    = info["price"]
    currency = info["currency"]

    if market == "KR" and not is_korean_market_open():
        raise HTTPException(status_code=400, detail="한국 장이 마감되어 거래할 수 없습니다.")
    if market == "US" and not is_us_market_open():
        raise HTTPException(status_code=400, detail="미국 장이 마감되어 거래할 수 없습니다.")

    qty = req.quantity

    # Slippage on full position size (buy, unfavorable = higher price)
    fx_rate_val     = get_exchange_rate()["rate"] if market == "KR" else 1.0
    slip_pct        = calculate_slippage(price, qty, market, "buy", fx_rate_val)
    execution_price = price * (1 + slip_pct)

    rnd             = 2 if currency == "USD" else 0
    slippage_amount = round(abs(execution_price - price) * qty, rnd)
    price           = execution_price

    position_value  = round(qty * price, rnd)
    margin_amount   = round(position_value / lev, rnd)
    borrowed_amount = round(position_value - margin_amount, rnd)

    fee_rate   = get_fee_rate(user_id, market)
    fee_amount = round(position_value * fee_rate, rnd)
    total_deducted = round(margin_amount + fee_amount, rnd)

    daily_interest_rate = {
        "KR": KRX_LEVERAGE_INTEREST,
        "US": US_LEVERAGE_INTEREST,
    }.get(market, CRYPTO_LEVERAGE_INTEREST)

    with get_db() as conn:
        bal_row = conn.execute(
            "SELECT amount FROM balances WHERE user_id = ? AND currency = ?", (user_id, currency)
        ).fetchone()
        bal = bal_row["amount"] if bal_row else 0.0
        if bal < total_deducted:
            raise HTTPException(
                status_code=400,
                detail=f"증거금 부족: {currency} {bal:,.2f} (필요 {total_deducted:,.2f})"
            )

        conn.execute(
            "UPDATE balances SET amount = amount - ? WHERE user_id = ? AND currency = ?",
            (total_deducted, user_id, currency),
        )
        now = datetime.now().isoformat()
        conn.execute(
            "INSERT INTO leveraged_positions "
            "(user_id, ticker, market, quantity, entry_price, leverage, "
            "margin_amount, borrowed_amount, currency, daily_interest_rate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, qty, price, lev,
             margin_amount, borrowed_amount, currency, daily_interest_rate),
        )
        pos_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO transactions "
            "(user_id, symbol, market, action, quantity, price, total, currency, timestamp, "
            "fee_amount, slippage_amount, position_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, "lev_open", qty, price, total_deducted,
             currency, now, fee_amount, slippage_amount, "LEVERAGE"),
        )

    liq      = round(_liq_price(price, lev), rnd)
    move_pct = round((1 / lev - 0.005) * 100, 2)

    result = {
        "success":            True,
        "message":            f"{symbol} {lev}× 레버리지 매수: {qty:.4f}주 @ {price:,.2f} {currency}",
        "position_id":        pos_id,
        "entry_price":        price,
        "quantity":           qty,
        "leverage":           lev,
        "margin_amount":      margin_amount,
        "borrowed_amount":    borrowed_amount,
        "fee_amount":         fee_amount,
        "liquidation_price":  liq,
        "move_pct_to_liq":    move_pct,
        "daily_interest_rate": daily_interest_rate,
        "currency":           currency,
    }
    if lev >= 50:
        result["warning"] = (
            f"At {lev}×, a {move_pct}% price move will liquidate your position. Extreme risk."
        )
    elif lev >= 20:
        result["warning"] = (
            f"At {lev}×, a {move_pct}% price move will liquidate your position."
        )
    return result


@app.post("/api/leverage/close")
def api_leverage_close(req: LeverageCloseRequest, user_id: int = Depends(require_auth)):
    with get_db() as conn:
        pos = conn.execute(
            "SELECT * FROM leveraged_positions WHERE id = ? AND user_id = ? AND status = 'OPEN'",
            (req.position_id, user_id),
        ).fetchone()
        if not pos:
            raise HTTPException(status_code=404, detail="레버리지 포지션을 찾을 수 없습니다.")
        pos = dict(pos)

    symbol         = pos["ticker"]
    market         = pos["market"]
    currency       = pos["currency"]
    qty            = pos["quantity"]
    entry_price    = pos["entry_price"]
    margin_amount  = pos["margin_amount"]
    borrowed_amount = pos["borrowed_amount"]

    if market == "KR" and not is_korean_market_open():
        raise HTTPException(status_code=400, detail="한국 장이 마감되어 거래할 수 없습니다.")
    if market == "US" and not is_us_market_open():
        raise HTTPException(status_code=400, detail="미국 장이 마감되어 거래할 수 없습니다.")

    info = get_stock_info(symbol, market)
    if not info:
        raise HTTPException(status_code=404, detail="종목 정보를 불러올 수 없습니다.")

    current_price = info["price"]

    # Slippage (sell, unfavorable = lower price)
    fx_rate_val     = get_exchange_rate()["rate"] if market == "KR" else 1.0
    slip_pct        = calculate_slippage(current_price, qty, market, "sell", fx_rate_val)
    execution_price = current_price * (1 - slip_pct)

    rnd             = 2 if currency == "USD" else 0
    slippage_amount = round(abs(execution_price - current_price) * qty, rnd)
    current_price   = execution_price

    fee_rate   = get_fee_rate(user_id, market)
    current_value = round(qty * current_price, rnd)
    fee_amount    = round(current_value * fee_rate, rnd)

    # Interest accrued since open
    opened_at        = datetime.fromisoformat(pos["opened_at"])
    minutes_held     = (datetime.now() - opened_at).total_seconds() / 60
    interest_accrued = round(
        pos["daily_interest_rate"] * borrowed_amount * (minutes_held / 1440), rnd
    )

    gross_pnl    = round(current_value - qty * entry_price, rnd)
    net_proceeds = round(margin_amount + gross_pnl - interest_accrued - fee_amount, rnd)
    now          = datetime.now().isoformat()

    with get_db() as conn:
        conn.execute(
            "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
            (net_proceeds, user_id, currency),
        )
        conn.execute(
            "UPDATE leveraged_positions SET status = 'CLOSED' WHERE id = ?", (req.position_id,)
        )
        conn.execute(
            "INSERT INTO transactions "
            "(user_id, symbol, market, action, quantity, price, total, currency, timestamp, "
            "fee_amount, slippage_amount, position_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, symbol, market, "lev_close", qty, current_price, net_proceeds,
             currency, now, fee_amount, slippage_amount, "LEVERAGE"),
        )

    return {
        "success":          True,
        "message":          f"{symbol} {pos['leverage']}× 레버리지 청산: {qty:.4f}주 @ {current_price:,.2f} {currency}",
        "entry_price":      entry_price,
        "close_price":      current_price,
        "gross_pnl":        gross_pnl,
        "interest_accrued": interest_accrued,
        "fee_amount":       fee_amount,
        "net_proceeds":     net_proceeds,
        "currency":         currency,
    }


@app.get("/api/leverage/positions")
def api_leverage_positions(user_id: int = Depends(require_auth)):
    with get_db() as conn:
        positions = [dict(r) for r in conn.execute(
            "SELECT * FROM leveraged_positions WHERE user_id = ? AND status = 'OPEN' "
            "ORDER BY opened_at DESC",
            (user_id,),
        ).fetchall()]

    enriched = []
    for pos in positions:
        info          = get_stock_info(pos["ticker"], pos["market"])
        current_price = info["price"] if info else pos["entry_price"]

        qty            = pos["quantity"]
        entry          = pos["entry_price"]
        margin_amount  = pos["margin_amount"]
        borrowed_amount = pos["borrowed_amount"]

        current_value    = qty * current_price
        gross_pnl        = current_value - qty * entry
        liq              = _liq_price(entry, pos["leverage"])
        near_liquidation = current_price <= liq * 1.10  # within 10% of liq price

        opened_at        = datetime.fromisoformat(pos["opened_at"])
        minutes_held     = (datetime.now() - opened_at).total_seconds() / 60
        interest_accrued = pos["daily_interest_rate"] * borrowed_amount * (minutes_held / 1440)

        unrealized_pnl = gross_pnl - interest_accrued
        margin_ratio   = (margin_amount + gross_pnl) / margin_amount if margin_amount else 0

        rnd = 2 if pos["currency"] == "USD" else 0
        enriched.append({
            **pos,
            "name":              info["name"] if info else pos["ticker"],
            "current_price":     round(current_price, rnd),
            "current_value":     round(current_value, rnd),
            "unrealized_pnl":    round(unrealized_pnl, rnd),
            "gross_pnl":         round(gross_pnl, rnd),
            "interest_accrued":  round(interest_accrued, rnd),
            "liquidation_price": round(liq, rnd),
            "near_liquidation":  near_liquidation,
            "margin_ratio":      round(margin_ratio, 4),
        })

    return enriched


# ─────────────────────────────────────────
# Dividend endpoints
# ─────────────────────────────────────────

@app.get("/api/dividends/pending")
def api_dividends_pending(user_id: int = Depends(require_auth)):
    """Return and clear any queued dividend toast entries for this user."""
    with _dividend_toast_lock:
        entries = _pending_dividend_toasts.pop(user_id, [])
    return entries


@app.get("/api/dividends/total")
def api_dividends_total(user_id: int = Depends(require_auth)):
    """Return total dividends received by currency."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT currency, SUM(total) as total FROM transactions "
            "WHERE user_id = ? AND action = 'DIVIDEND' GROUP BY currency",
            (user_id,),
        ).fetchall()
    return {r["currency"]: round(r["total"], 2 if r["currency"] == "USD" else 0) for r in rows}


# ─────────────────────────────────────────
# Top Movers
# ─────────────────────────────────────────

import pandas as _pd
import time as _time_mod
import concurrent.futures as _cf

# KRX ticker pool — used because pykrx bulk API (get_market_price_change_by_ticker)
# is broken on this system: the KRX business-day API returns empty data, causing
# all market-wide pykrx functions to fail. Per-ticker OHLCV (get_market_ohlcv_by_date)
# is the only working approach.
KRX_TICKERS = [
    "005930","000660","035420","005380","000270","051910","006400","035720",
    "028260","096770","207940","068270","003550","032830","017670","030200",
    "105560","055550","316140","010130","086790","034730","010950","251270",
    "009150","012330","003490","024110","000810","011200","003670","015760",
    "009830","033780","002790","071050","004020","042660","000120","034220",
    "064350","018880","010140","047810","009540","011070","003410","039490",
    "004170","004050","000100","001040","003230","006800","007070","009240",
    "010060","011790","012750","017800","018260","020560","021240","024070",
    "025540","027740","028050","032500","036460","039130","042700",
    "051600","054540","058650","065690","066570","067080","069960","079550",
    "035900","041510","122870","263750","091990","241560","290510","094170",
    "064760","036570","066970","080160","086520","145020","196170","214150",
    "247540","258790","293490","357780","393890","950130",
]

MOVERS_CACHE: dict = {}


def _krx_trading_date(now_kst) -> str:
    """Return the most recent KST trading date (YYYYMMDD) without any API call."""
    candidate = now_kst.date()
    if now_kst.time() < _time(9, 0):
        candidate = candidate - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate = candidate - timedelta(days=1)
    return candidate.strftime("%Y%m%d")


def _normalize_krx_df(df: "_pd.DataFrame") -> "_pd.DataFrame":
    """Rename pykrx columns to stable English names regardless of pykrx version."""
    col_map = {}
    for col in df.columns:
        c = str(col)
        if any(x in c for x in ["등락률", "등락", "변동", "수익률"]): col_map[col] = "change_pct"
        elif any(x in c for x in ["종가", "현재가"]):                  col_map[col] = "price"
        elif "고가" in c:                                               col_map[col] = "high"
        elif "저가" in c:                                               col_map[col] = "low"
        elif "거래량" in c:                                             col_map[col] = "volume"
        elif any(x in c for x in ["거래대금", "거래금액"]):             col_map[col] = "trading_value"
    df = df.rename(columns=col_map)
    if "trading_value" not in df.columns:
        df["trading_value"] = (
            _pd.to_numeric(df.get("price",  0), errors="coerce").fillna(0) *
            _pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
        )
    return df


@app.get("/api/movers/debug")
async def movers_debug():
    import traceback
    result = {}

    # Test 1a: pure Python date (no pykrx)
    import pytz as _pytz2
    _KST2 = _pytz2.timezone("Asia/Seoul")
    _now2  = datetime.now(_KST2)
    _cand  = _now2.date()
    if _now2.time() < _time(9, 0):
        _cand = _cand - timedelta(days=1)
    while _cand.weekday() >= 5:
        _cand = _cand - timedelta(days=1)
    date_str = _cand.strftime("%Y%m%d")
    result["krx_date_python"] = date_str

    # Test 1b: import pykrx separately
    try:
        from pykrx import stock as krx_stock
        result["krx_import"] = "ok"
    except Exception as e:
        result["krx_import_error"] = str(e)
        return result

    # Test 2a: get_market_price_change_by_ticker (bulk)
    try:
        import pandas as _pd2
        df_k = krx_stock.get_market_price_change_by_ticker(date_str, date_str, market="KOSPI")
        result["krx_price_change_rows"]    = len(df_k)
        result["krx_price_change_columns"] = df_k.columns.tolist()
        if len(df_k) > 0:
            result["krx_price_change_sample"] = df_k.head(2).to_dict()
    except Exception as e:
        result["krx_price_change_error"] = str(e)

    # Test 2b: get_market_ohlcv_by_ticker (whole-market OHLCV, different function)
    try:
        import pandas as _pd2
        df_ohlcv = krx_stock.get_market_ohlcv_by_ticker(date_str, market="KOSPI")
        result["krx_ohlcv_by_ticker_rows"]    = len(df_ohlcv)
        result["krx_ohlcv_by_ticker_columns"] = df_ohlcv.columns.tolist()
        if len(df_ohlcv) > 0:
            result["krx_ohlcv_by_ticker_sample"] = df_ohlcv.head(2).to_dict()
    except Exception as e:
        result["krx_ohlcv_by_ticker_error"] = str(e)

    # Test 2c: get_market_ticker_list (all ticker codes for market)
    try:
        tickers = krx_stock.get_market_ticker_list(date_str, market="KOSPI")
        result["krx_ticker_list_count"]  = len(tickers)
        result["krx_ticker_list_sample"] = tickers[:5]
    except Exception as e:
        result["krx_ticker_list_error"] = str(e)

    # Test 3: US screener (yf.screen)
    try:
        r = yf.screen("day_gainers", count=5)
        result["us_screener_keys"]   = list(r.keys()) if r else []
        result["us_screener_count"]  = len(r.get("quotes", []))
        result["us_screener_sample"] = r.get("quotes", [])[:2]
    except Exception as e:
        result["us_screener_error"]     = str(e)
        result["us_screener_traceback"] = traceback.format_exc()

    # Test 4: US screener fallback (direct HTTP)
    try:
        import requests
        url = (
            "https://query1.finance.yahoo.com/v1/finance/screener"
            "/predefined/saved?formatted=false&scrIds=day_gainers&count=5"
        )
        resp   = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        data   = resp.json()
        quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        result["us_fallback_count"]  = len(quotes)
        result["us_fallback_sample"] = quotes[:2]
    except Exception as e:
        result["us_fallback_error"] = str(e)

    return result


@app.get("/api/movers")
async def api_movers(
    market:    str = "KRX",
    direction: str = "up",
    user_id: int = Depends(require_auth),
):
    import traceback as _tb

    market    = market.upper()
    direction = direction.lower()
    if market    not in ("KRX", "US"):            market    = "KRX"
    if direction not in ("up", "down", "volume"): direction = "up"

    cache_key = f"{market}_{direction}"
    if cache_key in MOVERS_CACHE:
        cached = MOVERS_CACHE[cache_key]
        if _time_mod.time() - cached["ts"] < 300:
            return cached["data"]

    items: list = []

    # ── KRX ──────────────────────────────────────────────────────────
    if market == "KRX":
        try:
            from pykrx import stock as krx_stock
            import pytz

            KST        = pytz.timezone("Asia/Seoul")
            now_kst    = datetime.now(KST)
            date_str   = _krx_trading_date(now_kst)
            start_str  = (now_kst - timedelta(days=7)).strftime("%Y%m%d")
            time_label = f"{date_str[4:6]}/{date_str[6:8]}"

            def _fetch_one(ticker):
                try:
                    ohlcv = krx_stock.get_market_ohlcv_by_date(start_str, date_str, ticker)
                    if ohlcv is None or len(ohlcv) < 2:
                        return None
                    last    = ohlcv.iloc[-1]
                    prev    = ohlcv.iloc[-2]
                    close   = float(last.get("종가", 0))
                    p_close = float(prev.get("종가", 1))
                    vol     = int(last.get("거래량", 0))
                    if vol == 0 or close == 0 or p_close == 0:
                        return None
                    chg = float(last.get("등락률", (close - p_close) / p_close * 100))
                    return {
                        "ticker":     ticker,
                        "change_pct": round(chg, 2),
                        "price":      close,
                        "high":       float(last.get("고가", close)),
                        "low":        float(last.get("저가", close)),
                        "volume":     vol,
                    }
                except Exception:
                    return None

            rows = [r for r in _cf.ThreadPoolExecutor(max_workers=16).map(_fetch_one, KRX_TICKERS) if r]
            print(f"[KRX movers] {len(rows)} tickers fetched")

            if not rows:
                return {"items": [], "count": 0, "market": market, "direction": direction}

            if direction == "up":
                rows.sort(key=lambda x: x["change_pct"], reverse=True)
            elif direction == "down":
                rows.sort(key=lambda x: x["change_pct"])
            else:
                rows.sort(key=lambda x: x["volume"], reverse=True)

            for r in rows[:20]:
                ticker = r["ticker"]
                try:
                    name = krx_stock.get_market_ticker_name(ticker) or ticker
                except Exception:
                    name = ticker
                items.append({
                    "ticker":     ticker,
                    "name":       name,
                    "price":      int(r["price"]),
                    "high":       int(r["high"]) if r["high"] > 0 else None,
                    "low":        int(r["low"])  if r["low"]  > 0 else None,
                    "change_pct": r["change_pct"],
                    "volume":     r["volume"],
                    "currency":   "KRW",
                    "time":       time_label,
                })

        except Exception as e:
            print(f"[KRX MOVERS ERROR] {e!r}\n{_tb.format_exc()}")
            return {"items": [], "count": 0, "market": market, "direction": direction}

    # ── US ───────────────────────────────────────────────────────────
    elif market == "US":
        try:
            SCREENER_MAP = {"up": "day_gainers", "down": "day_losers", "volume": "most_actives"}
            screen_id    = SCREENER_MAP[direction]

            quotes = []
            try:
                result = yf.screen(screen_id, count=20)
                quotes = result.get("quotes", [])
            except Exception as e:
                print(f"[US movers] yf.screen failed: {e!r}")

            print(f"[US movers] {len(quotes)} quotes from screener '{screen_id}'")

            for q in quotes:
                try:
                    items.append({
                        "ticker":     q.get("symbol", ""),
                        "name":       q.get("shortName") or q.get("longName") or q.get("symbol", ""),
                        "price":      round(float(q.get("regularMarketPrice", 0)), 2),
                        "high":       round(float(q.get("regularMarketDayHigh", 0)), 2) or None,
                        "low":        round(float(q.get("regularMarketDayLow",  0)), 2) or None,
                        "change_pct": round(float(q.get("regularMarketChangePercent", 0)), 2),
                        "volume":     int(q.get("regularMarketVolume", 0)),
                        "currency":   "USD",
                        "time":       "live",
                    })
                except Exception as qe:
                    print(f"[US movers] skip {q.get('symbol')}: {qe!r}")

        except Exception as e:
            print(f"[US MOVERS ERROR] {e!r}\n{_tb.format_exc()}")
            return {"items": [], "count": 0, "market": market, "direction": direction}

    result = {
        "market":    market,
        "direction": direction,
        "count":     len(items),
        "items":     items,
    }
    MOVERS_CACHE[cache_key] = {"ts": _time_mod.time(), "data": result}
    return result


# ─────────────────────────────────────────
# News
# ─────────────────────────────────────────

NEWS_CACHE_MINUTES = 30

RSS_FEEDS = [
    "https://news.google.com/rss/search?q=경제+주식&hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/search?q=stock+market+economy&hl=en&gl=US&ceid=US:en",
    "https://feeds.reuters.com/reuters/businessNews",
]

_news_general_cache: dict = {"ts": None, "articles": []}
_news_holdings_cache: dict = {}   # keyed by user_id


def _parse_feed(url: str, ticker: str | None = None) -> list[dict]:
    """Parse one RSS feed and return a list of article dicts. Silently skip on error."""
    try:
        feed = feedparser.parse(url)
        out = []
        for e in feed.entries:
            pub = e.get("published", e.get("updated", ""))
            try:
                dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc) if e.get("published_parsed") else None
            except Exception:
                dt = None
            article = {
                "title":        e.get("title", "").strip(),
                "link":         e.get("link", ""),
                "source":       feed.feed.get("title", url),
                "published_at": dt.isoformat() if dt else pub,
                "_dt":          dt,
            }
            if ticker:
                article["ticker"] = ticker
            if article["title"]:
                out.append(article)
        return out
    except Exception:
        return []


def _sort_articles(articles: list[dict]) -> list[dict]:
    def key(a):
        return a.get("_dt") or datetime.min.replace(tzinfo=timezone.utc)
    return sorted(articles, key=key, reverse=True)


@app.get("/api/news/general")
def api_news_general():
    now = datetime.now(timezone.utc)
    cache = _news_general_cache
    if cache["ts"] and (now - cache["ts"]).total_seconds() < NEWS_CACHE_MINUTES * 60:
        articles = [{k: v for k, v in a.items() if k != "_dt"} for a in cache["articles"]]
        return {"articles": articles[:20]}

    all_articles: list[dict] = []
    for url in RSS_FEEDS:
        all_articles.extend(_parse_feed(url))

    all_articles = _sort_articles(all_articles)
    cache["ts"]       = now
    cache["articles"] = all_articles

    return {"articles": [{k: v for k, v in a.items() if k != "_dt"} for a in all_articles[:20]]}


@app.get("/api/news/holdings")
def api_news_holdings(user_id: int = Depends(require_auth)):
    now = datetime.now(timezone.utc)
    user_cache = _news_holdings_cache.get(user_id, {"ts": None, "articles": []})
    if user_cache["ts"] and (now - user_cache["ts"]).total_seconds() < NEWS_CACHE_MINUTES * 60:
        articles = [{k: v for k, v in a.items() if k != "_dt"} for a in user_cache["articles"]]
        return {"articles": articles[:40]}

    with get_db() as conn:
        holdings = [dict(r) for r in conn.execute(
            "SELECT symbol, market FROM holdings WHERE user_id = ?", (user_id,)
        ).fetchall()]

    all_articles: list[dict] = []
    for h in holdings:
        ticker = h["symbol"]
        # yfinance news
        try:
            yf_news = yf.Ticker(ticker).news or []
            for n in yf_news[:5]:
                try:
                    dt = datetime.fromtimestamp(n["providerPublishTime"], tz=timezone.utc)
                except Exception:
                    dt = None
                all_articles.append({
                    "title":        n.get("title", "").strip(),
                    "link":         n.get("link", ""),
                    "source":       n.get("publisher", "Yahoo Finance"),
                    "published_at": dt.isoformat() if dt else "",
                    "ticker":       ticker,
                    "_dt":          dt,
                })
        except Exception:
            pass
        # Google News RSS per ticker
        rss_url = f"https://news.google.com/rss/search?q={ticker}&hl=ko&gl=KR&ceid=KR:ko"
        all_articles.extend(_parse_feed(rss_url, ticker=ticker))

    all_articles = _sort_articles(all_articles)
    # Deduplicate by title
    seen: set[str] = set()
    unique: list[dict] = []
    for a in all_articles:
        key = a["title"][:80]
        if key and key not in seen:
            seen.add(key)
            unique.append(a)

    _news_holdings_cache[user_id] = {"ts": now, "articles": unique}
    return {"articles": [{k: v for k, v in a.items() if k != "_dt"} for a in unique[:40]]}


# ─────────────────────────────────────────
# Deposit
# ─────────────────────────────────────────

class DepositRequest(BaseModel):
    currency: str   # "KRW" or "USD"
    amount: float


@app.post("/api/deposit")
async def api_deposit(req: DepositRequest, user_id: int = Depends(require_auth)):
    if req.currency not in ("KRW", "USD"):
        raise HTTPException(400, "currency must be KRW or USD")
    if req.amount <= 0:
        raise HTTPException(400, "amount must be positive")

    with get_db() as conn:
        conn.execute(
            "UPDATE balances SET amount = amount + ? WHERE user_id = ? AND currency = ?",
            (req.amount, user_id, req.currency),
        )
        conn.execute(
            """INSERT INTO transactions
               (user_id, symbol, market, action, quantity, price, total, currency, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, "DEPOSIT", "DEPOSIT", "deposit", 0, 0, req.amount, req.currency,
             datetime.now().isoformat()),
        )
        _record_snapshot(user_id, conn)
        conn.commit()
        new_krw = conn.execute(
            "SELECT amount FROM balances WHERE user_id = ? AND currency = 'KRW'", (user_id,)
        ).fetchone()
        new_usd = conn.execute(
            "SELECT amount FROM balances WHERE user_id = ? AND currency = 'USD'", (user_id,)
        ).fetchone()

    return {
        "success": True,
        "new_balance_krw": new_krw["amount"] if new_krw else 0,
        "new_balance_usd": new_usd["amount"] if new_usd else 0,
    }


# ─────────────────────────────────────────
# P&L Analytics
# ─────────────────────────────────────────

def _record_snapshot(user_id: int, conn) -> None:
    """Snapshot current portfolio equity (KRW equivalent) after each trade/deposit."""
    try:
        fx = get_exchange_rate()["usd_to_krw"]
    except Exception:
        fx = 1380.0
    equity = _compute_actual_equity(user_id, conn, fx)
    conn.execute(
        "INSERT INTO portfolio_snapshots (user_id, timestamp, equity_krw) VALUES (?, ?, ?)",
        (user_id, datetime.now().isoformat(), round(equity, 0)),
    )


def _compute_actual_equity(user_id: int, conn, fx: float) -> float:
    """Current equity: cash + holdings (avg price) + leveraged equity (market - borrowed) + short equity (margin + PnL)."""
    bal_rows = conn.execute(
        "SELECT currency, amount FROM balances WHERE user_id = ?", (user_id,)
    ).fetchall()
    cash_krw = sum(r["amount"] * (fx if r["currency"] == "USD" else 1.0) for r in bal_rows)

    hold_rows = conn.execute(
        "SELECT avg_price, quantity, currency FROM holdings WHERE user_id = ? AND quantity > 0",
        (user_id,),
    ).fetchall()
    hold_krw = sum(
        h["avg_price"] * h["quantity"] * (fx if h["currency"] == "USD" else 1.0)
        for h in hold_rows
    )

    # Leveraged positions: user equity = current_market_value - borrowed_amount
    lev_rows = conn.execute(
        "SELECT ticker, market, quantity, entry_price, borrowed_amount, margin_amount, currency "
        "FROM leveraged_positions WHERE user_id=? AND status='OPEN'",
        (user_id,),
    ).fetchall()
    lev_krw = 0.0
    for lev in lev_rows:
        try:
            info = get_stock_info(lev["ticker"], lev["market"])
            current_price = info["price"] if info else lev["entry_price"]
        except Exception:
            current_price = lev["entry_price"]
        equity = lev["quantity"] * current_price - lev["borrowed_amount"]
        lev_krw += equity * (fx if lev["currency"] == "USD" else 1.0)

    # Short positions: user equity = margin + unrealized PnL = margin + (entry - current) * qty
    short_rows = conn.execute(
        "SELECT ticker, market, quantity, entry_price, margin_amount, currency "
        "FROM short_positions WHERE user_id=? AND status='OPEN'",
        (user_id,),
    ).fetchall()
    short_krw = 0.0
    for sp in short_rows:
        try:
            info = get_stock_info(sp["ticker"], sp["market"])
            current_price = info["price"] if info else sp["entry_price"]
        except Exception:
            current_price = sp["entry_price"]
        unrealized_pnl = (sp["entry_price"] - current_price) * sp["quantity"]
        equity = sp["margin_amount"] + unrealized_pnl
        short_krw += equity * (fx if sp["currency"] == "USD" else 1.0)

    return cash_krw + hold_krw + lev_krw + short_krw


def _backfill_snapshots(user_id: int, conn) -> None:
    """Seed equity curve with anchor points when no live snapshots exist yet.

    The old per-transaction simulation was too error-prone for portfolios with
    leveraged/short positions (borrowed amounts leaked into equity). We now simply
    insert two anchor points — the first transaction timestamp and now — both at
    actual current equity.  Live snapshots recorded by _record_snapshot() after
    each trade will fill in the real history going forward.
    """
    try:
        fx = get_exchange_rate()["usd_to_krw"]
    except Exception:
        fx = 1380.0

    actual_equity = _compute_actual_equity(user_id, conn, fx)

    # Force-clear any snapshots built by the old simulation (detectable by a
    # suspiciously high peak — more than 3× current equity — or by a version flag).
    ver_row = conn.execute(
        "SELECT value FROM settings WHERE user_id=? AND key='equity_snap_ver'", (user_id,)
    ).fetchone()
    if ver_row and ver_row["value"] == "3":
        # Already on current version — just check staleness
        last_row = conn.execute(
            "SELECT equity_krw FROM portfolio_snapshots WHERE user_id=? ORDER BY timestamp DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        if last_row:
            if actual_equity <= 0 or abs(last_row["equity_krw"] - actual_equity) / actual_equity < 0.20:
                return
        else:
            pass  # no snapshots — fall through to seed
    # Wipe old/stale snapshots and mark version
    conn.execute("DELETE FROM portfolio_snapshots WHERE user_id=?", (user_id,))
    conn.execute(
        "INSERT OR REPLACE INTO settings VALUES (?,?,?)", (user_id, "equity_snap_ver", "3")
    )

    first_tx = conn.execute(
        "SELECT timestamp FROM transactions WHERE user_id=? ORDER BY timestamp ASC LIMIT 1",
        (user_id,),
    ).fetchone()
    if not first_tx:
        return

    eq = round(actual_equity, 0)
    # Anchor at first-ever transaction time
    conn.execute(
        "INSERT INTO portfolio_snapshots (user_id, timestamp, equity_krw) VALUES (?,?,?)",
        (user_id, first_tx["timestamp"], eq),
    )
    # Anchor at now (will be followed by live snapshots from _record_snapshot)
    conn.execute(
        "INSERT INTO portfolio_snapshots (user_id, timestamp, equity_krw) VALUES (?,?,?)",
        (user_id, datetime.now().isoformat(), eq),
    )


@app.get("/api/analytics/equity")
def api_analytics_equity(user_id: int = Depends(require_auth)):
    with get_db() as conn:
        _backfill_snapshots(user_id, conn)
        rows = conn.execute(
            "SELECT timestamp, equity_krw FROM portfolio_snapshots WHERE user_id = ? ORDER BY timestamp",
            (user_id,),
        ).fetchall()
        # Always compute live equity so the chart's right edge reflects current prices
        try:
            fx = get_exchange_rate()["usd_to_krw"]
        except Exception:
            fx = 1380.0
        live_equity = round(_compute_actual_equity(user_id, conn, fx), 0)
    if not rows:
        return {"snapshots": [], "mdd_pct": 0, "mdd_krw": 0,
                "initial_equity": 0, "current_equity": 0, "peak_equity": 0, "return_pct": 0}
    snapshots = [{"time": r["timestamp"], "value": r["equity_krw"]} for r in rows]
    # Append live current equity as the final point (not stored in DB, computed fresh each call)
    snapshots.append({"time": datetime.now().isoformat(), "value": live_equity})
    peak = snapshots[0]["value"]
    max_dd_pct = 0.0
    max_dd_krw = 0.0
    for s in snapshots:
        if s["value"] > peak:
            peak = s["value"]
        dd_krw = peak - s["value"]
        dd_pct = dd_krw / peak * 100 if peak > 0 else 0
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct
            max_dd_krw = dd_krw
    initial    = snapshots[0]["value"]
    current    = live_equity  # always live, not last stored snapshot
    peak_eq    = max(s["value"] for s in snapshots)
    return_pct = (current - initial) / initial * 100 if initial > 0 else 0
    return {
        "snapshots":      snapshots,
        "mdd_pct":        round(max_dd_pct, 2),
        "mdd_krw":        round(max_dd_krw, 0),
        "initial_equity": round(initial, 0),
        "current_equity": round(current, 0),
        "peak_equity":    round(peak_eq, 0),
        "return_pct":     round(return_pct, 2),
        "equity_note":    "레버리지 차입금 제외, 실제 자기자본 기준",
    }


@app.get("/api/analytics")
async def api_analytics(period: str = "all", user_id: int = Depends(require_auth)):
    """Return FIFO-matched P&L analytics for closed trades."""
    now = datetime.now()
    period_map = {
        "1w":  now - timedelta(weeks=1),
        "1m":  now - timedelta(days=30),
        "3m":  now - timedelta(days=90),
        "6m":  now - timedelta(days=180),
        "1y":  now - timedelta(days=365),
        "all": None,
    }
    if period not in period_map:
        raise HTTPException(400, "invalid period")
    since_dt = period_map[period]

    with get_db() as conn:
        # Fetch ALL position transactions in chronological order (FIFO needs opens too)
        rows = conn.execute(
            """SELECT symbol, market, action, quantity, price, total, currency, timestamp,
                      fee_amount, tax_amount, slippage_amount, sec_fee, position_type
               FROM transactions
               WHERE user_id = ? AND action IN ('buy','sell','short_open','short_close','lev_open','lev_close','lev_liquidated')
               ORDER BY timestamp ASC""",
            (user_id,),
        ).fetchall()
        # Fetch short positions to get actual margin_amount (handles leveraged shorts)
        short_pos_rows = conn.execute(
            "SELECT ticker, leverage, margin_amount FROM short_positions WHERE user_id = ? ORDER BY opened_at ASC",
            (user_id,),
        ).fetchall()

    # Build per-ticker queues of short position margin data (FIFO order by opened_at)
    _short_margin_queues: dict[str, list] = {}
    for sp in short_pos_rows:
        ticker = sp["ticker"]
        if ticker not in _short_margin_queues:
            _short_margin_queues[ticker] = []
        lev = float(sp["leverage"] or 1.0)
        margin = float(sp["margin_amount"] or 0.0)
        _short_margin_queues[ticker].append({"leverage": lev, "margin": margin})

    fx_rate = get_exchange_rate()["rate"]  # KRW per 1 USD

    # FIFO queues per (symbol, position_type)
    open_queues: dict[tuple, list] = {}
    closed_trades: list[dict] = []

    for r in rows:
        sym = r["symbol"]
        ptype = r["position_type"] or "LONG"
        act = r["action"]
        qty = float(r["quantity"])
        total = float(r["total"])
        currency = r["currency"]
        ts = r["timestamp"]
        market = r["market"]

        key = (sym, ptype)

        if act in ("buy", "lev_open"):
            # LONG / LEVERAGE open: cost basis = total per share (includes margin + open fee)
            cost_per_share = total / qty if qty else 0
            if key not in open_queues:
                open_queues[key] = []
            open_queues[key].append({"qty": qty, "cost_per_share": cost_per_share,
                                     "currency": currency, "market": market})

        elif act == "short_open":
            # Look up actual margin from short_positions (handles leveraged shorts)
            sp_queue = _short_margin_queues.get(sym, [])
            if sp_queue:
                sp_info = sp_queue.pop(0)
                actual_margin = sp_info["margin"] if sp_info["margin"] > 0 else 1.5 * total
            else:
                actual_margin = 1.5 * total  # fallback for old records
            cost_per_share = actual_margin / qty if qty else 0
            if key not in open_queues:
                open_queues[key] = []
            open_queues[key].append({"qty": qty, "cost_per_share": cost_per_share,
                                     "currency": currency, "market": market})

        elif act in ("sell", "short_close", "lev_close", "lev_liquidated"):
            queue = open_queues.get(key, [])
            remaining_sell_qty = qty
            total_cost_basis = 0.0

            while remaining_sell_qty > 0 and queue:
                lot = queue[0]
                if lot["qty"] <= remaining_sell_qty:
                    total_cost_basis += lot["qty"] * lot["cost_per_share"]
                    remaining_sell_qty -= lot["qty"]
                    queue.pop(0)
                else:
                    total_cost_basis += remaining_sell_qty * lot["cost_per_share"]
                    lot["qty"] -= remaining_sell_qty
                    remaining_sell_qty = 0

            # proceeds = total (already net of fees/tax for sell; net_proceeds for lev; returned for short)
            proceeds = total
            pl = proceeds - total_cost_basis  # P&L in original currency

            # Determine category
            if ptype == "SHORT":
                category = "short"
            elif ptype == "LEVERAGE":
                category = "leverage"
            elif market == "KR":
                category = "KRX"
            elif market == "US":
                category = "US"
            else:
                category = "crypto"

            # Convert P&L to KRW and USD for aggregation
            pl_krw = pl if currency == "KRW" else pl * fx_rate
            pl_usd = pl if currency == "USD" else pl / fx_rate

            closed_trades.append({
                "symbol": sym,
                "market": market,
                "category": category,
                "currency": currency,
                "qty": qty,
                "pl": pl,
                "pl_krw": pl_krw,
                "pl_usd": pl_usd,
                "timestamp": ts,
            })

    # Apply period filter to closed trades only
    if since_dt:
        closed_trades = [t for t in closed_trades
                         if t["timestamp"] >= since_dt.isoformat()]

    total_profit_krw = sum(t["pl_krw"] for t in closed_trades if t["pl_krw"] > 0)
    total_loss_krw   = sum(t["pl_krw"] for t in closed_trades if t["pl_krw"] < 0)
    net_krw          = total_profit_krw + total_loss_krw
    total_profit_usd = sum(t["pl_usd"] for t in closed_trades if t["pl_usd"] > 0)
    total_loss_usd   = sum(t["pl_usd"] for t in closed_trades if t["pl_usd"] < 0)
    net_usd          = total_profit_usd + total_loss_usd
    wins             = sum(1 for t in closed_trades if t["pl_krw"] > 0)
    total_trades     = len(closed_trades)
    win_rate         = round(wins / total_trades * 100, 1) if total_trades else 0

    # Sort winners (highest P&L) and losers (lowest P&L)
    winners = sorted([t for t in closed_trades if t["pl_krw"] > 0],
                     key=lambda x: x["pl_krw"], reverse=True)[:20]
    losers  = sorted([t for t in closed_trades if t["pl_krw"] < 0],
                     key=lambda x: x["pl_krw"])[:20]

    # Category breakdown
    categories = {}
    for t in closed_trades:
        cat = t["category"]
        if cat not in categories:
            categories[cat] = {"trades": 0, "wins": 0, "pl_krw": 0.0, "pl_usd": 0.0}
        categories[cat]["trades"] += 1
        categories[cat]["pl_krw"] += t["pl_krw"]
        categories[cat]["pl_usd"] += t["pl_usd"]
        if t["pl_krw"] > 0:
            categories[cat]["wins"] += 1

    by_category = []
    for cat, data in categories.items():
        wr = round(data["wins"] / data["trades"] * 100, 1) if data["trades"] else 0
        by_category.append({
            "category": cat,
            "trades":   data["trades"],
            "win_rate": wr,
            "pl_krw":   round(data["pl_krw"], 0),
            "pl_usd":   round(data["pl_usd"], 2),
        })

    def fmt_trade(t):
        return {
            "symbol":    t["symbol"],
            "category":  t["category"],
            "currency":  t["currency"],
            "pl":        round(t["pl"], 2),
            "pl_krw":    round(t["pl_krw"], 0),
            "pl_usd":    round(t["pl_usd"], 2),
            "timestamp": t["timestamp"],
        }

    return {
        "summary": {
            "total_profit_krw": round(total_profit_krw, 0),
            "total_loss_krw":   round(total_loss_krw, 0),
            "net_krw":          round(net_krw, 0),
            "total_profit_usd": round(total_profit_usd, 2),
            "total_loss_usd":   round(total_loss_usd, 2),
            "net_usd":          round(net_usd, 2),
            "win_rate":         win_rate,
            "total_trades":     total_trades,
        },
        "winners":     [fmt_trade(t) for t in winners],
        "losers":      [fmt_trade(t) for t in losers],
        "by_category": by_category,
    }


# ─────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
