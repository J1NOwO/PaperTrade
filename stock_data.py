import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pytz
import yfinance as yf
from pykrx import stock as krx_stock

KST = pytz.timezone("Asia/Seoul")
EST = pytz.timezone("America/New_York")

# ──────────────────────────────────────────────────────────
# Market status
# ──────────────────────────────────────────────────────────

def is_korean_market_open() -> bool:
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    open_t  = now.replace(hour=9,  minute=0,  second=0, microsecond=0)
    close_t = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_t <= now <= close_t


def is_us_market_open() -> bool:
    now = datetime.now(EST)
    if now.weekday() >= 5:
        return False
    open_t  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_t = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return open_t <= now <= close_t


# ──────────────────────────────────────────────────────────
# Market auto-detection
# ──────────────────────────────────────────────────────────

_CRYPTO_SYMBOLS = {
    "BTC","ETH","SOL","DOGE","ADA","XRP","MATIC","AVAX",
    "LINK","DOT","UNI","LTC","BCH","ATOM","BNB","TRX",
    "SHIB","TON","APT","OP","NEAR","FIL","ALGO","MANA",
}

def detect_market(symbol: str) -> str:
    if symbol.isdigit() and len(symbol) == 6:
        return "KR"
    base = symbol.split("-")[0]
    if symbol.endswith("-USD") or base in _CRYPTO_SYMBOLS:
        return "CRYPTO"
    return "US"


# ──────────────────────────────────────────────────────────
# KR name cache
# ──────────────────────────────────────────────────────────

_kr_name_cache: dict[str, str] = {}
_kr_name_lock = threading.Lock()

# KR yfinance suffix cache (.KS / .KQ)
_kr_suffix_cache: dict[str, str] = {}   # ticker → ".KS" | ".KQ" | ""
_kr_suffix_lock  = threading.Lock()

# KR OHLCV result cache  (ticker, days) → (DataFrame | None, fetch_timestamp)
_kr_ohlcv_cache: dict = {}
_KR_CACHE_TTL = 300  # 5 minutes


def _kr_yf_suffix(ticker: str) -> str:
    """Return '.KS' or '.KQ' for the ticker (probes yfinance once, then caches)."""
    with _kr_suffix_lock:
        if ticker in _kr_suffix_cache:
            return _kr_suffix_cache[ticker]
        for sfx in (".KS", ".KQ"):
            try:
                h = yf.Ticker(f"{ticker}{sfx}").history(period="5d")
                if not h.empty:
                    _kr_suffix_cache[ticker] = sfx
                    return sfx
            except Exception:
                pass
        _kr_suffix_cache[ticker] = ""
        return ""


def get_kr_name(ticker: str) -> str:
    with _kr_name_lock:
        if ticker not in _kr_name_cache:
            name = ticker
            try:
                info = yf.Ticker(f"{ticker}.KS").info
                en_name = info.get("longName") or info.get("shortName")
                if en_name:
                    name = en_name
                else:
                    raise ValueError("yfinance returned no name")
            except Exception:
                try:
                    pykrx_name = krx_stock.get_market_ticker_name(ticker)
                    if pykrx_name and pykrx_name != ticker:
                        name = pykrx_name
                except Exception:
                    pass
            _kr_name_cache[ticker] = name
        return _kr_name_cache[ticker]


# US name cache
_us_name_cache: dict[str, str] = {}
_us_name_lock = threading.Lock()


def get_us_name(symbol: str) -> str:
    with _us_name_lock:
        if symbol not in _us_name_cache:
            try:
                info = yf.Ticker(symbol).info
                _us_name_cache[symbol] = (
                    info.get("longName") or info.get("shortName") or symbol
                )
            except Exception:
                _us_name_cache[symbol] = symbol
        return _us_name_cache[symbol]


# ──────────────────────────────────────────────────────────
# KR stock helpers
# ──────────────────────────────────────────────────────────

def _kr_ohlcv(ticker: str, days: int):
    """Fetch KR OHLCV for the last `days` calendar days. pykrx primary, yfinance fallback. Cached 5 min."""
    cache_key = (ticker, days)
    now_ts    = time.time()
    if cache_key in _kr_ohlcv_cache:
        df, ts = _kr_ohlcv_cache[cache_key]
        if now_ts - ts < _KR_CACHE_TTL:
            return df

    now_kst = datetime.now(KST)
    end   = now_kst.strftime("%Y%m%d")
    start = (now_kst - timedelta(days=days + 30)).strftime("%Y%m%d")

    df = None
    try:
        result = krx_stock.get_market_ohlcv_by_date(start, end, ticker)
        if result is not None and not result.empty:
            df = result
    except Exception as e:
        print(f"[KR OHLCV pykrx] {ticker}: {e}")

    # yfinance fallback
    if df is None:
        sfx = _kr_yf_suffix(ticker)
        if sfx:
            try:
                import pandas as pd
                start_dt = (now_kst - timedelta(days=days + 35)).date()
                hist = yf.Ticker(f"{ticker}{sfx}").history(
                    start=start_dt, end=now_kst.date(), interval="1d"
                )
                if not hist.empty:
                    # Normalize index to KST date so strftime("%Y-%m-%d") is correct
                    if hist.index.tz is not None:
                        hist.index = hist.index.tz_convert(KST)
                    hist.index = hist.index.normalize()
                    hist = hist[["Open", "High", "Low", "Close", "Volume"]].copy()
                    hist.columns = ["시가", "고가", "저가", "종가", "거래량"]
                    df = hist
            except Exception as e:
                print(f"[KR OHLCV yfinance] {ticker}: {e}")

    _kr_ohlcv_cache[cache_key] = (df, now_ts)
    return df


def get_kr_stock_info(ticker: str) -> dict | None:
    df = _kr_ohlcv(ticker, days=10)
    if df is None:
        return None

    latest    = df.iloc[-1]
    prev      = df.iloc[-2] if len(df) > 1 else latest
    price     = float(latest["종가"])
    prev_p    = float(prev["종가"])
    change    = price - prev_p
    change_pct = (change / prev_p * 100) if prev_p else 0.0

    return {
        "symbol":      ticker,
        "name":        get_kr_name(ticker),
        "price":       price,
        "change":      change,
        "change_pct":  change_pct,
        "market":      "KR",
        "currency":    "KRW",
        "market_open": is_korean_market_open(),
    }


def get_kr_chart_data(ticker: str, period: str = "3mo") -> dict:
    if period == "max":
        df = _kr_ohlcv(ticker, days=9999)
        chart_type = "candle"
    elif period in ("1d", "5d"):
        days = 8 if period == "1d" else 12
        df = _kr_ohlcv(ticker, days=days)
        chart_type = "line"
    else:
        now_kst = datetime.now(KST)
        if period == "ytd":
            days = (now_kst - now_kst.replace(month=1, day=1)).days + 10
        else:
            day_map = {"1mo": 40, "6mo": 200, "1y": 380, "5y": 1850}
            days = day_map.get(period, 40)
        df = _kr_ohlcv(ticker, days=days)
        chart_type = "candle"

    if df is None:
        return {"chart_type": chart_type, "data": []}

    # Slice to exact number of trading days for intraday-style periods
    if period == "1d":
        df = df.iloc[-1:]
    elif period == "5d":
        df = df.iloc[-5:]

    if chart_type == "line":
        data = [{"time": date.strftime("%Y-%m-%d"), "value": float(row["종가"])}
                for date, row in df.iterrows()]
    else:
        data = [
            {
                "time":  date.strftime("%Y-%m-%d"),
                "open":  float(row["시가"]),
                "high":  float(row["고가"]),
                "low":   float(row["저가"]),
                "close": float(row["종가"]),
            }
            for date, row in df.iterrows()
        ]
    return {"chart_type": chart_type, "data": data}


# ──────────────────────────────────────────────────────────
# US stock helpers
# ──────────────────────────────────────────────────────────

def get_us_stock_info(symbol: str) -> dict | None:
    try:
        hist = yf.Ticker(symbol).history(period="5d")
        if hist.empty:
            return None
        latest_p = float(hist["Close"].iloc[-1])
        prev_p   = float(hist["Close"].iloc[-2]) if len(hist) > 1 else latest_p
        change   = latest_p - prev_p
        change_pct = (change / prev_p * 100) if prev_p else 0.0

        return {
            "symbol":      symbol.upper(),
            "name":        get_us_name(symbol.upper()),
            "price":       latest_p,
            "change":      change,
            "change_pct":  change_pct,
            "market":      "US",
            "currency":    "USD",
            "market_open": is_us_market_open(),
        }
    except Exception as e:
        print(f"[US info] {symbol}: {e}")
        return None


def get_us_chart_data(symbol: str, period: str = "3mo") -> dict:
    try:
        if period == "1d":
            hist = yf.Ticker(symbol).history(period="1d", interval="1m")
            chart_type = "line"
        elif period == "5d":
            hist = yf.Ticker(symbol).history(period="5d", interval="5m")
            chart_type = "line"
        elif period == "ytd":
            year_start = datetime.now().replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            hist = yf.Ticker(symbol).history(start=year_start, interval="1d")
            chart_type = "candle"
        elif period == "5y":
            hist = yf.Ticker(symbol).history(period="5y", interval="1wk")
            chart_type = "candle"
        elif period == "max":
            hist = yf.Ticker(symbol).history(period="max", interval="1wk")
            chart_type = "candle"
        else:
            yf_map = {"1mo": "1mo", "6mo": "6mo", "1y": "1y"}
            hist = yf.Ticker(symbol).history(period=yf_map.get(period, "1mo"), interval="1d")
            chart_type = "candle"

        if hist.empty:
            return {"chart_type": chart_type, "data": []}

        if chart_type == "line":
            data = [{"time": int(dt.timestamp()), "value": float(row["Close"])}
                    for dt, row in hist.iterrows()]
        else:
            data = [
                {
                    "time":  date.strftime("%Y-%m-%d"),
                    "open":  float(row["Open"]),
                    "high":  float(row["High"]),
                    "low":   float(row["Low"]),
                    "close": float(row["Close"]),
                }
                for date, row in hist.iterrows()
            ]
        return {"chart_type": chart_type, "data": data}
    except Exception as e:
        print(f"[US chart] {symbol}: {e}")
        return {"chart_type": "candle", "data": []}


# ──────────────────────────────────────────────────────────
# Unified public API
# ──────────────────────────────────────────────────────────

def _crypto_sym(symbol: str) -> str:
    """Normalize to BTC-USD format."""
    return symbol if symbol.endswith("-USD") else f"{symbol}-USD"


def get_crypto_stock_info(symbol: str) -> dict | None:
    ticker_sym = _crypto_sym(symbol)
    try:
        hist = yf.Ticker(ticker_sym).history(period="2d")
        if hist.empty:
            return None
        latest_p = float(hist["Close"].iloc[-1])
        prev_p   = float(hist["Close"].iloc[-2]) if len(hist) > 1 else latest_p
        change   = latest_p - prev_p
        change_pct = (change / prev_p * 100) if prev_p else 0.0
        return {
            "symbol":      ticker_sym,
            "name":        get_us_name(ticker_sym),
            "price":       latest_p,
            "change":      change,
            "change_pct":  change_pct,
            "market":      "CRYPTO",
            "currency":    "USD",
            "market_open": True,
        }
    except Exception as e:
        print(f"[CRYPTO info] {symbol}: {e}")
        return None


def get_stock_info(symbol: str, market: str = "auto") -> dict | None:
    if market == "auto":
        market = detect_market(symbol)
    if market == "CRYPTO":
        return get_crypto_stock_info(symbol)
    return get_kr_stock_info(symbol) if market == "KR" else get_us_stock_info(symbol)


def get_chart_data(symbol: str, market: str = "auto", period: str = "1mo") -> dict:
    if market == "auto":
        market = detect_market(symbol)
    if market == "CRYPTO":
        return get_us_chart_data(_crypto_sym(symbol), period)
    return get_kr_chart_data(symbol, period) if market == "KR" else get_us_chart_data(symbol, period)


# ──────────────────────────────────────────────────────────
# Stock stats (for detail panel)
# ──────────────────────────────────────────────────────────

def get_stock_stats(symbol: str, market: str = "auto") -> dict:
    if market == "auto":
        market = detect_market(symbol)
    if market == "CRYPTO":
        return _get_us_stats(_crypto_sym(symbol))
    return _get_kr_stats(symbol) if market == "KR" else _get_us_stats(symbol)


def _get_kr_stats(ticker: str) -> dict:
    try:
        df = _kr_ohlcv(ticker, days=380)
        if df is None:
            return {}
        latest = df.iloc[-1]
        result = {
            "open":        float(latest["시가"]),
            "high":        float(latest["고가"]),
            "low":         float(latest["저가"]),
            "week52_high": float(df["고가"].max()),
            "week52_low":  float(df["저가"].min()),
        }
        try:
            now_kst = datetime.now(KST)
            end   = now_kst.strftime("%Y%m%d")
            start = (now_kst - timedelta(days=7)).strftime("%Y%m%d")
            fund = krx_stock.get_market_fundamental_by_date(start, end, ticker)
            if not fund.empty:
                latest_f = fund.iloc[-1]
                if "PER" in fund.columns:
                    result["per"] = float(latest_f["PER"])
            cap_df = krx_stock.get_market_cap_by_date(start, end, ticker)
            if not cap_df.empty and "시가총액" in cap_df.columns:
                result["market_cap"] = float(cap_df["시가총액"].iloc[-1])
        except Exception:
            pass
        return result
    except Exception as e:
        print(f"[KR stats] {ticker}: {e}")
        return {}


def _get_us_stats(symbol: str) -> dict:
    try:
        info = yf.Ticker(symbol).info
        result = {}
        for src_key, dst_key in [
            ("regularMarketOpen",    "open"),
            ("regularMarketDayHigh", "high"),
            ("regularMarketDayLow",  "low"),
            ("marketCap",            "market_cap"),
            ("trailingPE",           "per"),
            ("fiftyTwoWeekHigh",     "week52_high"),
            ("fiftyTwoWeekLow",      "week52_low"),
        ]:
            v = info.get(src_key)
            if v is not None:
                result[dst_key] = v
        return result
    except Exception as e:
        print(f"[US stats] {symbol}: {e}")
        return {}


# ──────────────────────────────────────────────────────────
# Related stocks
# ──────────────────────────────────────────────────────────

_KR_POPULAR     = ["005930","000660","035420","051910","005380","035720","068270","105560","000270","096770"]
_US_POPULAR     = ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V","JNJ"]
_CRYPTO_POPULAR = ["BTC-USD","ETH-USD","SOL-USD","DOGE-USD","XRP-USD","ADA-USD","AVAX-USD","LINK-USD","DOT-USD","BNB-USD"]


def get_related_stocks(symbol: str, market: str = "auto") -> list[dict]:
    if market == "auto":
        market = detect_market(symbol)
    if market == "CRYPTO":
        pool = _CRYPTO_POPULAR
        norm = _crypto_sym(symbol)
    elif market == "KR":
        pool, norm = _KR_POPULAR, symbol
    else:
        pool, norm = _US_POPULAR, symbol
    candidates = [s for s in pool if s != norm][:5]

    def _fetch(s):
        info = get_stock_info(s, market)
        if info:
            return {
                "symbol":     info["symbol"],
                "name":       info["name"],
                "price":      info["price"],
                "change_pct": info["change_pct"],
                "currency":   info["currency"],
            }
        return None

    results_map = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(_fetch, s): s for s in candidates}
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                results_map[futures[fut]] = r

    # preserve pool order
    return [results_map[s] for s in candidates if s in results_map]


# ──────────────────────────────────────────────────────────
# Exchange rate (USD/KRW)
# ──────────────────────────────────────────────────────────

_fx_cache: dict = {"rate": None, "ts": 0.0}


def get_exchange_rate() -> dict:
    """Returns KRW per 1 USD, cached 60 seconds."""
    now = time.time()
    if _fx_cache["rate"] and now - _fx_cache["ts"] < 60:
        rate = _fx_cache["rate"]
    else:
        try:
            hist = yf.Ticker("KRW=X").history(period="2d")
            rate = float(hist["Close"].iloc[-1]) if not hist.empty else 1380.0
        except Exception as e:
            print(f"[FX] {e}")
            rate = _fx_cache["rate"] or 1380.0
        _fx_cache["rate"] = rate
        _fx_cache["ts"] = now

    return {
        "rate":       rate,
        "usd_to_krw": rate,
        "krw_to_usd": round(1.0 / rate, 8),
        "display":    f"1 USD = ₩{rate:,.2f}",
    }


def calc_shares(price: float, market: str, quantity: float | None, amount: float | None):
    """Return (quantity, total) or raise ValueError."""
    if quantity is not None:
        qty = float(quantity)
    elif amount is not None:
        raw = float(amount) / price
        qty = math.floor(raw) if market == "KR" else raw
    else:
        raise ValueError("quantity 또는 amount 중 하나를 입력하세요.")

    if qty <= 0:
        raise ValueError("수량이 0 이하입니다. 금액을 확인하세요.")

    return qty, qty * price
