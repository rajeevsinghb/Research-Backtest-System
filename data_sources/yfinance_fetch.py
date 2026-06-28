"""
data_sources/yfinance_fetch.py
---------------------------------
Data source: fetch historical OHLCV from Yahoo Finance via yfinance.
Covers indices (Nasdaq, S&P500, Nifty, Bank Nifty), commodities (Gold,
Silver, Copper, Oil), forex (EUR/USD, USD/INR, etc.), volatility (VIX,
India VIX), DXY, approx bond yields, and most listed stocks/ETFs.

Registered name: "yfinance_fetch"

Same caching behavior as ccxt_fetch.py (default / update_latest / force_refresh)
so the rest of the system (indicators, scenarios, main.py) doesn't need to
know or care which library actually fetched the data.

params expected:
    ticker        : str   Yahoo Finance ticker symbol, e.g.:
                           "^NSEI" (Nifty 50), "^NSEBANK" (Bank Nifty),
                           "GC=F" (Gold), "SI=F" (Silver), "HG=F" (Copper),
                           "CL=F" (Crude Oil), "DX-Y.NYB" (DXY),
                           "^VIX" (VIX), "^INDIAVIX" (India VIX),
                           "EURUSD=X", "USDJPY=X", "USDCHF=X", "USDINR=X",
                           "^TNX" (US 10Y yield, approx — see fred source
                           for the official figure), "^GSPC" (S&P500),
                           "^IXIC" (Nasdaq)
    interval      : str   yfinance interval, e.g. "1d", "1h", "15m"
                           (note: yfinance limits how far back intraday
                           intervals go — 1m/5m typically only ~60 days)
    since_date    : str   "YYYY-MM-DD"
    until_date    : str   "YYYY-MM-DD" (optional, default = today)
    cache_path    : str   e.g. "data/macro/raw/NIFTY50_1d.parquet"
    force_refresh : bool  (optional, default False)
    update_latest : bool  (optional, default False)
"""

import os
import pandas as pd
import yfinance as yf

from core.registry import register_data_source


def _fetch_range(ticker, interval, since_date, until_date):
    df = yf.download(ticker, start=since_date, end=until_date, interval=interval,
                      auto_adjust=False, progress=False)

    if df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    # yfinance sometimes returns multi-index columns — flatten if needed
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = df.reset_index()
    df = df.rename(columns={
        "Date": "timestamp", "Datetime": "timestamp",
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Volume": "volume",
    })
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


@register_data_source("yfinance_fetch")
def get_data(params: dict) -> pd.DataFrame:
    cache_path = params.get("cache_path")
    force_refresh = params.get("force_refresh", False)
    update_latest = params.get("update_latest", False)

    ticker = params["ticker"]
    interval = params.get("interval", "1d")
    since_date = params["since_date"]
    until_date = params.get("until_date")

    if not cache_path:
        print(f"[no cache] Fetching {ticker} from Yahoo Finance ...")
        return _fetch_range(ticker, interval, since_date, until_date)

    cache_exists = os.path.exists(cache_path)

    if force_refresh:
        print(f"[force_refresh] Re-fetching FULL range for {ticker} ...")
        df = _fetch_range(ticker, interval, since_date, until_date)
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_parquet(cache_path, index=False)
        return df

    if update_latest:
        if cache_exists:
            existing_df = pd.read_parquet(cache_path)
            last_ts = existing_df["timestamp"].max()
            new_since = last_ts.strftime("%Y-%m-%d")
            print(f"[update_latest] Fetching {ticker} new candles since {new_since} ...")
            new_df = _fetch_range(ticker, interval, new_since, until_date)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
        else:
            print(f"[update_latest] No existing cache for {ticker}. Doing full initial fetch ...")
            combined = _fetch_range(ticker, interval, since_date, until_date)
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        combined.to_parquet(cache_path, index=False)
        return combined

    if cache_exists:
        print(f"[cache hit] Loading existing file: {cache_path}")
        return pd.read_parquet(cache_path)

    print(f"[cache miss] Fetching {ticker} from Yahoo Finance ...")
    df = _fetch_range(ticker, interval, since_date, until_date)
    if not df.empty:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_parquet(cache_path, index=False)
    return df
