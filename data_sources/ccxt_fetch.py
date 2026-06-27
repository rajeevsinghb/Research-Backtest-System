"""
data_sources/ccxt_fetch.py
----------------------------
Data source: fetch OHLCV from a crypto exchange via CCXT, with built-in
Parquet caching so you never re-fetch the same historical data twice.

Registered name: "ccxt_fetch"

params expected:
    exchange      : str   e.g. "okx"
    symbol        : str   e.g. "BTC/USDT"
    timeframe     : str   e.g. "15m"
    since_date    : str   ISO8601, e.g. "2021-01-01T00:00:00Z"  (used only on first fetch)
    until_date    : str or None
    limit         : int   (optional, default 1000)
    cache_path    : str   e.g. "data/crypto/raw/BTCUSDT_15m_okx.parquet"
                          If provided, caching behavior kicks in (see modes below).

CACHING MODES (controlled by params, default = normal cache-read):
    1) Default (no extra flag):
         - If cache_path file exists -> load it directly, NO network call.
         - If it doesn't exist -> fetch fresh from since_date, then save it.

    2) update_latest = True:
         - Loads existing cached file (if any), finds its last timestamp,
           fetches ONLY the new candles from that point to now, and
           appends them to the cached file. Cheap, fast, for daily refresh.

    3) force_refresh = True:
         - Ignores any existing cache. Re-fetches the FULL range from
           since_date to until_date/now and overwrites the cache file.
           Use rarely (e.g. if you suspect the cached data is corrupted).
"""

import os
import asyncio
import pandas as pd
import ccxt.async_support as ccxt_async

from core.registry import register_data_source


async def _fetch_range(exchange_name, symbol, timeframe, since_date, until_date, limit):
    exchange_class = getattr(ccxt_async, exchange_name)
    exchange = exchange_class({"enableRateLimit": True})

    try:
        since_ts = exchange.parse8601(since_date) if isinstance(since_date, str) else since_date
        until_ts = exchange.parse8601(until_date) if isinstance(until_date, str) else (
            until_date if until_date else exchange.milliseconds()
        )

        all_candles = []
        cursor = since_ts

        while cursor < until_ts:
            batch = await exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=limit)
            if not batch:
                break
            all_candles += batch
            last_ts = batch[-1][0]
            if last_ts <= cursor:
                break
            cursor = last_ts + 1
            await asyncio.sleep(exchange.rateLimit / 1000)

        if not all_candles:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
        if until_date:
            df = df[df["timestamp"] <= pd.to_datetime(until_ts, unit="ms")]
        return df
    finally:
        await exchange.close()


@register_data_source("ccxt_fetch")
def get_data(params: dict) -> pd.DataFrame:
    cache_path = params.get("cache_path")
    force_refresh = params.get("force_refresh", False)
    update_latest = params.get("update_latest", False)

    exchange = params["exchange"]
    symbol = params["symbol"]
    timeframe = params["timeframe"]
    since_date = params["since_date"]
    until_date = params.get("until_date")
    limit = params.get("limit", 1000)

    # ---- No cache_path given: always fetch fresh, no saving ----
    if not cache_path:
        print(f"[no cache] Fetching fresh data from {exchange} ({symbol}, {timeframe}) ...")
        return asyncio.run(_fetch_range(exchange, symbol, timeframe, since_date, until_date, limit))

    cache_exists = os.path.exists(cache_path)

    # ---- Mode: force_refresh -> full re-fetch, overwrite cache ----
    if force_refresh:
        print(f"[force_refresh] Re-fetching FULL range for {symbol} from {exchange} ...")
        df = asyncio.run(_fetch_range(exchange, symbol, timeframe, since_date, until_date, limit))
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_parquet(cache_path, index=False)
        print(f"[force_refresh] Overwrote cache -> {cache_path} ({len(df):,} rows)")
        return df

    # ---- Mode: update_latest -> append only new candles since last cached timestamp ----
    if update_latest:
        if cache_exists:
            existing_df = pd.read_parquet(cache_path)
            last_ts = existing_df["timestamp"].max()
            new_since = last_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"[update_latest] Existing cache found. Fetching new candles since {new_since} ...")
            new_df = asyncio.run(_fetch_range(exchange, symbol, timeframe, new_since, until_date, limit))
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
        else:
            print(f"[update_latest] No existing cache. Doing full initial fetch ...")
            combined = asyncio.run(_fetch_range(exchange, symbol, timeframe, since_date, until_date, limit))

        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        combined.to_parquet(cache_path, index=False)
        print(f"[update_latest] Cache updated -> {cache_path} ({len(combined):,} total rows)")
        return combined

    # ---- Default mode: cache-read if exists, else fetch once and save ----
    if cache_exists:
        print(f"[cache hit] Loading existing file: {cache_path}")
        return pd.read_parquet(cache_path)

    print(f"[cache miss] Fetching fresh data from {exchange} ({symbol}, {timeframe}) ...")
    df = asyncio.run(_fetch_range(exchange, symbol, timeframe, since_date, until_date, limit))
    if not df.empty:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_parquet(cache_path, index=False)
        print(f"[cached] Saved -> {cache_path} ({len(df):,} rows)")
    return df
