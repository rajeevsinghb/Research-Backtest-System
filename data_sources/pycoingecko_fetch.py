"""
data_sources/pycoingecko_fetch.py
------------------------------------
Data source: market-breadth data from CoinGecko (free public API, no key
needed). Covers things CCXT/yfinance can't give you: BTC Dominance and
Total Crypto Market Cap (and individual coin price history as a bonus).

Registered name: "pycoingecko_fetch"

Same caching pattern as ccxt_fetch.py / yfinance_fetch.py.

params expected:
    metric        : str   one of:
                           "btc_dominance"        -> % BTC market cap dominance, daily
                           "total_market_cap"     -> total crypto market cap (USD), daily
                           "coin_price"           -> a specific coin's price history
                                                       (requires coin_id, e.g. "bitcoin")
    coin_id       : str   required only when metric == "coin_price", e.g. "bitcoin", "ethereum"
    days          : int   how many days of history to fetch (CoinGecko free tier:
                           max ~365 days per call for daily granularity)
    cache_path    : str   e.g. "data/macro/raw/btc_dominance.parquet"
    force_refresh : bool  (optional, default False)
    update_latest : bool  (optional, default False — for this source, update_latest
                           simply re-fetches the requested `days` window and replaces
                           the cache, since CoinGecko's free global-data endpoints
                           don't support incremental "since" queries cleanly)

Output columns standardized to: timestamp, open, high, low, close, volume
(open/high/low mirror close, volume is 0) so it fits the same indicator/
scenario pipeline as every other source — most metrics here are single
daily values, not real OHLCV candles.
"""

import os
import pandas as pd
from pycoingecko import CoinGeckoAPI

from core.registry import register_data_source

cg = CoinGeckoAPI()


def _standardize(timestamps_ms, values):
    df = pd.DataFrame({"timestamp": pd.to_datetime(timestamps_ms, unit="ms"), "close": values})
    df["open"] = df["close"]
    df["high"] = df["close"]
    df["low"] = df["close"]
    df["volume"] = 0
    return df[["timestamp", "open", "high", "low", "close", "volume"]].sort_values("timestamp").reset_index(drop=True)


def _fetch_metric(metric, coin_id, days):
    if metric == "btc_dominance":
        data = cg.get_global()
        # get_global() only gives the CURRENT snapshot, not history.
        # CoinGecko's free tier has no clean historical dominance endpoint,
        # so we return today's single value. For a real historical series,
        # a paid CoinGecko plan or a different data provider would be needed.
        now_ms = pd.Timestamp.utcnow().value // 10**6
        dominance = data["market_cap_percentage"]["btc"]
        return _standardize([now_ms], [dominance])

    elif metric == "total_market_cap":
        data = cg.get_global_market_cap_chart(days=days)
        points = data.get("market_cap", [])
        timestamps_ms = [p[0] for p in points]
        values = [p[1] for p in points]
        return _standardize(timestamps_ms, values)

    elif metric == "coin_price":
        if not coin_id:
            raise ValueError("coin_id is required when metric == 'coin_price'")
        data = cg.get_coin_market_chart_by_id(id=coin_id, vs_currency="usd", days=days)
        points = data["prices"]
        timestamps_ms = [p[0] for p in points]
        values = [p[1] for p in points]
        return _standardize(timestamps_ms, values)

    else:
        raise ValueError(f"Unknown metric: {metric}")


@register_data_source("pycoingecko_fetch")
def get_data(params: dict) -> pd.DataFrame:
    cache_path = params.get("cache_path")
    force_refresh = params.get("force_refresh", False)
    update_latest = params.get("update_latest", False)

    metric = params["metric"]
    coin_id = params.get("coin_id")
    days = params.get("days", 365)

    if not cache_path:
        print(f"[no cache] Fetching '{metric}' from CoinGecko ...")
        return _fetch_metric(metric, coin_id, days)

    cache_exists = os.path.exists(cache_path)

    if force_refresh or update_latest or not cache_exists:
        mode = "force_refresh" if force_refresh else ("update_latest" if update_latest else "cache_miss")
        print(f"[{mode}] Fetching '{metric}' from CoinGecko ...")
        df = _fetch_metric(metric, coin_id, days)
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_parquet(cache_path, index=False)
        return df

    print(f"[cache hit] Loading existing file: {cache_path}")
    return pd.read_parquet(cache_path)
