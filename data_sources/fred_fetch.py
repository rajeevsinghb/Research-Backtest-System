"""
data_sources/fred_fetch.py
------------------------------
Data source: official US macroeconomic data from FRED (Federal Reserve
Economic Data) via the fredapi library. Use this for the OFFICIAL US 10Y
Treasury Yield and other Fed-published series — yfinance's ^TNX is only
an approximation, this is the authoritative source.

IMPORTANT — requires a free API key:
  1. Register at https://fred.stlouisfed.org/docs/api/api_key.html (free, instant)
  2. Set it as an environment variable FRED_API_KEY (recommended), or pass
     it directly in params as "api_key" (not recommended to hardcode/commit).

  For GitHub Actions: add FRED_API_KEY as a repository secret, then in the
  workflow yml add:
      env:
        FRED_API_KEY: ${{ secrets.FRED_API_KEY }}

Registered name: "fred_fetch"

params expected:
    series_id     : str   FRED series code, e.g.:
                           "DGS10"  -> US 10-Year Treasury Yield (daily)
                           "DGS2"   -> US 2-Year Treasury Yield
                           "CPIAUCSL" -> US CPI (inflation, monthly)
                           "FEDFUNDS" -> Federal Funds Rate
    since_date    : str   "YYYY-MM-DD"
    until_date    : str   "YYYY-MM-DD" (optional, default = today)
    cache_path    : str   e.g. "data/macro/raw/US10Y_official.parquet"
    force_refresh : bool  (optional, default False)
    update_latest : bool  (optional, default False)
    api_key       : str   (optional — only if you don't want to use the
                           FRED_API_KEY environment variable)

Output columns standardized to: timestamp, open, high, low, close, volume
(FRED series are single daily values, not OHLCV candles — open/high/low
mirror close, volume is 0 — same convention as pycoingecko_fetch.py, so it
fits the same indicator/scenario pipeline without special-casing).
"""

import os
import pandas as pd
from fredapi import Fred

from core.registry import register_data_source


def _get_client(params: dict) -> Fred:
    api_key = params.get("api_key") or os.environ.get("FRED_API_KEY")
    if not api_key:
        raise ValueError(
            "FRED API key not found. Set the FRED_API_KEY environment variable "
            "(or a GitHub Actions secret of the same name), or pass api_key in params."
        )
    return Fred(api_key=api_key)


def _fetch_range(fred_client, series_id, since_date, until_date):
    series = fred_client.get_series(series_id, observation_start=since_date, observation_end=until_date)
    df = series.reset_index()
    df.columns = ["timestamp", "close"]
    df = df.dropna(subset=["close"])
    df["open"] = df["close"]
    df["high"] = df["close"]
    df["low"] = df["close"]
    df["volume"] = 0
    return df[["timestamp", "open", "high", "low", "close", "volume"]].sort_values("timestamp").reset_index(drop=True)


@register_data_source("fred_fetch")
def get_data(params: dict) -> pd.DataFrame:
    cache_path = params.get("cache_path")
    force_refresh = params.get("force_refresh", False)
    update_latest = params.get("update_latest", False)

    series_id = params["series_id"]
    since_date = params["since_date"]
    until_date = params.get("until_date")

    fred_client = _get_client(params)

    if not cache_path:
        print(f"[no cache] Fetching FRED series '{series_id}' ...")
        return _fetch_range(fred_client, series_id, since_date, until_date)

    cache_exists = os.path.exists(cache_path)

    if force_refresh:
        print(f"[force_refresh] Re-fetching FULL range for FRED series '{series_id}' ...")
        df = _fetch_range(fred_client, series_id, since_date, until_date)
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_parquet(cache_path, index=False)
        return df

    if update_latest:
        if cache_exists:
            existing_df = pd.read_parquet(cache_path)
            last_ts = existing_df["timestamp"].max()
            new_since = last_ts.strftime("%Y-%m-%d")
            print(f"[update_latest] Fetching FRED '{series_id}' new data since {new_since} ...")
            new_df = _fetch_range(fred_client, series_id, new_since, until_date)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
        else:
            print(f"[update_latest] No existing cache for '{series_id}'. Doing full initial fetch ...")
            combined = _fetch_range(fred_client, series_id, since_date, until_date)
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        combined.to_parquet(cache_path, index=False)
        return combined

    if cache_exists:
        print(f"[cache hit] Loading existing file: {cache_path}")
        return pd.read_parquet(cache_path)

    print(f"[cache miss] Fetching FRED series '{series_id}' ...")
    df = _fetch_range(fred_client, series_id, since_date, until_date)
    if not df.empty:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_parquet(cache_path, index=False)
    return df
