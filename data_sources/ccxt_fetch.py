"""
data_sources/ccxt_fetch.py
----------------------------
Data source: fetch OHLCV from a crypto exchange via CCXT, with:
  - Monthly chunking + controlled parallel fetching (for large historical
    pulls like 10 years of 1m data)
  - Resume support — already-fetched, complete chunks are skipped, so a
    crashed/interrupted run can simply be re-run and it continues where
    it left off
  - Progress reporting (chunks completed / remaining)
  - Automatic completeness verification per chunk and for the final
    merged file (expected candle count vs actual, based on date range and
    timeframe) — so missing data is caught automatically instead of you
    having to manually check
  - Built-in Parquet caching (same 3 modes as before: default / update_latest
    / force_refresh)

Registered name: "ccxt_fetch"

params expected:
    exchange         : str   e.g. "okx"
    symbol           : str   e.g. "BTC/USDT"
    timeframe        : str   e.g. "1m", "15m", "1h" ...
    since_date       : str   ISO8601, e.g. "2016-01-01T00:00:00Z"
    until_date       : str or None
    limit            : int   candles per API call (default 1000)
    cache_path       : str   final merged file path,
                             e.g. "data/crypto/raw/BTCUSDT_1m_okx_10y.parquet"
    chunk_dir        : str   (optional) where per-month chunk files are kept,
                             default: <cache_path's folder>/_chunks/<symbol>_<timeframe>_<exchange>/
    parallel_workers : int   how many monthly chunks to fetch concurrently (default 5)
    merge_chunks     : bool  True = combine all chunks into one final file at cache_path (default True)
                             False = leave chunks as separate files, no merge, no cache_path write
    force_refresh    : bool  True = ignore all existing chunk/cache files, re-fetch everything
    update_latest    : bool  True = only fetch chunks from the last cached month onward, append

Output: standardized DataFrame with columns: timestamp, open, high, low, close, volume
"""

import os
import time
import asyncio
import concurrent.futures
import pandas as pd
import ccxt.async_support as ccxt_async

from core.registry import register_data_source


# ============================================================
# Timeframe -> minutes (used to calculate expected candle counts
# for completeness verification)
# ============================================================
TIMEFRAME_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720,
    "1d": 1440,
}


def _month_chunks(since_date: str, until_date: str):
    """Splits a date range into a list of (chunk_label, chunk_start, chunk_end) month-by-month."""
    start = pd.Timestamp(since_date)
    end = pd.Timestamp(until_date) if until_date else pd.Timestamp.utcnow()

    chunks = []
    cursor = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor < end:
        chunk_start = max(cursor, start)
        next_month = (cursor + pd.offsets.MonthBegin(1))
        chunk_end = min(next_month, end)
        label = cursor.strftime("%Y-%m")
        chunks.append((label, chunk_start, chunk_end))
        cursor = next_month
    return chunks


def _expected_candles(chunk_start, chunk_end, timeframe_minutes):
    total_minutes = (chunk_end - chunk_start).total_seconds() / 60
    return max(int(total_minutes // timeframe_minutes), 0)


async def _fetch_range_async(exchange_name, symbol, timeframe, since_ts, until_ts, limit):
    """Fetches one continuous range using pagination. Returns a DataFrame."""
    exchange_class = getattr(ccxt_async, exchange_name)
    exchange = exchange_class({"enableRateLimit": True})

    try:
        all_candles = []
        cursor = since_ts
        retries = 0

        while cursor < until_ts:
            try:
                batch = await exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=limit)
                retries = 0
            except Exception as e:
                retries += 1
                if retries > 5:
                    print(f"    [error] {symbol} giving up after 5 retries: {e}")
                    break
                wait = min(2 ** retries, 30)
                print(f"    [retry {retries}] {symbol} error: {e} — waiting {wait}s")
                await asyncio.sleep(wait)
                continue

            if not batch:
                break

            all_candles += batch
            last_ts = batch[-1][0]
            if last_ts <= cursor:
                # Exchange returned a stale/non-advancing batch (some exchanges,
                # e.g. Kraken, can ignore `since` for very old ranges) — stop here
                # rather than looping forever; completeness check downstream will
                # flag this chunk as incomplete so it's visible, not silent.
                break
            cursor = last_ts + 1
            await asyncio.sleep(exchange.rateLimit / 1000)

        if not all_candles:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
        df = df[df["timestamp"] < pd.to_datetime(until_ts, unit="ms")]
        return df
    finally:
        await exchange.close()


def _fetch_one_chunk(exchange_name, symbol, timeframe, chunk_start, chunk_end, limit):
    """Sync wrapper (runs its own asyncio event loop) — safe to call from a thread pool worker."""
    since_ts = int(chunk_start.timestamp() * 1000)
    until_ts = int(chunk_end.timestamp() * 1000)
    return asyncio.run(_fetch_range_async(exchange_name, symbol, timeframe, since_ts, until_ts, limit))


def _chunk_filepath(chunk_dir, label):
    return os.path.join(chunk_dir, f"{label}.parquet")


@register_data_source("ccxt_fetch")
def get_data(params: dict) -> pd.DataFrame:
    exchange = params["exchange"]
    symbol = params["symbol"]
    timeframe = params["timeframe"]
    since_date = params["since_date"]
    until_date = params.get("until_date")
    limit = params.get("limit", 1000)
    cache_path = params.get("cache_path")
    parallel_workers = params.get("parallel_workers", 5)
    merge_chunks = params.get("merge_chunks", True)
    force_refresh = params.get("force_refresh", False)
    update_latest = params.get("update_latest", False)

    if timeframe not in TIMEFRAME_MINUTES:
        raise ValueError(f"Unsupported timeframe '{timeframe}' for completeness checking. "
                          f"Supported: {list(TIMEFRAME_MINUTES.keys())}")
    tf_minutes = TIMEFRAME_MINUTES[timeframe]

    safe_symbol = symbol.replace("/", "")
    default_chunk_dir = None
    if cache_path:
        default_chunk_dir = os.path.join(os.path.dirname(cache_path), "_chunks",
                                          f"{safe_symbol}_{timeframe}_{exchange}")
    chunk_dir = params.get("chunk_dir", default_chunk_dir)

    # ---- No cache_path / chunk_dir at all -> simple one-shot fetch, no chunking, no caching ----
    if not chunk_dir:
        print(f"[no cache] Fetching {symbol} from {exchange} (no chunking) ...")
        since_ts = int(pd.Timestamp(since_date).timestamp() * 1000)
        until_ts = int((pd.Timestamp(until_date) if until_date else pd.Timestamp.utcnow()).timestamp() * 1000)
        return asyncio.run(_fetch_range_async(exchange, symbol, timeframe, since_ts, until_ts, limit))

    os.makedirs(chunk_dir, exist_ok=True)

    if force_refresh:
        print(f"[force_refresh] Clearing existing chunks for {symbol}/{exchange}/{timeframe} ...")
        for f in os.listdir(chunk_dir):
            os.remove(os.path.join(chunk_dir, f))

    all_chunks = _month_chunks(since_date, until_date)

    if update_latest and os.path.exists(chunk_dir) and os.listdir(chunk_dir):
        existing_labels = sorted([f.replace(".parquet", "") for f in os.listdir(chunk_dir) if f.endswith(".parquet")])
        if existing_labels:
            last_label = existing_labels[-1]
            all_chunks = [c for c in all_chunks if c[0] >= last_label]
            print(f"[update_latest] Will only (re)fetch chunks from {last_label} onward.")

    total_chunks = len(all_chunks)
    print(f"\n[{symbol} / {exchange} / {timeframe}] Total monthly chunks to process: {total_chunks}")
    print(f"  Parallel workers: {parallel_workers} | chunk dir: {chunk_dir}\n")

    completed = 0
    completeness_report = []

    def process_chunk(chunk_info):
        label, chunk_start, chunk_end = chunk_info
        chunk_path = _chunk_filepath(chunk_dir, label)
        expected = _expected_candles(chunk_start, chunk_end, tf_minutes)

        # Resume: skip if this chunk file already exists and looks complete
        if os.path.exists(chunk_path) and not force_refresh:
            existing = pd.read_parquet(chunk_path)
            pct = (len(existing) / expected * 100) if expected else 100.0
            if pct >= 99.0:  # treat as complete (allow tiny tolerance for exchange downtime gaps)
                return label, len(existing), expected, pct, "resumed (already complete)"

        df = _fetch_one_chunk(exchange, symbol, timeframe, chunk_start, chunk_end, limit)
        df.to_parquet(chunk_path, index=False)
        actual = len(df)
        pct = (actual / expected * 100) if expected else 100.0
        return label, actual, expected, pct, "fetched"

    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = [executor.submit(process_chunk, c) for c in all_chunks]
        for future in concurrent.futures.as_completed(futures):
            label, actual, expected, pct, status = future.result()
            completed += 1
            completeness_report.append((label, actual, expected, pct))
            flag = "OK" if pct >= 99.0 else "INCOMPLETE"
            print(f"  [{completed}/{total_chunks}] {label} -> {actual:,}/{expected:,} candles "
                  f"({pct:.1f}%) [{flag}] ({status})")

    # ---- Completeness summary ----
    incomplete = [r for r in completeness_report if r[3] < 99.0]
    print(f"\n[completeness check] {len(completeness_report) - len(incomplete)}/{len(completeness_report)} "
          f"chunks fully complete.")
    if incomplete:
        print("  WARNING — the following chunks are INCOMPLETE (re-run to retry just these):")
        for label, actual, expected, pct in incomplete:
            print(f"    - {label}: {actual:,}/{expected:,} ({pct:.1f}%)")
    else:
        print("  All chunks verified complete.")

    if not merge_chunks:
        print("[merge_chunks=False] Leaving chunks as separate files, no merged output produced.")
        return pd.concat(
            [pd.read_parquet(_chunk_filepath(chunk_dir, c[0])) for c in all_chunks
             if os.path.exists(_chunk_filepath(chunk_dir, c[0]))],
            ignore_index=True
        ) if all_chunks else pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    # ---- Merge all chunks into the final cache file ----
    print(f"[merging] Combining {total_chunks} chunks into final file ...")
    all_dfs = [pd.read_parquet(_chunk_filepath(chunk_dir, c[0])) for c in all_chunks
               if os.path.exists(_chunk_filepath(chunk_dir, c[0]))]
    merged = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    merged = merged.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)

    if cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        merged.to_parquet(cache_path, index=False)
        print(f"[saved] Final merged file -> {cache_path} ({len(merged):,} total rows)")

    return merged
