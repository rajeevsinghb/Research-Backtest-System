"""
data_sources/ccxt_fetch.py
----------------------------
Data source: fetch OHLCV from a crypto exchange via CCXT, with:
  - Monthly chunking + controlled parallel fetching (for large historical
    pulls like 10 years of 1m data)
  - Resume support — already-complete chunks are skipped on re-run
  - Configurable exponential-backoff retries (tune per exchange — some,
    like Coinbase, need slower/more patient retrying than others)
  - "fill_missing" mode — re-fetches ONLY the chunks that are still
    incomplete, without re-downloading chunks that are already complete
  - Automatic completeness verification (expected candle count vs actual)
    per chunk, with a structured report returned alongside the data so
    main.py can save it to outputs/ for easy viewing (no need to dig
    through GitHub Actions logs)
  - Built-in Parquet caching

Registered name: "ccxt_fetch"

params expected:
    exchange         : str   e.g. "okx"
    symbol           : str   e.g. "BTC/USDT"
    timeframe        : str   e.g. "1m", "15m", "1h" ...
    since_date       : str   ISO8601, e.g. "2016-01-01T00:00:00Z"
    until_date       : str or None
    limit            : int   candles per API call (default 1000)
    cache_path       : str   final merged file path
    chunk_dir        : str   (optional) per-month chunk file location
    parallel_workers : int   how many monthly chunks fetched concurrently (default 5;
                             use 1-2 for strict exchanges like Coinbase)
    merge_chunks     : bool  True = combine all chunks into one final file (default True)
    force_refresh    : bool  True = ignore all existing data, re-fetch everything
    update_latest    : bool  True = only fetch chunks from the last cached month onward
    fill_missing     : bool  True = re-fetch ONLY chunks that are currently incomplete
                             (the ones flagged INCOMPLETE in a previous run), leaving
                             complete chunks untouched. Use this to "top up" a dataset
                             after a rate-limited run, without re-downloading everything.
    retry_count      : int   max retry attempts per failed API call (default 8)
    retry_base_wait  : float seconds to wait before the FIRST retry (default 3)
    retry_max_wait   : float maximum seconds to wait between retries, cap (default 60)
                             Backoff formula: wait = min(retry_base_wait * 2^(attempt-1), retry_max_wait)

Output: standardized DataFrame with columns: timestamp, open, high, low, close, volume

The completeness report (per chunk: expected/actual/pct) is attached to the
returned DataFrame as df.attrs["completeness_report"] (a list of dicts) so
main.py can pick it up and save it without changing this function's return
type.
"""

import os
import asyncio
import concurrent.futures
import pandas as pd
import ccxt.async_support as ccxt_async

from core.registry import register_data_source


TIMEFRAME_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720,
    "1d": 1440,
}


def _month_chunks(since_date: str, until_date: str):
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


async def _fetch_range_async(exchange_name, symbol, timeframe, since_ts, until_ts, limit,
                              retry_count, retry_base_wait, retry_max_wait):
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
                if retries > retry_count:
                    print(f"    [give up] {symbol} after {retry_count} retries: {e}")
                    break
                wait = min(retry_base_wait * (2 ** (retries - 1)), retry_max_wait)
                print(f"    [retry {retries}/{retry_count}] {symbol} error: {e} — waiting {wait:.0f}s")
                await asyncio.sleep(wait)
                continue

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
        df = df[df["timestamp"] < pd.to_datetime(until_ts, unit="ms")]
        return df
    finally:
        await exchange.close()


def _fetch_one_chunk(exchange_name, symbol, timeframe, chunk_start, chunk_end, limit,
                      retry_count, retry_base_wait, retry_max_wait):
    since_ts = int(chunk_start.timestamp() * 1000)
    until_ts = int(chunk_end.timestamp() * 1000)
    return asyncio.run(_fetch_range_async(
        exchange_name, symbol, timeframe, since_ts, until_ts, limit,
        retry_count, retry_base_wait, retry_max_wait
    ))


def _chunk_filepath(chunk_dir, label):
    return os.path.join(chunk_dir, f"{label}.parquet")


def _save_parquet(df: pd.DataFrame, path: str):
    """Save with float32 dtype + zstd compression.
    - float32 vs float64: half the bytes, no meaningful precision loss for price/volume data
    - zstd vs snappy (default): 30-40% smaller, same or faster read/write speed
    Neither change affects downstream pandas operations — dtypes are transparent to
    indicators/scenarios which only care about column names, not internal storage format."""
    df = df.copy()
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = df[col].astype("float32")
    df.to_parquet(path, index=False, compression="zstd")


@register_data_source("ccxt_fetch")
def get_data(params: dict) -> pd.DataFrame:
    exchange = params["exchange"]
    symbol = params["symbol"]
    timeframe = params["timeframe"]
    since_date = params["since_date"]
    until_date = params.get("until_date")
    limit = params.get("limit", 1000)
    cache_path = params.get("cache_path")
    parallel_workers = max(params.get("parallel_workers", 5), 1)  # never allow 0/negative
    merge_chunks = params.get("merge_chunks", True)
    force_refresh = params.get("force_refresh", False)
    update_latest = params.get("update_latest", False)
    fill_missing = params.get("fill_missing", False)
    retry_count = params.get("retry_count", 8)
    retry_base_wait = params.get("retry_base_wait", 3)
    retry_max_wait = params.get("retry_max_wait", 60)

    if timeframe not in TIMEFRAME_MINUTES:
        raise ValueError(f"Unsupported timeframe '{timeframe}'. Supported: {list(TIMEFRAME_MINUTES.keys())}")
    tf_minutes = TIMEFRAME_MINUTES[timeframe]

    safe_symbol = symbol.replace("/", "")
    default_chunk_dir = None
    if cache_path:
        default_chunk_dir = os.path.join(os.path.dirname(cache_path), "_chunks",
                                          f"{safe_symbol}_{timeframe}_{exchange}")
    chunk_dir = params.get("chunk_dir", default_chunk_dir)

    if not chunk_dir:
        print(f"[no cache] Fetching {symbol} from {exchange} (no chunking) ...")
        since_ts = int(pd.Timestamp(since_date).timestamp() * 1000)
        until_ts = int((pd.Timestamp(until_date) if until_date else pd.Timestamp.utcnow()).timestamp() * 1000)
        df = asyncio.run(_fetch_range_async(exchange, symbol, timeframe, since_ts, until_ts, limit,
                                             retry_count, retry_base_wait, retry_max_wait))
        df.attrs["completeness_report"] = []
        return df

    os.makedirs(chunk_dir, exist_ok=True)

    if force_refresh:
        print(f"[force_refresh] Clearing existing chunks for {symbol}/{exchange}/{timeframe} ...")
        for f in os.listdir(chunk_dir):
            os.remove(os.path.join(chunk_dir, f))

    all_chunks = _month_chunks(since_date, until_date)

    if update_latest and os.listdir(chunk_dir):
        existing_labels = sorted([f.replace(".parquet", "") for f in os.listdir(chunk_dir) if f.endswith(".parquet")])
        if existing_labels:
            last_label = existing_labels[-1]
            all_chunks = [c for c in all_chunks if c[0] >= last_label]
            print(f"[update_latest] Will only (re)fetch chunks from {last_label} onward.")

    if fill_missing:
        print(f"[fill_missing] Checking which chunks are still incomplete ...")
        to_refetch = []
        for label, chunk_start, chunk_end in all_chunks:
            chunk_path = _chunk_filepath(chunk_dir, label)
            expected = _expected_candles(chunk_start, chunk_end, tf_minutes)
            if os.path.exists(chunk_path):
                existing = pd.read_parquet(chunk_path)
                pct = (len(existing) / expected * 100) if expected else 100.0
                if pct < 99.0:
                    to_refetch.append((label, chunk_start, chunk_end))
            else:
                to_refetch.append((label, chunk_start, chunk_end))
        print(f"[fill_missing] {len(to_refetch)}/{len(all_chunks)} chunks need (re)fetching.")
        all_chunks = to_refetch

    total_chunks = len(all_chunks)
    print(f"\n[{symbol} / {exchange} / {timeframe}] Chunks to process: {total_chunks}")
    print(f"  Parallel workers: {parallel_workers} | retries: {retry_count} "
          f"(base {retry_base_wait}s, max {retry_max_wait}s) | chunk dir: {chunk_dir}\n")

    completed = 0
    completeness_report = []

    def process_chunk(chunk_info):
        label, chunk_start, chunk_end = chunk_info
        chunk_path = _chunk_filepath(chunk_dir, label)
        expected = _expected_candles(chunk_start, chunk_end, tf_minutes)

        if os.path.exists(chunk_path) and not force_refresh and not fill_missing:
            existing = pd.read_parquet(chunk_path)
            pct = (len(existing) / expected * 100) if expected else 100.0
            if pct >= 99.0:
                size_kb = round(os.path.getsize(chunk_path) / 1024, 1)
                return label, len(existing), expected, pct, "resumed (already complete)", size_kb

        df = _fetch_one_chunk(exchange, symbol, timeframe, chunk_start, chunk_end, limit,
                               retry_count, retry_base_wait, retry_max_wait)
        _save_parquet(df, chunk_path)
        actual = len(df)
        pct = (actual / expected * 100) if expected else 100.0
        size_kb = round(os.path.getsize(chunk_path) / 1024, 1)
        return label, actual, expected, pct, "fetched", size_kb

    if total_chunks > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = [executor.submit(process_chunk, c) for c in all_chunks]
            for future in concurrent.futures.as_completed(futures):
                label, actual, expected, pct, status, size_kb = future.result()
                completed += 1
                completeness_report.append({
                    "month": label, "actual_candles": actual, "expected_candles": expected,
                    "pct_complete": round(pct, 2), "status": "OK" if pct >= 99.0 else "INCOMPLETE",
                    "file_size_kb": size_kb, "fetch_status": status,
                })
                flag = "OK" if pct >= 99.0 else "INCOMPLETE"
                print(f"  [{completed}/{total_chunks}] {label} -> {actual:,}/{expected:,} candles "
                      f"({pct:.1f}%) [{flag}] ({size_kb} KB) ({status})")

    incomplete = [r for r in completeness_report if r["status"] == "INCOMPLETE"]
    print(f"\n[completeness check] {len(completeness_report) - len(incomplete)}/{len(completeness_report)} "
          f"chunks fully complete (this run).")
    if incomplete:
        print("  WARNING — these chunks are still INCOMPLETE (use fill_missing=True to retry just these):")
        for r in incomplete:
            print(f"    - {r['month']}: {r['actual_candles']:,}/{r['expected_candles']:,} ({r['pct_complete']}%)")

    # Always read the FULL set of chunks on disk for the actual returned data
    # (covers chunks that already existed before fill_missing/incremental runs)
    full_chunk_list = _month_chunks(since_date, until_date)
    all_dfs = [pd.read_parquet(_chunk_filepath(chunk_dir, c[0])) for c in full_chunk_list
               if os.path.exists(_chunk_filepath(chunk_dir, c[0]))]

    if not merge_chunks:
        print("[merge_chunks=False] Leaving chunks as separate files, no merged output produced.")
        result = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        result.attrs["completeness_report"] = completeness_report
        return result

    print(f"[merging] Combining {len(all_dfs)} chunks into final file ...")
    merged = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    merged = merged.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)

    if cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        _save_parquet(merged, cache_path)
        print(f"[saved] Final merged file -> {cache_path} ({len(merged):,} total rows)")

    merged.attrs["completeness_report"] = completeness_report
    return merged
