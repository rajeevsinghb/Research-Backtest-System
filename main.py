"""
main.py
---------
SINGLE CONTROL PANEL.
Edit only the CONFIG dictionary below to select what to run.
You should never need to edit core/, data_sources/, indicators/, or
scenarios/ logic from here — this file just selects and orchestrates.
"""

import asyncio
import concurrent.futures
import os

from core.loader import load_everything
from core.registry import DATA_SOURCE_REGISTRY, INDICATOR_REGISTRY, SCENARIO_REGISTRY, list_registered
from core.output_writer import save_result


# ============================================================
# CONFIG — change this section to run different research setups
# ============================================================
CONFIG = {
    "datasets": {
        # key name (your choice) -> {source, params}
        "okx_btc": {
            "enabled": True,          # set False to skip this dataset without deleting/commenting it
            "source": "ccxt_fetch",
            "params": {
                "exchange": "coinbase",
                "symbol": "BTC/USDT",
                "timeframe": "1m",
                "since_date": "2025-01-01T00:00:00Z",   # set your start date
                "until_date": "2026-01-01T00:00:00Z",   # set your end date
                "cache_path": "data/leadlag/raw/BTCUSDT_1m_coinbase_2025.parquet",
                "parallel_workers": 5,    # how many monthly chunks fetched concurrently
                "merge_chunks": False,     # True = single final file, False = keep chunks separate
                "force_refresh": False,   # True = full re-fetch, overwrite cache
                "update_latest": True,   # True = fetch only new candles since last cache, append
            },
        },

        "kucoin_btc": {
            "enabled": False,          # set False to skip this dataset without deleting/commenting it
            "source": "ccxt_fetch",
            "params": {
                "exchange": "kucoin",
                "symbol": "BTC/USDT",
                "timeframe": "1m",
                "since_date": "2025-11-27T00:00:00Z",
                "until_date": "2026-06-27T00:00:00Z",
                "cache_path": "data/leadlag/raw/BTCUSDT_1m_kucoin.parquet",
                "parallel_workers": 5,
                "merge_chunks": True,
                "force_refresh": False,
                "update_latest": False,
            },
        },

        # Example of loading externally-sourced data (already-saved Parquet, any source):
        # "external_data": {
        #     "source": "local_parquet",
        #     "params": {"path": "data/crypto/raw/some_other_source.parquet"},
        # },
    },

    "indicators": [],          # single or multiple — empty list [] = skip indicators

    "scenarios": [],        # single or multiple

    "scenario_params": {
        "exchange_price_gap": {"thresholds": [0.5, 1.0, 1.5, 2.0]},   # band edges, in %
    },

    "save_outputs": True,   # if True, every scenario result is also saved as CSV in outputs/
}
# ============================================================


# ============================================================
# READY-MADE TEMPLATES — copy the block you need into CONFIG["datasets"]
# above, give it a key name, uncomment, and change ONLY the highlighted
# value (ticker / symbol / series_id / metric). Nothing else needs typing.
# ============================================================
#
# --- ccxt_fetch (crypto, any exchange — supports large historical pulls via chunking) ---
# "my_dataset_name": {
#     "enabled": True,
#     "source": "ccxt_fetch",
#     "params": {
#         "exchange": "okx",                    # <-- change exchange
#         "symbol": "BTC/USDT",                 # <-- change coin pair
#         "timeframe": "1m",
#         "since_date": "2016-01-01T00:00:00Z", # <-- e.g. for 10 years of data
#         "until_date": "2026-06-27T00:00:00Z",
#         "cache_path": "data/crypto/raw/CHANGE_ME.parquet",
#         "parallel_workers": 8,                # <-- how many months fetched concurrently
#         "merge_chunks": True,                 # <-- False = keep monthly chunk files separate
#         "force_refresh": False,
#         "update_latest": False,
#     },
# },
#
# --- yfinance_fetch (stocks/forex/commodities/indices) ---
# "my_dataset_name": {
#     "enabled": True,
#     "source": "yfinance_fetch",
#     "params": {
#         "ticker": "GC=F",                     # <-- change ticker (see README table for codes)
#         "interval": "1d",
#         "since_date": "2024-01-01",
#         "until_date": "2026-06-27",
#         "cache_path": "data/macro/raw/CHANGE_ME.parquet",
#         "force_refresh": False,
#         "update_latest": False,
#     },
# },
#
# --- pycoingecko_fetch (BTC dominance / total market cap / coin price) ---
# "my_dataset_name": {
#     "enabled": True,
#     "source": "pycoingecko_fetch",
#     "params": {
#         "metric": "total_market_cap",         # <-- change: btc_dominance / total_market_cap / coin_price
#         "coin_id": None,                      # <-- only needed if metric == "coin_price", e.g. "bitcoin"
#         "days": 365,
#         "cache_path": "data/macro/raw/CHANGE_ME.parquet",
#         "force_refresh": False,
#         "update_latest": False,
#     },
# },
#
# --- fred_fetch (official US macro data — needs FRED_API_KEY env var) ---
# "my_dataset_name": {
#     "enabled": True,
#     "source": "fred_fetch",
#     "params": {
#         "series_id": "DGS10",                 # <-- change series code (see README table)
#         "since_date": "2024-01-01",
#         "until_date": "2026-06-27",
#         "cache_path": "data/macro/raw/CHANGE_ME.parquet",
#         "force_refresh": False,
#         "update_latest": False,
#     },
# },
#
# --- local_parquet (any externally-sourced Parquet you've placed manually) ---
# "my_dataset_name": {
#     "enabled": True,
#     "source": "local_parquet",
#     "params": {"path": "data/CHANGE_ME.parquet"},
# },
# ============================================================


def _load_datasets_parallel(datasets_config: dict) -> dict:
    """Loads all ENABLED datasets concurrently (e.g. fetching 2 exchanges
    at the same time instead of one after another) using a thread pool —
    each data source function itself stays plain/synchronous, this just
    runs several of them at once.

    Any dataset entry with "enabled": False is skipped entirely (no fetch,
    no network call) — set it to True (or simply omit the key, which
    defaults to True) to include it again. No need to comment/delete it."""
    active_datasets = {
        key: spec for key, spec in datasets_config.items()
        if spec.get("enabled", True)
    }
    skipped = [key for key in datasets_config if key not in active_datasets]
    if skipped:
        print(f"[skipped] Disabled datasets (enabled=False): {skipped}")

    data = {}
    if not active_datasets:
        print("[warning] No enabled datasets to load.")
        return data

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(active_datasets) or 1) as executor:
        future_to_key = {}
        for key, spec in active_datasets.items():
            source_func = DATA_SOURCE_REGISTRY[spec["source"]]
            future = executor.submit(source_func, spec["params"])
            future_to_key[future] = (key, spec["source"])

        for future in concurrent.futures.as_completed(future_to_key):
            key, source_name = future_to_key[future]
            df = future.result()
            data[key] = df
            size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

            cache_path = active_datasets[key]["params"].get("cache_path")
            disk_size_str = ""
            if cache_path and os.path.exists(cache_path):
                disk_mb = os.path.getsize(cache_path) / (1024 * 1024)
                disk_size_str = f", {disk_mb:.2f} MB on disk (Parquet)"

            print(f"[loaded] {key} -> {len(df):,} rows, ~{size_mb:.2f} MB in memory{disk_size_str} (source: {source_name})")

    # preserve original config order in the returned dict (enabled ones only)
    return {key: data[key] for key in active_datasets}


def run(config: dict):
    load_everything()  # auto-discovers everything in data_sources/, indicators/, scenarios/

    # 1. Load all configured datasets (in parallel, not one-by-one)
    data = _load_datasets_parallel(config["datasets"])

    # 2. Apply selected indicators to every dataset
    for key in data:
        for ind_name in config["indicators"]:
            indicator_func = INDICATOR_REGISTRY[ind_name]
            data[key] = indicator_func(data[key])
        if config["indicators"]:
            print(f"[indicators applied] {key} -> {config['indicators']}")

    # 3. Run selected scenarios
    all_results = {}
    for scenario_name in config["scenarios"]:
        scenario_func = SCENARIO_REGISTRY[scenario_name]
        params = config.get("scenario_params", {}).get(scenario_name, {})
        result = scenario_func(data, params)
        all_results[scenario_name] = result
        print(f"\n=== Scenario: {scenario_name} ===")
        print(result)

        if config.get("save_outputs", True):
            saved_path = save_result(scenario_name, result)
            print(f"[saved] Result written -> {saved_path}")

    return all_results


if __name__ == "__main__":
    load_everything()
    print("Available components:", list_registered())
    print()
    run(CONFIG)
