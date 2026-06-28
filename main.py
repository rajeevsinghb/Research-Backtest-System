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
            "source": "ccxt_fetch",
            "params": {
                "exchange": "okx",
                "symbol": "BTC/USDT",
                "timeframe": "1m",
                "since_date": "2025-11-27T00:00:00Z",   # set your start date
                "until_date": "2026-06-27T00:00:00Z",   # set your end date
                "cache_path": "data/leadlag/raw/BTCUSDT_1m_okx.parquet",
                "force_refresh": False,   # True = full re-fetch, overwrite cache
                "update_latest": False,   # True = fetch only new candles since last cache, append
            },
        },

        "kucoin_btc": {
            "source": "ccxt_fetch",
            "params": {
                "exchange": "kucoin",
                "symbol": "BTC/USDT",
                "timeframe": "1m",
                "since_date": "2025-11-27T00:00:00Z",
                "until_date": "2026-06-27T00:00:00Z",
                "cache_path": "data/leadlag/raw/BTCUSDT_1m_kucoin.parquet",
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

    "scenarios": ["exchange_price_gap"],        # single or multiple

    "scenario_params": {
        "exchange_price_gap": {"thresholds": [0.5, 1.0, 1.5, 2.0]},   # band edges, in %
    },

    "save_outputs": True,   # if True, every scenario result is also saved as CSV in outputs/
}
# ============================================================


def _load_datasets_parallel(datasets_config: dict) -> dict:
    """Loads all configured datasets concurrently (e.g. fetching 2 exchanges
    at the same time instead of one after another) using a thread pool —
    each data source function itself stays plain/synchronous, this just
    runs several of them at once."""
    data = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(datasets_config) or 1) as executor:
        future_to_key = {}
        for key, spec in datasets_config.items():
            source_func = DATA_SOURCE_REGISTRY[spec["source"]]
            future = executor.submit(source_func, spec["params"])
            future_to_key[future] = (key, spec["source"])

        for future in concurrent.futures.as_completed(future_to_key):
            key, source_name = future_to_key[future]
            df = future.result()
            data[key] = df
            print(f"[loaded] {key} -> {len(df):,} rows (source: {source_name})")

    # preserve original config order in the returned dict
    return {key: data[key] for key in datasets_config}


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
