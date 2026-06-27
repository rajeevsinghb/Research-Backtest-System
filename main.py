"""
main.py
---------
SINGLE CONTROL PANEL.
Edit only the CONFIG dictionary below to select what to run.
You should never need to edit core/, data_sources/, indicators/, or
scenarios/ logic from here — this file just selects and orchestrates.
"""

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
                "timeframe": "15m",
                "since_date": "2026-01-01T00:00:00Z",
                "cache_path": "data/crypto/raw/BTCUSDT_15m_okx.parquet",
                "force_refresh": False,   # True = full re-fetch, overwrite cache
                "update_latest": False,   # True = fetch only new candles since last cache, append
            },
        },

        "bybit_btc": {
            "source": "ccxt_fetch",
            "params": {
                "exchange": "kucoin",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "since_date": "2026-01-01T00:00:00Z",
                "cache_path": "data/crypto/raw/BTCUSDT_15m_bybit.parquet",
                "force_refresh": False,
                "update_latest": False,
            },
        },
        # ^ Uncomment-style usage: this is enabled here as an example of a
        # second dataset for multi-dataset scenarios like "leadlag". Comment
        # out or remove this block if you only need a single dataset.

        # Example of loading externally-sourced data (already-saved Parquet, any source):
        # "external_data": {
        #     "source": "local_parquet",
        #     "params": {"path": "data/crypto/raw/some_other_source.parquet"},
        # },
    },

    "indicators": ["RSI", "ATR"],          # single or multiple — empty list [] = skip indicators

    "scenarios": ["simple_summary", "leadlag"],        # single or multiple, e.g. ["simple_summary", "leadlag"]

    "scenario_params": {
        # optional per-scenario params, e.g.
        # "leadlag": {"move_threshold_pct": 0.05, "max_lag": 10}
    },

    "save_outputs": True,   # if True, every scenario result is also saved as CSV in outputs/
}
# ============================================================


def run(config: dict):
    load_everything()  # auto-discovers everything in data_sources/, indicators/, scenarios/

    # 1. Load all configured datasets
    data = {}
    for key, spec in config["datasets"].items():
        source_func = DATA_SOURCE_REGISTRY[spec["source"]]
        df = source_func(spec["params"])
        data[key] = df
        print(f"[loaded] {key} -> {len(df):,} rows (source: {spec['source']})")

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
