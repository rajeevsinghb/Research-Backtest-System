# Research-Backtest-Engine

A modular, registry-based quant research ecosystem. Select any combination
of data sources, indicators, and scenarios from a single control panel
(`main.py`) — no other file needs to be touched for day-to-day use.

## How to run

```bash
pip install -r requirements.txt
python main.py
```

Edit the `CONFIG` dictionary inside `main.py` to choose:
- `datasets`   — single or multiple (coin/exchange/timeframe combinations)
- `indicators` — single or multiple
- `scenarios`  — single or multiple

## Folder structure

```
core/            engine internals — never edit
  registry.py    registration system
  loader.py      auto-discovery of indicators/scenarios/data_sources
  output_writer.py  saves scenario results as CSV in outputs/

data_sources/    one file per data source (CCXT fetch, local parquet, future: stocks/forex/macro...)
indicators/      one file per indicator (RSI, ATR, ... add more anytime)
scenarios/       one file per research scenario (leadlag, simple_summary, ... add more anytime)

data/            raw cached Parquet files (organized by category)
outputs/         CSV results from every scenario run — check here without running any code

main.py          the single control panel
requirements.txt
```

## Adding a new component (no existing file ever needs editing)

**New indicator:**
```python
# indicators/macd.py
from core.registry import register_indicator

@register_indicator("MACD")
def calculate(df, params=None):
    ...
    return df
```

**New scenario:**
```python
# scenarios/backtest.py
from core.registry import register_scenario

@register_scenario("backtest")
def run(data, params=None):
    ...
    return results
```

**New data source (e.g. stocks via yfinance):**
```python
# data_sources/yfinance_fetch.py
from core.registry import register_data_source

@register_data_source("yfinance_fetch")
def get_data(params):
    ...
    return df  # must have columns: timestamp, open, high, low, close, volume
```

Then just reference the new name (`"MACD"`, `"backtest"`, `"yfinance_fetch"`) inside
`main.py`'s `CONFIG`. No other code changes needed.

## Removing a component

Just delete the file. The registry will no longer know about it. If `CONFIG`
still references it, you'll get a clear error telling you to update `CONFIG`.

## Caching behavior (`ccxt_fetch` data source)

Every dataset using `ccxt_fetch` needs a `cache_path`. Behavior:
- **Default** — if the cache file exists, it's loaded directly (no network call).
  If not, a fresh fetch happens once and gets saved for next time.
- **`update_latest: True`** — fetches only new candles since the last cached
  timestamp and appends them. Use this for routine/daily updates.
- **`force_refresh: True`** — ignores the cache entirely and re-fetches the
  full range, overwriting the file. Use rarely (e.g. suspected bad data).

## Outputs

Every scenario run automatically saves a CSV (and a companion summary JSON
where applicable) into `outputs/`, named `{scenario_name}_{timestamp}.csv`,
so results can be inspected directly on GitHub without running any code.
