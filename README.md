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

## Data source configuration reference

Each `data_sources/*.py` file's own docstring (top of the file) is the
authoritative reference for its exact params — open that file if in doubt.
This table is just a quick lookup to know which library to use for what,
and what its CONFIG block in `main.py` roughly looks like.

| Source name (`"source"` in CONFIG) | Library | API key needed? | Covers | Example params |
|---|---|---|---|---|
| `ccxt_fetch` | ccxt | No | Crypto OHLCV from any exchange (OKX, Bybit, KuCoin, Binance, ...) | `exchange, symbol, timeframe, since_date, until_date, cache_path, force_refresh, update_latest` |
| `local_parquet` | pandas (no fetch) | No | Any already-saved Parquet file (e.g. manually placed/converted data from any source) | `path` |
| `yfinance_fetch` | yfinance | No | Stocks, indices (Nifty, Nasdaq, S&P500), commodities (Gold, Silver, Oil), forex (EUR/USD, USD/INR), VIX, DXY, approx. bond yields | `ticker, interval, since_date, until_date, cache_path, force_refresh, update_latest` |
| `pycoingecko_fetch` | pycoingecko | No (free public API) | BTC Dominance (current value only on free tier), Total Crypto Market Cap, individual coin price history | `metric, coin_id (only for "coin_price"), days, cache_path, force_refresh, update_latest` |
| `fred_fetch` | fredapi | **Yes** — free key from fred.stlouisfed.org | Official US macro data: 10Y/2Y Treasury Yield, CPI, Fed Funds Rate, etc. | `series_id, since_date, until_date, cache_path, force_refresh, update_latest, api_key (or env var FRED_API_KEY)` |

**Common pattern across every source (so switching libraries never feels different):**
- Every source returns the same standardized columns: `timestamp, open, high, low, close, volume`
  (for sources that aren't real OHLCV, like FRED/CoinGecko macro series, open/high/low just mirror
  close and volume is 0 — this keeps every indicator/scenario working unchanged regardless of source).
- Every source supports the same 3 caching modes via params: default (cache-read), `update_latest`,
  `force_refresh` — except `local_parquet` which has no caching logic since it never fetches anything.
- The only thing that's genuinely different per library is the **input params** (because each
  library's own API is different) — that's the only part you ever need to look up per source.

**Where to make changes when adding a new dataset using any of these sources:**
1. Open `main.py`
2. Add a new entry under `CONFIG["datasets"]`
3. Set `"source"` to the source name from the table above
4. Fill in that source's specific params (check its file's docstring or the table above)
5. Nothing else needs to change — `core/`, `indicators/`, `scenarios/` stay untouched

