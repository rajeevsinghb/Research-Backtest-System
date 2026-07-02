# Workflow Guide — Run Research System

Quick reference for all parameters and advanced usage.
Open this file when you need to look something up — the GitHub Actions form stays clean.

---

## Form fields (GitHub Actions → Run workflow)

### Exchange & Coin

| Field | What it does | Tips |
|---|---|---|
| Exchange 1 | Primary data source | okx/bybit/kucoin recommended — more reliable, less rate-limiting |
| Coin pair | Trading pair to fetch | BTC/USDT most stable data across all exchanges |
| Timeframe | Candle granularity | 1m for lead-lag analysis; 1h/1d for trend/indicator research |
| From date | Start of data range | Format: YYYY-MM-DD |
| To date | End of data range | Format: YYYY-MM-DD |

### Second Exchange

| Field | What it does | Tips |
|---|---|---|
| Enable 2nd exchange | Adds a second dataset | Required for exchange_price_gap and leadlag scenarios |
| Exchange 2 | Second data source | Use a different exchange than Exchange 1 for comparison |

> Symbol and timeframe of Exchange 2 automatically match Exchange 1 — same coin, same period.

### Fetch Mode

| Mode | What happens | When to use |
|---|---|---|
| `normal` | Load cache if exists, fetch only if missing | Default — fastest, no re-downloading |
| `fill_missing` | Re-fetch only months that were INCOMPLETE last run | After a rate-limited or interrupted run |
| `update_latest` | Append new candles since last cached date | Daily/weekly refresh |
| `force_refresh` | Ignore all cache, re-fetch everything | Data corruption suspected, or major gap found |

### Parallel Workers

| Value | Best for |
|---|---|
| 1 | Coinbase, Kraken (strict rate limits) |
| 2-3 | Default safe choice for any exchange |
| 5 | OKX, Bybit, KuCoin (tolerant exchanges) |
| 8-10 | Only if exchange allows, watch for 429 errors |

> If you see many `[retry N]` 429 errors in logs, reduce parallel workers.

### Scenarios

| Scenario | What it does | Datasets needed |
|---|---|---|
| `none` | Only fetch/cache data, no analysis | 1 |
| `exchange_price_gap` | Price gap % bands between 2 exchanges | 2 (different exchanges, same coin) |
| `leadlag` | Which exchange moves first, follow-through lag | 2 (different exchanges, same coin) |
| `simple_summary` | Min/max/avg price + any selected indicators | 1 or more |

---

## Advanced settings (edit main.py CONFIG directly)

For anything not in the form, open `main.py` on GitHub (pencil icon), change the CONFIG block, commit, then run the workflow.

### Retry tuning (for strict exchanges like Coinbase)
```python
"retry_count": 10,        # default 8 — increase for unstable connections
"retry_base_wait": 5,     # default 3 — increase for Coinbase/Kraken
"retry_max_wait": 90,     # default 60
```

### Merge chunks
```python
"merge_chunks": False,    # default False — keep monthly files separate (safer for large data)
"merge_chunks": True,     # combine into one file (only if total < 100MB)
```

### Custom cache path
```python
"cache_path": "data/leadlag/raw/BTCUSDT_1m_okx.parquet",   # override auto-generated path
```

### exchange_price_gap thresholds
```python
"scenario_params": {
    "exchange_price_gap": {"thresholds": [0.1, 0.3, 0.5, 1.0]},  # finer bands
}
```

### leadlag sensitivity
```python
"scenario_params": {
    "leadlag": {"move_threshold_pct": 0.03, "max_lag": 5},
}
```

### Third dataset (not in form)
```python
"datasets": {
    "okx_btc": { ... },
    "kucoin_btc": { ... },
    "bybit_btc": {            # add as many as needed
        "enabled": True,
        "source": "ccxt_fetch",
        "params": { ... }
    },
}
```

### Non-crypto data sources (yfinance, FRED, CoinGecko)
```python
"gold": {
    "source": "yfinance_fetch",
    "params": {
        "ticker": "GC=F",
        "interval": "1d",
        "since_date": "2024-01-01",
        "until_date": "2026-06-30",
        "cache_path": "data/macro/raw/GOLD_1d.parquet",
    },
},
```
See `data_sources/yfinance_fetch.py`, `fred_fetch.py`, `pycoingecko_fetch.py` for full param reference.

---

## Outputs

After every run, check `outputs/` folder in the repo:

| File | Contents |
|---|---|
| `completeness_{dataset}_{timestamp}.csv` | Per-month fetch status: expected vs actual candles, % complete, file size |
| `exchange_price_gap_{timestamp}.csv` | Gap event details: start/end time (UTC + IST), duration, leader, max gap % |
| `exchange_price_gap_{timestamp}_row_level_summary.csv` | % of rows in each gap band per threshold |
| `leadlag_{timestamp}.csv` | Lead-lag event details |
| `simple_summary_{timestamp}.csv` | Basic stats per dataset |

---

## Common run patterns

**Just fetch data (no analysis):**
- Scenarios → `none`

**Lead-lag analysis (OKX vs KuCoin):**
- Exchange 1 → `okx`, Enable 2nd → ✓, Exchange 2 → `kucoin`
- Scenarios → `exchange_price_gap,leadlag`

**After incomplete run (rate limit errors):**
- Fetch mode → `fill_missing` (only retries incomplete months)

**Daily refresh (append today's data):**
- Fetch mode → `update_latest`
