"""
data_sources/local_parquet.py
-------------------------------
Data source: load an already-existing Parquet file from disk — no network
call at all. Use this for:
  - Data manually downloaded/converted from any OTHER source (stocks,
    forex, macro data, etc.) and placed in the data/ folder as Parquet.
  - Any historical data you already fetched and just want to re-load.

Registered name: "local_parquet"

params expected:
    path : str   e.g. "data/crypto/raw/BTCUSDT_15m_okx.parquet"

Required columns in the file: timestamp, open, high, low, close, volume
(Whatever the original source was, just make sure the Parquet file has
these column names before placing it in data/.)
"""

import pandas as pd
from core.registry import register_data_source


@register_data_source("local_parquet")
def get_data(params: dict) -> pd.DataFrame:
    df = pd.read_parquet(params["path"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df
