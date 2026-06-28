"""
indicators/adx.py
-------------------
Indicator: ADX (Average Directional Index) — trend strength
Registered name: "ADX"

params expected (optional):
    length : int, default 14
"""

import pandas_ta_classic as ta
from core.registry import register_indicator


@register_indicator("ADX")
def calculate(df, params: dict = None):
    params = params or {}
    length = params.get("length", 14)
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=length)
    if adx_df is not None:
        df["ADX"] = adx_df[f"ADX_{length}"]
        df["DMP"] = adx_df[f"DMP_{length}"]   # +DI
        df["DMN"] = adx_df[f"DMN_{length}"]   # -DI
    return df
