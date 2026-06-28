"""
indicators/ema.py
--------------------
Indicator: EMA (Exponential Moving Average)
Registered name: "EMA"

params expected (optional):
    length : int, default 50
"""

import pandas_ta_classic as ta
from core.registry import register_indicator


@register_indicator("EMA")
def calculate(df, params: dict = None):
    params = params or {}
    length = params.get("length", 50)
    df[f"EMA_{length}"] = ta.ema(df["close"], length=length)
    return df
