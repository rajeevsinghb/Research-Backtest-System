"""
indicators/sma.py
--------------------
Indicator: SMA (Simple Moving Average)
Registered name: "SMA"

params expected (optional):
    length : int, default 50
"""

import pandas_ta_classic as ta
from core.registry import register_indicator


@register_indicator("SMA")
def calculate(df, params: dict = None):
    params = params or {}
    length = params.get("length", 50)
    df[f"SMA_{length}"] = ta.sma(df["close"], length=length)
    return df
