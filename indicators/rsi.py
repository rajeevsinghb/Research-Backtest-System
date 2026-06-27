"""
indicators/rsi.py
-------------------
Indicator: RSI (Relative Strength Index)
Registered name: "RSI"

params expected (optional):
    length : int, default 14
"""

import pandas_ta_classic as ta
from core.registry import register_indicator


@register_indicator("RSI")
def calculate(df, params: dict = None):
    params = params or {}
    length = params.get("length", 14)
    df["RSI"] = ta.rsi(df["close"], length=length)
    return df
