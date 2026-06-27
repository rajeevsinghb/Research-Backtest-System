"""
indicators/atr.py
-------------------
Indicator: ATR (Average True Range)
Registered name: "ATR"

params expected (optional):
    length : int, default 14
"""

import pandas_ta_classic as ta
from core.registry import register_indicator


@register_indicator("ATR")
def calculate(df, params: dict = None):
    params = params or {}
    length = params.get("length", 14)
    df["ATR"] = ta.atr(df["high"], df["low"], df["close"], length=length)
    return df
