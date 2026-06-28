"""
indicators/macd.py
---------------------
Indicator: MACD (Moving Average Convergence Divergence)
Registered name: "MACD"

params expected (optional):
    fast   : int, default 12
    slow   : int, default 26
    signal : int, default 9
"""

import pandas_ta_classic as ta
from core.registry import register_indicator


@register_indicator("MACD")
def calculate(df, params: dict = None):
    params = params or {}
    fast = params.get("fast", 12)
    slow = params.get("slow", 26)
    signal = params.get("signal", 9)

    macd_df = ta.macd(df["close"], fast=fast, slow=slow, signal=signal)
    if macd_df is not None:
        df["MACD"] = macd_df[f"MACD_{fast}_{slow}_{signal}"]
        df["MACD_signal"] = macd_df[f"MACDs_{fast}_{slow}_{signal}"]
        df["MACD_hist"] = macd_df[f"MACDh_{fast}_{slow}_{signal}"]
    return df
