"""
indicators/bollinger_bands.py
--------------------------------
Indicator: Bollinger Bands
Registered name: "BollingerBands"

params expected (optional):
    length : int, default 20
    std    : float, default 2.0
"""

import pandas_ta_classic as ta
from core.registry import register_indicator


@register_indicator("BollingerBands")
def calculate(df, params: dict = None):
    params = params or {}
    length = params.get("length", 20)
    std = params.get("std", 2.0)

    bb = ta.bbands(df["close"], length=length, std=std)
    if bb is not None:
        df["BB_upper"] = bb[f"BBU_{length}_{float(std)}"]
        df["BB_mid"] = bb[f"BBM_{length}_{float(std)}"]
        df["BB_lower"] = bb[f"BBL_{length}_{float(std)}"]
    return df
