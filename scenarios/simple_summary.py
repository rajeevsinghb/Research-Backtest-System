"""
scenarios/simple_summary.py
------------------------------
Scenario: Simple Summary
A minimal scenario — just basic stats per dataset. Used to verify the
multi-scenario / multi-dataset selection mechanism works correctly.

Registered name: "simple_summary"

Expects a dict of {data_key: dataframe} (one or more datasets).
"""

from core.registry import register_scenario


@register_scenario("simple_summary")
def run(data: dict, params: dict = None):
    results = {}
    for key, df in data.items():
        row = {
            "rows": len(df),
            "min_close": df["close"].min(),
            "max_close": df["close"].max(),
            "avg_close": round(df["close"].mean(), 2),
        }
        if "RSI" in df.columns:
            row["avg_RSI"] = round(df["RSI"].mean(), 2)
        if "ATR" in df.columns:
            row["avg_ATR"] = round(df["ATR"].mean(), 2)
        results[key] = row
    return results
