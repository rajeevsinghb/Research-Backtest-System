"""
scenarios/leadlag.py
-----------------------
Scenario: Lead-Lag Analysis
Compares two datasets (same coin, same timeframe, two different exchanges)
to determine which one tends to move first, and by how much lag (in candles).

Registered name: "leadlag"

Expects exactly 2 datasets in the `data` dict, e.g.:
    {"exchange_a": df_a, "exchange_b": df_b}

params expected (optional):
    move_threshold_pct : float, default 0.05
    max_lag            : int, default 10   (in number of candles)
"""

import numpy as np
import pandas as pd
from core.registry import register_scenario


def _detect_moves(df, col, threshold_pct):
    pct_change = df[col].pct_change() * 100
    moved = (pct_change > threshold_pct) | (pct_change < -threshold_pct)
    return moved, pct_change


@register_scenario("leadlag")
def run(data: dict, params: dict = None):
    params = params or {}
    threshold_pct = params.get("move_threshold_pct", 0.05)
    max_lag = params.get("max_lag", 10)

    keys = list(data.keys())
    if len(keys) != 2:
        raise ValueError("leadlag scenario requires exactly 2 datasets.")

    label_a, label_b = keys
    df_a = data[label_a][["timestamp", "close"]].rename(columns={"close": "close_a"})
    df_b = data[label_b][["timestamp", "close"]].rename(columns={"close": "close_b"})

    merged = pd.merge(df_a, df_b, on="timestamp", how="inner").sort_values("timestamp").reset_index(drop=True)

    moves_a, pct_a = _detect_moves(merged, "close_a", threshold_pct)
    moves_b, pct_b = _detect_moves(merged, "close_b", threshold_pct)

    move_idx_a = merged.index[moves_a].tolist()
    move_idx_b = merged.index[moves_b].tolist()
    set_a, set_b = set(move_idx_a), set(move_idx_b)

    events = []
    for i in move_idx_a:
        direction = np.sign(pct_a.iloc[i])
        for lag in range(max_lag + 1):
            j = i + lag
            if j in set_b and np.sign(pct_b.iloc[j]) == direction:
                events.append((f"{label_a}_leads", lag))
                break
    for i in move_idx_b:
        direction = np.sign(pct_b.iloc[i])
        for lag in range(max_lag + 1):
            j = i + lag
            if j in set_a and np.sign(pct_a.iloc[j]) == direction:
                events.append((f"{label_b}_leads", lag))
                break

    results_df = pd.DataFrame(events, columns=["leader", "lag"])
    if results_df.empty:
        return {"summary": "No qualifying lead-lag events found.", "details": results_df}

    summary = results_df["leader"].value_counts(normalize=True).mul(100).round(1).to_dict()
    lag_stats = results_df.groupby("leader")["lag"].agg(["min", "max", "mean"]).round(2).to_dict("index")

    return {
        "total_events": len(results_df),
        "leader_pct": summary,
        "lag_stats_by_leader": lag_stats,
        "details": results_df,
    }
