"""
scenarios/exchange_price_gap.py
----------------------------------
Scenario: Exchange Price Gap (Lead-Lag with multiple thresholds)

Compares CLOSE price of two exchanges (same coin, same timeframe — designed
for 1m) at matching timestamps. For each given % threshold, it detects
periods where the price gap between the two exchanges exceeds that
threshold, identifies which exchange led (was first / higher at gap start)
and which followed (price adjusted to close the gap), and reports:

  1. EVENT-LEVEL detail (one row per gap event):
       threshold_pct, start_timestamp, end_timestamp, duration_minutes,
       leader, follower, max_gap_pct

  2. ROW-LEVEL OVERALL SUMMARY (per threshold):
       total_rows, % rows where exchange A led, % rows where exchange B
       led, % rows where gap stayed under threshold (no significant gap)

Registered name: "exchange_price_gap"

Expects exactly 2 datasets in the `data` dict, e.g.:
    {"exchange_a": df_a, "exchange_b": df_b}

params expected:
    thresholds : list of float, e.g. [0.1, 0.3, 0.5, 1.0]   (in %, default [0.5])
"""

import numpy as np
import pandas as pd
from core.registry import register_scenario


def _align(data: dict):
    keys = list(data.keys())
    if len(keys) != 2:
        raise ValueError("exchange_price_gap scenario requires exactly 2 datasets.")

    label_a, label_b = keys
    df_a = data[label_a][["timestamp", "close"]].rename(columns={"close": "close_a"})
    df_b = data[label_b][["timestamp", "close"]].rename(columns={"close": "close_b"})

    merged = pd.merge(df_a, df_b, on="timestamp", how="inner").sort_values("timestamp").reset_index(drop=True)
    merged["gap_pct"] = (merged["close_a"] - merged["close_b"]) / merged["close_b"] * 100
    merged["abs_gap_pct"] = merged["gap_pct"].abs()
    return label_a, label_b, merged


def _row_level_summary(merged: pd.DataFrame, label_a: str, label_b: str, threshold: float):
    total = len(merged)
    a_leads_mask = merged["gap_pct"] > threshold     # A's price is higher than B's by more than threshold
    b_leads_mask = merged["gap_pct"] < -threshold    # B's price is higher than A's by more than threshold
    no_gap_mask = ~(a_leads_mask | b_leads_mask)

    return {
        "threshold_pct": threshold,
        "total_rows": total,
        f"{label_a}_leads_pct": round(a_leads_mask.sum() / total * 100, 2),
        f"{label_b}_leads_pct": round(b_leads_mask.sum() / total * 100, 2),
        "no_significant_gap_pct": round(no_gap_mask.sum() / total * 100, 2),
    }


def _event_level_detail(merged: pd.DataFrame, label_a: str, label_b: str, threshold: float):
    """Groups consecutive rows where |gap| > threshold into discrete gap events."""
    state = np.where(merged["gap_pct"] > threshold, label_a,
             np.where(merged["gap_pct"] < -threshold, label_b, "none"))

    events = []
    i = 0
    n = len(state)
    while i < n:
        if state[i] == "none":
            i += 1
            continue
        leader = state[i]
        start_idx = i
        while i < n and state[i] == leader:
            i += 1
        end_idx = i - 1

        start_ts = merged["timestamp"].iloc[start_idx]
        end_ts = merged["timestamp"].iloc[end_idx]
        duration_minutes = (end_ts - start_ts).total_seconds() / 60 + (
            (merged["timestamp"].iloc[1] - merged["timestamp"].iloc[0]).total_seconds() / 60
        )
        max_gap = merged["abs_gap_pct"].iloc[start_idx:end_idx + 1].max()
        follower = label_b if leader == label_a else label_a

        events.append({
            "threshold_pct": threshold,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            "duration_minutes": round(duration_minutes, 2),
            "leader": leader,
            "follower": follower,
            "max_gap_pct": round(max_gap, 4),
        })

    return pd.DataFrame(events)


@register_scenario("exchange_price_gap")
def run(data: dict, params: dict = None):
    params = params or {}
    thresholds = params.get("thresholds", [0.5])

    label_a, label_b, merged = _align(data)

    all_summaries = []
    all_events = []

    for threshold in thresholds:
        summary = _row_level_summary(merged, label_a, label_b, threshold)
        all_summaries.append(summary)

        events_df = _event_level_detail(merged, label_a, label_b, threshold)
        if not events_df.empty:
            all_events.append(events_df)

    summary_df = pd.DataFrame(all_summaries)
    events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame(
        columns=["threshold_pct", "start_timestamp", "end_timestamp", "duration_minutes",
                 "leader", "follower", "max_gap_pct"]
    )

    return {
        "exchange_a": label_a,
        "exchange_b": label_b,
        "row_level_summary": summary_df,
        "details": events_df,   # this is what output_writer.py will save as the main CSV
    }
