"""
scenarios/exchange_price_gap.py
----------------------------------
Scenario: Exchange Price Gap (band-based, mutually exclusive thresholds)

Compares CLOSE price of two exchanges (same coin, same timeframe — designed
for 1m) at matching timestamps. Instead of overlapping ">X%" thresholds,
this version buckets every row's |gap%| into MUTUALLY EXCLUSIVE bands
(e.g. 0-0.5%, 0.5-1%, 1-1.5%, 1.5-2%, 2%+) so each row is counted exactly
once — no double-counting across thresholds.

For each band, it reports:
  - % of total rows falling in that band
  - within that band, % where exchange A was higher (leading) vs B
  - EVENT-LEVEL detail: consecutive-row gap events within that band
    (start_timestamp, end_timestamp, duration_minutes, leader, follower,
     max_gap_pct)

If a band has ZERO occurrences, it is still explicitly shown with 0.0%
(never blank) — so you can always tell "ran and found nothing" apart
from "didn't run".

Registered name: "exchange_price_gap"

Expects exactly 2 datasets in the `data` dict, e.g.:
    {"exchange_a": df_a, "exchange_b": df_b}

params expected:
    thresholds : list of float, e.g. [0.5, 1.0, 1.5, 2.0]
                 These define band EDGES. Bands built automatically as:
                 [0, t1), [t1, t2), [t2, t3), ..., [t_last, inf)
                 Default: [0.5, 1.0, 1.5, 2.0]
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


def _build_bands(thresholds):
    """Build mutually-exclusive band edges: [0, t1), [t1, t2), ..., [t_last, inf)"""
    edges = sorted(thresholds)
    bands = [(0.0, edges[0])]
    for i in range(len(edges) - 1):
        bands.append((edges[i], edges[i + 1]))
    bands.append((edges[-1], float("inf")))
    return bands


def _band_label(low, high):
    if high == float("inf"):
        return f"{low}%+"
    return f"{low}-{high}%"


def _row_level_summary(merged: pd.DataFrame, label_a: str, label_b: str, bands):
    total = len(merged)
    rows = []

    for low, high in bands:
        in_band = (merged["abs_gap_pct"] >= low) & (merged["abs_gap_pct"] < high)
        band_count = in_band.sum()

        a_leads = in_band & (merged["gap_pct"] > 0)   # A's close higher than B's, within this band
        b_leads = in_band & (merged["gap_pct"] < 0)

        rows.append({
            "band": _band_label(low, high),
            "band_low_pct": low,
            "band_high_pct": high,
            "rows_in_band": int(band_count),
            "pct_of_total_rows": round(band_count / total * 100, 4) if total else 0.0,
            f"{label_a}_leads_pct_of_total": round(a_leads.sum() / total * 100, 4) if total else 0.0,
            f"{label_b}_leads_pct_of_total": round(b_leads.sum() / total * 100, 4) if total else 0.0,
        })

    return pd.DataFrame(rows)


def _event_level_detail(merged: pd.DataFrame, label_a: str, label_b: str, low, high):
    """Groups consecutive rows whose |gap| falls within [low, high) into discrete gap events."""
    in_band = (merged["abs_gap_pct"] >= low) & (merged["abs_gap_pct"] < high)
    direction = np.where(merged["gap_pct"] > 0, label_a, label_b)
    state = np.where(in_band, direction, "none")

    n = len(state)
    if n == 0:
        return pd.DataFrame(columns=["band", "start_timestamp", "end_timestamp",
                                      "duration_minutes", "leader", "follower", "max_gap_pct"])

    candle_minutes = (merged["timestamp"].iloc[1] - merged["timestamp"].iloc[0]).total_seconds() / 60 if n > 1 else 1

    events = []
    i = 0
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
        duration_minutes = (end_ts - start_ts).total_seconds() / 60 + candle_minutes
        max_gap = merged["abs_gap_pct"].iloc[start_idx:end_idx + 1].max()
        follower = label_b if leader == label_a else label_a

        events.append({
            "band": _band_label(low, high),
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
    thresholds = params.get("thresholds", [0.5, 1.0, 1.5, 2.0])
    bands = _build_bands(thresholds)

    label_a, label_b, merged = _align(data)

    summary_df = _row_level_summary(merged, label_a, label_b, bands)

    all_events = []
    for low, high in bands:
        events_df = _event_level_detail(merged, label_a, label_b, low, high)
        all_events.append(events_df)  # appended even if empty -> band still traceable

    events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()

    return {
        "exchange_a": label_a,
        "exchange_b": label_b,
        "total_rows_compared": len(merged),
        "row_level_summary": summary_df,
        "details": events_df,
    }
