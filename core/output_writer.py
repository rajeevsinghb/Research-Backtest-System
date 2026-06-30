"""
core/output_writer.py
------------------------
Saves scenario results to outputs/ as CSV so you can inspect results
manually anytime (e.g. directly on GitHub) without running any code.

All underlying data stays in UTC (the international standard, used by
every exchange/library this system fetches from). For convenience, any
timestamp-like column found in a result is duplicated as an extra
"<column>_ist" column (converted to Asia/Kolkata) ONLY in the saved
output files — the in-memory data used by indicators/scenarios is never
touched, so nothing upstream is affected by this.
"""

import os
import json
import pandas as pd
from datetime import datetime, timezone

IST = "Asia/Kolkata"

# Column names that should get an "_ist" twin if present in any output DataFrame
TIMESTAMP_LIKE_COLUMNS = ["timestamp", "start_timestamp", "end_timestamp"]


def _add_ist_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a copy of df with an extra '<col>_ist' column next to every
    UTC timestamp column found, for easier reading. Safe no-op if none
    of the expected columns are present."""
    df = df.copy()
    for col in TIMESTAMP_LIKE_COLUMNS:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            ist_col = f"{col}_ist"
            series = df[col]
            if series.dt.tz is None:
                series = series.dt.tz_localize("UTC")
            df[ist_col] = series.dt.tz_convert(IST).dt.tz_localize(None)

            # place the new _ist column right after its UTC source column
            cols = list(df.columns)
            cols.remove(ist_col)
            insert_at = cols.index(col) + 1
            cols.insert(insert_at, ist_col)
            df = df[cols]
    return df


def save_result(scenario_name: str, result, output_dir: str = "outputs") -> str:
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{scenario_name}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Case 1: result is already a DataFrame
    if isinstance(result, pd.DataFrame):
        _add_ist_columns(result).to_csv(filepath, index=False)
        return filepath

    # Case 2: result is a dict that contains a DataFrame under "details" (e.g. leadlag, exchange_price_gap)
    if isinstance(result, dict) and "details" in result and isinstance(result["details"], pd.DataFrame):
        _add_ist_columns(result["details"]).to_csv(filepath, index=False)

        # If there's also a row_level_summary DataFrame, save it as a companion CSV
        if "row_level_summary" in result and isinstance(result["row_level_summary"], pd.DataFrame):
            summary_csv_path = filepath.replace(".csv", "_row_level_summary.csv")
            _add_ist_columns(result["row_level_summary"]).to_csv(summary_csv_path, index=False)

        # save remaining non-dataframe keys as a small companion json
        summary = {k: v for k, v in result.items() if not isinstance(v, pd.DataFrame)}
        summary_path = filepath.replace(".csv", "_summary.json")
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        return filepath

    # Case 3: result is a dict of simple values/nested dicts (e.g. simple_summary scenario)
    if isinstance(result, dict):
        try:
            df = pd.DataFrame(result).T  # rows = dataset keys, columns = metrics
            df.to_csv(filepath)
            return filepath
        except Exception:
            # fallback: dump as JSON if it doesn't fit a flat table
            json_path = filepath.replace(".csv", ".json")
            with open(json_path, "w") as f:
                json.dump(result, f, indent=2, default=str)
            return json_path

    # Case 4: anything else — just str() dump
    with open(filepath.replace(".csv", ".txt"), "w") as f:
        f.write(str(result))
    return filepath
