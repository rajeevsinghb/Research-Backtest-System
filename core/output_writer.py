"""
core/output_writer.py
------------------------
Saves scenario results to outputs/ as CSV so you can inspect results
manually anytime (e.g. directly on GitHub) without running any code.
"""

import os
import json
import pandas as pd
from datetime import datetime, timezone


def save_result(scenario_name: str, result, output_dir: str = "outputs") -> str:
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{scenario_name}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Case 1: result is already a DataFrame
    if isinstance(result, pd.DataFrame):
        result.to_csv(filepath, index=False)
        return filepath

    # Case 2: result is a dict that contains a DataFrame under "details" (e.g. leadlag scenario)
    if isinstance(result, dict) and "details" in result and isinstance(result["details"], pd.DataFrame):
        result["details"].to_csv(filepath, index=False)
        # also save the summary (non-dataframe) part as a small companion file
        summary = {k: v for k, v in result.items() if k != "details"}
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
