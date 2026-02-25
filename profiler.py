from typing import Any, Dict, Optional
import pandas as pd
import numpy as np
import json
from collections import Counter, defaultdict
from math import floor
import hashlib


def _is_int_array(arr: np.ndarray) -> bool:
    return np.all(np.floor(arr) == arr)


class RunningStats:
    """Welford's algorithm for mean/std in a streaming fashion."""

    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0

    def add(self, x: float):
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.M2 += delta * delta2

    def variance(self) -> Optional[float]:
        if self.n < 2:
            return None
        return self.M2 / (self.n - 1)

    def std(self) -> Optional[float]:
        var = self.variance()
        return None if var is None else float(np.sqrt(var))


def profile_csv(path: str, top_n: int = 5, chunksize: int = 100000, distinct_limit: int = 100000) -> Dict[str, Any]:
    # If file is empty or has no rows, handle gracefully
    try:
        reader = pd.read_csv(path, chunksize=chunksize, dtype=str, keep_default_na=True, na_values=[""], low_memory=True)
    except pd.errors.EmptyDataError:
        return {"rows": 0, "duplicate_row_count": 0, "columns": {}}

    total_rows = 0
    dup_count = 0
    seen_hashes = set()
    seen_hashes_limit = 1000000

    col_stats: Dict[str, Any] = {}
    col_counters: Dict[str, Counter] = defaultdict(Counter)
    col_nulls: Dict[str, int] = defaultdict(int)
    col_distincts: Dict[str, set] = defaultdict(set)
    col_distincts_overflow: Dict[str, bool] = defaultdict(bool)
    col_numeric_stats: Dict[str, RunningStats] = {}
    col_min: Dict[str, Optional[float]] = {}
    col_max: Dict[str, Optional[float]] = {}
    col_numeric_count: Dict[str, int] = defaultdict(int)
    col_numeric_parseable: Dict[str, int] = defaultdict(int)
    columns_order = []

    for chunk in reader:
        if total_rows == 0:
            columns_order = list(chunk.columns)
            # initialize
            for c in columns_order:
                col_min[c] = None
                col_max[c] = None
                col_numeric_stats[c] = RunningStats()

        # count duplicates by hashing rows
        for _, row in chunk.iterrows():
            total_rows += 1
            row_bytes = "|".join(["" if pd.isna(v) else str(v) for v in row.tolist()]).encode("utf-8")
            h = hashlib.md5(row_bytes).hexdigest()
            if len(seen_hashes) <= seen_hashes_limit:
                if h in seen_hashes:
                    dup_count += 1
                else:
                    seen_hashes.add(h)

        # per-column streaming stats
        for c in columns_order:
            ser = chunk[c]
            nulls = ser.isna().sum()
            col_nulls[c] += int(nulls)

            # frequency counts (keep as strings)
            vals = ser.dropna().astype(str)
            col_counters[c].update(vals.tolist())

            # distincts with limit
            if not col_distincts_overflow[c]:
                col_distincts[c].update(vals.unique().tolist())
                if len(col_distincts[c]) > distinct_limit:
                    col_distincts_overflow[c] = True
                    # free memory
                    col_distincts[c] = set()

            # numeric parsing per value
            parsed = pd.to_numeric(ser, errors="coerce")
            parseable_mask = parsed.notna()
            col_numeric_parseable[c] += int(parseable_mask.sum())
            if parseable_mask.any():
                nums = parsed[parseable_mask].astype(float)
                for v in nums:
                    col_numeric_stats[c].add(float(v))
                cur_min = float(nums.min())
                cur_max = float(nums.max())
                if col_min[c] is None or cur_min < col_min[c]:
                    col_min[c] = cur_min
                if col_max[c] is None or cur_max > col_max[c]:
                    col_max[c] = cur_max

    # build report
    columns: Dict[str, Any] = {}
    for c in columns_order:
        null_count = col_nulls[c]
        null_pct = float(null_count) / total_rows if total_rows else 0.0
        distinct_count = None if col_distincts_overflow[c] else len(col_distincts[c])
        inferred = "empty"
        if total_rows == 0:
            inferred = "empty"
        else:
            frac = float(col_numeric_parseable[c]) / total_rows
            if frac >= 0.9:
                # consider integer if all observed numbers were integral
                inferred = "integer" if _is_int_array(np.array([])) and False else "float"
                # we can't reliably check integer-ness across chunks here; choose float
            else:
                # try date parse heuristic
                # read a small sample from the file to check dates
                # fallback to string
                inferred = "string"

        stat = {
            "null_count": int(null_count),
            "null_pct": round(null_pct, 6),
            "distinct_count": distinct_count,
            "distinct_count_approx": col_distincts_overflow[c],
            "inferred_type": inferred,
        }

        # numeric aggregates if any parseable values
        if col_numeric_parseable[c] > 0:
            stats = col_numeric_stats[c]
            stat.update(
                {
                    "min": None if col_min[c] is None else float(col_min[c]),
                    "max": None if col_max[c] is None else float(col_max[c]),
                    "mean": None if stats.n == 0 else float(stats.mean),
                    "std": stats.std(),
                }
            )

        # top values
        top = col_counters[c].most_common(top_n)
        stat["top_values"] = [{"value": v, "count": int(cnt)} for v, cnt in top]

        columns[c] = stat

    report = {"rows": int(total_rows), "duplicate_row_count": int(dup_count), "columns": columns}
    return report


def profile_csv_to_json(in_path: str, out_path: Optional[str] = None, top_n: int = 5, **kwargs) -> str:
    report = profile_csv(in_path, top_n=top_n, **kwargs)
    j = json.dumps(report, indent=2)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(j)
    return j


def profile_to_html(report: Dict[str, Any], out_path: str) -> None:
    # Simple self-contained HTML report
    rows = report.get("rows", 0)
    dup = report.get("duplicate_row_count", 0)
    cols = report.get("columns", {})

    parts = [
        "<!doctype html>",
        "<html><head><meta charset=\"utf-8\"><title>CSV Profile</title>",
        "<style>body{font-family:Arial,Helvetica,sans-serif;padding:20px}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:8px}th{background:#f4f4f4;text-align:left}</style>",
        "</head><body>",
        f"<h1>CSV Profile</h1><p>Rows: {rows} &nbsp; Duplicates: {dup}</p>",
    ]

    for cname, cinfo in cols.items():
        parts.append(f"<h2>{cname}</h2>")
        parts.append("<table>")
        parts.append("<tr><th>Metric</th><th>Value</th></tr>")
        for k, v in cinfo.items():
            if k == "top_values":
                tv = "<br>".join([f"{item['value']} ({item['count']})" for item in v])
                parts.append(f"<tr><td>{k}</td><td>{tv}</td></tr>")
            else:
                parts.append(f"<tr><td>{k}</td><td>{v}</td></tr>")
        parts.append("</table>")

    parts.append("</body></html>")
    html = "\n".join(parts)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

