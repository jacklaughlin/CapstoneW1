import os
from csv_profiler.profiler import profile_csv


def _path(p):
    base = os.path.join(os.path.dirname(__file__), "data")
    return os.path.join(base, p)


def test_empty_file():
    report = profile_csv(_path("empty.csv"))
    assert report["rows"] == 0
    assert report["duplicate_row_count"] == 0
    assert report["columns"] == {}


def test_all_nulls():
    report = profile_csv(_path("all_nulls.csv"))
    assert report["rows"] > 0
    for col, info in report["columns"].items():
        assert info["null_count"] == report["rows"]


def test_sample_csv_metrics():
    report = profile_csv(_path("sample.csv"))
    assert report["rows"] == 5
    # duplicate row present once
    assert report["duplicate_row_count"] == 1
    cols = report["columns"]
    assert "age" in cols
    assert cols["name"]["top_values"][0]["value"] in ["Alice", "Bob", "Charlie", ""]
