# CSV Profiler

Small tool to profile CSV files and produce a JSON report of column metrics.

Usage
-----

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the CLI:

```bash
python csv_profiler_cli.py path/to/data.csv -o report.json
```

Or print to stdout:

```bash
python csv_profiler_cli.py path/to/data.csv
```

Output
------

The JSON contains per-column metrics: nulls, percent nulls, distinct count, inferred type, min/max (where applicable), numeric aggregates, and top values.

CI
--

This repository includes a GitHub Actions workflow which runs the unit tests on push and pull requests. The workflow is defined at `.github/workflows/ci.yml` and executes `pytest` across supported Python versions.

To run tests locally:

```powershell
cd "C:\Users\jackl\Downloads\CSV Profiler"
python -m pip install -r requirements.txt
python -m pytest -q
```
