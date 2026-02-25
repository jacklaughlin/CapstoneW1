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

Copilot Review
------
Initially, Copilot was very confusing. In VS code, I was unsure as to how the files that Copilot was creating were able to be downloaded or where to find them. After creating a folder and directing that path to VS code's Copilot chat, I was able to retrieve the files and analyze them. It fell short in its ability to explain things easily. I believe that other AI tools are more efficient in the simplification of things, however this AI tool is extremely smart and needs an intelligent user as well. With proper promting, it is super helpful. One thing that suprised me was that I never had to put in a sample csv. It was able to create one and run tests on it to imrpove its model. I'm excited to continue working with it!
