"""Simple CLI for CSV Profiler."""
import argparse
from csv_profiler import profile_csv_to_json, profile_to_html, profile_csv


def main():
    p = argparse.ArgumentParser(description="Profile a CSV and produce a report")
    p.add_argument("input", help="Path to input CSV")
    p.add_argument("-o", "--output", help="Path to write report (stdout if omitted)")
    p.add_argument("--top", type=int, default=5, help="Top N frequent values to include")
    p.add_argument("--format", choices=["json", "html"], default="json", help="Output format")
    args = p.parse_args()

    if args.format == "json":
        out = profile_csv_to_json(args.input, out_path=args.output, top_n=args.top)
        if not args.output:
            print(out)
    else:
        # generate html
        # if no output provided, default path
        out_path = args.output or "report.html"
        report = profile_csv(args.input, top_n=args.top)
        profile_to_html(report, out_path=out_path)
        print(f"Wrote HTML report to {out_path}")


if __name__ == "__main__":
    main()
