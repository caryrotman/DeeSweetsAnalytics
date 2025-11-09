#!/usr/bin/env python3
"""
Query Module Generator

Given a SQL query, generate a Python module that executes the query via BigQuery,
prints a human-readable query name, suggests a chart type, keeps raw output, and
saves results to CSV. The generated script follows the pattern used in the
Queries/ directory.

Example:
  python generate_query_module.py --sql-file my_query.sql --name "Weekly RPM" \
      --output-dir Queries

The generator attempts to infer a descriptive name and chart suggestion if not
provided. You can tweak the generated script afterwards as needed.
"""

from __future__ import annotations

import argparse
import os
import re
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from string import Template

DEFAULT_OUTPUT_DIR = Path("Queries")


def load_sql(sql: Optional[str], sql_file: Optional[str]) -> str:
    if sql:
        return sql.strip()
    if sql_file:
        sql_path = Path(sql_file)
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        return sql_path.read_text(encoding="utf-8").strip()
    raise ValueError("You must provide either --sql or --sql-file")


def slugify(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-")
    text = re.sub(r"-+", "-", text)
    return text.lower()


def infer_name(sql: str) -> str:
    for raw_line in sql.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("--"):
            comment = line[2:].strip()
            if comment:
                return comment
            continue
        break

    match = re.search(r"select\s+(.*)\s+from", sql, re.IGNORECASE | re.DOTALL)
    if match:
        select_part = match.group(1)
        first_field = select_part.split(",")[0]
        first_field = re.sub(r"\(.*?\)", "", first_field)  # remove functions
        first_field = first_field.strip().strip("`")
        if first_field:
            return f"{first_field.title()} Analysis"
    return "Custom SQL Query"


def infer_chart(sql: str) -> str:
    sql_lower = sql.lower()
    if "time" in sql_lower or "date" in sql_lower:
        if "group by" in sql_lower:
            return "Line chart over time"
    keywords = ["rpm", "revenue", "sessions", "count", "sum", "avg"]
    if any(k in sql_lower for k in keywords) and "group by" in sql_lower:
        return "Horizontal bar chart by dimension"
    if "rank" in sql_lower or "dense_rank" in sql_lower:
        return "Table sorted by metric"
    return "Table"


def create_module_code(
    base_filename: str,
    query_name: str,
    chart_suggestion: str,
    sql: str,
) -> str:
    doc_sql = textwrap.indent(sql, "    ")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    template = textwrap.dedent(
        """#!/usr/bin/env python3
"""
Auto-generated query module.

Generated on ${TIMESTAMP} by generate_query_module.py.

Query Name: ${QUERY_NAME_TEXT}
Recommended Visualization: ${CHART_TEXT}

Original SQL:
${DOC_SQL}
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

QUERY_NAME = ${QUERY_NAME}
RECOMMENDED_CHART = ${CHART}
SQL = ${SQL}
DEFAULT_PROJECT = os.getenv("GCP_PROJECT", "websitecountryspikes")
DEFAULT_DATASET = os.getenv("GA_DATASET_ID", "analytics_427048881")


def resolve_sql(project: str, dataset: str) -> str:
    text = SQL.replace("YOUR_PROJECT", project)
    text = text.replace("YOUR_DATASET", dataset)
    return text


def run_query(project: str, dataset: str) -> pd.DataFrame:
    client = bigquery.Client(project=project)
    rendered_sql = resolve_sql(project, dataset)
    job = client.query(rendered_sql)
    return job.result().to_dataframe(create_bqstorage_client=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=f"Run '{QUERY_NAME}' query")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project ID")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="BigQuery dataset ID (used for placeholder replacement)")
    parser.add_argument(
        "--output-prefix",
        default=${BASE_FILENAME},
        help="Prefix for CSV output",
    )
    parser.add_argument("--start-date", help="Optional date range start label")
    parser.add_argument("--end-date", help="Optional date range end label")
    args = parser.parse_args()

    df = run_query(args.project, args.dataset)

    print(f"Query: {QUERY_NAME}")
    print(f"Recommended visualization: {RECOMMENDED_CHART}")
    print(f"Project: {args.project}")
    print(f"Dataset: {args.dataset}")
    if args.start_date or args.end_date:
        print(f"Date range: {args.start_date or 'N/A'} -> {args.end_date or 'N/A'}")
    print(f"Returned {len(df)} rows")
    print(df.head())

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path(f"{args.output_prefix}_{ts}.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved raw results to {csv_path}")

    chart_path = None
    try:
        import matplotlib.pyplot as plt  # type: ignore

        numeric_cols: list[str] = []
        for column in df.columns:
            series = df[column]
            if pd.api.types.is_numeric_dtype(series):
                numeric_cols.append(column)
                continue
            coerced = pd.to_numeric(series, errors="coerce")
            if coerced.notna().any():
                df[column] = coerced
                if pd.api.types.is_numeric_dtype(df[column]):
                    numeric_cols.append(column)

        if numeric_cols:
            subset = df[numeric_cols].head(20)
            if not subset.empty:
                chart_path = Path(f"{args.output_prefix}_{ts}.png")
                plt.figure(figsize=(12, 6))
                subset.plot(ax=plt.gca())
                plt.title(QUERY_NAME)
                plt.tight_layout()
                plt.savefig(chart_path)
                plt.close()
                print(f"Saved chart to {chart_path}")
            else:
                print("Insufficient data to render chart.")
        else:
            print("No numeric columns available to chart.")
    except ImportError:
        print("matplotlib not installed; skipping chart generation.")
    except Exception as exc:
        print(f"Failed to build chart: {exc}")


if __name__ == "__main__":
    main()
"""
    )

    substitutions = {
        "TIMESTAMP": timestamp,
        "QUERY_NAME_TEXT": query_name,
        "CHART_TEXT": chart_suggestion,
        "DOC_SQL": doc_sql,
        "QUERY_NAME": repr(query_name),
        "CHART": repr(chart_suggestion),
        "SQL": repr(sql),
        "BASE_FILENAME": repr(base_filename),
    }

    return Template(template).substitute(substitutions)


def ensure_output_dir(path: Path) -> None:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)


def main_generator():
    parser = argparse.ArgumentParser(description="Generate a GA4 BigQuery query module from SQL")
    parser.add_argument("--sql", help="SQL query string")
    parser.add_argument("--sql-file", help="Path to SQL file")
    parser.add_argument("--name", help="Human-readable query name")
    parser.add_argument("--chart", help="Recommended chart description")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for the module")
    parser.add_argument("--prefix", help="Filename prefix (optional)")

    args = parser.parse_args()

    sql = load_sql(args.sql, args.sql_file)
    query_name = args.name or infer_name(sql)
    chart_suggestion = args.chart or infer_chart(sql)

    base_filename = args.prefix or slugify(query_name)
    module_filename = f"{base_filename}.py"

    output_dir = Path(args.output_dir)
    ensure_output_dir(output_dir)

    module_path = output_dir / module_filename

    code = create_module_code(base_filename, query_name, chart_suggestion, sql)
    module_path.write_text(code, encoding="utf-8")

    print(f"Generated module: {module_path}")
    print(f"  Query Name: {query_name}")
    print(f"  Recommended visualization: {chart_suggestion}")


if __name__ == "__main__":
    main_generator()
