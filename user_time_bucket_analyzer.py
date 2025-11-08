#!/usr/bin/env python3
"""
User Time Bucket Analyzer for GA4 BigQuery Export

This script pulls user engagement data from the GA4 BigQuery export,
calculates the total time each user spends on the site per day, and
buckets the users into duration ranges:
  (a) 0–1 minute
  (b) 1–3 minutes
  (c) 3–5 minutes
  (d) 5–10 minutes
  (e) 10–30 minutes
  (f) 30+ minutes

Outputs:
  1. Text file with counts per day per bucket and overall totals
  2. Bar chart (PNG) showing overall user counts per bucket

Requirements:
  - google-cloud-bigquery
  - pandas
  - matplotlib

Usage:
  python user_time_bucket_analyzer.py \
      --start-date 2025-11-02 \
      --end-date 2025-11-06 \
      --project websitecountryspikes \
      --dataset analytics_427048881 \
      --timezone America/Los_Angeles
"""

import argparse
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
import matplotlib.pyplot as plt

# Duration buckets in minutes (upper bounds)
BUCKETS = [
    (0, 1, "0-1 min"),
    (1, 3, "1-3 min"),
    (3, 5, "3-5 min"),
    (5, 10, "5-10 min"),
    (10, 30, "10-30 min"),
    (30, None, "30+ min"),
]

DEFAULT_PROJECT = os.getenv("GCP_PROJECT", "websitecountryspikes")
DEFAULT_DATASET = os.getenv("GA_DATASET_ID", "analytics_427048881")
DEFAULT_TZ = os.getenv("GA_TZ", "America/Los_Angeles")


def build_parser():
    parser = argparse.ArgumentParser(description="Analyze user time buckets from GA4 BigQuery export")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project ID")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="GA4 BigQuery dataset ID")
    parser.add_argument("--timezone", default=DEFAULT_TZ, help="Timezone for date conversion (IANA identifier)")
    parser.add_argument("--start-date", default="2025-11-02", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2025-11-06", help="End date (YYYY-MM-DD)")
    parser.add_argument("--prefix", default="user_time_buckets", help="Prefix for output files")
    return parser


def run_query(project: str, dataset: str, timezone: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Run BigQuery to get daily engagement time per user."""
    client = bigquery.Client(project=project)

    query = f"""
    DECLARE tz STRING DEFAULT '{timezone}';

    WITH user_daily_engagement AS (
      SELECT
        DATE(TIMESTAMP_MICROS(event_timestamp), tz) AS visit_date,
        user_pseudo_id,
        SUM(
          IFNULL(
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec'),
            0
          )
        ) AS engagement_time_msec
      FROM `{project}.{dataset}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN REPLACE('{start_date}', '-', '') AND REPLACE('{end_date}', '-', '')
        AND event_name = 'user_engagement'
      GROUP BY visit_date, user_pseudo_id
    )

    SELECT
      visit_date,
      user_pseudo_id,
      engagement_time_msec / 1000.0 / 60.0 AS engagement_minutes
    FROM user_daily_engagement
    """

    job = client.query(query)
    df = job.result().to_dataframe(create_bqstorage_client=True)
    return df


def assign_bucket(minutes: float) -> str:
    for lower, upper, label in BUCKETS:
        if upper is None:
            if minutes >= lower:
                return label
        else:
            if lower <= minutes < upper:
                return label
    # Fallback
    return "Unknown"


def bucketize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["visit_date", "bucket", "user_count"])

    df = df.copy()
    df["bucket"] = df["engagement_minutes"].apply(assign_bucket)

    # Count users per date per bucket
    grouped = (
        df.groupby(["visit_date", "bucket"])
        ["user_pseudo_id"].nunique()
        .reset_index()
        .rename(columns={"user_pseudo_id": "user_count"})
    )

    # Ensure all buckets exist for each date
    all_dates = sorted(df["visit_date"].unique())
    bucket_labels = [label for _, _, label in BUCKETS]
    complete_index = pd.MultiIndex.from_product([all_dates, bucket_labels], names=["visit_date", "bucket"])
    grouped = grouped.set_index(["visit_date", "bucket"]).reindex(complete_index, fill_value=0).reset_index()

    return grouped


def save_text_report(grouped: pd.DataFrame, prefix: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{prefix}_{ts}.txt"

    bucket_labels = [label for _, _, label in BUCKETS]

    with open(path, "w", encoding="utf-8") as f:
        f.write("User Time Buckets (counts of unique users per day)\n")
        f.write("=" * 60 + "\n\n")

        for visit_date in sorted(grouped["visit_date"].unique()):
            f.write(f"Date: {visit_date}\n")
            df_date = grouped[grouped["visit_date"] == visit_date]
            for label in bucket_labels:
                count = int(df_date[df_date["bucket"] == label]["user_count"].values[0])
                f.write(f"  {label:<8}: {count}\n")
            f.write("\n")

        f.write("Overall totals:\n")
        totals = grouped.groupby("bucket")["user_count"].sum()
        for label in bucket_labels:
            f.write(f"  {label:<8}: {int(totals.get(label, 0))}\n")
    return path


def save_bar_chart(grouped: pd.DataFrame, prefix: str) -> str:
    if grouped.empty:
        print("No data available to plot.")
        return ""

    totals = grouped.groupby("bucket")["user_count"].sum().reindex([label for _, _, label in BUCKETS], fill_value=0)

    plt.figure(figsize=(10, 6))
    bars = plt.bar(totals.index, totals.values, color="#4F81BD")
    plt.title("Users by Time-on-Site Buckets")
    plt.xlabel("Time Spent on Site (per day)")
    plt.ylabel("Unique Users")
    plt.grid(axis="y", linestyle="--", alpha=0.5)

    max_height = totals.values.max() if len(totals.values) > 0 else 0
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, height + max_height * 0.01, f"{int(height)}", ha="center", va="bottom")

    plt.tight_layout()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{prefix}_{ts}.png"
    plt.savefig(path)
    plt.close()
    return path


def main():
    parser = build_parser()
    args = parser.parse_args()

    print(f"Querying BigQuery for {args.start_date} to {args.end_date}...")
    df = run_query(args.project, args.dataset, args.timezone, args.start_date, args.end_date)

    if df.empty:
        print("No engagement data found for the specified date range.")
        return

    grouped = bucketize(df)

    txt_path = save_text_report(grouped, args.prefix)
    print(f"\nSaved text report to {txt_path}")

    chart_path = save_bar_chart(grouped, args.prefix)
    if chart_path:
        print(f"Saved bar chart to {chart_path}")


if __name__ == "__main__":
    main()
