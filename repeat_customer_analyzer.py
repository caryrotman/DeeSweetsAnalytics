#!/usr/bin/env python3
"""
Repeat Customer Analyzer for GA4 BigQuery Export

Finds users who spent more than a threshold number of minutes on the site in a single day
and then returned on a different day within the specified range. Outputs detailed
information including demographics and key events.

Requirements:
  - google-cloud-bigquery
  - pandas

Usage example:
  python repeat_customer_analyzer.py \
      --start-date 2025-11-02 \
      --end-date 2025-11-06 \
      --project websitecountryspikes \
      --dataset analytics_427048881 \
      --timezone America/Los_Angeles \
      --min-minutes 3 \
      --min-days 2
"""

import argparse
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

DEFAULT_PROJECT = os.getenv("GCP_PROJECT", "websitecountryspikes")
DEFAULT_DATASET = os.getenv("GA_DATASET_ID", "analytics_427048881")
DEFAULT_TZ = os.getenv("GA_TZ", "America/Los_Angeles")


def build_parser():
    parser = argparse.ArgumentParser(description="Find repeat visitors with high engagement from GA4 BigQuery export")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project ID")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="GA4 BigQuery dataset ID")
    parser.add_argument("--timezone", default=DEFAULT_TZ, help="Timezone for date conversion (IANA identifier)")
    parser.add_argument("--start-date", default="2025-11-02", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2025-11-06", help="End date (YYYY-MM-DD)")
    parser.add_argument("--min-minutes", type=float, default=3.0, help="Minimum minutes per day threshold (default 3)")
    parser.add_argument("--min-days", type=int, default=2, help="Minimum number of different days meeting threshold (default 2)")
    parser.add_argument("--prefix", default="repeat_customers", help="Prefix for output files")
    return parser


def run_query(project: str, dataset: str, timezone: str, start_date: str, end_date: str,
              min_minutes: float, min_days: int) -> pd.DataFrame:
    client = bigquery.Client(project=project)

    min_msec = int(min_minutes * 60 * 1000)

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
    ),

    high_days AS (
      SELECT
        visit_date,
        user_pseudo_id,
        engagement_time_msec / 1000.0 / 60.0 AS engagement_minutes
      FROM user_daily_engagement
      WHERE engagement_time_msec >= {min_msec}
    ),

    users_multi_days AS (
      SELECT
        user_pseudo_id,
        ARRAY_AGG(visit_date ORDER BY visit_date) AS visit_dates,
        COUNT(*) AS qualifying_days,
        SUM(engagement_minutes) AS total_qualifying_minutes
      FROM high_days
      GROUP BY user_pseudo_id
      HAVING COUNT(*) >= {min_days}
    )

    SELECT
      u.user_pseudo_id,
      u.qualifying_days,
      u.total_qualifying_minutes,
      ARRAY_TO_STRING(ARRAY(SELECT CAST(date_value AS STRING) FROM UNNEST(u.visit_dates) AS date_value), ', ') AS visit_dates,
      STRING_AGG(DISTINCT IF(geo.country IS NULL, NULL, geo.country), ', ') AS countries,
      STRING_AGG(DISTINCT IF(geo.region IS NULL, NULL, geo.region), ', ') AS regions,
      STRING_AGG(DISTINCT IF(geo.city IS NULL, NULL, geo.city), ', ') AS cities,
      STRING_AGG(DISTINCT IF(device.category IS NULL, NULL, device.category), ', ') AS device_categories,
      STRING_AGG(DISTINCT IF(device.operating_system IS NULL, NULL, device.operating_system), ', ') AS operating_systems,
      STRING_AGG(DISTINCT IF(traffic_source.source IS NULL, NULL, traffic_source.source), ', ') AS traffic_sources,
      STRING_AGG(DISTINCT IF(traffic_source.medium IS NULL, NULL, traffic_source.medium), ', ') AS traffic_mediums,
      STRING_AGG(DISTINCT IF(event_name IS NULL, NULL, event_name), ', ') AS events_triggered,
      STRING_AGG(DISTINCT CASE WHEN IFNULL((SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key='ga_is_conversion_event'), 0) = 1 THEN event_name END, ', ') AS conversion_events
    FROM users_multi_days u
    JOIN `{project}.{dataset}.events_*` e
      ON e.user_pseudo_id = u.user_pseudo_id
    WHERE e._TABLE_SUFFIX BETWEEN REPLACE('{start_date}', '-', '') AND REPLACE('{end_date}', '-', '')
    GROUP BY 1,2,3,4
    ORDER BY u.total_qualifying_minutes DESC
    """

    job = client.query(query)
    return job.result().to_dataframe(create_bqstorage_client=True)


def save_outputs(df: pd.DataFrame, prefix: str) -> tuple[str, str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"{prefix}_{ts}.csv"
    txt_path = f"{prefix}_{ts}.txt"

    if df.empty:
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("No repeat high-engagement users found for the specified criteria.\n")
        # create empty csv with headers
        pd.DataFrame(columns=[
            "user_pseudo_id", "qualifying_days", "total_qualifying_minutes", "visit_dates",
            "countries", "regions", "cities", "device_categories", "operating_systems",
            "traffic_sources", "traffic_mediums", "events_triggered", "conversion_events"
        ]).to_csv(csv_path, index=False)
        return csv_path, txt_path

    df.to_csv(csv_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("Repeat High-Engagement Customers\n")
        f.write("=" * 80 + "\n\n")
        for _, row in df.iterrows():
            f.write(f"User ID: {row['user_pseudo_id']}\n")
            f.write(f"  Qualifying days: {row['qualifying_days']}\n")
            f.write(f"  Total qualifying minutes: {row['total_qualifying_minutes']:.2f}\n")
            f.write(f"  Visit dates: {row['visit_dates']}\n")
            f.write(f"  Countries: {row['countries'] or 'N/A'}\n")
            f.write(f"  Regions: {row['regions'] or 'N/A'}\n")
            f.write(f"  Cities: {row['cities'] or 'N/A'}\n")
            f.write(f"  Device categories: {row['device_categories'] or 'N/A'}\n")
            f.write(f"  Operating systems: {row['operating_systems'] or 'N/A'}\n")
            f.write(f"  Traffic sources: {row['traffic_sources'] or 'N/A'}\n")
            f.write(f"  Traffic mediums: {row['traffic_mediums'] or 'N/A'}\n")
            f.write(f"  Events triggered: {row['events_triggered'] or 'N/A'}\n")
            f.write(f"  Conversion events: {row['conversion_events'] or 'None'}\n")
            f.write("-" * 80 + "\n")

    return csv_path, txt_path


def main():
    parser = build_parser()
    args = parser.parse_args()

    print(f"Analyzing repeat customers from {args.start_date} to {args.end_date}...")
    df = run_query(
        project=args.project,
        dataset=args.dataset,
        timezone=args.timezone,
        start_date=args.start_date,
        end_date=args.end_date,
        min_minutes=args.min_minutes,
        min_days=args.min_days
    )

    csv_path, txt_path = save_outputs(df, args.prefix)

    if df.empty:
        print("No repeat customers found matching the criteria.")
    else:
        print(f"Found {len(df)} repeat customers.")
    print(f"Saved CSV -> {csv_path}")
    print(f"Saved TXT -> {txt_path}")


if __name__ == "__main__":
    main()
