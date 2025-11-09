#!/usr/bin/env python3
"""
Auto-generated query module.

Generated on 2025-11-08 17:18:13 by generate_query_module.py.

Query Name: Engagement of email-acquired users vs others
Recommended Visualization: Line chart over time

Original SQL:
    -- Engagement of email-acquired users vs others

    WITH first_touch AS (
      -- Identify each user's first session and its acquisition source
      SELECT
        user_pseudo_id,
        MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_session_ts,
        ANY_VALUE(traffic_source.source) AS first_source,
        ANY_VALUE(traffic_source.medium) AS first_medium,
        ANY_VALUE(traffic_source.name) AS first_campaign
      FROM `websitecountryspikes.analytics_427048881.events_*`
      WHERE
        _TABLE_SUFFIX BETWEEN '20251101' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
        AND event_name = 'session_start'
      GROUP BY user_pseudo_id
    ),

    all_sessions AS (
      -- All subsequent sessions (for lifetime behavior)
      SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        CONCAT(user_pseudo_id, '.', CAST(
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ) AS full_session_id,
        event_name,
        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), DAY) AS event_day
      FROM `websitecountryspikes.analytics_427048881.events_*`
      WHERE
        _TABLE_SUFFIX BETWEEN '20251101' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
        AND event_name IN ('session_start', 'page_view')
    ),

    session_rollup AS (
      SELECT
        full_session_id,
        user_pseudo_id,
        MIN(event_ts) AS session_start_ts,
        MAX(event_ts) AS session_end_ts,
        TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS session_duration_seconds,
        COUNTIF(event_name = 'page_view') AS pageviews
      FROM all_sessions
      WHERE full_session_id IS NOT NULL
      GROUP BY full_session_id, user_pseudo_id
    ),

    user_engagement AS (
      -- Aggregate user-level engagement metrics
      SELECT
        user_pseudo_id,
        COUNT(DISTINCT full_session_id) AS total_sessions,
        AVG(session_duration_seconds) AS avg_session_duration_seconds,
        SUM(pageviews) AS total_pageviews
      FROM session_rollup
      GROUP BY user_pseudo_id
    ),

    email_vs_non AS (
      SELECT
        CASE
          WHEN LOWER(first_source) LIKE '%email%' OR LOWER(first_medium) LIKE '%email%' THEN 'email_acquired'
          ELSE 'non_email'
        END AS user_group,
        COUNT(DISTINCT ue.user_pseudo_id) AS users,
        AVG(total_sessions) AS avg_sessions_per_user,
        AVG(avg_session_duration_seconds) AS avg_session_duration_seconds,
        AVG(total_pageviews) AS avg_total_pageviews
      FROM user_engagement ue
      JOIN first_touch ft
        ON ue.user_pseudo_id = ft.user_pseudo_id
      GROUP BY user_group
    )

    SELECT
      user_group,
      users,
      ROUND(avg_sessions_per_user, 2) AS avg_sessions_per_user,
      ROUND(avg_session_duration_seconds, 1) AS avg_session_duration_seconds,
      ROUND(avg_total_pageviews, 1) AS avg_total_pageviews
    FROM email_vs_non
    ORDER BY user_group;
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

QUERY_NAME = 'Engagement of email-acquired users vs others'
RECOMMENDED_CHART = 'Line chart over time'
SQL = "-- Engagement of email-acquired users vs others\n\nWITH first_touch AS (\n  -- Identify each user's first session and its acquisition source\n  SELECT\n    user_pseudo_id,\n    MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_session_ts,\n    ANY_VALUE(traffic_source.source) AS first_source,\n    ANY_VALUE(traffic_source.medium) AS first_medium,\n    ANY_VALUE(traffic_source.name) AS first_campaign\n  FROM `websitecountryspikes.analytics_427048881.events_*`\n  WHERE\n    _TABLE_SUFFIX BETWEEN '20251101' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\n    AND event_name = 'session_start'\n  GROUP BY user_pseudo_id\n),\n\nall_sessions AS (\n  -- All subsequent sessions (for lifetime behavior)\n  SELECT\n    user_pseudo_id,\n    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,\n    CONCAT(user_pseudo_id, '.', CAST(\n      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)\n    ) AS full_session_id,\n    event_name,\n    TIMESTAMP_MICROS(event_timestamp) AS event_ts,\n    TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), DAY) AS event_day\n  FROM `websitecountryspikes.analytics_427048881.events_*`\n  WHERE\n    _TABLE_SUFFIX BETWEEN '20251101' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\n    AND event_name IN ('session_start', 'page_view')\n),\n\nsession_rollup AS (\n  SELECT\n    full_session_id,\n    user_pseudo_id,\n    MIN(event_ts) AS session_start_ts,\n    MAX(event_ts) AS session_end_ts,\n    TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS session_duration_seconds,\n    COUNTIF(event_name = 'page_view') AS pageviews\n  FROM all_sessions\n  WHERE full_session_id IS NOT NULL\n  GROUP BY full_session_id, user_pseudo_id\n),\n\nuser_engagement AS (\n  -- Aggregate user-level engagement metrics\n  SELECT\n    user_pseudo_id,\n    COUNT(DISTINCT full_session_id) AS total_sessions,\n    AVG(session_duration_seconds) AS avg_session_duration_seconds,\n    SUM(pageviews) AS total_pageviews\n  FROM session_rollup\n  GROUP BY user_pseudo_id\n),\n\nemail_vs_non AS (\n  SELECT\n    CASE\n      WHEN LOWER(first_source) LIKE '%email%' OR LOWER(first_medium) LIKE '%email%' THEN 'email_acquired'\n      ELSE 'non_email'\n    END AS user_group,\n    COUNT(DISTINCT ue.user_pseudo_id) AS users,\n    AVG(total_sessions) AS avg_sessions_per_user,\n    AVG(avg_session_duration_seconds) AS avg_session_duration_seconds,\n    AVG(total_pageviews) AS avg_total_pageviews\n  FROM user_engagement ue\n  JOIN first_touch ft\n    ON ue.user_pseudo_id = ft.user_pseudo_id\n  GROUP BY user_group\n)\n\nSELECT\n  user_group,\n  users,\n  ROUND(avg_sessions_per_user, 2) AS avg_sessions_per_user,\n  ROUND(avg_session_duration_seconds, 1) AS avg_session_duration_seconds,\n  ROUND(avg_total_pageviews, 1) AS avg_total_pageviews\nFROM email_vs_non\nORDER BY user_group;"
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
        default='engagement-of-email-acquired-users-vs-others',
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
