#!/usr/bin/env python3
"""
Auto-generated query module.

Generated on 2025-11-08 15:31:38 by generate_query_module.py.

Query Name: Session duration distribution by source medium
Recommended Visualization: Line chart over time

Original SQL:
    -- Session duration distribution by source medium

    WITH session_events AS (
      SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,

        -- Unique session key
        CONCAT(
          user_pseudo_id, '.',
          CAST(
            (SELECT value.int_value
             FROM UNNEST(event_params)
             WHERE key = 'ga_session_id') AS STRING
          )
        ) AS full_session_id,

        -- GA4 traffic source fields
        traffic_source.source   AS source,
        traffic_source.medium   AS medium,
        traffic_source.name     AS campaign,

        TIMESTAMP_MICROS(event_timestamp) AS event_ts
      FROM `websitecountryspikes.analytics_427048881.events_*`
      WHERE
        -- Always from 2025-11-01 through today
        _TABLE_SUFFIX BETWEEN '20251101' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
        -- We only need events that define the span of the session
        AND event_name IN ('session_start', 'page_view')
    ),

    sessions AS (
      SELECT
        full_session_id,
        user_pseudo_id,
        ANY_VALUE(source)   AS source,
        ANY_VALUE(medium)   AS medium,
        ANY_VALUE(campaign) AS campaign,
        MIN(event_ts) AS session_start_ts,
        MAX(event_ts) AS session_end_ts,
        TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS session_duration_seconds
      FROM session_events
      WHERE full_session_id IS NOT NULL
      GROUP BY full_session_id, user_pseudo_id
    ),

    sessions_bucketed AS (
      SELECT
        COALESCE(source, '(direct/unknown)') AS source,
        COALESCE(medium, '(none)') AS medium,
        session_duration_seconds,
        CASE
          WHEN session_duration_seconds <   30 THEN '<30s'
          WHEN session_duration_seconds <   60 THEN '30-59s'
          WHEN session_duration_seconds <  120 THEN '1-1.9m'
          WHEN session_duration_seconds <  180 THEN '2-2.9m'
          WHEN session_duration_seconds <  300 THEN '3-4.9m'
          WHEN session_duration_seconds <  600 THEN '5-9.9m'
          WHEN session_duration_seconds < 1200 THEN '10-19.9m'
          WHEN session_duration_seconds < 1800 THEN '20-29.9m'
          WHEN session_duration_seconds < 3600 THEN '30-59.9m'
          ELSE '60m+'
        END AS duration_bucket,
        CASE
          WHEN session_duration_seconds <   30 THEN 1
          WHEN session_duration_seconds <   60 THEN 2
          WHEN session_duration_seconds <  120 THEN 3
          WHEN session_duration_seconds <  180 THEN 4
          WHEN session_duration_seconds <  300 THEN 5
          WHEN session_duration_seconds <  600 THEN 6
          WHEN session_duration_seconds < 1200 THEN 7
          WHEN session_duration_seconds < 1800 THEN 8
          WHEN session_duration_seconds < 3600 THEN 9
          ELSE 10
        END AS bucket_order
      FROM sessions
    )

    SELECT
      source,
      medium,
      duration_bucket,
      COUNT(*) AS sessions,
      AVG(session_duration_seconds) AS avg_session_duration_seconds
    FROM sessions_bucketed
    GROUP BY source, medium, duration_bucket, bucket_order
    ORDER BY source, medium, bucket_order;
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

QUERY_NAME = 'Session duration distribution by source medium'
RECOMMENDED_CHART = 'Line chart over time'
SQL = "-- Session duration distribution by source medium\n\nWITH session_events AS (\n  SELECT\n    user_pseudo_id,\n    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,\n\n    -- Unique session key\n    CONCAT(\n      user_pseudo_id, '.',\n      CAST(\n        (SELECT value.int_value\n         FROM UNNEST(event_params)\n         WHERE key = 'ga_session_id') AS STRING\n      )\n    ) AS full_session_id,\n\n    -- GA4 traffic source fields\n    traffic_source.source   AS source,\n    traffic_source.medium   AS medium,\n    traffic_source.name     AS campaign,\n\n    TIMESTAMP_MICROS(event_timestamp) AS event_ts\n  FROM `websitecountryspikes.analytics_427048881.events_*`\n  WHERE\n    -- Always from 2025-11-01 through today\n    _TABLE_SUFFIX BETWEEN '20251101' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\n    -- We only need events that define the span of the session\n    AND event_name IN ('session_start', 'page_view')\n),\n\nsessions AS (\n  SELECT\n    full_session_id,\n    user_pseudo_id,\n    ANY_VALUE(source)   AS source,\n    ANY_VALUE(medium)   AS medium,\n    ANY_VALUE(campaign) AS campaign,\n    MIN(event_ts) AS session_start_ts,\n    MAX(event_ts) AS session_end_ts,\n    TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS session_duration_seconds\n  FROM session_events\n  WHERE full_session_id IS NOT NULL\n  GROUP BY full_session_id, user_pseudo_id\n),\n\nsessions_bucketed AS (\n  SELECT\n    COALESCE(source, '(direct/unknown)') AS source,\n    COALESCE(medium, '(none)') AS medium,\n    session_duration_seconds,\n    CASE\n      WHEN session_duration_seconds <   30 THEN '<30s'\n      WHEN session_duration_seconds <   60 THEN '30-59s'\n      WHEN session_duration_seconds <  120 THEN '1-1.9m'\n      WHEN session_duration_seconds <  180 THEN '2-2.9m'\n      WHEN session_duration_seconds <  300 THEN '3-4.9m'\n      WHEN session_duration_seconds <  600 THEN '5-9.9m'\n      WHEN session_duration_seconds < 1200 THEN '10-19.9m'\n      WHEN session_duration_seconds < 1800 THEN '20-29.9m'\n      WHEN session_duration_seconds < 3600 THEN '30-59.9m'\n      ELSE '60m+'\n    END AS duration_bucket,\n    CASE\n      WHEN session_duration_seconds <   30 THEN 1\n      WHEN session_duration_seconds <   60 THEN 2\n      WHEN session_duration_seconds <  120 THEN 3\n      WHEN session_duration_seconds <  180 THEN 4\n      WHEN session_duration_seconds <  300 THEN 5\n      WHEN session_duration_seconds <  600 THEN 6\n      WHEN session_duration_seconds < 1200 THEN 7\n      WHEN session_duration_seconds < 1800 THEN 8\n      WHEN session_duration_seconds < 3600 THEN 9\n      ELSE 10\n    END AS bucket_order\n  FROM sessions\n)\n\nSELECT\n  source,\n  medium,\n  duration_bucket,\n  COUNT(*) AS sessions,\n  AVG(session_duration_seconds) AS avg_session_duration_seconds\nFROM sessions_bucketed\nGROUP BY source, medium, duration_bucket, bucket_order\nORDER BY source, medium, bucket_order;"
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
        default='session-duration-distribution-by-source-medium',
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


if __name__ == "__main__":
    main()
