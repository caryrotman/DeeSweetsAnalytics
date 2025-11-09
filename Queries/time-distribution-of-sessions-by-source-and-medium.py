#!/usr/bin/env python3
"""
Auto-generated query module.

Generated on 2025-11-08 15:20:53 by generate_query_module.py.

Query Name: Time distribution of sessions by source and medium
Recommended Visualization: Line chart over time

Original SQL:
    -- Time distribution of sessions by source and medium

    -- Replace:
    --   YOUR_PROJECT.YOUR_DATASET.events_*   with your actual table
    --   date range filter in _TABLE_SUFFIX   with your preferred time window

    WITH session_info AS (
      SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,

        -- Make a unique session key
        CONCAT(
          user_pseudo_id, '.',
          CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
        ) AS full_session_id,

        -- Get traffic source info
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign,

        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        event_name
      FROM `YOUR_PROJECT.YOUR_DATASET.events_*`
      WHERE
        _TABLE_SUFFIX BETWEEN '20251001' AND '20251031'
        AND event_name IN ('session_start', 'page_view')
    ),

    sessions AS (
      SELECT
        full_session_id,
        user_pseudo_id,
        MIN(event_ts) AS session_start_ts,
        MAX(event_ts) AS session_end_ts,
        TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS session_duration_seconds,
        ANY_VALUE(source) AS source,
        ANY_VALUE(medium) AS medium,
        ANY_VALUE(campaign) AS campaign
      FROM session_info
      GROUP BY full_session_id, user_pseudo_id
    ),

    hour_distribution AS (
      SELECT
        source,
        medium,
        EXTRACT(HOUR FROM session_start_ts) AS session_hour,
        COUNT(DISTINCT full_session_id) AS sessions
      FROM sessions
      GROUP BY source, medium, session_hour
    )

    SELECT
      source,
      medium,
      session_hour,
      sessions,
      ROUND(100 * sessions / SUM(sessions) OVER (PARTITION BY source, medium), 2) AS percent_within_source
    FROM hour_distribution
    ORDER BY source, medium, session_hour;
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

QUERY_NAME = 'Time distribution of sessions by source and medium'
RECOMMENDED_CHART = 'Line chart over time'
SQL = "-- Time distribution of sessions by source and medium\n\n-- Replace:\n--   YOUR_PROJECT.YOUR_DATASET.events_*   with your actual table\n--   date range filter in _TABLE_SUFFIX   with your preferred time window\n\nWITH session_info AS (\n  SELECT\n    user_pseudo_id,\n    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,\n\n    -- Make a unique session key\n    CONCAT(\n      user_pseudo_id, '.',\n      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)\n    ) AS full_session_id,\n\n    -- Get traffic source info\n    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source,\n    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium,\n    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign,\n\n    TIMESTAMP_MICROS(event_timestamp) AS event_ts,\n    event_name\n  FROM `YOUR_PROJECT.YOUR_DATASET.events_*`\n  WHERE\n    _TABLE_SUFFIX BETWEEN '20251001' AND '20251031'\n    AND event_name IN ('session_start', 'page_view')\n),\n\nsessions AS (\n  SELECT\n    full_session_id,\n    user_pseudo_id,\n    MIN(event_ts) AS session_start_ts,\n    MAX(event_ts) AS session_end_ts,\n    TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS session_duration_seconds,\n    ANY_VALUE(source) AS source,\n    ANY_VALUE(medium) AS medium,\n    ANY_VALUE(campaign) AS campaign\n  FROM session_info\n  GROUP BY full_session_id, user_pseudo_id\n),\n\nhour_distribution AS (\n  SELECT\n    source,\n    medium,\n    EXTRACT(HOUR FROM session_start_ts) AS session_hour,\n    COUNT(DISTINCT full_session_id) AS sessions\n  FROM sessions\n  GROUP BY source, medium, session_hour\n)\n\nSELECT\n  source,\n  medium,\n  session_hour,\n  sessions,\n  ROUND(100 * sessions / SUM(sessions) OVER (PARTITION BY source, medium), 2) AS percent_within_source\nFROM hour_distribution\nORDER BY source, medium, session_hour;"
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
        default='time-distribution-of-sessions-by-source-and-medium',
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
