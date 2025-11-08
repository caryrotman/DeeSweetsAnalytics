#!/usr/bin/env python3
"""
Top Revenue-Driving Recipe Pages

Fetches the top recipe URLs sorted by publisher ad revenue along with engaged sessions
and average engagement time. Uses the GA4 Data API. Requires the
`google-analytics-data` package and either a service account key JSON file or
Application Default Credentials (ADC).

Example:
  python top_revenue_recipes.py --property-id 427048881 \
      --service-account-key websitecountryspikes-c44a6b026c7b.json \
      --start-date 2025-10-01 --end-date 2025-10-31 --limit 20
"""

import argparse
import os
from datetime import datetime

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, OrderBy, RunReportRequest

QUERY_NAME = "Top Revenue Recipe Pages"
RECOMMENDED_CHART = "Horizontal bar chart showing revenue per page"

def create_client(service_account_key: str | None) -> BetaAnalyticsDataClient:
    if service_account_key:
        from google.oauth2 import service_account

        credentials = service_account.Credentials.from_service_account_file(
            service_account_key,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        return BetaAnalyticsDataClient(credentials=credentials)
    return BetaAnalyticsDataClient()


def fetch_report(client: BetaAnalyticsDataClient, property_id: str, start_date: str, end_date: str, limit: int):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="pagePathPlusQueryString")],
        metrics=[
            Metric(name="totalAdRevenue"),
            Metric(name="engagedSessions"),
            Metric(name="averageSessionDuration"),
        ],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalAdRevenue"), desc=True)],
        limit=limit,
    )
    return client.run_report(request)


def format_seconds(value: float) -> str:
    minutes, seconds = divmod(int(round(value)), 60)
    return f"{minutes:02d}:{seconds:02d}"


def main():
    parser = argparse.ArgumentParser(description="List top revenue-driving recipe pages from GA4")
    parser.add_argument("--property-id", required=True, help="GA4 property ID (numbers only)")
    parser.add_argument("--service-account-key", help="Path to service account JSON key (optional)")
    parser.add_argument("--start-date", default="30daysAgo", help="Start date (YYYY-MM-DD or GA4 relative like 30daysAgo)")
    parser.add_argument("--end-date", default="yesterday", help="End date (YYYY-MM-DD or GA4 relative like yesterday)")
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to return (default 20)")

    args = parser.parse_args()

    client = create_client(args.service_account_key)
    response = fetch_report(client, args.property_id, args.start_date, args.end_date, args.limit)

    print(f"Query: {QUERY_NAME}")
    print(f"Recommended visualization: {RECOMMENDED_CHART}")
    print(f"Date range: {args.start_date} -> {args.end_date}")
    print(f"Top revenue-driving recipe pages ({args.start_date} to {args.end_date})")
    print("=" * 100)
    print(f"{'Rank':<5} {'Page URL':<60} {'Revenue':>12} {'Engaged Sessions':>18} {'Avg Engagement':>16}")
    print("-" * 100)

    if not response.rows:
        print("No data returned.")
        return

    for idx, row in enumerate(response.rows, start=1):
        page_url = row.dimension_values[0].value or "(not set)"
        revenue = float(row.metric_values[0].value or 0)
        engaged_sessions = int(row.metric_values[1].value or 0)
        avg_session_seconds = float(row.metric_values[2].value or 0)

        print(
            f"{idx:<5} {page_url[:60]:<60} "
            f"${revenue:>10,.2f} {engaged_sessions:>18} {format_seconds(avg_session_seconds):>16}"
        )


if __name__ == "__main__":
    main()
