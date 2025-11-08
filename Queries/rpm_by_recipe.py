#!/usr/bin/env python3
"""
Revenue Per Thousand Views (RPM) by Recipe Page

Pulls page-level metrics from the GA4 Data API and computes RPM as
(publisher ad revenue ÷ views) × 1000. Shows the highest RPM recipe pages.

Example:
  python rpm_by_recipe.py --property-id 427048881 \
      --service-account-key websitecountryspikes-c44a6b026c7b.json \
      --start-date 2025-10-01 --end-date 2025-10-31 --limit 20
"""

import argparse
import math
from typing import Optional

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, OrderBy, RunReportRequest

QUERY_NAME = "Recipe RPM Leaderboard"
RECOMMENDED_CHART = "Horizontal bar chart of RPM by recipe"

def create_client(service_account_key: Optional[str]) -> BetaAnalyticsDataClient:
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
            Metric(name="screenPageViews"),
            Metric(name="engagedSessions"),
        ],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalAdRevenue"), desc=True)],
        limit=limit,
    )
    return client.run_report(request)


def main():
    parser = argparse.ArgumentParser(description="Calculate RPM per recipe page from GA4")
    parser.add_argument("--property-id", required=True, help="GA4 property ID")
    parser.add_argument("--service-account-key", help="Path to service account JSON key")
    parser.add_argument("--start-date", default="30daysAgo", help="Start date (YYYY-MM-DD or relative)")
    parser.add_argument("--end-date", default="yesterday", help="End date (YYYY-MM-DD or relative)")
    parser.add_argument("--limit", type=int, default=50, help="Number of rows to fetch (default 50)")
    parser.add_argument("--min-views", type=int, default=100, help="Minimum page views required to be included")

    args = parser.parse_args()

    client = create_client(args.service_account_key)
    response = fetch_report(client, args.property_id, args.start_date, args.end_date, args.limit)

    print(f"Query: {QUERY_NAME}")
    print(f"Recommended visualization: {RECOMMENDED_CHART}")
    print(f"Date range: {args.start_date} -> {args.end_date}")
    print(f"RPM (Revenue per 1000 Views) by Recipe Page ({args.start_date} to {args.end_date})")
    print("=" * 110)
    print(f"{'Rank':<5} {'Page URL':<60} {'Views':>10} {'Revenue':>12} {'RPM':>10} {'Engaged Sessions':>18}")
    print("-" * 110)

    rows = []
    for row in response.rows:
        page_url = row.dimension_values[0].value or "(not set)"
        revenue = float(row.metric_values[0].value or 0)
        views = int(row.metric_values[1].value or 0)
        engaged_sessions = int(row.metric_values[2].value or 0)
        if views < args.min_views:
            continue
        rpm = revenue / views * 1000 if views else math.nan
        rows.append((page_url, views, revenue, rpm, engaged_sessions))

    if not rows:
        print("No pages met the minimum view threshold.")
        return

    rows.sort(key=lambda x: x[3], reverse=True)

    for idx, (page_url, views, revenue, rpm, engaged_sessions) in enumerate(rows, start=1):
        print(
            f"{idx:<5} {page_url[:60]:<60} {views:>10} "
            f"${revenue:>10,.2f} {rpm:>10.2f} {engaged_sessions:>18}"
        )


if __name__ == "__main__":
    main()
