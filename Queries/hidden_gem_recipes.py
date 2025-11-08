#!/usr/bin/env python3
"""
Low-Traffic, High-Engagement “Hidden Gem” Recipes

Identifies recipe pages with relatively few views but strong engagement metrics.
Useful for finding content worth boosting via SEO, email, or social.

Example:
  python hidden_gem_recipes.py --property-id 427048881 \
      --service-account-key websitecountryspikes-c44a6b026c7b.json \
      --start-date 2025-10-01 --end-date 2025-10-31 \
      --max-views 500 --min-engagement-rate 0.55 --min-engagement-seconds 120
"""

import argparse
from typing import Optional

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, OrderBy, RunReportRequest

QUERY_NAME = "Hidden Gem Recipes"
RECOMMENDED_CHART = "Scatter plot of engagement vs views or table sorted by engagement"

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
            Metric(name="screenPageViews"),
            Metric(name="engagementRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="engagedSessions"),
        ],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="engagementRate"), desc=True)],
        limit=limit,
    )
    return client.run_report(request)


def main():
    parser = argparse.ArgumentParser(description="Find low-traffic, high-engagement recipe pages")
    parser.add_argument("--property-id", required=True, help="GA4 property ID")
    parser.add_argument("--service-account-key", help="Path to service account JSON key")
    parser.add_argument("--start-date", default="30daysAgo", help="Start date (YYYY-MM-DD or relative)")
    parser.add_argument("--end-date", default="yesterday", help="End date (YYYY-MM-DD or relative)")
    parser.add_argument("--limit", type=int, default=200, help="Number of pages to fetch before filtering")
    parser.add_argument("--max-views", type=int, default=600, help="Maximum views to consider low traffic")
    parser.add_argument("--min-engagement-rate", type=float, default=0.50, help="Minimum engagement rate (0-1)")
    parser.add_argument("--min-engagement-seconds", type=float, default=90.0, help="Minimum average engagement time in seconds")

    args = parser.parse_args()

    client = create_client(args.service_account_key)
    response = fetch_report(client, args.property_id, args.start_date, args.end_date, args.limit)

    print(f"Query: {QUERY_NAME}")
    print(f"Recommended visualization: {RECOMMENDED_CHART}")
    print(f"Date range: {args.start_date} -> {args.end_date}")
    print(
        f"Hidden gem recipes ({args.start_date} to {args.end_date}) "
        f"[views <= {args.max_views}, engagement rate >= {args.min_engagement_rate}, "
        f"avg engagement >= {args.min_engagement_seconds}s]"
    )
    print("=" * 120)
    print(f"{'Views':>8} {'Engagement Rate':>17} {'Avg Eng Time':>15} {'Engaged Sessions':>18} {'Page URL':<60}")
    print("-" * 120)

    gems = []
    for row in response.rows:
        views = int(row.metric_values[0].value or 0)
        engagement_rate = float(row.metric_values[1].value or 0)
        avg_session_duration = float(row.metric_values[2].value or 0)
        engaged_sessions = int(row.metric_values[3].value or 0)
        page_url = row.dimension_values[0].value or "(not set)"

        if (
            views <= args.max_views
            and engagement_rate >= args.min_engagement_rate
            and avg_session_duration >= args.min_engagement_seconds
        ):
            gems.append((views, engagement_rate, avg_session_duration, engaged_sessions, page_url))

    if not gems:
        print("No pages met the criteria.")
        return

    gems.sort(key=lambda x: (x[1], x[2]), reverse=True)

    for views, engagement_rate, avg_session_duration, engaged_sessions, page_url in gems:
        minutes, seconds = divmod(int(round(avg_session_duration)), 60)
        print(
            f"{views:>8} {engagement_rate:>17.2%} {minutes:02d}:{seconds:02d}"
            f" {engaged_sessions:>18} {page_url[:60]}"
        )


if __name__ == "__main__":
    main()
