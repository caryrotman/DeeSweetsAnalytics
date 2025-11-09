#!/usr/bin/env python3
"""
Recipe Category Performance Analyzer

Summarizes engagement and monetization metrics per recipe category.
Accepts a custom dimension name for the category grouping (e.g.,
`customEvent:recipe_category` or a registered scope dimension).

Example:
  python category_performance.py --property-id 427048881 \
      --service-account-key websitecountryspikes-c44a6b026c7b.json \
      --category-dimension customEvent:recipe_category \
      --start-date 2025-10-01 --end-date 2025-10-31
"""

import argparse
from datetime import datetime
from typing import Optional

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, OrderBy, RunReportRequest

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - optional dependency
    plt = None

QUERY_NAME = "Recipe Category Performance"
RECOMMENDED_CHART = "Stacked bar or grouped bar chart per category"

def create_client(service_account_key: Optional[str]) -> BetaAnalyticsDataClient:
    if service_account_key:
        from google.oauth2 import service_account

        credentials = service_account.Credentials.from_service_account_file(
            service_account_key,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        return BetaAnalyticsDataClient(credentials=credentials)
    return BetaAnalyticsDataClient()


def fetch_report(
    client: BetaAnalyticsDataClient,
    property_id: str,
    category_dimension: str,
    start_date: str,
    end_date: str,
    limit: int,
):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=category_dimension)],
        metrics=[
            Metric(name="engagedSessions"),
            Metric(name="averageSessionDuration"),
            Metric(name="screenPageViews"),
            Metric(name="totalAdRevenue"),
        ],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalAdRevenue"), desc=True)],
        limit=limit,
    )
    return client.run_report(request)


def save_chart(categories, revenues, sessions, dimension_name, start_date, end_date):
    if plt is None or not categories:
        return ""

    indices = range(len(categories))
    fig, ax1 = plt.subplots(figsize=(12, 6))

    ax1.bar(indices, revenues, color="#5B4B8A", label="Total Ad Revenue ($)")
    ax1.set_ylabel("Total Ad Revenue ($)")
    ax1.set_ylim(bottom=0)

    ax2 = ax1.twinx()
    ax2.plot(indices, sessions, color="#FF8A3D", marker="o", label="Engaged Sessions")
    ax2.set_ylabel("Engaged Sessions")
    ax2.set_ylim(bottom=0)

    ax1.set_xticks(indices)
    ax1.set_xticklabels(categories, rotation=45, ha="right")
    ax1.set_title(
        f"Recipe Category Performance\nDimension: {dimension_name}\n{start_date} to {end_date}",
        fontsize=14,
    )

    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left")

    plt.tight_layout()
    filename = f"recipe_category_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    fig.savefig(filename, dpi=150)
    plt.close(fig)
    return filename


def main():
    parser = argparse.ArgumentParser(description="Summarize recipe category performance from GA4")
    parser.add_argument("--property-id", required=True, help="GA4 property ID")
    parser.add_argument("--service-account-key", help="Path to service account JSON key")
    parser.add_argument(
        "--category-dimension",
        default="customEvent:recipe_category",
        help="GA4 dimension name representing the recipe category",
    )
    parser.add_argument("--start-date", default="30daysAgo", help="Start date (YYYY-MM-DD or relative)")
    parser.add_argument("--end-date", default="yesterday", help="End date (YYYY-MM-DD or relative)")
    parser.add_argument("--limit", type=int, default=50, help="Maximum number of categories to display")

    args = parser.parse_args()

    client = create_client(args.service_account_key)

    response = fetch_report(
        client,
        args.property_id,
        args.category_dimension,
        args.start_date,
        args.end_date,
        args.limit,
    )

    print(f"Query: {QUERY_NAME}")
    print(f"Recommended visualization: {RECOMMENDED_CHART}")
    print(f"Date range: {args.start_date} -> {args.end_date}")
    print(
        f"Recipe category performance ({args.start_date} to {args.end_date})\n"
        f"Dimension: {args.category_dimension}"
    )
    print("=" * 120)
    print(
        f"{'Category':<40} {'Engaged Sessions':>18} {'Avg Engagement':>15} "
        f"{'Views':>12} {'Ad Revenue':>12}"
    )
    print("-" * 120)

    if not response.rows:
        print("No data returned.")
        return

    categories = []
    revenues = []
    sessions = []

    for row in response.rows:
        category = row.dimension_values[0].value or "(not set)"
        engaged_sessions = int(row.metric_values[0].value or 0)
        avg_session_duration = float(row.metric_values[1].value or 0)
        views = int(row.metric_values[2].value or 0)
        revenue = float(row.metric_values[3].value or 0)
        minutes, seconds = divmod(int(round(avg_session_duration)), 60)
        print(
            f"{category[:40]:<40} {engaged_sessions:>18} {minutes:02d}:{seconds:02d}"
            f" {views:>12} ${revenue:>10,.2f}"
        )
        categories.append(category[:40])
        revenues.append(revenue)
        sessions.append(engaged_sessions)

    chart_path = save_chart(categories, revenues, sessions, args.category_dimension, args.start_date, args.end_date)
    if chart_path:
        print(f"Saved bar chart to {chart_path}")
    elif plt is None:
        print("matplotlib not installed; skipping chart generation.")
    else:
        print("No chart was produced; insufficient numeric data.")


if __name__ == "__main__":
    main()
