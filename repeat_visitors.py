#!/usr/bin/env python3
"""
Repeat Visitors Analyzer for GA4
- Finds users who have visited the site multiple times
- Supports both BigQuery and GA4 Reporting API
- Can filter by date range and minimum visit count
- Outputs to CSV and text file

Usage:
  # Use GA4 API (recommended for historical data)
  python repeat_visitors.py --use-api --weeks 3 --property-id 427048881
  
  # Use BigQuery (only recent data)
  python repeat_visitors.py --weeks 3 --min-visits 2
  
  # Custom date range
  python repeat_visitors.py --use-api --start-date 2025-10-16 --end-date 2025-11-06 --min-visits 3
"""

import argparse
import os
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
import pandas as pd

DEFAULT_PROJECT = os.getenv("GCP_PROJECT", "websitecountryspikes")
DEFAULT_DATASET = os.getenv("GA_DATASET_ID", "analytics_427048881")
DEFAULT_TZ = os.getenv("GA_TZ", "America/Los_Angeles")
DEFAULT_WEEKS = int(os.getenv("GA_WEEKS", "3"))
DEFAULT_MIN_VISITS = int(os.getenv("GA_MIN_VISITS", "2"))
DEFAULT_PROPERTY_ID = os.getenv("GA_PROPERTY_ID", "")

SQL_TEMPLATE = """
DECLARE tz STRING DEFAULT '{tz}';

WITH user_sessions AS (
  SELECT
    user_pseudo_id,
    DATE(TIMESTAMP_MICROS(event_timestamp), tz) AS visit_date,
    COUNT(DISTINCT CONCAT(
      CAST(user_pseudo_id AS STRING),
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS sessions_per_day
  FROM `{project}.{dataset}.events_*`
  WHERE DATE(TIMESTAMP_MICROS(event_timestamp), tz) >= '{start_date}'
    AND DATE(TIMESTAMP_MICROS(event_timestamp), tz) <= '{end_date}'
  GROUP BY user_pseudo_id, visit_date
),

user_visit_counts AS (
  SELECT
    user_pseudo_id,
    COUNT(DISTINCT visit_date) AS total_visits,
    SUM(sessions_per_day) AS total_sessions,
    MIN(visit_date) AS first_visit,
    MAX(visit_date) AS last_visit,
    DATE_DIFF(MAX(visit_date), MIN(visit_date), DAY) AS days_between_first_last
  FROM user_sessions
  GROUP BY user_pseudo_id
)

SELECT
  user_pseudo_id,
  total_visits,
  total_sessions,
  first_visit,
  last_visit,
  days_between_first_last
FROM user_visit_counts
WHERE total_visits >= {min_visits}
ORDER BY total_visits DESC, total_sessions DESC;
"""

def query_ga4_api(property_id: str, start_date: str, end_date: str, service_account_key: str = None):
    """Query GA4 Reporting API for repeat visitors using userId dimension."""
    try:
        return query_ga4_api_grpc(property_id, start_date, end_date, service_account_key)
    except ImportError:
        pass
    except Exception as e:
        print(f"gRPC method failed ({e}), trying REST API...")
    
    # Fall back to REST API
    try:
        import requests
    except ImportError:
        raise ImportError("requests library required. Install with: pip install requests")
    return query_ga4_api_rest(property_id, start_date, end_date, service_account_key)

def query_ga4_api_grpc(property_id: str, start_date: str, end_date: str, service_account_key: str = None):
    """Query using gRPC client (preferred method).
    
    Note: GA4 Reporting API doesn't support userId dimension directly.
    We'll use sessionId and date to track repeat sessions, but this won't
    identify individual users across sessions like BigQuery can.
    """
    from google.analytics.data import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
    from google.oauth2 import service_account
    
    # Use service account if provided, otherwise use ADC
    if service_account_key:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_key,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        client = BetaAnalyticsDataClient(credentials=credentials)
    else:
        client = BetaAnalyticsDataClient()
    
    # Try userId first, but it may not be available
    # If it fails, we'll use a different approach
    try:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[
                Dimension(name="date"),
                Dimension(name="userId"),
            ],
            metrics=[
                Metric(name="sessions"),
            ],
        )
        
        response = client.run_report(request)
        
        rows = []
        for row in response.rows:
            date_str = row.dimension_values[0].value
            user_id = row.dimension_values[1].value
            sessions = int(row.metric_values[0].value)
            
            # Skip rows with no userId (anonymous users)
            if not user_id or user_id == "(not set)":
                continue
            
            date_obj = datetime.strptime(date_str, "%Y%m%d").date()
            
            rows.append({
                'visit_date': date_obj,
                'user_id': user_id,
                'sessions': sessions,
            })
        
        return pd.DataFrame(rows)
    except Exception as e:
        error_str = str(e).lower()
        if 'userId' in error_str or 'not a valid dimension' in error_str:
            raise ValueError(
                "GA4 Reporting API doesn't support 'userId' dimension for tracking repeat visitors.\n"
                "The API has limited user-level dimensions compared to BigQuery.\n"
                "For repeat visitor analysis, please use BigQuery (remove --use-api flag).\n"
                "BigQuery has access to user_pseudo_id which can track all users, not just logged-in ones."
            )
        raise

def query_ga4_api_rest(property_id: str, start_date: str, end_date: str, service_account_key: str = None):
    """Query GA4 Reporting API using REST API."""
    import requests
    from google.auth import default
    from google.auth.transport.requests import Request as AuthRequest
    from google.oauth2 import service_account
    
    # Use service account if provided, otherwise use ADC
    if service_account_key:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_key,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        credentials.refresh(AuthRequest())
    else:
        credentials, project = default(scopes=['https://www.googleapis.com/auth/analytics.readonly'])
        if not credentials.valid or credentials.expired:
            credentials.refresh(AuthRequest())
    
    url = f"https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
    headers = {
        'Authorization': f'Bearer {credentials.token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "dimensions": [{"name": "date"}, {"name": "userId"}],
        "metrics": [{"name": "sessions"}]
    }
    
    response = requests.post(url, json=payload, headers=headers, timeout=60)
    
    if response.status_code == 400:
        error_data = response.json() if response.text else {}
        error_str = str(error_data).lower()
        if 'userId' in error_str or 'not a valid dimension' in error_str or 'did you mean' in error_str:
            raise ValueError(
                "GA4 Reporting API doesn't support 'userId' dimension for tracking repeat visitors.\n"
                "The API has limited user-level dimensions compared to BigQuery.\n"
                "For repeat visitor analysis, please use BigQuery (remove --use-api flag).\n"
                "BigQuery has access to user_pseudo_id which can track all users, not just logged-in ones."
            )
    
    response.raise_for_status()
    data = response.json()
    
    rows = []
    for row in data.get('rows', []):
        date_str = row['dimensionValues'][0]['value']
        user_id = row['dimensionValues'][1]['value']
        sessions = int(row['metricValues'][0]['value'])
        
        if not user_id or user_id == "(not set)":
            continue
        
        date_obj = datetime.strptime(date_str, "%Y%m%d").date()
        rows.append({
            'visit_date': date_obj,
            'user_id': user_id,
            'sessions': sessions,
        })
    
    return pd.DataFrame(rows)

def process_api_data(df: pd.DataFrame, min_visits: int):
    """Process API data to calculate repeat visitor statistics."""
    if df.empty:
        return df
    
    # Group by user_id to calculate visit counts
    user_stats = df.groupby('user_id').agg({
        'visit_date': ['min', 'max', 'nunique'],
        'sessions': 'sum'
    }).reset_index()
    
    user_stats.columns = ['user_id', 'first_visit', 'last_visit', 'total_visits', 'total_sessions']
    user_stats['days_between_first_last'] = (user_stats['last_visit'] - user_stats['first_visit']).dt.days
    
    # Filter by minimum visits
    user_stats = user_stats[user_stats['total_visits'] >= min_visits]
    user_stats = user_stats.sort_values(['total_visits', 'total_sessions'], ascending=[False, False])
    
    return user_stats

def build_parser():
    p = argparse.ArgumentParser(description="Find repeat visitors from GA4 data")
    p.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project ID")
    p.add_argument("--dataset", default=DEFAULT_DATASET, help="GA4 BigQuery dataset ID")
    p.add_argument("--timezone", default=DEFAULT_TZ, help="IANA timezone")
    p.add_argument("--property-id", default=DEFAULT_PROPERTY_ID, help="GA4 Property ID (for API access)")
    p.add_argument("--use-api", action="store_true", help="Use GA4 Reporting API instead of BigQuery")
    p.add_argument("--weeks", type=int, default=DEFAULT_WEEKS, help="Number of weeks to look back (default: 3)")
    p.add_argument("--start-date", help="Start date (YYYY-MM-DD). If not provided, uses --weeks from today")
    p.add_argument("--end-date", help="End date (YYYY-MM-DD). Defaults to today")
    p.add_argument("--min-visits", type=int, default=DEFAULT_MIN_VISITS, 
                   help="Minimum number of visits to include (default: 2)")
    p.add_argument("--service-account-key", help="Path to service account JSON key file (for API access)")
    return p

def run_query(client: bigquery.Client, sql: str):
    job = client.query(sql)
    return job.result().to_dataframe(create_bqstorage_client=True)

def save_results(df: pd.DataFrame, prefix="repeat_visitors"):
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d_%H%M%S")
    
    if df.empty:
        # Create empty files with headers
        csv_path = f"{prefix}_{ts}.csv"
        txt_path = f"{prefix}_{ts}.txt"
        with open(csv_path, 'w') as f:
            f.write("user_id,total_visits,total_sessions,first_visit,last_visit,days_between_first_last\n")
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write("user_id\ttotal_visits\ttotal_sessions\tfirst_visit\tlast_visit\tdays_between\n")
        return csv_path, txt_path
    
    # Save CSV
    csv_path = f"{prefix}_{ts}.csv"
    df.to_csv(csv_path, index=False)
    
    # Save text file
    txt_path = f"{prefix}_{ts}.txt"
    user_id_col = 'user_id' if 'user_id' in df.columns else 'user_pseudo_id'
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(f"{user_id_col}\ttotal_visits\ttotal_sessions\tfirst_visit\tlast_visit\tdays_between\n")
        for _, row in df.iterrows():
            user_id = row.get('user_id') or row.get('user_pseudo_id', '')
            f.write(f"{user_id}\t{row['total_visits']}\t{row['total_sessions']}\t"
                   f"{row['first_visit']}\t{row['last_visit']}\t{row['days_between_first_last']}\n")
    
    return csv_path, txt_path

def main():
    args = build_parser().parse_args()
    
    # Calculate date range
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        end_date = datetime.now().date()
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start_date = end_date - timedelta(weeks=args.weeks)
    
    print(f"Analyzing repeat visitors from {start_date} to {end_date}...")
    print(f"Minimum visits: {args.min_visits}")
    
    df = None
    
    if args.use_api:
        if not args.property_id:
            print("ERROR: --property-id required when using --use-api")
            print("Set GA_PROPERTY_ID env var or use --property-id flag")
            return
        
        service_account_key = args.service_account_key or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if service_account_key and not os.path.exists(service_account_key):
            print(f"ERROR: Service account key file not found: {service_account_key}")
            return
        
        # Try to find service account key in current directory
        if not service_account_key:
            potential_keys = [f for f in os.listdir('.') if f.endswith('.json') and 'service' in f.lower() or 'websitecountryspikes' in f.lower()]
            if potential_keys:
                service_account_key = potential_keys[0]
                print(f"Using service account key: {service_account_key}")
        
        print(f"Querying GA4 API from {start_date} to {end_date}...")
        
        try:
            df_raw = query_ga4_api(args.property_id, start_date.strftime("%Y-%m-%d"), 
                                   end_date.strftime("%Y-%m-%d"), service_account_key)
            df = process_api_data(df_raw, args.min_visits)
        except ValueError as e:
            print(f"\n❌ {e}")
            print("\nSwitching to BigQuery for repeat visitor analysis...")
            print("(BigQuery has access to user_pseudo_id for all users)")
            args.use_api = False
    
    if not args.use_api:
        # Use BigQuery
        client = bigquery.Client(project=args.project)
        
        sql = SQL_TEMPLATE.format(
            tz=args.timezone,
            project=args.project,
            dataset=args.dataset,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            min_visits=args.min_visits,
        )
        
        print(f"Querying BigQuery from {start_date} to {end_date}...")
        df = run_query(client, sql)
    
    if df is None or df.empty:
        print(f"\nNo users found with {args.min_visits} or more visits in this period.")
        if args.use_api:
            print("\nNote: GA4 API only tracks logged-in users. If most users are anonymous,")
            print("      consider using BigQuery (remove --use-api flag) to see all users.")
    else:
        print(f"\nFound {len(df)} users with {args.min_visits}+ visits:")
        print(f"  - Total visits: {df['total_visits'].sum():,}")
        print(f"  - Total sessions: {df['total_sessions'].sum():,}")
        print(f"  - Average visits per user: {df['total_visits'].mean():.1f}")
        print(f"  - Max visits by single user: {df['total_visits'].max()}")
        
        print("\nTop 20 repeat visitors:")
        print(df.head(20).to_string(index=False))
    
    csv_path, txt_path = save_results(df if df is not None and not df.empty else pd.DataFrame())
    print(f"\nSaved CSV → {csv_path}")
    print(f"Saved TXT → {txt_path}")

if __name__ == "__main__":
    main()

