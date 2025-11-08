#!/usr/bin/env python3
"""
Country Weekly Views Report for GA4
Supports two data sources:
  1. BigQuery (raw events) - fastest, requires BigQuery export enabled
  2. GA4 Reporting API (aggregated) - can pull historical data

Usage:
  # BigQuery only (default)
  python country_spike_report.py --weeks 20

  # Use GA4 API for historical data (requires GA_PROPERTY_ID)
  python country_spike_report.py --use-api --weeks 20 --property-id GA4_PROPERTY_ID

  # Merge both (API for history, BigQuery for recent)
  python country_spike_report.py --use-api --merge-bq --weeks 20

Env vars:
  GCP_PROJECT, GA_DATASET_ID, GA_TZ, GA_WEEKS, GA_PROPERTY_ID
"""

import argparse
import os
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
# We'll use REST API directly with requests library instead of gRPC client
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

import pandas as pd

DEFAULT_PROJECT   = os.getenv("GCP_PROJECT", "websitecountryspikes")
DEFAULT_DATASET   = os.getenv("GA_DATASET_ID", "analytics_427048881")
DEFAULT_TZ        = os.getenv("GA_TZ", "America/Los_Angeles")
DEFAULT_WEEKS     = int(os.getenv("GA_WEEKS", "20"))
DEFAULT_PROPERTY_ID = os.getenv("GA_PROPERTY_ID", "")

SQL_TEMPLATE = """
DECLARE tz STRING DEFAULT '{tz}';

SELECT
  DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), tz), WEEK(MONDAY)) AS week_start,
  geo.country AS country,
  COUNT(DISTINCT user_pseudo_id) AS total_views
FROM `{project}.{dataset}.events_*`
WHERE geo.country IS NOT NULL
  AND geo.country != ''
  AND DATE(TIMESTAMP_MICROS(event_timestamp), tz)
      >= DATE_SUB(CURRENT_DATE(tz), INTERVAL {weeks} WEEK)
GROUP BY week_start, country
ORDER BY week_start DESC, total_views DESC;
"""

DATE_RANGE_SQL = """
DECLARE tz STRING DEFAULT '{tz}';

SELECT
  MIN(DATE(TIMESTAMP_MICROS(event_timestamp), tz)) AS earliest_date,
  MAX(DATE(TIMESTAMP_MICROS(event_timestamp), tz)) AS latest_date,
  COUNT(DISTINCT DATE(TIMESTAMP_MICROS(event_timestamp), tz)) AS distinct_days,
  COUNT(DISTINCT DATE_TRUNC(DATE(TIMESTAMP_MICROS(event_timestamp), tz), WEEK(MONDAY))) AS distinct_weeks,
  COUNT(*) AS total_events
FROM `{project}.{dataset}.events_*`
WHERE geo.country IS NOT NULL
  AND geo.country != '';
"""

def query_ga4_api(property_id: str, start_date: str, end_date: str, service_account_key: str = None):
    """Query GA4 Reporting API for country and user data by date."""
    # Try gRPC client first (better auth handling), fall back to REST
    try:
        return query_ga4_api_grpc(property_id, start_date, end_date, service_account_key)
    except ImportError:
        # gRPC library not available, use REST
        pass
    except Exception as e:
        print(f"gRPC method failed ({e}), trying REST API...")
    
    # Fall back to REST API
    if not REQUESTS_AVAILABLE:
        raise ImportError("requests library required. Install with: pip install requests")
    return query_ga4_api_rest(property_id, start_date, end_date, service_account_key)

def query_ga4_api_grpc(property_id: str, start_date: str, end_date: str, service_account_key: str = None):
    """Query using gRPC client (preferred method)."""
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
        # Create client - it will use ADC with proper scopes
        client = BetaAnalyticsDataClient()
    
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="country"),
        ],
        metrics=[
            Metric(name="activeUsers"),
        ],
    )
    
    response = client.run_report(request)
    
    rows = []
    for row in response.rows:
        date_str = row.dimension_values[0].value
        country = row.dimension_values[1].value
        users = int(row.metric_values[0].value)
        
        if country == "(not set)":
            continue
        
        date_obj = datetime.strptime(date_str, "%Y%m%d").date()
        days_since_monday = date_obj.weekday()
        week_start = date_obj - timedelta(days=days_since_monday)
        
        rows.append({
            'week_start': week_start,
            'country': country,
            'total_views': users,
            'date': date_obj
        })
    
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    
    df_weekly = df.groupby(['week_start', 'country'])['total_views'].sum().reset_index()
    return df_weekly

def query_ga4_api_rest(property_id: str, start_date: str, end_date: str, service_account_key: str = None):
    """Query GA4 Reporting API for country and user data by date using REST API."""
    if not REQUESTS_AVAILABLE:
        raise ImportError("requests library required. Install with: pip install requests")
    
    import requests
    from google.auth import default
    from google.auth.transport.requests import Request as AuthRequest
    from google.oauth2 import service_account
    
    try:
        # Use service account if provided, otherwise use ADC
        if service_account_key:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_key,
                scopes=['https://www.googleapis.com/auth/analytics.readonly']
            )
            credentials.refresh(AuthRequest())
        else:
            # Get credentials - explicitly request analytics scope
            # Note: This requires the stored credentials to have been created with this scope
            credentials, project = default(scopes=['https://www.googleapis.com/auth/analytics.readonly'])
        
        # Check if credentials have the required scope
        if hasattr(credentials, 'scopes') and credentials.scopes:
            print(f"Token scopes: {credentials.scopes}")
        
        # Always refresh to ensure we have a valid token
        if not credentials.valid or credentials.expired:
            credentials.refresh(AuthRequest())
        
        # Verify we have a token
        if not hasattr(credentials, 'token') or not credentials.token:
            raise ValueError("No access token available")
        
        # Make REST API call
        url = f"https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
        headers = {
            'Authorization': f'Bearer {credentials.token}',
            'Content-Type': 'application/json'
        }
        payload = {
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "dimensions": [{"name": "date"}, {"name": "country"}],
            "metrics": [{"name": "activeUsers"}]
        }
        
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        
        # Debug: print response for troubleshooting
        if response.status_code != 200:
            print(f"API Response Status: {response.status_code}")
            print(f"API Response: {response.text[:500]}")
        
        if response.status_code == 403:
            error_data = response.json() if response.text else {}
            error_str = str(error_data).lower()
            
            if 'insufficient authentication scopes' in error_str or 'access_token_scope_insufficient' in error_str:
                creds_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
                if os.name == 'nt':  # Windows
                    creds_path = os.path.join(os.getenv('APPDATA'), 'gcloud', 'application_default_credentials.json')
                
                print("\n" + "="*70)
                print("❌ Authentication scope issue detected")
                print("="*70)
                print("\nTo fix this, you need to re-authenticate with the Analytics scope:")
                print(f"\n1. Delete old credentials:")
                print(f'   del "{creds_path}"')
                print("\n2. Re-authenticate with proper scope:")
                print("   gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform")
                print("\n3. Then run this script again")
                print("\n" + "="*70 + "\n")
                
                raise PermissionError("Insufficient authentication scopes. See instructions above.")
            
            if 'api not enabled' in error_str or 'service not enabled' in error_str:
                print("\n" + "="*70)
                print("❌ GA4 Data API not enabled")
                print("="*70)
                print("\nEnable it by running:")
                print("   gcloud services enable analyticsdata.googleapis.com --project=websitecountryspikes")
                print("\nOr enable it in the Google Cloud Console:")
                print("   https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com")
                print("\n" + "="*70 + "\n")
                raise PermissionError("GA4 Data API not enabled. See instructions above.")
            
            raise PermissionError(f"Permission denied: {response.text}")
        
        response.raise_for_status()
        data = response.json()
        
        rows = []
        for row in data.get('rows', []):
            date_str = row['dimensionValues'][0]['value']
            country = row['dimensionValues'][1]['value']
            users = int(row['metricValues'][0]['value'])
            
            if country == "(not set)":
                continue
            
            date_obj = datetime.strptime(date_str, "%Y%m%d").date()
            days_since_monday = date_obj.weekday()
            week_start = date_obj - timedelta(days=days_since_monday)
            
            rows.append({
                'week_start': week_start,
                'country': country,
                'total_views': users,
                'date': date_obj
            })
        
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        
        df_weekly = df.groupby(['week_start', 'country'])['total_views'].sum().reset_index()
        return df_weekly
        
    except PermissionError:
        raise
    except Exception as e:
        raise Exception(f"GA4 API request failed: {e}\n"
                       f"You may need to:\n"
                       f"1. Enable GA4 Data API: https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com\n"
                       f"2. Re-authenticate: gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform\n"
                       f"3. Verify you have access to property {property_id}")

def get_data_from_api(property_id: str, weeks: int, tz: str, service_account_key: str = None):
    """Get historical data from GA4 API."""
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=weeks)
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    print(f"Querying GA4 API from {start_str} to {end_str}…")
    df = query_ga4_api(property_id, start_str, end_str, service_account_key)
    
    if not df.empty:
        print(f"Found {len(df)} country-week combinations from API")
    return df

def build_parser():
    p = argparse.ArgumentParser(description="GA4 Country Weekly Views Report")
    p.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project ID")
    p.add_argument("--dataset", default=DEFAULT_DATASET, help="GA4 BigQuery dataset ID (analytics_########)")
    p.add_argument("--timezone", default=DEFAULT_TZ, help="IANA timezone, e.g., America/Los_Angeles")
    p.add_argument("--weeks", type=int, default=DEFAULT_WEEKS, help="Number of weeks to look back")
    p.add_argument("--property-id", default=DEFAULT_PROPERTY_ID, help="GA4 Property ID (for API access)")
    p.add_argument("--use-api", action="store_true", help="Use GA4 Reporting API instead of BigQuery")
    p.add_argument("--merge-bq", action="store_true", help="Merge API data with BigQuery (API for history, BQ for recent)")
    p.add_argument("--check-dates", action="store_true", help="Check available date range in the table")
    p.add_argument("--service-account-key", help="Path to service account JSON key file (for API access)")
    return p

def run_query(client: bigquery.Client, sql: str):
    job = client.query(sql)
    return job.result().to_dataframe(create_bqstorage_client=True)

def save_text_file(df: pd.DataFrame, out_dir=".", prefix="country_weekly_views"):
    ts = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"{prefix}_{ts}.txt")
    with open(path, 'w') as f:
        # Write header
        f.write("week\tcountry\ttotal_views\n")
        # Write data
        for _, row in df.iterrows():
            f.write(f"{row['week_start']}\t{row['country']}\t{row['total_views']}\n")
    return path

def main():
    args = build_parser().parse_args()

    if args.check_dates:
        client = bigquery.Client(project=args.project)
        print("Checking available date range in BigQuery table…")
        date_sql = DATE_RANGE_SQL.format(
            tz=args.timezone,
            project=args.project,
            dataset=args.dataset,
        )
        date_df = run_query(client, date_sql)
        print("\nAvailable data in BigQuery table:")
        print(date_df.to_string(index=False))
        print()
        return

    df = None
    df_api = None
    df_bq = None

    # Get data from GA4 API if requested
    if args.use_api or args.merge_bq:
        if not args.property_id:
            print("ERROR: --property-id required when using --use-api or --merge-bq")
            print("Set GA_PROPERTY_ID env var or use --property-id flag")
            return
        
        service_account_key = args.service_account_key or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if service_account_key and not os.path.exists(service_account_key):
            print(f"ERROR: Service account key file not found: {service_account_key}")
            return
        
        df_api = get_data_from_api(args.property_id, args.weeks, args.timezone, service_account_key)
        if args.use_api:
            df = df_api

    # Get data from BigQuery if not using API only, or if merging
    if not args.use_api:
        client = bigquery.Client(project=args.project)
        sql = SQL_TEMPLATE.format(
            tz=args.timezone,
            project=args.project,
            dataset=args.dataset,
            weeks=args.weeks,
        )

        print(f"Querying last {args.weeks} weeks from BigQuery…")
        df_bq = run_query(client, sql)
        
        if not args.merge_bq:
            df = df_bq

    # Merge API and BigQuery data if requested
    if args.merge_bq and df_api is not None and df_bq is not None:
        print("\nMerging API and BigQuery data…")
        # Combine dataframes, preferring BigQuery for overlapping periods
        df_combined = pd.concat([df_api, df_bq])
        # Remove duplicates, keeping BigQuery data when there's overlap
        df_combined = df_combined.drop_duplicates(subset=['week_start', 'country'], keep='last')
        # Re-aggregate in case of any overlaps
        df = df_combined.groupby(['week_start', 'country'])['total_views'].sum().reset_index()
        df = df.sort_values(['week_start', 'total_views'], ascending=[False, False])
        print(f"Merged: {len(df)} country-week combinations")

    if df is None or df.empty:
        print("No data found for the specified period.")
        return

    print(f"\nFound {len(df)} country-week combinations across {df['week_start'].nunique()} weeks")
    print(f"Total countries: {df['country'].nunique()}")
    if df['week_start'].nunique() < args.weeks:
        print(f"\n⚠️  WARNING: Only {df['week_start'].nunique()} weeks of data found, but {args.weeks} weeks requested.")
        if not args.use_api:
            print("   Consider using --use-api to pull historical data from GA4 API.")

    txt_path = save_text_file(df)
    print(f"\nSaved report → {txt_path}")

if __name__ == "__main__":
    main()
