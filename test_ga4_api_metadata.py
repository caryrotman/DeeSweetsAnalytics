#!/usr/bin/env python3
"""
Test script to query GA4 API metadata and see exactly what dimensions/metrics are available.
This will show you the complete list of fields you can use with the Data API.
"""

import os
from google.analytics.data import BetaAnalyticsDataClient
from google.oauth2 import service_account

PROPERTY_ID = "427048881"
SERVICE_ACCOUNT_KEY = "websitecountryspikes-c44a6b026c7b.json"

def get_metadata():
    """Get metadata for available dimensions and metrics."""
    
    # Authenticate
    if os.path.exists(SERVICE_ACCOUNT_KEY):
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        client = BetaAnalyticsDataClient(credentials=credentials)
    else:
        client = BetaAnalyticsDataClient()
    
    # Get metadata
    metadata = client.get_metadata(name=f"properties/{PROPERTY_ID}/metadata")
    
    print("="*80)
    print("GA4 DATA API - AVAILABLE DIMENSIONS")
    print("="*80)
    
    user_related = []
    session_related = []
    geo_related = []
    other_dims = []
    
    for dimension in metadata.dimensions:
        if 'user' in dimension.api_name.lower():
            user_related.append(dimension)
        elif 'session' in dimension.api_name.lower():
            session_related.append(dimension)
        elif any(x in dimension.api_name.lower() for x in ['country', 'city', 'region', 'continent']):
            geo_related.append(dimension)
        else:
            other_dims.append(dimension)
    
    print(f"\n{'USER-RELATED DIMENSIONS:':<50} (Count: {len(user_related)})")
    print("-"*80)
    for dim in user_related:
        print(f"  {dim.api_name:<40} {dim.ui_name}")
    
    print(f"\n{'SESSION-RELATED DIMENSIONS:':<50} (Count: {len(session_related)})")
    print("-"*80)
    for dim in session_related:
        print(f"  {dim.api_name:<40} {dim.ui_name}")
    
    print(f"\n{'GEOGRAPHIC DIMENSIONS:':<50} (Count: {len(geo_related)})")
    print("-"*80)
    for dim in geo_related:
        print(f"  {dim.api_name:<40} {dim.ui_name}")
    
    print("\n" + "="*80)
    print("GA4 DATA API - AVAILABLE METRICS")
    print("="*80)
    
    user_metrics = []
    session_metrics = []
    event_metrics = []
    other_metrics = []
    
    for metric in metadata.metrics:
        if 'user' in metric.api_name.lower():
            user_metrics.append(metric)
        elif 'session' in metric.api_name.lower():
            session_metrics.append(metric)
        elif 'event' in metric.api_name.lower():
            event_metrics.append(metric)
        else:
            other_metrics.append(metric)
    
    print(f"\n{'USER-RELATED METRICS:':<50} (Count: {len(user_metrics)})")
    print("-"*80)
    for metric in user_metrics:
        print(f"  {metric.api_name:<40} {metric.ui_name}")
    
    print(f"\n{'SESSION-RELATED METRICS:':<50} (Count: {len(session_metrics)})")
    print("-"*80)
    for metric in session_metrics:
        print(f"  {metric.api_name:<40} {metric.ui_name}")
    
    print(f"\n{'EVENT-RELATED METRICS:':<50} (Count: {len(event_metrics)})")
    print("-"*80)
    for metric in event_metrics:
        print(f"  {metric.api_name:<40} {metric.ui_name}")
    
    print("\n" + "="*80)
    print(f"TOTALS: {len(metadata.dimensions)} dimensions, {len(metadata.metrics)} metrics")
    print("="*80)
    
    # Check specifically for user identification fields
    print("\n" + "="*80)
    print("CHECKING FOR USER IDENTIFICATION FIELDS")
    print("="*80)
    
    user_id_fields = ['userId', 'user_id', 'userPseudoId', 'user_pseudo_id', 'clientId', 'client_id']
    found_fields = []
    
    for field_name in user_id_fields:
        found = False
        for dim in metadata.dimensions:
            if field_name.lower() == dim.api_name.lower():
                found_fields.append(f"✅ Found: {dim.api_name} ({dim.ui_name})")
                found = True
                break
        if not found:
            found_fields.append(f"❌ Not found: {field_name}")
    
    for result in found_fields:
        print(result)
    
    print("\n" + "="*80)
    print("CONCLUSION:")
    print("="*80)
    print("For repeat visitor analysis, we need user-level identifiers.")
    print("If none of the user ID fields above are available (all ❌),")
    print("then the GA4 Data API CANNOT be used for repeat visitor tracking.")
    print("BigQuery export is the only option (has user_pseudo_id).")
    print("="*80)

if __name__ == "__main__":
    try:
        get_metadata()
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure you have the service account key file in the current directory.")

