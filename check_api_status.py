#!/usr/bin/env python3
"""Quick script to check if GA4 Data API is accessible."""

import requests
from google.auth import default
from google.auth.transport.requests import Request

try:
    credentials, project = default(scopes=['https://www.googleapis.com/auth/analytics.readonly'])
    if not credentials.valid:
        credentials.refresh(Request())
    
    # Test API access
    url = "https://analyticsdata.googleapis.com/v1beta/properties/427048881:runReport"
    headers = {
        'Authorization': f'Bearer {credentials.token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "dateRanges": [{"startDate": "2025-11-01", "endDate": "2025-11-06"}],
        "dimensions": [{"name": "country"}],
        "metrics": [{"name": "activeUsers"}],
        "limit": 1
    }
    
    response = requests.post(url, json=payload, headers=headers, timeout=10)
    
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print("✅ API is accessible!")
    elif response.status_code == 403:
        error = response.json()
        if 'insufficient authentication scopes' in str(error).lower():
            print("❌ Authentication scope issue")
            print("\nFix: Enable GA4 Data API at:")
            print("https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com?project=websitecountryspikes")
        else:
            print(f"❌ Permission denied: {error}")
    elif response.status_code == 404:
        print("❌ API not found - may need to enable GA4 Data API")
    else:
        print(f"Response: {response.text[:200]}")
        
except Exception as e:
    print(f"Error: {e}")

