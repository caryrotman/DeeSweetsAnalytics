#!/usr/bin/env python3
"""Comprehensive API troubleshooting script."""

import os
import json
import requests
from google.auth import default
from google.auth.transport.requests import Request

print("="*70)
print("GA4 API Authentication Troubleshooting")
print("="*70)

# Check 1: Credentials file
print("\n1. Checking credentials file...")
creds_path = os.path.join(os.getenv('APPDATA'), 'gcloud', 'application_default_credentials.json')
if os.path.exists(creds_path):
    print(f"   ✅ Credentials file exists: {creds_path}")
    try:
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
            print(f"   Type: {creds_data.get('type', 'unknown')}")
            if 'quota_project_id' in creds_data:
                print(f"   Quota project: {creds_data['quota_project_id']}")
    except Exception as e:
        print(f"   ⚠️  Could not read credentials: {e}")
else:
    print(f"   ❌ Credentials file not found: {creds_path}")

# Check 2: Token info
print("\n2. Checking access token...")
try:
    credentials, project = default(scopes=['https://www.googleapis.com/auth/analytics.readonly'])
    if not credentials.valid:
        credentials.refresh(Request())
    
    token = credentials.token
    print(f"   ✅ Token obtained")
    print(f"   Token length: {len(token)} characters")
    
    # Get token info
    r = requests.get(f'https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={token}')
    if r.status_code == 200:
        info = r.json()
        print(f"   ✅ Token is valid")
        print(f"   Scope: {info.get('scope', 'N/A')}")
        print(f"   Expires in: {info.get('expires_in', 'N/A')} seconds")
        print(f"   Audience: {info.get('audience', 'N/A')}")
        
        # Check if analytics scope is present
        scope_str = info.get('scope', '')
        if 'analytics' in scope_str.lower():
            print(f"   ✅ Analytics scope found in token")
        else:
            print(f"   ⚠️  Analytics scope NOT found in token")
            print(f"   This is likely the problem!")
    else:
        print(f"   ❌ Token invalid: {r.status_code}")
        print(f"   Response: {r.text[:200]}")
        
except Exception as e:
    print(f"   ❌ Error getting token: {e}")

# Check 3: Test API call with different scopes
print("\n3. Testing API calls with different scope requests...")

# Try with analytics.readonly explicitly
try:
    creds_analytics, _ = default(scopes=['https://www.googleapis.com/auth/analytics.readonly'])
    if not creds_analytics.valid:
        creds_analytics.refresh(Request())
    
    url = "https://analyticsdata.googleapis.com/v1beta/properties/427048881:runReport"
    headers = {
        'Authorization': f'Bearer {creds_analytics.token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "dateRanges": [{"startDate": "2025-11-01", "endDate": "2025-11-06"}],
        "dimensions": [{"name": "country"}],
        "metrics": [{"name": "activeUsers"}],
        "limit": 1
    }
    
    response = requests.post(url, json=payload, headers=headers, timeout=10)
    print(f"   With analytics.readonly scope: Status {response.status_code}")
    if response.status_code == 200:
        print(f"   ✅ SUCCESS! API is working!")
    elif response.status_code == 403:
        error = response.json()
        print(f"   ❌ Still getting 403: {error.get('error', {}).get('message', 'Unknown error')}")
    else:
        print(f"   Response: {response.text[:200]}")
except Exception as e:
    print(f"   Error: {e}")

# Check 4: OAuth consent screen
print("\n4. OAuth Consent Screen Configuration...")
print("   The issue might be in OAuth consent screen settings.")
print("   Check: https://console.cloud.google.com/apis/credentials/consent?project=websitecountryspikes")
print("   Make sure:")
print("   - Publishing status is set appropriately")
print("   - Scopes include Analytics API")
print("   - Test users (if in testing mode) include your account")

# Check 5: Property access
print("\n5. Property Access...")
print("   Verify in GA4: Admin → Property Access Management")
print("   Your account should have Viewer or Editor access to property 427048881")

print("\n" + "="*70)
print("Recommendations:")
print("="*70)
print("1. Check OAuth consent screen configuration")
print("2. Try creating a service account (more reliable for API access)")
print("3. Verify property access in GA4")
print("4. Check if there are any domain restrictions")
print("="*70)

