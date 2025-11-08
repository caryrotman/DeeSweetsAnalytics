#!/usr/bin/env python3
"""Verify GA4 API setup and permissions."""

print("="*70)
print("GA4 API Setup Verification")
print("="*70)

# Check 1: API enabled
print("\n1. Checking if API is enabled...")
print("   Visit: https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com?project=websitecountryspikes")
print("   Should show: 'API enabled' or 'MANAGE' button (not 'ENABLE')")

# Check 2: Credentials
print("\n2. Checking credentials...")
try:
    from google.auth import default
    from google.auth.transport.requests import Request
    import requests
    
    creds, project = default()
    creds.refresh(Request())
    
    # Check token info
    r = requests.get(f'https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={creds.token}')
    if r.status_code == 200:
        info = r.json()
        print(f"   ✅ Credentials valid")
        print(f"   Scope: {info.get('scope', 'N/A')}")
        print(f"   Expires in: {info.get('expires_in', 'N/A')} seconds")
    else:
        print(f"   ❌ Token invalid: {r.status_code}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Check 3: Property access
print("\n3. Property access check...")
print("   In GA4, go to: Admin → Property Access Management")
print("   Verify your account has Viewer or Editor access to property 427048881")

# Check 4: OAuth consent
print("\n4. OAuth consent screen...")
print("   When you authenticated, did you see a screen asking for permissions?")
print("   Did you click 'Allow' or 'Continue'?")
print("   If you clicked 'Deny' or didn't see Analytics permissions, that's the issue.")

print("\n" + "="*70)
print("Next steps if still not working:")
print("1. Verify API is enabled (step 1 above)")
print("2. Check property access in GA4 (step 3 above)")
print("3. Try using BigQuery instead (already working):")
print("   python country_spike_report.py --weeks 20")
print("="*70)

