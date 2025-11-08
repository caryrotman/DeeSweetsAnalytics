# Final OAuth Fix Steps

## The Issue
Even though you added `analytics.readonly` to the OAuth consent screen, the token still doesn't have it because:
1. When we authenticate with `--scopes=https://www.googleapis.com/auth/cloud-platform`, it only requests that specific scope
2. The Analytics API requires explicit `analytics.readonly` scope

## Solution Options

### Option 1: Check OAuth Consent Screen Status (Most Likely Fix)

1. Go to: https://console.cloud.google.com/apis/credentials/consent?project=websitecountryspikes
2. Look at the top - what does it say?
   - If it says **"Testing"**:
     - Scroll to "Test users" section
     - Add your email as a test user
     - This is REQUIRED for testing mode!
   - If it says "In production", skip to Option 2

### Option 2: Try Explicit Analytics Scope Request

We can try modifying the authentication to explicitly request analytics scope, but gcloud might not support this easily.

### Option 3: Use Service Account (Most Reliable)

Service accounts are more reliable for API access:

1. Go to: APIs & Services → Credentials → Create Credentials → Service Account
2. Create service account
3. Grant it "Viewer" role for Analytics
4. Download JSON key
5. Use it in the script

### Option 4: Use BigQuery (Working Now)

BigQuery is already working and has data. We can use that for now:
```bash
python country_spike_report.py --weeks 20
```
(It will only have 2 weeks because that's what's in BigQuery, but it works)

## What to Check Right Now:
1. Is OAuth consent screen in "Testing" mode?
2. If yes, are you added as a test user?
3. When you authenticate, does the browser show Analytics permissions in the list?

