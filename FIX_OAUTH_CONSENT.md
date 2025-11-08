# Fix OAuth Consent Screen for GA4 API

## The Problem
Your token has `cloud-platform` scope but NOT `analytics.readonly` scope. This means the OAuth consent screen isn't configured to grant Analytics permissions.

## Solution: Configure OAuth Consent Screen

### Step 1: Open OAuth Consent Screen
1. Go to: https://console.cloud.google.com/apis/credentials/consent?project=websitecountryspikes
2. Or navigate: APIs & Services → OAuth consent screen

### Step 2: Check Configuration
You should see one of these scenarios:

**Scenario A: No OAuth consent screen configured**
- You'll see a setup wizard
- Choose "Internal" (if you have Google Workspace) or "External"
- Fill in required fields (App name, User support email, Developer contact)

**Scenario B: OAuth consent screen exists**
- Check the "Scopes" section
- Make sure Analytics API scopes are added

### Step 3: Add Analytics Scopes
1. In the OAuth consent screen, click "ADD OR REMOVE SCOPES"
2. Search for "analytics" or "Google Analytics"
3. Add these scopes:
   - `https://www.googleapis.com/auth/analytics.readonly`
   - Or `https://www.googleapis.com/auth/analytics` (if readonly isn't available)
4. Click "UPDATE" or "SAVE"

### Step 4: Check Publishing Status
- If status is "Testing":
  - Add your Google account email to "Test users"
  - Or change to "In production" (if appropriate for your use case)

### Step 5: Re-authenticate
After updating scopes:
```powershell
del "$env:APPDATA\gcloud\application_default_credentials.json"
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform
# When browser opens, you should now see Analytics permissions
# Make sure to click "Allow" for all permissions
```

## Alternative: Use Service Account (More Reliable)

If OAuth consent screen is too complex, we can create a service account:

1. Go to: APIs & Services → Credentials → Create Credentials → Service Account
2. Create service account
3. Grant it Analytics Viewer role
4. Download JSON key
5. Use it in the script

Let me know which approach you prefer!

