# Finding Scopes in OAuth Consent Screen

## The scopes are in "Data Access", not Overview!

1. **In the left sidebar**, click **"Data Access"** (it has a gear icon)
   - It's below "Clients" in the navigation menu

2. In the **Data Access** page, you should see:
   - A section about scopes/permissions
   - An "ADD OR REMOVE SCOPES" button or similar
   - Or a list of currently configured scopes

3. **Add Analytics scope:**
   - Click "ADD OR REMOVE SCOPES" or "Edit scopes"
   - Search for: `analytics`
   - Add: `https://www.googleapis.com/auth/analytics.readonly`
   - Save

## Alternative: Check "Settings"

If "Data Access" doesn't show scopes, try:
- Click **"Settings"** in the left sidebar
- Look for scope-related configuration there

## Direct Link (if available):
The scopes might be at a URL like:
https://console.cloud.google.com/apis/credentials/consent?project=websitecountryspikes

But the new interface might have moved things around. The "Data Access" section is the most likely place.

