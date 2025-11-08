# How to Enable GA4 Data API - Step by Step

## Step 1: Open Google Cloud Console

1. Go to: https://console.cloud.google.com/
2. Make sure you're signed in with the same Google account that has access to your GA4 property

## Step 2: Select Your Project

1. At the top of the page, click the project dropdown (it might say "Select a project" or show a project name)
2. Select or search for: **websitecountryspikes**
3. If you don't see it, you may need to create it or be granted access

## Step 3: Navigate to APIs & Services

1. In the left sidebar, click **"APIs & Services"** (or look for it under the hamburger menu ☰)
2. If you don't see it, click **"APIs & Services"** → **"Library"**

## Step 4: Search for GA4 Data API

1. In the search bar at the top, type: **"Google Analytics Data API"** or **"analyticsdata"**
2. Click on **"Google Analytics Data API"** from the results

## Step 5: Enable the API

1. On the API page, click the blue **"ENABLE"** button
2. Wait for it to enable (usually takes 10-30 seconds)
3. You should see a green checkmark or "API enabled" message

## Step 6: Verify It's Enabled

1. Go back to: **APIs & Services** → **Enabled APIs** (or **Library** → **Enabled APIs**)
2. You should see **"Google Analytics Data API"** in the list

## Step 7: Re-authenticate (Important!)

After enabling the API, you need to refresh your authentication:

1. Open PowerShell or Command Prompt
2. Run these commands:

```powershell
# Delete old credentials
del "$env:APPDATA\gcloud\application_default_credentials.json"

# Re-authenticate (this will open a browser)
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform

# When the browser opens, make sure to click "Allow" or "Continue" for ALL permissions
# Look for any mention of "Analytics" or "Google Analytics" in the permissions list

# Set quota project
gcloud auth application-default set-quota-project websitecountryspikes
```

## Step 8: Test It

Run the script again:
```powershell
python country_spike_report.py --use-api --weeks 20 --property-id 427048881
```

## Troubleshooting

**If you get "API not enabled" error:**
- Make sure you enabled it in the correct project (websitecountryspikes)
- Wait a minute after enabling, then try again

**If you get "insufficient scopes" error:**
- Make sure you clicked "Allow" on ALL permissions when the browser opened
- Try deleting credentials and re-authenticating again

**If you can't find the API:**
- Make sure you're in the correct Google Cloud project
- Try this direct link: https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com?project=websitecountryspikes

**If you don't have access to the project:**
- You may need to be added as a member to the Google Cloud project
- Contact the project owner to grant you access

