# Service Account Setup for GA4 API

## Step 1: Create Service Account

1. Go to: https://console.cloud.google.com/iam-admin/serviceaccounts?project=websitecountryspikes
   - Or navigate: IAM & Admin → Service Accounts

2. Click **"CREATE SERVICE ACCOUNT"** (top of page)

3. Fill in:
   - **Service account name**: `ga4-analytics-reader` (or any name)
   - **Service account ID**: Will auto-fill (can leave as is)
   - **Description**: "Service account for GA4 Analytics API access"
   - Click **"CREATE AND CONTINUE"**

## Step 2: Grant Permissions

1. In "Grant this service account access to project":
   - **Role**: Search for and select **"Viewer"** (basic role)
   - Click **"CONTINUE"**

2. Click **"DONE"** (you can skip optional steps)

## Step 3: Grant GA4 Access

1. Go to GA4: https://analytics.google.com/
2. Navigate: **Admin** (gear icon) → **Property Access Management**
3. Click **"+"** or **"Add users"**
4. Enter the service account email (format: `ga4-analytics-reader@websitecountryspikes.iam.gserviceaccount.com`)
   - You can find this in Google Cloud Console → Service Accounts → your service account
5. Select role: **"Viewer"**
6. Click **"Add"**

## Step 4: Create and Download Key

1. Back in Google Cloud Console → Service Accounts
2. Click on your service account name
3. Go to **"KEYS"** tab
4. Click **"ADD KEY"** → **"Create new key"**
5. Select **"JSON"**
6. Click **"CREATE"**
7. The JSON file will download automatically
8. **Save it securely** - you'll need it for the script

## Step 5: Update Script

The script will be updated to use the service account. You'll need to:
- Set environment variable: `GOOGLE_APPLICATION_CREDENTIALS` to point to the JSON file
- Or pass `--service-account-key` flag with the path to the JSON file

## Quick Summary:
1. Create service account in Google Cloud
2. Grant it GA4 Viewer access
3. Download JSON key
4. Use it in the script

