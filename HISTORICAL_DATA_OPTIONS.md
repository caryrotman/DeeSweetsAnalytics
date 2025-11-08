# Getting 20 Weeks of Historical Data

## Current Status
- ✅ BigQuery export contains **2 weeks** of data (Nov 2-6, 2025)
- ❌ GA4 API requires additional authentication setup

## Available Data File
- `country_weekly_views_20251106_170107.txt` - Contains 2 weeks of data (all available in BigQuery)

## Options for Historical Data

### Option A: CSV Export from GA4 UI (Recommended - Fastest)
1. Go to GA4: Reports → Demographics → Country
2. Set date range to last 20 weeks
3. Export → Download CSV
4. The CSV will have country and user data by date
5. You can then import this into the script or manually merge with BigQuery data

### Option B: Fix GA4 API Authentication
The API authentication failed due to insufficient scopes. To fix:

1. **Enable GA4 Data API** in Google Cloud Console:
   - Go to: https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com
   - Enable the "Google Analytics Data API"

2. **Verify property access**:
   - Ensure your Google account has Viewer or Editor access to GA4 Property ID: 427048881

3. **Re-authenticate with proper scopes**:
   ```bash
   # Delete old credentials first (optional)
   del "%APPDATA%\gcloud\application_default_credentials.json"
   
   # Re-authenticate
   gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform
   ```

4. **Run with API**:
   ```bash
   python country_spike_report.py --use-api --weeks 20 --property-id 427048881
   ```

### Option C: Use Both (API + BigQuery merge)
Once API is working:
```bash
python country_spike_report.py --use-api --merge-bq --weeks 20 --property-id 427048881
```
This will use API for historical data and BigQuery for recent data.

## Current File Contents
The generated file contains:
- Format: `week` (tab) `country` (tab) `total_views`
- Weeks: 2025-10-27, 2025-11-03
- 128 countries with data
- 213 country-week combinations

