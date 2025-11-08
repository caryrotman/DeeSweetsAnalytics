# GA4 Data API vs BigQuery Export - Field Comparison

## Available GA4 API Calls

### 1. **GA4 Data API v1beta** (What we're using)

#### Main Methods:
- **`runReport`** - Returns customized reports of GA4 event data
  - URL: `https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}:runReport`
  - Used for: Standard historical reporting (aggregated data)
  - Limits: Up to 9 dimensions, 10 metrics per request
  
- **`batchRunReports`** - Returns multiple reports in a single request
  - URL: `https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}:batchRunReports`
  - Used for: Running multiple report queries at once
  
- **`runRealtimeReport`** - Returns real-time event data
  - URL: `https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}:runRealtimeReport`
  - Used for: Last 30 minutes of data
  
- **`runPivotReport`** - Returns pivoted dimension data
  - URL: `https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}:runPivotReport`
  - Used for: Multi-dimensional analysis with pivots
  
- **`batchRunPivotReports`** - Multiple pivot reports in one request
  - URL: `https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}:batchRunPivotReports`

- **`getMetadata`** - Gets available dimensions/metrics for your property
  - URL: `https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}/metadata`
  - Used for: Discovering what fields are available

### 2. **Audience Export API** (v1alpha)
- **Purpose:** Export audience membership snapshots
- **Key Feature:** Supports `userId` dimension (only API that does)
- **Limitation:** Not for historical behavior analysis, only current audience membership
- **URL Pattern:** `https://analyticsdata.googleapis.com/v1alpha/properties/{propertyId}/audienceExports`

### 3. **Admin API** (v1alpha/v1beta)
- **Purpose:** Manage GA4 property configuration
- **Not used for:** Data retrieval/analysis

---

## Field Comparison: API vs BigQuery

### User Identification Fields

| Field | GA4 Data API | BigQuery Export | Notes |
|-------|--------------|-----------------|-------|
| `user_pseudo_id` | ❌ No | ✅ Yes | **Critical for repeat visitor analysis** |
| `userId` | ❌ No* | ✅ Yes | *Only in Audience Export API |
| `user_id` | ❌ No | ✅ Yes | Same as userId (if implemented) |
| `user_first_touch_timestamp` | ❌ No | ✅ Yes | When user first visited |
| `user_ltv` | ❌ No | ✅ Yes | Lifetime value fields |
| `user_properties` | ❌ No | ✅ Yes | Custom user properties |

### Session & Event Fields

| Field | GA4 Data API | BigQuery Export | Notes |
|-------|--------------|-----------------|-------|
| `sessionId` | ⚠️ Limited | ✅ Yes | API has session dimensions but not raw IDs |
| `event_timestamp` | ⚠️ Aggregated | ✅ Yes (raw) | API returns by date, not exact timestamp |
| `event_name` | ✅ Yes | ✅ Yes | Both have this |
| `event_params` | ⚠️ Limited | ✅ Yes (all) | API has some predefined, BQ has all custom |
| `event_bundle_sequence_id` | ❌ No | ✅ Yes | Event ordering within upload bundle |

### Geographic & Device Fields

| Field | GA4 Data API | BigQuery Export | Notes |
|-------|--------------|-----------------|-------|
| `country` | ✅ Yes | ✅ Yes | Both have this |
| `city` | ✅ Yes | ✅ Yes | Both have this |
| `deviceCategory` | ✅ Yes | ✅ Yes | Both have this |
| `browser` | ✅ Yes | ✅ Yes | Both have this |
| `geo.continent` | ⚠️ Different | ✅ Yes | API has aggregated, BQ has nested structure |
| `geo.sub_continent` | ❌ No | ✅ Yes | Only in BigQuery |
| `device.mobile_*` | ❌ No | ✅ Yes | Detailed mobile fields only in BQ |

### Traffic Source Fields

| Field | GA4 Data API | BigQuery Export | Notes |
|-------|--------------|-----------------|-------|
| `source` | ✅ Yes | ✅ Yes | Both have this |
| `medium` | ✅ Yes | ✅ Yes | Both have this |
| `campaign` | ✅ Yes | ✅ Yes | Both have this |
| `traffic_source.source` | ⚠️ Different | ✅ Yes | BQ has more detailed nested structure |
| `collected_traffic_source` | ❌ No | ✅ Yes | First-click attribution only in BQ |

### Metrics Available

| Metric Type | GA4 Data API | BigQuery Export | Notes |
|-------------|--------------|-----------------|-------|
| `activeUsers` | ✅ Yes | ✅ Calculate | Pre-calculated in API, compute in BQ |
| `sessions` | ✅ Yes | ✅ Calculate | Pre-calculated in API, compute in BQ |
| `screenPageViews` | ✅ Yes | ✅ Calculate | Pre-calculated in API, compute in BQ |
| `conversions` | ✅ Yes | ✅ Calculate | Pre-calculated in API, compute in BQ |
| `eventCount` | ✅ Yes | ✅ Calculate | Pre-calculated in API, compute in BQ |
| Custom calculated | ❌ Limited | ✅ Full SQL | BQ allows any calculation |

---

## What We're Currently Using

**Script:** `country_spike_report.py`
- **API Call:** `runReport`
- **Endpoint:** `https://analyticsdata.googleapis.com/v1beta/properties/427048881:runReport`
- **Dimensions:** `date`, `country`
- **Metrics:** `activeUsers`
- **Date Range:** Configurable (last 20 weeks)

**Script:** `repeat_visitors.py`
- **Attempted API Call:** `runReport` with `date`, `userId`
- **Result:** ❌ Failed - `userId` not available
- **Fallback:** BigQuery with `user_pseudo_id`

---

## Key Differences Summary

### GA4 Data API Strengths:
- ✅ Easy to use, no SQL required
- ✅ Pre-calculated metrics
- ✅ Can query historical data (not just recent exports)
- ✅ Real-time data available
- ✅ No additional costs

### GA4 Data API Limitations:
- ❌ **No user-level identifiers** (`user_pseudo_id`, `userId`)
- ❌ Aggregated data only (no raw events)
- ❌ Limited custom dimensions/metrics
- ❌ Limited event parameters
- ❌ 9 dimension, 10 metric per request limit

### BigQuery Export Strengths:
- ✅ **Has `user_pseudo_id` for ALL users**
- ✅ Raw event-level data with exact timestamps
- ✅ All event parameters and user properties
- ✅ Full SQL querying capabilities
- ✅ Can join with other data sources
- ✅ No request limits

### BigQuery Export Limitations:
- ❌ Only data since export was enabled (no backfill)
- ❌ Requires SQL knowledge
- ❌ Additional costs for storage and queries
- ❌ 24-48 hour delay for data
- ❌ No real-time data

---

## For Repeat Visitor Analysis

**Requirement:** Track individual users across multiple visits over time

**API Approach:** ❌ **NOT POSSIBLE**
- `userId` not available in Data API (only in Audience Export API, which is for snapshots)
- `user_pseudo_id` not available at all
- Cannot identify same user across multiple days/sessions

**BigQuery Approach:** ✅ **REQUIRED**
- Has `user_pseudo_id` in every event row
- Can GROUP BY `user_pseudo_id` and count distinct dates
- Can track full user journey over time

---

## Official Documentation Links

1. **GA4 Data API Schema (all dimensions/metrics):**
   https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema

2. **GA4 Dimensions & Metrics Explorer (interactive):**
   https://ga-dev-tools.web.app/ga4/dimensions-metrics-explorer/

3. **GA4 Data API Guide:**
   https://developers.google.com/analytics/devguides/reporting/data/v1

4. **BigQuery Export Schema:**
   https://support.google.com/analytics/answer/7029846

5. **Audience Export API Schema:**
   https://developers.google.com/analytics/devguides/reporting/data/v1/audience-export-api-schema

6. **API Method Reference:**
   https://developers.google.com/analytics/devguides/reporting/data/v1/rest

---

## Conclusion for Your Use Case

**For tracking repeat visitors over 3 weeks:**
- ❌ GA4 Data API cannot do this
- ✅ BigQuery is the only option
- ⚠️ Your BigQuery only has 1 week of data currently

**Your options:**
1. Wait for BigQuery to accumulate more data over time
2. Analyze the 1 week you have
3. Check if you can request historical backfill (rare, property-dependent)

