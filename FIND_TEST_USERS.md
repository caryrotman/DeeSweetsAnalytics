# Finding Test Users in OAuth Consent Screen

## Direct Link:
https://console.cloud.google.com/apis/credentials/consent?project=websitecountryspikes

## Steps:

1. **Open the link above** (or go to: APIs & Services → OAuth consent screen)

2. **Look at the top of the page:**
   - Does it say "Testing" or "In production"?
   - This is usually shown as a banner or status indicator at the top

3. **If it says "Testing":**
   - Scroll down the page
   - Look for a section called **"Test users"** or **"User access"**
   - You should see a list of email addresses or an "ADD USERS" button
   - If your email is NOT in the list, click "ADD USERS" and add your Google account email

4. **If it says "In production":**
   - You don't need to add test users
   - The issue might be something else

## What to Look For:

The page should have sections like:
- **Publishing status** (Testing/In production)
- **App information**
- **Scopes** (this is where you added analytics.readonly)
- **Test users** (if in Testing mode)

## Quick Check:
After opening the page, tell me:
1. What does it say at the top? (Testing or In production?)
2. Do you see a "Test users" section?
3. Is your email in the test users list?

