# OAuth Consent Screen Setup Steps

## Step 1: App Information (Current Step)

Fill in the required fields:

1. **App name***: 
   - Enter: `Dee & Sweets Analytics` (or any name you prefer)
   - This is just for identification

2. **User support email***:
   - Click the dropdown
   - Select your email address (the one you use for Google Cloud)
   - This is where users can contact you about permissions

3. Click **"Next"** button (blue button at bottom)

## Step 2: Audience

1. Choose **"Internal"** if you have Google Workspace for your organization
   - OR choose **"External"** if this is a personal project
   - For most cases, choose **"External"**

2. Click **"Next"**

## Step 3: Contact Information

1. **Developer contact email***:
   - Enter your email address
   - This is for Google to contact you about the app

2. Click **"Next"** or **"Save and Continue"**

## Step 4: Finish / Scopes

After completing the initial setup, you'll be able to:
1. Go to the **"Scopes"** section (in the left menu)
2. Click **"ADD OR REMOVE SCOPES"**
3. Search for "analytics"
4. Add: `https://www.googleapis.com/auth/analytics.readonly`
5. Save

## Quick Summary

Just fill in:
- App name: `Dee & Sweets Analytics`
- User support email: (your email)
- Choose External audience
- Developer contact: (your email)
- Then add Analytics scopes

